from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file, make_response
import pyodbc
import pandas as pd
import io
import tempfile
import os
from datetime import datetime, timedelta
from auth import auth_bp, require_auth

app = Flask(__name__)

# Session configuration
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'heritage-foods-gap-analysis-2025-secure-key')
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=8)

# Register blueprints
app.register_blueprint(auth_bp)

from routes.data_routes import data_bp  # Adjust path as needed
app.register_blueprint(data_bp)


# Database configuration
DB_CONFIG = {
    'server': '202.53.88.202,4000',
    'database': 'wordDB',
    'username': 'HFLSQLReader',
    'password': 'HFL@12345',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Reasons for non-fulfillment
REASONS = [
    "Product non Availability at Factory",
    "Product non Availability at SO",
    "Product non availability at CFA",
    "Supply not made as per PO time lines",
    "PO price issue",
    "Appointment issues",
    "Supply rejected by customer",
    "Mutiple point Delivery",
    "Due to Quality Issue",
    "Due to Delayed Delivery"
]

def get_db_connection():
    """Create database connection"""
    try:
        conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

@app.route('/')
def home():
    """Home route - redirects to login or dashboard based on session"""
    if 'user_email' in session:
        # Check if session is still valid
        current_time = datetime.now().timestamp()
        login_time = session.get('login_time', 0)
        if current_time - login_time < (8 * 3600):  # 8 hours
            return redirect(url_for('dashboard'))
        else:
            session.clear()
    
    return redirect(url_for('login'))

@app.route('/login')
def login():
    """Login page"""
    if 'user_email' in session:
        return redirect(url_for('dashboard'))
    
    return render_template('login.html')

@app.route('/dashboard')
@require_auth()
def dashboard():
    """Main dashboard page - requires authentication"""
    user_email = session.get('user_email', '')
    return render_template('dashboard.html', user_email=user_email)

@app.route('/reports')
@require_auth()
def feedback_reports():
    """Feedback reports page - requires authentication"""
    user_email = session.get('user_email', '')
    return render_template('reports.html', user_email=user_email)

@app.route('/api/dashboard-stats')
@require_auth()
def get_dashboard_stats():
    """Get dashboard statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM zepto_automation")
        total_records = cursor.fetchone()[0]
        
        # Low fill rate records
        cursor.execute("""
            SELECT COUNT(*) FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
        """)
        low_fill_rate = cursor.fetchone()[0]
        
        # Average fill rate
        cursor.execute("SELECT AVG(Fill_Rate_Percent) FROM zepto_automation WHERE Fill_Rate_Percent > 0")
        avg_fill_rate = cursor.fetchone()[0] or 0
        
        # Records with feedback
        try:
            cursor.execute("""
                SELECT COUNT(*) FROM zepto_automation z
                INNER JOIN fill_rate_feedback f ON z.ID = f.record_id
                WHERE z.Fill_Rate_Percent < 95 
                AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
                AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
            """)
            with_feedback = cursor.fetchone()[0]
        except:
            with_feedback = 0
        
        # Records needing feedback
        needs_feedback = low_fill_rate - with_feedback
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'low_fill_rate_count': low_fill_rate,
            'average_fill_rate': round(avg_fill_rate, 2),
            'needs_feedback': needs_feedback,
            'with_feedback': with_feedback,
            'user_email': session.get('user_email', '')
        })
        
    except Exception as e:
        print(f"Stats error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/low-fill-rate-data')
@require_auth()
def get_low_fill_rate_data():
    """Get records with fill rate < 95% and valid state/plant, including feedback status"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Create feedback table if it doesn't exist
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fill_rate_feedback' AND xtype='U')
        CREATE TABLE fill_rate_feedback (
            id INT IDENTITY(1,1) PRIMARY KEY,
            record_id INT NOT NULL,
            reason NVARCHAR(255) NOT NULL,
            comments NVARCHAR(1000),
            user_email NVARCHAR(255),
            created_at DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        
        query = """
        SELECT z.ID, z.PO_No, z.Material_Description, z.Material, z.PO_Date, z.Delivery_Date, 
               z.UOM, z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent, 
               z.State, z.Plant_Name, z.Sales_District, z.Cust_Group, z.Processing_Date,
               f.reason, f.comments, f.created_at
        FROM zepto_automation z
        LEFT JOIN fill_rate_feedback f ON z.ID = f.record_id
        WHERE z.Fill_Rate_Percent < 95 
        AND z.State IS NOT NULL 
        AND z.State != '' 
        AND z.State != '0'
        AND z.Plant_Name IS NOT NULL 
        AND z.Plant_Name != '' 
        AND z.Plant_Name != '0'
        ORDER BY z.Delivery_Date DESC, z.Processing_Date DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'po_no': row[1],
                'material_description': row[2],
                'material': row[3],
                'po_date': row[4].strftime('%Y-%m-%d') if row[4] and str(row[4]) != '1900-01-01 00:00:00' else '',
                'delivery_date': row[5].strftime('%Y-%m-%d') if row[5] and str(row[5]) != '1900-01-01 00:00:00' else '',
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': row[14].strftime('%Y-%m-%d %H:%M') if row[14] and str(row[14]) != '1900-01-01 00:00:00' else '',
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': row[17].strftime('%Y-%m-%d %H:%M') if row[17] else ''
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Data fetch error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter-options')
@require_auth()
def get_filter_options():
    """Get unique states, plants, and materials for filters"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get unique states
        cursor.execute("""
            SELECT DISTINCT State 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
            ORDER BY State
        """)
        states = [row[0] for row in cursor.fetchall()]
        
        # Get all plants with their states and counts
        cursor.execute("""
            SELECT State, Plant_Name, COUNT(*) as record_count
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
            GROUP BY State, Plant_Name
            ORDER BY State, Plant_Name
        """)
        plants_data = cursor.fetchall()
        
        # Get unique materials
        cursor.execute("""
            SELECT DISTINCT Material_Description 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND Material_Description IS NOT NULL AND Material_Description != ''
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
            ORDER BY Material_Description
        """)
        materials = [row[0] for row in cursor.fetchall()]
        
        # Organize plants by state
        plants_by_state = {}
        for row in plants_data:
            state = row[0]
            plant = row[1]
            count = row[2]
            
            if state not in plants_by_state:
                plants_by_state[state] = []
            
            plants_by_state[state].append({
                'name': plant,
                'count': count
            })
        
        conn.close()
        return jsonify({
            'states': states, 
            'plants_by_state': plants_by_state,
            'materials': materials
        })
        
    except Exception as e:
        print(f"Filter options error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-data')
@require_auth()
def get_filtered_data():
    """Get filtered data based on state, plant, material, and date range"""
    try:
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        material_filter = request.args.get('material', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Build dynamic query based on filters with feedback status
        query = """
        SELECT z.ID, z.PO_No, z.Material_Description, z.Material, z.PO_Date, z.Delivery_Date, 
               z.UOM, z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent, 
               z.State, z.Plant_Name, z.Sales_District, z.Cust_Group, z.Processing_Date,
               f.reason, f.comments, f.created_at
        FROM zepto_automation z
        LEFT JOIN fill_rate_feedback f ON z.ID = f.record_id
        WHERE z.Fill_Rate_Percent < 95 
        AND z.Fill_Rate_Percent IS NOT NULL
        AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
        AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
        """
        
        params = []
        
        if state_filter:
            query += " AND z.State = ?"
            params.append(state_filter)
            
        if plant_filter:
            query += " AND z.Plant_Name = ?"
            params.append(plant_filter)
            
        if material_filter:
            query += " AND z.Material_Description = ?"
            params.append(material_filter)
            
        if date_from:
            query += " AND z.Delivery_Date >= ?"
            params.append(date_from)
            
        if date_to:
            query += " AND z.Delivery_Date <= ?"
            params.append(date_to)
            
        query += " ORDER BY z.Delivery_Date DESC, z.Processing_Date DESC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'po_no': row[1],
                'material_description': row[2],
                'material': row[3],
                'po_date': row[4].strftime('%Y-%m-%d') if row[4] and str(row[4]) != '1900-01-01 00:00:00' else '',
                'delivery_date': row[5].strftime('%Y-%m-%d') if row[5] and str(row[5]) != '1900-01-01 00:00:00' else '',
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': row[14].strftime('%Y-%m-%d') if row[14] and str(row[14]) != '1900-01-01 00:00:00' else '',
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': row[17].strftime('%Y-%m-%d %H:%M') if row[17] else ''
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Filtered data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-data')
@require_auth()
def download_data():
    """Download gap analysis data as Excel file"""
    try:
        # Get filter parameters
        state = request.args.get('state')
        plant = request.args.get('plant')
        material = request.args.get('material')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Build dynamic query for download
        base_query = """
        SELECT z.PO_No, z.Material_Description, z.Material, z.PO_Date, z.Delivery_Date, 
               z.UOM, z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent, 
               z.State, z.Plant_Name, z.Sales_District, z.Cust_Group, z.Processing_Date,
               CASE WHEN f.reason IS NOT NULL THEN f.reason ELSE 'Pending Feedback' END as Feedback_Status,
               ISNULL(f.comments, '') as Feedback_Comments,
               f.created_at as Feedback_Date
        FROM zepto_automation z
        LEFT JOIN fill_rate_feedback f ON z.ID = f.record_id
        WHERE z.Fill_Rate_Percent < 95 
        AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
        AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
        """
        
        conditions = []
        params = []
        
        if state:
            conditions.append("z.State = ?")
            params.append(state)
            
        if plant:
            conditions.append("z.Plant_Name = ?")
            params.append(plant)
            
        if material:
            conditions.append("z.Material_Description = ?")
            params.append(material)
            
        if date_from:
            conditions.append("z.Delivery_Date >= ?")
            params.append(date_from)
            
        if date_to:
            conditions.append("z.Delivery_Date <= ?")
            params.append(date_to)
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
            
        base_query += " ORDER BY z.Delivery_Date DESC, z.Processing_Date DESC"
        
        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries for DataFrame
        data_list = []
        for row in rows:
            data_list.append({
                'PO Number': row[0],
                'Material Description': row[1],
                'Material': row[2],
                'PO Date': row[3].strftime('%Y-%m-%d') if row[3] and str(row[3]) != '1900-01-01 00:00:00' else '',
                'Delivery Date': row[4].strftime('%Y-%m-%d') if row[4] and str(row[4]) != '1900-01-01 00:00:00' else '',
                'UOM': row[5],
                'PO Quantity (L)': float(row[6]) if row[6] else 0,
                'Sales Quantity': float(row[7]) if row[7] else 0,
                'Fill Rate %': float(row[8]) if row[8] else 0,
                'State': row[9],
                'Plant Name': row[10],
                'Sales District': row[11],
                'Customer Group': row[12],
                'Processing Date': row[13].strftime('%Y-%m-%d %H:%M') if row[13] and str(row[13]) != '1900-01-01 00:00:00' else '',
                'Feedback Status': row[14],
                'Feedback Comments': row[15],
                'Feedback Date': row[16].strftime('%Y-%m-%d %H:%M') if row[16] else ''
            })
        
        conn.close()
        
        if not data_list:
            return jsonify({'error': 'No data found for the selected filters'}), 404
        
        # Create DataFrame
        df = pd.DataFrame(data_list)
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        temp_filename = temp_file.name
        temp_file.close()
        
        try:
            # Create Excel file
            with pd.ExcelWriter(temp_filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Gap Analysis Data', index=False)
                
                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Gap Analysis Data']
                
                # Add formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # Format header row
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    
                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    max_length = max(df[col].astype(str).map(len).max(), len(col))
                    worksheet.set_column(i, i, min(max_length + 2, 50))
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            download_filename = f"gap_analysis_data_{timestamp}.xlsx"
            
            # Read the file and create response
            with open(temp_filename, 'rb') as f:
                file_data = f.read()
            
            # Clean up temp file
            os.unlink(temp_filename)
            
            # Create response
            response = make_response(file_data)
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f'attachment; filename="{download_filename}"'
            response.headers['Content-Length'] = len(file_data)
            
            return response
            
        except Exception as e:
            # Clean up temp file in case of error
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
            raise e
        
    except Exception as e:
        print(f"Download error: {e}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.route('/api/reasons')
@require_auth()
def get_reasons():
    """Get list of available reasons"""
    return jsonify({'reasons': REASONS})

@app.route('/api/submit-feedback', methods=['POST'])
@require_auth()
def submit_feedback():
    """Submit feedback for a record"""
    try:
        data = request.get_json()
        record_id = data.get('record_id')
        reason = data.get('reason')
        comments = data.get('comments', '')
        user_email = session.get('user_email', '')
        
        if not record_id or not reason:
            return jsonify({'error': 'Record ID and reason are required'}), 400
            
        if reason not in REASONS:
            return jsonify({'error': 'Invalid reason selected'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Check if feedback table exists, create if not
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fill_rate_feedback' AND xtype='U')
        CREATE TABLE fill_rate_feedback (
            id INT IDENTITY(1,1) PRIMARY KEY,
            record_id INT NOT NULL,
            reason NVARCHAR(255) NOT NULL,
            comments NVARCHAR(1000),
            user_email NVARCHAR(255),
            created_at DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        
        # Check if feedback already exists for this record
        check_query = """
        SELECT COUNT(*) FROM fill_rate_feedback 
        WHERE record_id = ?
        """
        cursor.execute(check_query, (record_id,))
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            conn.close()
            return jsonify({'error': 'Feedback already exists for this record'}), 400
        
        # Insert feedback
        insert_query = """
        INSERT INTO fill_rate_feedback (record_id, reason, comments, user_email) 
        VALUES (?, ?, ?, ?)
        """
        cursor.execute(insert_query, (record_id, reason, comments, user_email))
        
        conn.commit()
        conn.close()
        
        return jsonify({'message': 'Feedback submitted successfully'}), 200
        
    except Exception as e:
        print(f"Feedback submission error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/feedback-history/<int:record_id>')
@require_auth()
def get_feedback_history(record_id):
    """Get feedback history for a record"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Check if table exists first
        check_table = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'fill_rate_feedback'
        """
        cursor.execute(check_table)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            conn.close()
            return jsonify({'feedback_history': []})
        
        query = """
        SELECT reason, comments, user_email, created_at 
        FROM fill_rate_feedback 
        WHERE record_id = ? 
        ORDER BY created_at DESC
        """
        cursor.execute(query, (record_id,))
        rows = cursor.fetchall()
        
        feedback_history = []
        for row in rows:
            feedback_history.append({
                'reason': row[0],
                'comments': row[1],
                'user_email': row[2],
                'created_at': row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else ''
            })
        
        conn.close()
        return jsonify({'feedback_history': feedback_history})
        
    except Exception as e:
        print(f"Feedback history error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/check-feedback/<int:record_id>')
@require_auth()
def check_feedback(record_id):
    """Check if feedback exists for a record"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Check if table exists first
        check_table = """
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'fill_rate_feedback'
        """
        cursor.execute(check_table)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            conn.close()
            return jsonify({'has_feedback': False})
        
        query = """
        SELECT reason, comments, created_at 
        FROM fill_rate_feedback 
        WHERE record_id = ?
        """
        cursor.execute(query, (record_id,))
        row = cursor.fetchone()
        
        if row:
            feedback_data = {
                'has_feedback': True,
                'reason': row[0],
                'comments': row[1],
                'created_at': row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else ''
            }
        else:
            feedback_data = {
                'has_feedback': False
            }
        
        conn.close()
        return jsonify(feedback_data)
        
    except Exception as e:
        print(f"Check feedback error: {e}")
        return jsonify({'error': str(e)}), 500

# NEW FEEDBACK REPORTS API ENDPOINTS

@app.route('/api/feedback-reports-data')
@require_auth()
def get_feedback_reports_data():
    """Get feedback reports data with filters"""
    try:
        # Get filter parameters
        user_filter = request.args.get('user', '')
        reason_filter = request.args.get('reason', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Build dynamic query for feedback reports
            query = """
            SELECT f.id, f.record_id, f.reason, f.comments, f.user_email, f.created_at,
                   z.PO_No, z.Material_Description, z.PO_Date, z.Delivery_Date, 
                   z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent,
                   z.State, z.Plant_Name, z.Sales_District, z.Cust_Group
            FROM fill_rate_feedback f
            INNER JOIN zepto_automation z ON f.record_id = z.ID
            WHERE 1=1
            """
            
            params = []
            
            if user_filter:
                query += " AND f.user_email LIKE ?"
                params.append(f'%{user_filter}%')
                
            if reason_filter:
                query += " AND f.reason = ?"
                params.append(reason_filter)
                
            if date_from:
                query += " AND f.created_at >= ?"
                params.append(date_from)
                
            if date_to:
                query += " AND f.created_at <= ?"
                params.append(date_to + ' 23:59:59')
                
            if state_filter:
                query += " AND z.State = ?"
                params.append(state_filter)
                
            if plant_filter:
                query += " AND z.Plant_Name = ?"
                params.append(plant_filter)
                
            query += " ORDER BY f.created_at DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert rows to list of dictionaries
            data = []
            for row in rows:
                data.append({
                    'feedback_id': row[0],
                    'record_id': row[1],
                    'reason': row[2],
                    'comments': row[3] or '',
                    'user_email': row[4],
                    'feedback_date': row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else '',
                    'po_no': row[6],
                    'material_description': row[7],
                    'po_date': row[8].strftime('%Y-%m-%d') if row[8] and str(row[8]) != '1900-01-01 00:00:00' else '',
                    'delivery_date': row[9].strftime('%Y-%m-%d') if row[9] and str(row[9]) != '1900-01-01 00:00:00' else '',
                    'po_quantity': float(row[10]) if row[10] else 0,
                    'sales_quantity': float(row[11]) if row[11] else 0,
                    'fill_rate_percent': float(row[12]) if row[12] else 0,
                    'state': row[13],
                    'plant_name': row[14],
                    'sales_district': row[15],
                    'cust_group': row[16]
                })
            
        except Exception as table_error:
            print(f"Feedback table error: {table_error}")
            data = []
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Feedback reports data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/feedback-summary-stats')
@require_auth()
def get_feedback_summary_stats():
    """Get feedback summary statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Total feedback count
            cursor.execute("SELECT COUNT(*) FROM fill_rate_feedback")
            total_feedback = cursor.fetchone()[0]
            
            # Unique users who provided feedback
            cursor.execute("SELECT COUNT(DISTINCT user_email) FROM fill_rate_feedback")
            unique_users = cursor.fetchone()[0]
            
            # Feedback by reason
            cursor.execute("""
                SELECT reason, COUNT(*) as count 
                FROM fill_rate_feedback 
                GROUP BY reason 
                ORDER BY count DESC
            """)
            reason_stats = [{'reason': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Feedback by user (top 10)
            cursor.execute("""
                SELECT user_email, COUNT(*) as count 
                FROM fill_rate_feedback 
                GROUP BY user_email 
                ORDER BY count DESC
            """)
            user_stats = [{'user': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
            # Feedback by state
            cursor.execute("""
                SELECT z.State, COUNT(*) as count
                FROM fill_rate_feedback f
                INNER JOIN zepto_automation z ON f.record_id = z.ID
                GROUP BY z.State
                ORDER BY count DESC
            """)
            state_stats = [{'state': row[0], 'count': row[1]} for row in cursor.fetchall()]
            
        except Exception as table_error:
            print(f"Feedback table doesn't exist yet: {table_error}")
            total_feedback = 0
            unique_users = 0
            reason_stats = []
            user_stats = []
            state_stats = []
        
        conn.close()
        
        return jsonify({
            'total_feedback': total_feedback,
            'unique_users': unique_users,
            'reason_stats': reason_stats,
            'user_stats': user_stats,
            'state_stats': state_stats
        })
        
    except Exception as e:
        print(f"Feedback summary stats error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reports-filter-options')
@require_auth()
def get_reports_filter_options():
    """Get filter options for reports page"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Get unique users who provided feedback
            cursor.execute("""
                SELECT DISTINCT user_email 
                FROM fill_rate_feedback 
                ORDER BY user_email
            """)
            users = [row[0] for row in cursor.fetchall()]
            
            # Get unique states from feedback records
            cursor.execute("""
                SELECT DISTINCT z.State 
                FROM fill_rate_feedback f
                INNER JOIN zepto_automation z ON f.record_id = z.ID
                WHERE z.State IS NOT NULL AND z.State != '' AND z.State != '0'
                ORDER BY z.State
            """)
            states = [row[0] for row in cursor.fetchall()]
            
            # Get unique plants from feedback records
            cursor.execute("""
                SELECT DISTINCT z.Plant_Name 
                FROM fill_rate_feedback f
                INNER JOIN zepto_automation z ON f.record_id = z.ID
                WHERE z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
                ORDER BY z.Plant_Name
            """)
            plants = [row[0] for row in cursor.fetchall()]
            
        except Exception as table_error:
            print(f"Feedback table doesn't exist yet: {table_error}")
            users = []
            states = []
            plants = []
        
        conn.close()
        
        return jsonify({
            'users': users,
            'reasons': REASONS,
            'states': states,
            'plants': plants
        })
        
    except Exception as e:
        print(f"Reports filter options error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-feedback-reports')
@require_auth()
def download_feedback_reports():
    """Download feedback reports as Excel file"""
    try:
        # Get filter parameters
        user_filter = request.args.get('user', '')
        reason_filter = request.args.get('reason', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Build query same as reports data API
            query = """
            SELECT f.user_email, f.reason, f.comments, f.created_at,
                   z.PO_No, z.Material_Description, z.PO_Date, z.Delivery_Date, 
                   z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent,
                   z.State, z.Plant_Name, z.Sales_District, z.Cust_Group
            FROM fill_rate_feedback f
            INNER JOIN zepto_automation z ON f.record_id = z.ID
            WHERE 1=1
            """
            
            params = []
            
            if user_filter:
                query += " AND f.user_email LIKE ?"
                params.append(f'%{user_filter}%')
                
            if reason_filter:
                query += " AND f.reason = ?"
                params.append(reason_filter)
                
            if date_from:
                query += " AND f.created_at >= ?"
                params.append(date_from)
                
            if date_to:
                query += " AND f.created_at <= ?"
                params.append(date_to + ' 23:59:59')
                
            if state_filter:
                query += " AND z.State = ?"
                params.append(state_filter)
                
            if plant_filter:
                query += " AND z.Plant_Name = ?"
                params.append(plant_filter)
                
            query += " ORDER BY f.created_at DESC"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries for DataFrame
            data_list = []
            for row in rows:
                data_list.append({
                    'User Email': row[0],
                    'Reason': row[1],
                    'Comments': row[2] or '',
                    'Feedback Date': row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else '',
                    'PO Number': row[4],
                    'Material Description': row[5],
                    'PO Date': row[6].strftime('%Y-%m-%d') if row[6] and str(row[6]) != '1900-01-01 00:00:00' else '',
                    'Delivery Date': row[7].strftime('%Y-%m-%d') if row[7] and str(row[7]) != '1900-01-01 00:00:00' else '',
                    'PO Quantity (L)': float(row[8]) if row[8] else 0,
                    'Sales Quantity': float(row[9]) if row[9] else 0,
                    'Fill Rate %': float(row[10]) if row[10] else 0,
                    'State': row[11],
                    'Plant Name': row[12],
                    'Sales District': row[13],
                    'Customer Group': row[14]
                })
            
        except Exception as table_error:
            print(f"Feedback table error: {table_error}")
            data_list = []
        
        conn.close()
        
        if not data_list:
            return jsonify({'error': 'No feedback data found for the selected filters'}), 404
        
        # Create DataFrame
        df = pd.DataFrame(data_list)
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        temp_filename = temp_file.name
        temp_file.close()
        
        try:
            # Create Excel file
            with pd.ExcelWriter(temp_filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Feedback Reports', index=False)
                
                # Get workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Feedback Reports']
                
                # Add formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # Format header row
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    
                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    max_length = max(df[col].astype(str).map(len).max(), len(col))
                    worksheet.set_column(i, i, min(max_length + 2, 50))
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            download_filename = f"feedback_reports_{timestamp}.xlsx"
            
            # Read the file and create response
            with open(temp_filename, 'rb') as f:
                file_data = f.read()
            
            # Clean up temp file
            os.unlink(temp_filename)
            
            # Create response
            response = make_response(file_data)
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f'attachment; filename="{download_filename}"'
            response.headers['Content-Length'] = len(file_data)
            
            return response
            
        except Exception as e:
            # Clean up temp file in case of error
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
            raise e
        
    except Exception as e:
        print(f"Download feedback reports error: {e}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

# Add error handler for 404
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


# Add this endpoint to your main Flask app (app.py or main.py)

# Add these endpoints to your main Flask app (after your existing routes)

# Replace your incomplete get_plant_feedback_stats function with this complete version:

@app.route('/api/plant-feedback-stats')
@require_auth()
def get_plant_feedback_stats():
    """Get plant and date wise feedback statistics - UPDATED to exclude Unknown and >=95% fill rate"""
    try:
        # Get filter parameters
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Updated query to exclude Unknown Plant/State and fill rate >= 95%
            query = """
            WITH FeedbackStats AS (
                SELECT 
                    plant_name,
                    state,
                    CAST(delivery_date AS DATE) as delivery_date,
                    COUNT(*) as feedback_provided
                FROM fill_rate_feedback
                WHERE plant_name IS NOT NULL 
                AND plant_name != '' 
                AND plant_name != '0'
                AND plant_name != 'Unknown Plant'
                AND state IS NOT NULL 
                AND state != '' 
                AND state != '0'
                AND state != 'Unknown State'
                AND fill_rate_percent < 95
                GROUP BY plant_name, state, CAST(delivery_date AS DATE)
            ),
            TotalStats AS (
                SELECT 
                    Plant_Name as plant_name,
                    State as state,
                    CASE WHEN Delivery_Date IS NOT NULL AND Delivery_Date != '1900-01-01' 
                         THEN CAST(Delivery_Date AS DATE) 
                         ELSE CAST(Processing_Date AS DATE) END as delivery_date,
                    COUNT(*) as total_records
                FROM zepto_automation
                WHERE Fill_Rate_Percent < 95
                AND Plant_Name IS NOT NULL 
                AND Plant_Name != '' 
                AND Plant_Name != '0'
                AND Plant_Name != 'Unknown Plant'
                AND State IS NOT NULL 
                AND State != '' 
                AND State != '0'
                AND State != 'Unknown State'
                GROUP BY 
                    Plant_Name,
                    State,
                    CASE WHEN Delivery_Date IS NOT NULL AND Delivery_Date != '1900-01-01' 
                         THEN CAST(Delivery_Date AS DATE) 
                         ELSE CAST(Processing_Date AS DATE) END
            )
            SELECT 
                t.plant_name,
                t.state,
                t.delivery_date,
                t.total_records,
                ISNULL(f.feedback_provided, 0) as feedback_provided,
                t.total_records - ISNULL(f.feedback_provided, 0) as pending_feedback
            FROM TotalStats t
            LEFT JOIN FeedbackStats f ON t.plant_name = f.plant_name 
                                      AND t.state = f.state 
                                      AND t.delivery_date = f.delivery_date
            WHERE t.total_records > 0
            ORDER BY t.state, t.plant_name, t.delivery_date DESC
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            
            plant_stats = []
            for row in rows:
                date_str = row[2].strftime('%Y-%m-%d') if row[2] else 'N/A'
                
                plant_stats.append({
                    'plant_name': row[0],
                    'state': row[1], 
                    'date': date_str,
                    'total_records': row[3],
                    'feedback_provided': row[4],
                    'pending_feedback': row[5]
                })
        
        except Exception as table_error:
            print(f"Plant feedback stats error: {table_error}")
            plant_stats = []
        
        conn.close()
        return jsonify({'plant_stats': plant_stats})
        
    except Exception as e:
        print(f"Plant feedback stats error: {e}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/plant-feedback-stats-debug')
@require_auth()
def get_plant_feedback_stats_debug():
    """Debug version to see actual data"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Simple test query
        cursor.execute("""
            SELECT 
                CASE WHEN plant_name = '0' OR plant_name = '' OR plant_name IS NULL 
                     THEN 'Unknown Plant' ELSE plant_name END as plant_name,
                CASE WHEN state = '0' OR state = '' OR state IS NULL 
                     THEN 'Unknown State' ELSE state END as state,
                COUNT(*) as feedback_count
            FROM fill_rate_feedback
            GROUP BY 
                CASE WHEN plant_name = '0' OR plant_name = '' OR plant_name IS NULL 
                     THEN 'Unknown Plant' ELSE plant_name END,
                CASE WHEN state = '0' OR state = '' OR state IS NULL 
                     THEN 'Unknown State' ELSE state END
            ORDER BY feedback_count DESC
        """)
        
        feedback_summary = []
        for row in cursor.fetchall():
            feedback_summary.append({
                'plant': row[0],
                'state': row[1], 
                'feedback_count': row[2]
            })
        
        conn.close()
        return jsonify({
            'feedback_summary': feedback_summary,
            'total_feedback_records': len(feedback_summary)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# Add this endpoint to your Flask app to identify the ID mismatch

@app.route('/api/debug-record-ids')
@require_auth()
def debug_record_ids():
    """Debug endpoint to check record ID matching"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # 1. Check feedback record IDs
        cursor.execute("""
            SELECT TOP 10 record_id, reason, user_email, created_at
            FROM fill_rate_feedback 
            ORDER BY created_at DESC
        """)
        
        feedback_records = []
        for row in cursor.fetchall():
            feedback_records.append({
                'record_id': row[0],
                'reason': row[1],
                'user_email': row[2],
                'created_at': str(row[3])
            })
        
        # 2. Check if these record_ids exist in zepto_automation
        if feedback_records:
            record_ids = [str(r['record_id']) for r in feedback_records]
            placeholders = ','.join(['?' for _ in record_ids])
            
            check_query = f"""
            SELECT ID, Plant_Name, State, Fill_Rate_Percent, PO_No
            FROM zepto_automation 
            WHERE ID IN ({placeholders})
            """
            
            cursor.execute(check_query, record_ids)
            matching_records = []
            for row in cursor.fetchall():
                matching_records.append({
                    'id': row[0],
                    'plant_name': row[1],
                    'state': row[2],
                    'fill_rate_percent': row[3],
                    'po_no': row[4]
                })
        else:
            matching_records = []
        
        # 3. Get sample zepto_automation IDs for comparison
        cursor.execute("""
            SELECT TOP 10 ID, Plant_Name, State, Fill_Rate_Percent, PO_No
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95
            ORDER BY ID DESC
        """)
        
        sample_zepto_records = []
        for row in cursor.fetchall():
            sample_zepto_records.append({
                'id': row[0],
                'plant_name': row[1],
                'state': row[2],
                'fill_rate_percent': row[3],
                'po_no': row[4]
            })
        
        # 4. Test the actual JOIN query
        cursor.execute("""
            SELECT TOP 5
                z.ID,
                z.Plant_Name,
                f.record_id,
                f.reason
            FROM zepto_automation z
            INNER JOIN fill_rate_feedback f ON z.ID = f.record_id
            WHERE z.Fill_Rate_Percent < 95
        """)
        
        join_results = []
        for row in cursor.fetchall():
            join_results.append({
                'zepto_id': row[0],
                'plant_name': row[1],
                'feedback_record_id': row[2],
                'reason': row[3]
            })
        
        conn.close()
        
        return jsonify({
            'feedback_records': feedback_records,
            'matching_zepto_records': matching_records,
            'sample_zepto_records': sample_zepto_records,
            'successful_joins': join_results,
            'diagnosis': {
                'total_feedback_records': len(feedback_records),
                'matching_in_zepto': len(matching_records),
                'successful_joins': len(join_results)
            }
        })
        
    except Exception as e:
        print(f"Debug error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/download-plant-feedback-stats')
@require_auth()
def download_plant_feedback_stats():
    """Download plant feedback statistics as Excel file"""
    try:
        # Get filter parameters
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        try:
            # Build dynamic query for download - CORRECTED VERSION
            query = """
            SELECT 
                z.Plant_Name as 'Plant Name',
                z.State as 'State',
                CASE 
                    WHEN z.Delivery_Date IS NOT NULL AND z.Delivery_Date != '1900-01-01' 
                    THEN CAST(z.Delivery_Date AS DATE) 
                    ELSE CAST(z.Processing_Date AS DATE)
                END as 'Date',
                COUNT(z.ID) as 'Total Records',
                SUM(CASE WHEN f.record_id IS NOT NULL THEN 1 ELSE 0 END) as 'Feedback Provided',
                COUNT(z.ID) - SUM(CASE WHEN f.record_id IS NOT NULL THEN 1 ELSE 0 END) as 'Pending Feedback',
                CASE 
                    WHEN COUNT(z.ID) > 0 
                    THEN CAST((SUM(CASE WHEN f.record_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(z.ID)) AS DECIMAL(5,2))
                    ELSE 0.00
                END as 'Completion %'
            FROM zepto_automation z
            LEFT JOIN fill_rate_feedback f ON z.ID = f.record_id
            WHERE z.Fill_Rate_Percent < 95 
            AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
            AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
            """
            
            params = []
            conditions = []
            
            if state_filter:
                conditions.append("z.State = ?")
                params.append(state_filter)
                
            if plant_filter:
                conditions.append("z.Plant_Name = ?")
                params.append(plant_filter)
                
            if date_from:
                conditions.append("(z.Delivery_Date >= ? OR (z.Delivery_Date IS NULL AND z.Processing_Date >= ?))")
                params.extend([date_from, date_from])
                
            if date_to:
                conditions.append("(z.Delivery_Date <= ? OR (z.Delivery_Date IS NULL AND z.Processing_Date <= ?))")
                params.extend([date_to + ' 23:59:59', date_to + ' 23:59:59'])
            
            if conditions:
                query += " AND " + " AND ".join(conditions)
            
            query += """
            GROUP BY z.Plant_Name, z.State, 
                CASE 
                    WHEN z.Delivery_Date IS NOT NULL AND z.Delivery_Date != '1900-01-01' 
                    THEN CAST(z.Delivery_Date AS DATE) 
                    ELSE CAST(z.Processing_Date AS DATE)
                END
            HAVING COUNT(z.ID) > 0
            ORDER BY z.State, z.Plant_Name, 
                CASE 
                    WHEN z.Delivery_Date IS NOT NULL AND z.Delivery_Date != '1900-01-01' 
                    THEN CAST(z.Delivery_Date AS DATE) 
                    ELSE CAST(z.Processing_Date AS DATE)
                END DESC
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries for DataFrame
            data_list = []
            total_records = 0
            total_feedback = 0
            
            for row in rows:
                date_value = row[2]
                date_str = date_value.strftime('%Y-%m-%d') if date_value else 'N/A'
                
                feedback_provided = row[4] if row[4] is not None else 0
                pending_feedback = row[5] if row[5] is not None else row[3]
                
                record_data = {
                    'Plant Name': row[0],
                    'State': row[1],
                    'Date': date_str,
                    'Total Records': row[3],
                    'Feedback Provided': feedback_provided,
                    'Pending Feedback': pending_feedback,
                    'Completion %': float(row[6]) if row[6] is not None else 0.00
                }
                data_list.append(record_data)
                
                total_records += row[3]
                total_feedback += feedback_provided
            
            # Add grand total row
            overall_completion = (total_feedback / total_records * 100) if total_records > 0 else 0
            grand_total = {
                'Plant Name': 'GRAND TOTAL',
                'State': '',
                'Date': '',
                'Total Records': total_records,
                'Feedback Provided': total_feedback,
                'Pending Feedback': total_records - total_feedback,
                'Completion %': round(overall_completion, 2)
            }
            data_list.append(grand_total)
            
        except Exception as table_error:
            print(f"Plant feedback table error: {table_error}")
            # Handle case when feedback table doesn't exist - same fallback logic
            fallback_query = """
            SELECT 
                z.Plant_Name as 'Plant Name',
                z.State as 'State',
                CASE 
                    WHEN z.Delivery_Date IS NOT NULL AND z.Delivery_Date != '1900-01-01' 
                    THEN CAST(z.Delivery_Date AS DATE) 
                    ELSE CAST(z.Processing_Date AS DATE)
                END as 'Date',
                COUNT(*) as 'Total Records'
            FROM zepto_automation z
            WHERE z.Fill_Rate_Percent < 95 
            AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
            AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
            """
            
            # Apply same filters as above
            if conditions:
                fallback_query += " AND " + " AND ".join(conditions)
            
            fallback_query += """
            GROUP BY z.Plant_Name, z.State, 
                CASE 
                    WHEN z.Delivery_Date IS NOT NULL AND z.Delivery_Date != '1900-01-01' 
                    THEN CAST(z.Delivery_Date AS DATE) 
                    ELSE CAST(z.Processing_Date AS DATE)
                END
            ORDER BY z.State, z.Plant_Name
            """
            
            cursor.execute(fallback_query, params)
            rows = cursor.fetchall()
            
            data_list = []
            total_records = 0
            
            for row in rows:
                date_value = row[2]
                date_str = date_value.strftime('%Y-%m-%d') if date_value else 'N/A'
                
                record_data = {
                    'Plant Name': row[0],
                    'State': row[1],
                    'Date': date_str,
                    'Total Records': row[3],
                    'Feedback Provided': 0,
                    'Pending Feedback': row[3],
                    'Completion %': 0.00
                }
                data_list.append(record_data)
                total_records += row[3]
            
            # Add grand total row
            grand_total = {
                'Plant Name': 'GRAND TOTAL',
                'State': '',
                'Date': '',
                'Total Records': total_records,
                'Feedback Provided': 0,
                'Pending Feedback': total_records,
                'Completion %': 0.00
            }
            data_list.append(grand_total)
        
        conn.close()
        
        if not data_list or (len(data_list) == 1 and data_list[0]['Plant Name'] == 'GRAND TOTAL'):
            return jsonify({'error': 'No plant data found for the selected filters'}), 404
        
        # Create DataFrame and Excel file (rest of the function remains the same)
        df = pd.DataFrame(data_list)
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        temp_filename = temp_file.name
        temp_file.close()
        
        try:
            # Create Excel file with formatting
            with pd.ExcelWriter(temp_filename, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Plant Feedback Stats', index=False)
                
                # Get workbook and worksheet for formatting
                workbook = writer.book
                worksheet = writer.sheets['Plant Feedback Stats']
                
                # Add formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # Format for grand total row
                total_format = workbook.add_format({
                    'bold': True,
                    'fg_color': '#FFE6CC',
                    'border': 2
                })
                
                # Format header row
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Format grand total row (last row)
                if len(data_list) > 0:
                    last_row = len(data_list)
                    for col_num in range(len(df.columns)):
                        cell_value = df.iloc[-1, col_num]
                        worksheet.write(last_row, col_num, cell_value, total_format)
                    
                # Auto-adjust column widths
                for i, col in enumerate(df.columns):
                    max_length = max(df[col].astype(str).map(len).max(), len(col))
                    worksheet.set_column(i, i, min(max_length + 2, 30))
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            download_filename = f"plant_feedback_stats_{timestamp}.xlsx"
            
            # Read the file and create response
            with open(temp_filename, 'rb') as f:
                file_data = f.read()
            
            # Clean up temp file
            os.unlink(temp_filename)
            
            # Create response
            response = make_response(file_data)
            response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            response.headers['Content-Disposition'] = f'attachment; filename="{download_filename}"'
            response.headers['Content-Length'] = len(file_data)
            
            return response
            
        except Exception as e:
            # Clean up temp file in case of error
            if os.path.exists(temp_filename):
                os.unlink(temp_filename)
            raise e
        
    except Exception as e:
        print(f"Download plant feedback stats error: {e}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500                
                
# Add error handler for 500
@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

# Add error handler for authentication errors
@app.errorhandler(401)
def unauthorized(error):
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Authentication required'}), 401
    else:
        return redirect(url_for('login'))

if __name__ == '__main__':
    print("Starting Heritage Foods Gap Analysis Dashboard with Authentication...")
    print("Login will be available at: http://localhost:8000")
    print("Dashboard will be available at: http://localhost:8000/dashboard (after login)")
    print("Reports will be available at: http://localhost:8000/reports (after login)")

    app.run(debug=True, host='0.0.0.0', port=8000)
