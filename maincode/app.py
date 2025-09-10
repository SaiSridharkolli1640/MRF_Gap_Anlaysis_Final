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
        cursor.execute("""
            SELECT COUNT(*) FROM zepto_automation z
            INNER JOIN fill_rate_feedback f ON z.ID = f.record_id
            WHERE z.Fill_Rate_Percent < 95 
            AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
            AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
        """)
        with_feedback = cursor.fetchone()[0] if cursor.fetchone() else 0
        
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

# Add error handler for 404
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

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
    print("Dashboard will be available at: http://localhost:5000/dashboard (after login)")

    app.run(debug=True, host='0.0.0.0', port=8000)
