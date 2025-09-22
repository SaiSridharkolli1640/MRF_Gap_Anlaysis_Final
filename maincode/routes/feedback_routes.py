from flask import Blueprint, jsonify, request, send_file, make_response, session
import pyodbc
import pandas as pd
import io
from datetime import datetime
import tempfile
import os
from functools import wraps

data_bp = Blueprint('data', __name__)

# Database configuration
DB_CONFIG = {
    'server': '202.53.88.202,4000',
    'database': 'wordDB',
    'username': 'HFLSQLReader',
    'password': 'HFL@12345',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

# Reasons for feedback
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

def require_auth():
    """Decorator to require authentication for routes"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if 'user_email' not in session:
                return jsonify({'error': 'Authentication required'}), 401
            
            current_time = datetime.now().timestamp()
            login_time = session.get('login_time', 0)
            session_duration = 8 * 3600  # 8 hours
            
            if current_time - login_time > session_duration:
                session.clear()
                return jsonify({'error': 'Session expired'}), 401
            
            return f(*args, **kwargs)
        return wrapper
    return decorator

def safe_date_format(date_obj, format_str='%Y-%m-%d'):
    """Safely format dates, handling None and 1900-01-01 dates"""
    if not date_obj:
        return ''
    if str(date_obj) == '1900-01-01 00:00:00':
        return ''
    return date_obj.strftime(format_str)

def create_feedback_table_if_not_exists(cursor):
    """Create feedback table if it doesn't exist"""
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

# EXISTING ENDPOINTS (Enhanced)

@data_bp.route('/api/low-fill-rate-data')
@require_auth()
def get_low_fill_rate_data():
    """Get records with fill rate < 95% and valid state/plant, including feedback status"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Create feedback table if it doesn't exist
        create_feedback_table_if_not_exists(cursor)
        
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
                'po_date': safe_date_format(row[4]),
                'delivery_date': safe_date_format(row[5]),
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': safe_date_format(row[14], '%Y-%m-%d %H:%M'),
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': safe_date_format(row[17], '%Y-%m-%d %H:%M')
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Data fetch error: {e}")
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/filtered-data')
@require_auth()
def get_filtered_data():
    """Get filtered records with fill rate < 95%"""
    try:
        state = request.args.get('state')
        plant = request.args.get('plant')
        material = request.args.get('material')
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Build dynamic query
        base_query = """
        SELECT z.ID, z.PO_No, z.Material_Description, z.Material, z.PO_Date, z.Delivery_Date, 
               z.UOM, z.PO_Quantity_Liters, z.Sales_Quantity_Matched, z.Fill_Rate_Percent, 
               z.State, z.Plant_Name, z.Sales_District, z.Cust_Group, z.Processing_Date,
               f.reason, f.comments, f.created_at
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
        
        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'po_no': row[1],
                'material_description': row[2],
                'material': row[3],
                'po_date': safe_date_format(row[4]),
                'delivery_date': safe_date_format(row[5]),
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': safe_date_format(row[14], '%Y-%m-%d %H:%M'),
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': safe_date_format(row[17], '%Y-%m-%d %H:%M')
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Filtered data error: {e}")
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/download-data')
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
                'PO Date': safe_date_format(row[3]),
                'Delivery Date': safe_date_format(row[4]),
                'UOM': row[5],
                'PO Quantity (L)': float(row[6]) if row[6] else 0,
                'Sales Quantity': float(row[7]) if row[7] else 0,
                'Fill Rate %': float(row[8]) if row[8] else 0,
                'State': row[9],
                'Plant Name': row[10],
                'Sales District': row[11],
                'Customer Group': row[12],
                'Processing Date': safe_date_format(row[13], '%Y-%m-%d %H:%M'),
                'Feedback Status': row[14],
                'Feedback Comments': row[15],
                'Feedback Date': safe_date_format(row[16], '%Y-%m-%d %H:%M')
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

@data_bp.route('/api/filter-options')
@require_auth()
def get_filter_options():
    """Get filter options with record counts"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Get states
        cursor.execute("""
            SELECT DISTINCT State 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            ORDER BY State
        """)
        states = [row[0] for row in cursor.fetchall()]
        
        # Get plants grouped by state with counts
        cursor.execute("""
            SELECT State, Plant_Name, COUNT(*) as record_count
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
            GROUP BY State, Plant_Name
            ORDER BY State, Plant_Name
        """)
        plant_rows = cursor.fetchall()
        
        plants_by_state = {}
        for row in plant_rows:
            state = row[0]
            if state not in plants_by_state:
                plants_by_state[state] = []
            plants_by_state[state].append({
                'name': row[1],
                'count': row[2]
            })
        
        # Get materials
        cursor.execute("""
            SELECT DISTINCT Material_Description 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND Material_Description IS NOT NULL AND Material_Description != ''
            ORDER BY Material_Description
        """)
        materials = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'states': states,
            'plants_by_state': plants_by_state,
            'materials': materials
        })
        
    except Exception as e:
        print(f"Filter options error: {e}")
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/dashboard-stats')
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
        
        # Records with feedback (safe check if table exists)
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

# NEW FEEDBACK ENDPOINTS

@data_bp.route('/api/reasons')
@require_auth()
def get_reasons():
    """Get list of available reasons"""
    return jsonify({'reasons': REASONS})

@data_bp.route('/api/submit-feedback', methods=['POST'])
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
        
        # Create table if it doesn't exist
        create_feedback_table_if_not_exists(cursor)
        
        # Check if feedback already exists for this record
        check_query = "SELECT COUNT(*) FROM fill_rate_feedback WHERE record_id = ?"
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

@data_bp.route('/api/check-feedback/<int:record_id>')
@require_auth()
def check_feedback(record_id):
    """Check if feedback exists for a record"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Check if table exists first
        try:
            query = "SELECT reason, comments, created_at FROM fill_rate_feedback WHERE record_id = ?"
            cursor.execute(query, (record_id,))
            row = cursor.fetchone()
            
            if row:
                feedback_data = {
                    'has_feedback': True,
                    'reason': row[0],
                    'comments': row[1],
                    'created_at': safe_date_format(row[2], '%Y-%m-%d %H:%M:%S')
                }
            else:
                feedback_data = {'has_feedback': False}
        except:
            feedback_data = {'has_feedback': False}
        
        conn.close()
        return jsonify(feedback_data)
        
    except Exception as e:
        print(f"Check feedback error: {e}")
        return jsonify({'error': str(e)}), 500

# NEW REPORTS ENDPOINTS

@data_bp.route('/api/feedback-reports-data')
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
        
        # Check if feedback table exists
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
                    'feedback_date': safe_date_format(row[5], '%Y-%m-%d %H:%M:%S'),
                    'po_no': row[6],
                    'material_description': row[7],
                    'po_date': safe_date_format(row[8]),
                    'delivery_date': safe_date_format(row[9]),
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

@data_bp.route('/api/feedback-summary-stats')
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

@data_bp.route('/api/reports-filter-options')
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

@data_bp.route('/api/download-feedback-reports')
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
                    'Feedback Date': safe_date_format(row[3], '%Y-%m-%d %H:%M:%S'),
                    'PO Number': row[4],
                    'Material Description': row[5],
                    'PO Date': safe_date_format(row[6]),
                    'Delivery Date': safe_date_format(row[7]),
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
        
        
        @data_bp.route('/api/plant-feedback-stats')
@require_auth()
def get_plant_feedback_stats():
    """Get plant and date wise feedback statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        query = """
        SELECT 
            z.Plant_Name,
            z.State,
            CAST(z.Delivery_Date AS DATE) as delivery_date,
            COUNT(*) as total_records,
            COUNT(f.id) as feedback_provided,
            COUNT(*) - COUNT(f.id) as pending_feedback
        FROM zepto_automation z
        LEFT JOIN fill_rate_feedback f ON z.ID = f.record_id
        WHERE z.Fill_Rate_Percent < 95 
        AND z.State IS NOT NULL AND z.State != '' AND z.State != '0'
        AND z.Plant_Name IS NOT NULL AND z.Plant_Name != '' AND z.Plant_Name != '0'
        GROUP BY z.Plant_Name, z.State, CAST(z.Delivery_Date AS DATE)
        ORDER BY z.Plant_Name, CAST(z.Delivery_Date AS DATE) DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        plant_stats = []
        for row in rows:
            plant_stats.append({
                'plant_name': row[0],
                'state': row[1], 
                'date': row[2].strftime('%Y-%m-%d') if row[2] else '',
                'total_records': row[3],
                'feedback_provided': row[4],
                'pending_feedback': row[5]
            })
        
        conn.close()
        return jsonify({'plant_stats': plant_stats})
        

        return jsonify({'error': str(e)}), 500
    except Exception as e:
        print(f"Download feedback reports error: {e}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500