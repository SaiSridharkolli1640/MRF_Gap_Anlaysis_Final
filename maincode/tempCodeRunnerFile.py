from flask import Flask, render_template, request, jsonify
import pyodbc
from datetime import datetime

app = Flask(__name__)

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
    "Supply rejected by customer"
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
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/dashboard-stats')
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
        
        # Records needing feedback
        cursor.execute("""
            SELECT COUNT(*) FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
        """)
        needs_feedback = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'low_fill_rate_count': low_fill_rate,
            'average_fill_rate': round(avg_fill_rate, 2),
            'needs_feedback': needs_feedback
        })
        
    except Exception as e:
        print(f"Stats error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/low-fill-rate-data')
def get_low_fill_rate_data():
    """Get records with fill rate < 95% and valid state/plant"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        query = """
        SELECT ID, PO_No, Material_Description, Material, PO_Date, Delivery_Date, 
               UOM, PO_Quantity_Liters, Sales_Quantity_Matched, Fill_Rate_Percent, 
               State, Plant_Name, Sales_District, Cust_Group, Processing_Date
        FROM zepto_automation 
        WHERE Fill_Rate_Percent < 95 
        AND State IS NOT NULL 
        AND State != '' 
        AND State != '0'
        AND Plant_Name IS NOT NULL 
        AND Plant_Name != '' 
        AND Plant_Name != '0'
        ORDER BY Processing_Date DESC
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
                'processing_date': row[14].strftime('%Y-%m-%d %H:%M') if row[14] else ''
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Data fetch error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter-options')
def get_filter_options():
    """Get unique states and plants for filters"""
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
        
        # Get unique plants
        cursor.execute("""
            SELECT DISTINCT Plant_Name 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND State IS NOT NULL AND State != '' AND State != '0'
            AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
            ORDER BY Plant_Name
        """)
        plants = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({'states': states, 'plants': plants})
        
    except Exception as e:
        print(f"Filter options error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filtered-data')
def get_filtered_data():
    """Get filtered data based on state and plant"""
    try:
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Build dynamic query based on filters
        query = """
        SELECT ID, PO_No, Material_Description, Delivery_Date, 
               PO_Quantity_Liters, Sales_Quantity_Matched, Fill_Rate_Percent, 
               State, Plant_Name
        FROM zepto_automation 
        WHERE Fill_Rate_Percent < 95 
        AND Fill_Rate_Percent IS NOT NULL
        AND State IS NOT NULL AND State != '' AND State != '0'
        AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
        """
        
        params = []
        
        if state_filter:
            query += " AND State = ?"
            params.append(state_filter)
            
        if plant_filter:
            query += " AND Plant_Name = ?"
            params.append(plant_filter)
            
        query += " ORDER BY Fill_Rate_Percent ASC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'po_no': row[1],
                'material_description': row[2],
                'delivery_date': row[3].strftime('%Y-%m-%d') if row[3] and str(row[3]) != '1900-01-01 00:00:00' else '',
                'po_quantity': float(row[4]) if row[4] else 0,
                'sales_quantity': float(row[5]) if row[5] else 0,
                'fill_rate_percent': float(row[6]) if row[6] else 0,
                'state': row[7],
                'plant_name': row[8]
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Filtered data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reasons')
def get_reasons():
    """Get list of available reasons"""
    return jsonify({'reasons': REASONS})

@app.route('/api/filtered-data')
def get_filtered_data():
    """Get filtered data based on state and plant"""
    try:
        state_filter = request.args.get('state', '')
        plant_filter = request.args.get('plant', '')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
        # Build dynamic query based on filters
        query = """
        SELECT ID, PO_No, Material_Description, Delivery_Date, 
               PO_Quantity_Liters, Sales_Quantity_Matched, Fill_Rate_Percent, 
               State, Plant_Name
        FROM zepto_automation 
        WHERE Fill_Rate_Percent < 95 
        AND Fill_Rate_Percent IS NOT NULL
        AND State IS NOT NULL AND State != '' AND State != '0'
        AND Plant_Name IS NOT NULL AND Plant_Name != '' AND Plant_Name != '0'
        """
        
        params = []
        
        if state_filter:
            query += " AND State = ?"
            params.append(state_filter)
            
        if plant_filter:
            query += " AND Plant_Name = ?"
            params.append(plant_filter)
            
        query += " ORDER BY Fill_Rate_Percent ASC"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'id': row[0],
                'po_no': row[1],
                'material_description': row[2],
                'delivery_date': row[3].strftime('%Y-%m-%d') if row[3] and str(row[3]) != '1900-01-01 00:00:00' else '',
                'po_quantity': float(row[4]) if row[4] else 0,
                'sales_quantity': float(row[5]) if row[5] else 0,
                'fill_rate_percent': float(row[6]) if row[6] else 0,
                'state': row[7],
                'plant_name': row[8]
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        print(f"Filtered data error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/submit-feedback', methods=['POST'])
def submit_feedback():
    """Submit feedback for a record"""
    try:
        data = request.get_json()
        record_id = data.get('record_id')
        reason = data.get('reason')
        comments = data.get('comments', '')
        
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
            created_at DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        
        # Insert feedback
        insert_query = """
        INSERT INTO fill_rate_feedback (record_id, reason, comments) 
        VALUES (?, ?, ?)
        """
        cursor.execute(insert_query, (record_id, reason, comments))
        
        conn.commit()
        conn.close()
        
        return jsonify({'message': 'Feedback submitted successfully'}), 200
        
    except Exception as e:
        print(f"Feedback submission error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/feedback-history/<int:record_id>')
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
        SELECT reason, comments, created_at 
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
                'created_at': row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else ''
            })
        
        conn.close()
        return jsonify({'feedback_history': feedback_history})
        
    except Exception as e:
        print(f"Feedback history error: {e}")
        return jsonify({'error': str(e)}), 500

# Add error handler for 404
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

# Add error handler for 500
@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("Starting Fill Rate Dashboard...")
    print("Dashboard will be available at: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)