from flask import Blueprint, jsonify, request
import pyodbc
from datetime import datetime

feedback_bp = Blueprint('feedback', __name__)

# Database configuration
DB_CONFIG = {
    'server': '202.53.88.202,4000',
    'database': 'wordDB',
    'username': 'HFLSQLReader',
    'password': 'HFL@12345',
    'driver': '{ODBC Driver 17 for SQL Server}'
}

def get_db_connection():
    """Create database connection"""
    try:
        conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']}"
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

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

@feedback_bp.route('/api/submit-feedback', methods=['POST'])
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
            created_at DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (record_id) REFERENCES zepto_automation(ID)
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
        return jsonify({'error': str(e)}), 500

@feedback_bp.route('/api/feedback-history/<int:record_id>')
def get_feedback_history(record_id):
    """Get feedback history for a record"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
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
        return jsonify({'error': str(e)}), 500

@feedback_bp.route('/api/reasons')
def get_reasons():
    """Get list of available reasons"""
    return jsonify({'reasons': REASONS})