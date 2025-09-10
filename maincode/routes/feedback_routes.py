from flask import Blueprint, jsonify, request, send_file, make_response
import pyodbc
import pandas as pd
import io
from datetime import datetime
import tempfile
import os

data_bp = Blueprint('data', __name__)

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

@data_bp.route('/api/low-fill-rate-data')
def get_low_fill_rate_data():
    """Get records with fill rate < 95% and valid state/plant, including feedback status"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
            
        cursor = conn.cursor()
        
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
                'po_date': row[4].strftime('%Y-%m-%d') if row[4] else '',
                'delivery_date': row[5].strftime('%Y-%m-%d') if row[5] else '',
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': row[14].strftime('%Y-%m-%d %H:%M') if row[14] else '',
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': row[17].strftime('%Y-%m-%d %H:%M') if row[17] else ''
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/filtered-data')
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
            conditions.append("z.Material = ?")
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
                'po_date': row[4].strftime('%Y-%m-%d') if row[4] else '',
                'delivery_date': row[5].strftime('%Y-%m-%d') if row[5] else '',
                'uom': row[6],
                'po_quantity': float(row[7]) if row[7] else 0,
                'sales_quantity': float(row[8]) if row[8] else 0,
                'fill_rate_percent': float(row[9]) if row[9] else 0,
                'state': row[10],
                'plant_name': row[11],
                'sales_district': row[12],
                'cust_group': row[13],
                'processing_date': row[14].strftime('%Y-%m-%d %H:%M') if row[14] else '',
                'has_feedback': row[15] is not None,
                'feedback_reason': row[15],
                'feedback_comments': row[16],
                'feedback_date': row[17].strftime('%Y-%m-%d %H:%M') if row[17] else ''
            })
        
        conn.close()
        return jsonify({'data': data, 'count': len(data)})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/download-data')
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
            conditions.append("z.Material = ?")
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
                'PO Date': row[3].strftime('%Y-%m-%d') if row[3] else '',
                'Delivery Date': row[4].strftime('%Y-%m-%d') if row[4] else '',
                'UOM': row[5],
                'PO Quantity (L)': float(row[6]) if row[6] else 0,
                'Sales Quantity': float(row[7]) if row[7] else 0,
                'Fill Rate %': float(row[8]) if row[8] else 0,
                'State': row[9],
                'Plant Name': row[10],
                'Sales District': row[11],
                'Customer Group': row[12],
                'Processing Date': row[13].strftime('%Y-%m-%d %H:%M') if row[13] else '',
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
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@data_bp.route('/api/filter-options')
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
            SELECT DISTINCT Material 
            FROM zepto_automation 
            WHERE Fill_Rate_Percent < 95 
            AND Material IS NOT NULL AND Material != ''
            ORDER BY Material
        """)
        materials = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return jsonify({
            'states': states,
            'plants_by_state': plants_by_state,
            'materials': materials
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@data_bp.route('/api/dashboard-stats')
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
        with_feedback = cursor.fetchone()[0]
        
        # Records needing feedback
        needs_feedback = low_fill_rate - with_feedback
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'low_fill_rate_count': low_fill_rate,
            'average_fill_rate': round(avg_fill_rate, 2),
            'needs_feedback': needs_feedback,
            'with_feedback': with_feedback
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500