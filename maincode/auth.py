from flask import Blueprint, request, jsonify, session
import smtplib
import random
import string
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import hashlib
import os
from functools import wraps

auth_bp = Blueprint('auth', __name__)

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'email': 'saisridhar.k@heritagefoods.in',
    'password': 'iyau hnuf ilav wses'  # App password
}

# In-memory storage for OTPs (in production, use Redis or database)
otp_storage = {}
login_attempts = {}

# Rate limiting configuration
MAX_OTP_REQUESTS_PER_HOUR = 5
MAX_LOGIN_ATTEMPTS = 3
OTP_VALIDITY_MINUTES = 10
SESSION_DURATION_HOURS = 8

def generate_otp():
    """Generate a 6-digit OTP"""
    return ''.join(random.choices(string.digits, k=6))

def get_email_template(otp, email):
    """Generate HTML email template for OTP"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Heritage Foods - OTP Verification</title>
    </head>
    <body style="font-family: 'Arial', sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; background-color: #f4f4f4;">
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 15px; margin-bottom: 30px;">
            <h1 style="color: white; text-align: center; margin: 0; font-size: 28px;">Heritage Foods</h1>
            <p style="color: white; text-align: center; margin: 10px 0 0 0; opacity: 0.9;">Gap Analysis Dashboard</p>
        </div>
        
        <div style="background: white; padding: 40px; border-radius: 15px; box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
            <h2 style="color: #1e3a8a; text-align: center; margin-bottom: 20px;">OTP Verification</h2>
            
            <p style="font-size: 16px; margin-bottom: 25px;">Hello,</p>
            
            <p style="font-size: 16px; margin-bottom: 25px;">
                You have requested access to the Heritage Foods Gap Analysis Dashboard. 
                Please use the following One-Time Password (OTP) to complete your login:
            </p>
            
            <div style="background: #f8fafc; border: 2px dashed #3b82f6; border-radius: 12px; padding: 30px; text-align: center; margin: 30px 0;">
                <h1 style="font-size: 48px; font-weight: bold; color: #1e3a8a; margin: 0; letter-spacing: 8px; font-family: 'Courier New', monospace;">
                    {otp}
                </h1>
            </div>
            
            <div style="background: #fee2e2; border-left: 4px solid #dc2626; padding: 15px; margin: 25px 0; border-radius: 8px;">
                <p style="margin: 0; color: #dc2626; font-weight: 600;">⚠️ Important Security Information:</p>
                <ul style="margin: 10px 0; color: #dc2626;">
                    <li>This OTP is valid for <strong>{OTP_VALIDITY_MINUTES} minutes</strong> only</li>
                    <li>Do not share this code with anyone</li>
                    <li>If you didn't request this, please ignore this email</li>
                </ul>
            </div>
            
            <p style="font-size: 14px; color: #6b7280; margin-top: 30px;">
                This email was sent to: <strong>{email}</strong><br>
                Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST
            </p>
        </div>
        
        <div style="text-align: center; margin-top: 30px; color: #6b7280; font-size: 14px;">
            <p>© 2025 Heritage Foods. All rights reserved.</p>
            <p>This is an automated email. Please do not reply.</p>
        </div>
    </body>
    </html>
    """

def send_otp_email(email, otp):
    """Send OTP via email"""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Heritage Foods - Your OTP: {otp}"
        msg['From'] = EMAIL_CONFIG['email']
        msg['To'] = email
        
        # Create HTML content
        html_content = get_email_template(otp, email)
        html_part = MIMEText(html_content, 'html')
        
        # Create plain text content as fallback
        text_content = f"""
Heritage Foods - Gap Analysis Dashboard

Your OTP for login: {otp}

This OTP is valid for {OTP_VALIDITY_MINUTES} minutes only.
Do not share this code with anyone.

If you didn't request this, please ignore this email.

Email: {email}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST
        """
        text_part = MIMEText(text_content, 'plain')
        
        msg.attach(text_part)
        msg.attach(html_part)
        
        # Send email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['email'], EMAIL_CONFIG['password'])
            server.send_message(msg)
            
        return True, "OTP sent successfully"
        
    except Exception as e:
        print(f"Email sending error: {e}")
        return False, f"Failed to send email: {str(e)}"

def is_rate_limited(email, request_type='otp'):
    """Check if user is rate limited"""
    current_time = time.time()
    key = f"{email}_{request_type}"
    
    if key not in login_attempts:
        login_attempts[key] = []
    
    # Remove attempts older than 1 hour
    login_attempts[key] = [timestamp for timestamp in login_attempts[key] 
                          if current_time - timestamp < 3600]
    
    if request_type == 'otp':
        return len(login_attempts[key]) >= MAX_OTP_REQUESTS_PER_HOUR
    else:  # login attempts
        return len(login_attempts[key]) >= MAX_LOGIN_ATTEMPTS

def add_attempt(email, request_type='otp'):
    """Add attempt to rate limiting"""
    current_time = time.time()
    key = f"{email}_{request_type}"
    
    if key not in login_attempts:
        login_attempts[key] = []
    
    login_attempts[key].append(current_time)

@auth_bp.route('/api/send-otp', methods=['POST'])
def send_otp():
    """Send OTP to user's email"""
    try:
        data = request.get_json()
        email = data.get('email', '').lower().strip()
        
        # Validate email format
        if not email or '@' not in email:
            return jsonify({'error': 'Invalid email format'}), 400
        
        # Check if email is from Heritage Foods domain
        if not email.endswith('@heritagefoods.in'):
            return jsonify({'error': 'Only @heritagefoods.in email addresses are allowed'}), 403
        
        # Check rate limiting
        if is_rate_limited(email, 'otp'):
            return jsonify({
                'error': f'Too many OTP requests. Maximum {MAX_OTP_REQUESTS_PER_HOUR} requests per hour allowed.'
            }), 429
        
        # Generate OTP
        otp = generate_otp()
        
        # Send email
        success, message = send_otp_email(email, otp)
        
        if not success:
            return jsonify({'error': message}), 500
        
        # Store OTP with timestamp
        otp_hash = hashlib.sha256(f"{email}_{otp}".encode()).hexdigest()
        otp_storage[email] = {
            'otp_hash': otp_hash,
            'timestamp': time.time(),
            'attempts': 0
        }
        
        # Add to rate limiting
        add_attempt(email, 'otp')
        
        return jsonify({
            'message': 'OTP sent successfully',
            'email': email,
            'valid_for_minutes': OTP_VALIDITY_MINUTES
        }), 200
        
    except Exception as e:
        print(f"Send OTP error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/api/verify-otp', methods=['POST'])
def verify_otp():
    """Verify OTP and create session"""
    try:
        data = request.get_json()
        email = data.get('email', '').lower().strip()
        otp = data.get('otp', '').strip()
        
        if not email or not otp:
            return jsonify({'error': 'Email and OTP are required'}), 400
        
        # Check if email is from Heritage Foods domain
        if not email.endswith('@heritagefoods.in'):
            return jsonify({'error': 'Unauthorized email domain'}), 403
        
        # Check rate limiting for login attempts
        if is_rate_limited(email, 'login'):
            return jsonify({
                'error': f'Too many failed login attempts. Please try again later.'
            }), 429
        
        # Check if OTP exists for email
        if email not in otp_storage:
            return jsonify({'error': 'No OTP found. Please request a new one.'}), 404
        
        stored_otp_data = otp_storage[email]
        current_time = time.time()
        
        # Check if OTP has expired
        if current_time - stored_otp_data['timestamp'] > (OTP_VALIDITY_MINUTES * 60):
            del otp_storage[email]
            return jsonify({'error': 'OTP has expired. Please request a new one.'}), 410
        
        # Check attempt limit for this OTP
        if stored_otp_data['attempts'] >= 3:
            del otp_storage[email]
            return jsonify({'error': 'Maximum OTP attempts exceeded. Please request a new one.'}), 429
        
        # Verify OTP
        otp_hash = hashlib.sha256(f"{email}_{otp}".encode()).hexdigest()
        
        if otp_hash != stored_otp_data['otp_hash']:
            # Increment attempt counter
            stored_otp_data['attempts'] += 1
            otp_storage[email] = stored_otp_data
            
            # Add to rate limiting
            add_attempt(email, 'login')
            
            remaining_attempts = 3 - stored_otp_data['attempts']
            if remaining_attempts > 0:
                return jsonify({
                    'error': f'Invalid OTP. {remaining_attempts} attempts remaining.'
                }), 401
            else:
                del otp_storage[email]
                return jsonify({
                    'error': 'Invalid OTP. Maximum attempts exceeded. Please request a new OTP.'
                }), 429
        
        # OTP is valid, create session
        session['user_email'] = email
        session['login_time'] = current_time
        session['session_id'] = hashlib.sha256(f"{email}_{current_time}".encode()).hexdigest()
        session.permanent = True
        
        # Clean up OTP
        del otp_storage[email]
        
        # Clean up rate limiting for successful login
        login_key = f"{email}_login"
        if login_key in login_attempts:
            del login_attempts[login_key]
        
        return jsonify({
            'message': 'Login successful',
            'user_email': email,
            'session_duration_hours': SESSION_DURATION_HOURS
        }), 200
        
    except Exception as e:
        print(f"Verify OTP error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/api/logout', methods=['POST'])
def logout():
    """Logout user and clear session"""
    try:
        session.clear()
        return jsonify({'message': 'Logged out successfully'}), 200
    except Exception as e:
        print(f"Logout error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/api/verify-session', methods=['GET'])
def verify_session():
    """Verify if user session is valid"""
    try:
        if 'user_email' not in session or 'login_time' not in session:
            return jsonify({'valid': False, 'error': 'No active session'}), 401
        
        current_time = time.time()
        login_time = session['login_time']
        session_duration = SESSION_DURATION_HOURS * 3600  # Convert to seconds
        
        if current_time - login_time > session_duration:
            session.clear()
            return jsonify({'valid': False, 'error': 'Session expired'}), 401
        
        return jsonify({
            'valid': True,
            'user_email': session['user_email'],
            'time_remaining_minutes': int((session_duration - (current_time - login_time)) / 60)
        }), 200
        
    except Exception as e:
        print(f"Session verification error: {e}")
        return jsonify({'valid': False, 'error': 'Internal server error'}), 500

# Middleware function to check authentication
def require_auth():
    """Decorator to require authentication for routes"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if 'user_email' not in session:
                return jsonify({'error': 'Authentication required'}), 401
            
            current_time = time.time()
            login_time = session.get('login_time', 0)
            session_duration = SESSION_DURATION_HOURS * 3600
            
            if current_time - login_time > session_duration:
                session.clear()
                return jsonify({'error': 'Session expired'}), 401
            
            return f(*args, **kwargs)
        return wrapper
    return decorator