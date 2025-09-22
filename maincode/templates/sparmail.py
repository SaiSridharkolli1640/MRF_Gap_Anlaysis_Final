####SPAR final##
######SPAR
import os
import re
from imbox import Imbox  # pip install imbox
import traceback
from datetime import datetime, timedelta

# Enable less secure apps on your Google account
# https://myaccount.google.com/lesssecureapps

host = "imap.gmail.com"
username = "hflemt@heritagefoods.in"
password = 'bidy dcgu tkmz axiq'
download_folder = "E:\POAutomation\Input_PO\Spar"
sender_email = "spar.po@landmarkgroup.co.in"
subject_keyword = "PO"

if not os.path.isdir(download_folder):
    os.makedirs(download_folder, exist_ok=True)

def sanitize_subject(subject):
    invalid_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in invalid_chars:
        subject = subject.replace(char, '')
    return subject

def extract_po_number(subject):
    match = re.search(r'PO_(\d+)', subject)
    if match:
        return match.group(1)
    return None

def save_attachment(attachment, uid, index, po_number):
    att_fn = attachment.get('filename')
    
    if not att_fn:
        att_fn = f"attachment_{po_number}{index}.pdf" if po_number else f"attachment{uid}_{index}.pdf"
        
    sanitized_filename = sanitize_subject(att_fn)
    download_path = os.path.join(download_folder, sanitized_filename)

    print(f"Saving attachment to {download_path}")
    try:
        attachment_content = attachment.get('content')
        print(f"Attachment content type: {type(attachment_content)}")
        
        # Check if the content is bytes or a file-like object
        if isinstance(attachment_content, bytes):
            with open(download_path, "wb") as fp:
                fp.write(attachment_content)
            print(f"Downloaded: {att_fn}")
            # Verify the file size after writing
            file_size = os.path.getsize(download_path)
            print(f"File size after writing: {file_size} bytes")
        elif hasattr(attachment_content, 'read'):
            attachment_data = attachment_content.read()
            print(f"Attachment data length: {len(attachment_data)}")
            with open(download_path, "wb") as fp:
                fp.write(attachment_data)
            print(f"Downloaded: {att_fn}")
            # Verify the file size after writing
            file_size = os.path.getsize(download_path)
            print(f"File size after writing: {file_size} bytes")
        else:
            print(f"Attachment content is not a file-like object or bytes: {att_fn}")
    except Exception as e:
        print(f"Failed to save attachment {att_fn}: {e}")
        print(traceback.print_exc())

try:
    mail = Imbox(host, username=username, password=password, ssl=True, ssl_context=None, starttls=False)
    
    # Calculate the date for yesterday
    yesterday = datetime.today() - timedelta(days=1)
    yesterday = yesterday.date()
    
    # Define a batch size to avoid exceeding size limits
    batch_size = 100  # Adjust this value if necessary
    
    # Fetch all messages from the specified sender
    messages = mail.messages(date__on=yesterday, sent_from=sender_email)

    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]

        for uid, message in batch:
            email_subject = message.subject
            
            if subject_keyword in email_subject:
                po_number = extract_po_number(email_subject)
                print(f"Processing email with UID: {uid}")
                print(f"Email subject: {email_subject}")
                print(f"Extracted PO number: {po_number}")

                mail.mark_seen(uid)  # optional, mark message as read
                for index, attachment in enumerate(message.attachments):
                    if attachment['content-type'] == 'application/pdf':
                        save_attachment(attachment, uid, index, po_number)

    mail.logout()

except Exception as e:
    print(f"An error occurred: {e}")
    print(traceback.print_exc())