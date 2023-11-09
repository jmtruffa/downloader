import subprocess
import os
import tempfile
import pandas as pd
from dataBaseConn2 import DatabaseConnection
from datetime import datetime

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import timedelta
import re

def getCCL():
    db = DatabaseConnection(db_type="postgresql", db_name= "data")
    db.connect()

    query = "SELECT * FROM ccl"
    df = db.execute_select_query(query)

    db.disconnect()
    
    if df.empty:
        print("------------------------------------")
        print(f"CCL Table not found in database at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return None

    temp_dir = ""
    currentDate = datetime.now().strftime("%Y-%m-%d")

    # File path for the generated XLSM file
    file_path = os.path.join(temp_dir, f"{currentDate} CCL.xlsx")

    # Convert the dateTime to Excel date format

    #df["date"] = df["date"].apply(lambda x: (datetime(1970, 1, 1) + timedelta(days=x)).date())
     
    # Save the DataFrame to an Excel file
    df.to_excel(file_path, index=False)  # Use index=False to exclude the index column
    return file_path


def sendEmail(sender_email, recipient_emails_file, subject, body, file_path):

    # Get credentials from environment variables
    # 
    email = os.environ.get('CRON_EMAIL_ADDRESS')
    app_password = os.environ.get('CRON_EMAIL_PASSWORD')


    # Read Gmail credentials from the 'gmail.txt' file
    # with open('/home/juant/data/credentials.txt', 'r') as file:
    #     lines = file.read().splitlines()
    #     if len(lines) != 2:
    #         print("------------------------------------")
    #         print(f"Error: 'credentials.txt' should contain two lines - email and App Password at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    #         exit(1)
    #     email, app_password = lines

    # Create an SMTP object
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Use 587 for TLS
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()  # Use TLS encryption

    # Login with your Gmail credentials (email and App Password)
    server.login(email, app_password)
    # Log in to your email account
    #smtp.login('your-email@example.com', 'your-password')  # Replace with your email and password

    filename = file_path
    attachment = open(filename, 'rb').read()
    
    recipient_df = pd.read_csv(recipient_emails_file)
    recipient_emails = recipient_df['email'].tolist()

    # Loop through the recipient email addresses
    for recipient_email in recipient_emails:
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        part = MIMEBase('application', 'octet-stream')
        # Attach a file (e.g., 'attachment.txt') to the email
        part.set_payload(attachment)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        msg.attach(part)


        # Send the email
        try:
            server.sendmail(sender_email, recipient_email, msg.as_string())
            print("------------------------------------")
            print(f"Email sent to {recipient_email} successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print("------------------------------------")
            print(f"An error occurred when sending email to {recipient_email}: {e} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Quit the server
    server.quit()
    
        
    


# Example usage:
if __name__ == '__main__':

    file_path = getCCL()

    if file_path is None:
        print("No CCL data found")
        exit()

    sender_email = 'jmtruffa@gmail.com'
    recipient_emails_file =  './emailsPostgres.csv'#'/home/juant/data/apps/emailsPostgres.csv'
    #recipient_emails = ['jmtruffa@gmail.com'] #['recipient1@example.com', 'recipient2@example.com', 'recipient3@example.com']
    subject = 'CCL - Envío automático'
    body = 'Ver adjunto.'

    sendEmail(sender_email, recipient_emails_file, subject, body, file_path)
    
    os.remove(file_path)

    
