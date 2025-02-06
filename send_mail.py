import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

fromaddr="inavuploader@gtnmarketdataservices.com"
# toaddr="feed.alerts@gtngroup.com"
toaddr="p.ashan@gtngroup.com"

def mail_send(table):
    load_dotenv()
    username=os.getenv('USERNAME')
    passswrd=os.getenv('PASSWORD')

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "INAV UPLOAD STATUS"

    body = table
    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('email-smtp.us-east-1.amazonaws.com', 587)
        server.starttls()
        server.login(username, passswrd)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
        return True
    except Exception as e:
        return False


