import smtplib

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from python_code.utils import fetch_json_data
# https://myaccount.google.com/security?utm_source=OGB&utm_medium=act&pli=1#connectedapps


def send_mail(subject, body, recipients, filename=None):
    cred = fetch_json_data('gmail')
    msg = MIMEMultipart()
    msg['Cc'] = cred['user']
    msg['Bcc'] = recipients
    msg['subject'] = subject
    print("Python email %s" % subject)

    if filename:
        msg.attach(MIMEText(body))
        attachment = open(filename, "rb")
        p = MIMEBase('application', 'octet-stream')
        p.set_payload(attachment.read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(p)
    else:
        msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(cred['user'], cred['pwd'])
    text = msg.as_string()
    server.sendmail(cred['user'], recipients, text)
    print("Mail has been sent")
    server.close()

# email_send = 'lokesh.tg012@gmail.com,technical.tg012@gmail.com,sudha.tg012@gmail.com'
# send_mail('A New message','A new Body', email_send)
