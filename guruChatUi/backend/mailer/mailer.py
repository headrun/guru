import smtplib
import traceback
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders

#from cloudlibs.constants import ERROR
import logging, logging.handlers

def X(data):
    try:
        return ''.join([chr(ord(x)) for x in data]).decode('utf8', 'ignore').encode('utf8')
    except ValueError:
        return data.encode('utf8')

def send_mail(subject, body, to, cc, sender, bcc=[], server="localhost"):

    if not isinstance(to, list): to = [to]
    if not isinstance(cc, list): cc = [cc]
    if not isinstance(bcc, list): bcc = [bcc]
    if body: body = X(body)

    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = sender
    msgRoot['To'] = COMMASPACE.join(to)
    msgRoot['Date'] = formatdate(localtime=True)
    msgRoot['Cc'] = COMMASPACE.join(cc)
    msgRoot['Bcc'] = COMMASPACE.join(bcc)
    msgRoot.preamble = 'This is a multi-part message in MIME format.'

    msgAlternative = MIMEMultipart('alternative')
    msgRoot.attach(msgAlternative)

    msgText = MIMEText(body, 'text')
    msgAlternative.attach(msgText)

    msgText = MIMEText(body, 'html')
    msgAlternative.attach(msgText)

    addresses = []
    for x in to:
        addresses.append(x)
    for x in cc:
        addresses.append(x)
    for x in bcc:
        addresses.append(x)

    try:
        smtp = smtplib.SMTP(server)
        res = smtp.sendmail(sender, addresses, msgRoot.as_string())
        smtp.close()
        status = "Mail Sent Successfully"
    except smtplib.SMTPSenderRefused, er:
        status_msg = "Invalid Sender Mail id"
        status = dict(error=1, error_msg=status_msg)
    except smtplib.SMTPRecipientsRefused, er:
        status_msg = "Invalid Recipients Mail id"
        status = dict(error=1, error_msg=status_msg)
    except smtplib.SMTPException, er:
        status_msg = "Failed to Send mail"
        status = dict(error=1, error_msg=status_msg)

    return status

