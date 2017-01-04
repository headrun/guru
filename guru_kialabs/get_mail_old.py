import email
import imaplib
import urllib2
import json
import smtplib
from email.mime.text import MIMEText

user = 'headrun.mailtest@gmail.com'
pwd = 'headrun123'

def run():

    m = imaplib.IMAP4_SSL("imap.gmail.com")
    m.login(user,pwd)
    m.select('INBOX')

    resp, items = m.search(None, "UNSEEN")
    items = items[0].split() 

    for emailid in items:
        resp, data = m.fetch(emailid, "(RFC822)")
        email_body = data[0][1]
        mail = email.message_from_string(email_body)
        print(type(mail))
        print(mail.is_multipart())
        mail_sub = mail['Subject']
        mail_sender = mail['From']

        body_text = mail.get_payload()[0].get_payload()

        print(type(body_text))

        if type(body_text) is list:
            #body_text = ','.join(str(v) for v in body_text)
            body_text = str(body_text[0])
        print(body_text)
        body_text = body_text.replace('\n', '').replace('\r', '')
        if '--' in body_text:
            body_text = body_text.split('--')[0]
        api_url = urllib2.quote('http://144.76.48.157:9003/search?source=api&query=%s'%body_text, safe=':/=&?')
        content = urllib2.urlopen(api_url).read().strip('\n ')
        content_json = json.loads(content)
        response_message = content_json['result'][0]['data']
        reply_mail = 'Hello, \n\n' + response_message + '\n\nBest Regards'
        msg_obj = MIMEText(reply_mail)
        msg_obj['Subject'] = mail_sub
        msg_obj['In-Reply-To'] = mail["Message-ID"]
        msg_obj['To']      = mail_sender
        msg_obj['From']    = user

        s = smtplib.SMTP('smtp.gmail.com:587')
        s.ehlo()
        s.starttls()
        s.login(user, pwd)
        s.sendmail(user, mail_sender, msg_obj.as_string())
        #s.sendmail(user, [mail_sender,], msg_obj.as_string())
        s.quit()

if __name__ == "__main__":
    run()
