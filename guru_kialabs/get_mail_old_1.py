import email
import imaplib
import urllib2
import json
import smtplib
from email.mime.text import MIMEText
import BeautifulSoup as bs
import re
from HTMLParser import HTMLParser

user = 'headrun.mailtest@gmail.com'
pwd = 'headrun123'

BLACKLIST = [
        'sent from',
        'regards',
        'best regards',
        'thanks',
        'thank you',
        'greetings',
    ]

def text_filters(_text):
    _lines = []
    lines = _text.split('\n')
    for line in lines:
        line = line.strip()
        #line = re.sub(' +', ' ', line)
        if len(line) < 1:
            continue
        flag = 0
        for uw in BLACKLIST:
            if line.lower().startswith(uw.lower()):
                flag = 1
                break
        if flag == 1:
            continue

        _lines.append(line)
    return '\n'.join(_lines).strip()

def text_filters_2(body_text, mail_cc):
        mail_ids = []
        for _u in mail_cc:
            if len(_u.split()) > 1:
                for chunk in _u.split():
                    if chunk.startswith('<') and chunk.endswith('>'):
                        mail_ids.append(chunk[1:-1])
            else:
                mail_ids.append(_u)
        print(mail_ids)
        required_text = []
        flag = 0
        for _text in body_text.split('\n'):
            _text = _text.strip()
            if len(_text) < 1:
                continue
            if _text.startswith('On') or ('wrote:' in _text) or (_text.strip() in mail_ids):
                flag += 1
            else:
                flag = 0
            if "---------- Forwarded message ----------" in _text:
                return ' '.join(required_text)

            if flag >= 3:
                required_text = required_text[:-2]
                return ' '.join(required_text).strip()
            else:
                required_text.append(_text.strip())
        return ' '.join(required_text).strip()

def extract_text(body_html):
    #body_html = unicode(body_html, 'utf-8', 'ignore')
    blacklist = ['gmail_signature', 'gmail_quote']
    _html = bs.BeautifulSoup(body_html)
    body_text = ''
    bool_flag = 0
    node = _html.find("div", {"dir":"ltr"}) or _html.find("p", {"dir":"ltr"})
    if not node:
        return body_text
    for child in node.childGenerator():
        if type(child) is bs.NavigableString:
           body_text += '\n'+child
        elif child.get('class') in blacklist:
                continue
        else:
           for _ch in child.recursiveChildGenerator():
              if type(_ch) is bs.Tag:
                 if _ch.get('class') in blacklist:
                    bool_flag = 1
                    break
           if bool_flag == 1:
              continue
           else:
              body_text += '\n'+child.text
    return body_text.strip()

def multipart_extract(body):
    for part in body.get_payload():
        ctype = part.get_content_type()
        print(ctype)
        if ctype.lower().startswith("multipart"):
            for res in multipart_extract(body=part):
                print("1", type(res))
                if type(res) == str:
                    res = HTMLParser().unescape(unicode(res, 'utf-8', errors='ignore'))
                yield res
        if ctype == 'text/html':
            res = part.get_payload(decode=True)  # decode
            print("2", type(res))
            if type(res) == str:
                res = HTMLParser().unescape(unicode(res, 'utf-8', errors='ignore'))
            yield res

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

        mail_sub = mail['Subject'].strip()
        mail_sender = mail['From']
        mail_cc = mail['To'].split(',')
        mail_cc.append(mail_sender)
        mail_sender = mail_sender
        
        print '\n\n', mail_sender
        print '\n', mail_cc

        if mail.is_multipart():
            html_list = []
            for ele in multipart_extract(mail):
                html_list.append(ele)
            body_html = html_list[0]
            body_text = extract_text(body_html)
            body_text = body_text.encode('ascii', 'ignore')
        else:
            body_text = mail.get_payload(decode=True).strip()
            if type(body_text) == str:
                body_text = unicode(body_text, 'utf-8', errors='ignore')
            body_text = body_text.encode('ascii', 'ignore')
        #filter
        body_text = text_filters(body_text)
        body_text = text_filters_2(body_text, mail_cc)
        
        print('BODY:', body_text, len(body_text))
        if len(body_text) < 1:
            body_text = mail_sub
        
        print('FINAL:', body_text, len(body_text))
        
        api_url = urllib2.quote('http://144.76.48.157:9003/search?source=api&query=%s'%body_text, safe=':/=&?')
        content = urllib2.urlopen(api_url).read().strip('\n ')
        content_json = json.loads(content)
        response_message = content_json['result'][0]['data']
        reply_mail = 'Hello, \n\n' + response_message + '\n\nBest Regards,\nTeam Kialabs'
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
