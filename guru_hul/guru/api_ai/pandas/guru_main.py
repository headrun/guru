import os
import sys
import base64
import json
import urllib.request
import random
import time
import string
#import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

from django.http import JsonResponse
from decimal import Decimal
from django.utils import timezone
from django.conf import settings

from .settings import DOMAIN_NAME
from .actions import *

module_dir = os.path.dirname(__file__)  #get current directory

CLIENT_ACCESS_TOKEN = '873f6f5457b04192a79cd39bd6967e62'

from pprint import pprint

import apiai
ai = apiai.ApiAI(CLIENT_ACCESS_TOKEN)

def send_mail_alt(message, to, title):
    rec = []
    to_str = ''
    for addr in to.split(','):
        rec.append(addr.strip())
        to_str += "<%s>,"%addr.strip()
    message = """From: <%s>
To: %s
MIME-Version: 1.0
Content-Type: text/html
Subject: %s

%s""" % ('guru@headrun.com', to_str, title, message)

    print("\n\nfinal_message:" + message)
    try:
        smtpObj = smtplib.SMTP('localhost')
        s = smtpObj.sendmail('guru@headrun.com', rec , message)
        print('successful')
        #print(s)
        return 1
    except Exception as e:
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        print("Error: unable to send email")
        return 0

def get_answer(request):
    user_query = request.GET['query']
    print(user_query)
    exec_times = {}
    raw_info = {}
    answer = {}
    print('\n'*2)
    try:
    #if True:
        #apiai request
        req = ai.text_request()
        req.query = user_query
        t1 = time.time()
        resp = json.loads(req.getresponse().read().decode('utf-8'))
        exec_times['api.ai'] = time.time() - t1
        print('apiai_response:' + '\n\n')
        result = resp['result']
        pprint(str(result).encode('utf-8', 'ignore'))
        raw_info['action'] = result.get('action', '')
        raw_info['entities'] = result.get('parameters', {})
        raw_info['score'] = result.get('score', '')

        action = result.get('action', None)
        confidence = result.get('score', None)
        print('ACTION:', action)
        if not action:
            pass
        if action == 'input.unknown' or result.get('actionIncomplete', None):
            answer = {}
            answer['type'] = 'message'
            answer['data'] = result['fulfillment']['speech']
            return JsonResponse({'result': answer['data'], 'info': raw_info})
        if action.startswith(('smalltalk', 'wisdom', 'translate', 'news')):
            answer = {}
            answer['type'] = 'message'
            answer['data'] = result['fulfillment']['speech']
            return JsonResponse({'result': answer['data'], 'info': raw_info})
        if confidence < 0.5:
            answer = {}
            answer['type'] = 'message'
            answer['data'] = "Didn't quite get it."
            return JsonResponse({'result': answer['data'], 'info': raw_info})

        entities = result['parameters']
        if action == "get_help":
            answer = {}
            answer['type'] = 'message'
            path = os.path.join(settings.BASE_DIR, 'guru/templates/guru_help.html')
            print(path)
            answer['data'] = open(path, 'r').read()
            return JsonResponse({'result': answer['data'], 'info': raw_info})
        if action == "send_mail":
            email_address = entities.get('email_address', None)
            if not email_address:
                email_address = request.user.email
            prev_query_info = request.session.get('query_stack', {})
            print('\n\nprev query info:', prev_query_info)
            if not prev_query_info:
                answer = {}
                answer['type'] = 'message'
                answer['data'] = "Nothing to send. Perform some general Query first."
                return JsonResponse({'result': answer['data'], 'info': {}})
            pandas_query = prev_query_info['db_query']
            columns = prev_query_info['answer_format']
            raw_result = get_query_results(db_query=pandas_query, answer_type=prev_query_info['answer_type'], raw_info= {}, column_fields=columns)
            print(raw_result)
            raw_result['format'] = prev_query_info['answer_format']
            answer = generate_report_and_send_mail(raw_result=raw_result, nl_query=prev_query_info['nl_query'], email_address=email_address )
            print(answer)
            return JsonResponse({'result': answer['data'], 'info': {}})

        elif action == 'send_mail_cron':
            answer = {}
            answer['type'] = 'message'
            try:
            #if True:
                convert = {
                    'h': 3600,
                    'min': 60,
                    's': 1 }
                _duration = entities.get('cron_duration', None)
                if _duration.get('duration', None):
                    duration = _duration['duration']['amount'] * convert[_duration['duration']['unit']]
                elif _duration.get('period', None):
                    duration = int(_duration['period'])
                else:
                    answer['data'] = 'please specify the "duration" clearly.'
                    return JsonResponse({'result': answer['data'],'info': {}})
                print(duration)
                prev_query_info = request.session.get('query_stack', {})
                if not prev_query_info:
                    answer['data'] = "Nothing to Schedule. Perform some general Query first."
                    return JsonResponse({'result': answer['data'],'info': {}})
                request.user.profile.db_query = prev_query_info['db_query']
                request.user.profile.nl_query = prev_query_info['nl_query']
                request.user.profile.answer_type = prev_query_info['answer_type']
                request.user.profile.answer_format = prev_query_info['answer_format']
                request.user.profile.duration = duration
                request.user.profile.next_trigger_on = timezone.now() + timezone.timedelta(seconds=int(duration))
                request.user.profile.save()
                answer['data'] = "Cron job Scheduled for %s. You'll recieve Automated Notifications."% (request.user.email)
                return JsonResponse({'result': answer['data'], 'info': {}})
            except Exception as e:
            #if False:
                print('Error')
                print(e, 'line:', sys.exc_info()[-1].tb_lineno)
                answer['data'] = "Im having problem scheduling Cron."
                return JsonResponse({'result': answer['data'],'info': {}})
        else:
            answer = eval(action)(entities)
            if answer['type'] != "message":
                #request.session['query_stack'] = {'db_query': pandas_query, 'nl_query': user_query, 'answer_type': answer['type'], 'answer_format': answer['format']}
                pass
            return JsonResponse({'result': answer['data'], 'info': raw_info})
            """
            else: #pandas_query couldn't be generated
                answer['type'] = "message"
                answer['data'] = "Query couldn't be generated. Im sorry."
                if 'format' in answer.keys():
                    del answer['format']
                request.session['query_stack'] = {}
                return JsonResponse({'result': answer['data'], 'info': raw_info})
            """
    except Exception as e:
    #if False:
        print('\n')
        print('actual error:')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        answer['type'] = "message"
        answer['data'] = "Sorry couldn't understand your Question. You need to have special Clearance."
        if 'format' in answer.keys():
            del answer['format']
        request.session['query_stack']= {}
        return JsonResponse({'result': answer['data'], 'info': raw_info})
    else:
        answer['type'] = "message"
        answer['data'] = "Sorry couldn't understand your Question."
        return JsonResponse({'result': answer['data'], 'info':{}})

def generate_report_and_send_mail(raw_result, nl_query, email_address):
    print('\n\nInside generate and send', raw_result)
    if raw_result['type'] == 'graph':
        data = raw_result['data']
        html_file = os.path.join(settings.BASE_DIR, 'guru/templates/rendered.html')
        fp = open(html_file, 'wt')
        fp.write(data)
        fp.close()
        name = '_'.join(str(timezone.now()).split())
        image_file = os.path.join(settings.BASE_DIR,'guru/static/assets/img/' + name + '.png')

        #convert html to .png
        _phantom = os.path.join(settings.BASE_DIR, 'phantom/bin/phantomjs')
        _script = os.path.join(settings.BASE_DIR, 'html_to_image.js')
        _command = _phantom+' '+_script+' '+html_file+' '+image_file
        print(_command)
        _output = os.system(_command)
        print(_output)
        msg= '<h3>Query: %s</h3> <br><br><br><img width=670 height=380 style="background-color:#2a2a2a" src= "http://%s/%s">'%(nl_query, DOMAIN_NAME, 'static/assets/img/' + name + '.png')
        status= send_mail_alt(msg, email_address, 'guruBOT Automated Response')
        answer= {}
        answer['type']= "message"
        if status == 1:
            answer['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
        else:
            answer['data']= "Problem sending mail to "+ email_address + ". Make sure you have entered a valid Email Address."
        return answer

    elif raw_result['type'] == 'text':
        table_data = '<h3>Query: %s</h3>'%(nl_query)
        table_data += raw_result['data']
        status= send_mail_alt(table_data, email_address, 'guruBOT Automated Response')
        answer = {}
        answer['type']= "message"
        if status== 1:
            answer['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
        else:
            answer['data']= "Problem sending mail to " + email_address+ ". Make sure you have entered a valid Email Address."
        return answer

        """
        categories = raw_result['data'][0]['date']
        series = []
        for seq in (raw_result['data']):
            temp = {}
            temp['name'] = seq['key']
            temp['data'] = seq['values']
            series.append(temp)
        body = {}
        #name= ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
        name = '_'.join(str(timezone.now()).split())
        outfile = os.path.join(settings.BASE_DIR,'guru/static/assets/img/' + name + '.png')
        body["infile"] = "{title:{text: 'Report'}, chart: {backgroundColor: '#2a2a2a'}, xAxis: {categories: %s}, yAxis:{title: {text: '%s'}, plotLines:[{value: 0, width:1, color:'#808080'}]}, series: %s};"% (categories, raw_result['format'][1]['field'], series)
        body["outfile"] = outfile
        body["constr"] = "Chart"
        print(body)
        req = urllib.request.Request("http://localhost:3003")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        jsondata = json.dumps(body)
        jsondataasbytes = jsondata.encode('utf-8')
        req.add_header("Content-Length", len(jsondataasbytes))
        res= urllib.request.urlopen(req, jsondataasbytes)
        print(res.read())
        msg= '<h3>Query: %s</h3> <br><br><br><img src= "http://%s/%s">'%(nl_query, DOMAIN_NAME, 'static/assets/img/' + name + '.png')
        status= send_mail_alt(msg, email_address, 'guruBOT Automated Response')
        result= {}
        result['type']= "message"
        if status== 1:
            result['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
        else:
            result['data']= "Problem sending mail to "+ email_address + ". Make sure you have entered a valid Email Address."
        return result

    elif raw_result['type']== 'text':
        table_data= '<h3>Query: %s</h3><br><br><br><table style= "display: table; background-color: #2a2a2a; color: white; text-align: center; border: 1px solid #323232; padding: 10px">' %(nl_query)
        table_data+= '<thead style= "padding: 10px"><tr>'
        for chunk in raw_result['format']:
            table_data+= '<th>' + chunk['field'] + '</th>'
        table_data+= '</tr></thead><tbody style= "padding: 10px">'
        for chunk in raw_result['data']:
            table_data+= '<tr>'
            for i in range(len(chunk)):
                table_data+= '<td>' + str(chunk[i]) + '</td>'
            table_data+= '</tr>'
        table_data+= '</tbody></table>'
        print(table_data, file= open('temp.html','w'))
        status= send_mail_alt(table_data, email_address, 'guruBOT Automated Response')
        result= {}
        result['type']= "message"
        if status== 1:
            result['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
        else:
            result['data']= "Problem sending mail to " + email_address+ ". Make sure you have entered a valid Email Address."
        return result
        """
    else:
        result= {}
        result['type']= "message"
        result['data']= "Something's wrong."
        return result


