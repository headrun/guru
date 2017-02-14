import os
import sys
import base64
import json
import urllib.request
import random
import time
import string
from pprint import pprint
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
from .guru_responses import *
from .renderers import *

module_dir = os.path.dirname(__file__)  #get current directory

CLIENT_ACCESS_TOKEN = "05f1939c5abe4254be0efc147997734e"

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
    print(request.method)
    if request.method == "POST":
        source = request.POST.get('source', 'web')
        user_query = request.POST.get('query')
    else:
        source = request.GET.get('source', 'web')
        user_query = request.GET.get('query')

    print(user_query, source)
    exec_times = {}
    raw_info = {}
    answer = {}
    print('\n'*2)

    if True:
    #try:
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

            if source != 'web':
                return JsonResponse({'result':[answer], 'info':raw_info})

            return JsonResponse({'result': answer['data'], 'info': raw_info})

        if action.startswith(('smalltalk', 'wisdom', 'translate', 'news')):
            answer = {}
            answer['type'] = 'message'
            answer['data'] = result['fulfillment']['speech']

            if source != 'web':
                return JsonResponse({'result':[answer], 'info':raw_info})

            return JsonResponse({'result': answer['data'], 'info': raw_info})

        if confidence < 0.3:
            answer = {}
            answer['type'] = 'message'
            answer['data'] = get_resp_confidence()

            if source != 'web':
                return JsonResponse({'result':[answer], 'info':raw_info})

            return JsonResponse({'result': answer['data'], 'info': raw_info})

        entities = result['parameters']

        if action == "get_help":
            answer = {}
            answer['type'] = 'message'
            path = os.path.join(settings.BASE_DIR, 'guru/templates/guru_help.html')
            answer['data'] = open(path, 'r').read()

            if source != 'web':
                return JsonResponse({'result':[answer], 'info':raw_info})

            return JsonResponse({'result': answer['data'], 'info': raw_info})

        if action == "send_mail":

            email_address = entities.get('email_address', None)

            if source != 'web':
                if request.method == 'POST':
                    if not email_address:
                        email_address = request.POST.get('email')
                    prev_query = request.POST.get('prev_query')
                    reply = request.POST.get('reply')
                else:
                    if not email_address:
                        email_address = request.GET.get('email')

                    prev_query = request.GET.get('prev_query')
                    reply = request.GET.get('reply')

                answer = {}
                answer['type']= "message"

                if prev_query and reply and email_address:
                    print(prev_query, reply)
                    answer = prepare_report_and_send_mail(
                            nl_query=prev_query,
                            data=reply,
                            email_address=email_address
                        )

                else:
                    answer['data'] = "Nothing to send."

                return JsonResponse({"result":[answer], 'info':{}})
            else: # old UI specific code.

                if not email_address:
                    email_address = request.user.email

                prev_query_info = request.session.get('query_stack', {})

                print('\n\nprev query info:', prev_query_info)
                if not prev_query_info:
                    answer = {}
                    answer['type'] = 'message'
                    answer['data'] = "Nothing to send. Perform some general Query first."
                    return JsonResponse({'result': answer['data'], 'info': {}})

                answer = generate_report_and_send_mail(
                            nl_query=prev_query_info['nl_query'],
                            data=prev_query_info['data'],
                            email_address=email_address
                )

                return JsonResponse({'result': answer['data'], 'info': {}})

        elif action == 'send_mail_cron':
            answer = {}
            answer['type'] = 'message'

            convert = {
                    'h': 3600,
                    'min': 60,
                    's': 1 }
            try:
                _duration = entities.get('cron_duration', None)

                if _duration.get('duration', None):
                    duration = _duration['duration']['amount'] * convert[_duration['duration']['unit']]
                elif _duration.get('period', None):
                    duration = int(_duration['period'])
                else:
                    answer['data'] = 'please specify the "duration" clearly.'

                    if source != 'web':
                        return JsonResponse({'result': [answer], 'info': {}})

                    return JsonResponse({'result': answer['data'],'info': {}})

                prev_query_info = request.session.get('query_stack', {})

                if not prev_query_info:

                    answer['data'] = "Nothing to Schedule. \
                                Perform some general Query first."

                    if source != 'web':
                        return JsonResponse({'result': [answer], 'info': {}})

                    return JsonResponse({'result': answer['data'],'info': {}})

                request.user.profile.db_query = prev_query_info['db_query']
                request.user.profile.nl_query = prev_query_info['nl_query']
                request.user.profile.answer_type = prev_query_info['answer_type']
                request.user.profile.answer_format = prev_query_info['answer_format']
                request.user.profile.duration = duration
                request.user.profile.next_trigger_on = timezone.now() + timezone.timedelta(seconds=int(duration))
                request.user.profile.save()

                answer['data'] = "Cron job Scheduled for %s. \
                            You'll recieve Automated Notifications."% (request.user.email)

                if source != 'web':
                    return JsonResponse({'result':[answer], 'info':{}})

                return JsonResponse({'result': answer['data'], 'info': {}})

            except Exception as e:
            #if False:
                print('Error')
                print(e, 'line:', sys.exc_info()[-1].tb_lineno)
                answer['data'] = "Im having problem scheduling Cron."

                if source != 'web':
                        return JsonResponse({'result': [answer], 'info': {}})

                return JsonResponse({'result': answer['data'],'info': {}})

        else: #any other action

            res_data = eval(action)(entities, source)

            if source != 'web':
                return JsonResponse({'result':res_data, 'info':{}})

            answer = res_data[-1]

            if answer['type'] == "message":
                if request.session.get('query_stack', None):
                    del request.session['query_stack']
            else:
                request.session['query_stack'] = {'nl_query':user_query, 'data':answer['data']}

            return JsonResponse({'result':answer['data'], 'info':raw_info})

    #except Exception as e:
    if False:
        print('\n')
        print('actual error:')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)

        answer['type'] = "message"
        answer['data'] = get_resp_negative()

        if 'format' in answer.keys():
            del answer['format']
        request.session['query_stack']= {}

        if source != 'web':
            return JsonResponse({'result':[answer], 'info':raw_info})

        return JsonResponse({'result': answer['data'], 'info': raw_info})

#This is meant to work with the old interface
#converts HTML data to .png or similar

def generate_report_and_send_mail(nl_query, data, email_address):

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

    msg= '<h3>Query: %s</h3><br>\
                <img style="background-color:#2a2a2a" \
                src= "http://%s/%s">'%(nl_query, DOMAIN_NAME, 'static/assets/img/' + name + '.png')

    status= send_mail_alt(msg, email_address, 'guruBOT Automated Response')

    answer= {}
    answer['type']= "message"
    if status == 1:
        answer['data'] = get_resp_mail_success(email_address)
    else:
        answer['data'] = "Problem sending mail to "+ email_address + ". \
                    Make sure you have entered a valid Email Address."
    return answer

def prepare_report_and_send_mail(nl_query, data, email_address):

    html_file = os.path.join(settings.BASE_DIR, 'guru/templates/report_templates/'+'_'.join(str(timezone.now()).split())+'.html')
    #prepare HTML Report.
    r_html = GenerateHtml(save_in=html_file)
    data = eval(data)

    r_html.append(type='message', data=nl_query)
    if data['type'] == 'table':
        r_html.append(type='table', data=data['data'])
    elif data['type'] == 'chart':
        r_html.append(type='chart', data="series:"+str(data['data']))

    r_html.generate() #report created.

    #Now convert HTML Report to an Image using phantomjs.
    i_name = '_'.join(str(timezone.now()).split())
    image_file = os.path.join(settings.BASE_DIR,'guru/static/assets/img/'+i_name+'.png')

    _phantom = os.path.join(settings.BASE_DIR, 'phantom/bin/phantomjs')
    _script = os.path.join(settings.BASE_DIR, 'html_to_image.js')
    _command = _phantom+' '+_script+' '+html_file+' '+image_file

    print(_command)
    _output = os.system(_command)

    os.remove(html_file)

    msg= '<img src= "http://%s/%s">'%(DOMAIN_NAME, 'static/assets/img/'+i_name+'.png')

    status= send_mail_alt(msg, email_address, 'guruBOT Automated Response')

    answer= {}
    answer['type']= "message"
    if status == 1:
        answer['data'] = get_resp_mail_success(email_address)
    else:
        answer['data'] = "Problem sending mail to "+ email_address + ". \
                    Make sure you have entered a valid Email Address."
    return answer

