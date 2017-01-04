import os
import sys
import base64
import json
import urllib.request
import random
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
from .actions import (
    get_ros_info,
    get_sell_thru_info,
    get_location_info,
    get_location_state_info,
    get_location_city_info,
    get_brand_info,
    get_sub_brand_info,
    get_style_info,
    get_store_info,
    plot_ros,
    plot_sell_thru)

CLIENT_ACCESS_TOKEN = '1756b21aff5546b9bc915ac4bfdcddce'

module_dir = os.path.dirname(__file__)  #get current directory
from pprint import pprint
import mysql.connector
from mysql.connector import errorcode
import apiai
ai = apiai.ApiAI(CLIENT_ACCESS_TOKEN)

def mysql_connect():
    try:
        cnx = mysql.connector.connect(user='root', password='mieone^123', host='5.9.63.147', database='test_nlp')
    except Exception as e:
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
    else:
        return cnx

connector= mysql_connect()
cursor = connector.cursor()

def send_mail_alert(message, to, title):
    data = {'client_id' : 'FNVtgkQf7zrEaGyDUaS49j6I9c8AMyASHiZUB5M2',
            'client_secret': 'nC4R9llmMIdPXWci9BvP2J2q9RT7hHtLKO439iW8Eng5Sqwjezgws8ItdLnJ8x8MWd0mGAarYSEL8a9BeVaihom3TEwswUflKJFotQiMhqbfWb3JbTbrhzaS4jtTShz7',
            'call_back_uri': ''
            }
    data['to'] = []
    data['to'].extend(to)
    data['title'] = title
    data['message'] = message
    resp = requests.post("http://central.miebach.tech/api_calls/sendmail/", headers={'Content-Type': 'text/html', 'MIME-Version': 1.0}, data=data)
    print(resp)
    if '201' or '202' or '203' in resp:
        return 1
    else:
        return 0

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

def send_mail(request):
    #to= []
    #for e in request.values.get('email').split(','):
    #    to.append(e.strip())
    to = title= message= msg_type= ''
    to = request.POST['email']
    title = request.POST['title']
    message = request.POST['data']
    msg_type = request.POST['type']
    print(msg_type)
    print(to, title, message, msg_type)
    if msg_type == "graph":
        message = message[22:]
        file_path = os.path.join(module_dir, 'static/assets/img/imageSaved.png')
        print(file_path)
        fh= open(file_path, "wb")
        fh.write(base64.b64encode(message.encode('utf-8')))
        fh.close()
        message = '<img width="700" height="340" src="http://%s/static/assets/img/imageSaved.png">'%(DOMAIN_NAME)
    elif msg_type == 'table':
        print('inside sendmail:' + message)
        #status= send_mail_alert(to= to, title= title, message= message)
    else:
        print('Invalid type')
        return JsonResponse({'res':'error'})

    status = send_mail_alt(message, to, title)
    if status == 1:
        return JsonResponse({'res':'successful'})
    else:
        return JsonResponse({'res':'error'})

def get_answer(request):
    query = request.GET['query']
    print(query)
    raw_info = {}
    answer = {}
    print('\n'*2)
    try:
    #if True:
        #apiai request
        req = ai.text_request()
        req.query = query
        resp = json.loads(req.getresponse().read().decode('utf-8'))
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
            return JsonResponse({'result': answer, 'info': raw_info})
        if action.startswith(('smalltalk', 'wisdom', 'translate', 'news')):
            answer = {}
            answer['type'] = 'message'
            answer['data'] = result['fulfillment']['speech']
            return JsonResponse({'result': answer, 'info': raw_info})
        if confidence < 0.5:
            answer = {}
            answer['type'] = 'message'
            answer['data'] = "Didn't quite get it."
            return JsonResponse({'result': answer, 'info': raw_info})

        entities = result['parameters']
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
                return JsonResponse({'result': answer, 'info': {}})
            sql_query = prev_query_info['sql_query']
            raw_result = get_query_results(sql_query=sql_query, answer_type=prev_query_info['answer_type'], raw_info= {})
            print(raw_result)
            raw_result['format'] = prev_query_info['answer_format']
            answer = generate_report_and_send_mail(raw_result=raw_result, nl_query=prev_query_info['nl_query'], email_address=email_address )
            print(answer)
            return JsonResponse({'result': answer, 'info': {}})

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
                    return JsonResponse({'result': answer,'info': {}})
                print(duration)
                prev_query_info= request.session.get('query_stack', {})
                if not prev_query_info:
                    answer['data']= "Nothing to Schedule. Perform some general Query first."
                    return JsonResponse({'result': answer,'info': {}})
                print(request.user.profile.answer_type)
                print(request.user.profile.answer_format)
                print(request.user.profile.sql_query)
                print(request.user.profile.nl_query)
                print(request.user.profile.duration)
                print(request.user.profile.next_trigger_on)
                print(type(request.user.profile.duration))
                request.user.profile.sql_query= prev_query_info['sql_query']
                request.user.profile.nl_query= prev_query_info['nl_query']
                request.user.profile.answer_type= prev_query_info['answer_type']
                request.user.profile.answer_format= prev_query_info['answer_format']
                request.user.profile.duration= duration
                request.user.profile.next_trigger_on= timezone.now() + timezone.timedelta(seconds= int(duration))
                print(request.user.profile.answer_type)
                print(request.user.profile.answer_format)
                print(request.user.profile.sql_query)
                print(request.user.profile.nl_query)
                print(request.user.profile.duration)
                print(request.user.profile.next_trigger_on)
                print(type(request.user.profile.duration), type(request.user.profile), type(request.user))
                request.user.profile.save()
                answer['data']= "Cron job Scheduled for %s. You'll recieve Automated Notifications."% (request.user.email)
                return JsonResponse({'result': answer, 'info': {}})
            except Exception as e:
            #if False:
                print('Error')
                print(e, 'line:', sys.exc_info()[-1].tb_lineno)
                answer['data']= "Im having problem scheduling Cron."
                return JsonResponse({'result': answer,'info': {}})

        else:
            sql_query, columns, column_filters, answer_type = eval(action)(entities, extra_columns=[])
            answer_frmt = []
            print('*'*30)
            print('sql_query')
            print('*'*30)
            print(sql_query)
            raw_info['sql_query'] = sql_query
            if sql_query:
                answer = get_query_results(sql_query=sql_query, answer_type=answer_type, raw_info=raw_info, column_fields=columns)
                print(answer)
                if answer['type'] != "message":
                    request.session['query_stack'] = {'sql_query': sql_query, 'nl_query': query, 'answer_type': answer['type'], 'answer_format': answer['format']}

                return JsonResponse({'result': answer, 'info': raw_info})
            else: #sql_query couldn't be generated
                answer['type']= "message"
                answer['data']= "Sql query couldn't be generated. Im sorry."
                if 'format' in answer.keys():
                    del answer['format']
                request.session['query_stack']= {}
                return JsonResponse({'result': answer, 'info': raw_info})
    except Exception as e:
    #if False:
        print('\n')
        print('actual error :')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        answer['type']= "message"
        answer['data']= "Sorry couldn't understand your Question. You need to have some special Clearance."
        if 'format' in answer.keys():
            del answer['format']
        request.session['query_stack']= {}
        return JsonResponse({'result': answer, 'info': raw_info})
    else:
        answer['type']= "message"
        answer['data']= "Sorry couldn't understand your Question."
        return JsonResponse({'result': answer, 'info':{}})

def get_query_results(sql_query, answer_type, raw_info, column_fields= []):
    """executes ``sql_query`` and returns a dict."""
    answer= {}
    try:
    #if True:
        cursor.execute('set profiling = 1')
        cursor.execute(sql_query) #query execution
        if answer_type == 'graph':
            data = {}
            for row in cursor.fetchall():
                key, value, date = row
                _data = data.get(key, {'date':[], 'values':[], 'all':[]})
                _data['date'].append(str(date))
                _data['values'].append(float(value))
                _data['all'].append([date, float(value)])
                data[key] = _data

            final_data = []
            for k, v in data.items():
                v['key'] = k
                sorted_data = sorted(v['all'], key=lambda x: x[0])
                date, values = [], []
                for _date, _value in sorted_data:
                        date.append(str(_date))
                        values.append(_value)
                v['date'] = date
                v['values'] = values
                v.pop('all')
                final_data.append(v)
            answer['type'] = 'graph'
            answer['data'] = final_data
            answer['format'] = []
            for col in column_fields:
                answer['format'].append({'field': col, 'type': ''})
        else:
            data= []
            for row in cursor.fetchall():
                temp= []
                for r in row:
                    temp.append(str(r))
                data.append(temp)
            answer['type']= 'text'
            answer['data']= data
            answer['format']= []
            for col in column_fields:
                answer['format'].append({'field': col, 'type': ''})
        if not answer['data']:
            answer['type']= "message"
            answer['data']= "Didn't find any records. Sorry."
            if 'format' in answer.keys():
                del answer['format']

        cursor.execute('set profiling= 0')
        cursor.execute("SELECT query_id, SUM(duration) FROM information_schema.profiling GROUP BY query_id ORDER BY query_id DESC LIMIT 1")
        raw_info['execution_time']= float(cursor.fetchone()[1])
    except Exception as e:
    #if False:
        print('problem executing query')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        answer['type'] = "message"
        answer['data'] = "No records found."
        if 'format' in answer.keys():
            del answer['format']
        if request and request.session['query_stack']:
            request.session['query_stack']= {}
    finally:
    #if True:
        return answer

def generate_report_and_send_mail(raw_result, nl_query, email_address):
    if raw_result['type'] == 'graph':# then generate graph
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
    else:
        result= {}
        result['type']= "message"
        result['data']= "Something's wrong."
        return result


