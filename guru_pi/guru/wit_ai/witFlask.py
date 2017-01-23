#import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import base64
import os
from .settings import DOMAIN_NAME
from django.http import JsonResponse
from decimal import Decimal
import json
import urllib.request
import os
from django.utils import timezone
module_dir = os.path.dirname(__file__)  # get current directory
from django.conf import settings
import random
import string
from copy import deepcopy
token= 'Y4AEWGNLCWNLRFXXHTVUDZBGPAVW4GWD'

def say(session_id, context, msg):
    print(msg)

def merge(session_id, context, entities, msg):
    print(context)

def error(session_id, context, e):
    print(str(e))

actions = {
    'say': say,
    'merge': merge,
    'error': error,
}
from pprint import pprint
import mysql.connector
from mysql.connector import errorcode
from wit import *
client = Wit(token, actions)

from jinja2 import Template

templates= {}

templates['intents+items']= Template('SELECT {{items}} FROM {{intents}}')

templates['intents+items+conditions']= Template('SELECT {{items}} FROM {{intents}} WHERE {{conditions}}')

templates['intents+agg_functions+conditions']= Template("""SELECT {{agg_functions['agg_func'][0][0]}}({{agg_functions['agg_func'][0][1]}}) FROM {{intents}} WHERE {{conditions}}""")

templates['intents+items+agg_functions']=  Template("""SELECT {{items}} FROM {{intents}} WHERE {% for i in range(agg_functions['agg_func'] | length) %}{% set func=agg_functions['agg_func'][i][0] %}{% set arg= agg_functions['agg_func'][i][1] %}{% if i!=0 %} {{agg_functions['conn_agg_func'][i-1]}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{intents}}) {% endfor %}""")

templates['intents+items+conditions+agg_functions']= Template("""SELECT {{items}} FROM {{intents}} WHERE {% for i in range(agg_functions['agg_func'] | length) %}{% set func=agg_functions['agg_func'][i][0] %}{% set arg= agg_functions['agg_func'][i][1] %}{% if i!=0 %} {{agg_functions['conn_agg_func'][i-1]}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{intents}} WHERE {{conditions}}) {% endfor %} and {{conditions}}""")

templates['intents+items+conditions+groups+orderby']= Template('SELECT {{items}} FROM {{intents}} WHERE {{conditions}} GROUP BY {{groups}} ORDER BY{{orderby}}')

def get_query(query_parameters):
    #search templates
    print('inside')
    print(query_parameters)
    for key in templates.keys():
        key_set= set(key.split('+'))
        print(key_set, set(query_parameters.keys()))
        if key_set == set(query_parameters.keys()):
            print('yes')
            return templates[key].render(query_parameters)
        else:
            print('no')


#################################################################

def connect():
    global cnx
    global cursor
    try:
        cnx= mysql.connector.connect(user= 'root', password= 'mieone^123', host= '5.9.63.147')
        cursor= cnx.cursor()
        cursor.execute('use test_nlp')
        return 1
    except Exception as e:
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)

        return 0

#################################################################

def send_mail_alert(message, to, title):
    data = {'client_id' : 'FNVtgkQf7zrEaGyDUaS49j6I9c8AMyASHiZUB5M2',
            'client_secret': 'nC4R9llmMIdPXWci9BvP2J2q9RT7hHtLKO439iW8Eng5Sqwjezgws8ItdLnJ8x8MWd0mGAarYSEL8a9BeVaihom3TEwswUflKJFotQiMhqbfWb3JbTbrhzaS4jtTShz7',
            'call_back_uri': ''
            }
    data['to']= []
    data['to'].extend(to)
    data['title'] = title
    data['message']= message
    resp = requests.post("http://central.miebach.tech/api_calls/sendmail/", headers={'Content-Type': 'text/html', 'MIME-Version': 1.0}, data=data)
    print(resp)
    if '201' or '202' or '203' in resp:
        return 1
    else:
        return 0

def send_mail_alt(message, to, title):
    rec= []
    to_str= ''
    for addr in to.split(','):
        rec.append(addr.strip())
        to_str+= "<%s>,"%addr.strip()
    message = """From: <%s>
To: %s
MIME-Version: 1.0
Content-Type: text/html
Subject: %s

%s""" % ('guru@headrun.com', to_str, title, message)

    print("\n\nfinal_message:" + message)
    try:
        smtpObj = smtplib.SMTP('localhost')
        s= smtpObj.sendmail('guru@headrun.com', rec , message)
        print('successful')
        print(s)
        return 1
    except Exception as e:
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        print("Error: unable to send email")
        return 0

def sendMail(request):
    #to= []
    #for e in request.values.get('email').split(','):
    #    to.append(e.strip())
    to= title= message= msg_type= ''
    to = request.POST['email']
    title=request.POST['title']
    message= request.POST['data']
    msg_type= request.POST['type']
    print(msg_type)
    print(to, title, message, msg_type)
    #test code
    if msg_type== "graph":
        message= message[22:]
        #open("static/assets/image_raw", 'wb').write(message)
        #if os.path.exists("static/assets/img/imageSaved.png"):
        #    print('yes')
        #    os.remove('static/assets/img/imageSaved.png')
        #imgData= open('static/assets/image_raw', 'rb').read()
        file_path = os.path.join(module_dir, 'static/assets/img/imageSaved.png')
        print(file_path)
        fh= open(file_path, "wb")
        fh.write(base64.b64encode(message.encode('utf-8')))
        fh.close()
        message= '<img width="700" height="340" src="http://%s/static/assets/img/imageSaved.png">'%(DOMAIN_NAME)
    elif msg_type== 'table':
        print('inside sendmail:' + message) 
        #status= send_mail_alert(to= to, title= title, message= message)
    else:
        print('Invalid type')
        return JsonResponse({'res':'error'})

    status= send_mail_alt(message, to, title)
    if status== 1:
        return JsonResponse({'res':'successful'})
    else:
        return JsonResponse({'res':'error'})


def getAnswer(request):
    query= request.GET['query']
    print(query)
    answer_info= {}
    answer= {}

    print('\n'*2)
    try:
    #if True:
        #########  wit request   ###########
        resp= client.message(query)
        #########################################################################################
        print('wit_response:' + '\n\n')
        pprint(resp)
        #get the intent
        intent= resp['entities']['intent'][0]['value']

        if intent== "send_mail":
            if 'email' in resp['entities'].keys():
                email_address= resp['entities']['email'][0]['value']
            else:
                email_address= request.user.email
            prev_query_info= request.session.get('query_stack', {})
            print('\n\nprev query info:', prev_query_info)
            if not prev_query_info:
                answer= {}
                answer['type']= 'message'
                answer['data']= "Nothing to send. Perform some general Query first."
                return JsonResponse({'result': answer, 'info': {}})

            sql_query= prev_query_info['sql_query']
            answer['type']= prev_query_info['answer_type']
            answer['format']= prev_query_info['answer_format']
            raw_result= getQueryResults(sql_query= sql_query, answer= answer, answer_info= {})
            print(raw_result.content)
            raw_result= eval(raw_result.content)
            return generate_report_and_send_mail(raw_result= raw_result, nl_query= prev_query_info['nl_query'], email_address= email_address )

        elif intent== 'send_mail_cron':
            answer= {}
            answer['type']= 'message'
            try:
            #if True:
                if 'duration' in resp['entities'].keys():
                    duration= resp['entities']['duration'][0]['normalized']['value']
                elif 'period' in resp['entities'].keys():
                    duration= resp['entities']['period'][0]['value']
                print(type(duration))
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
                answer['data']= "Problem scheduling Cron."
                return JsonResponse({'result': answer,'info': {}})

        elif intent in ['ROS_sell_thru_Report', 'ROS_sell_thru_sc_Report']:#if intent specifies a DB Query
                raw_info= {'item': [], 'prop': [], 'intent': [], 'agg_func': [], 'agg_func_arg': [], 'operator': [], 'conn_expression': [], 'conn_agg_func': [], 'number': [], 'datetime': [], 'location': [], 'channel': [], 'brand': [],
    'conn_item': [], 'order_check': [], 'order_check_arg': [], 'interval': [], 'city': [], 'state': [], 'store_code': []}
                for key in resp['entities']:
                        print(key)
                        if key== 'datetime':
                            continue
                        list_temp= []
                        for i in range(len(resp['entities'][key])):
                            list_temp.append(resp['entities'][key][i]['value'])
                        raw_info[key]= list_temp
                for key in raw_info:
                        if raw_info[key]:
                            answer_info[key]= deepcopy(raw_info[key])
                            print('\t\t\t'+ key + ' : ' + str(raw_info[key]))
                print('answer_info: ', answer_info)
                agg_functions= {}
                if len(raw_info['conn_agg_func'])< len(raw_info['agg_func_arg'])- 1:
                    for i in range(len(raw_info['conn_agg_func']), len(raw_info['agg_func_arg'])- 1):
                        raw_info['conn_agg_func'].append('and')
                if len(raw_info['agg_func']) < len(raw_info['agg_func_arg']):
                    #replicate the last one
                    for i in range(len(raw_info['agg_func']), len(raw_info['agg_func_arg'])):
                        raw_info['agg_func'].append(raw_info['agg_func'][-1])

                #print(raw_info['conn_agg_func'])
                #print(raw_info['agg_func'])
                #print(raw_info['agg_func_arg'])
                temp_l= []
                for agg_func, agg_func_arg in zip(raw_info['agg_func'], raw_info['agg_func_arg']):
                    temp_l.append((agg_func, agg_func_arg))
                if temp_l:
                    agg_functions['agg_func']= temp_l
                temp_l= []
                for conn in raw_info['conn_agg_func']:
                    temp_l.append(conn)
                if temp_l:
                    agg_functions['conn_agg_func']= temp_l
                ###################################################################
                #              normalising expressions
                ###################################################################
                #print(raw_info['conn_expression'])
                #print(raw_info['prop'])
                #print(raw_info['operator'])
                #print(raw_info['number'])
                max_length= max(len(raw_info['operator']), len(raw_info['prop']))
                if len(raw_info['conn_expression']) < max_length- 1:
                    for i in range(len(raw_info['conn_expression']), max_length- 1):
                        raw_info['conn_expression'].append('and')
                if len(raw_info['prop']) < max_length:
                    for i in range(len(raw_info['prop']), max_length):
                        if len(raw_info['prop'])> 0:
                            raw_info['prop'].append(raw_info['prop'][-1])
                if len(raw_info['operator']) < max_length:
                        for i in range(len(raw_info['operator']), max_length):
                            if len(raw_info['operator'])> 0:
                                raw_info['operator'].append(raw_info['operator'][-1])
                            else:
                                raw_info['operator'].append('=')
                if len(raw_info['number']) < max_length:
                    for i in range(len(raw_info['number']), max_length):
                        if len(raw_info['number']) > 0:
                            raw_info['number'].append(raw_info['number'][-1])

                #print(raw_info['conn_expression'])
                #print(raw_info['prop'])
                #print(raw_info['operator'])
                #print(raw_info['number'])

                ###################################################################
                #	            joining expression to make a condition
                ###################################################################
                conditions= ''
                for i in range(max_length):
                    if i!= 0:
                        conditions+= ' ' + str(raw_info['conn_expression'][i-1])
                    conditions+= ' ' + str(raw_info['prop'][i]) + ' ' + str(raw_info['operator'][i]) + ' ' + str(raw_info['number'][i])
                """
                locations= ''
                for i, location in enumerate(raw_info['location']):
                    if i!= 0:
                            locations+= ' ' + 'and'
                    locations+= ' ' + 'location=' + '"' + str(location).upper() + '"'
                    #print('locations:', locations)
                if locations:
                    if len(conditions) < 1:
                        conditions+= ' ' + locations
                    else:
                        conditions+= ' ' + 'and' + ' ' + locations
                """
                #################### handling dates and intervals #########################
                if 'datetime' in resp['entities']:
                    for i, datetime in enumerate(resp['entities']['datetime']):
                        if i!= 0:
                            conditions+= ' and '
                        if (datetime['type']== 'value'):
                            if (datetime['grain']== 'week'):
                                if len(conditions)> 0:
                                    conditions+= ' and created_date between ' + '"'+datetime['value'].split('T')[0]+'"' + ' and ' + '"' + str(timezone.datetime.strptime(datetime['value'].split('T')[0], "%Y-%m-%d") + timezone.timedelta(days= 7)).split(' ')[0]+ '"'
                                else:
                                      conditions+= ' created_date between ' + '"'+datetime['value'].split('T')[0]+'"' + ' and ' + '"' + str(timezone.datetime.strptime(datetime['value'].split('T')[0], "%Y-%m-%d") + timezone.timedelta(days= 7)).split(' ')[0]+ '"'

                            elif (datetime['grain']== 'quarter'):
                                if len(conditions)> 0:
                                    conditions+= ' and created_date= ' + '"'+datetime['value'].split('T')[0]+'"'
                                else:
                                    conditions+= ' created_date= ' + '"'+datetime['value'].split('T')[0]+'"'

                            elif (datetime['grain'] in ['day', 'hour', 'minute', 'second']):
                                if len(conditions)> 0:
                                    conditions+= ' and created_date= ' + '"'+datetime['value'].split('T')[0]+'"'
                                else:
                                    conditions+= ' created_date= ' + '"'+datetime['value'].split('T')[0]+'"'
                        elif (datetime['type']== 'interval'):
                            if (datetime['from']['grain']== 'week'):
                                pass
                            elif (datetime['from']['grain']== 'quarter'):
                                pass
                            elif (datetime['from']['grain'] in ['day', 'hour', 'minute', 'second']):
                                if len(conditions)> 0:
                                    conditions+= ' and created_date between '+ '"'+ datetime['from']['value'].split('T')[0]+'"'
                                else:
                                    conditions+= ' created_date between ' + '"'+ datetime['from']['value'].split('T')[0]+'"'
                            if (datetime['to']['grain'] == 'week'):
                                pass
                            elif (datetime['to']['grain']== 'quarter'):
                                pass
                            elif (datetime['to']['grain'] in ['day', 'hour', 'minute', 'second']):
                                conditions+= ' and ' + '"'+ datetime['to']['value'].split('T')[0] + '"'

                ####################### brands ############################################
                brands= ''
                for i, brand in enumerate(raw_info['brand']):
                    if i!= 0:
                        brands+= ' ' + 'and'
                    brands+= ' ' + 'brand=' + '"' + str(brand).upper() + '"'
                    #print('brand:', brands)
                if brands:
                    if len(conditions) < 1:
                        conditions+= ' ' + brands
                    else:
                        conditions+= ' ' + 'and' + ' ' + brands
                        ########################## channels ###########################
                channels= ''
                for i, channel in enumerate(raw_info['channel']):
                    if i!= 0:
                            channels+= ' ' + 'and'
                    channels+= ' ' + 'channel=' + '"' + str(channel).upper() + '"'
                    #print('brand:', brands)
                if channels:
                    if len(conditions) < 1:
                        conditions+= ' ' + channels
                    else:
                        conditions+= ' ' + 'and' + ' ' + channels

                cities= ''
                for i, city in enumerate(raw_info['city']):
                    if i!= 0:
                            cities+= ' ' + 'and'
                    cities+= ' ' + 'city=' + '"' + str(city) + '"'
                if cities:
                    if len(conditions) < 1:
                        conditions+= ' ' + cities
                    else:
                        conditions+= ' ' + 'and' + cities
                states= ''
                for i, state in enumerate(raw_info['state']):
                    if i!= 0:
                            states+= ' ' + 'and'
                    states+= ' ' + 'state=' + '"' + str(state) + '"'
                if states:
                    if len(conditions) < 1:
                        conditions+= ' ' + states
                    else:
                        conditions+= ' ' + 'and' + states
                store_codes= ''
                for i, store_code in enumerate(raw_info['store_code']):
                    if i!= 0:
                        store_codes+= ' ' + 'and'
                    store_codes+= ' ' + 'store_code=' + '"' + str(store_code) + '"'
                if store_codes:
                    if len(conditions) < 1:
                        conditions+= ' ' + store_codes
                    else:
                        conditions+= ' ' + 'and' + store_codes
                print('sc', store_codes)

                         ####################################################################
                         #               intent means table_name
                         ####################################################################

                orderby= ''
                intents= ''
                groups= ''
                frmt= []

                if intent== "ROS_sell_thru_sc_Report":
                    if raw_info['order_check_arg']:   #order_check_arg is either 'sub_brand' or 'category'
                        raw_info['item'].append(raw_info['order_check_arg'][0])
                    else: #assume if not returned by wit 
                        raw_info['item'].append('sub_brand')

                    if "report_ros" in raw_info['item']:
                        raw_info['item'].append('avg(ros_week)')
                        raw_info['item'].append('created_date')
                        raw_info['item'].remove('report_ros')
                        groups+= 'created_date'

                        if 'sub_brand' in raw_info['item']:
                            groups+= ', sub_brand'
                            orderby+= ' sub_brand'
                        elif 'category' in raw_info['item']:
                            groups+= ', category'
                            orderby+= ' category'
                        else: #assume
                                groups+= ', sub_brand'
                                orderby+= ' sub_brand'
                        answer['type']= 'graph'
                        for item in raw_info['item']:
                            #if item== 'created_date':
                                frmt.append({'field': item, 'type': ''})
                            #else:
                            #    frmt.append({'field': item, 'type': ''})
                        answer['format']= frmt

                    if "report_sell_thru" in raw_info['item']:
                            raw_info['item'].append('avg(sell_thru)')
                            raw_info['item'].append('created_date')
                            raw_info['item'].remove('report_sell_thru')
                            groups+= 'created_date'
                            if 'sub_brand' in raw_info['item']:
                                    groups+= ', sub_brand'
                                    orderby+= ' sub_brand'
                            elif 'category' in raw_info['item']:
                                    groups+= ', category'
                                    orderby+= ' category'
                            else: #assume
                                    groups+= ', sub_brand'
                                    orderby+= ' sub_brand'
                            answer['type']= 'graph'
                            for item in raw_info['item']:
                                    #if item== 'created_date':
                                            frmt.append({'field': item, 'type': ''})
                                    #else:
                                    #        frmt.append({'field': item, 'type': ''})

                            answer['format']= frmt
                    if raw_info['interval'] or raw_info['datetime']:
                            #hardcoded, wit should return 'interval' but not returning in most of the cases
                            #if raw_info['interval'][0]== "week_last":
                                conditions+= ' and ' +  " created_date between DATE_SUB(now(), INTERVAL 7 DAY ) AND  now()"
                            #elif raw_info['interval'][0]== "week_current":
                                #conditions+= ' and ' + " created_date between DATE_SUB(now(), INTERVAL 7 DAY) AND  now()"
                    else:#assume
                                conditions+= ' and ' +  " created_date between DATE_SUB(now(), INTERVAL 7 DAY ) AND  now()"

                if intent== "ROS_sell_thru_Report":
                    #defaults
                    print('test', raw_info,'\n_sep_\n', answer_info)
                    if 'store_code' in raw_info['item']:
                        raw_info['item'].remove('store_code')
                    raw_info['item']= ['distinct store_code', 'store_name']+ raw_info['item']
                    print('\ntest', raw_info,'\n_sep_\n',  answer_info)
                    ##################################################
                    if raw_info['item']:
                        for prop in raw_info['prop']:
                            if prop not in raw_info['item']:
                                raw_info['item'].append(prop)
                        if raw_info['datetime'] and raw_info['item']:
                            raw_info['item'].append('created_date')
                        for loc in raw_info['location']:
                            if prop not in raw_info['item']:
                                raw_info['item'].append('location')
                        for agg_func in raw_info['agg_func_arg']:
                            if agg_func not in raw_info['item']:
                                raw_info['item'].append(agg_func)
                    answer['type']= 'text'
                    for item in raw_info['item']:
                        if item== 'distinct store_code':
                            frmt.append({'field': 'store_code', 'type': ''})
                        else:
                            frmt.append({'field': item, 'type': ''})
                    answer['format']= frmt
                    """datetimes= ''
                    for i, datetime in enumerate(raw_info['datetime']):
                        if i!= 0:
                                datetimes+= ' ' + 'and'
                        datetimes+= ' ' + 'created_date=' + '"' + str(datetime).split('T')[0] + '"'
                    #print('datetimes:', datetimes)
                    if datetimes:
                        if len(conditions) < 1:
                                conditions+= ' ' + datetimes
                        else:
                                conditions+= ' ' + 'and' + ' ' + datetimes
                    """
               ###################################################################
               #               joining items
               ###################################################################
                items= ''
                for i, item in enumerate(raw_info['item']):
                    if i!= 0:
                        items+= ','
                    items+= ' ' + item
                print('*'*30)
                print("COMPOSING SELECT QUERY PARAMETERS:")
                print('*'*30)
                print("\n\n\t\t\tINTENT / TABLE_NAME : " + str(intent))
                print("\n\n\t\t\tITEMS : " + str(items))
                print("\n\n\t\t\tAGG_FUNCTIONS : " + str(agg_functions))
                print("\n\n\t\t\tWHERE CONDITIONS : " + str(conditions))
                print("\n\n\t\t\tGROUPS : " + str(groups))
                print("\n\n\t\t\tORDER BY : " + str(orderby))

                #############################################################
                #    SELECT QUERY Parameters
                #############################################################
                query_parameters= {}
                query_parameters['intents']= intent
                if len(items)> 0:
                        query_parameters['items']= items
                if len(agg_functions)> 0:
                        query_parameters['agg_functions']= agg_functions
                if len(conditions)> 0:
                        query_parameters['conditions']= conditions
                if len(groups)> 0:
                        query_parameters['groups']= groups
                if len(orderby)> 0:
                        query_parameters['orderby']= orderby

                sql_query= get_query(query_parameters) ## invoke a template based on query parameters
                print('*'*30)
                print('sql_query')
                print('*'*30)
                print(sql_query)
                answer_info['sql_query']= sql_query
                #######################################################################################
                if sql_query:
                    return getQueryResults(sql_query= sql_query, nl_query= query, request= request, answer= answer, answer_info= answer_info)

                else:#sql couldn't be generated
                    answer['type']= 'message'
                    answer['data']= 'No records found.'
                    if 'format' in answer.keys():
                        del answer['format']
                    request.session['query_stack']= {}
                    return JsonResponse({'result': answer, 'info': answer_info})

    except Exception as e:
    #if False:
        print('\n')
        print('actual error :')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        answer['type']= 'message'
        answer['data']= "Sorry couldn't understand your Query. You need to have some special Clearance."
        if 'format' in answer.keys():
            del answer['format']
        request.session['query_stack']= {}
        return JsonResponse({'result': answer, 'info': answer_info})

def getQueryResults(sql_query, answer, nl_query= None, answer_info= {}, request= {}):
    """
        Always returns Json
    """
    print('answer_info:', answer_info)
    try:
            #########################################################
            #           Estabilishing db connection
            #########################################################
            if not connect():
                    answer['type']= 'message'
                    answer['data']= "connection problem"
                    return JsonResponse({'result': answer, 'info': answer_info})
            #########################################################
            cursor.execute('set profiling= 1')
            cursor.execute(sql_query) #query execution
            if answer['type']== 'graph':
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
                    answer['data']= final_data

            else: # answer['type']== 'text'
                data= []
                for row in cursor.fetchall():
                    temp= []
                    for r in row:
                        temp.append(str(r))
                    data.append(temp)
                answer['data']= data
            if not answer['data']:
                answer['type']= 'message'
                answer['data']= 'No records found.'
                if 'format' in answer.keys():
                    del answer['format']
                if request and request.session['query_stack']:
                    request.session['query_stack']= {}
    except Exception as e:
        print('problem executing query')
        print(e, 'line:', sys.exc_info()[-1].tb_lineno)
        answer['type']= 'message'
        answer['data']= 'No records found'
        if 'format' in answer.keys():
            del answer['format']
        if request and request.session['query_stack']:
            request.session['query_stack']= {}
    else:
        cursor.execute('set profiling= 0')
        cursor.execute("SELECT query_id, SUM(duration) FROM information_schema.profiling GROUP BY query_id ORDER BY query_id DESC LIMIT 1")
        answer_info['execution_time']= float(cursor.fetchone()[1])
        #saving NL query in session['query_stack']
        if request:
            request.session['query_stack']= {'sql_query': sql_query, 'nl_query': nl_query, 'answer_type': answer['type'], 'answer_format': answer['format']}
    finally:
        return JsonResponse({'result': answer, 'info': answer_info,} )

def generate_report_and_send_mail(raw_result, nl_query, email_address):
    print(raw_result)
    print(email_address)
    print(type(raw_result))
    if raw_result['result']['type']== 'graph':# then generate graph
        categories= raw_result['result']['data'][0]['date']
        series= []
        for seq in (raw_result['result']['data']):
            temp= {}
            temp['name']= seq['key']
            temp['data']= seq['values']
            series.append(temp)

        body= {}
        #name= ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
        name= '_'.join(str(timezone.now()).split())
        outfile= os.path.join(settings.BASE_DIR,'guru/static/assets/img/' + name + '.png')
        body["infile"]= "{title:{text: 'Report'}, chart: {backgroundColor: '#2a2a2a'}, xAxis: {categories: %s}, yAxis:{title: {text: '%s'}, plotLines:[{value: 0, width:1, color:'#808080'}]}, series: %s};"% (categories, raw_result['result']['format'][1]['field'], series)
        body["outfile"]= outfile
        body["constr"]= "Chart"
        print(body)


        req= urllib.request.Request("http://localhost:3003")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        jsondata= json.dumps(body)
        jsondataasbytes= jsondata.encode('utf-8')
        req.add_header("Content-Length", len(jsondataasbytes))
        res= urllib.request.urlopen(req, jsondataasbytes)
        print(res.read())
        msg= '<h3>Query: %s</h3> <br><br><br><img src= "http://%s/%s">'%(nl_query, DOMAIN_NAME, 'static/assets/img/' + name + '.png')
        status= send_mail_alt(msg, email_address, 'guruBOT Automated Response')
        answer= {}
        answer['type']= "message"
        if status== 1:
            answer['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
            return JsonResponse({'result': answer, 'info': {} })
        else:
            answer['data']= "Problem sending mail to "+ email_address + ". Make sure you have entered a valid Email Address."
            return JsonResponse({'result': answer, 'info': {}})

    elif raw_result['result']['type']== 'text':
        table_data= '<h3>Query: %s</h3><br><br><br><table style= "display: table; background-color: #2a2a2a; color: white; text-align: center; border: 1px solid #323232; padding: 10px">' %(nl_query)
        table_data+= '<thead style= "padding: 10px"><tr>'
        for chunk in raw_result['result']['format']:
            table_data+= '<th>' + chunk['field'] + '</th>'
        table_data+= '</tr></thead><tbody style= "padding: 10px">'
        for chunk in raw_result['result']['data']:
            table_data+= '<tr>'
            for i in range(len(chunk)):
                table_data+= '<td>' + str(chunk[i]) + '</td>'
            table_data+= '</tr>'
        table_data+= '</tbody></table>'
        print(table_data, file= open('temp.html','w'))
        status= send_mail_alt(table_data, email_address, 'guruBOT Automated Response')
        answer= {}
        answer['type']= "message"
        if status== 1:
            answer['data']= "Mail successfully delivered to " + email_address + '. It may appear as Spam.'
            return JsonResponse({'result': answer, 'info': {}})
        else:
            answer['data']= "Problem sending mail to " + email_address+ ". Make sure you have entered a valid Email Address."

            return JsonResponse({'result': answer, 'info': {}})
    else:
        answer= {}
        answer['type']= "message"
        if status== 1:
            answer['data']= "Something's wrong."
            return JsonResponse({'result': answer, 'info': {}})


