import mysql.connector
import datetime
import sys

table_names= ['ROS_sell_thru_sb_Report',
            'ROS_sell_thru_ssb_Report',
            'ROS_sell_thru_ss_Report',
            'ROS_sell_thru_sc_Report',
            'ROS_sell_thru_Report']

if (len(sys.argv) > 1) and (sys.argv[1]=='now'):
    cnx = mysql.connector.connect(user= 'root', password= 'mieone^123', host= '5.9.63.147')
    cursor = cnx.cursor()
    cursor.execute('use test_nlp')
    for table in table_names:
        cursor.execute('select distinct created_date from %s order by created_date desc limit 30'% (table))
        interval = -1
        for field in cursor.fetchall():
            update = 'update %s set created_date="%s" where created_date="%s"'%(table, str(datetime.datetime.now().date() + datetime.timedelta(days= interval)), str(field[0]))
            print(update)
            cursor.execute(update)
            interval -= 1
            cnx.commit()


print('stated at:', datetime.datetime.now())
#trigger at 12am
trigger_at = datetime.datetime.combine(datetime.datetime.now().date() + datetime.timedelta(days=1), datetime.datetime.min.time())
print('upcoming update at:', trigger_at)
while(1):
    if (trigger_at - datetime.datetime.now()).days >= 0:
        continue
    trigger_at += datetime.timedelta(days=1)
    cnx= mysql.connector.connect(user= 'root', password= 'mieone^123', host= '5.9.63.147')
    cursor = cnx.cursor()
    cursor.execute('use test_nlp')
    for table in table_names:
        cursor.execute('select distinct created_date from %s order by created_date desc limit 30'% (table))
        interval= -1
        for field in cursor.fetchall():
            update= 'update %s set created_date="%s" where created_date="%s"'%(table, str(datetime.datetime.now().date() + datetime.timedelta(days= interval)), str(field[0]))
            print(update)
            cursor.execute(update)
            interval-= 1
            cnx.commit()
    print('upcoming update at:', trigger_at)

