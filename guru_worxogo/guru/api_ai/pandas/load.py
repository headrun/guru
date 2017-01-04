import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

def load_hdf(filename):
    print('loading hdf...')
    t1 = time.time()
    df = pd.read_hdf(filename,)
    print('load time:', time.time() - t1)
    #df = df.set_index('created_date', drop=False)
    #convert Timestamps to just Date
    #df['created_date'] = df['created_date'].dt.date
    df = df.replace(np.inf, np.NaN).fillna(value=0)
    print('info:\n\n', df.describe())
    print('sample:\n\n', df.head())
    return df

def read_from_db_pandas(db, tables):
    # The format here is mysql+mysqldb://USERNAME:PASSWORD@HOST/DB
    engine = create_engine('mysql+mysqlconnector://root:@localhost/'+db)
    print(engine)
    t1 = time.time()
    print(t1)
    if not os.path.exists('dumps/'+db):
        os.mkdir('dumps/'+db)
    for tb in tables:
        df = pd.read_sql_query('SELECT * FROM {table}'.format(table=tb), engine)
        print('storing ', tb)
        df.to_csv('dumps/'+db+'/'+tb+'.csv')
        df.to_hdf('dumps/'+db+'/'+tb+'.h5', 'df', format='table')
        #df.to_hdf(outfile,'df',mode='w',format='table')

def cron():
    import datetime
    print("Its a cron!")
    print('started at:', datetime.datetime.now())
    trigger_at = datetime.datetime.combine(datetime.datetime.now().date() + datetime.timedelta(days=1), datetime.datetime.min.time()) + datetime.timedelta(hours=2) # @4am everyday
    print('upcoming update at:', trigger_at)
    while(True):
        if (trigger_at - datetime.datetime.now()).days >= 0:
            continue
        trigger_at += datetime.timedelta(days=1)
        columns = "id, store_code, store_name, style_ros, sell_thru, city, state, brand, sub_brand, style, size, category, created_date"
        read_from_db_pandas(table='ROS_sell_thru_Report', columns=columns, outfile='dumps/ROS_sell_thru_Report.h5')
        columns = "id, store_code, ros_week, sell_thru, city, state, brand, sub_brand, category, created_date"
        read_from_db_pandas(table='ROS_sell_thru_sc_Report', columns=columns, outfile='dumps/ROS_sell_thru_sc_Report.h5')
        print('upcoming update at:', trigger_at)

if __name__ == "__main__":
    db_name = 'worxogo_latest'
    tables = ['badges', 'clients', 'err_badges', 'err_leaderboard', 'err_obj_list', 'err_obj_progress', 'err_user_list', 'migrations', 'obj_leaderboard','obj_list', 'obj_progress', 'upload_status', 'users']
    read_from_db_pandas(db=db_name, tables=tables)


