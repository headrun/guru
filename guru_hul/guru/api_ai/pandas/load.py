import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import time

def load_hdf(filename, stop=None):
    print('loading hdf...')
    t1 = time.time()
    if stop:
        df = pd.read_hdf(filename, stop=stop)
    else:
        df = pd.read_hdf(filename, stop=1000)
    print('load time:', time.time() - t1)
    #df = df.set_index('created_date', drop=False)
    #convert Timestamps to just Date
    #df['created_date'] = df['created_date'].dt.date
    df = df.replace(np.inf, np.NaN).fillna(value=0)
    print('info:\n\n', df.describe())
    print('sample:\n\n', df.head())
    return df

def read_from_db_pandas(table, columns, outfile):
    # The format here is mysql+mysqldb://USERNAME:PASSWORD@HOST/DB
    engine = create_engine('mysql+mysqlconnector://root:mieone^123@5.9.63.147/test_nlp')
    print(engine)
    t1 = time.time()
    print(t1)
    df = pd.read_sql_query('SELECT {columns} FROM {table} where created_date between DATE_SUB(now(), INTERVAL 10 DAY) and now() order by created_date desc'.format(columns=columns, table=table), engine, parse_dates=['created_date'])
    print('Took: ', time.time() - t1)
    print(df.describe())
    print('sample:', df.head())
    df.info(memory_usage='deep')
    print('storing ', outfile)
    df.to_hdf(outfile,'df',mode='w',format='table')

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
    columns = "id, store_code, store_name, style_ros, sell_thru, city, state, brand, sub_brand, style, size, category, created_date"
    read_from_db_pandas(table='ROS_sell_thru_Report', columns=columns, outfile='dumps/ROS_sell_thru_Report.h5')
    columns = "id, store_code, ros_week, sell_thru, city, state, brand, sub_brand, category, created_date"
    read_from_db_pandas(table='ROS_sell_thru_sc_Report', columns=columns, outfile='dumps/ROS_sell_thru_sc_Report.h5')
