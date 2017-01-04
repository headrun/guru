import os

import pandas as pd
import numpy as np

months = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
year = 2016
dumps_dir = '/root/djangoApps/worxogo/guru/api_ai/pandas/dumps'
outfile = os.path.join(dumps_dir, 'data.h5')

columns_names = ['user_name', 'store', 'city', 'state', 'user_mobile_no', 'user_employee_code', 'objective_no', 'objective_text', 'objective_type', 'target_obj_month', 'target_obj_value', 'target_obj_ach_per', 'target_obj_ach_value', 'target_obj_tobe_ach_per', 'target_obj_tobe_ach_value', 'target_obj_skew_indicator', 'objective_points', 'status', 'client_name']

columns = "A,B,C,D,E,F,G,H,J,K,L,M,N,O,P,R,AL,AN,AO"

frames = []
for x, y, files in os.walk(dumps_dir):
    for filename in files:
        if filename.startswith('Objective') and filename.endswith('.xlsx'):
            print(filename)
            df = pd.read_excel(os.path.join(dumps_dir, filename), parse_cols=columns)
            df.columns = columns_names
            _date = filename.split('.')[0].split('_')[-2:]
            #print(_date)
            #print(months[_date[0].lower()], _date[1])
            df['created_date'] = pd.datetime(year, months[_date[0].lower()], int(_date[1]))
            frames.append(df)

dataframe = pd.concat(frames)

print('sample:\n', dataframe.head())
dataframe.info()
print('writing to ', outfile)
dataframe.to_hdf(outfile,'df',mode='w',format='table')
print('done!')

