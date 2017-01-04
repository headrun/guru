import os

import pandas as pd
import numpy as np

months = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
year = 2016
dumps_dir = '/root/djangoApps/guru_worxogo/guru/api_ai/pandas/dumps'


outfile = os.path.join(dumps_dir, 'obj_progress_master')


def to_lower(cols):
    new = []
    for c in cols:
        new.append(c.lower())
    return new



frames = []

infile = os.path.join(dumps_dir, 'objectives_extract_till_Oct_20.xlsx')

print('loading...'+infile)
sheets = pd.read_excel(infile, sheetname=None)

for key, df in sheets.items():
    _date = key.split()[-2:]
    print(_date)
    print(df.columns)
    #print(months[_date[0].lower()], _date[1])
    df.columns = to_lower(df.columns)
    print(df.columns)
    df['date'] = pd.datetime(year, months[_date[0].lower()], int(_date[1]))
    print(df.head(5))
    frames.append(df)

dataframe = pd.concat(frames)
dataframe = dataframe.reset_index(drop=True)
dataframe['seg_obj_txt'] = dataframe['seg_obj_txt'].astype('str')
print('sample:\n', dataframe.head())
dataframe.info()
print('writing to ', outfile)
dataframe.to_hdf(outfile+'.h5', 'df', mode='w', format='table')
dataframe.to_csv(outfile+'.csv', index=False)
print('done!')
print('u can run merge_tables.py now.')

