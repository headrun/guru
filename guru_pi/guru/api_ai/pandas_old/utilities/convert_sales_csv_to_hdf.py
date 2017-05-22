import os

import pandas as pd
import numpy as np

year = 2016
day = 1
dumps_dir = '/root/djangoApps/hul/guru/api_ai/pandas/dumps'
outfile = os.path.join(dumps_dir, 'data.h5')

columns = ['year', 'moc', 'countercode', 'basepackcode', 'mrp', 'salesunits', 'salesvalue', 'openingunits', 'receiptunits', 'closingstockunits']
frames = []
for x, y, files in os.walk(dumps_dir):
    for filename in files:
        if filename.startswith('sku_data_MOC') and filename.endswith('.csv'):
            print(filename)
            df = pd.read_csv(os.path.join(dumps_dir, filename), names=columns, low_memory=False)
            month = filename.split('.')[0][-2:]
            #print(_date)
            #print(months[_date[0].lower()], _date[1])
            df['created_date'] = pd.datetime(year, int(month), day)
            frames.append(df)

dataframe = pd.concat(frames)
dataframe = dataframe.reset_index(drop=True)

print('sample:\n', dataframe.head())
dataframe.info()
print('writing to ', outfile)
dataframe.to_hdf(outfile,'df',mode='w',format='table')
print('done!')

