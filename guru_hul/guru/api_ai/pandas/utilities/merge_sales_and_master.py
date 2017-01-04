import os

import pandas as pd
import numpy as np

dumps_dir = '/root/djangoApps/hul/guru/api_ai/pandas/dumps'
outfile = os.path.join(dumps_dir, 'hul.csv')

def df_to_upper(df):
    convert = lambda col:col.str.strip().str.upper()
    for key in df.keys():
        df[key] = convert(df[key].apply(str))
    return df

def to_upper(col):
     return col.apply(str).str.strip().str.upper()

frame = {}

print('reading "data.h5"')
frame['data'] = pd.read_hdf(os.path.join(dumps_dir, 'data.h5'))
frame['data']['basepackcode'] = to_upper(frame['data']['basepackcode'])
frame['data']['countercode'] = to_upper(frame['data']['countercode'])
frame['data']['moc'] = to_upper(frame['data']['moc'])
#frame['data']['year'] = frame['data']['year'].astype(int)
del frame['data']['year']

print('reading "product_info"')
frame['product_info'] = pd.read_excel(os.path.join(dumps_dir, 'Masters.xlsx'), sheetname=0)
_cols = [c.lower() for c in frame['product_info'].columns]
frame['product_info'].columns = _cols
frame['product_info']['basepackcode'] = to_upper(frame['product_info']['basepackcode'])
frame['product_info']['product_brand'] = to_upper(frame['product_info']['brand'])
del frame['product_info']['brand']

print('reading "counter_info"')
frame['counter_info'] = pd.read_excel(os.path.join(dumps_dir, 'Masters.xlsx'), sheetname=1)
_cols = [c.lower() for c in frame['counter_info'].columns]
frame['counter_info'].columns = _cols
frame['counter_info']['countercode'] = to_upper(frame['counter_info']['countercode'])
frame['counter_info']['moc'] = to_upper(frame['counter_info']['moc'])
frame['counter_info']['brand'] = to_upper(frame['counter_info']['brand'])
#frame['counter_info']['year'] = frame['counter_info']['year'].astype(int)
del frame['counter_info']['year']

print("merging...")
dataframe = pd.merge(frame['data'], frame['product_info'], on=['basepackcode'])
dataframe = pd.merge(dataframe, frame['counter_info'], on=['moc', 'countercode'])
dataframe['year'] = 2016
dataframe = dataframe.reset_index(drop=True)
print('sample:\n', dataframe.head())
dataframe.info()
print('writing to ', outfile)
dataframe.to_csv(outfile, index=False)
print('done!')

