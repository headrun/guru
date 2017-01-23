import pandas as pd
import numpy as np
import os
import datetime

dumps_dir = "../dumps/"
output_dir = os.path.join(dumps_dir, 'entities')

entities = ['year', 'month', 'type', 'geo_rgn_name', 'geo_city_name', 'geo_cnty_name', 'operator_name', 'abs_base']
month_map = ['JAN', 'FEB', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEPT', 'OCT', 'NOV', 'DEC']


file = "Telenor_Myanmar_Base_Jun_Sep_2016"
df = pd.read_hdf(os.path.join(dumps_dir, file+'.old.h5'))
df.columns = entities
df = df.apply(lambda x: x.astype(str).str.upper())
df['abs_base'] = pd.to_numeric(df['abs_base'])
df['start_date'] = df['month'].apply(lambda x: str(datetime.date(2016, int(x), 1)))
df['start_date'] = pd.to_datetime(df['start_date'])
df['month'] = df['month'].apply(lambda x: month_map[int(x)-1])

print(df.describe())
print(df.head())
outfile = os.path.join(dumps_dir, file+'.h5')
df.to_hdf(outfile,'df',mode='w',format='table')

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = df[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

file = "Telenor_Myanmar_Gross_Jul_Sep_2016"
df = pd.read_hdf(os.path.join(dumps_dir, file+'.old.h5'))
df.columns = entities
df = df.apply(lambda x: x.astype(str).str.upper())

df['abs_base'] = pd.to_numeric(df['abs_base'])
df['start_date'] = df['month'].apply(lambda x: str(datetime.date(2016, int(x), 1)))
df['start_date'] = pd.to_datetime(df['start_date'])
df['month'] = df['month'].apply(lambda x: month_map[int(x)-1])

print(df.describe())
print(df.head())
outfile = os.path.join(dumps_dir, file+'.h5')
df.to_hdf(outfile,'df',mode='w',format='table')

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = df[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

print('done!')

