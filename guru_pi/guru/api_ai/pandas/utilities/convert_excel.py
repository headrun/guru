import pandas as pd
import numpy as np
import os
import datetime

dumps_dir = "../dumps/"
output_dir = os.path.join(dumps_dir, 'entities')
#entities = ['year', 'month', 'type', 'geo_rgn_name', 'geo_city_name', 'geo_cnty_name', 'operator_name', 'abs_base']
month_map = ['JAN', 'FEB', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY', 'AUG', 'SEPT', 'OCT', 'NOV', 'DEC']


file = "Base_And_Gross_Ver3"
df = pd.read_excel(os.path.join(dumps_dir, file+'.xlsx'), sheetname=0)
entities = ['year', 'month', 'type', 'geo_rgn_name', 'geo_city_name', 'geo_cnty_name', 'operator_name', 'abs_base']
df.columns = entities
df = df.apply(lambda x: x.astype(str).str.strip().str.upper())
df['abs_base'] = pd.to_numeric(df['abs_base'])
df['start_date'] = df['month'].apply(lambda x: str(datetime.date(2016, int(x), 1)))
df['start_date'] = pd.to_datetime(df['start_date'])
df['month'] = df['month'].apply(lambda x: month_map[int(x)-1])

print(df.describe())
print(df.head())
outfile = os.path.join(dumps_dir, 'Base_Ver3.h5')
df.to_hdf(outfile,'df',mode='w',format='table')

file = "Base_And_Gross_Ver3"
df = pd.read_excel(os.path.join(dumps_dir, file+'.xlsx'), sheetname=1)
entities = ['year', 'month', 'type', 'geo_rgn_name', 'geo_city_name', 'geo_cnty_name', 'operator_name', 'abs_gross']
df.columns = entities
df = df.apply(lambda x: x.astype(str).str.strip().str.upper())
df['abs_gross'] = pd.to_numeric(df['abs_gross'])
df['start_date'] = df['month'].apply(lambda x: str(datetime.date(2016, int(x), 1)))
df['start_date'] = pd.to_datetime(df['start_date'])
df['month'] = df['month'].apply(lambda x: month_map[int(x)-1])

print(df.describe())
print(df.head())
outfile = os.path.join(dumps_dir, 'Gross_Ver3.h5')
df.to_hdf(outfile,'df',mode='w',format='table')

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = df[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

print('done!')

