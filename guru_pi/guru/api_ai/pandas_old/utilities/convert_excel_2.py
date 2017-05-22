import pandas as pd
import numpy as np
import os
import datetime

dumps_dir = "../dump_new/"
output_dir = os.path.join(dumps_dir, 'entities')
#entities = ['year', 'month', 'type', 'geo_rgn_name', 'geo_city_name', 'geo_cnty_name', 'operator_name', 'abs_base']
month_map = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


file = "sniper_data"
df = pd.read_excel(os.path.join(dumps_dir, file+'.xlsx'), sheetname=1)
df.columns = [x.strip("'") for x in df.columns]
df = df[['YEAR', 'MONTH', 'OPR', 'Base Absolute', '1-10 Gross Absolute', '1-20 Gross Absolute', 'Gross Absolute', 'T1M Quality Absolute', 'T2M Quality Absolute', 'T3M Quality Absolute', 'Migrant Base', 'Non Migrant Full Base', 'Bangladesh Base', 'Indonesia Base', 'Nepal Base', 'Migrant Full Gross', 'Non Migrant Full Gross', 'Bangladesh Gross', 'Indonesia Gross', 'Nepal Gross', 'HRCY3', 'HRCY4', 'HRCY5']]

df.columns = ['year', 'month', 'operator_name', 'abs_base', '1_10_abs_gross', '1_20_abs_gross', 'abs_gross', 't1m_quality_abs', 't2m_quality_abs', 't3m_quality_abs', 'migrant_base', 'non_migrant_base', 'bangladesh_base', 'indonesia_base', 'nepal_base', 'migrant_gross', 'non_migrant_gross', 'bangladesh_gross', 'indonesia_gross', 'nepal_gross', 'geo_cnty_name', 'geo_rgn_name', 'geo_city_name']

df = df.apply(lambda x: x.astype(str).str.strip("'").str.strip().str.upper() if x.dtype=='O' else x)
df['abs_base'] = pd.to_numeric(df['abs_base'])
df['abs_gross'] = pd.to_numeric(df['abs_gross'])
df['start_date'] = df['month'].apply(lambda x: str(datetime.date(2016, month_map.index(x)+1, 1)))
df['start_date'] = pd.to_datetime(df['start_date'])
#df['month'] = df['month'].apply(lambda x: month_map[int(x)-1])

print(df.describe())
print(df.head())
outfile = os.path.join(dumps_dir, 'sniper_data.h5')
df.to_hdf(outfile,'df',mode='w',format='table')

entities = ['year', 'month', 'operator_name', 'geo_cnty_name', 'geo_rgn_name', 'geo_city_name']

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = df[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

print('done!')

