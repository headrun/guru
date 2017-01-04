import pandas as pd
import numpy as np
import os

dumps_dir = "/root/djangoApps/guru_worxogo/guru/api_ai/pandas/dumps/"
output_dir = os.path.join(dumps_dir, 'entities')

entities = ['user_name', 'region', 'territory', 'objective_text', 'objective_type']

wor_master= pd.read_hdf(os.path.join(dumps_dir, 'worxogo_merged.h5'))

print(wor_master.head())

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = wor_master[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

