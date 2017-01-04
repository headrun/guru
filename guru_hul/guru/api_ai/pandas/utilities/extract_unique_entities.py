import pandas as pd
import numpy as np
import os

dumps_dir = "/root/djangoApps/hul/guru/api_ai/pandas/dumps/"
output_dir = os.path.join(dumps_dir, 'entities')

entities = ['account', 'productfamily', 'city', 'state', 'region', 'brand', 'region', 'countercode', 'basepackcode']

hul = pd.read_hdf(os.path.join(dumps_dir, 'hul.h5'))

print(hul.head())

for e in entities:
    fp = os.path.join(output_dir, e+'.csv')
    values = hul[[e]].drop_duplicates()
    values = values[[e,e]]
    values.to_csv(fp, index=False, header=False)

