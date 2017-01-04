
import pandas as pd
import numpy as np

import os

outfile = '../dumps/worxogo_merged_badges'

print('merging "badges" with "users" ')

wor_badges = pd.read_hdf('../dumps/badges.h5')

wor_users = pd.read_hdf('../dumps/users.h5', columns=['emp_code', 'email', 'region', 'territory', 'designation', 'status'])

wor_users = wor_users[ wor_users['status'] == 'A'].drop_duplicates()

wor_users.columns = ['emp_code', 'user_email', 'region', 'territory', 'designation', 'status']
del wor_users['status']

wor_badges['emp_code'] = wor_badges['emp_code'].astype(str)
wor_users['emp_code'] = wor_users['emp_code'].astype(str)


merged = wor_badges.merge(wor_users, on='emp_code')

merged['created_at'] = pd.to_datetime(merged['created_at'].dt.date)
merged['updated_at'] = pd.to_datetime(merged['updated_at'].dt.date)

merged = merged.sort_values(by=['created_at'], ascending=True)

print(wor_badges.info())
print(wor_users.info())
print(merged.info())

merged.to_hdf(outfile+'.h5', 'df', mode='w', format='table')
print(outfile+'.h5'+' written')
merged.to_csv(outfile+'.csv', index=False)
print(outfile+'.csv'+' written')
print('done!')

print('Now u can upload "worxogo_merged" and "worxogo_merged_badges" separately in GURU.')
