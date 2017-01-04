
import pandas as pd
import numpy as np

import os

outfile = '../dumps/worxogo_merged'

print('merging "obj_progress_master" with "users" (not merging "badges")')

wor_obj_progress = pd.read_hdf('../dumps/obj_progress_master.h5')

wor_users = pd.read_hdf('../dumps/users.h5', columns=['emp_code', 'email', 'region', 'territory', 'designation', 'user_points', 'reporting_user', 'reporting_name', 'reporting_designation'])

wor_users.columns = ['user_employee_code', 'user_email', 'region', 'territory', 'designation', 'user_points', 'reporting_user', 'reporting_name', 'reporting_designation']

wor_obj_progress['user_employee_code'] = wor_obj_progress['user_employee_code'].astype(str)

merged = wor_obj_progress.merge(wor_users, on='user_employee_code', how='left')

merged.to_hdf(outfile+'.h5', 'df', mode='w', format='table')
print(outfile+'.h5'+' written')
merged.to_csv(outfile+'.csv', index=False)
print(outfile+'.csv'+' written')
print('done!')
