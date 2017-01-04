import pandas as pd
import numpy as np
import os

def levels(_df):
    for key in _df.index.get_level_values(level=0).unique():
        #print('index_level', _df.xs(key).index.nlevels)
        #print(key)
        #print(_df.xs(key))
        if _df.xs(key).index.nlevels > 1:
            input('ok')
            levels(_df.xs(key))
        else:
            for col in _df.xs(key).columns:
                _list = []
                for x, y in zip(_df.xs(key).index, _df.xs(key)[col]):
                    _list.append([x, y])
                print('name:', key)
                print('data:', _list)

df = pd.read_hdf('dumps/ROS_sell_thru_sc_Report.h5')
data = df[ (df['brand'] == 'USPA') ][['brand', 'sub_brand', 'created_date', 'ros_week']]
data = data.groupby(['brand', 'sub_brand', 'created_date']).mean()

print(data.head())
levels(data)



