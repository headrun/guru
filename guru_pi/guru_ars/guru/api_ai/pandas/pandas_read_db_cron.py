import load
from load import read_from_db_pandas

import sys

if (len(sys.argv) > 1) and (sys.argv[1] == 'now'):
    columns = "id, store_code, store_name, style_ros, sell_thru, city, state, brand, sub_brand, style, size, category, created_date"
    read_from_db_pandas(table='ROS_sell_thru_Report', columns=columns, outfile='dumps/ROS_sell_thru_Report.h5')
    columns = "id, store_code, ros_week, sell_thru, city, state, brand, sub_brand, category, created_date"
    read_from_db_pandas(table='ROS_sell_thru_sc_Report', columns=columns, outfile='dumps/ROS_sell_thru_sc_Report.h5')

load.cron()
