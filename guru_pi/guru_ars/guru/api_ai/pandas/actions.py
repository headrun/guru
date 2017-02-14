import os
import time
import datetime
import pandas as pd
import numpy as np
#import plotly.plotly as py
#import plotly.graph_objs as go

from django.utils import timezone
from dateutil.relativedelta import relativedelta

from .renderers import RenderTemplate
from .load import load_hdf

from .guru_responses import *

module_dir = os.path.dirname(__file__)

ROS_sell_thru_Report = load_hdf(os.path.join(module_dir, 'dumps/ROS_sell_thru_Report.h5'))
ROS_sell_thru_sc_Report = load_hdf(os.path.join(module_dir, 'dumps/ROS_sell_thru_sc_Report.h5'))

class ParsingError(Exception):
    """raised whenever there is some problem parsing
       condition or aggregate function parameters
    """
    def __init__(self, message):
        Exception.__init__(self, message)

def error_mesg(mesg):
    _mesg = {}
    _mesg['type'] = 'message'
    _mesg['data'] = mesg
    return [_mesg]

def df_to_table_data(_df):
    json_data = {"header":[], "rows":[]}
    json_data["header"].extend(_df.dtypes.index.tolist())
    for value in _df.values:
        row_values = value.tolist()
        _time = row_values.pop(-1)
        row_values.append(str(_time))
        json_data["rows"].append(row_values)
    return json_data

#converts DataFrame to chart specific data.
def df_to_chart_data(_df, type='line'):
    json_data = []
    for _data in levels(_df, type):
        json_data.append(_data)
    return json_data

def levels(_df, _type):
    for key in _df.index.get_level_values(level=0).unique():
        if _df.xs(key).index.nlevels > 1:
            levels(_df.xs(key, _type))
        else:
            for col in _df.xs(key).columns:
                _list = []
                for x, y in zip(_df.xs(key).index, _df.xs(key)[col]):
                    _list.append([x, y])
                yield {'type': _type, 'name': key, 'data': _list}


# function accepts pandas DataFrame and return HTML.
def generate_result(df, result_type, section, **others):
    answer = {}
    print(others)
    print(df.head())
    if df.empty:
        return error_mesg(get_resp_no_records())
    if result_type == 'graph':
        result = RenderTemplate('test.html')
        content = others
        content['header'] = others.get('header', '')
        content['type'] = 'graph'
        content['graph_type'] = others.get('graph_type', 'line')
        content['body'] = df
        result.render_to(section=section, content=content)
        result = result.render()
        answer['type'] = 'graph'
        answer['data'] = result
        answer['format'] = list(df.columns)
        return answer
    else:
        df = df.drop_duplicates()
        # limiting row count
        df = df[:500]
        data = []
        answer_format = list(df.columns)
        for row in df.values:
            temp = []
            for r in row:
                temp.append(str(r))
            data.append(temp)

        table_data= '<br><br><table style= "width:100%; background-color:#2a2a2a; color:white; text-align:center; padding:10px;">'
        table_data+= '<tr>'
        for col in answer_format:
            table_data+= '<th style="border-bottom: 1px solid #ddd; text-align:center;height:40px;">' + col + '</th>'
        table_data+= '</tr>'
        for chunk in data:
            table_data+= '<tr>'
            for i in range(len(chunk)):
                table_data+= '<td style="border-bottom: 1px solid #ddd; text-align:center;height:40px;">' + str(chunk[i]) + '</td>'
            table_data+= '</tr>'
        table_data+= '</table>'

        result = RenderTemplate('test.html')
        content = others
        content['header'] = others.get('header', '')
        content['type'] = 'table'
        content['body'] = table_data
        result.render_to(section=1, content=content)
        result = result.render()
        answer['type'] = 'table'
        answer['data'] = result
        answer['format'] = list(df.columns)
        return answer

"""
templates = {}
limit = ''
templates['tb_name+columns'] = "{tb_name}[{columns}]"
templates['tb_name+columns+order_by'] = "{tb_name}[{columns}].sort_values(by={order_by}, ascending=False)"
templates['tb_name+columns+conditions'] = "{tb_name}.ix[{conditions}][{columns}]"
templates['tb_name+columns+conditions+order_by'] = "{tb_name}.ix[{conditions}][{columns}].sort_values(by={order_by}, ascending=False)"
templates['tb_name+columns+conditions+group_by+order_by+agg_func'] = "{tb_name}.ix[{conditions}][{columns}].groupby({group_by}){agg_func}"
"""

def get_query(query_parameters):
    #search templates
    print('inside get_query()')
    print(query_parameters)
    for key in templates.keys():
        key_set= set(key.split('+'))
        print(key_set, set(query_parameters.keys()))
        if key_set == set(query_parameters.keys()):
            print('Match Found!')
            return templates[key].format(**query_parameters)
        else:
            print('Not a Match.')
"""
def get_tbname(parameters):
    #accept some parameters (column_names)
    #return table name
    if 'style' in parameters:
        return 'ROS_sell_thru_ss_Report'
    elif 'sub_brand' in parameters:
        return 'ROS_sell_thru_ssb_Report'
    else:
        return 'ROS_sell_thru_sb_Report'
"""

table_column_map = {
        'ROS_sell_thru_Report' : [('df', 'ROS_sell_thru_Report'),('ros', 'style_ros')],
        'ROS_sell_thru_sb_Report': [('df', 'ROS_sell_thru_sb_Report'),('ros', 'ros_week'),],
        'ROS_sell_thru_ssb_Report': [('df', 'ROS_sell_thru_ssb_Report'),('ros', 'ros_week'),],
        'ROS_sell_thru_ss_Report': [('df', 'ROS_sell_thru_ss_Report'),('ros', 'style_ros'),],
        'ROS_sell_thru_sc_Report': [('df', 'ROS_sell_thru_sc_Report'),('ros', 'ros_week'),('sell_thru', 'sell_thru')]}

def modify_columns(tbname, columns):
    #modify columns according to table name.
    for _map in table_column_map[tbname]:
        if _map[0] in columns:
            i = columns.index(_map[0])
            columns[i] = _map[1]
    print('inside modify columns:', columns)
    return columns

def modify_query(tbname, query):
    # modify query parameters as per table name
    for s1, s2 in table_column_map[tbname]:
        query = query.replace('<<' + s1 + '>>', s2)
    query = query.replace('<<', '').replace('>>', '')
    return query


def agg_func_list_to_str(agg_func_list, df):
    for agg_func in agg_func_list:
        agg_func_str = ''
        agg_func_str += "({df}['{arg}'] == {df}['{arg}'].{func}())".format(arg=agg_func['arg'], func=agg_func['func'], df=df)
        if agg_func.get('conn', None):
            agg_func_str += ' ' + connectors[agg_func['conn']]
        print(agg_func_str)
        return agg_func_str

def get_ros_info(entities, source):
    columns = ['ros']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('created_date')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by='created_date')
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]

        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())
    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_sell_thru_info(entities, source):
    columns = ['sell_thru']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('created_date')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by='created_date')
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_brand_info(entities, source):
    columns = ['brand']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)
    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('brand')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1)) 
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_location_state_info(entities, source):
    columns = ['state']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('state')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_location_city_info(entities, source):
    columns = ['city']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('city')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_location_info(entities, source):
    columns = ['city', 'state']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('state')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = {"header":[], "rows":[]}
        json_data["header"].extend(data.dtypes.index.tolist())
        for value in data.values:
            row_values = value.tolist()
            _time = row_values.pop(-1)
            row_values.append(str(_time))
            json_data["rows"].append(row_values)

        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_store_info(entities, source):
    columns = ['store_name', 'store_code']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:# created_date not in columns
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('store_code')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_sub_brand_info(entities, source):
    columns = ['sub_brand']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('sub_brand')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_style_info(entities, source):
    columns = ['style']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_list, columns = get_agg_functions(entities, columns)

    order_by = []
    if 'created_date' in columns:
        order_by.append('created_date')
    else:
        if 'ros' in columns or 'sell_thru' in columns:
            condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=7), periods=7, freq='D')))")
            #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
            columns.append('created_date')
            order_by.append('created_date')
        else:
            order_by.append('style')

    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.sort_values(by=order_by)
        if agg_func_list:
            _query = agg_func_list_to_str(agg_func_list, df='data')
            _query = modify_query(tb_name, _query)
            print(_query)
            _condition = eval(_query)
            print(_condition)
            data = data.ix[_condition]
        data = data.drop_duplicates()
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = df_to_table_data(data)
        res.append({"type":"message", "data": get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def plot_ros(entities, source):
    columns = ['sub_brand', 'ros']
    order_by = ['sub_brand']
    group_by = ['sub_brand', 'created_date']

    condition_list, columns = get_conditions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('created_date')
    if 'brand' not in columns:
        error_mesg('Please specify some brand e.g Us Polo, Arrow etc.')

    #remove the brand from the columns
    columns.remove('brand')
    #modify the column_names as per the Table_name

    tb_name = 'ROS_sell_thru_sc_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).mean()

    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'graph', section=1))
    else:
        json_data = df_to_chart_data(data)
        res.append({"type":"message", "data": "Heres your ROS report."})
        res.append({"type":"chart", "data": json_data})
    return res

def plot_sell_thru(entities, source):
    columns = ['sub_brand', 'sell_thru']
    order_by = ['sub_brand']
    group_by = ['sub_brand', 'created_date']

    condition_list, columns = get_conditions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('created_date')
    if 'brand' not in columns:
        error_mesg('Please specify some brand e.g Us Polo, Arrow etc.')

    #remove the brand from the columns
    columns.remove('brand')
    #modify the column_names as per the Table_name

    tb_name = 'ROS_sell_thru_sc_Report' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(\
                    tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(
                    tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).mean()

    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())

    res = []

    if source == 'web':
         # generate graph or table or text
        res.append(generate_result(data, 'graph', section=1))
    else:
        json_data = df_to_chart_data(data, type='line')
        res.append({"type":"message", "data": "Heres your Sell thru report."})
        res.append({"type":"chart", "data":json_data})
    return res

def _get_value_from_context(context, keyword, current_index=0):
    #search for the keyword in context (list of dict's)
    #search left side
    for exp in context[current_index-1::-1]:
        value = exp.get(keyword, None)
        if value:
            return value
    #search right side
    for exp in context[current_index+1:]:
        value = exp.get(keyword, None)
        if value:
            return value

operators = {
    'gt': '>',
    'lt': '<',
    'gte': '>=',
    'lte': '<=',
    'eq': '='}

connectors = {
    'and': '&',
    'or': '|'}

def get_agg_functions(entities, columns):
    #normalising aggregate functions
    print("Inside aggregate functions")
    agg_func_list = entities.get('function_aggregate', None)
    print(agg_func_list)
    for i in range(len(agg_func_list)):
        func = agg_func_list[i]
        if not func.get('func', None):
            val = _get_value_from_context(agg_func_list, 'func', current_index=i)
            if val:
                agg_func_list[i]['func'] = val
            else:
                raise ParsingError('Not a valid agg_func name.')
        if not func.get('arg', None):
            val = _get_value_from_context(agg_func_list, 'arg', current_index=i)
            if val:
                agg_func_list[i]['arg'] = '<<' + val + '>>'
                columns.append(agg_func_list[i]['arg'])
            else:
                raise ParsingError('Not a valid argument to agg_func.')
        else:
            columns.append(agg_func_list[i]['arg'])
            agg_func_list[i]['arg'] = '<<' + agg_func_list[i]['arg'] + '>>'

        if i < len(agg_func_list)-1:
            if not func.get('conn', None):
                agg_func_list[i]['conn'] = '&'
        else:
            if func.get('conn', None):
                del agg_func_list[i]['conn']

    print(agg_func_list)
    columns = sorted(set(columns), key=columns.index) #removing duplicates while retaining original sequence
    return (agg_func_list, columns)

def get_conditions(entities, columns=[]):
    """
        composes SQL WHERE CONDITION from entities
    """
    condition_list = []
    print('InPut inside compose condition:', columns)
    #normalising expressions
    exp_l = entities.get('expression', {})
    for i in range(len(exp_l)):
        exp = exp_l[i]
        if not exp.get('prop', None):
            val  = _get_value_from_context(exp_l, 'prop', current_index=i)
            if val:
                exp_l[i]['prop'] = val
            else:
                #parsing error
                raise ParsingError('Not a valid expression...prop missing.')
        if not exp.get('operator', None):
            val = _get_value_from_context(exp_l, 'operator', current_index=i)
            if val:
                exp_l[i]['operator'] = operators[val]
            else:
                raise ParsingError('Not a valid expression...operator missing.')
        else:
            exp_l[i]['operator'] = operators[exp['operator']]
        if not exp.get('value', None):
            val = _get_value_from_context(exp_l, 'value', current_index=i)
            if val:
                exp_l[i]['value'] = ''.join(val.split( ))
            else:
                raise ParsingError('Not a valid expression...prop_value missing.')
        if i < len(exp_l)-1:
            if not exp.get('conn', None):
                exp_l[i]['conn'] = '&'
        else:
            if exp.get('conn', None):
                del exp_l[i]['conn']

        _exp = ''
        for i, exp in enumerate(exp_l):
            _exp += "(<<df>>['<<{prop}>>'] {operator} {value})".format(prop=exp['prop'],
                        operator=exp['operator'], value= ''.join(exp['value'].split( )))
            columns.append(exp['prop'])
            if exp.get('conn', None):
                _exp += ' ' + connectors[exp['conn']]
        condition_list.append(_exp)
        print(condition_list)

    brand = entities.get('brand')
    if brand:
        condition_list.append("(<<df>>['<<brand>>'].isin(%s))"%brand)
        columns.append('brand')

    store_code = entities.get('store_code')
    if store_code:
        condition_list.append("(<<df>>['<<store_code>>'].isin(%s))"%store_code)
        columns.append('store_code')

    city = entities.get('city')
    if city:
        condition_list.append("(<<df>>['<<city>>'].isin(%s))"%city)
        columns.append('city')

    state = entities.get('state')
    if state:
        condition_list.append("(<<df>>['<<state>>'].isin(%s))"%state)
        columns.append('state')

    style = entities.get('style')
    if style:
        condition_list.append("(<<df>>['<<style>>'].isin(%s))"%style)
        columns.append('style')

    sub_brand = entities.get('sub_brand')
    if sub_brand:
        condition_list.append("(<<df>>['<<sub_brand>>'].isin(%s))"%sub_brand)
        columns.append('sub_brand')

    created_date = entities.get('created_date', None)
    if created_date:
        try:
            date_period= created_date.get('date-period', None)
            if date_period:
                dates = tuple(date_period.split('/'))
                condition_list.append("(<<df>>['<<created_date>>'].isin(pd.date_range(start='%s', end='%s', freq='D')))"%dates)
                columns.append('created_date')
            elif created_date.get('date', None):
                condition_list.append("(<<df>>['<<created_date>>'] == '%s')"%created_date['date'])
                columns.append('created_date')
        except Exception as e:
            raise ParsingError('Not a valid date')

    #finally, ``where condition``
    #condition = ' and '.join(condition_list)
    print('Inside compose_condition: condition = ', condition_list)
    print('Inside compose condition: columns = ', columns)
    columns = sorted(set(columns), key=columns.index) #removing duplicates while retaining original sequence
    print('Inside compose_condition: columns = ', columns)
    return (condition_list, columns)


