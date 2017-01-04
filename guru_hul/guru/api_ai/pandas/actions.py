
import os
import time
from django.utils import timezone
import pandas as pd
import numpy as np

from .renderers import RenderTemplate
from .load import load_hdf

import datetime
from dateutil.relativedelta import relativedelta


module_dir = os.path.dirname(__file__)  #get current directory

hul_df = load_hdf(os.path.join(module_dir, 'dumps/hul.h5'))

class ParsingError(Exception):
    """Generated whenever there is problem parsing
       condition or aggregate function parameters
    """
    def __init__(self, message):
        Exception.__init__(self, message)


def execute_query(db_query, raw_info={}):
    """executes ``pandas_query``"""
    answer = {}
    exec_times = raw_info.get('execution_time', {})
    #execution
    for query in db_query:
        print('executing:', query)
        df = eval(query)
        print(df.head())
    return df

def generate_result(df, answer_type, raw_info={}):
    answer = {}
    exec_times = raw_info.get('execution_time', {})
    try:
    #if True:
        if df.empty:
            answer['type'] = "message"
            answer['data'] = "Didn't find any records. Sorry."
            return answer

        if answer_type == 'graph':
            result = RenderTemplate('test.html')
            content = {}
            content['header'] = ''
            content['type'] = 'graph'
            content['graph_type'] = 'line'
            content['body'] = df
            #print(content)
            result.render_to(section=1, content=content)

            result = result.render()
            answer['type'] = 'graph'
            answer['data'] = result
            answer['format'] = list(df.columns)
            return answer

        else:
            t1 = time.time()
            df = df.drop_duplicates()

            # limiting row count
            df = df[:5000]

            exec_times['drop_duplicates'] = time.time() - t1
            print(df.head())
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
            content = {}
            content['header'] = ''
            content['type'] = 'table'
            content['body'] = table_data
            result.render_to(section=1, content=content)

            result = result.render()
            answer['type'] = 'table'
            answer['data'] = result
            answer['format'] = list(df.columns)
            return answer
    except Exception as e:
    #if False:
        print('problem')
        print(e)
        answer['type'] = "message"
        answer['data'] = "Problem in execution. Sorry."
        if 'format' in answer.keys():
            del answer['format']
        return answer

templates = {}
limit = ''
templates['tb_name+columns'] = "{tb_name}[{columns}]"
templates['tb_name+columns+order_by'] = "{tb_name}[{columns}].sort_values(by={order_by}, ascending=False)"
templates['tb_name+columns+conditions'] = "{tb_name}.ix[{conditions}][{columns}]"
templates['tb_name+columns+conditions+order_by'] = "{tb_name}.ix[{conditions}][{columns}].sort_values(by={order_by}, ascending=False)"
templates['tb_name+columns+conditions+group_by'] = "{tb_name}.ix[{conditions}][{columns}].groupby({group_by})"
templates['tb_name+columns+conditions+group_by+order_by+agg_func'] = "{tb_name}.ix[{conditions}][{columns}].groupby({group_by}){agg_func}"

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

def get_tbname(parameters):
    #accept some parameters (column_names)
    #return table name
    if 'style' in parameters:
        return 'ROS_sell_thru_ss_Report'
    elif 'sub_brand' in parameters:
        return 'ROS_sell_thru_ssb_Report'
    else:
        return 'ROS_sell_thru_sb_Report'

table_column_map = {
        'hul_df': [('df', 'hul_df'),]}

def modify_columns(tbname, columns):
    #modify columns according to a table name.
    for _map in table_column_map[tbname]:
        if _map[0] in columns:
            i = columns.index(_map[0])
            columns[i] = _map[1]
    print('inside modify columns:', columns)
    return columns

def get_target(entities):
    columns = ['account', 'countercode', 'moctarget']
    group_by = []
    order_by = []
    column_filters = []
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append('df')

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res

def plot_target_sales(entities):
    columns = ['moctarget', 'salesvalue']
    group_by = ['moc']
    order_by = []
    column_filters = []
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        stop = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        start = datetime.date.today() + relativedelta(months=-6)
        dates = []
        for date in pd.date_range(start=start, end=stop, freq='M'):
                dates.append('%s-%s-%s'%(date.year, date.month, 1))

        condition_list.append("(<<df>>.<<created_date>>.isin(pd.DatetimeIndex(%s)))"%str(dates))
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')

    if set(['account', 'city', 'state', 'region']).intersection(set(columns)):
        columns.append('countercode')

    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    #if group_by:
        #query_parameters['group_by'] = str(group_by)

    query = get_query(query_parameters)

    for s1, s2 in table_column_map[tb_name]:
        query = query.replace('<<' + s1 + '>>', s2)
    query = query.replace('<<', '').replace('>>', '')

    t1 = execute_query([query, 'df'])
    t2 = t1[['moc', 'salesvalue']].groupby('moc').sum().reset_index()

    if set(['account', 'city', 'state', 'region']).intersection(set(columns)):
        t3 = t1[['moc', 'moctarget', 'countercode']].drop_duplicates().groupby('moc').sum().reset_index()
        data = t2.merge(t3, on='moc')

    elif 'countercode' in columns:
        t3 = t1[['moc', 'moctarget']].drop_duplicates()
        data = t2.merge(t3, on='moc')

    print(data)
    data = data.set_index('moc').unstack()
    data = data.to_frame('')
    res = generate_result(data, 'graph')
    return res
    #return (query, 'text')

def low_sales(entities):
    columns = ['account','countercode','salesvalue']
    group_by = ['countercode']
    order_by = []
    column_filters = []
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    #if group_by:
        #query_parameters['group_by'] = str(group_by)

    query = get_query(query_parameters)

    for s1, s2 in table_column_map[tb_name]:
        query = query.replace('<<' + s1 + '>>', s2)
    query = query.replace('<<', '').replace('>>', '')

    _df = execute_query([query, 'df'])
    _group = _df.groupby(group_by)['salesvalue'].sum().reset_index().sort_values(by=['salesvalue'], ascending=True).drop_duplicates().head(10)

    _cols = list(set(_df.columns)-(set(_group.columns)))
    _cols.append('countercode',)
    data = _group.merge(_df[_cols], on=['countercode'])
    data = data.drop_duplicates()
    res = generate_result(data, 'text')
    return res
    #return (query, 'text')

def top_sales(entities):
    columns = ['account', 'countercode', 'salesvalue']
    group_by = ['countercode']
    order_by = []
    column_filters = []
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    #if group_by:
        #query_parameters['group_by'] = str(group_by)

    query = get_query(query_parameters)

    for s1, s2 in table_column_map[tb_name]:
        query = query.replace('<<' + s1 + '>>', s2)
    query = query.replace('<<', '').replace('>>', '')

    _df = execute_query([query, 'df'])
    _group = _df.groupby(group_by)['salesvalue'].sum().reset_index().sort_values(by=['salesvalue'], ascending=False).drop_duplicates().head(10)

    _cols = list(set(_df.columns)-(set(_group.columns)))
    _cols.append('countercode',)
    data = _group.merge(_df[_cols], on=['countercode'])
    data = data.drop_duplicates()
    res = generate_result(data, 'text')
    return res

def store_performance(entities):
    columns = ['account', 'countercode', 'target_achieved', 'salesvalue', 'moctarget']
    group_by = []
    order_by = []
    column_filters = []
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append("df")

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    #return (query, 'text')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res

def get_mrp(entities):
    columns = ['account', 'countercode', 'mrp', 'basepackcode']
    group_by = []
    order_by = ['mrp']
    column_filters = ['mrp']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append("df")

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res
    #return (query, 'text')

def get_opening_units(entities):
    columns = ['account', 'countercode', 'openingunits', 'basepackcode']
    group_by = []
    order_by = ['openingunits']
    column_filters = ['openingunits']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append("df")

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res
    #return (query, 'text')

def get_receipt_units(entities):
    columns = ['account', 'countercode', 'receiptunits', 'basepackcode']
    group_by = []
    order_by = ['receiptunits']
    column_filters = ['receiptunits']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append("df")

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res
    #return (query, 'text')


def get_closing_stock_units(entities):
    columns = ['account', 'countercode', 'closingstockunits', 'basepackcode',]
    group_by = []
    order_by = ['closingstockunits']
    column_filters = ['closingstockunits']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')
    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')
    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    if group_by:
        query_parameters['group_by'] = str(group_by)

    query = []
    query.append(get_query(query_parameters))

    if not query[0]:
        return

    query.append("df")

    for s1, s2 in table_column_map[tb_name]:
        for i in range(len(query)):
            query[i] = query[i].replace('<<' + s1 + '>>', s2)

    for i in range(len(query)):
            query[i] = query[i].replace('<<', '').replace('>>', '')

    data = execute_query(query)
    res = generate_result(data, 'text')
    return res
    #return (query, 'text')

def get_sales_info(entities):
    columns = ['account', 'countercode', 'moc', 'salesvalue']
    group_by = ['countercode', 'moc']
    order_by = []
    column_filters = ['salesvalue']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        start = '%s-%s-%s'%(timezone.now().year, timezone.now().month, 1)
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start='%s', periods=1)))"%start)
        if 'moc' not in columns:
            columns.append('moc')

    else:
        columns.remove('created_date') # we donot want this column to appear in final results.
        if 'moc' not in columns:
            columns.append('moc')

    #add more to condition here
    tb_name = 'hul_df' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)

    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    if condition_list:
        query_parameters['conditions'] = ' & '.join(condition_list)
    if order_by:
        query_parameters['order_by'] = str(order_by)
    #if group_by:
        #query_parameters['group_by'] = str(group_by)

    query = get_query(query_parameters)

    for s1, s2 in table_column_map[tb_name]:
        query = query.replace('<<' + s1 + '>>', s2)
    query = query.replace('<<', '').replace('>>', '')

    _df = execute_query([query, 'df'])
    _group = _df.groupby(group_by)['salesvalue'].sum().reset_index().sort_values(by=['salesvalue',], ascending=False).drop_duplicates()

    _cols = list(set(_df.columns)-(set(_group.columns)))
    _cols.append('countercode',)
    data = _group.merge(_df[_cols], on=['countercode'])
    data = data.drop_duplicates()
    res = generate_result(data, 'text')
    return res

"""
def plot_store_report(entities, extra_columns=[]):
    columns = ['store', 'target_obj_skew_indicator','created_date']
    column_filters = []
    group_by = ['target_obj_skew_indicator', 'created_date']
    order_by = []
    condition_list, columns = get_conditions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('created_date')
    if 'store' not in columns:
       return
    #add more to condition here
    tb_name = 'worxogo_df' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #remove the brand from the columns
    columns.remove('store')
    #modify the column_names as per the Table_name
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = str(columns)
    query_parameters['conditions'] = ' & '.join(condition_list)
    query_parameters['order_by'] = str(order_by)
    query_parameters['group_by'] = str(group_by)
    query_parameters['agg_func'] = "['<<target_obj_skew_indicator>>'].count()"
    query_step_1 = get_query(query_parameters)
    print('query_step_1:',query_step_1)
    if not query_step_1:
        return
    query_step_2 = 'df'
    for s1, s2 in table_column_map[tb_name]:
        query_step_1 = query_step_1.replace('<<' + s1 + '>>', s2)
        query_step_2 = query_step_2.replace('<<' + s1 + '>>', s2)
    query_step_1 = query_step_1.replace('<<', '').replace('>>', '')
    query_step_2 = query_step_2.replace('<<', '').replace('>>', '')
    pandas_query = {'step_1': query_step_1, 'step_2': query_step_2}
    return (pandas_query, columns, column_filters, 'graph')
"""
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
    agg_func_list = []
    agg_func_str = ''
    _agg_func_list = entities.get('function_agg_list', None)
    print(_agg_func_list)
    if _agg_func_list:
        keys = list(_agg_func_list.keys())
        keys.sort()
        for e in keys:
            agg_func_list.append(_agg_func_list[e])
        print('functions', agg_func_list, len(agg_func_list))
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
        for agg_func in agg_func_list:
            agg_func_str += "(df['{arg}'] == df['{arg}'].{func}())".format(arg=agg_func['arg'], func=agg_func['func'])
            if agg_func.get('conn', None):
                agg_func_str += ' ' + connectors[agg_func['conn']]
    print(agg_func_str)
    columns = sorted(set(columns), key=columns.index) #removing duplicates while retaining original sequence
    return (agg_func_str, columns)


def get_conditions(entities, columns=[]):
    """
        composes WHERE CONDITION from entities
    """
    condition_list = []
    print('InPut inside compose condition:', columns)
    #normalising expressions
    _exp_l = entities.get('expression_list', {})
    exp_l = []
    if _exp_l:
        keys = list(_exp_l.keys())
        keys.sort()
        for e in keys:
            exp_l.append(_exp_l[e])
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
                    exp_l[i]['value'] = val
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
            _exp += "(<<df>>['<<{prop}>>'] {operator} {value})".format(prop=exp['prop'], operator=exp['operator'], value=exp['value'])
            columns.append(exp['prop'])
            if exp.get('conn', None):
                _exp += ' ' + connectors[exp['conn']]
        condition_list.append(_exp)
        print(condition_list)

    countercode = entities.get('countercode')
    if countercode:
        condition_list.append("(<<df>>['<<countercode>>'] == '%s')"%countercode)
        columns.append('countercode')

    basepackcode = entities.get('basepackcode')
    if basepackcode:
        condition_list.append("(<<df>>['<<basepackcode>>'] == '%s')"%basepackcode)
        columns.append('basepackcode')

    moc = entities.get('moc')
    if moc:
        condition_list.append("(<<df>>['<<moc>>'] == '%s')"%moc)
        columns.append('moc')

    city = entities.get('city')
    if city:
        condition_list.append("(<<df>>['<<city>>'] == '%s')"%city)
        columns.append('city')

    state = entities.get('state')
    if state:
        condition_list.append("(<<df>>['<<state>>'] == '%s')"%state)
        columns.append('state')

    region = entities.get('region')
    if region:
        condition_list.append("(<<df>>['<<region>>'] == '%s')"%region)
        columns.append('region')

    account = entities.get('account')
    if account:
        condition_list.append("(<<df>>['<<account>>'] == '%s')"%account)
        columns.append('account')

    productfamily= entities.get('productfamily')
    if productfamily:
        condition_list.append("(<<df>>['<<productfamily>>'] == '%s')"%productfamily)
        columns.append('productfamily')

    brand = entities.get('brand')
    if brand:
        condition_list.append("(<<df>>['<<brand>>'] == '%s')"%brand)
        columns.append('brand')

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
