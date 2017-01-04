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

wor_master = load_hdf(os.path.join(module_dir, 'dumps/worxogo_merged.h5'))
#wor_badges = load_hdf(os.path.join(module_dir, 'dumps/badges.h5'))
wor_badges = load_hdf(os.path.join(module_dir, 'dumps/worxogo_merged_badges.h5'))
#wor_users = load_hdf(os.path.join(module_dir, 'dumps/users.h5'))

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

def generate_result(df, result_type, section, **others):
    answer = {}
    print(others)
    print(df.head())

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
        df = df[:5000]
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
       'wor_master': [('df', 'wor_master')],
       'wor_badges': [('df', 'wor_badges'), ('date', 'created_at')]
       }

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

def improved_users(entities, source):
    columns = ['user_employee_code', 'user_name', 'objective_points']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    print(columns)
    if 'date' not in columns:
        condition_list.append("(<<df>>.<<date>>.isin(pd.date_range(\
                    start=timezone.now().date()-timezone.timedelta(\
                    days=30), periods=30, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        #columns.append('date')
    if 'objective_text' not in columns:
        condition_list.append("(<<df>>.<<objective_text>>=='MSP')")
        columns.append('objective_text')

    tb_name = 'wor_master' #get_tbname(columns)
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

        t2 = t1.groupby(['user_employee_code',] )['objective_points'].sum().to_frame('points').reset_index().sort_values(by='points', ascending=False)
        print(t2.head())

        data = t2[['user_employee_code']].merge(t1[['user_employee_code', 'user_name', 'objective_text']].drop_duplicates())

        data = data[['user_name', 'objective_text']].head(10)

        data.rename(columns={'user_name':'User', 'objective_text':'Objective'}, inplace=True)

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

def consistent_users(entities, source):
    columns = ['user_employee_code','user_name', 'target_obj_skew_indicator']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    print(columns)
    if 'date' not in columns:
        condition_list.append("(<<df>>.<<date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=30), periods=30, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        #columns.append('date')
    if 'objective_text' not in columns:
        condition_list.append("(<<df>>.<<objective_text>>=='MSP')")

    columns = ['user_employee_code', 'user_name', 'objective_text', 'target_obj_skew_indicator']
    tb_name = 'wor_master' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        t1 = eval(query)

        t2 = t1.groupby(['user_employee_code',] )['target_obj_skew_indicator'].count().to_frame('count').reset_index()

        t3 = t1.groupby(['user_employee_code', 'target_obj_skew_indicator'] )['target_obj_skew_indicator'].count().to_frame('count').reset_index()

        data = t2.merge(t3, on=['user_employee_code', 'count'])

        data = data[['user_employee_code', 'target_obj_skew_indicator']].merge(t1[['user_employee_code', 'user_name', 'objective_text']].drop_duplicates())

        data = data[['user_name', 'objective_text', 'target_obj_skew_indicator']]

        data.rename(columns={'user_name':'User', 'objective_text':'Objective', 'target_obj_skew_indicator':'Target Skew Indicator'}, inplace=True)
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

        res.append({"type":"message", "data":get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res

def get_badges(entities, source):

    columns = ['user_name', 'badge_name']

    if entities['objective_text']:
        entities['objective_text'] = 'Category Mix' if 'category mix' in entities['objective_text'].lower() else entities['objective_text']
        entities['badge_img_name'] = 'Badge_%s_L1.png' %entities.pop('objective_text')

    condition_list, columns = get_conditions(entities, columns)
    #print(condition_list)
    agg_func_str, columns = get_agg_functions(entities, columns)
    #print(columns)
    """
    if 'date' not in columns:
        #condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('date')
    """

    tb_name = 'wor_badges' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)

    # execution
    try:
        #get rows and columns
        cols = eval(query)
        print("rows and columns\n\n")
        print(cols.head())
        if cols.empty:
            return error_mesg(get_resp_no_records())
        #groupby
        data = cols.groupby(['user_name', 'badge_name']).size().to_frame('count').reset_index().sort_values(by=['count', 'user_name'], ascending=False )
        data.rename(columns={'user_name':'User', 'badge_name':'Badge', 'count':'Count'}, inplace=True)
    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        error_mesg(get_resp_no_records())

    res = []
    if source == 'web':
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = {"header":[], "rows":[]}
        json_data["header"].extend(data.dtypes.index.tolist())
        for value in data.values:
            row_values = value.tolist()
            _time = row_values.pop(-1)
            row_values.append(str(_time))
            json_data["rows"].append(row_values)

        res.append({"type":"message", "data":get_resp_positive()})
        res.append({"type":"table", "data":json_data})
    return res


def get_top_users(entities, source):
    columns = ['user_employee_code', 'user_name', 'region', 'territory', 'objective_points']
    condition_list, columns = get_conditions(entities, columns)
    agg_func_str, columns = get_agg_functions(entities, columns)
    print(columns)
    if 'date' not in columns:
        #condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('date')

    limit = "[0:]"
    filter = entities.get('filter')

    if filter and (len(filter) > 1):
        if isinstance(filter, dict):
            if filter['limit'] == 'top':
                limit = "[ : %s]"%filter['value']
            if filter['limit'] == 'bottom':
                limit = "[-%s: ]"%filter['value']

    tb_name = 'wor_master' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg(get_resp_negative())

    query = modify_query(tb_name, query)
    print(query)
    # execution
    try:
        #get rows and columns
        cols = eval(query)
        #print("rows and columns\n\n")
        #print(cols.head())
        if cols.empty:
            return error_mesg(get_resp_no_records())
        #groupby
        t1 = cols.groupby(['user_employee_code', 'date']).sum().sort_values(by='objective_points', ascending=False).reset_index()
        #print("grouping\n\n")
        #print(t1.head())

        _m = list(set(cols.columns) - set(t1.columns))
        _m.append('user_employee_code')
        #print(_m)
        data = t1.merge(cols[_m].drop_duplicates(), on=['user_employee_code'])
        #print('Merging\n\n')
        #print(data.head())
        final_cols = columns
        final_cols.remove('user_employee_code')
        #print("data["+str(final_cols)+"]"+limit)
        data = eval("data["+str(final_cols)+"]"+limit)

        data.rename(columns={'user_name':'User', 'region':'Region', 'territory':'Territory', 'objective_points':'Performance points', 'date':'Date', 'objective_text': 'Objective'}, inplace=True)

    except Exception as e:
        print(e)
        return error_mesg(get_resp_negative())

    if data.empty:
        error_mesg(get_resp_no_records())

    res = []
    if source == 'web':
        res.append(generate_result(data, 'text', section=1))
    else:
        json_data = {"header":[], "rows":[], 'type':'table'}
        json_data["header"].extend(data.dtypes.index.tolist())
        for value in data.values:
            row_values = value.tolist()
            _time = row_values.pop(-1)
            row_values.append(str(_time))
            json_data["rows"].append(row_values)

        res.append({"type":"message", "data":get_resp_positive()})
        res.append({"type":"table", "data":json_data})

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


def plot_store_report(entities):
    columns = ['store', 'target_obj_skew_indicator','created_date']
    column_filters = []
    group_by = ['target_obj_skew_indicator', 'created_date']
    order_by = []
    condition_list, columns = get_conditions(entities, columns)

    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('created_date')

    if 'store' not in columns:
       return error_mesg("Didn't understand your query. Sorry.")

    tb_name = 'worxogo_df' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}.[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg("Sorry, couldn't understand your query")

    query = modify_query(tb_name, query)

    # execution
    try:
        #get rows and columns
        data = eval(query)
        _groupby = ['created_date', 'target_obj_skew_indicator']
        data = data.groupby(_groupby)['target_obj_skew_indicator'].count()
    except Exception as e:
        print(e)
        return error_mesg('Problem in execution!')

    # generate graph or table or text
    print(data)
    data = data.to_frame().unstack()
    print(data)
    #res = generate_result(data, 'graph', section=1)
    #g_data = [go.Scatter(x=data['created_date'], y=data['target_obj_skew_indicator'])]
    #print(g_data)
    #cufflinks.set_config_file(offline=False, world_readable=True, theme='ggplot')
    #data = cufflinks.datagen.lines()
    py.sign_in('headrun', '9gt3caodzr')
    #url = py.plot(g_data, filename='pandas-line-naming-traces')
    src = data.iplot(kind='bar', filename='cf-simple-line')
    res = {}
    res['type'] = 'graph'
    res['data'] = src.embed_code
    return res

def plot_region_report(entities, source):
    columns = ['region', 'target_obj_skew_indicator']
    column_filters = []
    group_by = ['target_obj_skew_indicator', 'date']
    order_by = []
    condition_list, columns = get_conditions(entities, columns)
    if 'date' not in columns:
        condition_list.append("(<<df>>.<<date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('date')

    if 'region' not in columns:
       return error_mesg("Didn't understand your query. Sorry.")

    tb_name = 'wor_master' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}.[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg("Sorry, couldn't understand your query")

    query = modify_query(tb_name, query)

    # execution
    try:
        #get rows and columns
        data = eval(query)
        _groupby = ['target_obj_skew_indicator', 'date']
        data = data.groupby(_groupby)['target_obj_skew_indicator'].count()
    except Exception as e:
        print(e)
        return error_mesg('Problem in execution!')

    # generate graph or table or text
    data = data.to_frame()
    res = generate_result(data, 'graph', section=1, graph_type='barh')
    return [res]

def plot_city_report(entities):
    columns = ['city', 'target_obj_skew_indicator','created_date']
    column_filters = []
    group_by = ['target_obj_skew_indicator', 'created_date']
    order_by = []
    condition_list, columns = get_conditions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append("(<<df>>.<<created_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=8), periods=8, freq='D')))")
        columns.append('created_date')

    if 'city' not in columns:
       return error_mesg("Didn't understand your query. Sorry.")

    tb_name = 'worxogo_df' #get_tbname(columns)
    #modify column_names as per the table
    columns = modify_columns(tb_name, columns)
    conditions = ' & '.join(condition_list)
    if conditions:
        query = "{tb_name}.ix[{conditions}][{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)
    else:
        query = "{tb_name}.[{columns}]".format(tb_name=tb_name, conditions=conditions, columns=columns)

    if not query:
        return error_mesg("Sorry, couldn't understand your query")

    query = modify_query(tb_name, query)

    # execution
    try:
        #get rows and columns
        data = eval(query)
        _groupby = ['target_obj_skew_indicator', 'created_date']
        data = data.groupby(_groupby)['target_obj_skew_indicator'].count()
    except Exception as e:
        print(e)
        return error_mesg('Problem in execution!')

    # generate graph or table or text
    data = data.to_frame()
    res = generate_result(data, 'graph', section=1, graph_type='bar')
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
        composes SQL WHERE CONDITION from entities
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

    brand = entities.get('brand')
    if brand:
        condition_list.append("(<<df>>['<<brand>>'] == '%s')"%brand)
        columns.append('brand')

    store_code = entities.get('store_code')
    if store_code:
        condition_list.append("(<<df>>['<<store_code>>'] == '%s')"%store_code)
        columns.append('store_code')

    city = entities.get('city')
    if city:
        condition_list.append("(<<df>>['<<city>>'] == '%s')"%city)
        columns.append('city')

    state = entities.get('state')
    if state:
        condition_list.append("(<<df>>['<<state>>'] == '%s')"%state)
        columns.append('state')

    style = entities.get('style')
    if style:
        condition_list.append("(<<df>>['<<style>>'] == '%s')"%style)
        columns.append('style')

    sub_brand = entities.get('sub_brand')
    if sub_brand:
        condition_list.append("(<<df>>['<<sub_brand>>'] == '%s')"%sub_brand)
        columns.append('sub_brand')

    store = entities.get('store')
    if store:
        condition_list.append("(<<df>>['<<store>>'] == '%s')"%store)
        columns.append('store')

    region = entities.get('region')
    if region:
        condition_list.append("(<<df>>['<<region>>'] == '%s')"%region)
        columns.append('region')

    territory = entities.get('territory')
    if territory:
        condition_list.append("(<<df>>['<<territory>>'] == '%s')"%territory)
        columns.append('territory')

    objective_text = entities.get('objective_text')
    if objective_text:
        condition_list.append("(<<df>>['<<objective_text>>'] == '%s')"%objective_text)
        columns.append('objective_text')

    objective_type = entities.get('objective_type')
    if objective_type:
        condition_list.append("(<<df>>['<<objective_type>>'] == '%s')"%objective_type)
        columns.append('objective_type')

    user_name = entities.get('user_name')
    if user_name:
        condition_list.append("(<<df>>['<<user_name>>'] == '%s')"%user_name)
        columns.append('user_name')

    badge_img_name = entities.get('badge_img_name')
    if badge_img_name:
        condition_list.append("(<<df>>['<<badge_img_name>>'] == '%s')"%badge_img_name)
        columns.append('badge_img_name')

    date = entities.get('date', None)
    if date:
        try:
            date_period= date.get('date-period', None)
            if date_period:
                dates = tuple(date_period.split('/'))
                condition_list.append("(<<df>>['<<date>>'].isin(pd.date_range(start='%s', end='%s', freq='D')))"%dates)
                columns.append('date')
            elif date.get('date', None):
                condition_list.append("(<<df>>['<<date>>'] == '%s')"%date['date'])
                columns.append('date')
        except Exception as e:
            raise ParsingError('Not a valid date')

    #finally, ``where condition``
    #condition = ' and '.join(condition_list)
    print('Inside compose_condition: condition = ', condition_list)
    print('Inside compose condition: columns = ', columns)
    columns = sorted(set(columns), key=columns.index) #removing duplicates while retaining original sequence
    print('Inside compose_condition: columns = ', columns)
    return (condition_list, columns)

    """
    raw_info= {'prop': [], 'agg_func': [], 'agg_func_arg': [], 'operator': [], 'conn_expression': [], 'conn_agg_func': [], 'number': [], 'datetime': [], 'channel': [], 'brand': [], 'sub_brand': [], 'style': [], 'conn_item': [], 'city': [], 'state': [], 'store_code': []}
    for key in entities:
        print('entity:', key)
        if key== 'datetime':
            columns.append('created_date')
            #################### handling dates and intervals ######################
            for i, datetime in enumerate(entities['datetime']):
                if i!= 0:
                    condition+= ' and '
                if (datetime['type']== 'value'):
                    if (datetime['grain']== 'week'):
                        if len(condition)> 0:
                            condition+= ' and <<created_date>> between ' + '"'+datetime['value'].split('T')[0]+'"' + ' and ' + '"' + str(timezone.datetime.strptime(datetime['value'].split('T')[0], "%Y-%m-%d") + timezone.timedelta(days= 7)).split(' ')[0]+ '"'
                        else:
                              condition+= ' <<created_date>> between ' + '"'+datetime['value'].split('T')[0]+'"' + ' and ' + '"' + str(timezone.datetime.strptime(datetime['value'].split('T')[0], "%Y-%m-%d") + timezone.timedelta(days= 7)).split(' ')[0]+ '"'

                    elif (datetime['grain']== 'quarter'):
                        if len(condition)> 0:
                            condition+= ' and <<created_date>>= ' + '"'+datetime['value'].split('T')[0]+'"'
                        else:
                            condition+= ' <<created_date>>= ' + '"'+datetime['value'].split('T')[0]+'"'

                    elif (datetime['grain'] in ['day', 'hour', 'minute', 'second']):
                        if len(condition)> 0:
                            condition+= ' and <<created_date>>= ' + '"'+datetime['value'].split('T')[0]+'"'
                        else:
                            condition+= ' <<created_date>>= ' + '"'+datetime['value'].split('T')[0]+'"'
                elif (datetime['type']== 'interval'):
                    if (datetime['from']['grain']== 'week'):
                        pass
                    elif (datetime['from']['grain']== 'quarter'):
                        pass
                    elif (datetime['from']['grain'] in ['day', 'hour', 'minute', 'second']):
                        if len(condition)> 0:
                            condition+= ' and <<created_date>> between '+ '"'+ datetime['from']['value'].split('T')[0]+'"'
                        else:
                            condition+= ' <<created_date>> between ' + '"'+ datetime['from']['value'].split('T')[0]+'"'
                    if (datetime['to']['grain'] == 'week'):
                        pass
                    elif (datetime['to']['grain']== 'quarter'):
                        pass
                    elif (datetime['to']['grain'] in ['day', 'hour', 'minute', 'second']):
                        condition+= ' and ' + '"'+ datetime['to']['value'].split('T')[0] + '"'
        else:
            list_temp= []
            for i in range(len(entities[key])):
                list_temp.append(entities[key][i]['value'])
            raw_info[key]= list_temp

    ###################################################################
    #              normalising expressions
    ####i###############################################################
    #print(raw_info['conn_expression'])
    #print(raw_info['prop'])
    #print(raw_info['operator'])
    #print(raw_info['number'])
    if min(len(raw_info['prop']), len(raw_info['number'])) > 0:
        max_length= max(len(raw_info['operator']), len(raw_info['prop']))
        if len(raw_info['conn_expression']) < max_length- 1:
            for i in range(len(raw_info['conn_expression']), max_length- 1):
                raw_info['conn_expression'].append('and')
        if len(raw_info['prop']) < max_length:
            for i in range(len(raw_info['prop']), max_length):
                if len(raw_info['prop'])> 0:
                    raw_info['prop'].append(raw_info['prop'][-1])
        if len(raw_info['operator']) < max_length:
                for i in range(len(raw_info['operator']), max_length):
                    if len(raw_info['operator'])> 0:
                        raw_info['operator'].append(raw_info['operator'][-1])
                    else:
                        raw_info['operator'].append('=')
        if len(raw_info['number']) < max_length:
            for i in range(len(raw_info['number']), max_length):
                if len(raw_info['number']) > 0:
                    raw_info['number'].append(raw_info['number'][-1])

        #print(raw_info['conn_expression'])
        #print(raw_info['prop'])
        #print(raw_info['operator'])
        #print(raw_info['number'])

        ################## joining expressions ################
        expressions= ''
        for i in range(max_length):
            if i!= 0:
                expressions+= ' ' + str(raw_info['conn_expression'][i-1])
            expressions+= ' <<' + str(raw_info['prop'][i]) + '>> ' + str(raw_info['operator'][i]) + ' ' + str(raw_info['number'][i])
            columns.append(raw_info['prop'][i])
        if expressions:
            if len(condition) < 1:
                condition+= ' ' + expressions
            else:
                condition+= ' ' + 'and' + ' ' + expressions
    ################# other conditions ################
    brands= ''
    for i, brand in enumerate(raw_info['brand']):
        if i!= 0:
            brands+= ' ' + 'and'
        brands+= ' ' + '<<brand>>=' + '"' + str(brand).upper() + '"'
    if brands:
        columns.append('brand')
        if len(condition) < 1:
            condition+= ' ' + brands
        else:
            condition+= ' ' + 'and' + ' ' + brands
    channels= ''
    for i, channel in enumerate(raw_info['channel']):
        if i!= 0:
                channels+= ' ' + 'and'
        channels+= ' ' + '<<channel>>=' + '"' + str(channel).upper() + '"'
    if channels:
        columns.append('channel')
        if len(condition) < 1:
            condition+= ' ' + channels
        else:
            condition+= ' ' + 'and' + ' ' + channels
    cities= ''
    for i, city in enumerate(raw_info['city']):
        if i!= 0:
                cities+= ' ' + 'and'
        cities+= ' ' + '<<city>>=' + '"' + str(city) + '"'
    if cities:
        columns.append('city')
        if len(condition) < 1:
            condition+= ' ' + cities
        else:
            condition+= ' ' + 'and' + cities
    states= ''
    for i, state in enumerate(raw_info['state']):
        if i!= 0:
                states+= ' ' + 'and'
        states+= ' ' + '<<state>>=' + '"' + str(state) + '"'
    if states:
        columns.append('state')
        if len(condition) < 1:
            condition+= ' ' + states
        else:
            condition+= ' ' + 'and' + states
    store_codes= ''
    for i, store_code in enumerate(raw_info['store_code']):
        if i!= 0:
            store_codes+= ' ' + 'and'
        store_codes+= ' ' + '<<store_code>>=' + '"' + str(store_code) + '"'
    if store_codes:
        columns.append('store_code')
        if len(condition) < 1:
            condition+= ' ' + store_codes
        else:
            condition+= ' ' + 'and' + store_codes
    sub_brands= ''
    for i, sub_brand in enumerate(raw_info['sub_brand']):
        if i!= 0:
            sub_brands+= ' ' + 'and'
        sub_brands+= ' ' + '<<sub_brand>>=' + '"' + str(sub_brand) + '"'
    if sub_brands:
        columns.append('sub_brand')
        if len(condition) < 1:
            condition+= ' ' + sub_brands
        else:
            condition+= ' ' + 'and' + sub_brands
    styles= ''
    for i, style in enumerate(raw_info['style']):
        if i!= 0:
            store_codes+= ' ' + 'and'
        styles+= ' ' + '<<style>>=' + '"' + str(style) + '"'
    if styles:
        columns.append('style')
        if len(condition) < 1:
            condition+= ' ' + styles
        else:
            condition+= ' ' + 'and' + styles
    columns= list(dict.fromkeys(columns).keys()) #removing duplicates while retaining original sequence
    info= {}
    for k in raw_info.keys():
        if raw_info[k]:
            info[k]= raw_info[k]
    return (condition, columns, info)

"""
