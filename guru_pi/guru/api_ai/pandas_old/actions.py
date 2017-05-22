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

#table_sniper = load_hdf(os.path.join(module_dir, 'dumps/Telenor_Myanmar_Base_Jun_Sep_2016.h5'))
#table_sniper = load_hdf(os.path.join(module_dir, 'dumps/Telenor_Myanmar_Gross_Jul_Sep_2016.h5'))
#table_sniper['abs_gross'] = table_gross['abs_base']
#del table_sniper['abs_base']
table_sniper = load_hdf(os.path.join(module_dir, 'dump_new/sniper_data.h5'))
#table_sniper = load_hdf(os.path.join(module_dir, 'dumps/Gross_Ver3.h5'))


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
    print(_df)
    if isinstance(_df, pd.Series):
        print('yes')
    else:
        print('no')
    print(_df.index.nlevels)
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
        'table_sniper' : [('df', 'table_sniper')],
        'data' : [('df', 'data')],
        }

def beautify_columns(cols):
    _map = {
        'operator_name': 'Operator',
        'month': 'Month',
        'year': 'Year',
        'geo_city_name': 'City',
        'geo_rgn_name': 'Region',
        'geo_cnty_name': 'County',
        'abs_base': 'Absolute Base',
        'abs_gross': 'Absolute Gross',
        'base_share': 'Base Share (%)',
        'gross_share': 'Gross Share (%)',
        'base_share_variance': 'Base Share Variance (%)',
        'gross_share_variance': 'Gross Share Variance (%)',
        }
    _cols = []
    for _col in list(cols):
        if _map.get(_col):
            _cols.append(_map[_col])
        else:
            _cols.append(_col)
    return _cols


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

def get_base_share(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_abs = kpi_filter+'_base'
    else:
        kpi_abs = 'abs_base'

    columns.append(kpi_abs)

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]

    if 'month' in columns:
        columns.extend(['year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.extend(['month', 'year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    pivot_index = ['operator_name']
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
        pivot_index.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
        pivot_index.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')
        pivot_index.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
    final_filter = ''
    final_filters = []
    _checks = ['operator_name', 'start_date', 'geo_rgn_name', 'geo_city_name']
    for cond in list(condition_list):
        for _cn in _checks:
            if _cn in cond:
                final_filters.append(cond)
                condition_list.remove(cond)

    if final_filters:
        final_filter = ' & '.join(final_filters)

    conditions = ' & '.join(condition_list)
    print(final_filters,'::', conditions)

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
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).sum()
        base_share = data.groupby(level=1).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs]))*100)
        print("base:\n", base_share.head())
        if isinstance(base_share, pd.DataFrame):
            base_share = base_share.unstack()
            base_share.name = 'base_share'
            base_share.index = base_share.index.droplevel(level=4)
        else:
            base_share.index = base_share.index.droplevel(level=0)
        data['base_share'] = base_share
        data = data.reset_index().sort_values(by='start_date')
        if final_filter:
            _filters = modify_query('data', final_filter)
            print(_filters)
            data = data.ix[eval(_filters)]
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['base_share'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        data = data.round(1)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
        print('res:', data.head())
    except Exception as e:
        print('Error:', e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())
    res = []
    if source == 'web':
         # generate graph or table or text
        if chart_type:
            res.append(generate_result(data, 'graph', section=1))
        else:
            res.append(generate_result(data, 'text', section=1))
    else:
        res.append({"type":"message", "data": get_resp_positive()})
        if chart_type:
            json_data = df_to_chart_data(data, type=chart_type)
            res.append({"type": "chart", "data":json_data})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_gross_share(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_abs = kpi_filter+'_gross'
    else:
        kpi_abs = 'abs_gross'

    columns.append(kpi_abs)

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]

    if 'month' in columns:
        columns.extend(['year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.extend(['month', 'year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    pivot_index = ['operator_name']
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
        pivot_index.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
        pivot_index.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')
        pivot_index.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
    final_filter = ''
    final_filters = []
    _checks = ['operator_name', 'start_date', 'geo_rgn_name', 'geo_city_name']
    for cond in list(condition_list):
        for _cn in _checks:
            if _cn in cond:
                final_filters.append(cond)
                condition_list.remove(cond)

    if final_filters:
        final_filter = ' & '.join(final_filters)

    conditions = ' & '.join(condition_list)
    print(final_filters,'::', conditions)

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
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).sum()
        gross_share = data.groupby(level=1).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs]))*100)
        print("gross:\n", gross_share.head())
        if isinstance(gross_share, pd.DataFrame):
            gross_share = gross_share.unstack()
            gross_share.name = 'gross_share'
            gross_share.index = gross_share.index.droplevel(level=4)
        else:
            gross_share.index = gross_share.index.droplevel(level=0)
        data['gross_share'] = gross_share
        data = data.reset_index().sort_values(by='start_date')
        if final_filter:
            _filters = modify_query('data', final_filter)
            print(_filters)
            data = data.ix[eval(_filters)]
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['gross_share'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        data = data.round(1)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
        print('res:', data.head())
    except Exception as e:
        print('Error:', e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())
    res = []
    if source == 'web':
         # generate graph or table or text
        if chart_type:
            res.append(generate_result(data, 'graph', section=1))
        else:
            res.append(generate_result(data, 'text', section=1))
    else:
        res.append({"type":"message", "data": get_resp_positive()})
        if chart_type:
            json_data = df_to_chart_data(data, type=chart_type)
            res.append({"type": "chart", "data":json_data})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_base_share_variance(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_abs = kpi_filter+'_base'
    else:
        kpi_abs = 'abs_base'

    columns.append(kpi_abs)

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]

    if 'month' in columns:
        columns.extend(['year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.extend(['month', 'year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    pivot_index = ['operator_name']
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
        pivot_index.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
        pivot_index.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')
        pivot_index.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
    final_filter = ''
    final_filters = []
    _checks = ['operator_name', 'start_date', 'geo_rgn_name', 'geo_city_name']
    for cond in list(condition_list):
        for _cn in _checks:
            if _cn in cond:
                final_filters.append(cond)
                condition_list.remove(cond)

    if final_filters:
        final_filter = ' & '.join(final_filters)

    conditions = ' & '.join(condition_list)
    print(final_filters,'::', conditions)

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
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).sum()
        base_share = data.groupby(level=1).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs]))*100)
        print("base:\n", base_share.head())
        if isinstance(base_share, pd.DataFrame):
            base_share = base_share.unstack()
            base_share.name = 'base_share'
            base_share.index = base_share.index.droplevel(level=4)
        else:
            base_share.index = base_share.index.droplevel(level=0)
        data['base_share'] = base_share
        data = data.sort_index(level=3)
        data['base_share_variance'] = data.groupby(level=0)['base_share'].diff()
        print('base_share_variance', data.head())
        data = data.reset_index().sort_values(by='start_date')
        if final_filter:
            _filters = modify_query('data', final_filter)
            print(_filters)
            data = data.ix[eval(_filters)]
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['base_share_variance'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        data = data.round(1)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
        print('res:', data.head())
    except Exception as e:
        print('Error:', e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())
    res = []
    if source == 'web':
         # generate graph or table or text
        if chart_type:
            res.append(generate_result(data, 'graph', section=1))
        else:
            res.append(generate_result(data, 'text', section=1))
    else:
        res.append({"type":"message", "data": get_resp_positive()})
        if chart_type:
            json_data = df_to_chart_data(data, type=chart_type)
            res.append({"type": "chart", "data":json_data})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_gross_share_variance(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_abs = kpi_filter+'_gross'
    else:
        kpi_abs = 'abs_gross'

    columns.append(kpi_abs)

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]

    if 'month' in columns:
        columns.extend(['year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.extend(['month', 'year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    pivot_index = ['operator_name']
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
        pivot_index.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
        pivot_index.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')
        pivot_index.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
    final_filter = ''
    final_filters = []
    _checks = ['operator_name', 'start_date', 'geo_rgn_name', 'geo_city_name']
    for cond in list(condition_list):
        for _cn in _checks:
            if _cn in cond:
                final_filters.append(cond)
                condition_list.remove(cond)

    if final_filters:
        final_filter = ' & '.join(final_filters)

    conditions = ' & '.join(condition_list)
    print(final_filters,'::', conditions)

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
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        t1 = eval(query)
        data = t1.groupby(group_by).sum()
        gross_share = data.groupby(level=1).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs]))*100)
        print("gross:\n", gross_share.head())
        if isinstance(gross_share, pd.DataFrame):
            gross_share = gross_share.unstack()
            gross_share.name = 'gross_share'
            gross_share.index = gross_share.index.droplevel(level=4)
        else:
            gross_share.index = gross_share.index.droplevel(level=0)
        data['gross_share'] = gross_share
        data = data.sort_index(level=3)
        data['gross_share_variance'] = data.groupby(level=0)['gross_share'].diff()
        print('gross_share_variance', data.head())

        data = data.reset_index().sort_values(by='start_date')
        if final_filter:
            _filters = modify_query('data', final_filter)
            print(_filters)
            data = data.ix[eval(_filters)]
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['gross_share_variance'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        data = data.round(1)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
        print('res:', data.head())
    except Exception as e:
        print('Error:', e)
        return error_mesg(get_resp_negative())

    if data.empty:
        return error_mesg(get_resp_no_records())
    res = []
    if source == 'web':
         # generate graph or table or text
        if chart_type:
            res.append(generate_result(data, 'graph', section=1))
        else:
            res.append(generate_result(data, 'text', section=1))
    else:
        res.append({"type":"message", "data": get_resp_positive()})
        if chart_type:
            json_data = df_to_chart_data(data, type=chart_type)
            res.append({"type": "chart", "data":json_data})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_base_rank(entities, source):
    columns = ['operator_name', 'abs_base']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    if 'month' in columns:
        group_by.append('month')
    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('month')
        group_by.append('month')
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
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
        data = t1.groupby(group_by).sum()
        print(data)
        data['base_share'] = (data['abs_base']/sum(data['abs_base']))*100
        _d = data['base_share'].groupby(level=1).apply(lambda x: x.order(ascending=False))
        print(_d)
        _d.index = _d.index.droplevel(level=2)
        print(_d)
        _s = pd.DataFrame(_d).groupby(level=0)['base_share'].rank(ascending=False)
        print(_s)
        data['base_rank'] = _s.swaplevel(0, 1)
        print(data)
        del data['abs_base']
        data['base_rank'] = data['base_rank'].astype('int')
        data = data.reset_index()
        data = data.sort_values(by=['month', 'operator_name', 'rank'], ascending=True) 
        data.columns = beautify_columns(list(data.columns))
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


def get_gross_rank(entities, source):
    columns = ['operator_name', 'abs_gross']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    if 'month' in columns:
        group_by.append('month')
    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.append('month')
        group_by.append('month')
    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
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
        t1 = eval(query)
        data = t1.groupby(group_by).sum()
        print(data)
        data['gross_share'] = (data['abs_gross']/sum(data['abs_gross']))*100
        _d = data['gross_share'].groupby(level=1).apply(lambda x: x.order(ascending=False))
        print(_d)
        _d.index = _d.index.droplevel(level=2)
        print(_d)
        _s = pd.DataFrame(_d).groupby(level=0)['gross_share'].rank(ascending=False)
        data['gross_rank'] = _s.swaplevel(0, 1)
        print(data)
        del data['abs_gross']
        data = data.reset_index()
        data = data.sort_values(by=['month', 'operator_name', 'gross_rank'], ascending=False)
        data = data.round(2)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
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


def get_base_share_var(entities, source):
    columns = ['operator_name', 'abs_base']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    if 'month' in columns:
        columns.extend(['year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])
    if 'month' not in columns:
        condition_list.append("(<<df>>.<<start_date>>.isin(pd.date_range(start=timezone.now().date()-timezone.timedelta(days=365), periods=365, freq='D')))")
        #condition_list.append("(<<df>>['<<date>>']=='2016-10-20')")
        columns.extend(['month', 'year', 'start_date'])
        group_by.extend(['month', 'year', 'start_date'])

    if 'geo_city_name' in columns:
        group_by.append('geo_city_name')
    if 'geo_rgn_name' in columns:
        group_by.append('geo_rgn_name')
    if 'geo_cnty_name' in columns:
        group_by.append('geo_cnty_name')

    tb_name = 'table_sniper' #get_tbname(columns)
    #modify column_names as per the table
    #columns = modify_columns(tb_name, columns)
    final_filter = ''
    final_filters = []
    for cond in list(condition_list):
        if 'operator_name' in cond or 'start_date' in cond:
            final_filters.append(cond)
            condition_list.remove(cond)

    if final_filters:
        final_filter = ' & '.join(final_filters)

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
        data = t1.groupby(group_by).sum()
        base_share = data.groupby(level=1).apply(lambda x : (x['abs_base']/sum(x['abs_base']))*100)

        if isinstance(base_share, pd.DataFrame):
            base_share = base_share.unstack()
            base_share.name = 'base_share'
            base_share.index = base_share.index.droplevel(level=4)
        else:
            base_share.index = base_share.index.droplevel(level=0)

        print(base_share)
        data['base_share'] = base_share
        print(data)
        data = data.sort_index(level=3)
        data['base_share_variance'] = data.groupby(level=0)['base_share'].diff()
        print(data)
        data = data.reset_index().sort_values(['start_date'])

        if final_filter:
            #final_filter = ' & '.join(final_filters)
            _filters = modify_query('data', final_filter)
            data = data.ix[eval(_filters)]
        del data['start_date']
        del data['abs_base']
        data = data.round(2)
        data.fillna('-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
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

    operator_name = entities.get('operator_name')
    if operator_name:
        condition_list.append("(<<df>>['<<operator_name>>'].isin(%s))"%operator_name)
        columns.append('operator_name')

    geo_city_name = entities.get('geo_city_name')
    if geo_city_name:
        condition_list.append("(<<df>>['<<geo_city_name>>'].isin(%s))"%geo_city_name)
        columns.append('geo_city_name')

    geo_rgn_name = entities.get('geo_rgn_name')
    if geo_rgn_name:
        condition_list.append("(<<df>>['<<geo_rgn_name>>'].isin(%s))"%geo_rgn_name)
        columns.append('geo_rgn_name')

    geo_cnty_name = entities.get('geo_cnty_name')
    if geo_cnty_name:
        condition_list.append("(<<df>>['<<geo_cnty_name>>'].isin(%s))"%geo_cnty_name)
        columns.append('geo_cnty_name')

    start_date = entities.get('start_date', None)

    if start_date:
        try:
            if type(start_date) is dict:
                date_period= start_date.get('date-period', None)
                if date_period:
                    dates = tuple(date_period.split('/'))
                    condition_list.append("(<<df>>['<<start_date>>'].isin(pd.date_range(start='%s', end='%s', freq='D')))"%dates)
                    columns.append('month')
                elif start_date.get('date', None):
                    condition_list.append("(<<df>>['<<start_date>>'] == '%s')"%start_date['date'])
                    columns.append('month')
            elif type(start_date) is list:
                for date in start_date:
                    dates = tuple(date.split('/'))
                    condition_list.append("(<<df>>['<<start_date>>'].isin(pd.date_range(start='%s', end='%s', freq='D')))"%dates)
                    columns.append('month')
            elif type(start_date) is str:
                dates = tuple(start_date.split('/'))
                condition_list.append("(<<df>>['<<start_date>>'].isin(pd.date_range(start='%s', end='%s', freq='D')))"%dates)
                columns.append('month')

        except Exception as e:
            raise ParsingError('Not a valid date')

    #finally, ``where condition``
    #condition = ' and '.join(condition_list)
    print('Inside compose_condition: condition = ', condition_list)
    print('Inside compose condition: columns = ', columns)
    columns = sorted(set(columns), key=columns.index) #removing duplicates while retaining original sequence
    print('Inside compose_condition: columns = ', columns)
    return (condition_list, columns)


