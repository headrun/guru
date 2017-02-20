import os
import time
import datetime
import pandas as pd
import numpy as np
#import plotly.plotly as py
#import plotly.graph_objs as go

from django.utils import timezone
from dateutil.relativedelta import relativedelta

from ..renderers import RenderTemplate
from ..load import load_hdf

from ..guru_responses import *

module_dir = os.path.dirname(__file__)

#table_sniper = load_hdf(os.path.join(module_dir, 'dumps/Telenor_Myanmar_Base_Jun_Sep_2016.h5'))
#table_sniper = load_hdf(os.path.join(module_dir, 'dumps/Telenor_Myanmar_Gross_Jul_Sep_2016.h5'))
#table_sniper['abs_gross'] = table_gross['abs_base']
#del table_sniper['abs_base']
table_sniper = load_hdf(os.path.join(module_dir, '../dump_new/sniper_data.h5'))
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
    print('inside:', _df)
    if isinstance(_df, pd.Series):
        print('yes')
    else:
        print('no')
    for _data in levels(_df, type):
        json_data.append(_data)
    print(json_data)
    return json_data

def levels(_df, _type):
    print('inside levels:', _df)
    if isinstance(_df, pd.Series):
        print('yes')
    else:
        print('no')
    print(_df.index.nlevels)
    print(_df.index, _df.columns, _df.values.tolist())
    if _df.index.nlevels == 1:
        _list = []
        for x, y in zip(_df.index, _df.values.tolist()):
            _list.append([x, y[0]])
        yield {'type': _type, 'name': _df.columns[0], 'data': _list}

    else:
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
    print(cols)

    _map = {
        'operator_name': 'Operator',
        'month': 'Month',
        'year': 'Year',
        'geo_city_name': 'State Name',
        'geo_rgn_name': 'Region Name',
        'geo_cnty_name': 'County Name',
        'abs_base': 'Absolute Base',
        'abs_gross': 'Absolute Gross',
        'JUL': "Jul 16",
        'AUG': "Aug 16",
        'SEP': "Sep 16",
        'OCT': "Oct 16",
        'NOV': "Nov 16",
        'DEC': "Dec 16",
        }
    _months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    for i in range(len(_months)-1):
        for j in range(i+1, len(_months)):
            _map[_months[i]+'_vs_'+_months[j]] = _months[i].capitalize()+' 16' + ' Vs '+ _months[j].capitalize()+' 16'

    _kpi_filters = ['', '1-10', '1-20', 'migrant', 'non_migrant', 'bangladesh', 'indonesia', 'nepal', 'abs']
    _kpis = ['base_share', 'base_share_variance', 'base_share_variance_with_leader', 'base_rank', 'gross_rank',
            'gross_share', 'gross_share_variance', 'gross_share_variance_with_leader', 'base_share_minus_gross_share',
            'base_share_increase', 'base_share_decrease', 'base_variance_increase', 'base_variance_decrease',
            'gross_share_increase', 'gross_share_decrease', 'gross_variance_increase', 'gross_variance_decrease',
            'base_rank_increase', 'base_rank_decrease', 'gross_rank_increase', 'gross_rank_decrease',
            'base_share_variance_with_leader_increase', 'base_share_variance_with_leader_decrease',
            'gross_share_variance_with_leader_increase', 'gross_share_variance_with_leader_decrease',
            ]
    for k in _kpis:
        if 'rank' in k:
            _map[k] = ' '.join([x.strip().capitalize() for x in k.split('_')])
            continue
        _map[k] = ' '.join([x.strip().capitalize() for x in k.split('_')]) + '(%)'

    for kf in _kpi_filters:
        for k in _kpis:
            if 'rank' in k and kf.startswith('abs'):
                _map[(kf+'_'+k).strip()] = ' '.join([x.strip().capitalize() for x in k.split('_')])
                continue

            if kf.startswith('abs'):
                _map[(kf+'_'+k).strip()] = ' '.join([x.strip().capitalize() for x in k.split('_')]) + '(%)'
                continue
            if 'rank' in k:
                _map[(kf+'_'+k).strip()] = ' '.join([x.strip().capitalize() for x in (kf+'_'+k).split('_')])
                continue
            _map[(kf+'_'+k).strip()] = ' '.join([x.strip().capitalize() for x in (kf+'_'+k).split('_')]) + '(%)'

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
    exp_l = entities.get('rel_exp', [])
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
        if not str(exp.get('value', None)):
            val = _get_value_from_context(exp_l, 'value', current_index=i)
            if val:
                exp_l[i]['value'] = ''.join(val.split( ))
            else:
                raise ParsingError('Not a valid expression...prop_value missing.')
        if i < len(exp_l)-1:
            if not exp.get('conn', None):
                exp_l[i]['conn'] = 'and'
        else:
            if exp.get('conn', None):
                del exp_l[i]['conn']

    _exp = ''
    for i, exp in enumerate(exp_l):
        _exp += "(<<df>>['<<{prop}>>'] {operator} {value})".format(prop=exp['prop'],
                    operator=exp['operator'], value= ''.join(str(exp['value']).split( )))
        columns.append(exp['prop'])
        if exp.get('conn', None):
            _exp += ' ' + connectors[exp['conn']]
    if _exp:
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

