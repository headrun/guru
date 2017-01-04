from jinja2 import Template
from django.utils import timezone

class ParsingError(Exception):
    """Generated whenever there is problem parsing
       condition or aggregate function parameters
    """
    def __init__(self, message):
        Exception.__init__(self, message)

templates= {}
limit= ''
templates['tb_name+columns+order_by']= Template('SELECT distinct {{columns}} FROM {{tb_name}} ORDER BY {{order_by}} desc'+ limit)

templates['tb_name+columns+condition+order_by']= Template('SELECT distinct {{columns}} FROM {{tb_name}} WHERE {{condition}} ORDER BY {{order_by}} desc' + limit)

templates['tb_name+columns+agg_func_list+order_by']=  Template("""SELECT distinct {{columns}} FROM {{tb_name}} WHERE {% for i in range(agg_func_list | length) %}{% set func=agg_func_list[i]['func'] %}{% set arg= agg_func_list[i]['arg'] %}{% if i!=0 %} {{agg_func_list[i-1]['conn']}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{tb_name}}) {% endfor %} ORDER BY {{order_by}} desc""" + limit)

templates['tb_name+columns+condition+agg_func_list+order_by']= Template("""SELECT distinct {{columns}} FROM {{tb_name}} WHERE {% for i in range(agg_func_list | length) %}{% set func=agg_func_list[i]['func'] %}{% set arg= agg_func_list[i]['arg'] %}{% if i!=0 %} {{agg_func_list[i-1]['conn']}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{tb_name}} WHERE {{condition}}) {% endfor %} and {{condition}} ORDER BY {{order_by}} desc""" + limit)

templates['tb_name+columns+condition+group_by+order_by']= Template('SELECT distinct {{columns}} FROM {{tb_name}} WHERE {{condition}} GROUP BY {{group_by}} ORDER BY {{order_by}}' + limit)

def get_query(query_parameters):
    #search templates
    print('inside get_query()')
    print(query_parameters)
    for key in templates.keys():
        key_set= set(key.split('+'))
        print(key_set, set(query_parameters.keys()))
        if key_set == set(query_parameters.keys()):
            print('Match Found!')
            return templates[key].render(query_parameters)
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
        'ROS_sell_thru_Report' : [('ros', 'style_ros')],
        'ROS_sell_thru_sb_Report': [('ros', 'ros_week'),],
        'ROS_sell_thru_ssb_Report': [('ros', 'ros_week'),],
        'ROS_sell_thru_ss_Report': [('ros', 'style_ros'),],
        'ROS_sell_thru_sc_Report': [('ros', 'ros_week'),('sell_thru', 'sell_thru')]}
def modify_columns(tbname, columns):
    #modify columns according to a table name.
    for _map in table_column_map[tbname]:
        if _map[0] in columns:
            i = columns.index(_map[0])
            columns[i] = _map[1]
    return columns

def get_ros_info(entities, extra_columns):
    columns= []
    order_by = []
    columns.append('ros')
    column_filters= ['ros'] #column filters represent the actual answer required.
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    else:#default
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    #add more to condition here...
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_sell_thru_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('sell_thru')
    column_filters = ['sell_thru']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    else:#default
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')

    #add more to condition here 
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    #modify the column_names as per the Table_name
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #agg_functions = modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_brand_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('brand')
    column_filters= ['brand']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('brand')

    #add more to condition here

    tb_name= 'ROS_sell_thru_Report' #get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] =' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_location_state_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('state')
    column_filters = ['state']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('state')

    #add more to condition here
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_location_city_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('city')
    column_filters = ['city']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or '<<sell_thru>>' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('city')

    #add more to condition here
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_location_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.extend(['state', 'city'])
    column_filters = ['state', 'city']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('state')

    #add more to condition here
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_store_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.extend(['store_code', 'store_name'])
    column_filters = ['store_code', 'store_name']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('store_code')
    #add more to condition here
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_sub_brand_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('sub_brand')
    column_filters = ['sub_brand']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('sub_brand')
    #add more to condition here
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def get_style_info(entities, extra_columns):
    columns = []
    order_by = []
    columns.append('style')
    column_filters = ['style']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' in columns:
        order_by.append('created_date')
    elif 'ros' in columns or 'sell_thru' in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
        order_by.append('created_date')
    else:
        order_by.append('style')
    #add more to condition here
    agg_functions, columns = get_agg_functions(entities, columns)
    tb_name = 'ROS_sell_thru_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'text')

def plot_ros(entities, extra_columns):
    columns = ['sub_brand', 'avg(ros_week)']
    order_by = ['sub_brand']
    group_by = ['created_date', 'sub_brand']
    column_filters = ['sub_brand']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
    if 'brand' not in columns:
        return
    #add more to condition here
    tb_name = 'ROS_sell_thru_sc_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #remove the brand from the columns
    columns.remove('brand')
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if group_by:
        query_parameters['group_by'] = ', '.join(group_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'graph')


def plot_sell_thru(entities, extra_columns):
    columns = ['sub_brand', 'avg(sell_thru)']
    order_by = ['sub_brand']
    group_by = ['created_date', 'sub_brand']
    column_filters = ['sub_brand']
    columns.extend(extra_columns)
    condition_list, columns = compose_condition(entities, columns)
    agg_functions, columns = get_agg_functions(entities, columns)
    if 'created_date' not in columns:
        condition_list.append('<<created_date>> between DATE_SUB(now(), INTERVAL 7 DAY) and now()')
        columns.append('created_date')
    if 'brand' not in columns:
        return
    #add more to condition here
    tb_name = 'ROS_sell_thru_sc_Report' #get_tbname(columns)
    columns = modify_columns(tb_name, columns)
    column_filters = modify_columns(tb_name, column_filters)
    #remove the brand from the columns
    columns.remove('brand')
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    query_parameters['order_by'] = ', '.join(order_by)
    if group_by:
        query_parameters['group_by'] = ', '.join(group_by)
    if agg_functions:
        query_parameters['agg_func_list'] = agg_functions
    if condition_list:
        query_parameters['condition'] = ' and '.join(condition_list)
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query = sql_query.replace('<<' + s1 + '>>', s2)
        sql_query = sql_query.replace('<<', '').replace('>>', '')
        return (sql_query, columns, column_filters, 'graph')



def get_agg_functions(entities, columns):
    agg_func_list = []
    #normalising aggregate functions
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
            if not func.get('conn', None) and i < len(agg_func_list)-1:
                agg_func_list[i]['conn'] = 'and'
    print(agg_func_list)
    columns = sorted(set(columns),key=columns.index)
    return (agg_func_list, columns)


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

def compose_condition(entities, columns=[]):
    """
        composes SQL WHERE CONDITION from entities
    """
    operators = {
        'gt': '>',
        'lt': '<',
        'gte': '>=',
        'lte': '<=',
        'eq': '='}

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
            print('p', i, exp)
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
            if not exp.get('conn', None) and i < len(exp_l)-1:
                exp_l[i]['conn'] = 'and'

        _exp = ''
        for exp in exp_l:
            _exp += ' <<' + exp['prop'] + '>> ' + exp['operator'] + ' ' + exp['value']
            columns.append(exp['prop'])
            if exp.get('conn', None):
                _exp += ' ' + exp['conn']
        condition_list.append(_exp)
        print(condition_list)

    brand = entities.get('brand')
    if brand:
        condition_list.append('<<brand>> = "%s"'%brand)
        columns.append('brand')

    store_code = entities.get('store_code')
    if store_code:
        condition_list.append('<<store_code>> = "%s"'%store_code)
        columns.append('store_code')

    city = entities.get('city')
    if city:
        condition_list.append('<<city>> = "%s"'%city)
        columns.append('city')

    state = entities.get('state')
    if state:
        condition_list.append('<<state>> = "%s"'%state)
        columns.append('state')

    style = entities.get('style')
    if style:
        condition_list.append('<<style>> = "%s"'%style)
        columns.append('style')

    sub_brand = entities.get('sub_brand')
    if sub_brand:
        condition_list.append('<<sub_brand>> = "%s"'%sub_brand)
        columns.append('sub_brand')

    created_date = entities.get('created_date', None)
    if created_date:
        date_period= created_date.get('date-period', None)
        if date_period:
            dates = tuple(date_period.split('/'))
            condition_list.append('<<created_date>> between "%s" and "%s"'%dates)
        elif created_date.get('date', None):
            condition_list.append('<<created_date>> = "%s"'%created_date['date'])
        else:
            raise ParsingError('Not a valid date.')
        columns.append('created_date')

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
