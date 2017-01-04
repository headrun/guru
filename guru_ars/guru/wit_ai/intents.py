from jinja2 import Template
from django.utils import timezone

templates= {}
limit= ' limit 1000'
templates['tb_name+columns']= Template('SELECT distinct {{columns}} FROM {{tb_name}}'+ limit)

templates['tb_name+columns+condition']= Template('SELECT distinct {{columns}} FROM {{tb_name}} WHERE {{condition}}' + limit)

templates['tb_name+agg_functions+condition']= Template("""SELECT distinct {{agg_functions['agg_func'][0][0]}}({{agg_functions['agg_func'][0][1]}}) FROM {{tb_name}} WHERE {{condition}}""" + limit)

templates['tb_name+columns+agg_functions']=  Template("""SELECT distinct {{columns}} FROM {{tb_name}} WHERE {% for i in range(agg_functions['agg_func'] | length) %}{% set func=agg_functions['agg_func'][i][0] %}{% set arg= agg_functions['agg_func'][i][1] %}{% if i!=0 %} {{agg_functions['conn_agg_func'][i-1]}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{tb_name}}) {% endfor %}""" + limit)

templates['tb_name+columns+condition+agg_functions']= Template("""SELECT distinct {{columns}} FROM {{tb_name}} WHERE {% for i in range(agg_functions['agg_func'] | length) %}{% set func=agg_functions['agg_func'][i][0] %}{% set arg= agg_functions['agg_func'][i][1] %}{% if i!=0 %} {{agg_functions['conn_agg_func'][i-1]}}{% endif %} {{arg}}= (SELECT {{func}}({{arg}}) FROM {{tb_name}} WHERE {{condition}}) {% endfor %} and {{condition}}""" + limit)

templates['tb_name+columns+condition+groups+orderby']= Template('SELECT distinct {{columns}} FROM {{tb_name}} WHERE {{condition}} GROUP BY {{groups}} ORDER BY{{orderby}}' + limit)

def get_query(query_parameters):
    #search templates
    print('inside')
    print(query_parameters)
    for key in templates.keys():
        key_set= set(key.split('+'))
        print(key_set, set(query_parameters.keys()))
        if key_set == set(query_parameters.keys()):
            print('yes')
            return templates[key].render(query_parameters)
        else:
            print('no')

def get_tbname(parameters):
    if 'style' in parameters:
        return 'ROS_sell_thru_ss_Report'
    elif 'sub_brand' in parameters:
        return 'ROS_sell_thru_ssb_Report'
    else:
        return 'ROS_sell_thru_sb_Report'

table_column_map= {
        'ROS_sell_thru_sb_Report': [('ros', 'ros_week'),],
        'ROS_sell_thru_ssb_Report': [('ros', 'ros_week'),],
        'ROS_sell_thru_ss_Report': [('ros', 'style_ros'),],
        }

def modify_columns(tbname, columns):
    for map in table_column_map[tbname]:
        if map[0] in columns:
            columns.remove(map[0])
            columns.append(map[1])
    return columns

def handle_without_intent(entities):
    columns= []
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    column_filters= [columns[0]]
    tb_name= get_tbname(columns)
    #modify column names as per the Table_name
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_ros_info(entities, extra_columns):
    columns= []
    columns.append('ros')
    column_filters= ['ros'] #column filters represent the actual answer required.
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    #modify the column_names as per the Table_name
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_sell_thru_info(entities, extra_columns):
    columns = []
    columns.append('sell_thru')
    column_filters= ['sell_thru']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    #modify the column_names as per the Table_name
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_brand_info(entities, extra_columns):
    columns= []
    columns.append('brand')
    column_filters= ['brand']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_location_state_info(entities, extra_columns):
    columns= []
    columns.append('state')
    column_filters= ['state']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_location_city_info(entities, extra_columns):
    columns= []
    columns.append('city')
    column_filters= ['city']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_location_info(entities, extra_columns):
    columns= []
    columns.extend(['state', 'city'])
    column_filters= ['state', 'city']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_store_info(entities, extra_columns):
    columns= []
    columns.extend(['store_code', 'cust_name'])
    column_filters= ['store_code', 'cust_name']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_sub_brand_info(entities, extra_columns):
    columns= []
    columns.append('sub_brand')
    column_filters= ['sub_brand']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_style_info(entities, extra_columns):
    columns= []
    columns.append('style')
    column_filters= ['style']
    columns.extend(extra_columns)
    condition, columns, raw_info = compose_condition(entities, columns)
    #add more to condition here
    agg_functions, columns, raw_info = get_agg_functions(entities, columns, raw_info)
    tb_name= get_tbname(columns)
    columns= modify_columns(tb_name, columns)
    column_filters= modify_columns(tb_name, column_filters)
    #agg_functions= modify_columns(tb_name, agg_functions)
    query_parameters = {}
    query_parameters['tb_name'] = tb_name
    query_parameters['columns'] = ', '.join(columns)
    if agg_functions:
        query_parameters['agg_functions'] = agg_functions
    if condition:
        query_parameters['condition'] = condition
    sql_query = get_query(query_parameters)
    if sql_query:
        for s1, s2 in table_column_map[tb_name]:
            sql_query= sql_query.replace('<<' + s1 + '>>', s2)
        sql_query= sql_query.replace('<<', '').replace('>>', '')
        raw_info['sql_query']= sql_query
        return (sql_query, columns, column_filters, raw_info)

def get_agg_functions(entities, columns, raw_info):
    agg_functions= {}
    conn_agg_func= [item['value'] for item in entities.get('conn_agg_func', [])]
    agg_func= [item['value'] for item in entities.get('agg_func', [])]
    agg_func_arg= [item['value'] for item in entities.get('agg_func_arg', [])]
    print('test line', raw_info)
    raw_info['conn_agg_func']= conn_agg_func
    raw_info['agg_func']= agg_func
    raw_info['agg_func_arg']= agg_func_arg

    if len(conn_agg_func)< len(agg_func_arg)- 1:
        for i in range(len(conn_agg_func), len(agg_func_arg)- 1):
            conn_agg_func.append('and')
    if len(agg_func) < len(agg_func_arg):
        #replicate the last one
        for i in range(len(agg_func), len(agg_func_arg)):
            agg_func.append(agg_func)

    temp_l= []
    for agg_f, agg_f_arg in zip(agg_func, agg_func_arg):
        columns.append(agg_f_arg)
        agg_f_arg= '<<' + agg_f_arg+ '>>'
        temp_l.append((agg_f, agg_f_arg))
    if temp_l:
        agg_functions['agg_func']= temp_l
    temp_l= []
    for conn in conn_agg_func:
        temp_l.append(conn)
    if temp_l:
        agg_functions['conn_agg_func']= temp_l
    print(agg_functions)
    #print(conn_agg_func)
    #print(agg_func)
    #print(agg_func_arg)
    columns= list(dict.fromkeys(columns).keys())
    return (agg_functions, columns, raw_info)

def compose_condition(entities, columns= []):
    """
        composes SQL WHERE CONDITION from extracted entities
    """
    condition= ''
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


