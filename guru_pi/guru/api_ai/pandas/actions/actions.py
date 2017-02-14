from .core_func import *

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
    conditions = ' & '.join(condition_list)
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        #t1 = eval(query)
        if "geo_rgn_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).reset_index(level=[0, 1], drop=True).reset_index()
        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).reset_index(level=[0], drop=True).reset_index()

        print(data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(20))

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['base_share'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        print('final:', data)
        data = data.round(1)
        data.fillna('-', inplace=True)
        print(kpi_filter+'_base_share')
        if not kpi_filter:
            _col_name = 'base_share'
        else:
            _col_name = kpi_filter+'_base_share'
        print(_col_name)
        data.rename(columns={'operator_name': _col_name}, inplace=True)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+' Base Share(%)'}})
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

        if "geo_rgn_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).reset_index(level=[0, 1], drop=True).reset_index()
        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).reset_index(level=[0], drop=True).reset_index()

        print(data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(20))

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['gross_share'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        print('final:', data)
        data = data.round(1)
        data.fillna('-', inplace=True)
        print(kpi_filter+'_gross_share')
        if not kpi_filter:
            _col_name = 'gross_share'
        else:
            _col_name = kpi_filter+'_gross_share'
        print(_col_name)
        data.rename(columns={'operator_name': _col_name}, inplace=True)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+' Gross Share(%)'}})
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
    chart_type = entities.get('viz_type')
    # execution
    try:
        #get rows and columns
        if "geo_rgn_name" in columns:

            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[6]).groupby(level=[2, 3]).diff()
            data.name = 'base_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()

        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[6]).groupby(level=[2, 3]).diff()
            data.name = 'base_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()

        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[4]).groupby(level=1).diff()
            data.name = 'base_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()

        print(data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(20))

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['base_share_variance'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        print('final:', data)
        data = data.round(2)
        data.fillna('-', inplace=True)
        print(kpi_filter+'_base_share_variance')
        if not kpi_filter:
            _col_name = 'base_share_variance'
        else:
            _col_name = kpi_filter+'_base_share_variance'
        print(_col_name)
        data.rename(columns={'operator_name': _col_name}, inplace=True)
        data.columns = beautify_columns(list(data.columns))
        print('res:', data)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+' Base Share Variance(%)'}})
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
        if "geo_rgn_name" in columns:

            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[6]).groupby(level=[2, 3]).diff()
            data.name = 'gross_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()

        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[6]).groupby(level=[2, 3]).diff()
            data.name = 'gross_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()

        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_abs]/sum(x[kpi_abs])*100))
            data = c.sort_index(level=[4]).groupby(level=1).diff()
            data.name = 'gross_share_variance'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()

        print(data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(20))

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['gross_share_variance'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        print('final:', data)
        data = data.round(2)
        data.fillna('-', inplace=True)
        print(kpi_filter+'_gross_share_variance')
        if not kpi_filter:
            _col_name = 'gross_share_variance'
        else:
            _col_name = kpi_filter+'_gross_share_variance'
        print(_col_name)
        data.rename(columns={'operator_name': _col_name}, inplace=True)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+' Gross Share Variance(%)'}})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

