from .core_func import *

def get_bs_minus_gs(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        columns.extend([kpi_filter+'_base', kpi_filter+'_gross'])
    else:
        kpi_filter = 'abs'
        columns.extend([kpi_filter+'_base', kpi_filter+'_gross'])

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
            data = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs.name = 'bs_minus_gs'
            data = pd.DataFrame(bs_minus_gs).reset_index(level=[0, 1], drop=True).reset_index()

        elif "geo_city_name" in columns:
            data = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs.name = 'bs_minus_gs'
            data  = pd.DataFrame(bs_minus_gs).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            data  = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs.name = 'bs_minus_gs'
            data = pd.DataFrame(bs_minus_gs).reset_index(level=[0], drop=True).reset_index()

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
            data = data.pivot_table(values=['bs_minus_gs'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        print('final:', data)
        data.rename(columns={'operator_name': kpi_filter+'_base_share_minus_gross_share'}, inplace=True)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': entities.get('kpi_filter').capitalize()+' Base Share Minus Gross Share(%)'}})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_base_rank(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter+'_base'
    else:
        kpi_filter = 'abs_base'
    columns.append(kpi_filter)
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
        if 'geo_rgn_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'base_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'base_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[4]).groupby(level=0).rank(ascending=False)
            data.name = 'base_rank'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()
        print(data.head(10))
        data['base_rank'] = data['base_rank'].astype(int)
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('bef', data.head())
        data = data.pivot_table(values=['base_rank'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
        data.columns = data.columns.droplevel([0, 1])
        data = data.reset_index()
        data.rename(columns={'operator_name': kpi_filter+'_rank'}, inplace=True)

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

def get_gross_rank(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter+'_gross'
    else:
        kpi_filter = 'abs_gross'
    columns.append(kpi_filter)

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
        if 'geo_rgn_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'gross_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'gross_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[4]).groupby(level=0).rank(ascending=False)
            data.name = 'gross_rank'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()
        print(data.head(10))
        data['gross_rank'] = data['gross_rank'].astype(int)
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('af:', data.head(10))
        data = data.pivot_table(values=['gross_rank'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
        data.columns = data.columns.droplevel([0, 1])
        data = data.reset_index()
        data.rename(columns={'operator_name': kpi_filter+'_rank'}, inplace=True)
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

def get_bv_with_leader(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter+'_base'
    else:
        kpi_filter = 'abs_base'
    columns.append(kpi_filter)
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
        if 'geo_rgn_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[4]).groupby(level=0).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()

        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('af', data)
        data = data.pivot_table(values=[kpi_filter+'_share_variance_with_leader'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
        data.columns = data.columns.droplevel([0, 1])
        data = data.reset_index()
        data.rename(columns={'operator_name': kpi_filter+'_share_variance_with_leader'}, inplace=True)
        data = data.round(1)
        data.fillna('-', inplace=True)
        print("bf res", data)
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

def get_gv_with_leader(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter+'_gross'
    else:
        kpi_filter = 'abs_gross'
    columns.append(kpi_filter)
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
        if 'geo_rgn_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter].sum()
            c = pd.DataFrame(b).groupby(level=[1]).apply(lambda x : (x[kpi_filter]/sum(x[kpi_filter])*100))
            data = c.sort_index(level=[4]).groupby(level=0).apply(lambda x: x-x.max())
            data.name = kpi_filter+'_share_variance_with_leader'
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()

        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('af', data)
        data = data.pivot_table(values=[kpi_filter+'_share_variance_with_leader'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
        data.columns = data.columns.droplevel([0, 1])
        data = data.reset_index()
        data.rename(columns={'operator_name': kpi_filter+'_share_variance_with_leader'}, inplace=True)
        data = data.round(1)
        data.fillna('-', inplace=True)
        print("bf res", data)
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

