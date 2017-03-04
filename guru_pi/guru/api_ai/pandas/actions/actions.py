from .core_func import *

def get_abs_base(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'abs_base'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)

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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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
            b = (b/1000).round(1)
            b.name = 'abs_base'
            data = pd.DataFrame(b).reset_index()
        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            b = (b/1000).round(1)
            b.name = 'abs_base'
            data = pd.DataFrame(b).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            b = (b/1000).round(1)
            b.name = 'abs_base'
            data = pd.DataFrame(b).reset_index()

        print(data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(10))

        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_base_max'] - new_df['abs_base_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'abs_base_min', \
                            'abs_base_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_base_max'] - new_df['abs_base_min']
                _data = new_df[['operator_name', 'geo_city_name', 'abs_base_min', \
                                'abs_base_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_base_max'] - new_df['abs_base_min']
                _data = new_df[['operator_name', 'abs_base_min', 'abs_base_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_filter+'_abs_base_increase' if kpi_filter else 'abs_base_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_filter+'_abs_base_decrease' if kpi_filter else  'abs_base_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'abs_base_min': new_df['month_min'][0],
                                'abs_base_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else:
            if chart_type:
                data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
                data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
                data.index = data.index.droplevel(level=2)
                del data['year']
                print('chart data:\n', data)
                if chart_type in ['bar', 'pie']:
                    data.index = data.index.droplevel(level=1)
                    data = data.reset_index().set_index('operator_name')
            else:
                data = data.pivot_table(values=['abs_base'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
                data.columns = data.columns.droplevel([0, 1])
                data = data.reset_index()

        # Ready to Rocka!
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #sort by numeric columns
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data, data.dtypes)
        data = data.round(1)
        data.fillna('-', inplace=True)
        if not kpi_filter:
            _col_name = 'abs_base'
        else:
            _col_name = kpi_filter+'_abs_base'
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

def get_abs_gross(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'abs_gross'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)

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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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
            b = (b/1000).round(1)
            b.name = 'abs_gross'
            data = pd.DataFrame(b).reset_index()
        elif "geo_city_name" in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            b = (b/1000).round(1)
            b.name = 'abs_gross'
            data = pd.DataFrame(b).reset_index()
        else:
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_abs].sum()
            b = (b/1000).round(1)
            b.name = 'abs_gross'
            data = pd.DataFrame(b).reset_index()

        print(data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(10))

        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_gross_max'] - new_df['abs_gross_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'abs_gross_min', \
                            'abs_gross_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_gross_max'] - new_df['abs_gross_min']
                _data = new_df[['operator_name', 'geo_city_name', 'abs_gross_min', \
                                'abs_gross_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['abs_gross_max'] - new_df['abs_gross_min']
                _data = new_df[['operator_name', 'abs_gross_min', 'abs_gross_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_filter+'_abs_gross_increase' if kpi_filter else 'abs_gross_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_filter+'_abs_gross_decrease' if kpi_filter else  'abs_gross_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'abs_gross_min': new_df['month_min'][0],
                                'abs_gross_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else:
            if chart_type:
                data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
                data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
                data.index = data.index.droplevel(level=2)
                del data['year']
                print('chart data:\n', data)
                if chart_type in ['bar', 'pie']:
                    data.index = data.index.droplevel(level=1)
                    data = data.reset_index().set_index('operator_name')
            else:
                data = data.pivot_table(values=['abs_gross'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
                data.columns = data.columns.droplevel([0, 1])
                data = data.reset_index()

        # Ready to Rocka!
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #sort by numeric columns
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data, data.dtypes)
        data = data.round(1)
        data.fillna('-', inplace=True)
        if not kpi_filter:
            _col_name = 'abs_gross'
        else:
            _col_name = kpi_filter+'_abs_gross'
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

def get_industry_base(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'ind_base'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)

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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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
        b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])['abs_base'].sum()
        c = pd.DataFrame(b).groupby(level=[1, 2, 3]).apply(lambda x : x.sum())
        data = (c/1000).round(1)
        data['Industry Base (in 000s)'] = 'Industry'
        data = pd.DataFrame(data).reset_index()
        data.rename(columns={'abs_base':'ind_base'}, inplace=True)

        print(data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(10))
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            print('chart data:\n', data)
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
                data = data.reset_index().set_index('operator_name')
        else:
            data = data.pivot_table(values=['ind_base'], columns=['start_date', 'month'], index=['Industry Base (in 000s)'], aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        # Ready to Rocka!
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #sort by numeric columns
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data, data.dtypes)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+'  Industry Base'}})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_industry_gross(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'ind_gross'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)

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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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
        b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])['abs_gross'].sum()
        c = pd.DataFrame(b).groupby(level=[1, 2, 3]).apply(lambda x : x.sum()) 
        data = (c/1000).round(1)
        data['Industry Gross (in 000s)'] = 'Industry'
        data = pd.DataFrame(data).reset_index()
        data.rename(columns={'abs_gross':'ind_gross'}, inplace=True)

        print(data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(10))
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            print('chart data:\n', data)
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
                data = data.reset_index().set_index('operator_name')
        else:
            data = data.pivot_table(values=['ind_gross'], columns=['start_date', 'month'], index=['Industry Gross (in 000s)'], aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        # Ready to Rocka!
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #sort by numeric columns
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data, data.dtypes)
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': kpi_filter.capitalize()+'  Industry Gross'}})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

def get_base_share(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'base_share'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)

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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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

        print(data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print(data.head(10))

        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_share_max'] - new_df['base_share_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'base_share_min', \
                            'base_share_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_share_max'] - new_df['base_share_min']
                _data = new_df[['operator_name', 'geo_city_name', 'base_share_min', \
                                'base_share_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_share_max'] - new_df['base_share_min']
                _data = new_df[['operator_name', 'base_share_min', 'base_share_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_abs+'_share_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_abs+'_share_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'base_share_min': new_df['month_min'][0],
                                'base_share_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else:
            if chart_type:
                data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
                data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
                data.index = data.index.droplevel(level=2)
                del data['year']
                print('chart data:\n', data)
                if chart_type in ['bar', 'pie']:
                    data.index = data.index.droplevel(level=1)
                    data = data.reset_index().set_index('operator_name')
            else:
                data = data.pivot_table(values=['base_share'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
                data.columns = data.columns.droplevel([0, 1])
                data = data.reset_index()

        # Ready to Rocka!
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #sort by numeric columns
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data, data.dtypes)
        data = data.round(1)
        data.fillna('-', inplace=True)
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
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'gross_share'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp

    print(entities)
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

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    row_filter = entities.get('row_filter_exp')

    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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

        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_share_max'] - new_df['gross_share_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'gross_share_min', \
                            'gross_share_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_share_max'] - new_df['gross_share_min']
                _data = new_df[['operator_name', 'geo_city_name', 'gross_share_min', \
                                'gross_share_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_share_max'] - new_df['gross_share_min']
                _data = new_df[['operator_name', 'gross_share_min', 'gross_share_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_abs+'_share_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_abs+'_share_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'gross_share_min': new_df['month_min'][0],
                                'gross_share_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else: # Not "trendy"
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

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric
 
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
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'base_share_variance'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)
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

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

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
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'gross_share_variance'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)
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

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

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

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

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

