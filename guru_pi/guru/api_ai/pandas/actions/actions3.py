#
#actions t1m/t2m/t3m quality, net_to_gross, base_HHI

from .core_func import *

def get_quality(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'quality'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp
    print(entities)
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = False if rf_type in ['top', 'position'] else True
    rf_count = row_filter.get('count', 1)

    quality = entities.get('quality_measure', None)
    if not quality:
        return error_mesg(get_resp_negative())

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
        #if True:
        quality = quality.strip().lower()
        b = table_sniper[['operator_name', 'geo_rgn_name', 'geo_city_name', 'month', 'year', 'start_date', 'abs_gross', quality+'_quality_abs']].groupby(columns).sum()
        data = (b[quality+'_quality_abs']/b['abs_gross'])*100
        print(data)
        """
        if quality.strip().lower()=='t1m':
            shift_by = -1
        elif quality.strip().lower()=='t2m':
            shift_by = -2
        elif quality.strip().lower()=='t3m':
            shift_by = -3
        else:
            return error_mesg(get_resp_negative())
        data = b.sort_index(level=[0, 3]).groupby(level=0).apply(lambda x: (x.shift(shift_by)/x)*100)
        """
        data.name = 'quality'
        data = pd.DataFrame(data).reset_index()
        print(data)
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['quality_max'] - new_df['quality_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'quality_min', \
                            'quality_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['quality_max'] - new_df['quality_min']
                _data = new_df[['operator_name', 'geo_city_name', 'quality_min', \
                                'quality_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['quality_max'] - new_df['quality_min']
                _data = new_df[['operator_name', 'quality_min', 'quality_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = quality.upper()+' Quality Increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = quality.upper()+' Quality Decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            data = data.round(2)
            #data['quality_min'] = data['quality_min'].astype(str) + '%'
            #data['quality_max'] = data['quality_max'].astype(str) + '%'
            #data['difference'] = data['difference'].astype(str) + '%'
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'quality_min': new_df['month_min'][0],
                                'quality_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)

        else:
            if chart_type:
                data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
                data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
                data.index = data.index.droplevel(level=2)
                del data['year']
                del data[kpi_abs]
                if chart_type in ['bar', 'pie']:
                    data.index = data.index.droplevel(level=1)
            else:
                data = data.round(2)
                #data['quality'] = data['quality'].astype(str) + '%'
                data = data.pivot_table(values=['quality'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum) 
                if data.empty:
                    return error_mesg(get_resp_no_records())
                data.columns = data.columns.droplevel([0, 1])
                data = data.reset_index()
        print(data)
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        data.rename(columns={'operator_name': quality.upper()+' Quality(%)'}, inplace=True)
        data.fillna('-', inplace=True)
        data.replace(to_replace='0.0%', value='-', inplace=True)
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

def get_net_to_gross(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'net2gross'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp

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
        cols = ['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date', kpi_filter+'_base', kpi_filter+'_gross']
        if "geo_rgn_name" in columns:
            a = table_sniper[cols].groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date']).sum()
            b = a.sortlevel([0, 1, 4])
            data = b.groupby(level=[0, 1], group_keys=False).apply(lambda x: (x[kpi_filter+'_base']-x[kpi_filter+'_base'].shift(1))/x[kpi_filter+'_base'])
            data.name = 'net2gross'
            data = pd.DataFrame(data).reset_index()

        elif "geo_city_name" in columns:
            a = table_sniper[cols].groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date']).sum()
            b = a.sortlevel([0, 1, 4])
            data = b.groupby(level=[0, 1], group_keys=False).apply(lambda x: (x[kpi_filter+'_base']-x[kpi_filter+'_base'].shift(1))/x[kpi_filter+'_gross'])
            data.name = 'net2gross'
            data = pd.DataFrame(data).reset_index()
        else:
            a = table_sniper[cols].groupby(['operator_name', 'month', 'year', 'start_date']).sum()
            b = a.sortlevel([0, 3])
            data = b.groupby(level=[0], group_keys=False).apply(lambda x: (x[kpi_filter+'_base']-x[kpi_filter+'_base'].shift(1))/x[kpi_filter+'_gross'])
            data.name = 'net2gross'
            data = pd.DataFrame(data).reset_index()

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
            data = data.pivot_table(values=['net2gross'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count) 
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        print('final:', data)
        if kpi_filter.startswith('abs'):
            _col_name = 'N2G'
        else:
            _col_name = ' '.join(kpi_filter.split('_')).capitalize()+ ' N2G'
        data.rename(columns={'operator_name': _col_name}, inplace=True)
        data = data.round(2)
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

def get_base_hhi(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'base_HHI'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp

    columns = []
    group_by = []
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        columns.append(kpi_filter+'_base')
    else:
        kpi_filter = 'abs'
        columns.append(kpi_filter+'_base')

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

    pivot_index = []
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
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_base'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3, 4]).apply(lambda x : (sum(x['base_share']**2)))
            data.name = 'base_HHI'
            data = pd.DataFrame(data).reset_index()
            data['base_HHI'] = data['base_HHI'].round(0).astype(int)
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_base'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3, 4]).apply(lambda x : (sum(x['base_share']**2)))
            data.name = 'base_HHI'
            data = pd.DataFrame(data).reset_index()
            data['base_HHI'] = data['base_HHI'].round(0).astype(int)
        else: #overall base HHI
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter+'_base'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            c.name = 'base_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3]).apply(lambda x : (sum(x['base_share']**2)))
            data.name = 'base_HHI'
            data = pd.DataFrame(data).reset_index()
            data['base_HHI'] = data['base_HHI'].round(0).astype(int)
            data['main_col'] = "Overall"
            pivot_index.append('main_col')

        print('bf:', data.head(10))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('af:', data.dtypes)
        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['base_HHI'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            if data.empty:
                return error_mesg(get_resp_no_records())
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        data = data.astype(int, raise_on_error=False)
        if kpi_filter.startswith('abs'):
            _col_name = 'Base HHI'
        else:
            _col_name = ' '.join(kpi_filter.split('_')).capitalize()+' Base HHI'
        data.rename(columns={'geo_rgn_name': _col_name, 'geo_city_name': _col_name, 'main_col': _col_name}, inplace=True)
        data.fillna('-', inplace=True)
        data.replace(to_replace='0.0%', value='-', inplace=True)
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

def get_gross_hhi(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'gross_HHI'
            _rel_exp.append(exp)
        entities['rel_exp'] = _rel_exp

    columns = []
    group_by = []
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)
    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        columns.append(kpi_filter+'_gross')
    else:
        kpi_filter = 'abs'
        columns.append(kpi_filter+'_gross')

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

    pivot_index = []
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
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_gross'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3, 4]).apply(lambda x : (sum(x['gross_share']**2)))
            data.name = 'gross_HHI'
            data = pd.DataFrame(data).reset_index()
            data['gross_HHI'] = data['gross_HHI'].round(0).astype(int)
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_gross'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3, 4]).apply(lambda x : (sum(x['gross_share']**2)))
            data.name = 'gross_HHI'
            data = pd.DataFrame(data).reset_index()
            data['gross_HHI'] = data['gross_HHI'].round(0).astype(int)
        else: #overall gross HHI
            b = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter+'_gross'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2], group_keys=False).apply( \
                                lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            c.name = 'gross_share'
            data = pd.DataFrame(c).groupby(level=[1, 2, 3]).apply(lambda x : (sum(x['gross_share']**2)))
            data.name = 'gross_HHI'
            data = pd.DataFrame(data).reset_index()
            data['gross_HHI'] = data['gross_HHI'].round(0).astype(int)
            data['main_col'] = "Overall"
            pivot_index.append('main_col')

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
            del data[kpi_abs]
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['gross_HHI'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            if data.empty:
                return error_mesg(get_resp_no_records())
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric
        data = data.astype(int, raise_on_error=False)
        if kpi_filter.startswith('abs'):
            _col_name = 'Gross HHI'
        else:
            _col_name = ' '.join(kpi_filter.split('_')).capitalize()+' Gross HHI'
        data.rename(columns={'geo_rgn_name': _col_name, 'geo_city_name': _col_name, 'main_col': _col_name}, inplace=True)
        data.fillna('-', inplace=True)
        data.replace(to_replace='0.0%', value='-', inplace=True)
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
