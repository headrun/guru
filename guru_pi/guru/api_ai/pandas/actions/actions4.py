
from .core_func import *

rel_operator_map = {
    'gt': '>',
    'lt': '<',
    'gte': '>=',
    'lte': '<=',
    'eq': '=='}

def compare_ranks(entities, source):
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
    rf_order_asc = True if rf_type in ['top', 'position'] else False
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
            data = pd.DataFrame()
            _t = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[ \
                                kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(_t).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            data['base_rank'] = bs.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            gs = pd.DataFrame(_t).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            data['gross_rank'] = gs.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such regions found'
        elif "geo_city_name" in columns:
            data = pd.DataFrame()
            _t = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[ \
                                kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(_t).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            data['base_rank'] = bs.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            gs = pd.DataFrame(_t).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            data['gross_rank'] = gs.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such States found'
        else:
            kpi_filter = 'abs'
            data = pd.DataFrame()
            _t = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(_t).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            data['base_rank'] = bs.sort_index(level=[4]).groupby(level=0).rank(ascending=False)
            gs = pd.DataFrame(_t).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            data['gross_rank'] = gs.sort_index(level=[4]).groupby(level=0).rank(ascending=False)
            data = pd.DataFrame(data).reset_index(level=[0], drop=True).reset_index()
            no_data_error = 'No such Operators found'

        print('bf:', data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]

            if ("data['base_rank'] < data['gross_rank']" in _filters) or ("data['gross_rank'] > data['base_rank']" in _filters):
                var_disp_name = 'Base Rank > Gross Rank'
                to_show = 'base_rank'
            elif ("data['base_rank'] > data['gross_rank']" in _filters) or ("data['gross_rank'] < data['base_rank']" in _filters):
                var_disp_name = 'Gross Rank > Base Rank'
                to_show = 'gross_rank'
            else:
                return error_mesg(get_resp_negative())

        print('af:', data.head(20))
        if data.empty:
            return error_mesg(no_data_error)

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=[to_show], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric

        #convert float columns to int
        _cc = data.select_dtypes(include=numerics).columns.tolist()
        data[_cc] = data[_cc].apply(np.int16)

        print('final:', data)
        data.rename(columns={'operator_name': var_disp_name}, inplace=True)
        data = data.round(0)
        data.fillna('-', inplace=True)
        data.replace(to_replace=0, value='-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
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

def compare_base_rank(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter
    else:
        kpi_filter = 'abs'
    columns.append(kpi_filter)

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = True if rf_type in ['top', 'position'] else False
    rf_count = row_filter.get('count', 1)

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

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
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_base'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'base_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such Regions found'
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_base'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'base_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such Cities found'
        else:
            return error_mesg('Ask either Regionwise or Statewise')

        print(data.head(10))

        exp = entities.get('rel_exp_3', {})
        if exp:
            _series_1 = data[(data['operator_name']==exp['operand_1'])]
            _series_2 = data[(data['operator_name']==exp['operand_2'])]
            _filter = "_series_1.reset_index(drop=True)[_series_2['base_rank'].reset_index(drop=True) "+rel_operator_map[exp['operator']]+" _series_1['base_rank'].reset_index(drop=True)]"
            print(_filter)
            data = eval(_filter)
            if kpi_filter.startswith('abs'):
                var_disp_name = exp['operand_1']+' '+rel_operator_map[exp['operator']]+' '+exp['operand_2']+' (Base Rank)'
            else:
                var_disp_name = exp['operand_1']+' '+rel_operator_map[exp['operator']]+' '+exp['operand_2']+' ('+kpi_filter.capitalize()+' Base Rank)'
        else:
            return error_mesg(get_resp_negative())

        print('bf:', data.head(10))
        data['base_rank'] = data['base_rank'].astype(int)
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]

        print('af:', data.head(10))
        if data.empty:
            return error_mesg(no_data_error)
        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_rank_max'] - new_df['base_rank_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'base_rank_min', \
                            'base_rank_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_rank_max'] - new_df['base_rank_min']
                _data = new_df[['operator_name', 'geo_city_name', 'base_rank_min', \
                                'base_rank_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['base_rank_max'] - new_df['base_rank_min']
                _data = new_df[['operator_name', 'base_rank_min', 'base_rank_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_filter+'_rank_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_filter+'_rank_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'base_rank_min': new_df['month_min'][0],
                                'base_rank_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else:
            data = data.pivot_table(values=['base_rank'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
            del data['operator_name']
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)

        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric
        print('final:', data.head())

        #convert all numerics to int16
        _cc = data.select_dtypes(include=numerics).columns.tolist()
        data[_cc] = data[_cc].apply(np.int16)
        data.rename(columns={'geo_rgn_name': var_disp_name, 'geo_city_name': var_disp_name}, inplace=True)
        data = data.round(1)
        data.fillna('-', inplace=True)
        data.replace(to_replace=0, value='-', inplace=True)
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

def compare_gross_rank(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    kpi_filter = entities.get('kpi_filter')
    if kpi_filter:
        kpi_filter = kpi_filter
    else:
        kpi_filter = 'abs'
    columns.append(kpi_filter)

    row_filter = entities.get('row_filter_exp')
    if not row_filter:
        row_filter = {'type':'top', 'count': 100000}
    rf_type = row_filter.get('type')
    rf_order_asc = True if rf_type in ['top', 'position'] else False
    rf_count = row_filter.get('count', 1)

    trend_exp = entities.get('trend_exp')
    if trend_exp:
        trend_exp['trend'] = trend_exp['trend'].strip()
        trend_exp['deviation'] = str(trend_exp.get('deviation', 0)).strip('%')

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
            b = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_gross'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'gross_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such Regions found'
        elif 'geo_city_name' in columns:
            b = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_gross'].sum()
            c = pd.DataFrame(b).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            data = c.sort_index(level=[6]).groupby(level=[0, 1]).rank(ascending=False)
            data.name = 'gross_rank'
            data = pd.DataFrame(data).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such Cities found'
        else:
            return error_mesg('Ask either Regionwise or Statewise')

        print(data.head(10))

        exp = entities.get('rel_exp_3', {})
        if exp:
            _series_1 = data[(data['operator_name']==exp['operand_1'])]
            _series_2 = data[(data['operator_name']==exp['operand_2'])]
            _filter = "_series_1.reset_index(drop=True)[_series_2['gross_rank'].reset_index(drop=True) "+rel_operator_map[exp['operator']]+" _series_1['gross_rank'].reset_index(drop=True)]"
            print(_filter)
            data = eval(_filter)
            if kpi_filter.startswith('abs'):
                var_disp_name = exp['operand_1']+' '+rel_operator_map[exp['operator']]+' '+exp['operand_2']+' (Base Rank)'
            else:
                var_disp_name = exp['operand_1']+' '+rel_operator_map[exp['operator']]+' '+exp['operand_2']+' ('+kpi_filter.capitalize()+' Base Rank)'
        else:
            return error_mesg(get_resp_negative())

        print('bf:', data.head(10))
        data['gross_rank'] = data['gross_rank'].astype(int)
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]

        print('af:', data.head(10))
        if data.empty:
            return error_mesg(no_data_error)
        if trend_exp: # query is "trendy", need futher processing.
            group_min = data[data['start_date']==data['start_date'].min()].reset_index(drop=True)
            group_max = data[data['start_date']==data['start_date'].max()].reset_index(drop=True)
            if 'geo_rgn_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_rgn_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_rank_max'] - new_df['gross_rank_min']
                _data = new_df[['operator_name', 'geo_rgn_name', 'gross_rank_min', \
                            'gross_rank_max', 'difference']]
                _error_if_empty = 'No such Regions found'
            elif 'geo_city_name' in columns:
                new_df = group_min.merge(group_max, on=['operator_name', 'geo_city_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_rank_max'] - new_df['gross_rank_min']
                _data = new_df[['operator_name', 'geo_city_name', 'gross_rank_min', \
                                'gross_rank_max', 'difference']]
                _error_if_empty = 'No such Cities found'
            else:
                new_df = group_min.merge(group_max, on=['operator_name'], suffixes=['_min', '_max'])
                new_df['difference'] = new_df['gross_rank_max'] - new_df['gross_rank_min']
                _data = new_df[['operator_name', 'gross_rank_min', 'gross_rank_max', 'difference']]
                _error_if_empty = 'No such Operators found'

            print('min-max month:', _data)
            # Applying "trend" filter
            if trend_exp['trend'] == 'increase':
                _data = _data[new_df['difference'] > float(trend_exp['deviation'])]
                _title = kpi_filter+'_rank_increase'
            else:
                _data =  _data[new_df['difference'] < -float(trend_exp['deviation'])]
                _title = kpi_filter+'_rank_decrease'
            # Preparing required output format
            data = _data.reset_index(drop=True)
            if data.empty:
                return error_mesg(_error_if_empty)
            data.rename(columns={'operator_name': _title,
                                'gross_rank_min': new_df['month_min'][0],
                                'gross_rank_max': new_df['month_max'][0],
                                'difference': new_df['month_min'][0]+'_vs_'+new_df['month_max'][0]},
                                inplace=True)
        else:
            data = data.pivot_table(values=['gross_rank'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
            del data['operator_name']
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)

        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric
        print('final:', data.head())

        #convert all numerics to int16
        _cc = data.select_dtypes(include=numerics).columns.tolist()
        data[_cc] = data[_cc].apply(np.int16)
        data.rename(columns={'geo_rgn_name': var_disp_name, 'geo_city_name': var_disp_name}, inplace=True)
        data = data.round(1)
        data.fillna('-', inplace=True)
        data.replace(to_replace=0, value='-', inplace=True)
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

def get_bs_minus_gs_rank(entities, source):
    rel_exp = entities.get('rel_exp')
    if rel_exp:
        _rel_exp = []
        for exp in rel_exp:
            exp['prop'] = 'bs_minus_gs'
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
    rf_order_asc = True if rf_type in ['top', 'position'] else False
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
            data = table_sniper.groupby(['operator_name', 'geo_rgn_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs_rank = bs_minus_gs.sort_index(level=[6]).groupby(level=[2, 3]).rank(ascending=False)
            bs_minus_gs_rank.name = 'bs_minus_gs_rank'
            data = pd.DataFrame(bs_minus_gs_rank).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such regions found'
        elif "geo_city_name" in columns:
            data = table_sniper.groupby(['operator_name', 'geo_city_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1, 2]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs_rank = bs_minus_gs.sort_index(level=[6]).groupby(level=[2, 3]).rank(ascending=False)
            bs_minus_gs_rank.name = 'bs_minus_gs_rank'
            data  = pd.DataFrame(bs_minus_gs_rank).reset_index(level=[0, 1], drop=True).reset_index()
            no_data_error = 'No such States found'
        else:
            data  = table_sniper.groupby(['operator_name', 'month', 'year', 'start_date'])[kpi_filter+'_base', kpi_filter+'_gross'].sum()
            bs = pd.DataFrame(data).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_base']/sum(x[kpi_filter+'_base'])*100))
            gs = pd.DataFrame(data).groupby(level=[1]).apply(lambda x : (x[kpi_filter+'_gross']/sum(x[kpi_filter+'_gross'])*100))
            bs_minus_gs = bs-gs
            bs_minus_gs_rank = bs_minus_gs.sort_index(level=[4]).groupby(level=0).rank(ascending=False)
            bs_minus_gs_rank.name = 'bs_minus_gs_rank'
            data = pd.DataFrame(bs_minus_gs_rank).reset_index(level=[0], drop=True).reset_index()
            no_data_error = 'No such Operators found'
        print('bf:', data.head(20))
        if conditions:
            _filters = modify_query('data', conditions)
            print(_filters)
            data = data.ix[eval(_filters)]
        print('af:', data.head(20))
        if data.empty:
            return error_mesg(no_data_error)

        if chart_type:
            data['Month'] = data['month'].astype(str) +' '+ data['year'].astype(str)
            data = data.groupby(['operator_name', 'Month', 'start_date']).mean().sort_index(level=2)
            data.index = data.index.droplevel(level=2)
            del data['year']
            if chart_type in ['bar', 'pie']:
                data.index = data.index.droplevel(level=1)
        else:
            data = data.pivot_table(values=['bs_minus_gs_rank'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum)
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()

        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        data = data.sort_values(data.select_dtypes(include=numerics).columns.tolist(), ascending=rf_order_asc).head(rf_count)
        if rf_type == 'position': #get specific index
            data = data.iloc[rf_count - 1] # returns a series, need to convert to DataFrame
            data = data.to_frame().T
            data = data.apply(pd.to_numeric, errors='ignore') #convert all possible numeric columns to numeric
        _cc = data.select_dtypes(include=numerics).columns.tolist()
        data[_cc] = data[_cc].apply(np.int16)
        print('final:', data)
        data.rename(columns={'operator_name': kpi_filter+'_base_share_minus_gross_share_rank'}, inplace=True)
        data = data.round(1)
        data.fillna('-', inplace=True)
        data.replace(to_replace=0, value='-', inplace=True)
        data.columns = beautify_columns(list(data.columns))
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
            res.append({"type": "chart", "data":json_data, "extras": {'yaxis_title': entities.get('kpi_filter').capitalize()+' Base Share Minus Gross Share Rank'}})
        else:
            json_data = df_to_table_data(data)
            res.append({"type":"table", "data":json_data})
    return res

