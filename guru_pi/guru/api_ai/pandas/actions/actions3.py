#
#Implements t1m/t2m/t3m quality

from .core_func import *

def get_quality(entities, source):
    columns = ['operator_name']
    group_by = ['operator_name']
    condition_list, columns = get_conditions(entities, columns)
    #agg_func_list, columns = get_agg_functions(entities, columns)

    keywords = entities.get('keyword', [])
    columns += [k for k in keywords if k not in columns]
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
        quality = quality.strip().lower()
        b = table_sniper[['operator_name', 'geo_rgn_name', 'geo_city_name', 'month', 'year', 'start_date', 'abs_gross', quality+'_quality_abs']].groupby(columns).sum()
        data = (b[quality+'_quality_abs']/b['abs_gross'])*100
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
        data.name = quality.upper()+'_quality'
        data = data.round(2)
        data = data.astype(str) + '%'
        data = pd.DataFrame(data).reset_index()
        print(data)
        if conditions:
            _filters = modify_query('data', conditions)
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
            data = data.pivot_table(values=[quality.upper()+'_quality'], columns=['start_date', 'month'], index=pivot_index, aggfunc=np.sum) 
            if data.empty:
                return error_mesg(get_resp_no_records())
            data.columns = data.columns.droplevel([0, 1])
            data = data.reset_index()
        data.rename(columns={'operator_name': quality.upper()+' Quality'}, inplace=True)
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


