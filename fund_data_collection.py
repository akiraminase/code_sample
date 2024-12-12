#%%
import pandas as pd
import numpy as np
import pymssql
import time
# %%

conn = pymssql.connect(server='192.168.24.71', user='amperead', password='ampe2017read', database='suntime', port='2433')
cursor = conn.cursor()

# %%
start_time = time.perf_counter()
query = '''
    SELECT temp_table1.*, t_fund_org.org_name, t_fund_org.asset_mgt_scale, t_fund_org.researcher_scale
    FROM
        (SELECT fund_info.*, mapping.org_id
        FROM (
            SELECT t_fund_nv_data.*, fund_manager, fund_type_strategy, fund_type_strategy_level1, fund_type_strategy_level2, terminal_strategy, foundation_date
            FROM t_fund_nv_data 
                LEFT JOIN t_fund_info 
                on t_fund_info.fund_id = t_fund_nv_data.fund_id
            WHERE foundation_date >= '2010-01-01' AND
                (terminal_strategy = '股票多头' OR terminal_strategy = '股票市场中性' OR terminal_strategy = '管理期货')
            ) fund_info
        LEFT JOIN 
        (SELECT * FROM t_fund_org_mapping WHERE org_type_code = '100043') mapping
        on fund_info.fund_id = mapping.fund_id) temp_table1
    LEFT JOIN t_fund_org
    on temp_table1.org_id = t_fund_org.org_id
    '''
df = pd.read_sql_query(query, con=conn)
df.to_pickle('fund_data.pkl')
#df.to_excel('test_panel.xlsx')
print('Querying time:', time.perf_counter()-start_time,'s')
# %%
#df = pd.read_pickle('fund_data.pkl').pad(limit=31)
# %%
#多头数据
def process_raw_data(fund_strategy: str, start_date: str, nav_type:str = 'sanav', pad_limit:int = 31):
    df = pd.read_pickle('fund_data.pkl').pad(limit=pad_limit)
    #净值数据
    nav_df = df.loc[df['terminal_strategy'] == fund_strategy].pivot_table(values = nav_type, index = 'statistic_date', columns='fund_name', aggfunc='first')
    nav_df.loc[nav_df.index >= pd.to_datetime(start_date)].to_pickle('long_fund_sanav_2020.pkl')
    #成立日
    df.loc[df['terminal_strategy'] == fund_strategy][['fund_name','foundation_date']].set_index('fund_name').to_pickle('long_fund_foundation_dates_2020.pkl')
    #机构与产品对应表
    pd.read_sql_query('SELECT * FROM t_fund_org_mapping WHERE org_type_code = \'100043\'', con=conn).to_pickle('fund_org_mapping_2020.pkl')
process_raw_data('股票多头', '20200101')
# %%
'''
nav_df = df.loc[df['terminal_strategy'] == '股票市场中性'].pivot_table(values = 'sanav', index = 'statistic_date', columns='fund_name', aggfunc='first')
nav_df.to_pickle('neutral_fund_sanav.pkl')

nav_df = df.loc[df['terminal_strategy'] == '管理期货'].pivot_table(values = 'sanav', index = 'statistic_date', columns='fund_name', aggfunc='first')
nav_df.to_pickle('cta_fund_sanav.pkl')
#
'''
# %%
