#%%
import cx_Oracle
import pandas as pd
#%%
lib_dir = r"C:\Users\gaoang\Downloads\instantclient_19_11"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)
connection = cx_Oracle.connect('RSWINDDB/Abc123@192.168.142.83:1521/VAN')
cursor = connection.cursor()
#%%
index_mapping = {'399314.SZ': '巨潮大盘',
                 '399315.SZ': '巨潮中盘',
                 '399316.SZ': '巨潮小盘',
                 '399370.SZ': '国证成长',
                 '399371.SZ': '国证价值',
                 '399372.SZ': '大盘成长',
                 '399373.SZ': '大盘价值',
                 '399374.SZ': '中盘成长',
                 '399375.SZ': '中盘价值',
                 '399376.SZ': '小盘成长',
                 '399377.SZ': '小盘价值',
                 '000300.SH': '沪深300',
                 '885306.WI': 'Wind股票策略私募基金指数',
                 '885308.WI': 'Wind股票市场中性私募基金指数'}
#%%
#股票指数数据
try:
    query = '''
            SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_CLOSE 
            FROM WINDDF.AIndexEODPrices
            WHERE S_INFO_WINDCODE IN ('%s')
            '''%('\',\''.join(index_mapping.keys()))
    stock_index_df = pd.read_sql_query(query, con=connection)
except Exception as err:
    print("Error connecting: cx_Oracle.init_oracle_client()")
    print(err)
#%%
#基金指数数据
try:
    query = '''
            SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_CLOSE 
            FROM WINDDF.CHFIndexEOD
            WHERE S_INFO_WINDCODE IN ('%s')
            '''%('\',\''.join(index_mapping.keys()))
    fund_index_df = pd.read_sql_query(query, con=connection)
except Exception as err:
    print("Error connecting: cx_Oracle.init_oracle_client()")
    print(err)
# %%
df = pd.concat([stock_index_df, fund_index_df], axis=0, ignore_index=True).pivot_table(values = 'S_DQ_CLOSE', index = 'TRADE_DT', columns='S_INFO_WINDCODE', aggfunc='first')
df.index = pd.to_datetime(df.index)
df = df.rename(index_mapping,axis=1)
df.to_pickle('indices_data.pkl')
# %%

# %%
