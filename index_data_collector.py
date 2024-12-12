#%%
import pandas as pd
import tushare as ts
import sqlalchemy
from datetime import datetime, timedelta

# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')
# %%
# in datettime format
def get_last_date_for(benchmark):
    query= '''
        SELECT date
                
        FROM %s
    
        ORDER BY date DESC
        LIMIT 1
        ''' %benchmark
    
    return list(pd.read_sql(
        query,
        con=engine)['date'])[0]
# %%
sh_data = ts.get_k_data('sh', start=(get_last_date_for('上证综指序列')+timedelta(1)).strftime('%Y-%m-%d'), end=(datetime.today()-timedelta(1)).strftime('%Y-%m-%d'), ktype='D')
hs300_data = ts.get_k_data('399300', start=(get_last_date_for('沪深300序列')+timedelta(1)).strftime('%Y-%m-%d'), end=(datetime.today()-timedelta(1)).strftime('%Y-%m-%d'), ktype='D')
# %%
if len(sh_data) == 0:
    print('上证综指数据已为最新')
else:
    sh_data[['date', 'close']].to_sql('上证综指序列', con = engine, if_exists='append', index=False, method='multi')
    print('插入%s条数据至上证综指序列_ts' % str(len(sh_data)))

if len(hs300_data) == 0:
    print('沪深300数据已为最新')
else:
    hs300_data[['date', 'close']].to_sql('沪深300序列', con = engine, if_exists='append', index=False, method='multi')
    print('插入%s条数据至沪深300序列_ts' % str(len(hs300_data)))

# %%
#sh_data[['date','close']].to_excel('sh_data.xlsx')
# %%

# %%
#hs300_data[['date','close']].to_excel('hs300_data.xlsx')

# %%
