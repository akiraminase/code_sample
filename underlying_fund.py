#%%
import pandas as pd
import numpy as np
import sqlalchemy
import time
# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')

query= '''
        SELECT
            策略类别,
            基金全称,
            基金代码,
            累计净值,
            日期 
        FROM
            底层对应关系,底层净值序列 
        WHERE
            基金简称 = 底层净值序列.标的简称 
        ORDER BY
            日期 DESC
        ''' 
all_underlying_sers = pd.read_sql(query,con=engine)

def get_time_point_info_of(fund_code):
    time_start = time.perf_counter()
    df = all_underlying_sers
    df= df.loc[df['基金代码'] == fund_code].iloc[0:2]
    #print(df)
    return df
# %%
def get_strategy_type(fund_code):
    #print('ok')
    #print(get_time_point_info_of(fund_code))
    return list(get_time_point_info_of(fund_code)['策略类别'])[0] if len(get_time_point_info_of(fund_code)) !=0 else np.nan
def get_weekly_pl(fund_code):
    return list(get_time_point_info_of(fund_code)['累计净值'])[0]/list(get_time_point_info_of(fund_code)['累计净值'])[1]-1 if len(get_time_point_info_of(fund_code)) >1 else np.nan
def get_data_date(fund_code):
    return list(get_time_point_info_of(fund_code)['日期'])[0] if len(get_time_point_info_of(fund_code)) !=0 else np.nan
def get_latest_value(fund_code):
    return list(get_time_point_info_of(fund_code)['累计净值'])[0] if len(get_time_point_info_of(fund_code)) !=0 else np.nan
# %%
