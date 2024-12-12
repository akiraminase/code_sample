#%%
import pandas as pd
import numpy as np
#import pyecharts
#import pyecharts.options as opts
import sqlalchemy
from underlying_fund import get_strategy_type
import time

#pyecharts.globals._WarningControl.ShowWarning = False

# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')

# %%
def sql_query_for_fund(fund_code):
    query= '''
        SELECT
	* 
FROM
	(
	SELECT
		t.产品名称,
		t.持仓名称,
		t.证券代码,
		单位成本,
		成本,
		成本占比,
		行情,
		市值,
		市值占比,
		持仓日期,
		现金分红,
		分红再投资,
		(市值+累计现金分红)/(成本-累计分红再投资)- 1 AS 持仓盈亏 
	FROM
		(
		SELECT
			周频估值表.产品名称,
			科目名称 AS 持仓名称,
			证券代码,
			单位成本,
			成本,
			成本占比,
			行情,
			市值,
			市值占比,
			日期 AS 持仓日期,
			现金分红,
			分红再投资
		FROM
			( SELECT 备案编码 FROM 存续基金要素 UNION SELECT 备案编码 FROM 专户基金要素 ) all_funds
			INNER JOIN 周频估值表 ON 备案编码 = 产品代码 
		WHERE
			备案编码 = '%s' 
		) t
		LEFT JOIN (SELECT
			周频估值表.产品名称,
			科目名称 AS 持仓名称,
			SUM(现金分红) AS 累计现金分红,
			SUM(分红再投资) AS 累计分红再投资
		FROM
	    ( SELECT 备案编码 FROM 存续基金要素 UNION SELECT 备案编码 FROM 专户基金要素 ) all_funds
			INNER JOIN 周频估值表 ON 备案编码 = 产品代码 
		WHERE
			备案编码 = '%s' 
		GROUP BY 持仓名称) div_sheet ON t.持仓名称 = div_sheet.持仓名称 
	
	) filtered
	INNER JOIN ( SELECT 日期 FROM 周频估值表 WHERE 产品代码 = '%s' GROUP BY 日期 ORDER BY 日期 DESC LIMIT 2 ) dates ON dates.日期 = filtered.持仓日期 
ORDER BY
	持仓日期 DESC
        ''' % (fund_code, fund_code, fund_code)

    return pd.read_sql(
        query,
        con=engine).drop(['日期'], axis =1)
# %%
'''
def npv_validation(val_data, database_data):
    if any(pd.isnull([val_data, database_data])):
        return ''
    elif float(val_data) == float(database_data):
        return ''
    else:
        return str(val_data)+' '+str(database_data)+' 数据不一致请人工复核'
'''
def if_na_then_zero(x):
    if pd.isnull(x): return 0
    else: return x
def get_time_point_holding_of(fund_code):
    #time_start = time.perf_counter()
    df= sql_query_for_fund(fund_code)
    if len(df) == 0: return df
    prev_df = df.loc[df['持仓日期']==list(df['持仓日期'])[-1]][['市值','持仓名称','行情']].rename(columns = {'市值': '上周市值','行情':'上周行情'})
    df = df.loc[df['持仓日期']==list(df['持仓日期'])[0]]
    df = pd.merge(df, prev_df, how = 'left', on='持仓名称')
    weekly_pl = []
    for prev_val, cur_val, cur_nv, cash_div, reinv_div, prev_nv in zip(list(df['上周市值']), list(df['市值']), list(df['行情']), list(df['现金分红']), list(df['分红再投资']), list(df['上周行情'])):
        
        if pd.isnull(cur_nv):
            weekly_pl.append(np.nan)
        elif(cash_div)!=0:
            weekly_pl.append((cur_val+cash_div)/prev_val-1)
        elif(reinv_div)!=0:
            #print('here')
            weekly_pl.append(cur_val/prev_val-1)
        else:
            weekly_pl.append(cur_nv/prev_nv-1)
    if max([abs(if_na_then_zero(x)) for x in weekly_pl]) >= 0.05:
        print('数据验证:', df['产品名称'][0], '有持仓周涨跌大于5%')
    df['周盈亏'] = weekly_pl
    df['策略类别'] = [get_strategy_type(fund_code) for fund_code in list(df['证券代码'])]
    return df.drop(['上周市值', '上周行情'], axis=1)
#get_time_point_holding_of("SJZ863")

# %%

# %%
