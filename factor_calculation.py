#!/usr/bin/env python
"""
factor_calculation.py:
选基与回测的主方法，包含选基、回测、与输出的主要逻辑模块
"""
#%%
from datetime import timedelta
from typing import List, Dict, Tuple
import time
import os

import numpy as np
import pandas as pd
import xlsxwriter


#因子库，可添加因子，如需要添加非线性的因子组合方法，建议在该类内修改
from factor_loading import *
#自定义后的kmeans方法库，如需要输出截面kmeans结果，请在该类内直接修改
#这里的kmeans聚类可以替换成任何可以打标签的机器学习/传统统计方法
from kmeans import get_k_means
#回测框架
from backtester import *
#事前风控模块, 优选前使用该模块过滤数据错误和风格漂移基金
from risk_models import get_abnormal_products_by_nav_change, get_incomplete_products, get_null_products, get_wrong_date_products
# %%
nav_data = pd.read_pickle('long_fund_sanav_2020.pkl')#朝阳永续多头策略全量数据
fund_org_mapping = pd.read_pickle('fund_org_mapping_2020.pkl')[['fund_name', 'org_name']]#朝阳永续产品对应机构全量数据
fund_foundation_date = pd.read_pickle('long_fund_foundation_dates_2020.pkl').squeeze().to_dict()#朝阳永续多头策略全量成立日
indices = pd.read_pickle('indices_data.pkl')
six_style_indices_names = ['大盘成长', '大盘价值', '中盘成长', '中盘价值', '小盘成长', '小盘价值']
four_style_indices_names = ['巨潮大盘', '巨潮小盘', '国证成长', '国证价值']
long_fund_index = indices['Wind股票策略私募基金指数'].dropna()
#neutural_fund_index = indices['Wind股票市场中性私募基金指数'].dropna()

if len(nav_data.columns) != len(fund_foundation_date.keys()):
    raise ValueError('Data Length not aligned. Check Data Source')
#按周五resample,一周内数据自动线性前推
nav_data =nav_data.resample('W-FRI').last()#.iloc[:,2000:5000]

#%%
start_date = nav_data.index[0]
end_date = nav_data.index[-1] - timedelta(days=31)
signal_days = pd.date_range(start = start_date, end=end_date, freq='WOM-1FRI')
selling_days = pd.date_range(start = start_date, end=end_date, freq='WOM-2FRI')
buying_days = pd.date_range(start = start_date, end=end_date, freq='WOM-4FRI')
# %%
#基金池筛选
def fill_fund_pool(topx:int=10, min_periods:int=12, rolling_period:int = None, rebalance_months: List[int] = [12,3,6,9], neutralization: bool=False, factors: List[str]= ['Total_Score']) -> Dict[datetime,List[str]]:
    """生成优选基金池

    Args:
        topx (int, optional): 选取指标排序前x只基金. Defaults to 10.
        min_periods (int, optional): 最小观察周期，单位为月，观察期不在内的不入池. Defaults to 12.
        rolling_period (int, optional): 计算回滚期，单位为周，例如如果使用近26周（半年）夏普比率，使用该参数传入回滚周期，默认全历史. Defaults to None.
        rebalance_months (List[int], optional): 取值为1-12自然月，建议使用节假日较少的月份. Defaults to [12,3,6,9].
        neutralization (bool, optional): 是否中性化处理指标，使风格/聚类间基金指标可比. Defaults to False.
        factors (List[str], optional): 因子列表，名称必须为Total_Score(多因子百分位排序等权打分)或由已定义因子名组成的列表（见factor_laoding.py)，
                                       列表内顺序为排序优先级从前到后递减. Defaults to ['Total_Score'].

    Returns:
        Dict[datetime,List[str]]: 输出key为日期，value为基金名称列表的词典
    """     
    fund_pool = {}
    total_time = []
    #至少观察n个月
    for day in signal_days[min_periods:]:
        start_time = time.perf_counter()
        #定义调仓月，可改为参数传入，推荐选择节假日少的月份
        if int(day.month) not in rebalance_months:
            continue
        #切分数据，并删除该截面数据全空产品
        sliced_nav_data = nav_data.loc[nav_data.index<=day].dropna(axis=1,how='all')
        if len(sliced_nav_data.index) == 0:
            raise ValueError('You passed invalid min_periods which resulted empty sliced_data. Please check your passed parameters.')
        if rolling_period != None:
            sliced_nav_data = sliced_nav_data.iloc[rolling_period*-1:]
        merged_df = pd.merge(sliced_nav_data, indices, how='left', left_index=True, right_index=True)
        merged_df = np.log(1+merged_df.pct_change()).dropna(axis=0,how='all')
        #print('sliced_nav_data length:', len(sliced_nav_data.columns))
        #事前风控剔除黑名单
        #print('data amount：',len(sliced_nav_data.columns))
        sliced_nav_data = sliced_nav_data.drop(get_abnormal_products_by_nav_change(sliced_nav_data), axis=1)
        sliced_nav_data = sliced_nav_data.drop(get_null_products(sliced_nav_data), axis=1)
        #to do: make this work
        #sliced_nav_data = sliced_nav_data.drop(get_incomplete_products(sliced_nav_data, min_period=min_periods), axis=1)
        #sliced_nav_data = sliced_nav_data.drop(get_wrong_date_products(sliced_nav_data, fund_foundation_date), axis=1)
        fund_stats = {}
        #print(sliced_nav_data.columns)
        for fund_name in sliced_nav_data.columns:
            #如果无数据或已终止，不入池
            #if len(sliced_nav_data[fund_name].dropna()) == 0 or sliced_nav_data[fund_name].dropna().index[-1] != day:
            #    continue
            
            #封装：如果最早数据(即使是NA）可用日期早于成立日，不入池
            if sliced_nav_data[fund_name].index[0] >= fund_foundation_date[fund_name]:
                continue

            features = {}
            log_return_series = get_log_return(sliced_nav_data[fund_name].dropna())
            #封装：如果截面数据不足观察期或净值无变化，跳过
            #4.3为一个月平均周数
            if len(log_return_series) <= min_periods*4.3:# or log_return_series.std() == 0:
                continue
            for feature in factors:
                #print(merged_df)
                if  feature == 'Total_Score' or (feature not in get_all_defined_factors()):
                    #print(feature, 'passed')
                    continue
                else:
                    #如果需要国证六风格指数
                    #print(merged_df)
                    if ('Neutral' in feature )or ('SDS' in feature):
                        #print(feature,'HRE?')
                        factor_value = get_factor_value(feature, fund_name, merged_df = merged_df, log_return_series = log_return_series, benchmark_name=six_style_indices_names)
                    else:
                        factor_value = get_factor_value(feature, fund_name, merged_df = merged_df, log_return_series = log_return_series)
                        #print(factor_value)
                    features[feature] = factor_value
                    

            fund_stats[fund_name] = pd.Series(features).to_frame().T
            #print(fund_stats)
        #合并
        #print(fund_stats)
        df = pd.concat(fund_stats.values(), keys=fund_stats.keys(), axis=0).dropna().droplevel(1,axis=0)
        #同一特征在截面百分位化
        df = df.rank(pct=True, axis=0)
        #计算等权分数
        if 'Total_Score' in factors:
            df['Total_Score'] = df.sum(axis=1)
        #按传入因子进行总排序
        df = df.sort_values(by=factors,ascending=False)
        #如果不做中性化处理
        if neutralization==False:
            #print(df)
            df = pd.merge(df, fund_org_mapping, how='left', left_index=True, right_on='fund_name')[['fund_name', 'org_name']+factors].dropna().reset_index(drop=True)
            #机构间排序
            df = df.groupby(by='org_name', as_index=False).first().sort_values(by=factors, ascending=False).set_index('fund_name')
            fund_pool[day] = list(df.head(topx).index)

            total_time.append(time.perf_counter()-start_time)
            print(datetime.strftime(day,'%Y-%m-%d'), 'Completed in %1.2fs' %(time.perf_counter()-start_time))
            #print(fund_pool)
            continue

        #获取截面kmeans聚类
        df = pd.merge(df, get_k_means(sliced_nav_data, k_means_clusters = 7), how='left', left_index=True, right_index=True)
        #指标按聚类中性化
        relative_indicator = df.groupby(by='Kmeans_cluster')[factors].apply(lambda array: array-array.mean()).sort_values(by=factors, ascending=False)#.set_index('fund_name')
        #获取对应机构
        #print(relative_indicator)
        relative_indicator = pd.merge(relative_indicator, fund_org_mapping, how='left', left_index=True, right_on='fund_name')[['fund_name', 'org_name']+factors].dropna().reset_index(drop=True)
        #同一机构筛选最好的一只并在机构间排序
        relative_indicator = relative_indicator.groupby(by='org_name', as_index=False).first().sort_values(by=factors, ascending=False).set_index('fund_name')
        #选取topx
        fund_pool[day] = list(relative_indicator.head(topx).index)

        total_time.append(time.perf_counter()-start_time)
        print(datetime.strftime(day,'%Y-%m-%d'), 'Completed in %1.2fs' %(time.perf_counter()-start_time))

        #break

    print('Fund Selection Completed in %dmin%1.2fs'%((sum(total_time)//60), sum(total_time)%60))
    #print(fund_pool)
    return fund_pool

#%%
#获取清算/终止净值
def get_final_nav(fund_name:str)->float:
    """获取df内最后的特定基金净值数据

    Args:
        fund_name (str): 基金名称

    Returns:
        float: 基金最后净值
    """    
    return nav_data[fund_name].dropna()[-1]

def get_nav(fund_name:str, nav_date:datetime)->float:
    """获取特定基金特定日期净值

    Args:
        fund_name (str): 基金名称
        nav_date (datetime): 净值日期

    Returns:
        float: 基金净值
    """    
    try:
        #为确保单只基金数据无误，再次resample并外推
        #为什么这里要再次清洗？
        #因为单只基金数据被取出后可能存在两周不更新的，这个在全量清洗的时候是没有办法先dropna再处理的
        #这个不会影响时间序列本身的统计特征（时间戳不敏感），但是在回测按日期取净值的时候很关键
        ret = nav_data[fund_name].dropna().resample('W-FRI').last().pad()[nav_date]
    except Exception as e:
        #如果不存在，使用最后可用净值替代
        #print('Failed to fatch nav data:', fund_name, nav_date, 'used last nav')
        return get_final_nav(fund_name)
    
    return ret


# %%
#回测基金池组合
def backtest_pool(fund_pool: Dict[datetime, List[str]])-> Tuple[pd.Series, pd.Series, pd.Series]:
    """含有具体交易方法，通过调用backtester.py实现交易行为。如需要修改交易方法（调仓方法），在该方法内更改

    Args:
        fund_pool (Dict[datetime, List[str]]): key为日期，value为基金名称列表的词典

    Raises:
        ValueError: 现金不可为na

    Returns:
        Tuple[pd.Series, pd.Series, pd.Series]: 依次为
        1. 日期索引的组合净值序列
        2. 日期索引的换手率序列
        3. 日期索引的持仓序列
    """    
    portfolio_value = {}
    turnover_ratio = {}

    cnt=0
    bt = backtest(start_cash=10**7)
    for signal_day, selling_day, buying_day in zip(signal_days[12:], selling_days[12:], buying_days[12:]):
        #信号日对组合记录一次估值
        signal_day_portfolio = list(bt.portfolio.keys())
        if len(signal_day_portfolio) == 0:
            portfolio_value[signal_day]=(bt.cash)
        else:
            portfolio_value[signal_day]=bt.eval_all_asset(dict(zip(signal_day_portfolio, [get_nav(fund_name, signal_day) for fund_name in signal_day_portfolio])))

        #非调仓月仅估值，不交易
        if int(signal_day.month) not in [12, 3, 6, 9]:
            continue

        #假设卖出指令于信号日与卖出日间执行，以卖出日价值完成卖出并记录组合价值
        target_portfolio = fund_pool[signal_day]
        selling_day_value = bt.eval_all_asset(dict(zip(signal_day_portfolio, [get_nav(fund_name, selling_day) for fund_name in signal_day_portfolio])))

        current_portfolio = list(bt.portfolio.keys())

        selling_day_value = bt.eval_all_asset(dict(zip(current_portfolio, [get_nav(fund_name, selling_day) for fund_name in current_portfolio])))

        if len(bt.portfolio.keys()) == 0:
            pass
        else:
            for fund_name in current_portfolio:
                #卖出全部需要卖出的
                if fund_name not in target_portfolio:
                    fund_nav = get_nav(fund_name, selling_day)
                    bt.sell(fund_name, fund_nav , bt.portfolio[fund_name], selling_day)
                    turnover_ratio[selling_day] = bt.cash/selling_day_value
                
        portfolio_value[selling_day] = selling_day_value

        #买入日
        pre_buy_portfolio = list(bt.portfolio.keys())
        buying_day_value = bt.eval_all_asset(dict(zip(pre_buy_portfolio, [get_nav(fund_name,buying_day) for fund_name in pre_buy_portfolio])))

        for fund_name in target_portfolio:
            fund_nav = get_nav(fund_name, buying_day)
            
            if pd.isna(bt.cash): raise ValueError(buying_day_value)
            if fund_name not in pre_buy_portfolio:
                #等权买入
                #如需要对组合进行MV/风险平价/波动率倒数加权优化，可以在这里插入方法
                bt.buy(fund_name, fund_nav, buying_day_value/len(target_portfolio)/fund_nav, buying_day)
            else:
                #等权再平衡
                adj_share = buying_day_value/len(target_portfolio)/fund_nav-bt.portfolio[fund_name]
                if adj_share>0:
                    bt.buy(fund_name, fund_nav, adj_share, selling_day)
                else:
                    bt.sell(fund_name, fund_nav, adj_share*-1, selling_day)
        portfolio_value[buying_day] = buying_day_value
        
        cnt+=1

        portfolio_value = pd.Series(portfolio_value, dtype='float64')
        turnover_series = pd.Series(turnover_ratio, dtype='float64')
        turnover_series.name = 'turnover'
        holdings = pd.Series(fund_pool)
        holdings.name = 'holdings'
    return portfolio_value/portfolio_value[0], turnover_series, holdings 
# %%
def output_to_excel_sheet(plot_nav: pd.DataFrame, strategy:str, writer:pd.ExcelWriter)->pd.DataFrame:
    """将数据传入excel sheet，并用数据画图和计算组合绩效，如要修改绘图和绩效，请在改方法内修改

    Args:
        plot_nav (pd.DataFrame): 要传入的数据，可以是多个column
        strategy (str): 策略名称
        writer (pd.ExcelWriter): 方法外初始化过的writer，包含文件初始信息

    Returns:
        pd.DataFrame: 绩效评价df
    """    

    #计算绩效评价指标
    evaluation = {}
    evaluation['Annulized_Return'] = get_log_return(plot_nav['portfolio']).dropna().mean()*52
    evaluation['Annulized_Volatility'] = get_log_return(plot_nav['portfolio']).dropna().std()*np.sqrt(52)
    for factor in get_all_defined_factors()[:6]:
        evaluation[factor] = get_factor_value(factor, 'portfolio', get_log_return(plot_nav[['portfolio','沪深300']]), get_log_return(plot_nav['portfolio']))
    evaluation['Max_Drawdown'] = get_maxdrawdown(plot_nav['portfolio'].dropna())

    #获取writer信息并写入数据
    workbook = writer.book
    plot_nav.to_excel(writer, sheet_name = strategy, startrow=1)
    evaluation = pd.Series(evaluation, dtype='float64').to_frame().rename({0:''}).T
    evaluation.to_excel(writer, sheet_name=strategy, startrow=25, startcol=8)

    #选择某一策略的页面
    worksheet = writer.sheets[strategy]

    date_format = workbook.add_format({'num_format': 'yyyy/m/d'})

    row_end = len(plot_nav.index)+2
    #绘图
    chart1 = workbook.add_chart({'type': 'line'})
    chart1.add_series({
        'name': '组合净值',
        'categories': '=%s!$A$3:$A$%d' %(strategy, row_end),
        'values': '=%s!$B$3:$B$%d' %(strategy, row_end),
        'line': {'color': '#C0504D'}
    })
    chart1.add_series({
        'name': 'Wind股票策略私募基金指数',
        'categories': '=%s!$A$3:$A$%d' %(strategy, row_end),
        'values': '=%s!$C$3:$C$%d' %(strategy, row_end),
        'line': {'color': 'brown'}
    })
    chart1.add_series({
        'name': '沪深300',
        'categories': '=%s!$A$3:$A$%d' %(strategy, row_end),
        'values': '=%s!$D$3:$D$%d' %(strategy, row_end),
        'line': {'color': '#F79646'}
    })
    chart1.add_series({
        'name': '超额收益',
        'categories': '=%s!$A$3:$A$%d' %(strategy, row_end),
        'values': '=%s!$E$3:$E$%d' %(strategy, row_end),
        'line': {'color': '#4F81BD'}
    })
    chart1.add_series({
        'name': '换手率',
        'categories': '=%s!$A$3:$A$%d' %(strategy, row_end),
        'values': '=%s!$G$3:$G$%d' %(strategy, row_end),
        'line': {'color': 'green'}
    })
    chart1.set_title({'name':strategy})
    chart1.set_x_axis({'name':'日期'})
    chart1.set_y_axis({'name':'复权累计净值'})
    chart1.set_legend({'postion':'bottom'})
    chart1.set_size({'width': 1000, 'height':400})

    #插入图片并设定单元格格式
    worksheet.insert_chart('H3', chart1)
    worksheet.set_column('A:A', 10, date_format)
    #print(evaluation)
    return evaluation

#%%
def invoke_backtest(fund_pool_file_name: str, strategy_name: str, excel_writer: pd.ExcelWriter)->pd.DataFrame:
    """启动回测，衔接主回测方法与excel输出方法的协调性方法
       因为是协调性方法，如果需要自定义业绩指标等，请在此方法内修改，将其merge到big_df内

    Args:
        fund_pool_file_name (str): 需要回测的基金优选池文件名（非路径）
        strategy_name (str): 该文件对应的策略名
        excel_writer (pd.ExcelWriter): 外部传入的excelwriter，已定义好输出文件信息

    Returns:
        pd.DataFrame: 组合绩效df
    """    
    fund_pool = pd.read_pickle(fund_pool_file_name).to_dict()
    plot_nav, turnover_series, holding = backtest_pool(fund_pool)
    plot_nav.name='portfolio'
    plot_nav = (plot_nav/plot_nav[0]).to_frame()
    plot_nav = pd.merge(plot_nav, indices['Wind股票策略私募基金指数'].resample('W-FRI').last().pad(), how='left',left_index=True, right_index=True)
    plot_nav = pd.merge(plot_nav, indices['沪深300'], how='left',left_index=True, right_index=True)
    plot_nav['Wind股票策略私募基金指数'].bfill(inplace=True)
    plot_nav['Wind股票策略私募基金指数'] = (plot_nav['Wind股票策略私募基金指数']/plot_nav['Wind股票策略私募基金指数'][0]).pad()
    plot_nav['沪深300'] = (plot_nav['沪深300']/plot_nav['沪深300'][0]).pad()
    plot_nav['excess_return'] = plot_nav['portfolio'] - plot_nav['沪深300']
    big_df = pd.merge(plot_nav, holding, how='left', left_index=True, right_index=True)
    big_df = pd.merge(big_df, turnover_series, how='left', left_index=True, right_index=True)
    big_df['turnover'] = big_df['turnover'].pad().fillna(0)
    big_df['holdings'] = big_df['holdings'].pad()
    plot_nav.plot(title=strategy_name)
    return output_to_excel_sheet(big_df, strategy_name, excel_writer)

#%%
#主方法，选基存档
#to do: 测试其他新因子
for factor in get_all_defined_factors()[:6]: 
    #用pd.Series简单转换dict然后存为pkl
    #try:
    pd.Series(fill_fund_pool(factors=[factor], neutralization=True)).to_pickle('fund_pool_sf_%s_clustered_test_2020.pkl'%factor)
    #except Exception as e:
    #    print(factor, e)
    #    continue

# %%
#主方法，回测
#定义输出文件
with pd.ExcelWriter(datetime.today().strftime('%Y%m%d')+'_backtest_results_test.xlsx') as writer:
    strategy_perfs = {}
    #获取文件夹内全部文件名
    for file_name in pd.Series(os.listdir(os.getcwd()),dtype='str').sort_values().tolist():
        #只对特定文件回测
        #请不要传入非前文定义格式的pkl文件
        if '.pkl' in file_name and '_Neutral' in file_name:
            strategy_perf = invoke_backtest(file_name, ''.join(file_name[:-4].split('_')[3:]), writer)
            strategy_perfs[''.join(file_name[:-4].split('_')[3:])] = strategy_perf
    #将各个策略的组合绩效输出到最后一页
    pd.concat(strategy_perfs.values(), keys = strategy_perfs.keys(), axis=0).to_excel(writer, sheet_name='Performance_Summary')
# %%
#invoke_backtest('fund_pool_mf_all10k.pkl', ''.join('fund_pool_mf_all10k.pkl'[:-4].split('_')[3:]), pd.ExcelWriter(datetime.today().strftime('%Y%m%d')+'_backtest_results_MF.xlsx')).to_excel('多因子结果.xlsx')
# %%
