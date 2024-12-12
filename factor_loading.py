#!/usr/bin/env python
"""
factor_loadling.py:
因子库，包含因子的计算方法
"""
#%%
#from _typeshed import NoneType
import pandas as pd
import numpy as np
from pandas.core.reshape.merge import merge
import statsmodels.api as sm

from typing import Iterable, List, Tuple, Union
#设置可视化主题
import seaborn as sns
from statsmodels.multivariate import factor
sns.set_theme()
#显示中文
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = ['sans-serif']
plt.rcParams['font.sans-serif'] = ['SimHei']
#%%
CSI300 = pd.read_pickle('indices_data.pkl')['沪深300'].dropna()
#style_indices = pd.read_pickle('indices_data.pkl')['大盘成长', '大盘价值', '中盘成长', '中盘价值', '小盘成长', '小盘价值'].dropna()
# %%
def get_log_return(indexed_price_df: pd.DataFrame)->pd.DataFrame:
    """从价格序列计算对数收益率序列

    Args:
        indexed_price_df (pd.DataFrame): 带日期的价格序列/df

    Returns:
        pd.DataFrame: 带日期的对数收益率序列/df
    """    
    return np.log(1+indexed_price_df.pct_change()).dropna()
def get_nav_comparison_plot(asset_price_series: pd.Series, benchmark: pd.Series = CSI300):
    """简单绘制净值对比图
    Args:
        asset_price_series (pd.Series): 带日期的资产价格序列
        benchmark (pd.Series, optional): 带日期的基准资产价格序列. Defaults to CSI300.

    Returns:
        matplotlib.plot: 终端输出图片
    """    
    df = pd.merge(asset_price_series, benchmark, how='left', left_index=True, right_index=True)

    #价格净值化
    for i in range(0, len(df.columns)):
        df[df.columns[i]] = df[df.columns[i]]/df[df.columns[i]][0]
    return df.plot()
# %%
CSI300_log_return = get_log_return(CSI300)
#style_indices_return = np.log(1+style_indices.pct_change())
test_series = pd.read_pickle('long_fund_sanav.pkl')['九坤沪深300指数增强1号'].dropna()
#get_nav_comparison_plot(test_series)

#%%
'''以下皆为因子计算公式方法，一般由外部调用计算
'''
#风险调整收益因子
def get_sharpe(log_return_iterable: Iterable[float], r_f: float = 0.015, look_back_period: int = None) -> float:
    if look_back_period == None:
        pass
    else:
        log_return_iterable = log_return_iterable[look_back_period:]
        
    sharpe = (np.mean(pd.Series(log_return_iterable).dropna().values)*52-r_f)/(np.std(pd.Series(log_return_iterable).dropna().values)*np.sqrt(52))
    if np.isinf(sharpe) or np.isnan(sharpe):
        return np.nan
    else:
        return sharpe
def get_IR(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str, r_f:float=0.015)->float:
    #print(asset_name, benchmark_name)
    #print('-----', merged_df[[asset_name, benchmark_name]].dropna())
    df = merged_df[[asset_name, benchmark_name]].dropna()
    return (np.mean(df[asset_name].values*52-r_f)/(np.std(df[asset_name].values - df[benchmark_name].values)*np.sqrt(52)))
def get_beta(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str)->float:
    df = merged_df[[asset_name, benchmark_name]].dropna()
    return np.cov(np.array([df[asset_name].values, df[benchmark_name].values]))[0][1]/np.var(df[benchmark_name].values)
def get_jensen_alpha(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str, r_f:float=0.015)->float:
    df = merged_df[[asset_name, benchmark_name]].dropna()
    return np.mean(df[asset_name].values)*52-(r_f+get_beta(merged_df, asset_name, benchmark_name)*(np.mean(df[benchmark_name].values)-r_f))
def get_sortino():
    return None
def get_treynor(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str, r_f:float=0.015)->float:
    df = merged_df[[asset_name, benchmark_name]].dropna()
    return (np.mean(df[asset_name].values)*52-r_f)/get_beta(merged_df, asset_name, benchmark_name)
def get_maxdrawdown(nav_series:pd.Series)->float:
    #这里使用简单周间回报，方便截面可比，不做对数和复利处理
    #连续最大回撤
    #nav_series = (np.exp(log_return_indexed_series)+1).cumprod()
    nav_series = nav_series.values
    max_drawdown = [0]
    for i in range(1, len(nav_series)):
        max_value = max(nav_series[0:i])
        if nav_series[i]<max_value:
            max_drawdown.append(nav_series[i]/max_value-1)
        else:
            max_drawdown.append(0)
    #print(nav_series)
    return abs(min(max_drawdown))
def get_calmar(log_return_indexed_series:pd.Series)->float:
    nav_series = (np.exp(log_return_indexed_series.dropna())).cumprod()
    if get_maxdrawdown(nav_series) == 0: return np.nan
    return ((nav_series[-1]/nav_series[0])-1)/get_maxdrawdown(nav_series)
def get_weekly_winning_ratio(log_return_series: pd.Series)->float:
    return len(log_return_series.loc[log_return_series>0])/len(log_return_series)

#比较非传统的的测量方式
def get_C_L_results(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str, r_f:float=0.015)->Tuple[float]:
    #输出CL模型中牛市beta-熊市beta的差值-->择时能力
    """Chang-Lewellen模型

    Args:
        log_return_indexed_series (pd.Series): 资产日期索引对数收益率
        benchmark_log_return_indexed_series (pd.Series, optional): 市场日期索引对数收益率. Defaults to CSI300_log_return.
        r_f (float, optional): 年化无风险收益率. Defaults to 0.015.

    Returns:
        Tuple[float]: 
        1. CL模型alpha
        2. CL模型牛熊市beta差值
    """    
    df = merged_df[[asset_name, benchmark_name]].dropna()
    df.loc[df[benchmark_name]>=0.015/52, 'D1'] = 1
    df['D1'].fillna(0, inplace=True)
    df.loc[df['D1'] == 1, 'D2'] =0
    df['D2'].fillna(1, inplace=True)
    df['r_m-r_f'] = df[benchmark_name] - 0.015/52
    df['(r_m-r_f)*D1'] = df['r_m-r_f']*df['D1']
    df['(r_m-r_f)*D2'] = df['r_m-r_f']*df['D2']
    df['r_p-r_f'] = df[asset_name] - 0.015/52
    X = sm.add_constant(df[['(r_m-r_f)*D1','(r_m-r_f)*D2']])
    y = df['r_p-r_f']
    est = sm.OLS(y, X).fit()
    return est.params['const'], est.params['(r_m-r_f)*D1'], est.params['(r_m-r_f)*D2']
def get_H_M_results(merged_df: pd.DataFrame, asset_name: str, benchmark_name: str, r_f:float=0.015)->Tuple[float]:
    #华泰研报实证研究中使用的alpha
    """Henriksson-Merton模型, 如果第二个beta为正，则基金经理具有择时能力

    Args:
        log_return_indexed_series (pd.Series): 资产日期索引对数收益率
        benchmark_log_return_indexed_series (pd.Series, optional): 市场日期索引对数收益率. Defaults to CSI300_log_return.
        r_f (float, optional): 年化无风险收益率. Defaults to 0.015.

    Returns:
        Tuple[float]:
        1. HM模型alpha
        2. HM模型所测量的具有择时能力的beta
    """    
    df = merged_df[[asset_name, benchmark_name]].dropna()
    df.loc[df[benchmark_name]>=0.015/52, 'D'] = 1
    df['D'].fillna(0, inplace=True)
    df['r_m-r_f'] = df[benchmark_name] - 0.015/52
    df['(r_m-r_f)*D'] = df['r_m-r_f']*df['D']
    df['r_p-r_f'] = df[asset_name] - 0.015/52
    X = sm.add_constant(df[['r_m-r_f','(r_m-r_f)*D']])
    y = df['r_p-r_f']
    est = sm.OLS(y, X).fit()
    return est.params['const'], est.params['(r_m-r_f)*D']
def get_style_OLS_results(merged_df: pd.DataFrame, asset_name:str, style_indices_names: List[str])->Tuple[float, pd.Series]:
    """对各个风格指数做OLS回归，以回归后截距作为风格中性超额收益，各个变量的回归系数为风格暴露程度

        注：该方法为直接使用线性回归，与华泰研报中基于最小化残差平方和的威廉夏普指数不完全一样
    
    Args:
        merged_df (pd.DataFrame): 含有标的资产与风格指数收益率的合并后df
        asset_name (str): 标的资产在df中的名称
        style_indices_names (List[str]): 风格指数在df中的名称列表

    Returns:
        Tuple[float, pd.Series]: 
        1. 前文定义的风格中性超额收益
        2. 前文定义的与各个风格的因子暴露，按降序排序，如果取第一个值则为该方法测量出的最高风格因子暴露
    """    
    #风格中性后的alpha-->择券能力
    merged_df = merged_df[list([asset_name])+list(style_indices_names)].dropna()
    asset_series = merged_df[asset_name]
    X = sm.add_constant(merged_df[style_indices_names])
    y = asset_series
    try:
        est = sm.OLS(y, X).fit()
    except Exception as e:
        print(e , y)
        raise Exception('damn')
    return est.params['const'], est.params[1:].sort_values(ascending=False)
#风格漂移因子
def get_hurst(log_return_iterable):
    return None
def get_SDS(merged_df: pd.DataFrame, asset_name:str, style_indices_names: List[str], observation_period: str='Q')->float:
    """基于回归思想的SDS风格漂移指标计算实现，未使用原版威廉夏普风格指数

    Args:
        merged_df (pd.DataFrame): 含有标的资产与风格指数收益率的合并后df
        asset_name (str): 标的资产在df中的名称
        style_indices_names (List[str]): 风格指数在df中的名称列表
        observation_period (str, optional): 风格漂移观察周期，按照pd.DataFrane.resample()的格式传入str. Defaults to 'Q'.

    Returns:
        float: SDS风格漂移指数
    """    
    df = merged_df[list([asset_name])+list(style_indices_names)].copy()
    return df.resample(observation_period).apply(lambda sliced_df: get_style_OLS_results(sliced_df, asset_name, style_indices_names)[1].values.var()).sum()**(0.5)
#%%
#result = get_SDS(pd.merge(get_log_return(test_series), style_indices_return, how='left', left_index=True, right_index=True).dropna(), test_series.name, style_indices.columns)
#result
# %%
#get_C_L_results(get_log_return(test_series).dropna())
# %%
#np.exp(get_log_return(test_series)).cumprod().plot()

#%%
defined_factor_list = ['Sharpe_Ratio', 'Information_Ratio','Jensen_Alpha','Treynor_Ratio', 'Calmar_Ratio', 'Weekly_Winning_Ratio',
                       'CL_Alpha', 'CL_Beta_Diff', 'HM_Alpha', 'HM_Beta', 'Style_Neutral_Alpha','Max_Factor_Coef','SDS_Score']
# %%
import time
def get_all_defined_factors():
    """获取全部因子名称

    Returns:
        List[str]: 全部因子名称的列表
    """    
    return defined_factor_list
def get_factor_value(factor_name, asset_name, merged_df: pd.DataFrame = None, log_return_series: pd.Series = None, benchmark_name: Union[str, List[str]] = '沪深300')->float: 
    """定义string与方法的实际对应，方便批量获取因子数值

    Args:
        factor_name (str): 外部传入的因子名称，如果未明确定义则无法调用
        asset_name (str): 需要计算指标的资产名称
        merged_df (pd.DataFrame): 含有指数收益率的合并后的资产对数收益率df
        log_return_series (pd.Series): 对数收益率序列
        benchmark_name (Union(str, List[str])): 基准指数名称，单指数或多指数，默认沪深300为单指数

    Raises:
        ValueError: 如果merged_df和log_return_series都未传入，抛出错误
        ValueError: 如果因子未明确定义，抛出错误
        RuntimeError: 如果返回空值，输出警告

    Returns:
        float: 计算出的因子数值
    """      
    #start_time = time.perf_counter()
    #if type(merged_df) == pd.Series:
    #    log_return_series = merged_df
    #print(merged_df)
    if type(merged_df) == type(None) and type(log_return_series) == type(None):
        raise ValueError('Must Pass merged_df or log_return_series')
    if factor_name not in defined_factor_list: 
        raise ValueError('Factor Not Defined')

    if factor_name == 'Sharpe_Ratio': factor_value =  get_sharpe(log_return_series)
    elif factor_name == 'Information_Ratio': factor_value = get_IR(merged_df, asset_name, benchmark_name)
    elif factor_name == 'Jensen_Alpha': factor_value = get_jensen_alpha(merged_df, asset_name, benchmark_name)
    elif factor_name == 'Treynor_Ratio': factor_value = get_treynor(merged_df, asset_name, benchmark_name)
    elif factor_name == 'Calmar_Ratio': factor_value = get_calmar(log_return_series)
    elif factor_name == 'Weekly_Winning_Ratio': factor_value = get_weekly_winning_ratio(log_return_series)
    elif factor_name == 'CL_Alpha': factor_value = get_C_L_results(merged_df, asset_name, benchmark_name)[0]
    elif factor_name == ' CL_Beta_Diff':
        factor_value = get_C_L_results(merged_df, asset_name, benchmark_name)[1:]
        factor_value = factor_value[0] - factor_value[1]
    elif factor_name == 'HM_Alpha': factor_value = get_H_M_results(merged_df, asset_name, benchmark_name)[0]
    elif factor_name == 'HM_Beta': factor_value = get_H_M_results(merged_df, asset_name, benchmark_name)[1]
    elif factor_name == 'Style_Neutral_Alpha': factor_value = get_style_OLS_results(merged_df, asset_name, benchmark_name)[0]
    elif factor_name == 'Max_Factor_Coef': factor_value = get_style_OLS_results(merged_df, asset_name, benchmark_name)[1][0]
    elif factor_name == 'SDS_Score': factor_value = get_SDS(merged_df, asset_name, benchmark_name)
    else: factor_value = None

    if pd.isna(factor_value) ==True:
        try:
            #如果计算因子值为空，返回警告
            raise RuntimeError('Warning: returned NA as %s, please check input series'%factor_name)
        except Exception as e:
            print(e)
            print(log_return_series)
    #print('factor Completed in %1.10fs' %(time.perf_counter()-start_time))
    return factor_value
    
#%%
#get_factor_value('Sharpe_Ratio', (get_log_return(test_series)))
# %%
