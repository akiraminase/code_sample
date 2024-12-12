#!/usr/bin/env python
"""
risk_models.py:
事前风控模块，用于在筛选前剔除有明显问题的基金数据
"""
#%%
import pandas as pd
from typing import List
# %%
def get_abnormal_products_by_nav_change(nav_df: pd.DataFrame)->List[str]:
    """根据净值变化设定基金池黑名单

    Args:
        nav_df (pd.DataFrame): 原始基金池

    Returns:
        List[str]: 数据出错（非基金本身问题）的可能性较大的基金名称列表
    """    
    black_list=[]
    return_df = nav_df.pct_change()
    for _, row in return_df.iterrows():
        #简单地用净值周度变化绝对值大于30%过滤
        black_list = list(set(black_list))+list(row.loc[row.abs()>(0.3)].index)
    #print(len(black_list), 'removed due to abnormal nav')
    return black_list

def get_null_products(nav_df: pd.DataFrame)->List[str]:
    black_list=[]
    for col, series in nav_df.iteritems():
        #如果无可用数据或可用数据无变化
        if len(series.dropna()) == 0 or series.dropna().std() == 0:
            black_list = list(set(black_list))+list([col])
    #print(len(black_list), 'removed due to null data')
    return black_list

#施工中
def get_incomplete_products(nav_df: pd.DataFrame, min_period, min_completeness = 0.7)->List[str]:
    black_list=[]
    for col, series in nav_df.iteritems():
        #to do: make this work
        if len(series.dropna()) <= min_period*4.3:
        #    print(min_period)
            black_list = list(set(black_list))+list([col])
            continue
        if int((series.dropna().index[-1]-series.dropna().index[0]).days/7)*min_completeness < len(series):
            black_list = list(set(black_list))+list([col])
            #print('wtf')
    print(len(black_list), 'removed due to incomplete')
    return black_list
#施工中
def get_wrong_date_products(nav_df: pd.DataFrame, fund_foundation_date: dict)->List[str]:
    black_list=[]
    for col, series in nav_df.iteritems():
        if series.dropna().index[0] >= fund_foundation_date[col]:
            black_list = list(set(black_list))+list([col])
    print(len(black_list), 'removed due to wrong date')
    return black_list