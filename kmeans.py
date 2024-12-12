#!/usr/bin/env python
""" 
kmeans.py:
kmeans聚类相关方法库，对基金净值数据进行无监督式聚类的方法库
"""
#%%
import pandas as pd
import numpy as np

from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

import time
from typing import List
# %%
style_indices = pd.read_pickle('indices_data.pkl')[['巨潮大盘', '巨潮小盘', '国证成长', '国证价值']]#风格指数数据

''' 抛弃本方法
def get_log_return_ts_corr(asset_prices: pd.Series, market_prices: pd.Series)->float:
    """计算某资产与某指数之对数收益率的皮尔逊线性相关系数

    Args:
        asset_prices (pd.Series): 日期索引的资产价格序列
        market_prices (pd.Series): 日期索引的市场指数价格序列

    Returns:
        float: 皮尔逊线性相关系数
    """    
    asset_prices = asset_prices.squeeze()#.dropna().squeeze()
    market_prices = market_prices.squeeze()#.dropna().squeeze()

    #if type(asset_prices) == np.float64:
    #    return np.nan

    asset_prices.name = 'asset'
    market_prices.name = 'market'

    #log_return_ts = pd.merge(asset_prices, market_prices, how = 'left', left_index=True, right_index=True)
    #log_return_ts = np.log(1+log_return_ts.pct_change())
    #print(log_return_ts)
    return asset_prices.corr(market_prices)
'''
def get_stats_df(raw_df: pd.DataFrame)->pd.DataFrame:
    """通过原始价格数据计算每一个资产的统计特征

    Args:
        raw_df (pd.DataFrame): 日历索引的各个基金的价格序列

    Returns:
        pd.DataFrame: 各个基金的统计特征
    """    
    start_time = time.perf_counter()
    fund_stats = {}
    error = {}
    cnt=0
    big_raw_df = pd.merge(raw_df, style_indices, how='left', left_index=True,right_index=True)
    #big_raw_df = big_raw_df.dropna(0,how='any')
    big_raw_df = np.log(1+big_raw_df.pct_change()).dropna(axis=0, how='all')
    #print(big_raw_df)
    for col in raw_df.columns:
        log_return_series = big_raw_df[col]
        if len(log_return_series.dropna())<30 or log_return_series.dropna().std() == 0:
                continue
        try:
            stats_results=pd.Series(dtype='float64')

            for style in list(style_indices.columns):
                #stats_results[style] = get_log_return_ts_corr(log_return_series, big_raw_df[style])
                stats_results[style] = log_return_series.corr(big_raw_df[style])
            #if col == '得利宝-至尊7号': print(log_return_series.dropna())
            #临时方案 去除na
            #if True in (pd.isna(stats_results.values())): continue
            fund_stats[col] = stats_results
            #print(raw_df.index[-1], col, stats_results)
            #cnt+=1

            #if cnt==3:break
        except Exception as e:
            print(e)
            error[col] = 'Error: '+str(e)
    #pd.Series(error).to_excel('data_processing_error.xlsx')
    #print('corr calculation Elapsed time:', time.perf_counter()-start_time,'s')
    return pd.concat(fund_stats.values(), keys = fund_stats.keys(), axis=1).T
# %%
import plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from datetime import datetime
#%%
def save_plot(X_scaled: pd.DataFrame, stats: pd.DataFrame, date: datetime, path:str='C:\\Users\\gaoang\\Desktop\\backtest\\kmeans_results\\'):
    """输出某一时间截面的全部存续资产无监督聚类图片

    Args:
        X_scaled (pd.DataFrame): 包含kmeans聚类类别的标准化后数据
        stats (pd.DataFrame): 统计特征df
        date (datetime): 时间截面时间戳
        path (str, optional): 输出路径. Defaults to 'C:\\Users\\gaoang\Desktop\\backtest\\kmeans_results\\'.
    """    
    plotX = X_scaled

    processed_stats = pd.merge(plotX, stats, how='left', left_index=True,right_index=True)
    cluster = {}

    for i in range(0, plotX['Kmeans_cluster'].max()+1):
        cluster['kmeans_cluster_'+str(i)] = processed_stats[processed_stats['Kmeans_cluster']==i]

    traces = []
    colors = [
        'rgba(255, 128, 2, 0.8)',
        'rgba(255, 128, 255, 0.8)',
        'yellow', 'blue', 'red', 'purple', 'green','gray','brown'
    ]
    #二维特征
    x_axis_feature = '国证成长'
    y_axis_feature = '国证价值'

    for key, value in cluster.items():
        traces.append(
            go.Scatter(
                x = value[x_axis_feature],
                y = value[y_axis_feature],
                mode = "markers",
                name = key,
                marker = dict(color = colors[int(key[-1])]),
                text = value.index
            )

        )

    data = traces

    #图片标题
    title = "成长 vs 价值 "+datetime.strftime(date, '%Y-%m-%d')

    layout = dict(title = title,
                xaxis= dict(title= x_axis_feature,ticklen= 5,zeroline= False),
                yaxis= dict(title= y_axis_feature,ticklen= 5,zeroline= False)
                )

    fig = dict(data = data, layout = layout)

    #iplot(fig)
    filename = datetime.strftime(date, '%Y%m%d')+'.html'
    py.offline.plot(fig, filename=path+filename, auto_open=False)
#%%
def get_k_means(sliced_raw_df: pd.DataFrame, plot:bool=False, k_means_clusters: int=7, k_means_columns:List[str] = ['国证成长','国证价值','巨潮大盘','巨潮小盘'])->pd.DataFrame:
    """外部调用的方法，通过该方法对切分后的价格df进行无监督式聚类

    Args:
        sliced_raw_df (pd.DataFrame): 切分后的价格df，需要确保无未来数据
        plot (bool, optional): 是否输出画图. Defaults to False.
        k_means_columns (List[str], optional): 想要使用的风格指数名称，须保持与该文件的公用变量内存在的风格指数一致. Defaults to ['国证成长','国证价值','巨潮大盘','巨潮小盘'].

    Returns:
        pd.DataFrame: 由资产名索引的聚类数字类别
    """    
    
    stats = get_stats_df(sliced_raw_df)
    #stats
    #start_time = time.perf_counter()
    #尝试使用kmeans
    #k_means_columns = ['大盘成长', '大盘价值', '中盘成长', '中盘价值', '小盘成长', '小盘价值']
    X = pd.DataFrame(stats[k_means_columns])
    X_scaled = pd.DataFrame(StandardScaler().fit_transform(X))
    X_scaled.columns = [var+'_scaled' for var in X]
    X_scaled.index = X.index
    X_scaled = X_scaled.dropna()

    #如果需要调整参数请在这里面调，为确保安全目前不可由传入参数更改，请按需修改
    kmeans = KMeans(n_clusters=k_means_clusters)
    kmeans.fit(X_scaled)
    KMeans(algorithm='auto', copy_x=True, init='k-means++', max_iter=300,
        n_clusters=k_means_clusters, n_init=10, n_jobs=4, precompute_distances='auto',
        random_state=None, tol=0.0001, verbose=0)
    X_scaled['Kmeans_cluster'] = kmeans.predict(X_scaled)

    if plot == True:
        save_plot(X_scaled, stats, sliced_raw_df.index[-1])
    #print('kmeans Completed in %1.10fs' %(time.perf_counter()-start_time))
    return X_scaled['Kmeans_cluster'].copy()



# %%
