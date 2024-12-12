#%%
import pandas as pd
import numpy as np
import sqlalchemy


# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')
# %%
def sql_query_for_fund(fund_code):
    query= '''
        SELECT
        all_funds.产品名称,
        成立日,
        净值日期,
        DATEDIFF(净值日期, 成立日 ) AS 运行天数,
        累计净值,
        沪深300序列.close AS 沪深300净值,
        上证综指序列.close AS 上证综指净值,
        中证1000序列.close AS 中证1000净值,
        中证全债序列.close AS 中证全债净值
                
        FROM
	        (SELECT
	        备案编码,
	        产品名称,
	        成立日 
            FROM
	        存续基金要素 UNION
            SELECT
	        备案编码,
	        产品名称,
	        成立日 
            FROM
	        专户基金要素
            ) all_funds
        , 净值序列, 沪深300序列, 上证综指序列, 中证全债序列, 中证1000序列
        
        WHERE 
	    备案编码 = 净值序列.产品代码 AND
	    净值序列.净值日期 = 沪深300序列.date AND
        净值序列.净值日期 = 中证全债序列.date AND
        净值序列.净值日期 = 上证综指序列.date AND
        净值序列.净值日期 = 中证1000序列.date AND
        备案编码 = '%s' AND
        净值序列.净值日期 < '2021-01-01'
    
        ORDER BY
        净值日期 ASC
        ''' % fund_code
    #rint('letter is', query[289])
    return pd.read_sql(
        query,
        con=engine)
#%%
def get_fund_info_for(fund_code):
    
    fund_series = sql_query_for_fund(fund_code)
    fund_name = list(fund_series['产品名称'])[0]
    
    for benchmark in ['沪深300净值', '上证综指净值', '中证全债净值', '中证1000净值']:
        fund_series[benchmark] = fund_series[benchmark].array/list(fund_series[benchmark])[0]
    if '多元' in fund_name:
        fund_series['业绩基准'] = fund_series['沪深300净值'].array*0.3 +fund_series['中证全债净值'].array*0.7
        fund_series = fund_series.drop(['沪深300净值', '上证综指净值', '中证全债净值', '中证1000净值'], axis =1 )
    elif '先锋' in fund_name:
        fund_series['业绩基准'] = fund_series['中证1000净值']
        fund_series = fund_series.drop(['沪深300净值', '上证综指净值', '中证全债净值', '中证1000净值'], axis =1 )
    elif any([name in fund_name for name in ['星耀', '新经济']]):
        fund_series = fund_series.drop(['中证全债净值', '中证1000净值'], axis =1 )
    else: #核心系列及老产品
        fund_series['业绩基准'] = fund_series['沪深300净值'].array*0.6 +fund_series['中证全债净值'].array*0.4
        fund_series = fund_series.drop(['沪深300净值', '上证综指净值', '中证全债净值', '中证1000净值'], axis =1 )

    net_value = list(fund_series['累计净值'])
    if any([name in fund_name for name in ['星耀', '新经济']]):
        sh_value =  list(fund_series['上证综指净值'])
        csi300_value = list(fund_series['沪深300净值'])
    else:
        bench_value = list(fund_series['业绩基准'])
    
    weekly_pl = [0]

    for i in range(1, len(net_value)): weekly_pl.append(net_value[i]/net_value[i-1]-1)
    fund_series['产品周收益率'] = weekly_pl
    
    if any([name in fund_name for name in ['星耀', '新经济']]):
        max_drawdown = [0]
        sh_max_drawdown = [0]
        csi300_max_drawdown = [0]
        for i in range(1, len(net_value)):
            max_value = max(net_value[0:i])
            if net_value[i]<max_value:
                max_drawdown.append(net_value[i]/max_value-1)
            else:
                max_drawdown.append(0)

            sh_max_value = max(sh_value[0:i])
            if sh_value[i]<sh_max_value:
                sh_max_drawdown.append(sh_value[i]/sh_max_value-1)
            else:
                sh_max_drawdown.append(0)
            
            csi300_max_value = max(csi300_value[0:i])
            if csi300_value[i]<csi300_max_value:
                csi300_max_drawdown.append(csi300_value[i]/csi300_max_value-1)
            else:
                csi300_max_drawdown.append(0)
            

            
    else:     
        max_drawdown = [0]
        bench_max_drawdown = [0]
        for i in range(1, len(net_value)):
            max_value = max(net_value[0:i])
            if net_value[i]<max_value:
                max_drawdown.append(net_value[i]/max_value-1)
            else:
                max_drawdown.append(0)

            bench_max_value = max(bench_value[0:i])
            if bench_value[i]<bench_max_value:
                bench_max_drawdown.append(bench_value[i]/bench_max_value-1)
            else:
                bench_max_drawdown.append(0)

   

    fund_series['最大连续回撤'] = max_drawdown

    if any([name in fund_name for name in ['星耀', '新经济']]):
        fund_series['上证综指最大连续回撤'] = sh_max_drawdown
        fund_series['沪深300最大连续回撤'] = csi300_max_drawdown
    else:
        fund_series['业绩基准最大连续回撤'] = bench_max_drawdown
    #fund_series
    
    fund_summary = pd.DataFrame({
        '产品名称': [list(fund_series['产品名称'])[0]],
        '成立日': [list(fund_series['成立日'])[0]],
        '更新日期': [list(fund_series['净值日期'])[-1]],
        '运行天数': [list(fund_series['运行天数'])[-1]],
        '最新净值': [list(fund_series['累计净值'])[-1]],
        '周收益': [list(fund_series['累计净值'])[-1]/list(fund_series['累计净值'])[-2]-1 if len(fund_series['累计净值'])>1 else np.nan],
        '年化收益': [(list(fund_series['累计净值'])[-1]/1)**(365/(list(fund_series['运行天数'])[-1]))-1],
        '成立至今收益': [(list(fund_series['累计净值'])[-1]/1-1)],
        '最大连续回撤': [min(list(fund_series['最大连续回撤']))],
        '年化波动率': [np.std(list(fund_series['产品周收益率']))*(52**0.5)],
    })
    #print(list(fund_series['净值日期'])[-1])

    #无风险收益假设0
    fund_summary['夏普比率'] = [(list(fund_summary['年化收益'])[0]-0.0)/list(fund_summary['年化波动率'])[0] if list(fund_summary['年化波动率'])[0] != 0 else np.nan]
    fund_summary['卡玛比率'] = [abs(list(fund_summary['年化收益'])[0]/list(fund_summary['最大连续回撤'])[0]) if list(fund_summary['最大连续回撤'])[0] != 0 else np.nan]
    
    return fund_summary, fund_series

# %%
'''
#pyecharts可视化库 integration with html

import pyecharts
import pyecharts.options as opts
from pyecharts.charts import Line
plot_max_value = max(list(fund_series['上证综指净值']) + list(fund_series['累计净值']))
plot_min_value = min(list(fund_series['上证综指净值']) + list(fund_series['累计净值']))
plot_range = (max(abs(plot_max_value),abs(plot_min_value)))
(Line(init_opts=opts.InitOpts(width="400px", height="300px"))
    .add_xaxis(xaxis_data=list(fund_series['净值日期']))
    .add_yaxis(
        series_name="核心价值",
        y_axis=list(fund_series['累计净值']),
        #areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        linestyle_opts=opts.LineStyleOpts(width=3),
        label_opts=opts.LabelOpts(is_show=False),
    )
    .add_yaxis(
        series_name="上证综指",
        y_axis=fund_series['上证综指净值'],
        #yaxis_index=1,
        #areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        linestyle_opts=opts.LineStyleOpts(width=3,type_='dashed'),
        label_opts=opts.LabelOpts(is_show=False),
        
    )
    .add_yaxis(
        series_name="最大回撤",
        y_axis=fund_series['最大连续回撤'],
        yaxis_index=1,
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        linestyle_opts=opts.LineStyleOpts(),
        label_opts=opts.LabelOpts(is_show=False),
    )
    .extend_axis(
        yaxis=opts.AxisOpts(
            name="最大回撤",
            name_location="start",
            type_="value",
            max_=0,
            min_=-0.05,
            #is_inverse=True,
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        )
    )
    .set_global_opts(
        #tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        legend_opts=opts.LegendOpts(pos_left="center"),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(name="净值", type_="value", max_='dataMax', min_='dataMin'),
    )
    .set_series_opts(
        axisline_opts=opts.AxisLineOpts(),
    )
    .render("rainfall.html")
)
'''
# %%