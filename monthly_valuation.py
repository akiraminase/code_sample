#%%
import pandas as pd
import pyecharts
import pyecharts.options as opts
import sqlalchemy

pyecharts.globals._WarningControl.ShowWarning = False

# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')

# %%
def sql_query_for_fund(fund_code):
    query= '''
        SELECT
	        专户基金要素.产品名称,
	        备案编码,
	        基金经理,
	        成立日,
	        日期,
	        产品净值,
	        科目名称,
	        证券代码,
	        市值占比,
            单位成本,
	        行情,
            (行情/单位成本 -1)  AS 投资至今盈亏
        
        FROM
	        专户基金要素
	        INNER JOIN 月末估值表 ON 备案编码 = 产品代码

        WHERE
	        备案编码 = '%s'

        ORDER BY
	        产品代码 ASC, 日期 ASC
        ''' % fund_code
        
    return pd.read_sql(
        query,
        con=engine)
# %%
fund_valuation = sql_query_for_fund('SLR051')
# %%
holding = fund_valuation.loc[pd.notnull(fund_valuation['证券代码'])].drop(['产品名称', '备案编码', '基金经理'], axis = 1)
holding['成立日'] = pd.to_datetime(holding['成立日'])
holding['日期'] = pd.to_datetime(holding['日期'])
# %%
this_month_holding = holding.loc[holding['日期'] >= pd.to_datetime('2020-11')]
previous_holding = holding.loc[holding['日期'] < pd.to_datetime('2020-11')]
holding_pl = []
for security_code, this_month_price in zip(list(this_month_holding['证券代码']),list(this_month_holding['行情'])):
    try:
        holding_pl.append(this_month_price/list(previous_holding.loc[previous_holding['证券代码']==security_code]['行情'])[-1]-1)
    except:
        holding_pl.append(pd.NA)
this_month_holding['本月盈亏'] = holding_pl
this_month_holding
# %%
holding_pl = this_month_holding[['科目名称','本月盈亏', '市值占比']].set_index('科目名称')
# %%
from pyecharts.charts import Pie

sizes = list(this_month_holding['市值占比'])+[(1-sum(list(this_month_holding['市值占比'])))]
labels = [name.split('私募证券投资基金')[0] for name in list(this_month_holding['科目名称'])]+['现金及其他']
(Pie(init_opts=opts.InitOpts(width="800px", height="800px"))
    .add(
    series_name="本月持仓",
    data_pair=[list(z) for z in zip(labels, sizes)],
    label_opts=opts.LabelOpts(is_show=True, position="inside", font_size =30, formatter='{d}%')
    )
    .set_global_opts(legend_opts = opts.LegendOpts(is_show=True, pos_top = 'bottom', 
                    #toolbox_opts=opts.ToolboxOpts(),
                    textstyle_opts=opts.TextStyleOpts(font_size=23)))
    .render('本月持仓.html')
)
# %%
from pyecharts.charts import Bar

(Bar(init_opts=opts.InitOpts(width="1000px", height="800px"))
.add_xaxis([name.split('私募证券投资基金')[0] for name in list(this_month_holding['科目名称'])])
.add_yaxis('本月盈亏',[round(decimal*100,2) for decimal in list(this_month_holding['本月盈亏'])])
.reversal_axis()
.set_series_opts(label_opts=opts.LabelOpts(position="inside", formatter='{b}: {c}%', font_size=26))
.set_global_opts(legend_opts = opts.LegendOpts(is_show=False), 
                #toolbox_opts=opts.ToolboxOpts(),
                #textstyle_opts=opts.TextStyleOpts(font_size=23),
                xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="{value}%", font_size=23)),
                yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)))
.render("本月盈亏.html")
)
# %%

# %%
