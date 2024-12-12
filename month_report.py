#%%
import pandas as pd
import numpy as np
import sqlalchemy
from fund_info_query import get_fund_info_for
#from time_point_holding import get_time_point_holding_of
from datetime import datetime, timedelta
import time
# %%
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')
query= '''
        SELECT
	        备案编码
        FROM
	        ( SELECT 备案编码, 成立日 FROM 存续基金要素 UNION SELECT 备案编码, 成立日 FROM 专户基金要素 ) all_fund
	    INNER JOIN 净值序列 ON 备案编码 = 产品代码 
        WHERE
	        成立日 IS NOT NULL 
	        AND 成立日 > '2000-01-01' 
        GROUP BY
	        产品代码 
        ORDER BY
	        成立日 ASC,
	        净值序列.产品名称 ASC
        ''' 
        
code_list = list(pd.read_sql(query, con=engine)['备案编码'])
# %%
def get_monthly_holding(fund_code):
    query = '''
    SELECT
	a2.产品代码,
	a2.科目名称,
    a2.科目代码,
	a2.单位成本,
	上月行情,
	本月行情,
	IF(ISNULL(上月行情), 本月行情/a2.单位成本-1, 本月行情 / 上月行情 - 1) AS 本月涨跌幅,
	a2.`市值占比` AS 持仓占比,
    上月净值,
    本月净值,
    本月净值/上月净值-1 AS 产品本月收益率,
	a2.日期 AS 统计结束日 
    FROM
	( SELECT *,行情 AS 上月行情, 产品净值 AS 上月净值 FROM `月末估值表` WHERE `日期` = '2020-11-30' AND `产品代码` = '%s' ) a
	RIGHT JOIN ( SELECT * ,行情 AS 本月行情, 产品净值 AS 本月净值 FROM `月末估值表` WHERE `日期` = '2020-12-31' AND `产品代码` = '%s' ) a2 ON a.`科目代码` = a2.`科目代码`
    ''' %(fund_code, fund_code)

    return pd.read_sql(
        query,
        con=engine)
get_monthly_holding('SJY505')
# %%
zipped_dfs = []
holding_dfs = []
summary_df = pd.DataFrame()
for fund_code in code_list:
    if pd.notnull(fund_code):
        try:
            fund_summary, fund_detail = get_fund_info_for(fund_code)
            #print(single_df)
            zipped_dfs.append(fund_detail)
            holding_dfs.append(get_monthly_holding(fund_code))
            summary_df = summary_df.append(fund_summary)
        except Exception as e:
            #print(fund_code)
            print(e, fund_code)
            zipped_dfs.append(pd.DataFrame({'产品代码': [fund_code]}))
            holding_dfs.append(pd.DataFrame({'产品代码': [fund_code]}))
            summary_df = summary_df.append(pd.DataFrame({'产品代码': [fund_code]}))
# %%
def plot_pie(workbook, fund_holding, fund_name):
    chart = workbook.add_chart({'type': 'pie'})
    chart.add_series({
        'categories': '=%s!$C$%s:$C$%s' %(fund_name, str(5), str(5+len(fund_holding)-1)),
        'values':     '=%s!$I$%s:$I$%s' %(fund_name, str(5), str(5+len(fund_holding)-1)),
        'data_labels': {'value': True, 'position': 'center'}
    })
    #chart.set_title({'name': '%s持仓情况'%(list(fund_holding['产品名称'])[0])})
    chart.set_size({'width': 800, 'height': 300})

    return chart

def plot_bar(workbook, fund_holding, fund_name):
    chart = workbook.add_chart({'type': 'bar'})
    chart.add_series({
        'categories': '=%s!$C$%s:$C$%s' %(fund_name, str(5), str(5+len(fund_holding)-1)),
        'values':     '=%s!$H$%s:$H$%s' %(fund_name, str(5), str(5+len(fund_holding)-1)),
        'data_labels': {'value': True, 'position': 'center'}
    })
    #chart.set_title({'name': '%s持仓情况'%(list(fund_holding['产品名称'])[0])})
    chart.set_size({'width': 800, 'height': 300})

    return chart

def plot_series(workbook, fund_detail, row):
    row_end = len(list(fund_detail['产品名称']))+row-1
    fund_name = list(fund_detail['产品名称'])[0]

    maxi = max(list(fund_detail['累计净值'])+list(fund_detail['上证综指净值'])+[1.3])
    mini = min(list(fund_detail['累计净值'])+list(fund_detail['上证综指净值'])+[0.9])

    chart1 = workbook.add_chart({'type': 'line'})
    chart2 = workbook.add_chart({'type': 'area'})
    chart1.add_series({
            'name':       '%s' %fund_name.replace('实创天成',''),
            'categories': '=%s!$D$%d:$D$%d' %(fund_name, row, row_end),
            'values':     '=%s!$F$%d:$F$%d' %(fund_name, row, row_end),
        'line':      {'color': '#C0504D'}
        })
    chart1.add_series({
            'name':       '上证综指',
            'categories': '=%s!$D$%d:$D$%d' %(fund_name, row, row_end),
            'values':     '=%s!$I$%d:$I$%d' %(fund_name, row,row_end),
            'line':      {'color': '#4F81BD', 'dash_type': 'dash'}
        })
    drawdown_col = 'J'
        
    chart2.add_series({
        'name':       '最大连续回撤',
        'categories': '=%s!$D$%d:$D$%d' %(fund_name, row,row_end),
        'values':     '=%s!$%s$%d:$%s$%d' %(fund_name, drawdown_col,row,drawdown_col,row_end),
        'fill':      {'color': '#D3D3D3'},
        'y2_axis': True
    })
    chart2.set_y2_axis({'name': '最大连续回撤', 'min': -0.05, 'max':0})
    
    chart1.combine(chart2)
    #chart1.set_title ({'name': '%s净值走势' % fund_name})
    chart1.set_x_axis({'name': '日期'})
    chart1.set_y_axis({'name': '累计单位净值', 'min': mini, 'max': maxi})

    chart1.set_legend({'position': 'bottom'})
    chart1.set_size({'width': 600, 'height': 400})
    
    return chart1

# %%

with pd.ExcelWriter('%s月报数据.xlsx'%datetime.today().strftime('%Y%m%d')) as writer:
    sh = pd.read_sql('SELECT date AS 净值日期, close AS 上证综指净值 FROM 上证综指序列',con=engine)
    sh_1130_raw = list(pd.read_sql(
        'SELECT close FROM 上证综指序列 WHERE date = \'2020-11-30\'',
        con=engine)['close'])[0]
    for fund_detail, fund_holding in zip(zipped_dfs, holding_dfs):
        #row = 4
        sh_1130=sh_1130_raw
        fund_name = list(fund_detail['产品名称'])[0]
        if '实创天成' not in fund_name: continue
        fund_detail = pd.merge(fund_detail, sh, how='left', on= '净值日期').drop(['业绩基准', '业绩基准最大连续回撤'], axis=1)
        print('前', sh_1130)

        this_df = summary_df.loc[summary_df['产品名称'] == fund_name]
        sh_max_drawdown = [0]
        sh_value = list(fund_detail['上证综指净值'])

        
        sh_1130 = sh_1130/list(fund_detail['上证综指净值'])[0]
        print(sh_1130)
        fund_detail['上证综指净值'] = np.array(fund_detail['上证综指净值'])/list(fund_detail['上证综指净值'])[0]

        #if this_df['成立日'][0] > datetime.date(datetime(2020,11,30,0,0)): sh_1130 = fund_detail['上证综指净值'][0]
        
        for i in range(1, len(sh_value)):
            sh_max_value = max(sh_value[0:i])
            if sh_value[i]<sh_max_value:
                sh_max_drawdown.append(sh_value[i]/sh_max_value-1)
            else:
                sh_max_drawdown.append(0)
        this_df['上证综指最大连续回撤'] = min(sh_max_drawdown)
        this_df['上证综指今年收益'] = sh_value[-1]/sh_value[0]-1
        
        this_df['上证综指本月收益'] = sh_value[-1]/sh_1130-1
        if this_df['成立日'][0] > datetime.date(datetime(2020,11,30,0,0)): this_df['上证综指本月收益'] = this_df['上证综指今年收益'][0]
        fund_detail['上证综指最大连续回撤'] = sh_max_drawdown
        if (float(list(fund_detail['累计净值'])[-1]) != float(list(fund_holding['本月净值'])[0])):
            print(fund_name, '净值数据不一致， 请确认分红')

        this_df.to_excel(writer, sheet_name = fund_name, startrow=0)
        fund_holding.to_excel(writer, sheet_name = fund_name, startrow=3)
        fund_detail.to_excel(writer, sheet_name = fund_name, startrow=5+len(fund_holding))

        workbook = writer.book
        worksheet = writer.sheets[fund_name]

        percentage_format = workbook.add_format({'num_format': '0.00%', 'align': 'center'})
        worksheet.set_column('H:I', 12, percentage_format)

        worksheet.insert_chart('M4', plot_pie(workbook, fund_holding, fund_name))
        worksheet.insert_chart('M20', plot_bar(workbook, fund_holding, fund_name))
        worksheet.insert_chart('M36', plot_series(workbook, fund_detail, 7+len(fund_holding)))

        
        
        
        
        
# %%

# %%

# %%
