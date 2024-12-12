#%%
import pandas as pd
import numpy as np
import sqlalchemy
from fund_info_query import get_fund_info_for
from time_point_holding import get_time_point_holding_of
from datetime import datetime, timedelta
import time

start_time = time.perf_counter()
#%%
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
zipped_dfs = []
holding_dfs = []
summary_df = pd.DataFrame()
for fund_code in code_list:
    if pd.notnull(fund_code):
        try:
            fund_summary, fund_detail = get_fund_info_for(fund_code)
            #print(single_df)
            zipped_dfs.append(fund_detail)
            holding_dfs.append(get_time_point_holding_of(fund_code))
            summary_df = summary_df.append(fund_summary)
        except Exception as e:
            #print(fund_code)
            print(e, fund_code)
            zipped_dfs.append(pd.DataFrame({'产品代码': [fund_code]}))
            holding_dfs.append(pd.DataFrame({'产品代码': [fund_code]}))
            summary_df = summary_df.append(pd.DataFrame({'产品代码': [fund_code]}))

# %%
hxjz = summary_df.loc[summary_df['产品名称'].str.contains('核心|睿泰')].set_index(['产品名称'])
dypz = summary_df.loc[summary_df['产品名称'].str.contains('多元')].set_index(['产品名称'])
xfdl = summary_df.loc[summary_df['产品名称'].str.contains('先锋')].set_index(['产品名称'])
cxcp = summary_df.loc[summary_df['成立日'] < datetime.date(datetime(2020,1,1))].set_index(['产品名称'])
#%%
def plot_series(workbook, fund_detail, width, length):
    
    row_end = len(list(fund_detail['产品名称']))+2
    fund_name = list(fund_detail['产品名称'])[0]

    if any([name in fund_name for name in ['星耀', '新经济']]):
        maxi = max(list(fund_detail['累计净值'])+list(fund_detail['上证综指净值'])+list(fund_detail['沪深300净值'])+[1.3])
        mini = min(list(fund_detail['累计净值'])+list(fund_detail['上证综指净值'])+list(fund_detail['沪深300净值'])+[0.9])
    else:   
        maxi = max(list(fund_detail['累计净值'])+list(fund_detail['业绩基准'])+[1.3])
        mini = min(list(fund_detail['累计净值'])+list(fund_detail['业绩基准'])+[0.9])

    chart1 = workbook.add_chart({'type': 'line'})
    chart2 = workbook.add_chart({'type': 'area'})
    if '实创天成' not in fund_name and '新经济' not in fund_name:
        chart1.add_series({
            'name':       '%s(调整后)' %fund_name,
            'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
            'values':     '=%s!$AA$3:$AA$%d' %(fund_name, row_end),
            'line':      {'color': '#C0504D'}
        })
    else:
        chart1.add_series({
            'name':       '%s' %fund_name,
            'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
            'values':     '=%s!$D$3:$D$%d' %(fund_name, row_end),
            'line':      {'color': '#C0504D'}
        })
    if any([name in fund_name for name in ['星耀', '新经济']]):
        #print(fund_name, '非加权')
        chart1.add_series({
            'name':       '沪深300',
            'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
            'values':     '=%s!$E$3:$E$%d' %(fund_name, row_end),
            'line':      {'color': '#4F81BD', 'dash_type': 'dash'}
        })
        chart1.add_series({
            'name':       '上证综指',
            'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
            'values':     '=%s!$F$3:$F$%d' %(fund_name, row_end),
            'line':      {'color': '#F79646', 'dash_type': 'dash'}
        })
    else: 
        #print('使用加权业绩基准')
        chart1.add_series({
            'name':       '业绩基准',
            'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
            'values':     '=%s!$E$3:$E$%d' %(fund_name, row_end),
            'line':      {'color': '#4F81BD', 'dash_type': 'dash'}
        })
    if any([name in fund_name for name in ['星耀', '新经济']]):
        drawdown_col = 'H'
    else:
        drawdown_col = 'G'
        
    chart2.add_series({
        'name':       '最大连续回撤',
        'categories': '=%s!$B$3:$B$%d' %(fund_name, row_end),
        'values':     '=%s!$%s$3:$%s$%d' %(fund_name, drawdown_col,drawdown_col,row_end),
        'fill':      {'color': '#D3D3D3'},
        'y2_axis': True
    })
    chart2.set_y2_axis({'name': '最大连续回撤', 'min': -0.05, 'max':0})
    
    chart1.combine(chart2)
    chart1.set_title ({'name': '%s净值走势' % fund_name})
    chart1.set_x_axis({'name': '日期'})
    chart1.set_y_axis({'name': '累计单位净值', 'min': mini, 'max': maxi})

    chart1.set_legend({'position': 'bottom'})
    chart1.set_size({'width': width, 'height': length})
    
    return chart1

def plot_pie(workbook, fund_holding, row):
    chart = workbook.add_chart({'type': 'pie'})
    chart.add_series({
        'name':       '%s' %(list(fund_holding['产品名称'])[0]),
        'categories': '=持仓情况汇总!$A$%s:$A$%s' %(str(row+2), str(row+len(fund_holding)+1)),
        'values':     '=持仓情况汇总!$H$%s:$H$%s' %(str(row+2), str(row+len(fund_holding)+1)),
        'data_labels': {'value': True, 'position': 'center'}
    })
    chart.set_title({'name': '%s持仓情况'%(list(fund_holding['产品名称'])[0])})
    chart.set_size({'width': 800, 'height': 300})
    #chart.set_legend({'none': True})

    return chart

#%%
with pd.ExcelWriter('%s产品综合台账.xlsx'%datetime.today().strftime('%Y%m%d')) as writer:
    workbook = writer.book
    percentage_format = workbook.add_format({'num_format': '0.00%', 'align': 'center'})
    decimal_format = workbook.add_format({'num_format': '0.0000', 'align': 'center'})
    currency_format = workbook.add_format({'num_format': '#,##0.00'})
    
    row_step = max([len(df) for df in holding_dfs])+3
    row = 1
    new_high_cnt = 0
    #产品信息概览
    for product_name, product_data in zip(['核心价值系列', '多元配置系列', '先锋动力系列','存续产品'], [hxjz,dypz,xfdl,cxcp]):
        product_data.to_excel(writer, sheet_name=product_name, startrow = 1)
        workbook  = writer.book
        worksheet = writer.sheets[product_name]
        title_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'border':1})
        worksheet.merge_range('A1:L1', product_name, title_format)
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'align': 'center',
            'valign': 'top',
            'fg_color': '#95BBD5',
            'border': 1})
        for col_num, value in enumerate(product_data.columns.values):
            worksheet.write(1, col_num + 1, value, header_format)

        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:E', 12, workbook.add_format({'align': 'center'}))
        worksheet.set_column('F:J', 14, percentage_format)
        worksheet.set_column('K:L', 10, decimal_format)

    for fund_detail, fund_holding in zip(zipped_dfs, holding_dfs):
        fund_name = list(fund_detail['产品名称'])[0]
        #持仓情况汇总
        fund_holding.drop(['产品名称'], axis = 1).set_index(['持仓名称']).to_excel(writer, sheet_name='持仓情况汇总', startrow = row, startcol = 0)
        
        if (list(fund_detail['运行天数'])[-1] >= 90) and (list(fund_detail['累计净值'])[-1] == max(list(fund_detail['累计净值']))) and '实创天成' in fund_name:
            new_high_cnt +=1

        worksheet = writer.sheets['持仓情况汇总']
        worksheet.write(row-1, 0, fund_name, workbook.add_format({'bold': True, 'font_color': 'red'}))
        worksheet.set_column('A:A', 40)
        worksheet.set_column('I:I', 12)
        worksheet.set_column('E:E', 12, percentage_format)
        worksheet.set_column('H:H', 12, percentage_format)
        worksheet.set_column('G:G', 15, currency_format)
        worksheet.set_column('D:D', 15, currency_format)
        worksheet.set_column('J:K', 15, currency_format)
        worksheet.set_column('L:M', 12, percentage_format)
        
        worksheet.conditional_format('L%s:M%s' %(str(row+2),str(row+len(fund_holding)+1)), {'type': 'data_bar', 'bar_color': 'red'})
        if len(fund_holding) > 0:
            worksheet.insert_chart('O%s' %str(row), plot_pie(workbook, fund_holding, row))
        row += row_step

        #详情页面
        
        fund_detail.drop(['产品名称', '成立日'], axis = 1).to_excel(writer, sheet_name=fund_name, startrow = 1)
        worksheet = writer.sheets[fund_name]
        title_format = workbook.add_format({
            'bold': True,
            'align': 'center',
            'border':1})
        worksheet.set_column('B:B', 10)
        worksheet.merge_range('A1:H1', fund_name, title_format)
        worksheet.set_column('E:F', 12, decimal_format)
        if any([name in fund_name for name in ['星耀', '新经济']]):
            worksheet.set_column('G:K', 20, percentage_format)
            worksheet.insert_chart('K3', plot_series(workbook, fund_detail, 600, 400))
            fund_holding.drop(['产品名称'], axis = 1).set_index(['持仓名称']).to_excel(writer, sheet_name=fund_name, startrow = 24, startcol = 10)
            worksheet.set_column('T:U', 15, currency_format)
            worksheet.set_column('V:W', 12, percentage_format)
            worksheet.conditional_format('V1:W50', {'type': 'data_bar', 'bar_color': 'red'})
            worksheet.set_column('K:K', 30)
            worksheet.set_column('O:O', 12, percentage_format)
            worksheet.set_column('R:R', 12, percentage_format)
            worksheet.set_column('N:N', 15, currency_format)
            worksheet.set_column('Q:Q', 15, currency_format)
            worksheet.set_column('S:S', 12)
        else:
            worksheet.set_column('F:H', 20, percentage_format)
            worksheet.insert_chart('I3', plot_series(workbook, fund_detail, 600, 400))
            fund_holding.drop(['产品名称'], axis = 1).set_index(['持仓名称']).to_excel(writer, sheet_name=fund_name, startrow = 24, startcol = 8)
            worksheet.conditional_format('T1:U50', {'type': 'data_bar', 'bar_color': 'red'})
            worksheet.set_column('R:S', 15, currency_format)
            worksheet.set_column('T:U', 12, percentage_format)
            worksheet.set_column('I:I', 30)
            worksheet.set_column('M:M', 12, percentage_format)
            worksheet.set_column('P:P', 12, percentage_format)
            worksheet.set_column('L:L', 15, currency_format)
            worksheet.set_column('O:O', 15, currency_format)
            worksheet.set_column('Q:Q', 12)
        if '新经济' not in fund_name and '实创天成' not in fund_name: 
            pd.DataFrame({'调整后净值': fund_detail['累计净值'].array/list(fund_detail['累计净值'])[0]}).to_excel(writer, sheet_name=fund_name, startrow = 1, startcol= 25)
        
        print(fund_name+' 处理完成')
print(f"总运行时间: {time.perf_counter() - start_time:0.4f}s")
#print('创历史新高数量',new_high_cnt) 
# %%
