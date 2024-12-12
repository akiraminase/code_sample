#%%
import pandas as pd
import numpy as np
import os
# %%
def extract_name(filename):
    if '_' in filename:
        return filename.split('_')[1]
    else:
        return filename.split('委托')[0][6:]

# %%
def extract_info(filename):
    df = pd.read_excel(filename)
    if any(['国泰君安' in str(element) for element in list(df.iloc[:,0])[:5]]):
        offset = -1
        colon = '：'
        match_seq = '100201$|103106$|1103..01......|1105.010.|1108.010.|11090601......|^3003..02|1204$'
    else:
        offset = 0
        colon = ':'
        match_seq = '^1002\.01\.01|^1031\.|(^1103\...\.01\.)|(^1105\...\.01\.)|(^1108\...\.01\.)|(^3003\...\.02\.)'
    dated_net_value = list(df.iloc[2+offset].dropna())
    #print(dated_net_value)
    date = dated_net_value[0].split('：')[1]
    if'-' not in date:
        date = date[:4]+'-'+date[4:6]+'-'+date[6:]
    net_value = float(dated_net_value[1].split(colon)[1])
    df.columns = df.iloc[3+offset]

    df['科目代码'] = [str(code).strip() for code in list(df['科目代码'])]
    df = df.loc[df['科目代码'].str.match(match_seq)]

    if '成本-本币' in list(df.columns):
        df = df.rename(columns = {'成本-本币': '成本', '市值-本币': '市值'})
        #print(df.columns)
    elif '市价' in list(df.columns):
        df['成本占净值%'] = np.array(pd.to_numeric(df['成本占净值%'], errors='ignore'))/100
        df['市值占净值%'] = np.array(pd.to_numeric(df['市值占净值%'], errors='ignore'))/100
        df = df.rename(columns = {'成本占净值%':'成本占比', '市价':'行情', '市值占净值%':'市值占比'})
    print(df)
    df = df[['科目代码', '科目名称', '成本', '单位成本', '成本占比', '行情', '市值', '市值占比']]
    df = df.loc[:,~df.columns.duplicated()]
    
    df['证券代码'] = [code.split(' ')[0][-6:] if not np.isnan(value) else '' for code, value in zip(list(df['科目代码']),list(df['行情']))]
    df['日期'] = date
    df['产品名称'] = extract_name(filename)
    df['产品代码'] = filename[0:6]
    df['产品净值'] = net_value
    df['现金分红'] = 0
    df['分红再投资'] = 0

    return df    

# %%
path = os.getcwd()
files = os.listdir(path)
aggregated_df = pd.DataFrame()
cnt = 0
exception_cnt = 0
for filename in files:
    if filename[-3:] == 'xls':
        try:
            aggregated_df = aggregated_df.append(extract_info(filename), ignore_index=True)
            #print(filename, 'processed')
        except Exception as e:
            print('exception in',  filename, e)
            exception_cnt += 1
        cnt += 1
print(str(cnt-exception_cnt)+' records processed successfully')        
aggregated_df.to_excel('aggregated_valuation_sheet.xlsx')
# %%
import sqlalchemy
engine = sqlalchemy.create_engine('mysql+pymysql://root:19971223@localhost:3306/专户信息台账')
aggregated_df.to_sql('周频估值表', con = engine, if_exists='append', index=False, method='multi')
# %%
#删除重复记录
engine.connect().execute('''
    DELETE a
    FROM 周频估值表 a
        INNER JOIN 周频估值表 a2
    WHERE a.id < a2.id
    AND a.日期=a2.日期
    AND a.产品代码=a2.产品代码
    AND a.科目代码=a2.科目代码
''')
# %%
