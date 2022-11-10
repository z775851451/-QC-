# %%
#coding:utf-8
import _scproxy
import pymssql
import pandas as pd
import numpy as np  

# %%
def sql_connect(server = '192.168.0.15',user='zhongxin_zyanbo',password='ZhangYB_068',sql = None):
    
    syntun_conn = pymssql.connect(server=server,
                              user=user,
                              password=password)
    syntun_cursor = syntun_conn.cursor()
    try:
        syntun_cursor.execute(sql)
        s = syntun_cursor.fetchall()
        syntun_cursor.close()
        syntun_conn.close()
    except:
        return print(f'请检查字段配置是否有误数据库:{sql},已跳过此数据库')
    return s

# %%
import os
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 细分市场规则表_20220923.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
                input('放置后确认将运行')
        else:
                # print('正在存放至 [模版] 📁')
                pass
mkdir('模版')

# %%
df = pd.read_excel('模版/细分市场规则表.xlsx')
df_merge = df[['数据库名','大类','客户','制造商']].drop_duplicates()

# %%
print(set(df['客户'].to_list()))
us_i = input('输入模版内要运行的 [客户]名称 或输入 [all] 全部运行:')

# %%
yes_ = pd.merge(
    
        df.groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'包含内容1':','.join}).reset_index()
        ,df[(df['包含内容2'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'包含内容2':','.join}).reset_index()
,on=['数据库名','产品名称','判断字段','字段内容'],how='left')


no_ = pd.merge(
         df[(df['不包含内容1'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容1':','.join}).reset_index()
        ,df[(df['不包含内容2'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容2':','.join}).reset_index()
,on=['数据库名','产品名称','判断字段','字段内容'],how='left'
).merge(
        df[(df['不包含内容3'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容3':','.join}).reset_index()
    
,on=['数据库名','产品名称','判断字段','字段内容'],how='left')

# %%
gz_li = yes_.merge(no_,how='left',on=['数据库名','产品名称','判断字段','字段内容'])

# %%
# gz_li[gz_li['数据库名'] == '[info].[dbo].[syntun_Infant_milk_powder_593]']
# gz_li.groupby(by=['数据库名','产品名称','判断字段','字段内容'])

# %%
df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()

# %%
if 'all' in us_i:
    sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
else:
    sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()

# %%
def lg_df(k):
    pc_list = []
    gz_df = gz_li[gz_li['数据库名'] == k]
    zd_li = gz_df['判断字段'].drop_duplicates().to_list()
    zd_li_sql = ','.join(zd_li)
    cpmc = gz_li[gz_li['数据库名'] == k]['产品名称'].drop_duplicates().values[0]
    sql = f'SELECT distinct {cpmc},{zd_li_sql} FROM {k}'
    sql_df = pd.DataFrame(sql_connect(sql = sql),columns=[cpmc]+zd_li)
    return sql_df

# %%
def if_na(x):
    if pd.isnull(x):
        return False
    else:
        return x.split(',')

# %%
res = []
from tqdm import tqdm
from time import sleep

for k in tqdm(sjk_li):
    tqdm.write(k)
    sleep(0.05)
    pc_list = []
    res_df_li = []
    gz_df = gz_li[gz_li['数据库名'] == k]
    zd_li = gz_df['判断字段'].drop_duplicates().to_list()
    zd_li_sql = ','.join(zd_li)
    cpmc = gz_li[gz_li['数据库名'] == k]['产品名称'].drop_duplicates().values[0]
    sql = f'SELECT distinct cast({cpmc} as nvarchar (2000)),{zd_li_sql} FROM {k}'
    try:
        sql_df = pd.DataFrame(sql_connect(sql = sql),columns=[cpmc]+zd_li)
    except:
        continue
    
    for i in range(len(gz_df)):
        
        pdzd = gz_li[gz_li['数据库名'] == k]['判断字段'].to_list()[i]
        zdnr = gz_li[gz_li['数据库名'] == k]['字段内容'].to_list()[i]
        bhnr_1 = if_na(gz_li[gz_li['数据库名'] == k]['包含内容1'].to_list()[i])
        bhnr_2 = if_na(gz_li[gz_li['数据库名'] == k]['包含内容2'].to_list()[i])
        bbhnr_1 = if_na(gz_li[gz_li['数据库名'] == k]['不包含内容1'].to_list()[i])
        bbhnr_2 = if_na(gz_li[gz_li['数据库名'] == k]['不包含内容2'].to_list()[i])
        bbhnr_3 = if_na(gz_li[gz_li['数据库名'] == k]['不包含内容3'].to_list()[i])
        
        lg_df = sql_df[sql_df[pdzd] == zdnr].reset_index(drop = True)
        
        ts_li = []
        for i in bhnr_1:
            lg_df[cpmc].map(lambda x:ts_li.append(x) if i in x else x)
        # pd.DataFrame(set(ts_li))
        # pd.DataFrame(ts_li)
        ts_li = list(set(lg_df[cpmc]).difference(set(ts_li)))
        
        if ts_li:
            if bhnr_2:
                for i in bhnr_2:
                    ts_li = np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]
            if bbhnr_1 and ts_li:
                for i in bbhnr_1:
                    ts_li = np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]
            if bbhnr_2 and ts_li:
                for i in bbhnr_2:
                    ts_li = np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]
            if bbhnr_3 and ts_li:
                for i in bbhnr_3:
                    ts_li = np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]
                    
        ls_df = pd.DataFrame(ts_li,columns =['产品名称']).assign(数据库名= k).merge(df_merge,how='left',on='数据库名')
        ls_df=ls_df.assign(
            异常字段 = pdzd,
            字段内容 = zdnr,
            应包含内容 = str(bhnr_1),
            应包含内容2 = str(bhnr_2),
            不应包含内容1 = str(bbhnr_1),
            不应包含内容2 = str(bbhnr_2),
            不应包含内容3 = str(bbhnr_3)
            )[['数据库名','大类','客户','制造商','产品名称','异常字段','字段内容','应包含内容','应包含内容2','不应包含内容1','不应包含内容2','不应包含内容3']]
                    
        res_df_li.append(ls_df)
        
           
    res.append(pd.concat(res_df_li))
if res:
    yc_data = pd.concat(res)

# %%
import os
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                tqdm.write('检测无结果文件夹,程序将自动创建 📁')#判断是否存在文件夹如果不存在则创建为文件夹
        else:
                tqdm.write('正在存放至 [结果] 📁')
                pass
mkdir('结果')

# %%
import openpyxl
from openpyxl import load_workbook
df_workbook = load_workbook(r'模版/细分市场规则表.xlsx')

df_writer = pd.ExcelWriter(r'模版/细分市场规则表.xlsx',
                        engine='openpyxl')
df_writer.book= df_workbook

df_workbook.save(r'模版/细分市场规则表.xlsx')

yc_data.to_excel(df_writer, sheet_name='抛出',na_rep='',index=False,startrow=0,startcol=0)

df_workbook.save(r'结果/细分市场规则表_抛出结果.xlsx')
df_workbook.close()

input('已保存')

