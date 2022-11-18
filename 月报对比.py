# %%
# import _scproxy
import pymssql
import pandas as pd
import numpy as np
import openpyxl
import warnings

warnings.filterwarnings('ignore')

# %%
def sql_connect(server='192.168.0.15',user='zhongxin_zyanbo',password='ZhangYB_068',sql=None):
    syntun_conn = pymssql.connect(server=server,
                            user=user,
                            password=password)
    syntun_cursor = syntun_conn.cursor()

    syntun_cursor.execute(sql)
    s = syntun_cursor.fetchall()
    syntun_cursor.close()
    syntun_conn.close()
    return s

d_=sql_connect(sql = 'select CAST ( 客户 AS nvarchar ( 500 ) ),	CAST ( 品类 AS nvarchar ( 500 ) ),	CAST ( 数据库名 AS nvarchar ( 500 ) ),	CAST ( 对比数据库名 AS nvarchar ( 500 ) ),	CAST ( 平台 AS nvarchar ( 500 ) ),	CAST ( 月份 AS nvarchar ( 500 ) ),	CAST ( 销售额 AS nvarchar ( 500 ) ),	CAST ( 升销量 AS nvarchar ( 500 ) ),	CAST ( 对比数据库平台 AS nvarchar ( 500 ) ),	CAST ( 对比数据库月份 AS nvarchar ( 500 ) ),	CAST ( 对比数据库销售额 AS nvarchar ( 500 ) ),	CAST ( 对比数据库升销量 AS nvarchar ( 500 ) )  from [QC].[dbo].历史对比配置')
QC_DF = pd.DataFrame(d_,columns =['客户','品类','数据库名','对比数据库名','平台','月份','销售额','升销量','对比数据库平台','对比数据库月份','对比数据库销售额','对比数据库升销量'])

# %%
columns_dict = {0:'平台名称',1:'品牌',2:'产品名称'}

# %%
print(QC_DF['客户'])

# %%
# input('要运行的客户:')
# inp_ = '伊利,蒙牛'.split(',')
inp_ = input('要运行的客户列表逗号分隔:').split(',')
# input('请输入需要对比的库后缀,如果库的名称已配置完整此处可以为空:')
# inp_date = '202210'
inp_date = input('请输入需要对比的库后缀,如果库的名称已配置完整此处可以为空:')
print(inp_)
# %%
use_df_ = QC_DF[QC_DF['客户'].isin(inp_)]
mer_df = use_df_[['客户','品类','数据库名']]

# %%
pd.set_option('display.float_format',lambda x : '%.2f' % x)

# %%
a_box,b_box,c_box = [pd.DataFrame()],[pd.DataFrame()],[pd.DataFrame()]
def lg(n):
    use_df = use_df_[n:n+1]
    sjk = use_df[['数据库名','平台','月份','销售额','升销量']]
    db_sjk = use_df[['对比数据库名','对比数据库平台','对比数据库月份','对比数据库销售额','对比数据库升销量']]
    sjk_ = pd.concat([sjk[['数据库名','月份','销售额','升销量']],sjk['平台'].str.split(',',expand=True).rename(columns = columns_dict)],axis=1)
    db_sjk_ = pd.concat([db_sjk[['对比数据库名','对比数据库月份','对比数据库销售额','对比数据库升销量']],db_sjk['对比数据库平台'].str.split(',',expand=True).rename(columns = columns_dict)],axis=1)

    sql_li_a = []
    sql_li_b = []
    for i in sjk_.columns:
        sql_li_a.append( sjk_[i].values[0])
    for i in db_sjk_.columns:
        sql_li_b.append(db_sjk_[i].values[0])
    
    try:
        s,a,b,c,d,e,f = sql_li_a
        S,A,B,C,D,E,F = sql_li_b
        S = S+inp_date
    except:
        try:
            s,a,b,c,d,e = sql_li_a
            S,A,B,C,D,E = sql_li_b
            S = S+inp_date
        except:
            s,a,b,c,d = sql_li_a
            S,A,B,C,D = sql_li_b
            S = S+inp_date
    
    if len(sjk['平台'].str.split(',').values[0]) >= 1:
        # a,b,c,d = sql_li_a
        a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d}"
        
        # A,B,C,D = sql_li_b
        b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D}"
        
        
        a_df = pd.DataFrame(sql_connect(sql=a_sql),columns=['月份','平台','新数据库销售额','新数据库升销量'])
        b_df = pd.DataFrame(sql_connect(sql=b_sql),columns=['月份','平台','备份数据库销售额','备份数据库升销量'])
        c_df = a_df.merge(b_df,how='left',on=['月份','平台']).assign(
            数据库名 = s,
            备份数据库名 = S,
            销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
            升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量'],
            是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001)
        
        a_box.append(c_df[c_df['是否差异'] == True])
        
    if len(sjk['平台'].str.split(',').values[0]) >= 2:
        # a,b,c,d,e = sql_li_a
        a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),CAST( {e} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d},{e}"
        
        # A,B,C,D,E = sql_li_b
        b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),CAST( {E} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D},{E}"
        
        a_df = pd.DataFrame(sql_connect(sql=a_sql),columns=['月份','平台','品牌','新数据库销售额','新数据库升销量'])
        b_df = pd.DataFrame(sql_connect(sql=b_sql),columns=['月份','平台','品牌','备份数据库销售额','备份数据库升销量'])
        c_df = a_df.merge(b_df,how='left',on=['月份','平台','品牌']).assign(
            数据库名 = s,
            备份数据库名 = S,
            销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
            升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量']
            ,
            是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001
            )
        
        b_box.append(c_df[c_df['是否差异'] == True])
    
    if len(sjk['平台'].str.split(',').values[0]) == 3:
        # a,b,c,d,e,f = sql_li_a
        a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),CAST( {e} AS nvarchar ( 500 ) ),CAST( {f} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d},{e},{f}"
        # A,B,C,D,E,F = sql_li_b
        b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),CAST( {E} AS nvarchar ( 500 ) ),CAST( {F} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D},{E},{F}"
        print(a_sql,b_sql)
        
        a_df = pd.DataFrame(sql_connect(sql=a_sql),columns=['月份','平台','品牌','产品名称','新数据库销售额','新数据库升销量'])
        b_df = pd.DataFrame(sql_connect(sql=b_sql),columns=['月份','平台','品牌','产品名称','备份数据库销售额','备份数据库升销量'])
        c_df = a_df.merge(b_df,how='left',on=['月份','平台','品牌','产品名称']).assign(
            数据库名 = s,
            备份数据库名 = S,
            销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
            升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量'],
            是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001)

        c_box.append(c_df[c_df['是否差异'] == True])

    # sjk_len = len(sjk_.columns[~sjk_.columns.isin(['数据库名','月份','销售额','升销量'])])
    
    
    # sql = 
    # sql_ = 
    
    return a_box,b_box,c_box
    return len(sjk['平台'].str.split(','))
    
for i in range(len(use_df_)):
    lg(i)
# %%
# use_df_[0:1][['数据库名','平台','月份','销售额','升销量']]['平台'].str.split(',').values[0]

pd.concat(a_box,axis=0)

# mer_df.merge(pd.concat(a_box,axis=0)[['月份','数据库名','备份数据库名','平台','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名'])

# %%
# a_box,b_box,c_box
import openpyxl
from openpyxl import load_workbook
with pd.ExcelWriter(f'结果/{inp_}月报对比_结果.xlsx') as mn_writer:
    mer_df.merge(pd.concat(a_box,axis=0)[['月份','数据库名','备份数据库名','平台','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).to_excel(mn_writer,sheet_name='平台',na_rep='',index=False)
    mer_df.merge(pd.concat(b_box,axis=0)[['月份','数据库名','备份数据库名','平台','品牌','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).to_excel(mn_writer,sheet_name='平台 品牌',na_rep='',index=False)
    try:
        mer_df.merge(pd.concat(c_box,axis=0)[['月份','数据库名','备份数据库名','平台','品牌','产品名称','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).to_excel(mn_writer,sheet_name='平台 品牌 产品名称',na_rep='',index=False)
    except:
        pass




# %%
