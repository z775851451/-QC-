
# import _scproxy
import pandas as pd 
import numpy as np
import openpyxl
import pymssql
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

def sql_connect(server,user,password,database,sql):
    syntun_conn = pymssql.connect(server=server,
                              user=user,
                              password=password,
                              database=database)
    syntun_cursor = syntun_conn.cursor()

    syntun_cursor.execute(sql)
    s = syntun_cursor.fetchall()
    syntun_cursor.close()
    syntun_conn.close()
    return s

mn_sql = "SELECT \
            D_TIME,\
            CAST ( 平台名称 AS nvarchar ),\
            CAST ( 制造商 AS nvarchar ),\
            CAST ( 品类 AS nvarchar ),\
            CAST ( 品牌 AS nvarchar ( 2000 ) ),\
            CAST ( 子品牌 AS nvarchar ( 2000 ) ),\
            CAST ( 产品名称 AS nvarchar ( 2000 ) ),\
            CAST ( ( \
                CASE \
                    WHEN 产品名称 LIKE '%装' THEN SUBSTRING(产品名称,0,CHARINDEX('装',产品名称,-1))+'装'\
                    WHEN 产品名称 LIKE '%版' THEN SUBSTRING(产品名称,0,CHARINDEX('版',产品名称,-1))+'版'\
                    WHEN 产品名称 LIKE '%款' THEN SUBSTRING(产品名称,0,CHARINDEX('款',产品名称,-1))+'款'\
      ELSE SUBSTRING(产品名称+'*',0,CHARINDEX('*',产品名称+'*',1)) \
        END\
        )AS nvarchar ( 2000 ) ),\
            CAST ( 店铺名称 AS nvarchar ( 2000 ) ),\
            CAST ( 店铺类型 AS nvarchar ( 2000 ) ),\
            [销售额SKU)]*10000,\
            [销量(L/KG)], \
            [销量(SKU)] \
    FROM MN_DATA_YTN_NEW \
    WHERE D_TIME between '202201' and '202209'  and 制造商 in('蒙牛','伊利') and" \
         " Platform_id in (1,5) and 店铺类型  In  ('平台自营','品牌旗舰店')"
    
mn_df = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_zyanbo','ZhangYB_068','send_out',mn_sql),columns=['月份','平台名称','制造商','品类','品牌','子品牌','产品名称','系列','店铺名称','店铺类型','销售额','升销量','件销量'])



df_url = mn_df[['制造商','系列','销售额','升销量']]

url = mn_df[['制造商','系列']].drop_duplicates()


def lg_(cpmc):

    plot5 = df_url[df_url['系列']==cpmc][['系列','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])

    ages = plot5.升价格
    lower_q=np.quantile(ages,0.15,interpolation='lower')#下四分位数
    higher_q=np.quantile(ages,0.85,interpolation='higher')#上四分位数
    int_r=higher_q-lower_q#四分位距
    lg = pd.cut(plot5.升价格, bins=[0,lower_q-0.00001, higher_q+0.00001,100000]).value_counts()
    return f"{lower_q}|{higher_q}|{int_r}|{lg.to_dict()}"


res = url['系列'].map(lambda x:lg_(x))
# 取数4列
# url['lower_q'],url['higher_q'],url['int_r'],url['lg'] = res.str.split('|').str[0],res.str.split('|').str[1],res.str.split('|').str[2],res.str.split('|').str[3]
# url['lower_q'], url['higher_q'], url['int_r'] = res.str.split('|').str[0], res.str.split('|').str[1], res.str.split('|').str[2]
url['lower_q'], url['higher_q'], url['int_r'] = res.str.split('|').str[0], res.str.split('|').str[1], res.str.split('|').str[2]
url['lower_q'] = url['lower_q'].astype(float)
url['higher_q'] = url['higher_q'].astype(float)
url['int_r'] = url['int_r'].astype(float)


with pd.ExcelWriter(r'价格区间_系列结果.xlsx') as mn_writer:
    # mn_writer.book = mn_workbook
    url.to_excel(mn_writer,sheet_name='价格区间',na_rep='',index=False,startcol=0,header=True,float_format = "%0.2f")
# url.to_excel('价格区间_系列结果.xlsx',float_format = "%0.2f")





