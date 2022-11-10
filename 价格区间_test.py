
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
            CAST ( 店铺名称 AS nvarchar ( 2000 ) ),\
            CAST ( 店铺类型 AS nvarchar ( 2000 ) ),\
            [销售额SKU)]*10000,\
            [销量(L/KG)], \
            [销量(SKU)] \
    FROM MN_DATA_YTN_NEW \
    WHERE D_TIME between '202201' and '202209'  and 制造商 in('蒙牛','伊利')"
    
mn_df = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_zyanbo','ZhangYB_068','send_out',mn_sql),columns=['月份','平台名称','制造商','品类','品牌','子品牌','产品名称','店铺名称','店铺类型','销售额','升销量','件销量'])



df_url = mn_df[['制造商','产品名称','销售额','升销量']]
url = mn_df[['制造商','产品名称']].drop_duplicates()


def lg_(cpmc):

    plot5 = df_url[df_url['产品名称']==cpmc][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])

    ages = plot5.升价格
    lower_q=np.quantile(ages,0.15,interpolation='lower')#下四分位数
    higher_q=np.quantile(ages,0.85,interpolation='higher')#上四分位数
    int_r=higher_q-lower_q#四分位距
    lg = pd.cut(plot5.升价格, bins=[0,lower_q-0.00001, higher_q+0.00001,100000]).value_counts()
    return f"{lower_q}|{higher_q}|{lg.to_dict()}"


res = url['产品名称'].map(lambda x:lg_(x))




url['lower_q'],url['higher_q'],url['lg'] = res.str.split('|').str[0],res.str.split('|').str[1],res.str.split('|').str[2]


url.to_excel('价格区间_.xlsx')





