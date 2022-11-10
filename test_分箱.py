import _scproxy
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
    WHERE D_TIME >= 202201"
    
mn_df = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_zyanbo','ZhangYB_068','send_out',mn_sql),columns=['月份','平台名称','制造商','品类','品牌','子品牌','产品名称','店铺名称','店铺类型','销售额','升销量','件销量'])



df_url = mn_df[mn_df['产品名称'] == '蒙牛 特仑苏 纯牛奶 250ml*12']



plot = df_url[df_url['月份'] == '202209'][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])
plot1 = df_url[df_url['月份'] == '202208'][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])
plot2 = df_url[df_url['月份'] == '202207'][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])
plot3 = df_url[df_url['月份'] == '202206'][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])
plot4 = df_url[df_url['月份'] == '202205'][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])
plot5 = df_url[df_url['月份'].isin(['202209','202208','202207','202206','202205'])][['产品名称','销售额','升销量']].assign(升价格  = lambda x:x['销售额']/x['升销量'])


import numpy as np
ages = plot5.升价格
lower_q=np.quantile(ages,0.15,interpolation='lower')#下四分位数
higher_q=np.quantile(ages,0.85,interpolation='higher')#上四分位数
int_r=higher_q-lower_q#四分位距
print(lower_q,higher_q,int_r)


print(pd.cut(plot.升价格, bins=[0,lower_q, higher_q,100000]).value_counts())


import pandas as pd
import matplotlib.pyplot as plt
 
#读取数据
box_1, box_2, box_3, box_4 ,box_5, box_6 = plot['升价格'], plot1['升价格'], plot2['升价格'], plot3['升价格'], plot4['升价格'], plot5['升价格']
 
plt.figure(figsize=(10,9))#设置画布的尺寸
plt.title('Examples of boxplot',fontsize=20)#标题，并设定字号大小
labels = '9月','8月','7月','6月','5月','all'#图例
plt.boxplot([box_1, box_2, box_3, box_4, box_5, box_6], labels = labels)#grid=False：代表不显示背景中的网格线
# data.boxplot()#画箱型图的另一种方法，参数较少，而且只接受dataframe，不常用
plt.show()#显示图像






