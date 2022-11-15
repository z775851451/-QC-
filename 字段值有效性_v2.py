#coding:utf-8
# import _scproxy
import pymssql
import pandas as pd
import datetime
import smtplib

start  = datetime.datetime.now()

from tqdm import tqdm
from time import sleep

def sql_connect(server='192.168.0.15',user='zhongxin_zyanbo',password='ZhangYB_068',database='QC',sql=None):
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


# import os
# def mkdir(path):
#         folder = os.path.exists(path)
#         if not folder:    
#                 os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
#                 print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户字段及内容_有效性.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
#                 input('放置后确认将运行')
#         else:
#                 # print('正在存放至 [模版] 📁')
#                 pass
# mkdir('模版')

# df = pd.read_excel(r'模版/客户字段及内容_有效性.xlsx')

df = pd.DataFrame(sql_connect(sql = 'select CAST ( 客户名 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),CAST ( 字段名 AS nvarchar ( 500 ) ),CAST ( 字段内容 AS nvarchar ( 500 ) ) from 客户字段及内容_有效性'),columns = ['客户名','品类','数据库名','字段名','字段内容'])

# df_sta = df.copy()
# df_sta['数据库名'] = df_sta['数据库名'].str.replace(']', '')
# df_sta['数据库名'] = df_sta['数据库名'].str.replace('[', '')
# df_sta['数据库表名'] = df_sta['数据库名'].str.split('.').str[0]
# 关注库内表名列表
df=df[['客户名','品类','数据库名','字段名','字段内容']]
df1=df.reset_index()
# 筛选客户品类
# 客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多
print("客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多")
kehu=input('请键入要运行的库户名称:')
if kehu != 'all':
    df=df1[df1['客户名']== kehu]
else:
    df=df1



df_t = df[['数据库名','字段名']].drop_duplicates()
# .assign(t = lambda x:f" cast({str(x['字段名'])} as nvarchar)as {x['字段名']}")
df_t_ = df_t.assign(s = df_t['字段名'].map(lambda x: f" cast({str(x)} as nvarchar)as {x} "))
df_t_ = df_t_.groupby(by=['数据库名']).agg({'s':','.join}).reset_index()
df_t_ = df_t_.assign(k = df_t_['数据库名'].map(lambda x: f" FROM {str(x)}"))
sqllis = df_t_.apply(lambda x:f"SELECT distinct {x['s']} {x['k']}",axis=1).to_list()

# df['数据库名'] = df['数据库名'].map(str.strip)
uniq = df['数据库名'].str.replace(' ', '').unique().tolist()
print(len(uniq))
# uniq = uniq[2:3]
print("uniq=",uniq)

dd_list=[]
for mm in tqdm(uniq):
    coln_lis = []  # 装表的列名
    xiaodf_lis = []  # 装没有列名的表
    for w in ['utf8']:
        # 连接数据库
        syntun_conn = pymssql.connect(server='192.168.0.15',
                                      user='zhongxin_yanfa',
                                      password='Xin_yanfa',
                                      charset=w)
        syntun_cursor = syntun_conn.cursor()
        
        for i in sqllis:
            sql = i
            # print(sql)
            m=str(sql[sql.rfind("from "):]).replace("from","").replace(" ","")
            # print ("m=",m)
            try:
                syntun_cursor.execute(sql)
            except:
                print(i + ':读取失败………………', w)
                continue
            if m == mm:
                xcolumns = [e[0] for e in syntun_cursor.description]
                # print("xcolumns=",xcolumns)
                sqljieguo = syntun_cursor.fetchall()
                # print("sqljieguo=", sqljieguo)
                sqljieguo = pd.DataFrame(sqljieguo, columns=xcolumns)
                # print(sqljieguo.columns)
                # 库内每张表关注的所有字段名列表：a
                excel_lie = df[df['数据库名'] == m]['字段名'].unique().tolist()
                # print("excel_lie=",excel_lie)
                for h in excel_lie:
                    # 每个关注的字段名对应值的列表：b
                    excel_zhi = df[(df['数据库名'] == m)
                           & (df['字段名'] == h)]['字段内容'].unique().tolist()
                    # print("excel_zhi=",excel_zhi)
                    # 判断库内表字段h的值是否存在于b中(两个列表内的元素是否相同？在库列表中而不在对照列表中为错误
                    ku_lis = sqljieguo[h].unique().tolist()
                    # print("ku_lis=",ku_lis)
                    err_lis = [x for x in ku_lis if x not in excel_zhi]
                    # print('err_lis',err_lis)
                    if err_lis != []:
                        tqdm.write(f'在{m}表中{h}列发现异常值{err_lis}!')
                        p=f'在{m}表中{h}列发现异常值{err_lis}!'
                        dd_list.append(p)
                    # else:
                    #     print(m,f'表',h,'正常')
            # else:
            #     print('表名未匹配')
list1=pd.DataFrame(dd_list)
print("list1=",list1)

import os
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                print('检测无结果文件夹,程序将自动创建 📁')#判断是否存在文件夹如果不存在则创建为文件夹
        else:
                print('正在存放至 [结果] 📁')
                pass
mkdir('结果')


list1.to_excel(f"结果/{kehu}youxiaoxing.xlsx")

syntun_cursor.close()
syntun_conn.close()



end  = datetime.datetime.now()
print("程序运行时间："+str((end-start).seconds)+"秒")

input('文件已输出,请到 [结果] 文件下获取')
