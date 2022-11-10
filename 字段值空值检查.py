#coding:utf-8
import _scproxy
import pymssql
import pandas as pd
import datetime
import smtplib



start  = datetime.datetime.now()


from tqdm import tqdm
from time import sleep

import os
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户字段及内容_空值.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
                input('放置后确认将运行')
        else:
                # print('正在存放至 [模版] 📁')
                pass
mkdir('模版')

# df = pd.read_excel(r'模版/客户字段及内容_空值.xlsx')


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


df = pd.DataFrame(sql_connect(sql = 'select * from 客户字段及内容_空值'),columns = ['客户名','品类','数据库名','字段名'])


df=df[['客户名','品类','数据库名']]
df=df.reset_index()
print("客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多")
kehu=input('请键入要运行的库户名称:')
# 客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多
df=df[df['客户名']==kehu]
# print(len(df))
# print(df.iloc[0:3])
dd_list=[]
for i in range(len(df)):
    a=df.iloc[i].tolist()
    # print("a",a)
    syntun_conn = pymssql.connect(server='192.168.0.15', user='zhongxin_yanfa', password='Xin_yanfa', charset='utf8')
    sql="select distinct cast(" + a[4]+" as nvarchar(1000)) as "+a[4]+" from "+ a[3]
    # print(sql)
    syntun_cursor = syntun_conn.cursor()
    try:
        syntun_cursor.execute(sql)
    except:
        print(a[3]+" "+a[4] + ':读取失败………………')
        continue
    sqljieguo = syntun_cursor.fetchall()
    xcolumns = [e[0] for e in syntun_cursor.description]
    for jieguo in sqljieguo:
        p = ''
        if jieguo[0] == "#N/A":
            p = [a[3],  str(xcolumns), "列存在#N/A:", jieguo[0]]
        elif jieguo[0] == "0":
            p = [a[3],  str(xcolumns), "列存在0:", jieguo[0]]
        elif jieguo[0] == "null":
            p = [a[3],  str(xcolumns), "列存在null:", jieguo[0]]
        elif jieguo[0] is None:
            p = [a[3],  str(xcolumns), "列存在空值:", jieguo[0]]
        elif jieguo[0].startswith(' '):
            p = [a[3],  str(xcolumns), "列空格开头:", jieguo[0]]
        elif jieguo[0].endswith(' '):
            p = [a[3],  str(xcolumns), "列空格结尾:", jieguo[0]]
        if p != "":
            dd_list.append(p)
        # break

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

list1.to_excel(f"结果/{kehu}kongzhi.xlsx")

end  = datetime.datetime.now()
print("程序运行时间："+str((end-start).seconds)+"秒")

input('文件已输出,请到 [结果] 文件下获取')
