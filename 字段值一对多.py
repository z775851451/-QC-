#coding:utf-8
# import _scproxy
import pymssql
import pandas as pd
import datetime
import smtplib


from tqdm import tqdm
from time import sleep

start  = datetime.datetime.now()


# import subprocess

# # 打开文件或者速度最快, 推荐，不过只适用于Windows
# def start_file(file_path):
#     os.startfile(file_path)
    

import os
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户字段及内容_一对多.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
                input('放置后确认将运行')
        else:
                # print('正在存放至 [模版] 📁')
                pass
mkdir('模版')




df = pd.read_excel('模版/客户字段及内容_一对多.xlsx')
# ,sheet_name='')
df=df[['客户名','品类','数据库名','字段1','字段2','判断']]
df1=df.reset_index()
# 筛选客户品类
# df1=df1['客户名']=='恒天然'
# HN_1=HB[(HB['品类']=='功能饮料') & HB['品类细分'].isin(a)]
# 客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多
print("客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多")
kehu=input('请键入要运行的库户名称:')
if kehu != 'all':
    df=df1[df1['客户名']== kehu]
else:
    df=df1

print('正在运行.............')
# print(len(df))
# print(df.iloc[0:3])
dd_list=[]
for i in tqdm(range(len(df))):
    sleep(0.05)
    a=df.iloc[i].tolist()
    # print(a)
    syntun_conn = pymssql.connect(server='192.168.0.15', user='zhongxin_yanfa', password='Xin_yanfa', charset='utf8')
    if a[5]!= a[4] :
        sql="select cast(" + a[4]+" as nvarchar ( 1000 ) ) as "+a[4]+",cast(" + a[5]+" as nvarchar ( 1000 )) as "+a[5]+",count(distinct "+a[6]+ ") as k from "+ a[3] +" group by "+a[4]+","+a[5]+" having count(distinct "+a[6]+ ")>1"
        # print(sql)
        syntun_cursor = syntun_conn.cursor()
        try:
            syntun_cursor.execute(sql)
        except:
            tqdm.write(a[3] +a[4] +'列:读取失败………………')
            continue
        sqljieguo = syntun_cursor.fetchall()
        # print("sqljieguo=", sqljieguo)
        if sqljieguo != []:
            # print(f'在{a[3]}表中{a[4]},{a[5]}列发现{a[6]}一对多!',sqljieguo)
            p = f'在{a[3]}表中{a[4]}列发现{a[6]}一对多:'+str(sqljieguo)
            # print("p=", p)
            dd_list.append(p)
    else:
        sql="select cast(" + a[4]+" as nvarchar ( 1000 )) as "+a[4]+",count(distinct "+a[6]+ ") as k from "+ a[3] +" group by "+a[4]+" having count(distinct "+a[6]+ ")>1"
        # print(sql)
        syntun_cursor = syntun_conn.cursor()
        try:
            syntun_cursor.execute(sql)
        except:
            tqdm.write(a[3] + a[4]+ ':读取失败………………')
            continue
        sqljieguo = syntun_cursor.fetchall()
        # with pd.ExcelWriter(r'QCwrongdata.xlsx') as writer:
        # print("sqljieguo=", sqljieguo)
        if sqljieguo != []:
            # print(f'在{a[3]}表中{a[4]}列发现{a[6]}一对多!',sqljieguo)
            p=f'在{a[3]}表中{a[4]}列发现{a[6]}一对多:'+str(sqljieguo)
            # print("p=",p)
            dd_list.append(p)
            # print("list=",list)
syntun_cursor.close()
syntun_conn.close()
list1=pd.DataFrame(dd_list)
# print("list1=",list1)



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

list1.to_excel(f"结果/{kehu}yiduiduo.xlsx")

end  = datetime.datetime.now()
print("程序运行时间："+str((end-start).seconds)+"秒")

input('文件已输出,请到 [结果] 文件下获取')


    