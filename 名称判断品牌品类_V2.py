"""
客户产品名称和规则表的大类\客户值字段值需对应
规则表的列名不要重复
"""
#%%
# import _scproxy

import pymssql
import pandas as pd
import numpy as np
import openpyxl
import datetime
from openpyxl import load_workbook

from tqdm import tqdm
from time import sleep

# import os
# def mkdir(path):
#         folder = os.path.exists(path)
#         if not folder:    
#                 os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
#                 print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户名称判断规则表.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
#                 input('放置后确认将运行')
#         else:
#                 # print('正在存放至 [模版] 📁')
#                 pass
# mkdir('模版')


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

#%%
start  = datetime.datetime.now()
# namegz = pd.read_excel(r'模版/客户名称判断规则表.xlsx',sheet_name=['客户产品名称','规则表','抛出'])
# guize = namegz['规则表']
# kehudf = namegz['客户产品名称']


guize = pd.DataFrame(sql_connect(sql = 'select CAST ( 大类 AS nvarchar ( 500 ) ),CAST ( 大类 AS nvarchar ( 500 ) ),CAST (  客户 AS nvarchar ( 500 ) ),CAST (  源制造商 AS nvarchar ( 500 ) ),CAST (  产品系列 AS nvarchar ( 500 ) ),CAST (  包含内容1 AS nvarchar ( 500 ) ),CAST (  包含内容2 AS nvarchar ( 500 ) ),CAST (  不包含内容1 AS nvarchar ( 500 ) ),CAST (  不包含内容2 AS nvarchar ( 500 ) ),CAST (  不包含内容3 AS nvarchar ( 500 ) ),CAST (  品牌 AS nvarchar ( 500 ) ),CAST (  制造商 AS nvarchar ( 500 ) ),CAST (  子品类 AS nvarchar ( 500 ) ) from 产品名称判断品牌品类_基础表'),columns=['0','大类','客户','源制造商','产品系列','包含内容1','包含内容2','不包含内容1','不包含内容2','不包含内容3','品牌','制造商','子品类'])
kehudf = pd.DataFrame(sql_connect(sql = 'select CAST (大类 AS nvarchar ( 500 ) ),CAST ( 大类 AS nvarchar ( 500 ) ),CAST (  客户 AS nvarchar ( 500 ) ),CAST (  品类 AS nvarchar ( 500 ) ),CAST (  数据库名 AS nvarchar ( 500 ) ),CAST (  字段名 AS nvarchar ( 500 ) ),CAST (  判断制造商 AS nvarchar ( 500 ) ),CAST (  判断品牌 AS nvarchar ( 500 ) ),CAST (  判断品类 AS nvarchar ( 500 ) ) from 产品名称判断品牌品类_对照表'),columns=['0','大类','客户','品类','数据库名','字段名','判断制造商','判断品牌','判断品类'])

guize.replace(np.nan, '', inplace=True)

sheet_lis = []
for i in tqdm(range(len(kehudf))):
    Account = kehudf.loc[i,:].tolist()[-5:]
    fillcol = kehudf.loc[i,:].tolist()[1:5]

    # 数据库连接
    syntun_conn = pymssql.connect(server='192.168.0.15',user='zhongxin_yanfa',password='Xin_yanfa')
    syntun_cursor = syntun_conn.cursor()
    sql = "SELECT DISTINCT CAST ( "+ Account[1] + " AS nvarchar ( 500 ) ), CAST (" + Account[2] +  " AS nvarchar),CAST (" + Account[3] +  " AS nvarchar),CAST (" + Account[4] +  " AS nvarchar) FROM "  + Account[0] +" where "+ Account[1] +" is not null"
    syntun_cursor.execute(sql)
    sql_df = syntun_cursor.fetchall()
    syntun_cursor.close()
    syntun_conn.close()
    startdf = pd.DataFrame(sql_df, columns=['产品名称', '制造商', '品牌', '子品类'])
    
    """判断前提：库内的产品名称是正确的，制造商、品牌、品类有可能存在错误"""
    df0 = startdf.copy()
    xx = r"^[^*]*(?:\*[^*]*){2}$"
    # 产品名称中存在+区分系列之间的连接符+（只针对品牌名字最后面字符为加号+的情况）
    df0['产品名称'] = df0['产品名称'].str.replace ('+ ', '暂时替换 ',regex=False)
    df0['产品系列'] = df0['产品名称'].str.split('+').str[0].str.replace ('暂时替换', '+')
    df0['产品名称'] = df0['产品名称'].str.replace ('暂时替换', '+')
    # 同一个系列多箱包装去重
    df0['产品系列辅助'] = df0['产品系列'].str.split('*').str[0:-1].str.join("*")
    df0['产品系列'] = df0['产品系列'].mask(df0['产品系列'].str.contains(xx), df0['产品系列辅助'])
        
    """客户产品名称和规则表的大类及客户值匹配"""
    guizedf = guize[(guize['大类']==fillcol[0]) & (guize['客户']==fillcol[1])]
    if len(guizedf) > 0:
        # 规则表对应值
        guize_lis = [[
            i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7],i[8]
        ] for i in guizedf[['源制造商', '包含内容1', '包含内容2','不包含内容1','不包含内容2','不包含内容3', '制造商', '品牌', '子品类']].values]
        df_lis = []
        df0_copy = df0.copy()
        for m in guize_lis:
            # print(m)
            # 库内产品系列包含1和2条件相对应的制造商, 品牌, 品类
            ku_df0 = df0_copy.loc[df0_copy['制造商'].str.contains(str(m[0]))
            # ku_df0 = df0.loc[(str(df0['制造商'])==str(m[0]))
                             & df0_copy['产品系列'].str.contains(str(m[1]))
                             & df0_copy['产品系列'].str.contains(str(m[2]))
                             & ~(df0_copy['产品系列'].str.contains(str(m[3])))
                             & ~(df0_copy['产品系列'].str.contains(str(m[4])))
                             & ~(df0_copy['产品系列'].str.contains(str(m[5])))
                             ,['产品名称', '产品系列', '制造商', '品牌', '子品类']]
            df_a_filter = df0_copy[~ df0_copy['产品名称'].isin(ku_df0['产品名称'])]
            df0_copy = df_a_filter.copy()
            # 选取非规则内的数据（品牌或者品类）
            ku_df1 = ku_df0[(ku_df0['品牌'] != m[7]) |
                            (ku_df0['子品类'] != m[8])].drop_duplicates(
                                ['产品系列', '制造商', '品牌', '子品类'])

            ku_df1['规则判断品牌']=m[7]
            ku_df1['规则判断品类']=m[8]


            df_lis.append(ku_df1)

        ku_df = pd.concat(df_lis,axis=0)

        ku_df['大类'] = fillcol[0]
        ku_df['客户'] = fillcol[1]
        ku_df['品类'] = fillcol[2]
        ku_df['数据库名'] = fillcol[3]
        ku_df = ku_df[['大类','客户','品类','数据库名','产品名称', '产品系列', '制造商', '品牌', '子品类','规则判断品牌','规则判断品类']]
        sheet_lis.append(ku_df)
# 库表合并
#%%

# ku_df0 = pd.concat(sheet_lis,axis=0)
ku_df0

#%%

# def mkdir(path):
#         folder = os.path.exists(path)
#         if not folder:    
#                 os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
#                 print('检测无结果文件夹,程序将自动创建 📁')#判断是否存在文件夹如果不存在则创建为文件夹
#         else:
#                 print('正在存放至 [结果] 📁')
#                 pass
# mkdir('结果')

with pd.ExcelWriter('结果/名称判断异常-结果.xlsx') as mc_writer:
    ku_df0.to_excel(mc_writer, sheet_name='抛出', na_rep='', index=False, startrow=0, startcol=0, header=True)
    guize.to_excel(mc_writer,  sheet_name='规则表',na_rep='',index=False,startrow=0,startcol=0,header=True)
    kehudf.to_excel(mc_writer, sheet_name='客户产品名称', na_rep='', index=False, startrow=0, startcol=0, header=True)

end  = datetime.datetime.now()
print("程序运行时间："+str((end-start).seconds)+"秒")


# %%
