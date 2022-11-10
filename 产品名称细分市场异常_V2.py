# import _scproxy
def xfsc():
    #coding:utf-8
    
    import pymssql
    import pandas as pd
    import numpy as np  

    import datetime
    from dateutil.relativedelta import relativedelta



    month_a = (datetime.date.today() - relativedelta(months = 13)).strftime('%Y%m')
    month_b = (datetime.date.today() - relativedelta(months = 13)).strftime('%Y-%m-01')
    month_a
    month_b


    # sql = f"SELECT distinct {cpmc},{zd_li_sql},sum({xse}) FROM {k} where SUBSTRING(REPLACE({month},'-',''),0,7) < {month_a} groupby {cpmc}"
    # # sql = 'SELECT distinct 产品名称,含糖量 FROM send_out.dbo.yakult_data_new where SUBSTRING(REPLACE(month,'-',''),0,7) < {month_a}'
    # pd.DataFrame(sql_connect(sql = sql),columns=['a'])


    def sql_connect(server = '192.168.0.15',user='zhongxin_yanfa',password='Xin_yanfa',sql = None):
        
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


    df = pd.read_excel('模版/细分市场规则表.xlsx')
    df['包含内容1'] = df['包含内容1'].fillna('一二三')
    df_merge = df[['数据库名','大类','客户','制造商','月份','销售额']].drop_duplicates()


    print(set(df['客户'].to_list()))


    us_i = input('输入要运行的 [客户名称] 或输入 [all] 全部运行:')


    yes_ = pd.merge(
        
            df.groupby(by=['数据库名','产品名称','判断字段','字段内容','月份','销售额']).agg({'包含内容1':','.join}).reset_index()
            ,df[(df['包含内容2'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容','月份','销售额']).agg({'包含内容2':','.join}).reset_index()
    ,on=['数据库名','产品名称','判断字段','字段内容','月份','销售额'],how='left')


    no_ = pd.merge(
            df[(df['不包含内容1'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容1':','.join}).reset_index()
            ,df[(df['不包含内容2'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容2':','.join}).reset_index()
    ,on=['数据库名','产品名称','判断字段','字段内容'],how='left'
    ).merge(
            df[(df['不包含内容3'].notnull())].groupby(by=['数据库名','产品名称','判断字段','字段内容']).agg({'不包含内容3':','.join}).reset_index()
        
    ,on=['数据库名','产品名称','判断字段','字段内容'],how='left')


    gz_li = yes_.merge(no_,how='left',on=['数据库名','产品名称','判断字段','字段内容'])





    if 'all' in us_i:
        sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
    else:
        sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()


    # def lg_df(k):
    #     pc_list = []
    #     gz_df = gz_li[gz_li['数据库名'] == k]
    #     zd_li = gz_df['判断字段'].drop_duplicates().to_list()
    #     zd_li_sql = ','.join(zd_li)
    #     cpmc = gz_li[gz_li['数据库名'] == k]['产品名称'].drop_duplicates().values[0]
    #     sql = f'SELECT distinct cast({cpmc} as nvarchar (2000)),{zd_li_sql} FROM {k}'
    #     sql_df = pd.DataFrame(sql_connect(sql = sql),columns=[cpmc]+zd_li)
    #     return sql_df


    def if_na(x):
        if pd.isnull(x):
        # if x == '0':
            return False
        else:
            return x.split(',')


    import itertools
    res = []
    from tqdm import tqdm
    from time import sleep
    xse_li = []
    for k in tqdm(sjk_li):
        tqdm.write(k)
        sleep(0.05)
        pc_list = []
        res_df_li = []
        gz_df = gz_li[gz_li['数据库名'] == k]
        zd_li = gz_df['判断字段'].drop_duplicates().to_list()
        zd_li_sql = ','.join(zd_li)
        cpmc = gz_li[gz_li['数据库名'] == k]['产品名称'].drop_duplicates().values[0]
        month = gz_li[gz_li['数据库名'] == k]['月份'].drop_duplicates().values[0]
        xse = gz_li[gz_li['数据库名'] == k]['销售额'].drop_duplicates().values[0]
        
        sql_xse = f"SELECT distinct {cpmc},sum({xse}) FROM {k} where SUBSTRING(REPLACE({month},'-',''),0,7) < {month_a} GROUP BY {cpmc}"
        sql = f"SELECT distinct {cpmc},{zd_li_sql} FROM {k} where SUBSTRING(REPLACE({month},'-',''),0,7) < {month_a}"
        
        try:
            sql_df = pd.DataFrame(sql_connect(sql = sql),columns=[cpmc]+zd_li)
            xse_li.append(pd.DataFrame(sql_connect(sql = sql_xse),columns=['产品名称','销售额']).assign(数据库名 = k))
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
            
            if '一二三' not in set(bhnr_1):
                for i in set(bhnr_1):
                    lg_df[cpmc].map(lambda x:ts_li.append(x) if i in x else x)
                ts_li = list(set(lg_df[cpmc]).difference(set(ts_li)))
            else:
                ts_li = lg_df[cpmc].to_list()
                    
            # pd.DataFrame(set(ts_li))
            # pd.DataFrame(ts_li)
            
            if ts_li:
                if bhnr_2:
                    a_1 = []
                    for i in bhnr_2:
                        a_1.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_1))))
                    
                if bbhnr_1 and len(ts_li) >= 1:
                    a_2 = []
                    for i in bbhnr_1:
                        a_2.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_2))))

                if bbhnr_2 and len(ts_li) >= 1:
                    a_3 = []
                    for i in bbhnr_2:
                        a_3.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_3))))
                    
                if bbhnr_3 and len(ts_li) >= 1:
                    a_4 = []
                    for i in bbhnr_3:
                        a_4.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x,ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_4))))
                        
                # print(len(list(itertools.chain.from_iterable(a))))
                
                    
            ls_df = pd.DataFrame(ts_li,columns =['产品名称']).assign(数据库名= k).merge(df_merge,how='left',on='数据库名')

            
            # if len(ls_df) >= 1:
            #     # sql_x = sql_xse.rename(columns = ['产品名称','销售额'])
            #     ls_df = ls_df.merge(sql_xse,on = ['产品名称'],how='left')
            
            
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


    yc_data = yc_data.merge(pd.concat(xse_li,axis=0),on=['数据库名','产品名称'],how='left')


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


    yc_data['应包含内容'] = yc_data['应包含内容'].str.replace('一二三', '').str.replace("''", '').str.replace(",", '').str.replace("[", '').str.replace("]", '')
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



    yc_data['字段内容'].value_counts()




import traceback
import logging

logging.basicConfig(filename='细分市场检查.log')

try:
    xfsc()
except:
    s = traceback.format_exc()
    print('Error:已停止运行,请查看log')
    logging.error(s)    
