# import _scproxy

def yiduiduo(us_=1):
    #coding:utf-8
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


    if type(us_) == str and us_ != 'all':
        kehu =  us_
        df=df1[df1['客户名']== kehu]
    else:
        if us_ != 0 and us_ != 'all':
            kehu=input('a请键入要运行的库户名称 可运行全部 [all]:')
            if kehu !='all':
                df=df1[df1['客户名']== kehu]
            else:
                df=df1
                kehu = 'all'
        else:
            df=df1
            kehu = 'all'
            # print('a')
            
            
            
            
            
            
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

    print('文件已输出,请到 [结果] 文件下获取')
    
    
def guigetaozhuang():
    # coding=utf-8
    # import _scproxy
    import numpy as np
    import pandas as pd
    import re
    import pymssql
    import datetime
    import warnings
    warnings.filterwarnings("ignore")

    def is_number(s):
        try:
            float(s)
            return True
        except ValueError:
            pass
        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass
        return False
    mssql = pymssql.connect ('192.168.0.15', 'zhongxin_yanfa', 'Xin_yanfa', 'info')
    cs0 = mssql.cursor()
    start  = datetime.datetime.now()


    from tqdm import tqdm
    from time import sleep

    import os
    def mkdir(path):
            folder = os.path.exists(path)
            if not folder:    
                    os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                    print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户规格套装数判断.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
                    input('放置后确认将运行')
            else:
                    # print('正在存放至 [模版] 📁')
                    pass
    mkdir('模版')

    namegz = pd.read_excel(r'模版/客户规格套装数判断.xlsx',sheet_name=['数据库及字段名'])
    kehudf = namegz['数据库及字段名']
    paochu_group1 = []
    yichang_group1= []
    a_li=[]
    for i in tqdm(range(len(kehudf))):
        Account = kehudf.loc[i,:].tolist()[-7:]
        fillcol = kehudf.loc[i,:].tolist()[1:6]
        print("Account=",i,Account)
        # print("fill=",fillcol)
    # #####备注：单规格是带单位的
    # 冰品
        sql0="SELECT distinct cast("+ Account[2] + " as NVARCHAR(1000)) 产品名称,cast("+ Account[3] + " as NVARCHAR(1000)) 单位包装规格," \
            " cast("+ Account[4] + " as float(1)) 套装数,cast("+ Account[5] + " as float(1))  规格数,cast("+ Account[6] + " as NVARCHAR(1000)) 制造商 from "  + Account[0] +"  "
        # print(sql0)
        count0 = cs0.execute(sql0)
        a0 = cs0.fetchall()
        a=pd.DataFrame(a0,columns=['产品名称','单规格','套装数','总规格','制造商'])
        a['数据库名']=fillcol[2]
        a['客户'] = fillcol[0]
        a['品类'] = fillcol[1]
        a['单规格是否带单位'] = fillcol[3]
        if a['单规格是否带单位'][0] == '否':
            a = a.astype({'单规格': 'float' })
        a = a.astype({'总规格': 'float'})
        for i in range(len(a)):
            # a['总规格'][i]=a['总规格'][i].format('总规格','0.1f')
            a['总规格'][i] = round(a['总规格'][i],1)
            # print(a['总规格'][i])

        # cs0.close()
        # mssql.close()
        # print (a)
        a0=a['产品名称']
        # name=a0.iloc[:,0]
        # name_old=a0.iloc[:,0]
        name=a0
        name_old=a0
        # print(name)
        name=np.array(name)

        paochu=[]
        yichang=[]
        for s in range(len(name)):
            str_name0=name[s]
            # print(str_name0)
            try:
                pattern0 = re.compile(r'\*\d*\*\d*')
                pattern = re.compile(r'\d+ml|\d+g|\d+\.\d+g|\d+\.\d+ml')  # 匹配规格
                pattern2 = re.compile(r'\dml.\d*|\dg.\d*|\dml.|\dg.')  # 匹配套装数
                str_name原 = str_name0

                str_name00 = str_name0.replace('3.8g乳蛋白', '乳蛋白').replace('3.5g乳蛋白', '乳蛋白').replace('3.6g乳蛋白', '乳蛋白').replace(
                    '3.3g乳蛋白', '乳蛋白').replace('3.6g蛋白', '蛋白').replace('*+', '+').replace('M', 'm').replace('L', 'l').replace('G', 'g').replace('5g蛋白', '蛋白').\
                    replace('3.8g纯牛奶', '纯牛奶').replace('3.6g纯牛奶', '纯牛奶').replace('3.3g纯牛奶', '纯牛奶').replace('3.6g 纯牛奶', '纯牛奶').replace('3.3g 纯牛奶', '纯牛奶')\
                    .replace('3.2g纯牛奶', '纯牛奶').replace('八克白 30g', '八克白').replace('八克白 14g', '八克白').\
                    replace('106℃', '').replace('八克白 5g', '八克白').replace('八克白 21g', '八克白').replace('遵义 5.7g', '').\
                    replace(' 2018世界杯20周年珍藏版', '').replace('9mlk', '').replace('33d', '').replace('ha 3gopobbe', '').replace('3.7g倍鲜', '').replace('卡士 3.3g', '')
                for i in re.compile(r'([^*|+|*\d+|+\d+]\d+[^ \d+ml| \d+g|\d*|\d+\.g|\d+\.ml]+)').findall(str_name00):
                    str_name00 = str_name00.replace(i, '')
                if str_name00.endswith("ml") or str_name00.endswith("g"):
                    str_name = str_name00 + '*1'
                else:
                    str_name = str_name00

                result0 = pattern0.findall(str_name)

                # 对两个*的处理开始
                # print("result0=", result0)
                shaungxing = []
                shaungxing_yuan = []
                if len(result0) > 0:
                    for s in result0:
                        s_yuan = s[1:]
                        s = str(s[1:]).split("*")
                        shaungxing.append(s)
                        shaungxing_yuan.append(s_yuan)

                    # print("shaungxing=", shaungxing)
                    # print("shaungxing_yuan=", shaungxing_yuan)
                    shaungxing_jisuan = []
                    for i in shaungxing:
                        i = int(i[0]) * int(i[1])
                        shaungxing_jisuan.append(i)
                    # print("shaungxing_jisuan=", shaungxing_jisuan)
                    # 双星替换
                    for i in range(len(shaungxing_yuan)):
                        # print("shaungxing_yuan[i]=", shaungxing_yuan[i])
                        # print("shaungxing_jisuan[i]=", shaungxing_jisuan[i])
                        str_name1 = str_name.replace(str(shaungxing_yuan[i]), str(shaungxing_jisuan[i]))
                        str_name = str_name1
                    # print(str_name1)
                else:
                    str_name1 = str_name
                # -------------对两个*的处理结束
                result1 = pattern.findall(str_name1)
                result2 = pattern2.findall(str_name1)
                # print("result2=", result2)
                # print("result1=", result1)
                result3 = []
                for i in result2:
                    if i[0] in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'):
                        # print('i[0]=', i[0])
                        i = i[1:]
                        result3.append(i)

                # print("result3=", result3)
                result1_new = [str(result1[index]).replace('ml', '').replace('g', '') for index, value in enumerate(result1)]

                replace_dict = {'ml+': '1','g+':'1','ml ': '1','g ': '1', 'ml*': '', 'g*': ''}
                new_result = [str(replace_dict[i]) if i in replace_dict else i for i in result3]
                result2_new = [str(new_result[index]).replace('g*', '').replace('ml*', '') for index, value in
                            enumerate(new_result)]

                # 类型转换
                result_unit = [float(x) for x in result1_new]
                result_ru = [float(x) for x in result2_new]

                # print("result_unit=", result_unit)
                # print("result_ru=", result_ru)
                # if result_ru == []:
                #     result_ru = [1]
                func = lambda x, y: x * y
                result = map(func, result_unit, result_ru)
                result_guige = list(result)
                result_zongguige = str(round(float(sum(result_guige)),1))
                # print("result_zongguige=", result_zongguige)
                # print("result_guige=", result_guige)
                # print("a['单规格是否带单位']=", a['单规格是否带单位'][0])
                if a['单规格是否带单位'][0]=='是':
                    result_danguige = result1[0]
                else :
                    result_danguige = round(result_unit[0],1) #代表单位规格去掉单位
                #未去掉单位
                # print("result_danguige=", result_danguige)
                result_taozhuangshu = str(round(float(sum(result_ru)),1))

                result = [fillcol[0],fillcol[1],fillcol[2],fillcol[3],str_name原, str_name1, result_danguige, result_taozhuangshu, result_zongguige]
                paochu.append(result)

            except:
                result_yichang = [fillcol[0], fillcol[1], fillcol[2],fillcol[3], str_name原]
                yichang.append(result_yichang)
                # break
            continue
        jieguo = pd.DataFrame(paochu, columns=['客户', '品类', '数据库名', '单规格是否带单位', '产品名称', '产品名称1', '单规格结果', '套装数结果', '总规格结果'])
        yichang_group = pd.DataFrame(yichang, columns=['客户', '品类', '数据库名', '单规格是否带单位','产品名称'])

        a_li.append(a)
        yichang_group1.append(yichang_group)
        paochu_group1.append(jieguo)

    a = pd.concat(a_li)
    jieguo = pd.concat(paochu_group1)
    yichang_group = pd.concat(yichang_group1)

    pipei = pd.merge(left=a, right=jieguo, on=['产品名称','数据库名','客户','品类', '单规格是否带单位'], how="left")
    pipei = pipei.astype({'单规格': 'str', '套装数': 'str', '总规格': 'str', '单规格结果': 'str', '套装数结果': 'str', '总规格结果': 'str'})
    pipei_result= pipei[(pipei['单规格'] != pipei['单规格结果'])|
                        (pipei['套装数'] != pipei['套装数结果'])|
                        (pipei['总规格'] != pipei['总规格结果'])][['客户', '品类', '数据库名', '制造商','产品名称', '单规格是否带单位','单规格', '套装数', '总规格','单规格结果', '套装数结果', '总规格结果' ]]

    pipei_result.drop_duplicates()
    # print (pipei_result)

    def mkdir(path):
            folder = os.path.exists(path)
            if not folder:    
                    os.makedirs(path)            #makedirs 创建文件时如果路径不存在会创建这个路径
                    print('检测无结果文件夹,程序将自动创建 📁')#判断是否存在文件夹如果不存在则创建为文件夹
            else:
                    print('正在存放至 [结果] 📁')
                    pass
    mkdir('结果')

    with pd.ExcelWriter('结果/规格异常-结果.xlsx') as mc_writer:
        # jieguo.to_excel(mc_writer, sheet_name='计算结果', na_rep='', index=False, startrow=0, startcol=0, header=True)
        yichang_group.to_excel(mc_writer, sheet_name='异常产品名称',na_rep='',index=False,startrow=0,startcol=0,header=True)
        pipei_result.to_excel(mc_writer, sheet_name='匹配不一致', na_rep='', index=False, startrow=0, startcol=0, header=True)
        kehudf.to_excel(mc_writer, sheet_name='数据库及字段名', na_rep='', index=False, startrow=0, startcol=0, header=True)

    end  = datetime.datetime.now()

    print("程序运行时间："+str((end-start).seconds)+"秒")



def ziduanyouxiaoxing(us_ = 1):
    #coding:utf-8
    # import _scproxy
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
                    print('检测无 [模版] 文件夹,程序将自动创建,请将模版( 客户字段及内容_有效性.xlsx )放置到此处')#判断是否存在文件夹如果不存在则创建为文件夹
                    input('放置后确认将运行')
            else:
                    # print('正在存放至 [模版] 📁')
                    pass
    mkdir('模版')

    df = pd.read_excel(r'模版/客户字段及内容_有效性.xlsx')
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

    if type(us_) == str and us_ != 'all':
        kehu =  us_
        df=df1[df1['客户名']== kehu]
    else:
        if us_ != 0 and us_ != 'all':
            kehu=input('a请键入要运行的库户名称 可运行全部 [all]:')
            df=df1[df1['客户名']== kehu]
            
            if kehu !='all':
                df=df1[df1['客户名']== kehu]
            else:
                df=df1
                kehu = 'all'
        else:
            df=df1
            kehu = 'all'
            # print('a')




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
            sqllis = [
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(重点品牌 as nvarchar)as 重点品牌, cast(品类 as nvarchar)as 品类, cast(商品品类 as nvarchar)as 商品品类, cast(包装 as nvarchar)as 包装, cast(是否进口 as nvarchar)as 是否进口, cast(店铺类型 as nvarchar)as 店铺类型,cast(规格分组 as nvarchar)as 规格分组, cast(价格分组 as nvarchar)as 价格分组   from send_out.dbo.HJ_DATA_BFJ_NEW ",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(店铺类型 as nvarchar)as 店铺类型, cast(重点品牌 as nvarchar)as 重点品牌, cast(品类 as nvarchar)as 品类, cast(商品品类 as nvarchar)as 商品品类, cast(适用人群 as nvarchar)as 适用人群, cast(是否有机 as nvarchar)as 是否有机, cast(包装 as nvarchar)as 包装, cast(是否减盐 as nvarchar)as 是否减盐, cast(是否进口 as nvarchar)as 是否进口, cast(是否零添加 as nvarchar)as 是否零添加, cast(规格分组 as nvarchar)as 规格分组, cast(价格分组 as nvarchar)as 价格分组   from send_out.dbo.HJ_DATA_JIANGYOU_NEW",
                    "select distinct cast(平台名称 as nvarchar)as 平台名称,cast(价格区间 as nvarchar)as 价格区间,cast(PLATFORM_ID as nvarchar)as PLATFORM_ID,cast(品类 as nvarchar)as 品类,cast(店铺类型 as nvarchar)as 店铺类型,cast(混合店铺类型 as nvarchar)as 混合店铺类型,cast(单规格分组 as nvarchar)as 单规格分组,cast(产品类型 as nvarchar)as 产品类型,cast(包装 as nvarchar)as 包装  from send_out.dbo.JIALESHI_DATA_MAIPIAN_E_NEW",
                    "select distinct cast(平台 as nvarchar)as 平台,cast(店铺类型 as nvarchar)as 店铺类型,cast(适用季节 as nvarchar)as 适用季节,cast(是否防爆 as nvarchar)as 是否防爆,cast(自修复 as nvarchar)as 自修复,cast(是否静音 as nvarchar)as 是否静音 from send_out.dbo.luntai_Continental",
                    "select distinct cast(平台 as nvarchar)as 平台,cast(品类 as nvarchar)as 品类 from send_out.dbo.HJ_TOP品牌_醋料酒",
                    "select distinct cast(平台 as nvarchar)as 平台,cast(品类 as nvarchar)as 品类 from send_out.dbo.HJ_平台_醋料酒",
                    "select distinct cast(platform_id as nvarchar)as platform_id, cast(平台 as nvarchar)as 平台, cast(子品类 as nvarchar)as 子品类, cast(店铺类型 as nvarchar)as 店铺类型, cast(脂肪含量 as nvarchar)as 脂肪含量, cast(是否有机 as nvarchar)as 是否有机, cast(单规格分组 as nvarchar)as 单规格分组, cast(套装数分组 as nvarchar)as 套装数分组, cast(升价格分组 as nvarchar)as 升价格分组, cast(件价格分组 as nvarchar)as 件价格分组 from item.dbo.kashi_data_new",
                    "select distinct cast(平台名称 as nvarchar)as 平台名称, cast(店铺类型 as nvarchar)as 店铺类型, cast(品类 as nvarchar)as 品类, cast(是否进口 as nvarchar) as 是否进口, cast(包装类型 as nvarchar) as 包装类型, cast(单规格分组 as nvarchar)as 单规格分组, cast([价格分组/L] as nvarchar)as [价格分组/L], cast(套装数分组 as nvarchar)as 套装数分组, cast([价格分组/件] as nvarchar)as [价格分组/件], cast(功能饮料细分 as nvarchar)as 功能饮料细分, cast(贸易模式 as nvarchar)as 贸易模式, cast(生产工艺 as nvarchar)as 生产工艺  from send_out.dbo.REDBULL_DATA_NEW",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(平台名称 as nvarchar)as 平台名称, cast(模式 as nvarchar)as 模式, cast(店铺类型 as nvarchar)as 店铺类型, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(混合店铺类型 as nvarchar)as 混合店铺类型, cast(品类 as nvarchar)as 品类, cast(是否有促销 as nvarchar)as 是否有促销, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(价格分组 as nvarchar)as 价格分组, cast(目标人群 as nvarchar)as 目标人群, cast(品牌所属地 as nvarchar)as 品牌所属地 , cast(是否有机 as nvarchar)as 是否有机, cast(是否含糖 as nvarchar)as 是否含糖, cast(产品种类 as nvarchar)as 产品种类, cast(YILI_SHOPTYPE as nvarchar)as YILI_SHOPTYPE, cast(产品包装规格 as nvarchar)as 产品包装规格, cast(钙质 as nvarchar)as 钙质, cast(特殊功能 as nvarchar)as 特殊功能, cast(ANIMAL as nvarchar)as ANIMAL from send_out.dbo.anjia_data_cn_new",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(平台名称 as nvarchar)as 平台名称, cast(模式 as nvarchar)as 模式, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(混合店铺类型 as nvarchar)as 混合店铺类型, cast(品类 as nvarchar)as 品类, cast(子品类 as nvarchar)as 子品类, cast(是否有促销 as nvarchar)as 是否有促销, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(价格分组 as nvarchar)as 价格分组, cast(总规格分组 as nvarchar)as 总规格分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(适用人群 as nvarchar)as 适用人群, cast([Imported or Local] as nvarchar)as [Imported or Local], cast(含盐量 as nvarchar)as 含盐量, cast(包装类型 as nvarchar)as 包装类型, cast(口味 as nvarchar)as 口味 , cast(成分 as nvarchar)as 成分  from send_out.dbo.anjia_data_huangyou_new",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(平台名称 as nvarchar)as 平台名称, cast(模式 as nvarchar)as 模式, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(混合店铺类型 as nvarchar)as 混合店铺类型, cast(品类 as nvarchar)as 品类, cast(子品类 as nvarchar)as 子品类, cast(是否有促销 as nvarchar)as 是否有促销, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(价格分组 as nvarchar)as 价格分组, cast(总规格分组 as nvarchar)as 总规格分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(安佳适用人群 as nvarchar)as 安佳适用人群, cast(是否进口 as nvarchar)as 是否进口, cast(产品种类 as nvarchar)as 产品种类, cast(档位 as nvarchar)as 档位, cast(钙含量 as nvarchar)as 钙含量 , cast(脂肪含量 as nvarchar)as 脂肪含量, cast(安佳口味 as nvarchar)as 安佳口味, cast(供货方式 as nvarchar)as 供货方式, cast(安佳奶酪形状 as nvarchar)as 安佳奶酪形状, cast(安佳奶酪分类 as nvarchar)as 安佳奶酪分类, cast(产品形态 as nvarchar)as 产品形态  from send_out.dbo.anjia_data_naiyou_new",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(CHANNEL as nvarchar)as CHANNEL, cast(SEGMENT as nvarchar)as SEGMENT, cast([Imported/local] as nvarchar)as [Imported/local], cast(D_P_PACKINGTYPE as nvarchar)as D_P_PACKINGTYPE, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(D_C_PLATFORM_EN as nvarchar)as D_C_PLATFORM_EN, cast(D_P_CATEGORY_EN as nvarchar)as D_P_CATEGORY_EN, cast(D_P_PACKINGQUANTITYGROUP as nvarchar)as D_P_PACKINGQUANTITYGROUP, cast(D_P_PACKINGTYPE_EN as nvarchar)as D_P_PACKINGTYPE_EN , cast(PRICELEVEL as nvarchar)as PRICELEVEL, cast(运动饮料细分 as nvarchar)as 运动饮料细分, cast(能量饮料价格分组 as nvarchar)as 能量饮料价格分组, cast(能量饮料规格分组 as nvarchar)as 能量饮料规格分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY from send_out.dbo.BAISHI_COKE_E_NEW",
                    "select distinct cast(PLATFORM as nvarchar)as PLATFORM, cast(SUBSEGMENT as nvarchar)as SUBSEGMENT, cast(SEGMENT as nvarchar)as SEGMENT, cast([Imported/local] as nvarchar)as [Imported/local], cast(ORGANIC as nvarchar)as ORGANIC, cast(PACKAGING as nvarchar)as PACKAGING, cast(FLAVOR as nvarchar)as FLAVOR, cast(CHANNEL as nvarchar)as CHANNEL, cast(CHANNEL_TYPE as nvarchar)as CHANNEL_TYPE, cast(CHANNEL_TYPE_NEW as nvarchar)as CHANNEL_TYPE_NEW, cast(子品类 as nvarchar)as 子品类  from send_out.dbo.BAISHI_DATA_MAIPIAN_E_NEW",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_P_SUB_CATEGORY as nvarchar)as D_P_SUB_CATEGORY, cast(是否国产 as nvarchar)as 是否国产, cast(D_P_PACKINGTYPE as nvarchar)as D_P_PACKINGTYPE, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(供货方式 as nvarchar)as 供货方式, cast(D_C_PLATFORM_EN as nvarchar)as D_C_PLATFORM_EN, cast(D_P_CATEGORY_EN as nvarchar)as D_P_CATEGORY_EN, cast(D_P_PACKINGQUANTITYGROUP as nvarchar)as D_P_PACKINGQUANTITYGROUP, cast(D_P_PACKINGTYPE_EN as nvarchar)as D_P_PACKINGTYPE_EN, cast(平均成交价分组 as nvarchar)as 平均成交价分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY from send_out.dbo.COKE_E_NEW",
                    "select distinct cast(SEASONALITY as nvarchar)as SEASONALITY, cast([SHOP TYPE] as nvarchar)as [SHOP TYPE], cast(PLATFORM as nvarchar)as PLATFORM, cast([RUN FLAT] as nvarchar)as [RUN FLAT], cast([SEAL INSIDE] as nvarchar)as [SEAL INSIDE], cast(NCS as nvarchar)as NCS, cast(XL as nvarchar)as XL  from send_out.dbo.LUNTAI_BEINAILI_CHUSHU_ZHONG",
                    "select distinct cast(CUSTOMER as nvarchar)as CUSTOMER, cast(PLATFORM as nvarchar)as PLATFORM, cast(TYPE as nvarchar)as TYPE, cast([RSC Y/N] as nvarchar)as [RSC Y/N], cast(SEASONAL as nvarchar)as SEASONAL, cast(NEW_PATTERN as nvarchar)as NEW_PATTERN, cast(轮胎技术 as nvarchar)as 轮胎技术  from send_out.dbo.LUNTAI_DATA_NEW ",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(PLATFORM_NAME as nvarchar)as PLATFORM_NAME, cast(CATEGORY_ID as nvarchar)as CATEGORY_ID, cast(CATEGORY_ST as nvarchar)as CATEGORY_ST, cast(商品类型 as nvarchar)as 商品类型, cast(套装数分组 as nvarchar)as 套装数分组, cast(件价格分组 as nvarchar)as 件价格分组 , cast(升价格分组 as nvarchar)as 升价格分组, cast(包装分组 as nvarchar)as 包装分组, cast(口味 as nvarchar)as 口味 from send_out.dbo.MN_DATA_DIWENrsj_NEW",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(PLATFORM_NAME as nvarchar)as PLATFORM_NAME, cast(CATEGORY_ID as nvarchar)as CATEGORY_ID, cast(CATEGORY_ST as nvarchar)as CATEGORY_ST, cast(子品类 as nvarchar)as 子品类, cast(商品类型 as nvarchar)as 商品类型, cast(套装数分组 as nvarchar)as 套装数分组 , cast(件价格分组 as nvarchar)as 件价格分组, cast(升价格分组 as nvarchar)as 升价格分组, cast(包装分组 as nvarchar)as 包装分组, cast(是否有机 as nvarchar)as 是否有机, cast(口味 as nvarchar)as 口味, cast(脂肪含量 as nvarchar)as 脂肪含量, cast(含糖量 as nvarchar)as 含糖量 from send_out.dbo.MN_DATA_DIWENSUAN_NEW",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(PLATFORM_NAME as nvarchar)as PLATFORM_NAME, cast(CATEGORY_ID as nvarchar)as CATEGORY_ID, cast(CATEGORY_ST as nvarchar)as CATEGORY_ST, cast(商品类型 as nvarchar)as 商品类型, cast(套装数分组 as nvarchar)as 套装数分组, cast(件价格分组 as nvarchar)as 件价格分组 , cast(升价格分组 as nvarchar)as 升价格分组, cast(包装分组 as nvarchar)as 包装分组, cast(是否有机 as nvarchar)as 是否有机, cast(脂肪含量 as nvarchar)as 脂肪含量 from send_out.dbo.MN_DATA_DIWENxian_NEW",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(贸易模式 as nvarchar)as 贸易模式, cast(平台名称 as nvarchar)as 平台名称, cast(店铺类型 as nvarchar)as 店铺类型, cast(CATEGORY_ID as nvarchar)as CATEGORY_ID, cast(品类 as nvarchar)as 品类, cast(是否国产 as nvarchar)as 是否国产 , cast(包装类型 as nvarchar)as 包装类型, cast(是否有机 as nvarchar)as 是否有机, cast(钙含量 as nvarchar)as 钙含量, cast(适用人群 as nvarchar)as 适用人群 , cast(脂肪含量 as nvarchar)as 脂肪含量, cast(是否含糖 as nvarchar)as 是否含糖, cast(价格区间 as nvarchar)as 价格区间, cast(包装形式 as nvarchar)as 包装形式 from send_out.dbo.MN_DATA_YTN_NEW ",
                    "select distinct cast(平台 as nvarchar)as 平台, cast(店铺类型 as nvarchar)as 店铺类型, cast(主机版本 as nvarchar)as 主机版本, cast(主机名 as nvarchar)as 主机名, cast(产品 as nvarchar)as 产品, cast(官方配件 as nvarchar)as 官方配件 from send_out.dbo.SWITCH",
                    "select distinct cast(平台 as nvarchar)as 平台, cast(店铺类型 as nvarchar)as 店铺类型 from send_out.dbo.SWITCH_GAME",
                    "select distinct cast(平台名称 as nvarchar)as 平台名称, cast(店铺类型 as nvarchar)as 店铺类型, cast(品类 as nvarchar)as 品类, cast(子品类 as nvarchar)as 子品类, cast(是否进口 as nvarchar)as 是否进口, cast(包装类型 as nvarchar)as 包装类型, cast(单容量段 as nvarchar)as 单容量段, cast(总容量段 as nvarchar)as 总容量段, cast(价格段 as nvarchar)as 价格段, cast(类目 as nvarchar)as 类目, cast(品类_VITA as nvarchar)as 品类_VITA, cast(套装数分组 as nvarchar)as 套装数分组 from send_out.dbo.VITA_DATA_NEW ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_SHOPTYPE_NEW as nvarchar)as D_C_SHOPTYPE_NEW, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_LEVEL as nvarchar)as D_P_LEVEL, cast(D_P_PACKAGETYPE as nvarchar)as D_P_PACKAGETYPE, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(D_P_ORGANIC as nvarchar)as D_P_ORGANIC, cast(M_S_PRCIELEVEL as nvarchar)as M_S_PRCIELEVEL, cast(套装数分组 as nvarchar)as 套装数分组, cast(是否国产 as nvarchar)as 是否国产, cast(特殊品类 as nvarchar)as 特殊品类, cast(D_C_TRADE_NEW as nvarchar)as D_C_TRADE_NEW, cast(D_C_TRADE_NEW_1 as nvarchar)as D_C_TRADE_NEW_1, cast(D_C_SHOPTYPE_NEW_1 as nvarchar)as D_C_SHOPTYPE_NEW_1, cast(D_C_NEWSHOPTYPE as nvarchar)as D_C_NEWSHOPTYPE from send_out.dbo.XIBAO_DATA_593_E_MONTH_NEW ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_P_SUBCATEGORY as nvarchar)as D_P_SUBCATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_PACKAGETYPE as nvarchar)as D_P_PACKAGETYPE, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(价格分组 as nvarchar)as 价格分组, cast(套装数分组 as nvarchar)as 套装数分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(SHOP_CATEGORY_NEW as nvarchar)as SHOP_CATEGORY_NEW, cast(是否进口 as nvarchar)as 是否进口, cast(渠道 as nvarchar)as 渠道, cast(新店铺类型 as nvarchar)as 新店铺类型  from send_out.dbo.YILI_DATA_BINGPIN_E_MONTH_NEW ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_SUBCATEGORY as nvarchar)as D_P_SUBCATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_TOTALGAUGEGROUP as nvarchar)as D_P_TOTALGAUGEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(是否有机 as nvarchar)as 是否有机, cast(D_P_PRICELEVEL as nvarchar)as D_P_PRICELEVEL, cast(脂肪含量 as nvarchar)as 脂肪含量, cast(套装数分组 as nvarchar)as 套装数分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(渠道 as nvarchar)as 渠道, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(蛋白质 as nvarchar)as 蛋白质   from send_out.dbo.YILI_DATA_DIWEN_XIAN_E_MONTH_NEW ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_P_SUBCATEGORY as nvarchar)as D_P_SUBCATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_TOTALGAUGEGROUP as nvarchar)as D_P_TOTALGAUGEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(价格分组 as nvarchar)as 价格分组, cast(适用人群 as nvarchar)as 适用人群, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(SHOP_CATEGORY_NEW as nvarchar)as SHOP_CATEGORY_NEW, cast(是否进口 as nvarchar)as 是否进口, cast(渠道 as nvarchar)as 渠道, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(类型 as nvarchar)as 类型, cast(奶酪形状 as nvarchar)as 奶酪形状, cast(分类 as nvarchar)as 分类, cast(奶酪分类 as nvarchar)as 奶酪分类, cast(奶酪一级分类 as nvarchar)as 奶酪一级分类, cast(奶酪二级分类 as nvarchar)as 奶酪二级分类   from send_out.dbo.YILI_DATA_NAIYOU_E_MONTH_NEW ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_P_SUBCATEGORY as nvarchar)as D_P_SUBCATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_PACKAGETYPE as nvarchar)as D_P_PACKAGETYPE, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(是否有机 as nvarchar)as 是否有机, cast(D_P_PRICELEVEL as nvarchar)as D_P_PRICELEVEL, cast(适用人群 as nvarchar)as 适用人群, cast(脂肪含量 as nvarchar)as 脂肪含量, cast(套装数分组 as nvarchar)as 套装数分组, cast(新套装分组 as nvarchar)as 新套装分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(SHOP_CATEGORY_NEW as nvarchar)as SHOP_CATEGORY_NEW, cast(是否国产 as nvarchar)as 是否国产, cast(品牌是否进口 as nvarchar)as 品牌是否进口, cast(特殊品类 as nvarchar)as 特殊品类, cast(渠道 as nvarchar)as 渠道, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(乳糖含量 as nvarchar)as 乳糖含量, cast(钙含量 as nvarchar)as 钙含量   from send_out.dbo.YILI_DATA_YTN_E_MONTH_NEW ",
                    "select distinct cast(PLATFORM_ID as nvarchar)as PLATFORM_ID, cast(平台名称 as nvarchar)as 平台名称, cast(模式 as nvarchar)as 模式, cast(店铺类型 as nvarchar)as 店铺类型, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(混合店铺类型 as nvarchar)as 混合店铺类型, cast(品类 as nvarchar)as 品类, cast(是否有促销 as nvarchar)as 是否有促销, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(价格分组 as nvarchar)as 价格分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(目标人群 as nvarchar)as 目标人群, cast(进出口 as nvarchar)as 进出口, cast(是否有机 as nvarchar)as 是否有机, cast(乳糖含量 as nvarchar)as 乳糖含量, cast(包装类型分组 as nvarchar)as 包装类型分组, cast(产品种类 as nvarchar)as 产品种类, cast(YILI_SHOPTYPE as nvarchar)as YILI_SHOPTYPE, cast(产品包装规格分组 as nvarchar)as 产品包装规格分组, cast(产品档次 as nvarchar)as 产品档次, cast(蛋白含量 as nvarchar)as 蛋白含量, cast(添加额外营养 as nvarchar)as 添加额外营养  from send_out.dbo.anjia_data_ytn_new ",
                    "select distinct cast(D_C_TRADE as nvarchar)as D_C_TRADE, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE, cast(D_C_PLATFORM as nvarchar)as D_C_PLATFORM, cast(D_C_SHOPTYPE as nvarchar)as D_C_SHOPTYPE, cast(D_C_MIXEDSHOPTYPE as nvarchar)as D_C_MIXEDSHOPTYPE, cast(D_P_SUBCATEGORY as nvarchar)as D_P_SUBCATEGORY, cast(D_S_PROMOTION as nvarchar)as D_S_PROMOTION, cast(D_P_PRICEGROUP as nvarchar)as D_P_PRICEGROUP, cast(D_P_TOTALGAUGEGROUP as nvarchar)as D_P_TOTALGAUGEGROUP, cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP, cast(D_P_FLAVOR as nvarchar)as D_P_FLAVOR, cast(D_P_PRICELEVEL as nvarchar)as D_P_PRICELEVEL, cast(D_P_PRICELEVEL_UNIT as nvarchar)as D_P_PRICELEVEL_UNIT, cast(脂肪含量 as nvarchar)as 脂肪含量, cast(套装数分组 as nvarchar)as 套装数分组, cast(SHOP_CATEGORY as nvarchar)as SHOP_CATEGORY, cast(渠道 as nvarchar)as 渠道, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(蛋白质 as nvarchar)as 蛋白质, cast(含糖量 as nvarchar)as 含糖量  from send_out.dbo.YILI_DATA_DIWEN_suan_E_MONTH_NEW ",
                    "select distinct cast(月份 as nvarchar)as 月份,cast(平台 as nvarchar)as 平台, cast(店铺名称 as nvarchar)as 店铺名称, cast(店铺类型 as nvarchar)as 店铺类型 , cast(产品名称 as nvarchar ( 1000 ))as 产品名称, cast(包装类型 as nvarchar)as 包装类型,cast(品类 as nvarchar)as 品类, cast(子品类 as nvarchar)as 子品类, cast(制造商 as nvarchar)as 制造商, cast(品牌 as nvarchar)as 品牌,cast(单规格 as nvarchar)as 单规格, cast(套装数 as nvarchar)as 套装数, cast(总规格 as nvarchar)as 总规格, cast(口味 as nvarchar)as 口味,cast(套装数分组 as nvarchar)as 套装数分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(升价格分组 as nvarchar)as 升价格分组, cast([销量(件)] as nvarchar)as [销量(件)], cast(升销量 as nvarchar)as 升销量, cast([销售额(万)] as nvarchar)as [销售额(万)],cast(升价格 as nvarchar)as 升价格 from send_out.dbo.yili_data_water_e_month_new",
                    "select distinct cast(月份 as nvarchar)as 月份,cast(平台 as nvarchar)as 平台, cast(店铺名称 as nvarchar)as 店铺名称, cast(店铺类型 as nvarchar)as 店铺类型 , cast(产品名称 as nvarchar ( 1000 ))as 产品名称, cast(包装类型 as nvarchar)as 包装类型,cast(品类 as nvarchar)as 品类, cast(是否进口 as nvarchar)as 是否进口, cast(制造商 as nvarchar)as 制造商, cast(品牌 as nvarchar)as 品牌,cast(单包装规格 as nvarchar)as 单包装规格, cast(套装数 as nvarchar)as 套装数, cast(总包装规格 as nvarchar)as 总包装规格, cast(口味 as nvarchar)as 口味,cast(套装数分组 as nvarchar)as 套装数分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(升价格分组 as nvarchar)as 升价格分组, cast([销量] as nvarchar)as [销量], cast(升销量 as nvarchar)as 升销量, cast(件价格分组 as nvarchar)as 件价格分组, cast([销售额] as nvarchar)as [销售额],cast(升价格 as nvarchar)as 升价格 from send_out.dbo.costa_data_e_new",
                    "select distinct cast(D_T_MONTH as nvarchar)as D_T_MONTH,cast(D_T_YTD as nvarchar)as D_T_YTD,cast(D_T_MAT as nvarchar)as D_T_MAT, cast(D_C_BUSINESSMODE as nvarchar)as D_C_BUSINESSMODE , cast(D_C_PLATFORM as nvarchar ( 1000 ))as D_C_PLATFORM, cast(D_C_SHOPNAME as nvarchar)as D_C_SHOPNAME,cast(店铺类型 as nvarchar)as 店铺类型, cast(新店铺类型 as nvarchar)as 新店铺类型, cast(D_P_CATEGORY as nvarchar)as D_P_CATEGORY, cast(D_P_BRAND as nvarchar)as D_P_BRAND,cast(D_P_PRODUCTNAME as nvarchar)as D_P_PRODUCTNAME,cast(D_P_UNITGAUGE as nvarchar)as D_P_UNITGAUGE, cast(D_P_TOTALGAUGE as nvarchar)as D_P_TOTALGAUGE,cast(D_P_UNITGAUGEGROUP as nvarchar)as D_P_UNITGAUGEGROUP,cast(D_P_TOTALGAUGEGROUP as nvarchar)as D_P_TOTALGAUGEGROUP, cast(价格分组 as nvarchar)as 价格分组, cast(口味 as nvarchar)as 口味, cast(M_S_SALES as nvarchar)as M_S_SALES, cast(M_S_SKUVOLUME as nvarchar)as M_S_SKUVOLUME, cast(M_S_PHYSICALVOLUME as nvarchar)as M_S_PHYSICALVOLUME,cast(是否进口 as nvarchar)as 是否进口,cast(是否夹心 as nvarchar)as 是否夹心,cast(外包装类型 as nvarchar)as 外包装类型,cast(系列 as nvarchar)as 系列 from send_out.dbo.YILI_DATA_GTRJ_E_MONTH_NEW ,"
                    "select distinct cast(month as nvarchar)as month,cast(平台 as nvarchar)as 平台, cast(店铺名称 as nvarchar)as 店铺名称, cast(店铺类型 as nvarchar)as 店铺类型 , cast(产品名称 as nvarchar ( 1000 ))as 产品名称, cast(包装类型 as nvarchar)as 包装类型,cast(品类 as nvarchar)as 品类, cast(进口国产 as nvarchar)as 进口国产, cast(制造商 as nvarchar)as 制造商, cast(品牌 as nvarchar)as 品牌,cast(单包装规格 as nvarchar)as 单包装规格, cast(套装数 as nvarchar)as 套装数, cast(规格数 as nvarchar)as 规格数, cast(动物奶源 as nvarchar)as 动物奶源,cast(升价格分组 as nvarchar)as 升价格分组, cast(单规格分组 as nvarchar)as 单规格分组, cast(件价格分组 as nvarchar)as 件价格分组, cast([销量] as nvarchar)as [销量], cast(升销量 as nvarchar)as 升销量, cast(适用人群 as nvarchar)as 适用人群, cast(是否有机 as nvarchar)as 是否有机,cast(模式 as nvarchar)as 模式 from send_out.dbo.YASHILY_DATA_594_NEW ,"
                    "select distinct cast(month as nvarchar)as month,cast(平台 as nvarchar)as 平台, cast(进口国产 as nvarchar)as 进口国产, cast(品类 as nvarchar)as 品类 , cast(产品名称 as nvarchar ( 1000 ))as 产品名称, cast(PLATFORM_ID as nvarchar)as PLATFORM_ID,cast(子品类 as nvarchar)as 子品类, cast(特殊配方 as nvarchar)as 特殊配方, cast(制造商 as nvarchar)as 制造商, cast(品牌 as nvarchar)as 品牌,cast(规格数 as nvarchar)as 规格数, cast(适用年龄段 as nvarchar)as 适用年龄段 from item.dbo.MENGNIU_DATA_SFMP_NEW   "
                    ]
            for i in sqllis:
                sql = i
                # print(sql)
                m=str(sql[sql.rfind("from "):]).replace("from","").replace(" ","")
                # print ("m=",m)
                try:
                    syntun_cursor.execute(sql)
                except:
                    print(m + ':读取失败………………', w)
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

    print('文件已输出,请到 [结果] 文件下获取')


def kongzhi(us_=1):
    #coding:utf-8
    # import _scproxy
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

    df = pd.read_excel(r'模版/客户字段及内容_空值.xlsx')
    df=df[['客户名','品类','数据库名','字段名']]
    df=df.reset_index()
    print("客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多")
    
    # 客户名：蒙牛、恒天然、喜宝、伊利、维他奶、百事可乐、红牛、桂格、可口可乐、倍耐力、马牌、固特异、任天堂、好记、家乐氏、卡士、Costa、雅士利、养乐多
    

    if type(us_) == str and us_ != 'all':
        kehu =  us_
        df=df[df['客户名']== kehu]
    else:
        if us_ != 0 and us_ != 'all':
            kehu=input('a请键入要运行的库户名称 可运行全部 [all]:')
            df=df[df['客户名']== kehu]
            
            if kehu !='all':
                df=df[df['客户名']== kehu]
            else:
                df=df
                kehu = 'all'
            
        else:
            df=df
            kehu = 'all'
            # print('a')

    print(len(df))
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

    print('文件已输出,请到 [结果] 文件下获取')
    
    
    
# import _scproxy
def xfsc(us_= 1):
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
    
    # if us_:
    #     us_i =  us_
    # else:
    #     us_i=input('请键入要运行的库户名称 可运行全部 [all]:')


        


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


    # if us_i != 'all' or len(us_) <= 1:
    #     us_i = us_i
    # else:
    #     us_i = 'all'
        
    if type(us_) == str and us_ != 'all':
        us_i =  us_
        sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()
    else:
        if us_ != 0 and us_ != 'all':
            us_i=input('a请键入要运行的库户名称 可运行全部 [all]:')
            sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()
        
            if us_i !='all':
                sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()
            else:
                sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
                # kehu = 'all'
        
        else:
            sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
            # print('a')    
            
    #   if 'all' in us_i:
    #     sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
    # else:
    #     sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()

        



    # if 'all' in us_i:
    #     sjk_li = gz_li['数据库名'].drop_duplicates().to_list()
    # else:
    #     sjk_li = df[df['客户'].isin(us_i.split(','))]['数据库名'].drop_duplicates().to_list()


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
        
        sql_xse = f"SELECT distinct CAST ( {cpmc} AS nvarchar ( 2000 ) ),sum({xse}) FROM {k} where SUBSTRING(REPLACE({month},'-',''),0,7) < {month_a} GROUP BY {cpmc}"
        sql = f"SELECT distinct CAST ( {cpmc} AS nvarchar ( 2000 ) ),{zd_li_sql} FROM {k} where SUBSTRING(REPLACE({month},'-',''),0,7) < {month_a}"
        
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
            
            #前面替换空值后进行判断 跳过包含1,
            #产品名称包含+号的,截取至加号进行判断
            if '一二三' not in set(bhnr_1):
                if k not in (['[item].[dbo].yili_593','send_out.dbo.YILI_DATA_593_E_MONTH_NEW']):
                    for i in set(bhnr_1):
                        set(lg_df[cpmc].map(lambda x:ts_li.append(x) if i in x[0:x.find('+')] else x))
                else:
                    for i in set(bhnr_1):
                        set(lg_df[cpmc].map(lambda x:ts_li.append(x) if i in x else x))
                        
                ts_li = list(set(lg_df[cpmc]).difference(set(ts_li)))
            else:
                ts_li = lg_df[cpmc].to_list()
                    
            # pd.DataFrame(set(ts_li))
            # pd.DataFrame(ts_li)
            if ts_li:
                if bhnr_2:
                    a_1 = []
                    for i in bhnr_2:
                        a_1.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x[0:x.find('+')] ,ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_1))))
                    
                if bbhnr_1 and len(ts_li) >= 1:
                    a_2 = []
                    for i in bbhnr_1:
                        a_2.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x[0:x.find('+')],ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_2))))

                if bbhnr_2 and len(ts_li) >= 1:
                    a_3 = []
                    for i in bbhnr_2:
                        a_3.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x[0:x.find('+')],ts_li)))]))
                    ts_li = list(set(list(itertools.chain.from_iterable(a_3))))
                    
                if bbhnr_3 and len(ts_li) >= 1:
                    a_4 = []
                    for i in bbhnr_3:
                        a_4.append(list(np.array(ts_li)[np.array(list(map(lambda x:i in x[0:x.find('+')],ts_li)))]))
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
    else:
        yc_data = pd.DataFrame( columns =['数据库名','大类','客户','制造商','产品名称','异常字段','字段内容','应包含内容','应包含内容2','不应包含内容1','不应包含内容2','不应包含内容3','销售额'])

        
    if  len(yc_data)>1:
        yc_data = yc_data.merge(pd.concat(xse_li,axis=0),on=['数据库名','产品名称'],how='left')
    else:
        yc_data = yc_data
        
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



pc_zd = {
    '1':yiduiduo,
    '2':guigetaozhuang,
    '3':ziduanyouxiaoxing,
    '4':kongzhi,
    '5':xfsc
         }
print('程序list:(1,一对多 2,规格套装数 3,字段有效性 4,字段空值检查 5,细分市场检查),依次运行所有程序? 选择序号时 输入 all . 注意1,如报错将中断运行 2,在运行时请勿打开模版')
us_ = input('请输入要运行的程序序号:')


import traceback
import logging

logging.basicConfig(filename='五合一.log')


if len(us_.split()) == 1 and us_.split()[0] != 'all':
    try:
        pc_zd[us_]()
    except:
        s = traceback.format_exc()
        print('Error:已停止运行,请查看log')
        logging.error(s) 
else:
    try:
        if len(us_.split()) > 1:
            # print(us_.split()[1])
            yiduiduo(us_ = us_.split()[1])
            # guigetaozhuang()
            print('跳过规格套装')
            ziduanyouxiaoxing(us_ = us_.split()[1])
            kongzhi(us_ = us_.split()[1])
            xfsc(us_ = us_.split()[1])
        else:
            yiduiduo(us_ = 0)
            guigetaozhuang()
            ziduanyouxiaoxing(us_ = 0)
            kongzhi(us_ = 0)
            xfsc(us_ = 0)
    except:
        s = traceback.format_exc()
        print('Error:已停止运行,请查看log')
        logging.error(s)  
