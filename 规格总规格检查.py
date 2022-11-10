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

