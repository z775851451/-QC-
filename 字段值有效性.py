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
kehu=input('请键入要运行的库户名称:')
if kehu != 'all':
    df=df1[df1['客户名']== kehu]
else:
    df=df1




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

input('文件已输出,请到 [结果] 文件下获取')
