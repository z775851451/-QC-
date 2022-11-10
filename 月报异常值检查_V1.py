# import _scproxy
import pymssql
import pandas as pd
import numpy as np
import sys
import os
import shutil
import datetime
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.mime.application import MIMEApplication
import smtplib
import openpyxl

from tqdm import tqdm
from time import sleep

start  = datetime.datetime.now()


input_ = {
    # 蒙牛常温
    # 'MN_DATA_YTN_NEW'
    1: ['item', 'MN_DATA_YTN_NEW', 'D_TIME', '平台名称','制造商','品类', '子品牌', '产品名称', '店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','URL_ID','SKU_ID','万元'],
    #安佳
    # 'anjia_data_cn_new'
    2: ['send_out','anjia_data_cn_new','D_TIME','平台名称','制造商','品类','子品牌','精准名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','URL_ID','SKU_ID','万元'],
   
    # 'anjia_data_ytn_new'
    3: ['send_out','anjia_data_ytn_new','D_TIME','平台名称','制造商','品类','子品牌','精准名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','URL_ID','SKU_ID','万元'],
    
    # 'anjia_data_naiyou_new'
    4: ['send_out', 'anjia_data_naiyou_new', 'D_TIME', '平台名称','制造商', '品类','子品牌', '产品名称', '店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','URL_ID','SKU_ID','万元'],
    
    # 'anjia_data_huangyou_new'
    5: ['send_out', 'anjia_data_huangyou_new', 'D_TIME', '平台名称','制造商', '品类','子品牌', '产品名称', '店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','URL_ID','SKU_ID','万元'],
    
    # 蒙牛低温 
    #鲜奶
    # 'MN_DATA_DIWENxian_NEW'
    6: ['send_out', 'MN_DATA_DIWENxian_NEW', 'MONTH', 'PLATFORM_NAME','品牌','商品类型', '子品牌_ST', '标准名称', 'SHOP_NAME', '销售额','','[销量(L/KG)]','URL_ID','SKU_ID','元'],
    #酸奶
    # 'MN_DATA_DIWENSUAN_NEW'
    7: ['item', 'MN_DATA_DIWENSUAN_NEW', 'MONTH', 'PLATFORM_NAME','品牌','商品类型', '子品牌_ST', '标准名称', 'SHOP_NAME', '销售额','','[销量(L/KG)]','URL_ID','SKU_ID','元'],
    #乳酸菌
    # 'MN_DATA_DIWENRSJ_NEW'
    8: ['item', 'MN_DATA_DIWENRSJ_NEW', 'MONTH', 'PLATFORM_NAME','品牌','商品类型', '子品牌_ST', '标准名称', 'SHOP_NAME', '销售额','','[销量(L/KG)]','URL_ID','SKU_ID','元'],

    #雀巢
    # 'quechao_milk_quan_temp'
    9: ['item', 'quechao_milk_quan_temp', 'month', '平台','制造商', 'Category','子品牌', '产品名称', '店铺名称', '[销售额(万)]','升价格','[升销量]','url_id','sku_id','万元'],

    #蒙牛 
    # 'MENGNIU_DATA_SFMP_NEW'
    10:['item','MENGNIU_DATA_SFMP_NEW','MONTH','平台','制造商','子品类','品牌','产品名称','进口国产', '销售额','','[销量（L/KG）]','URL_ID','SKU_ID','元'],


    #百事麦片
    # 'baishi_data_maipian'
    11: ['item', 'baishi_data_maipian','月份', 'PLATFORM_NAME','品牌','产品品类', '品牌', '产品名称', 'SHOP_NAME', '[销售额(KRMB)]','','[销量(KG)]','URL_ID','SKU_ID','千元'],

    #伊利水
    # 'yili_data_water_new'
    12: ['item', 'yili_data_water_new', '月份', '平台','制造商','品类', '品牌', '产品名称', '店铺名称', '[销售额(万)]','','升销量','url_id','sku_id','万元'],


    #伊利
    #液奶
    # 'yili_DATA_YTN_NEW'
    13: ['item','yili_DATA_YTN_NEW','D_TIME','平台名称','制造商','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],
    #低温酸奶
    # 'YILI_DATA_diwen_suan_new'
    14: ['item','YILI_DATA_diwen_suan_new','MONTH','平台名称','制造商','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],
    #低温鲜奶
    # 'YILI_DATA_DIWEN_xian_new'
    15: ['item','YILI_DATA_DIWEN_xian_new','MONTH','平台名称','制造商','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],
    #冰品
    # 'YILI_DATA_BINGPIN_NEW'
    16: ['item','YILI_DATA_BINGPIN_NEW','D_TIME','平台名称','制造商','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],

    #伊利婴儿粉 1
    # 'YILI_DATA_593_new'
    17: ['item','YILI_DATA_593_new','MONTH','平台名称','品牌','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],

    # 奶酪
    # 'yili_DATA_naiyou_NEW'
    18: ['item','yili_DATA_naiyou_NEW','MONTH','平台名称','制造商','品类','子品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],

    # 厚乳酪
    # 'YILI_DATA_GTRJ_NEW'
    19: ['item','YILI_DATA_GTRJ_NEW','MONTH','平台名称','品牌','品类','品牌','产品名称','店铺名称', '[销售额SKU)]','升价格','[销量(L/KG)]','url_id','sku_id','万元'],

    #百事可乐
    # 'baishi_coke_new'
    20: ['item','baishi_coke_new','MONTH','平台名称','制造商','品类','品牌','产品名称','店铺名称', '[销售额SKU)]','','[销量(L/KG)]','URL','SKU_ID','千元'],

    # 可口可乐
    # 'COKE_E_NEW'
    21: ['send_out', 'COKE_E_NEW', 'D_T_MONTH', 'D_C_PLATFORM','D_P_MANUFACTURE','D_P_CATEGORY','D_P_BRAND', 'D_P_PRODUCTNAME', 'D_C_SHOPNAME', 'M_S_SALES','','M_S_PHYSICALVOLUME','URL','SKU_ID','万元'],

    # Costa  销额单位：元
    # 'costa_data_new'
    22: ['item', 'costa_data_new', '月份', '平台','制造商', '品类','品牌', '产品名称', '店铺名称', '销售额','','升销量','url_id','SKU_ID','元'],

    # 雅士利
    # 'YASHILY_DATA_594_NEW'
    23: ['item', 'YASHILY_DATA_594_NEW', 'Month', '平台','制造商','品类', '品牌', '产品名称', '店铺名称', '销售额','','[销售量(L)]','URL_ID','SKU_ID','万元'],

    # 红牛
    # 'redbull_data_new'
    24: ['item', 'redbull_data_new', '月份', '平台名称','制造商','品类' ,'品牌', '产品名称', '店铺名称', '[销售额(万)]','','[销量(L/KG)]','url_id','sku_id','万元'],


    # 家乐氏麦片
    # 'jialeshi_data_maipian'
    25: ['item', 'jialeshi_data_maipian', 'Month', '平台名称','制造商','品类' , '品牌', '产品名称', '店铺名称', '销售额','','[销售量（KG）]','url_id','sku_id','元'],

    # 卡士低温奶
    # 'kashi_data_new'
    26: ['item', 'kashi_data_new', 'month', '平台','制造商','子品类' , '品牌', '产品名称', 'shop_name', '销售额','','升销量','url_id','sku_id','元'],


    # 德国马牌
    # 'mapai_data_new'
    27: ['item', 'mapai_data_new', 'MONTH', 'platform_name','品牌','category_name', '品牌', '标准名称', 'shop_name', '销售额','','销量','url_id','sku_id','元'],

    #倍耐力
    # 'beinaili_data_new'
    28: ['item', 'beinaili_data_new', 'MONTH', 'shop_info','品牌','轮胎类型', '品牌', '标准名称', 'shop_name', '销售额','','销量','url_id','sku_id','元'],


    # 好记（拌饭酱和酱油） HJ_DATA_BFJ_NEW、HJ_DATA_jiangyou_NEW
    # 'HJ_DATA_BFJ_NEW'
    29: ['item', 'HJ_DATA_BFJ_NEW', 'MONTH', 'PLATFORM_ID','品牌','品类', '品牌', '标准名称', 'SHOP_NAME', '销售额','','销量','URL_ID','SKU_ID','元'],

    # 'HJ_DATA_jiangyou_NEW'
    30: ['item', 'HJ_DATA_jiangyou_NEW', 'MONTH', 'PLATFORM_ID','品牌','品类', '品牌', '标准名称', 'SHOP_NAME', '销售额','','销量','URL_ID','SKU_ID','元'],

    # 'HJ_TOP品牌_醋料酒'
    31: ['item', 'HJ_TOP品牌_醋料酒', '时间', '店铺类型','品牌','品类', '品牌', '标准名称', 'SHOP_NAME', '销售额','','销量','URL_ID','SKU_ID','元'],


    #养乐多

    # 'yakult_data_new'
    32: ['item', 'yakult_data_new', 'month', '平台','制造商','子品类' ,'品牌', '产品名称', '店铺名称', '销售额','','[销量(L/KG)]','url_id','sku_id','万元']

}

st = ['1:蒙牛_常温      MN_DATA_YTN_NEW','2:安佳_安佳1      anjia_data_cn_new','3:安佳_安佳2      anjia_data_ytn_new','4:安佳_安佳3      anjia_data_naiyou_new','5:安佳_安佳4      anjia_data_huangyou_new','6:蒙牛低温_鲜奶      MN_DATA_DIWENxian_NEW','7:蒙牛低温_酸奶      MN_DATA_DIWENSUAN_NEW','8:蒙牛低温_乳酸菌      MN_DATA_DIWENRSJ_NEW','9:蒙牛低温_雀巢      quechao_milk_quan_temp','10:蒙牛_MENGNIU_DATA_SFMP_NEW      MENGNIU_DATA_SFMP_NEW','11:百事麦片_百事麦片      baishi_data_maipian','12:伊利_伊利水      yili_data_water_new','13:伊利_液奶      yili_DATA_YTN_NEW','14:伊利_低温酸奶      YILI_DATA_diwen_suan_new','15:伊利_低温鲜奶      YILI_DATA_DIWEN_xian_new','16:伊利_冰品      YILI_DATA_BINGPIN_NEW','17:伊利_伊利婴儿粉1      YILI_DATA_593_new','18:伊利_奶酪      yili_DATA_naiyou_NEW','19:伊利_厚乳酪      YILI_DATA_GTRJ_NEW','20:百事可乐_百事可乐      baishi_coke_new','21:可口可乐_可口可乐      COKE_E_NEW','22:Costa_Costa      costa_data_new','23:雅士利_雅士利      YASHILY_DATA_594_NEW','24:红牛_红牛      redbull_data_new','25:家乐氏麦片_家乐氏麦片      jialeshi_data_maipian','26:卡士低温奶_卡士低温奶      kashi_data_new','27:德国马牌_德国马牌      mapai_data_new','28:倍耐力_倍耐力      beinaili_data_new','29:好记_好记1      HJ_DATA_BFJ_NEW','30:好记_好记2      HJ_DATA_jiangyou_NEW','31:好记_好记3      HJ_TOP品牌_醋料酒','32:养乐多_养乐多      yakult_data_new']
for i in st:
    print(i)

u_input = input('请输入需要检查的序号支持多个(例如：8,13,15):')
d_month = input('请输入日期(例如:202207):')
print('键入回车以确认,取消运行请键入快捷键: Ctrl+C')

for i in tqdm(u_input.split(',')):
    sleep(0.05)
    tqdm.write(f'即将运行{i}_{input_[int(i)]}')

    Account = input_[int(i)]

    # 字符串转为日期,计算 mat

    import datetime
    from datetime import date, timedelta
    from dateutil.relativedelta import relativedelta
    import pandas as pd

    #日期函数
    #MONTH_4 最近3个月+去年当月
    def Month_(MONTH,YTD = 0,MAT = 0,MONTH_4 = 0,MONTH_N = 0):
        if YTD:
            edate = datetime.datetime.strptime(MONTH, '%Y%m')+ relativedelta(months=1)
            sdate = (edate - relativedelta(months=edate.month-1))
            date_range = pd.date_range(sdate,edate,freq='M').strftime('%Y%m').tolist()
            return date_range    
        elif MAT:
            edate = datetime.datetime.strptime(MONTH, '%Y%m')+ relativedelta(months=1)
            sdate = (edate - relativedelta(months=12))
            date_range = pd.date_range(sdate,edate,freq='M').strftime('%Y%m').tolist()
            return date_range
        elif MONTH_4:
            eyear = datetime.datetime.strptime(MONTH, '%Y%m')- relativedelta(years=1)
            edate = datetime.datetime.strptime(MONTH, '%Y%m')+ relativedelta(months=1)
            sdate = (edate - relativedelta(months=3))
            date_range = pd.date_range(sdate,edate,freq='M').strftime('%Y%m').tolist()+[eyear.strftime('%Y%m')]
            return date_range
        elif MONTH_N:#加自定义日期list,因为日期计算特性,会不包括结束日期,所有要加一
            edate = datetime.datetime.strptime(MONTH, '%Y%m')+ relativedelta(months=1)
            sdate = (edate - relativedelta(months=MONTH_N))
            date_range = pd.date_range(sdate,edate,freq='M').strftime('%Y%m').tolist()
            return date_range



    # ==========================以下变量按实际运行所需修改================================
    #DF日期开始时间
    # d_month = '202207'
    # dayu_month = "'202103','202104','202105','202106','202107','202108','202109','202110','202111','202112','202201','202202','202203','202204','202205','202206','202207'"

    # 库内表销售额的单位
    unit_price = Account[-1]

    # 分类列表
    pivot_index1 = [['平台名称', '制造商'], ['平台名称', '制造商', '品牌'],
                ['平台名称', '店铺名称'], ['平台名称', '制造商', '店铺名称'],['平台名称','品类']]

    #同比日期
    # tb_month = ['202103','202104','202105','202106','202107']
    #展示日期和主要计算日期
    # month = ['202107','202203','202204','202205','202206','202207']
    month = [Month_(d_month,MONTH_N=13)[0]]+Month_(d_month,MONTH_N=5)
    #YTD #升价格 YTD top20 使用到
    # YTD_month = ['202203','202204','202205','202206','202207']
    YTD_month = Month_(d_month,MONTH_N=5)
    # m_zip = list(zip(tb_month,month))

    #之前的变量
    # sort_month = ['202105','202204', '202205']
    # sort_month_SKU = ['202105', '202201', '202202', '202203', '202204', '202205']

    # 计算 与 展示 相关变量
    #标准差倍数
    df_std_ = 2
    #topN 百分比
    top_bf_top = 1
    #去除近2个月都小于5的行
    yc_ = 5
    #是否保留为False的行 0:保留, 1:不保留
    QC = 1
    #环比是否筛选 hb 1: 开启筛选,hb 0 : 保留所有, hb_min - hb_max : 大于且小于绝对值的 区间
    hb,hb_min,hb_max = 0,0.2,10000
    #升价格 top20
    top20_ = 20


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
        
    # if Account[1] == "costa_data_new" or Account[1] == 'MN_DATA_DIWENxian_NEW' or Account[1] == 'baishi_data_maipian' or Account[1] == 'yili_data_water_new' or Account[1] == 'redbull_data_new':
    #月份格式 为 2022-06-01,有-的放在这个位置
    if Account[1] in(["MENGNIU_DATA_SFMP_NEW","quechao_milk_quan_temp","yakult_data_new","HJ_DATA_jiangyou_NEW","HJ_DATA_BFJ_NEW","beinaili_data_new","mapai_data_new","costa_data_new",'MN_DATA_DIWENxian_NEW','baishi_data_maipian','yili_data_water_new','redbull_data_new','jialeshi_data_maipian','MN_DATA_DIWENSUAN_NEW','MN_DATA_DIWENRSJ_NEW','kashi_data_new']):
        sql = "SELECT \
            SUBSTRING( REPLACE(" + Account[2] + ",'-',''),0,7),\
            CAST ( " + Account[3] + " AS nvarchar ),\
            CAST ( " + Account[4] + " AS nvarchar ),\
            CAST ( " + Account[5] + " AS nvarchar ),\
            CAST ( " + Account[6] + " AS nvarchar ( 1000 ) ),\
            CAST ( " + Account[7] + " AS nvarchar ( 2000 ) ),\
            CAST ( " + Account[8] + " AS nvarchar ( 2000 ) ),\
            " + Account[9] + ",\
            " + Account[11] + ",\
            " + Account[12] + ",\
            " + Account[13] + "\
        FROM " + Account[1] + "\
        WHERE SUBSTRING( REPLACE(" + Account[2] + f",'-',''),0,7) in ('{month[0]}','{month[1]}','{month[2]}','{month[3]}','{month[4]}','{month[5]}')"

        df = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_zyanbo','ZhangYB_068',Account[0],sql),columns=['月份', '平台名称', '制造商', '品类','品牌', '产品名称', '店铺名称', '销售额', '销量','URL_ID','SKU_ID'])
        coke = df[['月份', '平台名称', '制造商', '品类','品牌', '产品名称', '店铺名称', '销售额', '销量']]
    else:
         #send_out
         sql = "SELECT \
              " + Account[2] + ",\
              CAST ( " + Account[3] + " AS nvarchar ),\
              CAST ( " + Account[4] + " AS nvarchar ),\
              CAST ( " + Account[5] + " AS nvarchar ),\
              CAST ( " + Account[6] + " AS nvarchar ( 1000 ) ),\
              CAST ( " + Account[7] + " AS nvarchar ( 2000 ) ),\
              CAST ( " + Account[8] + " AS nvarchar ( 2000 ) ),\
              " + Account[9] + ",\
              " + Account[11] + ",\
              " + Account[12] + ",\
              " + Account[13] + "\
         FROM " + Account[1] + "\
         WHERE " + Account[2] + f" in ('{month[0]}','{month[1]}','{month[2]}','{month[3]}','{month[4]}','{month[5]}')"

         df = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_zyanbo','ZhangYB_068',Account[0],sql),columns=['月份', '平台名称', '制造商', '品类','品牌', '产品名称', '店铺名称', '销售额', '销量','URL_ID','SKU_ID'])
         coke = df[['月份', '平台名称', '制造商', '品类','品牌', '产品名称', '店铺名称', '销售额', '销量']]

    df_url = df[df.月份.isin(month)][['月份', '平台名称','品类','产品名称', '销售额','URL_ID','SKU_ID']].copy()

    # 25数据库

    #天猫
    sql_tm = "SELECT * FROM TM_category_original_sku \
         WHERE 月份 >= " + "'" + month[-5]+'01' + "'" + " "

    # 15 京东
    sql_jd = "SELECT * FROM JD_REAL_ORIGINAL_sku\
         WHERE 月份 >= " + "'" + month[-5]+'01' + "'" + " "
    #拼多多
    sql_pdd = "SELECT * FROM PDD_CATEGORY_ORIGINAL_SKU\
         WHERE 月份 >= " + "'" + month[-5]+'01' + "'" + " "
    #25   
    syntun_conn_zs = pymssql.connect(server='192.168.0.25',
                                  user='liang',
                                  password='liangjianqing',
                                  database='QC')
    syntun_cursor_zs = syntun_conn_zs.cursor()

    ## 新增
    tm_url = pd.DataFrame(sql_connect('192.168.0.25','liang','liangjianqing','QC',sql_tm),columns=['月份','品类','URL_ID','销售额'])
    tm_url.月份 = tm_url.月份.astype('datetime64').dt.strftime('%Y%m')
    tm_url = tm_url.assign(平台名称='天猫')

    jd_url = pd.DataFrame(sql_connect('192.168.0.15','zhongxin_yanfa','Xin_yanfa','item',sql_jd),columns=['月份','是否自营','品类','四级类目','URL_ID','销售额']).loc[:,['月份','品类','URL_ID','销售额']]	
    jd_url.月份 = jd_url.月份.astype('datetime64').dt.strftime('%Y%m')
    jd_url = jd_url.assign(平台名称='京东')

    pdd_url = pd.DataFrame(sql_connect('192.168.0.25','liang','liangjianqing','QC',sql_pdd),columns=['月份','品类','URL_ID','销售额'])
    pdd_url.月份 = pdd_url.月份.astype('datetime64').dt.strftime('%Y%m')
    pdd_url = pdd_url.assign(平台名称='拼多多')


    #店铺真实值
    #天猫
    sql_tm_zs = "SELECT * FROM TM_category_original_shop\
        WHERE 月份 = " + "'" + month[-1]+'01' + "'" + " "
    tm_dp_df = pd.DataFrame(sql_connect('192.168.0.25','liang','liangjianqing','QC',sql_tm_zs),columns=['月份','品类','店铺','销售额'])
    tm_dp_df.月份 = tm_dp_df.月份.astype('datetime64').dt.strftime('%Y%m')
    tm_dp_df = tm_dp_df.assign(平台名称='天猫')
    tm_dp_df.rename(columns={"店铺":"店铺名称","销售额":"校对值_销额"},inplace=True)

    month[-1]

    if df['月份'].max() != month[-1]:
        tqdm.write('缺少输入的最大日期')
        input('程序已停止运行')
        sys.exit() 

    #计算标准差与平均值,并抛出两者之外的数据(除了升价格以外的所有sheet)
    #df : 经过处理后的Dataframe
    #w : 非计算的需要展示的字段
    #month :计算std列 数值类型的字段 -> list(确保df内有)
    #num : std的倍数 -> 标量
    #db : 对比字段的位置,默认为最后一列 -> 列表
    #QC : 是否抛出默认为1
    #n : group 计算 字段
    def df_std(df,month,n,df_std_ = df_std_,db = -1,QC=QC):
        # 取df columns 中 month 的 差集 列 最后合并使用
        if ('产品名称')in n :
            bl = df[df.columns.difference(month)]
        else:
            bl = df[n]

        df[month[-2]+'_div'] = df[month[-2]].div(df.groupby(by = n[0:-1])[month[-2]].transform('sum'),axis=0)
        df[month[-3]+'_div'] = df[month[-3]].div(df.groupby(by = n[0:-1])[month[-3]].transform('sum'),axis=0)
        
        test_std = df.copy().loc[:,month]
        test_std = test_std.fillna(0.00001)
        
        test_std = test_std.assign(
            #标准差 及 倍数
            std = test_std.iloc[:,1:].std(axis=1)*df_std_,
            avg = test_std.iloc[:,1:-1].mean(axis=1)
                                )
        test_std['avg-std'] = test_std['avg'] - test_std['std']
        test_std['avg+std'] = test_std['avg'] + test_std['std']
        #新增
        test_std['环比'] = test_std[month[-1]]/test_std[month[-2]]-1
        
        test_std['js环比>=0.5_近两个月>=0.05'] = ((abs(test_std['环比']) >= 0.5) & ((df[month[-2]+'_div'] >= 0.05) | (df[month[-3]+'_div'] >= 0.05)))
        # test_std['js环比>=0.5_近两个月>=0.05'] = ((abs(test_std['环比']) >= 0.5) & ((df[month[-2]+'_div'] >= 0.05)))
        
        #--
        # test_std['是否抛出']= np.where((test_std[month[db]] >= test_std['avg-std']) & (test_std[month[db]] <= test_std['avg+std']),'False','True')    
        test_std['std_是否抛出']= np.where((test_std[month[db]] >= test_std['avg-std']) & (test_std[month[db]] <= test_std['avg+std']),False,True)
        test_std['是否抛出'] = (test_std['std_是否抛出']|(~(test_std['std_是否抛出'])&test_std['js环比>=0.5_近两个月>=0.05']))
        
        r = pd.concat([bl,test_std],axis=1)
        
        #保留STD不为空的
        res = r[r['std'].notna()]
        if QC:
            res = res[res['是否抛出'] == True]
        # 抛出环比区间,未使用
        if hb:
            res = res[(abs(res['环比']) >= hb_min) & (abs(res['环比']) <= hb_max)]
        
        return res

    # 格式调整(所有sheet均使用)
    # 千分位字段
    def qfw(x,dw = 0,dis = 0):
        #空将填充 -
        if pd.isnull(x): 
            return '-'
        elif dw and dis == 0:
            # return format(float(round(x/10000,0)),',') 
            return format(int(x/10000),',') 
        elif dis == 1:
            return x
        else:
            return format(int(x),',')
            # return format(float(round(x,0)),',')

    #千分位保留两位小数、价格保留一位小数
    #百分比字段
    def bf(x):
        if pd.isnull(x): 
            return '-'
        else:
            return format(float(x),'.1%')
        
    # 求占比|(SKUsheet)
    # df->list : DataFrame,
    # ind->list : pivot_table参数index,top_bf
    # agg->list : 累计字段与排序字段,
    # by->list : 聚合的维度字段
    # num : 前百分之N 默认为100
    # colname : 列名称 默认为 top
    # if_ : 删除 销售额 columns 默认 为 1
    def top_bf(df,ind,agg,by,top_bf_top=top_bf_top,colname='top',if_=1):
        top80 = df.pivot_table(
            index=ind,
            aggfunc={agg[0]:"sum"}
            ).reset_index().sort_values(by=agg,ascending=False)
        top_fz = top80.groupby(by=by)[agg]
        top80 = top80.assign( **{colname : top_fz.cumsum()/top_fz.transform('sum')} )
        top80 = top80[top80[colname]<=top_bf_top]
        if if_ :
            #删除 销售额 columns
            return top80.drop(agg,axis=1)
        else:
            return top80
        
    #df : 需要处理的数字列df,标记大于5的数值列(使用标准差算法的sheet), | month 列
    def yc(df,yc_ = yc_):
        if df >= yc_:
            return True 
        else: 
            return False

    #合并25数据库数据
    zs_url = pd.concat([tm_url,jd_url,pdd_url])
    zs_url = zs_url[zs_url['月份'] == month[-1]]
    #近一个月df
    sku_url_5 =  df_url.query("月份 in(@month[-1])").copy()

    df_url_copy =df_url.drop_duplicates(subset=['平台名称','品类','产品名称','URL_ID','SKU_ID'])

    df_url_top = df_url.pivot_table(
        index=['平台名称','品类','产品名称'],
        columns=['月份'],
        aggfunc={"销售额":"sum"}
    ).reset_index()
    df_url_top.columns = ['平台名称','品类','产品名称']+month
    df_url_top = df_url_copy[df_url_copy['月份']== month[-1]].merge(df_url_top,how='inner',on=['平台名称','品类','产品名称'])

    #产品top
    sku_url_top80 = top_bf(sku_url_5,['平台名称','品类','产品名称'],['销售额'],['平台名称','品类'],top_bf_top,'产品_top80')

    url_top80 = top_bf(sku_url_5,['平台名称','品类','产品名称','URL_ID'],['销售额'],['平台名称','品类','产品名称'],top_bf_top,'URL_top80',0).rename(columns = {"销售额":"URL_销售额"})
    url_top80 = url_top80[url_top80['URL_销售额'] > 1]

    #url的小于80的,如果为0 取大于80的第一个
    url_top80_da = url_top80[url_top80['URL_top80']>0.8].pivot_table(
        index=['平台名称','品类','产品名称'],
        aggfunc={"URL_top80":"min","URL_ID":"min"}
        ).reset_index()

    url_top80_xiao = url_top80[url_top80['URL_top80']<0.8][['平台名称','品类','产品名称','URL_ID','URL_top80']]

    url_top80_hb = pd.concat([url_top80_xiao,url_top80_da],axis=0)
    url_top80 = url_top80.merge(url_top80_hb,how='right', on=['平台名称','品类','产品名称','产品名称','URL_ID','URL_top80'])

    sku_url_sales = df_url_top.merge(sku_url_top80,how = 'left',on = ['平台名称','品类','产品名称'])

    sku_sales = sku_url_sales.merge(url_top80,how = 'left',on = ['平台名称','品类','产品名称','URL_ID'])

    #伊利的平台名称链接真实值数据库
    sku_sales['平台名称'].replace('B2C-Tmall', '天猫',inplace=True)
    sku_sales['平台名称'].replace('B2C-JD', '京东',inplace=True)
    sku_sales['平台名称'].replace('B2C-PDD', '拼多多',inplace=True)

    sku_url_sales = pd.merge(sku_sales,zs_url,how='left',on=['平台名称','URL_ID'])[['平台名称','品类_x','产品名称','产品_top80']+month+['URL_销售额','URL_ID','SKU_ID','销售额_y','URL_top80']]

    sku_url_sales.columns = ['平台名称','品类','产品名称','产品_top80']+month+['URL_销售额','URL_ID','SKU_ID','校对值_销额','URL_top80']

    sku_url_sales['校对值_销额'] = sku_url_sales['校对值_销额'].map(lambda x:qfw(x/10000))
    sku_url = sku_url_sales

    test = coke.copy()
    ytd = test.query("月份 in(@YTD_month)")
    test = test.query("月份 in(@month)")

    ## 计算销售额标准差，将平均值加减标准差作为抛出范围，超出的抛出
    res = []
    for n in pivot_index1:
        test_r = test.pivot_table(
            index = n,
            columns = ['月份'],
            values = ['销售额'],
            aggfunc = {"销售额":"sum"},
            dropna=True).sort_values(
            by=('销售额', month[-1]), ascending=False)
        test_r.columns = test_r.columns.droplevel(0)
        test_r = test_r.reset_index()
        
        test_std = df_std(test_r,month,n)
        # .to_excel('df_std_test.xlsx')
        res.append(test_std)

    sku_url.rename(columns={"产品_top80":"产品_top(当前平台->当前品类->SKU TOP)","URL_top80":"URL_top(当前平台->当前品类->当前SKU->URL TOP)"},inplace=True)
    t = ['平台名称','品类','产品名称','产品_top(当前平台->当前品类->SKU TOP)']+month+['URL_ID','URL_销售额','SKU_ID','校对值_销额','URL_top(当前平台->当前品类->当前SKU->URL TOP)','std','avg','avg-std','avg+std','是否抛出','环比','js环比>=0.5_近两个月>=0.05','std_是否抛出']
    #url销售额为空的,去重与不为空的合并
    sku_url = pd.concat([sku_url[sku_url['URL_销售额'].isnull()].drop_duplicates(subset=['平台名称','品类','产品名称',month[-1],month[-2],month[-3],month[-4]]),sku_url[~sku_url['URL_销售额'].isnull()]])
    sku_url.sort_values(by=[month[-1],'URL_销售额'],ascending=False,inplace=True)

    # sku_url = sku_url[sku_url['URL_销售额'].notnull()]
    res.append(df_std(sku_url,month,['平台名称','品类','产品名称'])[t])

    #平台、店铺真实值
    tm_dp_df_a = tm_dp_df.groupby(by =['平台名称','店铺名称']).agg({'校对值_销额':'sum'})
    tm_dp_df_a = tm_dp_df_a.reset_index()
    tm_dp_df_a['校对值_销额'] = tm_dp_df_a['校对值_销额'].map(lambda x:qfw(x/1000))
    tm_dp_df_a = tm_dp_df_a

#真实值存在&不存在
    if tm_dp_df_a.shape[0] != 0:
        res[2] = res[2].merge(tm_dp_df_a,how='left',on=['平台名称','店铺名称'])
    else:
        pass

    #升价格处理
    def sjg(coke_toushi,unit_price=unit_price):
        if unit_price == '万元':
            op = 10000
        elif unit_price == '千元':
            op = 1000
        else:
            op = 1
            
        coke_toushi[month[-6]] = coke_toushi[('销售额', month[-6])] / coke_toushi[('销量', month[-6])] * op
        coke_toushi[month[-5]] = coke_toushi[('销售额', month[-5])] / coke_toushi[('销量', month[-5])] * op
        coke_toushi[month[-4]] = coke_toushi[('销售额', month[-4])] / coke_toushi[('销量', month[-4])] * op
        coke_toushi[month[-3]] = coke_toushi[('销售额', month[-3])] / coke_toushi[('销量', month[-3])] * op
        coke_toushi[month[-2]] = coke_toushi[('销售额', month[-2])] / coke_toushi[('销量', month[-2])] * op
        coke_toushi[month[-1]] = coke_toushi[('销售额', month[-1])] / coke_toushi[('销量', month[-1])] * op
        # 层级更改前先算出top
        # 更改层级重新命名columns
        list_columns = []
        for x,y in coke_toushi.columns:
            list_columns.append(y+''+x)

        coke_toushi.columns = list_columns
        coke_toushi = coke_toushi.reset_index()

        """# 对SKU加一个升价格的计算"""
        coke_toushi['当月升价格'] = coke_toushi[month[-1]+'销售额'] / coke_toushi[month[-1]+'销量']  * op
        coke_toushi['上月升价格'] = coke_toushi[month[-2]+'销售额']   / coke_toushi[month[-2]+'销量']  * op
        coke_toushi['当月升价格环比'] = coke_toushi['当月升价格'] / coke_toushi['上月升价格']-1
        
        # return op

        coke_toushi['常数列']=0.4 #升价格环比单独设定
        
        # coke_toushi = coke_toushi[abs(coke_toushi['当月升价格环比']) > coke_toushi['常数列']]
        if coke_toushi.shape[0] != 0:
            coke_toushi = coke_toushi[abs(coke_toushi['当月升价格环比']) > coke_toushi['常数列']]
        else:
            pass
        return coke_toushi



    #升价格 YTD top20
    s_index = ['制造商','品类','平台名称','产品名称']

    #最近一个月的URL——top
    sjg_top = top_bf(df[df.月份 == month[-1]],['产品名称','URL_ID'],['销售额'],['产品名称'],top_bf_top,'URL_top80')

    #重点品牌 YTD top20
    test_ytd = ytd.copy()
    top20 = (
            test_ytd.groupby(
            by = ['品类','制造商'])
            .agg({"销售额":"sum"})
            .sort_values(['品类','销售额'], ascending = False)
            .reset_index()  
            )

    top20 = top20.assign(排名 = top20.groupby(by=['品类']).cumcount())
    top20 = top20.assign(重点 = top20['品类']+'-'+top20['制造商']+'_top'+(top20['排名']+1).astype('str'))
    top20 = top20[top20.排名 <= top20_][['品类','制造商','重点']]

    test_sjg = test.copy()
    test_sjg = pd.merge(test_sjg,top20,how='left',on=['品类','制造商'])
    test_sjg.重点 = test_sjg.重点.fillna('非重点')

    coke_toushi = test_sjg.pivot_table(
                        values=['销售额', '销量'],
                        columns='月份',
                        index=s_index+['重点'],
                        aggfunc={
                            '销售额': np.sum,
                            '销量': np.sum,},
                        dropna=True).sort_values(
                            by=('销售额', month[-1]), ascending=False)
    coke_toushi = coke_toushi.fillna(0)
    coke_toushi = sjg(coke_toushi)


    
    #展示字段 month[-1] = 最近一个月日期
    zd_list = s_index+[month[-2]+'销售额',month[-1]+'销售额']+month+['当月升价格环比','重点']
    coke_toushi = coke_toushi.loc[:,zd_list]
    coke_toushi.rename(columns={"重点":"是否重点(分平台分品类TOP20制造商)"},inplace=True)
    #合并url_id,sku_id
    coke_toushi = coke_toushi.merge(df_url_copy,on=['平台名称','品类','产品名称'])
    coke_toushi.rename(columns={"销售额":"url销售额"},inplace=True)
    #合并URL_top
    coke_toushi = coke_toushi.merge(sjg_top,how='left',on=['产品名称','URL_ID'])

#20220818 修改top 规则

    if coke_toushi.shape[0] != 0:
        tt = coke_toushi.pivot_table(index = ['制造商','品类','平台名称','产品名称','URL_ID'],values=["URL_top80"]).reset_index().sort_values(by=['产品名称','URL_top80'],ascending=True)
        a = tt.drop_duplicates(subset=['产品名称'])
        pc = a[(a['URL_top80'] >= 0.8)]
        # pc = a[(a['URL_top80'] >= 0.8)]
        pc_li = pc['URL_ID'].to_list()
        # coke_toushi = coke_toushi[coke_toushi['URL_top80'] <= 0.8].sort_values(by=[month[-1],'url销售额'], ascending = False)
        #原始条件+需要排除的top中没有小于0.8,保留升序后的第一个
        coke_toushi = coke_toushi[(coke_toushi['URL_top80'] <= 0.8 ) | (coke_toushi['URL_ID'].isin(pc_li))].sort_values(by=[month[-1],'url销售额'], ascending = False)
        coke_toushi = coke_toushi[coke_toushi.columns[~coke_toushi.columns.str.contains('月份')]]
    else:
        pass

    res.append(coke_toushi)


    #删除std、avg 相关字段
    for i in range(len(res)):
        res[i] = res[i].filter(regex ='^[^std|^avg|^js]')

    # 处理销售额小于5的
    for i in range(len(res)):
        res[i] = res[i][res[i][month[-2:]].applymap(lambda x:yc(x)).sum(axis=1) >= 1]

    res[5][month] = res[5][month].applymap(lambda x:float(round(x,1)))

    #格式处理
    for i in range(len(res)-1):
        res[i][month] = res[i][month].applymap(lambda x:qfw(x))
    for i in range(len(res)):
        t = res[i].columns[(res[i].columns.str.contains('top|环比$'))]
        res[i][t] = res[i][t].applymap(lambda x:bf(x))
        
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
    #-*-coding:utf-8-*-
    from openpyxl import load_workbook
    with pd.ExcelWriter(f'结果/{Account[1]}-报告检查异常-结果.xlsx') as mn_writer:
        res[4].to_excel(mn_writer,sheet_name='平台 品类',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='utf-8')
        res[0].to_excel(mn_writer,sheet_name='制造商',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='utf-8')
        res[1].to_excel(mn_writer,sheet_name='制造商 品牌',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='utf-8')
        res[2].to_excel(mn_writer,sheet_name='店铺',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='gb2312')
        res[3].to_excel(mn_writer,sheet_name='制造商 店铺',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='gb2312')
        res[5].to_excel(mn_writer,sheet_name='SKU',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='utf-8')
        # res[6].to_excel(mn_writer,sheet_name='sku_升价格',na_rep='',index=False,startcol=0,header=True,float_format = "%0.2f",freeze_panes=(1,0), encoding='utf-8')
        res[6].to_excel(mn_writer,sheet_name='升价格',na_rep='',index=False,startcol=0,header=True,freeze_panes=(1,0), encoding='utf-8')

input('文件已输出,请到【结果】文件夹下获取')
