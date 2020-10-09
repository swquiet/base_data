# BlockingScheduler定时任务
from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
# 输出时间
def job():
    import datetime
    now = datetime.datetime.now()
    zt = now - datetime.timedelta(days=1)
    zt_year = zt.year
    zt_month = zt.month
    zt_day = zt.day
    qt = now - datetime.timedelta(days=2)
    qt_year = qt.year
    qt_month = qt.month
    qt_day = qt.day
    # 连接MongoDB,提取昨天点击数据
    import pandas as pd
    import pymongo
    from pymongo import MongoClient
    client = MongoClient('192.168.3.193', 27017)
    biao = client['local']['SYS_OPER_LOG']
    list_data = []
    print('今天是：',now)
    print('提取：', zt_year, zt_month, zt_day,'日数据')
    need_data = biao.find({"startTime": {"$gte": datetime.datetime(qt_year, qt_month, qt_day, 16, 00, 00),
                                         "$lt": datetime.datetime(zt_year, zt_month, zt_day, 16, 00, 00)}},
                          {'userName': 1, 'userId': 1, 'startTime': 1, 'operAction': 1, 'bussDesc': 1},
                          no_cursor_timeout=True)
    for n in need_data:
        list_data.append(n)
    data = pd.DataFrame(list_data)

    # 删除userName,userId均为空的行
    data = data.dropna(axis=0, how='any', subset=['userName', 'userId'])
    # 更正时差
    data.startTime = pd.to_datetime(data.startTime, format='%Y-%m-%d')
    data.startTime = data.startTime + pd.Timedelta(hours=8)
    # 增加YYMMDD时间格式
    data['Time'] = data.startTime.map(lambda x: x.strftime('%Y-%m-%d'))
    # 选取有用的列
    data = data[['userName', 'userId', 'Time', 'startTime', 'operAction', 'bussDesc']]

    #保存原始数据  数据太大，每个月调整一次，放在不同的CSV文件里
    data.to_csv(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\B2B数据\原数据\10月原数据.csv', mode='a+', index=False, header=False)

    # 找到项目，获取需要的列
    data1 = data[['userName', 'userId', 'startTime', 'Time']]
    data1.startTime = pd.to_datetime(data.startTime, format='%Y-%m-%d')

    # python 连接cx_Oracle数据库,找到AN8进行客户匹配
    import cx_Oracle
    ora = cx_Oracle.connect('TONGTJ', ' TONGTJ', '192.168.3.220:1521/TMJDEDB')
    cursor = ora.cursor()
    # 执行SQL语句
    cursor.execute("select USER_ID,USER_NAME,AN8 from tongb2b.ACL_USER")

    list_data = []
    for row in cursor.fetchall():
        list_data.append(row)
    cursor.close()
    ora.close()

    data_m = pd.DataFrame(list_data)
    data_m.columns = ['userId', 'USER_NAME', 'AN8']

    # 匹配数据
    data_match = pd.merge(data1, data_m, on='userId', how='left')
    data_match = data_match[['userName', 'AN8', 'Time', 'startTime']]
    data_match = data_match.dropna(axis=0, how='any', subset=['AN8'])

    #处理每日数据
    # 按客户名，日期，排序  得dm1表
    dm1 = data_match.sort_values(by=['AN8', 'startTime'], ascending=True)
    dm1 = dm1.reset_index(drop=True)
    # 删除第一行，得dm2表
    dm2 = dm1[['AN8', 'startTime', 'Time']]
    dm2 = dm2.drop(0)
    dm2 = dm2.reset_index(drop=True)
    dm2.columns = ['an8', 'endtime', 'time']
    # dm1和dm2并表
    table = pd.concat((dm1, dm2), axis=1)
    table['时间差'] = table['endtime'] - table['startTime']
    table['时间差'] = table['时间差'].dt.total_seconds() / 60
    # 把时间不同的行，删除
    t_error = table[table.Time != table.time].index
    table = table.drop(t_error)
    # 把客户名不同的行删除
    an8_error = table[table.AN8 != table.an8].index
    table = table.drop(an8_error)
    table = table.reset_index(drop=True)
    # 选取新表需要的数据
    table_new = table[['userName', 'AN8', 'Time', '时间差']]

    # 选取时间差在10分钟以内的数据
    t = []
    label = []
    for i in range(0, 145):
        n = i * 10
        t.append(n)
        if i > 0:
            label.append(n)
    table_new['十分钟分类'] = pd.cut(table_new['时间差'], t, labels=label)
    table_use = table_new[table_new['十分钟分类'] == 10]
    table_use = table_use[['userName', 'AN8', 'Time', '时间差']]
    table_use['AN8'] = table_use['AN8'].astype('int')

    # 一个AN8存在多个userName，故去重后拼接
    table_name = table_use[['userName', 'AN8']]
    table_name = table_name.drop_duplicates(subset='AN8', keep='first')
    table_name.columns = ['客户名', '客户号']

    # 按每个客户、每天，做时间差的累加，得：每个客户 每天 真正在线时长
    table_s = table_use.groupby(['AN8', 'Time'])['时间差'].sum().to_frame()
    table_s = table_s.reset_index()
    table_s.columns = ['客户号', '日期', '在线时长']
    #跟有客户名的table_name表拼接
    table_sum = pd.merge(table_s, table_name, on='客户号', how='left')

    # 重要数据保存
    # 累加：2020年6月1日起，日在线时长_中转数据
    table_sum.to_csv(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\B2B数据\日在线时长_中转数据.csv', mode='a+', index=False, header=False)

    #设定提取昨天销售数据的时间
    JT = now.strftime('%Y-%m-%d')
    ZT = zt.strftime('%Y-%m-%d')
    # python 连接cx_Oracle数据库：获取昨天的销售数据
    import cx_Oracle
    ora = cx_Oracle.connect('TONGTJ', ' TONGTJ', '192.168.3.220:1521/TMJDEDB')
    cursor = ora.cursor()
    # 执行SQL语句
    cursor.execute("select SHAN8, SO_WEIGHT, ORDER_DATE from tongb2b.cust_so_mas \
                  where ORDER_DATE  between to_date( '" + ZT + "', 'yyyy-mm-dd') and to_date('" + JT + "' , 'yyyy-mm-dd') ")

    list_data = []
    columns = []
    for c in cursor.description:
        columns.append(c[0])
    for row in cursor.fetchall():
        list_data.append(row)
    cursor.close()
    ora.close()
    data_sale = pd.DataFrame(list_data)
    data_sale.columns = columns
    data_sale = pd.DataFrame(list_data)
    data_sale.columns = columns
    # 增加YYMMDD时间格式
    data_sale['Time'] = data_sale.ORDER_DATE.map(lambda x: x.strftime('%Y-%m-%d'))
    sales = data_sale.groupby(['SHAN8', 'Time'])['SO_WEIGHT'].sum().to_frame().reset_index()
    sales.columns = ['客户号', '日期', '销售重量']

    #前面的时间表和提取的销售表拼接
    ts = pd.merge(table_sum, sales, on=['客户号', '日期'], how='left')

    #重要数据保存
    # 累加：2020年6月1日起，每个客户每天在线时长和销售重量 数据
    ts.to_csv(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\B2B数据\每个客户每天在线时长和销售重量.csv', mode='a+', index=False, header=False)

    a = ts.groupby(['日期'])['客户号'].count().to_frame().reset_index()
    b = ts.groupby(['日期'])['在线时长'].sum().to_frame().reset_index()
    c = ts.groupby(['日期'])['销售重量'].sum().to_frame().reset_index()
    ts0 = pd.merge(a, b, on='日期', how='left')
    ts_avg = pd.merge(ts0, c, on='日期', how='left')
    ts_avg['平均在线时长'] = ts_avg.在线时长 / ts_avg.客户号
    ts_avg['平均销售重量'] = ts_avg.销售重量 / ts_avg.客户号
    ts_avg.columns = ['日期', '每日在线人数', '在线时长', '销售重量', '平均在线时长', '平均销售重量']
    ts_avg = ts_avg[['日期', '每日在线人数', '平均在线时长', '平均销售重量']]

    #结果数据保存
    # 累加：平均看：日均在线人数时长和销售重量
    ts_avg.to_csv(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\B2B数据\日均在线人数时长和销售重量.csv', mode='a+', index=False, header=False)
    print('数据制作和保存完成')
# BlockingScheduler
scheduler = BlockingScheduler()
#每天4点30分执行一次。
scheduler.add_job(job, "cron",hour = 4, minute = 30)
scheduler.start()


