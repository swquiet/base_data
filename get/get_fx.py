import time
import random
import datetime
import requests
import numpy as np
import pandas as pd
from lxml import etree

nt = datetime.datetime.now()
# 0和6提取的是周六周日的数据，不开市。
if nt.weekday() == 6 or nt.weekday() == 0:
    pass
# 1——5提取的是周一至周五的数据。
if nt.weekday() in [1, 2, 3, 4, 5]:
    t = random.randint(0, 120)
    time.sleep(t)
    zt = nt - datetime.timedelta(days=1)
    day = zt.strftime('%Y-%m-%d')
    url = 'https://www.usd-cny.com/'
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
    response = requests.get(url, headers=header)
    df = response.content
    html = etree.HTML(df)
    el_list = html.xpath("//article[@class='content']/table//tr")
    u = []
    for el in el_list[1:3]:
        items = []
        item = el.xpath("./td/text()")[0:3:2]
        items.append(float(item[0]) / 100)
        items.append(float(item[1]) / 100)
        u.append(pd.DataFrame(items).T)
    u1 = u[0]
    u1['币种'] = 'CNY/USD'
    u2 = u[1]
    u2['币种'] = 'CNY/EUR'
    fx = pd.concat([u1, u2])
    fx.columns = ['买入价', '卖出价', '币种']
    fx['日期'] = day
    fx['日期'] = pd.to_datetime(fx['日期'], format='%Y-%m-%d')

    import psycopg2
    connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h",
                                  host="192.168.2.156", port="5432")
    cur = connection.cursor()
    cur.execute("SELECT distinct 日期  FROM  汇率数据  ")
    list_data = []
    columns = []
    for c in cur.description:
        columns.append(c[0])
    for row in cur.fetchall():
        list_data.append(row)
    connection.commit()
    cur.close()
    connection.close()
    data = pd.DataFrame(list_data)
    data.columns = columns
    data['日期'] = pd.to_datetime(data['日期'], format='%Y-%m-%d')

    ap = fx[fx.日期.isin(data.日期) == False]

    # 存入数据
    import sqlalchemy
    from sqlalchemy import create_engine
    engine = create_engine(
        'postgresql+psycopg2://' + 'chengben' + ':' + 'np69gk48fo5kd73h' + '@192.168.2.156' + ':' + str(
            5432) + '/' + 'chengben')
    # engine.connect().execute(" DROP TABLE 汇率数据# ")
    ap.to_sql('汇率数据', engine, if_exists='append', index=False,
              dtype={'日期': sqlalchemy.types.DATE(),
                     '买入价': sqlalchemy.types.FLOAT(),
                     '卖出价': sqlalchemy.types.FLOAT(),
                     '币种': sqlalchemy.types.String(length=20)})
    # engine.connect().execute(" ALTER TABLE 汇率数据 ADD PRIMARY KEY (日期,币种); ")


