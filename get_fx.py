# 获取10天中美汇率数据
import requests
from lxml import etree
import numpy as np
import pandas as pd
url='https://cn.investing.com/currencies/usd-cnh-historical-data'
header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
response=requests.get(url,headers=header)
df=response.content
df=df.decode().replace('<!--','').replace('-->','')
html = etree.HTML(df)
el_list=html.xpath("//div[@id='results_box']//tr")
#在每一组中继续进行数据的提取/text()
items=[]
for el in el_list[1:11]:
    item= el.xpath("./td/text()")[:5]
    item_frame=pd.DataFrame(item,index=['日期','收盘','开盘','高','低']).T
    items.append(item_frame)
usd=pd.concat(items)
usd['日期']=usd.日期.str.replace("年","-").str.replace("月","-").str.replace("日","")
usd['日期']=pd.to_datetime(usd.日期,format='%Y-%m-%d')
usd['币种']='CNY/USD'
for i in ['收盘','开盘','高','低']:
    usd[i]=usd[i].astype(float)
print('开始')
import datetime
# 获取提取数据的最小时间
near_day= pd.to_datetime(str(usd.sort_values(by='日期').日期.values[0]))
near_day = near_day.strftime('%Y-%m-%d')

#提取数据库最小时间的所有数据，跟爬取的时间进行比较
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  汇率数据 WHERE 日期 >='" + near_day + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
data = pd.DataFrame(list_data)
data.columns=columns
data['日期']=pd.to_datetime(data['日期'],format='%Y-%m-%d')
data_usd=data[data['币种']=='CNY/USD']

#获得要增加的 中美汇率 数据
d_add=usd[usd.日期.isin(data_usd.日期)==False]

print(d_add)

from sqlalchemy import create_engine
import sqlalchemy
#存入数据
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
d_add.to_sql('汇率数据', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                '收盘': sqlalchemy.types.FLOAT(),
                '开盘': sqlalchemy.types.FLOAT(),
                '高': sqlalchemy.types.FLOAT(),
                '低': sqlalchemy.types.FLOAT(),
                '币种': sqlalchemy.types.String(length=20)})

# 获取10天欧美汇率数据
import requests
from lxml import etree
url='https://cn.investing.com/currencies/eur-usd-historical-data'
header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
response=requests.get(url,headers=header)
df=response.content
df=df.decode().replace('<!--','').replace('-->','')
#创建element对象
html = etree.HTML(df)
el_list=html.xpath("//div[@id='results_box']//tr")
#在每一组中继续进行数据的提取/text()
items=[]
for el in el_list[1:11]:
    item= el.xpath("./td/text()")[:5]
    item_frame=pd.DataFrame(item,index=['日期','收盘','开盘','高','低']).T
    items.append(item_frame)
eur=pd.concat(items)
eur['日期']=eur.日期.str.replace("年","-").str.replace("月","-").str.replace("日","")
eur['日期']=pd.to_datetime(eur.日期,format='%Y-%m-%d')
eur['币种']='USD/EUR'
for i in ['收盘','开盘','高','低']:
    eur[i]=eur[i].astype(float)

#跟已有的数据比较，找出要新增的数据（欧美汇率）
data_eur=data[data['币种']=='CNY/EUR']
deu=eur[eur.日期.isin(data_eur.日期)==False]

#对应时间的（中美汇率）
usd_m=usd[usd.日期.isin(deu.日期)]
usd_m.columns=['日期', '收盘u', '开盘u', '高u', '低u', '币种u']

#计算出 （中欧汇率）
mix=pd.merge(deu,usd_m,on='日期',how='left')
mix['收盘']=mix['收盘']*mix['收盘u']
mix['开盘']=mix['开盘']*mix['开盘u']
mix['高']=mix['高']*mix['高u']
mix['低']=mix['低']*mix['低u']
mix=mix[['日期', '收盘', '开盘', '高', '低']]
mix['币种']='CNY/EUR'

print(mix)

#存入数据
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
mix.to_sql('汇率数据', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                '收盘': sqlalchemy.types.FLOAT(),
                '开盘': sqlalchemy.types.FLOAT(),
                '高': sqlalchemy.types.FLOAT(),
                '低': sqlalchemy.types.FLOAT(),
                '币种': sqlalchemy.types.String(length=20)})

