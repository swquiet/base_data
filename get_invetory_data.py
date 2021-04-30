import time
import random
import  json
import jsonpath
import datetime
import requests
import pandas as pd
from lxml import etree
#不定时开始运行
t=random.randint(0,1200)
time.sleep( t )

nt=datetime.datetime.now()
day=nt.strftime('%Y-%m-%d')

url='http://www.aozhanego.com/aozhan-server/rest/front/quicksearch/level3/list'
header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
data_1 = {'pageNo':'1','pageSize':'12','type':'0','searchKey':''}
r1 = requests.post(url,data=data_1,headers= header)
text1 = r1.text
json_1 = json.loads(text1)
n = jsonpath.jsonpath(json_1, "$..totalPages")[0]

xx=[]
for i in range(n):
    print(i)
    t=random.uniform(0.3,5)
    time.sleep( t )
    data= {'pageNo':i+1,'pageSize':'12','type':'0','searchKey':''}
    r = requests.post(url,data=data,headers= header)
    text = r.text
    json_x = json.loads(text)
    a = jsonpath.jsonpath(json_x, "$..list[*].level3")
    b = jsonpath.jsonpath(json_x, "$..list[*].cardnum")
    c = jsonpath.jsonpath(json_x, "$..list[*].stand")
    d = jsonpath.jsonpath(json_x, "$..list[*].performancelevel")
    e = jsonpath.jsonpath(json_x, "$..list[*].surfacetreatment")
    f =jsonpath.jsonpath(json_x, "$..list[*].storename")
    g = jsonpath.jsonpath(json_x, "$..list[*].packagetype")
    h = jsonpath.jsonpath(json_x, "$..list[*].pdstorenum")
    x=pd.DataFrame([a,b,c,d,e,f,g,h],index=\
        ['标准','材质','规格','印记','表面处理','店铺','包装方式','库存/千支']).T
    xx.append(x)
table=pd.concat(xx)
table=table[table.标准!=False]
table=table.fillna('')
t_data=table.groupby(['标准', '材质', '规格', '印记', '表面处理', '店铺','包装方式'])\
   ['库存/千支'].sum().to_frame().reset_index()
t_data['日期']=day
t_data['日期']=pd.to_datetime(t_data['日期'],format='%Y-%m-%d')

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT distinct 日期  FROM  竞对库存数据  ")
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

ap=t_data[t_data.日期.isin(data.日期.unique())==False]

#存入数据
import sqlalchemy
from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 竞对库存数据# ")
ap.to_sql('竞对库存数据', engine, if_exists='append', index=False,
    dtype={'日期': sqlalchemy.types.DATE(),
           '标准': sqlalchemy.types.String(length=50),
           '材质': sqlalchemy.types.String(length=20),
           '规格': sqlalchemy.types.String(length=50),
           '印记': sqlalchemy.types.String(length=20),
           '表面处理': sqlalchemy.types.String(length=20),
           '店铺': sqlalchemy.types.String(length=20),
           '包装方式': sqlalchemy.types.String(length=20),
           '库存/千支': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 竞对库存数据 ADD PRIMARY KEY (日期,标准,材质,规格,印记,表面处理,店铺,包装方式); ")


