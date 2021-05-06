import datetime
import numpy as np
import pandas as pd
nt=datetime.datetime.now()
zt= nt - datetime.timedelta(days=1)
near_day=zt.strftime('%Y-%m-%d')
lt= nt-datetime.timedelta(days=10)
later_day=lt.strftime('%Y-%m-%d')

print('开始。。。。。。。。。。。。。。。。。。。。')

#提取报工表数据
import cx_Oracle
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("select a.pp75tccn 流程卡号20 \
    ,(select C.IMLITM FROM proddta.F4101 C WHERE C.IMITM = A.PPKIT ) 项目号20,ntod2(a.pppbdt) 日期20 \
    ,(case when a.ppopsc in ('A2','A4') then proddta.convtday(a.pppetm) when a.ppopsc = 'A1' then proddta.convtday(a.pppbtm) else '' end ) 时间20 \
    ,a.ppopsq/100 工序号20,a.ppsoqs/1000000*a.ppuorg/1000000 完工重量20 \
    ,a.ppkit 短项目号,a.pplotn 原料批号20,a.pplitm 原料项目号,A.PPUKID 序号20 from proddta.FE6TM01 a \
    where a.ppmmcu IN( '          P1') and a.ppdcto in ('F1','FT','C1','WW','CW','WL') \
    and a.ppopsc in ('A2') and a.ppopsq/100 in (10) \
    AND A.PPPBDT>=DTON('" +later_day+ "') AND A.PPPBDT<=DTON('" + near_day + "') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
data = pd.DataFrame(list_data)
data.columns=columns
for i in data.columns:
    data[i]=data[i].astype(str).replace(" ","")
    data[i]=data[i].str.replace(" ","")
    data[i]=data[i].str.replace("空白","")

#1,提取项目资料表数据
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("  select IBITM 短项目号,IMLITM 项目号11,IBSRP3 材质大类20,IMSEG1 分段111 \
    ,IMSEG6 分段611,IBSRP5 牙别11,IBSRP7 标准11 FROM proddta.F4102, proddta.F4101 \
    WHERE IMITM=IBITM and  IBMCU in ('          P1')  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
d1= pd.DataFrame(list_data)
d1.columns=columns
for i in d1.columns:
    d1[i]=d1[i].astype(str).replace(" ","")
    d1[i]=d1[i].str.replace(" ","")
    d1[i]=d1[i].str.replace("空白","")

#获取d2数据，拼成【短项目号和产品规格汇总20】表，再跟报工表拼接
#提取[工艺路线表]数据.跟项目资料表拼接(分段111)
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("  SELECT IRKITL 项目号11,substr(TRIM(IRMCU),'0','7') 标准机型11 \
  FROM proddta.F3003 A \
  LEFT JOIN proddta.F0006 B ON A.IRMCU=B.MCMCU \
  LEFT JOIN PRODCTL.F0005 C ON C.DRSY = '00' AND C.DRRT = '28' AND TRIM(C.DRKY) = TRIM(B.MCRP28) \
  LEFT JOIN PRODCTL.F0005 D ON D.DRSY = '00' AND D.DRRT = '29' AND TRIM(D.DRKY) = TRIM(B.MCRP29) \
  LEFT JOIN PRODCTL.F0005 E ON E.DRSY = '00' AND E.DRRT = '30' AND TRIM(E.DRKY) = TRIM(B.MCRP30) \
  WHERE IRMMCU IN ('          P1') and IROPSQ/100 in (10) and IRTRT in ('M') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
d2= pd.DataFrame(list_data)
d2.columns=columns
for i in d2.columns:
    d2[i]=d2[i].astype(str).replace(" ","")
    d2[i]=d2[i].str.replace(" ","")

table_a=pd.merge(d1,d2,on='项目号11',how='left')
table_a=table_a.fillna("")
for i in ['材质大类20','分段111','分段611','牙别11','标准机型11']:
    table_a[i]=table_a[i].astype('category')
table_a['产品规格汇总20']=table_a['材质大类20'].str.cat(table_a['分段111']).str.cat(table_a['分段611']).str.cat(table_a['牙别11']).str.cat(table_a['标准机型11'])
table_a=table_a[['短项目号', '产品规格汇总20']]

table=pd.merge(data,table_a,on='短项目号',how='left')

table['完工重量20']=table['完工重量20'].astype('float')
table.loc[:,'工单号16']=table['原料批号20'].apply(lambda x: x[0:8])

def choice(x):
    if len(x)==10:
        return '正常'
    if len(x)==9 or len(x)==14:
        return '加工'
    else:
        return '异常'
table['原料项目号位数']=table['原料项目号'].apply(choice)
#原料项目号正常的数据
df_have_name=table[table['原料项目号位数']=='正常']
df_have_name=df_have_name.drop(['原料项目号位数'], axis=1)
#原料项目号要加工的数据
df_no_name1=table[table['原料项目号位数']=='加工']

#python 连接cx_Oracle数据库
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
#执行SQL语句
cursor.execute(" SELECT distinct  WMDOCO 工单号16, WMCPIL 子件项目号16 \
  FROM proddta.F3111,proddta.F4801 \
  WHERE WADOCO=WMDOCO AND WADCTO=WMDCTO AND WMDCTO='WW' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df = pd.DataFrame(list_data)
df.columns=columns
for i in df.columns:
    df[i]=df[i].astype(str).replace(" ","")
    df[i]=df[i].str.replace(" ","")

df_no_name1=df_no_name1.reset_index()
df_no_name2=pd.merge(df_no_name1,df,on='工单号16',how='left').sort_values(by='index')
df_no_name2=df_no_name2[df_no_name2.子件项目号16.notna()]

df_no_name2['差分1']=df_no_name2['index'].diff()
df_no_name2['差分2']=df_no_name2['index'].diff(-1)

#如果 差分值=0 代表重复数据，需要删除重复部分即可。
a0=df_no_name2[df_no_name2['差分1']==0].index
a1=df_no_name2[df_no_name2['差分2']==0].index
a2=df_no_name2[(df_no_name2['差分1']==0)&(df_no_name2['差分2']==0)].index
a3= list(set(a0).difference(list(set(a2))))
a_list=a3+list(a1)
df_no_name3=df_no_name2[df_no_name2.index.isin(a_list)==False]
df_no_name=df_no_name3.drop(['原料项目号','index','原料项目号位数','差分1','差分2'], axis=1)
df_no_name=df_no_name.rename(columns={"子件项目号16":"原料项目号"})

#原料分析最终表
table_x=pd.concat([df_have_name,df_no_name]).reset_index(drop=True)
table_x=table_x.drop(['工单号16','短项目号','原料批号20'], axis=1)
table_x['日期20']=pd.to_datetime(table_x['日期20'],format='%Y-%m-%d')
table_x['时间20']=pd.to_datetime(table_x['时间20'],format='%H:%M:%S')
table_x['流程卡号20']=table_x['流程卡号20'].astype('int64')
table_x['工序号20']=table_x['工序号20'].astype(int)
table_x['序号20']=table_x['序号20'].astype(int)

print('开始存储。。。。。。。。。。。。。。。。')

from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  原料比例原表 WHERE 日期20 >='" + later_day + "' AND 日期20 <= '" + near_day + "'")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
x = pd.DataFrame(list_data)
x.columns=columns
x['日期20']=pd.to_datetime(x['日期20'],format='%Y-%m-%d')
x['时间20']=pd.to_datetime(x['时间20'],format='%H:%M:%S')

# 需要放入的数据是：最近10天内，没有的数据。
ap=table_x[table_x.日期20.isin(x.日期20.unique())==False]

#存入数据
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
ap.to_sql('原料比例原表', engine, if_exists='append', index=False,
          dtype={'产品规格汇总20': sqlalchemy.types.String(length=50),
                 '原料项目号': sqlalchemy.types.String(length=50),
                 '完工重量20': sqlalchemy.types.FLOAT(),
                 '序号20': sqlalchemy.types.INT(),
                 '流程卡号20':sqlalchemy.types.BIGINT(),
                 '项目号20':sqlalchemy.types.String(length=50),
                 '工序号20':sqlalchemy.types.INT(),
                 '日期20':sqlalchemy.types.DATE(),
                 '时间20': sqlalchemy.types.TIME(),})
#engine.connect().execute(" ALTER TABLE 原料比例原表 ADD PRIMARY KEY (日期20,时间20,流程卡号20,序号20); ")
print('结束。。。。。。。。。。。。。。。。。。')
