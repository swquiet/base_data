#每日更新存储 报工表,生产入库,包装入库,仓储出库,委外明细表,委外分摊表,
#物料耗用表,物料验收表,财务实际成本11,薪资,皮膜
import datetime
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
zt=datetime.datetime.now()-datetime.timedelta(days=1)
near_day=zt.strftime('%Y-%m-%d')
nd=zt.strftime('%Y-%m')+'-1'
ld=pd.Timestamp(nd).to_pydatetime()-relativedelta(months=2)
later_day=ld.strftime('%Y-%m-%d')

#提取报工表数据
import cx_Oracle
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("select  ntod2(a.pppbdt) 日期20 \
,TRIM((select C.IMITM FROM proddta.F4101 C WHERE C.IMITM = a.PPKIT )) 短项目号,\
a.ppsoqs/1000000 完工数量,TRIM(a.ppmcu) 工作中心,TRIM(PPURC1) 工序码,\
TRIM(a.ppopsq) 工序号,TRIM(PPMCUF) 机台编码,TRIM(b.TEMCUW) 原组别 ,\
TRIM(b.TEDSC1) 机型 from proddta.FE6TM01 a left join proddta.F5631001 b \
on TRIM(a.PPMCUF)=TRIM(b.TEDRID2) where TRIM(a.ppmmcu) IN('P1')  \
and a.ppopsc in ('A2')  \
AND a.PPPBDT>=DTON('" +later_day+ "') AND a.PPPBDT<=DTON('" + near_day + "') \
 AND PPURC1 !='C' and TRIM(a.PPMCUF) not in ('TN-01', 'TS-01') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df4x = pd.DataFrame(list_data)
df4x.columns=columns

#匹配单支重数据，进而通过数量和单支重计算稳定的完工重量
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("select distinct TRIM(IBITM) 短项目号,\
(SELECT UMCONV/10000000 FROM proddta.F41002 WHERE UMUM='PC' AND UMRUM='KG' AND IBITM=UMITM) 单支重 \
 FROM proddta.F4102, proddta.F4101 WHERE IMITM=IBITM")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
weight = pd.DataFrame(list_data)
weight.columns=columns

df4x=pd.merge(df4x,weight,on='短项目号',how='left')
df4x['完工重量']=df4x.完工数量*df4x.单支重

df4x['组别']=df4x.原组别.apply(lambda x :'B03' if x=='B05' else x)
df41=df4x[df4x.组别.isna()]
df42=df4x[df4x.组别.notna()]
for i in df41.index:
    if df41.loc[i,'机台编码'] in ['PS-11','PS-21','PS-32']:
        df41.loc[i,'机台编码']='PS-01'
        df41.loc[i,'组别']='P01'
    if df41.loc[i,'机台编码']=='SX-04':
        df41.loc[i,'机台编码']='SX-01'
        df41.loc[i,'组别']='QC-100001'
    if df41.loc[i,'工序码']=='B':
        df41.loc[i,'组别']='XP0'
df411=df41[df41.组别=='XP0']
df411['完工重量']=df411['完工数量']
df411['完工数量']=0
df412=df41[df41.组别!='XP0']
bg=pd.concat([df411,df412,df42])
bg['机型']=bg['机型'].fillna('')
bg['原组别']=bg['原组别'].fillna('')
bg=bg.groupby(['日期20','短项目号','工序号','工序码','组别','原组别','机型']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()


import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  报工表 \
WHERE 日期20 >='" + later_day + "' AND 日期20 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()


#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 报工表 ")
bg.to_sql('报工表', engine, if_exists='append', index=False,
          dtype={'日期20': sqlalchemy.types.DATE(),
                 '短项目号': sqlalchemy.types.INT(),
                 '工序号': sqlalchemy.types.INT(),
                 '工序码': sqlalchemy.types.String(length=20),
                 '组别': sqlalchemy.types.String(length=10),
                 '原组别': sqlalchemy.types.String(length=10),
                 '机型': sqlalchemy.types.String(length=10),
                 '完工数量': sqlalchemy.types.FLOAT(),
                 '完工重量': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 报工表 ADD PRIMARY KEY (日期20,短项目号,工序号,组别,原组别,机型); ")


# 入库表:
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(ILLITM) 项目号,TRIM(ILITM) 短项目号,ILTRQT/1000000 数量,ntod2(ILTRDJ) 入库日期, \
TRIM(ILMCU) 分部,(SELECT UMCONV/10000000 FROM proddta.F41002 \
WHERE UMUM='PC' AND UMRUM='KG' AND ILITM=UMITM) 单支重, ILDCTO,ILDCT \
from proddta.F4111  where \
ILTRDJ>=DTON('" +later_day+ "') AND ILTRDJ<=DTON('" + near_day + "') \
AND TRIM(ILDCT) IN ('RI','IC','I3','I4')  and TRIM(ILMCU) !='PW' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df5= pd.DataFrame(list_data)
df5.columns=columns
df5['重量']=df5.数量*df5.单支重

#产品入库重量和数量，【生产入库】要卡p1，ILDCT卡['IC','I3']
df5d=df5[(df5['分部']=='P1')&(df5['ILDCT'].isin(['IC','I3']))]
scrk=df5d.groupby(['入库日期','短项目号']).agg({
    '数量':'sum','重量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  生产入库表 \
WHERE 入库日期 >='" + later_day + "' AND 入库日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()


#存入数据  【生产入库】
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 生产入库表# ")
scrk.to_sql('生产入库表', engine, if_exists='append', index=False,
          dtype={'入库日期': sqlalchemy.types.DATE(),
                 '短项目号': sqlalchemy.types.INT(),
                 '数量': sqlalchemy.types.FLOAT(),
                 '重量': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 生产入库表 ADD PRIMARY KEY (入库日期,短项目号); ")


#包装材料总金额，按【包装入库】重量进行分摊 要卡'PP','PT'
df5b=df5[(df5['分部'].isin(['PP','PT']))&(df5['ILDCT'].isin(['IC','I4']))]
bzrk=df5b.groupby(['入库日期','短项目号']).agg({
    '数量':'sum','重量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  包装入库表 \
WHERE 入库日期 >='" + later_day + "' AND 入库日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()


#存入数据  【包装入库】
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 包装入库表x ")
bzrk.to_sql('包装入库表', engine, if_exists='append', index=False,
          dtype={'入库日期': sqlalchemy.types.DATE(),
                 '短项目号': sqlalchemy.types.INT(),
                 '数量': sqlalchemy.types.FLOAT(),
                 '重量': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 包装入库表 ADD PRIMARY KEY (入库日期,短项目号); ")


# 仓储出库表:
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(ILITM) 短项目号,TRIM(ILLITM) 项目号,ILTRQT/1000000*-1 数量,ntod2(ILTRDJ) 入库日期, \
(SELECT UMCONV/10000000 FROM proddta.F41002 \
WHERE UMUM='PC' AND UMRUM='KG' AND ILITM=UMITM) 单支重, ILDCTO,TRIM(ILMCU) 经营单位 \
from proddta.F4111 where \
ILTRDJ>=DTON('" +later_day+ "') AND ILTRDJ<=DTON('" + near_day + "') \
AND TRIM(ILMCU) in ('P1','PT','PP')  \
AND TRIM(ILDCTO) IN ('S6','SO','S2','ST','S1','EB','ED','SR','SJ','EJ','EF','EY','EH','S3','SZ','SY')  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df6= pd.DataFrame(list_data)
df6.columns=columns
df6['重量']=df6.数量*df6.单支重

ccck=df6[df6.单支重.notna()].groupby(['入库日期','短项目号']).agg({
    '数量':'sum','重量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  仓储出库表 \
WHERE 入库日期 >='" + later_day + "' AND 入库日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据  【仓储出库表】
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 仓储出库表 x")
ccck.to_sql('仓储出库表', engine, if_exists='append', index=False,
          dtype={'入库日期': sqlalchemy.types.DATE(),
                 '短项目号': sqlalchemy.types.INT(),
                 '数量': sqlalchemy.types.FLOAT(),
                 '重量': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 仓储出库表 ADD PRIMARY KEY (入库日期,短项目号); ")


# 委外
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  ntod(b.PRRCDJ) 收到日期,TRIM(a.WAITM) 短项目号,\
TRIM(WALITM) 项目号,TRIM(b.PRLITM) 第2项目号,b.PRUOM 规格,b.PRUREC/1000000 验收数量,\
b.PRPRRC/10000 单位成本,(SELECT UMCONV/10000000 FROM proddta.F41002 \
WHERE UMUM='PC' AND UMRUM='KG' AND a.WAITM=UMITM) 单支重 \
FROM proddta.f43121 b LEFT JOIN  proddta.F4801 a \
on to_char(a.WADOCO)=substr(b.PRVR01,0,8) \
WHERE TRIM(a.WAMMCU) ='P1' and \
b.PRTRDJ>=DTON('" +later_day+ "') AND b.PRTRDJ<=DTON('" + near_day + "') \
 and b.PRDCT='OV' AND TRIM(b.PRMCU)='P1' \
and b.PRDCTO='OO' and b.PRMATC='1' and b.PRUREC>'0' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df8x= pd.DataFrame(list_data)
df8x.columns=columns

#提取工艺路线表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" SELECT TRIM(IRKITL) 项目号,TRIM(IROPSQ) 工序号\
  FROM proddta.F3003 A \
LEFT JOIN proddta.F0006 B ON A.IRMCU=B.MCMCU \
LEFT JOIN PRODCTL.F0005 C ON C.DRSY = '00' AND C.DRRT = '28' AND TRIM(C.DRKY) = TRIM(B.MCRP28) \
LEFT JOIN PRODCTL.F0005 D ON D.DRSY = '00' AND D.DRRT = '29' AND TRIM(D.DRKY) = TRIM(B.MCRP29) \
LEFT JOIN PRODCTL.F0005 E ON E.DRSY = '00' AND E.DRRT = '30' AND TRIM(E.DRKY) = TRIM(B.MCRP30) \
WHERE TRIM(A.IRMMCU)='P1'  and TRIM(A.IRTRT)='M' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
gy2 = pd.DataFrame(list_data)
gy2.columns=columns

ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(RULITM) 项目号,TRIM(RUOPSQ) 工序号,\
TRIM(RUUITM) 第2项目号 from proddta.FE63101 where RUTRT='M'  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
sm2= pd.DataFrame(list_data)
sm2.columns=columns

smg=pd.merge(gy2,sm2,on=['项目号','工序号'],how='left')

df8=pd.merge(df8x,smg,on=['项目号','第2项目号'],how='left')
df8=df8[df8.工序号.notna()]
df8.第2项目号=df8.第2项目号.fillna('')
df8=df8.groupby(['收到日期','短项目号','第2项目号','规格','工序号',\
        '单位成本','单支重'])['验收数量'].sum().to_frame().reset_index()
df8['汇总']=df8['短项目号'].str.cat(df8['第2项目号'])

#规格为KG的，验收数量代表重量
df8kg=df8[df8['规格']=='KG']
df8kg['重量']=df8kg.验收数量
df8kg['金额']=df8kg.验收数量*df8kg.单位成本
df8kg['委外每吨成本']=df8kg.单位成本*1000
df8kg['委外单支成本']=df8kg.单位成本*df8kg.单支重
#规格为mp(千支)的，验收数量就是数量*1000
df8mp=df8[df8['规格']=='MP']
df8mp['验收数量']=df8mp.验收数量*1000
df8mp['单位成本']=df8mp.单位成本/1000
df8mp['重量']=df8mp.验收数量*df8mp.单支重
df8mp['金额']=df8mp.验收数量*df8mp.单位成本
df8mp['委外每吨成本']=df8mp.单位成本*1000/df8mp.单支重
df8mp['委外单支成本']=df8mp.单位成本
#规格为PC的，验收数量就是数量
df8pc=df8[df8['规格']=='PC']
df8pc['重量']=df8pc.验收数量*df8pc.单支重
df8pc['金额']=df8pc.验收数量*df8pc.单位成本
df8pc['委外每吨成本']=df8pc.单位成本*1000/df8pc.单支重
df8pc['委外单支成本']=df8pc.单位成本
ww=pd.concat([df8kg,df8mp,df8pc])

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  委外明细表 \
WHERE 收到日期 >='" + later_day + "' AND 收到日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据  【委外明细表】
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 委外明细表# ")
ww.to_sql('委外明细表', engine, if_exists='append', index=False,
          dtype={'收到日期': sqlalchemy.types.DATE(),
                 '汇总':sqlalchemy.types.String(length=20),
                 '短项目号': sqlalchemy.types.INT(),
                 '第2项目号':sqlalchemy.types.String(length=20),
                 '工序号':sqlalchemy.types.INT(),
                 '规格':sqlalchemy.types.String(length=10),
                 '验收数量': sqlalchemy.types.FLOAT(),
                 '单位成本': sqlalchemy.types.FLOAT(),
                 '单支重': sqlalchemy.types.FLOAT(),
                 '重量': sqlalchemy.types.FLOAT(),
                 '金额':sqlalchemy.types.FLOAT(),
                 '委外每吨成本': sqlalchemy.types.FLOAT(),
                 '委外单支成本': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 委外明细表 ADD PRIMARY KEY (收到日期,汇总,工序号); ")


#【委外明细表】每次增量完，【分摊表】更新（replace）一下
import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM 委外明细表 ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
data5x = pd.DataFrame(list_data)
data5x.columns=columns
data5x['收到日期']=pd.to_datetime(data5x['收到日期'],format='%Y-%m-%d')
td=[]
for i in data5x.汇总.unique():
    a=data5x[data5x.汇总==i].sort_values(by='收到日期',ascending=False)[:1]
    td.append(a)
ww_new=pd.concat(td).reset_index(drop=True)

#存入数据  【委外分摊表】
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 委外分摊表# ")
ww_new.to_sql('委外分摊表', engine, if_exists='replace', index=False,
          dtype={'收到日期': sqlalchemy.types.DATE(),
                 '汇总':sqlalchemy.types.String(length=20),
                 '短项目号': sqlalchemy.types.INT(),
                 '第2项目号':sqlalchemy.types.String(length=20),
                 '工序号':sqlalchemy.types.INT(),
                 '规格':sqlalchemy.types.String(length=10),
                 '验收数量': sqlalchemy.types.FLOAT(),
                 '单位成本': sqlalchemy.types.FLOAT(),
                 '单支重': sqlalchemy.types.FLOAT(),
                 '重量': sqlalchemy.types.FLOAT(),
                 '金额':sqlalchemy.types.FLOAT(),
                 '委外每吨成本': sqlalchemy.types.FLOAT(),
                 '委外单支成本': sqlalchemy.types.FLOAT()})
engine.connect().execute(" ALTER TABLE 委外分摊表 ADD PRIMARY KEY (收到日期,汇总,工序号); ")


# 物料耗用数据:
import cx_Oracle
import numpy as np
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select ntod2(ILTRDJ) 日期,TRIM(ILITM) 模具项目号,\
TRIM(ILLOTN) 批次序列号,ILSQOR/1000000*-1  主计量,\
ILTRQT/1000000*-1  辅计量 ,TRIM(ILRCD) 原组别 ,ILDCT 单据类型 from proddta.f4111 where \
TRIM(ILMCU)='W1' and  ILTRDJ>=DTON('" +later_day+ "') AND \
ILTRDJ<=DTON('" + near_day + "') and ILDCT in ('IG','IL','IM','IY','IC')  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df2= pd.DataFrame(list_data)
df2.columns=columns
df2['组别']=df2.原组别.apply(lambda x :'B03' if x=='B05' else x)
#匹配 工序码(abc)
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(IBITM) 模具项目号,TRIM(DRDL02)  工序码 FROM proddta.F4102 a \
LEFT JOIN PRODCTL.F0005 b on TRIM(IBPRP2)= \
TRIM(CASE WHEN trim(DRKY) IS NULL THEN '   ' ELSE to_char(DRKY) END) \
where b.DRRT='P2' and b.DRSY='41' and TRIM(a.IBMCU)='W1' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
sm= pd.DataFrame(list_data)
sm.columns=columns
df2=pd.merge(df2,sm,on='模具项目号',how='left')
def get_tail(x):
    if x=='' :
        return x
    else :
        return x[-1]
# 批次序列号 尾数为Z的数据，不用
df2['尾数']=df2.批次序列号.apply(get_tail)
df2=df2[df2.尾数!='Z']

# 把物料大类 拼接到耗用表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct  TRIM(IMPRP1) 物料大类,\
TRIM(IMITM) 模具项目号 from proddta.F4101 where TRIM(IMPRP2)!='FM' and \
(TRIM(IMPRP4)='SM' or TRIM(IMPRP4)='PM') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df32= pd.DataFrame(list_data)
df32.columns=columns
df2=pd.merge(df2,df32,on='模具项目号',how='left')


#提取匹配【定单类型】
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(IOLOTN) 批次序列号,TRIM(IOITM) 模具项目号,\
IODCTO 定单类型 from proddta.F4108 where TRIM(IOMCU)='W1'  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
tj= pd.DataFrame(list_data)
tj.columns=columns

df2t=pd.merge(df2,tj,on=['批次序列号','模具项目号'],how='left')
df2t=df2t[df2t.定单类型.isin(['A1','IK'])==False].drop('尾数',axis=1)
df2t['主计量']=df2t.主计量.fillna(0)
df2t['辅计量']=df2t.辅计量.fillna(0)
df2t=df2t.fillna('')
df2x=df2t.groupby(['日期','模具项目号','批次序列号','物料大类','原组别','组别','单据类型',
       '工序码', '定单类型']).agg({'主计量':'sum','辅计量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  物料耗用表 \
WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 物料耗用表 ")
df2x.to_sql('物料耗用表', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                 '模具项目号': sqlalchemy.types.INT(),
                 '批次序列号': sqlalchemy.String(length=20),
                '原组别': sqlalchemy.String(length=20),
                 '组别': sqlalchemy.types.String(length=10),
                 '工序码': sqlalchemy.types.String(length=10),
                 '物料大类':sqlalchemy.types.String(length=10),
                 '单据类型': sqlalchemy.types.String(length=10),
                 '定单类型': sqlalchemy.types.String(length=20),
                 '主计量': sqlalchemy.types.INT(),
                 '辅计量': sqlalchemy.types.INT()})
#engine.connect().execute(" ALTER TABLE 物料耗用表 ADD PRIMARY KEY (日期,模具项目号,批次序列号,原组别,组别,工序码,物料大类,单据类型,定单类型); ")


# 物料验收表数据
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  ntod(PRTRDJ) 定单日期,PRUREC/1000000 验收数量,\
TRIM(PRITM) 模具项目号,TRIM(PRLOTN) 批次序列号,PRPRRC/10000 单位成本,\
TRIM(PRITM) 短项目号,TRIM(PRLITM) 项目号,TRIM(PRKCOO) 一,TRIM(PRDOCO) 二,TRIM(PRDCTO) 三,TRIM(PRLNID) 四 \
from proddta.f43121 a WHERE a.PRDCT='OV' AND TRIM(a.PRMCU)='W1' \
and (PRDCTO='OM' or PRDCTO='ON' or  PRDCTO='OW') and a.PRMATC='1' and a.PRUREC>'0' \
and PRTRDJ>=DTON('" +later_day+ "') AND PRTRDJ<=DTON('" + near_day + "')  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df31= pd.DataFrame(list_data)
df31.columns=columns



# 匹配短项目号用
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct TRIM(IMITM) 模具项目号,TRIM(IMLITM) 项目号 from proddta.F4101  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
match= pd.DataFrame(list_data)
match.columns=columns

#验收表，没有短项目号的，先根据项目号，匹配进短项目号
df31_1=pd.merge(df31[df31.模具项目号=='0'].drop('模具项目号',axis=1),\
         match,on='项目号',how='left')
df31_m=pd.concat([df31[df31.模具项目号!='0'],df31_1]).drop('项目号',axis=1)

# 把物料小类，物料大类 拼接到验收表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct TRIM(IMPRP3) 工序码_油, TRIM(IMPRP2) 物料小类,TRIM(IMPRP1) 物料大类_,\
TRIM(IMITM) 模具项目号,TRIM(IMLITM) 项目号 from proddta.F4101 where TRIM(IMPRP2)!='FM' and \
(TRIM(IMPRP4)='SM' or TRIM(IMPRP4)='PM') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df32= pd.DataFrame(list_data)
df32.columns=columns

df312=pd.merge(df31_m,df32,on='模具项目号',how='left').dropna(subset=['模具项目号'],axis=0)


#匹配组别
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  distinct TRIM(PDKCOO) 一,TRIM(PDDOCO) 二,\
TRIM(PDDCTO) 三,TRIM(PDLNID) 四,TRIM(PDPDP3) 组别 from proddta.f4311  where TRIM(PDMCU)='W1'  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
mdb= pd.DataFrame(list_data)
mdb.columns=columns

df34=pd.merge(df312,mdb,on=['一', '二', '三', '四'],how='left').drop(['一', '二', '三', '四'],axis=1)
df34['验收数量']=df34['验收数量'].fillna(0)
df34['单位成本']=df34['单位成本'].fillna(0)
df34=df34.fillna('')
df34=df34.groupby(['定单日期','模具项目号','批次序列号','工序码_油','物料小类',
       '物料大类_','组别','单位成本']).agg({'验收数量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  物料验收表 \
WHERE 定单日期 >='" + later_day + "' AND 定单日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 物料验收表# ")
df34.to_sql('物料验收表', engine, if_exists='append', index=False,
          dtype={'定单日期': sqlalchemy.types.DATE(),
                 '模具项目号': sqlalchemy.String(length=20),
                 '批次序列号': sqlalchemy.String(length=20),
                 '组别': sqlalchemy.String(length=20),
                 '工序码_油': sqlalchemy.types.String(length=10),
                 '物料大类_':sqlalchemy.types.String(length=10),
                 '物料小类':sqlalchemy.types.String(length=10),
                 '验收数量': sqlalchemy.types.FLOAT(),
                 '单位成本': sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 物料验收表 ADD PRIMARY KEY (定单日期,模具项目号,批次序列号,组别,工序码_油,物料大类_,物料小类,单位成本,验收数量); ")


# 财务表:
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select ntod(GLDGJ) 总帐日期,TRIM(GLMCU) 经营单位,\
TRIM(GLDCT) 单据类型,TRIM(GLOBJ) 科目帐,TRIM(GLSUB) 明细帐,GLAA/100 金额,\
TRIM(GLEXA) 说明 \
from proddta.f0911  where TRIM(GLOBJ) in ('4201','5141') and \
 GLDGJ>=DTON('" +later_day+ "') AND GLDGJ<=DTON('" + near_day + "')  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df7= pd.DataFrame(list_data)
df7.columns=columns


#环保废水、污泥费
#经营单位='P1' AND 科目帐='4201' AND 明细帐='20'
hb2=df7[(df7.科目帐=='4201')&(df7['经营单位']=='P1')&(df7['明细帐']=='20')]
hb2.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
hb2['成本类型1']='环保'
hb2['成本类型2']='财报污泥处置'

#设备折旧
#科目帐='4201' AND 明细帐='05' and 经营单位<>'PW'
zj=df7[(df7.科目帐=='4201')&(df7['明细帐']=='05')]
zj.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
zj['成本类型1']='折旧'
zj['成本类型2']='折旧明细'

#研发
#科目帐='4201' AND 明细帐='05' and 经营单位<>'PW'
yf=df7[(df7.科目帐=='5141')&(df7['经营单位']=='11000')&\
       (df7['明细帐'].isin(['280489','280490','280491','280492','280493','280494']))]
yf.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
yf['成本类型1']='研发'
yf['成本类型2']='研发明细'

#其他，分摊给全部产品
other=df7[(df7['明细帐'].isin(['04','06','08','11','13','16','17','18','19','22']))\
 &(df7['经营单位']!='PW')&(df7.科目帐=='4201')]
other.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
other['成本类型1']='其他'

#水电蒸汽总金额
wt_mx=df7[(df7.科目帐=='4201')&(df7.明细帐=='03')]
wt_mx.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
wt_mx['成本类型1']='水电蒸汽'
wt_mx['成本类型2']='水电'

qt_mx=df7[(df7.科目帐=='4201')&(df7.明细帐=='07')]
qt_mx.columns=['日期','经营单位','单据类型','科目帐','明细帐','金额','说明']
qt_mx['成本类型1']='水电蒸汽'
qt_mx['成本类型2']='蒸汽'

zy=pd.concat([zj,yf])
zy['年月']=zy.日期.dt.strftime('%Y-%m')
zy['年月']=pd.to_datetime(zy.年月,format='%Y-%m')


import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  * FROM  固定资产折旧表  ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
data11 = pd.DataFrame(list_data)
data11.columns=columns
data11['开始日期']=pd.to_datetime(data11.开始日期,format='%Y-%m-%d')
data11['结束日期']=pd.to_datetime(data11.结束日期,format='%Y-%m-%d')

import datetime
from dateutil.relativedelta import relativedelta
tot=[]
for i1 in zy.年月.unique():
    i2=pd.Timestamp(i1).to_pydatetime()
    i3=i2+relativedelta(months=1)
    i4=np.datetime64(i3)-np.timedelta64(1,'D')
    df11=data11[(data11.结束日期>i1)&(data11.开始日期<i4)].dropna(subset=['原值'],axis=0)
    df11['月初']=i1
    df11['月末']=i4
    df11['天数1']=df11.结束日期-df11.月初
    df11['天数1']=df11['天数1'].astype(str).apply(lambda x:x[:-23])
    df11['天数1']=df11['天数1'].astype(int)
    df11['天数2']=df11.月末-df11.开始日期
    df11['天数2']=df11['天数2'].astype(str).apply(lambda x:x[:-24])
    df11['天数2']=df11['天数2'].astype(int)
    a1=df11[df11.天数1<30]
    a1['金额']=(a1.原值-a1.预计残值)*a1.天数1/(a1.折旧年份*365)
    a2=df11[df11.天数2<30]
    a2['金额']=(a2.原值-a2.预计残值)*a2.天数2/(a2.折旧年份*365)
    a3=df11[df11.index.isin(a1.index|a2.index)==False]
    a3['金额']=(a3.原值-a3.预计残值)/a3.折旧年份
    a4=pd.concat([a1,a2,a3])
    z=pd.read_excel(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\手工数据\组别分部.xlsx')
    a5=pd.merge(a4,z,on='组别',how='left')
    a6=a5[a5.分部.isin(['P1','PW'])].groupby(['组别','分部'])['金额'].sum().to_frame().reset_index()
    a6['金额']=a6.金额*zy[(zy.日期>=i1)&(zy.日期<=i4)].金额.sum()/a6.金额.sum()
    a6['日期']=i1
    a6['科目帐']=0
    a6['明细帐']=0
    a6['成本类型1']='折旧'
    tot.append(a6)
totx=pd.concat(tot)

totx1=totx[totx.分部=='P1']
totx1['成本类型2']='一期'
totx2=totx[totx.分部=='PW']
totx2['成本类型2']='二期'

cw=pd.concat([hb2,zj,yf,other,wt_mx,qt_mx,totx1,totx2])
cw=cw.fillna('')
cw=cw.groupby(['日期','组别','经营单位','单据类型','科目帐','明细帐','说明', '成本类型1',
       '成本类型2']).agg({'金额':'sum'}).reset_index()

def get1(x):
    if  x[:2]=='暂估' or x[:1]=='暂' or x[1:2]=='估':
        return x[:2]
    else:
        return ''
cw['标记']=cw.说明.apply(get1)

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  财务实际成本11 \
WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 财务实际成本11 ")
cw.to_sql('财务实际成本11', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                 '组别': sqlalchemy.types.String(length=10),
                 '经营单位': sqlalchemy.types.String(length=20),
                 '单据类型': sqlalchemy.types.String(length=20),
                 '科目帐': sqlalchemy.types.INT(),
                 '明细帐':sqlalchemy.types.INT(),
                 '说明':sqlalchemy.types.String(length=100),
                 '金额': sqlalchemy.types.FLOAT(),
                 '成本类型1': sqlalchemy.types.String(length=10),
                 '成本类型2': sqlalchemy.types.String(length=10),
                 '标记': sqlalchemy.types.String(length=10)})
#engine.connect().execute(" ALTER TABLE 财务实际成本11 ADD PRIMARY KEY (日期,组别,经营单位,科目帐,明细帐,金额,说明,成本类型1,成本类型2); ")


#皮膜   生产二部录入成本95:取最大值
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select ntod2(BPTRDJ) 日期,BPAEXP/100 一期倒立式95 \
from PRODDTA.F560309 where BPTRDJ>=DTON('2020-1-1') \
and  BPTRDJ=(select max(BPTRDJ) from PRODDTA.F560309) ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df14= pd.DataFrame(list_data)
df14.columns=columns

ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(IXKITL) 第二项目号,TRIM(IXLITM)  项目号,\
TRIM(IXITM) 项目号短,TRIM(IXKIT) 父项号 from proddta.f3002  \
where TRIM(IXMMCU)='P1'  and (TRIM(IXCMCU)='M1' or TRIM(IXCMCU)='PW')  AND TRIM(IXTBM)='O' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df15= pd.DataFrame(list_data)
df15.columns=columns

#提取工艺路线表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" SELECT TRIM(IRKITL) 项目号,TRIM(IROPSQ) 工序号,\
TRIM(C.DRDL01) 制程,TRIM(IRDSC1) 说明  FROM proddta.F3003 A \
LEFT JOIN proddta.F0006 B ON A.IRMCU=B.MCMCU \
LEFT JOIN PRODCTL.F0005 C ON C.DRSY = '00' AND C.DRRT = '28' AND TRIM(C.DRKY) = TRIM(B.MCRP28) \
LEFT JOIN PRODCTL.F0005 D ON D.DRSY = '00' AND D.DRRT = '29' AND TRIM(D.DRKY) = TRIM(B.MCRP29) \
LEFT JOIN PRODCTL.F0005 E ON E.DRSY = '00' AND E.DRRT = '30' AND TRIM(E.DRKY) = TRIM(B.MCRP30) \
WHERE TRIM(A.IRMMCU)='PW'  and TRIM(A.IRTRT)='M' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
gy = pd.DataFrame(list_data)
gy.columns=columns

gy1=gy[gy.说明=='倒立式SX1-03'][['项目号','说明']]
gy1['每吨制造成本']=df14.一期倒立式95.values[0]

df15_2=pd.merge(df15[df15.项目号.isin(gy1.项目号)],gy1,on='项目号',how='left')

gy21=gy[gy.说明!='倒立式SX1-03'].groupby(['项目号','制程'])['工序号'].count().to_frame().reset_index()
gy22=gy21[gy21.制程.isin(['线材伸线','线材退火'])]
gy22['制程简']=gy22.制程.apply(lambda x:x[2:3])
gy22['制程简']=gy22.制程简.apply(lambda x:'抽' if x=='伸' else x)
gy22['工序号']=gy22.工序号.astype(str)
gy22['汇总']=gy22['工序号'].str.cat(gy22['制程简'])

name=[]
ty=[]
for i in gy22.项目号.unique():
    name.append(i)
    if gy22[gy22.项目号==i].shape[0]>1:
        ty.append(gy22[gy22.项目号==i].汇总.values[0]+\
              gy22[gy22.项目号==i].汇总.values[1])
    else:
        ty.append(gy22[gy22.项目号==i].汇总.values[0])

gy23=pd.DataFrame(ty,name).reset_index()
gy23.columns=['项目号','制程汇总']
def g_change(x):
    if len(x)==2 and x[1]=='抽':
        return x+'0退'
    if len(x)==2 and x[1]=='退':
        return '0抽'+x
    else:
        return x
gy23['制程汇总']=gy23.制程汇总.apply(g_change)
gy23['数值']=gy23.项目号.apply(lambda x:'大' if int(x[4:8])>=450 else '小')

gy231=gy23[gy23.制程汇总=='1抽0退']
gy231['抽退说明']=gy231['制程汇总'].str.cat(gy231['数值'])
gy231=gy231[['项目号','抽退说明']]
gy232=gy23[gy23.制程汇总!='1抽0退'][['项目号','制程汇总']]
gy232.columns=['项目号','抽退说明']
gy3=pd.concat([gy231,gy232])

df15_3=pd.merge(df15[df15.项目号.isin(gy1.项目号)==False],gy3,on='项目号',how='left')

ct_cost=pd.read_csv(r'\\172.16.6.20\public\BI\数据中心\正式开发文档\Script\PMGDataQVD\计算后表格\抽退方式制造成本.csv')
ct_cost['t']=ct_cost.年月.apply(lambda x:x.split('月')[1]+'-'+x.split('月')[0])
ct_cost['t']=pd.to_datetime(ct_cost['t'],format='%Y-%m')
ct_cost.sort_values(by='t',ascending=False)
ct=[]
for i in ct_cost.抽退说明.unique():
    a=ct_cost[ct_cost.抽退说明==i].sort_values(by='t',ascending=False)[:1]
    ct.append(a)
ct_std=pd.concat(ct)[['抽退说明','制造成本']]
df15_3x=pd.merge(df15_3,ct_std,on='抽退说明',how='left').dropna(subset=["制造成本"])
df15_3x.columns=['第二项目号','项目号','项目号短','父项号','说明','每吨制造成本']

gy5=pd.concat([df15_2,df15_3x])

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 皮膜表x ")
gy5.to_sql('皮膜表', engine, if_exists='replace', index=False,
          dtype={'第二项目号': sqlalchemy.types.String(length=50),
                 '项目号': sqlalchemy.types.String(length=20),
                 '项目号短': sqlalchemy.types.INT(),
                 '父项号': sqlalchemy.types.INT(),
                 '说明':sqlalchemy.types.String(length=20),
                 '每吨制造成本': sqlalchemy.types.FLOAT()})
engine.connect().execute(" ALTER TABLE 皮膜表 ADD PRIMARY KEY (项目号短,父项号,每吨制造成本); ")


#薪资
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute("select NTOD(a.XZTRDJ) 日期,TRIM(b.XZDL01) 组别名,TRIM(b.XZDL02) 科,\
TRIM(b.XZDL03) 部门,TRIM(a.XZSEG1) 组别新,\
a.XZAN02/100 薪资,a.XZNA7/100 实际出勤时长,a.XZNA8/100 平日加班时数,\
a.XZNA9/100 假日加班时数,a.XZNA10/100 法定日加班时数,\
a.XZAA/100 人数,a.XZAEXP/100 养老险,a.XZAREC/100 医疗险,a.XZECST/100 失业险,\
a.XZARLV/100 工商险,a.XZACHG/100 公积金,a.XZAN03/100 奖金 from proddta.F560311 a \
left join proddta.F560310 b on TRIM(a.XZSEG1)=TRIM(b.XZSEG1)  \
where a.XZTRDJ>=DTON('" +later_day+ "') AND a.XZTRDJ<=DTON('" + near_day + "') ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
dfs= pd.DataFrame(list_data)
dfs.columns=columns
dfs['总薪资']=dfs.薪资+dfs.养老险+dfs.医疗险+dfs.失业险+dfs.工商险+dfs.公积金+dfs.奖金
dfs['总时长']=dfs.实际出勤时长+dfs.平日加班时数+dfs.假日加班时数+dfs.法定日加班时数
for i in ['薪资','实际出勤时长','平日加班时数','假日加班时数',
    '法定日加班时数','养老险','医疗险','失业险','工商险','公积金','奖金','总薪资','总时长']:
    dfs[i]=dfs[i].astype(float)
dfs=dfs.fillna('')
dfs1 = dfs[(dfs.部门.isin(['生产一部', '品保课', '环境安全课'])) & \
           (dfs.科.isin(['品保二课', '仓储课']) == False)]
dfs1['成本类型1'] = '薪资'
smx4 = dfs[(dfs.科 == '仓储课') & (dfs.组别名 == '一期仓管组')]
smx4['成本类型1'] = '薪资'
smx4['成本类型2'] = '仓储薪资'
smx5 = dfs[(dfs.科 == '仓储课') & (dfs.组别名 != '一期仓管组')]
smx5['成本类型1'] = '薪资'
smx5['成本类型2'] = '包装薪资'
smx6 = dfs[dfs.组别新 == 'F02']
smx6['成本类型1'] = '薪资'
smx6['成本类型2'] = '废料薪资'
salary = pd.concat([dfs1, smx4, smx5, smx6])
salary = salary.fillna('')

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  薪资实际成本10 \
WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 薪资实际成本10# ")
salary.to_sql('薪资实际成本10', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                 '组别新': sqlalchemy.types.String(length=20),
                 '处理后组别': sqlalchemy.types.String(length=20),
                 '组别名': sqlalchemy.types.String(length=20),
                 '科': sqlalchemy.types.String(length=10),
                 '部门':sqlalchemy.types.String(length=10),
                 '薪资': sqlalchemy.types.FLOAT(),
                 '实际出勤时长': sqlalchemy.types.FLOAT(),
                 '平日加班时数': sqlalchemy.types.FLOAT(),
                 '假日加班时数': sqlalchemy.types.FLOAT(),
                 '法定日加班时数': sqlalchemy.types.FLOAT(),
                 '人数': sqlalchemy.types.INT(),
                 '养老险': sqlalchemy.types.FLOAT(),
                 '医疗险': sqlalchemy.types.FLOAT(),
                 '失业险': sqlalchemy.types.FLOAT(),
                 '工商险': sqlalchemy.types.FLOAT(),
                 '公积金': sqlalchemy.types.FLOAT(),
                 '奖金': sqlalchemy.types.FLOAT(),
                 '总薪资': sqlalchemy.types.FLOAT(),
                 '总时长': sqlalchemy.types.FLOAT(),
                 '成本类型1': sqlalchemy.types.String(length=10),
                 '成本类型2': sqlalchemy.types.String(length=10)})
#engine.connect().execute(" ALTER TABLE 薪资实际成本10 ADD PRIMARY KEY \
#(组别新, 日期, 总薪资, 总时长, 组别名, 科, 部门,成本类型1, 成本类型2); ")


# 外采验收表:
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select ntod(PRRCDJ) 日期,TRIM(PRITM) 短项目号,TRIM(PRUOM) 计量单位,\
PRUREC/1000000 数量 from proddta.f43121 where  \
(PRDCTO='OP' OR  PRDCTO='OG') and  \
PRRCDJ>=DTON('" +later_day+ "') AND PRRCDJ<=DTON('" + near_day + "')  \
and (TRIM(PRMCU)='P1' or TRIM(PRMCU)='GZ' or TRIM(PRMCU)='SZ') \
AND PRDCT='OV' and PRAN8='55120157' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
f= pd.DataFrame(list_data)
f.columns=columns

#规格为mp(千支)的，验收数量就是数量*1000
f1=f[f['计量单位']=='MP']
f1['数量']=f1.数量*1000
#规格为PC的，验收数量就是数量
f2=f[f['计量单位']=='PC']
#规格为KG的，验收数量代表重量
f3=f[f['计量单位']=='KG']
f3['重量']=f3.数量
f4=pd.merge(pd.concat([f1,f2]),weight,on='短项目号',how='left')
f4['重量']=f4.数量*f4.单支重
f5=pd.concat([f3,f4.drop('单支重',axis=1)])
f5=f5.groupby(['日期','短项目号','计量单位']).agg({
    '数量':'sum','重量':'sum'}).reset_index()

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
try:
   cur.execute("DELETE  FROM  外采验收表 \
WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'")
   connection.commit()
   print("delete OK")
except:
   connection.rollback()

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 外采验收表# ")
f5.to_sql('外采验收表', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                 '短项目号': sqlalchemy.types.INT(),
                 '计量单位': sqlalchemy.types.String(length=10),
                 '数量': sqlalchemy.types.FLOAT(),
                 '重量':sqlalchemy.types.FLOAT()})
#engine.connect().execute(" ALTER TABLE 外采验收表 ADD PRIMARY KEY (日期,短项目号,计量单位,数量,重量); ")


# 固定资产折旧表:
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select ntod(FAEFTB) 开始日期,FANUMB 资产号,\
TRIM(FAMCU) 组别 from proddta.F1201 where FAXOBJ='4201' AND FAXSUB='05' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df7x1= pd.DataFrame(list_data)
df7x1.columns=columns

ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct FLNUMB 资产号,FLADLM/12  折旧年份,FLAPYC/100 原值,\
FLTKER/100 预计残值,FLFY from proddta.F1202 WHERE  FLOBJ='1601' and FLAPYC>0 ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df7x2= pd.DataFrame(list_data)
df7x2.columns=columns
x3=[]
for i in df7x2.资产号.unique():
    a=df7x2[df7x2.资产号==i]
    b=a[a.FLFY==a.FLFY.max()].sort_values(by='原值')[-1:]
    x3.append(b)
df7x3=pd.concat(x3).drop('FLFY',axis=1)

df7x4=pd.merge(df7x1,df7x3,on='资产号',how='left').dropna(subset=['折旧年份'],axis=0)

df7x4['开始日期']=df7x4['开始日期'].dt.strftime('%Y-%m-%d')
df7x4['开始日期']=pd.to_datetime(df7x4.开始日期,format='%Y-%m-%d')
for i in df7x4.index:
    j=int(df7x4.loc[i,'折旧年份']*365)
    df7x4.loc[i,'结束日期']=df7x4.loc[i,'开始日期']+np.timedelta64(j,'D')

df7x4['结束日期']=df7x4['结束日期'].dt.strftime('%Y-%m-%d')
df7x4['结束日期']=pd.to_datetime(df7x4.结束日期,format='%Y-%m-%d')

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
engine.connect().execute(" DROP TABLE 固定资产折旧表 ")
df7x4.to_sql('固定资产折旧表', engine, if_exists='append', index=False,
          dtype={'开始日期': sqlalchemy.types.DATE(),
                 '结束日期': sqlalchemy.types.DATE(),
                 '资产号': sqlalchemy.types.INT(),
                 '组别': sqlalchemy.types.String(length=10),
                 '折旧年份': sqlalchemy.types.INT(),
                 '原值':sqlalchemy.types.FLOAT(),
                 '预计残值':sqlalchemy.types.FLOAT()})
engine.connect().execute(" ALTER TABLE 固定资产折旧表 ADD PRIMARY KEY (开始日期,资产号,组别,折旧年份,原值,预计残值); ")

print('完成')
