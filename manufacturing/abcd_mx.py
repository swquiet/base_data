import datetime
import numpy as np
import pandas as pd
nt=datetime.datetime.now()
zt= nt - datetime.timedelta(days=1)
near_day=zt.strftime('%Y-%m-%d')
lt= nt-datetime.timedelta(days=14)
later_day=lt.strftime('%Y-%m-%d')
print(near_day)
print(later_day)

# 物料耗用数据:
import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM 物料耗用表 \
    WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'  ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df2x = pd.DataFrame(list_data)
df2x.columns=columns
df2x['模具项目号']=df2x.模具项目号.astype(str)

# 物料验收表全部数据，用于匹配单位成本
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  ntod(PRTRDJ) 定单日期,\
TRIM(PRITM) 模具项目号,TRIM(PRLOTN) 批次序列号,PRPRRC/10000 单位成本,\
TRIM(PRITM) 短项目号,TRIM(PRLITM) 项目号 \
from proddta.f43121 a WHERE a.PRDCT='OV' AND TRIM(a.PRMCU)='W1' \
and (PRDCTO='OM' or PRDCTO='ON' or  PRDCTO='OW') and a.PRMATC='1' and a.PRUREC>'0'  ")
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
cursor.execute(" select distinct TRIM(IMITM) 短项目号,TRIM(IMLITM) 项目号 from proddta.F4101  ")
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
df31_1=pd.merge(df31[df31.短项目号=='0'].drop('短项目号',axis=1),\
         match,on='项目号',how='left')
df31_m=pd.concat([df31[df31.短项目号!='0'],df31_1]).drop('项目号',axis=1)


# 把物料小类 拼接到验收表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct  TRIM(IMPRP2) 物料小类,\
TRIM(IMITM) 短项目号 from proddta.F4101 where TRIM(IMPRP2)!='FM' and \
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

df312=pd.merge(df31_m,df32,on='短项目号',how='left').drop('短项目号',axis=1)
df312['批次序列号']=df312.批次序列号.fillna('')
def get_tail(x):
    if x=='' :
        return x
    else :
        return x[-1]
df312['尾数']=df312.批次序列号.apply(get_tail)
df312=df312[df312.尾数!='Z'].drop('尾数',axis=1)
# 取日期最前的值作为单价，通过去重删除后面日期的单价。
df_p=df312.sort_values(by='定单日期').drop_duplicates(\
        subset=['模具项目号','批次序列号'],keep='first')


#物料耗用表为主表，进行拼接。
df33=pd.merge(df2x,df_p,on=['模具项目号','批次序列号'],how='left')
# w12 为化学品，需要单独处理
dx1=df33[df33['物料小类']!='W12']
dx2=df33[df33['物料小类']=='W12']

#获取标准成本，再次进行匹配
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(IBITM) 模具项目号,IBSETL/100 单位成本 \
FROM proddta.F4102 where TRIM(IBMCU)='W1' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
sp= pd.DataFrame(list_data)
sp.columns=columns


x1=dx1[dx1.单位成本.notna()]
x2=dx1[dx1.单位成本.isna()].drop(['单位成本'], axis=1)
#能取到标准成本用标准成本
x21=pd.merge(x2,sp,on='模具项目号',how='left')
x211=x21[x21.单位成本!=0]

#不能取到标准成本，需要用最早日期作为成本
x212=x21[x21.单位成本==0].drop(['单位成本'], axis=1)
# 模具项目号，获取验收表最早日期为其单位价格
price=df312[['定单日期','模具项目号','单位成本']].sort_values(\
    by=['定单日期','单位成本'],ascending=[True,False]).drop_duplicates(\
    subset=['模具项目号'],keep='last')[['模具项目号','单位成本']]
x212a=pd.merge(x212,price,on='模具项目号',how='left')

# 依然匹配不到单价的，不用
x212a=x212a[x212a.单位成本.notna()]

dx11=pd.concat([x211,x212a,x1]).drop(['定单日期'],axis=1)

a=dx11[dx11['物料大类']=='00AM']
b=dx11[dx11['物料大类']=='00BM']
c=dx11[dx11['物料大类']=='00CM']
a['总金额']=a.主计量*a.单位成本
a=a.rename(columns={'主计量':'数量'})
b['总金额']=b.主计量*b.单位成本
b=b.rename(columns={'主计量':'数量'})
c['总金额']=c.辅计量*c.单位成本
c=c.rename(columns={'辅计量':'数量'})
abc_x=pd.concat([a,b,c]).reset_index(drop=True)
abc_x['组别']=abc_x.组别.fillna('')
abc_x['工序码']=abc_x.工序码.fillna('')
abc_x['物料小类']=abc_x.物料小类.fillna('')
abc_x['批次序列号']=abc_x.批次序列号.fillna('')
abc=abc_x.groupby(['日期','模具项目号','批次序列号','组别','工序码','物料大类','物料小类','单位成本']).agg({
    '数量':'sum','总金额':'sum'
}).reset_index()
abc['成本类型1']='ABC物料'


# 化学品（w12）
dx2['总金额']=dx2['辅计量']*dx2['单位成本']
dx2['数量']=dx2.辅计量
w12=dx2[(dx2.组别!='L02')&(dx2.组别.notna())].groupby(
    ['日期','模具项目号','组别','物料大类','物料小类','单位成本']).agg({
    '数量':'sum','总金额':'sum'}).reset_index()
w12['成本类型1']='化学品'

#环保 lo2和组别为空的部分
hb1_mx=pd.concat([dx2[dx2.组别.isna()].where(dx2[dx2.组别.isna()].notnull(),''),dx2[dx2.组别=='L02']])
hb1=hb1_mx.groupby(
    ['日期','模具项目号','组别','物料大类','物料小类','单位成本']).agg({
    '数量':'sum','总金额':'sum'}).reset_index()
hb1['成本类型1']='化学品'
hb1['成本类型2']='化学品_环保'

#验收表
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM 物料验收表 \
WHERE 定单日期 >='" + later_day + "' AND 定单日期 <= '" + near_day + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df34 = pd.DataFrame(list_data)
df34.columns=columns

#D类 用到的数据
d=df34[(df34.物料大类_=='DM')&(df34.物料小类!='MMJ')&(df34.组别!='L04')]
d['金额']=d.验收数量*d.单位成本

# D类中的 油品'LLO',根据报工表（成型1000），按重量分摊
d2=d[d['物料小类']=='LLO']

# D类中的 油品'LLO',根据报工表（成型1000），按重量分摊
d2=d[(d['物料小类']=='LLO')&(d['组别']!='K01')]
llo_mix1=d2[d2.工序码_油!=''].groupby(
    ['定单日期','模具项目号','工序码_油','物料大类_','物料小类','组别','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
llo_mix1.columns=['日期','模具项目号','工序码_油','物料大类','物料小类','组别','单位成本','数量','总金额']
llo_mix1['成本类型1']='油'
llo_mix2=d2[d2.工序码_油==''].groupby(
    ['定单日期','模具项目号','物料大类_','物料小类','组别','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
llo_mix2.columns=['日期','模具项目号','物料大类','物料小类','组别','单位成本','数量','总金额']
llo_mix2['成本类型1']='油'
llo_mix3=d[(d['物料小类']=='LLO')&(d['组别']=='K01')].groupby(
    ['定单日期','模具项目号','工序码_油','物料大类_','物料小类','组别','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
llo_mix3.columns=['日期','模具项目号','工序码_油','物料大类','物料小类','组别','单位成本','数量','总金额']
llo_mix3['成本类型1']='油'
llo_mix3['成本类型3']='仓储物料'
llo_mx=pd.concat([llo_mix1,llo_mix2,llo_mix3])

#仓储：‘K01'
dc=d[(d['组别']=='K01')&(d['物料小类']!='LLO')]
dc_mx=dc.groupby(
    ['定单日期','模具项目号','组别','物料大类_','物料小类','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
dc_mx['成本类型1']='D类物料'
dc_mx['成本类型2']='仓储物料'
dc_mx.columns=['日期','模具项目号','组别','物料大类','物料小类','单位成本','数量','总金额','成本类型1','成本类型2']

#d类剔除 ['LLO','K01','Z01','L01','L02']
d1=d[(d.组别.isin(['LLO','K01','Z01','L01','L02'])==False)&\
     (d['物料小类']!='LLO')]
d_mx=d1.groupby(['定单日期','模具项目号','组别','物料大类_','物料小类','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
d_mx['成本类型1']='D类物料'
d_mx.columns=['日期','模具项目号','组别','物料大类','物料小类','单位成本','数量','总金额','成本类型1']


#包装材料 金额来源有2部分：db 和 ['Z01']
db=df34[(df34['物料大类_'].isin(['PM','PL']))&(df34.组别!='L04')]
db['金额']=db.验收数量*db.单位成本
db['组别']=db.组别.fillna('')
db_mx=pd.concat([db,d[d.组别.isin(['Z01'])]]).groupby(
    ['定单日期','模具项目号','组别','物料大类_','物料小类','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
db_mx['成本类型1']='包装材料'
db_mx.columns=['日期','模具项目号','组别','物料大类','物料小类','单位成本','数量','总金额','成本类型1']


#'L01','L02'归为环保
hb_d=d[d.组别.isin(['L01','L02'])].groupby(
    ['定单日期','模具项目号','组别','物料大类_','物料小类','单位成本']).agg({
    '验收数量':'sum','金额':'sum'}).reset_index()
hb_d.columns=['日期','模具项目号','组别','物料大类','物料小类','单位成本','数量','总金额']
hb_d['成本类型1']='D类物料'
hb_d['成本类型2']='环保'

mx=pd.concat([abc,w12,hb1,llo_mx,dc_mx,d_mx,db_mx,hb_d])
mx=mx.fillna('')


import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  日期  FROM  物料实际成本9 \
WHERE 日期 >='" + later_day + "' AND 日期 <= '" + near_day + "'")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
data9 = pd.DataFrame(list_data)
data9.columns=columns

ap=mx[mx.日期.isin(data9.日期)==False]

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 物料实际成本9 ")
ap.to_sql('物料实际成本9', engine, if_exists='append', index=False,
          dtype={'日期': sqlalchemy.types.DATE(),
                 '模具项目号': sqlalchemy.types.INT(),
                 '批次序列号':sqlalchemy.types.String(length=20),
                 '工序码_油': sqlalchemy.types.String(length=20),
                 '工序码': sqlalchemy.types.String(length=20),
                 '组别': sqlalchemy.types.String(length=10),
                 '物料大类':sqlalchemy.types.String(length=10),
                 '物料小类':sqlalchemy.types.String(length=10),
                 '单位成本': sqlalchemy.types.FLOAT(),
                 '数量': sqlalchemy.types.FLOAT(),
                 '总金额': sqlalchemy.types.FLOAT(),
                 '成本类型1': sqlalchemy.types.String(length=20),
                 '成本类型2': sqlalchemy.types.String(length=20),
                 '成本类型3': sqlalchemy.types.String(length=20)})
#engine.connect().execute(" ALTER TABLE 物料实际成本9 ADD PRIMARY KEY (日期,模具项目号,批次序列号,组别,物料大类,物料小类,工序码,工序码_油,单位成本,数量); ")

