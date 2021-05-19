#制造费用分摊表，abc类2年计算，其余用1年数据计算
import datetime
import numpy as np
import pandas as pd
nt=datetime.datetime.now()
nt=int(nt.strftime('%Y'))
#去年一整年时间
ay1=str(nt-1)+'-01-01'
ay2=str(nt-1)+'-12-31'
#去年和前年两年时间
by1=str(nt-2)+'-01-01'
by2=str(nt-1)+'-12-31'

# 产品BOM:
import cx_Oracle
import numpy as np
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  TRIM(IXKIT) 短项目号,TRIM(IXITM) 模具项目号, \
TRIM(IXOPSQ)  工序号,TRIM(IXTBM) R from proddta.f3002   where  \
substr(IXTBM,0,1)='R'  AND TRIM(IXMMCU)='P1' and TRIM(IXCMCU)='W1' ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df1x= pd.DataFrame(list_data)
df1x.columns=columns
df1x['汇总']=df1x['短项目号'].str.cat(df1x['模具项目号']).str.cat(df1x['工序号'])
R=[]
for i in df1x.汇总.unique():
    a=df1x[df1x.汇总==i]
    if a.R.unique().shape[0]==2:
        R.append(a[a.R=='R'])
    else:
        R.append(a)
df1=pd.concat(R)
df1['短项目号']=df1.短项目号.astype(int)
df1['工序号']=df1.工序号.astype(int)

#ABC类算2年，耗用表，报工表取2年数据
# 物料耗用数据:
import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM 物料耗用表 \
 WHERE 日期 >='" + by1 + "' AND 日期 <= '" + by2 + "' ")
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
df2x['日期']=pd.to_datetime(df2x['日期'],format='%Y-%m-%d')

# 物料验收表获取单位成本
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  ntod(PRTRDJ) 定单日期,PRUREC/1000000 验收数量,\
TRIM(PRITM) 模具项目号,TRIM(PRLOTN) 批次序列号,PRPRRC/10000 单位成本,PRDOCO 定单号,\
PRITM 短项目号 from proddta.f43121 a WHERE a.PRDCT='OV' AND TRIM(a.PRMCU)='W1' \
and (PRDCTO='OM' or PRDCTO='ON' or  PRDCTO='OW') and a.PRMATC='1' and a.PRUREC>'0' ")
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
df31['批次序列号']=df31.批次序列号.fillna('')
df31['模具和批号']=df31['模具项目号'].str.cat(df31['批次序列号'])
# 把物料小类，物料大类 拼接到验收表
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select distinct TRIM(IMPRP3) 工序码_油, TRIM(IMPRP2) 物料小类,TRIM(IMPRP1) 物料大类_,\
IMITM 短项目号 from proddta.F4101 where TRIM(IMPRP2)!='FM' and \
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
df312=pd.merge(df31,df32,on='短项目号',how='left').drop('短项目号',axis=1)
def get_tail(x):
    if x=='' :
        return x
    else :
        return x[-1]
df312['尾数']=df312.批次序列号.apply(get_tail)
df312=df312[df312.尾数!='Z'].drop('尾数',axis=1)
# 取日期最前的值作为单价，通过去重删除后面日期的单价。
df_p=df312.sort_values(by='定单日期').drop_duplicates(subset=['模具和批号'],keep='first')

#物料耗用表为主表，进行拼接。
df33=pd.merge(df2x,df_p,on=['模具项目号','批次序列号'],how='left')
# w12 为化学品，需要单独处理
dx1=df33[df33['物料小类']!='W12']
dx2=df33[df33['物料小类']=='W12']

#获取单位成本
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

x21=pd.merge(x2,sp,on='模具项目号',how='left')
#能取到标准成本用标准成本
x211=x21[x21.单位成本!=0]
#不能取到标准成本，需要用最晚日期作为成本
x212=x21[x21.单位成本==0].drop(['单位成本'], axis=1)

# 模具项目号，获取验收表最早日期为其单位价格
price=df312[['定单日期','模具项目号','单位成本']].sort_values(\
    by=['定单日期','单位成本'],ascending=[True,False]).drop_duplicates(\
    subset=['模具项目号'],keep='last')[['模具项目号','单位成本']]
x212a=pd.merge(x212,price,on='模具项目号',how='left')
# 依然匹配不到单价的，不用
x212a=x212a[x212a.单位成本.notna()]

dx11=pd.concat([x211,x212a,x1]).drop(['定单日期','模具和批号'],axis=1)

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
abc_x['工序码_油']=abc_x.工序码_油.fillna('')
abc_x['工序码']=abc_x.工序码.fillna('')
abc_x['物料小类']=abc_x.物料小类.fillna('')
abc_x['批次序列号']=abc_x.批次序列号.fillna('')
abc=abc_x.groupby(['日期','模具项目号','批次序列号','组别','工序码_油','工序码','物料大类','物料小类','单位成本']).agg({
    '数量':'sum','总金额':'sum'
}).reset_index()
abc['成本类型1']='ABC物料'

#ABC类算2年，耗用表，报工表取2年数据
#获取报工整合后的原数据
import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute(" SELECT  *  FROM  报工表 \
WHERE 日期20 >='" + by1 + "' AND 日期20 <= '" + by2 + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df4x = pd.DataFrame(list_data)
df4x.columns=columns
df4x['日期20']=pd.to_datetime(df4x['日期20'],format='%Y-%m-%d')

#报工表 加一个组别P04，为外采的重量和数量
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  sum(数量) 完工数量,sum(重量) 完工重量 FROM  外采验收表 \
WHERE 日期 >='" + ay1 + "' AND 日期 <= '" + ay2 + "'")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
dw10 = pd.DataFrame(list_data)
dw10.columns=columns
dw10['组别']='P04'
dw10["日期20"]=ay2
dw10["日期20"]=pd.to_datetime(dw10["日期20"],format='%Y-%m-%d')

df4=pd.concat([dw10,df4x])

#abc类用2年报工数据
dfg=df4.groupby(['短项目号','工序号','组别']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()
dfb=pd.merge(dfg,df1[['短项目号', '模具项目号', '工序号']],\
    on=['短项目号','工序号'],how='left').dropna(subset=['模具项目号'],axis=0)

b2=df4.groupby(['组别']).agg({'完工数量':'sum','完工重量':'sum'}).reset_index()

b3=df4[df4.工序号==1000].groupby(['工序号']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()

#其他用1年报工数据
df4_0=df4[(df4.日期20>=ay1)&(df4.日期20<=ay2)]
b2x=df4_0.groupby(['组别']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()
# 根据 成型1000 对应的组别，按重量分摊
b5=df4_0[(df4_0.工序号==1000)&(df4_0.组别!='')].groupby(['组别']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()

#1，ABC有组别有bom，按模具分摊
ABC=pd.merge(abc[abc.模具项目号.isin(dfb.模具项目号)].groupby(\
    ['模具项目号']).agg({'数量':'sum','总金额':'sum'}).reset_index(),\
    dfb.groupby(['模具项目号']).agg({'完工重量':'sum','完工数量':'sum'}).reset_index(),\
on=['模具项目号'],how='left')
ABC=ABC[ABC.总金额>0]
ABC['单支分摊']=ABC.总金额/ABC.完工数量
ABC['每吨分摊']=ABC.总金额*1000/ABC.完工数量
ABC['模具寿命']=ABC.完工数量/ABC.数量
ABC['分摊类型1']='abc分摊'
ABC['分摊类型2']='有bom'

#2:根据无bom有组别 ，根据模具项目号、组别分摊
y_amt=pd.merge(abc[(abc.模具项目号.isin(dfb.模具项目号)==False)\
       &(abc.组别.isin(dfg.组别.unique()))&(abc.组别!='')].groupby(['模具项目号','组别'])\
      ['总金额'].sum().to_frame().reset_index(),b2,on='组别',how='left')
y_amt=y_amt[y_amt.总金额>0]
y_amt['单支分摊']=y_amt.总金额/y_amt.完工数量
y_amt['每吨分摊']=y_amt.总金额*1000/y_amt.完工重量
y_amt['分摊类型1']='abc分摊'
y_amt['分摊类型2']='无bom有组别'

#a3:对不到产品和组别，但组别不为空的部分
#a4：对不到产品，组别为空的部分
a3=abc[(abc.模具项目号.isin(dfb.模具项目号)==False)\
       &(abc.组别.isin(dfg.组别)==False)&(abc.组别!='')]
a4=abc[(abc.组别=='')&(abc.模具项目号.isin(dfb.模具项目号)==False)]
a34=pd.concat([a3,a4])
#通过报工表，获取工序码对应的组别
a34_z=df4[(df4.工序码!='')&(df4.组别!='')][['工序码','组别']].drop_duplicates(\
    subset=['工序码','组别'],keep='first')
#3，通过工序码能对到组别分摊
h_g=pd.merge(a34[a34.工序码!=''].groupby(['模具项目号','工序码']).agg({
    '总金额':'sum'}).reset_index(),a34_z,on='工序码',how='left')
hgg=h_g.groupby(['模具项目号','工序码','组别']).agg({
    '总金额':'sum'}).reset_index()
hgg=hgg[hgg.总金额>0]
hgg_b=pd.merge(hgg,b2,on='组别',how='left')
hgg_b['模具项目号']=hgg_b.模具项目号.astype(str)
hgg_b['汇总']=hgg_b['模具项目号'].str.cat(hgg_b['工序码'])
hgg_bs=[]
for i in hgg_b.汇总.unique():
    a=hgg_b[hgg_b.汇总==i]
    a['总金额']=a.总金额*a.完工重量/a.完工重量.sum()
    hgg_bs.append(a)
h_amt=pd.concat(hgg_bs).drop(['汇总'],axis=1)
h_amt['单支分摊']=h_amt.总金额/h_amt.完工数量
h_amt['每吨分摊']=h_amt.总金额*1000/h_amt.完工重量
h_amt['分摊类型1']='abc分摊'
h_amt['分摊类型2']='无bom无组别-有组别'

#4,工序号为空，找不到组别，给他组别进行分摊
n_g=a34[a34.工序码==''].groupby(['模具项目号']).agg({'总金额':'sum'}).reset_index()
n_g=n_g[n_g.总金额>0]
n_gs=[]
for i in n_g.模具项目号.unique():
    a=pd.concat([n_g[n_g.模具项目号==i],b5])
    a=a.fillna(method='ffill').reset_index(drop=True).drop(0)
    a['总金额']=a.总金额*a.完工重量/a.完工重量.sum()
    a['单支分摊']=a.总金额/a.完工数量
    a['每吨分摊']=a.总金额*1000/a.完工重量
    n_gs.append(a)
n_amt=pd.concat(n_gs)
n_amt['分摊类型1']='abc分摊'
n_amt['分摊类型2']='无bom无组别-无组别'


# 计算化学品（w12）的分摊
dx2['总金额']=dx2['辅计量']*dx2['单位成本']
dx2['数量']=dx2.辅计量
#按1年的时间分摊。
dx2=dx2[(dx2.日期>=ay1)&(dx2.日期<=ay2)]

w12=dx2[(dx2.组别!='L02')&(dx2.组别!='')].groupby(
    ['日期','模具项目号','组别','物料大类','物料小类','单位成本']).agg({
    '数量':'sum','总金额':'sum'}).reset_index()
def conv(x):
    if x in ['P11','P12', 'P13', 'P14']:
        return 'P01'
    if x=='R02':
        return 'R01'
    else:
        return x
w12['处理后组别']=w12.组别.apply(conv)
dxx=w12.groupby(['模具项目号','处理后组别'])['总金额'].sum().to_frame().reset_index()
hup_amt=pd.merge(dxx,b2x,left_on='处理后组别',right_on='组别',how='left').drop('处理后组别',axis=1)
hup_amt['单支分摊']=hup_amt.总金额/hup_amt.完工数量
hup_amt['每吨分摊']=hup_amt.总金额*1000/hup_amt.完工重量
hup_amt['分摊类型1']='化学品分摊'

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM 物料验收表 \
WHERE 定单日期 >='" + ay1 + "' AND 定单日期 <= '" + ay2 + "' ")
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
d['总金额']=d.验收数量*d.单位成本

# D类中的 油品'LLO',根据报工表（成型1000），按重量分摊
d2=d[d['物料小类']=='LLO']
k1=df4_0[(df4_0.工序码!='')&(df4_0.组别!='')][['工序码','组别']]\
        .drop_duplicates(subset=['工序码','组别'],keep='first')
llo_mx1=pd.merge(d2[d2.工序码_油!=''].drop('组别',axis=1),\
        k1,left_on='工序码_油',right_on='工序码',how='left')
oill_hg=llo_mx1.groupby(['模具项目号','工序码','组别']).agg({
    '总金额':'sum'}).reset_index()
#油品有组别分摊
oill1=pd.merge(oill_hg,b2x,on='组别',how='left')
oill1['汇总']=oill1['模具项目号'].str.cat(oill1['工序码'])
oill1x=[]
for i in oill1.汇总.unique():
    a=oill1[oill1.汇总==i]
    a['总金额']=a.总金额*a.完工重量/a.完工重量.sum()
    oill1x.append(a)
oill1_amt=pd.concat(oill1x).drop(['汇总'],axis=1)
oill1_amt['单支分摊']=oill1_amt.总金额/oill1_amt.完工数量
oill1_amt['每吨分摊']=oill1_amt.总金额*1000/oill1_amt.完工重量
oill1_amt['分摊类型1']='油品分摊'
oill1_amt['分摊类型2']='有组别'

#油品无组别分摊,给他组别
llo_mx2=d2[d2.工序码_油==''].groupby(['模具项目号'])\
        ['总金额'].sum().to_frame().reset_index()
oill2xs=[]
for i in llo_mx2.模具项目号.unique():
    a=pd.concat([llo_mx2[llo_mx2.模具项目号==i],b5])
    a=a.fillna(method='ffill').reset_index(drop=True).drop(0)
    a['总金额']=a.总金额*a.完工重量/a.完工重量.sum()
    a['单支分摊']=a.总金额/a.完工数量
    a['每吨分摊']=a.总金额*1000/a.完工重量
    oill2xs.append(a)
oill2_amt=pd.concat(oill2xs)
oill2_amt['分摊类型1']='油品分摊'
oill2_amt['分摊类型2']='无组别'

#仓储分摊
dc=d[d['组别']=='K01'].groupby(['模具项目号','组别'])['总金额'].sum().to_frame().reset_index()
#仓储出库表
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  仓储出库表 \
WHERE 入库日期 >='" + ay1 + "' AND 入库日期 <= '" + ay2 + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df6x = pd.DataFrame(list_data)
df6x.columns=columns
df6x['入库日期']=pd.to_datetime(df6x['入库日期'],format='%Y-%m-%d')
dc['完工数量']=df6x.数量.sum()
dc['完工重量']=df6x.重量.sum()
dc['单支分摊']=dc.总金额/dc.完工数量
dc['每吨分摊']=dc.总金额*1000/dc.完工重量
dc['分摊类型1']='仓储分摊'
dc['分摊类型2']=np.nan

#d类   (剔除 ['LLO','K01','Z01','L01','L02'])
d1=d[d.组别.isin(['LLO','K01','Z01','L01','L02'])==False]

#d类总金额，按产品报工分摊，对得到组别归到组
d1_yes=pd.merge(d1[d1.组别.isin(df4_0[df4_0.组别!=''].组别.unique())].groupby([\
        '模具项目号','组别'])['总金额'].sum().to_frame().reset_index()\
        ,b2x,on='组别',how='left')
d1_yes['单支分摊']=d1_yes.总金额/d1_yes.完工数量
d1_yes['每吨分摊']=d1_yes.总金额*1000/d1_yes.完工重量
d1_yes['分摊类型1']='D类分摊'
d1_yes['分摊类型2']='D类有组别'

#对不到组别按工序码【'S','N'】分配
b4=df4_0[df4_0.工序码.isin(['S','N'])].groupby(\
    ['工序码']).agg({'完工数量':'sum','完工重量':'sum'}).reset_index()

d_no1=d1[d1.组别.isin(df4_0[df4_0.组别!=''].组别.unique())==False].groupby(\
    ['模具项目号'])['总金额'].sum().reset_index()
d_no1['完工数量']=b4[b4.工序码=='N'].完工数量.values[0]
d_no1['完工重量']=b4[b4.工序码=='N'].完工重量.values[0]
d_no1['工序码']='N'
#60%的金额放在N里分摊
d_no1['总金额']=d_no1.总金额*0.6
d_no1['单支分摊']=d_no1.总金额/d_no1.完工数量
d_no1['每吨分摊']=d_no1.总金额*1000/d_no1.完工重量
d_no1['分摊类型1']='D类分摊'
d_no1['分摊类型2']='D类无组别N'

d_no2=d1[d1.组别.isin(df4_0[df4_0.组别!=''].组别.unique())==False].groupby(\
    ['模具项目号'])['总金额'].sum().reset_index()
d_no2['完工数量']=b4[b4.工序码=='S'].完工数量.values[0]
d_no2['完工重量']=b4[b4.工序码=='S'].完工重量.values[0]
d_no2['工序码']='S'
#40%的金额放在S里分摊
d_no2['总金额']=d_no2.总金额*0.4
d_no2['单支分摊']=d_no2.总金额/d_no2.完工数量
d_no2['每吨分摊']=d_no2.总金额*1000/d_no2.完工重量
d_no2['分摊类型1']='D类分摊'
d_no2['分摊类型2']='D类无组别S'
d_no=pd.concat([d_no1,d_no2])


#包装
import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  包装入库表 \
WHERE 入库日期 >='" + ay1 + "' AND 入库日期 <= '" + ay2 + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df5b = pd.DataFrame(list_data)
df5b.columns=columns
df5b['入库日期']=pd.to_datetime(df5b['入库日期'],format='%Y-%m-%d')

db=df34[(df34['物料大类_'].isin(['PM','PL']))&(df34.组别!='L04')]
db['总金额']=db.单位成本*db.验收数量

table_b=db.groupby(['模具项目号'])['总金额'].sum().to_frame().reset_index()
table_b['完工数量']=df5b.数量.sum()
table_b['完工重量']=df5b.重量.sum()
#包装材料分摊金额
table_b['单支分摊']=table_b.总金额/table_b.完工数量
table_b['每吨分摊']=table_b.总金额*1000/table_b.完工重量
table_b['分摊类型1']='包装材料'
table_b['分摊类型2']=np.nan


#环保分摊(化学品部分) 66%
hb1_mx=pd.concat([dx2[dx2.组别.isna()].where(dx2[dx2.组别.isna()].notnull(),''),dx2[dx2.组别=='L02']])
hb1_mx=hb1_mx.groupby(['模具项目号']).agg({'总金额':'sum'}).reset_index()

hb_std=pd.DataFrame([25,1,21,34,10,9],index=\
        ['P01','P03','P04','R01','G01','XP0'],columns=['比例']).reset_index()
hb_std.columns=['组别','比例']
hb_std=pd.merge(hb_std,b2x,on='组别',how='left')

#66%的分摊函数
def six(data):
    x1=[]
    for i in data.模具项目号.unique():
        a=pd.concat([data[data.模具项目号==i],hb_std])
        a=a.fillna(method='ffill').reset_index(drop=True)
        a['总金额']=a.总金额*0.66*a.比例/100
        a['单支分摊']=a.总金额/a.完工数量
        a['每吨分摊']=a.总金额*1000/a.完工重量
        x1.append(a.drop(0))
    amt=pd.concat(x1).reset_index(drop=True).drop(['比例'],axis=1)
    return  amt

hb1_amt=six(hb1_mx)
hb1_amt['分摊类型1']='环保分摊'
hb1_amt['分摊类型2']='化学品分摊1'

#34%的分摊函数
def three(data):
    x2=[]
    for i in data.模具项目号.unique():
        a=pd.concat([data[data.模具项目号==i],b5])
        a=a.fillna(method='ffill').reset_index(drop=True).drop(0)
        a['总金额']=a.总金额*0.34*a.完工重量/a.完工重量.sum()
        a['单支分摊']=a.总金额/a.完工数量
        a['每吨分摊']=a.总金额*1000/a.完工重量
        x2.append(a)
    amt=pd.concat(x2).reset_index(drop=True)
    return  amt

hb2_amt=three(hb1_mx)
hb2_amt['分摊类型1']='环保分摊'
hb2_amt['分摊类型2']='化学品分摊2'

#环保D类
hb3_mx=d[d.组别.isin(['L01','L02'])].groupby(['模具项目号']).agg({'总金额':'sum'}).reset_index()
#环保D类，金额的0.66进行分摊
hb3_amt=six(hb3_mx)
hb3_amt['分摊类型1']='环保分摊'
hb3_amt['分摊类型2']='环保D类分摊1'
#环保D类，金额的0.34进行分摊
hb4_amt=three(hb3_mx)
hb4_amt['分摊类型1']='环保分摊'
hb4_amt['分摊类型2']='环保D类分摊2'

import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  财务实际成本11 \
WHERE 日期 >='" + ay1 + "' AND 日期 <= '" + ay2 + "'  ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
cw = pd.DataFrame(list_data)
cw.columns=columns

#财报污泥处置 66%的金额 通过组别分摊
hb5_amt=hb_std.copy()
hb5_amt['总金额']=cw[cw.成本类型2=='财报污泥处置'].金额.sum()*hb5_amt.比例*0.66/100
hb5_amt['每吨分摊']=hb5_amt.总金额*1000/hb5_amt.完工重量
hb5_amt['单支分摊']=hb5_amt.总金额/hb5_amt.完工数量
hb5_amt['模具项目号']=0
hb5_amt['分摊类型1']='环保分摊'
hb5_amt['分摊类型2']='污泥处置1'
hb5_amt=hb5_amt.drop(['比例'],axis=1)

#财报污泥处置 34%的金额 通过组别分摊
hb6_amt=b5.copy()
hb6_amt['总金额']=cw[cw.成本类型2=='财报污泥处置'].金额.sum()*0.34*hb6_amt.完工重量/hb6_amt.完工重量.sum()
hb6_amt['单支分摊']=hb6_amt.总金额/hb6_amt.完工数量
hb6_amt['每吨分摊']=hb6_amt.总金额*1000/hb6_amt.完工重量
hb6_amt['模具项目号']=0
hb6_amt['分摊类型1']='环保分摊'
hb6_amt['分摊类型2']='污泥处置2'

#折旧
#P1:归为包装折旧 ,按df5b重量和数量分摊
zj1=pd.DataFrame([df5b.数量.sum(),df5b.重量.sum(),\
cw[cw.成本类型2=='包装折旧'].金额.sum()],index=['数量','重量','金额']).T
zj1.columns=['完工数量','完工重量','总金额']
zj1['模具项目号']=0
zj1['组别']='P1'
zj1['单支分摊']=zj1.总金额/zj1.完工数量
zj1['每吨分摊']=zj1.总金额*1000/zj1.完工重量
zj1['分摊类型1']='折旧分摊'
zj1['分摊类型2']='包装折旧'


import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  生产入库表 \
WHERE 入库日期 >='" + ay1 + "' AND 入库日期 <= '" + ay2 + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df5d = pd.DataFrame(list_data)
df5d.columns=columns

#['10700','D02','C04']和70%【11000】 按df5d重量和数量分
cw_q=cw[cw.成本类型2=='其他折旧']
zj2=pd.DataFrame([df5d.数量.sum(),df5d.重量.sum(),\
    cw_q[cw_q.经营单位.isin(['10700','D02','C04'])].金额.sum()+\
    cw_q[cw_q.经营单位=='11000'].金额.sum()*0.7],index=['数量','重量','金额']).T
zj2.columns=['完工数量','完工重量','总金额']
zj2['模具项目号']=0
zj2['单支分摊']=zj2.总金额/zj2.完工数量
zj2['每吨分摊']=zj2.总金额*1000/zj2.完工重量
zj2['分摊类型1']='折旧分摊'
zj2['分摊类型2']='其他折旧'

import pandas as pd
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  *  FROM  仓储出库表 \
WHERE 入库日期 >='" + ay1 + "' AND 入库日期 <= '" + ay2 + "'  ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
df6 = pd.DataFrame(list_data)
df6.columns=columns

#PP:归为仓储折旧  按仓储df6重量和数量分摊
zj3=pd.DataFrame([df6.数量.sum(),df6.重量.sum(),\
    cw[cw.成本类型2=='仓储折旧'].金额.sum()],index=['数量','重量','金额']).T
zj3.columns=['完工数量','完工重量','总金额']
zj3['模具项目号']=0
zj3['组别']='PP'
zj3['单支分摊']=zj3.总金额/zj3.完工数量
zj3['每吨分摊']=zj3.总金额*1000/zj3.完工重量
zj3['分摊类型1']='折旧分摊'
zj3['分摊类型2']='仓储折旧'

#有组别折旧
zj4=cw[cw.成本类型2=='有组别折旧'].groupby(\
            ['组别'])['金额'].sum().to_frame().reset_index()
zj4.columns=['组别','金额']
zj_amt=pd.merge(zj4,b2x,on='组别',how='left')
zj_amt['模具项目号']=0
zj_amt['单支分摊']=zj_amt.金额/zj_amt.完工数量
zj_amt['每吨分摊']=zj_amt.金额*1000/zj_amt.完工重量
zj_amt.columns=['组别','总金额','完工数量','完工重量','模具项目号','单支分摊','每吨分摊']
zj_amt['分摊类型1']='折旧分摊'
zj_amt['分摊类型2']='有组别折旧'

#其他，分摊给全部产品
other_amt=pd.DataFrame([df5d.数量.sum(),df5d.重量.sum(),\
        cw[cw.成本类型1=='其他'].金额.sum()],index=['数量','重量','金额']).T
other_amt['模具项目号']=0
other_amt['单支分摊']=other_amt.金额/other_amt.数量
other_amt['每吨分摊']=other_amt.金额*1000/other_amt.重量.sum()
other_amt.columns=['完工数量','完工重量','总金额','模具项目号','单支分摊','每吨分摊']
other_amt['分摊类型1']='其他分摊'
other_amt['分摊类型2']=np.nan


# 水电蒸汽用量
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select  TRIM(ZQSEG1) 代码,ZQSOQS/1000000 量 from proddta.F560318 \
where ZQTRDJ>=DTON('" + ay1 + "') AND ZQTRDJ<DTON('" + ay2 + "')")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df9= pd.DataFrame(list_data)
df9.columns=columns

one=df9.groupby(['代码'])['量'].sum().to_frame().reset_index()
one_l={}
for i in one.itertuples():
    one_l[i[1]]=i[2]
#生产一部总电量
oe_total=one_l['DL0102']+one_l['DL0103']+one_l['DL02']+one_l['DL04']+one_l['DL08']\
    -one_l['DL0801']-one_l['DL0802']+one_l['DL0503']+one_l['DL0504']+one_l['DL0505']\
    +one_l['DL0506']+one_l['DL06']+one_l['DL0701']-(one_l['DL0803']+one_l['DL0104'])*0.05
#所有总电量
total_e=one_l['DL01']+one_l['DL02']+one_l['DL03']+one_l['DL04']+one_l['DL05']\
    +one_l['DL06']+one_l['DL07']+one_l['DL08']
#生产一部电量总金额
one_e=cw[cw.成本类型2=='水电'].金额.sum()*oe_total/total_e

#生产一部总蒸汽用量
oq_total=one_l['ZQ01']+one_l['ZQ04']+one_l['ZQ05']-one_l['ZQ07']
#总蒸汽用量
total_q=one_l['ZQZ1']
#生产一部电量总金额
one_q=cw[cw.成本类型2=='蒸汽'].金额.sum()*oq_total/total_q

#水电蒸汽总金额
eq_total=one_e+one_q

eq_std=pd.DataFrame(['包装','B01','B02','B03','B04','D01','C01','C02','C03','T01','A01','A02','A03','A04','P01','R01','P03','P04','G01','PC1'],index=\
        [5,83,197,107,281,1.7,33,50,41,5,10,57,99,100,3.6,3.1,101,37.6,7.8,3]).reset_index()
eq_std.columns=['吨生产用电','组别']

eq_amt=pd.merge(eq_std,b2x,on='组别',how='left')
eq_amt.loc[0,'完工数量']=df5b.数量.sum()
eq_amt.loc[0,'完工重量']=df5b.重量.sum()
eq_amt['模具项目号']=0
eq_amt['标准生产用电']=eq_amt.吨生产用电*eq_amt.完工重量/1000
eq_amt['总金额']=eq_total
eq_amt['每吨分摊']=eq_amt.总金额*eq_amt.标准生产用电*1000/(eq_amt.标准生产用电.sum()*eq_amt.完工重量)
eq_amt['单支分摊']=eq_amt.总金额*eq_amt.标准生产用电/(eq_amt.标准生产用电.sum()*eq_amt.完工数量)
eq_amt['分摊类型1']='水电蒸汽分摊'
eq_amt['分摊类型2']='水电蒸汽'
eq_amt=eq_amt[['组别','模具项目号','完工数量','完工重量','总金额','每吨分摊','单支分摊','分摊类型1','分摊类型2']]


#薪资
import cx_Oracle
import pandas as pd
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','172.16.4.14:1521/TMJDEDB')
cursor = ora.cursor()
cursor.execute(" select TRIM(a.MEITM) 短项目号,TRIM(MEDSC1) 机型,TRIM(MEDL02) 工序码\
,a.MESOQS/1000000 千支生产时间h from proddta.F564724A  a  ")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
df10= pd.DataFrame(list_data)
df10.columns=columns
df10['汇总']=df10['短项目号'].str.cat(df10['工序码'])
df10['短项目号']=df10['短项目号'].astype(int)

st=pd.merge(df4_0.groupby(['短项目号', '机型', '工序码','组别']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index(),df10,on=['短项目号', '机型', '工序码'],how='left')
st['标准生产工时']=st.完工数量*st.千支生产时间H/1000
def change(x):
    if x=='E01' or x=='E02' or x=='P03':
        return 'YT1'
    if x=='QC-100001':
        return 'Z03'
    if x=='T02'or x=='Y01' or x=='TG1':
        return 'T01'
    else:
        return x
st['组别新']=st.组别.apply(change)

#有标准生产工时的部分
s0=st[st.标准生产工时.notna()]

#针对没有匹配到标准生产工时的数据，根据短项目号和工序码再进行一次匹配
s1=st[st.标准生产工时.isna()][['短项目号','机型','工序码','组别','完工数量', '完工重量',
       '组别新']]
s1['短项目号']=s1['短项目号'].astype(str)
s1['汇总']=s1['短项目号'].str.cat(s1['工序码'])

ran=[]
for i in df10.汇总.unique():
    a=df10[df10.汇总==i][:1]
    ran.append(a)
rand=pd.concat(ran)

s11=pd.merge(s1,rand[['汇总','千支生产时间H']],on='汇总',how='left').drop(['汇总'],axis=1)

s12=s11[s11.千支生产时间H.notna()]
s12=s12.drop(s12[s12.组别新=='R01'].index)
s12['标准生产工时']=s12.完工数量*s12.千支生产时间H/1000

#最终有标准工时
s_have=pd.concat([s0,s12])
#最终匹配不到标准生产工时的部分
s_no=s11[(s11.千支生产时间H.isna())&(s11.组别新.isin(s_have.组别新.unique())==False)]

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
where a.XZTRDJ>=DTON('" + ay1 + "') and a.XZTRDJ<DTON('" + ay2 + "')  ")
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
dfs1=dfs[(dfs.部门.isin(['生产一部','品保课','环境安全课']))&\
        (dfs.科.isin(['品保二课','仓储课'])==False)]

hg=s_have.groupby(['工序码','组别新']).agg({
    '标准生产工时':'sum'}).reset_index()
ratio=hg[hg.组别新=='T01']
ratio['比例']=ratio.标准生产工时/ratio.标准生产工时.sum()

dfsx1=pd.merge(dfs1[dfs1.组别新.isin(hg.组别新)],hg,on='组别新',how='left')
dfsx11=dfsx1[dfsx1.组别新=='T01']
dfsx12=pd.merge(dfsx11,ratio,on=['工序码','组别新','标准生产工时'],how='left')
for i in['薪资', '实际出勤时长', '平日加班时数', '假日加班时数',
       '法定日加班时数', '人数', '养老险', '医疗险', '失业险', '工商险', '公积金', '奖金', '总薪资', '总时长']:
    dfsx12[i]=dfsx12[i]*dfsx12.比例
smx1=pd.concat([dfsx12,dfsx1[dfsx1.组别新!='T01']]).drop('标准生产工时',axis=1)

sh=pd.merge(smx1.groupby(['工序码'])['总薪资'].sum().to_frame().reset_index(),\
    hg.groupby(['工序码'])['标准生产工时'].sum().to_frame().reset_index(),on='工序码',how='left')
sh['标准小时薪资']=sh.总薪资/sh.标准生产工时

sh10=pd.merge(df10,sh,on='工序码',how='left').drop(['汇总'],axis=1)
sh10['单支分摊']=sh10.标准小时薪资*sh10.千支生产时间H/1000
s1=pd.merge(sh10[['短项目号','机型','工序码','总薪资','标准小时薪资','单支分摊']],\
    df4_0.groupby(['短项目号','机型']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index(),on=['短项目号','机型'],how='left')
s1.columns=['短项目号','机型','工序码','总金额','标准小时薪资','单支分摊','完工数量','完工重量']
s1['分摊类型1']='薪资分摊'
s1['分摊类型2']='有标准工时'

n_hg0=s_no[s_no.组别新!=''].groupby(['组别新']).agg({
    '完工数量':'sum','完工重量':'sum'}).reset_index()
n_hg1=n_hg0[n_hg0.组别新.isin(['YT1','P04'])==False]
n_hg2=n_hg0[n_hg0.组别新=='P04']
n_hg2['完工数量']=n_hg2.完工数量+dw10.完工数量.values[0]
n_hg2['完工重量']=n_hg2.完工重量+dw10.完工重量.values[0]
#上述YT1的完工重量和数量，用E02的完工数量和重量替代
e0=s_no[s_no.组别新!=''].groupby(['组别']).agg({
    '完工重量':'sum','完工数量':'sum'}).reset_index()
n_hg3=e0[e0.组别=='E02']
n_hg3.loc[1,'组别']='YT1'
n_hg3.columns=['组别新','完工重量','完工数量']
n_hg=pd.concat([n_hg1,n_hg2,n_hg3])

smx2=dfs1[dfs1.组别新.isin(n_hg.组别新)]

no_hgs=pd.merge(n_hg,smx2.groupby(['组别新'])['总薪资'].sum().\
                to_frame().reset_index(),on='组别新',how='left')
s2=no_hgs[no_hgs.总薪资.notna()]
s2['单支分摊']=s2.总薪资/s2.完工数量
s2['每吨分摊']=s2.总薪资*1000/s2.完工重量
s2.columns=['组别新','完工数量','完工重量','总金额','单支分摊','每吨分摊']
s2['分摊类型1']='薪资分摊'
s2['分摊类型2']='有组别'

smx3=dfs1[dfs1.组别新.isin(list(set(hg.组别新) | set(n_hg.组别新)))==False]

s3=pd.DataFrame([df5d.数量.sum(),df5d.重量.sum(),\
    smx3.总薪资.sum()],index=['数量','重量','金额']).T
s3['单支分摊']=s3.金额/s3.数量
s3['每吨分摊']=s3.金额*1000/s3.重量.sum()
s3.columns=['完工数量','完工重量','总金额','单支分摊','每吨分摊']
s3['分摊类型1']='薪资分摊'
s3['分摊类型2']='整体分摊'

smx4=dfs[(dfs.科=='仓储课')&(dfs.组别名=='一期仓管组')]

s4=pd.DataFrame([df6x.数量.sum(),df6x.重量.sum(),\
  smx4.总薪资.sum()],index=['数量','重量','总金额']).T
s4['单支分摊']=s4.总金额/s4.数量
s4['每吨分摊']=s4.总金额*1000/s4.重量.sum()
s4.columns=['完工数量','完工重量','总金额','单支分摊','每吨分摊']
s4['处理后组别']='仓储薪资'
s4['分摊类型1']='薪资分摊'
s4['分摊类型2']='仓储薪资'

smx5=dfs[(dfs.科=='仓储课')&(dfs.组别名!='一期仓管组')]

s5=pd.DataFrame([df5b.数量.sum(),df5b.重量.sum(),\
  smx5.总薪资.sum()],index=['数量','重量','金额']).T
s5['单支分摊']=s5.金额/s5.数量
s5['每吨分摊']=s5.金额*1000/s5.重量.sum()
s5.columns=['完工数量','完工重量','总金额','单支分摊','每吨分摊']
s5['处理后组别']='包装薪资'
s5['分摊类型1']='薪资分摊'
s5['分摊类型2']='包装薪资'

smx6=dfs[dfs.组别新=='F02']

s6=b2x[b2x.组别=='XP0']
s6['总金额']=smx6.总薪资.sum()
s6['单支分摊']=s6.总金额/s6.完工数量
s6['每吨分摊']=s6.总金额*1000/s6.完工重量.sum()
s6['分摊类型1']='薪资分摊'
s6['分摊类型2']='废料薪资'

amt=pd.concat([ABC,y_amt,h_amt,n_amt,hup_amt,oill1_amt,oill2_amt,dc,d1_yes,d_no,\
 table_b,hb1_amt,hb2_amt,hb3_amt,hb4_amt,hb5_amt,hb6_amt,zj1,zj2,zj3,zj_amt,\
other_amt,eq_amt,s1,s2,s3,s4,s5,s6])
amt['单支分摊']=amt['单支分摊'].apply(lambda x:0 if x==float('inf') else x)

for i in ['模具项目号','数量','完工重量','完工数量','单支分摊','每吨分摊','模具寿命',
      '短项目号','标准小时薪资']:
    amt[i]=amt[i].fillna(0)
for i in ['分摊类型2','组别','工序码','机型','组别新','处理后组别']:
    amt[i]=amt[i].fillna('')
for i in ['模具项目号','短项目号']:
    amt[i]=amt[i].astype(int)

#一年算一次，计算的是上一年的分摊数据
t=nt-1
amt['分摊计算时间']=t

import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  distinct 分摊计算时间  FROM 分摊表   ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
ft = pd.DataFrame(list_data)
ft.columns=columns

ap=amt[amt.分摊计算时间.isin(ft.分摊计算时间)==False]

#存入数据
from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
engine = create_engine('postgresql+psycopg2://'+'chengben'+':\
'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
#engine.connect().execute(" DROP TABLE 分摊表 ")
ap.to_sql('分摊表', engine, if_exists='append', index=False,
          dtype={'分摊计算时间': sqlalchemy.types.INT(),
                 '模具项目号': sqlalchemy.types.INT(),
                 '短项目号': sqlalchemy.types.INT(),
                 '工序码':sqlalchemy.types.String(length=20),
                 '机型': sqlalchemy.types.String(length=20),
                 '组别':sqlalchemy.types.String(length=20),
                 '组别新':sqlalchemy.types.String(length=10),
                 '处理后组别':sqlalchemy.types.String(length=10),
                 '数量':sqlalchemy.types.FLOAT(),
                 '完工数量': sqlalchemy.types.FLOAT(),
                 '完工数量': sqlalchemy.types.FLOAT(),
                 '标准小时薪资': sqlalchemy.types.FLOAT(),
                 '模具寿命': sqlalchemy.types.FLOAT(),
                 '单支分摊': sqlalchemy.types.FLOAT(),
                 '每吨分摊': sqlalchemy.types.FLOAT(),
                 '分摊类型1':sqlalchemy.types.String(length=30),
                 '分摊类型2': sqlalchemy.types.String(length=30),})
#engine.connect().execute(" ALTER TABLE 分摊表 ADD PRIMARY KEY (模具项目号,单支分摊,每吨分摊,模具寿命,短项目号,分摊类型2,组别,工序码,机型,处理后组别); ")

print('完成')