import datetime
import numpy as np
import pandas as pd
nt=datetime.datetime.now()
FD=nt.strftime('%Y-%m')
zt= nt - datetime.timedelta(days=25)
ZT = zt.strftime('%Y-%m')
#日期<f ：当前时间年月的第一天
f=str(FD)+'-1'
LD= nt-datetime.timedelta(days=725)
LT=LD.strftime('%Y-%m')
#日期>=l :当前时间的2年前年月的第一天
l=str(LT)+'-1'

print('开始。。。。。。。。。。。。。。。。。')

#找到最近2年的原数据
import psycopg2
connection = psycopg2.connect(database="chengben", user="chengben", password="np69gk48fo5kd73h", host="192.168.2.156", port="5432")
cur=connection.cursor()
cur.execute("SELECT  日期20,产品规格汇总20,原料项目号,完工重量20  FROM  原料比例原表 \
            WHERE 日期20 >='" + l + "' AND 日期20 < '" + f + "' ")
list_data=[]
columns=[]
for c in cur.description:
    columns.append(c[0])
for row in cur.fetchall():
    list_data.append(row)
connection.commit()
cur.close()
connection.close()
table_x = pd.DataFrame(list_data)
table_x.columns=columns
table_x['日期20']=pd.to_datetime(table_x['日期20'],format='%Y-%m-%d')
table_x['日期']=table_x['日期20'].dt.strftime('%Y-%m')
table_x=table_x.sort_values(by=['日期','日期20','产品规格汇总20','原料项目号','完工重量20']).reset_index(drop=True)

#定义一个函数，把数据放进去，可以计算每个月的原料占比，并拼成一个数据
def collect(data):
    #汇总产品2年时间的原料占比
    t1=data.groupby(['产品规格汇总20','原料项目号'])['完工重量20'].sum().to_frame()
    t1=t1.reset_index()
    use=[]
    for i in t1.产品规格汇总20.unique():
        a=t1[t1['产品规格汇总20']==i]
        a['原料占比']=a.完工重量20*100/a.完工重量20.sum()
        use.append(a)
    t2=pd.concat(use)
    t2=t2.round(decimals=2)
    t2=t2[['产品规格汇总20','原料项目号','原料占比']]
    #汇总产品每个月的原料占比
    n1=[]
    for i in data['日期'].unique():
        l='原料占比'+str(i)
        tt=data[data['日期']==i]
        ttt=tt.groupby(['产品规格汇总20','原料项目号'])['完工重量20'].sum().to_frame()
        ttt=ttt.reset_index()
        #每个月中每个产品比例计算
        n2=[]
        for i in ttt.产品规格汇总20.unique():
            a=ttt[ttt['产品规格汇总20']==i]
            a['原料占比']=a.完工重量20*100/a.完工重量20.sum()
            n2.append(a)
        t4=pd.concat(n2)
        t4=t4.round(decimals=2)
        t4=t4[['产品规格汇总20','原料项目号','原料占比']]
        t4.columns=['产品规格汇总20','原料项目号',l]
        n1.append(t4)
    for i in range(len(n1)):
        # 把每月的原料占比，跟直接汇总的，进行拼接
        t2=t2.merge(n1[i],on=['产品规格汇总20','原料项目号'],how='left')
    # 把出现0的值替换成NAN
    for i in t2.columns[2:]:
        t2[i]=t2[i].apply(lambda x: np.NaN if x==0 else x)
    t2=t2.dropna(axis=0,thresh=3)
    #有的没法替换成nan，或者其他情况，再次删除。
    t2=t2[t2.原料占比!=0]
    t2=t2[t2.原料占比.isna()==False].reset_index(drop=True)
    return t2
x1=collect(table_x)

#提取6个月及以内的原table_x
lis=[]
for i in x1.产品规格汇总20.unique():
    a=x1[x1['产品规格汇总20']==i]
    a=a.dropna(axis=1, how='all')
    if len(a.columns)>9:
        #数据大于6个月，选取最近6个月的数据，记录最近第六个月是哪个时间。
        date=a.columns[-6][4:]
        #从原数据中，只提取最近6个月的数据，重新计算【原料占比】
        new=table_x[(table_x['产品规格汇总20']==i)&(table_x['日期20']>=date)]
        lis.append(new)
data6=pd.concat(lis)
#2年都不够6月的数据，取2年（原数据）进行计算
table_other6=table_x[table_x['产品规格汇总20'].isin(data6.产品规格汇总20.unique())==False]
table_new=pd.concat([table_other6,data6])

# 所有产品数据在6个月及以内
x6=collect(table_new)

#原料顺序表
import cx_Oracle
import pandas as pd
# 账号，密码，ip:端口号/数据库
ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
#执行SQL语句
cursor.execute("select distinct IXKITL 父项目号 ,IXLITM 原料项目号 \
               from proddta.F3002 where IXMMCU in('          P1')and IXTBM='M1'")
list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()
data1 = pd.DataFrame(list_data)
data1.columns=columns
data1['父项目号']=data1['父项目号'].str.replace(" ","")
data1['原料项目号']=data1['原料项目号'].str.replace(" ","")
data1['标准']=data1.原料项目号.apply(lambda x: x[:3]+x[-2:])


ora = cx_Oracle.connect('TONGTJ',' TONGTJ','192.168.3.220:1521/TMJDEDB')
cursor = ora.cursor()
#执行SQL语句
cursor.execute('select IMUKID as 顺序号,IMSEG1 分段161,\
IMSEG4 分段461,IMSEG5 分段561 from proddta.f560514')

list_data=[]
columns=[]
for c in cursor.description:
    columns.append(c[0])
for row in cursor.fetchall():
    list_data.append(row)
cursor.close()
ora.close()

df= pd.DataFrame(list_data)
df.columns=columns
df['标准']=df['分段161'].str.cat(df['分段461']).str.cat(df['分段561'])
df['标准']=df['标准'].str.replace(" ","")
df=df[['顺序号','标准']]
df.sort_values(by='顺序号')

table=pd.merge(data1,df,on='标准',how='left')
table=table[['原料项目号','顺序号']]
table=table[table.顺序号.notna()]
table=table.drop_duplicates(subset=['原料项目号','顺序号'],keep='first')

df=table_x[['产品规格汇总20','原料项目号']].drop_duplicates(subset=['原料项目号','产品规格汇总20'],keep='first')

tx=pd.merge(df,table,on='原料项目号',how='left')

n=[]
for name in tx['产品规格汇总20'].unique():
    a=tx[tx['产品规格汇总20']==name].sort_values(by='顺序号').reset_index(drop=True).fillna(99)
    if a.shape[0]==1 and a['顺序号'].values==99:
        a['用料顺序']=99
        n.append(a)
    if a.shape[0]==1 and a['顺序号'].values!=99:
        a['用料顺序']=1
        n.append(a)
    if a.shape[0]>1:
        for i in range(a.shape[0]-1):
            if i==0 and a.loc[i,'顺序号']!=99:
                a.loc[i,'用料顺序']=1
            if i==0 and a.loc[i,'顺序号']==99:
                a.loc[i,'用料顺序']=99
            if a.loc[i+1,'顺序号']==a.loc[i,'顺序号']:
                a.loc[i+1,'用料顺序']=a.loc[i,'用料顺序']
            if a.loc[i+1,'顺序号']!=a.loc[i,'顺序号'] and a.loc[i+1,'顺序号']!=99:
                a.loc[i+1,'用料顺序']=a.loc[i,'用料顺序']+1
            if a.loc[i+1,'顺序号']!=a.loc[i,'顺序号'] and a.loc[i+1,'顺序号']==99:
                a.loc[i+1,'用料顺序']=99
        n.append(a)
table_xx=pd.concat(n)
order=table_xx[['产品规格汇总20','原料项目号','用料顺序']].reset_index(drop=True)
order['用料顺序']=order['用料顺序'].astype(int)

def yun(data):
    p_name85=[]
    f_name85=[]
    for i in data.index:
        if data.loc[i,'原料占比']>=85:
            p_name85.append(data.loc[i,'产品规格汇总20'])
            f_name85.append(data.loc[i,'原料项目号'])
    list85=pd.DataFrame(f_name85,p_name85).reset_index()
    list85.columns=['产品规格汇总20','原料项目号']
    list85['比例']=100 #该产品，某原料占比大于85，则取100%用该原料作为参考
    tx=data[data['产品规格汇总20'].isin(list85.产品规格汇总20.unique())==False]
    p_name5=[]
    f_name5=[]
    for i in tx.index:
        if tx.loc[i,'原料占比']<=5: # 5是占比的25分位数
            p_name5.append(tx.loc[i,'产品规格汇总20'])
            f_name5.append(tx.loc[i,'原料项目号'])
    list5=pd.DataFrame(f_name5,p_name5).reset_index()
    list5.columns=['产品规格汇总20','原料项目号']
    list5['标记']=0 #标记为0，是若某原料占低于5%，则删除权重，按比例分配到其他原料里。
    # 对不存在单一原料大于95%的产品，进行如上处理
    t_other=table_new[table_new['产品规格汇总20'].isin(list85.产品规格汇总20.unique())==False]
    t_group=t_other.groupby(['产品规格汇总20','原料项目号'])['完工重量20'].sum().to_frame()
    t_group=t_group.reset_index()
    process1=t_group[t_group['产品规格汇总20'].isin(list5.产品规格汇总20.unique())]
    process2=pd.merge(process1,list5,on=['产品规格汇总20','原料项目号'],how='left')
    process=process2[process2.标记.isna()]
    process=process[['产品规格汇总20', '原料项目号', '完工重量20']]
    no_process=t_group[t_group['产品规格汇总20'].isin(list5.产品规格汇总20.unique())==False]
    other_table=pd.concat([process,no_process])
    use=[]
    for i in other_table.产品规格汇总20.unique():
        a=other_table[other_table['产品规格汇总20']==i]
        a['比例']=a.完工重量20*100/a.完工重量20.sum()
        use.append(a)
    list_other=pd.concat(use)
    list_other=list_other.round(decimals=2)
    list_other=list_other[['产品规格汇总20','原料项目号','比例']]
    list_other=list_other[list_other.比例!=0]
    list_other=list_other[list_other.比例.isna()==False].reset_index(drop=True)
    need_table=pd.concat([list85,list_other])
    nt=pd.merge(need_table,order,on=['产品规格汇总20','原料项目号'],how='left')
    return nt

nt6=yun(x6)
nt6['计算日期']=ZT
nt6['计算日期']=pd.to_datetime(nt6.计算日期,format='%Y-%m')

from sqlalchemy import create_engine
import sqlalchemy
import psycopg2
#存入数据
engine = create_engine('postgresql+psycopg2://'+'chengben'+':'+'np69gk48fo5kd73h'+'@192.168.2.156'+':'+str(5432) + '/' + 'chengben')
nt6.to_sql('原料比例', engine, if_exists='append', index=False,
          dtype={'产品规格汇总20': sqlalchemy.types.String(length=50),
                 '原料项目号': sqlalchemy.types.String(length=50),
                 '比例': sqlalchemy.types.FLOAT(),
                 '用料顺序': sqlalchemy.types.INT(),
                 '计算日期': sqlalchemy.types.DATE()})
#engine.connect().execute(" CREATE INDEX 原料比例索引 ON 原料比例(用料顺序); ")
#engine.connect().execute(" ALTER TABLE 原料比例 ADD PRIMARY KEY (计算日期,产品规格汇总20,原料项目号); ")
print('结束。。。。。。。。。。。。。。。。。')
