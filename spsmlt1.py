import pymssql as pm
import math
import time



iniAmt=1000000
start = time.time()
cnn = pm.connect(host="127.0.0.1", user="sa", password="Admin123", database="TS")
cur = cnn.cursor()
cur.execute("""TRUNCATE TABLE SP_Smlt""")
cnn.commit()
cur.execute("""SELECT DISTINCT [day] FROM SP_Daily where [day]>20180101  order by [day] """)
rcds = cur.fetchall()

i=1
askd=[]
bidd=[]
for d in rcds:
    if i==1:
        bidd.append(d)
        i=0
    else:
        askd.append(d)
        i=1

dtsql = """SELECT top 3 STCode FROM [SPChange]  WHERE [day]= %d and AdjCloseV>100 and AdjCloseV<2000  order by changeRate desc"""
buysql = """SELECT OpenV  FROM [SP_Daily]  WHERE [day]= %d AND stcode=%d"""
logsql = """INSERT INTO SP_Smlt VALUES (%d,%d,%d,%d,%d,%d,%d,%d,%d)"""
salesql =  """SELECT OpenV  FROM [SP_Daily]  WHERE [day]= %d AND stcode=%d"""
lstsql = """SELECT MAX([DAY]) FROM SP_Smlt"""
amount=iniAmt
lstbuyprice=[] #买入价
lstqty=[] #数量
lstsalePrice=[] #卖出价
lstst=[] #买入股
lstsc=[] #选股
lstcnt=[] #Seq
lstsaledst=[]
lstsaledqty=[]
lstsaledcnt=[]
cnt=1

for d in rcds:
    print(d)
    if len(lstst)!=0:
        lstsaledst = []
        lstsaledqty = []
        lstsaledcnt = []
        for st,qtys ,cnts in zip(lstst,lstqty,lstcnt):
            cur.execute(salesql,(d,st))
            saleprice = cur.fetchall()

            if cur.rowcount >0:
                amount=amount + saleprice[0][0] * qtys
                cur.execute(logsql, (1, d, st,cnts, -1, saleprice[0][0], saleprice[0][0] * qtys, qtys, amount))
                cnn.commit()
                lstsaledst.append(st)
                lstsaledqty.append(qtys)
                lstsaledcnt.append(cnts)
            print(lstst)
        for i,j,k in zip(lstsaledst,lstsaledqty,lstsaledcnt):
            lstst.remove(i)
            lstqty.remove(j)
            lstcnt.remove(k)

    if len(lstsc) != 0:
        peramount=math.floor(amount / len(lstsc))
        for st2 in lstsc:
            cur.execute(buysql, (d, st2))
            buyprice = cur.fetchall()
            if cur.rowcount >0:
                #print(buyprice[0][0])
                qty = math.floor(peramount / float(buyprice[0][0]))
                amount = amount - qty * buyprice[0][0]
                cur.execute(logsql, (1, d, st2,cnt, buyprice[0][0], -1, buyprice[0][0] * qty, qty, amount))
                cnn.commit()
                lstst.append(st2)
                lstqty.append(qty)
                lstcnt.append(cnt)
                cnt=cnt+1

    cur.execute(dtsql, d) #跌幅前三
    rs = cur.fetchall() #株コード
    lstsc = []
    for r in rs:
        lstsc.append(r[0])

elapsed_time = time.time() - start
print ("Elapsed_time:{0}".format(elapsed_time) + "[sec]")