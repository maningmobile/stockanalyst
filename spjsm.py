import jsm as j
import pandas as pd
import pymssql as pm
import datetime
import dbutil

class spjsmFinanceParser(object):

    def getspjsm(self):
        cnn = pm.connect(host="127.0.0.1", user="sa", password="Admin123", database="TS")
        cur = cnn.cursor()
        cur.execute("""SELECT BCCode FROM M_Business_Category_YH  """)
        rcds = cur.fetchall()
        cur.execute("""SELECT max([day]) FROM SP_Daily  """)
        curmaxday= cur.fetchall()
        q=j.Quotes()
        now = datetime.datetime.now()
        nowstr = '{0:%Y%m%d}'.format(now)

        curday=curmaxday[0][0]
        for cd in rcds:
            d = q.get_brand(cd[0])
            ccodes = [data.ccode for data in d]
            markets= [data.market for data in d]
            names = [data.name for data in d]
            infos = [data.info for data in d]
            data = [ccodes, markets, names, infos]

            for (ccode,market, name,info) in zip(*data):
                arg2=(ccode, int(nowstr), market, name, info,cd[0])
                cur.callproc('UpdateStockInfo',arg2)
                cnn.commit()


                a = q.get_finance(ccode)
                market_cap = a.market_cap
                shares_issued = a.shares_issued
                dividend_yield = a.dividend_yield
                dividend_one = a.dividend_one
                per = a.per
                pbr = a.pbr
                eps = a.eps
                bps = a.bps
                price_min = a.price_min
                round_lot = a.round_lot
                years_high = a.years_high
                years_low = a.years_low


                arg2=(ccode,int(curday),market_cap,shares_issued,dividend_yield,dividend_one,per,pbr,eps,bps,price_min,round_lot,years_high,years_low)
                print('[[' + ccode + ' ]]')
                cur.callproc('UpdateStockFinance', arg2)
                cnn.commit()
                print('[[' + ccode + ' Update Over]]')
        cnn.close()

