import urllib.request, urllib.error
import csv
import sys
import datetime
import re
import pymssql
import spjsm as jsm
import idx

from bs4 import BeautifulSoup
from timeout_decorator import timeout, TimeoutError

# Format html 
def format1(msg):
	msg=msg.replace('\\n','')
	msg=msg.replace('\\','')
	msg=msg.replace('&lt;','<')
	msg=msg.replace('&gt;','>')
	return msg
	
# Format float:
def formatFloat(val):
	val=val.replace(',','')
	val=val.replace('---','-1')
	val=val.replace('--','-1')
	val=val.replace('-','-1')
	if val=='':
		return -1
	else:
		return float(val)

# Format int:
def formatInt(val):
	val=val.replace('/','')
	val=val.replace(',','')
	val=val.replace('---','-1')
	val=val.replace('--','-1')
	val=val.replace('-','-1')
	if val=='':
		return -1
	else:
		return int(val)		

# Insert data
def insertSP(STCode,Day,Starts,Highs,Lows,Ends,AdjEnds,Yield,cnn,cur):
	cur.execute("""INSERT INTO SP_Daily(STCode, 
	Day,
	OpenV,
	HighV,
	LowV,
	CloseV,
	AdjCloseV,
	Volumn)VALUES (%s, %s,%s,%s,%s,%s,%s,%s)""" , (STCode,Day,Starts,Highs,Lows,Ends,AdjEnds,Yield))
	cnn.commit()
	
# Update data
def updateSP(STCode,Day,PSR,PER,PBR,DY,cnn,cur):
	cur.execute("""UPDATE SP_Daily SET PSR=%s,
	PER=%s,
	PBR=%s,
	DY=%s WHERE STCode=%s AND Day=%s""" , (PSR,PER,PBR,DY,STCode,Day))
	cnn.commit()
	
# Update FinishDate
def updateFinishDate(STCode,CreateDate,cnn,cur):
	cur.callproc('UpdateStock', (STCode,CreateDate))
	cnn.commit()
	
# Delete Unfinish Data
def deleteUnfinishData(STCode,Day,cnn,cur):
	cur.callproc('DeleteSP_Dailiy', (STCode,Day))
	cnn.commit()
	
# Insert Log
def insertLog(createdBy,infoLevel,info,cnn,cur):
	cur.callproc('InsertInfoLog', (datetime.datetime.today(),createdBy,infoLevel,info))
	cnn.commit()
# Calculate Date Sequence
def CalDateSeq(cnn,cur):
	cur.callproc('CalDateSeq')
	cnn.commit()
# Get Soup
def getSoupString(urlstr):
	html = urllib.request.urlopen(urlstr,timeout=100)
	soup = BeautifulSoup(html, "html.parser")
	match = re.search(r'[(](.+)[)].hide()', str(soup))
	return match

# Get Data Exist
def getExist(STCode,Day,cur):
	cur.execute("""SELECT count(STCode) FROM SP_Daily WHERE STCode=%d AND [Day]=%d""",(STCode,Day))
	result=cur.fetchone()
	return result[0]

# Get DailySP 	
def getDailySP():
	now = datetime.datetime.now()
	nowstr='{0:%Y%m%d}'.format(now)

	# CSV file
	#filename='d:/' + nowstr + '.csv'
	#f = open(filename, 'w')
	#writer = csv.writer(f, lineterminator='\n')

	# Database
	cnn = pymssql.connect(host="127.0.0.1", user="sa", password="Admin123", database="TS")
	cur = cnn.cursor()
	print('[[-----Connection OK------]]')
	cur.execute("""SELECT STCode FROM Stock ORDER BY STCode """,(int(nowstr)))
	rcds = cur.fetchall()

	for item in rcds:
		print('[[-----------Starting Download ['+str(item[0])+']----------]]')
		i=1
		while 1==1:
			urlstr="https://minkabu.jp/stock/"+str(item[0])+"/daily_bar.js?page="+str(i)+"&_=1516462945228"
			urlstr2="https://minkabu.jp/stock/"+str(item[0])+"/daily_valuation.js?page="+str(i)+"&_=1516462945228"
			#print('[Page [' + str(i) + ']]')
			try:
				#Create Main Info===============Begin===============
				match = getSoupString(urlstr)
				if match: 
					htmtxt=match.group(1)
					if len(htmtxt)==2:
						print('[[---------- ['+str(item[0])+'] Downloading Over-----------]]')
						updateFinishDate(int(item[0]),int(nowstr),cnn,cur)
						break 
				else:
					print('[[---------- ['+str(item[0])+'] Downloading Over-----------]]')
					updateFinishDate(int(item[0]),int(nowstr),cnn,cur)
					break

				htmtxt=format1(htmtxt)
				soup = BeautifulSoup(htmtxt,"lxml")		    
				rows = soup.findAll("tr")
				
				for row in rows:
					STCode=int(item[0])
					csvlist=[]
					for cell in row.findAll(['td', 'th']):
						csvlist.append(cell.get_text())
					#print('[[Debug]]'+csvlist[0])
					
					if getExist(STCode,int(csvlist[0].replace('/','')),cur)!=0:
						i=9999
						break
					try:
						
						insertSP(STCode,formatInt(csvlist[0]),formatFloat(csvlist[1]),formatFloat(csvlist[2]),formatFloat(csvlist[3]),formatFloat(csvlist[4]),formatFloat(csvlist[5]),formatInt(csvlist[6]),cnn,cur)
					except:
						insertLog('sp.getDailySP()',3,'Unexpected error:'+str(sys.exc_info()[0])+' STCode:'+str(item[0])+' Page:' +str(i) +' Date:'+formatInt(csvlist[0]),cnn,cur)
						print("Unexpected error:", sys.exc_info()[0])
				#Create Main Info===============Over===============
				
				
				#Update Sub Info===============Begin-===============
				match = getSoupString(urlstr2)
				htmtxt=match.group(1)
				htmtxt=format1(htmtxt)
				soup = BeautifulSoup(htmtxt,"lxml")		    
				rows = soup.findAll("tr")
				for row in rows:
					csvlist=[]
					for cell in row.findAll(['td', 'th']):
						csvlist.append(cell.get_text())
					#print(csvlist)
					updateSP(STCode,formatInt(csvlist[0]),formatFloat(csvlist[1]),formatFloat(csvlist[2]),formatFloat(csvlist[3]),formatFloat(csvlist[4]),cnn,cur)
				#exit()
				#Update Sub Info===============Over===============
				
					#writer.writerow(csvlist)	
				i=i+1
			except:
				insertLog('sp.getDailySP()',3,'Unexpected error:'+str(sys.exc_info()[0])+' STCode:'+str(item[0])+' Page:' +str(i),cnn,cur)
				print("Unexpected error:", sys.exc_info()[0])
	CalDateSeq(cnn,cur)
	cnn.close()
	#f.close()

#■■■■■■■■■■■■■■■■■■
getDailySP()
idx.idxparser().getDailyIndex()
jsm.spjsmFinanceParser().getspjsm()






#■■■■■■■■■■■■■■■■■■




