import urllib.request, urllib.error
import csv
import sys
import datetime
import re
import pymssql
from urllib.request import urlopen
from bs4 import BeautifulSoup
from timeout_decorator import timeout, TimeoutError

# Format html
class idxparser(object):

	def format1(self,msg):
		msg=msg.replace('\\n','')
		msg=msg.replace('\\','')
		msg=msg.replace('&lt;','<')
		msg=msg.replace('&gt;','>')
		return msg

	# Format float:
	def formatFloat(self,val):
		val=val.replace(',','')
		val=val.replace('---','-1')
		val=val.replace('--','-1')
		val=val.replace('-','-1')
		val=val.replace('－','-1')
		if val=='':
			return -1
		else:
			return float(val)

	# Format int:
	def formatInt(self,val):
		val=val.replace('/','')
		val=val.replace(',','')
		val=val.replace('---','-1')
		val=val.replace('--','-1')
		val=val.replace('-','-1')
		val=val.replace('－','-1')
		if val=='':
			return -1
		else:
			return int(val)

	# Format Day:
	def formatDay(self,val):
		val=val.replace('/','')
		val=val.replace(',','')
		val=val.replace('---','-1')
		val=val.replace('--','-1')
		val=val.replace('-','-1')
		val=val.replace('－','-1')
		if val=='':
			return -1
		else:
			return int('20'+val)

	# Get Table
	def getTable(self,Code):
		if Code=='1697':
			return 'IDX_NYD_Daily'
		if Code=='0951':
			return 'IDX_FX_UY_Daily'
		elif Code=='0950':
			return'IDX_FX_DY_Daily'
		elif Code=='0000':
			return 'IDX_NK_Daily'
		elif Code=='0010':
			return 'IDX_TOPIX_Daily'
		elif Code=='0100':
			return 'IDX_JSDQ_Daily'
		elif Code=='0012':
			return 'IDX_TMTS_Daily'
		elif Code=='0011':
			return 'IDX_T2_Daily'
		elif Code=='0018':
			return 'IDX_TB_Daily'
		elif Code=='0019':
			return 'IDX_TM_Daily'
		elif Code=='0020':
			return 'IDX_TS_Daily'
		else:
			return ''

	# Insert data
	def insertIdx(self,Code,Day,OpenV,HighV,LowV,CloseV,Change,ChangeRate,Volumn,cnn,cur):
		cur.execute("""INSERT INTO """+self.getTable(Code)+"""(
		Day,
		OpenV,
		HighV,
		LowV,
		CloseV,
		Change,
		ChangeRate,
		Volumn)VALUES (%s, %s,%s,%s,%s,%s,%s,%s)""" , (Day,OpenV,HighV,LowV,CloseV,Change,ChangeRate,Volumn))
		cnn.commit()



	# Insert Log
	def insertLog(self,createdBy,infoLevel,info,cnn,cur):
		cur.callproc('InsertInfoLog', (datetime.datetime.today(),createdBy,infoLevel,info))
		cnn.commit()


	# Get Data Exist
	def getExist(self,Code,Day,cur):
		cur.execute("""SELECT count(Day) FROM """+self.getTable(Code)+""" WHERE Day=%s""",(Day))
		result=cur.fetchone()
		return result[0]

	# Get DailySP
	def getDailyIdx(self,Code):
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

		print('[[-------------Starting Download ['+ Code +']-------------]]')
		i=1
		while 1==1:
			urlstr="https://kabutan.jp/stock/kabuka?code="+Code+"&ashi=day&page="+str(i)
			endflag=0
			print('[Page [' + str(i) + ']]')
			try:
				#Create Main Info===============Begin===============
				html = urlopen(urlstr,timeout=100)
				bsObj = BeautifulSoup(html,"html.parser")
				if i==1:
					table = bsObj.findAll("table",{"class":"stock_kabuka0"})[0]
					rows = table.findAll("tr")
					j=1
					try:

						for row in rows:
							if j!=1:
								rcRow=[]
								for cell in row.findAll(['td', 'th']):
									rcRow.append(cell.get_text())
								self.insertIdx(Code,self.formatDay(rcRow[0]),self.formatFloat(rcRow[1]),self.formatFloat(rcRow[2]),self.formatFloat(rcRow[3]),self.formatFloat(rcRow[4]),self.formatFloat(rcRow[5]),self.formatFloat(rcRow[6]),self.formatInt(rcRow[7]),cnn,cur)
							j=j+1
					except:
						self.insertLog('nyd.getDailyIdx()',3,'Data is Exist:'+str(sys.exc_info()[0]),cnn,cur)
						print("Data is Exist:", sys.exc_info()[0])

				table = bsObj.findAll("table",{"class":"stock_kabuka1"})[0]
				rows = table.findAll("tr")
				j=1
				for row in rows[1:]:
					rcRow=[]
					for cell in row.findAll(['td', 'th']):
						rcRow.append(cell.get_text())


					if self.getExist(Code,self.formatDay(rcRow[0]),cur)!=0:
						print('[[-------------Download ['+ Code +'] Over-------------]]')
						endflag=1
						break
					self.insertIdx(Code,self.formatDay(rcRow[0]),self.formatFloat(rcRow[1]),self.formatFloat(rcRow[2]),self.formatFloat(rcRow[3]),self.formatFloat(rcRow[4]),self.formatFloat(rcRow[5]),self.formatFloat(rcRow[6]),self.formatInt(rcRow[7]),cnn,cur)

					if endflag==1:
						break

					j=j+1

				if endflag==1:
					break

				if j==2:
					print('[[-------------Download ['+ Code +'] Over-------------]]')
					break
				#Create Main Info===============Over===============

				i=i+1

			except:
				insertLog(self,'nyd.getDailyIdx()',3,'Unexpected error:'+str(sys.exc_info()[0]),cnn,cur)
				cnn.close()
				print("Unexpected error:", sys.exc_info()[0],rcRow)
		cnn.close()
		#f.close()

	def getDailyIndex(self):
		self.getDailyIdx('1697')
		self.getDailyIdx('0951')
		self.getDailyIdx('0950')
		self.getDailyIdx('0000')
		self.getDailyIdx('0010')
		self.getDailyIdx('0100')
		self.getDailyIdx('0012')
		self.getDailyIdx('0011')
		self.getDailyIdx('0018')
		self.getDailyIdx('0019')
		self.getDailyIdx('0020')




	
