import requests
import pymysql
import sys
import os
from xml.etree.ElementTree import parse
from lxml import etree
import re
import time
import datetime
job_num = sys.argv[1]

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT * FROM JOB where J_ID ="+job_num
curs.execute(query)
row = curs.fetchone()
print(row['QUERY'], row['START_DATE'], row['END_DATE'])
query_string = "("+row['QUERY']+")"
#+" AND (\""+str(row['START_DATE'])+"\"[Date - Create] : \""+str(row['END_DATE'])+"\"[Date - Create]) "
print (query_string)

query ="SELECT J_ID,QUERY, START_DATE, END_DATE FROM JOB WHERE J_ID != (%s) AND QUERY=(%s)  AND END_DATE <= (%s) AND DATEDIFF((%s),START_DATE) < 365 ORDER BY START_DATE DESC;"
curs.execute(query, (job_num,row['QUERY'],  row['END_DATE'],row['START_DATE']))
row2 = curs.fetchone()
if row2 is not None:
	print(row2)
	query ="INSERT IGNORE INTO JOB_PMID (J_ID,PMID) SELECT (%s), PMID FROM JOB_PMID WHERE J_ID =(%s)"
	curs.execute(query,(job_num,row2['J_ID']))
	print (row2['J_ID'])
	print(query)
	print(row['START_DATE'], row['END_DATE'], row2['START_DATE'], row2['END_DATE'])
	period = " AND ((\""+str(row2['END_DATE'])+"\"[Date - Create] : \""+str(row['END_DATE'])+"\"[Date - Create]))"
	delete_period = " AND ((\""+str(row2['START_DATE'])+"\"[Date - Create] : \""+str(row['START_DATE']-datetime.timedelta(days=1))+"\"[Date - Create]))"
	delete_string = query_string + delete_period
	query_string += period

else :
	query_string += " AND (\""+str(row['START_DATE'])+"\"[Date - Create] : \""+str(row['END_DATE'])+"\"[Date - Create]) "
print(query_string)
print(delete_string)
