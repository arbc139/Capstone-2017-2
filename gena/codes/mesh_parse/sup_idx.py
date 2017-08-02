import pymysql
import requests
from xml.etree import ElementTree
import time
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)


query ="SELECT S_ID FROM SUP"
curs.execute(query)
rows = curs.fetchall()
idx = 1
for row in rows:
	s_id = row['S_ID']
	query = "UPDATE SUP SET IDX =(%s) WHERE S_ID = (%s)"
	curs.execute(query,(idx, s_id))
	idx += 1