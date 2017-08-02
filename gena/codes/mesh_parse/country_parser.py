import re
import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

f = open('/home/hogking/hubmed/backend/meshfile/country.txt', 'r')
lines = f.readlines()
for line in lines:
	processed = line.replace('\n','')
	query = "INSERT IGNORE INTO COUNTRY(C_NAME) VALUES ((%s))"
	curs.execute(query, processed)
	print (line)