import pymysql
import re

p = re.compile('\d+')
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = " SELECT * FROM GENE"
curs.execute(query)
rows = curs.fetchall()
for row in rows:
	hgnc_id = row['HGNC_ID']
	result = p.findall(hgnc_id)
	query = "  UPDATE GENE SET HGNC_ID =(%s) WHERE HGNC_ID =(%s);"
	curs.execute(query,(result[0],hgnc_id))
