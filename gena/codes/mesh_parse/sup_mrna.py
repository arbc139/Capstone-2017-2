import re
import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

bar = ' .*'
bar_reg = re.compile(bar, re.IGNORECASE)

query = "select * FROM SUP WHERE PROCESSED REGEXP 'MIRN' AND PROCESSED REGEXP ' ';"
curs.execute(query)
rows = curs.fetchall()
for row in rows:
	target = row['PROCESSED']
	s_id = row['S_ID']
	target = bar_reg.sub('', target)
	print (target)
	query = "UPDATE SUP SET PROCESSED =(%s) WHERE S_ID = (%s)"
	curs.execute(query,(target, s_id))
