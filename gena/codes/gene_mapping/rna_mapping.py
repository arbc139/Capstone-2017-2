import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query =  "SELECT * FROM SUP WHERE S_NAME REGEXP 'RNA' AND NOT S_NAME REGEXP 'MICRORNA' AND F_SCORE > 5 AND F_SCORE-N_SCORE >2"

curs.execute(query)
rows = curs.fetchall()
for row in rows:
	query = "INSERT IGNORE INTO GENE (HGNC_ID, SYMBOL) VALUES (%s, %s)"
	curs.execute(query,(row['F_ID'],row['F_NAME']))
	query = "UPDATE SUP SET HGNC_ID = (%s) WHERE S_ID = (%s)"
	curs.execute(query,(row['F_ID'],row['S_ID']))