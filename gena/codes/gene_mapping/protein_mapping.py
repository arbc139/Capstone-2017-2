import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = "SELECT * FROM SUP WHERE S_NAME REGEXP 'PROTEIN, ' AND F_SCORE !=-1 AND N_SCORE = -1 AND F_SCORE > 5;"
curs.execute(query)
rows = curs.fetchall()
for row in rows:
	query = "INSERT IGNORE INTO GENE (HGNC_ID, SYMBOL) VALUES (%s, %s)"
	curs.execute(query,(row['F_ID'],row['F_NAME']))
	query = "UPDATE SUP SET HGNC_ID = (%s) WHERE S_ID = (%s)"
	curs.execute(query,(row['F_ID'],row['S_ID']))
