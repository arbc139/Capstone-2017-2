import subprocess
import sys
import pymysql
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

j_id = sys.argv[1]

query = "SELECT * FROM JOB_COUNTRY A, COUNTRY B WHERE A.J_ID = (%s) AND A.C_ID = B.C_ID ;"
curs.execute(query,j_id)
rows = curs.fetchall()
query_or = ""
if len(rows)  == 0:
	query = "UPDATE JOB SET DO_PMID_INSERT =5 WHERE J_ID = (%s)"
	curs.execute(query,j_id)      
	exit()
counter = 0
for row in rows:
	if counter == 0:
		if row['ONLY'] == 1:
			query_or += " NOT ( C.C_ID="+str(row['C_ID'])
		else :
			query_or += " ( C.C_ID="+str(row['C_ID'])
	else :
		query_or += " OR C.C_ID="+str(row['C_ID'])
	counter = counter+1
query_or += ")"

print (query_or)


query = "DELETE A FROM JOB_PMID A INNER JOIN PMID B INNER JOIN COUNTRY C  WHERE A.J_ID = (%s) AND A.PMID = B.PMID AND B.COUNTRY = C.C_NAME AND"
query += query_or
curs.execute(query,j_id)
query = "UPDATE JOB SET DO_PMID_INSERT =5 WHERE J_ID = (%s)"
curs.execute(query,j_id)      
