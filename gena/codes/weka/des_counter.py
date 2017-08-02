import sys
import pymysql
j_id = sys.argv[1]
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = "SELECT  A.PMID, B.D_ID FROM JOB_PMID A, PMID_DES B WHERE A.J_ID = (%s) AND  B.PMID = A.PMID  AND B.MAJOR='Y' "
curs.execute(query,j_id)
pmids = curs.fetchall()
des_list = {}
for pmid in pmids:
	if pmid['D_ID'] in des_list :
		des_list[pmid['D_ID']] = des_list[pmid['D_ID']]+1
	else :
		des_list[pmid['D_ID']] = 1
print (des_list)
des_input = []
for des in des_list:
	des_input.append((j_id,des,des_list[des]))

print(des_input	)
query = "INSERT INTO JOB_DES(J_ID,D_ID,COUNT) VALUES ( (%s),(%s),(%s) )"
curs.executemany(query,des_input)
