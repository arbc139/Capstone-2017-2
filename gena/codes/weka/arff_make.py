import sys
import pymysql
j_id = sys.argv[1]

filename = "/home/hogking/hubmed/backend/files/"+j_id+"/input.arff"
arff = open(filename,'w')

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = "SELECT COUNT(*) FROM JOB_PMID WHERE J_ID = (%s) ;"
curs.execute(query,j_id)
row = curs.fetchone()
pmid_count = row['COUNT(*)']
print(pmid_count)
curs.close()

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "UPDATE JOB SET PMID_COUNT = (%s) WHERE J_ID = (%s);"
curs.execute(query,(pmid_count,j_id))

query = "select DISTINCT E.HGNC_ID from JOB_PMID A, PMID B, PMID_SUP C, SUP D, GENE E WHERE A.J_ID = "+j_id+" AND A.PMID = B.PMID AND B.PMID = C.PMID AND C.S_ID = D.S_ID AND D.HGNC_ID = E.HGNC_ID AND E.HGNC_ID != 'N/A';"
curs.execute(query)
symbols = curs.fetchall()

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

if len(symbols) == 0:
	query = "UPDATE JOB SET TOO_SMALL=1 WHERE J_ID=(%s)"
	curs.execute(query,j_id)
relation_name = "@relation "+j_id+".symbolic\n\n"
arff.write(relation_name)
for sym in symbols:
	arff.write('@attribute '+sym['HGNC_ID']+' {0,1}\n')
arff.write('@data\n')



curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = "SELECT DISTINCT A.PMID FROM JOB_PMID A, PMID_SUP B, SUP C, GENE D WHERE A.J_ID = "+j_id+" AND  B.PMID = A.PMID AND B.S_ID = C.S_ID AND C.HGNC_ID = D.HGNC_ID AND D.HGNC_ID != 'N/A' "
curs.execute(query)
pm_ids = curs.fetchall()


curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

thesis = []
for pm_id in pm_ids:
	del thesis[:] 
	#print (pm_id['PMID'])
	query ="SELECT DISTINCT B.HGNC_ID FROM PMID_SUP A, SUP B, GENE C WHERE A.PMID = (%s) AND A.S_ID = B.S_ID AND B.HGNC_ID = C.HGNC_ID AND C.HGNC_ID != 'N/A';"
	#query = "SELECT  SYMBOL FROM "+disease_name+"_GENES WHERE  IS_FAMILY = 0 AND PM_ID="+str(pm_id[0])+" AND MAX_SCORE > " + max_score 
	curs.execute(query,pm_id['PMID'])
	match = curs.fetchall()
	#print (match)
	for sym in symbols:
		if sym in match:
			#print (sym)
			#print (match)
			thesis.append('1')
		else:
			thesis.append('?')
	outstr = ', '.join(thesis)
	#print (outstr)
	arff.write(outstr+'\n')

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "UPDATE JOB SET WEKA =2 WHERE J_ID = (%s)"
curs.execute(query,j_id	)      
conn.close()	


