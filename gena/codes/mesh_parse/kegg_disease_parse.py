import re
import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

f = open('/home/hogking/hubmed/backend/meshfile/br08402.keg', 'r')
lines = f.readlines()
for line in lines:
	if line[0] == 'C':
		processed = re.sub(r"^C\s*H\S*\s*", r'', line)
		processed_2 = re.sub(r"\s*[\(|\[][\s*|\S*]*$", r'', processed)
		processed_3 = processed_2.replace('\n','')
		query = "INSERT IGNORE INTO DISEASE(DIS_NAME) VALUES ((%s))"
		curs.execute(query, processed_3)
f.close()