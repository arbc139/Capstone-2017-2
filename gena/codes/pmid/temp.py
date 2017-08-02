import pymysql
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "CREATE TEMPORARY TABLE temp_uilist_1 (pmid int(11));"
curs.execute(query)