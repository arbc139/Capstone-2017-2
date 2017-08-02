import requests
import pymysql
import sys
import os
from xml.etree.ElementTree import parse
from lxml import etree
import re
import time
job_num = sys.argv[1]

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
tree = parse("/home/hogking/hubmed/backend/files/"+job_num+"/esearch.xml")
esearch_root = tree.getroot()

web = esearch_root.find("WebEnv").text;
key = esearch_root.find("QueryKey").text;
count = esearch_root.find("Count").text;
f = open("/home/hogking/hubmed/backend/files/"+job_num+"/uilist.text", 'r')
job_has_pmid = []
while True:
	line = f.readline().rstrip('\n')
	if len(line) > 0:
		if line[0] == "<":
			query = "UPDATE JOB SET TOO_SMALL=1 WHERE J_ID=(%s)"
			curs.execute(query,job_num)
	if not line : break
	job_has_pmid.append(int(line))
f.close()
step = 50000
exist_pmid = []
new_pmid = []
for i in range(0,int(int(count)/step)+1):
	curs = conn.cursor(pymysql.cursors.DictCursor)
	#query = "CREATE TEMPORARY TABLE temp_uilist_"+job_num+"_"+str(i)+"(pmid int(11));"
	query1 = "CREATE TEMPORARY TABLE temp_uilist_"+job_num+"_"+str(i)+"(pmid int(11));"
	curs.execute(query1)

	start = time.time()
	query2 = "INSERT INTO temp_uilist_"+job_num+"_"+str(i)+" (pmid) VALUES (%s)"
	curs.executemany(query2, job_has_pmid[i*step:(i+1)*step-1])
	end = time.time()-start
	print (query2, end)
	start = time.time()
	query3 = "SELECT a.pmid FROM PMID a JOIN temp_uilist_"+job_num+"_"+str(i)+" b ON a.pmid = b.pmid;"
	#curs.execute(query3)
	end = time.time()-start
	print(query3,end)
	#rows = curs.fetchall()
	#for row in rows:
	#	exist_pmid.append((job_num,row['pmid']))

	start = time.time()
	query4 = "SELECT a.pmid FROM temp_uilist_"+job_num+"_"+str(i)+" a LEFT JOIN PMID b ON a.pmid = b.pmid WHERE b.pmid IS NULL;"
	curs.execute(query4)
	end = time.time()-start
	print(query4,end)
	rows = curs.fetchall()
	for row in rows:
		new_pmid.append(str(row['pmid']))
	query5 = "DROP TABLE temp_uilist_"+job_num+"_"+str(i)
	curs.execute(query5)
temp_exist_pmid = list(set(job_has_pmid) - set(new_pmid))
for elm in temp_exist_pmid	:
	exist_pmid.append((job_num,elm))	
#make temp table
"""curs = conn.cursor(pymysql.cursors.DictCursor)
query = "CREATE TEMPORARY TABLE temp_uilist_"+job_num+"(pmid int(11));"
curs.execute(query)
#print (job_has_pmid)
query = "INSERT INTO temp_uilist_"+job_num+" (pmid) VALUES (%s)"
curs.executemany(query, job_has_pmid)"""


#filter out pmids that already in the db
"""query = "SELECT a.pmid FROM PMID a JOIN temp_uilist_"+job_num+" b ON a.pmid = b.pmid;"
curs.execute(query)
rows = curs.fetchall()
exist_pmid = []
for row in rows:
	exist_pmid.append((job_num,row['pmid']))"""
for i in range(0,int(int(count)/step)+1):
		query = "INSERT IGNORE INTO JOB_PMID (J_ID, PMID) VALUES (%s,%s)"
		curs.executemany(query, exist_pmid[i*step:(i+1)*step-1])
#print(exist_pmid)


#select new pmids
"""query = "SELECT a.pmid FROM temp_uilist_"+job_num+"_"+i+" a LEFT JOIN PMID b ON a.pmid = b.pmid WHERE b.pmid IS NULL;"
curs.execute(query)
rows = curs.fetchall()
for row in rows:
	new_pmid.append(str(row['pmid']))"""
new_pmid_str = ", ".join(new_pmid)