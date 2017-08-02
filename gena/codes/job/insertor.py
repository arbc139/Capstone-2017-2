import pymysql
import subprocess
import os
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT * FROM JOB where DO_PMID_INSERT =1"
curs.execute(query)
row = curs.fetchone()

if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/pmid/pmid_insertor.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/insert_pmid_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET DO_PMID_INSERT =2 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)

query = "SELECT * FROM JOB where DO_PMID_INSERT =3"
curs.execute(query)
row = curs.fetchone()

if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/pmid/process_country.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/process_country_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET DO_PMID_INSERT =4 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)

