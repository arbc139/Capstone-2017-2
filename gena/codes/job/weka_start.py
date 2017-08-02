import pymysql
import subprocess
import os
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT * FROM JOB where PMID_COLLECT=2 AND DO_PMID_INSERT =5 AND WEKA = 0"
curs.execute(query)
row = curs.fetchone()
if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/weka/arff_make.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/arff_make_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET WEKA =1 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)

query = "SELECT * FROM JOB where PMID_COLLECT=2 AND DO_PMID_INSERT =5 AND WEKA = 2"
curs.execute(query)
row = curs.fetchone()
if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/weka/weka_run.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/weka_run_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET WEKA =3 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)