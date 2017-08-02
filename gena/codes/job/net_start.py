import pymysql
import subprocess
import os
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT * FROM JOB where WEKA = 4 AND NETWORK = 0"
curs.execute(query)
row = curs.fetchone()
if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/weka/weka_analysis.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/weka_analysis_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET NETWORK = 1 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)

query = "SELECT * FROM JOB where NETWORK = 2"
curs.execute(query)
row = curs.fetchone()
if row is not None:
	j_id = str(row['J_ID'])
	python_call ="nohup python "
	main_dir = " /home/hogking/hubmed/backend/codes/network/network_analysis.py "
	std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/network_analysis_log.out "
	command = ""
	command += python_call
	command += main_dir
	command += j_id
	command += std_out
	print (command)
	query = "UPDATE JOB SET NETWORK = 3 WHERE J_ID = (%s)"
	curs.execute(query,j_id)
	subprocess.call(command,shell=True)