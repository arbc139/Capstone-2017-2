import subprocess
import sys
import pymysql
import os
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

j_id = sys.argv[1]

if not os.path.exists("/home/hogking/hubmed/backend/files/"+j_id+"/net_asso.txt"):
    os.mknod("/home/hogking/hubmed/backend/files/"+j_id+"/net_asso.txt")

python_call ="nohup python "
main_dir = " /home/hogking/hubmed/backend/codes/weka/weka_analysis/FPGrowth/main.py "
input_dir = " -i /home/hogking/hubmed/backend/files/"+j_id+"/asso.out "
output_dir =" -o /home/hogking/hubmed/backend/files/"+j_id+"/net_asso.txt "
job_option =" -d "+j_id+ " "
std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/net_asso_log.out "
command = ""
command += python_call
command += main_dir
command += input_dir
command += output_dir
command += job_option	
command += std_out
subprocess.call(command,shell=True)

python_call ="nohup python "
main_dir = " /home/hogking/hubmed/backend/codes/weka/cooc.py "
std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/net_cooc_log.out "
command = ""
command += python_call
command += main_dir
command += j_id
command += std_out
subprocess.call(command,shell=True)
"""
python_call ="nohup python "
main_dir = " /home/hogking/hubmed/backend/codes/weka/des_counter.py "
std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/des_counter_log.out "
command = ""
command += python_call
command += main_dir
command += j_id
command += std_out
subprocess.call(command,shell=True)
"""

query = "UPDATE JOB SET NETWORK = 2 WHERE J_ID = (%s)"
curs.execute(query,j_id)
