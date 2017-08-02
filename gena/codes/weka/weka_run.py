import subprocess
import sys
import pymysql
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

j_id = sys.argv[1]

java_call =" java -cp /home/hogking/hubmed/backend/weka-3-8-1/weka.jar  -Xms30000m -Xmx50000m "
weka_dir = "  weka.associations.FPGrowth -t "
arff_dir = " /home/hogking/hubmed/backend/files/"+j_id+"/input.arff "
num_rule =" -N 1000000000 "
min_metric = " -C 0.0001"
upper_bound_min_supp=" -U 100 "
lower_bound_min_supp=" -M 0.0001 "
delta_mun_supp=" -D 0.5"
std_out = " > /home/hogking/hubmed/backend/files/"+j_id+"/asso.out "
command = ""
command += java_call
command += weka_dir
command += arff_dir
command += num_rule
command += min_metric
command += upper_bound_min_supp
command += lower_bound_min_supp
command += delta_mun_supp
command += std_out
subprocess.call(command,shell=True)
query = "UPDATE JOB SET WEKA =4 WHERE J_ID = (%s)"
curs.execute(query,j_id)      