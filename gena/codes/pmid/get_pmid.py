import requests
import pymysql
import sys
import os
from xml.etree.ElementTree import parse
from lxml import etree
import re
import time
import datetime
job_num = sys.argv[1]

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT * FROM JOB where J_ID ="+job_num
curs.execute(query)
row = curs.fetchone()
print(row['QUERY'], row['START_DATE'], row['END_DATE'])
query_string = "("+row['QUERY']+")"
#+" AND (\""+str(row['START_DATE'])+"\"[Date - Create] : \""+str(row['END_DATE'])+"\"[Date - Create]) "
print (query_string)
row_start = datetime.datetime.strptime( str(row['START_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")
row_end = datetime.datetime.strptime( str(row['END_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")

query ="SELECT J_ID,QUERY, START_DATE, END_DATE FROM JOB WHERE J_ID != (%s) AND NETWORK = 4 AND TOO_SMALL != 1 AND QUERY=(%s) AND START_DATE >= (%s) AND END_DATE <= (%s) ORDER BY DATEDIFF(END_DATE,START_DATE) DESC;"
curs.execute(query, (job_num,row['QUERY'],  row['START_DATE'],row['END_DATE']))
row2 = curs.fetchone()
query ="SELECT J_ID,QUERY, START_DATE, END_DATE FROM JOB WHERE J_ID != (%s) AND NETWORK = 4 AND QUERY=(%s)  AND END_DATE <= (%s) AND DATEDIFF((%s),START_DATE) < 365 ORDER BY START_DATE DESC;"
curs.execute(query, (job_num,row['QUERY'],  row['END_DATE'],row['START_DATE']))
row3 = curs.fetchone()
delete_string = ""
if row2 is not None:
	print(row2)
	query ="INSERT IGNORE INTO JOB_PMID (J_ID,PMID) SELECT (%s), PMID FROM JOB_PMID WHERE J_ID =(%s)"
	curs.execute(query,(job_num,row2['J_ID']))
	print(row['START_DATE'], row['END_DATE'], row2['START_DATE'], row2['END_DATE'])
	row2_start = datetime.datetime.strptime( str(row2['START_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")
	row2_end = datetime.datetime.strptime( str(row2['END_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")
	period = " AND ((\""+str(row_start)+"\"[Date - Entrez] : \""+str(row2_start)+"\"[Date - Entrez]) OR (\""+str(row2_end)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez]))"
	query_string += period
elif row3 is not None :
	print(row3)
	query ="INSERT IGNORE INTO JOB_PMID (J_ID,PMID) SELECT (%s), PMID FROM JOB_PMID WHERE J_ID =(%s)"
	curs.execute(query,(job_num,row3['J_ID']))
	print (row3['J_ID'])
	row3_start = datetime.datetime.strptime( str(row3['START_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")
	row3_end = datetime.datetime.strptime( str(row3['END_DATE']), "%Y-%m-%d").strftime("%Y/%m/%d")
	print(query)
	print(row['START_DATE'], row['END_DATE'], row3['START_DATE'], row3['END_DATE'])
	period = " AND ((\""+str(row3_end)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez]))"
	delete_period = " AND ((\""+str(row3_start)+"\"[Date - Entrez] : \""+str(row['START_DATE']-datetime.timedelta(days=1))+"\"[Date - Entrez]))"
	delete_string = query_string + delete_period
	#query_string += " AND (\""+str(row3_end)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez] OR (\""+str(row_start)+"\"[Date - Entrez] : \""+str(row_start)+"\"[Date - Entrez])OR (\""+str(row_end)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez])) "
	query_string += " AND ((\""+str(row_start)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez]) )"
else:
	query_string += " AND ((\""+str(row_start)+"\"[Date - Entrez] : \""+str(row_end)+"\"[Date - Entrez]) )"
print(query_string)
"""if delete_string != "":
	entrez_db = 'pubmed'
	#query = row['QUERY']
	#assemble the esearch URL
	base = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/';
	delete_url = base + "esearch.fcgi?db="+entrez_db+"&term="+delete_string+"&usehistory=y";

	#do esearch
	delete_output = requests.get(delete_url)

	#save esearch result
	if not os.path.exists("/home/hogking/hubmed/backend/files/"+job_num+"/"):
	    os.makedirs("/home/hogking/hubmed/backend/files/"+job_num+"/")
	f = open("/home/hogking/hubmed/backend/files/"+job_num+"/delete_esearch.xml", 'w')
	f.write(delete_output.text)
	f.close()

	#esearch --> parse WebEnv, QueryKey
	delete_tree = parse("/home/hogking/hubmed/backend/files/"+job_num+"/delete_esearch.xml")
	delete_esearch_root = delete_tree.getroot()

	delete_web = delete_esearch_root.find("WebEnv").text;
	delete_key = delete_esearch_root.find("QueryKey").text;
	delete_count = delete_esearch_root.find("Count").text;

	#print (count);

	#get uilist
	delete_retmax = 5000
	delete_f = open("/home/hogking/hubmed/backend/files/"+job_num+"/delete_uilist.text",'w')
	for i in range(0,int(int(delete_count)/delete_retmax)+1):
		delete_efetch_url = base +"efetch.fcgi?db="+entrez_db+"&WebEnv="+delete_web
		delete_efetch_url += "&query_key="+delete_key+"&retstart="+str(i*delete_retmax)
		delete_efetch_url += "&retmax="+str(delete_retmax)+"&rettype=uilist&retmode=text"
		while True:
			try:
				delete_efetch_out = requests.get(delete_efetch_url)
			except:
				print("request failed, retry after 5 seconds")
				time.sleep(5)
				continue
			else:
				break
			if delete_efetch_out.status_code == 200:
				break
		print (delete_efetch_out.status_code)
		pattern = r"Unable to obtain query"
		m = re.search(pattern,delete_efetch_out.text)
		if m is not None :
			print("request failed, retry after 5 seconds")
			time.sleep(5)
			i = i-1
		delete_f.write(delete_efetch_out.text)
	delete_f.close()

	#write all pmid list
	f = open("/home/hogking/hubmed/backend/files/"+job_num+"/delete_uilist.text", 'r')
	delete_job_has_pmid = []
	while True:
		line = f.readline().rstrip('\n')
		if len(line) > 0:
			if line[0] == "<":
				curs.close()
				conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
				curs = conn.cursor(pymysql.cursors.DictCursor)
				#query = "UPDATE JOB SET TOO_SMALL=1 WHERE J_ID=(%s)"
				#curs.execute(query,job_num)
		if not line : break
		delete_job_has_pmid.append(str(line))
	f.close()
	delete_job_has_pmid_str = " ,".join(delete_job_has_pmid)
	curs.close()
	conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
	curs = conn.cursor(pymysql.cursors.DictCursor)
	query = "DELETE FROM JOB_PMID WHERE J_ID = (%s) AND PMID = (%s)"
	for pmid in delete_job_has_pmid:
		curs.execute(query,(job_num,pmid))

"""
entrez_db = 'pubmed'
#query = row['QUERY']
#assemble the esearch URL
base = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/';
url = base + "esearch.fcgi?db="+entrez_db+"&term="+query_string+"&usehistory=y";

#do esearch
output = requests.get(url)

#save esearch result
if not os.path.exists("/home/hogking/hubmed/backend/files/"+job_num+"/"):
    os.makedirs("/home/hogking/hubmed/backend/files/"+job_num+"/")
f = open("/home/hogking/hubmed/backend/files/"+job_num+"/esearch.xml", 'w')
f.write(output.text)
f.close()

#esearch --> parse WebEnv, QueryKey
tree = parse("/home/hogking/hubmed/backend/files/"+job_num+"/esearch.xml")
esearch_root = tree.getroot()

web = esearch_root.find("WebEnv").text;
key = esearch_root.find("QueryKey").text;
count = esearch_root.find("Count").text;

#print (count);

#get uilist
retmax = 5000
f = open("/home/hogking/hubmed/backend/files/"+job_num+"/uilist.text",'w')
for i in range(0,int(int(count)/retmax)+1):
	efetch_url = base +"efetch.fcgi?db="+entrez_db+"&WebEnv="+web
	efetch_url += "&query_key="+key+"&retstart="+str(i*retmax)
	efetch_url += "&retmax="+str(retmax)+"&rettype=uilist&retmode=text"
	while True:
		try:
			efetch_out = requests.get(efetch_url)
		except:
			print("request failed, retry after 5 seconds")
			time.sleep(5)
			continue
		else:
			break
		if efetch_out.status_code == 200:
			break
	print (efetch_out.status_code)
	pattern = r"Unable to obtain query"
	m = re.search(pattern,efetch_out.text)
	if m is not None :
		print("request failed, retry after 5 seconds")
		time.sleep(5)
		i = i-1
	f.write(efetch_out.text)
f.close()

#write all pmid list
f = open("/home/hogking/hubmed/backend/files/"+job_num+"/uilist.text", 'r')
job_has_pmid = []
while True:
	line = f.readline().rstrip('\n')
	if len(line) > 0:
		if line[0] == "<":
			curs.close()
			conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
			curs = conn.cursor(pymysql.cursors.DictCursor)
			query = "UPDATE JOB SET TOO_SMALL=1 WHERE J_ID=(%s)"
			curs.execute(query,job_num)
	if not line : break
	job_has_pmid.append(int(line))
f.close()
step = 500000
exist_pmid = []
new_pmid = []

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
for i in range(0,int(int(count)/step)+1):
	curs.close()
	curs = conn.cursor(pymysql.cursors.DictCursor)
	#query = "CREATE TEMPORARY TABLE temp_uilist_"+job_num+"_"+str(i)+"(pmid int(11));"
	query1 = "CREATE TABLE temp_uilist_"+job_num+"_"+str(i)+"(pmid int(11));"
	curs.execute(query1)
	print (query1	)
	query2 = "INSERT INTO temp_uilist_"+job_num+"_"+str(i)+" (pmid) VALUES (%s)"
	curs.executemany(query2, job_has_pmid[i*step:(i+1)*step-1])

	"""query3 = "SELECT a.pmid FROM PMID a JOIN temp_uilist_"+job_num+"_"+str(i)+" b ON a.pmid = b.pmid;"
	curs.execute(query3)
	rows = curs.fetchall()
	for row in rows:
		exist_pmid.append((job_num,row['pmid']))"""
	curs.close()
	conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
	curs = conn.cursor(pymysql.cursors.DictCursor)
	query4 = "SELECT STRAIGHT_JOIN a.pmid FROM temp_uilist_"+job_num+"_"+str(i)+" a LEFT JOIN PMID b ON a.pmid = b.pmid WHERE b.pmid IS NULL;"
	curs.execute(query4)
	rows = curs.fetchall()
	for row in rows:
		new_pmid.append(str(row['pmid']))
	curs.close()
	conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
	curs = conn.cursor(pymysql.cursors.DictCursor)
	query1 = "DROP  TABLE temp_uilist_"+job_num+"_"+str(i)
	curs.execute(query1)
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
curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
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
#print(new_pmid_str)


#epost and save new pmids
epost_param = {'db' : entrez_db, 'id' : new_pmid_str}
url = base + "epost.fcgi?"
epost_output = requests.post(url, data=epost_param)
#print (epost_output.text)
f = open("/home/hogking/hubmed/backend/files/"+job_num+"/epost.xml", 'w')
f.write(epost_output.text)
f.close()
curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "UPDATE JOB SET PMID_COLLECT =2 WHERE J_ID = (%s)"
curs.execute(query,job_num)      
if  len(new_pmid) != 0:
	#epost --> parse
	tree = parse("/home/hogking/hubmed/backend/files/"+job_num+"/epost.xml")                                                       
	epost_root = tree.getroot()                                                  

	web = epost_root.find("WebEnv").text;
	key = epost_root.find("QueryKey").text;                                                         
	#number of thesis to be collected
	#count = int(count) - len(exist_pmid);                                       
	count = len(new_pmid)                                                                             
	print (count);                                                                
	                                                                               
	#do efetch and save as xml                                                                                   
	f = open("/home/hogking/hubmed/backend/files/"+job_num+"/new_pmid_record.xml",'w')  
	f.write("<PubmedArticleSet>")      
	print(int(int(count)/retmax)+1)                                                 
	for i in range(0,int(int(count)/retmax)+1):                                    
		efetch_url = base +"efetch.fcgi?db="+entrez_db+"&WebEnv="+web          
		efetch_url += "&query_key="+key+"&retstart="+str(i*retmax)           
		efetch_url += "&retmax="+str(retmax)+"&rettype=&retmode=xml"  
		while True:
			try:
				efetch_out = requests.get(efetch_url)
			except:
				print("request failed, retry after 5 seconds")
				time.sleep(5)
				continue
			else:
				break
			print (efetch_out.status_code)
			if efetch_out.status_code == 200:
				break
		xmlString = re.sub(r"<\?xml .*\?>", r'', efetch_out.text)
		xmlString = re.sub(r"<\!DOCTYPE .*>", r'', xmlString)
		xmlString = re.sub(r"</PubmedArticleSet>", r'', xmlString)
		xmlString = re.sub(r"<PubmedArticleSet>", r'', xmlString)
		
		f.write(str(xmlString))                        
					        	                       
	f.write("</PubmedArticleSet>")
	f.close()
	curs.close()
	conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
	curs = conn.cursor(pymysql.cursors.DictCursor)
	query = "UPDATE JOB SET DO_PMID_INSERT =1 WHERE J_ID = (%s)"
	curs.execute(query,job_num)         
else:
	curs.close()
	conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
	curs = conn.cursor(pymysql.cursors.DictCursor)
	query = "UPDATE JOB SET DO_PMID_INSERT =3 WHERE J_ID = (%s)"
	curs.execute(query,job_num)                           
	"""
	context = etree.iterparse("../files/"+job_num+"/new_pmid_record.xml", events=('end',), tag='PubmedArticle')
	pmid_list = []
	job_pmid_list = []
	sup_list =[]
	des_list = []
	qual_list = []
	context = etree.iterparse("../files/"+job_num+"/new_pmid_record.xml", events=('end',), tag='PubmedArticle')
	for event, element in context:
		med_cite = element.find("MedlineCitation")
		pmid = str(med_cite.find("PMID").text.encode('utf-8'))
		#print (pmid)
		article = med_cite.find("Article")
		medline = med_cite.find("MedlineJournalInfo")
		abstract = ""
		title = ""
		country =""
		if article is not None:
			if article.find("ArticleTitle").text is not None:
				title = str(article.find("ArticleTitle").text.encode('utf-8'))
			if article.find("Abstract") is not None:
				for child in article.find("Abstract") :
					if child.text is not None:
						abstract += str(child.text.encode('utf-8'))
		if medline is not None:
			if medline.find("Country") is not None:
				country = str(medline.find("Country").text.encode('utf-8'))
		chemical_list = med_cite.find("ChemicalList")
		heading_list = med_cite.find("MeshHeadingList")
		#print (country)
		mesh_list = []
		if chemical_list is not None:
			for child in chemical_list:
				mesh_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
				if child.find("NameOfSubstance").attrib['UI'] == "C":
					sup_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
					query	= "INSERT IGNORE INTO PMID_SUP(S_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
			#		curs.execute(query,(mesh))
				elif child.find("NameOfSubstance").attrib['UI'] =="D":
					des_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
					query	= "INSERT IGNORE INTO PMID_DES(D_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
			#		curs.execute(query,(mesh))
		if heading_list is not None:
			for child in heading_list:
				if child.find("DescriptorName") is not None:
					des_list.append((child.find("DescriptorName").attrib['UI'] , child.find("DescriptorName").attrib['MajorTopicYN'],pmid))
				if child.find("QualifierName") is not None:
					qual_list.append((child.find("QualifierName").attrib['UI'], child.find("DescriptorName").attrib['MajorTopicYN'],pmid))
		pmid_list.append((pmid,title,abstract,country))
		job_pmid_list.append((job_num,pmid))
	#print (sup_list)
	#print  (des_list)
	#print (qual_list)
	query = "INSERT IGNORE INTO PMID (PMID, TITLE, ABSTRACT,COUNTRY ) VALUES (%s, %s, %s, %s)"
	curs.executemany(query, pmid_list)
	query = "INSERT IGNORE INTO JOB_PMID (JOB_NUM, PMID) VALUES (%s, %s)"
	curs.executemany(query, job_pmid_list)
	query	= "INSERT IGNORE INTO PMID_SUP(S_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
	curs.executemany(query, sup_list)
	query	= "INSERT IGNORE INTO PMID_DES(D_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
	curs.executemany(query, des_list)
	query	= "INSERT IGNORE INTO PMID_QUAL(Q_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
	curs.executemany(query, qual_list)"""