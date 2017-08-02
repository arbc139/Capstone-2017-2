import pymysql
import requests
from xml.etree import ElementTree
import time
import sys

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
#query ="SELECT S_NAME,S_ID,PROCESSED FROM SUP WHERE  F_NAME IS NULL ORDER BY IDX asc"
query = "SELECT * FROM SUP WHERE S_NAME REGEXP 'MIRN-';"
curs.execute(query)
#print (query)
rows = curs.fetchall()
name = ""
hgnc_id = ""
score = ""
f_name = "N/A"
f_id  = "N/A"
f_score = "-1"
n_name ="N/A"
n_id = "N/A"
n_score = "-1"
url = "http://rest.genenames.org/search/"
for row in rows:
	s_id = row['S_ID']
	processed = row['PROCESSED']
	while True:
		try:
			hgnc_out = requests.get(url+processed)
		except:
			print("request failed, retry after 5 seconds")
			time.sleep(5)
			continue
		else:
			break
		if hgnc_out.status_code == 200:
			break
	tree = ElementTree.fromstring(hgnc_out.content)
	f_name = "N/A"
	f_id  = "N/A"
	f_score = "-1"
	n_name ="N/A"
	n_id = "N/A"
	n_score = "-1"
	for child in tree.getiterator("result"):
		#print("S_NAME : "+row['S_NAME'])
		#print ("PROCESSED : "+processed+"\n")
		num = int(child.get('numFound'))
		if num == 1:
			doc = child.find("doc")
			for elm in doc:
				if elm.get('name') == "symbol":
					f_name = elm.text
				elif elm.get('name') == "hgnc_id":
					f_id = elm.text
				elif elm.get('name') == "score":
					f_score = elm.text
		elif num > 1:
			counter = 0
			for doc in child.getiterator("doc"):
				for elm in doc:
					if elm.get('name') == "symbol":
						name = elm.text
					elif elm.get('name') == "hgnc_id":
						hgnc_id = elm.text
					elif elm.get('name') == "score":
						score = elm.text
					if counter == 0:
						f_name = name
						f_id = hgnc_id
						f_score = score
					elif counter == 1:
						n_name = name
						n_id = hgnc_id
						n_score = score
				if counter > 2:
					break
				counter += 1
		else:
			do_nothing = 1
	query = "UPDATE  SUP SET F_NAME=(%s), F_ID=(%s), F_SCORE=(%s), N_NAME=(%s), N_ID=(%s), N_SCORE=(%s) WHERE S_ID = (%s)"
	curs.execute(query,(f_name,f_id,f_score,n_name,n_id,n_score,s_id))
	"""print ("Name : "+f_name)
	print ("Id : "+f_id)
	print ("Score : "+f_score)
	print ("Name : "+n_name)
	print ("Id : "+n_id)
	print ("Score : "+n_score)
	print("\n")"""