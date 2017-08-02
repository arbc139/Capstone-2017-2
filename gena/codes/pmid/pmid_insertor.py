import requests
import pymysql
import sys
import os
from xml.etree.ElementTree import parse
from lxml import etree
import re
import time

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
job_num = sys.argv[1]
pmid_list = []
job_pmid_list = []
sup_list =[]
des_list = []
qual_list = []
pmid_content_list = []
if os.path.isfile("/home/hogking/hubmed/backend/files/"+job_num+"/new_pmid_record.xml") is False:
	exit()
context = etree.iterparse("/home/hogking/hubmed/backend/files/"+job_num+"/new_pmid_record.xml", events=('end',), tag='PubmedArticle')
for event, element in context:
	med_cite = element.find("MedlineCitation")
	pmid = str(int(med_cite.find("PMID").text))
	#print (pmid)
	article = med_cite.find("Article")
	medline = med_cite.find("MedlineJournalInfo")
	abstract = ""
	title = ""
	country =""
	if article is not None:
		if article.find("ArticleTitle").text is not None:
			title = str(article.find("ArticleTitle").text)
		if article.find("Abstract") is not None:
			for child in article.find("Abstract") :
				if child.text is not None:
					abstract += str(child.text)
	if medline is not None:
		if medline.find("Country") is not None:
			country = str(medline.find("Country").text)
	chemical_list = med_cite.find("ChemicalList")
	heading_list = med_cite.find("MeshHeadingList")
	#print (country)
	mesh_list = []
	if chemical_list is not None:
		for child in chemical_list:
			mesh_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
			if len(child.find("NameOfSubstance").attrib['UI']) > 0:
				if child.find("NameOfSubstance").attrib['UI'][0] == "C":
					sup_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
				elif child.find("NameOfSubstance").attrib['UI'][0] =="D":
					des_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
	"""if heading_list is not None:
		for child in heading_list:
			if child.find("DescriptorName") is not None:
				if child.find("DescriptorName").attrib['MajorTopicYN'] == "Y":
					des_list.append((child.find("DescriptorName").attrib['UI'] , child.find("DescriptorName").attrib['MajorTopicYN'],pmid))
			if child.find("QualifierName") is not None:
				if child.find("DescriptorName").attrib['MajorTopicYN'] == "Y":
					qual_list.append((child.find("QualifierName").attrib['UI'], child.find("DescriptorName").attrib['MajorTopicYN'],pmid))"""
	pmid_list.append((pmid,country))
	pmid_content_list.append((pmid,title,abstract))

	job_pmid_list.append((job_num,pmid))

if os.path.isfile("/home/hogking/hubmed/backend/files/"+job_num+"/new_pmid_record.xml") is False:
	exit()
context = etree.iterparse("/home/hogking/hubmed/backend/files/"+job_num+"/new_pmid_record.xml", events=('end',), tag='PubmedBookArticle')
for event, element in context:
	med_cite = element.find("BookDocument")
	pmid = str(int(med_cite.find("PMID").text))
	#print (pmid)
	article = med_cite.find("Article")
	medline = med_cite.find("MedlineJournalInfo")
	abstract = ""
	title = ""
	country =""
	if article is not None:
		if article.find("ArticleTitle").text is not None:
			title = str(article.find("ArticleTitle").text)
		if article.find("Abstract") is not None:
			for child in article.find("Abstract") :
				if child.text is not None:
					abstract += str(child.text)
	if medline is not None:
		if medline.find("Country") is not None:
			country = str(medline.find("Country").text)
	chemical_list = med_cite.find("ChemicalList")
	heading_list = med_cite.find("MeshHeadingList")
	#print (country)
	mesh_list = []
	if chemical_list is not None:
		for child in chemical_list:
			mesh_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
			if len(child.find("NameOfSubstance").attrib['UI']) > 0:
				if child.find("NameOfSubstance").attrib['UI'][0] == "C":
					sup_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
				elif child.find("NameOfSubstance").attrib['UI'][0] =="D":
					des_list.append((child.find("NameOfSubstance").attrib['UI'], "S",pmid))
	"""if heading_list is not None:
		for child in heading_list:
			if child.find("DescriptorName") is not None:
				if child.find("DescriptorName").attrib['MajorTopicYN'] == "Y":
					des_list.append((child.find("DescriptorName").attrib['UI'] , child.find("DescriptorName").attrib['MajorTopicYN'],pmid))
			if child.find("QualifierName") is not None:
				if child.find("DescriptorName").attrib['MajorTopicYN'] == "Y":
					qual_list.append((child.find("QualifierName").attrib['UI'], child.find("DescriptorName").attrib['MajorTopicYN'],pmid))"""
	pmid_list.append((pmid,country))
	pmid_content_list.append((pmid,title,abstract))
	job_pmid_list.append((job_num,pmid))
query = "INSERT IGNORE INTO PMID (PMID,COUNTRY ) VALUES (%s,%s)"
curs.executemany(query, pmid_list)
query = "INSERT IGNORE INTO PMID_CONTENT (PMID,TITLE,ABSTRACT ) VALUES (%s,%s, %s)"
curs.executemany(query, pmid_content_list)
query	= "INSERT IGNORE INTO PMID_SUP(S_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
curs.executemany(query, sup_list)
#query	= "INSERT IGNORE INTO PMID_DES(D_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
#curs.executemany(query, des_list)
#query	= "INSERT IGNORE INTO PMID_QUAL(Q_ID, MAJOR,PMID) VALUES	(%s,%s, %s)"
#curs.executemany(query, qual_list)
query = "INSERT IGNORE INTO JOB_PMID (J_ID, PMID) VALUES (%s, %s)"
curs.executemany(query, job_pmid_list)



query = "UPDATE JOB SET DO_PMID_INSERT =3 WHERE J_ID = (%s)"
curs.execute(query,job_num)      
