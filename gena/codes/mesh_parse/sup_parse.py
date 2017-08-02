from lxml import etree
import pymysql
import re
filename = "/home/hogking/hubmed/backend/meshfile/supp2017.xml"
context = etree.iterparse(filename, events=('end',), tag='SupplementalRecord')

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

human_reg = re.compile(', [h|H]uman')

for event, element in context:
	ui_num = element.find("SupplementalRecordUI").text
	#print (ui_num)
	ui_name = element.find("SupplementalRecordName")
	ui_string = ui_name.find("String").text
	#print(ui_string)
	human_ui = human_reg.findall(ui_string)
	#print (human_ui)
	if human_ui:
		is_human = "1"
	else :
		is_human = "0"
	query = "INSERT INTO SUP (S_ID, S_NAME, IS_HUMAN,HGNC_ID) VALUES (%s, %s, %s,%s)"
	curs.execute(query, (ui_num, ui_string,is_human,"N/A"))

	concept_list = element.find("ConceptList")
	for concept in concept_list:
		if (concept.attrib['PreferredConceptYN']) == 'N':
			concept_ui_num = concept.find("ConceptUI").text
			concept_ui_name = concept.find("ConceptName")
			concept_ui_string = concept_ui_name.find("String").text
			#print(ui_string)
			query = "INSERT INTO NICKNAME (N_ID, N_NAME, S_ID) VALUES (%s, %s,%s)"
			curs.execute(query, (concept_ui_num, concept_ui_string,ui_num))

	element.clear()
	#This line tells that you won't be accessing any child elements of the element now. So the parser can just throw them off.


	#Now clearing the parent elements of the 'element'
	while element.getprevious() is not None:
    		del element.getparent()[0]
	# 'not None' is used here because if the element you are parsing is root itself, then it will raise an exception because there is no parent for it, so you might have to handle that exception too in that case.