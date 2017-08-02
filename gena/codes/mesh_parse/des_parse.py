from lxml import etree
import pymysql
import re
filename = "/home/hogking/hubmed/backend/meshfile/desc2017.xml"
context = etree.iterparse(filename, events=('end',), tag='DescriptorRecord')

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

human_reg = re.compile(', [h|H]uman')

for event, element in context:
	ui_num = element.find("DescriptorUI").text
	#print (ui_num)
	ui_name = element.find("DescriptorName")
	ui_string = ui_name.find("String").text
	
	query = "INSERT INTO DES (D_ID, D_NAME) VALUES (%s, %s)"
	curs.execute(query, (ui_num, ui_string))

	element.clear()
	#This line tells that you won't be accessing any child elements of the element now. So the parser can just throw them off.


	#Now clearing the parent elements of the 'element'
	while element.getprevious() is not None:
    		del element.getparent()[0]
	# 'not None' is used here because if the element you are parsing is root itself, then it will raise an exception because there is no parent for it, so you might have to handle that exception too in that case.