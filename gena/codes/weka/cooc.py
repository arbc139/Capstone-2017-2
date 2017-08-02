import sys
import pymysql
j_id = sys.argv[1]



conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

query = "SELECT D.HGNC_ID, B.PMID FROM JOB_PMID A, PMID_SUP B, SUP C, GENE D WHERE A.J_ID = "+j_id+" AND A.PMID=B.PMID AND B.S_ID = C.S_ID AND C.HGNC_ID = D.HGNC_ID AND D.HGNC_ID !='N/A' GROUP BY D.HGNC_ID, B.PMID ORDER BY PMID;"
curs.execute(query)
rows = curs.fetchall()
relation = {}
node = {}
syms = []
temp_pmid=rows[0]['PMID']
#print (temp_pmid)
print(rows)

for row in rows:
	if temp_pmid == row['PMID']:
		syms.append(row['HGNC_ID'])
	else:
		if len(syms) == 1:
			if syms[0] in node :
				node[syms[0]] = node[syms[0]]+1
			else :
				node[syms[0]] = 1
		else :
			syms.sort()
			for i in range(0,len(syms)):
				for j in range(i+1, len(syms)):
					if (syms[i],syms[j]) in relation:
						relation[(syms[i],syms[j])] = relation[(syms[i],syms[j])] + 1
					else :
						relation[(syms[i],syms[j])] = 1
		del syms[:]
		syms.append(row['HGNC_ID'])
		temp_pmid = row['PMID']
if len(syms) > 1:
	syms.sort()
	for i in range(0,len(syms)):
		for j in range(i+1, len(syms)):
			if (syms[i],syms[j]) in relation:
				relation[(syms[i],syms[j])] = relation[(syms[i],syms[j])] + 1
			else :
				relation[(syms[i],syms[j])] = 1
if len(syms) == 1:
	if syms[0] in node :
		node[syms[0]] = node[syms[0]]+1
	else :
		node[syms[0]] = 1
print(relation)
print(node)
if len(relation) != 0:
	max_relation = float(max(relation.values()))
	min_relation = float(min(relation.values()))
if len(node) != 0:
	max_node = float(max(node.values()))
	min_node = float(min(node.values()))


filename = "/home/hogking/hubmed/backend/files/"+j_id+"/net_cooc.txt"
filename_2 = "/home/hogking/hubmed/backend/files/"+j_id+"/node.csv"
cooc = open(filename,'w')
node_out = open(filename_2,'w')
cooc_counter = 0
node_counter = 0
for elm in relation:
	#print(elm, relation[elm])
	if max_relation == min_relation:
		score = str(max_relation/len(relation))
	else:
		score = str((float(relation[elm])-min_relation)/(max_relation-min_relation))
	#print(elm[0]+" "+elm[1]+" {'weight' : " +score+"}\n")
	cooc_counter = cooc_counter+1
	cooc.write(elm[0]+" "+elm[1]+" {'weight' : " +score+"}\n")

for nod in node:
	#print(nod, node[nod])
	#print((float(node[nod])-min_node)/(max_node-min_node))
	if min_node == max_node:
		score = str(max_node/len(node))
	else:
		score = str((float(node[nod])-min_node)/(max_node-min_node))
	node_counter = node_counter + 1
	node_out.write(nod+","+score+"\n")

query = "UPDATE JOB SET EDGE_NUM_COOC = (%s) WHERE J_ID = (%s) ;"
curs.execute(query ,(cooc_counter,j_id	))
query = "UPDATE JOB SET SINGLE_OCCUR_NODE = (%s) WHERE J_ID = (%s) ;"
curs.execute(query ,(node_counter,j_id	))
conn.close()	


