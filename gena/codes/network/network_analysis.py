import networkx as nx
import sys
import csv
import pymysql
j_id = sys.argv[1]



conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "SELECT NODE_SIZE FROM JOB where J_ID = "+j_id
curs.execute(query)
row = curs.fetchone()
node_size = row['NODE_SIZE']

asso_dir = "/home/hogking/hubmed/backend/files/"+j_id+"/net_asso.txt"
cooc_dir = "/home/hogking/hubmed/backend/files/"+j_id+"/net_cooc.txt"
node_dir = "/home/hogking/hubmed/backend/files/"+j_id+"/node.csv"
csv_F = csv.reader(open(node_dir),delimiter=';')
csv_F_2 = csv.reader(open(node_dir),delimiter=';')

asso=open(asso_dir, 'rb')
G=nx.read_edgelist(asso)
asso.close()

cooc=open(cooc_dir, 'rb')
G_2=nx.read_edgelist(cooc)
cooc.close()

asso_list =[]
cooc_list=[]
node_dict ={}

if len(G) != 0:
	eigenvector = nx.eigenvector_centrality_numpy(G)
	for node in eigenvector:
		asso_list.append((j_id,"6","1",node,str(eigenvector[node])))

	degree = nx.degree_centrality(G)
	for node in degree:
		asso_list.append((j_id,"1","1",node,str(degree[node])))

	betweenness = nx.betweenness_centrality(G)
	for node in betweenness:
		asso_list.append((j_id,"2","1",node,str(betweenness[node])))

	closeness = nx.closeness_centrality(G)
	for node in closeness:
		asso_list.append((j_id,"3","1",node,str(closeness[node])))

	ktaz = nx.katz_centrality_numpy(G)
	for node in ktaz:
		asso_list.append((j_id,"7","1",node,str(ktaz[node])))

	load = nx.load_centrality(G)
	for node in load:
		asso_list.append((j_id,"4","1",node,str(load[node])))

	harmonic = nx.harmonic_centrality(G)
	for node in harmonic:
		asso_list.append((j_id,"5","1",node,str(harmonic[node])))


	for row in csv_F:
		#print (row[0])
		attributes=row[0].split(',')
		node_dict[attributes[0]]=attributes[1]
		G.add_node(attributes[0], weight = float(attributes[1])*float(node_size))

	eigenvector = nx.eigenvector_centrality_numpy(G)
	for node in eigenvector:
		asso_list.append((j_id,"6","2",node,str(eigenvector[node])))

	degree = nx.degree_centrality(G)
	for node in degree:
		asso_list.append((j_id,"1","2",node,str(degree[node])))

	betweenness = nx.betweenness_centrality(G)
	for node in betweenness:
		asso_list.append((j_id,"2","2",node,str(betweenness[node])))

	closeness = nx.closeness_centrality(G)
	for node in closeness:
		asso_list.append((j_id,"3","2",node,str(closeness[node])))

	ktaz = nx.katz_centrality_numpy(G)
	for node in ktaz:
		asso_list.append((j_id,"7","2",node,str(ktaz[node])))

	load = nx.load_centrality(G)
	for node in load:
		asso_list.append((j_id,"4","2",node,str(load[node])))

	harmonic = nx.harmonic_centrality(G)
	for node in harmonic:
		asso_list.append((j_id,"5","2",node,str(harmonic[node])))



if len(G_2) != 0:
	eigenvector = nx.eigenvector_centrality_numpy(G_2)
	for node in eigenvector:
		cooc_list.append((j_id,"6","3",node,str(eigenvector[node])))

	degree = nx.degree_centrality(G_2)
	for node in degree:
		cooc_list.append((j_id,"1","3",node,str(degree[node])))

	betweenness = nx.betweenness_centrality(G_2)
	for node in betweenness:
		cooc_list.append((j_id,"2","3",node,str(betweenness[node])))

	closeness = nx.closeness_centrality(G_2)
	for node in closeness:
		cooc_list.append((j_id,"3","3",node,str(closeness[node])))

	ktaz = nx.katz_centrality_numpy(G_2)
	for node in ktaz:
		cooc_list.append((j_id,"7","3",node,str(ktaz[node])))

	load = nx.load_centrality(G_2)
	for node in load:
		cooc_list.append((j_id,"4","3",node,str(load[node])))

	harmonic = nx.harmonic_centrality(G_2)
	for node in harmonic:
		cooc_list.append((j_id,"5","3",node,str(harmonic[node])))


	for row in csv_F_2:
		#print (row[0])
		attributes=row[0].split(',')
		node_dict[attributes[0]]=attributes[1]
		G_2.add_node(attributes[0], weight = float(attributes[1])*float(node_size))

	eigenvector = nx.eigenvector_centrality_numpy(G_2)
	for node in eigenvector:
		cooc_list.append((j_id,"6","4",node,str(eigenvector[node])))

	degree = nx.degree_centrality(G_2)
	for node in degree:
		cooc_list.append((j_id,"1","4",node,str(degree[node])))

	betweenness = nx.betweenness_centrality(G_2)
	for node in betweenness:
		cooc_list.append((j_id,"2","4",node,str(betweenness[node])))

	closeness = nx.closeness_centrality(G_2)
	for node in closeness:
		cooc_list.append((j_id,"3","4",node,str(closeness[node])))

	ktaz = nx.katz_centrality_numpy(G_2)
	for node in ktaz:
		cooc_list.append((j_id,"7","4",node,str(ktaz[node])))

	load = nx.load_centrality(G_2)
	for node in load:
		cooc_list.append((j_id,"4","4",node,str(load[node])))

	harmonic = nx.harmonic_centrality(G_2)
	for node in harmonic:
		cooc_list.append((j_id,"5","4",node,str(harmonic[node])))

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "INSERT IGNORE INTO JOB_GENE (J_ID, R_ID,NET_ID,HGNC_ID,SCORE) VALUES (%s, %s,%s, %s,%s)"
curs.executemany(query, asso_list)

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "INSERT IGNORE INTO JOB_GENE (J_ID, R_ID,NET_ID,HGNC_ID,SCORE) VALUES (%s, %s,%s, %s,%s)"
curs.executemany(query, cooc_list)

curs.close()
conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)
query = "UPDATE JOB SET NETWORK = 4 WHERE J_ID = (%s)"
curs.execute(query,j_id)