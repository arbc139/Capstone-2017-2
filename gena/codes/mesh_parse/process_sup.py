import re
import pymysql

conn = pymysql.connect(autocommit ='True', host='localhost', user='hogking', password='',db='HUBMED', charset='utf8') 
curs = conn.cursor(pymysql.cursors.DictCursor)

protein = 'protein, .+'
microrna = ' microrna, .+'
antigen = ' antigen, .+'
peptide = ' peptide, .+'
non_coding = ' non-coding RNA, .+'
noncoding = ' noncoding RNA, .+'
long_non_coding = ' long non-coding RNA, .+'
long_noncoding = ' long noncoding RNA, .+'
rna = ' RNA, .+'
comma = ', .+'
protein_reg = re.compile(protein, re.IGNORECASE)
microrna_reg = re.compile(microrna, re.IGNORECASE) 
antigen_reg = re.compile(antigen, re.IGNORECASE) 
peptide_reg = re.compile(peptide, re.IGNORECASE) 
non_coding_reg = re.compile(non_coding, re.IGNORECASE) 
noncoding_reg = re.compile(noncoding, re.IGNORECASE) 
long_non_coding_reg = re.compile(long_non_coding, re.IGNORECASE) 
long_noncoding_reg = re.compile(long_noncoding, re.IGNORECASE) 
rna_reg = re.compile(rna, re.IGNORECASE) 
comma_reg = re.compile(comma, re.IGNORECASE) 
query = "SELECT S_ID, S_NAME FROM SUP"
curs.execute(query)
rows = curs.fetchall()
for row in rows:
	target = row['S_NAME']
	s_id = row['S_ID']
	temp = target
	target = protein_reg.sub('', target)
	target = microrna_reg.sub('', target)
	target = antigen_reg.sub('', target)
	target = peptide_reg.sub('', target)
	target = non_coding_reg.sub('', target)
	target = noncoding_reg.sub('', target)
	target = long_non_coding_reg.sub('', target)
	target = long_noncoding_reg.sub('', target)
	target = rna_reg.sub('', target)
	target = comma_reg.sub('', target)
	query = "UPDATE SUP SET PROCESSED =(%s) WHERE S_ID = (%s)"
	curs.execute(query,(target, s_id))