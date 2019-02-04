##################################################################
#### This file is a re-write of govtInterest_v1.0.pl
#### input files: merged csvs, NER, omitLocs
#### output files: "NER_output.txt","distinctOrgs.txt", "distinctLocs.txt"
##################################################################

# run under pl_rewrite virtualenv
# python G:\PatentsView\cssip\PatentsView-DB\Development\government_interest\govtinterest_v2.0.py

import pandas as pd
import sys
import math
import subprocess
import os
from os import listdir
import re 
from itertools import chain 
import string 


# Requires: omitLocs.csv filepath
# Modifies: nothing
# Effects: read in omitLocs file and return with data dict  
def read_omitLocs(fp):
	print("Reading in omitLocs.csv from: " + fp)
	try: 
		omitLocs_df = pd.read_csv(fp + 'omitLocs.csv')
	except:
		print("Error reading in file, check specified filepath")
	
	test_dataframe(omitLocs_df, 482, 3)
	
	omitted_data = {}

	for index, row in omitLocs_df.iterrows():
		if row['Omit'] == 1:
			omitted_data[row['Location']] = row['Omit']

	return  omitLocs_df, omitted_data

# Requires: mergedcsvs.csv filepath
# Modifies: nothing
# Effects: read in mergedcsvs file, return data dict. + dataframe
def read_mergedCSV(fp):
	print("Reading in mergedcsvs.csv from: " + fp + 'mergedcsvs.csv')
	try: 
		merged_df = pd.read_csv(fp, header=None, encoding="ISO-8859-1") #+ 'mergedcsvs.csv')
	except:
		print("Error reading in file, check specified filepath")
	
	test_dataframe(merged_df, 6127, 4)
	
	# set column headers
	merged_df.columns = ['patent_num','twin_arch','gi_title', 'gi_stmt' ]
	
	#merged_df.to_csv("G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/output/test.csv")

			
	return merged_df 

# Requires: NER DataBase filepath, input
# Modifies: nothing
# Effects: Run NER
def run_NER(fp, merged_df, classif, classif_dirs ):
	
	patents = merged_df['patent_num'].tolist()
	
	# add acronym cleanup func later - doesn't appear to need replacement
	
	gi_stmt_full = merged_df['gi_stmt'].tolist()
	nerfc = 5000

	num_files = int(math.ceil(len(gi_stmt_full) / nerfc))
	input_files = []
	
	fp = fp + 'stanford-ner-2017-06-09/'
	os.chdir(fp)
	print("Current working directory: " + os.getcwd())
	# Note: Rewrite - support more than 2 files?
	text1 = ''
	text2 = ''
	for num in range(0,num_files):
		
		with open(fp + 'in/' + str(num) + '_test2.txt', 'w', encoding='utf-8') as f:
		 	if(num == 0):
		 		gi_stmt_str = '\n'.join(gi_stmt_full[0:nerfc])
		 		text1 = gi_stmt_str
		 	else:
		 		gi_stmt_str = '\n'.join(gi_stmt_full[nerfc:len(gi_stmt_full)])
		 		text2 = gi_stmt_str
		 	f.write(gi_stmt_str)
		 	input_files.append(str(num) + '_test2.txt')
	
	# run java call for NER
	for cf in range(0,len(classif)):
		for f in input_files:	    
			cmd_pt1 = 'java -mx500m -classpath stanford-ner.jar;lib/* edu.stanford.nlp.ie.crf.CRFClassifier'
			cmd_pt2 = '-loadClassifier ' + './' + classif[cf]
			cmd_pt3 = '-textFile ./in/' + f + ' -outputFormat inlineXML 2>> error.log'
			cmd_full = cmd_pt1 + ' ' + cmd_pt2 + ' ' + cmd_pt3
			cmdline_params = cmd_full.split()
			print(cmdline_params)
			with open("./out/" + classif_dirs[cf] + f, "w") as xml_out: 

				subprocess.run(cmdline_params, stdout=xml_out)
			
	return


# Requires: data dict
# Modifies: nothing
# Effects: Process NER on data dict from merged_csvs
# Check if uniq function definition is needed
def process_NER(fp):
	os.chdir(fp + 'stanford-ner-2017-06-09/out/')
	print(os.getcwd())
	ner_output = listdir(os.getcwd())
	print(ner_output)
	orgs_full_list = []
	locs_full_list = []
	for f in ner_output:
		with open(f, "r") as output:
			content = output.readlines()
	    	orgs_full_list, locs_full_list = parse_xml_ner(orgs_full_list, locs_full_list, content)	

	# flatten list of lists 
	print(len(orgs_full_list))
	print(len(locs_full_list))

	flat_orgs = [y for x in orgs_full_list for y in x]
	flat_locs = [y for x in locs_full_list for y in x]
	
	orgs_final = set(flat_orgs)
	locs_final = set(flat_locs)
	print(len(orgs_final))
	print(len(locs_final))
	
	output_path = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/"
	
	with open(output_path + "/test_output/orgs.txt", "w") as p:
		for item in orgs_final:
			p.write(str(item) + "\n")

	with open(output_path + "/test_output/locs.txt", "w") as p:
		for item in locs_final:
			p.write(str(item) + "\n")

	return


# Requires: data dict
# Modifies: nothing
# Effects: Writes 3 output files: 
def write_output(data):
	return

#--------Helper Functions-------#

# Requires: data dict
# Modifies: nothing
# Effects: checks email validity & phone #s, also performs string processing
# (check on trimWord())
def parse_contact_info():
	return

# Requires: data dict
# Modifies: nothing
# Effects: parses XML file for orgs, locs, has_location fields
def parse_xml_ner(orgs_full, locs_full, content):
	# done, move code here 
	for line in content: 
				orgs = re.findall("<ORGANIZATION>[^<]+</ORGANIZATION>", line)
				orgs_clean = [re.sub("<ORGANIZATION>", "", x) for x in orgs]
				orgs_clean = [re.sub("</ORGANIZATION>", "", x) for x in orgs_clean]
				
				locs = re.findall("<LOCATION>[^<]+</LOCATION>", line)
				locs_clean = [re.sub("<LOCATION>", "", x) for x in locs]
				locs_clean = [re.sub("</LOCATION>", "", x) for x in locs_clean]
				
				orgs_full.append(orgs_clean)
				locs_full.append(locs_clean)

	return orgs_full, locs_full


# Requires: data dict
# Modifies: nothing
# Effects: clean giStatement field for certain contract #s
# Note: look at this again, right now Bethesda & SD related only
def cleanContracts(data):
	#print(data.columns.get_values())
	gi_statements = data['gi_stmt'].tolist()
	contracts = []
	# STEP 1. Public Law - Don't need contract awards - make thesenempty
	# all law gi statements have "Public Law" in them
	contract_nums = data['gi_stmt'].str.contains("Public Law")
	
	# get index of law ones
	law_stmts = contract_nums[contract_nums].index
	law_stmts = law_stmts.tolist()

	for law in law_stmts:
		gi_statements[law] = ""

	for gi in gi_statements:
	# STEP 2. Extract contract awards
	############################# Expression 1
	# [A-Za-z\d] start with alphanumeric char
	# [A-Za-z\d-] 2nd char alphanumeric or - 
	# [^\s] no spacing
	# [\d] at least one more digit 
	# [A-Za-z\d-]+  finish with alphanumeric char or - 1 or more times
	############################ Expression 2 
	# [A-Z\d]{1,3} - alphanumeric 1-3 times, capital A-Z only
	# \s single space
	# [A-Z\d-]+\d - alphanumeric or - 1 or more times, followed by digit (redundant but stops
	# expression for case like "IN AGREEMENT")

		contract_nums = re.findall("[A-Za-z\d][A-Za-z\d-]+[^\s][\d][A-Za-z\d-]+|[A-Z\d]{1,3}\s[A-Z\d-]+\d", gi)
		
		contract_nums = '|'.join(contract_nums)
		contracts.append(contract_nums)
	# create single column for contract award numbers
	merged_df['contracts'] = pd.Series(contracts)
	# print(len(gi_statements))
	# print(len(contracts))


	print(merged_df.head())
	output_path = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/"
	with open(output_path + "/test_output/merged_df.txt", "w") as p:
		

	return 



#--------Test Function-------#
def test_dataframe(df, rw, col):
	if df.shape[0] != rw:
		print('Incorrect # of rows')
	elif df.shape[1] != col:
		print('Incorrect # of cols')
	else:
		print('pass')

	return 


if __name__ == '__main__':

	print("Hello World")

	# set up vars + directories
	omitLocs_dir = 'D:/DataBaseUpdate/2018_Nov/contract_award_patch/'
	merged_dir = 'D:/DataBaseUpdate/2018_Nov/contract_award_patch/merged_csvs.csv'
	ner_dir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/"
	
	classifiers = ['classifiers/english.all.3class.distsim.crf.ser.gz', 'classifiers/english.conll.4class.distsim.crf.ser.gz', 'classifiers/english.muc.7class.distsim.crf.ser.gz']
	ner_classif_dirs = ['out-3class', 'out-4class', 'out-7class']

	omitLocs_df, omitLocs = read_omitLocs(omitLocs_dir)
	merged_df = read_mergedCSV(merged_dir)


	#run_NER(ner_dir, merged_df, classifiers, ner_classif_dirs)
	
	#process_NER(ner_dir)
	cleanContracts(merged_df)
	
	# next steps 
	# Check on email, phone numbers, special california considerations




# CA152813 and HL107153
#  This invention was made with government support under grant nos. CA152813 and HL107153 awarded by the NIH. The government has certain rights in the invention.

# Federally-Sponsored Research and Development The United States 
#Government has ownership rights in this invention. 
#Licensing inquiries may be directed to Office of Research 
#and Technical Applications, Space and Naval Warfare Systems 
#Center, Pacific, Code 72120, San Diego, Calif., 92152; 
#telephone (619)553-5118; email: ssc_pac_t2@navy.mil. 
#Reference Navy Case No. 103081.

#5023






