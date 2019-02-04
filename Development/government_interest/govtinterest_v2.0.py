import pandas as pd
import sys
import math
import subprocess
import os
#from nltk.tag import StanfordNERTagger
#from nltk.tokenize import word_tokenize, sent_tokenize
#from sner import NER 
# run under pl_rewrite virtualenv
# python G:\PatentsView\cssip\PatentsView-DB\Development\government_interest\govtinterest_v2.0.py
# Python version 3.6
##################################################################
#### This file is a re-write of govtInterest_v1.0.pl
#### input files: merged csvs, NER, omitLocs
#### output files: "NER_output.txt","distinctOrgs.txt", "distinctLocs.txt"
##################################################################

# Requires: omitLocs.csv filepath
# Modifies: nothing
# Effects: read in omitLocs file and return with data dict  
def read_omitLocs(fp):
	#print("Now reading in omitLocs.csv from: " + fp)
	try: 
		omitLocs_df = pd.read_csv(fp + 'omitLocs.csv')
	except:
		print("Error reading in file, check specified filepath")
	
	#test_dataframe(omitLocs_df, 482, 3)
	
	omitted_data = {}

	for index, row in omitLocs_df.iterrows():
		if row['Omit'] == 1:
			omitted_data[row['Location']] = row['Omit']
	
	#print(omitted_data)

	return  omitLocs_df, omitted_data

# Requires: mergedcsvs.csv filepath
# Modifies: nothing
# Effects: read in mergedcsvs file, return data dict. + dataframe
# Data dict. - key = patent #, value = twinArch, giTitle, giStatement
# Consider 1 vs. multiple? 
def read_mergedCSV(fp):
	print("Now reading in mergedcsvs.csv from: " + fp + 'mergedcsvs.csv')
	try: 
		fp = 'D:/DataBaseUpdate/2018_Nov/contract_award_patch/merged_csvs.csv'
		merged_df = pd.read_csv(fp, header=None, encoding="ISO-8859-1") #+ 'mergedcsvs.csv')
	except:
		print("Error reading in file, check specified filepath")
	
	#test_dataframe(merged_df, 6127, 4)
	# set column headers
	merged_df.columns = ['patent_num','twin_arch','gi_title', 'gi_stmt' ]
	#print(merged_df.head(n=1))
	#merged_df.to_csv("G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/output/test.csv")

	#twin_arch = {}
	#gi_title = {}
	#gi_stmt = {}
	
	#for index, row in merged_df.iterrows():
	#	twin_arch[row['patent_num']] = row['twin_arch']
	#	gi_title[row['patent_num']] = row['gi_title']
	#	gi_stmt[row['patent_num']] = row['gi_stmt']
	
			
	return merged_df 

# Requires: NER DataBase filepath, input
# Modifies: nothing
# Effects: Run NER
def run_NER(fp, merged_df, classif, classif_dirs ):
	# loop through patents (6127)
	patents = merged_df['patent_num'].tolist()
	#print(len(patents))
	
	#print(patents[0:5])
	# add acronym cleanup func later - doesn't appear to need replacement
	#merged_df['gi_stmt'].str.replace()
	gi_stmt_full = merged_df['gi_stmt'].tolist()
	nerfc = 5000


	num_files = int(math.ceil(len(gi_stmt_full) / nerfc))
	input_files = []
	#print(str(num_files) + ' is # of files')
	fp = fp + 'stanford-ner-2017-06-09/'
	os.chdir(fp)
	print("Current working directory: " + os.getcwd())
	# Note: Rewrite - support more than 2 files...
	text1 = ''
	text2 = ''
	for num in range(0,num_files):
		#print("now running file " + str(num))
		with open(fp + 'in/' + str(num) + '_test2.txt', 'w', encoding='utf-8') as f:
		 	if(num == 0):
		 		gi_stmt_str = '\n'.join(gi_stmt_full[0:nerfc])
		 		text1 = gi_stmt_str
		 	else:
		 		gi_stmt_str = '\n'.join(gi_stmt_full[nerfc:len(gi_stmt_full)])
		 		text2 = gi_stmt_str
		 	f.write(gi_stmt_str)
		 	input_files.append(str(num) + '_test2.txt')
	

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
def process_NER(data):

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
def parse_xml_ner():
	return


# Requires: data dict
# Modifies: nothing
# Effects: clean giStatement field for certain contract #s
# Note: look at this again, right now Bethesda & SD related only
def cleanContracts():
	return 



#--------Test Functions-------#
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
	omitLocs_dir = "D:/DataBaseUpdate/2018_Nov/contract_award_patch/"
	merged_dir = "D:/DataBaseUpdate/2018_Nov/contract_award_patch/"
	ner_dir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/"
	
	classifiers = ['classifiers/english.all.3class.distsim.crf.ser.gz', 'classifiers/english.conll.4class.distsim.crf.ser.gz', 'classifiers/english.muc.7class.distsim.crf.ser.gz']
	ner_classif_dirs = ['out-3class', 'out-4class', 'out-7class']

	
	omitLocs_df, omitLocs = read_omitLocs(omitLocs_dir)
	merged_df = read_mergedCSV(merged_dir)
	run_NER(ner_dir, merged_df, classifiers, ner_classif_dirs)

	# Check on xml, email validity, java calls for NER extraction (subprocess)

	# General Function Flow:
	#read_omitLocs - done
	#read_mergedCSV - done
	#run_NER - done 
	#process_NER
	#### helper funcs
	#parse_contact_info
	#parse_xml_ner
	#cleanContracts
	####
	#write_output

# doNER:

# loop through all patents - take corresponding giStatement
# acronym cleaning (may not need this first glance)
# NERFC - specifices # o flines to feed NER / java call
# if nersIN > 5000 or i = # of patent keys
# open a new file, write to it entire array, split by /n
# do next iteration - write the rest 

# run java call on NER data - input txt file, 5000 lines/call
# each line for patent

# NER output - flagged organization + location 


# process():
# giStatement field -look at all contract/award #s from giStatement field
#split lines by space, trim punc, loop through all words
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





# email code line 410...
# clean Contracts .. 
# questions for sarah - do we need emails?
# clarify those steps

#NER processing ----> adds organiz. and loc.
# use to create distinct orgs, distinct loc. output

