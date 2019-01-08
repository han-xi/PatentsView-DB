import pandas as pd
import sys
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

# Requires: NER DataBase filepath
# Modifies: nothing
# Effects: Process NER data
def do_NER(fp, merged_df):
	# loop through patents (6127)
	patents = merged_df['patent_num'].tolist()
	print(len(patents))
	#print(patents[0:5])
	# add acronym cleanup func later - doesn't appear to need replacement
	#merged_df['gi_stmt'].str.replace('')
	gi_stmt_full = merged_df['gi_stmt'].tolist()
	# take gi_stmts, all together - 5000 split?
	print(len(gi_stmt_full))
	nerfc = 5000
	num_files = int(len(gi_stmt_full) / nerfc)
	idx = 0
	for num in range(0,num_files):
		 with open(fp + 'in/' + str(num) + 'test.txt', 'w', encoding='utf-8') as f:
		 	if(num == 1):
		 		gi_stmt_str = '\n'.join(gi_stmt_full[0:nerfc])
		 		idx = 5000
		 
		 	else:
		 		gi_stmt_str = '\n'.join(gi_stmt_full[idx + 1:len(gi_stmt_full)])
		 
		 	f.write(gi_stmt_str)
		 	f.close()


	#for gi in gi_stmt_full:


	return


# Requires: data dict
# Modifies: nothing
# Effects: Process NER on data dict from merged_csvs
# Check if uniq function definition is needed
def process_data_NER(data):

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

	# declare filepaths
	omitLocs_dir = "D:/DataBaseUpdate/2018_Nov/contract_award_patch/"
	merged_dir = "D:/DataBaseUpdate/2018_Nov/contract_award_patch/"
	ner_dir = "G:/PatentsView/cssip/PatentsView-DB/Development/government_interest/NER/"
	omitLocs_df, omitLocs = read_omitLocs(omitLocs_dir)

	merged_df = read_mergedCSV(merged_dir)

	do_NER(ner_dir, merged_df)

	# Check on xml, email validity, java calls for NER extraction

	# General Function Flow:
	#read_omitLocs
	#read_mergedCSV
	#do_NER
	#process_data_NER
	#### helper funcs
	#parse_contact_info
	#parse_xml_ner
	#cleanContracts
	####
	#write_output


