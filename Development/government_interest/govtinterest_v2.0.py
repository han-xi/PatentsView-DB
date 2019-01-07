import pandas as pd

##################################################################
#### This file is a re-write of govtInterest_v1.0.pl
#### input files: merged csvs, NER, omitLocs
#### output files: "NER_output.txt","distinctOrgs.txt", "distinctLocs.txt"
##################################################################

# Requires: omitLocs.csv filepath
# Modifies: nothing
# Effects: read in omitLocs file as pd dataframe 
def read_omitLocs(omit_fp):

	return 

# Requires: mergedcsvs.csv filepath
# Modifies: nothing
# Effects: read in mergedcsvs file, return data dict. + dataframe
# Data dict. - key = patent #, value = twinArch, giTitle, giStatement
# Consider 1 vs. multiple? 
def read_mergedCSV(merged_fp):

	return 

# Requires: NER DataBase filepath
# Modifies: nothing
# Effects: Process NER data
def do_NER(ner_fp):
	
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
def parse_xml_ner:
	return


# Requires: data dict
# Modifies: nothing
# Effects: clean giStatement field for certain contract #s
# Note: look at this again, right now Bethesda & SD related only
def cleanContracts():
	return 

if __name__ == '__main__':

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


