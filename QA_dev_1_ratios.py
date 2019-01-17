#Ratio of Total to Distinct IDs

#set up sql alchemy engine
import pandas as pd
import os
import configparser
import sys
sys.path.append('/project/Development')
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('/project/Development/config.ini')
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']
previous_qa_loc = config['FOLDERS']['OLD_QA_LOC']
new_qa_loc = config['FOLDERS']['OLD_QA_LOC']
latest_expected_date = config['CONSTANTS']['LATEST_DATE']

engine = general_helpers.connect_to_db(host, username, password, new_database)
data = pd.read_excel("{}/1_distinct_to_total.xlsx".format(previous_qa_loc))
#print (data.head())

def create_ratio_description(old_db_ratio, new_db_ratio, max_dif = .05 ):
    if abs(old_db_ratio - new_db_ratio)/float(old_db_ratio) > max_dif:
        return "Problem: The ratio of distinct to total ids are very different from last year "
    else:
        return "No Problem"
def get_ratios(previous_qa_loc, new_qa_loc, new_database):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    engine.execute('use {}'.format(new_database))
    new_ratios = []
    table_col = zip(ratios['Table'], ratios['Column'])
    for table, col in table_col:
        query = "select count({0}), count(distinct {0}) from {1}.{2}".format(col, new_database, table)
        counts = engine.execute(query)
        for row in counts:
            new_ratios.append(row[0]/float(row[1]))
    return new_ratios

def write_distinct_excel(new_ratios, previous_qa_loc, new_qa_loc, new_database):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    ratios[new_database] = new_ratios
    del ratios['Description']
    #the last row of the table is now the most recent previous database!
    ratios['Description'] = ratios.apply(lambda row: create_ratio_description(row[ratios.columns[-4]], row[new_database]), axis=1)
    ratios.to_excel('{}/1_distinct_to_total.xlsx'.format(new_qa_loc), index = False)
    
ratios = get_ratios(previous_qa_loc, new_qa_loc, new_database)
write_distinct_excel(ratios, previous_qa_loc, new_qa_loc, new_database)
