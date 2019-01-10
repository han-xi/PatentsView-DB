#config file

import os
import configparser
import sys
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
sys.path.append('/usr/local/airflow/PatentsView-DB/Development')
from helpers import general_helpers
import configparser
config = configparser.ConfigParser()
config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')
host = config['DATABASE']['HOST']
username = config['DATABASE']['USERNAME']
password = config['DATABASE']['PASSWORD']
new_database = config['DATABASE']['NEW_DB']
old_database = config['DATABASE']['OLD_DB']
temporary_upload = config['DATABASE']['TEMP_UPLOAD_DB']

engine = general_helpers.connect_to_db(host, username, password, new_database)

#locations and db

previous_qa_loc = "H:/share/Science Policy Portfolio/PatentsView IV/QA/November_2018_Update_QA/Previous_QA_Results"
new_qa_loc = 'H:/share/Science Policy Portfolio/PatentsView IV/QA/November_2018_Update_QA/QA_Results'
new_db = 'patent_20181127'
latest_expected_data = '2018-11-27'

#counts functions

def create_ratio_description(old_db_ratio, new_db_ratio, max_dif = .05 ):
    if abs(old_db_ratio - new_db_ratio)/float(old_db_ratio) > max_dif:
        return "Problem: The ratio of distinct to total ids are very different from last year "
    else:
        return "No Problem"
def get_ratios(previous_qa_loc, new_qa_loc, new_db):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    engine.execute('use {}'.format(new_db))
    new_ratios = []
    table_col = zip(ratios['Table'], ratios['Column'])
    for table, col in table_col:
        query = "select count({0}), count(distinct {0}) from {1}.{2}".format(col, new_db, table)
        counts = engine.execute(query)
        new_ratios.append(counts[0]/float(counts[1]))
    return new_ratios

def write_distinct_excel(new_ratios, previous_qa_loc, new_qa_loc, new_db):
    ratios = pd.read_excel('{}/1_distinct_to_total.xlsx'.format(previous_qa_loc))
    ratios[new_db] = new_ratios
    del ratios['Description']
    #the last row of the table is now the most recent previous database!
    ratios['Description'] = ratios.apply(lambda row: create_ratio_description(row[ratios.columns[-4]], row[new_db]), axis=1)
    ratios.to_excel('{}/1_distinct_to_total.xlsx'.format(new_qa_loc), index = False)

ratios = get_ratios(previous_qa_loc, new_qa_loc, new_db)
write_distinct_excel(ratios, previous_qa_loc, new_qa_loc, new_db)
