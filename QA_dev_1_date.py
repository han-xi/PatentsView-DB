#Date

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
data = pd.read_excel("{}/1_latest_date_check.xlsx".format(previous_qa_loc))
#print (data.head())

def check_latest_date(newdb, last_date):
    date_dict = {}
    found_date = engine.execute("select max(date) from {0}.patent;".format(new_database))
    if str(found_date) != str(latest_expected_date):
        date_error =  "The latest date is {0}, and it should be {1}".format(str(found_date), str(latest_expected_date))
        date_dict.update({new_database:date_error})
    else:
        date_match = "date matches"
        date_dict.update({new_database:date_match})
    return date_dict
def date_check_to_excel(previous_qa_loc, date_dict, new_qa_loc):
    df = pd.read_excel('{}/1_latest_date_check.xlsx'.format(previous_qa_loc))
    df_2 = pd.DataFrame.from_dict(date_dict, orient = 'index')
    df_2.reset_index(inplace=True)
    df_2.rename(columns={'index':'database', 0:'latest date check'}, inplace=True)
    df_3 = df.append(df_2)
    df_3.to_excel('{}/1_latest_date_check.xlsx'.format(new_qa_loc), engine='xlsxwriter', index=False)
    
date_dict = check_latest_date(new_database, latest_expected_date)
date_check_to_excel(previous_qa_loc, date_dict, new_qa_loc)
