
#config

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

#date check functions

def check_latest_date(newdb, last_date):
    date_dict = {}
    found_date =  engine.execute("select max(date) from {0}.patent;".format(newdb))
    if str(found_date) != str(last_date):
        date_error =  "The latest date is {0}, and it should be {1}".format(str(found_date), str(last_date))
        date_dict.update({newdb:date_error})
    else:
        date_match = "date matches"
        date_dict.update({newdb:date_match})
    return date_dict
def date_check_to_excel(previous_qa_loc, newdb_date_dict, new_qa_loc):
    df = pd.read_excel('{}/1_latest_date_check.xlsx'.format(previous_qa_loc))
    df_2 = pd.DataFrame.from_dict(newdb_date_dict, orient = 'index')
    df_2.reset_index(inplace=True)
    df_2.rename(columns={'index':'database', 0:'latest date check'}, inplace=True)
    df_3 = df.append(df_2)
    df_3.to_excel('{}/1_latest_date_check.xlsx'.format(new_qa_loc), engine='xlsxwriter', index=False)

date_dict = check_latest_date(new_db, latest_expected_data)
date_check_to_excel(previous_qa_loc, date_dict, new_qa_loc)


