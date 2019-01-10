*config file
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

*folder locations and database

new_qa_loc='/user/local/airflow/PatentsView-DB/Development/QA'
temp_upload = 'november_upload'

*Counts function

tables=['application',	'botanic',	'brf_sum_text',	'claim','detail_desc_text',	'draw_desc_text',	'figures',	'foreign_priority',	'foreigncitation',	'government_interest',	'ipcr',	'mainclass',	'non_inventor_applicant',	'otherreference',	'patent',	'pct_data',	'rawassignee',	'rawexaminer',	'rawinventor',	'rawlawyer',	'rawlocation',	'rel_app_text',	'subclass',	'us_term_of_grant',	'usapplicationcitation',	'uspatentcitation',	'uspc',		'usreldoc']
new_counts = []
description= []

def temp_upload_count (temp_upload, new_qa_loc):
    cursor = connect()
    cursor.execute('use {}'.format(temp_upload))
    for table in tables:
        cursor.execute('select count(*) from {}'.format(table))
        count = cursor.fetchall()[0][0]
        new_counts.append(count)
    for count in new_counts:
        if count == 0:
            description.append("Problem: Empty Table")
        else:
            description.append("No Problem!")

    df=pd.DataFrame({'Table': tables, 'Count':new_counts, 'Description':description})
    df_temp_upload=df[['Table', 'Count', 'Description']]
    
    writer = pd.ExcelWriter('{0}/1_table_counts_temp_upload.xlsx'.format(new_qa_loc), engine='xlsxwriter')
    df_temp_upload.to_excel(writer, index = False)
    writer.save()

temp_upload_count (temp_upload, new_qa_loc)
