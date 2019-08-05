import MySQLdb
import os
import csv
import sys
import pandas as pd
#sys.path.append('/usr/local/airflow/PatentsView-DB/Scripts/Website_Database_Generator/database/')

from helpers import general_helpers


# TODO: General items to do, separate into functions?
# 1. Truncate and load to PatentsView_Detail_Description db
# 2. Trigger to upload data to solr  - will need data-config.xml...
# 3. figure out way to monitor upload progress?
# 4. export file + one more step /



# 1. Truncate and Load to PatentsView_Detail_Description db
import configparser
config = configparser.ConfigParser()
config.read('/usr/local/airflow/PatentsView-DB/Development/config.ini')
db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])disambig_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'disambig_out')

# detail_desc_text_db is a fixed value. the db name will not change
detail_desc_text_db = 'PatentsView_Detail_Description'
new_db = config['DATABASE']['NEW_DB']
new_update_date = new_db[-8:]

# truncate patent_detail_desc_text table in PatentsView_Detail_Description
db_con.execute('truncate table {}.patent_detail_desc_text;'.format(detail_desc_text_db))

# load data into the table
# this can occur by joining the reporting_db version of patent table with the upload_db version of the detail_desc_text table

db_con.execute('insert into {}.patent_detail_desc_text....;'.format(detail_desc_text_db))


