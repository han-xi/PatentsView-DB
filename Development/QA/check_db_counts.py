#Counts QA

#set up sql alchemy engine
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

latest_expected_data = '2018-11-27'

#Counts- creating count description
def create_count_description(old_db_count, new_db_count, table, min_inc = 1.01, max_inc = 1.1):
    unchanging = ['cpc_subsection', 'wipo_field', 'nber_category', 'nber', 'cpc_group',
                  'nber_subcategory', 'mainclass', 'mainclass_current', 'subclass', 'subclass_current']
    slight_changes = ['cpc_subgroup']
    if not table in unchanging + slight_changes:
        if new_db_count < old_db_count:
            return "Problem: New table has fewer rows than old table "
        elif new_db_count == old_db_count:
            return "Problem: No new entries"
        elif new_db_count > old_db_count*max_inc:
            return "Problem: Too many new entries"
        elif new_db_count < old_db_count*min_inc:
            return "Problem: Too few new entries"
        elif old_db_count*min_inc < new_db_count < old_db_count*max_inc:
            return "No Problem!"
        else:
            return "Check the logic!"
    elif table in unchanging:
        if new_db_count == old_db_count:
            return "No Problem!"
        else:
            return "Problem: Number of rows in unchanging table changed"
    elif table in slight_changes:
        if old_db_count*.9 < new_db_count < old_db_count*1.1:
            return "No Problem!"
        else: 
            return "Problem: Number of rows in slightly changing table changed"

#Counts- Get counts
def get_counts(OLD_QA_LOC, NEW_QA_LOC, new_db):
    counts = pd.read_excel('{}/1_table_counts.xlsx'.format(previous_qa_loc))
    cursor = connect()
    cursor.execute('use {}'.format(new_db))
    new_counts = []
    for table in counts['Table']:
       # print table
        cursor.execute('select count(*) from {}'.format(table))
        count = cursor.fetchall()[0][0]
        new_counts.append(count)
    return new_counts

#Counts- write to excel
def make_excel(new_counts, OLD_QA_LOC, NEW_QA_LOC, new_db):
    counts = pd.read_excel('{}/1_table_counts.xlsx'.format(OLD_QA))
    counts[new_db] = new_counts
    del counts['Description']

    #the last row of the table is now the most recent previous database!
    counts['Description'] = counts.apply(lambda row: create_count_description(row[counts.columns[-3]], row[new_db],row['Table']), axis=1)
    counts.to_excel('{}/1_table_counts.xlsx'.format(NEW_QA_LOC), index = False)

#Approximately 
new_counts = get_counts(OLD_QA_LOC, NEW_QA_LOC, new_db)
make_excel(new_counts,OLD_QA_LOC, NEW_QA_LOC, new_db)

