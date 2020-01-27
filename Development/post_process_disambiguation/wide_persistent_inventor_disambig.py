import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from warnings import filterwarnings
import csv
import re, os, random, string, codecs
import sys
from collections import Counter, defaultdict
import time
import tqdm
project_home = os.environ['PACKAGE_HOME']
from Development.helpers import general_helpers

import configparser

config = configparser.ConfigParser()
config.read(project_home + '/Development/config.ini')

db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                       config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

db_folder = 'upload_20191231'
disambig_folder = "{}{}/disambig_output".format(config['FOLDERS']['WORKING_FOLDER'], db_folder)


print(disambig_folder)
old_db = config['DATABASE']['OLD_DB']
new_db = config['DATABASE']['NEW_DB']
new_db_timestamp = new_db.replace('patent_', '')
print(old_db, "|", new_db, "|" , new_db_timestamp)


############ 1. create output file for wide format

# get disambig cols needed
result = db_con.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}'".format(new_db, 'persistent_inventor_disambig'))
pid_cols = [r[0] for r in result]
disambig_cols = [x for x in pid_cols if x.startswith('disamb')]
print(disambig_cols)
print('######################################################################\n')

outfile_name = disambig_folder +'/persistent_inventor_wide.tsv'

# TODO: add back in the + part below for final script - ignoring for now since persistent_inventor_disambig has that column
header = ['current_rawinventor_id', 'old_rawinventor_id'] + disambig_cols #+ ['disamb_inventor_id' + new_db_timestamp]
print(header)
header_df = pd.DataFrame(columns = header)
header_df.to_csv(outfile_name, index=False, header=True, sep='\t')


############ 2. get column names of persistent_inventor_disambig_long table for pivoting long -> wide
pid_long_cols = ['current_uuid', 'old_uuid', 'database_update', 'inventor_id']


############ 3. Need to convert long table into wide and output .tsv - grabbing all uuid rows together for the set of uuids
limit = 100000
offset = 0

start = time.time()
itr = 0

print('Estimated # of rounds: ', total_rows/300000)

while True:
    print('###########################################\n')
    print('Next iteration... ', itr)
    
    sql_stmt_inner = "( select uuid from {0}.{1} order by uuid limit {2} offset {3}) ri".format(new_db, 'rawinventor',  limit, offset)
    sql_stmt_template = "select lf.uuid as current_uuid, ri. lf.database_update, lf.inventor_id from {0} inner join {1}.{2} lf on ri.uuid = lf.uuid;".format(sql_stmt_inner,new_db,'persistent_inventor_disambig_long')

    print(sql_stmt_template)
    result = db_con.execute(sql_stmt_template)

    chunk_results = [r for r in result]
    print(len(chunk_results))
    
    # means we have no more result batches to process! done
    if len(chunk_results) == 0:
        break

    chunk_df = pd.DataFrame(chunk_results, columns = pid_long_cols)
    chunk_df['database_update'] = 'disamb_inventor_id_' + chunk_df['database_update']
    
    # pre pivot check
    print(chunk_df.head())
    print('######################################################################\n')
    
    # reset index to get back uuid as column, rename axis to get rid of database_update axis value
    chunk_df = chunk_df.pivot(index='uuid', columns='database_update', values='inventor_id').reset_index().rename_axis(None,1)
    
    # post pivot & processing check
    print(chunk_df.head())    
    print('######################################################################\n')

    # sort = False will preserve col order
    formatted_chunk_df = pd.concat([header_df, chunk_df], sort=False)
    
    # replace NAs with empty strings
    formatted_chunk_df.to_csv(outfile_name, index=False, header=False, mode = 'a', sep='\t', na_rep = None)
    
    offset+=limit 
    itr+=1

    if itr == 1:
        print('Time for 1 iteration: ', time.time() - start, ' seconds')
    print('###########################################\n')
    
    
print('###########################################')
print('total time taken:', round(time.time() - start, 2), ' seconds')
print('###########################################')