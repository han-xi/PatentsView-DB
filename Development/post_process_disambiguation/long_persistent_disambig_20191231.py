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
print(old_db, "|", new_db)

# 2. create output file
outfile_name = disambig_folder +'/persistent_inventor_long.tsv'
header = ['uuid', 'database_update', 'inventor_id']
header_df = pd.DataFrame(columns = header)
header_df.to_csv(outfile_name, index=False, header=True, sep='\t')

# 2. get disambig cols needed
result = db_con.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}'".format(new_db, 'persistent_inventor_disambig_type_2'))
pid_t2_cols = [r[0] for r in result]
disambig_cols = [x for x in pid_t2_cols if x.startswith('disamb')]
print(disambig_cols)
print('######################################################################\n')
wide_pid_cols = ['uuid'] + disambig_cols
print(wide_pid_cols)


# 2. build sql query - need to join rawinventor, old rawinventor and persistent_inventor_disambig_type2 together

select_piece = "select ri.uuid, pid_t2.disamb_inventor_id_20170808, pid_t2.disamb_inventor_id_20171003, pid_t2.disamb_inventor_id_20171226, pid_t2.disamb_inventor_id_20180528, pid_t2.disamb_inventor_id_20181127, pid_t2.disamb_inventor_id_20190312, pid_t2.disamb_inventor_id_20190820, pid_t2.disamb_inventor_id_20191008 " 
from_piece = "from {0} pid_t2 inner join {1} ri on pid_t2.patent_id = ri.patent_id and pid_t2.sequence = ri.sequence ".format('persistent_inventor_disambig_type_2', 'rawinventor', old_db)
limit_piece = "limit {0} offset {1};"
sql_stmt_template = select_piece + from_piece + limit_piece
print(select_piece + '\n\n' + from_piece + '\n\n' + limit_piece)


result = db_con.execute('select count(*) from {}.{}'.format(new_db,'persistent_inventor_disambig_type_2'))
total_rows = [r[0] for r in result][0]
total_rows


limit = 300000
offset = 0

start = time.time()
itr = 0

print('Estimated # of rounds: ', total_rows/300000)

while True:
    print('###########################################\n')
    print('Next iteration... ', itr)
    
    limit_piece = "limit {0} offset {1}".format(limit, offset)
    sql_stmt_template = select_piece + from_piece + limit_piece
    print(sql_stmt_template)
    result = db_con.execute(sql_stmt_template)

    chunk_results = [r for r in result]
    #print(len(chunk_results))
    
    if len(chunk_results) == 0:
        break

    chunk_df = pd.DataFrame(chunk_results, columns = wide_pid_cols)
#     print(chunk_df.head())
#     print('######################################################################\n')
    melted_chunk_df = chunk_df.melt(id_vars=['uuid'], value_vars = disambig_cols, var_name = 'database_update', value_name = 'inventor_id')
#     print(melted_chunk_df)
    # filter here [] only keep rows values in 'inventor id '
    melted_chunk_df = melted_chunk_df[melted_chunk_df['inventor_id']!= '']
    melted_chunk_df['database_update'] = melted_chunk_df['database_update'].str.replace('disamb_inventor_id_', '')
#     print('######################################################################\n')
#     print(melted_chunk_df)
    melted_chunk_df.to_csv(outfile_name, index=False, header=False, mode = 'a', sep='\t')
    
    offset+=limit 
    itr+=1

    if itr == 1:
        
        print('Time for 1 iteration: ', time.time() - start, ' seconds')
        
    print('###########################################\n')
    
    
print('###########################################')
print('total time taken:', round(time.time() - start, 2), ' seconds')
print('###########################################')


