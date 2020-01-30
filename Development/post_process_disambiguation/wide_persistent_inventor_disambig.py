# converts long persistent inventor disambig table to wide format for bulk downloads
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

disambig_folder = "{}/disambig_output/".format(config['FOLDERS']['WORKING_FOLDER'])

outfile_name = 'persistent_inventor_wide.tsv'
outfile_fp = disambig_folder + outfile_name

old_db = config['DATABASE']['OLD_DB']
new_db = config['DATABASE']['NEW_DB']
new_db_timestamp = new_db.replace('patent_', '')


############ 1. Create output file for wide format and needed wide/long column lists
# get disambig cols from old db's persistent_inventor_disambig
result = db_con.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';".format(old_db, 'persistent_inventor_disambig'))
pid_cols = [r[0] for r in result]
disambig_cols = [x for x in pid_cols if x.startswith('disamb')]
disambig_cols.sort()
print(disambig_cols)


# Add new column for this data update:
header = ['current_rawinventor_id', 'old_rawinventor_id'] + disambig_cols + ['disamb_inventor_id' + new_db_timestamp]
print(header)
header_df = pd.DataFrame(columns = header)
header_df.to_csv(outfile_fp, index=False, header=True, sep='\t')

# fixed
pid_long_cols = ['current_rawinventor_id', 'old_rawinventor_id', 'database_update', 'inventor_id']


############ 2. Convert long -> wide and output .tsv: grab all uuid rows together for a set of uuids
limit = 300000
offset = 0

start = time.time()
itr = 0

while True:
   
    print('###########################################\n')
   
    print('Next iteration... ', itr)
   
    sql_stmt_inner = "(select uuid from {0}.{1} order by uuid limit {2} offset {3}) ri".format(new_db, 'rawinventor',  limit, offset)
   sql_stmt_template = "select lf.uuid as current_rawinventor_id, ri_old.uuid as old_rawinventor_id, lf.database_update, lf.inventor_id from {0} left join {1}.{2} lf on ri.uuid = lf.uuid left join {3}.{4} ri_old on lf.uuid = ri_old.uuid;".format(sql_stmt_inner,new_db,'persistent_inventor_disambig_long', old_db, 'rawinventor')

    print(sql_stmt_template)
    result = db_con.execute(sql_stmt_template)

    chunk_results = [r for r in result]

    # no more result batches to process! done
    if len(chunk_results) == 0:
        break

    # 0. Preprocess dataupdate column to add prefix + save current/old uuid lookup
    chunk_df = pd.DataFrame(chunk_results, columns = pid_long_cols)
    chunk_df['database_update'] = 'disamb_inventor_id_' + chunk_df['database_update']

    uuid_lookup = chunk_df[['current_rawinventor_id', 'old_rawinventor_id']].drop_duplicates()

    # 1. Pivot, reset index & get back uuid as column, rename axis & remove database_update axis value
    pivoted_chunk_df = chunk_df.pivot(index='current_rawinventor_id', columns='database_update', values='inventor_id').reset_index().rename_axis(None,1)

    # 2. Merge back old rawinventor id column
    merged_df = pd.merge(pivoted_chunk_df,uuid_lookup)

    # 3. Concat with sort = False (preserves desired col order from header_df)
    formatted_chunk_df = pd.concat([header_df, merged_df], sort=False)

    # 4. Write to outfile
    formatted_chunk_df.to_csv(outfile_fp, index=False, header=False, mode = 'a', sep='\t', na_rep = None)

    offset+=limit 
    itr+=1

    if itr == 1:
    print('Time for 1 iteration: ', time.time() - start, ' seconds')
    print('###########################################\n')
   
   
print('###########################################\n')
print('total time taken:', round(time.time() - start, 2), ' seconds')
print('###########################################\n')


####### 3. create table in database
db_con.execute('drop table if exists {}.{}'.format(new_db, 'persistent_inventor_disambig')

# only read header for creating table
wide_pid_df = pd.read_csv(outfile_fp, sep='\t', nrows = 1)
pid_cols = list(wide_pid_df.columns.values)

create_stmt = 'create table {0}.{1} ( '.format(new_db, 'persistent_inventor_disambig')
primary_key_stmt = 'PRIMARY KEY (`current_rawinventor_id`));'

for col in pid_cols:
    
    if col == 'current_rawinventor_id':
        add_col_stmt = "`{0}` varchar(32),".format(col)
        
    elif col == "old_rawinventor_id":
        add_col_stmt = "`{0}` varchar(32),".format(col)

    else:
        add_col_stmt = "`{0}` varchar(16), ".format(col)
        
    create_stmt += add_col_stmt
    
create_stmt = create_stmt + primary_key_stmt
print(create_stmt)
db_con.execute(create_stmt)

######### 4. load data
db_con.execute("LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}.{2} FIELDS TERMINATED BY '\t' IGNORE 1 lines;".format(outfile_fp, new_db, 'persistent_inventor_disambig'))

