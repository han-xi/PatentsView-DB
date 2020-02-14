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
import argparse
import configparser


#########################################################################################################
# UPDATE LONG TABLE
#########################################################################################################
# REQUIRES: db_con, new_db, long table name
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
def get_long_entity_cols(db_con, new_db, persistent_long_table):
    
    result = db_con.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';".format(new_db, persistent_long_table))
    result_cols = [r[0] for r in result]
    # skip auto-increment column, we don't need it
    long_cols = [x for x in result_cols if x != 'id']
    print(long_cols)
    
    return long_cols

# REQUIRES: db_con, new_db, raw entity table name, id_col (assignee/inventor id), total rows in raw entity table, new_db timestamp, outfile path, long table header cols
# MODIFIES: nothing
# EFFECTS: returns list of long persistent entity columns
def write_long_outfile_newrows(db_con, new_db, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp, header_long):
    
    ############# 2.output rawinventor rows from newdb
    limit = 300000
    offset = 0

    start = time.time()
    itr = 0

    print('Estimated # of rounds: ', total_rows/300000)

    while True:
        print('###########################################\n')
        print('Next iteration... ', itr)

        sql_stmt_template = "select uuid, {0} from {1}.{2} order by uuid limit {3} offset {4};".format(id_col,new_db, raw_table, limit, offset)

        print(sql_stmt_template)
        result = db_con.execute(sql_stmt_template)

        # r = tuples of (uuid, new db timestamp, entity id)
        chunk_results = [(r[0], new_db_timestamp, r[1]) for r in result]

        # means we have no more result batches to process! done
        if len(chunk_results) == 0:
            break

        chunk_df = pd.DataFrame(chunk_results, columns = header_long)
        chunk_df.to_csv(outfile_fp, index=False, header=False, mode = 'a', sep='\t')

        # continue
        offset+=limit 
        itr+=1

        if itr == 1:
            print('Time for 1 iteration: ', time.time() - start, ' seconds')
            print('###########################################\n')


    print('###########################################')
    print('total time taken:', round(time.time() - start, 2), ' seconds')
    print('###########################################')

    return 



# REQUIRES: entity (either inventor or assignee)
# MODIFIES: nothing
# EFFECTS: updates persistent long entity table with new rows from db update
def update_persistent_long_entity(entity, config):
    
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                           config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    disambig_folder = "{}/disambig_output/".format(config['FOLDERS']['WORKING_FOLDER'])
    outfile_name_long = 'persistent_{0}_long_{1}.tsv'.format(entity, new_db_timestamp)
    outfile_fp_long = disambig_folder + outfile_name_long

    old_db = config['DATABASE']['OLD_DB']
    new_db = config['DATABASE']['NEW_DB']
    new_db_timestamp = new_db.replace('patent_', '')
    
    # set of values that change depending on entity
    persistent_long_table = 'persistent_{0}_disambig_long'.format(entity)
    raw_table = 'raw{0}'.format(entity)
    id_col = '{0}_id'.entity
    
    header_long = get_long_entity_cols(db_con,new_db,persistent_long_table)
    
    # generate header for output file
    header_df = pd.DataFrame(columns = header_long)
    header_df.to_csv(outfile_fp_long, index=False, header=True, sep='\t')

    # get total rows in raw entity table
    result = db_con.execute('select count(*) from {0}.{1}'.format(new_db, raw_table))
    total_rows = [r[0] for r in result][0]
    
    write_long_outfile_newrows(db_con, new_db, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp_long, header_long)
    
    
    ######### 3. load data
    db_con.execute("LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}.{2} FIELDS TERMINATED BY '\t' NULL DEFINED BY '' IGNORE 1 lines (uuid, database_update, {3});".format(outfile_fp, new_db, persistent_long_table, id_col))

    return

#########################################################################################################
# WIDE TABLE CREATION BELOW
#########################################################################################################
# REQUIRES: db_con, old_db, new_db, entity, raw entity table name, total rows in raw entity table, new_db timestamp
# MODIFIES: nothing
# EFFECTS: returns list of wide persistent entity columns
def get_wide_entity_disambig_cols(db_con, old_db, persistent_disambig_table):
    
    result = db_con.execute("select column_name from information_schema.columns where table_schema = '{0}' and table_name = '{1}';".format(old_db, persistent_disambig_table))
    result_cols = [r[0] for r in result]
    disambig_cols = [x for x in result_cols if x.startswith('disamb')]
    disambig_cols.sort()
    print(disambig_cols)
    
    return disambig_cols

# REQUIRES: db_con, old_db, new_db, entity, raw entity table name, total rows in raw entity table, new_db timestamp
# MODIFIES: nothing
# EFFECTS: writes .tsv of table in wide format
def write_wide_outfile(db_con, new_db, entity, persistent_long_table, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp, header_df):
    
    # fixed
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    disamb_str = 'disamb_{}_id_'.format(entity)
    
    # fixed
    chunk_cols = [current_rawentity, old_rawentity, 'database_update', id_col]


    ############ 2. Convert long -> wide and output .tsv: grab all uuid rows together for a set of uuids
    limit = 300000
    offset = 0

    start = time.time()
    itr = 0
    
    print('Estimated # of rounds: ', total_rows/300000)

    while True:

        print('###########################################\n')

        print('Next iteration... ', itr)

        sql_stmt_inner = "(select uuid from {0}.{1} order by uuid limit {2} offset {3}) raw".format(new_db, raw_table, limit, offset)
        sql_stmt_template = "select lf.uuid as {0}, raw_old.uuid as {1}, lf.database_update, lf.{2} from {3} left join {4}.{5} lf on raw.uuid = lf.uuid left join {5}.{6} raw_old on lf.uuid = raw_old.uuid;".format(current_rawentity, old_rawentity, id_col, sql_stmt_inner, new_db, persistent_long_table, old_db, raw_table)

        print(sql_stmt_template)
        result = db_con.execute(sql_stmt_template)

        chunk_results = [r for r in result]

        # no more result batches to process! done
        if len(chunk_results) == 0:
            break

        # 0. Preprocess dataupdate column to add prefix + save current/old uuid lookup
        chunk_df = pd.DataFrame(chunk_results, columns = chunk_cols)
        chunk_df['database_update'] = disamb_str + chunk_df['database_update']

        uuid_lookup = chunk_df[[current_rawentity, old_rawentity]].drop_duplicates()

        # 1. Pivot, reset index & get back uuid as column, rename axis & remove database_update axis value
        pivoted_chunk_df = chunk_df.pivot(index=current_rawentity, columns='database_update', values=id_col).reset_index().rename_axis(None,1)

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

    return

# REQUIRES: entity, varchar, entity database id cols, sql create statement
# MODIFIES: nothing
# EFFECTS: creates persistent entity table syntax
def get_create_syntax(db_con, entity, entity_db_cols, create_stmt):
    
    current_rawentity = 'current_raw{0}_id'.format(entity)
    old_rawentity = 'old_raw{0}_id'.format(entity)
    
    
    # loop through to construct create syntax for entity disambig cols
    for col in entity_db_cols:

        # regardless of entity, uuid column fixed at 32
        if col == current_rawentity or col == old_rawentity:
            add_col_stmt = "`{0}` varchar(32),".format(col)
        
        # if assignee is entity - then disambig cols need to be varchar 64
        else if entity == 'assignee':
            add_col_stmt = "`{0}` varchar(64), ".format(col)
        
        # inventor entity -  disambig cols need to be varchar 16
        else:
            add_col_stmt = "`{0}` varchar(16), ".format(col)

        create_stmt += add_col_stmt

    return create_stmt


# REQUIRES: db_con, entity, persistent entity table, outfile folder path
# MODIFIES: nothing
# EFFECTS: creates persistent entity table for new database
def create_wide_table_database(db_con, entity, persistent_disambig_table, outfile_fp):
        
    ####### 3. create table in database
    db_con.execute('drop table if exists {}.{}'.format(new_db, persistent_disambig_table)

    current_rawentity = 'current_raw{0}_id'.format(entity)
                   
    # only read header for creating table
    wide_df = pd.read_csv(outfile_fp, sep='\t', nrows = 1)
    entity_db_cols = list(wide_df.columns.values)

    create_stmt = 'create table {0}.{1} ( '.format(new_db, persistent_disambig_table)
    primary_key_stmt = 'PRIMARY KEY (`{0}`));'.format(current_rawentity)
                   
                   
    create_stmt = get_create_syntax(db_con, entity, entity_db_cols, create_stmt)

    create_stmt = create_stmt + primary_key_stmt
    print(create_stmt)
    db_con.execute(create_stmt)
                   
    return


# REQUIRES: entity (either inventor or assignee)
# MODIFIES: nothing
# EFFECTS: creates persistent wide entity table
def create_persistent_wide_entity(entity):
    config = configparser.ConfigParser()
    config.read(project_home + '/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'],
                                           config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    disambig_folder = "{}/disambig_output/".format(config['FOLDERS']['WORKING_FOLDER'])
    outfile_name_long = 'persistent_{0}_long_{1}.tsv'.format(entity, new_db_timestamp)
    outfile_fp_long = disambig_folder + outfile_name_long

    old_db = config['DATABASE']['OLD_DB']
    new_db = config['DATABASE']['NEW_DB']
    new_db_timestamp = new_db.replace('patent_', '')
    
    # set of values that change depending on entity
    persistent_long_table = 'persistent_{0}_disambig_long'.format(entity)
    raw_table = 'raw{0}'.format(entity)
    id_col = '{0}_id'.entity
    
    outfile_name_wide = 'persistent_{}_wide.tsv'.format(entity)
    outfile_fp_wide = disambig_folder + outfile_name_wide
    
    persistent_disambig_table = 'persistent_{0}_disambig'.format(entity)
    
    # get disambig cols from old db's persistent_inventor_disambig
    disambig_cols = get_wide_entity_disambig_cols(db_con, old_db, persistent_disambig_table)

    # Add new column for this data update:
    raw_cols = ['current_{0}_id'.format(raw_table), 'old_{0}_id'.format(raw_table)]
    header_wide = [raw_cols[0], raw_cols[1]] + disambig_cols + ['disamb_{0}_id_'.format(entity) + new_db_timestamp]
    print(header_wide)
    header_df = pd.DataFrame(columns = header_wide)
    header_df.to_csv(outfile_fp_wide, index=False, header=True, sep='\t')
    
    write_wide_outfile(db_con, new_db, entity, persistent_long_table, raw_table, id_col, total_rows, new_db_timestamp, outfile_fp_wide, header_df)
    
    
    ####### 3. create table in database
    create_wide_table_database(db_con, entity, persistent_disambig_table, outfile_fp_wide)
                             
    ######### 4. load data
    db_con.execute("LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1}.{2} FIELDS TERMINATED BY '\t' NULL DEFINED BY '' IGNORE 1 lines;".format(outfile_fp_wide, new_db, persistent_disambig_table))

    return True



#########################################################################################################
# OPERATOR TASKS
#########################################################################################################

update_persistent_long_inventor = PythonOperator(
    task_id='update_persistent_long_inventor',
    python_callable=update_persistent_long_entity,
    op_kwargs={'entity': 'inventor', 'config':config}
    dag=dag
    )
      

update_persistent_long_assignee = PythonOperator(
    task_id='update_persistent_long_assignee',
    python_callable=update_persistent_long_entity,
    op_kwargs={'entity': 'assignee', 'config':config}
    dag=dag
    )

create_persistent_wide_inventor = PythonOperator(
    task_id='create_persistent_wide_inventor',
    python_callable=create_persistent_wide_entity,
    op_kwargs={'entity': 'inventor', 'config':config}
    dag=dag
    )


create_persistent_wide_assignee = PythonOperator(
    task_id='create_persistent_wide_assignee',
    python_callable=create_persistent_wide_entity,
    op_kwargs={'entity': 'assignee', 'config':config}
    dag=dag
    )
