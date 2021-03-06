import sys
import os
import MySQLdb
sys.path.append('/project/Development')
sys.path.append('{}/{}'.format(os.getcwd(), 'Development'))
from helpers import general_helpers
from warnings import filterwarnings
import csv
import pandas as pd
import re,os,random,string,codecs
import multiprocessing
import time

def write_cpc_current(cpc_input, cpc_output, error_log, patent_dict, patent_set,db_con):
    
    #Create CPC_current table off full master classification list
    cpc_data= csv.reader(open(cpc_input,'r'),delimiter = ',')
    errorlog = csv.writer(open(error_log, 'w'))
    current_exist = {}
    cpc_out = csv.writer(open(cpc_output,'w'),delimiter = '\t')
    print(cpc_input)
    for row in cpc_data:
        try:
            if str(row[0]) in patent_set:
                towrite = [re.sub('"',"'",item) for item in row[:3]]
                towrite.insert(0,general_helpers.id_generator())
                towrite[1] = patent_dict[row[0]]
                for t in range(len(towrite)):
                    try:
                        gg = int(towrite[t])
                        towrite[t] = str(int(towrite[t]))
                    except:
                        pass
                primaries = towrite[2].split("; ")
                cpcnum = 0
                for p in primaries:
                    try:
                        needed = [general_helpers.id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'inventional',str(cpcnum)]
                        clean = [i if not i =="NULL" else "" for i in needed]
                        cpcnum+=1
                        cpc_out.writerow(clean)
                    except: #sometimes a row doesn't have all the information
                        errorlog.writerow(row)
                try:
                    additionals = [t for t in towrite[3].split('; ') if t!= '']
                except:
                    additionals = []
                    errorlog.writerow(row)

                for p in additionals:
                    try:
                        needed = [general_helpers.id_generator(),towrite[1]]+[p[0],p[:3],p[:4],p,'additional',str(cpcnum)]
                        clean = [i if not i =="NULL" else "" for i in needed]
                        cpcnum+=1
                        cpc_out.writerow(clean)
                    except:
                        errorlog.writerow(row)
        except IndexError as e:
            print ("ERROR@!!!")
            print(cpc_input)
            print(e)
            raise(e)

def upload_cpc_current(db_con, cpc_current_loc):
    cpc_current_files = [f for f in os.listdir(cpc_current_loc) if f.startswith('out')]
    for outfile in cpc_current_files:
        f = '{}/{}'.format(cpc_current_loc,outfile)
        print(f)
        if os.path.getsize(f) > 0: #some files empyt because of patents pre-1976
            #TODO: very strangely pd.read_csv works on this file in the interpreter and not in the script
            #any combinaiton of delim_whitespace=True, delimiter = '\t', encoding = 'utf-8' vs no encodign doesnt help
            #so for now read as csv then to data frame, which I hate
            #data=pd.read_csv(f, sep="\t",chunksize=1000, header=None)
            load_data_command = "LOAD DATA LOCAL INFILE '{0}' INTO TABLE cpc_current COLUMNS TERMINATED BY '\\t' lines terminated BY '\\n'; ".format(f)
            print(load_data_command)
            start = time.time()
            with db_con.begin() as conn:
                conn.execute(load_data_command)
            end = time.time()
            print("Load Time:" + str(round(end - start)))
            # input_data =[]
            # for data_chunk in data:
            #     data_chunk.columns = ['uuid',  'patent_id', 'section_id', 'subsection_id', 'group_id', 'subgroup_id', 'category', 'sequence']
            #     data_chunk.to_sql('cpc_current', db_con, if_exists='append', index=False, method="multi")
            # # with open(f, 'r') as myfile:
            # #      for line in myfile.readlines():
            # #          input_data.append(line.split('\t'))
            # # data = pd.DataFrame(input_data)




if __name__ == '__main__':
    import configparser
    config = configparser.ConfigParser()
    config.read('/project/Development/config.ini')

    db_con = general_helpers.connect_to_db(config['DATABASE']['HOST'], config['DATABASE']['USERNAME'], 
                                            config['DATABASE']['PASSWORD'], config['DATABASE']['NEW_DB'])

    
    patent_set, patent_dict = general_helpers.get_patent_ids(db_con, config['DATABASE']['NEW_DB'])

    cpc_folder = '{}/{}'.format(config['FOLDERS']['WORKING_FOLDER'],'cpc_output')
    #split up the grant file for processing
    os.system('split -a 1 -n l/7 {0}/grants_classes.csv {0}/grants_pieces_'.format(cpc_folder))

    in_files = ['{0}/grants_pieces_{1}'.format(cpc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    out_files = ['{0}/out_file_{1}.csv'.format(cpc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    error_log = ['{0}/error_log_{1}'.format(cpc_folder, item) for item in  ['a', 'b', 'c', 'd', 'e', 'f', 'g']]
    pat_dicts = [patent_dict for item in in_files]
    pat_sets = [patent_set for item in in_files]
    db_cons = [db_con for item in in_files]
    files = zip(in_files, out_files, error_log, pat_dicts, pat_sets,db_cons)


    print("Processing Files", flush=True)
    total_cpus = multiprocessing.cpu_count()
    desired_processes = (total_cpus // 2) + 1
    jobs = []
    for f in files:
        jobs.append(multiprocessing.Process(target = write_cpc_current, args=(f)))

    for segment in general_helpers.chunks(jobs, desired_processes):
        print(segment)
        for job in segment:
            job.start()

    # wait until all jobs finish processing to move on to uploading cpc current data
    for job in jobs:
        job.join()


    print("Uploading", flush=True)
    upload_cpc_current(db_con, cpc_folder)
 
