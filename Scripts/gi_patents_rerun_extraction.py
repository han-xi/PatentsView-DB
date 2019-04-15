
import os
import pandas as pd
import mysql.connector
from os.path import basename
import re
import csv
os.chdir('/Users/npatel/Documents/PatentsView/PatentsView-DB/Scripts')

from Government_Interest.govint_common import separate_into_govint_docs_as_lines_pftaps
from Government_Interest.govint_common import separate_into_govint_docs_as_lines_pg
from Government_Interest.govint_common import separate_into_govint_docs_as_lines_ipg

patent_list_dir = "/Volumes/Final/"
raw_data_dir_1976_2001 = "/Volumes/Raw Data/1976-2001/"
raw_data_dir_2002_2004 = "/Volumes/Raw Data/2002-2004/"
raw_data_dir_2005_2018 = "/Volumes/Raw Data/2005-2018/"

out_dir = '/Volumes/Raw Data/nish_gi_revisions_testing/rerun_output/'

curr_dir = ""

# to do:

# 1. read in the csv file with patents to extract DONE
# 2. query each in sql DONE
# 3. create list of files DONE
# 4. go through the directories 1976-2002 and 2002-after to grab those exact files
# 5. reparse

# for patents 1976-2002 - run with regex changes
# for patents 2002-after - run with no regex changes, simply parse statements

#NPnotes_Copy of results_NULL Part 1.xlsx


####### line searching

def line_search(pat, lines_list):
    counter = 0
    for line in lines_list:
        pat_search = re.search(pat, line)
        if pat_search:
            print(counter)
            return counter
        counter += 1
    return counter



def main():


    config = {
        "user" : "",
        "password" : "",
        "host" : "",
        "database" : ""


    }


    df = pd.read_csv(patent_list_dir + "patents_to_reparse.csv")

    # 1. list of patents to extract
    patents_list = df['patent_id'].tolist()


    # 2. open connection to sql db
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()



    patents = []
    filenames = []

    for pat in patents_list:
        #query = ("SELECT id, filename FROM patent WHERE id = " + "\"" + pat + "\"")
        #print(query)

        query = ("""SELECT id, filename FROM patent WHERE id = %s""" )


        cursor.execute(query, (pat,))
        records = cursor.fetchall()

        for row in records:
            patents.append(row[0])
            filenames.append(row[1])


    final_dict = {"patent_id": patents, "filename": filenames}
    final_df = pd.DataFrame(final_dict, columns=["patent_id", "filename"])


    final_df.to_csv(patent_list_dir + "patent_filenames.csv")


    count = 0
    # 3. go through list of files --- yield based on function
    for fn in final_df['filename'][198:]:
        fn = str(fn)
        base_name = basename(fn).lower()

        if base_name.startswith("ipg"):
            curr_dir = raw_data_dir_2005_2018
            separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_ipg

        elif base_name.startswith("pftaps"):
            curr_dir = raw_data_dir_1976_2001
            separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pftaps
        elif base_name.startswith("pg"):
            curr_dir = raw_data_dir_2002_2004
            separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pg
        else:
            print("Directory not found for this specific file: " + fn)

        gi_ct = 0
        count += 1
        with open(out_dir + fn, "w") as out_file:

            for lines in separate_into_govint_docs_as_lines(curr_dir + fn):
                gi_ct += 1
                out_file.writelines(lines)

        print("gi count for " + fn + " was " + str(gi_ct))
        print("row number is: " + str(count))


###################################################


def rerun_1976_2002(out_fd, out_fn, patent):
    # 2. parse gi lines
    # AIR_USPTO_GI_PARSER  1976-2001

    for i in infile:
        check = re.search('GOVERNMENT INTEREST|\nGOVT', i, re.I)
        if check:
            try:
                num = re.search('WKU\s+(\w+).*?\n', i).group(1)
            except:
                num = ''
            try:
                apn = re.search('APN\s+(\w+).*?\n', i).group(1)
            except:
                apn = ''
            if num == '' and apn == '':
                print>> bad, d + '\n' + i + '\n' + '____'
            govt = re.search('GOVT\nPAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}', i, re.DOTALL)
            try:
                text = re.sub('[\n\t\r\f]+', '', govt.group(2))
                text = re.sub('\s+', ' ', text)
                outp.writerow([num, apn, govt.group(1), text])
            except:
                try:
                    # govt = re.search('GOVT\s+\nPAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
                    govt = re.search('GOVT\s+\nPAC\s+(.*?)\nPAR\s+(.*?)\nBSUM', i, re.DOTALL)
                    text = re.sub('[\n\t\r\f]+', '', govt.group(2))
                    text = re.sub('\s+', ' ', text)
                    outp.writerow([num, apn, govt.group(1), text])
                except:
                    try:
                        govt = re.search('GOVT.*?PAR\s+(.*?)\n[A-Z]{3,10}', i, re.DOTALL)
                        ######
                        gi = re.search('GOVT.*?BACKGROUND', govt.group(), re.DOTALL)
                        ######
                        text = re.sub('[\n\t\r\f]+|BACKGROUND|GOVT|BSUM|PAC', '', gi.group())
                        # text = re.sub('[\n\t\r\f]+', '', govt.group(1))
                        text = re.sub('\s+', ' ', text)
                        outp.writerow([num, apn, 'Statement of Government Interest', text])
                    except:
                        try:
                            govt = re.search('PARN.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}', i, re.DOTALL)
                            text = re.sub('[\n\t\r\f]+', '', govt.group(2))
                            text = re.sub('\s+', ' ', text)
                            if re.search('government', govt.group(1), re.I):
                                outp.writerow([num, apn, govt.group(1), text])
                            else:
                                go = data[num]
                        except:
                            try:
                                govt = re.search('BSUM.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}', i, re.DOTALL)
                                text = re.sub('[\n\t\r\f]+', '', govt.group(2))
                                text = re.sub('\s+', ' ', text)
                                if re.search('government', govt.group(1), re.I):
                                    outp.writerow([num, apn, govt.group(1), text])
                                else:
                                    go = data[num]
                            except:

                                try:
                                    govt = re.search('ABST.*?PAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}', i,
                                                     re.DOTALL)
                                    text = re.sub('[\n\t\r\f]+', '', govt.group(2))
                                    text = re.sub('\s+', ' ', text)
                                    if re.search('government', govt.group(1), re.I):
                                        outp.writerow([num, apn, govt.group(1), text])

                                    else:
                                        go = data[num]
                                except:
                                    print>> bad, d + '\n' + i + '____'
                                    print d, i

def rerun_2002_2004(out_fd, out_fn, patent, inp, idx):

    outp = csv.writer(open(out_fd + out_fn[:-4],'wb'))

    g = gg[idx]
    numi=0
    ch = re.search('GOVINT|government interest', g, re.I)
    if ch:
        numi += 1
        try:
            text = re.search('government interest.*?<PDAT>(.*?)</PDAT>', g, re.I | re.DOTALL).group(1)
            try:
                num = re.search('<DNUM><PDAT>(\w+)', g).group(1)
            except:
                print "BADDDDDD"
            # print text
            outp.writerow([num, 'STATEMENT OF GOVERNMENT INTEREST', text])
        except:
            text = re.search('<GOVINT>.*?</GOVINT>', g, re.I | re.DOTALL).group()
            try:
                num = re.search('<DNUM><PDAT>(\w+)', g).group(1)
            except:
                print "BADDDDDD"

            need = list(re.finditer('<PDAT>(.*?)</PDAT>', text))
            if len(need) > 1:
                # print "1",need[0].group(1),need[1].group(1)
                print([num, need[0].group(1), need[1].group(1)])
                outp.writerow([num, need[0].group(1), need[1].group(1)])

            else:
                # print "2",need[0].group(1)
                outp.writerow([num, 'STATEMENT OF GOVERNMENT INTEREST', need[0].group(1)])


def rerun_2005_2018(out_fd, out_fn, patent):






###### process - parse here, then rewrite lines in govtint_common/extraction code
all_files_rerun = os.listdir(out_dir)
out_dir_gi = "'/Volumes/Raw Data/nish_gi_revisions_testing/gi_rerun_output/'"
# loop through patents, find corresponding file to open ---
for fn in final_df['filename']:

    rows = final_df[final_df['filename'] == fn]

    if basename(fn).startswith("ipg"):
        print("skipping for now")

    elif basename(fn).startswith("pg"):
        inp = open(out_dir + fn, 'rb').read()
        gg = inp.split('<!DOCTYPE')
        print len(gg)
        del gg[0]
        # more than one patent from file
        for patent in rows['patent_id']:
                idx = line_search(patent,gg )
                if idx != -1:
                    rerun_2002_2004(out_dir_gi, "gi" + fn, patent, gg, idx)

    elif basename(fn).startswith("pftaps"):
        inp = open(out_dir + fn).read().split('PATN')

        for patent in rows['patent_id']:
            idx = line_search(patent, inp)
            rerun_1976_2002(out_dir_gi, "gi" + fn, patent)














