
from glob import glob
from gzip import GzipFile
from optparse import OptionParser
import os
os.chdir('/Users/npatel/Documents/PatentsView/PatentsView-DB/Scripts')
from os.path import basename, isdir, isfile, join, splitext
import re

from Government_Interest.govint_common import separate_into_govint_docs_as_lines_pftaps
from Government_Interest.govint_common import separate_into_govint_docs_as_lines_pg
from Government_Interest.govint_common import separate_into_govint_docs_as_lines_ipg


fd = '/Volumes/Raw Data/nish_gi_revisions_testing/'
out_fd = '/Volumes/Raw Data/nish_gi_revisions_testing/output/'


# 1. read in original xml --- yield gi lines (Extract govintdocs.py_)
def process_file(fd, in_file_name, out_file_name):
    """
    Uses the start of the in_file_   name to determine what kind of documents it
    contains, and from that reads in the government interest documents and
    writes them to the output file.
    :param input_dir: directory where the input files are
    :param in_file_name: input file name.
    :param out_file_name: output file name.
    :return: Number of government interest documents found.
    """
    print("processing...")
    base_name = basename(in_file_name).lower()
    if base_name.startswith('ipg'):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_ipg
    elif base_name.startswith(('pftaps')):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pftaps
    elif base_name.startswith('pg'):
        separate_into_govint_docs_as_lines = separate_into_govint_docs_as_lines_pg
    else:
        msg = 'Unable to determine file type of %s from name.' % (in_file_name,)
        raise ValueError(msg)

    gi_ct = 0
    with open(out_fd + out_file_name, "w") as out_file:

        for lines in separate_into_govint_docs_as_lines(input_dir + in_file_name):
            gi_ct += 1
            out_file.writelines(lines)

    return gi_ct

# 11 infile - BACKGROUND STATEMENT
out_fn = "gi_3932674.txt"
x = process_file(fd, "pftaps19760113_wk02.zip", out_fn)



# 23 infile - BACKGROUND STATEMENT
out_fn = "gi_3962706.txt"
q = process_file(fd, "pftaps19760608_wk23.zip", out_fn)


##################################

# 16 infile - 1. Field of Invention
out_fn = "gi_all_4143369.txt"
y = process_file(fd, "pftaps19790306_wk10.zip", out_fn)

# 25 infile - 1. Field of Invention
out_fn = "gi_all_4287488.txt"
y = process_file(fd, "pftaps19810901_wk35.zip", out_fn)

# RE3517 ----- random language
out_fn = 'gi_all_RE35317.txt'
y = process_file(fd, "pftaps19960827_wk35.zip", out_fn)

##################################

# -- Null - nothing wrong
out_fn = "gi_all_6420539.txt"
z = process_file(fd, "pg020716.zip", out_fn)

# --- Null - nothing wrong
out_fn = "gi_all_6712574.txt"
w = process_file(fd, "pg040330.zip", out_fn)




patent = "RE035317"


# 2. parse gi lines
# AIR_USPTO_GI_PARSER  1976-2001
infile = open(out_fd + out_fn).read().split('PATN')
line_search(patent, infile)

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
                #govt = re.search('GOVT\s+\nPAC\s+(.*?)\nPAR\s+(.*?)\n[A-Z]{3,10}',i,re.DOTALL)
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
                    #text = re.sub('[\n\t\r\f]+', '', govt.group(1))
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



# 2.b 2002-2015 parser
inp = open(out_fd + out_fn, 'rb').read()
line_search(patent, inp)

print>> checks, d
gg = inp.split('<!DOCTYPE')
print len(gg)
del gg[0]
for g in gg:
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




####### line searching

def line_search(pat, lines_list):
    counter = 0
    for line in lines_list:
        pat_search = re.search(pat, line)
        if pat_search:
            print(counter)
        counter += 1


