import re,csv,os
import string,random

def id_generator(size=25, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

fd = 'Z:/share/Science Policy Portfolio/Patent Data 1976-Present/XML/1976-2001/'

diri = os.listdir(fd)
outp = csv.writer(open('c:/All_data_1976-2001.csv','wb'))
checks = open('c:/check_files-1976.txt','w')
#bad = open('g:/uspto_GI/bads.txt','w')
loggroups = ['PATN','INVT','ASSG','PRIR','REIS','RLAP','CLAS','UREF','FREF','OREF','LREP','PCTA','ABST','GOVT','PARN','BSUM','DRWD','DETD','CLMS','DCLM']

fd2 = 'c:/uspto_data/1976-2001/'
error = open(fd2+'error.txt','w')
numii = 0
for d in diri[1224:]:
    print d
    infile = open(fd+d).read().split('PATN')
    del infile[0]
    for i in infile:
      numii+=1
      try:    
        # Get relevant logical groups from patent records according to documentation
        # Some patents can contain several INVT, ASSG and other logical groups - so, is important to retain all
        avail_fields = {}
        num = 1
        avail_fields['PATN'] = i.split('INVT')[0]
        runnums = []
        for n in range(1,len(loggroups)):
            try:
                gg = re.search('\n'+loggroups[n],i).group()
                if num-n == 0:
                    runnums.append(n)
                    num+=1
                    go = list(re.finditer('\n'+loggroups[n-1],i))
                    if len(go) == 1:
                        needed = i.split(loggroups[n-1])[1]
                        avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                    elif len(go) > 1:
                        needed = '\n\n\n\n\n'.join(i.split(loggroups[n-1])[1:])
                        avail_fields[loggroups[n-1]] = needed.split(loggroups[n])[0]
                    else:
                        pass
                else:
                    go = list(re.finditer('\n'+loggroups[runnums[-1]],i))
                    if len(go) == 1:
                        needed = i.split(loggroups[runnums[-1]])[1]
                        avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
                    elif len(go) > 1:
                        needed = '\n\n\n\n\n'.join(i.split(loggroups[runnums[-1]])[1:])
                        avail_fields[loggroups[runnums[-1]]] = needed.split(loggroups[n])[0]
                    else:
                        pass
                    runnums.append(n)
                    num = n+1
                
            except:
                pass
        
        # Create containers based on existing Berkeley DB schema (not all are currently used - possible compatibility issues)
        application = {}
        assignee = {}
        claims = {}
        foreigncitation = {}
        inventor = {}
        ipcr = {}
        lawyer = {}
        location = {}
        location_assignee = {}
        location_inventor = {}
        mainclassdata = {}
        otherreference = {}
        patentdata = {}
        patent_assignee = {}
        patent_inventor = {}
        patent_lawyer = {}
        rawassignee = {}
        rawinventor = {}
        rawlawyer = {}
        rawlocation = {}
        subclassdata = {}
        usappcitation = {}
        uspatentcitation = {}
        uspc = {}
        usreldoc = {}
        crossrefclassdata = {}
        
        
        
        ###                PARSERS FOR LOGICAL GROUPS                  ###
        #PATN
        updnum = 'NULL'
        appnum = 'NULL'
        apptype = 'NULL'
        appdate = 'NULL'
        title = 'NULL'
        issdate = 'NULL'
        numclaims = 'NULL'
        
        try:
            patent = avail_fields['PATN'].split('\n')
            for line in patent:
                if line.startswith("WKU"):
                    patnum = re.search('(?<=\s)\w+',line).group()
                    updnum = re.sub('^H0','H',patnum)[:8]
                    updnum = re.sub('^RE0','RE',updnum)[:8]
                    updnum = re.sub('^PP0','PP',updnum)[:8]
                    updnum = re.sub('^D0', 'D', updnum)[:8]
                    updnum = re.sub('^T0', 'T', updnum)[:8]
                    if len(patnum) > 7 and patnum.startswith('0'):
                        updnum = patnum[1:8]
                    #data['patnum'] = updnum
                if line.startswith('APN'):
                    appnum = re.search('(?<=\s)\w+',line).group()[:6]
                    if len(appnum) != 6:
                        appnum = 'NULL'
                        #data['appnum'] = appnum
                if line.startswith('APT'):
                    apptype = re.search('(?<=\s)\w+',line).group()
                if line.startswith('APD'):
                    appdate = re.search('(?<=\s)\w+',line).group()
                    appdate = appdate[:4]+'-'+appdate[4:6]+'-'+appdate[6:]
                    #print appdate
                if line.startswith('TTL'):
                    title = re.search('TTL\s+(.*)$',line).group(1)
                if line.startswith('ISD'):
                    issdate = re.search('(?<=\s)\w+',line).group()
                    if issdate[6:] != "00":
                        issdate = issdate[:4]+'-'+issdate[4:6]+'-'+issdate[6:]
                    else:
                        issdate = issdate[:4]+'-'+issdate[4:6]+'-'+'01'
                    year = issdate[:4]
                    #print issdate
                if line.startswith("NCL"):
                    numclaims = re.search('(?<=\s)\w+',line).group()
        except:
            pass
        
        patent_id = id_generator()
        
        application[id_generator()] = [patent_id,apptype,appnum,'US',appdate]
        
        #INVT - can be several
        try:
            inv_info = avail_fields['INVT'].split("\n\n\n\n\n")
            for n in range(len(inv_info)):
                fname = 'NULL'
                lname = 'NULL'
                invtcity = 'NULL'
                invtstate = 'NULL'
                invtcountry = 'NULL'
                invtzip = 'NULL'
                for line in inv_info[n].split("\n"):
                    if line.startswith("NAM"):
                        invtname = re.search('NAM\s+(.*?)$',line).group(1).split("; ")
                        fname = invtname[1]
                        lname = invtname[0]
                        
                    if line.startswith("CTY"):
                        invtcity = re.search('CTY\s+(.*?)$',line).group(1)
                        
                    if line.startswith("STA"):
                        invtstate = re.search('STA\s+(.*?)$',line).group(1)
                    
                    if line.startswith("CNT"):
                        invtcountry = re.search('CNT\s+(.*?)$',line).group(1)
                    
                    if line.startswith("ZIP"):
                        invtzip = re.search('ZIP\s+(.*?)$',line).group(1)
                
                try:
                    gg = rawlocation[invtcity+'|'+invtstate+'|'+invtcountry+'|'+invtzip]
                except:
                    rawlocation[invtcity+'|'+invtstate+'|'+invtcountry+'|'+invtzip] = [invtcity,invtstate,invtcountry,invtzip]
                
                rawinventor[id_generator()] = [patent_id,patent_id+'-'+str(n),invtcity+'|'+invtstate+'|'+invtcountry+'|'+invtzip,fname,lname,str(n)]
        except:
            pass
        
        #ASSG - can be several
        try:
            assg_info = avail_fields['ASSG'].split('\n\n\n\n\n')
            for n in range(len(assg_info)):    
                assorg = 'NULL'
                assgfname = 'NULL'
                assglname = 'NULL'
                assgcity = 'NULL'
                assgstate = 'NULL'
                assgcountry = 'NULL'
                assgzip = 'NULL'
                assgtype = 'NULL'
                for line in assg_info[n].split("\n"):
                    if line.startswith("NAM"):
                        assgname = re.search('NAM\s+(.*?)$',line).group(1).split("; ")
                        if len(assgname) == 1:
                            assgorg = assgname[0]
                            assgfname = 'NULL'
                            assglname = 'NULL'
                        else:
                            assgfname = assgname[1]
                            assglname = assgname[0]
                            assgorg = 'NULL'
                        
                    if line.startswith("CTY"):
                        assgcity = re.search('CTY\s+(.*?)$',line).group(1)
                        
                    if line.startswith("STA"):
                        assgstate = re.search('STA\s+(.*?)$',line).group(1)
                    
                    if line.startswith("CNT"):
                        assgcountry = re.search('CNT\s+(.*?)$',line).group(1)
                    
                    if line.startswith("ZIP"):
                        assgzip = re.search('ZIP\s+(.*?)$',line).group(1)
                    
                    if line.startswith("COD"):
                        assgtype = re.search("COD\s+(.*?)$",line).group(1)
                try:
                    gg = rawlocation[assgcity+'|'+assgstate+'|'+assgcountry+'|'+assgzip]
                except:
                    rawlocation[assgcity+'|'+assgstate+'|'+assgcountry+'|'+assgzip] = [assgcity,assgstate,assgcountry,assgzip]
                
                rawassignee[id_generator()] = [patent_id,'ASSG'+patent_id+'-'+str(n),assgcity+'|'+assgstate+'|'+assgcountry+'|'+assgzip,assgtype,assgfname,assglname,assgorg,assgcountry,'NULL',str(n)]
        except:
            pass
        
        #Related U.S. Application Data - can be several
        try:
            appsrel = avail_fields['RLAP'].split('\n\n\n\n\n')
            for n in range(len(appsrel)):
                usappnum = 'NULL'
                usappdate = 'NULL'
                parentstatscode = 'NULL'
                parentpatnum = 'NULL'
                parentissdate = 'NULL'
                for line in appsrel[n].split("\n"):
                    if line.startswith("APN"):
                        usappnum = re.search('APN\s+(.*?)$',line).group(1)
                    
                    if line.startswith("APD"):
                        usappdate = re.search('(?<=\s)\w+',line).group()
                        usappdate = usappdate[:4]+'-'+usappdate[4:6]+'-'+usappdate[6:]
                    
                    if line.startswith("PSC"):
                        parentstatscode = re.search('PSC\s+(.*?)$',line).group(1)
                        
                    if line.startswith("PNO"):
                        parentpatnum = re.search('PNO\s+(.*?)$',line).group(1)
                    
                    if line.startswith("ISD"):
                        parentissdate = re.search('(?<=\s)\w+',line).group()
                        if parentissdate[6:] != "00":
                            parentissdate = parentissdate[:4]+'-'+parentissdate[4:6]+'-'+parentissdate[6:]
                        else:
                            parentissdate = parentissdate[:4]+'-'+parentissdate[4:6]+'-01'
                usappcitation[id_generator()] = [patent_id,"RELAPP"+patent_id+'-'+str(n),usappdate,"NULL","NULL",usappnum,'US','NULL',str(n)]
        except:
            pass
        
        #CLAS - should be several
        try:
            num = 0
            for line in avail_fields['CLAS'].split('\n'):
                if line.startswith('ICL'):
                    intsec = 'NULL'
                    mainclass = 'NULL'
                    subclass = 'NULL'
                    group = 'NULL'
                    subgroup = 'NULL'
                    
                    intclass = re.search('ICL\s+(.*?)$',line).group(1)
                    if int(year) <= 1984:
                        intsec = intclass[0]
                        mainclass = intclass[1:3]
                        if intsec == "D":
                            subclass = intclass[3:5]
                            group = "NULL"
                            subgroup = "NULL"
                        else:
                            subclass = intclass[3]
                            group = re.sub('^\s+','',intclass[4:7])
                            subgroup = re.sub('^\s+','',intclass[7:])
                    elif int(year) >= 1997 and updnum.startswith("D"):
                        intsec = "D"
                        try:
                            mainclass = intclass[0:2]
                            subclass = intclass[2:]
                            group = 'NULL'
                            subgroup = "NULL"
                        except:
                            mainclass = "NULL"
                            subclass = "NULL"
                            group = "NULL"
                            subgroup = "NULL"
                    else:
                        intsec = intclass[0]
                        mainclass = intclass[1:3]
                        subclass = intclass[3]
                        group = re.sub('^\s+','',intclass[4:7])
                        subgroup = re.sub('^\s+','',intclass[7:])
               
                    ipcr[id_generator()] = [patent_id,mainclass,intsec,subclass, group,subgroup,"NULL","NULL","NULL","NULL","NULL","NULL",str(num)]
                    num+=1     
                    
                if line.startswith("OCL"):
                    origmainclass = 'NULL'
                    origsubclass = 'NULL'
                    origclass = re.search('OCL\s+(.*?)$',line).group(1)
                    origmainclass = origclass[0:3]
                    origsubclass = origclass[3:]
                    mainclassdata[id_generator()] = [patent_id,origmainclass]
                    subclassdata[id_generator()] = [patent_id,origsubclass]
                    #print line
                    
                if line.startswith("XCL"):
                    crossrefmain = "NULL"
                    crossrefsub = "NULL"
                    crossrefclass = re.search('XCL\s+(.*?)$',line).group(1)
                    crossrefmain = crossrefclass[:3]
                    crossrefsub = crossrefclass[3:]
                    crossrefclassdata[id_generator()] = [patent_id,crossrefmain,crossrefsub]
                    
        except:
            pass
                       
        # U.S. Patent Reference - can be several
        try:
            uspatref = avail_fields['UREF'].split("\n\n\n\n\n")
            for n in range(len(uspatref)):
                refpatnum = 'NULL'
                refpatname = 'NULL'
                refpatdate = 'NULL'
                refpatclass = 'NULL'
                for line in uspatref[n].split("\n"):
                    if line.startswith('PNO'):
                        refpatnum = re.search('PNO\s+(.*?)$',line).group(1)
                    
                    if line.startswith('ISD'):
                        refpatdate = re.search('ISD\s+(.*?)$',line).group(1)
                        if refpatdate[6:] != '00':
                            refpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-'+refpatdate[6:]
                        else:
                            refpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-01'
                    
                    if line.startswith('NAM'):
                        refpatname = re.search('NAM\s+(.*?)$',line).group(1)
                        
                    if line.startswith('OCL'):
                        refpatclass = re.search('OCL\s+(.*?)$',line).group(1) 
                uspatentcitation[id_generator()] = [patent_id,'USPATCIT'+patent_id+'-'+str(n),refpatdate,refpatname,'U.S. PATENT',refpatnum,'US',refpatclass,str(n)]
        except:
            pass
        
        #Foreign reference - can be several
        try:
            foreignref = avail_fields['FREF'].split('\n\n\n\n\n')
            for n in range(len(foreignref)):
                forrefpatnum = 'NULL'
                forrefpatdate = 'NULL'
                forrefpatcountry = 'NULL'
                forrefpatclass = 'NULL'
                for line in foreignref[n].split("\n"):
                    if line.startswith('PNO'):
                        forrefpatnum = re.search('PNO\s+(.*?)$',line).group(1)
                    
                    if line.startswith('ISD'):
                        forrefpatdate = re.search('ISD\s+(.*?)$',line).group(1)
                        if forrefpatdate[6:]!='00':
                            forrefpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-'+refpatdate[6:]
                        else:
                            forrefpatdate = refpatdate[:4]+'-'+refpatdate[4:6]+'-01'
                        
                    if line.startswith('CNT'):
                        forrefpatcountry = re.search('CNT\s+(.*?)$',line).group(1)
                        
                    if line.startswith('ICL'):
                        forrefpatclass = re.search('ICL\s+(.*?)$',line).group(1) 
                foreigncitation[id_generator()] = [patent_id,forrefpatdate,"TAKE KIND FROM PATENT NUMBER?",forrefpatnum,forrefpatcountry,forrefpatclass,str(n)] 
        except:
            pass
        
        
        #Other reference - can be several
        try:
            otherreflist = avail_fields['OREF'].split('\n\n\n\n\n')
            for n in range(len(otherreflist)):
                otherref = 'NULL'
                otherref = re.sub('\s+',' ',otherreflist[n])
                otherref = re.sub('^\s+[A-Z]+\s+','',otherref)
                #print otherref
                otherreference[id_generator()] = [patent_id,otherref,str(n)]
        except:
            pass
        
        #Legal information - can be several
        try:
            legal_info = avail_fields['LREP'].split("\n\n\n\n\n")
            for n in range(len(legal_info)):
                legalcountry = 'NULL'
                legalfirm = 'NULL'
                attfname = 'NULL'
                attlname = 'NULL'
                for line in legal_info[n].split('\n'):
                    if line.startswith("FRM"):
                        legalfirm = re.search('FRM\s+(.*?)$',line).group(1)
                        #print legalfirm
                    if line.startswith("FR2"):
                        attorney = re.search('FR2\s+(.*?)$',line).group(1).split('; ')
                        attfname = attorney[1]
                        attlname = attorney[0]
                        #print attlname
                    if line.startswith('CNT'):
                        legalcountry = re.search('CNT\s+(.*?)$',line).group('; ')        
                
                rawlawyer[id_generator()] = ['LAWYER'+patent_id+'-'+str(n),patent_id,attfname,attlname,legalfirm,legalcountry,str(n)]
                    
        except:
            pass
        
        
        
        # Abstract - can be several
        abst = 'NULL'
        try:
            abst = re.sub('PAL\s+','',avail_fields['ABST'])
            abst = re.sub('PAR\s+','',abst)
            abst = re.sub('TBL\s+','',abst)
            abst = re.sub('\s+',' ',abst)
            
        except:
            pass
        
        patentdata[patent_id] = ['TAKE FROM KIND',updnum,'US',issdate,abst,title,re.sub('\d$','',updnum[0:2]),numclaims]
        
        patfile = csv.writer(open(fd2+'patent.csv','ab'))
        for k,v in patentdata.items():
            patfile.writerow([k]+v)
        
        appfile = csv.writer(open(fd2+'application.csv','ab'))
        for k,v in application.items():
            appfile.writerow([k]+v)
        
        rawlocfile = csv.writer(open(fd2+'rawlocation.csv','ab'))
        for k,v in rawlocation.items():
            rawlocfile.writerow([k]+v)
        
        rawinvfile = csv.writer(open(fd2+'rawinventor.csv','ab'))
        for k,v in rawinventor.items():
            rawinvfile.writerow([k]+v)
        
        rawassgfile = csv.writer(open(fd2+'rawassignee.csv','ab'))
        for k,v in rawassignee.items():
            rawassgfile.writerow([k]+v)
        
        usappcitfile = csv.writer(open(fd2+'usapplicationcitation.csv','ab'))
        for k,v in usappcitation.items():
            usappcitfile.writerow([k]+v)
        
        ipcrfile = csv.writer(open(fd2+'ipcr.csv','ab'))
        for k,v in ipcr.items():
            ipcrfile.writerow([k]+v)
        
        mainclassfile = csv.writer(open(fd2+'original_mainclass.csv','ab'))
        for k,v in mainclassdata.items():
            mainclassfile.writerow([k]+v)
        
        subclassfile = csv.writer(open(fd2+'original_subclass.csv','ab'))
        for k,v in subclassdata.items():
            subclassfile.writerow([k]+v)
        
        uspatentcitfile = csv.writer(open(fd2+'uspatentcitation.csv','ab'))
        for k,v in uspatentcitation.items():
            uspatentcitfile.writerow([k]+v)
        
        foreigncitfile = csv.writer(open(fd2+'foreigncitation.csv','ab'))
        for k,v in foreigncitation.items():
            foreigncitfile.writerow([k]+v)
        
        otherreffile = csv.writer(open(fd2+'otherreference.csv','ab'))
        for k,v in otherreference.items():
            otherreffile.writerow([k]+v)
        
        rawlawyerfile = csv.writer(open(fd2+'rawlawyer.csv','ab'))
        for k,v in rawlawyer.items():
            rawlawyerfile.writerow([k]+v)
        
        crossreffile = csv.writer(open(fd2+'crossreferenceclass_XCL.csv','ab'))
        for k,v in crossrefclassdata.items():
            crossreffile.writerow([k]+v)
        
      except:
          print>>error,i
          print i
          
error.close()
print numii