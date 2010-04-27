'''
client methods

'''

import os
import re
import sys
import time
import stat
import types
import random
import urllib
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import tempfile

import PLogger

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'

baseURLDQ2     = 'http://atlddmcat-reader.cern.ch/dq2'
baseURLDQ2SSL  = 'https://atlddmcat-writer.cern.ch:443/dq2'
baseURLSUBHome = "http://www.usatlas.bnl.gov/svn/panda/pathena"
baseURLSUB     = baseURLSUBHome+'/trf'
baseURLMON     = "http://panda.cern.ch:25980/server/pandamon/query"

# exit code
EC_Failed = 255

# default max size per job
maxTotalSize = long(14*1024*1024*1024)

# retrieve pathena config
try:
    # get default timeout
    defTimeOut = socket.getdefaulttimeout()
    # set timeout
    socket.setdefaulttimeout(60)
except:
    pass
# get panda server's name
try:
    getServerURL = baseURL + '/getServer'
    res = urllib.urlopen(getServerURL)
    # overwrite URL
    baseURLSSL = "https://%s/server/panda" % res.read()
except:
    type, value, traceBack = sys.exc_info()
    print type,value
    print "ERROR : could not getServer from %s" % getServerURL
    sys.exit(EC_Failed)
try:
    # reset timeout
    socket.setdefaulttimeout(defTimeOut)
except:
    pass


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print "No valid grid proxy certificate found"
    return ''


# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except:
        pass
    # get X509_CERT_DIR
    gridSrc = _getGridSrc()
    com = "%s echo $X509_CERT_DIR" % gridSrc
    tmpOut = commands.getoutput(com)
    return tmpOut.split('\n')[-1]


# keep list of tmp files for cleanup
globalTmpDir = ''


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl"'
        # verification of the host certificate
        self.verifyHost = False
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False

    # GET method
    def get(self,url,data):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()            
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # POST method
    def post(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        else:
            com += ' --capath %s' %  _x509_CApath()
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD,strData)
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print strData[:-1]
        s,o = commands.getstatusoutput(com)
        if o != '\x00':
            try:
                tmpout = urllib.unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print com
        # execute
        ret = commands.getstatusoutput(com)
        ret = self.convRet(ret)
        if self.verbose:
            print ret
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')            
        return ret
    

'''
public methods

'''

# get site specs
def getSiteSpecs(siteType=None):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getSiteSpecs'
    data = {}
    if siteType != None:
        data['siteType'] = siteType
    status,output = curl.get(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getSiteSpecs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# get cloud specs
def getCloudSpecs():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getCloudSpecs'
    status,output = curl.get(url,{})
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getCloudSpecs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr
                                                                

# get Panda Sites
tmpStat,PandaSites = getSiteSpecs()
if tmpStat != 0:
    print "ERROR : cannot get Panda Sites" 
    sys.exit(EC_Failed)

# get cloud info
tmpStat,PandaClouds = getCloudSpecs()
if tmpStat != 0:
    print "ERROR : cannot get Panda Clouds" 
    sys.exit(EC_Failed)


# get LRC
def getLRC(site):
    ret = None
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['dq2url'] in [None,"","None"]:
                ret = val['dq2url']
                break
    return ret


# get LFC
def getLFC(site):
    ret = None
    # use explicit matching for sitename
    if PandaSites.has_key(site):
        val = PandaSites[site]
        if not val['lfchost'] in [None,"","None"]:
            ret = val['lfchost']
            return ret
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['lfchost'] in [None,"","None"]:
                ret = val['lfchost']
                break
    return ret


# get SEs
def getSE(site):
    ret = []
    # use explicit matching for sitename
    if PandaSites.has_key(site):
        val = PandaSites[site]
        if not val['se'] in [None,"","None"]:
            for tmpSE in val['se'].split(','):
                match = re.search('.+://([^:/]+):*\d*/*',tmpSE)
                if match != None:
                    ret.append(match.group(1))
            return ret        
    # look for DQ2ID
    for id,val in PandaSites.iteritems():
        if id == site or val['ddm'] == site:
            if not val['se'] in [None,"","None"]:
                for tmpSE in val['se'].split(','):
                    match = re.search('.+://([^:/]+):*\d*/*',tmpSE)
                    if match != None:
                        ret.append(match.group(1))
                break
    # return
    return ret


# convert DQ2 ID to Panda siteid 
def convertDQ2toPandaID(site):
    keptSite = ''
    for tmpID,tmpSpec in PandaSites.iteritems():
        # # exclude long,xrootd,local queues
        if isExcudedSite(tmpID):
            continue
        # get list of DQ2 IDs
        srmv2ddmList = []
        for tmpDdmID in tmpSpec['setokens'].values():
            srmv2ddmList.append(convSrmV2ID(tmpDdmID))
        # use Panda sitename
        if convSrmV2ID(site) in srmv2ddmList:
            keptSite = tmpID
            # keep non-online site just in case
            if tmpSpec['status']=='online':
                return keptSite
    return keptSite


# convert to long queue
def convertToLong(site):
    tmpsite = re.sub('ANALY_','ANALY_LONG_',site)
    tmpsite = re.sub('_\d+$','',tmpsite)
    # if sitename exists
    if PandaSites.has_key(tmpsite):
        site = tmpsite
    return site


# submit jobs
def submitJobs(jobs,verbose=False):
    # set hostname
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/submitJobs'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR submitJobs : %s %s" % (type,value)
        return EC_Failed,None


# get job status
def getJobStatus(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getJobStatus : %s %s" % (type,value)
        return EC_Failed,None


# kill jobs
def killJobs(ids,verbose=False):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/killJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR killJobs : %s %s" % (type,value)
        return EC_Failed,None


# reassign jobs
def reassignJobs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR reassignJobs : %s %s" % (type,value)
        return EC_Failed,None


# query PandaIDs
def queryPandaIDs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryPandaIDs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR queryPandaIDs : %s %s" % (type,value)
        return EC_Failed,None


# query last files in datasets
def queryLastFilesInDataset(datasets,verbose=False):
    # serialize
    strDSs = pickle.dumps(datasets)
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose    
    # execute
    url = baseURL + '/queryLastFilesInDataset'
    data = {'datasets':strDSs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR queryLastFilesInDataset : %s %s" % (type,value)
        return EC_Failed,None


# put file
def putFile(file,verbose=False):
    # size check for noBuild
    sizeLimit = 10*1024*1024
    if not file.startswith('sources.'):
        fileSize = os.stat(file)[stat.ST_SIZE]
        if fileSize > sizeLimit:
            errStr  = 'Exceeded size limit (%sB >%sB). ' % (fileSize,sizeLimit)
            errStr += 'Your working directory contains too large files which cannot be put on cache area. '
            errStr += 'Please submit job without --noBuild/--libDS so that your files will be uploaded to SE'
            # get logger
            tmpLog = PLogger.getPandaLogger()
            tmpLog.error(errStr)
            return EC_Failed,'False'
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/putFile'
    data = {'file':file}
    return curl.put(url,data)


# delete file
def deleteFile(file):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/deleteFile'
    data = {'file':file}
    return curl.post(url,data)


# check dataset in map by ignoring case sensitivity
def checkDatasetInMap(name,outMap):
    try:
        for tmpKey in outMap.keys():
            if name.upper() == tmpKey.upper():
                return True
    except:
        pass
    return False


# get real dataset name from map by ignoring case sensitivity
def getDatasetValueInMap(name,outMap):
    for tmpKey in outMap.keys():
        if name.upper() == tmpKey.upper():
            return tmpKey
    # return original name    
    return name


# query files in dataset
def queryFilesInDataset(name,verbose=False,v_vuids=None,getDsString=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # for container failure
    status,out = 0,''
    nameVuidsMap = {}
    dsString = ''
    try:
        errStr = ''
        # get VUID
        if v_vuids == None:
            url = baseURLDQ2 + '/ws_repository/rpc'
            if re.search(',',name) != None:
                # comma-separated list
                names = name.split(',')
            elif name.endswith('/'):
                # container
                names = [name]
            else:
                names = [name]
            # loop over all names
            vuidList = []
            iLookUp = 0
            for tmpName in names:
                iLookUp += 1
                if iLookUp % 20 == 0:
                    time.sleep(1)
                data = {'operation':'queryDatasetByName','dsn':tmpName,
                        'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
                status,out = curl.get(url,data)
                if status != 0 or out == '\x00' or (re.search('\*',tmpName) == None and not checkDatasetInMap(tmpName,out)):
                    errStr = "ERROR : could not find %s in DQ2 DB. Check if the dataset name is correct" \
                             % tmpName
                    sys.exit(EC_Failed)
                # parse
                if re.search('\*',tmpName) == None:
                    # get real dataset name
                    tmpName = getDatasetValueInMap(tmpName,out)
                    vuidList.append(out[tmpName]['vuids'])
                    # mapping between name and vuids
                    nameVuidsMap[tuple(out[tmpName]['vuids'])] = tmpName
                    # string to expand wildcard                    
                    dsString += '%s,' % tmpName
                else:
                    # using wildcard
                    for outKeyName in out.keys():
                        # skip sub/dis
                        if re.search('_dis\d+$',outKeyName) != None or re.search('_sub\d+$',outKeyName) != None:
                            continue
                        # append
                        vuidList.append(out[outKeyName]['vuids'])
                        # mapping between name and vuids
                        nameVuidsMap[tuple(out[outKeyName]['vuids'])] = outKeyName
                        # string to expand wildcard
                        dsString += '%s,' % outKeyName
        else:
            vuidList = [v_vuids]
        # reset for backward comatiblity when * or , is not used
        if re.search('\*',name) == None and re.search(',',name) == None:
            nameVuidsMap = {}
            dsString = ''
        # get files
        url = baseURLDQ2 + '/ws_content/rpc'
        ret = {}
        generalLFNmap = {}
        iLookUp = 0
        for  vuids in vuidList:
            iLookUp += 1
            if iLookUp % 20 == 0:
                time.sleep(1)
            data = {'operation': 'queryFilesInDataset','vuids':vuids,
                    'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out =  curl.post(url,data)
            if status != 0:
                errStr = "ERROR : could not get files in %s" % name
                sys.exit(EC_Failed)
            # parse
            if out == '\x00' or len(out) < 2 or out==():
                # empty
                continue
            for guid,vals in out[0].iteritems():
                # remove attemptNr
                generalLFN = re.sub('\.\d+$','',vals['lfn'])
                # choose greater attempt to avoid duplication
                if generalLFNmap.has_key(generalLFN):
                    if vals['lfn'] > generalLFNmap[generalLFN]:
                        # remove lesser attempt
                        del ret[generalLFNmap[generalLFN]]
                    else:
                        continue
                # append to map
                generalLFNmap[generalLFN] = vals['lfn']
                ret[vals['lfn']] = {'guid'   : guid,
                                    'fsize'  : vals['filesize'],
                                    'md5sum' : vals['checksum']}
                # add dataset name
                if nameVuidsMap.has_key(tuple(vuids)):
                    ret[vals['lfn']]['dataset'] = nameVuidsMap[tuple(vuids)]
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)
    if getDsString:
        return ret,dsString[:-1]
    return ret            


# get datasets
def getDatasets(name,verbose=False,withWC=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
        status,out = curl.get(url,data)
        if status != 0:
            errStr = "ERROR : could not access DQ2 server"
            sys.exit(EC_Failed)
        # parse
        datasets = {}
        if out == '\x00' or ((not withWC) and (not checkDatasetInMap(name,out))):
            # no datasets
            return datasets
        # get VUIDs
        for dsname,idMap in out.iteritems():
            # check format
            if idMap.has_key('vuids') and len(idMap['vuids'])>0:
                datasets[dsname] = idMap['vuids'][0]
            else:
                # wrong format
                errStr = "ERROR : could not parse HTTP response for %s" % name
                sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)
    return datasets


# query files in shadow datasets associated to container
def getFilesInShadowDataset(contName,suffixShadow,verbose=False):
    fileList = []
    # get elements in container
    elements = getElementsFromContainer(contName,verbose)
    for tmpEle in elements:
        shadowDsName = "%s%s" % (tmpEle,suffixShadow)
        # check existence
        tmpDatasets = getDatasets(shadowDsName,verbose)
        if len(tmpDatasets) == 0:
            continue
        # get files in shadow dataset
        tmpList = queryFilesInDataset(shadowDsName,verbose)
        for tmpItem in tmpList:
            if not tmpItem in fileList:
                # append
                fileList.append(tmpItem)
        # query files in PandaDB
        tmpList = getFilesInUseForAnal(tmpEle,verbose)
        for tmpItem in tmpList:
            if not tmpItem in fileList:
                # append
                fileList.append(tmpItem)
    return fileList
    

# register dataset
def addDataset(name,verbose=False,location=''):
    # generate DUID/VUID
    duid = commands.getoutput("uuidgen")
    vuid = commands.getoutput("uuidgen")
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # add
        url = baseURLDQ2SSL + '/ws_repository/rpc'
        data = {'operation':'addDataset','dsn': name,'duid': duid,'vuid':vuid,
                'API':'0_3_0','tuid':commands.getoutput('uuidgen'),'update':'yes'}
        status,out = curl.post(url,data)
        if status != 0 or (out != None and re.search('Exception',out) != None):
            errStr = "ERROR : could not add dataset to DQ2 repository"
            sys.exit(EC_Failed)
        # add replica
        if re.search('SCRATCHDISK$',location) != None or re.search('USERDISK$',location) != None \
           or re.search('LOCALGROUPDISK$',location) != None:
            url = baseURLDQ2SSL + '/ws_location/rpc'
            data = {'operation':'addDatasetReplica','vuid':vuid,'site':location,
                    'complete':0,'transferState':1,
                    'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out = curl.post(url,data)
            if status != 0 or out != 1:
                errStr = "ERROR : could not register location : %s" % location
                sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# create dataset container
def createContainer(name,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # add
        url = baseURLDQ2SSL + '/ws_dq2/rpc'        
        data = {'operation':'container_create','name': name,
                'API':'030','tuid':commands.getoutput('uuidgen')}
        status,out = curl.post(url,data)
        if status != 0 or (out != None and re.search('Exception',out) != None):
            errStr = "ERROR : could not create container in DQ2"
            sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# add datasets to container
def addDatasetsToContainer(name,datasets,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # add
        url = baseURLDQ2SSL + '/ws_dq2/rpc'        
        data = {'operation':'container_register','name': name,
                'datasets':datasets,'API':'030',
                'tuid':commands.getoutput('uuidgen')}
        status,out = curl.post(url,data)
        if status != 0 or (out != None and re.search('Exception',out) != None):
            errStr = "ERROR : could not add DQ2 datasets to container"
            sys.exit(EC_Failed)
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# get container elements
def getElementsFromContainer(name,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get elements
        url = baseURLDQ2 + '/ws_dq2/rpc'
        data = {'operation':'container_retrieve','name': name,
                'API':'030','tuid':commands.getoutput('uuidgen')}
        status,out = curl.get(url,data)
        if status != 0 or (isinstance(out,types.StringType) and re.search('Exception',out) != None):
            errStr = "ERROR : could not get container %s from DQ2" % name
            sys.exit(EC_Failed)
        return out
    except:
        print status,out
        type, value, traceBack = sys.exc_info()
        print "%s %s" % (type,value)
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# convert srmv2 site to srmv1 site ID
def convSrmV2ID(tmpSite):
    # keep original name to avoid double conversion
    origSite = tmpSite
    # doesn't convert FR/IT/UK sites 
    for tmpPrefix in ['IN2P3-','INFN-','UKI-','GRIF-','DESY-','UNI-','RU-',
                      'LIP-','RO-']:
        if tmpSite.startswith(tmpPrefix):
            tmpSite = re.sub('_[A-Z,0-9]+DISK$', 'DISK',tmpSite)
            tmpSite = re.sub('_[A-Z,0-9]+TAPE$', 'DISK',tmpSite)
            tmpSite = re.sub('_PHYS-[A-Z,0-9]+$','DISK',tmpSite)
            tmpSite = re.sub('_PERF-[A-Z,0-9]+$','DISK',tmpSite)
            return tmpSite
    # patch for SRM v2
    tmpSite = re.sub('-[^-_]+_[A-Z,0-9]+DISK$', 'DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_[A-Z,0-9]+TAPE$', 'DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_PHYS-[A-Z,0-9]+$','DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_PERF-[A-Z,0-9]+$','DISK',tmpSite)
    # SHOULD BE REMOVED Once all sites and DQ2 migrate to srmv2
    # patch for BNL
    if tmpSite in ['BNLDISK','BNLTAPE']:
        tmpSite = 'BNLPANDA'
    # patch for LYON
    if tmpSite in ['LYONDISK','LYONTAPE']:
        tmpSite = 'IN2P3-CCDISK'
    # patch for TAIWAN
    if tmpSite.startswith('ASGC'):
        tmpSite = 'TAIWANDISK'
    # patch for CERN
    if tmpSite.startswith('CERN'):
        tmpSite = 'CERNDISK'
    # patche for some special sites where automatic conjecture is impossible
    if tmpSite == 'UVIC':
        tmpSite = 'VICTORIA'
    # US T2s
    if origSite == tmpSite:
        tmpSite = re.sub('_[A-Z,0-9]+DISK$', '',tmpSite)
        tmpSite = re.sub('_[A-Z,0-9]+TAPE$', '',tmpSite)
        tmpSite = re.sub('_PHYS-[A-Z,0-9]+$','',tmpSite)
        tmpSite = re.sub('_PERF-[A-Z,0-9]+$','',tmpSite)                
    if tmpSite == 'NET2':
        tmpSite = 'BU'
    # return
    return tmpSite


# get locations
def getLocations(name,fileList,cloud,woFileCheck,verbose=False,expCloud=False,getReserved=False,
                 getTapeSites=False,getDQ2IDs=False,locCandidates=None,removeDS=False,
                 removedDatasets=[]):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # get logger
    tmpLog = PLogger.getPandaLogger()
    try:
        errStr = ''
        names = name.split(',')
        # loop over all names
        retSites      = []
        retSiteMap    = {}
        resRetSiteMap = {}
        resBadStSites = {}
        resTapeSites  = []
        retDQ2IDs     = []
        countSite     = {}
        allOut        = {}
        iLookUp       = 0
        resUsedDsMap  = {}
        # convert candidates for SRM v2
        if locCandidates != None:
            locCandidatesSrmV2 = []
            for locTmp in locCandidates:
                locCandidatesSrmV2.append(convSrmV2ID(locTmp))
        # loop over all names        
        for tmpName in names:
            iLookUp += 1
            if iLookUp % 20 == 0:
                time.sleep(1)
            # container
            containerFlag = False
            if tmpName.endswith('/'):
                containerFlag = True
            # get VUID
            url = baseURLDQ2 + '/ws_repository/rpc'
            data = {'operation':'queryDatasetByName','dsn':tmpName,'version':0,
                    'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out = curl.get(url,data)
            if status != 0 or out == '\x00' or (not checkDatasetInMap(tmpName,out)):
                errStr = "ERROR : could not find %s in DQ2 DB. Check if the dataset name is correct" \
                         % tmpName
                if getReserved and getTapeSites:
                    sys.exit(EC_Failed)
                if verbose:
                    print errStr
                return retSites
            # get real datasetname
            tmpName = getDatasetValueInMap(tmpName,out)
            # parse
            duid  = out[tmpName]['duid']
            # get replica location
            url = baseURLDQ2 + '/ws_location/rpc'
            if containerFlag:
                data = {'operation':'listContainerReplicas','cn':tmpName,
                        'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            else:
                data = {'operation':'listDatasetReplicas','duid':duid,
                        'API':'0_3_0','tuid':commands.getoutput('uuidgen')}
            status,out = curl.post(url,data)
            if status != 0:
                errStr = "ERROR : could not query location for %s" % tmpName
                sys.exit(EC_Failed)
            # convert container format to dataset's one
            outTmp = {}
            if containerFlag:
                # count number of complete elements
                for tmpEleName,tmpEleVal in out.iteritems():
                    # ignore removed datasets
                    if tmpEleName in removedDatasets:
                        continue
                    for tmpEleVUID,tmpEleLocs in tmpEleVal.iteritems():
                        # get complete locations
                        for tmpEleLoc in tmpEleLocs[1]:
                            if not outTmp.has_key(tmpEleLoc):
                                outTmp[tmpEleLoc] = [{'found':0,'useddatasets':[]}]
                            # increment    
                            outTmp[tmpEleLoc][0]['found'] += 1
                            # append list
                            if not tmpEleName in outTmp[tmpEleLoc][0]['useddatasets']:
                                outTmp[tmpEleLoc][0]['useddatasets'].append(tmpEleName)
                        # use incomplete locations for user container if no complete replicas
                        if tmpEleLocs[1] == [] and (tmpEleName.startswith('user') or \
                                                    tmpEleName.startswith('group')):
                            for tmpEleLoc in tmpEleLocs[0]:
                                if not outTmp.has_key(tmpEleLoc):
                                    outTmp[tmpEleLoc] = [{'found':0,'useddatasets':[]}]
                                # increment
                                outTmp[tmpEleLoc][0]['found'] += 1
                                # append list
                                if not tmpEleName in outTmp[tmpEleLoc][0]['useddatasets']:
                                    outTmp[tmpEleLoc][0]['useddatasets'].append(tmpEleName)
                # replace
                out = outTmp
            # sum
            for tmpOutKey,tmpOutVar in out.iteritems():
                # protection against unchecked
                tmpNfound = tmpOutVar[0]['found']
                if not isinstance(tmpNfound,types.IntType):
                    tmpNfound = 1
                if allOut.has_key(tmpOutKey):
                    allOut[tmpOutKey][0]['found'] += tmpNfound
                else:
                    allOut[tmpOutKey] = [{'found':tmpNfound}]
                if tmpOutVar[0].has_key('useddatasets'):
                    if not allOut[tmpOutKey][0].has_key('useddatasets'):
                        allOut[tmpOutKey][0]['useddatasets'] = []
                    allOut[tmpOutKey][0]['useddatasets'] += tmpOutVar[0]['useddatasets']    
        # replace
        out = allOut
        if verbose:
            print out
        # choose sites where most files are available
        if not woFileCheck:
            tmpMaxFiles = -1
            for origTmpSite,origTmpInfo in out.iteritems():
                # get PandaID
                tmpPandaSite = convertDQ2toPandaID(origTmpSite)
                # check status
                if PandaSites.has_key(tmpPandaSite) and PandaSites[tmpPandaSite]['status'] == 'online':
                    # don't use TAPE
                    if re.search('TAPE$',origTmpSite) != None or \
                           re.search('_TZERO$',origTmpSite) != None or \
                           re.search('_DAQ$',origTmpSite) != None:
                        if not origTmpSite in resTapeSites:
                            resTapeSites.append(origTmpSite)
                        continue
                    # check the number of available files
                    if tmpMaxFiles < origTmpInfo[0]['found']:
                        tmpMaxFiles = origTmpInfo[0]['found']
            # remove sites
            for origTmpSite in out.keys():
                if out[origTmpSite][0]['found'] < tmpMaxFiles:
                    del out[origTmpSite]
            if verbose:
                print out
        tmpFirstDump = True
        for origTmpSite,origTmpInfo in out.iteritems():
            # don't use TAPE
            if re.search('TAPE$',origTmpSite) != None:
                if not origTmpSite in resTapeSites:
                    resTapeSites.append(origTmpSite)
                continue
            # collect DQ2 IDs
            if not origTmpSite in retDQ2IDs:
                retDQ2IDs.append(origTmpSite)
            # count number of available files
            if not countSite.has_key(origTmpSite):
                countSite[origTmpSite] = 0
            countSite[origTmpSite] += origTmpInfo[0]['found']
            # patch for SRM v2
            tmpSite = convSrmV2ID(origTmpSite)
            # if candidates are limited
            if locCandidates != None and (not tmpSite in locCandidatesSrmV2):
                continue
            if verbose:
                tmpLog.debug('%s : %s->%s' % (tmpName,origTmpSite,tmpSite))
            # check cloud, DQ2 ID and status
            for tmpID,tmpSpec in PandaSites.iteritems():
                # get list of DQ2 IDs
                srmv2ddmList = []
                for tmpDdmID in tmpSpec['setokens'].values():
                    srmv2ddmList.append(convSrmV2ID(tmpDdmID))
                # dump                        
                if tmpFirstDump:
                    if verbose:
                        pass
                if tmpSite in srmv2ddmList or convSrmV2ID(tmpSpec['ddm']).startswith(tmpSite):
                    # overwrite tmpSite for srmv1
                    tmpSite = convSrmV2ID(tmpSpec['ddm'])
                    # exclude long,xrootd,local queues
                    if isExcudedSite(tmpID):
                        continue
                    if not tmpSite in retSites:
                        retSites.append(tmpSite)
                    # just collect locations when file check is disabled
                    if woFileCheck:    
                        break
                    # append site
                    if tmpSpec['status'] == 'online':
                        # return sites in a cloud when it is specified or all sites
                        if tmpSpec['cloud'] == cloud or (not expCloud):
                            appendMap = retSiteMap
                        else:
                            appendMap = resRetSiteMap
                        # mapping between location and Panda siteID
                        if not appendMap.has_key(tmpSite):
                            appendMap[tmpSite] = []
                        if not tmpID in appendMap[tmpSite]:
                            appendMap[tmpSite].append(tmpID)
                        if not tmpID in resUsedDsMap and origTmpInfo[0].has_key('useddatasets'):
                            resUsedDsMap[tmpID] = origTmpInfo[0]['useddatasets']
                    else:
                        # not interested in another cloud
                        if tmpSpec['cloud'] != cloud and expCloud:
                            continue
                        # keep bad status sites for info
                        if not resBadStSites.has_key(tmpSpec['status']):
                            resBadStSites[tmpSpec['status']] = []
                        if not tmpID in resBadStSites[tmpSpec['status']]:    
                            resBadStSites[tmpSpec['status']].append(tmpID)
            tmpFirstDump = False
        # retrun DQ2 IDs
        if getDQ2IDs:
            return retDQ2IDs
        # return list when file check is not required
        if woFileCheck:
            return retSites
        # use reserved map when the cloud doesn't hold the dataset
        if retSiteMap == {} and (not expCloud) and (not getReserved):
            retSiteMap = resRetSiteMap
        # reset reserved map for expCloud
        if getReserved and expCloud:
            resRetSiteMap = {}
        # return map
        if verbose:
            if not getReserved:
                tmpLog.debug("getLocations -> %s" % retSiteMap)
            else:
                tmpLog.debug("getLocations pri -> %s" % retSiteMap)
                tmpLog.debug("getLocations sec -> %s" % resRetSiteMap)
        # print bad status sites for info    
        if retSiteMap == {} and resRetSiteMap == {} and resBadStSites != {}:
            tmpLog.warning("the following sites hold %s but they are not online" % name)
            for tmpStatus,tmpSites in resBadStSites.iteritems():
                print "   status=%s : %s" % (tmpStatus,tmpSites)
        if not getReserved:        
            return retSiteMap
        elif not getTapeSites:
            return retSiteMap,resRetSiteMap
        elif not removeDS:
            return retSiteMap,resRetSiteMap,resTapeSites
        else:
            return retSiteMap,resRetSiteMap,resTapeSites,resUsedDsMap            
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            type, value, traceBack = sys.exc_info()
            print "ERROR : invalid DQ2 response - %s %s" % (type,value)
        sys.exit(EC_Failed)
                

#@ Returns number of events per file in a given dataset
#SP 2006
#
def nEvents(name, verbose=False, askServer=True, fileList = {}, scanDir = '.', askUser=True):
    
    # @  These declarations can be moved to the configuration section at the very beginning
    # Here just for code clarity
    #
    # Parts of the query
    str1="/?dset="
    str2="&get=evperfile"
    # Form full query string
    m_query = baseURLMON+str1+name+str2
    manualEnter = True
    # Send query get number of events per file
    if askServer:
        nEvents=urllib.urlopen(m_query).read()
        if verbose:
            print m_query
            print nEvents
        if re.search('HTML',nEvents) == None and nEvents != '-1':
            manualEnter = False            
    else:
        # use ROOT to get # of events
        try:
            import ROOT
            rootFile = ROOT.TFile("%s/%s" % (scanDir,fileList[0]))
            tree = ROOT.gDirectory.Get( 'CollectionTree' )
            nEvents = tree.GetEntriesFast()
            # disable
            if nEvents > 0:
                manualEnter = False
        except:
            if verbose:
                type, value, traceBack = sys.exc_info()
                print "ERROR : could not get nEvents with ROOT - %s %s" % (type,value)
    # In case of error PANDAMON server returns full HTML page
    # Normally return an integer
    if manualEnter:
        if askUser:
            if askServer:
                print "Could not get the # of events from MetaDB for %s " % name
            while True:
                str = raw_input("Enter the number of events per file : ")
                try:
                    nEvents = int(str)
                    break
                except:
                    pass
        else:
            print "ERROR : Could not get the # of events from MetaDB for %s " % name
            sys.exit(EC_Failed)
    if verbose:
       print "Dataset ", name, "has ", nEvents, " per file"
    return int(nEvents)


# get PFN from LRC
def _getPFNsLRC(lfns,dq2url,verbose):
    pfnMap   = {}
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # get PoolFileCatalog
    iLFN = 0
    strLFNs = ''
    url = dq2url + 'lrc/PoolFileCatalog'
    firstError = True
    # check if GUID lookup is supported
    useGUID = True
    status,out = curl.get(url,{'guids':'test'})
    if status ==0 and out == 'Must GET or POST a list of LFNs!':
        useGUID = False
    for lfn,vals in lfns.iteritems():
        iLFN += 1
        # make argument
        if useGUID:
            strLFNs += '%s ' % vals['guid']
        else:
            strLFNs += '%s ' % lfn
        if iLFN % 40 == 0 or iLFN == len(lfns):
            # get PoolFileCatalog
            strLFNs = strLFNs.rstrip()
            if useGUID:
                data = {'guids':strLFNs}
            else:
                data = {'lfns':strLFNs}
            # avoid too long argument
            strLFNs = ''
            # execute
            status,out = curl.get(url,data)
            time.sleep(2)
            if out.startswith('Error'):
                # LFN not found
                continue
            if status != 0 or (not out.startswith('<?xml')):
                if firstError:
                    print status,out
                    print "ERROR : LRC %s returned invalid response" % dq2url
                    firstError = False
                continue
            # parse
            try:
                root  = xml.dom.minidom.parseString(out)
                files = root.getElementsByTagName('File')
                for file in files:
                    # get PFN and LFN nodes
                    physical = file.getElementsByTagName('physical')[0]
                    pfnNode  = physical.getElementsByTagName('pfn')[0]
                    logical  = file.getElementsByTagName('logical')[0]
                    lfnNode  = logical.getElementsByTagName('lfn')[0]
                    # convert UTF8 to Raw
                    pfn = str(pfnNode.getAttribute('name'))
                    lfn = str(lfnNode.getAttribute('name'))
                    # remove /srm/managerv1?SFN=
                    pfn = re.sub('/srm/managerv1\?SFN=','',pfn)
                    # append
                    pfnMap[lfn] = pfn
            except:
                print status,out
                type, value, traceBack = sys.exc_info()
                print "ERROR : could not parse XML - %s %s" % (type, value)
                sys.exit(EC_Failed)
    # return        
    return pfnMap


# get list of missing LFNs from LRC
def getMissLFNsFromLRC(files,url,verbose=False,nFiles=0):
    # get PFNs
    pfnMap = _getPFNsLRC(files,url,verbose)
    # check Files
    missFiles = []
    for file in files:
        if not file in pfnMap.keys():
            missFiles.append(file)
    return missFiles
                

# get PFN list from LFC
def _getPFNsLFC(fileMap,site,explicitSE,verbose=False,nFiles=0):
    pfnMap = {}
    for path in sys.path:
        # look for base package
        basePackage = __name__.split('.')[-2]
        if os.path.exists(path) and basePackage in os.listdir(path):
            lfcClient = '%s/%s/LFCclient.py' % (path,basePackage)
            if explicitSE:
                stList = getSE(site)
            else:
                stList = []
            lfcHost   = getLFC(site)
            inFile    = '%s_in'  % commands.getoutput('uuidgen')
            outFile   = '%s_out' % commands.getoutput('uuidgen')
            # write GUID/LFN
            ifile = open(inFile,'w')
            fileKeys = fileMap.keys()
            fileKeys.sort()
            for lfn in fileKeys:
                vals = fileMap[lfn]
                ifile.write('%s %s\n' % (vals['guid'],lfn))
            ifile.close()
            # construct command
            gridSrc = _getGridSrc()
            com = '%s python -Wignore %s -l %s -i %s -o %s -n %s' % (gridSrc,lfcClient,lfcHost,inFile,outFile,nFiles)
            for index,stItem in enumerate(stList):
                if index != 0:
                    com += ',%s' % stItem
                else:
                    com += ' -s %s' % stItem
            if verbose:
                com += ' -v'
                print com
            # exeute
            status = os.system(com)
            if status == 0:
                ofile = open(outFile)
                line = ofile.readline()
                line = re.sub('\n','',line)
                exec 'pfnMap = %s' %line
                ofile.close()
            # remove tmp files    
            try:    
                os.remove(inFile)
                os.remove(outFile)
            except:
                pass
            # failed
            if status != 0:
                print "ERROR : failed to access LFC %s" % lfcHost
                sys.exit(EC_Failed)
            break
    # return
    return pfnMap


# get list of missing LFNs from LFC
def getMissLFNsFromLFC(fileMap,site,explicitSE,verbose=False,nFiles=0):
    missList = []
    # get PFNS
    pfnMap = _getPFNsLFC(fileMap,site,explicitSE,verbose,nFiles)
    for lfn,vals in fileMap.iteritems():
        if not vals['guid'] in pfnMap.keys():
            missList.append(lfn)
    # return
    return missList
    

# get grid source file
def _getGridSrc():
    # set Grid setup.sh if needed
    status,out = commands.getstatusoutput('which voms-proxy-info')
    stLFC,outLFC = commands.getstatusoutput('python -c "import lfc"')
    if status == 0 and stLFC == 0:
        gridSrc = ''
        status,athenaPath = commands.getstatusoutput('which athena.py')
        if status == 0 and athenaPath.startswith('/afs/in2p3.fr'):
            # for LYON, to avoid missing LD_LIBRARY_PATH
            gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
        elif status == 0 and athenaPath.startswith('/afs/cern.ch'):
            # for CERN, VDT is already installed
            gridSrc = '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh'
    else:
        # set Grid setup.sh
        if os.environ.has_key('PATHENA_GRID_SETUP_SH'):
            gridSrc = os.environ['PATHENA_GRID_SETUP_SH']
        else:
            if not os.environ.has_key('CMTSITE'):
                print "ERROR : CMTSITE is no defined in envvars"
                return False
            if os.environ['CMTSITE'] == 'CERN':
		gridSrc = '/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh'
            elif os.environ['CMTSITE'] == 'BNL':
                gridSrc = '/afs/usatlas.bnl.gov/osg/client/@sys/current/setup.sh'
            else:
                # try to determin site using path to athena
                status,athenaPath = commands.getstatusoutput('which athena.py')
                if status == 0 and athenaPath.startswith('/afs/in2p3.fr'):
                    # LYON
                    gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
                else:
                    print "ERROR : PATHENA_GRID_SETUP_SH is not defined in envvars"
                    print "  for CERN : export PATHENA_GRID_SETUP_SH=/afs/cern.ch/project/gd/LCG-share/current/etc/profile.d/grid_env.sh"                
                    print "  for LYON : export PATHENA_GRID_SETUP_SH=/afs/in2p3.fr/grid/profiles/lcg_env.sh"
                    print "  for BNL  : export PATHENA_GRID_SETUP_SH=/afs/usatlas.bnl.gov/osg/client/@sys/current/setup.sh"
                    return False
    # check grid-proxy
    if gridSrc != '':
        gridSrc = 'source %s;' % gridSrc
        # some grid_env.sh doen't correct PATH/LD_LIBRARY_PATH
        gridSrc = "unset LD_LIBRARY_PATH; unset PYTHONPATH; export PATH=/usr/local/bin:/bin:/usr/bin; %s" % gridSrc
    # return
    return gridSrc


# get DN
def getDN(origString):
    shortName = ''
    distinguishedName = ''
    for line in origString.split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = re.sub('\.','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) != None:
                # look for full name
                distinguishedName = distinguishedName.replace(' ','')
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # return
    return distinguishedName



from HTMLParser import HTMLParser

class _monHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.map = {}
        self.switch = False
        self.td = False

    def getMap(self):
        retMap = {}
        if len(self.map) > 1:
            names = self.map[0]
            vals  = self.map[1]
            # values
            try:
                retMap['total']    = int(vals[names.index('Jobs')])
            except:
                retMap['total']    = 0
            try:    
                retMap['finished'] = int(vals[names.index('Finished')])
            except:
                retMap['finished'] = 0
            try:    
                retMap['failed']   = int(vals[names.index('Failed')])
            except:
                retMap['failed']   = 0
            retMap['running']  = retMap['total'] - retMap['finished'] - \
                                 retMap['failed']
        return retMap

    def handle_data(self, data):
        if self.switch:
            if self.td:
                self.td = False
                self.map[len(self.map)-1].append(data)
            else:
                self.map[len(self.map)-1][-1] += data
        else:
            if data == "Job Sets:":
                self.switch = True
        
    def handle_starttag(self, tag, attrs):
        if self.switch and tag == 'tr':
            self.map[len(self.map)] = []
        if self.switch and tag == 'td':
            self.td = True

    def handle_endtag(self, tag):
        if self.switch and self.td:
            self.map[len(self.map)-1].append("")
            self.td = False

# get jobInfo from Mon
def getJobStatusFromMon(id,verbose=False):
    # get name
    shortName = ''
    distinguishedName = ''
    for line in commands.getoutput('%s grid-proxy-info -identity' % _getGridSrc()).split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) != None:
                # look for full name
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    data = {'job':'*',
            'jobDefinitionID' : id,
            'user' : distinguishedName,
            'days' : 100}
    # execute
    status,out = curl.get(baseURLMON,data)
    if status != 0 or re.search('Panda monitor and browser',out)==None:
        return {}
    # parse
    parser = _monHTMLParser()
    for line in out.split('\n'):
        if re.search('Job Sets:',line) != None:
            parser.feed( line )
            break
    return parser.getMap()


# run brokerage
def runBrokerage(sites,atlasRelease,cmtConfig=None,verbose=False,trustIS=False,cacheVer='',processingType=''):
    if sites == []:
        return 0,'ERROR : no candidate'
    # choose at most 20 sites randomly to avoid too many lookup
    random.shuffle(sites)
    sites = sites[:20]
    # serialize
    strSites = pickle.dumps(sites)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/runBrokerage'
    data = {'sites':strSites,
            'atlasRelease':atlasRelease}
    if cmtConfig != None:
        data['cmtConfig'] = cmtConfig
    if trustIS:
        data['trustIS'] = True
    if cacheVer != '':
        # change format if needed
        cacheVer = re.sub('^-','',cacheVer)
        match = re.search('^([^_]+)_(\d+\.\d+\.\d+\.\d+\.*\d*)$',cacheVer)
        if match != None:
            cacheVer = '%s-%s' % (match.group(1),match.group(2))
        # use cache for brokerage
        data['atlasRelease'] = cacheVer
    if processingType != '':
        # set processingType mainly for HC
        data['processingType'] = processingType
    return curl.get(url,data)


# run rebrokerage
def runReBrokerage(jobID,libDS='',cloud=None,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/runReBrokerage'
    data = {'jobID':jobID}
    if cloud != None:
        data['cloud'] = cloud
    if not libDS in ['',None,'NULL']:
        data['libDS'] = libDS
    retVal = curl.get(url,data)
    # communication error
    if retVal[0] != 0:
        return retVal
    # succeeded
    if retVal[1] == True:
        return 0,''
    # server error
    errMsg = retVal[1]
    if errMsg.startswith('ERROR: '):
        # remove ERROR:
        errMsg = re.sub('ERROR: ','',errMsg)
    return EC_Failed,errMsg


# exclude long,xrootd,local queues
def isExcudedSite(tmpID):
    excludedSite = False
    for exWord in ['ANALY_LONG_','_LOCAL','_test']:
        if re.search(exWord,tmpID,re.I) != None:
            excludedSite = True
            break
    return excludedSite


# get default space token
def getDefaultSpaceToken(fqans,defaulttoken):
    # mapping is not defined
    if defaulttoken == '':
        return ''
    # loop over all groups
    for tmpStr in defaulttoken.split(','):
        # extract group and token
        items = tmpStr.split(':')
        if len(items) != 2:
            continue
        tmpGroup = items[0]
        tmpToken = items[1]
        # look for group
        if re.search(tmpGroup+'/',fqans) != None:
            return tmpToken
    # not found
    return ''


# use dev server
def useDevServer():
    global baseURL
    baseURL = 'http://voatlas48.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://voatlas48.cern.ch:25443/server/panda'    


# set server
def setServer(urls):
    global baseURL
    baseURL = urls.split(',')[0]
    global baseURLSSL
    baseURLSSL = urls.split(',')[-1]
    

# register proxy key
def registerProxyKey(credname,origin,myproxy,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    curl.verifyHost = True
    # execute
    url = baseURLSSL + '/registerProxyKey'
    data = {'credname': credname,
            'origin'  : origin,
            'myproxy' : myproxy
            }
    return curl.post(url,data)


# get proxy key
def getProxyKey(verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/getProxyKey'
    status,output = curl.post(url,{})
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getProxyKey : %s %s" % (type,value)
        return EC_Failed,None


# add site access
def addSiteAccess(siteID,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/addSiteAccess'
    data = {'siteID': siteID}
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR listSiteAccess : %s %s" % (type,value)
        return EC_Failed,None


# list site access
def listSiteAccess(siteID,verbose=False,longFormat=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/listSiteAccess'
    data = {}
    if siteID != None:
        data['siteID'] = siteID
    if longFormat:
        data['longFormat'] = True
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR listSiteAccess : %s %s" % (type,value)
        return EC_Failed,None


# update site access
def updateSiteAccess(method,siteid,userName,verbose=False,value=''):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/updateSiteAccess'
    data = {'method':method,'siteid':siteid,'userName':userName}
    if value != '':
        data['attrValue'] = value
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,output
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR updateSiteAccess : %s %s" % (type,value)
        return EC_Failed,None


# site access map
SiteAcessMapForWG = None
    
# add allowed sites
def addAllowedSites(verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    if verbose:
        tmpLog.debug('check site access')
    # get access list
    global SiteAcessMapForWG
    SiteAcessMapForWG = {}
    tmpStatus,tmpOut = listSiteAccess(None,verbose,True)
    if tmpStatus != 0:
        return False
    global PandaSites
    for tmpVal in tmpOut:
        tmpID = tmpVal['primKey']
        # keep info to map 
        SiteAcessMapForWG[tmpID] = tmpVal
        # set online if the site is allowed
        if tmpVal['status']=='approved':
           if PandaSites.has_key(tmpID):
               PandaSites[tmpID]['status'] = 'online'
               if verbose:
                   tmpLog.debug('set %s online' % tmpID)
    return True


# check permission
def checkSiteAccessPermission(siteName,workingGroup,verbose):
    # get site access if needed
    if SiteAcessMapForWG == None:
        ret = addAllowedSites(verbose)
        if not ret:
            return True
    # don't check if site name is undefined
    if siteName == None:
        return True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    if verbose:
        tmpLog.debug('checking site access permission')
        tmpLog.debug('site=%s workingGroup=%s map=%s' % (siteName,workingGroup,str(SiteAcessMapForWG)))
    # check
    if (not SiteAcessMapForWG.has_key(siteName)) or SiteAcessMapForWG[siteName]['status'] != 'approved':
        errStr = "You don't have permission to send jobs to %s with workingGroup=%s. " % (siteName,workingGroup)
        # allowed member only
        if PandaSites[siteName]['accesscontrol'] == 'grouplist':
            tmpLog.error(errStr)
            return False
        else:
            # reset workingGroup
            if not workingGroup in ['',None]:
                errStr += 'Resetting workingGroup to None'
                tmpLog.warning(errStr)
            return True
    elif not workingGroup in ['',None]:
        # check workingGroup
        wgList = SiteAcessMapForWG[siteName]['workingGroups'].split(',')
        if not workingGroup in wgList:
            errStr  = "Invalid workingGroup=%s. Must be one of %s. " % (workingGroup,str(wgList))
            errStr += 'Resetting workingGroup to None'
            tmpLog.warning(errStr)
            return True
    # no problems
    return True

        
# get JobIDs in a time range
def getJobIDsInTimeRange(timeRange,dn=None,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getJobIDsInTimeRange'
    data = {'timeRange':timeRange}
    if dn != None:
        data['dn'] = dn
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getJobIDsInTimeRange : %s %s" % (type,value)
        return EC_Failed,None


# get PandaIDs for a JobID
def getPandIDsWithJobID(jobID,dn=None,nJobs=0,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getPandIDsWithJobID'
    data = {'jobID':jobID, 'nJobs':nJobs}
    if dn != None:
        data['dn'] = dn
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,None
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getPandIDsWithJobID : %s %s" % (type,value)
        return EC_Failed,None


# get full job status
def getFullJobStatus(ids,verbose):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getFullJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getFullJobStatus : %s %s" % (type,value)
        return EC_Failed,None


# get slimmed file info
def getSlimmedFileInfoPandaIDs(ids,verbose):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getSlimmedFileInfoPandaIDs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getSlimmedFileInfoPandaIDs : %s %s" % (type,value)
        return EC_Failed,None


# get input files currently in used for analysis
def getFilesInUseForAnal(outDataset,verbose):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getFilesInUseForAnal'
    data = {'outDataset':outDataset}
    status,output = curl.post(url,data)
    try:
        return pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getFilesInUseForAnal : %s %s" % (type,value)
        sys.exit(EC_Failed)


# set tmp dir
def setGlobalTmpDir(tmpDir):
    global globalTmpDir
    globalTmpDir = tmpDir


# exclude site
def excludeSite(excludedSite):
    if excludedSite == '':
        return
    # sites composed of long/short queues
    compSites = ['CERN','LYON','BNL']
    # remove sites
    global PandaSites
    for tmpPatt in excludedSite.split(','):
        # check if it is a composite
        for tmpComp in compSites:
            if tmpComp in tmpPatt:
                # use generic ID to remove all queues
                tmpPatt = tmpComp
                break
        sites = PandaSites.keys()
        for site in sites:
            # look for pattern
            if tmpPatt in site:
                try:
                    del PandaSites[site]
                except:
                    pass


# use certain sites
def useCertainSites(sitePat):
    if re.search(',',sitePat) == None:
        return sitePat,[]
    # remove sites
    global PandaSites
    sites = PandaSites.keys()
    cloudsForRandom = []
    for site in sites:
        # look for pattern
        useThisSite = False
        for tmpPatt in sitePat.split(','):
            if tmpPatt in site:
                useThisSite = True
                break
        # delete
        if not useThisSite:
            PandaSites[site]['status'] = 'skip'
        else:
            if not PandaSites[site]['cloud'] in cloudsForRandom:
                cloudsForRandom.append(PandaSites[site]['cloud'])
    # return
    return 'AUTO',cloudsForRandom


# get client version
def getPandaClientVer(verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getPandaClientVer'
    status,output = curl.get(url,{})
    # failed
    if status != 0:
        return status,output
    # check format
    if re.search('^\d+\.\d+\.\d+$',output) == None:
        return EC_Failed,"invalid version '%s'" % output
    # return
    return status,output


# get files in dataset with filte
def getFilesInDatasetWithFilter(inDS,filter,shadowList,inputFileListName,verbose,dsStringFlag=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # query files in dataset
    tmpLog.info("query files in %s" % inDS)
    if dsStringFlag:
        inputFileMap,inputDsString = queryFilesInDataset(inDS,verbose,getDsString=True)
    else:
        inputFileMap = queryFilesInDataset(inDS,verbose)
    # read list of files to be used
    filesToBeUsed = []
    if inputFileListName != '':
        rFile = open(inputFileListName)
        for line in rFile:
            line = re.sub('\n','',line)
            filesToBeUsed.append(line)
        rFile.close()
    # get list of filters
    filters = []
    if filter != '':
        filters = filter.split(',')
    # remove redundant files
    tmpKeys = inputFileMap.keys()
    for tmpLFN in tmpKeys:
        # remove log
        if re.search('\.log(\.tgz)*(\.\d+)*$',tmpLFN) != None or \
               re.search('\.log(\.\d+)*(\.tgz)*$',tmpLFN) != None:
            del inputFileMap[tmpLFN]            
            continue
        # filename matching
        if filter != '':
            matchFlag = False
            for tmpFilter in filters:
                if re.search(tmpFilter,tmpLFN) != None:
                    matchFlag = True
                    break
            if not matchFlag:    
                del inputFileMap[tmpLFN]
                continue
        # files in shadow
        if tmpLFN in shadowList:
            if inputFileMap.has_key(tmpLFN):
                del inputFileMap[tmpLFN]            
            continue
        # files to be used
        if filesToBeUsed != []:
            # check matching    
            matchFlag = False
            for pattern in filesToBeUsed:
                # normal matching
                if pattern == tmpLFN:
                    matchFlag =True
                    break
            # doesn't match
            if not matchFlag:
                del inputFileMap[tmpLFN]
    # no files in filelist are available
    if inputFileMap == {} and (filter != '' or inputFileListName != ''):
        if inputFileListName != '':
            errStr =  "No files in %s are available in %s. " % (inputFileListName,inDS)
        else:
            errStr =  "%s are not available in %s. " % (filters,inDS)
        errStr += "Make sure if you specify correct LFNs"            
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    if dsStringFlag:
        return inputFileMap,inputDsString
    return inputFileMap


# check if DQ2-free site
def isDQ2free(site):
    if PandaSites.has_key(site) and PandaSites[site]['ddm'] == 'local':
        return True
    return False


# check queued analysis jobs at a site
def checkQueuedAnalJobs(site,verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getQueuedAnalJobs'
    data = {'site':site}
    status,output = curl.post(url,data)
    try:
        # get queued analysis
        queuedMap = pickle.loads(output)
        if queuedMap.has_key('running') and queuedMap.has_key('queued'):
            if queuedMap['running'] > 20 and queuedMap['queued'] > 2 * queuedMap['running']:
                warStr  = 'Your job might be delayed since %s is busy. ' % site
                warStr += 'There are %s jobs already queued by other users while %s jobs are running. ' \
                          % (queuedMap['queued'],queuedMap['running'])
                warStr += 'Please consider replicating the input dataset to a free site '
                warStr += 'or avoiding the --site/--cloud option so that the brokerage will '
                warStr += 'find a free site'
                tmpLog.warning(warStr)
    except:
        type, value, traceBack = sys.exc_info()
        tmpLog.error("checkQueuedAnalJobs %s %s" % (type,value))


# get latest DBRelease
def getLatestDBRelease(verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('trying to get the latest version number for DBRelease=LATEST')
    # get ddo datasets
    ddoDatasets = getDatasets('ddo.*',verbose,True)
    if ddoDatasets == {}:
        tmpLog.error('failed to get a list of DBRelease datasets from DQ2')
        sys.exit(EC_Failed)
    # reverse sort to avoid redundant lookup   
    ddoDatasets = ddoDatasets.keys()
    ddoDatasets.sort()
    ddoDatasets.reverse()
    # extract version number
    latestVerMajor = 0
    latestVerMinor = 0
    latestVerBuild = 0
    latestVerRev   = 0
    latestDBR = ''
    for tmpName in ddoDatasets:
        match = re.search('\.v(\d+)_*[^\.]*$',tmpName)
        if match == None:
            tmpLog.warning('cannot extract version number from %s' % tmpName)
            continue
        # get major,minor,build,revision numbers
        tmpVerStr = match.group(1)
        tmpVerMajor = 0
        tmpVerMinor = 0
        tmpVerBuild = 0
        tmpVerRev   = 0
        try:
            tmpVerMajor = int(tmpVerStr[0:2])
        except:
            pass
        try:
            tmpVerMinor = int(tmpVerStr[2:4])
        except:
            pass
        try:
            tmpVerBuild = int(tmpVerStr[4:6])
        except:
            pass
        try:
            tmpVerRev = int(tmpVerStr[6:])
        except:
            pass
        # compare
        if latestVerMajor > tmpVerMajor:
            continue
        elif latestVerMajor == tmpVerMajor:
            if latestVerMinor > tmpVerMinor:
                continue
            elif latestVerMinor == tmpVerMinor:
                if latestVerBuild > tmpVerBuild:
                    continue
                elif latestVerBuild == tmpVerBuild:
                    if latestVerRev > tmpVerRev:
                        continue
        # check replica locations to use well distributed DBRelease. i.e. to avoid DBR just created
        tmpLocations = getLocations(tmpName,[],'',False,verbose,getDQ2IDs=True)
        if len(tmpLocations) < 10:
            continue
        # check contents to exclude reprocessing DBR
        tmpDbrFileMap = queryFilesInDataset(tmpName,verbose)
        if len(tmpDbrFileMap) != 1 or not tmpDbrFileMap.keys()[0].startswith('DBRelease'):
            continue
        # higher or equal version
        latestVerMajor = tmpVerMajor
        latestVerMinor = tmpVerMinor
        latestVerBuild = tmpVerBuild
        latestVerRev   = tmpVerRev
        latestDBR = tmpName
    # failed
    if latestDBR == '':
        tmpLog.error('failed to get the latest version of DBRelease dataset from DQ2')
        sys.exit(EC_Failed)
    # get DBRelease file name
    tmpList = queryFilesInDataset(latestDBR,verbose)
    if len(tmpList) == 0:
        tmpLog.error('DBRelease=%s is empty' % latestDBR)
        sys.exit(EC_Failed)
    # retrun dataset:file
    retVal = '%s:%s' % (latestDBR,tmpList.keys()[0])
    tmpLog.info('use %s' % retVal)
    return retVal
