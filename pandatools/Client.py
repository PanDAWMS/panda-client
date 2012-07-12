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
import struct
import commands
import cPickle as pickle
import xml.dom.minidom
import socket
import tempfile

import MiscUtils
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
baseURLSUB     = "http://pandaserver.cern.ch:25080/trf/user"
baseURLMON     = "http://panda.cern.ch:25980/server/pandamon/query"
baseURLCSRV    = "http://pandacache.cern.ch:25080/server/panda"
baseURLCSRVSSL = "http://pandacache.cern.ch:25443/server/panda"

# exit code
EC_Failed = 255

# default max size per job
maxTotalSize = long(14*1024*1024*1024)

# safety size for input size calculation
safetySize = long(500*1024*1024)

# suffix for shadow dataset
suffixShadow = "_shadow"

# limit on maxCpuCount
maxCpuCountLimit = 1000000000
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
# get panda cache server's name
try:
    getServerURL = baseURLCSRV + '/getServer'
    res = urllib.urlopen(getServerURL)
    # overwrite URL
    baseURLCSRVSSL = "https://%s/server/panda" % res.read()
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
        self.path = 'curl --user-agent "dqcurl" '
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
    def get(self,url,data,rucioAccount=False):
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
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):    
                data['appid'] = os.environ['RUCIO_APPID']
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
    def post(self,url,data,rucioAccount=False):
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
        # add rucio account info
        if rucioAccount:
            if os.environ.has_key('RUCIO_ACCOUNT'):
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if os.environ.has_key('RUCIO_APPID'):    
                data['appid'] = os.environ['RUCIO_APPID']
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
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')            
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')            
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
                                                                

# refresh spacs at runtime
def refreshSpecs():
    global PandaSites
    global PandaClouds
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


# initialize spacs
refreshSpecs()


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


# convert DQ2 ID to Panda site IDs 
def convertDQ2toPandaIDList(site):
    sites = []
    sitesOff = []
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
            # append
            if tmpSpec['status']=='online':
                if not tmpID in sites:
                    sites.append(tmpID)
            else:
                # keep non-online site just in case
                if not tmpID in sitesOff:
                    sitesOff.append(tmpID)
    # return
    if sites != []:
        return sites
    return sitesOff


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
def putFile(file,verbose=False,useCacheSrv=False,reuseSandbox=False):
    # size check for noBuild
    sizeLimit = 10*1024*1024
    fileSize = os.stat(file)[stat.ST_SIZE]
    if not file.startswith('sources.'):
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
    # check duplicationn
    if reuseSandbox:
        # get CRC
        fo = open(file)
        fileContent = fo.read()
        fo.close()
        footer = fileContent[-8:]
        checkSum,isize = struct.unpack("II",footer)
        # check duplication
        url = baseURLSSL + '/checkSandboxFile'
        data = {'fileSize':fileSize,'checkSum':checkSum}
        status,output = curl.post(url,data)
        if status != 0:
            return EC_Failed,'ERROR: Could not check Sandbox duplication with %s' % status
        elif output.startswith('FOUND:'):
            # found reusable sandbox
            hostName,reuseFileName = output.split(':')[1:]
            # set cache server hostname
            global baseURLCSRVSSL
            baseURLCSRVSSL = "https://%s:25443/server/panda" % hostName
            # return reusable filename
            return 0,"NewFileName:%s" % reuseFileName
    # execute
    if useCacheSrv:
        url = baseURLCSRVSSL + '/putFile'
    else:
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
def queryFilesInDataset(name,verbose=False,v_vuids=None,getDsString=False,dsStringOnly=False):
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
                        'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
                status,out = curl.get(url,data,rucioAccount=True)
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
        if dsStringOnly:
            return dsString[:-1]
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
                    'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
            status,out =  curl.post(url,data,rucioAccount=True)
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
def getDatasets(name,verbose=False,withWC=False,onlyNames=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
        if onlyNames:
            data['API'] = '30'
            data['onlyNames'] = int(onlyNames)
        status,out = curl.get(url,data,rucioAccount=True)
        if status != 0:
            errStr = "ERROR : could not access DQ2 server"
            sys.exit(EC_Failed)
        # parse
        datasets = {}
        if out == '\x00' or ((not withWC) and (not checkDatasetInMap(name,out))):
            # no datasets
            return datasets
        # get names only
        if isinstance(out,types.DictionaryType): 
            return out
        else:
            # wrong format
            errStr = "ERROR : DQ2 didn't give a dictionary for %s" % name
            sys.exit(EC_Failed)
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

# disable expiring file check
globalUseShortLivedReplicas = False
def useExpiringFiles():
    global globalUseShortLivedReplicas
    globalUseShortLivedReplicas = True
    
# get expiring files
globalCompleteDsMap = {}
globalExpFilesMap = {}
globalExpOkFilesMap = {}
globalExpCompDq2FilesMap = {}
def getExpiringFiles(dsStr,removedDS,siteID,verbose,getOKfiles=False):
    # convert * in dsStr
    if re.search('\*',dsStr) != None:
        dsStr = queryFilesInDataset(dsStr,verbose,dsStringOnly=True)
    # reuse map
    global globalExpFilesMap
    global globalExpOkFilesMap
    global expCompDq2FilesList
    global globalUseShortLivedReplicas
    mapKey = (dsStr,siteID)
    if globalExpFilesMap.has_key(mapKey):
        if getOKfiles:
            return globalExpFilesMap[mapKey],globalExpOkFilesMap[mapKey],globalExpCompDq2FilesMap[mapKey]
        return globalExpFilesMap[mapKey]
    # get logger
    tmpLog = PLogger.getPandaLogger()
    if verbose:
        tmpLog.debug("checking metadata for %s, removed=%s " % (dsStr,str(removedDS)))
    # get DQ2 location and used data
    tmpLocations,dsUsedDsMap = getLocations(dsStr,[],'',False,verbose,getDQ2IDs=True,
                                            removedDatasets=removedDS,
                                            useOutContainer=True,
                                            includeIncomplete=True,
                                            notSiteStatusCheck=True)
    # get all sites matching with site's DQ2ID here, to work with brokeroff sites
    fullSiteList = convertDQ2toPandaIDList(PandaSites[siteID]['ddm'])
    # get datasets at the site
    datasets = []
    for tmpDsUsedDsMapKey,tmpDsUsedDsVal in dsUsedDsMap.iteritems():
        siteMatched = False
        for tmpTargetID in fullSiteList:
            # check with short/long siteID
            if tmpDsUsedDsMapKey in [tmpTargetID,convertToLong(tmpTargetID)]:
                datasets = tmpDsUsedDsVal
                siteMatched = True
                break
        if siteMatched:
            break
    # not found    
    if datasets == []:
        tmpLog.error("cannot find datasets at %s for replica metadata check" % siteID)
        sys.exit(EC_Failed)
    # loop over all datasets
    convertedOrigSite = convSrmV2ID(PandaSites[siteID]['ddm'])
    expFilesMap = {'datasets':[],'files':[]}
    expOkFilesList = []
    expCompDq2FilesList = []
    for dsName in datasets:
        # get DQ2 IDs for the siteID
        dq2Locations = []
        if tmpLocations.has_key(dsName):
            for tmpLoc in tmpLocations[dsName]:
                # check Panda site IDs
                for tmpPandaSiteID in convertDQ2toPandaIDList(tmpLoc):
                    if tmpPandaSiteID in fullSiteList:
                        if not tmpLoc in dq2Locations:
                            dq2Locations.append(tmpLoc)
                        break
                # check prefix mainly for MWT2 and MWT2_UC    
                convertedScannedID = convSrmV2ID(tmpLoc)
                if convertedOrigSite.startswith(convertedScannedID) or \
                       convertedScannedID.startswith(convertedOrigSite):
                    if not tmpLoc in dq2Locations:
                        dq2Locations.append(tmpLoc)
        # empty
        if dq2Locations == []:
            tmpLog.error("cannot find replica locations for %s:%s to check metadata" % (siteID,dsName))
            sys.exit(EC_Failed)
        # check completeness
        compInDQ2 = False
        global globalCompleteDsMap
        if globalCompleteDsMap.has_key(dsName):
            for tmpDQ2Loc in dq2Locations:
                if tmpDQ2Loc in globalCompleteDsMap[dsName]:
                    compInDQ2 = True
                    break
        # get metadata
        metaList = getReplicaMetadata(dsName,dq2Locations,verbose)
        # check metadata
        metaOK = False
        for metaItem in metaList:
            # replica deleted
            if isinstance(metaItem,types.StringType) and "No replica found at the location" in metaItem:
                continue
            if not globalUseShortLivedReplicas:
                # check the archived attribute
                if isinstance(metaItem['archived'],types.StringType) and metaItem['archived'].lower() in ['tobedeleted',]:
                    continue
                # check replica lifetime
                if metaItem.has_key('expirationdate') and isinstance(metaItem['expirationdate'],types.StringType):
                    try:
                        import datetime
                        expireDate = datetime.datetime.strptime(metaItem['expirationdate'],'%Y-%m-%d %H:%M:%S')
                        # expire in 7 days
                        if expireDate-datetime.datetime.utcnow() < datetime.timedelta(days=7):
                            continue
                    except:
                        pass
            # all OK
            metaOK = True
            break
        # expiring
        if not metaOK:
            # get files
            expFilesMap['datasets'].append(dsName)
            expFilesMap['files'] += queryFilesInDataset(dsName,verbose)
        else:
            tmpFilesList = queryFilesInDataset(dsName,verbose)
            expOkFilesList += tmpFilesList
            # complete
            if compInDQ2:
                expCompDq2FilesList += tmpFilesList
    # keep to avoid redundant lookup
    globalExpFilesMap[mapKey] = expFilesMap
    globalExpOkFilesMap[mapKey] = expOkFilesList
    globalExpCompDq2FilesMap[mapKey] = expCompDq2FilesList
    if expFilesMap['datasets'] != []:
        msgStr = 'ignore replicas of '
        for tmpDsStr in expFilesMap['datasets']:
            msgStr += '%s,' % tmpDsStr
        msgStr = msgStr[:-1]
        msgStr += ' at %s due to archived=ToBeDeleted or short lifetime < 7days. ' % siteID
        msgStr += 'If you want to use those replicas in spite of short lifetime, use --useShortLivedReplicas'
        tmpLog.info(msgStr)
    # return
    if getOKfiles:
        return expFilesMap,expOkFilesList,expCompDq2FilesList
    return expFilesMap
    

# get replica metadata
def getReplicaMetadata(name,dq2Locations,verbose):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    if verbose:
        tmpLog.debug("getReplicaMetadata for %s" % (name))
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    try:
        errStr = ''
        # get VUID
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
        status,out = curl.get(url,data,rucioAccount=True)
        if status != 0:
            errStr = "ERROR : could not access DQ2 server"
            sys.exit(EC_Failed)
        # parse
        datasets = {}
        if out == '\x00' or not checkDatasetInMap(name,out):
            errStr = "ERROR : VUID for %s was not found in DQ2" % name
            sys.exit(EC_Failed)
        # get VUIDs
        vuid = out[name]['vuids'][0]
        # get replica metadata
        retList = []
        for location in dq2Locations:
            url = baseURLDQ2 + '/ws_location/rpc'
            data = {'operation':'queryDatasetReplicaMetadata','vuid':vuid,
                    'location':location,'API':'0_3_0',
                    'tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.post(url,data,rucioAccount=True)
            if status != 0:
                errStr = "ERROR : could not access DQ2 server to get replica metadata"
                sys.exit(EC_Failed)
            # append
            retList.append(out)
        # return
        return retList
    except:
        print status,out
        if errStr != '':
            print errStr
        else:
            print "ERROR : invalid DQ2 response"
        sys.exit(EC_Failed)


# query files in shadow datasets associated to container
def getFilesInShadowDataset(contName,suffixShadow,verbose=False):
    fileList = []
    # query files in PandaDB first to get running/failed files + files which are being added
    tmpList = getFilesInUseForAnal(contName,verbose)
    for tmpItem in tmpList:
        if not tmpItem in fileList:
            # append
            fileList.append(tmpItem)
    # get elements in container
    elements = getElementsFromContainer(contName,verbose)
    for tmpEle in elements:
        # remove merge
        tmpEle = re.sub('\.merge$','',tmpEle)
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
    return fileList
    

# query files in shadow dataset associated to old dataset
def getFilesInShadowDatasetOld(outDS,suffixShadow,verbose=False):
    shadowList = []    
    # query files in PandaDB first to get running/failed files + files which are being added
    tmpShadowList = getFilesInUseForAnal(outDS,verbose)
    for tmpItem in tmpShadowList:
        shadowList.append(tmpItem)
    # query files in shadow dataset        
    for tmpItem in queryFilesInDataset("%s%s" % (outDS,suffixShadow),verbose):
        if not tmpItem in shadowList:
            shadowList.append(tmpItem)
    return shadowList        


# list datasets by GUIDs
def listDatasetsByGUIDs(guids,dsFilter,verbose=False,forColl=False):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # get filter
    dsFilters = []
    if dsFilter != '':
        dsFilters = dsFilter.split(',')
    # get logger
    tmpLog = PLogger.getPandaLogger()
    retMap = {}
    allMap = {}
    iLookUp = 0
    guidLfnMap = {}
    checkedDSList = []
    # loop over all GUIDs
    for guid in guids:
        # check existing map to avid redundant lookup
        if guidLfnMap.has_key(guid):
            retMap[guid] = guidLfnMap[guid]
            continue
        iLookUp += 1
        if iLookUp % 20 == 0:
            time.sleep(1)
        # get vuids
        url = baseURLDQ2 + '/ws_content/rpc'
        data = {'operation': 'queryDatasetsWithFileByGUID','guid':guid,
                'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
        status,out =  curl.get(url,data,rucioAccount=True)
        # failed
        if status != 0:
            if not verbose:
                print status,out
            errStr = "could not get dataset vuids for %s" % guid
            tmpLog.error(errStr)
            sys.exit(EC_Failed)
        # GUID was not registered in DQ2
        if out == '\x00' or out == ():
            if verbose:            
                errStr = "DQ2 gave an empty list for GUID=%s" % guid
                tmpLog.debug(errStr)
            allMap[guid] = []
            continue
        tmpVUIDs = list(out)
        # get dataset name
        url = baseURLDQ2 + '/ws_repository/rpc'
        data = {'operation':'queryDatasetByVUIDs','vuids':tmpVUIDs,
                'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
        status,out = curl.post(url,data,rucioAccount=True)
        # failed
        if status != 0:
            if not verbose:
                print status,out
            errStr = "could not get dataset name for %s" % guid
            tmpLog.error(errStr)
            sys.exit(EC_Failed)
        # empty
        if out == '\x00':
            errStr = "DQ2 gave an empty list for VUID=%s" % tmpVUIDs
            tmpLog.error(errStr)
            sys.exit(EC_Failed)
        # datasets are deleted
        if out == {}:
            allMap[guid] = []
            continue
        # check with filter
        tmpDsNames = []
        tmpAllDsNames = []
        for tmpDsName in out.keys():
            # ignore junk datasets
            if tmpDsName.startswith('panda') or \
                   tmpDsName.startswith('user') or \
                   tmpDsName.startswith('group') or \
                   re.search('_sub\d+$',tmpDsName) != None or \
                   re.search('_dis\d+$',tmpDsName) != None or \
                   re.search('_shadow$',tmpDsName) != None:
                continue
            tmpAllDsNames.append(tmpDsName)
            # check with filter            
            if dsFilters != []:
                flagMatch = False
                for tmpFilter in dsFilters:
                    # replace . to \.                    
                    tmpFilter = tmpFilter.replace('.','\.')                    
                    # replace * to .*
                    tmpFilter = tmpFilter.replace('*','.*')
                    if re.search('^'+tmpFilter,tmpDsName) != None:
                        flagMatch = True
                        break
                # not match
                if not flagMatch:
                    continue
            # append    
            tmpDsNames.append(tmpDsName)
        # empty
        if tmpDsNames == []:
            # there may be multiple GUIDs for the same event, and some may be filtered by --eventPickDS
            allMap[guid] = tmpAllDsNames
            continue
        # duplicated
        if len(tmpDsNames) != 1:
            if not forColl:
                errStr = "there are multiple datasets %s for GUID:%s. Please set --eventPickDS and/or --eventPickStreamName to choose one dataset"\
                         % (str(tmpAllDsNames),guid)
            else:
                errStr = "there are multiple datasets %s for GUID:%s. Please set --eventPickDS to choose one dataset"\
                         % (str(tmpAllDsNames),guid)
            tmpLog.error(errStr)
            sys.exit(EC_Failed)
        # get LFN
        if not tmpDsNames[0] in checkedDSList:
            tmpMap = queryFilesInDataset(tmpDsNames[0],verbose)
            for tmpLFN,tmpVal in tmpMap.iteritems():
                guidLfnMap[tmpVal['guid']] = (tmpDsNames[0],tmpLFN)
            checkedDSList.append(tmpDsNames[0])
        # append
        if not guidLfnMap.has_key(guid):
            errStr = "LFN for %s in not found in %s" % (guid,tmpDsNames[0])
            tmpLog.error(errStr)
            sys.exit(EC_Failed)
        retMap[guid] = guidLfnMap[guid]
    # return
    return retMap,allMap

                                
# register dataset
def addDataset(name,verbose=False,location='',dsExist=False,allowProdDisk=False,dsCheck=True):
    # generate DUID/VUID
    duid = MiscUtils.wrappedUuidGen()
    vuid = MiscUtils.wrappedUuidGen()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    try:
        errStr = ''
        # add
        if not dsExist:
            url = baseURLDQ2SSL + '/ws_repository/rpc'
            nTry = 3
            for iTry in range(nTry):
                data = {'operation':'addDataset','dsn': name,'duid': duid,'vuid':vuid,
                        'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen(),'update':'yes'}
                status,out = curl.post(url,data,rucioAccount=True)
                if not dsCheck and out != None and re.search('DQDatasetExistsException',out) != None:
                    dsExist = True
                    break
                elif status != 0 or (out != None and re.search('Exception',out) != None):
                    if iTry+1 == nTry:
                        errStr = "ERROR : could not add dataset to DQ2 repository"
                        sys.exit(EC_Failed)
                    time.sleep(20)    
                else:
                    break
        # get VUID        
        if dsExist:
            # check location
            tmpLocations = getLocations(name,[],'',False,verbose,getDQ2IDs=True)
            if location in tmpLocations:
                return
            # get VUID
            url = baseURLDQ2 + '/ws_repository/rpc'
            data = {'operation':'queryDatasetByName','dsn':name,'version':0,
                    'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.get(url,data,rucioAccount=True)
            if status != 0:
                errStr = "ERROR : could not get VUID from DQ2"
                sys.exit(EC_Failed)
            # parse
            vuid = out[name]['vuids'][0]
        # add replica
        if re.search('SCRATCHDISK$',location) != None or re.search('USERDISK$',location) != None \
           or re.search('LOCALGROUPDISK$',location) != None \
           or (allowProdDisk and (re.search('PRODDISK$',location) != None or \
                                  re.search('DATADISK$',location) != None)):
            url = baseURLDQ2SSL + '/ws_location/rpc'
            nTry = 3
            for iTry in range(nTry):
                data = {'operation':'addDatasetReplica','vuid':vuid,'site':location,
                        'complete':0,'transferState':1,
                        'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
                status,out = curl.post(url,data,rucioAccount=True)
                if status != 0 or out != 1:
                    if iTry+1 == nTry:
                        errStr = "ERROR : could not register location : %s" % location
                        sys.exit(EC_Failed)
                    time.sleep(20)
                else:
                    break
        else:
            errStr = "ERROR : registration at %s is disallowed" % location
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
        nTry = 3
        for iTry in range(nTry):
            data = {'operation':'container_create','name': name,
                    'API':'030','tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.post(url,data,rucioAccount=True)
            if status != 0 or (out != None and re.search('Exception',out) != None):
                if iTry+1 == nTry:
                    errStr = "ERROR : could not create container in DQ2"
                    sys.exit(EC_Failed)
                time.sleep(20)
            else:
                break
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
        nTry = 3
        for iTry in range(nTry):
            data = {'operation':'container_register','name': name,
                    'datasets':datasets,'API':'030',
                    'tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.post(url,data,rucioAccount=True)
            if status != 0 or (out != None and re.search('Exception',out) != None):
                if iTry+1 == nTry:
                    errStr = "ERROR : could not add DQ2 datasets to container"
                    sys.exit(EC_Failed)
                time.sleep(20)
            else:
                break
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
                'API':'030','tuid':MiscUtils.wrappedUuidGen()}
        status,out = curl.get(url,data,rucioAccount=True)
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
            tmpSite = re.sub('_DET-[A-Z,0-9]+$', 'DISK',tmpSite)
            tmpSite = re.sub('_SOFT-[A-Z,0-9]+$','DISK',tmpSite)
            tmpSite = re.sub('_TRIG-DAQ$','DISK',tmpSite)            
            return tmpSite
    # parch for CERN EOS
    if tmpSite.startswith('CERN-PROD_EOS'):
        return 'CERN-PROD_EOSDISK'
    # parch for CERN TMP
    if tmpSite.startswith('CERN-PROD_TMP'):
        return 'CERN-PROD_TMPDISK'
    # parch for CERN OLD
    if tmpSite.startswith('CERN-PROD_OLD') or tmpSite.startswith('CERN-PROD_LOCAL'):
        return 'CERN-PROD_OLDDISK'
    # patch for SRM v2
    tmpSite = re.sub('-[^-_]+_[A-Z,0-9]+DISK$', 'DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_[A-Z,0-9]+TAPE$', 'DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_PHYS-[A-Z,0-9]+$','DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_PERF-[A-Z,0-9]+$','DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_DET-[A-Z,0-9]+$', 'DISK',tmpSite)
    tmpSite = re.sub('-[^-_]+_SOFT-[A-Z,0-9]+$','DISK',tmpSite)        
    tmpSite = re.sub('-[^-_]+_TRIG-DAQ$','DISK',tmpSite)    
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
        tmpSite = re.sub('_DET-[A-Z,0-9]+$', '',tmpSite)
        tmpSite = re.sub('_SOFT-[A-Z,0-9]+$','',tmpSite)                
        tmpSite = re.sub('_TRIG-DAQ$','',tmpSite)
    if tmpSite == 'NET2':
        tmpSite = 'BU'
    if tmpSite == 'MWT2_UC':
        tmpSite = 'MWT2'
    # return    
    return tmpSite


# check tape sites
def isTapeSite(origTmpSite):
    if re.search('TAPE$',origTmpSite) != None or \
           re.search('PROD_TZERO$',origTmpSite) != None or \
           re.search('PROD_DAQ$',origTmpSite) != None:
        return True
    return False


# check online site
def isOnlineSite(origTmpSite):
    # get PandaID
    tmpPandaSite = convertDQ2toPandaID(origTmpSite)
    # check if Panda site
    if not PandaSites.has_key(tmpPandaSite):
        return False
    # exclude long,local queues
    if isExcudedSite(tmpPandaSite):
        return False
    # status    
    if PandaSites[tmpPandaSite]['status'] == 'online':
        return True
    return False
                

# get locations
def getLocations(name,fileList,cloud,woFileCheck,verbose=False,expCloud=False,getReserved=False,
                 getTapeSites=False,getDQ2IDs=False,locCandidates=None,removeDS=False,
                 removedDatasets=[],useOutContainer=False,includeIncomplete=False,
                 notSiteStatusCheck=False):
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
        resTapeSites  = {}
        retDQ2IDs     = []
        retDQ2IDmap   = {}
        allOut        = {}
        iLookUp       = 0
        resUsedDsMap  = {}
        global globalCompleteDsMap
        # convert candidates for SRM v2
        if locCandidates != None:
            locCandidatesSrmV2 = []
            for locTmp in locCandidates:
                locCandidatesSrmV2.append(convSrmV2ID(locTmp))
        # loop over all names        
        for tmpName in names:
            # ignore removed datasets
            if tmpName in removedDatasets:
                continue
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
                    'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.get(url,data,rucioAccount=True)
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
                        'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
            else:
                data = {'operation':'listDatasetReplicas','duid':duid,
                        'API':'0_3_0','tuid':MiscUtils.wrappedUuidGen()}
            status,out = curl.post(url,data,rucioAccount=True)
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
                        tmpFoundFlag = False
                        for tmpEleLoc in tmpEleLocs[1]:
                            # don't use TAPE
                            if isTapeSite(tmpEleLoc):
                                if not resTapeSites.has_key(tmpEleLoc):
                                    resTapeSites[tmpEleLoc] = []
                                if not tmpEleName in resTapeSites[tmpEleLoc]:    
                                    resTapeSites[tmpEleLoc].append(tmpEleName)
                                continue
                            # append
                            if not outTmp.has_key(tmpEleLoc):
                                outTmp[tmpEleLoc] = [{'found':0,'useddatasets':[]}]
                            # increment    
                            outTmp[tmpEleLoc][0]['found'] += 1
                            # append list
                            if not tmpEleName in outTmp[tmpEleLoc][0]['useddatasets']:
                                outTmp[tmpEleLoc][0]['useddatasets'].append(tmpEleName)
                            # found online site    
                            if isOnlineSite(tmpEleLoc):
                                tmpFoundFlag = True
                            # add to global map
                            if not globalCompleteDsMap.has_key(tmpEleName):
                                globalCompleteDsMap[tmpEleName] = []
                            globalCompleteDsMap[tmpEleName].append(tmpEleLoc)    
                        # use incomplete locations if no complete replica at online sites
                        if includeIncomplete or not tmpFoundFlag:
                            for tmpEleLoc in tmpEleLocs[0]:
                                # don't use TAPE
                                if isTapeSite(tmpEleLoc):
                                    if not resTapeSites.has_key(tmpEleLoc):
                                        resTapeSites[tmpEleLoc] = []
                                    if not tmpEleName in resTapeSites[tmpEleLoc]:    
                                        resTapeSites[tmpEleLoc].append(tmpEleName)
                                    continue
                                # append
                                if not outTmp.has_key(tmpEleLoc):
                                    outTmp[tmpEleLoc] = [{'found':0,'useddatasets':[]}]
                                # increment
                                outTmp[tmpEleLoc][0]['found'] += 1
                                # append list
                                if not tmpEleName in outTmp[tmpEleLoc][0]['useddatasets']:
                                    outTmp[tmpEleLoc][0]['useddatasets'].append(tmpEleName)
            else:
                # check completeness
                tmpIncompList = []
                tmpFoundFlag = False
                for tmpOutKey,tmpOutVar in out.iteritems():
                    # don't use TAPE
                    if isTapeSite(tmpOutKey):
                        if not resTapeSites.has_key(tmpOutKey):
                            resTapeSites[tmpOutKey] = []
                        if not tmpName in resTapeSites[tmpOutKey]:
                            resTapeSites[tmpOutKey].append(tmpName)
                        continue
                    # protection against unchecked
                    tmpNfound = tmpOutVar[0]['found']
                    # complete or not
                    if isinstance(tmpNfound,types.IntType) and tmpNfound == tmpOutVar[0]['total']:
                        outTmp[tmpOutKey] = [{'found':1,'useddatasets':[tmpName]}]
                        # found online site
                        if isOnlineSite(tmpOutKey):
                            tmpFoundFlag = True
                        # add to global map
                        if not globalCompleteDsMap.has_key(tmpName):
                            globalCompleteDsMap[tmpName] = []
                        globalCompleteDsMap[tmpName].append(tmpOutKey)
                    else:
                        # keep just in case
                        if not tmpOutKey in tmpIncompList:
                            tmpIncompList.append(tmpOutKey)
                # use incomplete replicas when no complete at online sites
                if includeIncomplete or not tmpFoundFlag:
                    for tmpOutKey in tmpIncompList:
                        outTmp[tmpOutKey] = [{'found':1,'useddatasets':[tmpName]}]
            # replace
            out = outTmp
            # sum
            for tmpOutKey,tmpOutVar in out.iteritems():
                if not allOut.has_key(tmpOutKey):
                    allOut[tmpOutKey] = [{'found':0,'useddatasets':[]}]
                allOut[tmpOutKey][0]['found'] += tmpOutVar[0]['found']
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
                if PandaSites.has_key(tmpPandaSite) and (notSiteStatusCheck or PandaSites[tmpPandaSite]['status'] == 'online'):
                    # don't use TAPE
                    if isTapeSite(origTmpSite):
                        if not resTapeSites.has_key(origTmpSite):
                            if origTmpInfo[0].has_key('useddatasets'):
                                resTapeSites[origTmpSite] = origTmpInfo[0]['useddatasets']
                            else:
                                resTapeSites[origTmpSite] = names
                        continue
                    # check the number of available files
                    if tmpMaxFiles < origTmpInfo[0]['found']:
                        tmpMaxFiles = origTmpInfo[0]['found']
            # remove sites
            for origTmpSite in out.keys():
                if out[origTmpSite][0]['found'] < tmpMaxFiles:
                    # use sites where most files are avaialble if output container is not used
                    if not useOutContainer:
                        del out[origTmpSite]
            if verbose:
                print out
        tmpFirstDump = True
        for origTmpSite,origTmpInfo in out.iteritems():
            # don't use TAPE
            if isTapeSite(origTmpSite):
                if not resTapeSites.has_key(origTmpSite):
                    resTapeSites[origTmpSite] = origTmpInfo[0]['useddatasets']
                continue
            # collect DQ2 IDs
            if not origTmpSite in retDQ2IDs:
                retDQ2IDs.append(origTmpSite)
            for tmpUDS in origTmpInfo[0]['useddatasets']:    
                if not retDQ2IDmap.has_key(tmpUDS):
                    retDQ2IDmap[tmpUDS] = []
                if not origTmpSite in retDQ2IDmap[tmpUDS]:
                    retDQ2IDmap[tmpUDS].append(origTmpSite)
            # patch for SRM v2
            tmpSite = convSrmV2ID(origTmpSite)
            # if candidates are limited
            if locCandidates != None and (not tmpSite in locCandidatesSrmV2):
                continue
            if verbose:
                tmpLog.debug('%s : %s->%s' % (tmpName,origTmpSite,tmpSite))
            # check cloud, DQ2 ID and status
            tmpSiteBeforeLoop = tmpSite 
            for tmpID,tmpSpec in PandaSites.iteritems():
                # reset
                tmpSite = tmpSiteBeforeLoop
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
                    if tmpSpec['status'] == 'online' or notSiteStatusCheck:
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
                        if origTmpInfo[0].has_key('useddatasets'):
                            if not tmpID in resUsedDsMap:
                                resUsedDsMap[tmpID] = []
                            resUsedDsMap[tmpID] += origTmpInfo[0]['useddatasets']
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
            if includeIncomplete:
                return retDQ2IDmap,resUsedDsMap
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
            msgFirstFlag = True
            for tmpStatus,tmpSites in resBadStSites.iteritems():
                # ignore panda secific site
                if tmpStatus.startswith('panda_'):
                    continue
                if msgFirstFlag:
                    tmpLog.warning("the following sites hold %s but they are not online" % name)
                    msgFirstFlag = False
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
                str = raw_input("Enter the number of events per file (or set --nEventsPerFile) : ")
                try:
                    nEvents = int(str)
                    break
                except:
                    pass
        else:
            print "ERROR : Could not get the # of events from MetaDB for %s " % name
            sys.exit(EC_Failed)
    if verbose:
       print "Dataset %s has %s evetns per file" % (name,nEvents)
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
        if os.path.exists(path) and os.path.isdir(path) and basePackage in os.listdir(path):
            lfcClient = '%s/%s/LFCclient.py' % (path,basePackage)
            if explicitSE:
                stList = getSE(site)
            else:
                stList = []
            lfcHost   = getLFC(site)
            inFile    = '%s_in'  % MiscUtils.wrappedUuidGen()
            outFile   = '%s_out' % MiscUtils.wrappedUuidGen()
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
                return {}
            break
    # return
    return pfnMap


# get list of missing LFNs from LFC
def getMissLFNsFromLFC(fileMap,site,explicitSE,verbose=False,nFiles=0,shadowList=[],dsStr='',removedDS=[],
                       skipScan=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    missList = []
    # ignore files in shadow
    if shadowList != []:
        tmpFileMap = {}
        for lfn,vals in fileMap.iteritems():
            if not lfn in shadowList:
                tmpFileMap[lfn] = vals
    else:
        tmpFileMap = fileMap
    # ignore expiring files
    if dsStr != '':
        tmpTmpFileMap = {}
        expFilesMap,expOkFilesList,expCompInDQ2FilesList = getExpiringFiles(dsStr,removedDS,site,verbose,getOKfiles=True)
        # collect files in incomplete replicas
        for lfn,vals in tmpFileMap.iteritems():
            if lfn in expOkFilesList and not lfn in expCompInDQ2FilesList:
                tmpTmpFileMap[lfn] = vals
        tmpFileMap = tmpTmpFileMap
        # skipScan use only complete replicas
        if skipScan and expCompInDQ2FilesList == []:
            tmpLog.info("%s may hold %s files at most in incomplete replicas but they are not used when --skipScan is set" % \
                        (site,len(expOkFilesList)))
    # get PFNS
    if tmpFileMap != {} and not skipScan:
        tmpLog.info("scanning LFC %s for files in incompete datasets at %s" % (getLFC(site),site))
        pfnMap = _getPFNsLFC(tmpFileMap,site,explicitSE,verbose,nFiles)
    else:
        pfnMap = {}
    for lfn,vals in fileMap.iteritems():
        if (not vals['guid'] in pfnMap.keys()) and (not lfn in shadowList) \
               and not lfn in expCompInDQ2FilesList:
            missList.append(lfn)
    # return
    return missList
    

# get grid source file
def _getGridSrc():
    # set Grid setup.sh if needed
    status,out = commands.getstatusoutput('which voms-proxy-info')
    stLFC,outLFC = commands.getstatusoutput('python -c "import lfc"')
    athenaStatus,athenaPath = commands.getstatusoutput('which athena.py')
    if status == 0 and stLFC == 0:
        gridSrc = ''
        if athenaStatus == 0 and athenaPath.startswith('/afs/in2p3.fr'):
            # for LYON, to avoid missing LD_LIBRARY_PATH
            gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
        elif athenaStatus == 0 and re.search('^/afs/\.*cern.ch',athenaPath) != None:
            # for CERN, VDT is already installed
            gridSrc = '/afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid_env.sh'
    else:
        # set Grid setup.sh
        if os.environ.has_key('PATHENA_GRID_SETUP_SH'):
            gridSrc = os.environ['PATHENA_GRID_SETUP_SH']
        else:
            if not os.environ.has_key('CMTSITE'):
                print "ERROR : CMTSITE is no defined in envvars"
                return False
            if os.environ['CMTSITE'] == 'CERN' or (athenaStatus == 0 and \
                                                   re.search('^/afs/\.*cern.ch',athenaPath) != None):
		gridSrc = '/afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid_env.sh'
            elif os.environ['CMTSITE'] == 'BNL':
                gridSrc = '/afs/usatlas.bnl.gov/osg/client/@sys/current/setup.sh'
            else:
                # try to determin site using path to athena
                if athenaStatus == 0 and athenaPath.startswith('/afs/in2p3.fr'):
                    # LYON
                    gridSrc = '/afs/in2p3.fr/grid/profiles/lcg_env.sh'
                elif athenaStatus == 0 and athenaPath.startswith('/cvmfs/atlas.cern.ch'):
                    # CVMFS
                    if not os.environ.has_key('ATLAS_LOCAL_ROOT_BASE'):
                        os.environ['ATLAS_LOCAL_ROOT_BASE'] = '/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase'
                    gridSrc = os.environ['ATLAS_LOCAL_ROOT_BASE'] + '/user/pandaGridSetup.sh > /dev/null'
                else:
                    print "ERROR : PATHENA_GRID_SETUP_SH is not defined in envvars"
                    print "  for CERN : export PATHENA_GRID_SETUP_SH=/afs/cern.ch/project/gd/LCG-share/current_3.2/etc/profile.d/grid_env.sh"
                    print "  for LYON : export PATHENA_GRID_SETUP_SH=/afs/in2p3.fr/grid/profiles/lcg_env.sh"
                    print "  for BNL  : export PATHENA_GRID_SETUP_SH=/afs/usatlas.bnl.gov/osg/client/@sys/current/setup.sh"
                    return False
    # check grid-proxy
    if gridSrc != '':
        gridSrc = 'source %s;' % gridSrc
        # some grid_env.sh doen't correct PATH/LD_LIBRARY_PATH
        gridSrc = "unset LD_LIBRARY_PATH; unset PYTHONPATH; unset MANPATH; export PATH=/usr/local/bin:/bin:/usr/bin; %s" % gridSrc
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


def isDirectAccess(site,usingRAW=False,usingTRF=False,usingARA=False):
    # unknown site
    if not PandaSites.has_key(site):
        return False
    # parse copysetup
    params = PandaSites[site]['copysetup'].split('^')
    # doesn't use special parameters
    if len(params) < 5:
        return False
    # directIn
    directIn = params[4]
    if directIn != 'True':
        return False
    # xrootd uses local copy for RAW
    newPrefix = params[2]
    if newPrefix.startswith('root:'):
        if usingRAW:
            return False
    # official TRF doesn't work with direct dcap/xrootd
    if usingTRF and (not usingARA):
        if newPrefix.startswith('root:') or newPrefix.startswith('dcap:') or \
               newPrefix.startswith('dcache:') or newPrefix.startswith('gsidcap:'):
            return False
    # return
    return True


# run brokerage
def runBrokerage(sites,atlasRelease,cmtConfig=None,verbose=False,trustIS=False,cacheVer='',processingType='',
                 loggingFlag=False,memorySize=0,useDirectIO=False,siteGroup=None):
    # use only directIO sites
    nonDirectSites = []
    if useDirectIO:
        tmpNewSites = []
        for tmpSite in sites:
            if isDirectAccess(tmpSite):
                tmpNewSites.append(tmpSite)
            else:
                nonDirectSites.append(tmpSite)
        sites = tmpNewSites        
    if sites == []:
        if not loggingFlag:
            return 0,'ERROR : no candidate.'
        else:
            return 0,{'site':'ERROR : no candidate.','logInfo':[]}
    # choose at most 50 sites randomly to avoid too many lookup
    random.shuffle(sites)
    sites = sites[:50]
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
        else:
            # nightlies
            match = re.search('_(rel_\d+)$',cacheVer)
            if match != None:
                # use base release as cache version 
                cacheVer = '%s:%s' % (atlasRelease,match.group(1))
        # use cache for brokerage
        data['atlasRelease'] = cacheVer
    if processingType != '':
        # set processingType mainly for HC
        data['processingType'] = processingType
    # enable logging
    if loggingFlag:
        data['loggingFlag'] = True
    # memory size
    if not memorySize in [-1,0,None,'NULL']:
        data['memorySize'] = memorySize
    # site group
    if not siteGroup in [None,-1]:
        data['siteGroup'] = siteGroup
    status,output = curl.get(url,data)
    try:
        if not loggingFlag:
            return status,output
        else:
            outputPK = pickle.loads(output)
            # add directIO info
            if nonDirectSites != []:
                if not outputPK.has_key('logInfo'):
                    outputPK['logInfo'] = []
                for tmpSite in nonDirectSites:
                    msgBody = 'action=skip site=%s reason=nondirect - not directIO site' % tmpSite
                    outputPK['logInfo'].append(msgBody)
            return status,outputPK
    except:
        type, value, traceBack = sys.exc_info()
        print output
        print "ERROR runBrokerage : %s %s" % (type,value)
        return EC_Failed,None


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


# retry failed jobs in Active
def retryFailedJobsInActive(jobID,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose    
    # execute
    url = baseURLSSL + '/retryFailedJobsInActive'
    data = {'jobID':jobID}
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


# send brokerage log
def sendBrokerageLog(jobID,jobsetID,brokerageLogs,verbose):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    msgList = []
    for tmpMsgBody in brokerageLogs:
        if not jobsetID in [None,'NULL']:
            tmpMsg = ' : jobset=%s jobdef=%s : %s' % (jobsetID,jobID,tmpMsgBody)
        else:
            tmpMsg = ' : jobdef=%s : %s' % (jobID,tmpMsgBody)            
        msgList.append(tmpMsg)
    # execute
    url = baseURLSSL + '/sendLogInfo'
    data = {'msgType':'analy_brokerage',
            'msgList':pickle.dumps(msgList)}
    retVal = curl.post(url,data)
    return True


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
    baseURL = 'http://voatlas220.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://voatlas220.cern.ch:25443/server/panda'    
    global baseURLCSRV
    baseURLCSRV = 'https://voatlas220.cern.ch:25443/server/panda'
    global baseURLCSRVSSL
    baseURLCSRVSSL = 'https://voatlas220.cern.ch:25443/server/panda'


# set server
def setServer(urls):
    global baseURL
    baseURL = urls.split(',')[0]
    global baseURLSSL
    baseURLSSL = urls.split(',')[-1]


# set cache server
def setCacheServer(urls):
    global baseURLCSRV
    baseURLCSRV = urls.split(',')[0]
    global baseURLCSRVSSL
    baseURLCSRVSSL = urls.split(',')[-1]


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
               if PandaSites[tmpID]['status'] in ['brokeroff']:
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


# check merge job generation for a JobID
def checkMergeGenerationStatus(jobID,dn=None,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/checkMergeGenerationStatus'
    data = {'jobID':jobID}
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
        print "ERROR checkMergeGenerationStatus : %s %s" % (type,value)
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
    url = baseURLSSL + '/getDisInUseForAnal'
    data = {'outDataset':outDataset}
    status,output = curl.post(url,data)
    try:
        inputDisList = pickle.loads(output)
        # failed
        if inputDisList == None:
            print "ERROR getFilesInUseForAnal : failed to get shadow dis list from the panda server"
            sys.exit(EC_Failed)
        # split to small chunks to avoid timeout
        retLFNs = []
        nDis = 3
        iDis = 0
        while iDis < len(inputDisList):
            # serialize
            strInputDisList = pickle.dumps(inputDisList[iDis:iDis+nDis])
            # get LFNs
            url = baseURLSSL + '/getLFNsInUseForAnal'
            data = {'inputDisList':strInputDisList}
            status,output = curl.post(url,data)
            tmpLFNs = pickle.loads(output)
            if tmpLFNs == None:
                print "ERROR getFilesInUseForAnal : failed to get LFNs in shadow dis from the panda server"
                sys.exit(EC_Failed)
            retLFNs += tmpLFNs
            iDis += nDis
            time.sleep(1)
        return retLFNs
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR getFilesInUseForAnal : %s %s" % (type,value)
        sys.exit(EC_Failed)


# set debug mode
def setDebugMode(pandaID,modeOn,verbose):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/setDebugMode'
    data = {'pandaID':pandaID,'modeOn':modeOn}
    status,output = curl.post(url,data)
    try:
        return status,output
    except:
        type, value = sys.exc_info()[:2]
        errStr = "setDebugMode failed with %s %s" % (type,value)
        return EC_Failed,errStr

    
# set tmp dir
def setGlobalTmpDir(tmpDir):
    global globalTmpDir
    globalTmpDir = tmpDir


# exclude site
def excludeSite(excludedSiteList,origFullExecString='',infoList=[]):
    if excludedSiteList == []:
        return
    # decompose
    excludedSite = []
    for tmpItemList in excludedSiteList:
        for tmpItem in tmpItemList.split(','):
            if tmpItem != '' and not tmpItem in excludedSite:
                excludedSite.append(tmpItem)
    # get list of original excludedSites
    origExcludedSite = []
    if origFullExecString != '':
        # extract original excludedSite
        origFullExecString = urllib.unquote(origFullExecString)
        matchItr = re.finditer('--excludedSite\s*=*([^ "]+)',origFullExecString)
        for match in matchItr:
            origExcludedSite += match.group(1).split(',')
    else:
        # use excludedSite since this is the first loop
        origExcludedSite = excludedSite
    # remove empty
    if '' in origExcludedSite:
        origExcludedSite.remove('')
    # sites composed of long/short queues
    compSites = ['CERN','LYON','BNL']
    # remove sites
    global PandaSites
    for tmpPatt in excludedSite:
        # skip empty
        if tmpPatt == '':
            continue
        # check if the user sepcified
        userSpecified = False
        if tmpPatt in origExcludedSite:
            userSpecified = True
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
                    # add brokerage info
                    if userSpecified and PandaSites[site]['status'] == 'online' and not isExcudedSite(site):
                        msgBody = 'action=exclude site=%s reason=useroption - excluded by user' % site
                        if not msgBody in infoList:
                            infoList.append(msgBody)
                        PandaSites[site]['status'] = 'excluded'
                    else:
                        # already used by previous submission cycles
                        PandaSites[site]['status'] = 'panda_excluded'
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


# get list of cache prefix
def getCachePrefixes(verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getCachePrefixes'
    status,output = curl.get(url,{})
    # failed
    if status != 0:
        print output
        errStr = "cannot get the list of Athena projects"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    try:
        return pickle.loads(output)
    except:
        print output
        errType,errValue = sys.exc_info()[:2]
        print "ERROR: getCachePrefixes : %s %s" % (errType,errValue)
        sys.exit(EC_Failed)


# get files in dataset with filte
def getFilesInDatasetWithFilter(inDS,filter,shadowList,inputFileListName,verbose,dsStringFlag=False,isRecursive=False,
                                antiFilter='',notSkipLog=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # query files in dataset
    if not isRecursive or verbose:
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
            line = line.strip()
            if line != '':
                filesToBeUsed.append(line)
        rFile.close()
    # get list of filters
    filters = []
    if filter != '':
        filters = filter.split(',')
    antifilters = []
    if antiFilter != '':
        antifilters = antiFilter.split(',')
    # remove redundant files
    tmpKeys = inputFileMap.keys()
    filesPassFilter = []
    for tmpLFN in tmpKeys:
        # remove log
        if not notSkipLog:
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
        # anti matching
        if antiFilter != '':
            antiMatchFlag = False
            for tmpFilter in antifilters:
                if re.search(tmpFilter,tmpLFN) != None:
                    antiMatchFlag = True
                    break
            if antiMatchFlag:
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
                continue
        # files which pass the matching filters    
        filesPassFilter.append(tmpLFN)            
        # files in shadow
        if tmpLFN in shadowList:
            if inputFileMap.has_key(tmpLFN):
                del inputFileMap[tmpLFN]            
            continue
    # no files in filelist are available
    if inputFileMap == {} and (filter != '' or antiFilter != '' or inputFileListName != '') and filesPassFilter == []:
        if inputFileListName != '':
            errStr =  "Files specified in %s are unavailable in %s. " % (inputFileListName,inDS)
        elif filter != '':
            errStr =  "Files matching with %s are unavailable in %s. " % (filters,inDS)
        else:
            errStr =  "Files unmatching with %s are unavailable in %s. " % (antifilters,inDS)
        errStr += "Make sure that you specify correct file names or matching patterns"
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


# request EventPicking
def requestEventPicking(eventPickEvtList,eventPickDataType,eventPickStreamName,
                        eventPickDS,eventPickAmiTag,fileList,fileListName,outDS,
                        lockedBy,params,verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # list of input files
    strInput = ''
    for tmpInput in fileList:
        if tmpInput != '':
            strInput += '%s,' % tmpInput
    if fileListName != '':
        for tmpLine in open(fileListName):
            tmpInput = re.sub('\n','',tmpLine)
            if tmpInput != '':
                strInput += '%s,' % tmpInput
    strInput = strInput[:-1]
    # make dataset name
    userDatasetName = '%s.%s.%s/' % tuple(outDS.split('.')[:2]+[MiscUtils.wrappedUuidGen()])
    # open run/event number list
    evpFile = open(eventPickEvtList)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/putEventPickingRequest'
    data = {'runEventList'        : evpFile.read(),
            'eventPickDataType'   : eventPickDataType,
            'eventPickStreamName' : eventPickStreamName,
            'eventPickDS'         : eventPickDS,
            'eventPickAmiTag'     : eventPickAmiTag,
            'userDatasetName'     : userDatasetName,
            'lockedBy'            : lockedBy,
            'params'              : params,
            'inputFileList'       : strInput,
            }
    evpFile.close()
    status,output = curl.post(url,data)
    # failed
    if status != 0 or output != True: 
        print output
        errStr = "failed to request EventPicking"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return user dataset name    
    return True,userDatasetName


# check if enough sites have DBR
def checkEnoughSitesHaveDBR(dq2IDs):
    # collect sites correspond to DQ2 IDs
    sitesWithDBR = []
    for tmpDQ2ID in dq2IDs:
        tmpPandaSiteList = convertDQ2toPandaIDList(tmpDQ2ID)
        for tmpPandaSite in tmpPandaSiteList:
            if PandaSites.has_key(tmpPandaSite) and PandaSites[tmpPandaSite]['status'] == 'online':
                if isExcudedSite(tmpPandaSite):
                    continue
                sitesWithDBR.append(tmpPandaSite)
    # count the number of online sites with DBR
    nOnline = 0
    nOnlineWithDBR = 0
    nOnlineT1 = 0
    nOnlineT1WithDBR = 0    
    for tmpPandaSite,tmpSiteStat in PandaSites.iteritems():
        if tmpSiteStat['status'] == 'online':
            # exclude test,long,local
            if isExcudedSite(tmpPandaSite):
                continue
            # DQ2 free
            if tmpSiteStat['ddm'] == 'local':
                continue
            nOnline += 1
            if tmpPandaSite in PandaTier1Sites:
                nOnlineT1 += 1
            if tmpPandaSite in sitesWithDBR:
                nOnlineWithDBR += 1
                if tmpPandaSite in PandaTier1Sites:
                    nOnlineT1WithDBR += 1
    # threshold 90%
    if float(nOnlineWithDBR) < 0.9 * float(nOnline):
        return False
    # not all T1s have the DBR
    if nOnlineT1 != nOnlineT1WithDBR:
        return False
    # all OK
    return True
    

# get latest DBRelease
def getLatestDBRelease(verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('trying to get the latest version number for DBRelease=LATEST')
    # get ddo datasets
    ddoDatasets = getDatasets('ddo.*',verbose,True,onlyNames=True)
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
        # ignore CDRelease
        if ".CDRelease." in tmpName:
            continue
        # ignore user
        if tmpName.startswith('ddo.user'):
            continue
        # use Atlas.Ideal
        if not ".Atlas.Ideal." in tmpName:
            continue
        match = re.search('\.v(\d+)(_*[^\.]*)$',tmpName)
        if match == None:
            tmpLog.warning('cannot extract version number from %s' % tmpName)
            continue
        # ignore special DBRs
        if match.group(2) != '':
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
        if len(tmpLocations) < 40 or not checkEnoughSitesHaveDBR(tmpLocations):
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


# get inconsistent datasets which are complete in DQ2 but not in LFC 
def getInconsistentDS(missList,newUsedDsList):
    if missList == [] or newUsedDsList == []:
        return []
    inconDSs = []
    # loop over all datasets
    for tmpDS in newUsedDsList:
        # escape
        if missList == []:
            break
        # get file list
        tmpList = queryFilesInDataset(tmpDS)
        newMissList = []
        # look for missing files
        for tmpFile in missList:
            if tmpList.has_key(tmpFile):
                # append
                if not tmpDS in inconDSs:
                    inconDSs.append(tmpDS)
            else:
                # keep as missing
                newMissList.append(tmpFile)
        # use new missing list for the next dataset
        missList = newMissList
    # return
    return inconDSs


# get T1 sites
def getTier1sites():
    global PandaTier1Sites
    PandaTier1Sites = []
    # FIXME : will be simplified once schedconfig has a tier field
    for tmpCloud,tmpCloudVal in PandaClouds.iteritems():
        for tmpDQ2ID in tmpCloudVal['tier1SE']:
            # ignore NIKHEF
            if tmpDQ2ID.startswith('NIKHEF'):
                continue
            # convert DQ2 ID to Panda Sites
            tmpPandaSites = convertDQ2toPandaIDList(tmpDQ2ID)
            for tmpPandaSite in tmpPandaSites:
                if not tmpPandaSite in PandaTier1Sites:
                    PandaTier1Sites.append(tmpPandaSite)
getTier1sites()


