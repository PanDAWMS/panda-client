'''
client methods

'''

import os
import re
import sys
import stat
import json
try:
    from urllib import urlencode, unquote_plus
except ImportError:
    from urllib.parse import urlencode, unquote_plus
import struct
try:
    import cPickle as pickle
except ImportError:
    import pickle
import socket
import random
import tempfile

from . import MiscUtils
from .MiscUtils import commands_get_status_output, commands_get_output, pickle_loads
from . import PLogger

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'

baseURLCSRVSSL = "http://pandacache.cern.ch:25443/server/panda"

# exit code
EC_Failed = 255

# limit on maxCpuCount
maxCpuCountLimit = 1000000000

# resolve panda cache server's name
netloc = baseURLCSRVSSL.split('/')[2]
tmp_host = random.choice(socket.getaddrinfo(*netloc.split(':')))
baseURLCSRVSSL = "https://%s:%s/server/panda" % (socket.getfqdn(tmp_host[-1][0]), tmp_host[-1][1])


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
    print("No valid grid proxy certificate found")
    return ''


# look for a CA certificate directory
def _x509_CApath():
    # use X509_CERT_DIR
    try:
        return os.environ['X509_CERT_DIR']
    except Exception:
        return '/etc/grid-security/certificates'

# keep list of tmp files for cleanup
globalTmpDir = ''


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        self.verifyHost = True
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
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # max time of 10 min
        com += ' -m 600'
        # add rucio account info
        if rucioAccount:
            if 'RUCIO_ACCOUNT' in os.environ:
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if 'RUCIO_APPID' in os.environ:
                data['appid'] = os.environ['RUCIO_APPID']
            data['client_version'] = '2.4.1'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urlencode({key:data[key]})
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
            print(com)
            print(strData[:-1])
        s,o = commands_get_status_output(com)
        if o != '\x00':
            try:
                tmpout = unquote_plus(o)
                o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # POST method
    def post(self,url,data,rucioAccount=False, is_json=False):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # max time of 10 min
        com += ' -m 600'
        # add rucio account info
        if rucioAccount:
            if 'RUCIO_ACCOUNT' in os.environ:
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if 'RUCIO_APPID' in os.environ:
                data['appid'] = os.environ['RUCIO_APPID']
            data['client_version'] = '2.4.1'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD, strData.encode('utf-8'))
        os.close(tmpFD)
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print(com)
            print(strData[:-1])
        s,o = commands_get_status_output(com)
        if o != '\x00':
            try:
                if is_json:
                    o = json.loads(o)
                else:
                    tmpout = unquote_plus(o)
                    o = eval(tmpout)
            except:
                pass
        ret = (s,o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not url.startswith('https://'):
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        if self.verbose:
            print(com)
        # execute
        ret = commands_get_status_output(com)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
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


# dump log
def dump_log(func_name, exception_obj, output):
    print(output)
    err_str = "{} failed : {}".format(func_name, str(exception_obj))
    tmp_log = PLogger.getPandaLogger()
    tmp_log.error(err_str)
    return err_str


'''
public methods

'''

# submit jobs
def submitJobs(jobs,verbose=False):
    # set hostname
    hostname = commands_get_output('hostname')
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
    if status != 0:
        print(output)
        return status,None
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("submitJobs", e, output)
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
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getJobStatus", e, output)
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
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("killJobs", e, output)
        return EC_Failed,None


# kill task
def killTask(jediTaskID,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/killTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("killTask", e, output)
        return EC_Failed,None


# finish task
def finishTask(jediTaskID,soft=False,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/finishTask'
    data = {'jediTaskID':jediTaskID}
    if soft:
        data['soft'] = True
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("finishTask", e, output)
        return EC_Failed,None


# retry task
def retryTask(jediTaskID,verbose=False,properErrorCode=False,newParams=None):
    if newParams == None:
        newParams = {}
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/retryTask'
    data = {'jediTaskID':jediTaskID,
            'properErrorCode':properErrorCode}
    if newParams != {}:
        data['newParams'] = json.dumps(newParams)
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("retryTask", e, output)
        return EC_Failed,None



# put file
def putFile(file,verbose=False,useCacheSrv=False,reuseSandbox=False):
    # size check for noBuild
    sizeLimit = 10*1024*1024
    fileSize = os.stat(file)[stat.ST_SIZE]
    if not os.path.basename(file).startswith('sources.'):
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
    # check duplication
    if reuseSandbox:
        # get CRC
        fo = open(file, 'rb')
        fileContent = fo.read()
        fo.close()
        footer = fileContent[-8:]
        checkSum, i_size = struct.unpack("II",footer)
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
            setCacheServer(hostName)
            # return reusable filename
            return 0,"NewFileName:%s" % reuseFileName
    # execute
    if useCacheSrv:
        url = baseURLCSRVSSL + '/putFile'
    else:
        url = baseURLSSL + '/putFile'
    data = {'file':file}
    return curl.put(url,data)


# get grid source file
def _getGridSrc():
    return ''


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


# use dev server
def useDevServer():
    global baseURL
    baseURL = 'http://aipanda007.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://aipanda007.cern.ch:25443/server/panda'
    global baseURLCSRVSSL
    baseURLCSRVSSL = 'https://aipanda007.cern.ch:25443/server/panda'


# use INTR server
def useIntrServer():
    global baseURL
    baseURL = 'http://aipanda059.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://aipanda059.cern.ch:25443/server/panda'


# set cache server
def setCacheServer(host_name):
    global baseURLCSRVSSL
    baseURLCSRVSSL = "https://%s:25443/server/panda" % host_name


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
        print(output)
        return status,None
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getProxyKey", e, output)
        return EC_Failed,None


# get JobIDs and jediTasks in a time range
def getJobIDsJediTasksInTimeRange(timeRange, dn=None, minTaskID=None, verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getJediTasksInTimeRange'
    data = {'timeRange': timeRange,
            'fullFlag': True}
    if dn != None:
        data['dn'] = dn
    if minTaskID is not None:
        data['minTaskID'] = minTaskID
    status,output = curl.post(url,data)
    if status!=0:
        print(output)
        return status, None
    try:
        jediTaskDicts = pickle_loads(output)
        return 0, jediTaskDicts
    except Exception as e:
        dump_log("getJediTasksInTimeRange", e, output)
        return EC_Failed, None


# get details of jedi task
def getJediTaskDetails(taskDict,fullFlag,withTaskInfo,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getJediTaskDetails'
    data = {'jediTaskID':taskDict['jediTaskID'],
            'fullFlag':fullFlag,
            'withTaskInfo':withTaskInfo}
    status,output = curl.post(url,data)
    if status != 0:
        print(output)
        return status,None
    try:
        tmpDict = pickle_loads(output)
        # server error
        if tmpDict == {}:
            print("ERROR getJediTaskDetails got empty")
            return EC_Failed,None
        # copy 
        for tmpKey in tmpDict:
            tmpVal = tmpDict[tmpKey]
            taskDict[tmpKey] = tmpVal
        return 0,taskDict
    except Exception as e:
        dump_log("getJediTaskDetails", e, output)
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
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getFullJobStatus", e, output)
        return EC_Failed,None


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
    except Exception as e:
        errStr = dump_log("setDebugMode", e, output)
        return EC_Failed,errStr

    
# set tmp dir
def setGlobalTmpDir(tmpDir):
    global globalTmpDir
    globalTmpDir = tmpDir


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
        print(output)
        errStr = "cannot get the list of Athena projects"
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    try:
        tmpList = pickle_loads(output)
        tmpList.append('AthAnalysisBase')
        return tmpList
    except Exception as e:
        dump_log("getCachePrefixes", e, output)
        sys.exit(EC_Failed)


# get list of cmtConfig
def getCmtConfigList(athenaVer,verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getCmtConfigList'
    data = {}
    data['relaseVer'] = athenaVer
    status,output = curl.get(url,data)
    # failed
    if status != 0:
        print(output)
        errStr = "cannot get the list of cmtconfig for %s" % athenaVer
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    try:
        return pickle_loads(output)
    except Exception as e:
        dump_log("getCmtConfigList", e, output)
        sys.exit(EC_Failed)


# request EventPicking
def requestEventPicking(eventPickEvtList,eventPickDataType,eventPickStreamName,
                        eventPickDS,eventPickAmiTag,fileList,fileListName,outDS,
                        lockedBy,params,eventPickNumSites,eventPickWithGUID,ei_api,
                        verbose=False):
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
            'giveGUID'            : eventPickWithGUID,
            'params'              : params,
            'inputFileList'       : strInput,
            }
    if eventPickNumSites > 1:
        data['eventPickNumSites'] = eventPickNumSites
    if ei_api:
        data['ei_api'] = ei_api
    evpFile.close()
    status,output = curl.post(url,data)
    # failed
    if status != 0 or output != True: 
        print(output)
        errStr = "failed to request EventPicking"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return user dataset name    
    return True,userDatasetName


# submit task
def insertTaskParams(taskParams,verbose,properErrorCode=False):
    """Insert task parameters

       args:
           taskParams: a dictionary of task parameters
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           tuple of return code and message from the server
                 0: request is processed
                 1: duplication in DEFT
                 2: duplication in JEDI
                 3: accepted for incremental execution
                 4: server error
    """
    # serialize
    taskParamsStr = json.dumps(taskParams)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/insertTaskParams'
    data = {'taskParams':taskParamsStr,
            'properErrorCode':properErrorCode}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("insertTaskParams", e, output)
        return EC_Failed,output+'\n'+errStr


# get PanDA IDs with TaskID
def getPandaIDsWithTaskID(jediTaskID,verbose=False):
    """Get PanDA IDs with TaskID

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of PanDA IDs
    """
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getPandaIDsWithTaskID'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getPandaIDsWithTaskID", e, output)
        return EC_Failed,output+'\n'+errStr


# reactivate task
def reactivateTask(jediTaskID,verbose=False):
    """Reactivate task

       args:
           jediTaskID: jediTaskID of the task to be reactivated
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: unknown task
                 1: succeeded
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/reactivateTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("reactivateTask", e, output)
        return EC_Failed,output+'\n'+errStr

# resume task
def resumeTask(jediTaskID,verbose=False):
    """resume task

       args:
           jediTaskID: jediTaskID of the task to be resumed
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: request is registered
                 1: server error
                 2: task not found
                 3: permission denied
                 4: irrelevant task status
                 100: non SSL connection
                 101: irrelevant taskID
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/resumeTask'
    data = {'jediTaskID': jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle.loads(output)
    except:
        errtype, errvalue = sys.exc_info()[:2]
        errStr = "ERROR resumeTask : %s %s" % (errtype, errvalue)
        return EC_Failed,output+'\n'+errStr

# get task status TaskID
def getTaskStatus(jediTaskID,verbose=False):
    """Get task status

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the status string
    """
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getTaskStatus'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getTaskStatus", e, output)
        return EC_Failed,output+'\n'+errStr


# get taskParamsMap with TaskID
def getTaskParamsMap(jediTaskID):
    """Get task status

       args:
           jediTaskID: jediTaskID of the task to get taskParamsMap
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tuple of return code and taskParamsMap
                 1: logical error
                 0: success
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getTaskParamsMap'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getTaskParamsMap", e, output)
        return EC_Failed,output+'\n'+errStr



# get user job metadata
def getUserJobMetadata(task_id, verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getUserJobMetadata'
    data = {'jediTaskID': task_id}
    status,output = curl.post(url, data, is_json=True)
    try:
        return (0, output)
    except Exception as e:
        errStr = dump_log("getUserJobMetadata", e, output)
        return EC_Failed, errStr
