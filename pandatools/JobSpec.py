"""
job specification

"""

import re
import datetime

reserveChangedState = False


class JobSpec(object):
    # attributes
    _attributes = ('PandaID', 'jobDefinitionID', 'schedulerID', 'pilotID', 'creationTime', 'creationHost',
                   'modificationTime', 'modificationHost', 'AtlasRelease', 'transformation', 'homepackage',
                   'prodSeriesLabel', 'prodSourceLabel', 'prodUserID', 'assignedPriority', 'currentPriority',
                   'attemptNr', 'maxAttempt', 'jobStatus', 'jobName', 'maxCpuCount', 'maxCpuUnit', 'maxDiskCount',
                   'maxDiskUnit', 'ipConnectivity', 'minRamCount', 'minRamUnit', 'startTime', 'endTime',
                   'cpuConsumptionTime', 'cpuConsumptionUnit', 'commandToPilot', 'transExitCode', 'pilotErrorCode',
                   'pilotErrorDiag', 'exeErrorCode', 'exeErrorDiag', 'supErrorCode', 'supErrorDiag',
                   'ddmErrorCode', 'ddmErrorDiag', 'brokerageErrorCode', 'brokerageErrorDiag',
                   'jobDispatcherErrorCode', 'jobDispatcherErrorDiag', 'taskBufferErrorCode',
                   'taskBufferErrorDiag', 'computingSite', 'computingElement', 'jobParameters',
                   'metadata', 'prodDBlock', 'dispatchDBlock', 'destinationDBlock', 'destinationSE',
                   'nEvents', 'grid', 'cloud', 'cpuConversion', 'sourceSite', 'destinationSite', 'transferType',
                   'taskID', 'cmtConfig', 'stateChangeTime', 'prodDBUpdateTime', 'lockedby', 'relocationFlag',
                   'jobExecutionID', 'VO', 'pilotTiming', 'workingGroup', 'processingType', 'prodUserName',
                   'nInputFiles', 'countryGroup', 'batchID', 'parentID', 'specialHandling', 'jobsetID',
                   'coreCount', 'nInputDataFiles', 'inputFileType', 'inputFileProject', 'inputFileBytes',
                   'nOutputDataFiles', 'outputFileBytes', 'jobMetrics', 'workQueue_ID', 'jediTaskID',
                   'jobSubStatus', 'actualCoreCount', 'reqID', 'maxRSS', 'maxVMEM', 'maxSWAP', 'maxPSS',
                   'avgRSS', 'avgVMEM', 'avgSWAP', 'avgPSS', 'maxWalltime', 'nucleus', 'eventService',
                   'failedAttempt', 'hs06sec', 'gshare', 'hs06', 'totRCHAR', 'totWCHAR', 'totRBYTES',
                   'totWBYTES', 'rateRCHAR', 'rateWCHAR', 'rateRBYTES', 'rateWBYTES', 'resource_type',
                   'diskIO', 'memory_leak', 'memory_leak_x2', 'container_name', 'job_label'
                   )
    # slots
    __slots__ = _attributes+('Files','_changedAttrs')
    # attributes which have 0 by default
    _zeroAttrs = ('assignedPriority', 'currentPriority', 'attemptNr', 'maxAttempt', 'maxCpuCount', 'maxDiskCount',
                  'minRamCount', 'cpuConsumptionTime', 'pilotErrorCode', 'exeErrorCode', 'supErrorCode', 'ddmErrorCode',
                  'brokerageErrorCode', 'jobDispatcherErrorCode', 'taskBufferErrorCode', 'nEvents', 'relocationFlag',
                  'jobExecutionID', 'nOutputDataFiles', 'outputFileBytes')
    # attribute to be suppressed. They are in another table
    _suppAttrs = ('jobParameters','metadata')
    # mapping between sequence and attr
    _seqAttrMap = {'PandaID': 'ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval'}
    # limit length
    _limitLength = {'ddmErrorDiag'           : 500,
                    'taskBufferErrorDiag'    : 300,
                    'jobDispatcherErrorDiag' : 250,
                    'brokerageErrorDiag'     : 250,
                    'pilotErrorDiag'         : 500,
                    'exeErrorDiag'           : 500,
                    'jobSubStatus'           : 80,
                    'supErrorDiag'           : 250,
                    }
    # tag for special handling
    _tagForSH = {'altStgOut'          : 'ao',
                 'allOkEvents'        : 'at',
                 'notDiscardEvents'   : 'de',
                 'decAttOnFailedES'   : 'df',
                 'dynamicNumEvents'   : 'dy',
                 'fakeJobToIgnore'    : 'fake',
                 'homeCloud'          : 'hc',
                 'inFilePosEvtNum'    : 'if',
                 'inputPrestaging'    : 'ip',
                 'lumiBlock'          : 'lb',
                 'mergeAtOs'          : 'mo',
                 'noExecStrCnv'       : 'nc',
                 'putLogToOS'         : 'po',
                 'registerEsFiles'    : 're',
                 'resurrectConsumers' : 'rs',
                 'requestType'        : 'rt',
                 'jobCloning'         : 'sc',
                 'scoutJob'           : 'sj',
                 'usePrefetcher'      : 'up',
                 'useZipToPin'        : 'uz',
                 'writeInputToFile'   : 'wf',
                 }



    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self,attr,None)
        # files list
        object.__setattr__(self,'Files',[])
        # map of changed attributes
        object.__setattr__(self,'_changedAttrs',{})


    # override __getattribute__ for SQL
    def __getattribute__(self,name):
        ret = object.__getattribute__(self,name)
        if ret is None:
            return "NULL"
        return ret


    # override __setattr__ to collecte the changed attributes
    def __setattr__(self,name,value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        if name == 'jobStatus':
            if oldVal != newVal:
                self.stateChangeTime = datetime.datetime.utcnow()
        # collect changed attributes
        if oldVal != newVal and name not in self._suppAttrs:
            self._changedAttrs[name] = value


    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'_changedAttrs',{})


    # add File to files list
    def addFile(self,file):
        # set owner
        file.setOwner(self)
        # append
        self.Files.append(file)


    # pack tuple into JobSpec
    def pack(self,values):
        for i in range(len(self._attributes)):
            attr= self._attributes[i]
            val = values[i]
            object.__setattr__(self,attr,val)


    # return a tuple of values
    def values(self):
        ret = []
        for attr in self._attributes:
            val = getattr(self,attr)
            ret.append(val)
        return tuple(ret)


    # return map of values
    def valuesMap(self,useSeq=False,onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            if useSeq and attr in self._seqAttrMap:
                continue
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self,attr)
            if val == 'NULL':
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            # jobParameters/metadata go to another table
            if attr in self._suppAttrs:
                val = None
            # truncate too long values
            if attr in self._limitLength:
                if val is not None:
                    val = val[:self._limitLength[attr]]
            ret[':%s' % attr] = val
        return ret


    # return state values to be pickled
    def __getstate__(self):
        state = []
        for attr in self._attributes:
            val = getattr(self,attr)
            state.append(val)
        if reserveChangedState:
            state.append(self._changedAttrs)
        # append File info
        state.append(self.Files)
        return state


    # restore state from the unpickled state values
    def __setstate__(self,state):
        for i in range(len(self._attributes)):
            # schema evolution is supported only when adding attributes
            if i+1 < len(state):
                object.__setattr__(self,self._attributes[i],state[i])
            else:
                object.__setattr__(self,self._attributes[i],'NULL')
        object.__setattr__(self,'Files',state[-1])
        if reserveChangedState:
            object.__setattr__(self,'_changedAttrs',state[-2])
        else:
            object.__setattr__(self,'_changedAttrs',{})


    # return column names for INSERT or full SELECT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
        return ret
    columnNames = classmethod(columnNames)


    # return expression of values for INSERT
    def valuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += "%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        ret += ")"
        return ret
    valuesExpression = classmethod(valuesExpression)


    # return expression of bind values for INSERT
    def bindValuesExpression(cls,useSeq=False):
        from pandaserver.config import panda_config
        ret = "VALUES("
        for attr in cls._attributes:
            if useSeq and attr in cls._seqAttrMap:
                if panda_config.backend == 'mysql':
                    # mysql
                    ret += "%s," % "NULL"
                else:
                    # oracle
                    ret += "%s," % cls._seqAttrMap[attr]
            else:
                ret += ":%s," % attr
        ret = ret[:-1]
        ret += ")"
        return ret
    bindValuesExpression = classmethod(bindValuesExpression)


    # return an expression for UPDATE
    def updateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret = ret + attr + "=%s"
            if attr != cls._attributes[len(cls._attributes)-1]:
                ret += ","
        return ret
    updateExpression = classmethod(updateExpression)


    # return an expression of bind variables for UPDATE
    def bindUpdateExpression(cls):
        ret = ""
        for attr in cls._attributes:
            ret += '%s=:%s,' % (attr,attr)
        ret = ret[:-1]
        ret += ' '
        return ret
    bindUpdateExpression = classmethod(bindUpdateExpression)


    # comparison function for sort
    def compFunc(cls,a,b):
        iPandaID  = list(cls._attributes).index('PandaID')
        iPriority = list(cls._attributes).index('currentPriority')
        if a[iPriority] > b[iPriority]:
            return -1
        elif a[iPriority] < b[iPriority]:
            return 1
        else:
            if a[iPandaID] > b[iPandaID]:
                return 1
            elif a[iPandaID] < b[iPandaID]:
                return -1
            else:
                return 0
    compFunc = classmethod(compFunc)


    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs:
                ret += '%s=:%s,' % (attr,attr)
        ret  = ret[:-1]
        ret += ' '
        return ret


    # check if goint to merging
    def produceUnMerge(self):
        for tmpFile in self.Files:
            if tmpFile.isUnMergedOutput():
                return True
        return False



    # truncate string attribute
    def truncateStringAttr(cls,attr,val):
        if attr not in cls._limitLength:
            return val
        if val is None:
            return val
        return val[:cls._limitLength[attr]]
    truncateStringAttr = classmethod(truncateStringAttr)



    # set DDM backend
    def setDdmBackEnd(self,backEnd):
        if self.specialHandling in [None,'']:
            self.specialHandling = 'ddm:'+backEnd
        else:
            if 'ddm:' in self.specialHandling:
                self.specialHandling = re.sub('ddm:[,]+','ddm:'+backEnd,
                                              self.specialHandling)
            else:
                self.specialHandling = self.specialHandling+','+ \
                    'ddm:'+backEnd


    # set LB number
    def setLumiBlockNr(self,lumiBlockNr):
        if self.specialHandling in ['',None,'NULL']:
            self.specialHandling = 'lb:{0}'.format(lumiBlockNr)
        else:
            self.specialHandling += ',lb:{0}'.format(lumiBlockNr)



    # get LB number
    def getLumiBlockNr(self):
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(','):
                if tmpItem.startswith('lb:'):
                    return int(tmpItem.split(':')[-1])
        return None



    # get DDM backend
    def getDdmBackEnd(self):
        if self.specialHandling is None:
            return None
        for tmpItem in self.specialHandling.split(','):
            if tmpItem.startswith('ddm:'):
                return tmpItem.split(':')[-1]
        return None



    # set to accept partial finish
    def setToAcceptPartialFinish(self):
        token = 'ap'
        if self.specialHandling in [None,'']:
            self.specialHandling = token
        else:
            if token not in self.specialHandling.split(','):
                self.specialHandling = self.specialHandling+','+token



    # accept partial finish
    def acceptPartialFinish(self):
        token = 'ap'
        if self.specialHandling in [None,'']:
            return False
        else:
            return token in self.specialHandling.split(',')


    # set home cloud
    def setHomeCloud(self,homeCloud):
        if self.specialHandling in ['',None,'NULL']:
            self.specialHandling = 'hc:{0}'.format(homeCloud)
        else:
            self.specialHandling += ',hc:{0}'.format(homeCloud)



    # get cloud
    def getCloud(self):
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(','):
                if tmpItem.startswith('hc:'): # hc: Home Cloud
                    return tmpItem.split(':')[-1]
        return self.cloud


    # check if cancelled or it's flavor
    def isCancelled(self):
        return self.jobStatus in ['cancelled','closed']



    # get file names which were uploaded to alternative locations
    def altStgOutFileList(self):
        try:
            if self.jobMetrics is not None:
                for item in self.jobMetrics.split():
                    if item.startswith('altTransferred='):
                        return item.split('=')[-1].split(',')
        except Exception:
            pass
        return []



    # get mode for alternative stage-out
    def getAltStgOut(self):
        if self.specialHandling is not None:
            for tmpItem in self.specialHandling.split(','):
                if tmpItem.startswith('{0}:'.format(self._tagForSH['altStgOut'])):
                    return tmpItem.split(':')[-1]
        return None



    # set alternative stage-out
    def setAltStgOut(self,mode):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        # remove old value
        newItems = []
        for tmpItem in items:
            if tmpItem.startswith('{0}:'.format(self._tagForSH['altStgOut'])):
                continue
            newItems.append(tmpItem)
        newItems.append('{0}:{1}'.format(self._tagForSH['altStgOut'],mode))
        self.specialHandling = ','.join(newItems)



    # put log files to OS
    def putLogToOS(self):
        if self.specialHandling is not None:
            return self._tagForSH['putLogToOS'] in self.specialHandling.split(',')
        return False



    # set to put log files to OS
    def setToPutLogToOS(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['putLogToOS'] not in items:
            items.append(self._tagForSH['putLogToOS'])
        self.specialHandling = ','.join(items)



    # write input to file
    def writeInputToFile(self):
        if self.specialHandling is not None:
            return self._tagForSH['writeInputToFile'] in self.specialHandling.split(',')
        return False



    # set to write input to file
    def setToWriteInputToFile(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['writeInputToFile'] not in items:
            items.append(self._tagForSH['writeInputToFile'])
        self.specialHandling = ','.join(items)



    # set request type
    def setRequestType(self,reqType):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        newItems = []
        setFlag = False
        for item in items:
            if not item.startswith(self._tagForSH['requestType']):
                newItems.append(item)
        newItems.append('{0}={1}'.format(self._tagForSH['requestType'],reqType))
        self.specialHandling = ','.join(newItems)



    # sort files
    def sortFiles(self):
        try:
            lfnMap = {}
            for tmpFile in self.Files:
                if tmpFile.lfn not in lfnMap:
                    lfnMap[tmpFile.lfn] = []
                lfnMap[tmpFile.lfn].append(tmpFile)
            lfns = list(lfnMap)
            lfns.sort()
            newFiles = []
            for tmpLFN in lfns:
                for tmpFile in lfnMap[tmpLFN]:
                    newFiles.append(tmpFile)
            self.Files = newFiles
        except Exception:
            pass



    # get zip file map
    def getZipFileMap(self):
        zipMap = dict()
        try:
            if self.jobParameters is not None:
                zipStr = re.search('<ZIP_MAP>(.+)</ZIP_MAP>',self.jobParameters)
                if zipStr is not None:
                    for item in zipStr.group(1).split():
                        zipFile,conFiles = item.split(':')
                        conFiles = conFiles.split(',')
                        zipMap[zipFile] = conFiles
        except Exception:
            pass
        return zipMap



    # suppress execute string conversion
    def noExecStrCnv(self):
        if self.specialHandling is not None:
            return self._tagForSH['noExecStrCnv'] in self.specialHandling.split(',')
        return False



    # set to suppress execute string conversion
    def setNoExecStrCnv(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['noExecStrCnv'] not in items:
            items.append(self._tagForSH['noExecStrCnv'])
        self.specialHandling = ','.join(items)



    # in-file positional event number
    def inFilePosEvtNum(self):
        if self.specialHandling is not None:
            return self._tagForSH['inFilePosEvtNum'] in self.specialHandling.split(',')
        return False



    # set to use in-file positional event number
    def setInFilePosEvtNum(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['inFilePosEvtNum'] not in items:
            items.append(self._tagForSH['inFilePosEvtNum'])
        self.specialHandling = ','.join(items)



    # register event service files
    def registerEsFiles(self):
        if self.specialHandling is not None:
            return self._tagForSH['registerEsFiles'] in self.specialHandling.split(',')
        return False



    # set to register event service files
    def setRegisterEsFiles(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['registerEsFiles'] not in items:
            items.append(self._tagForSH['registerEsFiles'])
        self.specialHandling = ','.join(items)



    # set background-able flag
    def setBackgroundableFlag(self):
        self.jobExecutionID = 0
        if self.prodSourceLabel not in ['managed', 'test']:
            return
        try:
            if self.inputFileBytes / self.maxWalltime > 5000:
                return
        except Exception:
            return
        try:
            if self.coreCount <= 1:
                return
        except Exception:
            return
        if self.currentPriority > 250:
            return
        self.jobExecutionID = 1



    # use prefetcher
    def usePrefetcher(self):
        if self.specialHandling is not None:
            return self._tagForSH['usePrefetcher'] in self.specialHandling.split(',')
        return False



    # set to use prefetcher
    def setUsePrefetcher(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['usePrefetcher'] not in items:
            items.append(self._tagForSH['usePrefetcher'])
        self.specialHandling = ','.join(items)



    # use zip to pin
    def useZipToPin(self):
        if self.specialHandling is not None:
            return self._tagForSH['useZipToPin'] in self.specialHandling.split(',')
        return False



    # set to use zip to pin
    def setUseZipToPin(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['useZipToPin'] not in items:
            items.append(self._tagForSH['useZipToPin'])
        self.specialHandling = ','.join(items)



    # not discard events
    def notDiscardEvents(self):
        if self.specialHandling is not None:
            return self._tagForSH['notDiscardEvents'] in self.specialHandling.split(',')
        return False



    # set not to discard events
    def setNotDiscardEvents(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['notDiscardEvents'] not in items:
            items.append(self._tagForSH['notDiscardEvents'])
        self.specialHandling = ','.join(items)



    # all events are done
    def allOkEvents(self):
        if self.specialHandling is not None:
            return self._tagForSH['allOkEvents'] in self.specialHandling.split(',')
        return False



    # set all events are done
    def setAllOkEvents(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['allOkEvents'] not in items:
            items.append(self._tagForSH['allOkEvents'])
        self.specialHandling = ','.join(items)



    # set scout job flag
    def setScoutJobFlag(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['scoutJob'] not in items:
            items.append(self._tagForSH['scoutJob'])
        self.specialHandling = ','.join(items)



    # check if scout job
    def isScoutJob(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        return self._tagForSH['scoutJob'] in items



    # decrement attemptNr of events only when failed
    def decAttOnFailedES(self):
        if self.specialHandling is not None:
            return self._tagForSH['decAttOnFailedES'] in self.specialHandling.split(',')
        return False



    # set to decrement attemptNr of events only when failed
    def setDecAttOnFailedES(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['decAttOnFailedES'] not in items:
            items.append(self._tagForSH['decAttOnFailedES'])
        self.specialHandling = ','.join(items)



    # set fake flag to ignore in monigoring
    def setFakeJobToIgnore(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['fakeJobToIgnore'] not in items:
            items.append(self._tagForSH['fakeJobToIgnore'])
        self.specialHandling = ','.join(items)


    # remove fake flag to ignore in monigoring
    def removeFakeJobToIgnore(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['fakeJobToIgnore'] in items:
            items.remove(self._tagForSH['fakeJobToIgnore'])
        self.specialHandling = ','.join(items)



    # set task attribute
    def set_task_attribute(self, key, value):
        if not isinstance(self.metadata, list):
            self.metadata = [None, None]
        if len(self.metadata) != 3:
            self.metadata.append({})
        self.metadata[2][key] = value



    # get task attribute
    def get_task_attribute(self, key):
        try:
            return self.metadata[2][key]
        except Exception:
            return None

    # set input prestaging
    def setInputPrestaging(self):
        if self.specialHandling is not None:
            items = self.specialHandling.split(',')
        else:
            items = []
        if self._tagForSH['inputPrestaging'] not in items:
            items.append(self._tagForSH['inputPrestaging'])
        self.specialHandling = ','.join(items)

    # use input prestaging
    def useInputPrestaging(self):
        if self.specialHandling is not None:
            return self._tagForSH['inputPrestaging'] in self.specialHandling.split(',')
        return False

    # to a dictionary
    def to_dict(self):
        ret = {}
        for a in self._attributes:
            v = getattr(self, a)
            if isinstance(v, datetime.datetime):
                v = str(v)
            elif v == 'NULL':
                v = None
            ret[a] = v
        return ret
