"""
local job specification

"""

import re
try:
    from urllib import quote, unquote
except ImportError:
    from urllib.parse import quote, unquote
import datetime
try:
    long()
except Exception:
    long = int
try:
    unicode()
except Exception:
    unicode = str


class LocalJobSpec(object):
    # attributes
    _attributes = ('id','JobID','PandaID','jobStatus','site','cloud','jobType',
                   'jobName','inDS','outDS','libDS','provenanceID','creationTime',
                   'lastUpdate','jobParams','dbStatus','buildStatus','retryID',
                   'commandToPilot')
    # appended attributes
    appended = {
        'groupID'       : 'INTEGER',
        'releaseVar'    : 'VARCHAR(128)',
        'cacheVar'      : 'VARCHAR(128)',
        'retryJobsetID' : 'INTEGER',
        'parentJobsetID': 'INTEGER',
        'mergeJobStatus': 'VARCHAR(20)',
        'mergeJobID'    : 'TEXT',
        'nRebro'        : 'INTEGER',
        'jediTaskID'    : 'INTEGER',
        'taskStatus'    : 'VARCHAR(16)',
        }

    _attributes += tuple(appended.keys())
    # slots
    __slots__ = _attributes + ('flag_showSubstatus','flag_longFormat')


    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self,attr,None)
        self.flag_showSubstatus = ''
        self.flag_longFormat = False

    # string format
    def __str__(self):
        # job status
        statusMap = {}
        for item in self.jobStatus.split(','):
            match = re.search('^(\w+)\*(\d+)$',item)
            if match is None:
                # non compact
                if item not in statusMap:
                    statusMap[item] = 0
                statusMap[item] += 1
            else:
                # compact
                tmpStatus = match.group(1)
                tmpCount  = int(match.group(2))
                if tmpStatus not in statusMap:
                    statusMap[tmpStatus] = 0
                statusMap[tmpStatus] += tmpCount
        # show PandaIDs in particular states
        pandaIDstatusMap = {}
        if self.flag_showSubstatus != '':
            # get PandaIDs for each status
            tmpStatusList  = self.jobStatus.split(',')
            tmpPandaIDList = self.PandaID.split(',')
            for tmpIndex,tmpPandaID in enumerate(tmpPandaIDList):
                if tmpIndex < len(tmpStatusList):
                    tmpStatus = tmpStatusList[tmpIndex]
                else:
                    # use unkown for out-range
                    tmpStatus = 'unknown'
                # status of interest
                if tmpStatus not in self.flag_showSubstatus.split(','):
                    continue
                # append
                if tmpStatus not in pandaIDstatusMap:
                    pandaIDstatusMap[tmpStatus] = 'PandaID='
                pandaIDstatusMap[tmpStatus] += '%s,' % tmpPandaID
        statusStr = self.dbStatus
        for tmpStatus in statusMap:
            tmpCount = statusMap[tmpStatus]
            statusStr += '\n%8s   %10s : %s' % ('',tmpStatus,tmpCount)
            if self.flag_showSubstatus:
                if tmpStatus in pandaIDstatusMap:
                    statusStr += '\n%8s   %10s   %s' % ('','',pandaIDstatusMap[tmpStatus][:-1])
        # disable showSubstatus
        self.flag_showSubstatus = ''
        # number of jobs
        nJobs = len(self.PandaID.split(','))
        if self.buildStatus != '':
            # including buildJob
            nJobsStr = "%d + 1(build)" % (nJobs-1)
        else:
            nJobsStr = "%d" % nJobs
        # remove duplication in inDS and outDS
        strInDS = ''
        try:
            tmpInDSList = []
            for tmpItem in str(self.inDS).split(','):
                if tmpItem not in tmpInDSList:
                    tmpInDSList.append(tmpItem)
                    strInDS += '%s,' % tmpItem
            strInDS = strInDS[:-1]
        except Exception:
            pass
        strOutDS = ''
        try:
            tmpOutDSList = []
            for tmpItem in str(self.outDS).split(','):
                if tmpItem not in tmpOutDSList:
                    tmpOutDSList.append(tmpItem)
                    strOutDS += '%s,' % tmpItem
            strOutDS = strOutDS[:-1]
        except Exception:
            pass
        # parse
        relStr = ''
        if self.releaseVar not in ['','NULL','None',None]:
            relStr = self.releaseVar
        # cache
        cacheStr = ''
        if self.cacheVar not in ['','NULL','None',None]:
            cacheStr = self.cacheVar
        # string representation
        strFormat = "%15s : %s\n"
        strOut =  ""
        strOut += strFormat % ("JobID",        self.JobID)
        if self.groupID in ['','NULL',0,'0',-1,'-1']:
            strOut += strFormat % ("JobsetID",     '')
        else:
            strOut += strFormat % ("JobsetID", self.groupID)
        strOut += strFormat % ("type",         self.jobType)
        strOut += strFormat % ("release",      relStr)
        strOut += strFormat % ("cache",        cacheStr)
        strOut += strFormat % ("PandaID",      self.encodeCompact()['PandaID'])
        strOut += strFormat % ("nJobs",        nJobsStr)
        strOut += strFormat % ("site",         self.site)
        strOut += strFormat % ("cloud",        self.cloud)
        strOut += strFormat % ("inDS",         strInDS)
        strOut += strFormat % ("outDS",        strOutDS)
        strOut += strFormat % ("libDS",        str(self.libDS))
        strOut += strFormat % ("retryID",      self.retryID)
        strOut += strFormat % ("provenanceID", self.provenanceID)
        if self.mergeJobStatus not in ['NA']:
            strOut += strFormat % ("mergeJobStatus", self.mergeJobStatus)
            strOut += strFormat % ("mergeJobID",     self.mergeJobID)
        strOut += strFormat % ("creationTime", self.creationTime.strftime('%Y-%m-%d %H:%M:%S'))
        strOut += strFormat % ("lastUpdate",   self.lastUpdate.strftime('%Y-%m-%d %H:%M:%S'))
        strOut += strFormat % ("params",       self.jobParams)
        strOut += strFormat % ("jobStatus",    statusStr)
        # return
        return strOut


    # override __getattribute__ for SQL
    def __getattribute__(self,name):
        ret = object.__getattribute__(self,name)
        if ret is None:
            return "NULL"
        return ret


    # pack tuple into JobSpec
    def pack(self,values):
        for i in range(len(self._attributes)):
            attr= self._attributes[i]
            val = values[i]
            setattr(self,attr,val)
        # expand compact values
        self.decodeCompact()


    # return a tuple of values
    def values(self,forUpdate=False):
        # make compact values
        encVal = self.encodeCompact()
        if forUpdate:
            # for UPDATE
            retS = ""
        else:
            # for INSERT
            retS = "("
        # loop over all attributes
        for attr in self._attributes:
            if attr in encVal:
                val = encVal[attr]
            else:
                val = getattr(self,attr)
            # convert datetime to str
            if type(val) == datetime.datetime:
                val = val.strftime('%Y-%m-%d %H:%M:%S')
            # add colum name for UPDATE
            if forUpdate:
                if attr == 'id':
                    continue
                retS += '%s=' % attr
            # value
            if val == 'NULL':
                retS += 'NULL,'
            else:
                retS += "'%s'," % str(val)
        retS  = retS[:-1]
        if not forUpdate:
            retS += ')'
        return retS


    # expand compact values
    def decodeCompact(self):
        # PandaID
        pStr = ''
        for item in self.PandaID.split(','):
            match = re.search('^(\d+)-(\d+)$',item)
            if match is None:
                # non compact
                pStr += (item+',')
            else:
                # compact
                sID = long(match.group(1))
                eID = long(match.group(2))
                for tmpID in range(sID,eID+1):
                    pStr += "%s," % tmpID
        self.PandaID = pStr[:-1]
        # status
        sStr = ''
        for item in self.jobStatus.split(','):
            match = re.search('^(\w+)\*(\d+)$',item)
            if match is None:
                # non compact
                sStr += (item+',')
            else:
                # compact
                tmpStatus = match.group(1)
                tmpCount  = int(match.group(2))
                for tmpN in range(tmpCount):
                    sStr += "%s," % tmpStatus
        self.jobStatus = sStr[:-1]
        # job parameters
        self.jobParams = unquote(self.jobParams)
        # datetime
        for attr in self._attributes:
            val = getattr(self, attr)
            if not isinstance(val, (str, unicode)):
                continue
            # convert str to datetime
            match = re.search('^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$',val)
            if match is not None:
                tmpDate = datetime.datetime(year   = int(match.group(1)),
                                            month  = int(match.group(2)),
                                            day    = int(match.group(3)),
                                            hour   = int(match.group(4)),
                                            minute = int(match.group(5)),
                                            second = int(match.group(6)))
                setattr(self,attr,tmpDate)
        # jobsetID
        if self.groupID in ['','NULL']:
            self.groupID = 0


    # make compact values
    def encodeCompact(self,includeMerge=False):
        ret = {}
        if self.isJEDI():
            if self.taskStatus in ['finished','failed','done','broken','aborted']:
                self.dbStatus = 'frozen'
            else:
                self.dbStatus = 'running'
        # job parameters
        ret['jobParams'] = quote(self.jobParams)
        # PandaID
        pStr = ''
        sID = None
        eID = None
        tmpPandaIDs = self.PandaID.split(',')
        if includeMerge:
            tmpPandaIDs += self.mergeJobID.split(',')
        for item in tmpPandaIDs:
            if item in ['','None']:
                continue
            # convert to long
            try:
                tmpID = long(item)
            except Exception:
                sID = item
                eID = item
                break
            # set start/end ID
            if sID is None:
                sID = tmpID
                eID = tmpID
                continue
            # successive number
            if eID+1 == tmpID:
                eID = tmpID
                continue
            # jump
            if sID == eID:
                pStr += '%s,' % sID
            else:
                pStr += '%s-%s,' % (sID,eID)
            # reset
            sID = tmpID
            eID = tmpID
        # last bunch
        if sID == eID:
            pStr += '%s,' % sID
        else:
            pStr += '%s-%s,' % (sID,eID)
        ret['PandaID'] = pStr[:-1]
        if self.isJEDI():
            return ret
        # job status
        sStr = ''
        sStatus = None
        nStatus = 0
        toBeFrozen = True
        for tmpStatus in self.jobStatus.split(','):
            # check if is should be frozen
            if toBeFrozen and tmpStatus not in ['finished','failed','partial','cancelled']:
                toBeFrozen = False
            # set start status
            if sStatus is None:
                sStatus = tmpStatus
                nStatus += 1
                continue
            # same status
            if sStatus == tmpStatus:
                nStatus += 1
                continue
            # jump
            if nStatus == 1:
                sStr += '%s,' % sStatus
            else:
                sStr += '%s*%s,' % (sStatus,nStatus)
            # reset
            sStatus = tmpStatus
            nStatus = 1
        # last bunch
        if nStatus == 1:
            sStr += '%s,' % sStatus
        else:
            sStr += '%s*%s,' % (sStatus,nStatus)
        ret['jobStatus'] = sStr[:-1]
        # set merge job status
        if '--mergeOutput' in self.jobParams and self.jobType not in ['usermerge']:
            if self.mergeJobStatus not in ['NA','standby','generating','generated','aborted']:
                self.mergeJobStatus = 'standby'
        else:
            self.mergeJobStatus = 'NA'
        # set dbStatus
        if toBeFrozen:
            if self.mergeJobStatus in ['standby','generating']:
                # intermediate state while generating merge jobs
                self.dbStatus = 'running'
            else:
                self.dbStatus = 'frozen'
        else:
            if self.commandToPilot=='tobekilled':
                self.dbStatus = 'killing'
            else:
                self.dbStatus = 'running'
        # return
        return ret


    # merge job generation is active
    def activeMergeGen(self):
        if '--mergeOutput' in self.jobParams and self.mergeJobStatus in ['standby','generating'] \
               and self.jobType not in ['usermerge']:
            return True
        return False


    # return column names for INSERT or full SELECT
    def columnNames(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ','
            ret += attr
        return ret
    columnNames = classmethod(columnNames)


    # check if JEDI
    def isJEDI(self):
        if self.jediTaskID in [-1,'-1','']:
            return False
        return True
