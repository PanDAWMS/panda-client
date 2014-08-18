"""
local jobset specification

"""

import re
import datetime

class LocalJobsetSpec(object):
    # attributes
    _attributes = ('JobsetID','dbStatus','JobMap','PandaID','inDS','outDS',
                   'parentSetID','retrySetID','creationTime','jobStatus',
                   'jediTaskID','taskStatus')
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
        # set longFormat when showSubstatus
        if self.flag_showSubstatus != '':
            self.flag_longFormat = True
        # get JobID list
        jobIDs = self.JobMap.keys()
        jobIDs.sort()
        # initialize
        firstJob = True
        strFormat = "%15s : %s\n"
        strOut1 =  ""
        strOut2 =  ""        
        strOutJob = ''
        strJobID  = ''
        for jobID in jobIDs:
            strJobID += '%s,' % jobID
        strJobID = strJobID[:-1]    
        # loop over all jobs
        totalBuild = 0
        totalRun   = 0
        totalMerge = 0
        totalJobStatus = {}
        usingMerge = False
        for jobID in jobIDs:
            job = self.JobMap[jobID]
            if firstJob:
                # get common values from the first jobID
                firstJob = False
                # release
                relStr = ''
                if not job.releaseVar in ['','NULL','None',None]:
                    relStr = job.releaseVar
                # cache
                cacheStr = ''
                if not job.cacheVar in ['','NULL','None',None]:
                    cacheStr = job.cacheVar
                # common string representation
                if self.isJEDI():
                    strOut1 += strFormat % ("jediTaskID", self.jediTaskID)
                    strOut1 += strFormat % ("taskStatus", self.taskStatus)
                strOut1 += strFormat % ("JobsetID",     self.JobsetID)                
                strOut1 += strFormat % ("type",         job.jobType)
                strOut1 += strFormat % ("release",      relStr)
                strOut1 += strFormat % ("cache",        cacheStr)
                #strOut2 += strFormat % ("JobID"  ,      strJobID)
                strOut2 += strFormat % ("PandaID",      self.PandaID)
                strOut2 += strFormat % ("inDS",         self.inDS)
                strOut2 += strFormat % ("outDS",        self.outDS)
                if not self.isJEDI():
                    strOut2 += strFormat % ("parentSetID",  self.parentSetID)                                
                    strOut2 += strFormat % ("retrySetID",   self.retrySetID)
                strOut2 += strFormat % ("creationTime", job.creationTime.strftime('%Y-%m-%d %H:%M:%S'))
                strOut2 += strFormat % ("lastUpdate",   job.lastUpdate.strftime('%Y-%m-%d %H:%M:%S'))
                strOut2 += strFormat % ("params",       job.jobParams)
                if not self.isJEDI():
                    strOut2 += strFormat % ("status",   self.dbStatus)
                else:
                    strOut2 += strFormat % ("inputStatus",'')
            # job status
            statusMap = {}
            for item in job.jobStatus.split(','):
                match = re.search('^(\w+)\*(\d+)$',item)
                if match == None:
                    # non compact
                    if not statusMap.has_key(item):
                        statusMap[item] = 0
                    statusMap[item] += 1
                else:
                    # compact
                    tmpStatus = match.group(1)
                    tmpCount  = int(match.group(2))
                    if not statusMap.has_key(tmpStatus):
                        statusMap[tmpStatus] = 0
                    statusMap[tmpStatus] += tmpCount
            # merge
            if not job.mergeJobStatus in ['NA']:
                usingMerge = True
            # get PandaIDs for each status 
            pandaIDstatusMap = {}
            if not self.isJEDI():
                tmpStatusList  = job.jobStatus.split(',')
                tmpPandaIDList = job.PandaID.split(',')
                for tmpIndex,tmpPandaID in enumerate(tmpPandaIDList):
                    if tmpIndex < len(tmpStatusList):
                        tmpStatus = tmpStatusList[tmpIndex]
                    else:
                        # use unkown for out-range
                        tmpStatus = 'unknown'
                    # append for all jobs
                    if not totalJobStatus.has_key(tmpStatus):
                        totalJobStatus[tmpStatus] = 0
                    totalJobStatus[tmpStatus] += 1    
                    # status of interest
                    if not tmpStatus in self.flag_showSubstatus.split(','):
                        continue
                    # append for individual job
                    if not pandaIDstatusMap.has_key(tmpStatus):
                        pandaIDstatusMap[tmpStatus] = 'PandaID='
                    pandaIDstatusMap[tmpStatus] += '%s,' % tmpPandaID
            else:
                totalJobStatus = statusMap
            statusStr = job.dbStatus
            for tmpStatus,tmpCount in statusMap.iteritems():
                statusStr += '\n%8s   %10s : %s' % ('',tmpStatus,tmpCount)
                if self.flag_showSubstatus:
                    if pandaIDstatusMap.has_key(tmpStatus):
                        statusStr += '\n%8s   %10s   %s' % ('','',pandaIDstatusMap[tmpStatus][:-1])
            # number of jobs
            nJobs = len(job.PandaID.split(','))
            if job.buildStatus != '':
                # including buildJob
                nJobsStr = "%d + 1(build)" % (nJobs-1)
                totalBuild += 1
                if usingMerge and job.jobType in ['usermerge']:
                    totalMerge += (nJobs-1)
                else:
                    totalRun += (nJobs-1)
            else:
                nJobsStr = "%d" % nJobs
                if usingMerge and job.jobType in ['usermerge']:
                    totalMerge += nJobs
                else:
                    totalRun += nJobs
            # merging
            if self.isJEDI() and job.mergeJobID != '':
                totalMerge += len(job.mergeJobID.split(','))
            # job specific string representation
            if self.flag_longFormat:
                strOutJob += '\n'
                strOutJob += strFormat % ("JobID",        job.JobID)
                strOutJob += strFormat % ("nJobs",        nJobsStr)
                strOutJob += strFormat % ("site",         job.site)
                strOutJob += strFormat % ("libDS",        str(job.libDS))
                strOutJob += strFormat % ("retryID",      job.retryID)        
                strOutJob += strFormat % ("provenanceID", job.provenanceID)
                strOutJob += strFormat % ("jobStatus",    statusStr)
        # number of jobs
        nJobsStr = "%d" % totalRun
        if usingMerge:
            nJobsStr += " + %s(merge)" % totalMerge
        if totalBuild != 0:
            nJobsStr += " + %s(build)" % totalBuild
        strOut1 += strFormat % ("nJobs",        nJobsStr)
        strOut = strOut1 + strOut2
        # not long format
        if not self.flag_longFormat:
            for tmpStatus,tmpCount in totalJobStatus.iteritems():
                strOut += '%8s   %10s : %s\n' % ('',tmpStatus,tmpCount)
        else:
            strOut += strOutJob
        # disable showSubstatus and longFormat
        self.flag_showSubstatus = ''
        self.flag_longFormat = False
        # return
        return strOut

    
    # override __getattribute__
    def __getattribute__(self,name):
        # scan all JobIDs to get dbStatus
        if name == 'dbStatus':
            tmpJobs = object.__getattribute__(self,'JobMap')
            runningFlag = False
            for tmpJobID,tmpJob in tmpJobs.iteritems():
                if tmpJob.dbStatus == 'killing':
                    return 'killing'
                    break
                if tmpJob.dbStatus == 'running':
                    runningFlag = True
            if runningFlag:
                return 'running'
            return 'frozen'
        ret = object.__getattribute__(self,name)
        return ret


    # set jobs
    def setJobs(self,jobs):
        # append
        retryIDs = []
        parentIDs = []
        for job in jobs:
            # set initial parameters
            if self.JobsetID == None:
                self.JobsetID = job.groupID
                self.JobMap = {}
                self.creationTime = job.creationTime
                self.jediTaskID = job.jediTaskID
                self.taskStatus = job.taskStatus
            self.JobMap[job.JobID] = job
            # get parent/retry
            if not job.retryJobsetID in [0,-1,'0','-1']:
                if not job.retryJobsetID in retryIDs:
                    retryIDs.append(job.retryJobsetID)
            if not job.parentJobsetID in [0,-1,'0','-1']:
                if not job.parentJobsetID in parentIDs:
                    parentIDs.append(job.parentJobsetID)
        # set parent/retry
        retryIDs.sort()
        parentIDs.sort()
        self.retrySetID = ''
        for tmpID in retryIDs:
            self.retrySetID += '%s,' % tmpID
        self.retrySetID = self.retrySetID[:-1]
        self.parentSetID = ''
        for tmpID in parentIDs:
            self.parentSetID += '%s,' % tmpID
        self.parentSetID = self.parentSetID[:-1]    
        # aggregate some info    
        pStr = ''
        sStatus = ''
        strInDS = ''
        strOutDS = ''
        tmpInDSList = []
        tmpOutDSList = []
        jobIDs = self.JobMap.keys()
        jobIDs.sort()
        for jobID in jobIDs:
            job = self.JobMap[jobID]
            # PandaID
            tmpPStr = job.encodeCompact(includeMerge=True)['PandaID']
            if tmpPStr != '':
                pStr += tmpPStr
                pStr += ','
            # inDS and outDS
            try:
                for tmpItem in str(job.inDS).split(','):
                    if not tmpItem in tmpInDSList:
                        tmpInDSList.append(tmpItem)
                        strInDS += '%s,' % tmpItem
            except:
                pass
            try:
                for tmpItem in str(job.outDS).split(','):
                    if not tmpItem in tmpOutDSList:
                        tmpOutDSList.append(tmpItem)
                        strOutDS += '%s,' % tmpItem
            except:
                pass
            # set job status
            sStatus += job.jobStatus + ','
        # set    
        self.PandaID   = pStr[:-1]
        self.inDS      = strInDS[:-1]
        self.outDS     = strOutDS[:-1]
        self.jobStatus = sStatus[:-1]


    # check if JEDI
    def isJEDI(self):
        if self.jediTaskID in [-1,'-1','']:
            return False
        return True
