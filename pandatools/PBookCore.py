import json
import datetime
import time

try:
    long()
except Exception:
    long = int

from . import PdbUtils
from . import Client
from . import BookConfig
from . import PLogger
from . import PsubUtils

# core class for book keeping
class PBookCore:

    # constructor
    def __init__(self,enforceEnter=False,verbose=False,restoreDB=False):
        # verbose
        self.verbose = verbose
        # restore database
        self.restoreDB = restoreDB
        # initialize database
        PdbUtils.initialzieDB(self.verbose,self.restoreDB)
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # map between jobset and jediTaskID
        self.jobsetTaskMap = {}
 


    # synchronize database
    def sync(self):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        tmpLog.info("Synchronizing local repository ...")
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # get nickname
        nickName = PsubUtils.getNickname()
        # set Rucio accounting
        PsubUtils.setRucioAccount(nickName,'pbook',True)
        # get JobIDs in local repository
        localJobIDs = PdbUtils.getListOfJobIDs()
        # get recent JobIDs from panda server
        syncTimeRaw = datetime.datetime.utcnow()
        syncTime = syncTimeRaw.strftime('%Y-%m-%d %H:%M:%S')
        # set sync time for the first attempt
        bookConf = BookConfig.getConfig()
        if self.restoreDB:
            # reset last_synctime to restore database 
            bookConf.last_synctime = ''
        # disable
        self.restoreDB = False
        tmpLog.info("It may take several minutes to restore local repository ...")
        if bookConf.last_synctime == '':
            bookConf.last_synctime = datetime.datetime.utcnow()-datetime.timedelta(days=180)
            bookConf.last_synctime = bookConf.last_synctime.strftime('%Y-%m-%d %H:%M:%S')
        maxTaskID = None
        while True:
            status, jediTaskDicts = Client.getJobIDsJediTasksInTimeRange(bookConf.last_synctime,
                                                                         minTaskID=maxTaskID,
                                                                         verbose=self.verbose)
            if status != 0:
                tmpLog.error("Failed to get tasks from panda server")
                return
            if len(jediTaskDicts) == 0:
                break
            tmpLog.info("Got %s tasks to be updated" % len(jediTaskDicts))
            # insert if missing
            for remoteJobID in jediTaskDicts.keys():
                taskID = jediTaskDicts[remoteJobID]['jediTaskID']
                # get max
                if maxTaskID is None or taskID > maxTaskID:
                    maxTaskID = taskID
                # check local status
                job = None
                if remoteJobID in localJobIDs:
                    # get job info from local repository
                    job = PdbUtils.readJobDB(remoteJobID, self.verbose)
                    # skip if frozen
                    if job.dbStatus == 'frozen':
                        continue
                tmpLog.info("Updating taskID=%s ..." % taskID)
                # convert JEDI task
                localJob = PdbUtils.convertJTtoD(jediTaskDicts[remoteJobID],job)
                # update database
                if not remoteJobID in localJobIDs:
                    # insert to DB
                    try:
                        PdbUtils.insertJobDB(localJob,self.verbose)
                    except Exception as e:
                        tmpLog.error("Failed to insert taskID=%s to local repository: %s" % (taskID, str(e)))
                        return
                else:
                    # update
                    try:
                        PdbUtils.updateJobDB(localJob,self.verbose,syncTimeRaw)
                    except Exception as e:
                        tmpLog.error("Failed to update local repository for taskID=%s: %s" % (taskID, str(e)))
                        return
        # update sync time
        bookConf = BookConfig.getConfig()
        bookConf.last_synctime = syncTime
        BookConfig.updateConfig(bookConf)
        self.updateTaskJobsetMap()
        tmpLog.info("Synchronization Completed")
        

    # update task and jobset map
    def updateTaskJobsetMap(self):
        self.jobsetTaskMap = PdbUtils.getJobsetTaskMap()


    # get local job info
    def getJobInfo(self,JobID):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # convert taskID to jobsetID
        JobID = self.convertTaskToJobID(JobID)
        # get job info from local repository
        job = PdbUtils.readJobDB(JobID,self.verbose)
        # not found
        if job == None:
            tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % JobID)
            return None
        # return
        return job


    # get local job/jobset info
    def getJobJobsetInfo(self,id):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # try to get jobset
        job = PdbUtils.readJobsetDB(id,self.verbose)
        # get job info from local repository
        if job == None:
            job = PdbUtils.readJobDB(id,self.verbose)
        # not found
        if job == None:
            tmpLog.warning("JobsetID/JobID=%s not found in local repository. Synchronization may be needed" % JobID)
            return None
        # return
        return job


    # get local job list
    def getLocalJobList(self):
        # get jobs
        localJobs = PdbUtils.bulkReadJobDB(self.verbose)
        # return
        return localJobs


    # get JobIDs with JobsetID
    def getJobIDsWithSetID(self,jobsetID):
        # convert taskID to jobsetID
        jobsetID = self.convertTaskToJobID(jobsetID)
        idMap = PdbUtils.getMapJobsetIDJobIDs(self.verbose)
        if jobsetID in idMap:
            return idMap[jobsetID]
        return None


    # make JobSetSpec
    def makeJobsetSpec(self,jobList):
        return PdbUtils.makeJobsetSpec(jobList)

    # get status
    def status(self,JobID,forceUpdate=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # get job info from local repository
        job = self.getJobInfo(JobID)
        if job == None:
            # not found
            return None
        # update if needed
        if job.dbStatus != 'frozen' or forceUpdate:
            tmpLog.info("Getting status for TaskID=%s ..." % job.jediTaskID)
            # get JEDI task
            status,jediTaskDict = Client.getJediTaskDetails(
                    {'jediTaskID':job.jediTaskID},
                    False,
                    True,
                    verbose=self.verbose)
            if status != 0:
                tmpLog.error("Failed to get task details for %s" % JobID)
                return
            # convert JEDI task
            job = PdbUtils.convertJTtoD(jediTaskDict,job)
            # update DB
            try:
                PdbUtils.updateJobDB(job,self.verbose)
            except Exception as e:
                tmpLog.error("Failed to update local repository for JobID=%s: %s" % (JobID, str(e)))
                return None
            if not job.isJEDI():
                tmpLog.info("Updated JobID=%s" % JobID)                        
            else:
                tmpLog.info("Updated TaskID=%s ..." % job.jediTaskID)
        # return
        return job

    # get status for JobSet and Job
    def statusJobJobset(self, id, forceUpdate=False):
        tmpJobIDList = self.getJobIDsWithSetID(id)
        if tmpJobIDList == None:
            # not a jobset
            job = self.status(id, forceUpdate)
        else:
            # jobset
            tmpJobs = []
            tmpMergeIdList = []
            isJEDI = False
            for tmpJobID in tmpJobIDList:
                tmpJob = self.status(tmpJobID, forceUpdate)
                if tmpJob == None:
                    return None
                tmpJobs.append(tmpJob)
                if tmpJob.isJEDI():
                    isJEDI = True
                else:
                    if tmpJob.mergeJobID != '':
                        for tmpMergeID in tmpJob.mergeJobID.split(','):
                            tmpMergeIdList.append(long(tmpMergeID))
            if not isJEDI:
                # check merge jobs are already got
                tmpIDtoBeChecked = []
                for tmpMergeID in tmpMergeIdList:
                    if not tmpMergeID in tmpJobIDList:
                        tmpIDtoBeChecked.append(tmpMergeID)
                # sync to get merge job info
                if tmpIDtoBeChecked != []:
                    self.sync()
                    for tmpJobID in tmpIDtoBeChecked:
                        tmpJob = self.status(tmpJobID, forceUpdate)
                        tmpJobs.append(tmpJob)
            # make jobset
            job = self.makeJobsetSpec(tmpJobs)
        # return
        return job


    # kill
    def kill(self,JobID,useJobsetID=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        self.status(JobID,True)
        # get jobset
        jobList = self.getJobIDsWithSetID(JobID)
        if jobList == None:
            # works only for jobsetID
            if useJobsetID:
                return
            # works with jobID
            jobList = [JobID]
        else:
            tmpMsg = "ID=%s is composed of JobID=" % JobID
            for tmpJobID in jobList:
                tmpMsg += '%s,' % tmpJobID
            tmpMsg = tmpMsg[:-1]
            tmpLog.info(tmpMsg)
        for tmpJobID in jobList:    
            # get job info from local repository
            job = self.getJobInfo(tmpJobID)
            if job == None:
                tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % tmpJobID)            
                continue
            # skip frozen job
            if job.dbStatus == 'frozen':
                tmpLog.info('All subJobs in JobID=%s already finished/failed' % tmpJobID)
                continue
            # kill JEDI task
            tmpLog.info('Sending killTask command ...')
            status,output = Client.killTask(job.jediTaskID,self.verbose)
            # communication error
            if status != 0:
                tmpLog.error(output)
                tmpLog.error("Failed to kill JobID=%s" % tmpJobID)
                return False
            tmpStat,tmpDiag = output
            if not tmpStat:
                tmpLog.error(tmpDiag)
                tmpLog.error("Failed to kill JobID=%s" % tmpJobID)
                return False
            tmpLog.info(tmpDiag)
            # done
            tmpLog.info('Done. TaskID=%s will be killed in 30min' % job.jediTaskID)
        return True

    # finish
    def finish(self,JobID,soft=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        self.status(JobID,True)
        # get jobset
        jobList = self.getJobIDsWithSetID(JobID)
        if jobList == None:
            # works with jobID
            jobList = [JobID]
        else:
            tmpMsg = "ID=%s is composed of JobID=" % JobID
            for tmpJobID in jobList:
                tmpMsg += '%s,' % tmpJobID
            tmpMsg = tmpMsg[:-1]
            tmpLog.info(tmpMsg)
        for tmpJobID in jobList:    
            # get job info from local repository
            job = self.getJobInfo(tmpJobID)
            if job == None:
                tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % tmpJobID)            
                continue
            # skip frozen job
            if job.dbStatus == 'frozen':
                tmpLog.info('All subJobs in JobID=%s already finished/failed' % tmpJobID)
                continue
            # finish JEDI task
            tmpLog.info('Sending finishTask command ...')
            status,output = Client.finishTask(job.jediTaskID,soft,self.verbose)
            # communication error
            if status != 0:
                tmpLog.error(output)
                tmpLog.error("Failed to finish JobID=%s" % tmpJobID)
                return False
            tmpStat,tmpDiag = output
            if not tmpStat:
                tmpLog.error(tmpDiag)
                tmpLog.error("Failed to finish JobID=%s" % tmpJobID)
                return False
            tmpLog.info(tmpDiag)
        # done
        tmpLog.info('Done. TaskID=%s will be finished soon' % job.jediTaskID)
        return True

    # set debug mode
    def debug(self,pandaID,modeOn):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # set
        status,output = Client.setDebugMode(pandaID,modeOn,self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to set debug mode for %s" % pandaID)
            return
        # done
        tmpLog.info(output)
        return

    # clean
    def clean(self,nDays=180):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # delete
        try:
            PdbUtils.deleteOldJobs(nDays,self.verbose)
        except:
            tmpLog.error("Failed to delete old jobs")
            return
        # done
        tmpLog.info('Done')
        return


    # kill and retry
    def killAndRetry(self,JobID,newSite=False,newOpts={},ignoreDuplication=False,retryBuild=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill
        retK = self.kill(JobID)
        if not retK:
            return False
        # sleep
        tmpLog.info('Going to sleep for 5sec')
        time.sleep(5)
        nTry = 6
        for iTry in range(nTry):
            # get status
            job = self.status(JobID)
            if job == None:
                return False
            # check if frozen
            if job.dbStatus == 'frozen':
                break
            tmpLog.info('Some sub-jobs are still running')
            if iTry+1 < nTry:
                # sleep
                tmpLog.info('Going to sleep for 10min')
                time.sleep(600)
            else:
                tmpLog.info('Max attempts exceeded. Please try later')
                return False
        # retry
        self.retry(
            JobID,
            newSite=newSite,
            newOpts=newOpts,
            ignoreDuplication=ignoreDuplication,
            retryBuild=retryBuild)
        return
                        

    # retry
    def retry(self,JobsetID,newSite=False,newOpts={},noSubmit=False,ignoreDuplication=False,useJobsetID=False,retryBuild=False,reproduceFiles=[],unsetRetryID=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        self.status(JobsetID,True)
        # set an empty map since mutable default value is used
        if newOpts == {}:
            newOpts = {}
        # get jobset
        newJobsetID = -1
        jobList = self.getJobIDsWithSetID(JobsetID)
        if jobList == None:
            # works only for jobsetID
            if useJobsetID:
                return
            # works with jobID   
            isJobset = False
            jobList = [JobsetID]
        else:
            isJobset = True
            tmpMsg = "ID=%s is composed of JobID=" % JobsetID
            for tmpJobID in jobList:
                tmpMsg += '%s,' % tmpJobID
            tmpMsg = tmpMsg[:-1]
            tmpLog.info(tmpMsg)
        for JobID in jobList:    
            # get job info from local repository
            localJob = self.getJobInfo(JobID)
            if localJob == None:
                tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % JobID)            
                return None
            # for JEDI
            status,out = Client.retryTask(
                    localJob.jediTaskID,
                    verbose=self.verbose,
                    properErrorCode=True,
                    newParams=newOpts)
            if status != 0:
                tmpLog.error(status)
                tmpLog.error(out)
                tmpLog.error("Failed to retry TaskID=%s" % localJob.jediTaskID)
                return False
            tmpStat,tmpDiag = out
            if (not tmpStat in [0,True] and newOpts == {}) or (newOpts != {} and tmpStat != 3):
                tmpLog.error(tmpDiag)
                tmpLog.error("Failed to retry TaskID=%s" % localJob.jediTaskID)
                return False
            tmpLog.info(tmpDiag)
            continue

    # convert taskID to jobsetID
    def convertTaskToJobID(self,taskID):
        if taskID in self.jobsetTaskMap:
            return self.jobsetTaskMap[taskID]
        return taskID

    # get job metadata
    def getUserJobMetadata(self, jobID, output_filename):
        job = self.getJobInfo(jobID)
        # get logger
        tmpLog = PLogger.getPandaLogger()
        if job is None:
            tmpLog.error('cannot find a task with {0}. May need to sync first'.format(jobID))
            return False
        # get metadata
        task_id = job.jediTaskID
        tmpLog.info('getting metadata')
        status, metadata = Client.getUserJobMetadata(task_id, verbose=self.verbose)
        if status != 0:
            tmpLog.error(metadata)
            tmpLog.error("Failed to get metadata")
            return False
        with open(output_filename, 'w') as f:
            json.dump(metadata, f)
        tmpLog.info('dumped to {0}'.format(output_filename))
        # return
        return True
