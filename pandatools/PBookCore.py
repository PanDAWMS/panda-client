import json
import datetime
import time
import copy
import sys

try:
    long()
except Exception:
    long = int

# from . import PdbUtils
from . import Client
# from . import BookConfig
from . import PLogger
from . import PsubUtils
from pandatools import queryPandaMonUtils
from pandatools import localSpecs
from pandatools import MiscUtils


def _get_one_task(self, taskID):
    """
    get one task spec by ID
    """
    ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, jeditaskid=taskID)
    if isinstance(data, list) and data:
        task = data[0]
        taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
        return taskspec
    else:
        return None

def check_task_owner(func):
    """
    sanity check decorator of user ownership vs the task
    """
    # Wrapper
    def wrapper(self, *args, **kwargs):
        # Make logger
        tmpLog = PLogger.getPandaLogger()
        # initialize
        ret = None
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # check task owner
        try:
            jeditaskid = None
            if args:
                jeditaskid = args[0]
            elif kwargs:
                jeditaskid = kwargs.get('taskID')
            if jeditaskid is None:
                tmpLog.error('no taskID sepcified, nothing done')
                return
            taskspec = _get_one_task(self, jeditaskid)
        except Exception as e:
            tmpLog.error('got {0}: {1}'.format(e.__class__.__name__, e))
        else:
            if taskspec is not None and taskspec.username == self.username:
                ret = func(self, *args, **kwargs)
            else:
                sys.stdout.write('Permission denied: taskID={0} is not owned by {1} \n'.format(jeditaskid, self.username))
        return ret
    return wrapper


# core class for book keeping
class PBookCore(object):

    # constructor
    def __init__(self, verbose=False):
        # verbose
        self.verbose = verbose
        # restore database
        # self.restoreDB = restoreDB
        # initialize database
        # PdbUtils.initialzieDB(self.verbose,self.restoreDB)
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # user name
        self.username = 'DEFAULT_USER'
        username_from_proxy = MiscUtils.extract_voms_proxy_username()
        if username_from_proxy:
            self.username = username_from_proxy
        # map between jobset and jediTaskID
        self.jobsetTaskMap = {}


    # synchronize database
    # def sync(self):
    #     # get logger
    #     tmpLog = PLogger.getPandaLogger()
    #     tmpLog.info("Synchronizing local repository ...")
    #     # check proxy
    #     PsubUtils.check_proxy(self.verbose, None)
    #     # get nickname
    #     nickName = PsubUtils.getNickname()
    #     # set Rucio accounting
    #     PsubUtils.setRucioAccount(nickName,'pbook',True)
    #     # get JobIDs in local repository
    #     localJobIDs = PdbUtils.getListOfJobIDs()
    #     # get recent JobIDs from panda server
    #     syncTimeRaw = datetime.datetime.utcnow()
    #     syncTime = syncTimeRaw.strftime('%Y-%m-%d %H:%M:%S')
    #     # set sync time for the first attempt
    #     bookConf = BookConfig.getConfig()
    #     if self.restoreDB:
    #         # reset last_synctime to restore database
    #         bookConf.last_synctime = ''
    #     # disable
    #     self.restoreDB = False
    #     tmpLog.info("It may take several minutes to restore local repository ...")
    #     if bookConf.last_synctime == '':
    #         bookConf.last_synctime = datetime.datetime.utcnow()-datetime.timedelta(days=180)
    #         bookConf.last_synctime = bookConf.last_synctime.strftime('%Y-%m-%d %H:%M:%S')
    #     maxTaskID = None
    #     while True:
    #         status, jediTaskDicts = Client.getJobIDsJediTasksInTimeRange(bookConf.last_synctime,
    #                                                                      minTaskID=maxTaskID,
    #                                                                      verbose=self.verbose)
    #         if status != 0:
    #             tmpLog.error("Failed to get tasks from panda server")
    #             return
    #         if len(jediTaskDicts) == 0:
    #             break
    #         tmpLog.info("Got %s tasks to be updated" % len(jediTaskDicts))
    #         # insert if missing
    #         for remoteJobID in jediTaskDicts.keys():
    #             taskID = jediTaskDicts[remoteJobID]['jediTaskID']
    #             # get max
    #             if maxTaskID is None or taskID > maxTaskID:
    #                 maxTaskID = taskID
    #             # check local status
    #             job = None
    #             if remoteJobID in localJobIDs:
    #                 # get job info from local repository
    #                 job = PdbUtils.readJobDB(remoteJobID, self.verbose)
    #                 # skip if frozen
    #                 if job.dbStatus == 'frozen':
    #                     continue
    #             tmpLog.info("Updating taskID=%s ..." % taskID)
    #             # convert JEDI task
    #             localJob = PdbUtils.convertJTtoD(jediTaskDicts[remoteJobID],job)
    #             # update database
    #             if remoteJobID not in localJobIDs:
    #                 # insert to DB
    #                 try:
    #                     PdbUtils.insertJobDB(localJob,self.verbose)
    #                 except Exception as e:
    #                     tmpLog.error("Failed to insert taskID=%s to local repository: %s" % (taskID, str(e)))
    #                     return
    #             else:
    #                 # update
    #                 try:
    #                     PdbUtils.updateJobDB(localJob,self.verbose,syncTimeRaw)
    #                 except Exception as e:
    #                     tmpLog.error("Failed to update local repository for taskID=%s: %s" % (taskID, str(e)))
    #                     return
    #     # update sync time
    #     bookConf = BookConfig.getConfig()
    #     bookConf.last_synctime = syncTime
    #     BookConfig.updateConfig(bookConf)
    #     self.updateTaskJobsetMap()
    #     tmpLog.info("Synchronization Completed")


    # update task and jobset map
    # def updateTaskJobsetMap(self):
    #     self.jobsetTaskMap = PdbUtils.getJobsetTaskMap()


    # get local job info
    # def getJobInfo(self,JobID):
    #     # get logger
    #     tmpLog = PLogger.getPandaLogger()
    #     # convert taskID to jobsetID
    #     JobID = self.convertTaskToJobID(JobID)
    #     # get job info from local repository
    #     job = PdbUtils.readJobDB(JobID,self.verbose)
    #     # not found
    #     if job is None:
    #         tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % JobID)
    #         return None
    #     # return
    #     return job


    # get local job/jobset info
    # def getJobJobsetInfo(self,id):
    #     # get logger
    #     tmpLog = PLogger.getPandaLogger()
    #     # try to get jobset
    #     job = PdbUtils.readJobsetDB(id,self.verbose)
    #     # get job info from local repository
    #     if job is None:
    #         job = PdbUtils.readJobDB(id,self.verbose)
    #     # not found
    #     if job is None:
    #         tmpLog.warning("JobsetID/JobID=%s not found in local repository. Synchronization may be needed" % JobID)
    #         return None
    #     # return
    #     return job


    # get local job list
    # def getLocalJobList(self):
    #     # get jobs
    #     localJobs = PdbUtils.bulkReadJobDB(self.verbose)
    #     # return
    #     return localJobs


    # get JobIDs with JobsetID
    # def getJobIDsWithSetID(self,jobsetID):
    #     # convert taskID to jobsetID
    #     jobsetID = self.convertTaskToJobID(jobsetID)
    #     idMap = PdbUtils.getMapJobsetIDJobIDs(self.verbose)
    #     if jobsetID in idMap:
    #         return idMap[jobsetID]
    #     return None


    # make JobSetSpec
    # def makeJobsetSpec(self,jobList):
    #     return PdbUtils.makeJobsetSpec(jobList)

    # get status
    # def status(self,JobID,forceUpdate=False):
    #     # get logger
    #     tmpLog = PLogger.getPandaLogger()
    #     # check proxy
    #     PsubUtils.check_proxy(self.verbose, None)
    #     # get job info from local repository
    #     job = self.getJobInfo(JobID)
    #     if job is None:
    #         # not found
    #         return None
    #     # update if needed
    #     if job.dbStatus != 'frozen' or forceUpdate:
    #         tmpLog.info("Getting status for TaskID=%s ..." % job.jediTaskID)
    #         # get JEDI task
    #         status,jediTaskDict = Client.getJediTaskDetails(
    #                 {'jediTaskID':job.jediTaskID},
    #                 False,
    #                 True,
    #                 verbose=self.verbose)
    #         if status != 0:
    #             tmpLog.error("Failed to get task details for %s" % JobID)
    #             return
    #         # convert JEDI task
    #         job = PdbUtils.convertJTtoD(jediTaskDict,job)
    #         # update DB
    #         try:
    #             PdbUtils.updateJobDB(job,self.verbose)
    #         except Exception as e:
    #             tmpLog.error("Failed to update local repository for JobID=%s: %s" % (JobID, str(e)))
    #             return None
    #         if not job.isJEDI():
    #             tmpLog.info("Updated JobID=%s" % JobID)
    #         else:
    #             tmpLog.info("Updated TaskID=%s ..." % job.jediTaskID)
    #     # return
    #     return job

    # get status for JobSet and Job
    # def statusJobJobset(self, id, forceUpdate=False):
    #     tmpJobIDList = self.getJobIDsWithSetID(id)
    #     if tmpJobIDList is None:
    #         # not a jobset
    #         job = self.status(id, forceUpdate)
    #     else:
    #         # jobset
    #         tmpJobs = []
    #         tmpMergeIdList = []
    #         isJEDI = False
    #         for tmpJobID in tmpJobIDList:
    #             tmpJob = self.status(tmpJobID, forceUpdate)
    #             if tmpJob is None:
    #                 return None
    #             tmpJobs.append(tmpJob)
    #             if tmpJob.isJEDI():
    #                 isJEDI = True
    #             else:
    #                 if tmpJob.mergeJobID != '':
    #                     for tmpMergeID in tmpJob.mergeJobID.split(','):
    #                         tmpMergeIdList.append(long(tmpMergeID))
    #         if not isJEDI:
    #             # check merge jobs are already got
    #             tmpIDtoBeChecked = []
    #             for tmpMergeID in tmpMergeIdList:
    #                 if tmpMergeID not in tmpJobIDList:
    #                     tmpIDtoBeChecked.append(tmpMergeID)
    #             # sync to get merge job info
    #             if tmpIDtoBeChecked != []:
    #                 self.sync()
    #                 for tmpJobID in tmpIDtoBeChecked:
    #                     tmpJob = self.status(tmpJobID, forceUpdate)
    #                     tmpJobs.append(tmpJob)
    #         # make jobset
    #         job = self.makeJobsetSpec(tmpJobs)
    #     # return
    #     return job


    # kill
    # def kill(self,JobID,useJobsetID=False):
    @check_task_owner
    def kill(self, taskID):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # # check proxy
        # PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        # self.status(JobID,True)
        # get jobset
        # jobList = self.getJobIDsWithSetID(JobID)
        # if jobList is None:
        #     # works only for jobsetID
        #     # if useJobsetID:
        #     #     return
        #     # works with jobID
        #     jobList = [JobID]
        # else:
        #     tmpMsg = "ID=%s is composed of JobID=" % JobID
        #     for tmpJobID in jobList:
        #         tmpMsg += '%s,' % tmpJobID
        #     tmpMsg = tmpMsg[:-1]
        #     tmpLog.info(tmpMsg)
        # for tmpJobID in jobList:
        #     # get job info from local repository
        #     job = self.getJobInfo(tmpJobID)
        #     if job is None:
        #         tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % tmpJobID)
        #         continue
        #     # skip frozen job
        #     if job.dbStatus == 'frozen':
        #         tmpLog.info('All subJobs in JobID=%s already finished/failed' % tmpJobID)
        #         continue
        #     # kill JEDI task
        #     tmpLog.info('Sending killTask command ...')
        #     status,output = Client.killTask(job.jediTaskID,self.verbose)
        #     # communication error
        #     if status != 0:
        #         tmpLog.error(output)
        #         tmpLog.error("Failed to kill JobID=%s" % tmpJobID)
        #         return False
        #     tmpStat,tmpDiag = output
        #     if not tmpStat:
        #         tmpLog.error(tmpDiag)
        #         tmpLog.error("Failed to kill JobID=%s" % tmpJobID)
        #         return False
        #     tmpLog.info(tmpDiag)
        #     # done
        #     tmpLog.info('Done. TaskID=%s will be killed in 30min' % job.jediTaskID)

        # kill JEDI task
        tmpLog.info('Sending killTask command ...')
        status, output = Client.killTask(taskID, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to kill jediTaskID=%s' % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to kill jediTaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info('Done, jediTaskID=%s will be killed in 30 min' % taskID)
        return True

    # finish
    # def finish(self,JobID,soft=False):
    @check_task_owner
    def finish(self, taskID, soft=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        # PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        # self.status(JobID,True)
        # get jobset
        # jobList = self.getJobIDsWithSetID(JobID)
        # if jobList is None:
        #     # works with jobID
        #     jobList = [JobID]
        # else:
        #     tmpMsg = "ID=%s is composed of JobID=" % JobID
        #     for tmpJobID in jobList:
        #         tmpMsg += '%s,' % tmpJobID
        #     tmpMsg = tmpMsg[:-1]
        #     tmpLog.info(tmpMsg)
        # for tmpJobID in jobList:
        #     # get job info from local repository
        #     job = self.getJobInfo(tmpJobID)
        #     if job is None:
        #         tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % tmpJobID)
        #         continue
        #     # skip frozen job
        #     if job.dbStatus == 'frozen':
        #         tmpLog.info('All subJobs in JobID=%s already finished/failed' % tmpJobID)
        #         continue
        #     # finish JEDI task
        #     tmpLog.info('Sending finishTask command ...')
        #     status,output = Client.finishTask(job.jediTaskID,soft,self.verbose)
        #     # communication error
        #     if status != 0:
        #         tmpLog.error(output)
        #         tmpLog.error("Failed to finish JobID=%s" % tmpJobID)
        #         return False
        #     tmpStat,tmpDiag = output
        #     if not tmpStat:
        #         tmpLog.error(tmpDiag)
        #         tmpLog.error("Failed to finish JobID=%s" % tmpJobID)
        #         return False
        #     tmpLog.info(tmpDiag)

        # finish JEDI task
        tmpLog.info('Sending finishTask command ...')
        status, output = Client.finishTask(taskID, soft, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to finish jediTaskID=%s' % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to finish jediTaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info('Done, jediTaskID=%s will be finished soon' % taskID)
        return True

    # set debug mode
    def debug(self, pandaID, modeOn):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # set
        status,output = Client.setDebugMode(pandaID,modeOn,self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to set debug mode for %s' % pandaID)
            return
        # done
        tmpLog.info(output)
        return

    # clean
    # def clean(self,nDays=180):
    #     # get logger
    #     tmpLog = PLogger.getPandaLogger()
    #     # delete
    #     try:
    #         PdbUtils.deleteOldJobs(nDays,self.verbose)
    #     except Exception:
    #         tmpLog.error("Failed to delete old jobs")
    #         return
    #     # done
    #     tmpLog.info('Done')
    #     return


    # kill and retry
    # def killAndRetry(self,JobID,newSite=False,newOpts={},ignoreDuplication=False,retryBuild=False):
    def killAndRetry(self, taskID, newOpts={}):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill
        retK = self.kill(taskID)
        if not retK:
            return False
        # sleep
        tmpLog.info('Going to sleep for 3 sec')
        time.sleep(3)
        nTry = 6
        for iTry in range(nTry):
            # get status
            # job = self.status(JobID)
            # if job is None:
            #     return False
            # # check if frozen
            # if job.dbStatus == 'frozen':
            #     break
            # check if task terminated
            taskspec = _get_one_task(self, taskID)
            if taskspec is not None:
                if taskspec.is_terminated():
                    break
                else:
                    tmpLog.info('Some sub-jobs are still running')
            else:
                tmpLog.warning('Could not get task status from panda monitor...')
            if iTry + 1 < nTry:
                # sleep
                tmpLog.info('Going to sleep for 30 sec')
                time.sleep(30)
            else:
                tmpLog.info('Max attempts exceeded. Please try later')
                return False
        # retry
        self.retry(taskID, newOpts=newOpts)
        return


    # retry
    # def retry(self,JobsetID,newSite=False,newOpts={},noSubmit=False,ignoreDuplication=False,useJobsetID=False,retryBuild=False,reproduceFiles=[],unsetRetryID=False):
    @check_task_owner
    def retry(self, taskID, newOpts={}):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        # PsubUtils.check_proxy(self.verbose, None)
        # force update just in case
        # self.status(JobsetID,True)
        # set an empty map since mutable default value is used
        if newOpts == {}:
            newOpts = {}
        else:
            newOpts = copy.deepcopy(newOpts)
        # # get jobset
        # newJobsetID = -1
        # jobList = self.getJobIDsWithSetID(JobsetID)
        # if jobList is None:
        #     # works only for jobsetID
        #     if useJobsetID:
        #         return
        #     # works with jobID
        #     isJobset = False
        #     jobList = [JobsetID]
        # else:
        #     isJobset = True
        #     tmpMsg = "ID=%s is composed of JobID=" % JobsetID
        #     for tmpJobID in jobList:
        #         tmpMsg += '%s,' % tmpJobID
        #     tmpMsg = tmpMsg[:-1]
        #     tmpLog.info(tmpMsg)
        # for JobID in jobList:
        #     # get job info from local repository
        #     localJob = self.getJobInfo(JobID)
        #     if localJob is None:
        #         tmpLog.warning("JobID=%s not found in local repository. Synchronization may be needed" % JobID)
        #         return None
        #     # for JEDI
        #     status,out = Client.retryTask(
        #             localJob.jediTaskID,
        #             verbose=self.verbose,
        #             properErrorCode=True,
        #             newParams=newOpts)
        #     if status != 0:
        #         tmpLog.error(status)
        #         tmpLog.error(out)
        #         tmpLog.error("Failed to retry TaskID=%s" % localJob.jediTaskID)
        #         return False
        #     tmpStat,tmpDiag = out
        #     if (tmpStat not in [0,True] and newOpts == {}) or (newOpts != {} and tmpStat != 3):
        #         tmpLog.error(tmpDiag)
        #         tmpLog.error("Failed to retry TaskID=%s" % localJob.jediTaskID)
        #         return False
        #     tmpLog.info(tmpDiag)
        #     continue

        # for JEDI
        status,out = Client.retryTask(  taskID,
                                        verbose=self.verbose,
                                        properErrorCode=True,
                                        newParams=newOpts)
        if status != 0:
            tmpLog.error(status)
            tmpLog.error(out)
            tmpLog.error('Failed to retry TaskID=%s' % taskID)
            return False
        tmpStat,tmpDiag = out
        if (tmpStat not in [0,True] and newOpts == {}) or (newOpts != {} and tmpStat != 3):
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to retry TaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)

    # convert taskID to jobsetID
    # def convertTaskToJobID(self,taskID):
    #     if taskID in self.jobsetTaskMap:
    #         return self.jobsetTaskMap[taskID]
    #     return taskID

    # get job metadata
    def getUserJobMetadata(self, taskID, output_filename):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # get metadata
        tmpLog.info('getting metadata')
        status, metadata = Client.getUserJobMetadata(taskID, verbose=self.verbose)
        if status != 0:
            tmpLog.error(metadata)
            tmpLog.error("Failed to get metadata")
            return False
        with open(output_filename, 'w') as f:
            json.dump(metadata, f)
        tmpLog.info('dumped to {0}'.format(output_filename))
        # return
        return True

    # get task specs of active tasks
    def get_active_tasks(self):
        """
        get all reachable task specs of the user
        """
        active_superstatus_str = '|'.join(localSpecs.task_active_superstatus_list)
        ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, superstatus=active_superstatus_str)
        if isinstance(data, list) and list:
            taskspec_list = [ localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts) for task in data ]
            return taskspec_list
        else:
            return None

    # show status
    def show(self, username=None, limit=1000, taskname=None, days=14, jeditaskid=None,
                metadata=False, sync=False, format='standard', verbose=False):
        # user name
        if username is None:
            username = self.username
        # query
        ts, url, data = queryPandaMonUtils.query_tasks( username=username, limit=limit,
                                                        taskname=taskname, days=days, jeditaskid=jeditaskid,
                                                        metadata=metadata, sync=sync)
        # verbose
        if verbose:
            print('timestamp: {ts} \nquery_url: {url}'.format(ts=ts, url=url))
        # print header row
        _tmpts = localSpecs.LocalTaskSpec
        if format in ['json', 'plain']:
            pass
        elif format == 'long':
            print(_tmpts.head_dict['long'])
        else:
            print(_tmpts.head_dict['standard'])
        # print tasks
        if format == 'json':
            print(json.dumps(data, sort_keys=True, indent=4))
        elif format == 'plain':
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_plain()
        elif format == 'long':
            i_count = 1
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                if i_count % 10 == 0:
                    print(_tmpts.head_dict['long'])
                taskspec.print_long()
                i_count += 1
        else:
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_standard()
