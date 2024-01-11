import datetime
import os
import re
import sys
import time

from .MiscUtils import commands_get_status_output

try:
    long()
except Exception:
    long = int

from . import PLogger
from .LocalJobsetSpec import LocalJobsetSpec
from .LocalJobSpec import LocalJobSpec


class PdbProxy:
    # constructor
    def __init__(self, verbose=False):
        # database engine
        self.engine = "sqlite3"
        # version of database schema
        self.version = "0_0_1"
        # database file name
        self.filename = "pandajob.db"
        # database dir
        self.database_dir = os.path.expanduser(os.environ["PANDA_CONFIG_ROOT"])
        # full path of database file
        self.database = "%s/%s" % (self.database_dir, self.filename)
        # table name
        self.tablename = "jobtable_%s" % self.version
        # verbose
        self.verbose = verbose
        # connection
        self.con = None
        # logger
        self.log = PLogger.getPandaLogger()

    # set verbose
    def setVerbose(self, verbose):
        # verbose
        self.verbose = verbose

    # execute SQL
    def execute(self, sql, var={}):
        # logger
        tmpLog = PLogger.getPandaLogger()
        # expand variables
        for tmpKey in var:
            tmpVal = var[tmpKey]
            sql = sql.replqce(tmpKey, str(tmpVal))
        # construct command
        com = '%s %s "%s"' % (self.engine, self.database, sql)
        if self.verbose:
            tmpLog.debug("DB Req : " + com)
        # execute
        nTry = 5
        status = 0
        for iTry in range(nTry):
            if self.verbose:
                tmpLog.debug("   Try : %s/%s" % (iTry, nTry))
            status, output = commands_get_status_output(com)
            status %= 255
            if status == 0:
                break
            if iTry + 1 < nTry:
                time.sleep(2)
        # return
        if status != 0:
            tmpLog.error(status)
            tmpLog.error(output)
            return False, output
        else:
            if self.verbose:
                tmpLog.debug("   Ret : " + output)
            outList = output.split("\n")
            # remove ''
            try:
                outList.remove("")
            except Exception:
                pass
            # remove junk messages
            ngStrings = ["Loading resources from"]
            for tmpStr in tuple(outList):
                # look for NG strings
                flagNG = False
                for ngStr in ngStrings:
                    match = re.search(ngStr, tmpStr, re.I)
                    if match is not None:
                        flagNG = True
                        break
                # remove
                if flagNG:
                    try:
                        outList.remove(tmpStr)
                    except Exception:
                        pass
            return True, outList

    # execute SQL
    def execute_direct(self, sql, var=None, fetch=False):
        if self.con is None:
            import sqlite3

            self.con = sqlite3.connect(self.database, check_same_thread=False)
        if self.verbose:
            self.log.debug("DB Req : {0} var={1}".format(sql, str(var)))
        cur = self.con.cursor()
        try:
            if var is None:
                var = {}
            cur.execute(sql, var)
            retVal = True
        except Exception:
            retVal = False
            if not self.verbose:
                self.log.error("DB Req : {0} var={1}".format(sql, str(var)))
            err_type, err_value = sys.exc_info()[:2]
            err_str = "{0} {1}".format(err_type.__name__, err_value)
            self.log.error(err_str)
        if self.verbose:
            self.log.debug(retVal)
        outList = []
        if retVal:
            if fetch:
                outList = cur.fetchall()
                if self.verbose:
                    for item in outList:
                        self.log.debug("   Ret : " + str(item))
        self.con.commit()
        return retVal, outList

    # remove old database
    def deleteDatabase(self):
        commands_get_status_output("rm -f %s" % self.database)

    # initialize database
    def initialize(self):
        # import sqlite3
        # check if sqlite3 is available
        com = "which %s" % self.engine
        status, output = commands_get_status_output(com)
        if status != 0:
            errstr = "\n\n"
            errstr += "ERROR : %s is not available in PATH\n\n" % self.engine
            errstr += "There are some possible solutions\n"
            errstr += " * run this application under Athena runtime with Release 14 or higher. e.g.,\n"
            errstr += "   $ source setup.sh -tag=14.2.24,32,setup\n"
            errstr += "   $ source .../etc/panda/panda_setup.sh\n\n"
            errstr += " * set PATH and LD_LIBRARY_PATH to include %s. e.g., at CERN\n" % self.engine
            errstr += "   $ export PATH=/afs/cern.ch/sw/lcg/external/sqlite/3.4.0/slc3_ia32_gcc323/bin:$PATH\n"
            errstr += "   $ export LD_LIBRARY_PATH=/afs/cern.ch/sw/lcg/external/sqlite/3.4.0/slc3_ia32_gcc323/lib:$LD_LIBRARY_PATH\n"
            errstr += "   $ source .../etc/panda/panda_setup.sh\n\n"
            errstr += " * install %s from the standard SL4 repository. e.g.,\n" % self.engine
            errstr += "   $ yum install %s\n\n" % self.engine
            errstr += " * use SLC5\n"
            raise RuntimeError(errstr)
        # create dir for DB
        if not os.path.exists(self.database_dir):
            os.makedirs(self.database_dir)
        # the table already exist
        if self.checkTable():
            return
        # create table
        self.createTable()
        return

    # check table
    def checkTable(self):
        # get tables
        retS, retV = self.execute(".table")
        if not retS:
            raise RuntimeError("cannot get tables")
        # the table already exist or not
        if retV == []:
            return False
        if self.tablename not in retV[-1].split():
            return False
        # check schema
        self.checkSchema()
        return True

    # check schema
    def checkSchema(self, noAdd=False):
        # get colum names
        retS, retV = self.execute("PRAGMA table_info(%s)" % self.tablename)
        if not retS:
            raise RuntimeError("cannot get table_info")
        # parse
        columns = []
        for line in retV:
            items = line.split("|")
            if len(items) > 1:
                columns.append(items[1])
        # check
        for tmpC in LocalJobSpec.appended:
            tmpA = LocalJobSpec.appended[tmpC]
            if tmpC not in columns:
                if noAdd:
                    raise RuntimeError("%s not found in database schema" % tmpC)
                # add column
                retS, retV = self.execute("ALTER TABLE %s ADD COLUMN '%s' %s" % (self.tablename, tmpC, tmpA))
                if not retS:
                    raise RuntimeError("cannot add %s to database schema" % tmpC)
        if noAdd:
            return
        # check whole schema just in case
        self.checkSchema(noAdd=True)

    # create table
    def createTable(self):
        # ver 0_1_1
        sql = "CREATE TABLE %s (" % self.tablename
        sql += "'id'             INTEGER PRIMARY KEY,"
        sql += "'JobID'          INTEGER,"
        sql += "'PandaID'        TEXT,"
        sql += "'jobStatus'      TEXT,"
        sql += "'site'           VARCHAR(128),"
        sql += "'cloud'          VARCHAR(20),"
        sql += "'jobType'        VARCHAR(20),"
        sql += "'jobName'        VARCHAR(128),"
        sql += "'inDS'           TEXT,"
        sql += "'outDS'          TEXT,"
        sql += "'libDS'          VARCHAR(255),"
        sql += "'jobParams'      TEXT,"
        sql += "'retryID'        INTEGER,"
        sql += "'provenanceID'   INTEGER,"
        sql += "'creationTime'   TIMESTAMP,"
        sql += "'lastUpdate'     TIMESTAMP,"
        sql += "'dbStatus'       VARCHAR(20),"
        sql += "'buildStatus'    VARCHAR(20),"
        sql += "'commandToPilot' VARCHAR(20),"
        for tmpC in LocalJobSpec.appended:
            tmpA = LocalJobSpec.appended[tmpC]
            sql += "'%s' %s," % (tmpC, tmpA)
        sql = sql[:-1]
        sql += ")"
        # execute
        retS, retV = self.execute(sql)
        if not retS:
            raise RuntimeError("failed to create %s" % self.tablename)
        # confirm
        if not self.checkTable():
            raise RuntimeError("failed to confirm %s" % self.tablename)


# convert Panda jobs to DB representation
def convertPtoD(pandaJobList, pandaIDstatus, localJob=None, fileInfo={}, pandaJobForSiteID=None):
    statusOnly = False
    if localJob is not None:
        # update status only
        ddata = localJob
        statusOnly = True
    else:
        # create new spec
        ddata = LocalJobSpec()
    # sort by PandaID
    pandIDs = list(pandaIDstatus)
    pandIDs.sort()
    pStr = ""
    sStr = ""
    ddata.commandToPilot = ""
    for tmpID in pandIDs:
        # PandaID
        pStr += "%s," % tmpID
        # status
        sStr += "%s," % pandaIDstatus[tmpID][0]
        # commandToPilot
        if pandaIDstatus[tmpID][1] == "tobekilled":
            ddata.commandToPilot = "tobekilled"
    pStr = pStr[:-1]
    sStr = sStr[:-1]
    # job status
    ddata.jobStatus = sStr
    # PandaID
    ddata.PandaID = pStr
    # get panda Job
    pandaJob = None
    if pandaJobList != []:
        # look for buildJob since it doesn't have the first PandaID when retried
        for pandaJob in pandaJobList:
            if pandaJob.prodSourceLabel == "panda":
                break
    elif pandaJobForSiteID is not None:
        pandaJob = pandaJobForSiteID
    # extract libDS
    if pandaJob is not None:
        if pandaJob.prodSourceLabel == "panda":
            # build Jobs
            ddata.buildStatus = pandaJob.jobStatus
            for tmpFile in pandaJob.Files:
                if tmpFile.type == "output":
                    ddata.libDS = tmpFile.dataset
                    break
        else:
            # noBuild or libDS
            ddata.buildStatus = ""
            for tmpFile in pandaJob.Files:
                if tmpFile.type == "input" and tmpFile.lfn.endswith(".lib.tgz"):
                    ddata.libDS = tmpFile.dataset
                    break
        # release
        ddata.releaseVar = pandaJob.AtlasRelease
        # cache
        tmpCache = re.sub("^[^-]+-*", "", pandaJob.homepackage)
        tmpCache = re.sub("_", "-", tmpCache)
        ddata.cacheVar = tmpCache
    # return if update status only
    if statusOnly:
        # build job
        if ddata.buildStatus != "":
            ddata.buildStatus = sStr.split(",")[0]
        # set computingSite mainly for rebrokerage
        if pandaJobForSiteID is not None:
            ddata.site = pandaJobForSiteID.computingSite
            ddata.nRebro = pandaJobForSiteID.specialHandling.split(",").count("rebro") + pandaJobForSiteID.specialHandling.split(",").count("sretry")
        # return
        return ddata
    # job parameters
    ddata.jobParams = pandaJob.metadata
    # extract datasets
    iDSlist = []
    oDSlist = []
    if fileInfo != {}:
        if "inDS" in fileInfo:
            iDSlist = fileInfo["inDS"]
        if "outDS" in fileInfo:
            oDSlist = fileInfo["outDS"]
    else:
        for pandaJob in pandaJobList:
            for tmpFile in pandaJob.Files:
                if tmpFile.type == "input" and not tmpFile.lfn.endswith(".lib.tgz"):
                    if tmpFile.dataset not in iDSlist:
                        iDSlist.append(tmpFile.dataset)
                elif tmpFile.type == "output" and not tmpFile.lfn.endswith(".lib.tgz"):
                    if tmpFile.dataset not in oDSlist:
                        oDSlist.append(tmpFile.dataset)
    # convert to string
    ddata.inDS = ""
    for iDS in iDSlist:
        ddata.inDS += "%s," % iDS
    ddata.inDS = ddata.inDS[:-1]
    ddata.outDS = ""
    for oDS in oDSlist:
        ddata.outDS += "%s," % oDS
    ddata.outDS = ddata.outDS[:-1]
    # job name
    ddata.jobName = pandaJob.jobName
    # creation time
    ddata.creationTime = pandaJob.creationTime
    # job type
    ddata.jobType = pandaJob.prodSeriesLabel
    # site
    ddata.site = pandaJob.computingSite
    # cloud
    ddata.cloud = pandaJob.cloud
    # job ID
    ddata.JobID = pandaJob.jobDefinitionID
    # retry ID
    ddata.retryID = 0
    # provenance ID
    ddata.provenanceID = pandaJob.jobExecutionID
    # groupID
    ddata.groupID = pandaJob.jobsetID
    ddata.retryJobsetID = -1
    if pandaJob.sourceSite not in ["NULL", None, ""]:
        ddata.parentJobsetID = long(pandaJob.sourceSite)
    else:
        ddata.parentJobsetID = -1
    # job type
    ddata.jobType = pandaJob.processingType
    # the number of rebrokerage actions
    ddata.nRebro = pandaJob.specialHandling.split(",").count("rebro")
    # jediTaskID
    ddata.jediTaskID = -1
    # return
    return ddata


# convert JediTask to DB representation
def convertJTtoD(jediTaskDict, localJob=None):
    statusOnly = False
    if localJob is not None:
        # update status only
        ddata = localJob
        statusOnly = True
    else:
        # create new spec
        ddata = LocalJobSpec()
    # max IDs
    maxIDs = 20
    # task status
    ddata.taskStatus = jediTaskDict["status"]
    # statistic
    ddata.jobStatus = jediTaskDict["statistics"]
    # PandaID
    ddata.PandaID = ""
    for tmpPandaID in jediTaskDict["PandaID"][:maxIDs]:
        ddata.PandaID += "%s," % tmpPandaID
    ddata.PandaID = ddata.PandaID[:-1]
    if len(jediTaskDict["PandaID"]) > maxIDs:
        ddata.PandaID += ",+%sIDs" % (len(jediTaskDict["PandaID"]) - maxIDs)
    # merge status
    if "mergeStatus" not in jediTaskDict or jediTaskDict["mergeStatus"] is None:
        ddata.mergeJobStatus = "NA"
    else:
        ddata.mergeJobStatus = jediTaskDict["mergeStatus"]
    # merge PandaID
    ddata.mergeJobID = ""
    for tmpPandaID in jediTaskDict["mergePandaID"][:maxIDs]:
        ddata.mergeJobID += "%s," % tmpPandaID
    ddata.mergeJobID = ddata.mergeJobID[:-1]
    if len(jediTaskDict["mergePandaID"]) > maxIDs:
        ddata.mergeJobID += ",+%sIDs" % (len(jediTaskDict["mergePandaID"]) - maxIDs)
    # return if update status only
    if statusOnly:
        return ddata
    # release
    ddata.releaseVar = jediTaskDict["transUses"]
    # cache
    if jediTaskDict["transHome"] is None:
        tmpCache = ""
    else:
        tmpCache = re.sub("^[^-]+-*", "", jediTaskDict["transHome"])
        tmpCache = re.sub("_", "-", tmpCache)
    ddata.cacheVar = tmpCache
    # job parameters
    try:
        if isinstance(jediTaskDict["cliParams"], unicode):
            ddata.jobParams = jediTaskDict["cliParams"].encode("utf_8")
        else:
            ddata.jobParams = jediTaskDict["cliParams"]
        # truncate
        ddata.jobParams = ddata.jobParams[:1024]
    except Exception:
        pass
    # input datasets
    try:
        # max number of datasets to show
        maxDS = 20
        inDSs = jediTaskDict["inDS"].split(",")
        strInDS = ""
        # concatenate
        for tmpInDS in inDSs[:maxDS]:
            strInDS += "%s," % tmpInDS
        strInDS = strInDS[:-1]
        # truncate
        if len(inDSs) > maxDS:
            strInDS += ",+{0}DSs".format(len(inDSs) - maxDS)
        ddata.inDS = strInDS
    except Exception:
        ddata.inDS = jediTaskDict["inDS"]
    # output datasets
    ddata.outDS = jediTaskDict["outDS"]
    # job name
    ddata.jobName = jediTaskDict["taskName"]
    # creation time
    ddata.creationTime = jediTaskDict["creationDate"]
    # job type
    ddata.jobType = jediTaskDict["processingType"]
    # site
    ddata.site = jediTaskDict["site"]
    # cloud
    ddata.cloud = jediTaskDict["cloud"]
    # job ID
    ddata.JobID = jediTaskDict["reqID"]
    # retry ID
    ddata.retryID = 0
    # provenance ID
    ddata.provenanceID = 0
    # groupID
    ddata.groupID = jediTaskDict["reqID"]
    # jediTaskID
    ddata.jediTaskID = jediTaskDict["jediTaskID"]
    # IDs for retry
    ddata.retryJobsetID = -1
    ddata.parentJobsetID = -1
    # the number of rebrokerage actions
    ddata.nRebro = 0
    # return
    return ddata


# instantiate database proxy
pdbProxy = PdbProxy()


# just initialize DB
def initialzieDB(verbose=False, restoreDB=False):
    if restoreDB:
        pdbProxy.deleteDatabase()
    pdbProxy.initialize()
    pdbProxy.setVerbose(verbose)


# insert job info to DB
def insertJobDB(job, verbose=False):
    tmpLog = PLogger.getPandaLogger()
    # set update time
    job.lastUpdate = datetime.datetime.utcnow()
    # make sql
    sql1 = "INSERT INTO %s (%s) " % (pdbProxy.tablename, LocalJobSpec.columnNames())
    sql1 += "VALUES " + job.values()
    status, out = pdbProxy.execute_direct(sql1)
    if not status:
        raise RuntimeError("failed to insert job")


# update job info in DB
def updateJobDB(job, verbose=False, updateTime=None):
    # make sql
    sql1 = "UPDATE %s SET " % pdbProxy.tablename
    sql1 += job.values(forUpdate=True)
    sql1 += " WHERE JobID=%s " % job.JobID
    # set update time
    if updateTime is not None:
        job.lastUpdate = updateTime
        sql1 += " AND lastUpdate<'%s' " % updateTime.strftime("%Y-%m-%d %H:%M:%S")
    else:
        job.lastUpdate = datetime.datetime.utcnow()
    status, out = pdbProxy.execute_direct(sql1)
    if not status:
        raise RuntimeError("failed to update job")


# set retryID
def setRetryID(job, verbose=False):
    # make sql
    sql1 = "UPDATE %s SET " % pdbProxy.tablename
    sql1 += "retryID=%s,retryJobsetID=%s " % (job.JobID, job.groupID)
    sql1 += " WHERE JobID=%s AND (nRebro IS NULL OR nRebro=%s)" % (job.provenanceID, job.nRebro)
    status, out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError("failed to set retryID")


# delete old jobs
def deleteOldJobs(days, verbose=False):
    # time limit
    limit = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    # make sql
    sql1 = "DELETE FROM %s " % pdbProxy.tablename
    sql1 += " WHERE creationTime<'%s' " % limit.strftime("%Y-%m-%d %H:%M:%S")
    status, out = pdbProxy.execute_direct(sql1)
    if not status:
        raise RuntimeError("failed to delete old jobs")


# read job info from DB
def readJobDB(JobID, verbose=False):
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(), pdbProxy.tablename)
    sql1 += "WHERE JobID=%s" % JobID
    # execute
    status, out = pdbProxy.execute_direct(sql1, fetch=True)
    if not status:
        raise RuntimeError("failed to get JobID=%s" % JobID)
    if len(out) == 0:
        return None
    # instantiate LocalJobSpec
    for values in out:
        job = LocalJobSpec()
        job.pack(values)
        # return frozen job if exists
        if job.dbStatus == "frozen":
            return job
    # return any
    return job


# read jobset info from DB
def readJobsetDB(JobsetID, verbose=False):
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(), pdbProxy.tablename)
    sql1 += "WHERE groupID=%s" % JobsetID
    # execute
    status, out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError("failed to get JobsetID=%s" % JobsetID)
    if len(out) == 0:
        return None
    # instantiate LocalJobSpec
    tmpJobMap = {}
    for tmpStr in out:
        values = tmpStr.split("|")
        job = LocalJobSpec()
        job.pack(values)
        # return frozen job if exists
        if job.dbStatus == "frozen" or job.JobID not in tmpJobMap:
            tmpJobMap[job.JobID] = job
    # make jobset
    jobset = LocalJobsetSpec()
    # set jobs
    jobset.setJobs(tmpJobMap.values())
    # return any
    return jobset


# check jobset status in DB
def checkJobsetStatus(JobsetID, verbose=False):
    # logger
    tmpLog = PLogger.getPandaLogger()
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(), pdbProxy.tablename)
    sql1 += "WHERE groupID=%s" % JobsetID
    failedRet = False, None
    # execute
    status, out = pdbProxy.execute(sql1)
    if not status:
        tmpLog.error(out)
        tmpLog.error("failed to access local DB")
        return failedRet
    if len(out) == 0:
        tmpLog.error("failed to get JobsetID=%s from local DB" % JobsetID)
        return None
    # instantiate LocalJobSpec
    jobMap = {}
    for tmpStr in out:
        values = tmpStr.split("|")
        job = LocalJobSpec()
        job.pack(values)
        # use frozen job if exists
        if job.JobID not in jobMap or job.dbStatus == "frozen":
            jobMap[job.JobID] = job
    # check all job status
    for tmpJobID in jobMap:
        tmpJobSpec = jobMap[tmpJobID]
        if tmpJobSpec != "frozen":
            return True, "running"
    # return
    return True, "frozen"


# bulk read job info from DB
def bulkReadJobDB(verbose=False):
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(), pdbProxy.tablename)
    # execute
    status, out = pdbProxy.execute_direct(sql1, fetch=True)
    if not status:
        raise RuntimeError("failed to get jobs")
    if len(out) == 0:
        return []
    # instantiate LocalJobSpec
    retMap = {}
    jobsetMap = {}
    for values in out:
        job = LocalJobSpec()
        job.pack(values)
        # use frozen job if exists
        if job.JobID not in retMap or job.dbStatus == "frozen":
            if job.groupID in [0, "0", "NULL", -1, "-1"]:
                retMap[long(job.JobID)] = job
            else:
                # add jobset
                tmpJobsetID = long(job.groupID)
                if tmpJobsetID not in retMap or tmpJobsetID not in jobsetMap:
                    jobsetMap[tmpJobsetID] = []
                    jobset = LocalJobsetSpec()
                    retMap[tmpJobsetID] = jobset
                # add job
                jobsetMap[tmpJobsetID].append(job)
    # add jobs to jobset
    for tmpJobsetID in jobsetMap:
        tmpJobList = jobsetMap[tmpJobsetID]
        retMap[tmpJobsetID].setJobs(tmpJobList)
    # sort
    ids = list(retMap)
    ids.sort()
    retVal = []
    for id in ids:
        retVal.append(retMap[id])
    # return
    return retVal


# get list of JobID
def getListOfJobIDs(nonFrozen=False, verbose=False):
    # make sql
    sql1 = "SELECT JobID,dbStatus FROM %s " % pdbProxy.tablename
    # execute
    status, out = pdbProxy.execute_direct(sql1, fetch=True)
    if not status:
        raise RuntimeError("failed to get list of JobIDs")
    allList = []
    frozenList = []
    for item in out:
        # extract JobID
        tmpID = long(item[0])
        # status in DB
        tmpStatus = item[-1]
        # keep all jobs
        if tmpID not in allList:
            allList.append(tmpID)
        # keep frozen jobs
        if nonFrozen and tmpStatus == "frozen":
            if tmpID not in frozenList:
                frozenList.append(tmpID)
    # remove redundant jobs
    retVal = []
    for item in allList:
        if item not in frozenList:
            retVal.append(item)
    # sort
    retVal.sort()
    # return
    return retVal


# get map of jobsetID and JobIDs
def getMapJobsetIDJobIDs(verbose=False):
    # make sql
    sql1 = "SELECT groupID,JobID FROM %s WHERE groupID is not NULL and groupID != 0 and groupID != ''" % pdbProxy.tablename
    # execute
    status, out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError("failed to get list of JobIDs")
    allMap = {}
    for item in out:
        # JobsetID
        tmpJobsetID = long(item.split("|")[0])
        # JobID
        tmpJobID = long(item.split("|")[-1])
        # append
        if tmpJobsetID not in allMap:
            allMap[tmpJobsetID] = []
        if tmpJobID not in allMap[tmpJobsetID]:
            allMap[tmpJobsetID].append(tmpJobID)
    # sort
    for tmpKey in allMap.keys():
        allMap[tmpKey].sort()
    # return
    return allMap


# make JobSetSpec
def makeJobsetSpec(jobList):
    jobset = LocalJobsetSpec()
    jobset.setJobs(jobList)
    return jobset


# get map of jobsetID and jediTaskID
def getJobsetTaskMap(verbose=False):
    # make sql
    sql1 = (
        "SELECT groupID,jediTaskID FROM %s WHERE groupID is not NULL and groupID != 0 and groupID != '' and jediTaskID is not null and jediTaskID != ''"
        % pdbProxy.tablename
    )
    # execute
    status, out = pdbProxy.execute_direct(sql1, fetch=True)
    if not status:
        raise RuntimeError("failed to get list of JobIDs")
    allMap = {}
    for item in out:
        # JobsetID
        tmpJobsetID = long(item[0])
        # JobID
        jediTaskID = long(item[-1])
        # append
        allMap[jediTaskID] = tmpJobsetID
    # return
    return allMap
