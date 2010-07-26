import os
import re
import sys
import types
import time
import datetime
import commands

import PLogger
from LocalJobSpec import LocalJobSpec

class PdbProxy:

    # constructor
    def __init__(self,verbose=False):
        # database engine
        self.engine = 'sqlite3'
        # version of database schema
        self.version = '0_0_1'
        # database file name
        self.filename = 'pandajob.db'
        # database dir
        self.database_dir = os.path.expanduser(os.environ['PANDA_CONFIG_ROOT'])
        # full apth of database file
        self.database = '%s/%s' % (self.database_dir,self.filename)
        # table name
        self.tablename = 'jobtable_%s' % self.version
        # verbose
        self.verbose = verbose
                              

    # set verbose
    def setVerbose(self,verbose):
        # verbose
        self.verbose = verbose
                        
        
    # execute SQL
    def execute(self,sql,var={}):
        # logger  
        tmpLog = PLogger.getPandaLogger()
        # expanda variables
        for tmpKey,tmpVal in var.iteritems():
            sql = sql.replqce(tmpKey,str(tmpVal))
        # construct command
        com = '%s %s "%s"' % (self.engine,self.database,sql)
        if self.verbose:
            tmpLog.debug("DB Req : " + com)
        # execute
        nTry = 5
        for iTry in range(nTry):
            if self.verbose:
                tmpLog.debug("   Try : %s/%s" % (iTry,nTry))
            status,output = commands.getstatusoutput(com)
            status %= 255
            if status == 0:
                break
            if iTry+1 < nTry:
                time.sleep(2)
        # return
        if status != 0:
            tmpLog.error(status)
            tmpLog.error(output)
            return False,output
        else:
            if self.verbose:
                tmpLog.debug("   Ret : " + output)
            outList = output.split('\n')
            # remove ''
            try:
                outList.remove('')
            except:
                pass
            # remove junk messages
            ngStrings = ['Loading resources from']
            for tmpStr in tuple(outList):
                # look for NG strings
                flagNG = False
                for ngStr in ngStrings:
                    match = re.search(ngStr,tmpStr,re.I)
                    if match != None:
                        flagNG = True
                        break
                # remove
                if flagNG:
                    try:
                        outList.remove(tmpStr)
                    except:
                        pass
            return True,outList


    # initialize database
    def initialize(self):
        # import sqlite3
        # check if sqlite3 is available
        com = 'which %s' % self.engine
        status,output = commands.getstatusoutput(com)
        if status != 0:
            errstr  = "\n\n"
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
            raise RuntimeError,errstr
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
        retS,retV = self.execute('.table')
        if not retS:
            raise RuntimeError,"cannot get tables"
        # the table already exist or not
        if retV == []:
            return False
        if not self.tablename in retV[-1].split():
            return False
        # check schema
        self.checkSchema()
        return True

            
    # check schema
    def checkSchema(self,noAdd=False):
        # get colum names
        retS,retV = self.execute('PRAGMA table_info(%s)' % self.tablename)
        if not retS:
            raise RuntimeError,"cannot get table_info"
        # parse
        columns = []
        for line in retV:
            items = line.split('|')
            if len(items) > 1:
                columns.append(items[1])
        # check        
        for tmpC,tmpA in LocalJobSpec.appended.iteritems():
            if not tmpC in columns:
                if noAdd:
                    raise RuntimeError,"%s not found in database schema" % tmpC
                # add column
                retS,retV = self.execute("ALTER TABLE %s ADD COLUMN '%s' %s" % \
                                         (self.tablename,tmpC,tmpA))
                if not retS:
                    raise RuntimeError,"cannot add %s to database schema" % tmpC
        if noAdd:
            return
        # check whole schema just in case
        self.checkSchema(noAdd=True)
        


    # create table
    def createTable(self):
        # ver 0_1_1
        sql  = "CREATE TABLE %s (" % self.tablename
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
        for tmpC,tmpA in LocalJobSpec.appended.iteritems():
            sql += "'%s' %s," % (tmpC,tmpA)
        sql = sql[:-1]
        sql += ")"
        # execute
        retS,retV = self.execute(sql)
        if not retS:
            raise RuntimeError,"failed to create %s" % self.tablename
        # confirm
        if not self.checkTable():
            raise RuntimeError,"failed to confirm %s" % self.tablename


# convert Panda jobs to DB representation
def convertPtoD(pandaJobList,pandaIDstatus,localJob=None,fileInfo={},pandaJobForSiteID=None):
    statusOnly = False
    if localJob != None:
        # update status only 
        ddata = localJob
        statusOnly = True
    else:
        # create new spec
        ddata = LocalJobSpec()
    # sort by PandaID
    pandIDs = pandaIDstatus.keys()
    pandIDs.sort()
    pStr = ''
    sStr = ''
    ddata.commandToPilot = ''
    for tmpID in pandIDs:
        # PandaID
        pStr += '%s,' % tmpID
        # status
        sStr += '%s,' % pandaIDstatus[tmpID][0]
        # commandToPilot
        if pandaIDstatus[tmpID][1] == 'tobekilled':
            ddata.commandToPilot = 'tobekilled'
    pStr = pStr[:-1]
    sStr = sStr[:-1]
    # job status
    ddata.jobStatus = sStr
    # PandaID
    ddata.PandaID = pStr
    # get panda Job
    pandaJob = None
    if pandaJobList != []:
        pandaJob = pandaJobList[0]
    elif pandaJobForSiteID != None:
        pandaJob = pandaJobForSiteID
    # extract libDS
    if pandaJob != None:
        if pandaJob.prodSourceLabel == 'panda':
            # build Jobs
            ddata.buildStatus = pandaJob.jobStatus
            for tmpFile in pandaJob.Files:
                if tmpFile.type == 'output':
                    ddata.libDS = tmpFile.dataset
                    break
        else:
            # noBuild or libDS
            ddata.buildStatus = ''
            for tmpFile in pandaJob.Files:
                if tmpFile.type == 'input' and tmpFile.lfn.endswith('.lib.tgz'):
                    ddata.libDS = tmpFile.dataset
                    break
        # release
        ddata.releaseVar = pandaJob.AtlasRelease
        # cache
        tmpCache = re.sub('^[^-]+-*','',pandaJob.homepackage)
        tmpCache = re.sub('_','-',tmpCache)
        ddata.cacheVar = tmpCache
    # return if update status only
    if statusOnly:
        # build job
        if ddata.buildStatus != '':
            ddata.buildStatus = sStr.split(',')[0]
        # set computingSite mainly for rebrokerage
        if pandaJobForSiteID != None:
            ddata.site = pandaJobForSiteID.computingSite
        # return    
        return ddata
    # job parameters
    ddata.jobParams = pandaJob.metadata
    # extract datasets
    iDSlist  = []
    oDSlist = []
    if fileInfo != {}:
        if fileInfo.has_key('inDS'):
            iDSlist = fileInfo['inDS']
        if fileInfo.has_key('outDS'):
            oDSlist = fileInfo['outDS']
    else:
        for pandaJob in pandaJobList:
            for tmpFile in pandaJob.Files:
                if tmpFile.type == 'input' and not tmpFile.lfn.endswith('.lib.tgz'):
                    if not tmpFile.dataset in iDSlist:
                        iDSlist.append(tmpFile.dataset)
                elif tmpFile.type == 'output' and not tmpFile.lfn.endswith('.lib.tgz'):
                    if not tmpFile.dataset in oDSlist:
                        oDSlist.append(tmpFile.dataset)
    # convert to string
    ddata.inDS = ''
    for iDS in iDSlist:
        ddata.inDS += '%s,' % iDS
    ddata.inDS = ddata.inDS[:-1]
    ddata.outDS = ''
    for oDS in oDSlist:
        ddata.outDS += '%s,' % oDS
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
    ddata.groupID = 0
    # job type
    ddata.jobType = pandaJob.processingType
    # return
    return ddata



# instantiate database proxy
pdbProxy = PdbProxy()


# just initialize DB
def initialzieDB(verbose=False):
    pdbProxy.initialize()
    pdbProxy.setVerbose(verbose)
    

# insert job info to DB
def insertJobDB(job,verbose=False):
    # set update time
    job.lastUpdate = datetime.datetime.utcnow()
    # make sql
    sql1 = "INSERT INTO %s (%s) " % (pdbProxy.tablename,LocalJobSpec.columnNames())
    sql1+= "VALUES " + job.values()
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to insert job"


# update job info in DB
def updateJobDB(job,verbose=False):
    # set update time
    job.lastUpdate = datetime.datetime.utcnow()
    # make sql
    sql1  = "UPDATE %s SET " % pdbProxy.tablename
    sql1 += job.values(forUpdate=True)
    sql1 += " WHERE JobID=%s " % job.JobID
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to insert job"


# set retryID
def setRetryID(job,verbose=False):
    # make sql
    sql1  = "UPDATE %s SET " % pdbProxy.tablename
    sql1 += "retryID=%s " % job.JobID
    sql1 += " WHERE JobID=%s " % job.provenanceID
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to set retryID"


# delete old jobs
def deleteOldJobs(days,verbose=False):
    # time limit
    limit = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    # make sql
    sql1  = "DELETE FROM %s " % pdbProxy.tablename
    sql1 += " WHERE creationTime<'%s' " % limit.strftime('%Y-%m-%d %H:%M:%S')
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to delete old jobs"


# read job info from DB
def readJobDB(JobID,verbose=False):
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(),pdbProxy.tablename)
    sql1+= "WHERE JobID=%s" % JobID
    # execute
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to get JobID=%s" % JobID
    if len(out) == 0:
        return None
    # instantiate LocalJobSpec
    for tmpStr in out:
        values = tmpStr.split('|')
        job = LocalJobSpec()
        job.pack(values)
        # return frozen job if exists
        if job.dbStatus == 'frozen':
            return job
    # return any
    return job


# bulk read job info from DB
def bulkReadJobDB(verbose=False):
    # make sql
    sql1 = "SELECT %s FROM %s " % (LocalJobSpec.columnNames(),pdbProxy.tablename)
    # execute
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to get jobs"
    if len(out) == 0:
        return []
    # instantiate LocalJobSpec
    retMap = {}
    for tmpStr in out:
        values = tmpStr.split('|')
        job = LocalJobSpec()
        job.pack(values)
        # use frozen job if exists
        if (not retMap.has_key(job.JobID)) or job.dbStatus == 'frozen':
            retMap[long(job.JobID)] = job
    # sort
    ids = retMap.keys()
    ids.sort()
    retVal = []
    for id in ids:
        retVal.append(retMap[id])
    # return    
    return retVal


# get list of JobID
def getListOfJobIDs(nonFrozen=False,verbose=False):
    # make sql
    sql1 = "SELECT JobID,dbStatus FROM %s " % pdbProxy.tablename
    # execute
    status,out = pdbProxy.execute(sql1)
    if not status:
        raise RuntimeError,"failed to get list of JobIDs"
    allList = []
    frozenList = []
    for item in out:
        # extract JobID
        tmpID = long(item.split('|')[0])
        # status in DB
        tmpStatus = item.split('|')[-1]
        # keep all jobs
        if not tmpID in allList:
            allList.append(tmpID)
        # keep frozen jobs
        if nonFrozen and tmpStatus == 'frozen':
            if not tmpID in frozenList:
                frozenList.append(tmpID)
    # remove redundant jobs
    retVal = []
    for item in allList:
        if not item in frozenList:
            retVal.append(item)
    # sort
    retVal.sort()
    # return 
    return retVal
