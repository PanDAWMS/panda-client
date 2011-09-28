import re
import os
import sys
import tempfile
import time
import traceback

import SeqConfig
import PdbUtils
import PLogger

seqConf = SeqConfig.getConfig()


class PStep:
    
    # constructor
    def __init__(self,name,command,fetFactory,sn,verbose):
        self.name       = name
        self.command    = command
        self.ocommand   = command
        self.isPanda    = False
        self.JobID      = None
        self.fetFactory = fetFactory
        self.sn         = sn  
        self.verbose    = verbose
        self.cloneSN    = 0
        self.wait       = None
        self.env        = None
        self.status     = 0
        self.output     = ''
        self.tmpLog     = PLogger.getPandaLogger()


    # copy constructor
    def __call__(self):
        # clone serial number
        sn = '%s.%s' % (self.sn,self.cloneSN)
        # increment SN
        self.cloneSN += 1
        # make clone
        return PStep(self.name,self.ocommand,self.fetFactory,sn,self.verbose)
        
        
    # execute command
    def execute(self,wait=False,env={}):
        # set wait and env
        if self.wait == None:
            self.wait = wait
        if self.env == None:    
            self.env = env
        printStr = "=== %s start\n" % self.name
        # append env variables
        if self.env != {}:
            envStr = ''            
            for envKey,envVal in self.env.iteritems():
                envStr += 'export %s="%s"\n' % (envKey,envVal)
            self.command = envStr + self.ocommand
        # crete tmp output
        tmpOFD,tmpOF = tempfile.mkstemp(suffix='.%s' % self.sn)
        # create tmp script
        tmpFD,tmpF = tempfile.mkstemp(suffix='.%s' % self.sn)
        tmpFH = open(tmpF,'w')
        tmpFH.write(self.command)
        # add $? since tee overwrites the original status code
        tmpFH.write('\necho $?')        
        tmpFH.close()
        os.close(tmpFD)
        os.close(tmpOFD)
        os.chmod(tmpF,0755)
        # execute
        printStr += self.command
        printStr += "\n==="
        print printStr
        os.system("%s | tee %s" % (tmpF,tmpOF))
        # read outputs from tmp file
        tmpOFH = open(tmpOF)
        self.output = tmpOFH.read()
        tmpOFH.close()
        # remove tmp files
        os.remove(tmpF)
        os.remove(tmpOF)
        # get status
        self.status = None
        lines = self.output.split('\n')
        for idx,dummy in enumerate(lines):
            try:
                self.status = int(lines[-idx-1])
                break
            except:
                pass
        if self.status == None:
            raise RuntimeError,'faild to get status code for %s' % self.name
        # check status code
        if self.status != 0:
            print 'ERROR: %s failed' % self.name
            return
        # check whether command is pathena/prun
        matchA = re.search('(^|\s|\n|;|/)pathena\s',self.command)
        matchR = re.search('(^|\s|\n|;|/)prun\s',self.command)        
        if matchA != None or matchR != None:
            # parse output to extract JobID 
            for line in self.output.split('\n'):
                match = re.search('^\s*JobsetID\s+:\s+(\d+)',line)
                if match != None:
                    self.isPanda = True
                    self.JobID   = int(match.group(1))
        # wait result for pathena/prun
        if self.isPanda and self.wait:
            self.waitResult()
            

    # get result
    def result(self,blocking=True):
        # tmp class for result
        class tmpResult:
            pass
        res = tmpResult()
        del tmpResult
        # set status and output
        res.status = self.status
        res.output = self.output
        # wait result for pathena/prun
        if self.isPanda:
            pResult = self.waitResult(blocking)
            # return None when result is unavailable in non-blocking mode
            if (not blocking) and pResult == None:
                return None
            # set values of Notification
            for key,val in pResult.iteritems():
                if not hasattr(res,key):
                    setattr(res,key,val)
        # return            
        return res


    # wait result
    def waitResult(self,blocking=True):
        # non pathena jobs
        if not self.isPanda:
            return {}
        # instantiate fetcher
        fetcher = self.fetFactory.getFetcher()
        # wait notification
        self.tmpLog.info('waiting result of JobsetID=%s' % self.JobID) 
        while True:
            sys.stdout.write('.')
            sys.stdout.flush()
            # get mails
            try:
                pmails = fetcher.getPMails(self.verbose)
                for pmail in pmails:
                    # check JobID
                    if pmail.has_key('JobsetID') and pmail['JobsetID'] == self.JobID:
                        # return
                        return pmail
            except:
                type,value,traceBack = sys.exc_info()
                if self.verbose:
                    traceback.print_tb(traceBack)
                print '\nINFO: ignored %s %s' % (type,value)
            # escape if non-blocking mode
            if not blocking:
                return None
            # sleep    
            time.sleep(60*seqConf.mail_interval)


    # retry
    def retry(self):
        # execute again for non-Panda
        if not self.isPanda:
            execute()
            return
        # synchronize local database
        os.system('pbook -c "sync()"')
        # get job
        nTry = 3
        for iTry in range(nTry):
            job = PdbUtils.readJobDB(self.JobID,self.verbose)
            if job == None:
                self.status = 255
                self.output = "ERROR : cannot find JobsetID=%s in local DB" % self.JobID
                return
            # not yet retried
            if job.retryID in [0,'0']:
                break
            # check number of attempts
            if iTry+1 >= nTry:
                self.status = 255
                self.output = "ERROR : already retried %s times" % nTry
                return
            # set JobID to get job from DB
            self.JobID = long(job.retryJobsetID)
        # retry
        os.system('pbook -c "retry(%s)"' % self.JobID)
        # get new job
        job = PdbUtils.readJobDB(self.JobID,self.verbose)
        if job == None:
            self.status = 255
            self.output = "ERROR : cannot find JobsetID=%s in local DB after retry" % self.JobID
            return
        # check new JobID
        if job.retryJobsetID in [0,'0']:
            self.status = 255
            self.output = "ERROR : failed to retry JobsetID=%s" % self.JobID
            return
        # set jobID
        self.JobID = long(job.retryJobsetID)
        # set status and output
        self.status = 0
        self.output = ''
        return
