import re
import os
import sys
import tempfile
import time
import traceback

import SeqConfig

seqConf = SeqConfig.getConfig()


class PStep:
    
    # constructor
    def __init__(self,name,command,fetFactory,sn,verbose):
        self.name       = name
        self.command    = command
        self.isPanda    = False
        self.JobID      = None
        self.fetFactory = fetFactory
        self.sn         = sn  
        self.verbose    = verbose
        self.cloneSN    = 0


    # copy constructor
    def __call__(self):
        # clone serial number
        sn = '%s.%s' % (self.sn,self.cloneSN)
        # increment SN
        self.cloneSN += 1
        # make clone
        return PStep(self.name,self.command,self.fetFactory,sn,self.verbose)
        
        
    # execute command
    def execute(self,wait=True,env=''):
        printStr = "=== %s start\n" % self.name
        # append env variables
        self.command = env + '\n' + self.command
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
                match = re.search('^\s*JobID\s+:\s+(\d+)',line)
                if match != None:
                    self.isPanda = True
                    self.JobID   = int(match.group(1))
        # wait result for pathena/prun
        if self.isPanda and wait:
            self.waitResult()
            

    # get result
    def result(self):
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
            pResult = self.waitResult()
            # set values of Notification
            for key,val in pResult.iteritems():
                if not hasattr(res,key):
                    setattr(res,key,val)
        # return            
        return res


    # wait result
    def waitResult(self):
        # non pathena jobs
        if not self.isPanda:
            return {}
        # instantiate fetcher
        fetcher = self.fetFactory.getFetcher()
        # wait notification
        while True:
            sys.stdout.write('.')
            sys.stdout.flush()
            # get mails
            try:
                pmails = fetcher.getPMails(self.verbose)
                for pmail in pmails:
                    # check JobID
                    if pmail.has_key('JobID') and pmail['JobID'] == self.JobID:
                        # return
                        return pmail
            except:
                type,value,traceBack = sys.exc_info()
                if self.verbose:
                    traceback.print_tb(traceBack)
                print '\nINFO: ignored %s %s' % (type,value)
            # sleep    
            time.sleep(60*seqConf.mail_interval)
