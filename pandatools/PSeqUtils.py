import os

import PStep
import PdbUtils


# method objects used in Sequence of psequencer


# factory for a Panda job
class PandaJobFactory:
    # constructor
    def __init__(self,fetFactory,sn,verbose):
        # keep fetcher factory and SN
        self.fetFactory = fetFactory
        self.sn         = sn
        # verbose
        self.verbose    = verbose

    # method emulation    
    def __call__(self,*args):
        # JobID
        jobID = args[0]
        # step name
        stepName = 'PandaJob%s' % jobID
        # instantiate PStep
        pStep = PStep.PStep(stepName,'',self.fetFactory,self.sn,self.verbose)
        # set JobID and Panda mark
        pStep.JobID   = long(jobID)
        pStep.isPanda = True
        # return
        return pStep


# factory for all running Panda job
class RunningPandaFactory:
    # constructor
    def __init__(self,fetFactory,sn,verbose):
        # instantiate factory
        self.pandaFactory = PandaJobFactory(fetFactory,sn,verbose)
        # verbose
        self.verbose    = verbose

    # method emulation    
    def __call__(self,*args):
        # synchronize local database
        os.system('pbook -c "sync()"')
        # get all jobs from DB
        localJobs = PdbUtils.bulkReadJobDB(self.verbose)
        # get running jobs
        rJobs = []
        for tmpJob in localJobs:
            if tmpJob.dbStatus != 'frozen':
                # instantiate PStep
                if hasattr(tmpJob,'JobID'):
                    pStep = self.pandaFactory(tmpJob.JobID)
                else:
                    pStep = self.pandaFactory(tmpJob.JobsetID)
                # append
                rJobs.append(pStep)
        # return
        return rJobs


# factory for all frozen Panda job
class FrozenPandaFactory:
    # constructor
    def __init__(self,fetFactory,sn,verbose):
        # fetcher factory
        self.fetFactory = fetFactory
        # instantiate job factory
        self.pandaFactory = PandaJobFactory(fetFactory,sn,verbose)
        # verbose
        self.verbose    = verbose

    # method emulation    
    def __call__(self,*args):
        # get fetcher
        fetcher = self.fetFactory.getFetcher()
        # get mails
        pmails = fetcher.getPMails(self.verbose)
        # loop over all mails
        rJobs = []
        for pmail in pmails:
            # instantiate PStep
            if pmail.has_key('JobsetID'):
                pStep = self.pandaFactory(pmail['JobsetID'])
            else:
                pStep = self.pandaFactory(pmail['JobID'])                
            # append
            rJobs.append(pStep)
        # return
        return rJobs
