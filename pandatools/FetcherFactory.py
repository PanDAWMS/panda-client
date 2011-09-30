import datetime
import threading

import PopFetcher
import ImapFetcher

import SeqConfig

seqConf = SeqConfig.getConfig()


# wrapper to avoid redundant access to mail server
class WrappedFetcher:

    def __init__(self,fetcher,lock):
        self.fetcher  = fetcher
        self.lock     = lock
        self.prevTime = None
        self.prevRet  = []

    # wrapped method to get mails
    def getPMails(self,verbose=False):
        # lock
        self.lock.acquire()
        # call method if interval is larger than delta
        if self.prevTime != None:
            delta    = datetime.datetime.now()-self.prevTime
        else:
            delta = None
        interval = datetime.timedelta(minutes=seqConf.mail_interval)
        if self.prevTime == None or delta > interval:
            # update
            self.prevTime = datetime.datetime.now()
            self.prevRet += self.fetcher.getPMails(verbose)
        # unlock    
        self.lock.release()
        # return
        return self.prevRet

    # remove old info
    def removeOldPMail(self,jobsetID):
        for pmail in tuple(self.prevRet):
            if pmail.has_key('JobsetID') and pmail['JobsetID'] == jobsetID:
                self.prevRet.remove(pmail)
        

# factory class for fetchers
class FetcherFactory:

    def __init__(self):
        self.lock = threading.Lock()
        self.fetcher = None
        self.initScanDepth = None

    # set parameters
    def setParams(self,initScanDepth=None):
        if initScanDepth != None:
            self.initScanDepth = initScanDepth
                         
    # getter for fetcher    
    def getFetcher(self):
        # lock
        self.lock.acquire()
        # return existing fetcher
        if self.fetcher != None:
            # unlock
            self.lock.release()
            # return
            return self.fetcher
        # instantiate new fetcher    
        if seqConf.mail_protocol == 'pop3':
            fetcher = PopFetcher.PopFetcher()
        elif seqConf.mail_protocol == 'imap':
            if self.initScanDepth == None:
                fetcher = ImapFetcher.ImapFetcher()
            else:
                fetcher = ImapFetcher.ImapFetcher(firstTimeScanDepth=self.initScanDepth)                
        else:
            raise RuntimeError,'unsupported mail protocol %s' % seqConf.mail_protocol
        self.fetcher = WrappedFetcher(fetcher,self.lock)
        # unlock
        self.lock.release()
        # return
        return self.fetcher
    

# singleton
fetcherFactory = FetcherFactory()
del FetcherFactory

