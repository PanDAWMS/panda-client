import types
import poplib

import PMailParser
import SeqConfig

seqConf = SeqConfig.getConfig()


class PopFetcher:

    # constructor
    def __init__(self):
        self.uidl = None
        # set UIDL
        if hasattr(seqConf,'pop_uidl'):
            self.uidl = seqConf.pop_uidl
            
            
    # login
    def login(self,verbose=False):
        if verbose:
            print '\nlogin to %s' % seqConf.mail_host
        # instantiate POP instance
        if seqConf.mail_ssl:
            # POP3-over-SSL
            self.pop = poplib.POP3_SSL(seqConf.mail_host,seqConf.mail_port)
        else:
            # POP3
            self.pop = poplib.POP3(seqConf.mail_host,seqConf.mail_port)
        # login    
        out = self.pop.user(seqConf.mail_user)
        if verbose:
            print out
        out = self.pop.pass_(seqConf.mail_pass)
        if verbose:
            print out


    # start session
    def startSession(self,verbose=False):
        try:
            # ping
            out = self.pop.noop()
            if verbose:
                print out
        except:
            try:
                # close just in case
                out = self.pop.quit()
                if verbose:
                    print out
            except:
                pass
            # reconnect
            self.login(verbose)


    # end session
    def endSession(self,verbose=False):
        # close connection
        try:
            if not seqConf.mail_keepalive:
                out = self.pop.quit()
                if verbose:
                    print out
        except:
            pass
        

    # get panda mails
    def getPMails(self,verbose=False):
        # start session
        self.startSession(verbose)
        # get UIDLs
        res = self.pop.uidl()
        if verbose:
            print res
        uidls = []
        # string or list format
        if isinstance(res,types.StringType):
            # check response
            if not res.startswith('+OK'):
                # end session
                self.endSession(verbose)
                raise RuntimeError,"invalid responce from mail server %s " % res
            # extract IDs
            mnum = res.split()[-2]
            uidl = res.split()[-1]
            uidls.append((mnum,uidl))
        else:
            # check response
            if not res[0].startswith('+OK'):
                # end session
                self.endSession(verbose)
                raise RuntimeError,"invalid responce from mail server %s " % str(res)
            # extract IDs
            for str in res[1]:
                mnum = str.split()[0]
                uidl = str.split()[1]
                uidls.append((mnum,uidl))
        # find minimum index of unread mail
        minIdx  = None
        for idx in range(len(uidls)):
            mnum,uidl = uidls[-idx-1]
            if uidl == self.uidl:
                break
            minIdx  = -idx-1
        # check unread mails
        updateUIDL = False
        notList = []
        if minIdx != None:
            for mnum,uidl in uidls[minIdx:]:
                # doesn't fetch mails in the first cycle of the first session
                if self.uidl == None:
                    continue
                # get header
                res = self.pop.top(mnum,0)
                if verbose:
                    print res
                # check response
                if not res[0].startswith('+OK'):
                    # end session
                    self.endSession(verbose)
                    raise RuntimeError,"invalid responce from mail server %s " % str(res)
                # check header
                pandaHead = PMailParser.checkHeader(res)
                if pandaHead:
                    res = self.pop.top(mnum,1000)
                    if verbose:
                        print res
                    # check response
                    if not res[0].startswith('+OK'):
                        # end session
                        self.endSession()
                        raise RuntimeError,"invalid responce from mail server %s " % str(res)
                    # get mail contents
                    pandaNot = PMailParser.parseNotification(res,'pop3')
                    notList.append(pandaNot)
            # set last UIDL
            self.uidl  = uidls[-1][1]
            updateUIDL = True
        elif self.uidl == None:
            # didn't find mails in the first cycle of the first session
            self.uidl  = 'dummy'
            updateUIDL = True
        # update config
        if updateUIDL:
            # set pop_uidl
            global seqConf
            seqConf.pop_uidl = self.uidl
            # update
            SeqConfig.updateConfig(seqConf)
            if verbose:
                print 'set UIDL=%s' % self.uidl
        # end session
        self.endSession(verbose)
        # return
        return notList
