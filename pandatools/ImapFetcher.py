import re
import types
import imaplib
import datetime

import PMailParser
import SeqConfig

seqConf = SeqConfig.getConfig()


class ImapFetcher:

    # constructor
    def __init__(self,firstTimeScanDepth=3):
        # start time
        self.starttime = datetime.datetime.today()
        # first flag
        self.firstFlag = True
        # time depth for first scan 
        self.firstTimeScanDepth = firstTimeScanDepth
        # uid map 
        self.uidMap = None
        # parse uid
        if hasattr(seqConf,'imap_uid'):
            self.uidMap = {}
            items = seqConf.imap_uid.split(',')
            idx = 0
            while (idx+1) < len(items):
                self.uidMap[items[idx]] = int(items[idx+1])
                idx += 2
            
            
    # login
    def login(self,verbose=False):
        if verbose:
            print '\nlogin to %s' % seqConf.mail_host
        # instantiate IMAP instance
        if seqConf.mail_ssl:
            # IMAP4-over-SSL
            self.imap = imaplib.IMAP4_SSL(seqConf.mail_host,seqConf.mail_port)
        else:
            # IMAP4
            self.imap = imaplib.IMAP4(seqConf.mail_host,seqConf.mail_port)
        # login    
        out = self.imap.login(seqConf.mail_user,seqConf.mail_pass)
        if verbose:
            print out


    # start session
    def startSession(self,verbose=False):
        try:
            # ping
            out = self.imap.noop()
            if verbose:
                print out
        except:
            try:
                # close just in case
                out = self.imap.logout()
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
                out = self.imap.logout()
                if verbose:
                    print out
        except:
            pass
        

    # get panda mails
    def getPMails(self,verbose=False):
        # start session
        self.startSession(verbose)
        # get directories
        global seqConf
        dirs = []
        if hasattr(seqConf,'mail_dirs'):
            # use dirs in config
            dirs = seqConf.mail_dirs.split(',')
        else:
            # get list of dirs from imap server
            status,out = self.imap.list()
            if verbose:
                print status,out
            if status != 'OK':
                # end session
                self.endSession(verbose)
                raise RuntimeError,"invalid responce when list from mail server %s %s" % (status,out)
            for item in out:
                # skip Noselect
                match = re.search('\([^\)]*\\Noselect[^\)]*\)',item)
                if match != None:
                    continue
                # get folder name
                lastStr = item.split()[-1]
                if lastStr.endswith('"'):
                    # extract "folder name"
                    match = re.search('"([^"]+)"$',item)
                    if match != None:
                        dirs.append(match.group(1))
                elif lastStr.endswith("'"):
                    # extract 'folder name'
                    match = re.search("'([^']+)'$",item)
                    if match != None:
                        dirs.append(match.group(1))
                else:
                    dirs.append(lastStr)
        # fetch mails
        highUIDs = {}
        notList  = []
        for dir in dirs:
            # change mailbox 
            if verbose:
                print 'select %s' % dir
            status,out = self.imap.select(dir)
            if verbose:
                print status,out
            if status != 'OK':
                # end session
                self.endSession(verbose)
                raise RuntimeError,"invalid responce when select from mail server %s %s" % (status,out)
            # search
            args = PMailParser.getHeaderArgs()
            args.insert(0,'search')
            if self.firstFlag:
                args.append('SENTSINCE')
                args.append((self.starttime-datetime.timedelta(days=self.firstTimeScanDepth)).strftime("%d-%b-%Y"))                
            if verbose:
                print '%s' % str(args)
            status,out = self.imap.uid(*args)
            if verbose:
                print status,out
            if status != 'OK':
                # end session
                self.endSession(verbose)
                raise RuntimeError,"invalid responce when search from mail server %s %s" % (status,out)
            # get UIDs
            uids = []
            for item in out[0].split():
                try:
                    uids.append(int(item))
                except:
                    pass
            # empty    
            if uids == []:
                continue
            # sort
            uids.sort()
            # keep highest UID
            highUIDs[dir] = uids[-1]
            # check new mail
            if self.uidMap == None or (not self.uidMap.has_key(dir)) or uids[0] > self.uidMap[dir] or self.firstFlag:
                pass
            elif uids[-1] > self.uidMap[dir]:
                for idx,uid in enumerate(uids):
                    if uid > self.uidMap[dir]:
                        uids = uids[idx:]
                        break
            else:
                continue
            # fetch mail body
            for uid in uids:
                if verbose:
                    print '%s %s %s' % ('fetch',uid,"(BODY[TEXT])")
                status,out = self.imap.uid('fetch',uid,"(BODY[TEXT])")
                if verbose:
                    print status,out
                if status != 'OK':
                    # end session
                    self.endSession(verbose)
                    raise RuntimeError,"invalid responce when fetch from mail server %s %s" % (status,out)
                # get mail contents
                res = out[0][-1]
                pandaNot = PMailParser.parseNotification(res,type='imap')
                if verbose:
                    print pandaNot
                notList.append(pandaNot)
        # update uid map
        updatedFlag = False
        if self.uidMap == None:
            # set empty when nothing was found in the first cycle of the first session
            self.uidMap = {}
            updatedFlag = True
        for dir,uid in highUIDs.iteritems():
            if (not self.uidMap.has_key(dir)) or self.uidMap[dir] != uid:
                self.uidMap[dir] = uid
                updatedFlag = True
        if updatedFlag:
            # serialize uid
            strUIDs = ''
            for dir,uid in self.uidMap.iteritems():
                strUIDs += '%s,%s,' % (dir,uid)
            strUIDs = strUIDs[:-1]
            # set uid
            seqConf.imap_uid = strUIDs
            # update config
            SeqConfig.updateConfig(seqConf)
            if verbose:
                print 'set UID=%s' % strUIDs
        # end session
        self.endSession(verbose)
        # end of first loop
        if self.firstFlag:
            self.firstFlag = False
        # return
        return notList
