import re
import os
import gtk
import pango
import gobject
import gtk.glade
import threading

try:
    import Queue
except ImportError:
    import queue as Queue
from . import PLogger
from . import LocalJobSpec
from .MiscUtils import commands_get_output, commands_get_status_output
try:
    long()
except Exception:
    long = int

gobject.threads_init()
gtk.gdk.threads_init()


  
# thread to synchronize database in background
class Synchronizer(threading.Thread):
    
    # constructor
    def __init__(self,syncQueue,pEmitter):
        # init thread
        threading.Thread.__init__(self)
        # queue
        self.syncQueue = syncQueue
        # try to get core
        try:
            self.pbookCore = self.syncQueue.get_nowait()
        except Exception:
            self.pbookCore = None
        # emitter
        self.pEmitter = pEmitter

    # run
    def run(self):
        if self.pbookCore is None:
            return
        # synchronize database
        self.pbookCore.sync()
        # put back queue
        self.syncQueue.put(self.pbookCore)
        # emit signal
        gobject.idle_add(self.pEmitter.emit,"on_syncEnd")



# thread to retry in background
class RetryWorker(threading.Thread):
    
    # constructor
    def __init__(self,jobID,guiGlobal,retryQueue,pEmitter):
        # init thread
        threading.Thread.__init__(self)
        # JobID
        self.jobID = jobID
        # global data
        self.guiGlobal = guiGlobal
        # queue
        self.retryQueue = retryQueue
        # try to get core
        try:
            self.pbookCore = self.retryQueue.get_nowait()
        except Exception:
            self.pbookCore = None
        # emitter
        self.pEmitter = pEmitter

    # run
    def run(self):
        if self.pbookCore is None:
            return
        # retry
        self.pbookCore.retry(long(self.jobID))
        # put back queue
        self.retryQueue.put(self.pbookCore)
        # reset offset
        self.guiGlobal.resetJobOffset()
        # emit signal
        gobject.idle_add(self.pEmitter.emit,"on_syncEnd")




# thread to open url
class UrlOpener(threading.Thread):
    
    # constructor
    def __init__(self,url,queue):
        # init thread
        threading.Thread.__init__(self)
        # url
        self.url = url
        # queue
        self.queue = queue
        # browser type
        self.browser = 'firefox'
        # logger
        self.tmpLog = PLogger.getPandaLogger()


    # run
    def run(self):
        if self.browser == 'firefox':
            # check application
            status,output = commands_get_status_output('which %s' % self.browser)
            if status != 0:
                self.tmpLog.error('%s is unavailable' % self.browser)
                return
            # check version
            output = commands_get_output('%s -v' % self.browser)
            version = output.split()[2]
            version = version[:-1]
            if version < '2.0':
                self.tmpLog.warning("too old %s : version %s It wouldn't work properly" % (self.browser,version))
            # open url
            com = '%s %s' % (self.browser,self.url)
            commands_get_output(com)
        # release queue
        self.queue.put(True)
        return

        

# global data
class PBookGuiGlobal:

    # constructor
    def __init__(self):
        # list of jobs in local DB
        self.jobMap = {}
        # current JobID
        self.currentJob = None
        # current job offset
        self.jobOffset = 0
        # lock
        self.lock = Queue.Queue(1)
        self.lock.put(True)
        

    # set job map
    def setJobMap(self,jobMap):
        lock = self.lock.get()
        self.jobMap = jobMap
        self.lock.put(lock)


    # get job map
    def getJobMap(self):
        return self.jobMap


    # set current job
    def setCurrentJob(self,jobID):
        lock = self.lock.get()        
        if jobID in self.jobMap:
            self.currentJob = jobID
        self.lock.put(lock)
        

    # update job
    def updateJob(self,job):
        lock = self.lock.get()
        if isinstance(job,LocalJobSpec.LocalJobSpec):
            self.jobMap[job.JobID] = job
        else:
            self.jobMap[job.JobsetID] = job            
        self.lock.put(lock)
        

    # get current job
    def getCurrentJob(self):
        return self.currentJob


    # get job
    def getJob(self,jobID):
        return self.jobMap[jobID]


    # reset offset of jobs
    def resetJobOffset(self):
        lock = self.lock.get()
        # reset to 0
        self.jobOffset = 0
        self.lock.put(lock)
        
        
    # set offset of jobs
    def setJobOffset(self,change):
        lock = self.lock.get()        
        # try
        tmpOffset = self.jobOffset + change
        # reset if out of range
        if tmpOffset >= len(self.jobMap):
            tmpOffset = self.jobOffset
        elif tmpOffset < 0:
            tmpOffset = 0
        # set
        self.jobOffset = tmpOffset
        self.lock.put(lock)
        

    # get offset
    def getJobOffset(self):
        return self.jobOffset



# jump to JobID
class PJumper:

    # constructor
    def __init__(self,guiGlobal,pEmitter):
        # jobID to jump to
        self.jobID = None
        # global data
        self.guiGlobal = guiGlobal
        # emitter
        self.pEmitter = pEmitter


    # set jobID
    def setJobID(self,jobID):
        self.jobID = jobID

        
    # action
    def on_clicked(self,tag,textview,event,iter):
        # mouse clicked
        if event.type == gtk.gdk.BUTTON_PRESS:
            if self.jobID is not None:
                # set jobID to global
                self.guiGlobal.setCurrentJob(self.jobID)
                # emit
                self.pEmitter.emit("on_setNewJob")



# text view for summary
class PSumView:
    
    # constructor
    def __init__(self,sumView,guiGlobal,pEmitter):
        # widget
        self.sumView = sumView
        # global data
        self.guiGlobal = guiGlobal
        # emitter
        self.pEmitter = pEmitter
        # jumper
        self.jumper = {'retryID'      : PJumper(self.guiGlobal,self.pEmitter),
                       'provenanceID' : PJumper(self.guiGlobal,self.pEmitter),
                       'retrySetID'   : PJumper(self.guiGlobal,self.pEmitter),
                       'parentSetID'  : PJumper(self.guiGlobal,self.pEmitter),                       
                       }
        self.firstJump = {}
        for tmpJumpName in self.jumper.keys():
            self.firstJump[tmpJumpName] = True
        # sizes
        self.nLines = 20+1
        self.nColumns = 4
        # resize
        self.sumView.resize(self.nLines,self.nColumns)
        # create TextViews
        self.allBufList  = []
        self.textViewMap = {}
        for iLine in range(self.nLines):
            bufList = []
            for item in ('label','value'):
                # text buffer
                textBuf = gtk.TextBuffer()
                # set tag
                tag = textBuf.create_tag('default')
                tag.set_property("font", "monospace")
                tag = textBuf.create_tag('red')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','red2')
                tag = textBuf.create_tag('pink')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','deeppink')
                tag = textBuf.create_tag('green')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','green4')
                tag = textBuf.create_tag('yellow')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','darkgoldenrod')
                tag = textBuf.create_tag('navy')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','navy')
                tag = textBuf.create_tag('skyblue')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','darkturquoise')
                tag = textBuf.create_tag('purple')
                tag.set_property("font", "monospace")
                tag.set_property('foreground','blueviolet')
                # create textview
                textView = gtk.TextView(textBuf)
                self.textViewMap[textBuf] = textView
                # properties
                textView.set_editable(False)
                textView.set_cursor_visible(False)
                # set size and justification
                if item == 'label':
                    textView.set_size_request(120,-1)
                    textView.set_justification(gtk.JUSTIFY_RIGHT)
                else:
                    textView.set_size_request(460,-1)
                    textView.set_justification(gtk.JUSTIFY_LEFT)
                    textView.set_right_margin(20)
                # color
                textView.modify_base(gtk.STATE_NORMAL, gtk.gdk.color_parse("ghostwhite"))                
                # wrap mode
                textView.set_wrap_mode(gtk.WRAP_CHAR)
                # append
                if item == 'label':
                    self.sumView.attach(textView,0,1,iLine,iLine+1)
                else:
                    self.sumView.attach(textView,1,self.nColumns-1,iLine,iLine+1)
                bufList.append(textBuf)
            # append
            self.allBufList.append(bufList)
        # show
        self.sumView.show_all()
        # cursors
        self.cursors = {'normal' : gtk.gdk.Cursor(gtk.gdk.XTERM),
                        'link'   : gtk.gdk.Cursor(gtk.gdk.HAND2)
                        }

        
    # show summary
    def showJobInfo(self,widget):
        # get job
        jobID = self.guiGlobal.getCurrentJob()
        job   = self.guiGlobal.getJob(jobID)
        # make string
        strJob  = "\n"
        strJob += str(job)
        strJob += "\n"       
        # split to lines
        lines = strJob.split('\n')
        # delte
        for tmpBufList in self.allBufList:
            for textbuf in tmpBufList:
                # delete
                textbuf.delete(textbuf.get_start_iter(),
                               textbuf.get_end_iter())
        # fill
        jobStatusRows  = False
        jobStatusLines = ''
        jobStatusIdx   = 0
        for iLine in range(len(lines)):
            # check limit
            if iLine+1 > self.nLines:
                continue
            # decompose line to label and value
            line = lines[iLine]
            match = re.search('^([^:]+:)(.*)',line)
            if match is None:
                items = [line,'']
            else:
                items = match.groups()
            # fill
            if not jobStatusRows:
                iItem = 0
                for strLabel in items:
                    # remove redundant white spaces
                    strLabel = strLabel.strip()
                    strLabel += ' '
                    # get textbuffer
                    textbuf = self.allBufList[iLine][iItem]
                    # delete
                    textbuf.delete(textbuf.get_start_iter(),
                                   textbuf.get_end_iter())
                    # set color
                    tagname = 'default'
                    if (line.strip().startswith('jobStatus') or line.strip().startswith('status')) and iItem != 0:
                        if strLabel.find('frozen') != -1:
                            tagname = 'navy'
                        elif strLabel.find('killing') != -1:
                            tagname = 'pink'                            
                        else:
                            tagname = 'skyblue'
                    # add jumper
                    match = re.search('\s*(\S+)\s*:',line)
                    if match is not None:
                        realLabel = match.group(1)
                        if realLabel in self.jumper and iItem != 0:
                            # set jobID
                            jumper = self.jumper[realLabel]
                            jobID = strLabel.strip()
                            if jobID in ['0','']:
                                jumper.setJobID(None)
                                strLabel = ''
                                # change mouse cursor
                                self.textViewMap[textbuf].get_window(gtk.TEXT_WINDOW_TEXT).set_cursor(self.cursors['normal'])
                            else:
                                jumper.setJobID(jobID)
                                # set tagname
                                tagname = 'hyperlink'
                                # change mouse cursor
                                self.textViewMap[textbuf].get_window(gtk.TEXT_WINDOW_TEXT).set_cursor(self.cursors['link'])
                            # setup textview
                            if self.firstJump[realLabel]:
                                # add connection
                                tag = textbuf.create_tag("hyperlink",foreground='blue',
                                                         underline=pango.UNDERLINE_SINGLE)
                                tag.connect('event',jumper.on_clicked)
                                # disable first flag
                                self.firstJump[realLabel] = False
                    # write
                    textbuf.insert_with_tags_by_name(textbuf.get_end_iter(),
                                                     strLabel,tagname)
                    # increment
                    iItem += 1
                if line.strip().startswith('jobStatus') or line.strip().startswith('status'):
                    jobStatusRows = True
                    jobStatusIdx = iLine +1
                    try:
                        # get textbuffer
                        textbuf = self.allBufList[iLine+1][1]
                        # delete
                        textbuf.delete(textbuf.get_start_iter(),
                                       textbuf.get_end_iter())
                    except Exception:
                        pass
            else:
                # get textbuffer
                textbuf = self.allBufList[jobStatusIdx][1]
                # change color
                tagname = 'default'                
                if line.find('finished') != -1:
                    tagname = 'green'
                elif line.find('failed') != -1:
                    tagname = 'red'
                elif line.find('cancelled') != -1:
                    tagname = 'purple'
                else:
                    tagname = 'yellow'
                # reformat
                items = line.strip().split()
                if len(items) > 1:
                    strLine = '%10s : %s\n' % (items[0],items[-1])
                else:
                    strLine = '\n'
                textbuf.insert_with_tags_by_name(textbuf.get_end_iter(),
                                                 strLine,tagname)
        

# text view for status
class PStatView:

    # constructor
    def __init__(self,statView):
        # text view
        self.statView = statView
        # text buffer
        self.buffer = self.statView.get_buffer()
        # tags
        self.tags = {}
        # info
        tag = self.buffer.create_tag('INFO')
        tag.set_property("font", "monospace")
        #tag.set_property("size-points", 10)
        tag.set_property('foreground','blue')
        self.tags['INFO'] = tag
        # debug
        tag = self.buffer.create_tag('DEBUG')
        tag.set_property("font", "monospace")
        #tag.set_property("size-points", 10)
        tag.set_property('foreground','black')
        self.tags['DEBUG'] = tag
        # warning
        tag = self.buffer.create_tag('WARNING')
        tag.set_property("font", "monospace")
        #tag.set_property("size-points", 10)
        tag.set_property('foreground','orange')
        self.tags['WARNING'] = tag
        # error
        tag = self.buffer.create_tag('ERROR')
        tag.set_property("font", "monospace")
        #tag.set_property("size-points", 10)
        tag.set_property('foreground','red')
        self.tags['ERROR'] = tag
        # format
        self.format = ' %7s : %s\n'
        # dummy handlers
        self.handlers = ['']
        # set color
        self.statView.modify_base(gtk.STATE_NORMAL, gtk.gdk.color_parse("ghostwhite"))        
        # queue for serialization
        self.queue = Queue.Queue(1)
        self.queue.put(True)
        # wrap mode
        self.statView.set_wrap_mode(gtk.WRAP_CHAR)
        

    # formatter
    def formatter(self,level,msg):
        # format
        return self.format % (level,msg)


    # write message with level
    def write(self,level,msg):
        # insert message
        message = self.formatter(level,msg)
        self.buffer.insert_with_tags_by_name(self.buffer.get_end_iter(),
                                             message,level)
        # scroll
        mark = self.buffer.get_mark("insert")
        self.statView.scroll_mark_onscreen(mark)
        gtk.threads_leave()

        
    # emulation for logging
    def info(self,msg,withLock=False):
        sem = self.queue.get()
        if withLock:
            self.write('INFO',msg)
        else:
            gobject.idle_add(self.write,'INFO',msg)            
        self.queue.put(sem)


    # emulation for logging
    def debug(self,msg):
        sem = self.queue.get()        
        gobject.idle_add(self.write,'DEBUG',msg)        
        self.queue.put(sem)
        

    # emulation for logging
    def warning(self,msg):
        sem = self.queue.get()        
        gobject.idle_add(self.write,'WARNING',msg)
        self.queue.put(sem)
        

    # emulation for logging
    def error(self,msg):
        sem = self.queue.get()        
        gobject.idle_add(self.write,'ERROR',msg)        
        self.queue.put(sem)


# tree view for job list
class PTreeView:
    
    # constructor
    def __init__(self,treeView,guiGlobal,pbookCore,pEmitter):
        # tree view
        self.treeView = treeView
        # global data
        self.guiGlobal = guiGlobal
        # core of pbook
        self.pbookCore = pbookCore
        # emitter
        self.pEmitter = pEmitter
        # column names
        self.columnNames =  ['JobID','creationTime']
        # max number of jobs in tree
        self.maxJobs = 20
        # load job list
        self.reloadJobList()
        # set columns
        self.setColumns()
        # connect signals
        self.treeView.get_selection().connect('changed',self.showJob)
        # pixel buffer
        self.pbMap = {}
        icon_map = {'finished' : 'green.png',
                                    'failed'   : 'red.png',
                                    'running'  : 'yellow.png',
                                    'cancelled': 'orange.png',
                                   }
        for tmpStatus in icon_map:
            tmpFname = icon_map[tmpStatus]
            pixbuf = gtk.gdk.pixbuf_new_from_file(os.environ['PANDA_SYS'] \
                                                  + "/etc/panda/icons/" + tmpFname)
            self.pbMap[tmpStatus] = pixbuf
        # set color
        self.treeView.modify_base(gtk.STATE_NORMAL, gtk.gdk.color_parse("ghostwhite"))         

        
    # reload job list
    def reloadJobList(self,slot_obj=None,refresh=True):
        # read Jobs from DB
        if refresh:
            tmpList = self.pbookCore.getLocalJobList()
            # convert to map
            jobList = {}
            for tmpJob in tmpList:
                if isinstance(tmpJob,LocalJobSpec.LocalJobSpec):
                    jobList[tmpJob.JobID] = tmpJob
                else:
                    jobList[tmpJob.JobsetID] = tmpJob                    
            # set global data
            self.guiGlobal.setJobMap(jobList)
        else:
            jobList = self.guiGlobal.getJobMap()
        # make list sore
        listModel = gtk.ListStore(object)
        # sort jobs
        tmpJobIntoStrMap = {}
        for tmpJobIDStr in jobList.keys():
            tmpJobIntoStrMap[long(tmpJobIDStr)] = tmpJobIDStr
        tmpJobIDs = list(tmpJobIntoStrMap)
        tmpJobIDs.sort()
        tmpJobIDs.reverse()
        # get offset
        offset = self.guiGlobal.getJobOffset()
        # truncate
        jobIDs = []
        for tmpJobID in tmpJobIDs[offset:offset+self.maxJobs]:
            jobIDs.append(tmpJobIntoStrMap[tmpJobID])
        # append
        for jobID in jobIDs:
            listModel.append([jobID])
        # set model
        self.treeView.set_model(listModel)


    # set columns
    def setColumns(self):
        # JobID has icon+text
        cellpb = gtk.CellRendererPixbuf()
        tvcolumn = gtk.TreeViewColumn("  %s" % self.columnNames[0], cellpb)
        tvcolumn.set_cell_data_func(cellpb, self.setIcon)
        cell = gtk.CellRendererText()
        tvcolumn.pack_start(cell, False)
        tvcolumn.set_cell_data_func(cell, self.setJobID)
        self.treeView.append_column(tvcolumn)
        # text only for other parameters
        for attr in self.columnNames[1:]:
            cell = gtk.CellRendererText()
            tvcolumn = gtk.TreeViewColumn("     %s" % attr, cell)
            tvcolumn.set_cell_data_func(cell,self.setValue,attr)
            self.treeView.append_column(tvcolumn)


    # set ID column
    def setJobID(self,column,cell,model,iter):
        jobID = model.get_value(iter, 0)
        cell.set_property('text', jobID)


    # set icon
    def setIcon(self,column,cell,model,iter):
        jobID = model.get_value(iter, 0)
        job = self.guiGlobal.getJob(jobID)
        if job.dbStatus != 'frozen':
            pb = self.pbMap['running']
        elif job.jobStatus.find('failed') != -1:
            pb = self.pbMap['failed']
        elif job.jobStatus.find('cancelled') != -1:
            pb = self.pbMap['cancelled']
        else:
            pb = self.pbMap['finished']
        cell.set_property('pixbuf', pb)

        
    # set columns
    def setValue(self,column,cell,model,iter,attr):
        jobID = model.get_value(iter, 0)
        job = self.guiGlobal.getJob(jobID)
        var = getattr(job,attr)
        cell.set_property('text', "   %s" % var)  

        
    # show job info
    def showJob(self,selection):
        # get job info
        model,selected = selection.get_selected_rows()
        if len(selected) == 0:
            return
        iter = model.get_iter(selected[0])
        jobID = model.get_value(iter,0)
        self.guiGlobal.setCurrentJob(jobID)
        # emit signal
        self.pEmitter.emit("on_setNewJob")


# sync button
class PSyncButton:

    # constructor
    def __init__(self,syncButton,pbookCore,pEmitter):
        # sync button
        self.syncButton = syncButton
        # core
        self.pbookCore = pbookCore
        # queue for synchronizer
        self.syncQueue = Queue.Queue(1)
        self.syncQueue.put(pbookCore)
        # emitter
        self.pEmitter = pEmitter


    # synchronze database
    def on_clicked(self,widget):
        synchronizer = Synchronizer(self.syncQueue,self.pEmitter)
        synchronizer.start()



# range button
class PRangeButton:

    # constructor
    def __init__(self,rangeButton,guiGlobal,change,pEmitter):
        # button
        self.rangeButton = rangeButton
        # global
        self.guiGlobal = guiGlobal
        # changed value
        self.change = change
        # emitter
        self.pEmitter = pEmitter


    # clicked action
    def on_clicked(self,widget):
        # get current offset
        oldOffset = self.guiGlobal.getJobOffset()
        # chage offset
        self.guiGlobal.setJobOffset(self.change)
        # get new offset
        newOffset = self.guiGlobal.getJobOffset()
        # emit signal
        if oldOffset != newOffset:
            self.pEmitter.emit("on_rangeChanged",False)



# wev button
class PWebButton:

    # constructor
    def __init__(self,url):
        # url
        self.url = url
        # queue to avoid duplication
        self.queue = Queue.Queue(1)
        self.queue.put(True)


    # clicked action
    def on_clicked(self,widget):
        try:
            ret = self.queue.get_nowait()
        except Exception:
            return
        # make opener
        thr = UrlOpener(self.url,self.queue)
        thr.start()



# config button
class PConfigButton:

    # constructor
    def __init__(self,gladefile):
        # XML name
        self.gladefile = gladefile

    # clicked action
    def on_clicked(self,widget):
        return


# update button
class PUpdateButton:

    # constructor
    def __init__(self,pbookCore,guiGlobal,pEmitter):
        # core
        self.pbookCore = pbookCore
        # global
        self.guiGlobal = guiGlobal
        # emitter
        self.pEmitter = pEmitter


    # clicked action
    def on_clicked(self,widget):
        # logger
        tmpLog = PLogger.getPandaLogger()
        # get jobID
        jobID = self.guiGlobal.getCurrentJob()
        if jobID is None:
            tmpLog.warning('No job is selected. Please click a job in the left list first')
            return
        # skip if frozen
        job = self.guiGlobal.getJob(jobID)
        if job.dbStatus == 'frozen':
            tmpLog.info('Update is not required for frozen jobs')
            return
        # get updated info
        updatedJob = self.pbookCore.statusJobJobset(long(jobID))
        # update global data
        self.guiGlobal.updateJob(updatedJob)
        # emit signal
        self.pEmitter.emit("on_setNewJob")
        # update list
        self.pEmitter.emit("on_rangeChanged",False)
        


# kill button
class PKillButton:

    # constructor
    def __init__(self,pbookCore,guiGlobal,pEmitter):
        # core
        self.pbookCore = pbookCore
        # global
        self.guiGlobal = guiGlobal
        # emitter
        self.pEmitter = pEmitter


    # clicked action
    def on_clicked(self,widget):
        # logger
        tmpLog = PLogger.getPandaLogger()
        # get jobID
        jobID = self.guiGlobal.getCurrentJob()
        if jobID is None:
            tmpLog.warning('No job is selected. Please click a job in the left list first')
            return
        # skip if frozen
        job = self.guiGlobal.getJob(jobID)
        if job.dbStatus == 'frozen':
            tmpLog.info('All subJobs already finished/failed')            
            return
        # kill
        self.pbookCore.kill(long(jobID))
        # get job info
        updatedJob = self.pbookCore.getJobJobsetInfo(jobID)
        # update global data
        self.guiGlobal.updateJob(updatedJob)
        # emit signal
        self.pEmitter.emit("on_setNewJob")



# retry button
class PRetryButton:

    # constructor
    def __init__(self,pbookCore,guiGlobal,pEmitter):
        # core
        self.pbookCore = pbookCore
        # global
        self.guiGlobal = guiGlobal
        # emitter
        self.pEmitter = pEmitter
        # queue for retry workers
        self.retryQueue = Queue.Queue(1)
        self.retryQueue.put(pbookCore)


    # clicked action
    def on_clicked(self,widget):
        # logger
        tmpLog = PLogger.getPandaLogger()
        # get jobID
        jobID = self.guiGlobal.getCurrentJob()
        if jobID is None:
            tmpLog.warning('No job is selected. Please click a job in the left list first')
            return
        # skip if frozen
        job = self.guiGlobal.getJob(jobID)
        if job.dbStatus != 'frozen':
            tmpLog.warning('Cannot retry running jobs')
            return
        # retry
        thr = RetryWorker(jobID,self.guiGlobal,self.retryQueue,self.pEmitter)
        thr.start()



# emitter
class PEmitter(gobject.GObject):
    __gsignals__ = {
        'on_syncEnd'      : (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, ()),
        'on_setNewJob'    : (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, ()),
        'on_rangeChanged' : (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, (bool,)),
        'on_triggerSync'  : (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, ()),        
        }
    # constructor
    def __init__(self):
        gobject.GObject.__init__(self)

# register emitter
gobject.type_register(PEmitter)



# wrapper for gtk.main_quit
def overallQuit(*arg):
    gtk.main_quit()
    commands_get_output('kill -- -%s' % os.getpgrp())



# main
class PBookGuiMain:
    
    # constructor
    def __init__(self,pbookCore):

        # load glade file
        gladefile = os.environ['PANDA_SYS'] + "/etc/panda/pbook.glade"  
        mainWindow = gtk.glade.XML(gladefile,"mainWindow")
        # global data
        pbookGuiGlobal = PBookGuiGlobal()
        # emitter
        self.pEmitter = PEmitter()
        # instantiate components
        statView = PStatView(mainWindow.get_widget("statusTextView"))
        sumView  = PSumView(mainWindow.get_widget("summaryTable"),
                            pbookGuiGlobal,self.pEmitter)
        treeView = PTreeView(mainWindow.get_widget("mainTreeView"),
                             pbookGuiGlobal,pbookCore,self.pEmitter)
        syncButton     = PSyncButton(mainWindow.get_widget("syncButton"),
                                     pbookCore,self.pEmitter)
        backButton     = PRangeButton(mainWindow.get_widget("backButton"),
                                      pbookGuiGlobal,-treeView.maxJobs,self.pEmitter)
        forwardButton  = PRangeButton(mainWindow.get_widget("forwardButton"),
                                      pbookGuiGlobal,treeView.maxJobs,self.pEmitter)
        configButton   = PConfigButton(gladefile)
        pandaMonButton = PWebButton('http://panda.cern.ch')
        savannahButton = PWebButton('https://savannah.cern.ch/projects/panda/')
        updateButton   = PUpdateButton(pbookCore,pbookGuiGlobal,self.pEmitter)
        killButton     = PKillButton(pbookCore,pbookGuiGlobal,self.pEmitter)
        retryButton    = PRetryButton(pbookCore,pbookGuiGlobal,self.pEmitter)                
        # set icons
        iconMap = {
            'retryButton'    : 'retry.png',
            'updateButton'   : 'update.png',
            'killButton'     : 'kill.png',
            'pandaMonButton' : 'pandamon.png',
            'savannahButton' : 'savannah.png',
            'configButton'   : 'config.png',
            'backButton'     : 'back.png',
            'syncButton'  : 'sync.png',
            'forwardButton'  : 'forward.png',
            }
        for buttonName in iconMap:
            iconName = iconMap[buttonName]
            image = gtk.Image()
            image.set_from_file(os.environ['PANDA_SYS'] + "/etc/panda/icons/"
                                + iconName)
            image.show()
            button = mainWindow.get_widget(buttonName)
            button.set_icon_widget(image)
        # set gtk_main_quit handler
        signal_dic = { "gtk_main_quit"             : overallQuit,
                       "on_syncButton_clicked"     : syncButton.on_clicked,
                       "on_forwardButton_clicked"  : forwardButton.on_clicked,
                       "on_backButton_clicked"     : backButton.on_clicked,
                       "on_configButton_clicked"   : configButton.on_clicked,
                       "on_pandaMonButton_clicked" : pandaMonButton.on_clicked,
                       "on_savannahButton_clicked" : savannahButton.on_clicked,
                       "on_updateButton_clicked"   : updateButton.on_clicked,
                       "on_killButton_clicked"     : killButton.on_clicked,
                       "on_retryButton_clicked"    : retryButton.on_clicked,                       
                       }
        mainWindow.signal_autoconnect(signal_dic)
        # set logger
        PLogger.setLogger(statView)
        # connect signales
        self.pEmitter.connect('on_triggerSync',  syncButton.on_clicked)
        self.pEmitter.connect('on_syncEnd',      treeView.reloadJobList)
        self.pEmitter.connect('on_rangeChanged', treeView.reloadJobList)
        self.pEmitter.connect('on_setNewJob',    sumView.showJobInfo)
        # set timer for initial sync
        gtk.timeout_add(1000, self.runSynchronizer)

        
    # run synchronizer in background
    def runSynchronizer(self):
        self.pEmitter.emit("on_triggerSync")
        return False
