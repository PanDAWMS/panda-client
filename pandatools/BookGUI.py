import os
import sys
import gtk
import time
import gobject
import gtk.glade
import threading
import commands
import Queue

gobject.threads_init()
gtk.gdk.threads_init()
  
# thread to synchronize database in background
class Synchronizer(threading.Thread):
    
    # constructor
    def __init__(self,syncQueue):
        # init thread
        threading.Thread.__init__(self)
        # queue
        self.syncQueue = syncQueue
        # try to get core
        try:
            self.pbookCore = self.syncQueue.get_nowait()
        except:
            self.pbookCore = None

    # run
    def run(self):
        if self.pbookCore == None:
            return
        # synchronize database
        self.pbookCore.sync()
        # put back queue
        self.syncQueue.put(self.pbookCore)


# global data
class PBookGuiGlobal:

    # constructor
    def __init__(self):
        self.jobMap = {}
        self.currentJob = None
        self.jobOffset = 0

    # set job map
    def setJobMap(self,jobMap):
        self.jobMap = jobMap

    # get job map
    def getJobMap(self):
        return self.jobMap

    # set current job
    def setCurrentJob(self,jobID):
        if self.jobMap.has_key(jobID):
            self.currentJob = jobID

    # update job
    def updateJob(self,job):
        self.jobMap[job.JobID] = job

    # get current job
    def getCurrentJob(self):
        return self.currentJob

    # get job
    def getJob(self,jobID):
        return self.jobMap[jobID]

    # set offset of jobs
    def setJobOffset(self,change):
        # set
        self.offset += change
        # reset offset
        if self.jobOffset > len(self.jobMap):
            self.jobOffset = len(self.jobMap)
        elif self.jobOffset < 0:
            self.jobOffset = 0

    # get offset
    def getJobOffset(self):
        return self.jobOffset

    

# text view for summary
class PSumView:
    
    # constructor
    def __init__(self,sumView,guiGlobal):
        # text view
        self.sumView = sumView
        # global data
        self.guiGlobal = guiGlobal
        # text buffer
        self.buffer = self.sumView.get_buffer()
        # set tag
        self.tag = self.buffer.create_tag('default')
        self.tag.set_property("font", "monospace")
        # background
        """
        pixbuf = gtk.gdk.pixbuf_new_from_file('/usatlas/u/maeno/tmp/status.PNG')
        pixmap,mask = pixbuf.render_pixmap_and_mask()
        tv_win = self.sumView.get_window(gtk.TEXT_WINDOW_TEXT)
        tv_win.set_back_pixmap(pixmap, gtk.FALSE)
        """

    # show summary
    def showJobInfo(self):
        # get job
        jobID = self.guiGlobal.getCurrentJob()
        job   = self.guiGlobal.getJob(jobID)
        # make string
        strJob  = "\n\n"
        strJob += str(job)
        # flush
        self.buffer.delete(self.buffer.get_start_iter(),
                           self.buffer.get_end_iter())
        self.buffer.insert_with_tags_by_name(self.buffer.get_end_iter(),
                                             strJob,'default')
        
        

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
        tag.set_property("size-points", 10)
        tag.set_property('foreground','blue')
        self.tags['INFO'] = tag
        # debug
        tag = self.buffer.create_tag('DEBUG')
        tag.set_property("font", "monospace")
        tag.set_property("size-points", 10)
        tag.set_property('foreground','black')
        self.tags['DEBUG'] = tag
        # error
        tag = self.buffer.create_tag('ERROR')
        tag.set_property("font", "monospace")
        tag.set_property("size-points", 10)
        tag.set_property('foreground','red')
        self.tags['ERROR'] = tag
        # format
        self.format = ' %6s : %s\n'
        # dummy handlers
        self.handlers = ['']

    # formatter
    def formatter(self,level,msg):
        # format
        return self.format % (level,msg)
    
    # emulation for logging
    def info(self,msg):
        level = 'INFO'
        message = self.formatter(level,msg)
        self.buffer.insert_with_tags_by_name(self.buffer.get_end_iter(),
                                             message,level)

    # emulation for logging
    def debug(self,msg):
        level = 'DEBUG'
        message = self.formatter(level,msg)        
        self.buffer.insert_with_tags_by_name(self.buffer.get_end_iter(),
                                             message,level)

    # emulation for logging
    def error(self,msg):
        level = 'ERROR'
        message = self.formatter(level,msg)        
        self.buffer.insert_with_tags_by_name(self.buffer.get_end_iter(),
                                             message,level)


# tree view for job list
class PTreeView:
    
    # constructor
    def __init__(self,treeView,sumView,guiGlobal,pbookCore):
        # tree view
        self.treeView = treeView
        # global data
        self.guiGlobal = guiGlobal
        # core of pbook
        self.pbookCore = pbookCore
        # summary view
        self.sumView = sumView
        # column names
        self.columnNames =  ['JobID','creationTime']
        # max number of jobs in tree
        self.maxJobs = 25
        # load job list
        self.reloadJobList()
        # set columns
        self.setColumns()
        # connect signals
        self.treeView.get_selection().connect('changed',self.showJob)
        

    # reload job list
    def reloadJobList(self,refresh=True):
        # read Jobs from DB
        if refresh:
            tmpList = self.pbookCore.getLocalJobList()
            # convert to map
            jobList = {}
            for tmpJob in tmpList:
                jobList[tmpJob.JobID] = tmpJob
            # set global data
            self.guiGlobal.setJobMap(jobList)
        else:
            jobList = self.guiGlobal.getJobMap()
        # make list sore
        listModel = gtk.ListStore(object)
        # sort jobs
        jobIDs = jobList.keys()
        jobIDs.sort()
        jobIDs.reverse()
        # get offset
        offset = self.guiGlobal.getJobOffset()
        # truncate
        jobIDs = jobIDs[offset:offset+self.maxJobs]
        # append
        for jobID in jobIDs:
            listModel.append([jobID])
        # set model
        self.treeView.set_model(listModel)

    
    # set columns
    def setColumns(self):
        # JobID has icon+text
        cellpb = gtk.CellRendererPixbuf()
        tvcolumn = gtk.TreeViewColumn(self.columnNames[0], cellpb)
        cell = gtk.CellRendererText()
        tvcolumn.pack_start(cell, False)
        tvcolumn.set_cell_data_func(cell, self.getJobID)
        self.treeView.append_column(tvcolumn)
        # text only for other parameters
        for attr in self.columnNames[1:]:
            cell = gtk.CellRendererText()
            tvcolumn = gtk.TreeViewColumn(attr, cell)
            tvcolumn.set_cell_data_func(cell,self.getValue,attr)
            self.treeView.append_column(tvcolumn)

        
    # set columns
    def getJobID(self,column,cell,model,iter):
        jobID = model.get_value(iter, 0)
        cell.set_property('text', jobID)


    # set columns
    def getValue(self,column,cell,model,iter,attr):
        jobID = model.get_value(iter, 0)
        job = self.guiGlobal.getJob(jobID)
        var = getattr(job,attr)
        cell.set_property('text', var)  

        
    # show job info
    def showJob(self,selection):
        # get job info
        model,selected = selection.get_selected_rows()
        iter = model.get_iter(selected[0])
        jobID = model.get_value(iter,0)
        self.guiGlobal.setCurrentJob(jobID)
        # show job info in summary view
        self.sumView.showJobInfo()


# sync button
class PSyncButton:

    # constructor
    def __init__(self,syncButton,pbookCore):
        # sync button
        self.syncButton = syncButton
        # core
        self.pbookCore = pbookCore
        # queue for synchronizer
        self.syncQueue = Queue.Queue(1)
        self.syncQueue.put(pbookCore)

    # synchronze database
    def on_clicked(self,widget):
        synchronizer = Synchronizer(self.syncQueue)
        synchronizer.start()


# wrapper for gtk.main_quit
def overallQuit(*arg):
    gtk.main_quit()
    commands.getoutput('kill -9 -- -%s' % os.getpgrp())    

# main
class PBookGuiMain:
    
    # constructor
    def __init__(self,pbookCore):

        # load glade file
        gladefile = os.environ['PANDA_SYS'] + "/etc/panda/pbook.glade"  
        self.rootXML = gtk.glade.XML(gladefile)
        # global data
        pbookGuiGlobal = PBookGuiGlobal()
        # instantiate components
        statView = PStatView(self.rootXML.get_widget("statusTextView"))
        sumView  = PSumView(self.rootXML.get_widget("summaryTextView"),
                            pbookGuiGlobal)
        treeView = PTreeView(self.rootXML.get_widget("mainTreeView"),
                             sumView,
                             pbookGuiGlobal,
                             pbookCore)
        syncButton = PSyncButton(self.rootXML.get_widget("syncButton"),
                                 pbookCore)
        
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
        for buttonName,iconName in iconMap.iteritems():
            image = gtk.Image()
            image.set_from_file(os.environ['PANDA_SYS'] + "/etc/panda/icons/" + iconName)
            image.show()
            button = self.rootXML.get_widget(buttonName)
            button.set_icon_widget(image)

        # set gtk_main_quit handler
        signal_dic = { "gtk_main_quit"         : overallQuit,
                       "on_syncButton_clicked" : syncButton.on_clicked}
        self.rootXML.signal_autoconnect(signal_dic)
        
        # set logger
        import PLogger
        PLogger.setLogger(statView)

        # queue for synchronizer
        syncQueue = Queue.Queue(1)
        syncQueue.put(pbookCore)

        # trigger synchronizer
        class TriggerSync(gobject.GObject):
            __gsignals__ = {
                'on_triggerSync': (gobject.SIGNAL_RUN_LAST, gobject.TYPE_NONE, ()),
                }
            
            def __init__(self):
                gobject.GObject.__init__(self)
                
        gobject.type_register(TriggerSync)
        self.triggerSync = TriggerSync()
        self.triggerSync.connect('on_triggerSync', syncButton.on_clicked)
        # set timer
        gtk.timeout_add(1000, self.runSynchronizer)

        
    # run synchronizer in background
    def runSynchronizer(self):
        self.triggerSync.emit("on_triggerSync")
        return False
