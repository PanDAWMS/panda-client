import os
import sys
import gtk
import time
import gtk.glade


# text view for summary
class PSumView:
    
    # constructor
    def __init__(self,sumView):
        # text view
        self.sumView = sumView
        # text buffer
        self.buffer = self.sumView.get_buffer()
        # set tag
        self.tag = self.buffer.create_tag('default')
        self.tag.set_property("font", "monospace")
       

    # show summary
    def showJobInfo(self,job):
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


    # formatter
    def formatter(self,level,msg):
        # format
        #format = time.strftime('%Y-%m-%d %H:%M:%S')+' %6s : %s\n'
        format = ' %6s : %s\n'
        return format % (level,msg)
    

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
    def __init__(self,treeView,sumView,statView,pbookCore):
        # tree view
        self.treeView = treeView
        # summary view
        self.sumView = sumView
        # status view
        self.statView = statView
        # core of pbook
        self.pbookCore = pbookCore
        # offset for JobID list
        self.offset = 0
        # max number of jobs to show
        self.maxJobs = 100
        # column names
        self.columnNames =  ['JobID','creationTime']
        # job list
        self.jobList = {}
        # load job list
        self.reloadJobList()
        # set columns
        self.setColumns()
        # connect signals
        self.treeView.get_selection().connect('changed',self.showJob)
        

    # reload job list
    def reloadJobList(self):
        # read Jobs from DB
        tmpList = self.pbookCore.getLocalJobList()
        # convert to map
        self.jobList = {}
        for tmpJob in tmpList:
            self.jobList[tmpJob.JobID] = tmpJob
        # make list sore
        listModel = gtk.ListStore(object)
        # sort jobs
        jobIDs = self.jobList.keys()
        jobIDs.sort()
        jobIDs.reverse()
        # reset offset
        if self.offset+self.maxJobs > len(jobIDs):
            self.offset = len(jobIDs)-self.maxJobs
        if self.offset < 0:
            self.offset = 0
        # truncate
        jobIDs = jobIDs[self.offset:self.offset+self.maxJobs]
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
        job = self.jobList[jobID]
        var = getattr(job,attr)
        cell.set_property('text', var)  

        
    # show job info
    def showJob(self,selection):
        # get job info
        model,selected = selection.get_selected_rows()
        iter = model.get_iter(selected[0])
        jobID = model.get_value(iter,0)
        job = self.jobList[jobID]
        # show job info in summary view
        self.sumView.showJobInfo(job)



# main
class PBookGuiMain:
    
    # constructor
    def __init__(self,pbookCore):
  
        # load glade file
        gladefile = os.environ['PANDA_SYS'] + "/etc/panda/pbook.glade"  
        self.rootXML = gtk.glade.XML(gladefile)
        # set gtk_main_quit handler
        signal_dic = { "gtk_main_quit" : gtk.main_quit }
        self.rootXML.signal_autoconnect(signal_dic)
        # instantiate components
        statView = PStatView(self.rootXML.get_widget("statusTextView"))
        sumView  = PSumView(self.rootXML.get_widget("summaryTextView"))
        treeView = PTreeView(self.rootXML.get_widget("mainTreeView"),
                             sumView,
                             statView,
                             pbookCore)
        # set logger
        import PLogger
        PLogger.setLogger(statView)
