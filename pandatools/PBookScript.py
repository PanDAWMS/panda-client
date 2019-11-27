"""
Do NOT import this module in your code.
Import PBookCore instead.
"""

import re
import os
import sys
import code
import atexit
import signal
import tempfile

from pandatools.MiscUtils import commands_get_output
try:
    long()
except Exception:
    long = int

try:
    from concurrent.futures import ThreadPoolExecutor
except ImportError:
    def list_parallel_exec(func, array):
        return [ func(x) for x in array ]
else:
    def list_parallel_exec(func, array):
        with ThreadPoolExecutor(8) as thread_pool:
            dataIterator = thread_pool.map(func, array)
        return list(dataIterator)


import optparse
import readline

from pandatools import PLogger
from pandatools import Client
from pandatools import PandaToolsPkgInfo

# readline support
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set show-all-if-ambiguous On')

# history support
pconfDir = os.path.expanduser(os.environ['PANDA_CONFIG_ROOT'])
if not os.path.exists(pconfDir):
    os.makedirs(pconfDir)
historyFile = '%s/.history' % pconfDir
# history file
if os.path.exists(historyFile):
    readline.read_history_file(historyFile)
readline.set_history_length(1024)

# set dummy CMTSITE
if 'CMTSITE' not in os.environ:
    os.environ['CMTSITE'] = ''

# make tmp dir
tmpDir = tempfile.mkdtemp()

# set tmp dir in Client
Client.setGlobalTmpDir(tmpDir)

# fork PID
fork_child_pid = None

# exit action
def _onExit(dirName,hFile):
    # save history only for master process
    if fork_child_pid == 0:
        readline.write_history_file(hFile)
    # remove tmp dir
    commands_get_output('rm -rf %s' % dirName)
atexit.register(_onExit,tmpDir,historyFile)


# look for PandaTools package
for path in sys.path:
    if path == '':
        path = '.'
    if os.path.exists(path) and os.path.isdir(path) and 'pandatools' in os.listdir(path) \
           and os.path.exists('%s/pandatools/__init__.py' % path):
        # make symlink for module name
        os.symlink('%s/pandatools' % path,'%s/taskbuffer' % tmpDir)
        break
sys.path = [tmpDir]+sys.path

from pandatools import PBookCore    # noqa: E402


# main for interactive session
def intmain(pbookCore,comString):

    # help
    def help(*arg):
        """
        Show the help doc
        """
        if len(arg) > 0:
            func = arg[0]
            print(func.__doc__)
        else:
            # print available methods
            tmp_str = """
The following commands are available:

    help
    show
    kill
    retry
    finish
    killAndRetry
    getUserJobMetadata

For more info, do help(show) for example
"""
            print(tmp_str)

    # show status
    def show(*args, **kwargs):
        """
        Print job records. The first argument (non-keyword) can be an jediTaskID or reqID, and can be omitted. The following keyword arguments are available in the way of panda monitor url query: [username, limit, taskname, days, jeditaskid].
        If sync=True, it forces panda monitor to get the latest records rather than get from cache.
        Specify display format with format='xxx', available formats are ['standard', 'long', 'json', 'plain'].
        The default filter conditions are: username=(name from user voms proxy), limit=1000, days=14, sync=False, format='standard'.

        example:
        >>> show()
        >>> show(123)
        >>> show(12345678, format='long')
        >>> show(taskname='my_task_name')
        >>> show(superstatus='running', days=7, limit=100)
        >>> show(format='json', sync=True)
        """
        return pbookCore.show(*args, **kwargs)

    # kill
    def kill(taskIDs):
        """
        Kill all subJobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID). If 'all', kill all active tasks of the user.

         example:
           >>> kill(123)
           >>> kill([123, 345, 567])
           >>> kill('all')
        """
        if taskIDs == 'all':
            # active tasks
            task_list = pbookCore.get_active_tasks()
            ret = list_parallel_exec(lambda task: pbookCore.kill(task.jeditaskid), task_list)
        elif isinstance(taskIDs, (list, tuple)):
            ret = list_parallel_exec(lambda taskID: pbookCore.kill(taskID), taskIDs)
        elif isinstance(taskIDs, (int, long)):
            ret = [ pbookCore.kill(taskIDs) ]
        else:
            print('Error: Invalid argument')
            ret = None
        return ret

    # finish
    def finish(taskIDs, soft=False):
        """
        Finish all subJobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID). If 'all', finish all active tasks of the user. If soft is False (default), all running jobs are killed and the task finishes immediately. If soft is True, new jobs are not generated and the task finishes once all running jobs finish.

         example:
           >>> finish(123)
           >>> finish(234, soft=True)
           >>> finish([123, 345, 567])
           >>> finish('all')
        """
        if taskIDs == 'all':
            # active tasks
            task_list = pbookCore.get_active_tasks()
            ret = list_parallel_exec(lambda task: pbookCore.finish(task.jeditaskid, soft=soft), task_list)
        elif isinstance(taskIDs, (list, tuple)):
            ret = list_parallel_exec(lambda taskID: pbookCore.finish(taskID, soft=soft), taskIDs)
        elif isinstance(taskIDs, (int, long)):
            ret = [ pbookCore.finish(taskIDs, soft=soft) ]
        else:
            print('Error: Invalid argument')
            ret = None
        return ret

    # retry
    def retry(taskIDs, newOpts=None):
        """
        Retry failed/cancelled subJobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID). This means that you need to have the same runtime env (such as Athena version, run dir, source files) as the previous submission. One can use newOpts which is a map of options and new arguments like {'nFilesPerJob':10,'excludedSite':'ABC,XYZ'} to overwrite task parameters. The list of changeable parameters is site,excludedSite,includedSite,nFilesPerJob,nGBPerJob,nFiles,nEvents. If input files were used or are being used by other jobs for the same output dataset container, those file are skipped to avoid job duplication when retrying failed subjobs.

         example:
           >>> retry(123)
           >>> retry([123, 345, 567])
           >>> retry(789, newOpts={'excludedSite':'siteA,siteB'})
        """
        if newOpts is None:
            newOpts = {}
        if isinstance(taskIDs, (list, tuple)):
            ret = list_parallel_exec(lambda taskID: pbookCore.retry(taskID, newOpts=newOpts), taskIDs)
        elif isinstance(taskIDs, (int, long)):
            ret = [ pbookCore.retry(taskIDs, newOpts=newOpts) ]
        else:
            print('Error: Invalid argument')
            ret = None
        return ret

    # debug mode
    def debug(PandaID, modeOn):
        """
        Turn the debug mode on/off for a subjob with PandaID. modeOn is True/False to enable/disable the debug mode. Note that the maxinum number of debug subjobs is limited. If you already hit the limit you need to disable the debug mode for a subjob before debugging another subjob

         example:
           >>> debug(1234, True)
        """
        pbookCore.debug(PandaID, modeOn)

    # kill and retry
    def killAndRetry(taskIDs, newOpts=None):
        """
        Kill JobID and then retry failed/cancelled sub-jobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID). Concerning newOpts, see help(retry)

         example:
           >>> killAndRetry(123)
           >>> killAndRetry([123, 345, 567])
           >>> killAndRetry(789, newOpts={'excludedSite':'siteA,siteB'})
        """
        if newOpts is None:
            newOpts = {}
        if isinstance(taskIDs, (list, tuple)):
            ret = list_parallel_exec(lambda taskID: pbookCore.killAndRetry(taskID, newOpts=newOpts), taskIDs)
        elif isinstance(taskIDs, (int, long)):
            ret = [ pbookCore.killAndRetry(taskIDs, newOpts=newOpts) ]
        else:
            print('Error: Invalid argument')
            ret = None
        return ret

    # get user job metadata
    def getUserJobMetadata(taskID, outputFileName):
        """
        Get user metadata of successful jobs in a task and write them in a json file

         example:
           >>> getUserJobMetadata(123, 'meta.json')
        """
        pbookCore.getUserJobMetadata(taskID, outputFileName)

    # execute command in the batch mode
    if comString != '':
        # decompose to function name and argument
        match = re.search('^([^\(]+)\(([^\)]*)\)$',comString)
        comName = match.group(1)
        comArgS = match.group(2)
        comArg = []
        comMap = {}
        if comArgS != '':
            for item in comArgS.split(','):
                item = item.strip()
                if '=' in item:
                    tmpKey = item.split('=')[0]
                    tmpVal = item.split('=')[-1]
                    comMap[tmpKey.strip()] = eval(tmpVal)
                else:
                    comArg.append(eval(item))
        comArg = tuple(comArg)
        # update map
        # pbookCore.updateTaskJobsetMap()
        # exec : exec cannot be used due to unqualified exec with nested functions
        locals()[comName](*comArg, **comMap)
        # exit
        sys.exit(0)
    # go to interactive prompt
    code.interact(banner="\nStart pBook %s" % PandaToolsPkgInfo.release_version,
                  local=locals())


# kill whole process
def catch_sig(sig, frame):
    # cleanup
    _onExit(tmpDir,historyFile)
    # kill
    commands_get_output('kill -9 -- -%s' % os.getpgrp())


# overall main
def main():
    # parse option
    parser = optparse.OptionParser()
    parser.add_option("-v",action="store_true",dest="verbose",default=False,
                      help="verbose")
    parser.add_option('-c',action='store',dest='comString',default='',type='string',
                      help='execute a command in the batch mode')
    parser.add_option('--version',action='store_const',const=True,dest='version',default=False,
                      help='Displays version')
    parser.add_option('--devSrv',action='store_const',const=True,dest='devSrv',default=False,
                      help="Please don't use this option. Only for developers to use the dev panda server")
    parser.add_option('--intrSrv',action='store_const',const=True, dest='intrSrv',default=False,
                      help="Please don't use this option. Only for developers to use the intr panda server")

    options,args = parser.parse_args()

    # display version
    if options.version:
        print("Version: %s" % PandaToolsPkgInfo.release_version)
        sys.exit(0)

    # use dev server
    if options.devSrv:
        Client.useDevServer()

    # use INTR server
    if options.intrSrv:
        Client.useIntrServer()

    # fork for Ctl-c
    fork_child_pid = os.fork()
    if fork_child_pid == -1:
        print("ERROR : Failed to fork")
        sys.exit(1)
    if fork_child_pid == 0:
        # main
        # instantiate core
        if options.verbose:
            print(options)
        pbookCore = PBookCore.PBookCore(verbose=options.verbose)

        # CUI
        intmain(pbookCore,options.comString)
    else:
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM,catch_sig)
        os.wait()
