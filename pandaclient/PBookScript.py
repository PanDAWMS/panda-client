"""
Do NOT import this module in your code.
Import PBookCore instead.
"""

import os
import sys
import code
import atexit
import signal
import tempfile

from pandaclient.MiscUtils import commands_get_output
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


import argparse
import readline

from pandaclient import Client
from pandaclient import PandaToolsPkgInfo

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
    try:
        # except for macOS X
        readline.read_history_file(historyFile)
    except Exception:
        pass
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
    if os.path.exists(path) and os.path.isdir(path) and 'pandaclient' in os.listdir(path) \
           and os.path.exists('%s/pandaclient/__init__.py' % path):
        # make symlink for module name
        os.symlink('%s/pandaclient' % path,'%s/taskbuffer' % tmpDir)
        break
sys.path = [tmpDir]+sys.path

from pandaclient import PBookCore    # noqa: E402

orig_help = help


# main for interactive session
def intmain(pbookCore,comString):

    # help
    def help(*arg):
        """
        Show the help doc
        """
        if len(arg) > 0:
            try:
                if isinstance(arg[0], str):
                    func = main_locals[arg[0]]
                else:
                    func = arg[0]
                orig_help(func)
                return
            except Exception:
                print("Unknown command : {0}".format(str(arg[0])))
        # print available methods
        tmp_str = """
The following commands are available:

    help
    show
    showl
    kill
    retry
    finish
    kill_and_retry
    get_user_job_metadata
    recover_lost_files
    show_workflow
    kill_workflow
    retry_workflow
    finish_workflow
    pause_workflow
    resume_workflow
    set_secret
    list_secrets
    delete_secret
    delete_all_secrets

For more info, do help(show) for example
"""
        print(tmp_str)

    # show status
    def show(*args, **kwargs):
        """
        Print task records. The first argument (non-keyword) can be an jediTaskID or reqID, or 'run' (show active tasks only), or 'fin' (show terminated tasks only), or can be omitted. The following keyword arguments are available in the way of panda monitor url query: [username, limit, taskname, days, jeditaskid].
        If sync=True, it forces panda monitor to get the latest records rather than get from cache.
        Specify display format with format='xxx', available formats are ['standard', 'long', 'json', 'plain'].
        The default filter conditions are: username=(name from user voms proxy), limit=1000, days=14, sync=False, format='standard'.

        example:
        >>> show()
        >>> show(123)
        >>> show(12345678, format='long')
        >>> show(taskname='my_task_name')
        >>> show('run')
        >>> show('fin', days=7, limit=100)
        >>> show(format='json', sync=True)
        """
        return pbookCore.show(*args, **kwargs)

    # shortcut to show long status
    def showl(*args, **kwargs):
        """
        Print task records in long format; shortcut function of show(..., format='long'). See help message of show() for other keywords arguments

        example:
        >>> showl()
        >>> showl(123)
        >>> showl(12345678)
        >>> showl(taskname='my_task_name')
        """
        kwargs['format'] = 'long'
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
    def retry(taskIDs, newOpts=None, **kwargs):
        """
        Retry failed/cancelled subJobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID).
        This means that you need to have the same runtime env (such as Athena version, run dir, source files)
        as the previous submission. One can use newOpts which is a map of options and new arguments like
        {'nFilesPerJob':10,'excludedSite':'ABC,XYZ'} to overwrite task parameters. The list of changeable
        parameters is site, excludedSite, includedSite, nFilesPerJob, nGBPerJob, nFiles, nEvents, loopingCheck,
        nMaxFilesPerJob, ramCount, avoidVP. It is also possible to specify those parameters as named arguments
        in the retry call, e.g. nFilesPerJob=10, excludedSite='ABC,XYZ'.
        If input files were used or are being used by other jobs for the same
        output dataset container, those file are skipped to avoid job duplication when retrying failed subjobs.

         example:
           >>> retry(123)
           >>> retry([123, 345, 567])
           >>> retry(789, newOpts={'excludedSite':'siteA,siteB'})
           >>> retry(789, excludedSite='siteA,siteB')
        """
        if newOpts is None:
            newOpts = kwargs
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

    # kill and retry
    def kill_and_retry(taskIDs, newOpts=None, **kwargs):
        """
        Kill running sub-jobs and then retry failed/cancelled sub-jobs in taskIDs (ID or a list of ID, can be either jediTaskID or reqID). Concerning newOpts, see help(retry)

         example:
           >>> kill_and_retry(123)
           >>> kill_and_retry([123, 345, 567])
           >>> kill_and_retry(789, newOpts={'excludedSite':'siteA,siteB'})
           >>> kill_and_retry(789, excludedSite='siteA,siteB')
        """
        if newOpts is None:
            newOpts = kwargs
        return killAndRetry(taskIDs, newOpts)

    # get user job metadata
    def getUserJobMetadata(taskID, outputFileName):
        pbookCore.getUserJobMetadata(taskID, outputFileName)

    # get user job metadata
    def get_user_job_metadata(taskID, outputFileName):
        """
        Get user metadata of successful jobs in a task and write them in a json file

         example:
           >>> getUserJobMetadata(123, 'output.json')
        """
        getUserJobMetadata(taskID, outputFileName)

    # recover lost files
    def recover_lost_files(taskID, test_mode=False):
        """
        Send a request to recove lost files produced by a task. Set test_mode=True for testing

        example:
          >>> recover_lost_files(123)
          >>> recover_lost_files(123, test_mode=True)
        """
        pbookCore.recover_lost_files(taskID, test_mode)

    # finish a workflow
    def finish_workflow(request_id):
        """
        Send a request to finish a workflow

        """
        status, output = pbookCore.execute_workflow_command('finish', request_id)
        if output:
            print(output[0][-1])

    # kill a workflow
    def kill_workflow(request_id):
        """
        Send a request to kill a workflow

        """
        status, output = pbookCore.execute_workflow_command('abort', request_id)
        if output:
            print(output[0][-1])

    # pause a workflow
    def pause_workflow(request_id):
        """
        Send a request to pause a workflow

        """
        status, output = pbookCore.execute_workflow_command('suspend', request_id)
        if output:
            print(output[0][-1])

    # resume a workflow
    def resume_workflow(request_id):
        """
        Send a request to resume a workflow

        """
        status, output = pbookCore.execute_workflow_command('resume', request_id)
        if output:
            print(output[0][-1])

    # retry a workflow
    def retry_workflow(request_id):
        """
        Send a request to retry a workflow

        """
        status, output = pbookCore.execute_workflow_command('retry', request_id)
        if output:
            print(output[0][-1])

    # show a workflow
    def show_workflow(request_id):
        """
        Show a workflow

        """
        status, output = pbookCore.execute_workflow_command('get_status', request_id)
        if output:
            print(output)

    # set a secret
    def set_secret(key, value, is_file=False):
        """
        Define a pair of secret key-value strings. The value can be a file path to upload a secret file when is_file=True

        """
        pbookCore.set_secret(key, value, is_file)

    # delete a secret
    def delete_secret(key):
        """
        Delete a secret

        """
        pbookCore.set_secret(key, None)

    # delete all secrets
    def delete_all_secrets():
        """
        Delete all secrets

        """
        pbookCore.set_secret(None, None)

    # list secrets
    def list_secrets(full=False):
        """
        List secrets. Value strings are truncated by default. full=True to see entire strings

        """
        pbookCore.list_secrets(full)

    # execute command in the batch mode
    if comString != '':
        exec(comString) in globals(), locals()
        # exit
        if PBookCore.func_return_value:
            sys.exit(0)
        else:
            sys.exit(1)
    main_locals = locals()
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
    parser = argparse.ArgumentParser(conflict_handler="resolve")
    parser.add_argument("-v",action="store_true",dest="verbose",default=False,
                      help="Verbose")
    parser.add_argument('-c',action='store',dest='comString',default='',type=str,
                      help='Execute a command in the batch mode')
    parser.add_argument("-3", action="store_true", dest="python3", default=False,
                      help="Use python3")
    parser.add_argument('--version',action='store_const',const=True,dest='version',default=False,
                      help='Displays version')
    parser.add_argument('--devSrv',action='store_const',const=True,dest='devSrv',default=False,
                      help=argparse.SUPPRESS)
    parser.add_argument('--intrSrv',action='store_const',const=True, dest='intrSrv',default=False,
                      help=argparse.SUPPRESS)
    # option for jupyter notebook
    parser.add_argument('--prompt_with_newline', action='store_const', const=True, dest='prompt_with_newline',
                        default=False, help=argparse.SUPPRESS)

    options,args = parser.parse_known_args()

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
    global fork_child_pid
    fork_child_pid = os.fork()
    if fork_child_pid == -1:
        print("ERROR : Failed to fork")
        sys.exit(1)
    if fork_child_pid == 0:
        # main
        # instantiate core
        if options.verbose:
            print(options)
        if options.prompt_with_newline:
            sys.ps1 = ">>> \n"
        pbookCore = PBookCore.PBookCore(verbose=options.verbose)

        # CUI
        intmain(pbookCore,options.comString)
    else:
        # set handler
        signal.signal(signal.SIGINT, catch_sig)
        signal.signal(signal.SIGHUP, catch_sig)
        signal.signal(signal.SIGTERM,catch_sig)
        pid, status = os.wait()
        if os.WIFSIGNALED(status):
            sys.exit(-os.WTERMSIG(status))
        elif os.WIFEXITED(status):
            sys.exit(os.WEXITSTATUS(status))
        else:
            sys.exit(0)
