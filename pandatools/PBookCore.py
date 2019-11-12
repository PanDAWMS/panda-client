import json
import datetime
import time
import copy
import sys

try:
    long()
except Exception:
    long = int

# from . import PdbUtils
from . import Client
# from . import BookConfig
from . import PLogger
from . import PsubUtils
from pandatools import queryPandaMonUtils
from pandatools import localSpecs
from pandatools import MiscUtils


def _get_one_task(self, taskID, verbose=False):
    """
    get one task spec by ID
    """
    ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, jeditaskid=taskID,
                                                    verbose=verbose)
    if isinstance(data, list) and data:
        task = data[0]
        taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
        return taskspec
    else:
        return None

def check_task_owner(func):
    """
    sanity check decorator of user ownership vs the task
    """
    # Wrapper
    def wrapper(self, *args, **kwargs):
        # Make logger
        tmpLog = PLogger.getPandaLogger()
        # initialize
        ret = None
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # check task owner
        try:
            jeditaskid = None
            if args:
                jeditaskid = args[0]
            elif kwargs:
                jeditaskid = kwargs.get('taskID')
            if jeditaskid is None:
                tmpLog.error('no taskID sepcified, nothing done')
                return
            taskspec = _get_one_task(self, jeditaskid, self.verbose)
        except Exception as e:
            tmpLog.error('got {0}: {1}'.format(e.__class__.__name__, e))
        else:
            if taskspec is not None and taskspec.username == self.username:
                ret = func(self, *args, **kwargs)
            else:
                sys.stdout.write('Permission denied: taskID={0} is not owned by {1} \n'.format(jeditaskid, self.username))
        return ret
    return wrapper


# core class for book keeping
class PBookCore(object):

    # constructor
    def __init__(self, verbose=False):
        # verbose
        self.verbose = verbose
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # user name
        self.username = 'DEFAULT_USER'
        username_from_proxy = MiscUtils.extract_voms_proxy_username()
        if username_from_proxy:
            self.username = username_from_proxy
        # map between jobset and jediTaskID
        self.jobsetTaskMap = {}

    # kill
    @check_task_owner
    def kill(self, taskID):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill JEDI task
        tmpLog.info('Sending killTask command ...')
        status, output = Client.killTask(taskID, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to kill jediTaskID=%s' % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to kill jediTaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info('Done, jediTaskID=%s will be killed in 30 min' % taskID)
        return True

    # finish
    @check_task_owner
    def finish(self, taskID, soft=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # finish JEDI task
        tmpLog.info('Sending finishTask command ...')
        status, output = Client.finishTask(taskID, soft, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to finish jediTaskID=%s' % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to finish jediTaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info('Done, jediTaskID=%s will be finished soon' % taskID)
        return True

    # set debug mode
    def debug(self, pandaID, modeOn):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # check proxy
        PsubUtils.check_proxy(self.verbose, None)
        # set
        status,output = Client.setDebugMode(pandaID,modeOn,self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error('Failed to set debug mode for %s' % pandaID)
            return
        # done
        tmpLog.info(output)
        return

    # kill and retry
    def killAndRetry(self, taskID, newOpts={}):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill
        retK = self.kill(taskID)
        if not retK:
            return False
        # sleep
        tmpLog.info('Going to sleep for 3 sec')
        time.sleep(3)
        nTry = 6
        for iTry in range(nTry):
            # check if task terminated
            taskspec = _get_one_task(self, taskID, self.verbose)
            if taskspec is not None:
                if taskspec.is_terminated():
                    break
                else:
                    tmpLog.info('Some sub-jobs are still running')
            else:
                tmpLog.warning('Could not get task status from panda monitor...')
            if iTry + 1 < nTry:
                # sleep
                tmpLog.info('Going to sleep for 30 sec')
                time.sleep(30)
            else:
                tmpLog.info('Max attempts exceeded. Please try later')
                return False
        # retry
        self.retry(taskID, newOpts=newOpts)
        return


    # retry
    @check_task_owner
    def retry(self, taskID, newOpts={}):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # set an empty map since mutable default value is used
        if newOpts == {}:
            newOpts = {}
        else:
            newOpts = copy.deepcopy(newOpts)
        # for JEDI
        status,out = Client.retryTask(  taskID,
                                        verbose=self.verbose,
                                        properErrorCode=True,
                                        newParams=newOpts)
        if status != 0:
            tmpLog.error(status)
            tmpLog.error(out)
            tmpLog.error('Failed to retry TaskID=%s' % taskID)
            return False
        tmpStat,tmpDiag = out
        if (tmpStat not in [0,True] and newOpts == {}) or (newOpts != {} and tmpStat != 3):
            tmpLog.error(tmpDiag)
            tmpLog.error('Failed to retry TaskID=%s' % taskID)
            return False
        tmpLog.info(tmpDiag)

    # get job metadata
    def getUserJobMetadata(self, taskID, output_filename):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # get metadata
        tmpLog.info('getting metadata')
        status, metadata = Client.getUserJobMetadata(taskID, verbose=self.verbose)
        if status != 0:
            tmpLog.error(metadata)
            tmpLog.error("Failed to get metadata")
            return False
        with open(output_filename, 'w') as f:
            json.dump(metadata, f)
        tmpLog.info('dumped to {0}'.format(output_filename))
        # return
        return True

    # get task specs of active tasks
    def get_active_tasks(self):
        """
        get all reachable task specs of the user
        """
        active_superstatus_str = '|'.join(localSpecs.task_active_superstatus_list)
        ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, superstatus=active_superstatus_str,
                                                        verbose=self.verbose)
        if isinstance(data, list) and list:
            taskspec_list = [ localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts) for task in data ]
            return taskspec_list
        else:
            return None

    # show status
    def show(self, username=None, limit=1000, taskname=None, days=14, jeditaskid=None,
                metadata=False, sync=False, format='standard'):
        # user name
        if username is None:
            username = self.username
        # query
        ts, url, data = queryPandaMonUtils.query_tasks( username=username, limit=limit,
                                                        taskname=taskname, days=days, jeditaskid=jeditaskid,
                                                        metadata=metadata, sync=sync, verbose=self.verbose)
        # verbose
        # if self.verbose:
        #     print('timestamp: {ts} \nquery_url: {url}'.format(ts=ts, url=url))
        # print header row
        _tmpts = localSpecs.LocalTaskSpec
        if format in ['json', 'plain']:
            pass
        elif format == 'long':
            print(_tmpts.head_dict['long'])
        else:
            print(_tmpts.head_dict['standard'])
        # print tasks
        if format == 'json':
            print(json.dumps(data, sort_keys=True, indent=4))
        elif format == 'plain':
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_plain()
        elif format == 'long':
            i_count = 1
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                if i_count % 10 == 0:
                    print(_tmpts.head_dict['long'])
                taskspec.print_long()
                i_count += 1
        else:
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_standard()
