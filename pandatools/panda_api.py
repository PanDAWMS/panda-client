import os
import sys
import json
import copy
import tempfile
import importlib

from . import PLogger
from . import Client
from . import PBookCore


class PandaAPI(object):

    # constructor
    def __init__(self):
        self.command_body = {}
        self.pbook = None
        self.log = PLogger.getPandaLogger()

    # kill a task
    def kill_task(self, task_id, verbose=True):
        """kill a task
           args:
              task_id: jediTaskID of the task to be killed
              verbose: True to see debug messages
           returns:
              status code
                 0: communication succeeded to the panda server
               255: communication failure
              tuple of return code and diagnostic message
                 0: request is registered
                 1: server error
                 2: task not found
                 3: permission denied
                 4: irrelevant task status
               100: non SSL connection
               101: irrelevant taskID
        """
        return Client.killTask(task_id)

    # finish a task
    def finish_task(self, task_id, wait_running=False, verbose=False):
        """finish a task
           args:
              task_id: jediTaskID of the task to finish
              wait_running: True to wait until running jobs are done
              verbose: True to see debug messages
           returns:
              status code
                 0: communication succeeded to the panda server
               255: communication failure
              tuple of return code and diagnostic message
                 0: request is registered
                 1: server error
                 2: task not found
                 3: permission denied
                 4: irrelevant task status
               100: non SSL connection
               101: irrelevant taskID
        """
        return Client.finishTask(task_id, wait_running, verbose)

    # retry a task
    def retry_task(self, task_id, new_parameters=None, verbose=False):
        """retry a task
           args:
              task_id: jediTaskID of the task to retry
              new_parameters: a dictionary of task parameters to overwrite
              verbose: True to see debug messages
           returns:
              status code
                 0: communication succeeded to the panda server
               255: communication failure
              tuple of return code and diagnostic message
                 0: request is registered
                 1: server error
                 2: task not found
                 3: permission denied
                 4: irrelevant task status
               100: non SSL connection
               101: irrelevant taskID
        """
        return Client.retryTask(task_id, verbose, True, new_parameters)

    # get tasks
    def get_tasks(self, task_ids=None, limit=1000, days=14, status=None, username=None):
        """get a list of task dictionaries
           args:
              task_ids: a list of task IDs, or None to get recent tasks
              limit: the max number of tasks to fetch from the server
              days: tasks for last N days to fetch
              status: filtering with task status
              username: user name of the tasks, or None to get own tasks
           returns:
              a list of task dictionaries
        """
        if not self.pbook:
            self.pbook = PBookCore.PBookCore()
        return self.pbook.show(task_ids, limit=limit, days=days, format='json', status=status,
                               username=username)

    # show tasks
    def show_tasks(self, task_ids=None, limit=1000, days=14, format='standard', status=None, username=None):
        """show tasks
           args:
              task_ids: a list of task IDs, or None to get recent tasks
              limit: the max number of tasks to fetch from the server
              days: tasks for last N days to fetch
              format: standard, long, or plain
              status: filtering with task status
              username: user name of the tasks, or None to get own tasks
           returns:
              None
        """
        if not self.pbook:
            self.pbook = PBookCore.PBookCore()
        self.pbook.show(task_ids, limit=limit, days=days, format=format, status=status, username=username)

    # submit a task
    def submit_task(self, task_params, verbose=False):
        """submit a task using low-level API
           args:
              task_params: a dictionary of task parameters
              verbose: True to see debug messages
           returns:
              status code
                 0: communication succeeded to the panda server
               255: communication failure
              tuple of return code, message from the server, and task ID if successful
                 0: request is processed
                 1: duplication in DEFT
                 2: duplication in JEDI
                 3: accepted for incremental execution
                 4: server error
        """
        return Client.insertTaskParams(task_params, verbose=verbose, properErrorCode=True)

    # get metadata of all jobs in a task
    def get_job_metadata(self, task_id, output_json_filename):
        """get metadata of all jobs in a task
           args:
              task_id: task ID
              output_json_filename: output json filename
        """
        if not self.pbook:
            self.pbook = PBookCore.PBookCore()
        return self.pbook.getUserJobMetadata(task_id, output_json_filename)

    # execute xyz
    def execute_xyz(self, command_name, module_name, args, console_log=True):
        dump_file = None
        stat = False
        ret = None
        err_str = None
        try:
            # convert args
            sys.argv = copy.copy(args)
            sys.argv.insert(0, command_name)
            # set dump file
            if '--dumpJson' not in sys.argv:
                sys.argv.append('--dumpJson')
                with tempfile.NamedTemporaryFile(delete=False) as f:
                    sys.argv.append(f.name)
                    dump_file = f.name
            # disable logging
            if not console_log:
                PLogger.disable_logging()
            # run
            if command_name in self.command_body:
                self.command_body[command_name].reload()
            else:
                self.command_body[command_name] = importlib.import_module(module_name)
            stat = True
        except SystemExit as e:
            if e.code == 0:
                stat = True
            else:
                err_str = 'failed with code={0}'.format(e.code)
        except Exception as e:
            err_str = 'failed with {0}'.format(str(e))
        finally:
            # enable logging
            if not console_log:
                PLogger.enable_logging()
            if err_str:
                self.log.error(err_str)
            # read dump fle
            try:
                with open(sys.argv[sys.argv.index('--dumpJson') + 1]) as f:
                    ret = json.load(f)
                    if len(ret) == 1:
                        ret = ret[0]
            except Exception:
                pass
        # delete dump file
        if not dump_file:
            os.remove(dump_file)
        return stat, ret

    # execute prun
    def execute_prun(self, args, console_log=True):
        """execute prun command

           args:
               args: The arguments used to execute prun. This is a list of strings.
               console_log: False to disable console logging

           returns:
               status: True if succeeded. Otherwise, False
               a dictionary: Task submission attributes including jediTaskID
        """
        return self.execute_xyz('prun', 'pandatools.PrunScript', args, console_log)

    # execute pathena
    def execute_pathena(self, args, console_log=True):
        """execute pathena command

           args:
               args: The arguments used to execute prun. This is a list of strings.
               console_log: False to disable console logging

           returns:
               status: True if succeeded. Otherwise, False
               a dictionary: Task submission attributes including jediTaskID
        """
        return self.execute_xyz('pathena', 'pandatools.PathenaScript', args, console_log)

    # execute phpo
    def execute_phpo(self, args, console_log=True):
        """execute phpo command

           args:
               args: The arguments used to execute prun. This is a list of strings.
               console_log: False to disable console logging

           returns:
               status: True if succeeded. Otherwise, False
               a dictionary: Task submission attributes including jediTaskID
        """
        return self.execute_xyz('phpo', 'pandatools.PhpoScript', args, console_log)

    # hello
    def hello(self, verbose=False):
        """Health check with the PanDA server
           args:
              verbose: True to see verbose message
           returns:
              status code
                 0: communication succeeded to the panda server
               255: communication failure
              diagnostic message
        """
        return Client.hello(verbose)


pandaAPI = PandaAPI()
del PandaAPI


def get_api():
    return pandaAPI
