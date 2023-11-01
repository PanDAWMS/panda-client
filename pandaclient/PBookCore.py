import base64
import copy
import json
import re
import sys
import time

try:
    long()
except Exception:
    long = int

from pandaclient import PsubUtils, localSpecs, queryPandaMonUtils

from . import Client, PLogger


def is_reqid(id):
    """
    whether an id is a reqID (otherwise jediTaskID)
    """
    return id < 10**7


def _get_one_task(self, taskID, verbose=False):
    """
    get one task spec by ID
    """
    ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, jeditaskid=taskID, verbose=verbose)
    if isinstance(data, list) and data:
        task = data[0]
        taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
        return taskspec
    else:
        return None


def _get_tasks_from_reqid(self, reqID, verbose=False):
    """
    get a list of task spec by reqID
    """
    ts, url, data = queryPandaMonUtils.query_tasks(username=self.username, reqid=reqID, verbose=verbose)
    if isinstance(data, list) and data:
        taskspec_list = []
        for task in data:
            taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
            taskspec_list.append(taskspec)
        return taskspec_list
    else:
        return None


func_return_value = True


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
        # check task owner
        try:
            taskid = None
            if args:
                taskid = args[0]
            if taskid is None:
                tmpLog.error("no taskID sepcified, nothing done")
                return
            # taskspec = _get_one_task(self, taskid, self.verbose)
            if is_reqid(taskid):
                taskspec_list = _get_tasks_from_reqid(self, taskid, self.verbose)
            else:
                taskspec_list = [_get_one_task(self, taskid, self.verbose)]
        except Exception as e:
            tmpLog.error("got {0}: {1}".format(e.__class__.__name__, e))
        else:
            ret = True
            if taskspec_list is None:
                sys.stdout.write("Permission denied: reqID={0} is not owned by {1} \n".format(taskid, self.username))
                ret = False
            else:
                for taskspec in taskspec_list:
                    if taskspec is not None and taskspec.username == self.username:
                        args_new = (taskspec.jeditaskid,) + args[1:]
                        ret = ret and func(self, *args_new, **kwargs)
                    else:
                        sys.stdout.write("Permission denied: taskID={0} is not owned by {1} \n".format(taskid, self.username))
                        ret = False
        global func_return_value
        func_return_value = ret
        return ret

    wrapper.original_func = func
    return wrapper


# core class for book keeping
class PBookCore(object):
    # constructor
    def __init__(self, verbose=False):
        # verbose
        self.verbose = verbose
        self.username = None

    # init
    def init(self, sanity_check=True):
        # check proxy
        if sanity_check:
            PsubUtils.check_proxy(self.verbose, None)
        # user name
        username_from_proxy = PsubUtils.extract_voms_proxy_username()
        if username_from_proxy:
            self.username = username_from_proxy
            sys.stdout.write("PBook user: {0} \n".format(self.username))
        else:
            sys.stderr.write("ERROR : Cannot get user name from proxy or token. " 'Please generate a new one using "generate_credential"\n')
            sys.exit(1)

    # kill
    @check_task_owner
    def kill(self, taskID):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill JEDI task
        tmpLog.info("Sending killTask command ...")
        status, output = Client.killTask(taskID, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to kill jediTaskID=%s" % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error("Failed to kill jediTaskID=%s" % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info("Done, jediTaskID=%s will be killed in 30 min" % taskID)
        return True

    # finish
    @check_task_owner
    def finish(self, taskID, soft=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # finish JEDI task
        tmpLog.info("Sending finishTask command ...")
        status, output = Client.finishTask(taskID, soft, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to finish jediTaskID=%s" % taskID)
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error("Failed to finish jediTaskID=%s" % taskID)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info("Done, jediTaskID=%s will be finished soon" % taskID)
        return True

    # set debug mode
    def debug(self, pandaID, modeOn):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # set
        status, output = Client.setDebugMode(pandaID, modeOn, self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to set debug mode for %s" % pandaID)
            return
        # done
        tmpLog.info(output)
        return

    # kill and retry
    def killAndRetry(self, taskID, newOpts=None):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill
        retK = self.kill(taskID)
        if not retK:
            return False
        # sleep
        tmpLog.info("Going to sleep for 3 sec")
        time.sleep(3)
        nTry = 6
        for iTry in range(nTry):
            # check if task terminated
            taskspec = _get_one_task(self, taskID, self.verbose)
            if taskspec is not None:
                if taskspec.is_terminated():
                    break
                else:
                    tmpLog.info("Some sub-jobs are still running")
            else:
                tmpLog.warning("Could not get task status from panda monitor...")
            if iTry + 1 < nTry:
                # sleep
                tmpLog.info("Going to sleep for 30 sec")
                time.sleep(30)
            else:
                tmpLog.info("Max attempts exceeded. Please try later")
                return False
        # retry
        return self.retry(taskID, newOpts=newOpts)

    # retry
    @check_task_owner
    def retry(self, taskID, newOpts=None):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # set an empty map since mutable default value is used
        if newOpts is None:
            newOpts = {}
        else:
            newOpts = copy.deepcopy(newOpts)
        # warning for PQ
        site = newOpts.get("site", None)
        excludedSite = newOpts.get("excludedSite", None)
        PsubUtils.get_warning_for_pq(site, excludedSite, tmpLog)
        # for JEDI
        status, out = Client.retryTask(taskID, verbose=self.verbose, properErrorCode=True, newParams=newOpts)
        if status != 0:
            tmpLog.error(status)
            tmpLog.error(out)
            tmpLog.error("Failed to retry TaskID=%s" % taskID)
            return False
        tmpStat, tmpDiag = out
        if (tmpStat not in [0, True] and newOpts == {}) or (newOpts != {} and tmpStat != 3):
            tmpLog.error(tmpDiag)
            tmpLog.error("Failed to retry TaskID=%s" % taskID)
            return False
        tmpLog.info(tmpDiag)
        return True

    # recover lost files
    @check_task_owner
    def recover_lost_files(self, taskID, test_mode=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # kill JEDI task
        tmpLog.info("Sending recovery request ...")
        status, output = Client.send_file_recovery_request(taskID, test_mode, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Communication failure with the server")
            return False
        tmpStat, tmpDiag = output
        if not tmpStat:
            tmpLog.error(tmpDiag)
            tmpLog.error("request was not received")
            return False
        # done
        tmpLog.info(tmpDiag)
        return True

    # get job metadata
    def getUserJobMetadata(self, taskID, output_filename):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # get metadata
        tmpLog.info("getting metadata")
        status, metadata = Client.getUserJobMetadata(taskID, verbose=self.verbose)
        if status != 0:
            tmpLog.error(metadata)
            tmpLog.error("Failed to get metadata")
            return False
        with open(output_filename, "w") as f:
            json.dump(metadata, f)
        tmpLog.info("dumped to {0}".format(output_filename))
        # return
        return True

    # get task specs of active tasks
    def get_active_tasks(self):
        """
        get all reachable task specs of the user
        """
        active_superstatus_str = "|".join(localSpecs.task_active_superstatus_list)
        ts, url, data = queryPandaMonUtils.query_tasks(
            username=self.username,
            superstatus=active_superstatus_str,
            verbose=self.verbose,
        )
        if isinstance(data, list) and list:
            taskspec_list = [localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts) for task in data]
            return taskspec_list
        else:
            return None

    # show status
    def show(
        self,
        some_ids=None,
        username=None,
        limit=1000,
        taskname=None,
        days=14,
        jeditaskid=None,
        reqid=None,
        status=None,
        superstatus=None,
        metadata=False,
        sync=False,
        format="standard",
    ):
        # user name
        if username is None:
            username = self.username
        # shortcut of jeditaskid and reqid
        if isinstance(some_ids, (int, long)):
            if is_reqid(some_ids):
                reqid = str(some_ids)
            else:
                jeditaskid = str(some_ids)
        elif isinstance(some_ids, (list, tuple)) and some_ids:
            first_id = some_ids[0]
            ids_str = "|".join([str(x) for x in some_ids])
            if first_id and isinstance(first_id, (int, long)) and is_reqid(first_id):
                reqid = ids_str
            else:
                jeditaskid = ids_str
        elif some_ids == "run":
            superstatus = "|".join(localSpecs.task_active_superstatus_list)
        elif some_ids == "fin":
            superstatus = "|".join(localSpecs.task_final_superstatus_list)
        # print
        if format != "json":
            sys.stderr.write(
                "Showing only max {limit} tasks in last {days} days. One can set days=N to see tasks in last N days, and limit=M to see at most M latest tasks \n".format(
                    days=days, limit=limit
                )
            )
        # query
        ts, url, data = queryPandaMonUtils.query_tasks(
            username=username,
            limit=limit,
            reqid=reqid,
            status=status,
            superstatus=superstatus,
            taskname=taskname,
            days=days,
            jeditaskid=jeditaskid,
            metadata=metadata,
            sync=sync,
            verbose=self.verbose,
        )
        # print header row
        _tmpts = localSpecs.LocalTaskSpec
        if format in ["json", "plain"]:
            pass
        elif format == "long":
            print(_tmpts.head_dict["long"])
        else:
            print(_tmpts.head_dict["standard"])
        # print tasks
        if format == "json":
            return data
        elif format == "plain":
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_plain()
        elif format == "long":
            i_count = 1
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                if i_count % 10 == 0:
                    print(_tmpts.head_dict["long"])
                taskspec.print_long()
                i_count += 1
        else:
            for task in data:
                taskspec = localSpecs.LocalTaskSpec(task, source_url=url, timestamp=ts)
                taskspec.print_standard()

    # execute workflow command
    def execute_workflow_command(self, command_name, request_id):
        tmpLog = PLogger.getPandaLogger()
        tmpLog.info("executing {}".format(command_name))
        status, output = Client.call_idds_user_workflow_command(command_name, {"request_id": request_id}, self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to execute {}".format(command_name))
            return False, None
        if not output[0]:
            tmpLog.error(output[-1])
            tmpLog.error("Failed to execute {}".format(command_name))
            return False, None
        return True, output[-1]

    # set secret
    def set_secret(self, key, value, is_file=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        if is_file:
            with open(value, "rb") as f:
                value = base64.b64encode(f.read()).decode()
            # add prefix
            key = "___file___:" + format(key)
        size_limit = 1000
        if value and len(value) > size_limit * 1024:
            tmpLog.error("The value length exceeds the limit ({0} kB)".format(size_limit))
            return False
        status, output = Client.set_user_secert(key, value, verbose=self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to set secret")
            return False
        status, msg = output
        if status:
            tmpLog.info(msg)
        else:
            tmpLog.error(msg)
        # return
        return status

    # list secrets
    def list_secrets(self, full=False):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        status, output = Client.get_user_secerts(verbose=self.verbose)
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to get secrets")
            return False
        status, data = output
        if status:
            if data:
                prefix = "^___[a-z]+___:"
                msg = "\n"
                keys = [re.sub(prefix, "", k) for k in data.keys()]
                big_key = len(max(keys, key=len))
                template = "{{:{}s}}: {{}}\n".format(big_key + 1)
                msg += template.format("Key", "Value")
                msg += template.format("-" * big_key, "-" * 20)
                keys.sort()
                max_len = 50
                for k in data:
                    value = data[k]
                    # hide prefix
                    if re.search(prefix, k):
                        k = re.sub(prefix, "", k)
                    if not full and len(value) > max_len:
                        value = value[:max_len] + "..."
                    msg += template.format(k, value)
            else:
                msg = "No secrets"
            print(msg)
        else:
            tmpLog.error(data)
        # return
        return status

    # pause
    def pause(self, task_id):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # pause JEDI task
        tmpLog.info("Sending pause command ...")
        status, output = Client.pauseTask(task_id, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to pause jediTaskID=%s" % task_id)
            return False
        tmpStat, tmpDiag = output
        if tmpStat != 0:
            print(tmpStat)
            tmpLog.error(tmpDiag)
            tmpLog.error("Failed to pause jediTaskID=%s" % task_id)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info("Done")
        return True

    # resume
    def resume(self, task_id):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # pause JEDI task
        tmpLog.info("Sending resume command ...")
        status, output = Client.resumeTask(task_id, self.verbose)
        # communication error
        if status != 0:
            tmpLog.error(output)
            tmpLog.error("Failed to resume jediTaskID=%s" % task_id)
            return False
        tmpStat, tmpDiag = output
        if tmpStat != 0:
            tmpLog.error(tmpDiag)
            tmpLog.error("Failed to resume jediTaskID=%s" % task_id)
            return False
        tmpLog.info(tmpDiag)
        # done
        tmpLog.info("Done")
        return True

    # generate_credential
    def generate_credential(self):
        return PsubUtils.check_proxy(self.verbose, None, generate_new=True)

    # reload input and then retry the task
    def reload_input(self, task_id):
        # get logger
        tmp_log = PLogger.getPandaLogger()
        # set
        status, output = Client.reload_input(task_id, self.verbose)
        if status != 0:
            tmp_log.error(output)
            tmp_log.error("Failed to reload input %s" % task_id)
            return False
        elif output[0] != 0:
            tmp_log.error(output[-1])
            tmp_log.error("Failed to reload input %s" % task_id)
            return False
        # done
        tmp_log.info("command is registered. will be executed in a few minutes")
        return True
