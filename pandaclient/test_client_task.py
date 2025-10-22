import argparse
import subprocess, re, uuid, tempfile, sys, os

from pandaclient.Client import insertTaskParams, getTaskParamsMap, killTask, pauseTask, resumeTask, getTaskStatus, finishTask, retryTask, reactivateTask, increase_attempt_nr, reload_input, getJediTaskDetails, get_files_in_datasets, getJobIDsJediTasksInTimeRange, getPandaIDsWithTaskID, getUserJobMetadata

def main(task_id):
    outds = "user.pandasv2.{0}".format(uuid.uuid4())
    with tempfile.TemporaryDirectory() as tmpdir:
        # go into temp dir
        cwd = os.getcwd()
        os.chdir(tmpdir)

        cmd = (
            '''prun --exec "pwd; ls; echo Hello-world > myout.txt" '''
            '''--outDS {outds} --nJobs 3 --output myout.txt'''.format(outds=outds)
        )

        res = subprocess.run(cmd, shell=True, text=True, capture_output=True)
        out = (res.stdout or "") + "\n" + (res.stderr or "")
        print(out)

        os.chdir(cwd)  # back to original dir

    m = re.search(r"new jediTaskID=(\d+)", out)
    if not m:
        print("Failed to find task ID in output of prun command:")
        print(out.strip())
        sys.exit(1)

    print("=============================================================")
    status_ret_old = getTaskStatus(task_id)
    params_ret_old = getTaskParamsMap(task_id)
    details_ret_old = getJediTaskDetails({"jediTaskID": task_id}, True, True)
    pause_ret_old = pauseTask(task_id)
    resume_ret_old = resumeTask(task_id)
    kill_ret_old = killTask(task_id)
    finish_ret_old = finishTask(task_id)
    retry_ret_old = retryTask(task_id)
    reactivate_ret_old = reactivateTask(task_id)
    get_jobs_old = getJobIDsJediTasksInTimeRange('2025-08-01 14:30:45')
    get_ids_old = getPandaIDsWithTaskID(task_id)
    increase_ret_old = increase_attempt_nr(task_id)
    reload_ret_old = reload_input(task_id)
    files_ret_old = get_files_in_datasets(task_id)
    metadata_old = getUserJobMetadata(task_id, verbose=True)

    print("getTaskStatus returned: {0}".format(status_ret_old))
    print("getTaskParams returned: {0}".format(params_ret_old))
    print("getJediTaskDetails returned: {0}".format(details_ret_old))
    print("pauseTask returned: {0}".format(pause_ret_old))
    print("resumeTask returned: {0}".format(resume_ret_old))
    print("killTask returned: {0}".format(kill_ret_old))
    print("finishTask returned: {0}".format(finish_ret_old))
    print("retryTask returned: {0}".format(retry_ret_old))
    print("reactivateTask returned: {0}".format(reactivate_ret_old))
    print("getJobIDsJediTasksInTimeRange returned: {0}".format(get_jobs_old))
    print("getPandaIDsWithTaskID returned: {0}".format(get_ids_old))
    print("increaseAttemptNr returned: {0}".format(increase_ret_old))
    print("reloadInput returned: {0}".format(reload_ret_old))
    print("get_files_in_datasets returned: {0}".format(files_ret_old))
    print("getUserJobMetadata returned: {0}".format(metadata_old))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id", type=int, help="The task ID to process.")
    args = parser.parse_args()

    main(args.task_id)