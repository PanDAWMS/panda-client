import argparse
import subprocess, re, uuid, tempfile, sys, os

from pandaclient.Client import insertTaskParams_new, getTaskParamsMap_new, killTask_new, pauseTask_new, resumeTask_new, getTaskStatus_new, finishTask_new, retryTask_new, reactivateTask_new, increase_attempt_nr_new, reload_input_new, getJediTaskDetails_new, get_files_in_datasets_new, getJobIDsJediTasksInTimeRange_new, getPandaIDsWithTaskID_new
from pandaclient.Client import insertTaskParams, getTaskParamsMap, killTask, pauseTask, resumeTask, getTaskStatus, finishTask, retryTask, reactivateTask, increase_attempt_nr, reload_input, getJediTaskDetails, get_files_in_datasets, getJobIDsJediTasksInTimeRange, getPandaIDsWithTaskID

def main(task_id):
    outds = f"user.pandasv2.{uuid.uuid4()}"

    with tempfile.TemporaryDirectory() as tmpdir:
        # go into temp dir
        cwd = os.getcwd()
        os.chdir(tmpdir)

        cmd = (
            f'''prun --exec "pwd; ls; echo Hello-world > myout.txt" '''
            f'''--outDS {outds} --nJobs 3 --output myout.txt'''
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
    details_ret_old = getJediTaskDetails(task_id)
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

    print("=============================================================")
    status_ret_new = getTaskStatus_new(task_id)
    params_ret_new = getTaskParamsMap_new(task_id)
    details_ret_new = getJediTaskDetails(task_id)
    pause_ret_new = pauseTask_new(task_id)
    resume_ret_new = resumeTask_new(task_id)
    kill_ret_new = killTask_new(task_id)
    finish_ret_new = finishTask_new(task_id)
    retry_ret_new = retryTask_new(task_id)
    reactivate_ret_new = reactivateTask_new(task_id)
    get_jobs_new = getJobIDsJediTasksInTimeRange_new('2025-08-01 14:30:45')
    get_ids_new = getPandaIDsWithTaskID_new(task_id)
    increase_ret_new = increase_attempt_nr_new(task_id)
    reload_ret_new = reload_input_new(task_id)
    files_ret_new = get_files_in_datasets_new(task_id)

    print("getTaskStatus_new returned: {0}".format(status_ret_new))
    print("getTaskParamsMap_new returned: {0}".format(params_ret_new))
    print("getJediTaskDetails_new returned: {0}".format(details_ret_new))
    print("pauseTask_new returned: {0}".format(pause_ret_new))
    print("resumeTask_new returned: {0}".format(resume_ret_new))
    print("killTask_new returned: {0}".format(kill_ret_new))
    print("finishTask_new returned: {0}".format(finish_ret_new))
    print("retryTask_new returned: {0}".format(retry_ret_new))
    print("reactivateTask_new returned: {0}".format(reactivate_ret_new))
    print("getJobIDsJediTasksInTimeRange_new returned: {0}".format(get_jobs_new))
    print("getPandaIDsWithTaskID_new returned: {0}".format(get_ids_new))
    print("increaseAttemptNr_new returned: {0}".format(increase_ret_new))
    print("reloadInput_new returned: {0}".format(reload_ret_new))
    print("get_files_in_datasets_new returned: {0}".format(files_ret_new))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id", type=int, help="The task ID to process.")
    args = parser.parse_args()

    main(args.task_id)