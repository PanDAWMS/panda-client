import argparse
import subprocess, re, uuid, tempfile, sys, os

from pandaclient.Client import insertTaskParams_new, killTask_new, pauseTask_new, resumeTask_new, getTaskStatus_new, finishTask_new, retryTask_new
from pandaclient.Client import insertTaskParams, killTask, pauseTask, resumeTask, getTaskStatus, finishTask, retryTask

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
    pause_ret_old = pauseTask(task_id)
    resume_ret_old = resumeTask(task_id)
    kill_ret_old = killTask(task_id)
    finish_ret_old = finishTask(task_id)
    retry_ret_old = retryTask(task_id)

    print("getTaskStatus returned: {0}".format(status_ret_old))
    print("pauseTask returned: {0}".format(pause_ret_old))
    print("resumeTask returned: {0}".format(resume_ret_old))
    print("killTask returned: {0}".format(kill_ret_old))
    print("finishTask returned: {0}".format(finish_ret_old))
    print("retryTask returned: {0}".format(retry_ret_old))

    print("=============================================================")
    status_ret_new = getTaskStatus_new(task_id)
    pause_ret_new = pauseTask_new(task_id)
    resume_ret_new = resumeTask_new(task_id)
    kill_ret_new = killTask_new(task_id)
    finish_ret_new = finishTask_new(task_id)
    retry_ret_new = retryTask_new(task_id)

    print("getTaskStatus_new returned: {0}".format(status_ret_new))
    print("pauseTask_new returned: {0}".format(pause_ret_new))
    print("resumeTask_new returned: {0}".format(resume_ret_new))
    print("killTask_new returned: {0}".format(kill_ret_new))
    print("finishTask_new returned: {0}".format(finish_ret_new))
    print("retryTask_new returned: {0}".format(retry_ret_new))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id", type=int, help="The task ID to process.")
    args = parser.parse_args()

    main(args.task_id)