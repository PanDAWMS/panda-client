import subprocess, re, uuid, tempfile, sys, os

from pandaclient.Client import insertTaskParams_new, killTask_new, pauseTask_new, resumeTask_new, getTaskStatus_new
from pandaclient.Client import insertTaskParams, killTask, pauseTask, resumeTask, getTaskStatus

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

        os.chdir(cwd)  # back to original dir

    m = re.search(r"new jediTaskID=(\d+)", out)
    if not m:
        print("Failed to find task ID in output of prun command:")
        print(out.strip())
        sys.exit(1)

    print("=============================================================")
    pause_ret_old = pauseTask(task_id)
    resume_ret_old = resumeTask(task_id)

    print("old functions returned {}".format(pause_ret_old, resume_ret_old))

    print("=============================================================")
    pause_ret_new = pauseTask_new(task_id)
    resume_ret_new = resumeTask_new(task_id)

    print("new functions returned {}".format(pause_ret_new, resume_ret_new))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id", type=int, help="The task ID to process.")
    args = parser.parse_args()

    main(args.task_id)