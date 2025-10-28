import argparse
from pandaclient.Client import getJobStatus, getFullJobStatus, killJobs

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")
job_status_ret_old = getJobStatus(ids=panda_ids, verbose=True)
job_full_status_ret_old = getFullJobStatus(ids=panda_ids, verbose=True)
kill_ret_old = killJobs(ids=panda_ids, verbose=True)

print("getJobStatus returned: {0}".format(job_status_ret_old))
print("getFullJobStatus returned: {0}".format(job_full_status_ret_old))
print("killJobs returned: {0}".format(kill_ret_old))