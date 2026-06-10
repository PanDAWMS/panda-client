import argparse

from pandaclient.Client import getFullJobStatus, getJobStatus, killJobs

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")
job_status_ret_old = getJobStatus(ids=panda_ids, verbose=True)
job_full_status_ret_old = getFullJobStatus(ids=panda_ids, verbose=True)
kill_ret_old = killJobs(ids=panda_ids, verbose=True)

print(f"getJobStatus returned: {job_status_ret_old}")
print(f"getFullJobStatus returned: {job_full_status_ret_old}")
print(f"killJobs returned: {kill_ret_old}")
