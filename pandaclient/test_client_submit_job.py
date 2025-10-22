import argparse
from pandaclient.Client import getJobStatus, submitJobs

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

if not panda_ids:
    print("No panda IDs provided. Please provide at least one panda ID.")
    exit(1)

print("=============================================================")
jobs_old = getJobStatus(ids=panda_ids, verbose=True)
job_specs = jobs_old[1]

submit_old = submitJobs(job_specs)
print("submitJobs returned {0}".format(submit_old))