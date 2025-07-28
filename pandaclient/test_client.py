import argparse
from pandaclient.Client import getJobStatus, getJobStatus_new

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")
old_ret = getJobStatus(ids=panda_ids, verbose=True)
print ("------------------------------------------------------------")
new_ret = getJobStatus_new(ids=panda_ids, verbose=True)

print("old function returned {}".format(old_ret))
old_ret[1][0].print_attributes()
print("new function returned: {}".format(new_ret))
new_ret[1][0].print_attributes()

print("=============================================================")
panda_ids = []
old_ret = getJobStatus(ids=panda_ids, verbose=True)
print ("------------------------------------------------------------")
new_ret = getJobStatus_new(ids=panda_ids, verbose=True)

print("old function returned {}".format(old_ret))
old_ret[1][0].print_attributes()
print("new function returned: {}".format(new_ret))
new_ret[1][0].print_attributes()
