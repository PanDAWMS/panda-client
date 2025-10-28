import argparse
from pandaclient.Client import set_user_secret, get_user_secrets, get_events_status

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")
set_secret_ret_old = set_user_secret('my_key', 'my_value')
get_secret_ret_old = get_user_secrets()
events_status_ret_old = get_events_status([{"task_id": 4004040, "job_id": 4674379348}])

print("set_user_secret returned: {0}".format(set_secret_ret_old))
print("get_user_secrets returned: {0}".format(get_secret_ret_old))
print("get_events_status returned: {0}".format(events_status_ret_old))