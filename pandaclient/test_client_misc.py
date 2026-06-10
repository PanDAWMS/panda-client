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

print(f"set_user_secret returned: {set_secret_ret_old}")
print(f"get_user_secrets returned: {get_secret_ret_old}")
print(f"get_events_status returned: {events_status_ret_old}")