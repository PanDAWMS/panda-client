import argparse
from pandaclient.Client import set_user_secret, set_user_secret_new, get_user_secrets, get_user_secrets_new

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")
set_secret_ret_old = set_user_secret('my_key', 'my_value')
get_secret_ret_old = get_user_secrets()

print("set_user_secret returned: {0}".format(set_secret_ret_old))
print("get_user_secrets returned: {0}".format(get_secret_ret_old))

print("=============================================================")
set_secret_ret_new = set_user_secret_new('my_key', 'my_value')
get_secret_ret_new = get_user_secrets()

print("set_user_secret_new returned: {0}".format(set_secret_ret_new))
print("get_user_secrets_new returned: {0}".format(get_secret_ret_new))