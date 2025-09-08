import argparse
from pandaclient.Client import putFile

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process panda IDs.")
parser.add_argument("panda_ids", nargs="*", type=int, help="List of panda IDs")
args = parser.parse_args()
panda_ids = args.panda_ids

print("=============================================================")

file_ret = putFile("/root/test/a.py", verbose=True)
print("putFile returned: {0}".format(file_ret))
