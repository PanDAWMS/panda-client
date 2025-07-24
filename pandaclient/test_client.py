from Client import getJobStatus, getJobStatus_new

panda_ids = [6744530186]
old_ret = getJobStatus(ids=panda_ids, verbose=True)
new_ret = getJobStatus_new(ids=panda_ids, verbose=True)

print(f"old function returned: {old_ret}")
print(f"new function returned: {new_ret}")

panda_ids = []
old_ret = getJobStatus(ids=panda_ids, verbose=True)
new_ret = getJobStatus_new(ids=panda_ids, verbose=True)

print(f"old function returned: {old_ret}")
print(f"new function returned: {new_ret}")
