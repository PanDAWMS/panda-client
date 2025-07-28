from pandaclient.Client import getJobStatus, getJobStatus_new

print("=============================================================")
panda_ids = [6744530186]
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
