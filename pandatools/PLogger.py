import logging


rootLog = None

# set logger
def setLogger(tmpLog):
    global rootLog
    rootLog = tmpLog


# return logger
def getPandaLogger():
    # use root logger
    global rootLog
    if rootLog is None:
        rootLog = logging.getLogger('')
    # add StreamHandler if no handler
    if rootLog.handlers == []:
        rootLog.setLevel(logging.DEBUG)
        console = logging.StreamHandler()
        #formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        formatter = logging.Formatter('%(levelname)s : %(message)s')
        console.setFormatter(formatter)
        rootLog.addHandler(console)
    # return
    return rootLog
