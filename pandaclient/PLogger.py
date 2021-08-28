import os
import sys
import logging


rootLog = None

# set logger
def setLogger(tmpLog):
    global rootLog
    rootLog = tmpLog


# return logger
def getPandaLogger(use_stdout=True):
    # use root logger
    global rootLog
    if rootLog is None:
        rootLog = logging.getLogger('panda-client')
    # add StreamHandler if no handler
    if rootLog.handlers == []:
        rootLog.setLevel(logging.DEBUG)
        if use_stdout:
            console = logging.StreamHandler(sys.stdout)
        else:
            console = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(levelname)s : %(message)s')
        console.setFormatter(formatter)
        rootLog.addHandler(console)
    # return
    return rootLog


# disable logging
def disable_logging():
    global rootLog
    if not rootLog:
        rootLog = logging.getLogger('')
    rootLog.disabled = True
    # keep orignal stdout mainly for jupyter
    sys.__stdout__ = sys.stdout
    sys.stdout = open(os.devnull, 'w')


# enable logging
def enable_logging():
    global rootLog
    if rootLog:
        rootLog.disabled = False
    sys.stdout.close()
    sys.stdout = sys.__stdout__
