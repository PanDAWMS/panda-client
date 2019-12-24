import os
import sys

from pandatools import pcontainer_core

os.environ['PANDA_EXEC_STRING'] = 'pcontainer'

optP = pcontainer_core.make_arg_parse()

options = optP.parse_args()

status, output = pcontainer_core.submit(options)
sys.exit(status)
