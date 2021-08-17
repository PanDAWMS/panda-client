import os
import json
import argparse
import tempfile
import subprocess

# make arg parse with option definitions
def make_arg_parse():
    usage = 'pcontainer [options]\n' \
            '  HowTo is available at https://twiki.cern.ch/twiki/bin/view/PanDA/PandaContainer'
    optP = argparse.ArgumentParser(usage=usage, conflict_handler="resolve")
    optP.add_argument('--version',action='store_const', const=True, dest='version', default=None,
                      help='Displays version')
    optP.add_argument('--loadJson', action='store', dest='loadJson', default=None,
                      help='Read command-line parameters from a json file which contains a dict of {parameter: value}')
    optP.add_argument('--dumpJson', action='store', dest='dumpJson', default=None,
                      help='Dump all command-line parameters and submission result such as returnCode, returnOut, ' 
                           'jediTaskID, etc to a json file')
    optP.add_argument('--cvmfs', action='store_const', const=True, dest='cvmfs', default=False,
                      help="Bind /cvmfs to the container, bool, default False")
    optP.add_argument('--noX509', action='store_const', const=True, dest='noX509', default=False,
                      help="Unset X509 environment in the container, bool, default False")
    optP.add_argument('--datadir', action='store', dest='datadir', default='',
                      help="Binds the job directory to datadir for I/O operations, string, default /ctrdata")
    optP.add_argument('--workdir', action='store', dest='workdir', default='',
                      help="chdir to workdir in the container, string, default /ctrdata")
    optP.add_argument('--debug', action='store_const', const=True, dest='debug', default=False,
                      help="Enable more verbose output from runcontainer, bool, default False")
    optP.add_argument('--containerImage', action='store', dest='containerImage', default=None,
                      help="Name of a container image")
    optP.add_argument('--useCentralRegistry', action='store_const', const=True,
                         dest='useCentralRegistry', default=False,
                         help="Use the central container registry when --containerImage is used")
    optP.add_argument('--excludedSite', action='append', dest='excludedSite', default=None,
                      help="list of sites which are not used for site section, e.g., ANALY_ABC,ANALY_XYZ")
    optP.add_argument('--site', action='store', dest='site', default=None,
                      help='Site name where jobs are sent. If omitted, jobs are automatically sent to sites '
                           'where input is available. A comma-separated list of sites can be specified '
                           '(e.g. siteA,siteB,siteC), so that best sites are chosen from the given site list')
    optP.add_argument('--architecture', action='store', dest='architecture', default='',
                      help="CPU and/or GPU requirements. #CPU_spec&GPU_spec where CPU or GPU spec can be "
                           "omitted. CPU_spec = architecture<-vendor<-instruction set>>, "
                           "GPU_spec = vendor<-model>. A wildcards can be used if there is no special "
                           "requirement for the attribute. E.g., #x86_64-*-avx2&nvidia to ask for x86_64 "
                           "CPU with avx2 support and nvidia GPU")
    optP.add_argument('--noSubmit', action='store_const', const=True, dest='noSubmit', default=None,
                      help=argparse.SUPPRESS)
    optP.add_argument('--outDS', action='store', dest='outDS', default=None,
                      help='Base name of the output dataset container. Actual output dataset name is defined '
                           'for each output file type')
    optP.add_argument('--outputs', action='store', dest='outputs', default=None,
                      help='Output file names. Comma separated. e.g., --outputs out1.dat,out2.txt. You can specify '
                           'a suffix for each output container like <datasetNameSuffix>:<outputFileName>. '
                           'e.g., --outputs AAA:out1.dat,BBB:out2.txt, so that output container names are outDS_AAA/ '
                           'and outDS_BBB/ instead of outDS_out1.dat/ and outDS_out2.txt/')
    optP.add_argument('--intrSrv', action='store_const', const=True, dest='intrSrv', default=None,
                      help=argparse.SUPPRESS)
    optP.add_argument('--exec', action='store', dest='exec', default=None,
                      help='execution string. e.g., --exec "./myscript arg1 arg2"')
    optP.add_argument('-v', '--verbose', action='store_const', const=True, dest='verbose', default=None,
                      help='Verbose')
    optP.add_argument('--priority', action='store', dest='priority', default=None, type=int,
                      help='Set priority of the task (1000 by default). The value must be between 900 and 1100. ' \
                           'Note that priorities of tasks are relevant only in ' \
                           "each user's share, i.e., your tasks cannot jump over other user's tasks " \
                           'even if you give higher priorities.')
    optP.add_argument('--useSandbox', action='store_const', const=True, dest='useSandbox', default=False,
                      help='To send files in the run directory to remote sites which are not sent out by default ' \
                           'when --containerImage is used')
    optP.add_argument("-3", action="store_true", dest="python3", default=False,
                      help="Use python3")

    return optP

# construct command-line options from arg parse
def construct_cli_options(options):
    options = vars(options)
    if 'loadJson' in options and options['loadJson'] is not None:
        newOpts = json.load(open(options['loadJson']))
    else:
        newOpts = dict()
    for key in options:
        val = options[key]
        if key in ['loadJson']:
            continue
        if key == 'architecture':
            key = 'cmtConfig'
        if key == 'cvmfs':
            key = 'ctrCvmfs'
        if key == 'noX509':
            key = 'ctrNoX509'
        if key == 'datadir':
            key = 'ctrDatadir'
        if key == 'workdir':
            key = 'ctrWorkdir'
        if key == 'debug':
            key = 'ctrDebug'
        if val is None:
            continue
        newOpts[key] = val
    newOpts['noBuild'] = True
    tmpLoadJson = tempfile.NamedTemporaryFile(delete=False, mode='w')
    json.dump(newOpts, tmpLoadJson)
    tmpLoadJson.close()
    return tmpLoadJson.name

# submit
def submit(options):
    tmpDumpJson = None
    if 'dumpJson' not in options:
        tmpDumpJson = tempfile.mkstemp()[-1]
        options['dumpJson'] = tmpDumpJson
    tmpLoadJson = construct_cli_options(options)
    com = ['prun']
    com += ['--loadJson={0}'.format(tmpLoadJson)]
    ret_val = subprocess.call(com)
    ret_dict = None
    if ret_val == 0:
        try:
            ret_dict = json.load(open(options['dumpJson']))
            if tmpDumpJson is not None:
                os.remove(tmpDumpJson)
        except Exception:
            pass
    os.remove(tmpLoadJson)
    return (ret_val, ret_dict)
