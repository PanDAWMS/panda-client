import sys
import re
import os
import shlex
import atexit

from pandatools.Group_argparse import GroupArgParser
from pandatools import PLogger
from pandatools import PandaToolsPkgInfo
from pandatools import MiscUtils
from pandatools import Client
from pandatools import PsubUtils
from pandatools import PrunScript

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

try:
    unicode
except Exception:
    unicode = str


# main
def main():
    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, 'pflow')

    usage = """pflow [options]
    """

    optP = GroupArgParser(usage=usage, conflict_handler="resolve")

    group_output = optP.add_group('output', 'output dataset/files')
    group_config = optP.add_group('config', 'single configuration file to set multiple options')
    group_submit = optP.add_group('submit', 'job submission/site/retry')
    group_expert = optP.add_group('expert', 'for experts/developers only')

    optP.add_helpGroup()

    group_config.add_argument('--version', action='store_const', const=True, dest='version', default=False,
                              help='Displays version')
    group_config.add_argument('-v', action='store_const', const=True, dest='verbose', default=False,
                              help='Verbose')
    group_output.add_argument('--cwl', action='store', dest='cwl', default=None, required=True,
                              help='Name of the main CWL file to describe the workflow')
    group_output.add_argument('--yaml', action='store', dest='yaml', default=None, required=True,
                              help='Name of the yaml file for workflow parameters')

    group_output.add_argument('--outDS', action='store', dest='outDS', default=None, required=True,
                              help='Name of the dataset for output and log files')
    group_output.add_argument('--official', action='store_const', const=True, dest='official', default=False,
                              help='Produce official dataset')
    group_submit.add_argument('--noSubmit', action='store_const', const=True, dest='noSubmit', default=False,
                              help="Dry-run")
    group_submit.add_argument("-3", action="store_true", dest="python3", default=False,
                              help="Use python3")
    group_submit.add_argument('--voms', action='store', dest='vomsRoles', default=None, type=str,
                              help="generate proxy with paticular roles. "
                                   "e.g., atlas:/atlas/ca/Role=production,atlas:/atlas/fr/Role=pilot")
    group_submit.add_argument('--noEmail', action='store_const', const=True, dest='noEmail', default=False,
                              help='Suppress email notification')

    group_expert.add_argument('--intrSrv', action='store_const', const=True, dest='intrSrv', default=False,
                              help="Please don't use this option. Only for developers to use the intr panda server")

    # get logger
    tmpLog = PLogger.getPandaLogger()

    options = optP.parse_args()
    option_names = set(vars(options).keys())

    if options.version:
        print("Version: %s" % PandaToolsPkgInfo.release_version)
        sys.exit(0)

    # check grid-proxy
    PsubUtils.check_proxy(options.verbose, options.vomsRoles)

    # check output name
    nickName = PsubUtils.getNickname()
    if not PsubUtils.checkOutDsName(options.outDS, options.official, nickName,
                                    verbose=options.verbose):
        tmpStr = "invalid output dataset name: %s" % options.outDS
        tmpLog.error(tmpStr)
        sys.exit(1)

    # use INTR server
    if options.intrSrv:
        Client.useIntrServer()

    # create tmp dir
    curDir = os.getcwd()
    tmpDir = os.path.join(curDir, MiscUtils.wrappedUuidGen())
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, del_command):
        del_command('rm -rf %s' % dir)

    atexit.register(_onExit, tmpDir, MiscUtils.commands_get_output)

    # sandbox
    if options.verbose:
        tmpLog.debug("making sandbox")
    archiveName = 'jobO.%s.tar.gz' % MiscUtils.wrappedUuidGen()
    archiveFullName = os.path.join(tmpDir, archiveName)
    extensions = ['cwl', 'yaml']
    find_opt = ' -o '.join(['-name "*.{0}"'.format(e) for e in extensions])
    tmpOut = MiscUtils.commands_get_output('find . {0} | tar cvfz {1} --files-from - '.format(find_opt, archiveFullName))

    if options.verbose:
        print(tmpOut + '\n')
        tmpLog.debug("checking sandbox")
        tmpOut = MiscUtils.commands_get_output('tar tvfz {0}'.format(archiveFullName))
        print(tmpOut + '\n')

    if not options.noSubmit:
        tmpLog.info("uploading workflow sandbox")
        os.chdir(tmpDir)
        status, out = Client.putFile(archiveName, options.verbose, useCacheSrv=True, reuseSandbox=True)
        os.chdir(curDir)
        if out.startswith('NewFileName:'):
            # found the same input sandbox to reuse
            archiveName = out.split(':')[-1]
        elif out != 'True':
            # failed
            print(out)
            tmpLog.error("Failed with %s" % status)
            sys.exit(1)

    matchURL = re.search("(http.*://[^/]+)/", Client.baseURLCSRVSSL)
    sourceURL = matchURL.group(1)

    # making task params with dummy exec
    prun_exec_str = '--exec __dummy_exec_str__ --containerImage __dummy_container__ --outDS {}'.format(options.outDS)
    if options.noSubmit:
        prun_exec_str += ' --noSubmit'
    if options.verbose:
        prun_exec_str += ' -v'
    taskParamMap = PrunScript.main(get_taskparams=True, ext_args=shlex.split(prun_exec_str))
    del taskParamMap['noInput']
    del taskParamMap['nEvents']
    del taskParamMap['nEventsPerJob']

    params = {'taskParams': taskParamMap,
              'sourceURL': sourceURL,
              'sandbox': archiveName,
              'workflowSpecFile': options.cwl,
              'workflowInputFile': options.yaml,
              'outDS': options.outDS
              }

    if options.noSubmit:
        if options.noSubmit:
            if options.verbose:
                tmpLog.debug("==== taskParams ====")
                tmpKeys = list(taskParamMap)
                tmpKeys.sort()
                for tmpKey in tmpKeys:
                    if tmpKey in ['taskParams']:
                        continue
                    print('%s : %s' % (tmpKey, taskParamMap[tmpKey]))
        sys.exit(0)

    tmpLog.info("submit {0}".format(options.outDS))
    tmpStat, tmpOut = Client.send_workflow_request(params, options.verbose)
    # result
    exitCode = None
    if tmpStat != 0:
        tmpStr = "task submission failed with {0}".format(tmpStat)
        tmpLog.error(tmpStr)
        exitCode = 1
    if tmpOut[0]:
        tmpStr = tmpOut[1]
        tmpLog.info(tmpStr)
    else:
        tmpStr = "task submission failed. {0}".format(tmpOut[1])
        tmpLog.error(tmpStr)
        exitCode = 1
    return exitCode
