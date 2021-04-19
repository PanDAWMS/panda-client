import json
import sys
import re
import os
import copy
import atexit
import types

from pandatools.Group_argparse import GroupArgParser
from pandatools import PLogger
from pandatools import PandaToolsPkgInfo
from pandatools import MiscUtils
from pandatools import Client
from pandatools import PsubUtils

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

# tweak sys.argv
sys.argv.pop(0)
sys.argv.insert(0, 'phpo')

usage = """phpo [options]
"""

optP = GroupArgParser(usage=usage, conflict_handler="resolve")

group_input = optP.add_group('input', 'input dataset(s)/files/format')
group_output = optP.add_group('output', 'output dataset/files')
group_config = optP.add_group('config', 'single configuration file to set multiple options')
group_submit = optP.add_group('submit', 'job submission/site/retry')
group_expert = optP.add_group('expert', 'for experts/developers only')

optP.add_helpGroup()

group_config.add_argument('--version', action='store_const', const=True, dest='version', default=False,
                          help='Displays version')
group_config.add_argument('--loadJson', action='store', dest='loadJson', default=None,
                          help='Read task parameters from a json file. Some parameters can be overridden '
                               'by using command-line arguments')
group_config.add_argument('--dumpJson', action='store', dest='dumpJson', default=None,
                          help='Dump all command-line parameters and submission result '
                               'such as returnCode, returnOut, and jediTaskID to a json file')
group_config.add_argument('--nParallelEvaluation', action='store', dest='nParallelEvaluation', default=1, type=int,
                          help='The number of hyperparameter points being evaluated concurrently. 1 by default')
group_config.add_argument('--maxPoints', action='store', dest='maxPoints', default=10, type=int,
                          help='The max number of hyperparameter points to be evaluated in the entire search '
                               '(for each segment in segmented HPO). '
                               '10 by default')
group_config.add_argument('--maxEvaluationJobs', action='store', dest='maxEvaluationJobs', default=None, type=int,
                          help='The max number of evaluation jobs in the entire search '
                               '(for each segment in segmented HPO). 2*maxPoints by default. '
                               'The task is terminated when all hyperparameter points are evaluated or '
                               'the number of evaluation jobs reaches maxEvaluationJobs')
group_config.add_argument('--nPointsPerIteration', action='store', dest='nPointsPerIteration', default=2, type=int,
                          help='The max number of hyperparameter points generated in each iteration. 2 by default '
                               'Simply speaking, the steering container is executed maxPoints/nPointsPerIteration '
                               'times when minUnevaluatedPoints is 0. The number of new points is '
                               'nPointsPerIteration-minUnevaluatedPoints')
group_config.add_argument('--minUnevaluatedPoints', action='store', dest='minUnevaluatedPoints', default=None, type=int,
                          help='The next iteration is triggered to generate new hyperparameter points when the number '
                               'of unevaluated hyperparameter points goes below minUnevaluatedPoints. 0 by default')
group_config.add_argument('--steeringContainer', action='store', dest='steeringContainer', default=None,
                          help='The container image for steering run by docker')
group_config.add_argument('--steeringExec', action='store', dest='steeringExec', default=None,
                          help='Execution string for steering. If --steeringContainer is specified, the string '
                               'is executed inside of the container. Otherwise, the string is used as command-line '
                               'arguments for the docker command')
group_config.add_argument('--searchSpaceFile', action='store', dest='searchSpaceFile', default=None,
                          help='External json filename to define the search space which is described as a dictionary. '
                               'None by default. '
                               'If this option is used together with --segmentSpecFile the json file contains a list '
                               'of search space dictionaries. It is possible to contain only one search space '
                               'dictionary if all segments use the same search space. In this case the search space '
                               'dictionary is cloned for every segment')
group_config.add_argument('--evaluationContainer', action='store', dest='evaluationContainer', default=None,
                          help='The container image for evaluation')
group_config.add_argument('--evaluationExec', action='store', dest='evaluationExec', default=None,
                          help='Execution string to run evaluation in singularity')
group_config.add_argument('--evaluationInput', action='store', dest='evaluationInput', default='input.json',
                          help='Input filename for evaluation where a json-formatted hyperparameter point is placed. '
                               'input.json by default')
group_config.add_argument('--evaluationTrainingData', action='store', dest='evaluationTrainingData',
                          default='input_ds.json',
                          help='Input filename for evaluation where a json-formatted list of training data filenames '
                               'is placed. input_ds.json by default. Can be omitted if the payload directly fetches '
                               'the training data using wget or something')
group_config.add_argument('--evaluationOutput', action='store', dest='evaluationOutput', default='output.json',
                          help='Output filename of evaluation. output.json by default')
group_config.add_argument('--evaluationMeta', action='store', dest='evaluationMeta', default=None,
                          help='The name of metadata file produced by evaluation')
group_config.add_argument('--evaluationMetrics', action='store', dest='evaluationMetrics', default=None,
                          help='The name of metrics file produced by evaluation')
group_config.add_argument('--checkPointToSave', action='store', dest='checkPointToSave', default=None,
                          help='A comma-separated list of files and/or directories to be periodically saved ' \
                               'to a tarball for checkpointing. Note that those files and directories must be placed ' \
                               'in the working directory. None by default')
group_config.add_argument('--checkPointToLoad', action='store', dest='checkPointToLoad', default=None,
                          help='The name of the saved tarball for checkpointing. The tarball is given to ' \
                               'the evaluation container when the training is resumed, if this option is specified. '
                               'Otherwise, the tarball is automatically extracted in the working directories')
group_config.add_argument('--checkPointInterval', action='store', dest='checkPointInterval', default=None, type=int,
                          help='Frequency to check files for checkpointing in minute. '
                               '5 by default')
group_config.add_argument('--alrbArgs', action='store', dest='alrbArgs', default=None,
                          help='Additional arguments for ALRB to run the evaluation container. ' \
                               '"setupATLAS -c --help" shows available ALRB arguments. For example, ' \
                               '--alrbArgs "--nocvmfs --nohome" to skip mounting /cvmfs and $HOME. ' \
                               'This option is mainly for experts who know how the system and the container ' \
                               'communicates with each other and how additional ALRB arguments affect ' \
                               'the consequence')
group_config.add_argument('--architecture', action='store', dest='architecture', default='',
                          help="Architecture or flag of the processor to run the evaluation container image")
group_config.add_argument('--segmentSpecFile', action='store', dest='segmentSpecFile', default=None,
                          help='External json filename to define segments for segmented HPO which has one model '
                               'for each segment to be optimized independently. The file '
                               "contains a list of dictionaries {'name': arbitrary_unique_segment_name, "
                               "'files': [filename_used_for_the_segment_in_the_training_dataset, ... ]}. "
                               "It is possible to specify 'datasets' instead of 'files' in those dictionaries "
                               "if the training dataset has constituent datasets and "
                               "is partitioned with the constituent dataset boundaries. "
                               'None by default')
group_config.add_argument('-v', action='store_const', const=True, dest='verbose', default=False,
                          help='Verbose')

group_input.add_argument('--trainingDS', action='store', dest='trainingDS', default=None,
                         help='Name of training dataset')

group_output.add_argument('--outDS', action='store', dest='outDS', default=None,
                          help='Name of the dataset for output and log files')
group_output.add_argument('--official', action='store_const', const=True, dest='official', default=False,
                          help='Produce official dataset')

group_submit.add_argument('--site', action='store', dest='site', default=None,
                          help='The site name where jobs are sent. If omitted, jobs are automatically sent to sites '
                               'where input is available. A comma-separated list of sites can be specified '
                               '(e.g. siteA,siteB,siteC), so that best sites are chosen from the given site list')
group_submit.add_argument('--workingGroup', action='store', dest='workingGroup', default=None,
                          help="set working group")
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

jsonExecStr = ''
if options.loadJson is not None:
    with open(options.loadJson) as f:
        json_options = json.load(f)
        for k in json_options:
            if k in option_names:
                v = json_options[k]
                setattr(options, k, v)
                if v is True:
                    jsonExecStr += ' --{0}'.format(k)
                else:
                    if isinstance(v, types.StringType):
                        jsonExecStr += " --{0}='{1}'".format(k, v)
                    else:
                        jsonExecStr += " --{0}={1}".format(k, v)
            else:
                tmpLog.warning('ignore unknown option {0} in {1}'.format(k, options.loadJson))

if options.version:
    print("Version: %s" % PandaToolsPkgInfo.release_version)
    sys.exit(0)

# check grid-proxy
PsubUtils.check_proxy(options.verbose, options.vomsRoles)

# check options
# non_null_opts = ['outDS', 'evaluationContainer', 'evaluationExec', 'steeringContainer', 'steeringExec']
non_null_opts = ['outDS', 'evaluationContainer', 'evaluationExec', 'steeringExec']
for opt_name in non_null_opts:
    if getattr(options, opt_name) is None:
        tmpLog.error('--{0} is not specified'.format(opt_name))
        sys.exit(1)

if not options.outDS.endswith('/'):
    options.outDS += '/'

if options.maxEvaluationJobs is None:
    options.maxEvaluationJobs = 2 * options.maxPoints

# check output name
nickName = PsubUtils.getNickname()
if not PsubUtils.checkOutDsName(options.outDS, options.official, nickName,
                                verbose=options.verbose):
    tmpStr = "invalid output dataset name: %s" % options.outDS
    tmpLog.error(tmpStr)
    sys.exit(1)

# full execution string
fullExecString = PsubUtils.convSysArgv()
fullExecString += jsonExecStr

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
    tmpLog.debug("=== making sandbox ===")
archiveName = 'jobO.%s.tar' % MiscUtils.wrappedUuidGen()
archiveFullName = os.path.join(tmpDir, archiveName)
extensions = ['json', 'py', 'sh', 'yaml']
find_opt = ' -o '.join(['-name "*.{0}"'.format(e) for e in extensions])
tmpOut = MiscUtils.commands_get_output('find . {0} | tar cvfz {1} --files-from - '.format(find_opt, archiveFullName))

if options.verbose:
    print(tmpOut + '\n')
    tmpLog.debug("=== checking sandbox ===")
    tmpOut = MiscUtils.commands_get_output('tar tvfz {0}'.format(archiveFullName))
    print(tmpOut + '\n')

if not options.noSubmit:
    if options.verbose:
        tmpLog.debug("=== uploading sandbox===")
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

# making task params
taskParamMap = {}

taskParamMap['noInput'] = True
taskParamMap['nEventsPerJob'] = 1
taskParamMap['nEvents'] = options.nParallelEvaluation
taskParamMap['maxNumJobs'] = options.maxEvaluationJobs
taskParamMap['totNumJobs'] = options.maxPoints
taskParamMap['taskName'] = options.outDS
taskParamMap['vo'] = 'atlas'
taskParamMap['architecture'] = options.architecture
taskParamMap['hpoWorkflow'] = True
taskParamMap['transUses'] = ''
taskParamMap['transHome'] = ''
taskParamMap['transPath'] = 'http://pandaserver.cern.ch:25080/trf/user/runHPO-00-00-01'
taskParamMap['processingType'] = 'panda-client-{0}-jedi-hpo'.format(PandaToolsPkgInfo.release_version)
taskParamMap['prodSourceLabel'] = 'user'
taskParamMap['useLocalIO'] = 1
taskParamMap['cliParams'] = fullExecString
taskParamMap['skipScout'] = True
if options.noEmail:
    taskParamMap['noEmail'] = True
if options.workingGroup is not None:
    taskParamMap['workingGroup'] = options.workingGroup
taskParamMap['coreCount'] = 1
if options.site is not None:
    if ',' in options.site:
        taskParamMap['includedSite'] = PsubUtils.splitCommaConcatenatedItems([options.site])
    else:
        taskParamMap['site'] = options.site
if options.evaluationContainer is not None:
    taskParamMap['container_name'] = options.evaluationContainer

taskParamMap['multiStepExec'] = {'preprocess': {'command': '${TRF}',
                                                'args': '--preprocess ${TRF_ARGS}'},
                                 'postprocess': {'command': '${TRF}',
                                                 'args': '--postprocess ${TRF_ARGS}'},
                                 'containerOptions': {'containerExec':
                                                          'while [ ! -f __payload_in_sync_file__ ]; do sleep 5; done; '
                                                          'echo "=== cat exec script ==="; '
                                                          'cat __run_main_exec.sh; '
                                                          'echo; '
                                                          'echo "=== exec script ==="; '
                                                          '/bin/sh __run_main_exec.sh; '
                                                          'REAL_MAIN_RET_CODE=$?; '
                                                          'touch __payload_out_sync_file__; '
                                                          'exit $REAL_MAIN_RET_CODE '
                                     ,
                                                      'containerImage': options.evaluationContainer}
                                 }
if options.checkPointToSave is not None:
    taskParamMap['multiStepExec']['coprocess'] = {'command': '${TRF}',
                                                  'args': '--coprocess ${TRF_ARGS}'}

if options.alrbArgs is not None:
    taskParamMap['multiStepExec']['containerOptions']['execArgs'] = options.alrbArgs

logDatasetName = re.sub('/$', '.log/', options.outDS)

taskParamMap['log'] = {'dataset': logDatasetName,
                       'container': logDatasetName,
                       'type': 'template',
                       'param_type': 'log',
                       'value': '{0}.$JEDITASKID.${{SN}}.log.tgz'.format(logDatasetName[:-1])
                       }

taskParamMap['hpoRequestData'] = {'sandbox': options.steeringContainer,
                                  'executable': 'docker',
                                  'arguments': options.steeringExec,
                                  'output_json': 'output.json',
                                  'max_points': options.maxPoints,
                                  'num_points_per_generation': options.nPointsPerIteration,
                                  }
if options.minUnevaluatedPoints is not None:
    taskParamMap['hpoRequestData']['min_unevaluated_points'] = options.minUnevaluatedPoints

if options.searchSpaceFile is not None:
    with open(options.searchSpaceFile) as json_file:
        taskParamMap['hpoRequestData']['opt_space'] = json.load(json_file)

taskParamMap['jobParameters'] = [
    {'type': 'constant',
     'value': '-o {0} -j "" --inSampleFile {1}'.format(options.evaluationOutput,
                                                       options.evaluationInput)
     },
    {'type': 'constant',
     'value': '-a {0} --sourceURL {1}'.format(archiveName, sourceURL)
     },
]

taskParamMap['jobParameters'] += [
    {'type': 'constant',
     'value': '-p "',
     'padding': False,
     },
]
taskParamMap['jobParameters'] += PsubUtils.convertParamStrToJediParam(options.evaluationExec, {}, '',
                                                                      True, False,
                                                                      includeIO=False)

taskParamMap['jobParameters'] += [
    {'type': 'constant',
     'value': '"',
     },
]

if options.checkPointToSave is not None:
    taskParamMap['jobParameters'] += [
        {'type': 'constant',
         'value': '--checkPointToSave {0}'.format(options.checkPointToSave)
         },
    ]
    if options.checkPointInterval is not None:
        taskParamMap['jobParameters'] += [
            {'type': 'constant',
             'value': '--checkPointInterval {0}'.format(options.checkPointInterval)
             },
        ]

if options.checkPointToLoad is not None:
    taskParamMap['jobParameters'] += [
        {'type': 'constant',
         'value': '--checkPointToLoad {0}'.format(options.checkPointToLoad)
         },
    ]

if options.trainingDS is not None:
    taskParamMap['jobParameters'] += [
        {'type': 'constant',
         'value': '--writeInputToTxt IN_DATA:{0}'.format(options.evaluationTrainingData)
         },
        {'type': 'template',
         'param_type': 'input',
         'value': '-i "${IN_DATA/T}"',
         'dataset': options.trainingDS,
         'attributes': 'nosplit,repeat',
         },
        {'type': 'constant',
         'value': '--inMap "{\'IN_DATA\': ${IN_DATA/T}}"'
         },
    ]

if options.evaluationMeta is not None:
    taskParamMap['jobParameters'] += [
        {'type': 'constant',
         'value': '--outMetaFile={0}'.format(options.evaluationMeta),
         },
    ]

if options.segmentSpecFile is not None:
    taskParamMap['segmentedWork'] = True

    with open(options.segmentSpecFile) as f:
        # read segments
        segments = json.load(f)
        # search space
        if 'opt_space' in taskParamMap['hpoRequestData'] and \
                isinstance(taskParamMap['hpoRequestData']['opt_space'], dict):
            space = taskParamMap['hpoRequestData']['opt_space']
            taskParamMap['hpoRequestData']['opt_space'] = []
        else:
            space = None
        # set model ID to each segment
        for i in range(len(segments)):
            segments[i].update({'id': i})
            # make clone of search space if needed
            if space is not None:
                new_space = dict()
                new_space['model_id'] = i
                new_space['search_space'] = copy.deepcopy(space)
                taskParamMap['hpoRequestData']['opt_space'].append(new_space)
        taskParamMap['segmentSpecs'] = segments
        # multiply by num of segments
        taskParamMap['maxNumJobs'] *= len(segments)
        taskParamMap['totNumJobs'] *= len(segments)
        taskParamMap['hpoRequestData']['max_points'] *= len(segments)

    taskParamMap['jobParameters'] += [
        {'type': 'constant',
         'value': '--segmentID=${SEGMENT_ID}',
         },
    ]


if options.evaluationMetrics is not None:
    lfn = '$JEDITASKID.metrics.${SN}.tgz'
    if options.segmentSpecFile is not None:
        lfn = '${MIDDLENAME}.' + lfn
    taskParamMap['jobParameters'] += [
        {'type': 'template',
         'param_type': 'output',
         'value': lfn,
         'dataset': options.outDS,
         'hidden': True,
         },
        {'type': 'constant',
         'value': '--outMetricsFile=${{OUTPUT0}}^{0}'.format(options.evaluationMetrics),
         },
    ]

if options.noSubmit:
    if options.noSubmit:
        if options.verbose:
            tmpLog.debug("==== taskParams ====")
            tmpKeys = list(taskParamMap)
            tmpKeys.sort()
            for tmpKey in tmpKeys:
                print('%s : %s' % (tmpKey, taskParamMap[tmpKey]))
    sys.exit(0)

tmpLog.info("submit {0}".format(options.outDS))
tmpStat, tmpOut = Client.insertTaskParams(taskParamMap, options.verbose, True)
# result
taskID = None
exitCode = None
if tmpStat != 0:
    tmpStr = "task submission failed with {0}".format(tmpStat)
    tmpLog.error(tmpStr)
    exitCode = 1
if tmpOut[0] in [0, 3]:
    tmpStr = tmpOut[1]
    tmpLog.info(tmpStr)
    try:
        m = re.search('jediTaskID=(\d+)', tmpStr)
        taskID = int(m.group(1))
    except Exception:
        pass
else:
    tmpStr = "task submission failed. {0}".format(tmpOut[1])
    tmpLog.error(tmpStr)
    exitCode = 1

dumpItem = copy.deepcopy(vars(options))
dumpItem['returnCode'] = exitCode
dumpItem['returnOut'] = tmpStr
dumpItem['jediTaskID'] = taskID

# dump
if options.dumpJson is not None:
    with open(options.dumpJson, 'w') as f:
        json.dump(dumpItem, f)
