import argparse
import atexit
import os
import re
import shutil
import sys
import time

from pandaclient.Group_argparse import get_parser

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

import copy
import json
import types

from pandaclient.MiscUtils import (
    commands_get_output,
    commands_get_status_output,
    parse_secondary_datasets_opt,
)

try:
    unicode
except Exception:
    unicode = str


# main
def main(get_taskparams=False, ext_args=None, dry_mode=False):
    # default cloud/site
    defaultCloud = None
    defaultSite = "AUTO"

    # error code
    EC_Config = 10
    EC_Post = 50
    EC_Archive = 60
    EC_Submit = 90

    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, "prun")

    usage = """prun [options]

      HowTo is available at https://panda-wms.readthedocs.io/en/latest/client/prun.html"""

    examples = """Examples:
      prun --exec "echo %IN > input.txt; root.exe; root -b -q macrotest.C" --athenaTag=22.0.0 --inDS ...
      prun --exec "cpptest %IN" --bexec "make" --athenaTag=22.0.0 --inDS ...
      prun --loadJson prunConfig.json   # read all prun options from one json file
    """

    removedOpts = [  # list of deprecated options w.r.t version 0.6.25
        "--buildInLastChunk",
        "--cloud",
        "--configJEM",
        "--crossSite",
        "--dbRunNumber",
        "--disableRebrokerage",
        "--enableJEM",
        "--eventPickNumSites",
        "--eventPickSkipDaTRI",
        "--eventPickStagedDS",
        "--individualOutDS",
        "--libDS",
        "--long",
        "--manaVer",
        "--myproxy",
        "--outputPath",
        "--provenanceID",
        "--removedDS",
        "--requireLFC",
        "--safetySize",
        "--seriesLabel",
        "--skipScan",
        "--transferredDS",
        "--useChirpServer",
        "--useContElementBoundary",
        "--useGOForOutput",
        "--useMana",
        "--useOldStyleOutput",
        "--useRucio",
        "--useShortLivedReplicas",
        "--useSiteGroup",
    ]

    optP = get_parser(usage=usage, conflict_handler="resolve")
    optP.set_examples(examples)

    # command-line parameters
    group_print = optP.add_group("print", "info print")
    group_prun = optP.add_group("prun", "about prun itself")
    group_config = optP.add_group("config", "single configuration file to set multiple options")
    group_input = optP.add_group("input", "input dataset(s)/files/format")
    group_output = optP.add_group("output", "output dataset/files")
    group_job = optP.add_group("job", "job running control on grid")
    group_build = optP.add_group("build", "build/compile the package and env setup")
    group_submit = optP.add_group("submit", "job submission/site/retry")
    group_evtFilter = optP.add_group("evtFilter", "event filter such as good run and event pick")
    group_expert = optP.add_group("expert", "for experts/developers only")

    usage_containerJob = """Visit the following wiki page for examples:
      https://twiki.cern.ch/twiki/bin/view/PanDA/PandaRun#Run_user_containers_jobs

    Please test the job interactively first prior to submitting to the grid.
    Check the following on how to test container job interactively:
      https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SingularityInAtlas
    """
    group_containerJob = optP.add_group("containerJob", "For container-based jobs", usage=usage_containerJob)

    optP.add_helpGroup(addHelp="Some options such as --inOutDsJson may SPAN several groups")

    group_prun.add_argument(
        "--version",
        action="store_const",
        const=True,
        dest="version",
        default=False,
        help="Displays version",
    )
    group_input.add_argument(
        "--inDS",
        action="store",
        dest="inDS",
        default="",
        help="Name of an input dataset or dataset container",
    )
    group_input.add_argument(
        "--notExpandInDS",
        action="store_const",
        const=True,
        dest="notExpandInDS",
        default=False,
        help="Allow jobs to use files across dataset boundaries in input dataset container",
    )
    group_input.add_argument(
        "--notExpandSecDSs",
        action="store_const",
        const=True,
        dest="notExpandSecDSs",
        default=False,
        help="Use files across dataset boundaries in secondary dataset containers",
    )
    group_input.add_argument(
        "--inDsTxt",
        action="store",
        dest="inDsTxt",
        default="",
        help="A text file which contains the list of datasets to run over. Newlines are replaced by commas and the result is set to --inDS. Lines starting with # are ignored",
    )
    group_input.add_argument(
        "--respectLB",
        action="store_const",
        const=True,
        dest="respectLB",
        default=False,
        help="To generate jobs repecting lumiblock boundaries",
    )
    group_output.add_argument(
        "--outDS",
        action="store",
        dest="outDS",
        default="",
        help="Name of an output dataset. OUTDS will contain all output files",
    )
    group_output.add_argument(
        "--outputs",
        action="store",
        dest="outputs",
        default="",
        help="Names of output files. Comma separated. e.g., --outputs out1.dat,out2.txt. You can specify a suffix for each output container like <datasetNameSuffix>:<outputFileName>. e.g., --outputs AAA:out1.dat,BBB:out2.txt. In this case output container names are outDS_AAA/ and outDS_BBB/ instead of outDS_out1.dat/ and outDS_out2.txt/",
    )
    group_output.add_argument(
        "--mergeOutput",
        action="store_const",
        const=True,
        dest="mergeOutput",
        default=False,
        help="merge output files",
    )
    group_output.add_argument(
        "--mergeLog",
        action="store_const",
        const=True,
        dest="mergeLog",
        default=False,
        help="merge log files. relevant only with --mergeOutput",
    )
    group_output.add_argument(
        "--destSE",
        action="store",
        dest="destSE",
        default="",
        help="Destination strorage element",
    )
    group_output.add_argument(
        "--noSeparateLog",
        action="store_const",
        const=True,
        dest="noSeparateLog",
        default=False,
        help="Set this option when jobs don't produce log files",
    )

    # the option is shared by both groups, group_input and group_output
    action = group_input.add_argument(
        "--inOutDsJson",
        action="store",
        dest="inOutDsJson",
        default="",
        help="A json file to specify input and output datasets for bulk submission. "
        "It contains a json dump of [{'inDS': a comma-concatenated input dataset names, "
        "'outDS': output dataset name}, ...]. "
        "When this option is used --bulkSubmission is automatically set internally.",
    )
    group_output.shareWithMe(action)

    group_evtFilter.add_argument(
        "--goodRunListXML",
        action="store",
        dest="goodRunListXML",
        default="",
        help="Good Run List XML which will be converted to datasets by AMI",
    )
    group_evtFilter.add_argument(
        "--goodRunListDataType",
        action="store",
        dest="goodRunDataType",
        default="",
        help="specify data type when converting Good Run List XML to datasets, e.g, AOD (default)",
    )
    group_evtFilter.add_argument(
        "--goodRunListProdStep",
        action="store",
        dest="goodRunProdStep",
        default="",
        help="specify production step when converting Good Run List to datasets, e.g, merge (default)",
    )
    action = group_evtFilter.add_argument(
        "--goodRunListDS",
        action="store",
        dest="goodRunListDS",
        default="",
        help='A comma-separated list of pattern strings. Datasets which are converted from Good Run List XML will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. If this option is omitted all datasets will be used',
    )
    group_input.shareWithMe(action)
    group_evtFilter.add_argument(
        "--eventPickEvtList",
        action="store",
        dest="eventPickEvtList",
        default="",
        help="a file name which contains a list of runs/events for event picking",
    )
    group_evtFilter.add_argument(
        "--eventPickDataType",
        action="store",
        dest="eventPickDataType",
        default="",
        help="type of data for event picking. one of AOD,ESD,RAW",
    )
    group_evtFilter.add_argument(
        "--ei_api",
        action="store",
        dest="ei_api",
        default="",
        help="flag to signalise mc in event picking",
    )
    group_evtFilter.add_argument(
        "--eventPickStreamName",
        action="store",
        dest="eventPickStreamName",
        default="",
        help="stream name for event picking. e.g., physics_CosmicCaloEM",
    )
    action = group_evtFilter.add_argument(
        "--eventPickDS",
        action="store",
        dest="eventPickDS",
        default="",
        help='A comma-separated list of pattern strings. Datasets which are converted from the run/event list will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. e.g., data\*',
    )
    group_input.shareWithMe(action)
    group_evtFilter.add_argument(
        "--eventPickAmiTag",
        action="store",
        dest="eventPickAmiTag",
        default="",
        help='AMI tag used to match TAG collections names. This option is required when you are interested in older data than the latest one. Either \ or "" is required when a wild-card is used. e.g., f2\*',
    )
    group_evtFilter.add_argument(
        "--eventPickWithGUID",
        action="store_const",
        const=True,
        dest="eventPickWithGUID",
        default=False,
        help="Using GUIDs together with run and event numbers in eventPickEvtList to skip event lookup",
    )

    group_submit.add_argument(
        "--express",
        action="store_const",
        const=True,
        dest="express",
        default=False,
        help="Send the job using express quota to have higher priority. The number of express subjobs in the queue and the total execution time used by express subjobs are limited (a few subjobs and several hours per day, respectively). This option is intended to be used for quick tests before large submission. Note that buildXYZ is not included in quota calculation. If this option is used when quota has already exceeded, the panda server will ignore the option so that subjobs have normal priorities. Also, if you submit 1 buildXYZ and N runXYZ subjobs when you only have quota of M (M < N),  only the first M runXYZ subjobs will have higher priorities",
    )
    group_print.add_argument(
        "--debugMode",
        action="store_const",
        const=True,
        dest="debugMode",
        default=False,
        help="Send the job with the debug mode on. If this option is specified the subjob will send stdout to the panda monitor every 5 min. The number of debug subjobs per user is limited. When this option is used and the quota has already exceeded, the panda server supresses the option so that subjobs will run without the debug mode. If you submit multiple subjobs in a single job, only the first subjob will set the debug mode on. Note that you can turn the debug mode on/off by using pbook after jobs are submitted",
    )
    group_output.add_argument(
        "--addNthFieldOfInDSToLFN",
        action="store",
        dest="addNthFieldOfInDSToLFN",
        default="",
        help="A middle name is added to LFNs of output files when they are produced from one dataset in the input container or input dataset list. The middle name is extracted from the dataset name. E.g., if --addNthFieldOfInDSToLFN=2 and the dataset name is data10_7TeV.00160387.physics_Muon..., 00160387 is extracted and LFN is something like user.hoge.TASKID.00160387.blah. Concatenate multiple field numbers with commas if necessary, e.g., --addNthFieldOfInDSToLFN=2,6.",
    )
    group_output.add_argument(
        "--addNthFieldOfInFileToLFN",
        action="store",
        dest="addNthFieldOfInFileToLFN",
        default="",
        help="A middle name is added to LFNs of output files similarly as --addNthFieldOfInDSToLFN, but strings are extracted from input file names",
    )
    group_build.add_argument(
        "--followLinks",
        action="store_const",
        const=True,
        dest="followLinks",
        default=False,
        help="Resolve symlinks to directories when building the input tarball. This option requires python2.6 or higher",
    )

    # I do not know which group "--useHomeDir" should go?
    group_build.add_argument(
        "--useHomeDir",
        action="store_const",
        const=True,
        dest="useHomeDir",
        default=False,
        help="execute prun just under the HOME dir",
    )
    group_build.add_argument(
        "--noBuild",
        action="store_const",
        const=True,
        dest="noBuild",
        default=False,
        help="Skip buildGen",
    )
    group_submit.add_argument(
        "--bulkSubmission",
        action="store_const",
        const=True,
        dest="bulkSubmission",
        default=False,
        help="Bulk submit tasks. When this option is used, --inOutDsJson is required while --inDS and --outDS are ignored. It is possible to use %%DATASET_IN and %%DATASET_OUT in --exec which are replaced with actual dataset names when tasks are submitted, and %%BULKSEQNUMBER which is replaced with a sequential number of tasks in the bulk submission",
    )
    group_build.add_argument(
        "--noCompile",
        action="store_const",
        const=True,
        dest="noCompile",
        default=False,
        help="Just upload a tarball in the build step to avoid the tighter size limit imposed by --noBuild. The tarball contains binaries compiled on your local computer, so that compilation is skipped in the build step on remote WN",
    )
    group_input.add_argument(
        "--secondaryDSs",
        action="store",
        dest="secondaryDSs",
        default="",
        help="List of secondary datasets when the job requires multiple inputs. "
        "Comma-separated strings in the format of StreamName:nFiles:DatasetName[:Pattern[:nSkipFiles[:FileNameList]]]. "
        "StreamName is the stream name used in --exec to expand to actual filenames. "
        "nFiles is the number of files per job by default, while it means the ratio to the number of primary "
        "files when --useNumFilesInSecDSsAsRatio is set. DatasetName is the dataset name. "
        "Pattern is used to filter files in the dataset. nSkipFiles is the number of files to skip in the dataset. "
        "FileNameList is a file listing names of files to be used in the dataset. ",
    )
    group_input.add_argument(
        "--reusableSecondary",
        action="store",
        dest="reusableSecondary",
        default="",
        help="A comma-separated list of secondary streams which reuse files when all files are used",
    )
    group_input.add_argument(
        "--useNumFilesInSecDSsAsRatio",
        action="store_const",
        const=True,
        dest="useNumFilesInSecDSsAsRatio",
        default=False,
        help="Set the option when the nFiles field in --secondaryDSs means the ratio to the number of primary files",
    )
    group_submit.add_argument(
        "--site",
        action="store",
        dest="site",
        default=defaultSite,
        help="Site name where jobs are sent. If omitted, jobs are automatically sent to sites where input is available. A comma-separated list of sites can be specified (e.g. siteA,siteB,siteC), so that best sites are chosen from the given site list. If AUTO is appended at the end of the list (e.g. siteA,siteB,siteC,AUTO), jobs are sent to any sites if input is not found in the previous sites",
    )
    group_input.add_argument(
        "--match",
        action="store",
        dest="match",
        default="",
        help="Use only files matching with given pattern",
    )
    group_input.add_argument(
        "--antiMatch",
        action="store",
        dest="antiMatch",
        default="",
        help="Skip files matching with given pattern",
    )
    group_input.add_argument(
        "--notSkipLog",
        action="store_const",
        const=True,
        dest="notSkipLog",
        default=False,
        help="Don't skip log files in input datasets (obsolete. use --useLogAsInput instead)",
    )
    group_submit.add_argument(
        "--memory",
        action="store",
        dest="memory",
        default=-1,
        type=int,
        help="Required memory size in MB per core. e.g., for 1GB per core --memory 1024",
    )
    group_submit.add_argument(
        "--fixedRamCount",
        action="store_const",
        const=True,
        dest="fixedRamCount",
        default=False,
        help="Use fixed memory size instead of estimated memory size",
    )
    group_submit.add_argument(
        "--nCore",
        action="store",
        dest="nCore",
        default=-1,
        type=int,
        help="The number of CPU cores. Note that the system distinguishes only nCore=1 and nCore>1. This means that even if you set nCore=2 jobs can go to sites with nCore=8 and your application must use the 8 cores there. The number of available cores is defined in an environment variable, $ATHENA_PROC_NUMBER, on WNs. Your application must check the env variable when starting up to dynamically change the number of cores",
    )
    group_submit.add_argument(
        "--maxCpuCount",
        action="store",
        dest="maxCpuCount",
        default=0,
        type=int,
        help=argparse.SUPPRESS,
    )
    group_expert.add_argument(
        "--noLoopingCheck",
        action="store_const",
        const=True,
        dest="noLoopingCheck",
        default=False,
        help="Disable looping job check",
    )
    group_submit.add_argument(
        "--useDirectIOSites",
        action="store_const",
        const=True,
        dest="useDirectIOSites",
        default=False,
        help="Use only sites which use directIO to read input files",
    )
    group_submit.add_argument(
        "--outDiskCount",
        action="store",
        dest="outDiskCount",
        default=None,
        type=int,
        help="Expected output size in kB per 1 MB of input. The system automatically calculates this "
        "value using successful jobs and the value contains a safety offset (100kB). "
        "Use this option to disable it when jobs cannot have enough input files "
        "due to the offset",
    )

    group_output.add_argument(
        "--official",
        action="store_const",
        const=True,
        dest="official",
        default=False,
        help="Produce official dataset",
    )
    group_output.add_argument(
        "--unlimitNumOutputs",
        action="store_const",
        const=True,
        dest="unlimitNumOutputs",
        default=False,
        help="Remove the limit on the number of outputs. Note that having too many outputs per job causes a severe load on the system. You may be banned if you carelessly use this option",
    )
    group_output.add_argument(
        "--descriptionInLFN",
        action="store",
        dest="descriptionInLFN",
        default="",
        help="LFN is user.nickname.jobsetID.something (e.g. user.harumaki.12345.AOD._00001.pool) by default. This option allows users to put a description string into LFN. i.e., user.nickname.jobsetID.description.something",
    )
    group_build.add_argument(
        "--useRootCore",
        action="store_const",
        const=True,
        dest="useRootCore",
        default=False,
        help="Use RootCore. See PandaRun wiki page for detail",
    )
    group_build.add_argument(
        "--useAthenaPackages",
        action="store_const",
        const=True,
        dest="useAthenaPackages",
        default=False,
        help="Use Athena packages. See PandaRun wiki page for detail",
    )
    group_build.add_argument(
        "--gluePackages",
        action="store",
        dest="gluePackages",
        default="",
        help="list of glue packages which pathena cannot find due to empty i686-slc4-gcc34-opt. e.g., External/AtlasHepMC,External/Lhapdf",
    )
    group_input.add_argument(
        "--nFiles",
        action="store",
        dest="nFiles",
        default=0,
        type=int,
        help="Use a limited number of files in the input dataset",
    )
    group_input.add_argument(
        "--nSkipFiles",
        action="store",
        dest="nSkipFiles",
        default=0,
        type=int,
        help="Skip N files in the input dataset",
    )
    group_job.add_argument(
        "--exec",
        action="store",
        dest="jobParams",
        default="",
        help='execution string. e.g., --exec "./myscript arg1 arg2"',
    )
    group_output.add_argument(
        "--execWithRealFileNames",
        action="store_const",
        const=True,
        dest="execWithRealFileNames",
        default=False,
        help="Run the execution string with real output filenames",
    )
    group_job.add_argument(
        "--nFilesPerJob",
        action="store",
        dest="nFilesPerJob",
        default=None,
        type=int,
        help="Number of files on which each sub-job runs (default 50). Note that this is the number of files per sub-job in the primary dataset even if --secondaryDSs is used",
    )
    group_job.add_argument(
        "--nJobs",
        action="store",
        dest="nJobs",
        default=-1,
        type=int,
        help="Maximum number of sub-jobs. If the number of input files (N_in) is less than nJobs*nFilesPerJob, only N_in/nFilesPerJob sub-jobs will be instantiated",
    )
    group_job.add_argument(
        "--nEvents",
        action="store",
        dest="nEvents",
        default=-1,
        type=int,
        help="The total number of events to be processed. This option is considered only when either --inDS or --pfnList is not used",
    )
    group_job.add_argument(
        "--nEventsPerJob",
        action="store",
        dest="nEventsPerJob",
        default=-1,
        type=int,
        help="Number of events per subjob. This is used mainly for job splitting. If you set nEventsPerFile, the total number of subjobs is nEventsPerFile*nFiles/nEventsPerJob. Otherwise, it gets from rucio the number of events in each input file and subjobs are created accordingly. Note that you need to explicitly specify in --exec some parameters like %%MAXEVENTS, %%SKIPEVENTS and %%FIRSTEVENT and your application needs to process only an event chunk accordingly, to avoid subjobs processing the same events. All parameters descibed in https://twiki.cern.ch/twiki/bin/view/PanDA/PandaAthena#example_8_How_to_run_production are available",
    )
    action = group_job.add_argument(
        "--nEventsPerFile",
        action="store",
        dest="nEventsPerFile",
        default=0,
        type=int,
        help="Number of events per file",
    )
    group_input.shareWithMe(action)
    group_job.add_argument(
        "--nEventsPerChunk",
        action="store",
        dest="nEventsPerChunk",
        default=-1,
        type=int,
        help="Set granuarity to split events. The number of events per job is multiples of nEventsPerChunk. This option is considered only when --nEvents is used but --nJobs is not used. If this option is not set, nEvents/20 is used as nEventsPerChunk",
    )
    group_job.add_argument(
        "--nGBPerJob",
        action="store",
        dest="nGBPerJob",
        default=-1,
        help="Instantiate one sub job per NGBPERJOB GB of input files. --nGBPerJob=MAX sets the size to the default maximum value",
    )
    group_build.add_argument(
        "--maxFileSize",
        action="store",
        dest="maxFileSize",
        default=1024 * 1024,
        type=int,
        help="Maximum size of files to be sent to WNs (default 1024*1024B)",
    )
    group_build.add_argument(
        "--athenaTag",
        action="store",
        dest="athenaTag",
        default="",
        help="Tags to setup Athena on remote WNs, e.g., --athenaTag=AtlasProduction,14.2.24.3",
    )
    group_build.add_argument(
        "--rootVer",
        action="store",
        dest="rootVer",
        default="",
        help="Specify a ROOT version which is not included in Athena, e.g., --rootVer=5.28/00",
    )
    group_build.add_argument(
        "--workDir",
        action="store",
        dest="workDir",
        default=".",
        help="All files under WORKDIR will be transfered to WNs (default=./)",
    )
    group_build.add_argument(
        "--extFile",
        action="store",
        dest="extFile",
        default="",
        help="root or large files under WORKDIR are not sent to WNs by default. If you want to send some skipped files, specify their names, e.g., data.root,data.tgz",
    )
    group_build.add_argument(
        "--excludeFile",
        action="store",
        dest="excludeFile",
        default="",
        help='specify a comma-separated string to exclude files and/or directories when gathering files in local working area. Either \ or "" is required when a wildcard is used. e.g., doc,\*.C',
    )
    group_input.add_argument(
        "--inputFileList",
        action="store",
        dest="inputFileListName",
        default="",
        help="A local file which specifies names of files to be used in the input dataset. " "One filename per line in the the local file",
    )
    action = group_job.add_argument(
        "--allowNoOutput",
        action="store",
        dest="allowNoOutput",
        default="",
        help="A comma-separated list of regexp patterns. Output files are allowed not to be produced if their filenames match with one of regexp patterns. Jobs go to finish even if they are not produced on WN",
    )
    group_output.shareWithMe(action)
    group_submit.add_argument(
        "--excludedSite",
        action="append",
        dest="excludedSite",
        default=[],
        help="A comma-separated list of sites which are not used for site section, "
        "e.g., ABC,OPQ*,XYZ which excludes ABC, XYZ, and OPQ<blah> due to the wildcard",
    )
    group_input.add_argument(
        "--useLogAsInput",
        action="store_const",
        const=True,
        dest="useLogAsInput",
        default=False,
        help="log.tgz files in inDS are ignored by default. This option allows log files to be used as input",
    )
    group_submit.add_argument(
        "--noSubmit",
        action="store_const",
        const=True,
        dest="noSubmit",
        default=False,
        help="Don't submit jobs",
    )
    group_submit.add_argument(
        "--prodSourceLabel",
        action="store",
        dest="prodSourceLabel",
        default="",
        help="set prodSourceLabel",
    )
    group_submit.add_argument(
        "--processingType",
        action="store",
        dest="processingType",
        default="prun",
        help="set processingType",
    )
    group_submit.add_argument(
        "--workingGroup",
        action="store",
        dest="workingGroup",
        default=None,
        help="set workingGroup",
    )
    group_build.add_argument(
        "--tmpDir",
        action="store",
        dest="tmpDir",
        default="",
        help="Temporary directory where an archive file is created",
    )
    group_build.add_argument(
        "--voms",
        action="store",
        dest="vomsRoles",
        default=None,
        help="generate proxy with paticular roles. e.g., atlas:/atlas/ca/Role=production,atlas:/atlas/fr/Role=pilot",
    )
    group_build.add_argument("--vo", action="store", dest="vo", default=None, help="virtual orgnaiztion name")
    group_submit.add_argument(
        "--noEmail",
        action="store_const",
        const=True,
        dest="noEmail",
        default=False,
        help="Suppress email notification",
    )
    group_prun.add_argument(
        "--update",
        action="store_const",
        const=True,
        dest="update",
        default=False,
        help="Update panda-client to the latest version",
    )
    group_output.add_argument(
        "--spaceToken",
        action="store",
        dest="spaceToken",
        default="",
        help="spacetoken for outputs. e.g., ATLASLOCALGROUPDISK",
    )
    group_expert.add_argument(
        "--expertOnly_skipScout",
        action="store_const",
        const=True,
        dest="skipScout",
        default=False,
        help=argparse.SUPPRESS,
    )
    group_expert.add_argument(
        "--msgDriven",
        action="store_const",
        const=True,
        dest="msgDriven",
        default=False,
        help=argparse.SUPPRESS,
    )
    group_job.add_argument(
        "--respectSplitRule",
        action="store_const",
        const=True,
        dest="respectSplitRule",
        default=False,
        help="force scout jobs to follow split rules like nGBPerJob",
    )
    group_job.add_argument(
        "--nGBPerMergeJob",
        action="store",
        dest="nGBPerMergeJob",
        default="MAX",
        help="Instantiate one merge job per NGBPERMERGEJOB GB of pre-merged files",
    )
    group_expert.add_argument(
        "--devSrv",
        action="store_const",
        const=True,
        dest="devSrv",
        default=False,
        help="Please don't use this option. Only for developers to use the dev panda server",
    )
    group_expert.add_argument(
        "--intrSrv",
        action="store_const",
        const=True,
        dest="intrSrv",
        default=False,
        help="Please don't use this option. Only for developers to use the intr panda server",
    )
    group_expert.add_argument(
        "--persistentFile",
        action="store",
        dest="persistentFile",
        default="",
        help="Please don't use this option. Only for junction steps " "to keep persistent information in workflows",
    )
    group_build.add_argument(
        "--outTarBall",
        action="store",
        dest="outTarBall",
        default="",
        help="Save a gzipped tarball of local files which is the input to buildXYZ",
    )
    group_build.add_argument(
        "--inTarBall",
        action="store",
        dest="inTarBall",
        default="",
        help="Use a gzipped tarball of local files as input to buildXYZ. Generall the tarball is created by using --outTarBall",
    )
    group_build.add_argument(
        "--bexec",
        action="store",
        dest="bexec",
        default="",
        help='execution string for build stage. e.g., --bexec "make"',
    )
    group_submit.add_argument(
        "--disableAutoRetry",
        action="store_const",
        const=True,
        dest="disableAutoRetry",
        default=False,
        help="disable automatic job retry on the server side",
    )
    group_job.add_argument(
        "--maxNFilesPerJob",
        action="store",
        dest="maxNFilesPerJob",
        default=200,
        type=int,
        help="The maximum number of files per job is 200 by default since too many input files result in a too long command-line argument on WN which crashes the job. This option relax the limit. In many cases it is better to use this option together with --writeInputToTxt",
    )
    group_input.add_argument(
        "--writeInputToTxt",
        action="store",
        dest="writeInputToTxt",
        default="",
        help="Write the input file list to a file so that your application gets the list from the file instead of stdin. The argument is a comma separated list of StreamName:FileName. e.g., IN:input1.txt,IN2:input2.txt",
    )
    group_build.add_argument(
        "--dbRelease",
        action="store",
        dest="dbRelease",
        default="",
        help="DBRelease or CDRelease (DatasetName:FileName). e.g., ddo.000001.Atlas.Ideal.DBRelease.v050101:DBRelease-5.1.1.tar.gz. If --dbRelease=LATEST, the latest DBRelease is used. Most likely the --useAthenaPackages or --athenaTag option is required to setup Athena runtime on WN",
    )
    group_build.add_argument(
        "--notExpandDBR",
        action="store_const",
        const=True,
        dest="notExpandDBR",
        default=False,
        help="By default, DBRelease.tar.gz is expanded on WN and gets deleted after changing environment variables accordingly. If you need tar.gz, use this option",
    )
    action = group_job.add_argument(
        "--mergeScript",
        action="store",
        dest="mergeScript",
        default="",
        help="Specify user-defied script execution string for output merging",
    )
    group_output.shareWithMe(action)
    group_print.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        const=True,
        dest="verbose",
        default=False,
        help="Verbose",
    )
    group_input.add_argument(
        "--pfnList",
        action="store",
        dest="pfnList",
        default="",
        help="Name of file which contains a list of input PFNs. Those files can be un-registered in DDM",
    )
    group_build.add_argument(
        "--cmtConfig",
        action="store",
        dest="cmtConfig",
        default=None,
        help="CMTCONFIG is extracted from local environment variables when tasks are submitted, "
        "to set up the same environment on remote worker-nodes. "
        "This option allows to set up another CMTCONFIG "
        "remotely. e.g., --cmtConfig x86_64-slc5-gcc43-opt.",
    )
    group_config.add_argument(
        "--loadXML",
        action="store",
        dest="loadXML",
        default=None,
        help="Expert mode: load complete submission configuration from an XML file ",
    )
    group_config.add_argument(
        "--loadJson",
        action="store",
        dest="loadJson",
        default=None,
        help="Read command-line parameters from a json file which contains a dict of {parameter: value}",
    )
    group_config.add_argument(
        "--dumpJson",
        action="store",
        dest="dumpJson",
        default=None,
        help="Dump all command-line parameters and submission result such as returnCode, returnOut, jediTaskID, and bulkSeqNumber if --bulkSubmission is used, to a json file",
    )
    group_config.add_argument(
        "--dumpTaskParams",
        action="store",
        dest="dumpTaskParams",
        default=None,
        help="Dump task parameters to a json file",
    )
    group_config.add_argument(
        "--parentTaskID",
        "--parentTaskID",
        action="store",
        dest="parentTaskID",
        default=None,
        type=int,
        help="Set taskID of the paranet task to execute the task while the parent is still running",
    )
    group_config.add_argument(
        "--useSecrets",
        action="store_const",
        const=True,
        dest="useSecrets",
        default=False,
        help="Use secrets",
    )
    group_input.add_argument(
        "--forceStaged",
        action="store_const",
        const=True,
        dest="forceStaged",
        default=False,
        help="Force files from primary DS to be staged to local disk, even if direct-access is possible",
    )
    group_input.add_argument(
        "--forceStagedSecondary",
        action="store_const",
        const=True,
        dest="forceStagedSecondary",
        default=False,
        help="Force files from secondary DSs to be staged to local disk, even if direct-access is possible",
    )
    group_input.add_argument(
        "--avoidVP",
        action="store_const",
        const=True,
        dest="avoidVP",
        default=False,
        help="Not to use sites where virtual placement is enabled",
    )
    group_expert.add_argument(
        "--queueData",
        action="store",
        dest="queueData",
        default="",
        help="Please don't use this option. Only for developers",
    )

    group_submit.add_argument(
        "--useNewCode",
        action="store_const",
        const=True,
        dest="useNewCode",
        default=False,
        help="When task are resubmitted with the same outDS, the original souce code is used to re-run on failed/unprocessed files. This option uploads new source code so that jobs will run with new binaries",
    )
    group_output.add_argument(
        "--allowTaskDuplication",
        action="store_const",
        const=True,
        dest="allowTaskDuplication",
        default=False,
        help="As a general rule each task has a unique outDS and history of file usage is recorded per task. This option allows multiple tasks to contribute to the same outDS. Typically useful to submit a new task with the outDS which was used by another broken task. Use this option very carefully at your own risk, since file duplication happens when the second task runs on the same input which the first task successfully processed",
    )
    group_input.add_argument(
        "--skipFilesUsedBy",
        action="store",
        dest="skipFilesUsedBy",
        default="",
        help="A comma-separated list of TaskIDs. Files used by those tasks are skipped when running a new task",
    )
    group_submit.add_argument(
        "--maxAttempt",
        action="store",
        dest="maxAttempt",
        default=-1,
        type=int,
        help="Maximum number of reattempts for each job (3 by default and not larger than 50)",
    )
    group_submit.add_argument(
        "-y",
        action="store_true",
        dest="is_confirmed",
        default=False,
        help="Answer yes for all questions",
    )
    group_containerJob.add_argument(
        "--containerImage",
        action="store",
        dest="containerImage",
        default="",
        help="Name of a container image",
    )
    group_containerJob.add_argument(
        "--architecture",
        action="store",
        dest="architecture",
        default="",
        help="Base OS platform, CPU, and/or GPU requirements. "
        "The format is @base_platform#CPU_spec&GPU_spec "
        "where base platform, CPU, or GPU spec can be omitted. "
        "If base platform is not specified it is automatically taken from "
        "$ALRB_USER_PLATFORM. "
        "CPU_spec = architecture<-vendor<-instruction set>>, "
        "GPU_spec = vendor<-model>. A wildcards can be used if there is no special "
        "requirement for the attribute. E.g., #x86_64-*-avx2&nvidia to ask for x86_64 "
        "CPU with avx2 support and nvidia GPU",
    )
    group_containerJob.add_argument(
        "--ctrCvmfs",
        action="store_const",
        const=True,
        dest="ctrCvmfs",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Bind /cvmfs to the container, bool, default False")
    group_containerJob.add_argument(
        "--ctrNoX509",
        action="store_const",
        const=True,
        dest="ctrNoX509",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Unset X509 environment in the container, bool, default False")
    group_containerJob.add_argument(
        "--ctrDatadir",
        action="store",
        dest="ctrDatadir",
        default="",
        help=argparse.SUPPRESS,
    )
    # help="Binds the job directory to datadir for I/O operations, string, default /ctrdata")
    group_containerJob.add_argument(
        "--ctrWorkdir",
        action="store",
        dest="ctrWorkdir",
        default="",
        help=argparse.SUPPRESS,
    )
    # help="chdir to workdir in the container, string, default /ctrdata")
    group_containerJob.add_argument(
        "--ctrDebug",
        action="store_const",
        const=True,
        dest="ctrDebug",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Enable more verbose output from runcontainer, bool, default False")
    group_containerJob.add_argument(
        "--useSandbox",
        action="store_const",
        const=True,
        dest="useSandbox",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help='To send files in the run directory to remote sites which are not sent out by default ' \
    #'when --containerImage is used')
    group_containerJob.add_argument(
        "--useCentralRegistry",
        action="store_const",
        const=True,
        dest="useCentralRegistry",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Use the central container registry when --containerImage is used")
    group_containerJob.add_argument(
        "--notUseCentralRegistry",
        action="store_const",
        const=True,
        dest="notUseCentralRegistry",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Not use the central container registry when --containerImage is used")
    group_containerJob.add_argument(
        "--alrb",
        action="store_const",
        const=True,
        dest="alrb",
        default=True,
        help="Use ALRB for container execution",
    )
    group_containerJob.add_argument(
        "--wrapExecInContainer",
        action="store_const",
        const=False,
        dest="directExecInContainer",
        default=True,
        help="Execute the --exec string through runGen in the container",
    )
    group_containerJob.add_argument(
        "--alrbArgs",
        action="store",
        dest="alrbArgs",
        default=None,
        help="Additional arguments for ALRB to run the container. "
        '"setupATLAS -c --help" shows available ALRB arguments. For example, '
        '--alrbArgs "--nocvmfs	--nohome" to skip mounting /cvmfs and $HOME. '
        "This option is mainly for experts who know how the system and the container "
        "communicates with each other and how additional ALRB arguments affect "
        "the consequence",
    )
    group_containerJob.add_argument(
        "--oldContMode",
        action="store_const",
        const=True,
        dest="oldContMode",
        default=False,
        help="Use runcontainer for container execution. Note that this option will be "
        "deleted near future. Try the new ARLB scheme as soon as possible and report "
        "if there is a problem",
    )
    group_submit.add_argument(
        "--priority",
        action="store",
        dest="priority",
        default=None,
        type=int,
        help="Set priority of the task (1000 by default). The value must be between 900 and 1100. "
        "Note that priorities of tasks are relevant only in "
        "each user's share, i.e., your tasks cannot jump over other user's tasks "
        "even if you give higher priorities.",
    )
    group_submit.add_argument(
        "--osMatching",
        action="store_const",
        const=True,
        dest="osMatching",
        default=False,
        help="To let the brokerage choose sites which have the same OS as the local machine has.",
    )
    group_job.add_argument(
        "--cpuTimePerEvent",
        action="store",
        dest="cpuTimePerEvent",
        default=-1,
        type=int,
        help="Expected HS06 seconds per event (~= 10 * the expected duration per event in seconds)",
    )
    group_job.add_argument(
        "--fixedCpuTime",
        action="store_const",
        const=True,
        dest="fixedCpuTime",
        default=False,
        help="Use fixed cpuTime instead of estimated cpuTime",
    )
    group_job.add_argument(
        "--maxWalltime",
        action="store",
        dest="maxWalltime",
        default=0,
        type=int,
        help="Max walltime for each job in hours. Note that this option works only " "when the nevents metadata of input files are available in rucio",
    )
    group_build.add_argument("-3", action="store_true", dest="python3", default=False, help="Use python3")

    from pandaclient import MiscUtils

    # parse options
    # check against the removed options first
    for arg in sys.argv[1:]:
        optName = arg.split("=", 1)[0]
        if optName in removedOpts:
            print("!!Warning!! option %s has been deprecated, pls dont use anymore\n" % optName)
            sys.argv.remove(arg)

    # options, args = optP.parse_known_args()
    options = optP.parse_args(ext_args)

    if options.verbose:
        print(options)
        print("")
    # load json
    jsonExecStr = ""
    if options.loadJson is not None:
        loadOpts = MiscUtils.decodeJSON(options.loadJson)
        for k in loadOpts:
            v = loadOpts[k]
            if isinstance(v, (str, unicode)):
                try:
                    v = int(v)
                except Exception:
                    pass
            origK = k
            if k == "exec":
                k = "jobParams"
            if not hasattr(options, k):
                print("ERROR: unknown parameter {0} in {1}".format(k, options.loadJson))
                sys.exit(0)
            else:
                setattr(options, k, v)
            if v is True:
                jsonExecStr += " --{0}".format(origK)
            else:
                if isinstance(v, (str, unicode)):
                    jsonExecStr += " --{0}='{1}'".format(origK, v)
                else:
                    jsonExecStr += " --{0}={1}".format(origK, v)
        if options.verbose:
            print("options after loading json")
            print(options)
            print("")

    # display version
    from pandaclient import PandaToolsPkgInfo

    if options.version:
        print("Version: %s" % PandaToolsPkgInfo.release_version)
        sys.exit(0)

    from pandaclient import AthenaUtils, Client, PLogger, PsubUtils

    # update panda-client
    if options.update:
        res = PsubUtils.updatePackage(options.verbose)
        if res:
            sys.exit(0)
        sys.exit(1)

    # full execution string
    fullExecString = PsubUtils.convSysArgv()
    fullExecString += jsonExecStr

    # set dummy CMTSITE
    if "CMTSITE" not in os.environ:
        os.environ["CMTSITE"] = ""

    # get logger
    tmpLog = PLogger.getPandaLogger()

    # use dev server
    if options.devSrv:
        Client.useDevServer()

    # use INTR server
    if options.intrSrv:
        Client.useIntrServer()

    # noCompile uses noBuild stuff
    if options.noCompile:
        options.noBuild = True

    # not skip log files in inDS
    if options.notSkipLog:
        options.useLogAsInput = True

    # old container execution mode
    if options.oldContMode:
        options.alrb = False

    # use runGen
    if options.useAthenaPackages and options.alrb:
        options.directExecInContainer = True

    # container stuff
    if options.containerImage != "":
        options.noBuild = True
        if options.alrb:
            options.useSandbox = True
        if not options.useSandbox:
            tmpLog.warning(
                "Files in the run directory are not sent out by default when --containerImage is used. "
                "Please use --useSandbox if you need those files on the grid."
            )

    # files to be deleted
    delFilesOnExit = []

    # load submission configuration from xml file (if provided)
    xconfig = None
    if options.loadXML is not None:
        from pandaclient import ParseJobXML

        xconfig = ParseJobXML.dom_parser(options.loadXML)
        tmpLog.info("dump XML config")
        xconfig.dump(options.verbose)
        if options.outDS == "":
            options.outDS = xconfig.outDS()
        options.outputs = "all"
        options.jobParams = "${XML_EXESTR}"
        options.inDS = xconfig.inDS()
        # check XML
        try:
            xconfig.files_in_DS(options.inDS)
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            print(errvalue)
            tmpLog.error("verification of XML failed")
            sys.exit(EC_Config)
        # inDS match and secondaryDS filter will be determined later from xconfig
        options.match = ""
        options.secondaryDSs = xconfig.secondaryDSs_config(filter=False)
        # read XML
        xmlFH = open(options.loadXML)
        options.loadXML = xmlFH.read()
        xmlFH.close()

    # save current dir
    curDir = os.path.realpath(os.getcwd())

    # remove whitespaces
    if options.outputs != "":
        options.outputs = re.sub(" ", "", options.outputs)

    # warning for PQ
    PsubUtils.get_warning_for_pq(options.site, options.excludedSite, tmpLog)

    # warning for memory
    is_confirmed = PsubUtils.get_warning_for_memory(options.memory, options.is_confirmed, tmpLog)
    if not is_confirmed:
        sys.exit(0)

    # exclude sites
    if options.excludedSite != []:
        options.excludedSite = PsubUtils.splitCommaConcatenatedItems(options.excludedSite)

    # use certain sites
    includedSite = None
    if re.search(",", options.site) is not None:
        includedSite = PsubUtils.splitCommaConcatenatedItems([options.site])
        options.site = "AUTO"

    # set maxNFilesPerJob
    PsubUtils.limit_maxNumInputs = options.maxNFilesPerJob

    # site specified
    siteSpecified = True
    if options.site == "AUTO":
        siteSpecified = False

    # list of output files which can be skipped
    options.allowNoOutput = options.allowNoOutput.split(",")

    # read datasets from file
    if options.inDsTxt != "":
        options.inDS = PsubUtils.readDsFromFile(options.inDsTxt)

    # not expand inDS when setting parent
    if options.parentTaskID:
        options.notExpandInDS = True

    # bulk submission
    ioList = []
    if options.inOutDsJson != "":
        options.bulkSubmission = True
    if options.bulkSubmission:
        if options.inOutDsJson == "":
            tmpLog.error("--inOutDsJson is missing")
            sys.exit(EC_Config)
        if options.eventPickEvtList != "":
            tmpLog.error("cannnot use --eventPickEvtList and --inOutDsJson at the same time")
            sys.exit(EC_Config)
        ioList = MiscUtils.decodeJSON(options.inOutDsJson)
        for ioItem in ioList:
            if not ioItem["outDS"].endswith("/"):
                ioItem["outDS"] += "/"
        options.inDS = ioList[0]["inDS"]
        options.outDS = ioList[0]["outDS"]
    else:
        ioList = [{"inDS": options.inDS, "outDS": options.outDS}]

    # enforce to use output dataset container
    if not options.outDS.endswith("/"):
        options.outDS = options.outDS + "/"

    # absolute path for PFN list
    if options.pfnList != "":
        options.pfnList = os.path.realpath(options.pfnList)

    # extract DBR from exec
    tmpMatch = re.search("%DB:([^ '\";]+)", options.jobParams)
    if tmpMatch is not None:
        options.dbRelease = tmpMatch.group(1)
        options.notExpandDBR = True

    # check DBRelease
    if options.dbRelease != "" and (options.dbRelease.find(":") == -1 and options.dbRelease != "LATEST"):
        tmpLog.error("invalid argument for --dbRelease. Must be DatasetName:FileName or LATEST")
        sys.exit(EC_Config)

    # Good Run List
    if options.goodRunListXML != "" and options.inDS != "":
        tmpLog.error("cannnot use --goodRunListXML and --inDS at the same time")
        sys.exit(EC_Config)

    # event picking
    if options.eventPickEvtList != "" and options.inDS != "":
        tmpLog.error("cannnot use --eventPickEvtList and --inDS at the same time")
        sys.exit(EC_Config)

    # param check for event picking
    if options.eventPickEvtList != "":
        if options.eventPickDataType == "":
            tmpLog.error("--eventPickDataType must be specified")
            sys.exit(EC_Config)

    # check rootVer
    if options.rootVer != "":
        if options.useAthenaPackages or options.athenaTag:
            tmpLog.warning(
                "--rootVer is ignored when --athenaTag or --useAthenaPackages is used, " "not to break the runtime environment by superseding the root version"
            )
            options.rootVer = ""
        else:
            # change / to .
            options.rootVer = re.sub("/", ".", options.rootVer)

    # check writeInputToTxt
    if options.writeInputToTxt != "":
        # remove %
        options.writeInputToTxt = options.writeInputToTxt.replace("%", "")
        # loop over all StreamName:FileName
        for tmpItem in options.writeInputToTxt.split(","):
            tmpItems = tmpItem.split(":")
            if len(tmpItems) != 2:
                tmpLog.error("invalid StreamName:FileName in --writeInputToTxt : %s" % tmpItem)
                sys.exit(EC_Config)

    # read list of files to be used
    filesToBeUsed = []
    if options.inputFileListName != "":
        rFile = open(options.inputFileListName)
        for line in rFile:
            line = re.sub("\n", "", line)
            line = line.strip()
            if line != "":
                filesToBeUsed.append(line)
        rFile.close()

    # remove whitespaces
    if options.inDS != "":
        options.inDS = options.inDS.replace(" ", "")

    # persistent file
    if options.persistentFile:
        options.persistentFile = "{0}:sources.{1}.__ow__".format(options.persistentFile, MiscUtils.wrappedUuidGen())

    # warning
    if options.nFilesPerJob is not None and options.nFilesPerJob > 0 and options.nFilesPerJob < 5:
        tmpLog.warning(
            "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    if options.maxNFilesPerJob < 5:
        tmpLog.warning(
            "Very small --maxNFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    # check grid-proxy
    if not dry_mode:
        PsubUtils.check_proxy(options.verbose, options.vomsRoles)

    # convert in/outTarBall to full path
    if options.inTarBall != "":
        options.inTarBall = os.path.abspath(os.path.expanduser(options.inTarBall))
    if options.outTarBall != "":
        options.outTarBall = os.path.abspath(os.path.expanduser(options.outTarBall))

    # check working dir
    options.workDir = os.path.realpath(options.workDir)
    if options.workDir != curDir and (not curDir.startswith(options.workDir + "/")):
        tmpLog.error("you need to run prun in a directory under %s" % options.workDir)
        sys.exit(EC_Config)

    # avoid gathering the home dir
    if (
        "HOME" in os.environ
        and not options.useHomeDir
        and not options.useAthenaPackages
        and os.path.realpath(os.path.expanduser(os.environ["HOME"])) == options.workDir
        and not dry_mode
    ):
        tmpStr = "prun is executed just under the HOME directoy "
        tmpStr += "and is going to send all files under the dir including ~/Mail/* and ~/private/*. "
        tmpStr += "Do you really want that? (Please use --useHomeDir if you want to skip this confirmation)"
        tmpLog.warning(tmpStr)
        while True:
            tmpAnswer = input("y/N: ")
            tmpAnswer = tmpAnswer.strip()
            if tmpAnswer in ["y", "N"]:
                break
        if tmpAnswer == "N":
            sys.exit(EC_Config)

    # run dir
    runDir = "."
    if curDir != options.workDir:
        # remove special characters
        wDirString = re.sub("[\+]", ".", options.workDir)
        runDir = re.sub("^" + wDirString + "/", "", curDir)

    # check maxCpuCount
    if options.maxCpuCount > Client.maxCpuCountLimit:
        tmpLog.error("too large maxCpuCount. Must be less than %s" % Client.maxCpuCountLimit)
        sys.exit(EC_Config)

    # create tmp dir
    if options.tmpDir == "":
        tmpDir = "%s/%s" % (curDir, MiscUtils.wrappedUuidGen())
    else:
        tmpDir = "%s/%s" % (os.path.abspath(options.tmpDir), MiscUtils.wrappedUuidGen())
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, files, del_command):
        for tmpFile in files:
            del_command("rm -rf %s" % tmpFile)
        del_command("rm -rf %s" % dir)

    atexit.register(_onExit, tmpDir, delFilesOnExit, commands_get_output)

    # parse tag
    athenaVer = ""
    cacheVer = ""
    nightVer = ""
    groupArea = ""
    cmtConfig = ""
    if options.useAthenaPackages:
        # get Athena versions
        stA, retA = AthenaUtils.getAthenaVer()
        # failed
        if not stA:
            tmpLog.error("You need to setup Athena runtime to use --useAthenaPackages")
            sys.exit(EC_Config)
        workArea = retA["workArea"]
        athenaVer = "Atlas-%s" % retA["athenaVer"]
        groupArea = retA["groupArea"]
        cacheVer = retA["cacheVer"]
        nightVer = retA["nightVer"]
        cmtConfig = retA["cmtConfig"]
        # override run directory
        sString = re.sub("[\+]", ".", workArea)
        runDir = re.sub("^%s" % sString, "", curDir)
        if runDir == curDir:
            errMsg = "You need to run prun in a directory under %s. " % workArea
            errMsg += "If '%s' is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup." % workArea
            tmpLog.error(errMsg)
            sys.exit(EC_Config)
        elif runDir == "":
            runDir = "."
        elif runDir.startswith("/"):
            runDir = runDir[1:]
        runDir = runDir + "/"
    elif options.athenaTag != "":
        athenaVer, cacheVer, nightVer = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmpLog)

    # set CMTCONFIG
    options.cmtConfig = AthenaUtils.getCmtConfig(athenaVer, cacheVer, nightVer, options.cmtConfig, options.verbose)

    # check CMTCONFIG
    if not AthenaUtils.checkCmtConfig(cmtConfig, options.cmtConfig, options.noBuild):
        sys.exit(EC_Config)

    # event picking
    if options.eventPickEvtList != "":
        epLockedBy = "prun"
        if not options.noSubmit:
            # request event picking
            epStat, epOutput = Client.requestEventPicking(
                options.eventPickEvtList,
                options.eventPickDataType,
                options.eventPickStreamName,
                options.eventPickDS,
                options.eventPickAmiTag,
                [],
                options.inputFileListName,
                options.outDS,
                epLockedBy,
                fullExecString,
                1,
                options.eventPickWithGUID,
                options.ei_api,
                options.verbose,
            )
            # set input dataset
            options.inDS = epOutput
        else:
            options.inDS = "dummy"
        tmpLog.info("requested Event Picking service to stage input as %s" % options.inDS)

    # additional files
    if options.extFile == "":
        options.extFile = []
    else:
        tmpItems = options.extFile.split(",")
        options.extFile = []
        # convert * to .*
        for tmpItem in tmpItems:
            options.extFile.append(tmpItem.replace("*", ".*"))

    # user-specified merging script
    if options.mergeScript != "":
        # enable merging
        options.mergeOutput = True
        # add it to extFile
        if not options.mergeScript in options.extFile:
            options.extFile.append(options.mergeScript)

    # glue packages
    options.gluePackages = options.gluePackages.split(",")
    try:
        options.gluePackages.remove("")
    except Exception:
        pass

    # set excludeFile
    AthenaUtils.setExcludeFile(options.excludeFile)

    # LFN matching
    if options.match != "":
        # convert . to \.
        options.match = options.match.replace(".", "\.")
        # convert * to .*
        options.match = options.match.replace("*", ".*")

    # LFN anti-matching
    if options.antiMatch != "":
        # convert . to \.
        options.antiMatch = options.antiMatch.replace(".", "\.")
        # convert * to .*
        options.antiMatch = options.antiMatch.replace("*", ".*")

    # get job script
    jobScript = ""
    if options.jobParams == "":
        tmpLog.error("you need to give --exec\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(EC_Config)
    orig_execStr = options.jobParams
    orig_bexecStr = options.bexec

    # replace : to = for backward compatibility
    for optArg in ["RNDM"]:
        options.jobParams = re.sub("%" + optArg + ":", "%" + optArg + "=", options.jobParams)

    # check output dataset
    if options.outDS == "":
        tmpLog.error("no outDS is given\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(EC_Config)

    # avoid inDS+pfnList
    if options.pfnList != "":
        # don't use inDS
        if options.inDS != "":
            tmpLog.error("--pfnList and --inDS cannot be used at the same time")
            sys.exit(EC_Config)
        # use site
        if options.site == "AUTO":
            tmpLog.error("--site must be specified when --pfnList is used")
            sys.exit(EC_Config)

    # secondary datasets
    tmpStat, tmpOut = parse_secondary_datasets_opt(options.secondaryDSs)
    if not tmpStat:
        tmpLog.error(tmpOut)
        sys.exit(EC_Config)
    else:
        options.secondaryDSs = tmpOut

    # reusable secondary streams
    if options.reusableSecondary == "":
        options.reusableSecondary = []
    else:
        options.reusableSecondary = options.reusableSecondary.split(",")

    # get nickname
    if not dry_mode:
        nickName = PsubUtils.getNickname()
    else:
        nickName = "dummy"

    if nickName == "":
        sys.exit(EC_Config)

    # set Rucio accounting
    PsubUtils.setRucioAccount(nickName, "prun", True)

    # check nGBPerJob
    if not options.nGBPerJob in [-1, "MAX"]:
        # convert to int
        try:
            if options.nGBPerJob != "MAX":
                options.nGBPerJob = int(options.nGBPerJob)
        except Exception:
            tmpLog.error("--nGBPerJob must be an integer or MAX")
            sys.exit(EC_Config)
        # check negative
        if options.nGBPerJob <= 0:
            tmpLog.error("--nGBPerJob must be positive")
            sys.exit(EC_Config)
        # incompatible parameters
        if options.nFilesPerJob is not None and options.nFilesPerJob > 0:
            tmpLog.error("--nFilesPerJob and --nGBPerJob must be used exclusively")
            sys.exit(EC_Config)

    # split options are mutually exclusive
    if options.nFilesPerJob is not None and options.nFilesPerJob > 0 and options.nEventsPerJob > 0 and options.nGBPerJob != -1:
        tmpLog.error("split by files, split by events and split by file size can not be used simultaneously")
        sys.exit(EC_Config)

    # split options are mutually exclusive
    if options.nEventsPerJob > 0 and options.nGBPerJob != -1:
        tmpLog.error("split by events and split by file size can not be used simultaneously")
        sys.exit(EC_Config)

    #####################################################################
    # archive sources and send it to HTTP-reachable location

    # create archive
    archiveName = None
    if (options.containerImage == "" or options.useSandbox) and not dry_mode:
        if options.inTarBall == "":
            # copy RootCore packages
            if options.useRootCore:
                # check $ROOTCOREDIR
                if "ROOTCOREDIR" not in os.environ:
                    tmpErrMsg = "$ROOTCOREDIR is not defined in your environment. "
                    tmpErrMsg += "Please setup RootCore runtime beforehand"
                    tmpLog.error(tmpErrMsg)
                    sys.exit(EC_Config)
                # check grid_submit.sh
                rootCoreSubmitSh = os.environ["ROOTCOREDIR"] + "/scripts/grid_submit.sh"
                rootCoreCompileSh = os.environ["ROOTCOREDIR"] + "/scripts/grid_compile.sh"
                rootCoreRunSh = os.environ["ROOTCOREDIR"] + "/scripts/grid_run.sh"
                rootCoreSubmitNbSh = os.environ["ROOTCOREDIR"] + "/scripts/grid_submit_nobuild.sh"
                rootCoreCompileNbSh = os.environ["ROOTCOREDIR"] + "/scripts/grid_compile_nobuild.sh"
                rootCoreShList = [rootCoreSubmitSh, rootCoreCompileSh, rootCoreRunSh]
                if options.noBuild:
                    rootCoreShList.append(rootCoreSubmitNbSh)
                    if options.noCompile:
                        rootCoreShList.append(rootCoreCompileNbSh)
                for tmpShFile in rootCoreShList:
                    if not os.path.exists(tmpShFile):
                        tmpErrMsg = "%s doesn't exist. Please use a newer version of RootCore" % tmpShFile
                        tmpLog.error(tmpErrMsg)
                        sys.exit(EC_Config)
                tmpLog.info("copy RootCore packages to current dir")
                # destination
                pandaRootCoreWorkDirName = "__panda_rootCoreWorkDir"
                rootCoreDestWorkDir = curDir + "/" + pandaRootCoreWorkDirName
                # add all files to extFile
                options.extFile.append(pandaRootCoreWorkDirName + "/.*")
                # add to be deleted on exit
                delFilesOnExit.append(rootCoreDestWorkDir)
                if not options.noBuild:
                    tmpStat = os.system("%s %s" % (rootCoreSubmitSh, rootCoreDestWorkDir))
                else:
                    tmpStat = os.system("%s %s" % (rootCoreSubmitNbSh, rootCoreDestWorkDir))
                tmpStat %= 255
                if tmpStat != 0:
                    tmpErrMsg = "%s failed with %s" % (rootCoreSubmitSh, tmpStat)
                    tmpLog.error(tmpErrMsg)
                    sys.exit(EC_Config)
                # copy build and run scripts
                shutil.copy(rootCoreRunSh, rootCoreDestWorkDir)
                shutil.copy(rootCoreCompileSh, rootCoreDestWorkDir)
                if options.noCompile:
                    shutil.copy(rootCoreCompileNbSh, rootCoreDestWorkDir)
            # gather Athena packages
            archiveName = ""
            if options.useAthenaPackages:
                if AthenaUtils.useCMake():
                    # archive with cpack
                    archiveName, archiveFullName = AthenaUtils.archiveWithCpack(True, tmpDir, options.verbose)
                # set extFile
                AthenaUtils.setExtFile(options.extFile)
                if not options.noBuild:
                    # archive sources
                    archiveName, archiveFullName = AthenaUtils.archiveSourceFiles(
                        workArea,
                        runDir,
                        curDir,
                        tmpDir,
                        options.verbose,
                        options.gluePackages,
                        dereferenceSymLinks=options.followLinks,
                        archiveName=archiveName,
                    )
                else:
                    # archive jobO
                    archiveName, archiveFullName = AthenaUtils.archiveJobOFiles(
                        workArea,
                        runDir,
                        curDir,
                        tmpDir,
                        options.verbose,
                        archiveName=archiveName,
                    )
                # archive InstallArea
                AthenaUtils.archiveInstallArea(
                    workArea,
                    groupArea,
                    archiveName,
                    archiveFullName,
                    tmpDir,
                    options.noBuild,
                    options.verbose,
                )
            # gather normal files
            if True:
                if options.useAthenaPackages:
                    # go to workArea
                    os.chdir(workArea)
                    # gather files under work dir
                    tmpLog.info("gathering files under %s/%s" % (workArea, runDir))
                    archStartDir = runDir
                    archStartDir = re.sub("/+$", "", archStartDir)
                else:
                    # go to work dir
                    os.chdir(options.workDir)
                    # gather files under work dir
                    tmpLog.info("gathering files under %s" % options.workDir)
                    archStartDir = "."
                # get files in the working dir
                if options.noCompile:
                    skippedExt = []
                else:
                    skippedExt = [".o", ".a", ".so"]
                skippedFlag = False
                workDirFiles = []
                if options.followLinks:
                    osWalkList = os.walk(archStartDir, followlinks=True)
                else:
                    osWalkList = os.walk(archStartDir)
                for tmpRoot, tmpDirs, tmpFiles in osWalkList:
                    emptyFlag = True
                    for tmpFile in tmpFiles:
                        if options.useAthenaPackages:
                            if os.path.basename(tmpFile) == os.path.basename(archiveFullName):
                                if options.verbose:
                                    print("skip Athena archive %s" % tmpFile)
                                continue
                        tmpPath = "%s/%s" % (tmpRoot, tmpFile)
                        # get size
                        try:
                            size = os.path.getsize(tmpPath)
                        except Exception:
                            # skip dead symlink
                            if options.verbose:
                                type, value, traceBack = sys.exc_info()
                                print("  Ignore : %s:%s" % (type, value))
                            continue
                        # check exclude files
                        excludeFileFlag = False
                        for tmpPatt in AthenaUtils.excludeFile:
                            if re.search(tmpPatt, tmpPath) is not None:
                                excludeFileFlag = True
                                break
                        if excludeFileFlag:
                            continue
                        # skipped extension
                        isSkippedExt = False
                        for tmpExt in skippedExt:
                            if tmpPath.endswith(tmpExt):
                                isSkippedExt = True
                                break
                        # check root
                        isRoot = False
                        if re.search("\.root(\.\d+)*$", tmpPath) is not None:
                            isRoot = True
                        # extra files
                        isExtra = False
                        for tmpExt in options.extFile:
                            if re.search(tmpExt + "$", tmpPath) is not None:
                                isExtra = True
                                break
                        # regular files
                        if not isExtra:
                            # unset emptyFlag even if all files are skipped
                            emptyFlag = False
                            # skipped extensions
                            if isSkippedExt:
                                print("  skip %s %s" % (str(skippedExt), tmpPath))
                                skippedFlag = True
                                continue
                            # skip root
                            if isRoot:
                                print("  skip root file %s" % tmpPath)
                                skippedFlag = True
                                continue
                            # check size
                            if size > options.maxFileSize:
                                print("  skip large file %s:%sB>%sB" % (tmpPath, size, options.maxFileSize))
                                skippedFlag = True
                                continue
                        # remove ./
                        tmpPath = re.sub("^\./", "", tmpPath)
                        # append
                        workDirFiles.append(tmpPath)
                        if emptyFlag:
                            emptyFlag = False
                    # add empty directory
                    if emptyFlag and tmpDirs == [] and tmpFiles == []:
                        tmpPath = re.sub("^\./", "", tmpRoot)
                        # check exclude pattern
                        excludePatFlag = False
                        for tmpPatt in AthenaUtils.excludeFile:
                            if re.search(tmpPatt, tmpPath) is not None:
                                excludePatFlag = True
                                break
                        if excludePatFlag:
                            continue
                        # skip tmpDir
                        if tmpPath.split("/")[-1] == tmpDir.split("/")[-1]:
                            continue
                        # append
                        workDirFiles.append(tmpPath)
                if skippedFlag:
                    tmpLog.info("please use --extFile if you need to send the skipped files to WNs")
                # set archive name
                if not options.useAthenaPackages:
                    # create archive
                    if options.noBuild and not options.noCompile:
                        # use 'jobO' for noBuild
                        archiveName = "jobO.%s.tar" % MiscUtils.wrappedUuidGen()
                    else:
                        # use 'sources' for normal build
                        archiveName = "sources.%s.tar" % MiscUtils.wrappedUuidGen()
                    archiveFullName = "%s/%s" % (tmpDir, archiveName)
                # collect files
                for tmpFile in workDirFiles:
                    # avoid self-archiving
                    if os.path.basename(tmpFile) == os.path.basename(archiveFullName):
                        if options.verbose:
                            print("skip self-archiving for %s" % tmpFile)
                        continue
                    if os.path.islink(tmpFile):
                        status, out = commands_get_status_output("tar --exclude '.[a-zA-Z]*' -rh '%s' -f '%s'" % (tmpFile, archiveFullName))
                    else:
                        status, out = commands_get_status_output("tar --exclude '.[a-zA-Z]*' -rf '%s' '%s'" % (archiveFullName, tmpFile))
                    if options.verbose:
                        print(tmpFile)
                    if status != 0 or out != "":
                        print(out)
            # go to tmpdir
            os.chdir(tmpDir)

            # make empty if archive doesn't exist
            if not os.path.exists(archiveFullName):
                commands_get_status_output("tar cvf %s --files-from /dev/null " % archiveName)

            # compress
            status, out = commands_get_status_output("gzip %s" % archiveName)
            archiveName += ".gz"
            if status != 0 or options.verbose:
                print(out)

            # check archive
            status, out = commands_get_status_output("ls -l {0}".format(archiveName))
            if options.verbose:
                print(out)
            if status != 0:
                tmpLog.error("Failed to archive working area.\nIf you see 'Disk quota exceeded', try '--tmpDir /tmp'")
                sys.exit(EC_Archive)

            # check symlinks
            if options.useAthenaPackages:
                tmpLog.info("checking sandbox")
                for _ in range(5):
                    status, out = commands_get_status_output("tar tvfz %s" % archiveName)
                    if status == 0:
                        break
                    time.sleep(5)
                if status != 0:
                    tmpLog.error("Failed to expand sandbox. {0}".format(out))
                    sys.exit(EC_Archive)
                symlinks = []
                for line in out.split("\n"):
                    items = line.split()
                    if len(items) > 0 and items[0].startswith("l") and items[-1].startswith("/"):
                        symlinks.append(line)
                if symlinks != []:
                    tmpStr = "Found some unresolved symlinks which may cause a problem\n"
                    tmpStr += "     See, e.g., http://savannah.cern.ch/bugs/?43885\n"
                    tmpStr += "   Please ignore if you believe they are harmless"
                    tmpLog.warning(tmpStr)
                    for symlink in symlinks:
                        print("  %s" % symlink)
        else:
            # go to tmp dir
            os.chdir(tmpDir)
            # use a saved copy
            if options.noCompile or not options.noBuild:
                archiveName = "sources.%s.tar" % MiscUtils.wrappedUuidGen()
                archiveFullName = "%s/%s" % (tmpDir, archiveName)
            else:
                archiveName = "jobO.%s.tar" % MiscUtils.wrappedUuidGen()
                archiveFullName = "%s/%s" % (tmpDir, archiveName)
            # make copy to avoid name duplication
            shutil.copy(options.inTarBall, archiveFullName)

        # save
        if options.outTarBall != "":
            shutil.copy(archiveName, options.outTarBall)

        # upload source files
        if not options.noSubmit:
            # upload sources via HTTP POST
            tmpLog.info("upload sandbox")
            if options.vo is None:
                use_cache_srv = True
            else:
                use_cache_srv = False
            status, out = Client.putFile(
                archiveName,
                options.verbose,
                useCacheSrv=use_cache_srv,
                reuseSandbox=True,
            )
            if out.startswith("NewFileName:"):
                # found the same input sandbox to reuse
                archiveName = out.split(":")[-1]
            elif out != "True":
                print(out)
                tmpLog.error("failed to upload sandbox with %s" % status)
                sys.exit(EC_Post)
            # good run list
            if options.goodRunListXML != "":
                options.goodRunListXML = PsubUtils.uploadGzippedFile(
                    options.goodRunListXML,
                    curDir,
                    tmpLog,
                    delFilesOnExit,
                    options.noSubmit,
                    options.verbose,
                )

    # special handling
    specialHandling = ""
    if options.express:
        specialHandling += "express,"
    if options.debugMode:
        specialHandling += "debug,"
    specialHandling = specialHandling[:-1]

    #####################################################################
    # task making

    # job name
    jobName = "prun.%s" % MiscUtils.wrappedUuidGen()

    # make task
    taskParamMap = {}
    taskParamMap["taskName"] = options.outDS
    if not options.allowTaskDuplication:
        taskParamMap["uniqueTaskName"] = True
    if options.vo is None:
        taskParamMap["vo"] = "atlas"
    else:
        taskParamMap["vo"] = options.vo
    if options.containerImage != "" and options.alrb:
        taskParamMap["architecture"] = options.architecture
    else:
        taskParamMap["architecture"] = AthenaUtils.getCmtConfigImg(
            athenaVer,
            cacheVer,
            nightVer,
            options.cmtConfig,
            architecture=options.architecture,
        )
    taskParamMap["transUses"] = athenaVer
    if athenaVer != "":
        taskParamMap["transHome"] = "AnalysisTransforms" + cacheVer + nightVer
    else:
        taskParamMap["transHome"] = None
    if options.containerImage != "" and not options.alrb:
        taskParamMap["processingType"] = "panda-client-{0}-jedi-cont".format(PandaToolsPkgInfo.release_version)
    else:
        taskParamMap["processingType"] = "panda-client-{0}-jedi-run".format(PandaToolsPkgInfo.release_version)
    if options.eventPickEvtList != "":
        taskParamMap["processingType"] += "-evp"
        taskParamMap["waitInput"] = 1
    if options.goodRunListXML != "":
        taskParamMap["processingType"] += "-grl"
    if options.prodSourceLabel == "":
        taskParamMap["prodSourceLabel"] = "user"
    else:
        taskParamMap["prodSourceLabel"] = options.prodSourceLabel
    if options.site != "AUTO":
        taskParamMap["site"] = options.site
    else:
        taskParamMap["site"] = None
    taskParamMap["excludedSite"] = options.excludedSite
    if includedSite is not None and includedSite != []:
        taskParamMap["includedSite"] = includedSite
    else:
        taskParamMap["includedSite"] = None
    if options.priority is not None:
        taskParamMap["currentPriority"] = options.priority
    if options.nFiles > 0:
        taskParamMap["nFiles"] = options.nFiles
    if options.nFilesPerJob is not None:
        taskParamMap["nFilesPerJob"] = options.nFilesPerJob
    if not options.nGBPerJob in [-1, "MAX"]:
        # don't set MAX since it is the defalt on the server side
        taskParamMap["nGBPerJob"] = options.nGBPerJob
    if options.nEventsPerJob > 0:
        taskParamMap["nEventsPerJob"] = options.nEventsPerJob
        if options.nEventsPerFile <= 0:
            taskParamMap["useRealNumEvents"] = True
        else:
            taskParamMap["nEventsPerFile"] = options.nEventsPerFile
        if options.nJobs > 0 and options.nEvents < 0:
            taskParamMap["nEvents"] = options.nJobs * options.nEventsPerJob
    taskParamMap["cliParams"] = fullExecString
    if options.noEmail:
        taskParamMap["noEmail"] = True
    if options.skipScout:
        taskParamMap["skipScout"] = True
    if options.msgDriven:
        taskParamMap["messageDriven"] = True
    if options.respectSplitRule:
        taskParamMap["respectSplitRule"] = True
    if options.respectLB:
        taskParamMap["respectLB"] = True
    if options.osMatching:
        taskParamMap["osMatching"] = True
    taskParamMap["osInfo"] = PsubUtils.get_os_information()
    if options.parentTaskID:
        taskParamMap["noWaitParent"] = True
    if options.disableAutoRetry:
        taskParamMap["disableAutoRetry"] = 1
    if options.workingGroup is not None:
        # remove role
        taskParamMap["workingGroup"] = options.workingGroup.split(".")[0].split(":")[0]
    if options.official:
        taskParamMap["official"] = True
    taskParamMap["nMaxFilesPerJob"] = options.maxNFilesPerJob
    if options.useNewCode:
        taskParamMap["fixedSandbox"] = archiveName
    if options.maxCpuCount > 0:
        taskParamMap["walltime"] = -options.maxCpuCount
    if options.noLoopingCheck:
        taskParamMap["noLoopingCheck"] = True
    if options.maxWalltime > 0:
        taskParamMap["maxWalltime"] = options.maxWalltime
    if options.cpuTimePerEvent > 0:
        taskParamMap["cpuTime"] = options.cpuTimePerEvent
        taskParamMap["cpuTimeUnit"] = "HS06sPerEvent"
    if options.fixedCpuTime:
        taskParamMap["cpuTimeUnit"] = "HS06sPerEventFixed"
    if options.memory > 0:
        taskParamMap["ramCount"] = options.memory
        if options.fixedRamCount:
            taskParamMap["ramCountUnit"] = "MBPerCoreFixed"
        else:
            taskParamMap["ramCountUnit"] = "MBPerCore"
    if options.outDiskCount is not None:
        taskParamMap["outDiskCount"] = options.outDiskCount
        taskParamMap["outDiskUnit"] = "kBFixed"
    if options.nCore > 1:
        taskParamMap["coreCount"] = options.nCore
    if options.skipFilesUsedBy != "":
        taskParamMap["skipFilesUsedBy"] = options.skipFilesUsedBy
    taskParamMap["respectSplitRule"] = True
    if options.maxAttempt > 0 and options.maxAttempt <= 50:
        taskParamMap["maxAttempt"] = options.maxAttempt
    if options.useSecrets:
        taskParamMap["useSecrets"] = True
    if options.debugMode:
        taskParamMap["debugMode"] = True
    # source URL
    if options.vo is None:
        matchURL = re.search("(http.*://[^/]+)/", Client.baseURLCSRVSSL)
    else:
        matchURL = re.search("(http.*://[^/]+)/", Client.baseURLSSL)
    if matchURL is not None:
        taskParamMap["sourceURL"] = matchURL.group(1)
    # XML config
    if options.loadXML is not None:
        taskParamMap["loadXML"] = options.loadXML
    # middle name
    if options.addNthFieldOfInFileToLFN != "":
        taskParamMap["addNthFieldToLFN"] = options.addNthFieldOfInFileToLFN
        taskParamMap["useFileAsSourceLFN"] = True
    elif options.addNthFieldOfInDSToLFN != "":
        taskParamMap["addNthFieldToLFN"] = options.addNthFieldOfInDSToLFN
    if options.containerImage != "" and options.alrb:
        taskParamMap["container_name"] = options.containerImage
        if options.directExecInContainer:
            taskParamMap["multiStepExec"] = {
                "preprocess": {"command": "${TRF}", "args": "--preprocess ${TRF_ARGS}"},
                "postprocess": {
                    "command": "${TRF}",
                    "args": "--postprocess ${TRF_ARGS}",
                },
                "containerOptions": {
                    "containerExec": 'echo "=== cat exec script ==="; '
                    "cat __run_main_exec.sh; "
                    "echo; "
                    'echo "=== exec script ==="; '
                    "/bin/sh __run_main_exec.sh",
                    "containerImage": options.containerImage,
                },
            }
            if options.alrbArgs is not None:
                taskParamMap["multiStepExec"]["containerOptions"]["execArgs"] = options.alrbArgs

    outDatasetName = options.outDS
    logDatasetName = re.sub("/$", ".log/", options.outDS)
    # log
    if not options.noSeparateLog:
        taskParamMap["log"] = {
            "dataset": logDatasetName,
            "container": logDatasetName,
            "type": "template",
            "param_type": "log",
            "value": "{0}.$JEDITASKID.${{SN}}.log.tgz".format(logDatasetName[:-1]),
        }
        if options.addNthFieldOfInFileToLFN != "":
            loglfn = "{0}.{1}".format(*logDatasetName.split(".")[:2])
            loglfn += "${MIDDLENAME}.$JEDITASKID._${SN}.log.tgz"
            taskParamMap["log"]["value"] = loglfn
        if options.spaceToken != "":
            taskParamMap["log"]["token"] = options.spaceToken
        if options.mergeOutput and options.mergeLog:
            # log merge
            mLogDatasetName = re.sub(r"\.log/", r".merge_log/", logDatasetName)
            mLFN = re.sub(r"\.log\.tgz", ".merge_log.tgz", taskParamMap["log"]["value"])
            data = copy.deepcopy(taskParamMap["log"])
            data.update(
                {
                    "dataset": mLogDatasetName,
                    "container": mLogDatasetName,
                    "param_type": "output",
                    "mergeOnly": True,
                    "value": mLFN,
                }
            )
            taskParamMap["log_merge"] = data

    # job parameters
    taskParamMap["jobParameters"] = [
        {
            "type": "constant",
            "value": '-j "" --sourceURL ${SURL}',
        },
        {
            "type": "constant",
            "value": "-r {0}".format(runDir),
        },
    ]

    # delimiter
    taskParamMap["jobParameters"] += [
        {"type": "constant", "value": "__delimiter__", "hidden": True},
    ]

    # build
    if options.containerImage == "" or options.useSandbox:
        if options.noBuild and not options.noCompile:
            taskParamMap["jobParameters"] += [
                {
                    "type": "constant",
                    "value": "-a {0}".format(archiveName),
                },
            ]
        else:
            taskParamMap["jobParameters"] += [
                {
                    "type": "constant",
                    "value": "-l ${LIB}",
                },
            ]
    # output
    if options.outputs != "":
        outMap = {}
        dsSuffix = []
        dsIndex = 0
        for tmpLFN in options.outputs.split(","):
            tmpDsSuffix = ""
            if ":" in tmpLFN:
                tmpDsSuffix, tmpLFN = tmpLFN.split(":")
                if tmpDsSuffix in dsSuffix:
                    tmpErrMsg = "dataset name suffix '%s' is used for multiple files in --outputs. " % tmpDsSuffix
                    tmpErrMsg += "each output must have a unique suffix."
                    tmpLog.error(tmpErrMsg)
                    sys.exit(EC_Config)
                dsSuffix.append(tmpDsSuffix)
            if tmpLFN.startswith("regex|"):
                # regex
                lfn = tmpLFN
                if not tmpDsSuffix:
                    tmpDsSuffix = dsIndex
                    dsIndex += 1
            else:
                tmpNewLFN = tmpLFN
                # change * to XYZ and add .tgz
                if "*" in tmpNewLFN:
                    tmpNewLFN = tmpNewLFN.replace("*", "XYZ")
                    tmpNewLFN += ".tgz"
                if len(outDatasetName.split(".")) > 2:
                    lfn = "{0}.{1}".format(*outDatasetName.split(".")[:2])
                else:
                    lfn = outDatasetName[:-1]
                if options.addNthFieldOfInDSToLFN != "" or options.addNthFieldOfInFileToLFN != "":
                    lfn += "${MIDDLENAME}"
                lfn += ".$JEDITASKID._${{SN/P}}.{0}".format(tmpNewLFN)
                if tmpDsSuffix == "":
                    tmpDsSuffix = tmpNewLFN
            dataset = "{0}_{1}/".format(outDatasetName[:-1], tmpDsSuffix)
            taskParamMap["jobParameters"] += MiscUtils.makeJediJobParam(
                lfn,
                dataset,
                "output",
                hidden=True,
                destination=options.destSE,
                token=options.spaceToken,
                allowNoOutput=options.allowNoOutput,
            )
            outMap[tmpLFN] = lfn
        if options.loadXML:
            taskParamMap["jobParameters"] += [
                {
                    "type": "constant",
                    "value": '-o "${XML_OUTMAP}"',
                },
            ]
        else:
            taskParamMap["jobParameters"] += [
                {
                    "type": "constant",
                    "value": '-o "{0}"'.format(str(outMap)),
                },
            ]
    # input
    if options.inDS != "":
        tmpDict = {
            "type": "template",
            "param_type": "input",
            "value": '-i "${IN/T}"',
            "dataset": options.inDS,
            "exclude": "\.log\.tgz(\.\d+)*$",
        }
        if options.useLogAsInput:
            del tmpDict["exclude"]
        if options.loadXML is None and not options.notExpandInDS:
            tmpDict["expand"] = True
        if options.notExpandInDS:
            tmpDict["consolidate"] = ".".join(options.outDS.split(".")[:2]) + "." + MiscUtils.wrappedUuidGen() + "/"
        if options.nSkipFiles != 0:
            tmpDict["offset"] = options.nSkipFiles
        if options.match != "":
            tmpDict["include"] = options.match
        if options.antiMatch != "":
            if "exclude" in tmpDict:
                tmpDict["exclude"] += "," + options.antiMatch
            else:
                tmpDict["exclude"] = options.antiMatch
        if filesToBeUsed != []:
            tmpDict["files"] = filesToBeUsed
        taskParamMap["jobParameters"].append(tmpDict)
        taskParamMap["dsForIN"] = options.inDS
    elif options.pfnList != "":
        taskParamMap["pfnList"] = PsubUtils.getListPFN(options.pfnList)
        # use noInput
        taskParamMap["noInput"] = True
        if options.nFiles == 0:
            taskParamMap["nFiles"] = len(taskParamMap["pfnList"])
        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": '-i "${IN/T}"',
            },
        ]
    elif options.goodRunListXML != "":
        tmpDict = {
            "type": "template",
            "param_type": "input",
            "value": '-i "${IN/T}"',
            "dataset": "%%INDS%%",
            "expand": True,
            "exclude": "\.log\.tgz(\.\d+)*$",
            "files": "%%INLFNLIST%%",
        }
        taskParamMap["jobParameters"].append(tmpDict)
        taskParamMap["dsForIN"] = "%%INDS%%"
    else:
        # no input
        taskParamMap["noInput"] = True
        if options.nEvents > 0:
            taskParamMap["nEvents"] = options.nEvents
            if options.nJobs > 0:
                taskParamMap["nEventsPerJob"] = options.nEvents // options.nJobs
            else:
                # set granularity
                if options.nEventsPerChunk > 0:
                    taskParamMap["nEventsPerRange"] = options.nEventsPerChunk
                else:
                    # use 1/20 by default
                    taskParamMap["nEventsPerRange"] = options.nEvents // 20
                    if taskParamMap["nEventsPerRange"] <= 0:
                        taskParamMap["nEventsPerRange"] = 1
        elif options.nEventsPerJob > 0:
            taskParamMap["nEvents"] = options.nEventsPerJob * max(1, options.nJobs)
            taskParamMap["nEventsPerJob"] = options.nEventsPerJob
        else:
            if options.nJobs > 0:
                taskParamMap["nEvents"] = options.nJobs
            else:
                taskParamMap["nEvents"] = 1
            taskParamMap["nEventsPerJob"] = 1

    # exec string
    if options.loadXML is None:
        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": '-p "',
                "padding": False,
            },
        ]
        taskParamMap["jobParameters"] += PsubUtils.convertParamStrToJediParam(options.jobParams, {}, "", True, False, includeIO=False)
        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": '"',
            },
        ]
    else:
        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": '-p "{0}"'.format(options.jobParams),
            },
        ]

    # param for DBR
    if options.dbRelease != "":
        dbrDS = options.dbRelease.split(":")[0]
        # change LATEST to DBR_LATEST
        if dbrDS == "LATEST":
            dbrDS = "DBR_LATEST"
        dictItem = {
            "type": "template",
            "param_type": "input",
            "value": "--dbrFile=${DBR}",
            "dataset": dbrDS,
        }
        taskParamMap["jobParameters"] += [dictItem]
        # no expansion
        if options.notExpandDBR:
            dictItem = {
                "type": "constant",
                "value": "--noExpandDBR",
            }
            taskParamMap["jobParameters"] += [dictItem]

    # secondary
    if options.secondaryDSs != {}:
        inMap = {}
        streamNames = []
        if options.inDS != "":
            inMap["IN"] = "tmp_IN"
            streamNames.append("IN")
        for tmpDsName in options.secondaryDSs:
            tmpMap = options.secondaryDSs[tmpDsName]
            # make template item
            streamName = tmpMap["streamName"]
            if options.loadXML is None and not options.notExpandSecDSs:
                expandFlag = True
            else:
                expandFlag = False
            # re-usability
            reusableAtt = False
            if streamName in options.reusableSecondary:
                reusableAtt = True
            dictItem = MiscUtils.makeJediJobParam(
                "${" + streamName + "}",
                tmpDsName,
                "input",
                hidden=True,
                expand=expandFlag,
                include=tmpMap["pattern"],
                offset=tmpMap["nSkip"],
                nFilesPerJob=tmpMap["nFiles"],
                useNumFilesAsRatio=options.useNumFilesInSecDSsAsRatio,
                reusableAtt=reusableAtt,
                outDS=options.outDS,
                file_list=tmpMap["files"],
            )
            taskParamMap["jobParameters"] += dictItem
            inMap[streamName] = "tmp_" + streamName
            streamNames.append(streamName)
        # make constant item
        strInMap = str(inMap)
        # set placeholders
        for streamName in streamNames:
            strInMap = strInMap.replace("'tmp_" + streamName + "'", "${" + streamName + "/T}")
        dictItem = {
            "type": "constant",
            "value": '--inMap "%s"' % strInMap,
        }
        taskParamMap["jobParameters"] += [dictItem]

    # misc
    jobParameters = ""
    # given PFN
    if options.pfnList != "":
        jobParameters += "--givenPFN "
    # use Athena packages
    if options.useAthenaPackages:
        jobParameters += "--useAthenaPackages "
    # use CMake
    if AthenaUtils.useCMake():
        jobParameters += "--useCMake "
    # use RootCore
    if options.useRootCore:
        jobParameters += "--useRootCore "
    # root
    if options.rootVer != "":
        jobParameters += "--rootVer %s " % options.rootVer
    # cmt config
    if options.cmtConfig not in ["", "NULL", None]:
        jobParameters += "--cmtConfig %s " % options.cmtConfig
    # write input to txt
    if options.writeInputToTxt != "":
        jobParameters += "--writeInputToTxt %s " % options.writeInputToTxt
    # debug parameters
    if options.queueData != "":
        jobParameters += "--overwriteQueuedata=%s " % options.queueData
    # exec string with real output filenames
    if options.execWithRealFileNames:
        jobParameters += "--execWithRealFileNames "
    # container
    if options.containerImage != "" and not options.alrb:
        jobParameters += "--containerImage {0} ".format(options.containerImage)
        if options.ctrCvmfs:
            jobParameters += "--cvmfs "
        if options.ctrNoX509:
            jobParameters += "--noX509 "
        if options.ctrDatadir != "":
            jobParameters += "--datadir {0} ".format(options.ctrDatadir)
        if options.ctrWorkdir != "":
            jobParameters += "--workdir {0} ".format(options.ctrWorkdir)
        if options.ctrDebug:
            jobParameters += "--debug "
        if options.useCentralRegistry:
            jobParameters += "--useCentralRegistry=True "
        elif options.notUseCentralRegistry:
            jobParameters += "--useCentralRegistry=False "
    # persistent file
    if options.persistentFile:
        jobParameters += "--fileToSave={0} --fileToLoad={0} ".format(options.persistentFile)
    # set task param
    if jobParameters != "":
        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": jobParameters,
            },
        ]

    # force stage-in
    if options.forceStaged or options.forceStagedSecondary:
        taskParamMap["useLocalIO"] = 1

    # avoid VP
    if options.avoidVP:
        taskParamMap["avoidVP"] = True

    # build step
    if options.noBuild and not options.noCompile:
        pass
    else:
        jobParameters = "-i ${IN} -o ${OUT} --sourceURL ${SURL} "
        jobParameters += "-r {0} ".format(runDir)
        # exec
        if options.bexec != "":
            jobParameters += '--bexec "{0}" '.format(quote(options.bexec))
        # use Athena packages
        if options.useAthenaPackages:
            jobParameters += "--useAthenaPackages "
        # use RootCore
        if options.useRootCore:
            jobParameters += "--useRootCore "
        # no compile
        if options.noCompile:
            jobParameters += "--noCompile "
        # use CMake
        if AthenaUtils.useCMake():
            jobParameters += "--useCMake "
        # root
        if options.rootVer != "":
            jobParameters += "--rootVer %s " % options.rootVer
        # cmt config
        if not options.cmtConfig in ["", "NULL", None]:
            jobParameters += "--cmtConfig %s " % options.cmtConfig
        # debug parameters
        if options.queueData != "":
            jobParameters += "--overwriteQueuedata=%s " % options.queueData
        # container
        if options.containerImage != "" and not options.alrb:
            jobParameters += "--containerImage {0} ".format(options.containerImage)
            if options.ctrCvmfs:
                jobParameters += "--cvmfs "
            if options.ctrNoX509:
                jobParameters += "--noX509 "
            if options.ctrDatadir != "":
                jobParameters += "--datadir {0} ".format(options.ctrDatadir)
            if options.ctrWorkdir != "":
                jobParameters += "--workdir {0} ".format(options.ctrWorkdir)
            if options.ctrDebug:
                jobParameters += "--debug "

        # set task param
        taskParamMap["buildSpec"] = {
            "prodSourceLabel": "panda",
            "archiveName": archiveName,
            "jobParameters": jobParameters,
        }
        if options.prodSourceLabel != "":
            taskParamMap["buildSpec"]["prodSourceLabel"] = options.prodSourceLabel

    # preprocessing step

    # good run list
    if options.goodRunListXML != "":
        jobParameters = "--goodRunListXML {0} ".format(options.goodRunListXML)
        if options.goodRunDataType != "":
            jobParameters += "--goodRunListDataType {0} ".format(options.goodRunDataType)
        if options.goodRunProdStep != "":
            jobParameters += "--goodRunListProdStep {0} ".format(options.goodRunProdStep)
        if options.goodRunListDS != "":
            jobParameters += "--goodRunListDS {0} ".format(options.goodRunListDS)
        jobParameters += "--sourceURL ${SURL} "
        # set task param
        taskParamMap["preproSpec"] = {
            "prodSourceLabel": "panda",
            "jobParameters": jobParameters,
        }
        if options.prodSourceLabel != "":
            taskParamMap["preproSpec"]["prodSourceLabel"] = options.prodSourceLabel

    # merging
    if options.mergeOutput:
        jobParameters = "-r {0} ".format(runDir)
        if options.mergeScript != "":
            jobParameters += '-j "{0}" '.format(options.mergeScript)
        if options.rootVer != "":
            jobParameters += "--rootVer %s " % options.rootVer
        if options.cmtConfig not in ["", "NULL", None]:
            jobParameters += "--cmtConfig %s " % options.cmtConfig
        if options.useAthenaPackages:
            jobParameters += "--useAthenaPackages "
        if AthenaUtils.useCMake():
            jobParameters += "--useCMake "
        if options.useRootCore:
            jobParameters += "--useRootCore "
        if options.containerImage != "" and not options.alrb:
            jobParameters += "--containerImage {0} ".format(options.containerImage)
            if options.ctrCvmfs:
                jobParameters += "--cvmfs "
            if options.ctrNoX509:
                jobParameters += "--noX509 "
            if options.ctrDatadir != "":
                jobParameters += "--datadir {0} ".format(options.ctrDatadir)
            if options.ctrWorkdir != "":
                jobParameters += "--workdir {0} ".format(options.ctrWorkdir)
            if options.ctrDebug:
                jobParameters += "--debug "
        else:
            if not (options.noBuild and not options.noCompile):
                jobParameters += "-l ${LIB} "
            else:
                jobParameters += "-a {0} ".format(archiveName)
                jobParameters += "--sourceURL ${SURL} "
        jobParameters += "${TRN_OUTPUT:OUTPUT} "
        if options.mergeLog:
            jobParameters += "${TRN_LOG_MERGE:LOG_MERGE}"
        else:
            jobParameters += "${TRN_LOG:LOG}"
        taskParamMap["mergeSpec"] = {}
        taskParamMap["mergeSpec"]["useLocalIO"] = 1
        taskParamMap["mergeSpec"]["jobParameters"] = jobParameters
        taskParamMap["mergeOutput"] = True

        # check nGBPerJob
        if options.nGBPerMergeJob != "MAX":
            # convert to int
            try:
                options.nGBPerMergeJob = int(options.nGBPerMergeJob)
            except Exception:
                tmpLog.error("--nGBPerMergeJob must be an integer")
                sys.exit(EC_Config)
            # check negative
            if options.nGBPerMergeJob <= 0:
                tmpLog.error("--nGBPerMergeJob must be positive")
                sys.exit(EC_Config)
            taskParamMap["nGBPerMergeJob"] = options.nGBPerMergeJob

    #####################################################################
    # submission

    exitCode = 0
    dumpList = []

    # submit task
    for iSubmission, ioItem in enumerate(ioList):
        if options.verbose:
            print("== parameters ==")
            print("Site       : %s" % options.site)
            print("Athena     : %s" % athenaVer)
            if groupArea != "":
                print("Group Area : %s" % groupArea)
            if cacheVer != "":
                print("Cache      : %s" % cacheVer[1:])
            if nightVer != "":
                print("Nightly    : %s" % nightVer[1:])
            print("RunDir     : %s" % runDir)
            print("exec       : %s" % options.jobParams)

        if len(ioList) == 1:
            newTaskParamMap = taskParamMap
        else:
            # replace input and output
            options.inDS = ioItem["inDS"]
            options.outDS = ioItem["outDS"]
            newTaskParamMap = PsubUtils.replaceInputOutput(taskParamMap, ioItem["inDS"], ioItem["outDS"], iSubmission)
        exitCode = 0
        tmpStr = ""
        taskID = None
        # check outDS format
        if not dry_mode and not PsubUtils.checkOutDsName(
            options.outDS,
            options.official,
            nickName,
            options.mergeOutput,
            options.verbose,
        ):
            tmpStr = "invalid output datasetname:%s" % options.outDS
            tmpLog.error(tmpStr)
            exitCode = EC_Config
        # check task parameters
        if exitCode == 0:
            exitCode, tmpStr = PsubUtils.checkTaskParam(newTaskParamMap, options.unlimitNumOutputs)
        if exitCode != 0 and len(ioList) == 1:
            sys.exit(exitCode)
        if options.noSubmit:
            if options.verbose:
                tmpLog.debug("==== taskParams ====")
                tmpKeys = list(newTaskParamMap)
                tmpKeys.sort()
                for tmpKey in tmpKeys:
                    print("%s : %s" % (tmpKey, newTaskParamMap[tmpKey]))
        if options.dumpTaskParams is not None:
            with open(os.path.expanduser(options.dumpTaskParams), "w") as f:
                json.dump(newTaskParamMap, f)
        if get_taskparams:
            os.chdir(curDir)
            try:
                shutil.rmtree(tmpDir)
            except Exception:
                pass
            return newTaskParamMap
        if not options.noSubmit and exitCode == 0:
            tmpLog.info("submit {0}".format(options.outDS))
            status, tmpOut = Client.insertTaskParams(
                newTaskParamMap,
                options.verbose,
                properErrorCode=True,
                parent_tid=options.parentTaskID,
            )
            # result
            if status != 0:
                tmpStr = "task submission failed with {0}".format(status)
                tmpLog.error(tmpStr)
                exitCode = EC_Submit
            else:
                if tmpOut[0] in [0, 3]:
                    tmpStr = tmpOut[1]
                    tmpLog.info(tmpStr)
                    try:
                        m = re.search("jediTaskID=(\d+)", tmpStr)
                        taskID = int(m.group(1))
                    except Exception:
                        pass
                else:
                    tmpStr = "task submission failed. {0}".format(tmpOut[1])
                    tmpLog.error(tmpStr)
                    exitCode = EC_Submit
        dumpItem = copy.deepcopy(vars(options))
        dumpItem["returnCode"] = exitCode
        dumpItem["returnOut"] = tmpStr
        dumpItem["jediTaskID"] = taskID
        if len(ioList) > 1:
            dumpItem["bulkSeqNumber"] = iSubmission
        dumpList.append(dumpItem)

    # go back to current dir
    os.chdir(curDir)
    # dump
    if options.dumpJson is not None:
        with open(os.path.expanduser(options.dumpJson), "w") as f:
            json.dump(dumpList, f)
    # succeeded
    sys.exit(exitCode)
