"""Common CLI arguments and helper functions shared by pathena and prun"""

import argparse
import re
import sys

from pandaclient import MiscUtils

VALID_TRANSFER_TYPES = {"root", "direct", "davs", "file"}


def get_invalid_transfer_types(transfer_type_str):
    """Return the transfer types that are not allowed

    args:
        transfer_type_str: comma-separated transfer types, or None
    returns:
        set of transfer types not in VALID_TRANSFER_TYPES,
        or an empty set when transfer_type_str is None
    """
    if transfer_type_str is None:
        return set()
    return set(transfer_type_str.split(",")) - VALID_TRANSFER_TYPES


# Common arguments shared by pathena and prun, as (group_key, flags, kwargs[, shared_group_keys]) tuples.
# group_key is the name of the argparse group (as given to parser.add_group) where the argument is added.
# The optional shared_group_keys is a tuple of group keys where the argument is also shown in the help.
# Entries are added in this order, which determines the per-group help ordering.
_COMMON_ARGS = [
    (
        "submit",
        ["--useDirectIOSites"],
        dict(
            action="store_const",
            const=True,
            dest="useDirectIOSites",
            default=False,
            help="Use only sites which use directIO to read input files",
        ),
    ),
    (
        "submit",
        ["--transferType"],
        dict(
            action="store",
            dest="transferType",
            default=None,
            metavar="TYPE[,TYPE...]",
            help="Comma-separated transfer types to restrict input access. Allowed values: root, direct, davs, file. If not specified, all transfer types are allowed.",
        ),
    ),
    (
        "input",
        ["--forceStaged"],
        dict(
            action="store_const",
            const=True,
            dest="forceStaged",
            default=False,
            help="Force files from primary DS to be staged to local disk, even if direct-access is possible",
        ),
    ),
    (
        "job",
        ["--nEvents"],
        dict(
            action="store",
            dest="nEvents",
            default=-1,
            type=int,
            help="The total number of events to be processed. The nevents metadata of input files must be available in rucio",
        ),
    ),
    (
        "input",
        ["--nFiles", "--nfiles"],
        dict(
            action="store",
            dest="nFiles",
            default=0,
            type=int,
            help="Use a limited number of files in the input dataset",
        ),
    ),
    (
        "job",
        ["--nFilesPerJob"],
        dict(
            action="store",
            dest="nFilesPerJob",
            default=-1,
            type=int,
            help="Number of files on which each sub-job runs (default 50)",
        ),
    ),
    (
        "job",
        ["--nJobs", "--split"],
        dict(
            metavar="nJobs",
            action="store",
            dest="nJobs",
            default=-1,
            type=int,
            help="Maximum number of sub-jobs to be generated. If the number of input files (N_in) is less than nJobs*nFilesPerJob, only N_in/nFilesPerJob sub-jobs will be instantiated",
        ),
    ),
    (
        "job",
        ["--nEventsPerJob"],
        dict(
            action="store",
            dest="nEventsPerJob",
            default=-1,
            type=int,
            help="Number of events per subjob, mainly used for job splitting. If nEventsPerFile is set, the total number of subjobs is nEventsPerFile*nFiles/nEventsPerJob; otherwise the number of events per input file is retrieved from rucio and subjobs are created accordingly. "
            "Note that you need to explicitly specify in --trf (pathena) or --exec (prun) some parameters like %%MAXEVENTS, %%SKIPEVENTS and %%FIRSTEVENT and your application needs to process only an event chunk accordingly, to prevent subjobs from processing the same events",
        ),
    ),
    (
        "input",
        ["--nEventsPerFile"],
        dict(
            action="store",
            dest="nEventsPerFile",
            default=0,
            type=int,
            help="Number of events per file",
        ),
    ),
    (
        "input",
        ["--skipEmptyInput"],
        dict(
            action="store_const",
            const=True,
            dest="skipEmptyInput",
            default=False,
            help="Skip input files with zero events. The nevents metadata of input files must be available in rucio",
        ),
    ),
    (
        "job",
        ["--allowNoOutput"],
        dict(
            action="store",
            dest="allowNoOutput",
            default="",
            help="A comma-separated list of regexp patterns for output files which are allowed not to be produced. "
            "Jobs go to finished even if those files are not produced on WN. "
            "Patterns are evaluated at submission time against output filename templates such as user.X.$JEDITASKID._${SN/P}.out.root, "
            "and must match up to the end of the template, e.g. out.root or .*\\.root",
        ),
        ("output",),
    ),
    (
        "output",
        ["--mergeSingleFile"],
        dict(
            action="store_const",
            const=True,
            dest="mergeSingleFile",
            default=False,
            help="By default, the merge step simply renames a single input file to reduce memory and disk usage. This option forces execution of the user-provided merge script even for a single input file.",
        ),
    ),
    (
        "expert",
        ["--transPath"],
        dict(
            action="store",
            dest="transPath",
            default="",
            metavar="URL",
            help="Please don't use this option. Only for developers to override the transformation URL which is set by the server by default. "
            "e.g., http://pandaserver.cern.ch:25085/trf/user/runGen-00-00-02",
        ),
    ),
    (
        "expert",
        ["--mergeTransPath"],
        dict(
            action="store",
            dest="mergeTransPath",
            default="",
            metavar="URL",
            help="Please don't use this option. Only for developers to override the transformation URL for the merge step. Relevant only with --mergeOutput",
        ),
    ),
    (
        "job",
        ["--nGBPerJob"],
        dict(
            action="store",
            dest="nGBPerJob",
            default=-1,
            help="Instantiate one sub job per NGBPERJOB GB of input files. --nGBPerJob=MAX sets the size to the default maximum value",
        ),
    ),
    (
        "job",
        ["--nGBPerMergeJob"],
        dict(action="store", dest="nGBPerMergeJob", default="MAX", help="Instantiate one merge job per NGBPERMERGEJOB GB of pre-merged files"),
    ),
    (
        "submit",
        ["--site"],
        dict(
            action="store",
            dest="site",
            default="AUTO",
            help="Site name where jobs are sent. If omitted, jobs are automatically sent to sites where input is available. A comma-separated list of sites can be specified (e.g. siteA,siteB,siteC), so that best sites are chosen from the given site list. If AUTO is appended at the end of the list (e.g. siteA,siteB,siteC,AUTO), jobs are sent to any sites if input is not found in the previous sites",
        ),
    ),
    (
        "input",
        ["--notExpandInDS"],
        dict(
            action="store_const",
            const=True,
            dest="notExpandInDS",
            default=False,
            help="Allow jobs to use files across dataset boundaries in input dataset container",
        ),
    ),
    (
        "input",
        ["--inDsTxt"],
        dict(
            action="store",
            dest="inDsTxt",
            default="",
            help="A text file which contains the list of datasets to run over. Newlines are replaced by commas and the result is set to --inDS. Lines starting with # are ignored",
        ),
    ),
    (
        "input",
        ["--inOutDsJson"],
        dict(
            action="store",
            dest="inOutDsJson",
            default="",
            help="A json file to specify input and output datasets for bulk submission. "
            "It contains a json dump of [{'inDS': a comma-concatenated input dataset names, "
            "'outDS': output dataset name}, ...]. "
            "When this option is used --bulkSubmission is automatically set internally.",
        ),
        ("output",),
    ),
    (
        "input",
        ["--notExpandSecDSs"],
        dict(
            action="store_const", const=True, dest="notExpandSecDSs", default=False, help="Use files across dataset boundaries in secondary dataset containers"
        ),
    ),
    (
        "evtFilter",
        ["--goodRunListXML"],
        dict(action="store", dest="goodRunListXML", default="", help="Good Run List XML which will be converted to datasets by AMI"),
    ),
    (
        "evtFilter",
        ["--goodRunListDataType"],
        dict(action="store", dest="goodRunDataType", default="", help="specify data type when converting Good Run List XML to datasets, e.g, AOD (default)"),
    ),
    (
        "evtFilter",
        ["--goodRunListProdStep"],
        dict(
            action="store", dest="goodRunProdStep", default="", help="specify production step when converting Good Run List to datasets, e.g, merge (default)"
        ),
    ),
    (
        "evtFilter",
        ["--goodRunListDS"],
        dict(
            action="store",
            dest="goodRunListDS",
            default="",
            help=r'A comma-separated list of pattern strings. Datasets which are converted from Good Run List XML will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. If this option is omitted all datasets will be used',
        ),
        ("input",),
    ),
    (
        "evtFilter",
        ["--eventPickEvtList"],
        dict(action="store", dest="eventPickEvtList", default="", help="a file name which contains a list of runs/events for event picking"),
    ),
    (
        "evtFilter",
        ["--eventPickDataType"],
        dict(action="store", dest="eventPickDataType", default="", help="type of data for event picking. one of AOD,ESD,RAW"),
    ),
    (
        "evtFilter",
        ["--ei_api"],
        dict(action="store", dest="ei_api", default="", help="flag to signalise mc in event picking"),
    ),
    (
        "evtFilter",
        ["--eventPickStreamName"],
        dict(action="store", dest="eventPickStreamName", default="", help="stream name for event picking. e.g., physics_CosmicCaloEM"),
    ),
    (
        "evtFilter",
        ["--eventPickDS"],
        dict(
            action="store",
            dest="eventPickDS",
            default="",
            help=r'A comma-separated list of pattern strings. Datasets which are converted from the run/event list will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. e.g., data\*',
        ),
        ("input",),
    ),
    (
        "evtFilter",
        ["--eventPickAmiTag"],
        dict(
            action="store",
            dest="eventPickAmiTag",
            default="",
            help=r'AMI tag used to match TAG collections names. This option is required when you are interested in older data than the latest one. Either \ or "" is required when a wild-card is used. e.g., f2\*',
        ),
    ),
    (
        "evtFilter",
        ["--eventPickWithGUID"],
        dict(
            action="store_const",
            const=True,
            dest="eventPickWithGUID",
            default=False,
            help="Using GUIDs together with run and event numbers in eventPickEvtList to skip event lookup",
        ),
    ),
    (
        "print",
        ["--debugMode"],
        dict(
            action="store_const",
            const=True,
            dest="debugMode",
            default=False,
            help="Send the job with the debug mode on. If this option is specified the subjob will send stdout to the panda monitor every 5 min. The number of debug subjobs per user is limited. When this option is used and the quota has already exceeded, the panda server suppresses the option so that subjobs will run without the debug mode. If you submit multiple subjobs in a single job, only the first subjob will set the debug mode on. Note that you can turn the debug mode on/off by using pbook after jobs are submitted",
        ),
    ),
    (
        "output",
        ["--addNthFieldOfInDSToLFN"],
        dict(
            action="store",
            dest="addNthFieldOfInDSToLFN",
            default="",
            help="A middle name is added to LFNs of output files when they are produced from one dataset in the input container or input dataset list. The middle name is extracted from the dataset name. E.g., if --addNthFieldOfInDSToLFN=2 and the dataset name is data10_7TeV.00160387.physics_Muon..., 00160387 is extracted and LFN is something like user.hoge.TASKID.00160387.blah. Concatenate multiple field numbers with commas if necessary, e.g., --addNthFieldOfInDSToLFN=2,6.",
        ),
    ),
    (
        "output",
        ["--addNthFieldOfInFileToLFN"],
        dict(
            action="store",
            dest="addNthFieldOfInFileToLFN",
            default="",
            help="A middle name is added to LFNs of output files similarly as --addNthFieldOfInDSToLFN, but strings are extracted from input file names",
        ),
    ),
    (
        "output",
        ["--mergeOutput"],
        dict(action="store_const", const=True, dest="mergeOutput", default=False, help="merge output files"),
    ),
    (
        "output",
        ["--mergeLog"],
        dict(action="store_const", const=True, dest="mergeLog", default=False, help="merge log files. relevant only with --mergeOutput"),
    ),
    (
        "job",
        ["--mergeScript"],
        dict(action="store", dest="mergeScript", default="", help="Specify user-defined script or execution string for output merging"),
        ("output",),
    ),
    (
        "output",
        ["--outDS"],
        dict(action="store", dest="outDS", default="", help="Name of an output dataset. OUTDS will contain all output files"),
    ),
    (
        "output",
        ["--destSE"],
        dict(action="store", dest="destSE", default="", help="Destination storage element"),
    ),
    (
        "submit",
        ["--noEmail"],
        dict(action="store_const", const=True, dest="noEmail", default=False, help="Suppress email notification"),
    ),
    (
        "submit",
        ["--bulkSubmission"],
        dict(
            action="store_const",
            const=True,
            dest="bulkSubmission",
            default=False,
            help="Bulk submit tasks. When this option is used, --inOutDsJson is required while --inDS and --outDS are ignored. It is possible to use %%DATASET_IN and %%DATASET_OUT in --trf (pathena) or --exec (prun) which are replaced with actual dataset names when tasks are submitted, and %%BULKSEQNUMBER which is replaced with a sequential number of tasks in the bulk submission",
        ),
    ),
    (
        "build",
        ["--noCompile"],
        dict(
            action="store_const",
            const=True,
            dest="noCompile",
            default=False,
            help="Just upload a tarball in the build step to avoid the tighter size limit imposed by --noBuild. The tarball contains binaries compiled on your local computer, so that compilation is skipped in the build step on remote WN",
        ),
    ),
    (
        "submit",
        ["--memory"],
        dict(action="store", dest="memory", default=-1, type=int, help="Required memory size in MB per core. e.g., for 1GB per core --memory 1024"),
    ),
    (
        "submit",
        ["--fixedRamCount"],
        dict(action="store_const", const=True, dest="fixedRamCount", default=False, help="Use fixed memory size instead of estimated memory size"),
    ),
    (
        "submit",
        ["--outDiskCount"],
        dict(
            action="store",
            dest="outDiskCount",
            default=None,
            type=int,
            help="Expected output size in kB per 1 MB of input. The system automatically calculates this "
            "value using successful jobs and the value contains a safety offset (100kB). "
            "Use this option to disable it when jobs cannot have enough input files "
            "due to the offset",
        ),
    ),
    (
        "submit",
        ["--nCore"],
        dict(
            action="store",
            dest="nCore",
            default=-1,
            type=int,
            choices=[1, 8],
            help="The number of CPU cores. nCore=1 or 8. The number of available cores is defined in an environment variable, $ATHENA_CORE_NUMBER, on WNs, or %%CORE_NUMBER in --trf (pathena) or --exec (prun)",
        ),
    ),
    (
        "submit",
        ["--maxCore"],
        dict(
            action="store",
            dest="maxCore",
            default=None,
            type=int,
            help="The maximum number of CPU cores that a single job is allowed to utilize. Use this option to prevent jobs that do not scale efficiently with additional cores from occupying many core resources.",
        ),
    ),
    (
        "input",
        ["--avoidVP"],
        dict(action="store_const", const=True, dest="avoidVP", default=False, help="Not to use sites where virtual placement is enabled"),
    ),
    (
        "submit",
        ["--maxCpuCount"],
        dict(action="store", dest="maxCpuCount", default=0, type=int, help=argparse.SUPPRESS),
    ),
    (
        "expert",
        ["--noLoopingCheck"],
        dict(action="store_const", const=True, dest="noLoopingCheck", default=False, help="Disable looping job check"),
    ),
    (
        "output",
        ["--official"],
        dict(action="store_const", const=True, dest="official", default=False, help="Produce official dataset"),
    ),
    (
        "job",
        ["--unlimitNumOutputs"],
        dict(
            action="store_const",
            const=True,
            dest="unlimitNumOutputs",
            default=False,
            help="Remove the limit on the number of outputs. Note that having too many outputs per job causes a severe load on the system. You may be banned if you carelessly use this option",
        ),
        ("output",),
    ),
    (
        "output",
        ["--descriptionInLFN"],
        dict(
            action="store",
            dest="descriptionInLFN",
            default="",
            help="LFN is user.nickname.jobsetID.something (e.g. user.harumaki.12345.AOD._00001.pool) by default. This option allows users to put a description string into LFN. i.e., user.nickname.jobsetID.description.something",
        ),
    ),
    (
        "build",
        ["--excludeFile"],
        dict(
            action="store",
            dest="excludeFile",
            default="",
            help=r'specify a comma-separated string to exclude files and/or directories when gathering files in local working area. Either \ or "" is required when a wildcard is used. e.g., doc,\*.C',
        ),
    ),
    (
        "build",
        ["--gluePackages"],
        dict(
            action="store",
            dest="gluePackages",
            default="",
            help="list of glue packages which pathena cannot find due to empty i686-slc4-gcc34-opt. e.g., External/AtlasHepMC,External/Lhapdf",
        ),
    ),
    (
        "submit",
        ["--excludedSite"],
        dict(
            action="append",
            dest="excludedSite",
            default=[],
            help="A comma-separated list of sites which are not used for site section, "
            "e.g., ABC,OPQ*,XYZ which excludes ABC, XYZ, and OPQ<blah> due to the wildcard",
        ),
    ),
    (
        "submit",
        ["--noSubmit"],
        dict(action="store_const", const=True, dest="noSubmit", default=False, help="Don't submit jobs"),
    ),
    (
        "submit",
        ["--framework"],
        dict(action="store", dest="framework", default="", help="set framework used for task submission"),
    ),
    (
        "submit",
        ["--prodSourceLabel"],
        dict(action="store", dest="prodSourceLabel", default="", help="set prodSourceLabel"),
    ),
    (
        "submit",
        ["--workingGroup"],
        dict(action="store", dest="workingGroup", default=None, help="set workingGroup"),
    ),
    (
        "build",
        ["--tmpDir"],
        dict(action="store", dest="tmpDir", default="", help="Temporary directory where an archive file is created"),
    ),
    (
        "submit",
        ["--disableAutoRetry"],
        dict(action="store_const", const=True, dest="disableAutoRetry", default=False, help="disable automatic job retry on the server side"),
    ),
    (
        "input",
        ["--inputFileList"],
        dict(
            action="store",
            dest="inputFileList",
            default="",
            help="A local file which specifies names of files to be used in the input dataset. " "One filename per line in the local file",
        ),
    ),
    (
        "build",
        ["--voms"],
        dict(
            action="store",
            dest="vomsRoles",
            default=None,
            help="generate proxy with particular roles. e.g., atlas:/atlas/ca/Role=production,atlas:/atlas/fr/Role=pilot",
        ),
    ),
    (
        "output",
        ["--spaceToken"],
        dict(action="store", dest="spaceToken", default="", help="spacetoken for outputs. e.g., ATLASLOCALGROUPDISK"),
    ),
    (
        "expert",
        ["--expertOnly_skipScout"],
        dict(action="store_const", const=True, dest="skipScout", default=False, help=argparse.SUPPRESS),
    ),
    (
        "job",
        ["--respectSplitRule"],
        dict(action="store_const", const=True, dest="respectSplitRule", default=False, help="force scout jobs to follow split rules like nGBPerJob"),
    ),
    (
        "expert",
        ["--devSrv"],
        dict(
            action="store_const", const=True, dest="devSrv", default=False, help="Please don't use this option. Only for developers to use the dev panda server"
        ),
    ),
    (
        "expert",
        ["--intrSrv"],
        dict(
            action="store_const",
            const=True,
            dest="intrSrv",
            default=False,
            help="Please don't use this option. Only for developers to use the intr panda server",
        ),
    ),
    (
        "build",
        ["--outTarBall"],
        dict(action="store", dest="outTarBall", default="", help="Save a gzipped tarball of local files which is the input for buildXYZ"),
    ),
    (
        "build",
        ["--inTarBall"],
        dict(
            action="store",
            dest="inTarBall",
            default="",
            help="Use a gzipped tarball of local files as input for buildXYZ. Generally the tarball is created by using --outTarBall",
        ),
    ),
    (
        "build",
        ["--tarBallViaDDM"],
        dict(
            action="store",
            dest="tarBallViaDDM",
            default="",
            help="Use a gzipped tarball pre-uploaded to DDM as input for buildXYZ. --tarBallViaDDM=datasetName:fileName",
        ),
    ),
    (
        "input",
        ["--pfnList"],
        dict(action="store", dest="pfnList", default="", help="Name of file which contains a list of input PFNs. Those files can be un-registered in DDM"),
    ),
    (
        "build",
        ["--cmtConfig"],
        dict(
            action="store",
            dest="cmtConfig",
            default=None,
            help="CMTCONFIG is extracted from local environment variables when tasks are submitted, "
            "to set up the same environment on remote worker-nodes. "
            "This option allows to set up another CMTCONFIG "
            "remotely. e.g., --cmtConfig x86_64-slc5-gcc43-opt.",
        ),
    ),
    (
        "output",
        ["--allowTaskDuplication"],
        dict(
            action="store_const",
            const=True,
            dest="allowTaskDuplication",
            default=False,
            help="As a general rule each task has a unique outDS and history of file usage is recorded per task. This option allows multiple tasks to contribute to the same outDS. Typically useful to submit a new task with the outDS which was used by another broken task. Use this option very carefully at your own risk, since file duplication happens when the second task runs on the same input which the first task successfully processed",
        ),
    ),
    (
        "input",
        ["--skipFilesUsedBy"],
        dict(
            action="store",
            dest="skipFilesUsedBy",
            default="",
            help="A comma-separated list of TaskIDs. Files used by those tasks are skipped when running a new task",
        ),
    ),
    (
        "submit",
        ["--maxAttempt"],
        dict(action="store", dest="maxAttempt", default=-1, type=int, help="Maximum number of reattempts for each job (3 by default and not larger than 50)"),
    ),
    (
        "submit",
        ["-y"],
        dict(action="store_true", dest="is_confirmed", default=False, help="Answer yes for all questions"),
    ),
    (
        "containerJob",
        ["--containerImage"],
        dict(action="store", dest="containerImage", default="", help="Name of a container image"),
    ),
    (
        "containerJob",
        ["--architecture"],
        dict(
            action="store",
            dest="architecture",
            default="",
            help="Base OS platform, CPU, and/or GPU requirements. "
            "The format is @base_platform#CPU_spec&GPU_spec "
            "where base platform, CPU, or GPU spec can be omitted. "
            "If base platform is not specified it is automatically taken from "
            "$ALRB_USER_PLATFORM. "
            "CPU_spec = architecture<-vendor<-instruction set>>, "
            "GPU_spec = vendor<-model>. A wildcard can be used if there is no special "
            "requirement for the attribute. E.g., #x86_64-*-avx2&nvidia to ask for x86_64 "
            "CPU with avx2 support and nvidia GPU. "
            "GPU attributes can also be appended as colon-separated key=value pairs in the shorthand "
            "(e.g. #&nvidia:vram>=40960:driver>=575.0:model=.*A100.*); supported keys: vram, cuda, uarch, driver, model. "
            "Use = or == for exact match, != for model exclusion (e.g. model!=.*P100.* excludes P100 queues). "
            "This option also allows to specify a json-serialized dictionary. The gpu_spec supports: "
            'model (regexp, e.g. {"model": ".*A100.*"} to require an A100, or {"model": {"pattern": ".*P100.*", "excl": true}} to exclude P100 queues, case-insensitive); '
            'version (minimum CUDA version, e.g. {"version": ">=12.0"}); '
            'vram (GPU memory in MB as an operator-prefixed string, e.g. {"vram": ">=40960"} for at least 40 GB or {"vram": "==40960"} for exactly 40 GB); '
            'microarchitecture (GPU microarch generation, e.g. {"microarchitecture": "Ampere"} or {"microarchitecture": ["Ampere", "Hopper"]}); '
            'driver_version (NVIDIA kernel driver version, e.g. {"driver_version": ">=575.0"} for minimum or {"driver_version": "==575.51.03"} for exact). '
            "CRIC is used to identify GPU-capable queues; attribute checks (model, vram, microarchitecture, version, driver_version) use worker node GPU monitoring data. "
            "See https://panda-wms.readthedocs.io/en/latest/advanced/brokerage.html#checks-for-cpu-and-or-gpu-hardware",
        ),
    ),
    (
        "build",
        ["-3"],
        dict(action="store_true", dest="python3", default=False, help="Use python3"),
    ),
    (
        "input",
        ["--respectLB"],
        dict(action="store_const", const=True, dest="respectLB", default=False, help="To generate jobs respecting lumiblock boundaries"),
    ),
    (
        "expert",
        ["--queueData"],
        dict(action="store", dest="queueData", default="", help="Please don't use this option. Only for developers"),
    ),
    (
        "submit",
        ["--useNewCode"],
        dict(
            action="store_const",
            const=True,
            dest="useNewCode",
            default=False,
            help="When task are resubmitted with the same outDS, the original source code is used to re-run on failed/unprocessed files. This option uploads new source code so that jobs will run with new binaries",
        ),
    ),
    (
        "config",
        ["--dumpJson"],
        dict(
            action="store",
            dest="dumpJson",
            default=None,
            help="Dump all command-line parameters and submission result such as returnCode, returnOut, jediTaskID, and bulkSeqNumber if --bulkSubmission is used, to a json file",
        ),
    ),
    (
        "config",
        ["--parentTaskID"],
        dict(
            action="store",
            dest="parentTaskID",
            default=None,
            type=int,
            help="Specifies the parent task ID, allowing this task running concurrently with its parent task",
        ),
    ),
    (
        "submit",
        ["--priority"],
        dict(
            action="store",
            dest="priority",
            default=None,
            type=int,
            help="Set priority of the task (1000 by default). The value must be between 900 and 1100. "
            "Note that priorities of tasks are relevant only in "
            "each user's share, i.e., your tasks cannot jump over other user's tasks "
            "even if you give higher priorities.",
        ),
    ),
    (
        "submit",
        ["--osMatching"],
        dict(
            action="store_const",
            const=True,
            dest="osMatching",
            default=False,
            help="To let the brokerage choose sites which have the same OS as the local machine has.",
        ),
    ),
    (
        "job",
        ["--cpuTimePerEvent"],
        dict(
            action="store",
            dest="cpuTimePerEvent",
            default=-1,
            type=int,
            help="Expected HS06 seconds per event (~= 10 * the expected duration per event in seconds)",
        ),
    ),
    (
        "job",
        ["--fixedCpuTime"],
        dict(action="store_const", const=True, dest="fixedCpuTime", default=False, help="Use fixed cpuTime instead of estimated cpuTime"),
    ),
    (
        "job",
        ["--maxWalltime"],
        dict(
            action="store",
            dest="maxWalltime",
            default=0,
            type=int,
            help="Max walltime for each job in hours. Note that this option works only " "when the nevents metadata of input files are available in rucio",
        ),
    ),
]


def add_common_arguments(parser):
    """Register the common arguments onto the argparse groups of the parser

    Adds each entry in _COMMON_ARGS to the group selected by its group_key,
    and shares it with the groups selected by its optional shared_group_keys.
    The groups must have been created with parser.add_group before calling this function.

    args:
        parser: GroupArgParser of pathena or prun
    """
    groups = parser.groups_dict
    for group_key, flags, kwargs, *shared_group_keys in _COMMON_ARGS:
        action = groups[group_key.upper()].add_argument(*flags, **kwargs)
        for shared_group_key in shared_group_keys[0] if shared_group_keys else ():
            groups[shared_group_key.upper()].shareWithMe(action)


def set_n_files_from_n_jobs(options):
    """Translate --nJobs into nFiles / nFilesPerJob for file-based splitting

    Derives nFiles and nFilesPerJob from nJobs (together with nEventsPerJob,
    nEventsPerFile, and nFilesPerJob when available). Does nothing when nJobs
    is not set.

    args:
        options: parsed options namespace, modified in place
    """
    # translate --nJobs into nFiles / nFilesPerJob for file-based splitting
    if options.nJobs > 0:
        # set nFiles when nEventsPerJob and nEventsPerFile are set
        if options.nEventsPerJob > 0 and options.nEventsPerFile > 0:
            options.nFiles = (options.nEventsPerJob * options.nJobs) // options.nEventsPerFile
            if options.nFiles == 0:
                options.nFiles = 1

        # set nFiles when nFilesPerJob is set
        if options.nFilesPerJob > 0 and options.nFiles == 0:
            options.nFiles = options.nFilesPerJob * options.nJobs

        # set nFiles per job when nFiles is set
        if options.nFilesPerJob < 0 and options.nFiles > 0:
            options.nFilesPerJob = options.nFiles // options.nJobs
            if options.nFilesPerJob == 0:
                options.nFilesPerJob = 1


def load_json_options(options, key_aliases=None, special_keys=(), unknown_key_exit_code=1):
    """Overwrite options with the values in the --loadJson file

    Keys are option destinations, e.g. {"outDS": "user.hoge.test/", "noEmail": true}.
    Exits with unknown_key_exit_code when a key is not a known option.

    args:
        options: parsed options namespace, modified in place
        key_aliases: dict mapping JSON keys to option destinations when they differ,
            e.g. {"exec": "jobParams"} for prun
        special_keys: JSON keys which are not options and are returned to the caller as they are,
            e.g. "athena_args" for pathena
        unknown_key_exit_code: exit code when a key is not a known option
    returns:
        the loaded options as a command-line string to be appended to cliParams,
        and a dict with the values of special_keys found in the file
    """
    if key_aliases is None:
        key_aliases = {}
    json_exec_str = ""
    special_values = {}
    if options.loadJson is None:
        return json_exec_str, special_values
    load_opts = MiscUtils.decodeJSON(options.loadJson)
    for orig_key, value in load_opts.items():
        # numbers may be given as strings
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                pass
        if orig_key in special_keys:
            special_values[orig_key] = value
            continue
        key = key_aliases.get(orig_key, orig_key)
        if not hasattr(options, key):
            print(f"ERROR: unknown parameter {key} in {options.loadJson}")
            sys.exit(unknown_key_exit_code)
        setattr(options, key, value)
        # keep the JSON key in the command line, since it is the name the user wrote
        if value is True:
            json_exec_str += f" --{orig_key}"
        elif isinstance(value, str):
            json_exec_str += f" --{orig_key}='{value}'"
        else:
            json_exec_str += f" --{orig_key}={value}"
    if options.verbose:
        print("options after loading json")
        print(options)
        print("")
    return json_exec_str, special_values


def process_common_options(options, tmp_log):
    """Check and normalize the site, memory, and input options which pathena and prun handle the same way

    Exits with 0 when the user declines the warning about a large memory request.

    args:
        options: parsed options namespace, modified in place
        tmp_log: logger
    returns:
        list of sites when --site is a comma-separated list, otherwise None
    """
    # PsubUtils is imported lazily since it imports Client, which resolves the cache server via DNS
    from pandaclient import PsubUtils

    # warning for PQ
    PsubUtils.get_warning_for_pq(options.site, options.excludedSite, tmp_log)

    # warning for memory, which the user has to confirm unless --is_confirmed is used
    is_confirmed = PsubUtils.get_warning_for_memory(options.memory, options.is_confirmed, options.nCore, tmp_log)
    if not is_confirmed:
        sys.exit(0)

    # exclude sites
    if options.excludedSite != []:
        options.excludedSite = PsubUtils.splitCommaConcatenatedItems(options.excludedSite)

    # use certain sites. A list of sites is passed as included sites and the brokerage chooses among them
    included_site = None
    if re.search(",", options.site) is not None:
        included_site = PsubUtils.splitCommaConcatenatedItems([options.site])
        options.site = "AUTO"

    # list of output files which can be skipped
    options.allowNoOutput = options.allowNoOutput.split(",")

    # read datasets from file
    if options.inDsTxt != "":
        options.inDS = PsubUtils.readDsFromFile(options.inDsTxt)

    # not expand inDS when setting parent
    if options.parentTaskID:
        options.notExpandInDS = True

    return included_site


def check_split_options(options, tmp_log):
    """Check --nGBPerJob and that splitting by files, events, and file size are not combined

    Exits with EC_Config when the options are invalid.

    args:
        options: parsed options namespace, nGBPerJob is converted to int unless it is -1 or MAX
        tmp_log: logger
    """
    # check nGBPerJob. -1 means not set, and MAX leaves the size to the server
    if options.nGBPerJob not in [-1, "MAX"]:
        # convert to int
        try:
            options.nGBPerJob = int(options.nGBPerJob)
        except (TypeError, ValueError):
            tmp_log.error("--nGBPerJob must be an integer or MAX")
            sys.exit(MiscUtils.EC_Config)
        # check negative
        if options.nGBPerJob <= 0:
            tmp_log.error("--nGBPerJob must be positive")
            sys.exit(MiscUtils.EC_Config)
        # incompatible parameters
        if options.nFilesPerJob > 0:
            tmp_log.error("--nFilesPerJob and --nGBPerJob must be used exclusively")
            sys.exit(MiscUtils.EC_Config)

    # split options are mutually exclusive
    if options.nFilesPerJob > 0 and options.nEventsPerJob > 0 and options.nGBPerJob != -1:
        tmp_log.error("split by files, split by events and split by file size can not be used simultaneously")
        sys.exit(MiscUtils.EC_Config)
    if options.nEventsPerJob > 0 and options.nGBPerJob != -1:
        tmp_log.error("split by events and split by file size can not be used simultaneously")
        sys.exit(MiscUtils.EC_Config)


def get_bulk_io_list(options, tmp_log):
    """Make the list of input and output datasets, one item per task to submit

    --inOutDsJson enables bulk submission, i.e. one task per item in the JSON file,
    e.g. [{"inDS": "data.A", "outDS": "user.hoge.A/"}, {"inDS": "data.B", "outDS": "user.hoge.B/"}].
    Otherwise the list has only --inDS and --outDS. Exits with EC_Config when the options are inconsistent.

    args:
        options: parsed options namespace. With bulk submission, inDS and outDS are set to the first item
        tmp_log: logger
    returns:
        list of dicts with inDS and outDS
    """
    if options.inOutDsJson != "":
        options.bulkSubmission = True
    if not options.bulkSubmission:
        return [{"inDS": options.inDS, "outDS": options.outDS}]
    if options.inOutDsJson == "":
        tmp_log.error("--inOutDsJson is missing")
        sys.exit(MiscUtils.EC_Config)
    if options.eventPickEvtList != "":
        tmp_log.error("cannot use --eventPickEvtList and --inOutDsJson at the same time")
        sys.exit(MiscUtils.EC_Config)
    io_list = MiscUtils.decodeJSON(options.inOutDsJson)
    # use output dataset containers
    for io_item in io_list:
        if not io_item["outDS"].endswith("/"):
            io_item["outDS"] += "/"
    # the first task is used to check the options and to build the task parameters,
    # which are copied to the other tasks with their own datasets at submission
    options.inDS = io_list[0]["inDS"]
    options.outDS = io_list[0]["outDS"]
    return io_list


def check_input_options(options, tmp_log):
    """Check --dbRelease and the options which replace --inDS: good run lists and event picking

    Exits with EC_Config when the options are invalid.

    args:
        options: parsed options namespace
        tmp_log: logger
    """
    # check DBRelease
    if options.dbRelease != "" and (options.dbRelease.find(":") == -1 and options.dbRelease != "LATEST"):
        tmp_log.error("invalid argument for --dbRelease. Must be DatasetName:FileName or LATEST")
        sys.exit(MiscUtils.EC_Config)

    # Good Run List, which is converted to input datasets
    if options.goodRunListXML != "" and options.inDS != "":
        tmp_log.error("cannot use --goodRunListXML and --inDS at the same time")
        sys.exit(MiscUtils.EC_Config)

    # event picking, which makes the input dataset from a list of runs/events
    if options.eventPickEvtList != "" and options.inDS != "":
        tmp_log.error("cannot use --eventPickEvtList and --inDS at the same time")
        sys.exit(MiscUtils.EC_Config)

    # param check for event picking
    if options.eventPickEvtList != "" and options.eventPickDataType == "":
        tmp_log.error("--eventPickDataType must be specified")
        sys.exit(MiscUtils.EC_Config)


def process_sandbox_options(options):
    """Process the options about files in the sandbox: the merge script, glue packages, and excluded files

    Call it after options.extFile has been converted to a list.

    args:
        options: parsed options namespace, modified in place
    """
    # AthenaUtils is imported lazily since it imports Client, which resolves the cache server via DNS
    from pandaclient import AthenaUtils

    # user-specified merging script
    if options.mergeScript != "":
        # enable merging
        options.mergeOutput = True
        # add it to extFile so that it is shipped in the sandbox
        if options.mergeScript not in options.extFile:
            options.extFile.append(options.mergeScript)

    # glue packages
    options.gluePackages = options.gluePackages.split(",")
    try:
        options.gluePackages.remove("")
    except ValueError:
        pass

    # files which are not put in the sandbox
    AthenaUtils.setExcludeFile(options.excludeFile)


def parse_secondary_datasets(options, tmp_log):
    """Convert --secondaryDSs to a dict keyed by dataset name

    Exits with EC_Config when the option is malformed.

    args:
        options: parsed options namespace, secondaryDSs is replaced with the dict
        tmp_log: logger
    """
    tmp_stat, tmp_out = MiscUtils.parse_secondary_datasets_opt(options.secondaryDSs)
    if not tmp_stat:
        tmp_log.error(tmp_out)
        sys.exit(MiscUtils.EC_Config)
    options.secondaryDSs = tmp_out
