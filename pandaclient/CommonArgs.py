"""Common CLI arguments and helper functions shared by pathena and prun"""

import argparse

from pandaclient.MiscUtils import match_allow_no_output

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
        "submit",
        ["--express"],
        dict(
            action="store_const",
            const=True,
            dest="express",
            default=False,
            help="Send the job using express quota to have higher priority. The number of express subjobs in the queue and the total execution time used by express subjobs are limited (a few subjobs and several hours per day, respectively). This option is intended to be used for quick tests before bulk submission. Note that buildXYZ is not included in quota calculation. If this option is used when quota has already exceeded, the panda server will ignore the option so that subjobs have normal priorities. Also, if you submit 1 buildXYZ and N runXYZ subjobs when you only have quota of M (M < N), only the first M runXYZ subjobs will have higher priorities",
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


def get_allow_no_output_job_params(param_list):
    """Make job parameters to tell runGen/runAthena output files allowed not to be produced

    Collects filename templates of outputs flagged with allowNoOutput by
    MiscUtils.makeJediJobParam. The templates are resolved to real LFNs by JEDI.

    args:
        param_list: list of job parameter dicts including output templates
    returns:
        list with a constant job parameter for --allowNoOutput,
        or an empty list when no output matches --allowNoOutput
    """
    file_names = []
    for item in param_list:
        # skip regex outputs since they are not checked on WN
        if item.get("param_type") == "output" and item.get("allowNoOutput") and not item["value"].startswith("regex|"):
            if item["value"] not in file_names:
                file_names.append(item["value"])
    if not file_names:
        return []
    return [{"type": "constant", "value": f"--allowNoOutput={','.join(file_names)}"}]


def get_unmatched_allow_no_output_patterns(patterns, param_list):
    """Return --allowNoOutput patterns which match no output filename template

    args:
        patterns: list of --allowNoOutput patterns
        param_list: list of job parameter dicts including output templates
    returns:
        list of non-empty patterns which do not match any output filename template
    """
    templates = [item["value"] for item in param_list if item.get("param_type") == "output"]
    return [pattern for pattern in patterns if pattern != "" and not any(match_allow_no_output(pattern, template) for template in templates)]


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


def set_events_task_params(options, task_param_map, no_input=False):
    """Populate task parameters for event/file-based splitting

    Fills task_param_map with nEvents, nEventsPerJob, nFiles, nFilesPerJob,
    nEventsPerFile, useRealNumEvents, and skipEmptyInput as derived from options.

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
        no_input: True for jobs without input datasets, in which case the
            number of events is derived from --nEvents, --nJobs, and
            --nEventsPerJob rather than from input files
    """
    if no_input:
        # no input: derive nEventsPerJob / nEvents from --nEvents, --nJobs, --nEventsPerJob
        if options.nEvents > 0:
            task_param_map["nEvents"] = options.nEvents
            if options.nJobs > 0:
                task_param_map["nEventsPerJob"] = max(1, options.nEvents // options.nJobs)
            elif options.nEventsPerJob > 0:
                task_param_map["nEventsPerJob"] = options.nEventsPerJob
        elif options.nEventsPerJob > 0:
            task_param_map["nEvents"] = options.nEventsPerJob * max(1, options.nJobs)
            task_param_map["nEventsPerJob"] = options.nEventsPerJob
        else:
            task_param_map["nEvents"] = options.nJobs if options.nJobs > 0 else 1
            task_param_map["nEventsPerJob"] = 1
    else:
        # skip input files with zero events. The real number of events per file has to be
        # taken from rucio unless the user fixed it with --nEventsPerFile
        if options.skipEmptyInput:
            task_param_map["skipEmptyInput"] = True
            if options.nEventsPerFile <= 0:
                task_param_map["useRealNumEvents"] = True
        if options.nFiles > 0:
            task_param_map["nFiles"] = options.nFiles
        if options.nFilesPerJob > 0:
            task_param_map["nFilesPerJob"] = options.nFilesPerJob
        if options.nEventsPerJob > 0:
            task_param_map["nEventsPerJob"] = options.nEventsPerJob
            if options.nEventsPerFile <= 0:
                task_param_map["useRealNumEvents"] = True
        if options.nEventsPerFile > 0:
            task_param_map["nEventsPerFile"] = options.nEventsPerFile
        if options.nEvents > 0:
            task_param_map["nEvents"] = options.nEvents
        elif options.nJobs > 0 and options.nEventsPerJob > 0:
            task_param_map["nEvents"] = options.nJobs * options.nEventsPerJob
