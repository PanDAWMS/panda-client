"""Common CLI arguments and helper functions shared by pathena and prun"""

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


# Common arguments shared by pathena and prun, as (group_key, flags, kwargs) tuples.
# group_key ("submit"/"input"/"job") selects the argparse group in add_common_arguments.
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
]


def add_common_arguments(group_submit, group_input, group_job):
    """Register the common arguments onto the argparse groups

    Adds each entry in _COMMON_ARGS to the group selected by its group_key.

    args:
        group_submit: argparse group for the "submit" group_key
        group_input: argparse group for the "input" group_key
        group_job: argparse group for the "job" group_key
    """
    groups = {"submit": group_submit, "input": group_input, "job": group_job}
    for group_key, flags, kwargs in _COMMON_ARGS:
        groups[group_key].add_argument(*flags, **kwargs)


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
    nEventsPerFile, and useRealNumEvents as derived from options.

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
