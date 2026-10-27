"""Task parameter generation shared by pathena and prun"""

import copy
import re
import sys

from pandaclient import MiscUtils
from pandaclient.MiscUtils import match_allow_no_output


def set_common_task_params(options, task_param_map, processing_type, included_site, full_exec_string, archive_name):
    """Populate the task parameters which pathena and prun derive from options in the same way

    Tool-specific attributes such as vo, architecture, transHome, workingGroup, and sourceURL
    are left to the caller.

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
        processing_type: base processingType, suffixed here for event picking and good run lists
        included_site: list of sites to be included, or None
        full_exec_string: command line to be stored as cliParams
        archive_name: name of the sandbox archive, used as fixedSandbox with --useNewCode
    """
    # PsubUtils is imported lazily as in the callers, since it pulls in Client
    from pandaclient import PsubUtils

    # the task is named after the output dataset. Unless --allowTaskDuplication is used, the server
    # rejects a second task with the same outDS to avoid processing the same input twice
    task_param_map["taskName"] = options.outDS
    if not options.allowTaskDuplication:
        task_param_map["uniqueTaskName"] = True
    # developer option to override the transformation URL that the server sets by default
    if options.transPath != "":
        task_param_map["transPath"] = options.transPath
    # processingType tags the client and the workflow, e.g. -evp for event picking and -grl for good run lists.
    # Event picking waits for the input dataset, which is created asynchronously from the run/event list
    task_param_map["processingType"] = processing_type
    if options.eventPickEvtList != "":
        task_param_map["processingType"] += "-evp"
        task_param_map["waitInput"] = 1
    if options.goodRunListXML != "":
        task_param_map["processingType"] += "-grl"
    if options.framework != "":
        task_param_map["framework"] = options.framework
    # user analysis unless another label is requested, e.g. for tests
    if options.prodSourceLabel == "":
        task_param_map["prodSourceLabel"] = "user"
    else:
        task_param_map["prodSourceLabel"] = options.prodSourceLabel
    # sites: None lets the brokerage choose. The excluded and included lists restrict its choice
    if options.site != "AUTO":
        task_param_map["site"] = options.site
    else:
        task_param_map["site"] = None
    task_param_map["excludedSite"] = options.excludedSite
    if included_site is not None and included_site != []:
        task_param_map["includedSite"] = included_site
    else:
        task_param_map["includedSite"] = None
    # task priority within the user's share (1000 by default)
    if options.priority is not None:
        task_param_map["currentPriority"] = options.priority
    # one job per N GB of input files
    if options.nGBPerJob not in [-1, "MAX"]:
        # don't set MAX since it is the default on the server side
        task_param_map["nGBPerJob"] = options.nGBPerJob
    # splitting by files or events. Tasks without input (no inDS, PFN list or good run list) are split by events only
    no_input = options.inDS == "" and options.pfnList == "" and options.goodRunListXML == ""
    set_events_task_params(options, task_param_map, no_input)
    # the original command line, stored with the task
    task_param_map["cliParams"] = full_exec_string
    if options.noEmail:
        task_param_map["noEmail"] = True
    # skipScout: expert option to skip the scout jobs that measure memory, walltime, and output size.
    # respectSplitRule: force scout jobs to follow split rules like nGBPerJob
    if options.skipScout:
        task_param_map["skipScout"] = True
    if options.respectSplitRule:
        task_param_map["respectSplitRule"] = True
    # generate jobs respecting lumiblock boundaries
    if options.respectLB:
        task_param_map["respectLB"] = True
    # let the brokerage choose sites with the same OS as the local machine, which is described in osInfo
    if options.osMatching:
        task_param_map["osMatching"] = True
    task_param_map["osInfo"] = PsubUtils.get_os_information()
    # run concurrently with the parent task instead of waiting for it to finish
    if options.parentTaskID:
        task_param_map["noWaitParent"] = True
    if options.disableAutoRetry:
        task_param_map["disableAutoRetry"] = 1
    # produce official (group) datasets
    if options.official:
        task_param_map["official"] = True
    # when resubmitting with the same outDS, jobs normally reuse the original sandbox. --useNewCode uses the new one
    if options.useNewCode:
        task_param_map["fixedSandbox"] = archive_name
    if options.noLoopingCheck:
        task_param_map["noLoopingCheck"] = True
    # max walltime per job in hours. Works only when the number of events per input file is known in rucio
    if options.maxWalltime > 0:
        task_param_map["maxWalltime"] = options.maxWalltime
    # expected HS06 seconds per event. The server estimates it from scouts unless --fixedCpuTime is used
    if options.cpuTimePerEvent > 0:
        task_param_map["cpuTime"] = options.cpuTimePerEvent
        task_param_map["cpuTimeUnit"] = "HS06sPerEvent"
    if options.fixedCpuTime:
        task_param_map["cpuTimeUnit"] = "HS06sPerEventFixed"
    # memory in MB per core. The server estimates it from scouts unless --fixedRamCount is used
    if options.memory > 0:
        task_param_map["ramCount"] = options.memory
        if options.fixedRamCount:
            task_param_map["ramCountUnit"] = "MBPerCoreFixed"
        else:
            task_param_map["ramCountUnit"] = "MBPerCore"
    # expected output size in kB per MB of input, overriding the server estimate
    if options.outDiskCount is not None:
        task_param_map["outDiskCount"] = options.outDiskCount
        task_param_map["outDiskUnit"] = "kBFixed"
    # nCore: cores per job. maxCore: upper limit, for jobs which don't scale with more cores
    if options.nCore > 1:
        task_param_map["coreCount"] = options.nCore
    if options.maxCore and options.maxCore > 0:
        task_param_map["maxCoreCount"] = options.maxCore
    # skip input files already used by the given task IDs
    if options.skipFilesUsedBy != "":
        task_param_map["skipFilesUsedBy"] = options.skipFilesUsedBy
    # always set, so --respectSplitRule is effectively the default
    task_param_map["respectSplitRule"] = True
    # attempts per job, 3 by default on the server side and capped at 50
    if 0 < options.maxAttempt <= 50:
        task_param_map["maxAttempt"] = options.maxAttempt
    # jobs send their stdout to the monitor every few minutes, subject to a per-user quota
    if options.debugMode:
        task_param_map["debugMode"] = True
    # restrict input access to the given transfer types, e.g. root,davs
    if options.transferType is not None:
        task_param_map["transferType"] = options.transferType
    # don't use sites where virtual placement is enabled
    if options.avoidVP:
        task_param_map["avoidVP"] = True
    # middle name: add the Nth field of the input file or dataset name to output LFNs, e.g. the run number
    if options.addNthFieldOfInFileToLFN != "":
        task_param_map["addNthFieldToLFN"] = options.addNthFieldOfInFileToLFN
        task_param_map["useFileAsSourceLFN"] = True
    elif options.addNthFieldOfInDSToLFN != "":
        task_param_map["addNthFieldToLFN"] = options.addNthFieldOfInDSToLFN


def set_source_url(task_param_map, base_url):
    """Set sourceURL to the scheme and host of the given server URL

    args:
        task_param_map: task parameter dict, modified in place
        base_url: URL of the PanDA server
    """
    match_url = re.search("(http.*://[^/]+)/", base_url)
    if match_url is not None:
        task_param_map["sourceURL"] = match_url.group(1)


def set_log_task_params(options, task_param_map):
    """Set the log dataset, and the merged log dataset when --mergeOutput and --mergeLog are used

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
    """
    # the log container is outDS with .log appended, e.g. user.hoge.test/ -> user.hoge.test.log/
    log_dataset_name = re.sub("/$", ".log/", options.outDS)
    task_param_map["log"] = {
        "dataset": log_dataset_name,
        "container": log_dataset_name,
        "type": "template",
        "param_type": "log",
        "value": f"{log_dataset_name[:-1]}.$JEDITASKID.${{SN}}.log.tgz",
    }
    # with a middle name from input file names, the log LFN gets the middle name too
    if options.addNthFieldOfInFileToLFN != "":
        log_lfn = "{}.{}".format(*log_dataset_name.split(".")[:2])
        log_lfn += "${MIDDLENAME}.$JEDITASKID._${SN}.log.tgz"
        task_param_map["log"]["value"] = log_lfn
    # storage space token for the logs, e.g. ATLASLOCALGROUPDISK
    if options.spaceToken != "":
        task_param_map["log"]["token"] = options.spaceToken
    if options.mergeOutput and options.mergeLog:
        # log merge: merge jobs also merge the log tarballs into a separate .merge_log dataset
        merge_log_dataset_name = re.sub(r"\.log/", r".merge_log/", log_dataset_name)
        merge_log_lfn = re.sub(r"\.log\.tgz", r".merge_log.tgz", task_param_map["log"]["value"])
        data = copy.deepcopy(task_param_map["log"])
        data.update(
            {
                "dataset": merge_log_dataset_name,
                "container": merge_log_dataset_name,
                "param_type": "output",
                "mergeOnly": True,
                "value": merge_log_lfn,
            }
        )
        task_param_map["log_merge"] = data


def use_build_step(options):
    """Return True if the task has a build step

    A build step is skipped only with --noBuild without --noCompile.

    args:
        options: parsed options namespace
    returns:
        True if the build step is used
    """
    return not (options.noBuild and not options.noCompile)


def set_build_task_params(options, task_param_map, archive_name, job_parameters):
    """Set buildSpec for the build step, or tarBallViaDDM directly in the task without a build step

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
        archive_name: name of the sandbox archive
        job_parameters: tool-specific parameters of the build job. Ignored without a build step
    """
    # without a build step, jobs can still take the sandbox from a tarball pre-uploaded to DDM
    if not use_build_step(options):
        if options.tarBallViaDDM:
            task_param_map["tarBallViaDDM"] = options.tarBallViaDDM
        return
    task_param_map["buildSpec"] = {
        "prodSourceLabel": "panda",
        "archiveName": archive_name,
        "jobParameters": job_parameters,
    }
    # the build job takes the tarball from DDM instead of downloading the sandbox from the server
    if options.tarBallViaDDM:
        task_param_map["buildSpec"]["tarBallViaDDM"] = options.tarBallViaDDM
        task_param_map["buildSpec"]["jobParameters"] += "--noTarballDownload "
    if options.prodSourceLabel != "":
        task_param_map["buildSpec"]["prodSourceLabel"] = options.prodSourceLabel


def set_prepro_task_params(options, task_param_map):
    """Set preproSpec for the preprocessing step, which is used only with a good run list

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
    """
    if options.goodRunListXML == "":
        return
    # the preprocessing job converts the good run list XML to input datasets via AMI.
    # Data type (e.g. AOD), production step (e.g. merge), and dataset patterns narrow down the datasets
    job_parameters = f"--goodRunListXML {options.goodRunListXML} "
    if options.goodRunDataType != "":
        job_parameters += f"--goodRunListDataType {options.goodRunDataType} "
    if options.goodRunProdStep != "":
        job_parameters += f"--goodRunListProdStep {options.goodRunProdStep} "
    if options.goodRunListDS != "":
        job_parameters += f"--goodRunListDS {options.goodRunListDS} "
    job_parameters += "--sourceURL ${SURL} "
    task_param_map["preproSpec"] = {
        "prodSourceLabel": "panda",
        "jobParameters": job_parameters,
    }
    if options.prodSourceLabel != "":
        task_param_map["preproSpec"]["prodSourceLabel"] = options.prodSourceLabel


def set_merge_task_params(options, task_param_map, job_parameters, tmp_log):
    """Set mergeSpec and nGBPerMergeJob for --mergeOutput

    Exits with EC_Config when --nGBPerMergeJob is invalid.

    args:
        options: parsed options namespace, nGBPerMergeJob is converted to int
        task_param_map: task parameter dict, modified in place
        job_parameters: tool-specific parameters of the merge job, completed here with outputs and log
        tmp_log: logger
    """
    # placeholders which JEDI replaces with the pre-merged outputs and logs
    job_parameters += "${TRN_OUTPUT:OUTPUT} "
    if options.mergeLog:
        job_parameters += "${TRN_LOG_MERGE:LOG_MERGE}"
    else:
        job_parameters += "${TRN_LOG:LOG}"
    task_param_map["mergeSpec"] = {}
    task_param_map["mergeSpec"]["useLocalIO"] = 1
    task_param_map["mergeSpec"]["jobParameters"] = job_parameters
    # developer option to override the transformation URL of the merge step
    if options.mergeTransPath != "":
        task_param_map["mergeSpec"]["transPath"] = options.mergeTransPath
    task_param_map["mergeOutput"] = True
    # one merge job per N GB of pre-merged files. MAX leaves it to the server
    if options.nGBPerMergeJob != "MAX":
        # convert to int
        try:
            options.nGBPerMergeJob = int(options.nGBPerMergeJob)
        except (TypeError, ValueError):
            tmp_log.error("--nGBPerMergeJob must be an integer")
            sys.exit(MiscUtils.EC_Config)
        # check negative
        if options.nGBPerMergeJob <= 0:
            tmp_log.error("--nGBPerMergeJob must be positive")
            sys.exit(MiscUtils.EC_Config)
        task_param_map["nGBPerMergeJob"] = options.nGBPerMergeJob


def get_container_job_params(options):
    """Make runGen/runMerge parameters to run in a container without ALRB

    args:
        options: parsed prun options namespace
    returns:
        string of job parameters, empty when not running in a container or when using ALRB
    """
    if options.containerImage == "" or options.alrb:
        return ""
    # with ALRB the container is set up by the pilot, otherwise runGen/runMerge start it themselves
    job_parameters = f"--containerImage {options.containerImage} "
    # bind /cvmfs to the container
    if options.ctrCvmfs:
        job_parameters += "--cvmfs "
    # unset the X509 environment in the container
    if options.ctrNoX509:
        job_parameters += "--noX509 "
    # bind the job directory to this path for I/O
    if options.ctrDatadir != "":
        job_parameters += f"--datadir {options.ctrDatadir} "
    # chdir to this path in the container
    if options.ctrWorkdir != "":
        job_parameters += f"--workdir {options.ctrWorkdir} "
    # more verbose output from runcontainer
    if options.ctrDebug:
        job_parameters += "--debug "
    return job_parameters


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


def set_pfn_list_input_params(options, task_param_map):
    """Set the input for --pfnList. Files are given by PFN, so the task has no input dataset

    args:
        options: parsed options namespace
        task_param_map: task parameter dict, modified in place
    """
    # PsubUtils is imported lazily since it imports Client, which resolves the cache server via DNS
    from pandaclient import PsubUtils

    task_param_map["pfnList"] = PsubUtils.getListPFN(options.pfnList)
    # use noInput
    task_param_map["noInput"] = True
    if options.nFiles == 0:
        task_param_map["nFiles"] = len(task_param_map["pfnList"])
    task_param_map["jobParameters"] += [
        {
            "type": "constant",
            "value": '-i "${IN/T}"',
        },
    ]


def set_good_run_list_input_params(task_param_map):
    """Set the input for --goodRunListXML. The preprocessing job replaces the placeholders with datasets and files

    args:
        task_param_map: task parameter dict, modified in place
    """
    tmp_dict = {
        "type": "template",
        "param_type": "input",
        "value": '-i "${IN/T}"',
        "dataset": "%%INDS%%",
        "expand": True,
        "exclude": r"\.log\.tgz(\.\d+)*$",
        "files": "%%INLFNLIST%%",
    }
    task_param_map["jobParameters"].append(tmp_dict)
    task_param_map["dsForIN"] = "%%INDS%%"


def get_dbr_job_params(db_release, no_expand):
    """Make the job parameters for --dbRelease

    args:
        db_release: DatasetName:FileName or LATEST, or an empty string
        no_expand: True to pass --noExpandDBR, i.e. to use the DBRelease tarball without expanding it
    returns:
        list of job parameter dicts, empty without a DBRelease
    """
    if db_release == "":
        return []
    dbr_ds = db_release.split(":")[0]
    # change LATEST to DBR_LATEST
    if dbr_ds == "LATEST":
        dbr_ds = "DBR_LATEST"
    job_params = [
        {
            "type": "template",
            "param_type": "input",
            "value": "--dbrFile=${DBR}",
            "dataset": dbr_ds,
        }
    ]
    # no expansion
    if no_expand:
        job_params.append(
            {
                "type": "constant",
                "value": "--noExpandDBR",
            }
        )
    return job_params
