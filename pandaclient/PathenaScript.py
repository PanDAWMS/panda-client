import dataclasses
import json
import os
import pickle
import random
import re
import sys

from pandaclient import MiscUtils, PandaToolsPkgInfo, PathenaArgs
from pandaclient.CommonArgs import (
    VALID_TRANSFER_TYPES,
    check_input_options,
    check_pfn_list_options,
    check_scouting_options,
    check_split_options,
    get_bulk_io_list,
    get_invalid_transfer_types,
    load_json_options,
    parse_secondary_datasets,
    process_common_options,
    process_sandbox_options,
    set_n_files_from_n_jobs,
)
from pandaclient.task_param_utils import (
    get_allow_no_output_job_params,
    get_dbr_job_params,
    get_unmatched_allow_no_output_patterns,
    set_build_task_params,
    set_common_task_params,
    set_good_run_list_input_params,
    set_log_task_params,
    set_merge_task_params,
    set_pfn_list_input_params,
    set_prepro_task_params,
    set_source_url,
    use_build_step,
)

# AthenaUtils, Client, and PsubUtils are imported where they are used, after --version is handled, since
# importing Client resolves the panda cache host

# log files of the input datasets are not used as input
LOG_FILES_PATTERN = r"\.log\.tgz(\.\d+)*$"


#####################################################################
# options


def _check_unknown_args(args, tmp_log):
    """
    Exit with EC_Config for an option unknown to pathena among the positional arguments, i.e. the job options

    Arguments after -- are passed as they are
    """
    for arg in args:
        # separator
        if arg == "--":
            break
        if arg.startswith("-"):
            tmp_log.error(f"unrecognized argument: {arg}")
            sys.exit(MiscUtils.EC_Config)


def _apply_implied_options(options, tmp_log):
    """
    Handle --CA, and set the options implied by --noCompile and --containerImage

    Exits with 0 for --CA, which is run with --trf instead, and with EC_Config for --noBuild with --noCompile
    """
    # Athena --CA
    if options.componentAccumulator:
        tmp_log.info(
            "Please use --trf if you want to run athena.py --CA. See "
            "https://panda-wms.readthedocs.io/en/latest/client/pathena.html#running-arbitrary-executables-in-the-athena-runtime-environment"
        )
        sys.exit(0)

    # noCompile uses noBuild stuff
    if options.noCompile:
        if options.noBuild:
            tmp_log.error("--noBuild and --noCompile cannot be used simultaneously")
            sys.exit(MiscUtils.EC_Config)
        options.noBuild = True

    # set noBuild for container
    if options.containerImage != "":
        options.noBuild = True


def _check_and_normalize_options(options, tmp_log):
    """
    Check --outDS, --nJobs, and the PFN list, split, scouting, and input options, and set --useAMIEventLevelSplit

    Exits with EC_Config when an option is invalid
    """
    # the trailing / is added before, so the check never triggers
    if options.outDS == "":
        tmp_log.error("no outDS is given\n pathena [--inDS input] --outDS output myJobO.py")
        sys.exit(MiscUtils.EC_Config)
    if options.nJobs < -1:
        tmp_log.error("Number of jobs should be a positive integer")
        sys.exit(MiscUtils.EC_Config)

    # check PFN list options
    check_pfn_list_options(options, tmp_log)

    # absolute path for PFN list
    if options.pfnList != "":
        options.pfnList = os.path.realpath(options.pfnList)

    # check split options
    check_split_options(options, tmp_log)

    # check scouting options
    check_scouting_options(options, tmp_log)

    # AMI event-level split
    if options.useAMIEventLevelSplit is None:
        # use AMI for real data since the number of events per file is not uniform
        options.useAMIEventLevelSplit = options.inDS.startswith("data") or options.goodRunListXML != ""

    # check DBRelease, good run list, and event picking
    check_input_options(options, tmp_log)

    # event picking doesn't work with official transformations
    if options.eventPickEvtList != "" and options.trf != False:
        tmp_log.error("--eventPickEvtList doesn't work with --trf until official transformations support event picking")
        sys.exit(MiscUtils.EC_Config)


def _split_list_option(value):
    """Split a comma-separated option into a list, without the empty first item of an empty option"""
    items = value.split(",")
    if "" in items:
        items.remove("")
    return items


def _normalize_file_options(options):
    """
    Convert the file options into lists: --extFile, --extOutFile, --fileList with the files of --inputFileList, and --supStream

    --fileList patterns use * as wildcard, converted to regular expressions
    """
    from pandaclient import AthenaUtils

    # additional files
    options.extFile = _split_list_option(options.extFile)
    AthenaUtils.set_user_set_ext_files(options.extFile)

    options.extOutFile = _split_list_option(options.extOutFile.replace(" ", ""))

    # merge script, glue packages, and excluded files
    process_sandbox_options(options)

    # file list
    options.filelist = [file_name.replace("*", ".*") for file_name in options.filelist.split(",") if file_name != ""]

    # read file list from file
    if options.inputFileList != "":
        with open(options.inputFileList) as input_file_list:
            for line in input_file_list:
                line = line.replace("\n", "").strip()
                if line != "":
                    options.filelist.append(line)

    # suppressed streams
    options.supStream = _split_list_option(options.supStream.upper())


def _check_job_splitting(options, tmp_log):
    """
    Translate --nJobs into nFiles / nFilesPerJob, and check that the input can be split into jobs

    Exits with EC_Config when --nJobs is used with --inDS but without a unit to split the input
    """
    set_n_files_from_n_jobs(options)

    if options.inDS != "" and options.nJobs > 0 > options.nFilesPerJob and options.nFiles == 0 and options.nEventsPerJob < 0:
        tmp_log.error("--split requires --nFilesPerJob or --nFiles or --nEventsPerJob when --inDS is specified")
        sys.exit(MiscUtils.EC_Config)

    # remove whitespaces
    if options.inDS != "":
        options.inDS = options.inDS.replace(" ", "")

    if 0 < options.nFilesPerJob < 5:
        tmp_log.warning(
            "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )


def _make_paths_absolute(options):
    """Convert the paths of --inTarBall, --outTarBall, --inRunConfig, and --outRunConfig to absolute paths"""
    for option_name in ("inTarBall", "outTarBall", "inRunConfig", "outRunConfig"):
        path = getattr(options, option_name)
        if path != "":
            setattr(options, option_name, os.path.abspath(os.path.expanduser(path)))


#####################################################################
# work environment


@dataclasses.dataclass
class _WorkEnv:
    """Where pathena runs: the Athena setup and the run directory. The Athena fields are "" without Athena setup"""

    # Athena work area and group area
    work_area: str = ""
    group_area: str = ""
    # Athena release like Atlas-24.0.10, cache and nightly versions, from the Athena setup or --athenaTag
    athena_version: str = ""
    cache_version: str = ""
    nightly_version: str = ""
    # run directory relative to the work area, with a trailing /
    run_dir: str = "./"


def _resolve_work_env(options, current_dir, tmp_log):
    """
    Find the Athena setup and the run directory, and set options.cmtConfig

    args:
        options: parsed pathena options
        current_dir: directory where pathena was started
        tmp_log: logger
    returns:
        _WorkEnv. Exits with EC_CMT when the Athena setup is missing without --containerImage, or doesn't match
        --cmtConfig, and with EC_Config when current_dir is not under the work area
    """
    from pandaclient import AthenaUtils

    # get Athena versions
    cmt_verbose = options.verbose or options.containerImage == ""
    has_athena_env, athena_env = AthenaUtils.getAthenaVer(cmt_verbose)
    if not has_athena_env:
        if options.containerImage == "":
            sys.exit(MiscUtils.EC_CMT)
        # disable Athena checks when using container image without Athena runtime env
        athena_env = {
            "workArea": os.getcwd(),
            "athenaVer": "",
            "groupArea": "",
            "cacheVer": "",
            "nightVer": "",
            "cmtConfig": "",
        }

    work_env = _WorkEnv(
        work_area=athena_env["workArea"],
        group_area=athena_env["groupArea"],
        athena_version=f"Atlas-{athena_env['athenaVer']}" if athena_env["athenaVer"] else "",
        cache_version=athena_env["cacheVer"],
        nightly_version=athena_env["nightVer"],
    )

    # overwrite with athenaTag
    if options.athenaTag != "":
        work_env.athena_version, work_env.cache_version, work_env.nightly_version = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmp_log)

    # set CMTCONFIG
    options.cmtConfig = AthenaUtils.getCmtConfig(
        work_env.athena_version, work_env.cache_version, work_env.nightly_version, options.cmtConfig, verbose=options.verbose
    )

    # check CMTCONFIG
    if not AthenaUtils.checkCmtConfig(athena_env["cmtConfig"], options.cmtConfig, options.noBuild):
        sys.exit(MiscUtils.EC_CMT)

    tmp_log.info(f"using CMTCONFIG={options.cmtConfig}")

    # get run directory, removing special characters from the work area
    s_string = re.sub(r"[\+]", ".", work_env.work_area)
    run_dir = re.sub(f"^{s_string}", "", current_dir)
    if run_dir == current_dir and not AthenaUtils.useCMake() and options.containerImage == "":
        error_message = (
            f"You need to run pathena in a directory under {work_env.work_area}. "
            f"If '{work_env.work_area}' is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup."
        )
        tmp_log.error(error_message)
        sys.exit(MiscUtils.EC_Config)
    elif run_dir == "":
        run_dir = "."
    elif run_dir.startswith("/"):
        run_dir = run_dir[1:]
    work_env.run_dir = f"{run_dir}/"
    return work_env


def _request_event_picking(options, full_exec_string, current_dir, files_to_delete, tmp_log):
    """
    Ask the event picking service for a dataset with the events of --eventPickEvtList, and use it as input

    Without submission, the input dataset is "dummy". The run/event list is copied to a file shipped with the
    sandbox, which is removed at exit

    returns:
        path of the run/event list file shipped with the sandbox
    """
    from pandaclient import Client

    if not options.noSubmit:
        _, options.inDS = Client.requestEventPicking(
            options.eventPickEvtList,
            options.eventPickDataType,
            options.eventPickStreamName,
            options.eventPickDS,
            options.eventPickAmiTag,
            options.filelist,
            "",
            options.outDS,
            "pathena",
            full_exec_string,
            1,
            options.eventPickWithGUID,
            options.ei_api,
            options.verbose,
        )
    else:
        options.inDS = "dummy"
    tmp_log.info(f"requested Event Picking service to stage input as {options.inDS}")

    # make run/event list file for event picking
    run_event_file = f"{current_dir}/ep_{MiscUtils.wrappedUuidGen()}.dat"
    with open(options.eventPickEvtList) as ev_in, open(run_event_file, "w") as ev_out:
        ev_out.write(ev_in.read())
    files_to_delete.append(run_event_file)
    return run_event_file


#####################################################################
# job options and run configuration


def _get_job_options(options, args, tmp_log):
    """
    Return the job options: the --trf command, or the job options and -p of the command line

    Exits with EC_Config when there is none
    """
    job_options = ""
    if options.trf:
        # replace : to = for backward compatibility
        for placeholder in ["DB", "RNDM"]:
            options.trf = options.trf.replace(f"%{placeholder}:", f"%{placeholder}=")
        # use trf's parameters
        job_options = options.trf
    else:
        # get jobOs from command-line
        if options.preConfig != "":
            job_options += f"-p {options.preConfig} "
        if options.singleLine != "":
            options.singleLine = options.singleLine.replace('"', "'")
        for arg in args:
            job_options += f" {arg}"
    if job_options == "":
        tmp_log.error("no jobOptions is given\n   pathena [--inDS input] --outDS output myJobO.py")
        sys.exit(MiscUtils.EC_Config)
    return job_options


def _extract_run_config(options, job_options, tmp_dir, tmp_log):
    """
    Get the run configuration, from the job options with the Athena extractor, or from --inRunConfig

    The extracted configuration is saved in --outRunConfig

    returns:
        whether the extraction succeeded, and the run configuration
    """
    from pandaclient import AthenaUtils

    if options.inRunConfig == "":
        tmp_log.info("extracting run configuration")
        # run ConfigExtractor for normal job_options
        ret, run_config = AthenaUtils.extractRunConfig(
            job_options,
            options.supStream,
            options.shipinput,
            options.trf,
            verbose=options.verbose,
            useAMI=options.useAMIAutoConf,
            inDS=options.inDS,
            tmpDir=tmp_dir,
            one_liner=options.singleLine,
        )
        # save runconfig
        if options.outRunConfig != "":
            with open(options.outRunConfig, "w") as run_config_file:
                pickle.dump(run_config, run_config_file)
    else:
        # load from file
        ret = True
        with open(options.inRunConfig) as run_config_file:
            run_config = pickle.load(run_config_file)
    return ret, run_config


def _apply_run_config(options, run_config, extracted, job_options, tmp_log):
    """
    Add the files found by the extractor to the sandbox and the options, for job options run without --trf

    Shipped input files, generator files, and condition files are added to --extFile, --addPoolFC, and the
    shipped files of the run configuration. Outputs found by the extractor are removed from --extOutFile

    returns:
        job options, with -c for the one-liner. Exits with EC_Extractor when the extraction failed or a shipped
        input file is missing
    """
    if not extracted:
        sys.exit(MiscUtils.EC_Extractor)
    # shipped files
    if run_config.other.inputFiles:
        for file_name in run_config.other.inputFiles:
            # append .root for tag files
            if run_config.other.inColl and re.search(r"\.root(\.\d+)*$", file_name) is None:
                file_name = f"{file_name}.root"
            # check ship files in the current dir
            if not os.path.exists(file_name):
                tmp_log.error(f"{file_name} needs exist in the current directory when --shipInput is used")
                sys.exit(MiscUtils.EC_Extractor)
            options.extFile.append(file_name)
            if not run_config.input.shipFiles:
                run_config.input["shipFiles"] = []
            if file_name not in run_config.input["shipFiles"]:
                run_config.input["shipFiles"].append(file_name)
    # generator files
    if run_config.other.rndmGenFile:
        options.extFile += run_config.other.rndmGenFile
    # condition files
    if run_config.other.condInput:
        for file_name in run_config.other.condInput:
            if options.addPoolFC == "":
                options.addPoolFC = file_name
            else:
                options.addPoolFC += f",{file_name}"
    # set default ref name
    if not run_config.input.collRefName:
        run_config.input.collRefName = "Token"
    # check duplication in extOutFile
    if run_config.output.alloutputs != False:
        if options.verbose:
            tmp_log.debug(f"output files : {run_config.output.alloutputs!s}")
        for ext_out_file in tuple(options.extOutFile):
            if ext_out_file in run_config.output.alloutputs:
                options.extOutFile.remove(ext_out_file)
    # add -c
    if options.singleLine:
        job_options = f'-c "{options.singleLine}" {job_options}'
    return job_options


def _resolve_ami_tags(options, job_options, tmp_log):
    """
    Replace each ;-separated --trf command with AMI=tag by its full command from AMI

    Exits with EC_Config when AMI doesn't give the command
    """
    new_job_options = ""
    for trf_command in job_options.split(";"):
        if re.search(" AMI=", trf_command) is None:
            # use original command
            new_job_options += f"{trf_command};"
            continue
        tmp_log.info("getting configuration from AMI")
        # get configuration using GetCommand.py
        com = f"GetCommand.py {re.sub('^[^ ]+ ', '', trf_command.strip())}"
        if options.verbose:
            tmp_log.debug(com)
        ami_status, ami_output = MiscUtils.commands_get_status_output_with_env(com)
        ami_status %= 255
        if ami_status != 0:
            tmp_log.error(ami_output)
            tmp_log.error(
                "Failed to get configuration from AMI. "
                "Using AMI=tag in --trf is disallowed since it may overload the AMI server. "
                "Please use explicit configuration parameters in --trf"
            )
            sys.exit(MiscUtils.EC_Config)
        # get full command string, the last line which is not a comment
        full_command = ""
        for ami_line in ami_output.split("\n"):
            if ami_line != "" and not ami_line.startswith("#") and not ami_line.startswith("*"):
                full_command = ami_line
        if full_command == "":
            tmp_log.error(ami_output)
            tmp_log.error("Failed to extract configuration from AMI's output")
            sys.exit(MiscUtils.EC_Config)
        new_job_options += f"{full_command};"
    # remove redundant ;
    new_job_options = new_job_options[:-1]
    if new_job_options != "":
        job_options = new_job_options
        if options.verbose:
            tmp_log.debug(f"new job_options : {job_options}")
    return job_options


def _add_trf_outputs(options, job_options, tmp_log):
    """
    Add the outputs of the --trf command to --extOutFile, and the --argJSON file to --extFile

    The outputs are the %OUT. files, and the derivation outputs named after --outputDAODFile and --reductionConf
    or --formats, whose %OUT. is removed from the command

    returns:
        job options
    """
    one_out = False
    # replace ; for job sequence
    tmp_string = re.sub(";", " ", job_options)
    # look for --outputDAODFile/--outputD2AODFile and --reductionConf/formats
    match = re.search(r"(--outputDAODFile|--outputD2AODFile)[ =\"']+([^ \"',]+)", tmp_string)
    dxaod_output_filename = None
    if match is not None:
        dxaod_output_format = match.group(1)[8:][:-4]
        # remove %OUT
        dxaod_output_filename = re.sub(r"%OUT\.", "", match.group(2))
        match = re.search(r"(--reductionConf|--formats)[ =\"\']+([^ \"\']+)", tmp_string)
        if match is not None:
            # remove %OUT from outputDAODFile
            job_options = job_options.replace(f"%OUT.{dxaod_output_filename}", dxaod_output_filename)
            # make actual output names for derivation
            for reduction_item in match.group(2).split(","):
                reduction_item = reduction_item.strip()
                if reduction_item == "":
                    continue
                out_name = f"{dxaod_output_format}_{reduction_item}.{dxaod_output_filename}"
                if out_name not in options.extOutFile:
                    options.extOutFile.append(out_name)
                    one_out = True
    # look for %OUT
    for tmp_item in tmp_string.split():
        match = re.search("\\%OUT\\.([^ \"',]+)", tmp_item)
        if match:
            out_name = match.group(1)
            # skip basename of derivation
            if dxaod_output_filename is not None and dxaod_output_filename == out_name:
                continue
            if out_name not in options.extOutFile:
                options.extOutFile.append(out_name)
                one_out = True
    # look for --argjson
    match = re.search(r"--argjson[ =]+([^ ,;]+)", tmp_string, re.IGNORECASE)
    if match is not None:
        arg_json_file = match.group(1)
        if arg_json_file not in options.extFile:
            options.extFile.append(arg_json_file)
        options.useArgJson = True
    if not one_out:
        tmp_log.warning("argument of --trf doesn't contain any %OUT")
    return job_options


def _check_trf_events_and_cores(options, job_options, tmp_log):
    """Warn when the --trf command doesn't use the events per job or the cores of the job"""
    # check for maxEvents and skipEvents
    if 0 < options.nEventsPerJob < options.nEventsPerFile:
        if "%SKIPEVENTS" not in job_options:
            tmp_log.warning(
                "Argument of --trf doesn't contain %SKIPEVENTS. All jobs with the same input file "
                "may process the same events unless first events are skipped by using a trf parameter "
                "like skipEvents or something"
            )
        if "maxEvents" not in job_options:
            tmp_log.warning(
                "Argument of --trf doesn't contain maxEvents or something equivalent. Each job may process all events "
                "in the input file. Note that --nEventsPerJob doesn't automatically append maxEvents "
                "to the argument. Please ignore this message if you limit the number of events "
                "in each job by using another trf parameter"
            )
    # warning for multicore/thread
    if (options.nCore > 1 or options.nThreads > 1) and "--multiprocess" not in job_options and "--multithreaded" not in job_options:
        tmp_msg = "Neither --multiprocess or --multithreaded is set in --trf despite "
        if options.nCore > 1:
            tmp_msg += f"--nCore={options.nCore} "
        else:
            tmp_msg += f"--nThreads={options.nThreads} "
        tmp_msg += "which may cause inefficient CPU usage on MCORE resources"
        tmp_log.warning(tmp_msg)


def _check_outputs(options, run_config, tmp_log):
    """
    Check that the job has an output, set --extOutFile in the run configuration, and check the shipped files

    Exits with EC_Extractor when there is no output without --noOutput, or a shipped file is missing
    """
    # no output jobs
    output_keys = [key for key in run_config.output if key not in ("outUserData", "alloutputs")]
    if output_keys == [] and options.extOutFile == [] and not options.noOutput:
        error_message = "No output stream was extracted from jobOs or --trf. "
        if not options.trf:
            error_message += (
                "If your job defines an output without Athena framework (e.g., using ROOT.TFile.Open instead of THistSvc) "
                "please specify the output filename by using --extOutFile. Or if you define the output with a relatively new mechanism "
                "please report it to update the automatic extractor. "
            )
        error_message += "If you are sure that your job doesn't produce any output file (e.g., HelloWorldOptions.py) please use --noOutput. "
        tmp_log.error(error_message)
        sys.exit(MiscUtils.EC_Extractor)

    # set extOutFile to runConfig
    if options.extOutFile:
        run_config.output["extOutFile"] = options.extOutFile

    # check ship files in the current dir
    if not run_config.input.shipFiles:
        run_config.input.shipFiles = []
    for file_name in run_config.input.shipFiles:
        if not os.path.exists(file_name):
            tmp_log.error(f"{file_name} needs exist in the current directory when using --shipInput")
            sys.exit(MiscUtils.EC_Extractor)


def _set_random_seeds(options, run_config):
    """
    Set the initial seeds of the random streams and the G4 random seed of the run configuration

    The seeds are random, or asked to the user with --noRandom. The G4 random seed is -1 when not used
    """
    # random streams, two numbers each
    run_config.other["rndmNumbers"] = []
    if not run_config.other.rndmStream:
        run_config.other.rndmStream = []
    if len(run_config.other.rndmStream) != 0:
        if options.norandom:
            print()
            print("Initial random seeds need to be defined.")
            print("Enter two numbers for each random stream.")
            print("  e.g., PYTHIA : 4789899 989240512")
            print()
        for stream in run_config.other.rndmStream:
            if options.norandom:
                # enter manually
                while True:
                    numbers = input(f"{stream} : ").split()
                    if len(numbers) == 2:
                        break
                    print(" Two numbers are needed")
                run_config.other.rndmNumbers.append([int(numbers[0]), int(numbers[1])])
            else:
                # automatic
                run_config.other.rndmNumbers.append([random.randint(1, 5000000), random.randint(1, 5000000)])
        if options.norandom:
            print()

    # G4 random seed
    if not run_config.other.G4RandomSeeds:
        # set -1 to disable G4 Random Seeds
        run_config.other.G4RandomSeeds = -1
    elif options.norandom:
        print()
        print("Initial G4 random seeds need to be defined.")
        print("Enter one positive number.")
        print()
        # enter manually
        while True:
            number = input("SimFlags.SeedsG4=")
            try:
                number = int(number)
                if number > 0:
                    run_config.other.G4RandomSeeds = number
                    break
            except Exception:
                pass
        print()
    else:
        # automatic
        run_config.other.G4RandomSeeds = random.randint(1, 10000)


def _resolve_extra_input_datasets(options, run_config, job_options, tmp_log):
    """
    Get the datasets of the minimum bias, cavern, beam halo, and beam gas streams used by the job, and parse --secondaryDSs

    A stream is used when the extractor found it in the job options, or when its placeholder is in --trf. Datasets
    and numbers of files not given with the options are asked to the user
    """
    # placeholders of --trf for the streams
    for placeholder, stream_flag in (
        ("%MININ", "inMinBias"),
        ("%LOMBIN", "inLoMinBias"),
        ("%HIMBIN", "inHiMinBias"),
        ("%CAVIN", "inCavern"),
        ("%BHIN", "inBeamHalo"),
        ("%BGIN", "inBeamGas"),
    ):
        if options.trf and job_options.find(placeholder) != -1:
            run_config.input[stream_flag] = True

    # minimum bias datasets
    if run_config.input.inMinBias:
        options.minDS, options.nMin = MiscUtils.getDatasetNameAndNumFiles(options.minDS, options.nMin, "Minimum-Bias")
    if run_config.input.inLoMinBias:
        options.lowMinDS, options.nLowMin = MiscUtils.getDatasetNameAndNumFiles(options.lowMinDS, options.nLowMin, "Low pT Minimum-Bias")
    if run_config.input.inHiMinBias:
        options.highMinDS, options.nHighMin = MiscUtils.getDatasetNameAndNumFiles(options.highMinDS, options.nHighMin, "High pT Minimum-Bias")
    # cavern dataset
    if run_config.input.inCavern:
        options.cavDS, options.nCav = MiscUtils.getDatasetNameAndNumFiles(options.cavDS, options.nCav, "Cavern")
    # beam halo datasets, common or for each side
    if run_config.input.inBeamHalo:
        if options.useCommonHalo:
            options.beamHaloDS, options.nBeamHalo = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloDS, options.nBeamHalo, "BeamHalo")
        else:
            options.beamHaloADS, options.nBeamHaloA = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloADS, options.nBeamHaloA, "BeamHalo A-side")
            options.beamHaloCDS, options.nBeamHaloC = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloCDS, options.nBeamHaloC, "BeamHalo C-side")
    # beam gas datasets, common or for each gas
    if run_config.input.inBeamGas:
        if options.useCommonGas:
            options.beamGasDS, options.nBeamGas = MiscUtils.getDatasetNameAndNumFiles(options.beamGasDS, options.nBeamGas, "BeamGas")
        else:
            options.beamGasHDS, options.nBeamGasH = MiscUtils.getDatasetNameAndNumFiles(options.beamGasHDS, options.nBeamGasH, "BeamGas Hydrogen")
            options.beamGasCDS, options.nBeamGasC = MiscUtils.getDatasetNameAndNumFiles(options.beamGasCDS, options.nBeamGasC, "BeamGas Carbon")
            options.beamGasODS, options.nBeamGasO = MiscUtils.getDatasetNameAndNumFiles(options.beamGasODS, options.nBeamGasO, "BeamGas Oxygen")

    # general secondaries
    parse_secondary_datasets(options, tmp_log)


#####################################################################
# sandbox


def create_sandbox_archive(options, job_options, work_env, current_dir, tmp_dir, tmp_log):
    """
    Archive the sources, or only the job options without build step, and the InstallArea into a gzipped tarball

    Job options given with absolute paths are archived with unique names. Changes the working directory to tmp_dir

    returns:
        name of the gzipped tarball in tmp_dir
    """
    from pandaclient import AthenaUtils, PsubUtils

    # extract jobOs with full pathnames, with random names to avoid overwriting
    for tmp_item in job_options.split():
        if re.search(r"^/.*\.py$", tmp_item) is not None:
            AthenaUtils.fullPathJobOs[tmp_item] = f"{MiscUtils.wrappedUuidGen()}_{tmp_item.split('/')[-1]}"

    # copy some athena specific files
    AthenaUtils.copyAthenaStuff(current_dir)

    # set extFile
    AthenaUtils.setExtFile(options.extFile)

    archive_name = ""
    if not (options.noBuild and not options.noCompile):
        # archive with cpack
        if AthenaUtils.useCMake():
            archive_name, archive_full_name = AthenaUtils.archiveWithCpack(True, tmp_dir, options.verbose)
        # archive sources
        archive_name, archive_full_name = AthenaUtils.archiveSourceFiles(
            work_env.work_area,
            work_env.run_dir,
            current_dir,
            tmp_dir,
            options.verbose,
            options.gluePackages,
            archiveName=archive_name,
        )
    else:
        # archive with cpack
        if AthenaUtils.useCMake():
            archive_name, archive_full_name = AthenaUtils.archiveWithCpack(False, tmp_dir, options.verbose)
        # archive job_options
        archive_name, archive_full_name = AthenaUtils.archiveJobOFiles(
            work_env.work_area,
            work_env.run_dir,
            current_dir,
            tmp_dir,
            options.verbose,
            archiveName=archive_name,
        )

    # archive InstallArea
    AthenaUtils.archiveInstallArea(
        work_env.work_area,
        work_env.group_area,
        archive_name,
        archive_full_name,
        tmp_dir,
        options.noBuild,
        options.verbose,
    )
    # back to tmp dir
    os.chdir(tmp_dir)
    # remove some athena specific files
    AthenaUtils.deleteAthenaStuff(current_dir)
    # compress and check the sandbox
    return PsubUtils.finalize_sandbox(archive_name, options.verbose, tmp_log)


#####################################################################
# job parameters


def _pre_exec_job_params(options, run_config):
    """Job parameters for -f, the Python executed before the job options: the random seeds, and the events to process per job"""
    from pandaclient import PsubUtils

    job_params = []
    # random seeds
    pre_exec_seeds = ""
    if run_config.other.rndmStream != []:
        seeds = "".join(
            f"'{stream} ${{RNDMSEED}} {run_config.other.rndmNumbers[run_config.other.rndmStream.index(stream)][1]}'," for stream in run_config.other.rndmStream
        )
        pre_exec_seeds = f"AtRndmGenSvc=Service('AtRndmGenSvc');AtRndmGenSvc.Seeds=[{seeds}]"
        job_params.append(
            {
                "type": "template",
                "param_type": "number",
                "value": "${RNDMSEED}",
                "hidden": True,
                "offset": run_config.other.rndmStream[0][0],
            }
        )
    # split by event option was invoked
    pre_exec_events = ""
    if options.nEventsPerJob > 0 and (not options.trf):
        # number of events to be processed per job, skipping the events processed by the other jobs of the file
        pre_exec_events = "theApp.EvtMax=${MAXEVENTS}"
        if not run_config.input.noInput:
            pre_exec_events += ";EventSelector.SkipEvents=${SKIPEVENTS}"
    # set pre execution parameter
    pre_exec = ";".join(item for item in (pre_exec_seeds, pre_exec_events) if item != "")
    if pre_exec != "":
        job_params.append({"type": "constant", "value": '-f "', "padding": False})
        job_params += PsubUtils.convertParamStrToJediParam(pre_exec, {}, "", False, False)
        job_params.append({"type": "constant", "value": '"'})
    return job_params


def _runtime_args(options, run_config, run_dir, event_pick_file):
    """Arguments of the runAthena job script"""
    from pandaclient import AthenaUtils

    conditional_args = [
        (options.addPoolFC != "", f"--addPoolFC {options.addPoolFC}"),
        # disable to skip missing files
        (options.notSkipMissing, "--notSkipMissing"),
        # given PFN
        (options.pfnList != "", "--givenPFN"),
        (options.trf, "--trf"),
        (options.useArgJson, "--useArgJson"),
        (options.generalInput, "--generalInput"),
        # use theApp.nextEvent
        (options.useNextEvent, "--useNextEvent"),
        (AthenaUtils.useCMake() or options.containerImage != "", "--useCMake"),
        # AthenaMT
        (options.nThreads > 1, "--useAthenaMT"),
        (options.codeTrace, "--codeTrace"),
        # debug parameters
        (options.queueData != "", f"--overwriteQueuedata={options.queueData}"),
        # read BS
        (run_config.input.inBS, "-b"),
        # use back navigation
        (run_config.input.backNavi, "-e"),
        (options.shipinput, "--shipInput"),
        (event_pick_file is not None, f"--eventPickTxt={event_pick_file.split('/')[-1] if event_pick_file else ''}"),
    ]
    args = f"--sourceURL ${{SURL}} -r {run_dir} "
    args += "".join(f"{arg} " for condition, arg in conditional_args if condition)
    return args


def _input_job_params(options, task_param_map, input_map):
    """Add the job parameters of the primary input: --inDS, the PFN list, the good run list, or no input"""
    if options.inDS != "":
        input_param = {
            "type": "template",
            "param_type": "input",
            "value": '-i "${IN/T}"',
            "dataset": options.inDS,
            "expand": True,
            "exclude": LOG_FILES_PATTERN,
        }
        if options.notExpandInDS:
            del input_param["expand"]
            input_param["consolidate"] = f"{'.'.join(options.outDS.split('.')[:2])}.{MiscUtils.wrappedUuidGen()}/"
        if options.inputType != "":
            input_param["include"] = options.inputType
        if options.filelist:
            input_param["files"] = options.filelist
        task_param_map["jobParameters"].append(input_param)
        task_param_map["dsForIN"] = options.inDS
        input_map["IN"] = options.inDS
    elif options.pfnList != "":
        set_pfn_list_input_params(options, task_param_map)
    elif options.goodRunListXML != "":
        set_good_run_list_input_params(task_param_map)
        input_map["IN"] = "%%INDS%%"
    else:
        # no input
        task_param_map["noInput"] = True
        task_param_map["jobParameters"].append({"type": "constant", "value": '-i "[]"'})


def _add_stream_input(task_param_map, input_map, stream_name, value, dataset, n_files, input_map_dataset=None, **kwargs):
    """
    Add the job parameter of an extra input stream, with the given number of files per input file of the job

    args:
        task_param_map: task parameters
        input_map: stream name to dataset, extended with the stream
        stream_name: name of the stream
        value: value of the job parameter
        dataset: dataset of the stream
        n_files: number of files of the stream per input file
        input_map_dataset: dataset recorded in input_map, if different from dataset
        kwargs: more arguments of MiscUtils.makeJediJobParam
    """
    task_param_map["jobParameters"] += MiscUtils.makeJediJobParam(
        value, dataset, "input", exclude=LOG_FILES_PATTERN, nFilesPerJob=n_files, useNumFilesAsRatio=True, **kwargs
    )
    input_map[stream_name] = dataset if input_map_dataset is None else input_map_dataset


def _extra_input_job_params(options, task_param_map, input_map):
    """Add the job parameters of the minimum bias, cavern, beam halo, and beam gas streams"""
    # minimum bias streams, given to the job with -m
    min_bias_streams = []
    for stream_name, dataset, n_files, not_expand in (
        ("MININ", options.minDS, options.nMin, options.notExpandMinDS),
        ("LOMBIN", options.lowMinDS, options.nLowMin, options.notExpandLowMinDS),
        ("HIMBIN", options.highMinDS, options.nHighMin, options.notExpandHighMinDS),
    ):
        if dataset == "":
            continue
        _add_stream_input(
            task_param_map,
            input_map,
            stream_name,
            f"${{{stream_name}}}",
            dataset,
            n_files,
            hidden=True,
            expand=not not_expand,
            randomAtt=options.randomMin,
            outDS=options.outDS,
        )
        min_bias_streams.append(stream_name)
        if options.sameSecRetry:
            task_param_map["reuseSecOnDemand"] = True
    if min_bias_streams:
        task_param_map["jobParameters"].append({"type": "constant", "value": f'-m "${{{",".join(min_bias_streams)}/T}}"'})

    # cavern
    if options.cavDS != "":
        _add_stream_input(
            task_param_map,
            input_map,
            "CAVIN",
            '-n "${CAVIN/T}"',
            options.cavDS,
            options.nCav,
            expand=not options.notExpandCavDS,
            randomAtt=options.randomCav,
            outDS=options.outDS,
        )
        if options.sameSecRetry:
            task_param_map["reuseSecOnDemand"] = True

    # beam halo streams, given to the job with --beamHalo
    beam_halo_streams = []
    for stream_name, dataset, n_files in (
        ("BHIN", options.beamHaloDS, options.nBeamHalo),
        ("BHAIN", options.beamHaloADS, options.nBeamHaloA),
        ("BHCIN", options.beamHaloCDS, options.nBeamHaloC),
    ):
        if dataset == "":
            continue
        _add_stream_input(task_param_map, input_map, stream_name, f"${{{stream_name}}}", dataset, n_files, hidden=True, expand=True)
        beam_halo_streams.append(stream_name)
        if options.sameSecRetry:
            task_param_map["reuseSecOnDemand"] = True
    if beam_halo_streams:
        task_param_map["jobParameters"].append({"type": "constant", "value": f'--beamHalo "${{{",".join(beam_halo_streams)}/T}}"'})

    # beam gas streams, given to the job with --beamGas
    beam_gas_streams = []
    for stream_name, dataset, n_files, input_map_dataset in (
        ("BGIN", options.beamGasDS, options.nBeamGas, None),
        ("BGHIN", options.beamGasHDS, options.nBeamGasH, None),
        # the input map has the hydrogen dataset for the carbon stream, as it always had
        ("BGCIN", options.beamGasCDS, options.nBeamGasC, options.beamGasHDS),
        ("BGOIN", options.beamGasODS, options.nBeamGasO, None),
    ):
        if dataset == "":
            continue
        _add_stream_input(
            task_param_map, input_map, stream_name, f"${{{stream_name}}}", dataset, n_files, input_map_dataset=input_map_dataset, hidden=True, expand=True
        )
        beam_gas_streams.append(stream_name)
    if beam_gas_streams:
        task_param_map["jobParameters"].append({"type": "constant", "value": f'--beamGas "${{{",".join(beam_gas_streams)}/T}}"'})


def _secondary_job_params(options, task_param_map, input_map):
    """Add the job parameters of the --secondaryDSs streams, given to the job with -m"""
    if not options.secondaryDSs:
        return
    for dataset_name, secondary in options.secondaryDSs.items():
        stream_name = secondary["streamName"]
        task_param_map["jobParameters"] += MiscUtils.makeJediJobParam(
            f"${{{stream_name}}}",
            dataset_name,
            "input",
            hidden=True,
            expand=not options.notExpandSecDSs,
            include=secondary["pattern"],
            offset=secondary["nSkip"],
            nFilesPerJob=secondary["nFiles"],
            outDS=options.outDS,
            file_list=secondary["files"],
        )
        input_map[stream_name] = dataset_name
    stream_names = ",".join(secondary["streamName"] for secondary in options.secondaryDSs.values())
    task_param_map["jobParameters"].append({"type": "constant", "value": f'-m "${{{stream_names}/T}}"'})
    task_param_map["reuseSecOnDemand"] = True


def _output_job_params(options, run_config, task_param_map, tmp_log):
    """
    Add the job parameters of the outputs found in the run configuration and given with --extOutFile

    returns:
        map of the outputs, as given to the job with -o
    """
    from pandaclient import AthenaUtils

    description_in_lfn = "${MIDDLENAME}" if options.addNthFieldOfInDSToLFN != "" or options.addNthFieldOfInFileToLFN != "" else ""
    out_map, output_params = AthenaUtils.convertConfToOutput(
        run_config,
        options.extOutFile,
        options.outDS,
        destination=options.destSE,
        spaceToken=options.spaceToken,
        descriptionInLFN=description_in_lfn,
        allowNoOutput=options.allowNoOutput,
        appendFileFieldToStreamName=options.appendStrToExtStream or options.useFileFieldAsStream,
        useEXTStreamName=not options.useFileFieldAsStream,
        extOutStreams=options.outputStreamNames,
    )
    task_param_map["jobParameters"].append({"type": "constant", "value": f'-o "{out_map}" '})
    task_param_map["jobParameters"] += output_params
    # output files allowed not to be produced
    task_param_map["jobParameters"] += get_allow_no_output_job_params(output_params)
    for pattern in get_unmatched_allow_no_output_patterns(options.allowNoOutput, output_params):
        tmp_log.warning(
            f"--allowNoOutput pattern '{pattern}' matches none of output filename templates "
            f"{[item['value'] for item in output_params if item.get('param_type') == 'output']}"
        )
    return out_map


def _job_options_job_params(options, run_config, job_options, out_map, input_map, task_param_map):
    """
    Add the job parameters of -j, the job options or the --trf command

    Job options given with absolute paths are replaced with their names in the sandbox, and the G4 random seed is
    set in the one-liner. In the --trf command, %OUT. is removed from the outputs, and %DB= is replaced with ${DBR}
    """
    from pandaclient import AthenaUtils, PsubUtils

    job_options_param = job_options
    if not options.trf:
        # replace full-path jobOs
        for full_name, local_name in AthenaUtils.fullPathJobOs.items():
            job_options_param = re.sub(full_name, local_name, job_options_param)
        # modify one-liner for G4 random seeds
        if run_config.other.G4RandomSeeds > 0:
            if options.singleLine != "":
                job_options_param = re.sub(
                    f'-c "{options.singleLine}" ',
                    f'-c "{options.singleLine};from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${{RNDMSEED}}" ',
                    job_options_param,
                )
            else:
                job_options_param = f'-c "from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${{RNDMSEED}}" {job_options_param}'
            task_param_map["jobParameters"].append(
                {
                    "type": "template",
                    "param_type": "number",
                    "value": "${RNDMSEED}",
                    "hidden": True,
                    "offset": run_config.other.G4RandomSeeds,
                }
            )
    else:
        # output : basenames are in out_map['IROOT'] through extOutFile
        for name, _ in out_map.get("IROOT", []):
            job_options_param = job_options_param.replace(f"%OUT.{name}", name)
        # replace DBR
        job_options_param = re.sub("%DB=[^ '\";]+", "${DBR}", job_options_param)

    task_param_map["jobParameters"].append({"type": "constant", "value": '-j "', "padding": False})
    extra_in_list = [secondary["streamName"] for secondary in options.secondaryDSs.values()] if options.secondaryDSs else []
    task_param_map["jobParameters"] += PsubUtils.convertParamStrToJediParam(
        job_options_param,
        input_map,
        options.outDS[:-1],
        True,
        False,
        options.pfnList != "",
        extra_in_list=extra_in_list,
    )
    task_param_map["jobParameters"].append({"type": "constant", "value": '"'})


def _build_args(options):
    """Arguments of the buildJob script, or None without build step"""
    from pandaclient import AthenaUtils

    if not use_build_step(options):
        return None
    build_args = "-i ${IN} -o ${OUT} --sourceURL ${SURL} "
    if options.noCompile:
        build_args += "--noCompile "
    if AthenaUtils.useCMake() or options.containerImage != "":
        build_args += "--useCMake "
    # debug parameters
    if options.queueData != "":
        build_args += f"--overwriteQueuedata={options.queueData} "
    return build_args


def _merge_args(options, run_dir, archive_name):
    """Arguments of the merge job script"""
    from pandaclient import AthenaUtils

    merge_args = f"-r {run_dir} "
    if options.mergeScript != "":
        merge_args += f'-j "{options.mergeScript}" '
    if options.mergeSingleFile:
        merge_args += "--mergeSingleFile "
    if not options.noBuild:
        merge_args += "-l ${LIB} "
    else:
        merge_args += f"-a {archive_name} "
        merge_args += "--sourceURL ${SURL} "
    merge_args += "--useAthenaPackages "
    if AthenaUtils.useCMake() or options.containerImage != "":
        merge_args += "--useCMake "
    return merge_args


def _build_task_params(options, work_env, run_config, job_options, archive_name, included_sites, full_exec_string, event_pick_file, tmp_log):
    """
    Make the task parameters

    args:
        options: parsed pathena options
        work_env: Athena setup and run directory
        run_config: run configuration of the job options
        job_options: job options, or the --trf command
        archive_name: name of the sandbox on the server
        included_sites: sites given with --site
        full_exec_string: pathena command line, recorded in the task
        event_pick_file: path of the run/event list file for event picking, or None
        tmp_log: logger
    returns:
        task parameters
    """
    from pandaclient import AthenaUtils, Client

    task_param_map = {}
    processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-athena"
    if options.trf:
        processing_type += "-trf"
    set_common_task_params(options, task_param_map, processing_type, included_sites, full_exec_string, archive_name)
    task_param_map["vo"] = "atlas"
    if options.containerImage == "":
        task_param_map["architecture"] = AthenaUtils.getCmtConfigImg(
            work_env.athena_version,
            work_env.cache_version,
            work_env.nightly_version,
            options.cmtConfig,
            architecture=options.architecture,
        )
    else:
        task_param_map["architecture"] = options.architecture
        task_param_map["container_name"] = options.containerImage
    task_param_map["transUses"] = work_env.athena_version
    task_param_map["transHome"] = f"AnalysisTransforms{work_env.cache_version}{work_env.nightly_version}"
    if options.workingGroup is not None:
        task_param_map["workingGroup"] = options.workingGroup
    if options.maxCpuCount > 0:
        task_param_map["walltime"] = -options.maxCpuCount
    if options.nCore <= 1 < options.nThreads:
        task_param_map["coreCount"] = options.nThreads
    # source URL
    set_source_url(task_param_map, Client.baseURLCSRVSSL)
    # log
    set_log_task_params(options, task_param_map)

    # job parameters
    task_param_map["jobParameters"] = []

    # sandbox: the build step output, or the sandbox itself without build step
    if options.noBuild and not options.noCompile:
        sandbox_param = f"-a {archive_name}"
        if options.tarBallViaDDM:
            sandbox_param += " --noTarballDownload"
    else:
        sandbox_param = "-l ${LIB}"
    task_param_map["jobParameters"].append({"type": "constant", "value": sandbox_param})

    # pre execution string
    task_param_map["jobParameters"] += _pre_exec_job_params(options, run_config)

    # runtime arguments
    task_param_map["jobParameters"].append({"type": "constant", "value": _runtime_args(options, run_config, work_env.run_dir, event_pick_file)})

    # input
    input_map = {}
    _input_job_params(options, task_param_map, input_map)

    # extract DBR for --trf
    dbr_in_trf = False
    if options.trf:
        match = re.search("%DB=([^ '\";]+)", job_options)
        if match is not None:
            options.dbRelease = match.group(1)
            dbr_in_trf = True
    # param for DBR
    task_param_map["jobParameters"] += get_dbr_job_params(options.dbRelease, dbr_in_trf)

    # minimum bias, cavern, beam halo, beam gas, and general secondaries
    _extra_input_job_params(options, task_param_map, input_map)
    _secondary_job_params(options, task_param_map, input_map)

    # output
    out_map = _output_job_params(options, run_config, task_param_map, tmp_log)

    # job options
    _job_options_job_params(options, run_config, job_options, out_map, input_map, task_param_map)

    # use local IO for trf or BS
    if options.forceStaged or ((options.trf or run_config.input.inBS) and not options.forceDirectIO) or options.transferType == "file":
        task_param_map["useLocalIO"] = 1
    elif options.useDirectIOSites or options.transferType == "direct":
        task_param_map["allowInputLAN"] = "only"

    # use AMI to get the number of events per file
    if options.useAMIEventLevelSplit:
        task_param_map["getNumEventsInMetadata"] = True

    # build step
    set_build_task_params(options, task_param_map, archive_name, _build_args(options))

    # preprocessing step
    set_prepro_task_params(options, task_param_map)

    # merging
    if options.mergeOutput:
        set_merge_task_params(options, task_param_map, _merge_args(options, work_env.run_dir, archive_name), tmp_log)

    return task_param_map


#####################################################################
# pathena


def main():
    """
    Execute pathena with the arguments in sys.argv

    Always exits: with 0 after the submission, and with an error code from MiscUtils when it fails
    """
    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, "pathena")

    # parse options and check against the obsolete options first
    parser = PathenaArgs.build_parser()
    MiscUtils.remove_obsolete_options(sys.argv, MiscUtils.PATHENA_OBSOLETE_OPTIONS)
    # using parse_known_args for passing arguments with -
    options, args = parser.parse_known_args()

    if options.verbose:
        print(options)
        print(f"args={args}")
        print()

    # load json. athena_args holds the job options and arguments given as positional arguments on the command line
    json_exec_string, json_special_values = load_json_options(options, special_keys=("athena_args",))
    if "athena_args" in json_special_values:
        args = json_special_values["athena_args"]

    # display version
    if options.version:
        print(f"Version: {PandaToolsPkgInfo.release_version}")
        sys.exit(0)

    # imported after --version: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils, Client, PLogger, PsubUtils

    # full execution string
    full_exec_string = PsubUtils.convSysArgv()
    full_exec_string += json_exec_string

    tmp_log = PLogger.getPandaLogger()

    # job options can't look like options
    _check_unknown_args(args, tmp_log)

    # use dev server
    if options.devSrv:
        Client.useDevServer()

    # use INTR server
    if options.intrSrv:
        Client.useIntrServer()

    # --CA, and options implied by other options
    _apply_implied_options(options, tmp_log)

    # validate --transferType
    invalid_transfer_types = get_invalid_transfer_types(options.transferType)
    if invalid_transfer_types:
        tmp_log.error(f"--transferType: invalid value(s) {', '.join(sorted(invalid_transfer_types))}. Allowed: {', '.join(sorted(VALID_TRANSFER_TYPES))}")
        sys.exit(MiscUtils.EC_Config)

    # files to be deleted
    files_to_delete = []

    # save current dir, where pathena was started
    current_dir = os.path.realpath(os.getcwd())

    # site, memory, and input options handled the same way as in the other tools (e.g. prun)
    included_sites = process_common_options(options, tmp_log)

    # use outputPath as outDS
    if not options.outDS.endswith("/"):
        options.outDS = f"{options.outDS}/"

    # bulk submission
    io_list = get_bulk_io_list(options, tmp_log)

    # outDS, nJobs, PFN list, split, scouting, and input options
    _check_and_normalize_options(options, tmp_log)

    # file options to lists
    _normalize_file_options(options)

    # split related
    _check_job_splitting(options, tmp_log)

    # check grid-proxy
    PsubUtils.check_proxy(options.verbose, options.vomsRoles)

    # get nickname
    nickname = PsubUtils.getNickname(options.verbose)
    if nickname == "":
        sys.exit(MiscUtils.EC_Config)

    # set Rucio accounting
    PsubUtils.setRucioAccount(nickname, "pathena", True)

    # convert in/outTarBall and in/outRunConfig to full path
    _make_paths_absolute(options)

    # check maxCpuCount
    if options.maxCpuCount > Client.maxCpuCountLimit:
        tmp_log.error(f"too large maxCpuCount. Must be less than {Client.maxCpuCountLimit}")
        sys.exit(MiscUtils.EC_Config)

    # create tmp dir, removed at exit
    tmp_dir = PsubUtils.make_tmp_dir(options, current_dir, files_to_delete)

    # Athena setup, CMTCONFIG, and run directory
    work_env = _resolve_work_env(options, current_dir, tmp_log)

    # event picking: the picked events are the input dataset
    event_pick_file = None
    if options.eventPickEvtList != "":
        event_pick_file = _request_event_picking(options, full_exec_string, current_dir, files_to_delete, tmp_log)

    # get job options
    job_options = _get_job_options(options, args, tmp_log)

    # run configuration, and the files and outputs it implies
    extracted, run_config = _extract_run_config(options, job_options, tmp_dir, tmp_log)
    if not options.trf:
        job_options = _apply_run_config(options, run_config, extracted, job_options, tmp_log)
    else:
        job_options = _resolve_ami_tags(options, job_options, tmp_log)
        job_options = _add_trf_outputs(options, job_options, tmp_log)
        _check_trf_events_and_cores(options, job_options, tmp_log)

    # outputs and shipped files
    _check_outputs(options, run_config, tmp_log)

    # random seeds
    _set_random_seeds(options, run_config)

    # minimum bias, cavern, beam halo, beam gas, and secondary datasets
    if options.inDS != "" or options.shipinput or options.pfnList != "":
        _resolve_extra_input_datasets(options, run_config, job_options, tmp_log)

    #####################################################################
    # archive sources and send the archive to PanDA cache
    if options.inTarBall == "" and options.tarBallViaDDM == "":
        archive_name = create_sandbox_archive(options, job_options, work_env, current_dir, tmp_dir, tmp_log)
    else:
        archive_name, _ = PsubUtils.use_existing_sandbox(options, tmp_dir)

    # save for --outTarBall, and upload the sandbox and the good run list
    archive_name = PsubUtils.upload_sandbox(options, archive_name, current_dir, tmp_log, files_to_delete)

    #####################################################################
    # make task
    task_param_map = _build_task_params(
        options,
        work_env=work_env,
        run_config=run_config,
        job_options=job_options,
        archive_name=archive_name,
        included_sites=included_sites,
        full_exec_string=full_exec_string,
        event_pick_file=event_pick_file,
        tmp_log=tmp_log,
    )

    #####################################################################
    # submit the task

    dump_list = []

    for i_submission, io_item in enumerate(io_list):
        if options.verbose:
            cmt_config = AthenaUtils.getCmtConfigImg(work_env.athena_version, work_env.cache_version, work_env.nightly_version, options.cmtConfig)
            extra_lines = [("cmtConfig", cmt_config), ("jobO", job_options.lstrip())]
            PsubUtils.print_submission_parameters(
                options.site,
                work_env.athena_version,
                work_env.group_area,
                work_env.cache_version,
                work_env.nightly_version,
                work_env.run_dir,
                extra_lines,
            )

        if len(io_list) == 1:
            new_task_param_map = task_param_map
        else:
            # replace input and output
            options.inDS = io_item["inDS"]
            options.outDS = io_item["outDS"]
            new_task_param_map = PsubUtils.replaceInputOutput(task_param_map, io_item["inDS"], io_item["outDS"], i_submission)
        task_id = None
        # check outDS format
        if not PsubUtils.checkOutDsName(options.outDS, options.official, nickname, options.mergeOutput, options.verbose):
            tmp_log.error(f"invalid output datasetname:{options.outDS}")
            sys.exit(MiscUtils.EC_Config)
        # check task parameters
        exit_code, tmp_output = PsubUtils.checkTaskParam(new_task_param_map, options.unlimitNumOutputs)
        if exit_code != 0 and len(io_list) == 1:
            sys.exit(exit_code)
        if options.noSubmit and options.verbose:
            PsubUtils.print_task_params(new_task_param_map, tmp_log)
        if not options.noSubmit and exit_code == 0:
            exit_code, tmp_output, task_id = PsubUtils.submit_task(new_task_param_map, options, tmp_log)
        dump_list.append(PsubUtils.make_dump_item(options, exit_code, tmp_output, task_id, i_submission, len(io_list)))

    # go back to current dir
    os.chdir(current_dir)
    # dump
    if options.dumpJson is not None:
        with open(options.dumpJson, "w") as f:
            json.dump(dump_list, f)
    # succeeded, also when one of the bulk submissions failed
    sys.exit(0)


if __name__ == "__main__":
    main()
