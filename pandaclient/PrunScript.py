import atexit
import json
import os
import re
import shutil
import subprocess
import sys
from urllib.parse import quote

from pandaclient import MiscUtils, PandaToolsPkgInfo, PrunArgs
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
    get_container_job_params,
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


def _copy_root_core_packages(options, cur_dir, files_to_delete_on_exit, tmp_log):
    """Copy the user's RootCore packages and the RootCore grid scripts into the sandbox, for --useRootCore

    RootCore was the build system of the Run 2 ROOT-based analysis releases, before the CMake-based ones.
    The packages are copied into <cur_dir>/__panda_rootCoreWorkDir, which is shipped in the sandbox.
    On the worker node, buildGen sources grid_compile.sh (or grid_compile_nobuild.sh) to compile them,
    and runGen and runMerge source grid_run.sh to set up the environment before running --exec.

    Appends to options.extFile and to files_to_delete_on_exit, and exits with EC_Config on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started, which is under the work directory
        files_to_delete_on_exit: files and directories deleted when prun exits
        tmp_log: logger
    """
    # $ROOTCOREDIR is set by the RootCore setup, the scripts come from that installation
    if "ROOTCOREDIR" not in os.environ:
        tmp_err_msg = "$ROOTCOREDIR is not defined in your environment. Please setup RootCore runtime beforehand"
        tmp_log.error(tmp_err_msg)
        sys.exit(MiscUtils.EC_Config)

    # grid_submit*.sh copy the packages on the client, grid_compile*.sh build them on the worker node
    # and grid_run.sh sets up the environment of the jobs. The nobuild variants are for --noBuild
    root_core_submit_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_submit.sh"
    root_core_compile_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_compile.sh"
    root_core_run_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_run.sh"
    root_core_submit_nb_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_submit_nobuild.sh"
    root_core_compile_nb_sh = f"{os.environ['ROOTCOREDIR']}/scripts/grid_compile_nobuild.sh"

    # old RootCore versions don't have all the scripts
    root_core_sh_list = [root_core_submit_sh, root_core_compile_sh, root_core_run_sh]
    if options.noBuild:
        root_core_sh_list.append(root_core_submit_nb_sh)
    # copied below whenever --noCompile is used, with or without --noBuild
    if options.noCompile:
        root_core_sh_list.append(root_core_compile_nb_sh)
    for tmp_sh_file in root_core_sh_list:
        if not os.path.exists(tmp_sh_file):
            tmp_log.error(f"{tmp_sh_file} doesn't exist. Please use a newer version of RootCore")
            sys.exit(MiscUtils.EC_Config)
    tmp_log.info("copy RootCore packages to current dir")

    # the worker-node scripts expect this directory name under the run directory
    panda_root_core_work_dir_name = "__panda_rootCoreWorkDir"
    root_core_dest_work_dir = f"{cur_dir}/{panda_root_core_work_dir_name}"
    # ship everything in it, including the files skipped by default (.o, .a, .so, .root, large files)
    options.extFile.append(f"{panda_root_core_work_dir_name}/.*")
    # the copy is only needed for the sandbox
    files_to_delete_on_exit.append(root_core_dest_work_dir)

    # copy the packages, without building them for --noBuild
    if options.noBuild:
        submit_sh = root_core_submit_nb_sh
    else:
        submit_sh = root_core_submit_sh
    # the output of the script goes to the terminal, as before with os.system
    tmp_stat = subprocess.run([submit_sh, root_core_dest_work_dir]).returncode
    if tmp_stat != 0:
        tmp_err_msg = f"{submit_sh} failed with {tmp_stat}"
        tmp_log.error(tmp_err_msg)
        sys.exit(MiscUtils.EC_Config)

    # ship the scripts used on the worker node
    shutil.copy(root_core_run_sh, root_core_dest_work_dir)
    shutil.copy(root_core_compile_sh, root_core_dest_work_dir)
    if options.noCompile:
        shutil.copy(root_core_compile_nb_sh, root_core_dest_work_dir)


def _archive_athena_packages(options, cur_dir, tmp_dir, run_dir, work_area, group_area):
    """Archive the packages of the Athena work area into the sandbox tarball, for --useAthenaPackages

    Creates <tmp_dir>/sources.<uuid>.tar, or jobO.<uuid>.tar with --noBuild when CMake isn't used.
    The files under the work directory are appended to the same tarball afterwards.
    Changes the working directory, and exits on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started
        tmp_dir: temporary directory where the tarball is made
        run_dir: run directory relative to the work area
        work_area: Athena work area
        group_area: Athena group area, or "" when not set
    returns:
        (name of the uncompressed tarball, its full path)
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils

    archive_name = ""
    # CMake setup ($CMAKE_PREFIX_PATH): cpack packs the build directory (the compiled packages under usr/)
    # into a plain tarball, empty if the build has no CPackConfig.cmake. prun always asks for the "sources"
    # name here, so the next steps append to sources.<uuid>.tar also with --noBuild
    if AthenaUtils.useCMake():
        archive_name, archive_full_name = AthenaUtils.archiveWithCpack(True, tmp_dir, options.verbose)
    # make AthenaUtils.matchExtFile use the --extFile patterns. The list is shared, not copied,
    # so it includes the RootCore directory added by _copy_root_core_packages
    AthenaUtils.setExtFile(options.extFile)
    if not options.noBuild:
        # the package sources, compiled by the build job on the grid. --gluePackages adds CMT glue packages
        archive_name, archive_full_name = AthenaUtils.archiveSourceFiles(
            work_area,
            run_dir,
            cur_dir,
            tmp_dir,
            options.verbose,
            options.gluePackages,
            dereferenceSymLinks=options.followLinks,
            archiveName=archive_name,
        )
    else:
        # --noBuild: no build job, only the job options and python modules (the files matching matchExtFile)
        archive_name, archive_full_name = AthenaUtils.archiveJobOFiles(
            work_area,
            run_dir,
            cur_dir,
            tmp_dir,
            options.verbose,
            archiveName=archive_name,
        )
    # add the InstallArea of the work area. The one of the group area is packed into groupArea.<uuid>.tar,
    # which is added to the tarball unless --noBuild is used
    AthenaUtils.archiveInstallArea(
        work_area,
        group_area,
        archive_name,
        archive_full_name,
        tmp_dir,
        options.noBuild,
        options.verbose,
    )
    return archive_name, archive_full_name


def create_sandbox_archive(options, cur_dir, tmp_dir, run_dir, work_area, group_area, files_to_delete_on_exit, tmp_log):
    """Archive the user files into the sandbox tarball

    Copies the RootCore packages with --useRootCore, archives the Athena packages with --useAthenaPackages,
    adds the files under the work directory, then compresses and checks the tarball.
    Changes the working directory, ending in tmp_dir, appends to options.extFile and to files_to_delete_on_exit,
    and exits on errors.

    args:
        options: parsed prun options
        cur_dir: directory where prun was started
        tmp_dir: temporary directory where the tarball is made
        run_dir: run directory relative to the work area
        work_area: Athena work area, used only with --useAthenaPackages
        group_area: Athena group area, used only with --useAthenaPackages
        files_to_delete_on_exit: files and directories deleted when prun exits
        tmp_log: logger
    returns:
        name of the gzipped tarball in tmp_dir
    """
    # imported here as in main: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils, PsubUtils

    if options.useRootCore:
        _copy_root_core_packages(options, cur_dir, files_to_delete_on_exit, tmp_log)

    # without --useAthenaPackages the tarball is named after gathering the files below
    archive_name, archive_full_name = "", ""
    if options.useAthenaPackages:
        archive_name, archive_full_name = _archive_athena_packages(options, cur_dir, tmp_dir, run_dir, work_area, group_area)

    # gather normal files
    if options.useAthenaPackages:
        # go to work_area
        os.chdir(work_area)
        # gather files under work dir
        tmp_log.info(f"gathering files under {work_area}/{run_dir}")
        arch_start_dir = run_dir
        arch_start_dir = re.sub("/+$", "", arch_start_dir)
    else:
        # go to work dir
        os.chdir(options.workDir)
        # gather files under work dir
        tmp_log.info(f"gathering files under {options.workDir}")
        arch_start_dir = "."
    # get files in the working dir
    if options.noCompile:
        skipped_ext = []
    else:
        skipped_ext = [".o", ".a", ".so"]
    skipped_flag = False
    work_dir_files = []
    if options.followLinks:
        os_walk_list = os.walk(arch_start_dir, followlinks=True)
    else:
        os_walk_list = os.walk(arch_start_dir)
    for tmp_root, tmp_dirs, tmp_files in os_walk_list:
        empty_flag = True
        for tmp_file in tmp_files:
            if options.useAthenaPackages:
                if os.path.basename(tmp_file) == os.path.basename(archive_full_name):
                    if options.verbose:
                        print(f"skip Athena archive {tmp_file}")
                    continue
            tmp_path = f"{tmp_root}/{tmp_file}"
            # get size
            try:
                size = os.path.getsize(tmp_path)
            except Exception:
                # skip dead symlink
                if options.verbose:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    print(f"  Ignore : {exc_type}:{exc_value}")
                continue
            # check exclude files
            exclude_file_flag = False
            for tmp_patt in AthenaUtils.excludeFile:
                if re.search(tmp_patt, tmp_path) is not None:
                    exclude_file_flag = True
                    break
            if exclude_file_flag:
                continue
            # skipped extension
            is_skipped_ext = False
            for tmp_ext in skipped_ext:
                if tmp_path.endswith(tmp_ext):
                    is_skipped_ext = True
                    break
            # check root
            is_root = False
            if re.search(r"\.root(\.\d+)*$", tmp_path) is not None:
                is_root = True
            # extra files
            is_extra = False
            for tmp_ext in options.extFile:
                if re.search(f"{tmp_ext}$", tmp_path) is not None:
                    is_extra = True
                    break
            # regular files
            if not is_extra:
                # unset empty_flag even if all files are skipped
                empty_flag = False
                # skipped extensions
                if is_skipped_ext:
                    print(f"  skip {skipped_ext} {tmp_path}")
                    skipped_flag = True
                    continue
                # skip root
                if is_root:
                    print(f"  skip root file {tmp_path}")
                    skipped_flag = True
                    continue
                # check size
                if size > options.maxFileSize:
                    print(f"  skip large file {tmp_path}:{size}B>{options.maxFileSize}B")
                    skipped_flag = True
                    continue
            # remove ./
            tmp_path = re.sub(r"^\./", "", tmp_path)
            # append
            work_dir_files.append(tmp_path)
            if empty_flag:
                empty_flag = False
        # add empty directory
        if empty_flag and tmp_dirs == [] and tmp_files == []:
            tmp_path = re.sub(r"^\./", "", tmp_root)
            # check exclude pattern
            exclude_pat_flag = False
            for tmp_patt in AthenaUtils.excludeFile:
                if re.search(tmp_patt, tmp_path) is not None:
                    exclude_pat_flag = True
                    break
            if exclude_pat_flag:
                continue
            # skip tmp_dir
            if tmp_path.split("/")[-1] == tmp_dir.split("/")[-1]:
                continue
            # append
            work_dir_files.append(tmp_path)
    if skipped_flag:
        tmp_log.info("please use --extFile if you need to send the skipped files to WNs")
    # set archive name
    if not options.useAthenaPackages:
        # create archive
        if options.noBuild and not options.noCompile:
            # use 'jobO' for noBuild
            archive_name = f"jobO.{MiscUtils.wrappedUuidGen()}.tar"
        else:
            # use 'sources' for normal build
            archive_name = f"sources.{MiscUtils.wrappedUuidGen()}.tar"
        archive_full_name = f"{tmp_dir}/{archive_name}"
    # collect files
    for tmp_file in work_dir_files:
        # avoid self-archiving
        if os.path.basename(tmp_file) == os.path.basename(archive_full_name):
            if options.verbose:
                print(f"skip self-archiving for {tmp_file}")
            continue
        if os.path.islink(tmp_file):
            status, out = MiscUtils.commands_get_status_output(f"tar --exclude '.[a-zA-Z]*' -rh '{tmp_file}' -f '{archive_full_name}'")
        else:
            status, out = MiscUtils.commands_get_status_output(f"tar --exclude '.[a-zA-Z]*' -rf '{archive_full_name}' '{tmp_file}'")
        if options.verbose:
            print(tmp_file)
        if status != 0 or out != "":
            print(out)
    # go to tmpdir
    os.chdir(tmp_dir)

    # compress and check the sandbox
    return PsubUtils.finalize_sandbox(archive_name, options.verbose, tmp_log, check_symlinks=options.useAthenaPackages)


# main
def main(get_taskparams=False, ext_args=None, dry_mode=False, get_options=False):
    """
    Execute prun command
    :param get_taskparams: get task parameters and return them if True
    :param ext_args: external arguments to be passed to prun
    :param dry_mode: execute prun in dry mode
    :param get_options: get options and return them if True
    :return: task parameters or options if get_taskparams or get_options is True
    """
    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, "prun")

    # parse options and check against the obsolete options first
    optP = PrunArgs.build_parser()
    MiscUtils.remove_obsolete_options(sys.argv, MiscUtils.PRUN_OBSOLETE_OPTIONS)
    options = optP.parse_args(ext_args)

    if options.verbose:
        print(options)
        print()

    # load json
    jsonExecStr, _ = load_json_options(
        options,
        # "exec" is the option name, and inputFileListName the old destination of --inputFileList
        key_aliases={"exec": "jobParams", "inputFileListName": "inputFileList"},
        unknown_key_exit_code=0,
    )

    # return options
    if get_options:
        return vars(options)

    # display version
    if options.version:
        print(f"Version: {PandaToolsPkgInfo.release_version}")
        sys.exit(0)

    # imported after --version: importing Client resolves the panda cache host
    from pandaclient import AthenaUtils, Client, PLogger, PsubUtils

    # full execution string
    fullExecString = PsubUtils.convSysArgv()
    fullExecString += jsonExecStr

    tmp_log = PLogger.getPandaLogger()

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
        options.directExecInContainer = False

    # container stuff
    if options.containerImage != "":
        options.noBuild = True
        if options.alrb:
            options.useSandbox = True
        if not options.useSandbox:
            tmp_log.warning(
                "Files in the run directory are not sent out by default when --containerImage is used. "
                "Please use --useSandbox if you need those files on the grid."
            )

    # validate --transferType
    invalid = get_invalid_transfer_types(options.transferType)
    if invalid:
        optP.error(f"--transferType: invalid value(s) {', '.join(sorted(invalid))}. Allowed: {', '.join(sorted(VALID_TRANSFER_TYPES))}")

    # files to be deleted
    delFilesOnExit = []

    # save current dir
    curDir = os.path.realpath(os.getcwd())

    # remove spaces
    options.outputs = options.outputs.replace(" ", "")

    # site, memory, and input options handled the same way as in the other tool
    includedSite = process_common_options(options, tmp_log)

    # set maxNFilesPerJob
    PsubUtils.limit_maxNumInputs = options.maxNFilesPerJob

    # bulk submission
    ioList = get_bulk_io_list(options, tmp_log)

    # enforce to use output dataset container
    if not options.outDS.endswith("/"):
        options.outDS = f"{options.outDS}/"

    # absolute path for PFN list
    if options.pfnList != "":
        options.pfnList = os.path.realpath(options.pfnList)

    # extract DBR from exec
    tmpMatch = re.search("%DB:([^ '\";]+)", options.jobParams)
    if tmpMatch is not None:
        options.dbRelease = tmpMatch.group(1)
        options.notExpandDBR = True

    # check DBRelease, good run list, and event picking
    check_input_options(options, tmp_log)

    # check rootVer
    if options.rootVer != "":
        if options.useAthenaPackages or options.athenaTag:
            tmp_log.warning(
                "--rootVer is ignored when --athenaTag or --useAthenaPackages is used, not to break the runtime environment by superseding the root version"
            )
            options.rootVer = ""
        else:
            # change / to .
            options.rootVer = options.rootVer.replace("/", ".")

    # check writeInputToTxt
    if options.writeInputToTxt != "":
        # remove %
        options.writeInputToTxt = options.writeInputToTxt.replace("%", "")
        # loop over all StreamName:FileName
        for tmpItem in options.writeInputToTxt.split(","):
            tmpItems = tmpItem.split(":")
            if len(tmpItems) != 2:
                tmp_log.error(f"invalid StreamName:FileName in --writeInputToTxt : {tmpItem}")
                sys.exit(MiscUtils.EC_Config)

    # read list of files to be used
    filesToBeUsed = []
    if options.inputFileList != "":
        rFile = open(options.inputFileList)
        for line in rFile:
            line = line.replace("\n", "")
            line = line.strip()
            if line != "":
                filesToBeUsed.append(line)
        rFile.close()

    # remove whitespaces
    if options.inDS != "":
        options.inDS = options.inDS.replace(" ", "")

    # persistent file
    if options.persistentFile:
        options.persistentFile = f"{options.persistentFile}:sources.{MiscUtils.wrappedUuidGen()}.__ow__"

    # warning
    if 0 < options.nFilesPerJob < 5:
        tmp_log.warning(
            "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    if options.maxNFilesPerJob < 5:
        tmp_log.warning(
            "Very small --maxNFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    # warning for nFilesPerJob
    if options.nFilesPerJob and options.nFilesPerJob > options.maxNFilesPerJob:
        tmp_log.warning("--nFilesPerJob cannot be larger than --maxNFilesPerJob. It is set to the value of --maxNFilesPerJob")
        options.nFilesPerJob = options.maxNFilesPerJob

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
    if options.workDir != curDir and (not curDir.startswith(f"{options.workDir}/")):
        tmp_log.error(f"you need to run prun in a directory under {options.workDir}")
        sys.exit(MiscUtils.EC_Config)

    # avoid gathering the home dir
    if (
        "HOME" in os.environ
        and not options.useHomeDir
        and not options.useAthenaPackages
        and os.path.realpath(os.path.expanduser(os.environ["HOME"])) == options.workDir
        and not dry_mode
    ):
        tmpStr = (
            "prun is executed just under the HOME directory "
            "and is going to send all files under the dir including ~/Mail/* and ~/private/*. "
            "Do you really want that? (Please use --useHomeDir if you want to skip this confirmation)"
        )
        tmp_log.warning(tmpStr)
        while True:
            tmpAnswer = input("y/N: ")
            tmpAnswer = tmpAnswer.strip()
            if tmpAnswer in ["y", "N"]:
                break
        if tmpAnswer == "N":
            sys.exit(MiscUtils.EC_Config)

    # run dir
    runDir = "."
    if curDir != options.workDir:
        # remove special characters
        wDirString = re.sub(r"[\+]", ".", options.workDir)
        runDir = re.sub(f"^{wDirString}/", "", curDir)

    # check maxCpuCount
    if options.maxCpuCount > Client.maxCpuCountLimit:
        tmp_log.error(f"too large maxCpuCount. Must be less than {Client.maxCpuCountLimit}")
        sys.exit(MiscUtils.EC_Config)

    # create tmp dir
    if options.tmpDir == "":
        tmpDir = f"{curDir}/{MiscUtils.wrappedUuidGen()}"
    else:
        tmpDir = f"{os.path.abspath(options.tmpDir)}/{MiscUtils.wrappedUuidGen()}"
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, files, del_command):
        for tmpFile in files:
            del_command(f"rm -rf {tmpFile}")
        del_command(f"rm -rf {dir}")

    atexit.register(_onExit, tmpDir, delFilesOnExit, MiscUtils.commands_get_output)

    # parse tag
    athenaVer = ""
    cacheVer = ""
    nightVer = ""
    groupArea = ""
    workArea = ""
    cmtConfig = ""
    if options.useAthenaPackages:
        # get Athena versions
        has_athena_env, athena_env = AthenaUtils.getAthenaVer()
        # failed
        if not has_athena_env:
            tmp_log.error("You need to setup Athena runtime to use --useAthenaPackages")
            sys.exit(MiscUtils.EC_Config)
        workArea = athena_env["workArea"]
        athenaVer = f"Atlas-{athena_env['athenaVer']}"
        groupArea = athena_env["groupArea"]
        cacheVer = athena_env["cacheVer"]
        nightVer = athena_env["nightVer"]
        cmtConfig = athena_env["cmtConfig"]
        # override run directory
        sString = re.sub(r"[\+]", ".", workArea)
        runDir = re.sub(f"^{sString}", "", curDir)
        if runDir == curDir:
            error_message = (
                f"You need to run prun in a directory under {workArea}. "
                f"If {workArea} is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup."
            )
            tmp_log.error(error_message)
            sys.exit(MiscUtils.EC_Config)
        elif runDir == "":
            runDir = "."
        elif runDir.startswith("/"):
            runDir = runDir[1:]
        runDir = runDir + "/"
    elif options.athenaTag != "":
        athenaVer, cacheVer, nightVer = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmp_log)

    # set CMTCONFIG
    options.cmtConfig = AthenaUtils.getCmtConfig(athenaVer, cacheVer, nightVer, options.cmtConfig, options.verbose)

    # check CMTCONFIG
    if not AthenaUtils.checkCmtConfig(cmtConfig, options.cmtConfig, options.noBuild):
        sys.exit(MiscUtils.EC_Config)

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
                options.inputFileList,
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
        tmp_log.info(f"requested Event Picking service to stage input as {options.inDS}")

    # additional files
    if options.extFile == "":
        options.extFile = []
    else:
        tmpItems = options.extFile.split(",")
        options.extFile = []
        # convert * to .*
        for tmpItem in tmpItems:
            options.extFile.append(tmpItem.replace("*", ".*"))

    # merge script, glue packages, and excluded files
    process_sandbox_options(options)

    # LFN matching
    if options.match != "":
        # convert . to \.
        options.match = options.match.replace(".", r"\.")
        # convert * to .*
        options.match = options.match.replace("*", ".*")

    # LFN anti-matching
    if options.antiMatch != "":
        # convert . to \.
        options.antiMatch = options.antiMatch.replace(".", r"\.")
        # convert * to .*
        options.antiMatch = options.antiMatch.replace("*", ".*")

    # get job script
    if options.jobParams == "":
        tmp_log.error("you need to give --exec\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # replace : to = for backward compatibility
    for optional_arg in ["RNDM"]:
        options.jobParams = re.sub(f"%{optional_arg}:", f"%{optional_arg}=", options.jobParams)

    # check output dataset
    if options.outDS == "":
        tmp_log.error("no outDS is given\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # check PFN list options
    check_pfn_list_options(options, tmp_log)

    # secondary datasets
    parse_secondary_datasets(options, tmp_log)

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
        sys.exit(MiscUtils.EC_Config)

    # set Rucio accounting
    PsubUtils.setRucioAccount(nickName, "prun", True)

    # check split options
    check_split_options(options, tmp_log)

    # check scouting options
    check_scouting_options(options, tmp_log)

    # translate --nJobs into nFiles / nFilesPerJob
    set_n_files_from_n_jobs(options)

    #####################################################################
    # archive sources and send it to HTTP-reachable location

    # create archive
    archiveName = None
    if (options.containerImage == "" or options.useSandbox) and not dry_mode:
        if options.inTarBall == "" and options.tarBallViaDDM == "":
            archiveName = create_sandbox_archive(options, curDir, tmpDir, runDir, workArea, groupArea, delFilesOnExit, tmp_log)
        else:
            archiveName, _ = PsubUtils.use_existing_sandbox(options, tmpDir)

        # save for --outTarBall, and upload the sandbox and the good run list
        archiveName = PsubUtils.upload_sandbox(options, archiveName, curDir, tmp_log, delFilesOnExit, use_cache_srv=options.vo is None)

    #####################################################################
    # make task
    task_param_map = {}
    if options.containerImage != "" and not options.alrb:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-cont"
    else:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-run"
    set_common_task_params(options, task_param_map, processing_type, includedSite, fullExecString, archiveName)
    if options.vo is None:
        task_param_map["vo"] = "atlas"
    else:
        task_param_map["vo"] = options.vo
    if options.containerImage != "" and options.alrb:
        task_param_map["architecture"] = options.architecture
    else:
        task_param_map["architecture"] = AthenaUtils.getCmtConfigImg(
            athenaVer,
            cacheVer,
            nightVer,
            options.cmtConfig,
            architecture=options.architecture,
        )
    task_param_map["transUses"] = athenaVer
    if athenaVer != "":
        task_param_map["transHome"] = f"AnalysisTransforms{cacheVer}{nightVer}"
    else:
        task_param_map["transHome"] = None
    if options.msgDriven:
        task_param_map["messageDriven"] = True
    if options.workingGroup is not None:
        # remove role
        task_param_map["workingGroup"] = options.workingGroup.split(".")[0].split(":")[0]
    task_param_map["nMaxFilesPerJob"] = options.maxNFilesPerJob
    if options.useSecrets:
        task_param_map["useSecrets"] = True
    # source URL
    if options.vo is None:
        set_source_url(task_param_map, Client.baseURLCSRVSSL)
    else:
        set_source_url(task_param_map, Client.baseURLSSL)

    if options.containerImage != "" and options.alrb:
        task_param_map["container_name"] = options.containerImage
        if options.directExecInContainer:
            task_param_map["multiStepExec"] = {
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
                task_param_map["multiStepExec"]["containerOptions"]["execArgs"] = options.alrbArgs

    outDatasetName = options.outDS
    # log
    if not options.noSeparateLog:
        set_log_task_params(options, task_param_map)

    # job parameters
    task_param_map["jobParameters"] = [
        {
            "type": "constant",
            "value": '-j "" --sourceURL ${SURL}',
        },
        {
            "type": "constant",
            "value": f"-r {runDir}",
        },
    ]

    # delimiter
    task_param_map["jobParameters"] += [
        {"type": "constant", "value": "__delimiter__", "hidden": True},
    ]

    # build
    if options.containerImage == "" or options.useSandbox:
        if options.noBuild and not options.noCompile:
            tmp_str = f"-a {archiveName}"
            if options.tarBallViaDDM:
                tmp_str += " --noTarballDownload"
        else:
            tmp_str = "-l ${LIB}"
        task_param_map["jobParameters"] += [
            {
                "type": "constant",
                "value": tmp_str,
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
                    tmpErrMsg = f"dataset name suffix '{tmpDsSuffix}' is used for multiple files in --outputs. each output must have a unique suffix."
                    tmp_log.error(tmpErrMsg)
                    sys.exit(MiscUtils.EC_Config)
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
                # disallowed character
                if "/" in tmpNewLFN:
                    tmp_err_msg = f"An output file name {tmpNewLFN} contains '/'."
                    tmp_log.error(tmp_err_msg)
                    sys.exit(MiscUtils.EC_Config)
                # check invalid characters
                checked = PsubUtils.check_invalid_char(tmpNewLFN, is_file=True)
                if checked is not None:
                    tmp_err_msg = f'An output file name {tmpNewLFN} contains an invalid character "{checked}".'
                    tmp_log.error(tmp_err_msg)
                    sys.exit(MiscUtils.EC_Config)
                if len(outDatasetName.split(".")) > 2:
                    lfn = "{}.{}".format(*outDatasetName.split(".")[:2])
                else:
                    lfn = outDatasetName[:-1]
                if options.addNthFieldOfInDSToLFN != "" or options.addNthFieldOfInFileToLFN != "":
                    lfn += "${MIDDLENAME}"
                lfn += f".$JEDITASKID._${{SN/P}}.{tmpNewLFN}"
                if tmpDsSuffix == "":
                    tmpDsSuffix = tmpNewLFN
            dataset = f"{outDatasetName[:-1]}_{tmpDsSuffix}/"
            task_param_map["jobParameters"] += MiscUtils.makeJediJobParam(
                lfn,
                dataset,
                "output",
                hidden=True,
                destination=options.destSE,
                token=options.spaceToken,
                allowNoOutput=options.allowNoOutput,
            )
            outMap[tmpLFN] = lfn

        task_param_map["jobParameters"] += [
            {
                "type": "constant",
                "value": f'-o "{outMap!s}"',
            },
        ]
        # output files allowed not to be produced
        task_param_map["jobParameters"] += get_allow_no_output_job_params(task_param_map["jobParameters"])
    for tmpPatt in get_unmatched_allow_no_output_patterns(options.allowNoOutput, task_param_map["jobParameters"]):
        tmp_log.warning(
            f"--allowNoOutput pattern '{tmpPatt}' matches none of output filename templates "
            f"{[item['value'] for item in task_param_map['jobParameters'] if item.get('param_type') == 'output']}"
        )
    # input
    if options.inDS != "":
        tmpDict = {
            "type": "template",
            "param_type": "input",
            "value": '-i "${IN/T}"',
            "dataset": options.inDS,
            "exclude": r"\.log\.tgz(\.\d+)*$",
        }
        if options.useLogAsInput:
            del tmpDict["exclude"]
        if not options.notExpandInDS:
            tmpDict["expand"] = True
        if options.notExpandInDS:
            tmpDict["consolidate"] = f"{'.'.join(options.outDS.split('.')[:2])}.{MiscUtils.wrappedUuidGen()}/"
        if options.nSkipFiles != 0:
            tmpDict["offset"] = options.nSkipFiles
        if options.match != "":
            tmpDict["include"] = options.match
        if options.antiMatch != "":
            if "exclude" in tmpDict:
                tmpDict["exclude"] += f",{options.antiMatch}"
            else:
                tmpDict["exclude"] = options.antiMatch
        if filesToBeUsed != []:
            tmpDict["files"] = filesToBeUsed
        task_param_map["jobParameters"].append(tmpDict)
        task_param_map["dsForIN"] = options.inDS
    elif options.pfnList != "":
        set_pfn_list_input_params(options, task_param_map)
    elif options.goodRunListXML != "":
        set_good_run_list_input_params(task_param_map)
    else:
        # no input
        task_param_map["noInput"] = True

    # exec string
    task_param_map["jobParameters"] += [
        {
            "type": "constant",
            "value": '-p "',
            "padding": False,
        },
    ]
    task_param_map["jobParameters"] += PsubUtils.convertParamStrToJediParam(options.jobParams, {}, "", True, False, includeIO=False)
    task_param_map["jobParameters"] += [
        {
            "type": "constant",
            "value": '"',
        },
    ]

    # param for DBR
    task_param_map["jobParameters"] += get_dbr_job_params(options.dbRelease, options.notExpandDBR)

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
            if not options.notExpandSecDSs:
                expandFlag = True
            else:
                expandFlag = False
            # re-usability
            reusableAtt = False
            if streamName in options.reusableSecondary:
                reusableAtt = True
            dictItem = MiscUtils.makeJediJobParam(
                f"${{{streamName}}}",
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
            task_param_map["jobParameters"] += dictItem
            inMap[streamName] = f"tmp_{streamName}"
            streamNames.append(streamName)
        # make constant item
        strInMap = str(inMap)
        # set placeholders
        for streamName in streamNames:
            strInMap = strInMap.replace(f"'tmp_{streamName}'", f"${{{streamName}/T}}")
        dictItem = {
            "type": "constant",
            "value": f'--inMap "{strInMap}"',
        }
        task_param_map["jobParameters"] += [dictItem]
        task_param_map["reuseSecOnDemand"] = True

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
        jobParameters += f"--rootVer {options.rootVer} "
    # cmt config
    if options.cmtConfig not in ["", "NULL", None]:
        jobParameters += f"--cmtConfig {options.cmtConfig} "
    # write input to txt
    if options.writeInputToTxt != "":
        jobParameters += f"--writeInputToTxt {options.writeInputToTxt} "
    # debug parameters
    if options.queueData != "":
        jobParameters += f"--overwriteQueuedata={options.queueData} "
    # exec string with real output filenames
    if options.execWithRealFileNames:
        jobParameters += "--execWithRealFileNames "
    # container
    jobParameters += get_container_job_params(options)
    if options.containerImage != "" and not options.alrb:
        if options.useCentralRegistry:
            jobParameters += "--useCentralRegistry=True "
        elif options.notUseCentralRegistry:
            jobParameters += "--useCentralRegistry=False "
    # persistent file
    if options.persistentFile:
        jobParameters += f"--fileToSave={options.persistentFile} --fileToLoad={options.persistentFile} "
    # set task param
    if jobParameters != "":
        task_param_map["jobParameters"] += [
            {
                "type": "constant",
                "value": jobParameters,
            },
        ]

    # force stage-in
    if options.forceStaged or options.transferType == "file":
        task_param_map["useLocalIO"] = 1
    elif options.useDirectIOSites or options.transferType == "direct":
        task_param_map["allowInputLAN"] = "only"

    # build step
    build_job_params = None
    if use_build_step(options):
        build_job_params = "-i ${IN} -o ${OUT} --sourceURL ${SURL} "
        build_job_params += f"-r {runDir} "
        # exec
        if options.bexec != "":
            build_job_params += f'--bexec "{quote(options.bexec)}" '
        # use Athena packages
        if options.useAthenaPackages:
            build_job_params += "--useAthenaPackages "
        # use RootCore
        if options.useRootCore:
            build_job_params += "--useRootCore "
        # no compile
        if options.noCompile:
            build_job_params += "--noCompile "
        # use CMake
        if AthenaUtils.useCMake():
            build_job_params += "--useCMake "
        # root
        if options.rootVer != "":
            build_job_params += f"--rootVer {options.rootVer} "
        # cmt config
        if options.cmtConfig not in ["", "NULL", None]:
            build_job_params += f"--cmtConfig {options.cmtConfig} "
        # debug parameters
        if options.queueData != "":
            build_job_params += f"--overwriteQueuedata={options.queueData} "
        # container
        build_job_params += get_container_job_params(options)
    set_build_task_params(options, task_param_map, archiveName, build_job_params)

    # preprocessing step
    set_prepro_task_params(options, task_param_map)

    # merging
    if options.mergeOutput:
        merge_job_params = f"-r {runDir} "
        if options.mergeScript != "":
            merge_job_params += f'-j "{options.mergeScript}" '
        if options.mergeSingleFile:
            merge_job_params += "--mergeSingleFile "
        if options.rootVer != "":
            merge_job_params += f"--rootVer {options.rootVer} "
        if options.cmtConfig not in ["", "NULL", None]:
            merge_job_params += f"--cmtConfig {options.cmtConfig} "
        if options.useAthenaPackages:
            merge_job_params += "--useAthenaPackages "
        if AthenaUtils.useCMake():
            merge_job_params += "--useCMake "
        if options.useRootCore:
            merge_job_params += "--useRootCore "
        if options.containerImage != "" and not options.alrb:
            merge_job_params += get_container_job_params(options)
        elif use_build_step(options):
            merge_job_params += "-l ${LIB} "
        else:
            merge_job_params += f"-a {archiveName} "
            merge_job_params += "--sourceURL ${SURL} "
        set_merge_task_params(options, task_param_map, merge_job_params, tmp_log)

    #####################################################################
    # submission

    exitCode = 0
    dumpList = []

    # submit task
    for iSubmission, ioItem in enumerate(ioList):
        if options.verbose:
            PsubUtils.print_submission_parameters(options.site, athenaVer, groupArea, cacheVer, nightVer, runDir, [("exec", options.jobParams)])

        if len(ioList) == 1:
            newTaskParamMap = task_param_map
        else:
            # replace input and output
            options.inDS = ioItem["inDS"]
            options.outDS = ioItem["outDS"]
            newTaskParamMap = PsubUtils.replaceInputOutput(task_param_map, ioItem["inDS"], ioItem["outDS"], iSubmission)
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
            tmpStr = f"invalid output datasetname:{options.outDS}"
            tmp_log.error(tmpStr)
            exitCode = MiscUtils.EC_Config
        # check task parameters
        if exitCode == 0:
            exitCode, tmpStr = PsubUtils.checkTaskParam(newTaskParamMap, options.unlimitNumOutputs)
        if exitCode != 0 and len(ioList) == 1:
            sys.exit(exitCode)
        if options.noSubmit and options.verbose:
            PsubUtils.print_task_params(newTaskParamMap, tmp_log)
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
            exitCode, tmpStr, taskID = PsubUtils.submit_task(newTaskParamMap, options, tmp_log)
        dumpList.append(PsubUtils.make_dump_item(options, exitCode, tmpStr, taskID, iSubmission, len(ioList)))

    # go back to current dir
    os.chdir(curDir)
    # dump
    if options.dumpJson is not None:
        with open(os.path.expanduser(options.dumpJson), "w") as f:
            json.dump(dumpList, f)
    # succeeded
    sys.exit(exitCode)
