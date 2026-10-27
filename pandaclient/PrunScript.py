import atexit
import json
import os
import re
import shutil
import sys
from urllib.parse import quote

from pandaclient import MiscUtils, PandaToolsPkgInfo, PrunArgs
from pandaclient.CommonArgs import (
    VALID_TRANSFER_TYPES,
    check_input_options,
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
        print("")

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
        print("Version: %s" % PandaToolsPkgInfo.release_version)
        sys.exit(0)

    from pandaclient import AthenaUtils, Client, PLogger, PsubUtils

    # full execution string
    fullExecString = PsubUtils.convSysArgv()
    fullExecString += jsonExecStr

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
        options.directExecInContainer = False

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

    # validate --transferType
    invalid = get_invalid_transfer_types(options.transferType)
    if invalid:
        optP.error("--transferType: invalid value(s) {}. Allowed: {}".format(", ".join(sorted(invalid)), ", ".join(sorted(VALID_TRANSFER_TYPES))))

    # files to be deleted
    delFilesOnExit = []

    # save current dir
    curDir = os.path.realpath(os.getcwd())

    # remove whitespaces
    if options.outputs != "":
        options.outputs = re.sub(" ", "", options.outputs)

    # site, memory, and input options handled the same way as in the other tool
    includedSite = process_common_options(options, tmpLog)

    # set maxNFilesPerJob
    PsubUtils.limit_maxNumInputs = options.maxNFilesPerJob

    # bulk submission
    ioList = get_bulk_io_list(options, tmpLog)

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

    # check DBRelease, good run list, and event picking
    check_input_options(options, tmpLog)

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
                sys.exit(MiscUtils.EC_Config)

    # read list of files to be used
    filesToBeUsed = []
    if options.inputFileList != "":
        rFile = open(options.inputFileList)
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
        options.persistentFile = f"{options.persistentFile}:sources.{MiscUtils.wrappedUuidGen()}.__ow__"

    # warning
    if options.nFilesPerJob > 0 and options.nFilesPerJob < 5:
        tmpLog.warning(
            "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    if options.maxNFilesPerJob < 5:
        tmpLog.warning(
            "Very small --maxNFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
            "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
        )

    # warning for nFilesPerJob
    if options.nFilesPerJob and options.nFilesPerJob > options.maxNFilesPerJob:
        tmpLog.warning("--nFilesPerJob cannot be larger than --maxNFilesPerJob. It is set to the value of --maxNFilesPerJob")
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
    if options.workDir != curDir and (not curDir.startswith(options.workDir + "/")):
        tmpLog.error("you need to run prun in a directory under %s" % options.workDir)
        sys.exit(MiscUtils.EC_Config)

    # avoid gathering the home dir
    if (
        "HOME" in os.environ
        and not options.useHomeDir
        and not options.useAthenaPackages
        and os.path.realpath(os.path.expanduser(os.environ["HOME"])) == options.workDir
        and not dry_mode
    ):
        tmpStr = "prun is executed just under the HOME directory "
        tmpStr += "and is going to send all files under the dir including ~/Mail/* and ~/private/*. "
        tmpStr += "Do you really want that? (Please use --useHomeDir if you want to skip this confirmation)"
        tmpLog.warning(tmpStr)
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
        runDir = re.sub("^" + wDirString + "/", "", curDir)

    # check maxCpuCount
    if options.maxCpuCount > Client.maxCpuCountLimit:
        tmpLog.error("too large maxCpuCount. Must be less than %s" % Client.maxCpuCountLimit)
        sys.exit(MiscUtils.EC_Config)

    # create tmp dir
    if options.tmpDir == "":
        tmpDir = "{}/{}".format(curDir, MiscUtils.wrappedUuidGen())
    else:
        tmpDir = "{}/{}".format(os.path.abspath(options.tmpDir), MiscUtils.wrappedUuidGen())
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, files, del_command):
        for tmpFile in files:
            del_command("rm -rf %s" % tmpFile)
        del_command("rm -rf %s" % dir)

    atexit.register(_onExit, tmpDir, delFilesOnExit, MiscUtils.commands_get_output)

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
            sys.exit(MiscUtils.EC_Config)
        workArea = retA["workArea"]
        athenaVer = "Atlas-%s" % retA["athenaVer"]
        groupArea = retA["groupArea"]
        cacheVer = retA["cacheVer"]
        nightVer = retA["nightVer"]
        cmtConfig = retA["cmtConfig"]
        # override run directory
        sString = re.sub(r"[\+]", ".", workArea)
        runDir = re.sub("^%s" % sString, "", curDir)
        if runDir == curDir:
            errMsg = "You need to run prun in a directory under %s. " % workArea
            errMsg += "If '%s' is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup." % workArea
            tmpLog.error(errMsg)
            sys.exit(MiscUtils.EC_Config)
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
        tmpLog.error("you need to give --exec\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # replace : to = for backward compatibility
    for optArg in ["RNDM"]:
        options.jobParams = re.sub("%" + optArg + ":", "%" + optArg + "=", options.jobParams)

    # check output dataset
    if options.outDS == "":
        tmpLog.error("no outDS is given\n  prun [--inDS inputdataset] --outDS outputdataset --exec 'myScript arg1 arg2 ...'")
        sys.exit(MiscUtils.EC_Config)

    # avoid inDS+pfnList
    if options.pfnList != "":
        # don't use inDS
        if options.inDS != "":
            tmpLog.error("--pfnList and --inDS cannot be used at the same time")
            sys.exit(MiscUtils.EC_Config)
        # use site
        if options.site == "AUTO":
            tmpLog.error("--site must be specified when --pfnList is used")
            sys.exit(MiscUtils.EC_Config)

    # secondary datasets
    parse_secondary_datasets(options, tmpLog)

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
    check_split_options(options, tmpLog)

    # translate --nJobs into nFiles / nFilesPerJob
    set_n_files_from_n_jobs(options)

    #####################################################################
    # archive sources and send it to HTTP-reachable location

    # create archive
    archiveName = None
    if (options.containerImage == "" or options.useSandbox) and not dry_mode:
        if options.inTarBall == "" and options.tarBallViaDDM == "":
            # copy RootCore packages
            if options.useRootCore:
                # check $ROOTCOREDIR
                if "ROOTCOREDIR" not in os.environ:
                    tmpErrMsg = "$ROOTCOREDIR is not defined in your environment. "
                    tmpErrMsg += "Please setup RootCore runtime beforehand"
                    tmpLog.error(tmpErrMsg)
                    sys.exit(MiscUtils.EC_Config)
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
                        sys.exit(MiscUtils.EC_Config)
                tmpLog.info("copy RootCore packages to current dir")
                # destination
                pandaRootCoreWorkDirName = "__panda_rootCoreWorkDir"
                rootCoreDestWorkDir = curDir + "/" + pandaRootCoreWorkDirName
                # add all files to extFile
                options.extFile.append(pandaRootCoreWorkDirName + "/.*")
                # add to be deleted on exit
                delFilesOnExit.append(rootCoreDestWorkDir)
                if not options.noBuild:
                    tmpStat = os.system("{} {}".format(rootCoreSubmitSh, rootCoreDestWorkDir))
                else:
                    tmpStat = os.system("{} {}".format(rootCoreSubmitNbSh, rootCoreDestWorkDir))
                tmpStat %= 255
                if tmpStat != 0:
                    tmpErrMsg = "{} failed with {}".format(rootCoreSubmitSh, tmpStat)
                    tmpLog.error(tmpErrMsg)
                    sys.exit(MiscUtils.EC_Config)
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
                    tmpLog.info("gathering files under {}/{}".format(workArea, runDir))
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
                        tmpPath = "{}/{}".format(tmpRoot, tmpFile)
                        # get size
                        try:
                            size = os.path.getsize(tmpPath)
                        except Exception:
                            # skip dead symlink
                            if options.verbose:
                                type, value, traceBack = sys.exc_info()
                                print("  Ignore : {}:{}".format(type, value))
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
                        if re.search(r"\.root(\.\d+)*$", tmpPath) is not None:
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
                                print("  skip {} {}".format(str(skippedExt), tmpPath))
                                skippedFlag = True
                                continue
                            # skip root
                            if isRoot:
                                print("  skip root file %s" % tmpPath)
                                skippedFlag = True
                                continue
                            # check size
                            if size > options.maxFileSize:
                                print("  skip large file {}:{}B>{}B".format(tmpPath, size, options.maxFileSize))
                                skippedFlag = True
                                continue
                        # remove ./
                        tmpPath = re.sub(r"^\./", "", tmpPath)
                        # append
                        workDirFiles.append(tmpPath)
                        if emptyFlag:
                            emptyFlag = False
                    # add empty directory
                    if emptyFlag and tmpDirs == [] and tmpFiles == []:
                        tmpPath = re.sub(r"^\./", "", tmpRoot)
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
                    archiveFullName = "{}/{}".format(tmpDir, archiveName)
                # collect files
                for tmpFile in workDirFiles:
                    # avoid self-archiving
                    if os.path.basename(tmpFile) == os.path.basename(archiveFullName):
                        if options.verbose:
                            print("skip self-archiving for %s" % tmpFile)
                        continue
                    if os.path.islink(tmpFile):
                        status, out = MiscUtils.commands_get_status_output("tar --exclude '.[a-zA-Z]*' -rh '{}' -f '{}'".format(tmpFile, archiveFullName))
                    else:
                        status, out = MiscUtils.commands_get_status_output("tar --exclude '.[a-zA-Z]*' -rf '{}' '{}'".format(archiveFullName, tmpFile))
                    if options.verbose:
                        print(tmpFile)
                    if status != 0 or out != "":
                        print(out)
            # go to tmpdir
            os.chdir(tmpDir)

            # make empty if archive doesn't exist
            if not os.path.exists(archiveFullName):
                MiscUtils.commands_get_status_output("tar cvf %s --files-from /dev/null " % archiveName)

            # compress
            status, out = MiscUtils.commands_get_status_output("gzip %s" % archiveName)
            archiveName += ".gz"
            if status != 0 or options.verbose:
                print(out)

            # check archive
            status, out = MiscUtils.commands_get_status_output(f"ls -l {archiveName}")
            if options.verbose:
                print(out)
            if status != 0:
                tmpLog.error("Failed to archive working area.\nIf you see 'Disk quota exceeded', try '--tmpDir /tmp'")
                sys.exit(MiscUtils.EC_Archive)

            # check symlinks
            if options.useAthenaPackages:
                PsubUtils.check_sandbox_symlinks(archiveName, tmpLog)
        else:
            archiveName, archiveFullName = PsubUtils.use_existing_sandbox(options, tmpDir)

        # save for --outTarBall, and upload the sandbox and the good run list
        archiveName = PsubUtils.upload_sandbox(options, archiveName, curDir, tmpLog, delFilesOnExit, use_cache_srv=options.vo is None)

    #####################################################################
    # make task
    taskParamMap = {}
    if options.containerImage != "" and not options.alrb:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-cont"
    else:
        processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-run"
    set_common_task_params(options, taskParamMap, processing_type, includedSite, fullExecString, archiveName)
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
    if options.msgDriven:
        taskParamMap["messageDriven"] = True
    if options.workingGroup is not None:
        # remove role
        taskParamMap["workingGroup"] = options.workingGroup.split(".")[0].split(":")[0]
    taskParamMap["nMaxFilesPerJob"] = options.maxNFilesPerJob
    if options.useSecrets:
        taskParamMap["useSecrets"] = True
    # source URL
    if options.vo is None:
        set_source_url(taskParamMap, Client.baseURLCSRVSSL)
    else:
        set_source_url(taskParamMap, Client.baseURLSSL)

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
    # log
    if not options.noSeparateLog:
        set_log_task_params(options, taskParamMap)

    # job parameters
    taskParamMap["jobParameters"] = [
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
    taskParamMap["jobParameters"] += [
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
        taskParamMap["jobParameters"] += [
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
                    tmpErrMsg = "dataset name suffix '%s' is used for multiple files in --outputs. " % tmpDsSuffix
                    tmpErrMsg += "each output must have a unique suffix."
                    tmpLog.error(tmpErrMsg)
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
                    tmp_err_msg = "An output file name %s contains '/'." % tmpNewLFN
                    tmpLog.error(tmp_err_msg)
                    sys.exit(MiscUtils.EC_Config)
                # check invalid characters
                checked = PsubUtils.check_invalid_char(tmpNewLFN, is_file=True)
                if checked is not None:
                    tmp_err_msg = 'An output file name {} contains an invalid character "{}".'.format(tmpNewLFN, checked)
                    tmpLog.error(tmp_err_msg)
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

        taskParamMap["jobParameters"] += [
            {
                "type": "constant",
                "value": f'-o "{str(outMap)}"',
            },
        ]
        # output files allowed not to be produced
        taskParamMap["jobParameters"] += get_allow_no_output_job_params(taskParamMap["jobParameters"])
    for tmpPatt in get_unmatched_allow_no_output_patterns(options.allowNoOutput, taskParamMap["jobParameters"]):
        tmpLog.warning(
            f"--allowNoOutput pattern '{tmpPatt}' matches none of output filename templates "
            f"{[item['value'] for item in taskParamMap['jobParameters'] if item.get('param_type') == 'output']}"
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
        set_pfn_list_input_params(options, taskParamMap)
    elif options.goodRunListXML != "":
        set_good_run_list_input_params(taskParamMap)
    else:
        # no input
        taskParamMap["noInput"] = True

    # exec string
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

    # param for DBR
    taskParamMap["jobParameters"] += get_dbr_job_params(options.dbRelease, options.notExpandDBR)

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
        taskParamMap["reuseSecOnDemand"] = True

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
    jobParameters += get_container_job_params(options)
    if options.containerImage != "" and not options.alrb:
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
    if options.forceStaged or options.transferType == "file":
        taskParamMap["useLocalIO"] = 1
    elif options.useDirectIOSites or options.transferType == "direct":
        taskParamMap["allowInputLAN"] = "only"

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
            build_job_params += "--rootVer %s " % options.rootVer
        # cmt config
        if not options.cmtConfig in ["", "NULL", None]:
            build_job_params += "--cmtConfig %s " % options.cmtConfig
        # debug parameters
        if options.queueData != "":
            build_job_params += "--overwriteQueuedata=%s " % options.queueData
        # container
        build_job_params += get_container_job_params(options)
    set_build_task_params(options, taskParamMap, archiveName, build_job_params)

    # preprocessing step
    set_prepro_task_params(options, taskParamMap)

    # merging
    if options.mergeOutput:
        merge_job_params = f"-r {runDir} "
        if options.mergeScript != "":
            merge_job_params += f'-j "{options.mergeScript}" '
        if options.mergeSingleFile:
            merge_job_params += "--mergeSingleFile "
        if options.rootVer != "":
            merge_job_params += "--rootVer %s " % options.rootVer
        if options.cmtConfig not in ["", "NULL", None]:
            merge_job_params += "--cmtConfig %s " % options.cmtConfig
        if options.useAthenaPackages:
            merge_job_params += "--useAthenaPackages "
        if AthenaUtils.useCMake():
            merge_job_params += "--useCMake "
        if options.useRootCore:
            merge_job_params += "--useRootCore "
        if options.containerImage != "" and not options.alrb:
            merge_job_params += get_container_job_params(options)
        else:
            if use_build_step(options):
                merge_job_params += "-l ${LIB} "
            else:
                merge_job_params += f"-a {archiveName} "
                merge_job_params += "--sourceURL ${SURL} "
        set_merge_task_params(options, taskParamMap, merge_job_params, tmpLog)

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
            exitCode = MiscUtils.EC_Config
        # check task parameters
        if exitCode == 0:
            exitCode, tmpStr = PsubUtils.checkTaskParam(newTaskParamMap, options.unlimitNumOutputs)
        if exitCode != 0 and len(ioList) == 1:
            sys.exit(exitCode)
        if options.noSubmit and options.verbose:
            PsubUtils.print_task_params(newTaskParamMap, tmpLog)
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
            exitCode, tmpStr, taskID = PsubUtils.submit_task(newTaskParamMap, options, tmpLog)
        dumpList.append(PsubUtils.make_dump_item(options, exitCode, tmpStr, taskID, iSubmission, len(ioList)))

    # go back to current dir
    os.chdir(curDir)
    # dump
    if options.dumpJson is not None:
        with open(os.path.expanduser(options.dumpJson), "w") as f:
            json.dump(dumpList, f)
    # succeeded
    sys.exit(exitCode)
