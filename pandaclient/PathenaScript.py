import atexit
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

# tweak sys.argv
sys.argv.pop(0)
sys.argv.insert(0, "pathena")

# parse options and check against the obsolete options first
optP = PathenaArgs.build_parser()
MiscUtils.remove_obsolete_options(sys.argv, MiscUtils.PATHENA_OBSOLETE_OPTIONS)
# using parse_known_args for passing arguments with -
options, args = optP.parse_known_args()

if options.verbose:
    print(options)
    print(f"args={args}")
    print("")

# load json. athena_args holds the job options and arguments given as positional arguments on the command line
jsonExecStr, json_special_values = load_json_options(options, special_keys=("athena_args",))
if "athena_args" in json_special_values:
    args = json_special_values["athena_args"]

# display version
if options.version:
    print(f"Version: {PandaToolsPkgInfo.release_version}")
    sys.exit(0)

from pandaclient import AthenaUtils, Client, PLogger, PsubUtils

# full execution string
fullExecString = PsubUtils.convSysArgv()
fullExecString += jsonExecStr

# get logger
tmpLog = PLogger.getPandaLogger()

# check if unknown arg
for tmp_arg in args:
    # separator
    if tmp_arg == "--":
        break
    if tmp_arg.startswith("-"):
        tmpLog.error(f"unrecognized argument: {tmp_arg}")
        sys.exit(MiscUtils.EC_Config)

# use dev server
if options.devSrv:
    Client.useDevServer()

# use INTR server
if options.intrSrv:
    Client.useIntrServer()

# Athena --CA
if options.componentAccumulator:
    tmpLog.info(
        "Please use --trf if you want to run athena.py --CA. See "
        "https://panda-wms.readthedocs.io/en/latest/client/pathena.html#running-arbitrary-executables-in-the-athena-runtime-environment"
    )
    sys.exit(0)

# noCompile uses noBuild stuff
if options.noCompile:
    if options.noBuild:
        tmpLog.error("--noBuild and --noCompile cannot be used simultaneously")
        sys.exit(MiscUtils.EC_Config)
    options.noBuild = True

# set noBuild for container
if options.containerImage != "":
    options.noBuild = True

# validate --transferType
invalid = get_invalid_transfer_types(options.transferType)
if invalid:
    tmpLog.error(f"--transferType: invalid value(s) {', '.join(sorted(invalid))}. Allowed: {', '.join(sorted(VALID_TRANSFER_TYPES))}")
    sys.exit(MiscUtils.EC_Config)

# files to be deleted
delFilesOnExit = []

# save current dir
currentDir = os.path.realpath(os.getcwd())

# site, memory, and input options handled the same way as in the other tool
includedSite = process_common_options(options, tmpLog)

# site specified
siteSpecified = True
if options.site == "AUTO":
    siteSpecified = False

# use outputPath as outDS
if not options.outDS.endswith("/"):
    options.outDS = options.outDS + "/"

# bulk submission
ioList = get_bulk_io_list(options, tmpLog)

# error
if options.outDS == "":
    tmpLog.error("no outDS is given\n pathena [--inDS input] --outDS output myJobO.py")
    sys.exit(MiscUtils.EC_Config)
if options.nJobs < -1:
    tmpLog.error("Number of jobs should be a positive integer")
    sys.exit(MiscUtils.EC_Config)
if options.pfnList != "":
    if options.inDS != "":
        tmpLog.error("--pfnList and --inDS cannot be used at the same time")
        sys.exit(MiscUtils.EC_Config)
    if options.shipinput:
        tmpLog.error("--shipInput and --inDS cannot be used at the same time")
        sys.exit(MiscUtils.EC_Config)
    if options.site == "AUTO":
        tmpLog.error("--site must be specified when --pfnList is used")
        sys.exit(MiscUtils.EC_Config)

# absolute path for PFN list
usePfnList = False
if options.pfnList != "":
    options.pfnList = os.path.realpath(options.pfnList)
    usePfnList = True

# check split options
check_split_options(options, tmpLog)

# trf parameter
if options.trf == False:
    orig_trfStr = ""
else:
    orig_trfStr = options.trf

# AMI event-level split
if options.useAMIEventLevelSplit is None:
    if options.inDS.startswith("data") or options.goodRunListXML != "":
        # use AMI for real data since the number of events per file is not uniform
        options.useAMIEventLevelSplit = True
    else:
        options.useAMIEventLevelSplit = False

# check DBRelease, good run list, and event picking
check_input_options(options, tmpLog)

# event picking doesn't work with official transformations
if options.eventPickEvtList != "" and options.trf != False:
    tmpLog.error("--eventPickEvtList doesn't work with --trf until official transformations support event picking")
    sys.exit(MiscUtils.EC_Config)


# additional files
options.extFile = options.extFile.split(",")
try:
    options.extFile.remove("")
except Exception:
    pass
AthenaUtils.set_user_set_ext_files(options.extFile)

options.extOutFile = re.sub(" ", "", options.extOutFile)
options.extOutFile = options.extOutFile.split(",")
try:
    options.extOutFile.remove("")
except Exception:
    pass

# merge script, glue packages, and excluded files
process_sandbox_options(options)

# file list
tmpList = options.filelist.split(",")
options.filelist = []
for tmpItem in tmpList:
    if tmpItem == "":
        continue
    # wild card
    tmpItem = tmpItem.replace("*", ".*")
    # append
    options.filelist.append(tmpItem)
# read file list from file
if options.inputFileList != "":
    rFile = open(options.inputFileList)
    for line in rFile:
        line = re.sub("\n", "", line)
        line = line.strip()
        if line != "":
            options.filelist.append(line)
    rFile.close()

# suppressed streams
options.supStream = options.supStream.upper().split(",")
try:
    options.supStream.remove("")
except Exception:
    pass

# split related
set_n_files_from_n_jobs(options)

# check
if options.inDS != "" and options.nJobs > 0 and options.nFilesPerJob < 0 and options.nFiles == 0 and options.nEventsPerJob < 0:
    tmpLog.error("--split requires --nFilesPerJob or --nFiles or --nEventsPerJob when --inDS is specified")
    sys.exit(MiscUtils.EC_Config)

# remove whitespaces
if options.inDS != "":
    options.inDS = options.inDS.replace(" ", "")

# warning
if options.nFilesPerJob > 0 and options.nFilesPerJob < 5:
    tmpLog.warning(
        "Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
        "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option."
    )

# check grid-proxy
PsubUtils.check_proxy(options.verbose, options.vomsRoles)

# get nickname
nickName = PsubUtils.getNickname(options.verbose)

if nickName == "":
    sys.exit(MiscUtils.EC_Config)

# set Rucio accounting
PsubUtils.setRucioAccount(nickName, "pathena", True)

# convert in/outTarBall to full path
if options.inTarBall != "":
    options.inTarBall = os.path.abspath(os.path.expanduser(options.inTarBall))
if options.outTarBall != "":
    options.outTarBall = os.path.abspath(os.path.expanduser(options.outTarBall))

# convert n/outRunConfig to full path
if options.inRunConfig != "":
    options.inRunConfig = os.path.abspath(os.path.expanduser(options.inRunConfig))
if options.outRunConfig != "":
    options.outRunConfig = os.path.abspath(os.path.expanduser(options.outRunConfig))

# check maxCpuCount
if options.maxCpuCount > Client.maxCpuCountLimit:
    tmpLog.error(f"too large maxCpuCount. Must be less than {Client.maxCpuCountLimit}")
    sys.exit(MiscUtils.EC_Config)

# create tmp dir
if options.tmpDir == "":
    tmpDir = f"{currentDir}/{MiscUtils.wrappedUuidGen()}"
else:
    tmpDir = f"{os.path.abspath(options.tmpDir)}/{MiscUtils.wrappedUuidGen()}"
os.makedirs(tmpDir)


# exit action
def _onExit(dir, files, del_command):
    for tmpFile in files:
        del_command(f"rm -rf {tmpFile}")
    del_command(f"rm -rf {dir}")


atexit.register(_onExit, tmpDir, delFilesOnExit, MiscUtils.commands_get_output)


# get Athena versions
if options.verbose or options.containerImage == "":
    cmt_verbose = True
else:
    cmt_verbose = False
stA, retA = AthenaUtils.getAthenaVer(cmt_verbose)
# failed
if not stA:
    if options.containerImage == "":
        sys.exit(MiscUtils.EC_CMT)
    # disable Athena checks when using container image without Athena runtime env
    retA = {
        "workArea": os.getcwd(),
        "athenaVer": "",
        "groupArea": "",
        "cacheVer": "",
        "nightVer": "",
        "cmtConfig": "",
    }

workArea = retA["workArea"]
if retA["athenaVer"]:
    athenaVer = f"Atlas-{retA['athenaVer']}"
else:
    athenaVer = ""
groupArea = retA["groupArea"]
cacheVer = retA["cacheVer"]
nightVer = retA["nightVer"]

# overwrite with athenaTag
if options.athenaTag != "":
    athenaVer, cacheVer, nightVer = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmpLog)

# set CMTCONFIG
options.cmtConfig = AthenaUtils.getCmtConfig(athenaVer, cacheVer, nightVer, options.cmtConfig, verbose=options.verbose)

# check CMTCONFIG
if not AthenaUtils.checkCmtConfig(retA["cmtConfig"], options.cmtConfig, options.noBuild):
    sys.exit(MiscUtils.EC_CMT)

tmpLog.info(f"using CMTCONFIG={options.cmtConfig}")

# get run directory
# remove special characters
sString = re.sub(r"[\+]", ".", workArea)
runDir = re.sub(f"^{sString}", "", currentDir)
if runDir == currentDir and not AthenaUtils.useCMake() and options.containerImage == "":
    errMsg = f"You need to run pathena in a directory under {workArea}. "
    errMsg += f"If '{workArea}' is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup."
    tmpLog.error(errMsg)
    sys.exit(MiscUtils.EC_Config)
elif runDir == "":
    runDir = "."
elif runDir.startswith("/"):
    runDir = runDir[1:]
runDir = runDir + "/"

# event picking
if options.eventPickEvtList != "":
    epLockedBy = "pathena"
    if not options.noSubmit:
        epStat, epOutput = Client.requestEventPicking(
            options.eventPickEvtList,
            options.eventPickDataType,
            options.eventPickStreamName,
            options.eventPickDS,
            options.eventPickAmiTag,
            options.filelist,
            "",
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
    tmpLog.info(f"requested Event Picking service to stage input as {options.inDS}")
    # make run/event list file for event picking
    eventPickRunEvtDat = f"{currentDir}/ep_{MiscUtils.wrappedUuidGen()}.dat"
    evI = open(options.eventPickEvtList)
    evO = open(eventPickRunEvtDat, "w")
    evO.write(evI.read())
    # close
    evI.close()
    evO.close()
    # add to be deleted on exit
    delFilesOnExit.append(eventPickRunEvtDat)

# get job options
jobO = ""
if options.trf:
    # replace : to = for backward compatibility
    for optArg in ["DB", "RNDM"]:
        options.trf = re.sub("%" + optArg + ":", "%" + optArg + "=", options.trf)
    # use trf's parameters
    jobO = options.trf
else:
    # get jobOs from command-line
    if options.preConfig != "":
        jobO += f"-p {options.preConfig} "
    if options.singleLine != "":
        options.singleLine = options.singleLine.replace('"', "'")
    for arg in args:
        jobO += f" {arg}"
if jobO == "":
    tmpLog.error("no jobOptions is given\n   pathena [--inDS input] --outDS output myJobO.py")
    sys.exit(MiscUtils.EC_Config)


if options.inRunConfig == "":
    # extract run configuration
    tmpLog.info("extracting run configuration")
    # run ConfigExtractor for normal jobO
    ret, runConfig = AthenaUtils.extractRunConfig(
        jobO,
        options.supStream,
        options.shipinput,
        options.trf,
        verbose=options.verbose,
        useAMI=options.useAMIAutoConf,
        inDS=options.inDS,
        tmpDir=tmpDir,
        one_liner=options.singleLine,
    )
    # save runconfig
    if options.outRunConfig != "":
        cFile = open(options.outRunConfig, "w")
        pickle.dump(runConfig, cFile)
        cFile.close()
else:
    # load from file
    ret = True
    tmpRunConfFile = open(options.inRunConfig)
    runConfig = pickle.load(tmpRunConfFile)
    tmpRunConfFile.close()
if not options.trf:
    # extractor failed
    if not ret:
        sys.exit(MiscUtils.EC_Extractor)
    # shipped files
    if runConfig.other.inputFiles:
        for fileName in runConfig.other.inputFiles:
            # append .root for tag files
            if runConfig.other.inColl:
                match = re.search(r"\.root(\.\d+)*$", fileName)
                if match is None:
                    fileName = f"{fileName}.root"
            # check ship files in the current dir
            if not os.path.exists(fileName):
                tmpLog.error(f"{fileName} needs exist in the current directory when --shipInput is used")
                sys.exit(MiscUtils.EC_Extractor)
            # append to extFile
            options.extFile.append(fileName)
            if not runConfig.input.shipFiles:
                runConfig.input["shipFiles"] = []
            if fileName not in runConfig.input["shipFiles"]:
                runConfig.input["shipFiles"].append(fileName)
    # generator files
    if runConfig.other.rndmGenFile:
        # append to extFile
        for fileName in runConfig.other.rndmGenFile:
            options.extFile.append(fileName)
    # Condition file
    if runConfig.other.condInput:
        # append to extFile
        for fileName in runConfig.other.condInput:
            if options.addPoolFC == "":
                options.addPoolFC = fileName
            else:
                options.addPoolFC += f",{fileName}"
    # set default ref name
    if not runConfig.input.collRefName:
        runConfig.input.collRefName = "Token"
    # check duplication in extOutFile
    if runConfig.output.alloutputs != False:
        if options.verbose:
            tmpLog.debug(f"output files : {runConfig.output.alloutputs!s}")
        for tmpExtOutFile in tuple(options.extOutFile):
            if tmpExtOutFile in runConfig.output.alloutputs:
                options.extOutFile.remove(tmpExtOutFile)
    # add -c
    if options.singleLine:
        jobO = f'-c "{options.singleLine}" ' + jobO
else:
    # parse parameters for trf
    # AMI tag
    newJobO = ""
    for tmpString in jobO.split(";"):
        match = re.search(" AMI=", tmpString)
        if match is None:
            # use original command
            newJobO += tmpString + ";"
        else:
            tmpLog.info("getting configuration from AMI")
            # get configuration using GetCommand.py
            com = "GetCommand.py " + re.sub("^[^ ]+ ", "", tmpString.strip())
            if options.verbose:
                tmpLog.debug(com)
            amiSt, amiOut = MiscUtils.commands_get_status_output_with_env(com)
            amiSt %= 255
            if amiSt != 0:
                tmpLog.error(amiOut)
                errSt = "Failed to get configuration from AMI. "
                errSt += "Using AMI=tag in --trf is disallowed since it may overload the AMI server. "
                errSt += "Please use explicit configuration parameters in --trf"
                tmpLog.error(errSt)
                sys.exit(MiscUtils.EC_Config)
            # get full command string
            fullCommand = ""
            for amiStr in amiOut.split("\n"):
                if amiStr != "" and not amiStr.startswith("#") and not amiStr.startswith("*"):
                    fullCommand = amiStr
            # failed to extract configuration
            if fullCommand == "":
                tmpLog.error(amiOut)
                errSt = "Failed to extract configuration from AMI's output"
                tmpLog.error(errSt)
                sys.exit(MiscUtils.EC_Config)
            # replace
            newJobO += fullCommand + ";"
    # remove redundant ;
    newJobO = newJobO[:-1]
    # replace
    if newJobO != "":
        jobO = newJobO
        if options.verbose:
            tmpLog.debug("new jobO : " + jobO)
    # output
    oneOut = False
    # replace ; for job sequence
    tmpString = re.sub(";", " ", jobO)
    # look for --outputDAODFile/--outputD2AODFile and --reductionConf/formats
    match = re.search(r"(--outputDAODFile|--outputD2AODFile)[ =\"']+([^ \"',]+)", tmpString)
    dxaod_output_filename = None
    if match is not None:
        dxaod_output_format = match.group(1)[8:][:-4]
        dxaod_output_filename = match.group(2)
        # remove %OUT
        dxaod_output_filename = re.sub(r"%OUT\.", "", dxaod_output_filename)
        match = re.search(r"(--reductionConf|--formats)[ =\"\']+([^ \"\']+)", tmpString)
        if match is not None:
            # remove %OUT from outputDAODFile
            jobO = jobO.replace("%OUT." + dxaod_output_filename, dxaod_output_filename)
            # loop over all configs
            reductionConf = match.group(2)
            for reductionItem in reductionConf.split(","):
                reductionItem = reductionItem.strip()
                if reductionItem == "":
                    continue
                # make actual output names for derivation
                tmpOutName = f"{dxaod_output_format}_{reductionItem}.{dxaod_output_filename}"
                if tmpOutName not in options.extOutFile:
                    options.extOutFile.append(tmpOutName)
                    oneOut = True
    # look for %OUT
    for tmpItem in tmpString.split():
        match = re.search("\\%OUT\\.([^ \"',]+)", tmpItem)
        if match:
            # append basenames to extOutFile
            tmpOutName = match.group(1)
            # skip basename of derivation
            if dxaod_output_filename is not None and dxaod_output_filename == tmpOutName:
                continue
            if tmpOutName not in options.extOutFile:
                options.extOutFile.append(tmpOutName)
                oneOut = True
    # look for --argjson
    match = re.search(r"--argjson[ =]+([^ ,;]+)", tmpString, re.IGNORECASE)
    if match is not None:
        # append json file to extFile
        arg_json_file = match.group(1)
        if arg_json_file not in options.extFile:
            options.extFile.append(arg_json_file)
        options.useArgJson = True
    # warning if no output
    if not oneOut:
        tmpLog.warning("argument of --trf doesn't contain any %OUT")
    # check for maxEvents and skipEvents
    if options.nEventsPerJob > 0 and options.nEventsPerJob < options.nEventsPerFile:
        if "%SKIPEVENTS" not in jobO:
            tmpLog.warning(
                "Argument of --trf doesn't contain %SKIPEVENTS. All jobs with the same input file "
                "may process the same events unless first events are skipped by using a trf parameter "
                "like skipEvents or something"
            )
        if "maxEvents" not in jobO:
            tmpLog.warning(
                "Argument of --trf doesn't contain maxEvents or something equivalent. Each job may process all events "
                "in the input file. Note that --nEventsPerJob doesn't automatically append maxEvents "
                "to the argument. Please ignore this message if you limit the number of events "
                "in each job by using another trf parameter"
            )
    # warning for multicore/thread
    if options.nCore > 1 or options.nThreads > 1:
        if "--multiprocess" not in jobO and "--multithreaded" not in jobO:
            tmp_msg = "Neither --multiprocess or --multithreaded is set in --trf despite "
            if options.nCore > 1:
                tmp_msg += f"--nCore={options.nCore} "
            else:
                tmp_msg += f"--nThreads={options.nThreads} "
            tmp_msg += "which may cause inefficient CPU usage on MCORE resources"
            tmpLog.warning(tmp_msg)

# no output jobs
tmpOutKeys = list(runConfig.output)
for tmpIgnorKey in ["outUserData", "alloutputs"]:
    try:
        tmpOutKeys.remove(tmpIgnorKey)
    except Exception:
        pass
if tmpOutKeys == [] and options.extOutFile == [] and not options.noOutput:
    errStr = "No output stream was extracted from jobOs or --trf. "
    if not options.trf:
        errStr += "If your job defines an output without Athena framework "
        errStr += "(e.g., using ROOT.TFile.Open instead of THistSvc) "
        errStr += "please specify the output filename by using --extOutFile. "
        errStr += "Or if you define the output with a relatively new mechanism "
        errStr += "please report it to Savannah to update the automatic extractor. "
    errStr += "If you are sure that your job doesn't produce any output file "
    errStr += "(e.g., HelloWorldOptions.py) please use --noOutput. "
    tmpLog.error(errStr)
    sys.exit(MiscUtils.EC_Extractor)

# set extOutFile to runConfig
if options.extOutFile != []:
    runConfig.output["extOutFile"] = options.extOutFile

# check ship files in the current dir
if not runConfig.input.shipFiles:
    runConfig.input.shipFiles = []
for file in runConfig.input.shipFiles:
    if not os.path.exists(file):
        tmpLog.error(f"{file} needs exist in the current directory when using --shipInput")
        sys.exit(MiscUtils.EC_Extractor)

# get random number
runConfig.other["rndmNumbers"] = []
if not runConfig.other.rndmStream:
    runConfig.other.rndmStream = []
if len(runConfig.other.rndmStream) != 0:
    if options.norandom:
        print()
        print("Initial random seeds need to be defined.")
        print("Enter two numbers for each random stream.")
        print("  e.g., PYTHIA : 4789899 989240512")
        print("")
    for stream in runConfig.other.rndmStream:
        if options.norandom:
            # enter manually
            while True:
                randStr = input(f"{stream} : ")
                num = randStr.split()
                if len(num) == 2:
                    break
                print(" Two numbers are needed")
            runConfig.other.rndmNumbers.append([int(num[0]), int(num[1])])
        else:
            # automatic
            runConfig.other.rndmNumbers.append([random.randint(1, 5000000), random.randint(1, 5000000)])
    if options.norandom:
        print("")
if runConfig.other.G4RandomSeeds == True:
    if options.norandom:
        print("")
        print("Initial G4 random seeds need to be defined.")
        print("Enter one positive number.")
        print("")
        # enter manually
        while True:
            num = input("SimFlags.SeedsG4=")
            try:
                num = int(num)
                if num > 0:
                    runConfig.other.G4RandomSeeds = num
                    break
            except Exception:
                pass
        print("")
    else:
        # automatic
        runConfig.other.G4RandomSeeds = random.randint(1, 10000)
else:
    # set -1 to disable G4 Random Seeds
    runConfig.other.G4RandomSeeds = -1


#####################################################################
# input datasets

if options.inDS != "" or options.shipinput or options.pfnList != "":
    # minimum bias dataset
    if options.trf and jobO.find("%MININ") != -1:
        runConfig.input.inMinBias = True
    if runConfig.input.inMinBias:
        options.minDS, options.nMin = MiscUtils.getDatasetNameAndNumFiles(options.minDS, options.nMin, "Minimum-Bias")
    # low pT minimum bias dataset
    if options.trf and jobO.find("%LOMBIN") != -1:
        runConfig.input.inLoMinBias = True
    if runConfig.input.inLoMinBias:
        options.lowMinDS, options.nLowMin = MiscUtils.getDatasetNameAndNumFiles(options.lowMinDS, options.nLowMin, "Low pT Minimum-Bias")
    # high pT minimum bias dataset
    if options.trf and jobO.find("%HIMBIN") != -1:
        runConfig.input.inHiMinBias = True
    if runConfig.input.inHiMinBias:
        options.highMinDS, options.nHighMin = MiscUtils.getDatasetNameAndNumFiles(options.highMinDS, options.nHighMin, "High pT Minimum-Bias")
    # cavern dataset
    if options.trf and jobO.find("%CAVIN") != -1:
        runConfig.input.inCavern = True
    if runConfig.input.inCavern:
        options.cavDS, options.nCav = MiscUtils.getDatasetNameAndNumFiles(options.cavDS, options.nCav, "Cavern")
    # beam halo dataset
    if options.trf and jobO.find("%BHIN") != -1:
        runConfig.input.inBeamHalo = True
    if runConfig.input.inBeamHalo:
        # use common DS
        if options.useCommonHalo:
            options.beamHaloDS, options.nBeamHalo = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloDS, options.nBeamHalo, "BeamHalo")
        else:
            # get DS for A-side
            (
                options.beamHaloADS,
                options.nBeamHaloA,
            ) = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloADS, options.nBeamHaloA, "BeamHalo A-side")
            # get DS for C-side
            (
                options.beamHaloCDS,
                options.nBeamHaloC,
            ) = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloCDS, options.nBeamHaloC, "BeamHalo C-side")
    # beam gas dataset
    if options.trf and jobO.find("%BGIN") != -1:
        runConfig.input.inBeamGas = True
    if runConfig.input.inBeamGas:
        # use common DS
        if options.useCommonGas:
            options.beamGasDS, options.nBeamGas = MiscUtils.getDatasetNameAndNumFiles(options.beamGasDS, options.nBeamGas, "BeamGas")
        else:
            # get DS for H
            options.beamGasHDS, options.nBeamGasH = MiscUtils.getDatasetNameAndNumFiles(options.beamGasHDS, options.nBeamGasH, "BeamGas Hydrogen")
            # get DS for C
            options.beamGasCDS, options.nBeamGasC = MiscUtils.getDatasetNameAndNumFiles(options.beamGasCDS, options.nBeamGasC, "BeamGas Carbon")
            # get DS for O
            options.beamGasODS, options.nBeamGasO = MiscUtils.getDatasetNameAndNumFiles(options.beamGasODS, options.nBeamGasO, "BeamGas Oxygen")

    # general secondaries
    parse_secondary_datasets(options, tmpLog)


#####################################################################
# archive sources and send it to HTTP-reachable location

if True:
    if options.inTarBall == "" and options.tarBallViaDDM == "":
        # extract jobOs with full pathnames
        for tmpItem in jobO.split():
            if re.search(r"^/.*\.py$", tmpItem) is not None:
                # set random name to avoid overwriting
                tmpName = tmpItem.split("/")[-1]
                tmpName = f"{MiscUtils.wrappedUuidGen()}_{tmpName}"
                # set
                AthenaUtils.fullPathJobOs[tmpItem] = tmpName

        # copy some athena specific files
        AthenaUtils.copyAthenaStuff(currentDir)

        # set extFile
        AthenaUtils.setExtFile(options.extFile)

        archiveName = ""
        if not (options.noBuild and not options.noCompile):
            # archive with cpack
            if AthenaUtils.useCMake():
                archiveName, archiveFullName = AthenaUtils.archiveWithCpack(True, tmpDir, options.verbose)
            # archive sources
            archiveName, archiveFullName = AthenaUtils.archiveSourceFiles(
                workArea,
                runDir,
                currentDir,
                tmpDir,
                options.verbose,
                options.gluePackages,
                archiveName=archiveName,
            )
        else:
            # archive with cpack
            if AthenaUtils.useCMake():
                archiveName, archiveFullName = AthenaUtils.archiveWithCpack(False, tmpDir, options.verbose)
            # archive jobO
            archiveName, archiveFullName = AthenaUtils.archiveJobOFiles(
                workArea,
                runDir,
                currentDir,
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
        # back to tmp dir
        os.chdir(tmpDir)
        # remove some athena specific files
        AthenaUtils.deleteAthenaStuff(currentDir)
        if not os.path.exists(archiveName):
            MiscUtils.commands_get_status_output(f"tar -cf {archiveName} -T /dev/null")
        # compress
        status, out = MiscUtils.commands_get_status_output(f"gzip -f {archiveName}")
        if status != 0 or options.verbose:
            print(out)
        archiveName += ".gz"
        # check archive
        status, out = MiscUtils.commands_get_status_output(f"ls -l {archiveName}")
        if status != 0:
            print(out)
            tmpLog.error("Failed to archive working area.\n        If you see 'Disk quota exceeded', try '--tmpDir /tmp'")
            sys.exit(MiscUtils.EC_Archive)

        # check symlinks
        PsubUtils.check_sandbox_symlinks(archiveName, tmpLog)
    else:
        archiveName, archiveFullName = PsubUtils.use_existing_sandbox(options, tmpDir)

    # save for --outTarBall, and upload the sandbox and the good run list
    archiveName = PsubUtils.upload_sandbox(options, archiveName, currentDir, tmpLog, delFilesOnExit)


####################################################################3
# submit jobs

# append tmpdir to import taskbuffer module
sys.path = [tmpDir] + sys.path

# make task
taskParamMap = {}
processing_type = f"panda-client-{PandaToolsPkgInfo.release_version}-jedi-athena"
if options.trf:
    processing_type += "-trf"
set_common_task_params(options, taskParamMap, processing_type, includedSite, fullExecString, archiveName)
taskParamMap["vo"] = "atlas"
if options.containerImage == "":
    taskParamMap["architecture"] = AthenaUtils.getCmtConfigImg(
        athenaVer,
        cacheVer,
        nightVer,
        options.cmtConfig,
        architecture=options.architecture,
    )
else:
    taskParamMap["architecture"] = options.architecture
    taskParamMap["container_name"] = options.containerImage
taskParamMap["transUses"] = athenaVer
taskParamMap["transHome"] = "AnalysisTransforms" + cacheVer + nightVer
if options.workingGroup is not None:
    taskParamMap["workingGroup"] = options.workingGroup
if options.maxCpuCount > 0:
    taskParamMap["walltime"] = -options.maxCpuCount
if options.nCore <= 1 < options.nThreads:
    taskParamMap["coreCount"] = options.nThreads
# source URL
set_source_url(taskParamMap, Client.baseURLCSRVSSL)
# log
set_log_task_params(options, taskParamMap)

# make job parameters
taskParamMap["jobParameters"] = []

# build
if options.noBuild and not options.noCompile:
    tmp_str = f"-a {archiveName}"
    if options.tarBallViaDDM:
        tmp_str += " --noTarballDownload"
else:
    tmp_str = "-l ${LIB}"
taskParamMap["jobParameters"] += [
    {"type": "constant", "value": tmp_str},
]

# pre execution string
pStr1 = ""
if runConfig.other.rndmStream != []:
    pStr1 = "AtRndmGenSvc=Service('AtRndmGenSvc');AtRndmGenSvc.Seeds=["
    for stream in runConfig.other.rndmStream:
        num = runConfig.other.rndmNumbers[runConfig.other.rndmStream.index(stream)]
        pStr1 += f"'{stream} ${{RNDMSEED}} {num[1]}',"
    pStr1 += "]"
    dictItem = {
        "type": "template",
        "param_type": "number",
        "value": "${RNDMSEED}",
        "hidden": True,
        "offset": runConfig.other.rndmStream[0][0],
    }
    taskParamMap["jobParameters"] += [dictItem]
# split by event option was invoked
pStr2 = ""
if options.nEventsPerJob > 0 and (not options.trf):
    # @ Number of events to be processed per job
    param1 = "theApp.EvtMax=${MAXEVENTS}"
    # @ possibly skip events in a file
    if runConfig.input.noInput:
        pStr2 = param1
    else:
        param2 = "EventSelector.SkipEvents=${SKIPEVENTS}"
        # @ Form a string to add to job parameters
        pStr2 = f"{param1};{param2}"
# set pre execution parameter
if pStr1 != "" or pStr2 != "":
    if pStr1 == "" or pStr2 == "":
        preStr = pStr1 + pStr2
    else:
        preStr = f"{pStr1};{pStr2}"
    taskParamMap["jobParameters"] += [
        {
            "type": "constant",
            "value": '-f "',
            "padding": False,
        },
    ]
    taskParamMap["jobParameters"] += PsubUtils.convertParamStrToJediParam(preStr, {}, "", False, False)
    taskParamMap["jobParameters"] += [
        {
            "type": "constant",
            "value": '"',
        },
    ]

# misc
param = "--sourceURL ${SURL} "
param += f"-r {runDir} "
# addPoolFC
if options.addPoolFC != "":
    param += f"--addPoolFC {options.addPoolFC} "
# disable to skip missing files
if options.notSkipMissing:
    param += "--notSkipMissing "
# given PFN
if options.pfnList != "":
    param += "--givenPFN "
# run TRF
if options.trf:
    param += "--trf "
# use arg json
if options.useArgJson:
    param += "--useArgJson "
# general input format
if options.generalInput:
    param += "--generalInput "
# use theApp.nextEvent
if options.useNextEvent:
    param += "--useNextEvent "
# use CMake
if AthenaUtils.useCMake() or options.containerImage != "":
    param += "--useCMake "
# AthenaMT
if options.nThreads > 1:
    param += "--useAthenaMT "
# use code tracer
if options.codeTrace:
    param += "--codeTrace "
# debug parameters
if options.queueData != "":
    param += f"--overwriteQueuedata={options.queueData} "
# read BS
if runConfig.input.inBS:
    param += "-b "
# use back navigation
if runConfig.input.backNavi:
    param += "-e "
# ship input
if options.shipinput:
    param += "--shipInput "
# event picking
if options.eventPickEvtList != "":
    param += f"--eventPickTxt={eventPickRunEvtDat.split('/')[-1]} "
# assign
if param != "":
    taskParamMap["jobParameters"] += [
        {
            "type": "constant",
            "value": param,
        },
    ]

# input
inputMap = {}
if options.inDS != "":
    tmpDict = {
        "type": "template",
        "param_type": "input",
        "value": '-i "${IN/T}"',
        "dataset": options.inDS,
        "expand": True,
        "exclude": r"\.log\.tgz(\.\d+)*$",
    }
    if options.notExpandInDS:
        del tmpDict["expand"]
        tmpDict["consolidate"] = ".".join(options.outDS.split(".")[:2]) + "." + MiscUtils.wrappedUuidGen() + "/"
    if options.inputType != "":
        tmpDict["include"] = options.inputType
    if options.filelist != []:
        tmpDict["files"] = options.filelist
    taskParamMap["jobParameters"].append(tmpDict)
    taskParamMap["dsForIN"] = options.inDS
    inputMap["IN"] = options.inDS
elif options.pfnList != "":
    set_pfn_list_input_params(options, taskParamMap)
elif options.goodRunListXML != "":
    set_good_run_list_input_params(taskParamMap)
    inputMap["IN"] = "%%INDS%%"
else:
    # no input
    taskParamMap["noInput"] = True
    taskParamMap["jobParameters"] += [
        {
            "type": "constant",
            "value": '-i "[]"',
        },
    ]

# extract DBR for --trf
dbrInTRF = False
if options.trf:
    tmpMatch = re.search("%DB=([^ '\";]+)", jobO)
    if tmpMatch is not None:
        options.dbRelease = tmpMatch.group(1)
        dbrInTRF = True
# param for DBR
taskParamMap["jobParameters"] += get_dbr_job_params(options.dbRelease, dbrInTRF)

# minimum bias
minBiasStream = ""
if options.minDS != "":
    if options.notExpandMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam(
        "${MININ}",
        options.minDS,
        "input",
        hidden=True,
        expand=expand_flag,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nMin,
        useNumFilesAsRatio=True,
        randomAtt=options.randomMin,
        outDS=options.outDS,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["MININ"] = options.minDS
    minBiasStream += "MININ,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
if options.lowMinDS != "":
    if options.notExpandLowMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam(
        "${LOMBIN}",
        options.lowMinDS,
        "input",
        hidden=True,
        expand=expand_flag,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nLowMin,
        useNumFilesAsRatio=True,
        randomAtt=options.randomMin,
        outDS=options.outDS,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["LOMBIN"] = options.lowMinDS
    minBiasStream += "LOMBIN,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
if options.highMinDS != "":
    if options.notExpandHighMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam(
        "${HIMBIN}",
        options.highMinDS,
        "input",
        hidden=True,
        expand=expand_flag,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nHighMin,
        useNumFilesAsRatio=True,
        randomAtt=options.randomMin,
        outDS=options.outDS,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["HIMBIN"] = options.highMinDS
    minBiasStream += "HIMBIN,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
minBiasStream = minBiasStream[:-1]
if minBiasStream != "":
    dictItem = {
        "type": "constant",
        "value": f'-m "${{{minBiasStream}/T}}"',
    }
    taskParamMap["jobParameters"] += [dictItem]


# cavern
if options.cavDS != "":
    if options.notExpandCavDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam(
        '-n "${CAVIN/T}"',
        options.cavDS,
        "input",
        expand=expand_flag,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nCav,
        useNumFilesAsRatio=True,
        randomAtt=options.randomCav,
        outDS=options.outDS,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["CAVIN"] = options.cavDS
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True

# beam halo
beamHaloStream = ""
if options.beamHaloDS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BHIN}",
        options.beamHaloDS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamHalo,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BHIN"] = options.beamHaloDS
    beamHaloStream += "BHIN,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
if options.beamHaloADS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BHAIN}",
        options.beamHaloADS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamHaloA,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BHAIN"] = options.beamHaloADS
    beamHaloStream += "BHAIN,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
if options.beamHaloCDS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BHCIN}",
        options.beamHaloCDS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamHaloC,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BHCIN"] = options.beamHaloCDS
    beamHaloStream += "BHCIN,"
    if options.sameSecRetry:
        taskParamMap["reuseSecOnDemand"] = True
beamHaloStream = beamHaloStream[:-1]
if beamHaloStream != "":
    dictItem = {
        "type": "constant",
        "value": f'--beamHalo "${{{beamHaloStream}/T}}"',
    }
    taskParamMap["jobParameters"] += [dictItem]


# beam gas
beamGasStream = ""
if options.beamGasDS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BGIN}",
        options.beamGasDS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamGas,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BGIN"] = options.beamGasDS
    beamGasStream += "BGIN,"
if options.beamGasHDS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BGHIN}",
        options.beamGasHDS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamGasH,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BGHIN"] = options.beamGasHDS
    beamGasStream += "BGHIN,"
if options.beamGasCDS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BGCIN}",
        options.beamGasCDS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamGasC,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BGCIN"] = options.beamGasHDS
    beamGasStream += "BGCIN,"
if options.beamGasODS != "":
    dictItem = MiscUtils.makeJediJobParam(
        "${BGOIN}",
        options.beamGasODS,
        "input",
        hidden=True,
        expand=True,
        exclude=r"\.log\.tgz(\.\d+)*$",
        nFilesPerJob=options.nBeamGasO,
        useNumFilesAsRatio=True,
    )
    taskParamMap["jobParameters"] += dictItem
    inputMap["BGOIN"] = options.beamGasODS
    beamGasStream += "BGOIN,"
beamGasStream = beamGasStream[:-1]
if beamGasStream != "":
    dictItem = {
        "type": "constant",
        "value": f'--beamGas "${{{beamGasStream}/T}}"',
    }
    taskParamMap["jobParameters"] += [dictItem]

# general secondaries
if options.secondaryDSs:
    for tmpDsName in options.secondaryDSs:
        tmpMap = options.secondaryDSs[tmpDsName]
        # make template item
        streamName = tmpMap["streamName"]
        if not options.notExpandSecDSs:
            expandFlag = True
        else:
            expandFlag = False
        dictItem = MiscUtils.makeJediJobParam(
            "${" + streamName + "}",
            tmpDsName,
            "input",
            hidden=True,
            expand=expandFlag,
            include=tmpMap["pattern"],
            offset=tmpMap["nSkip"],
            nFilesPerJob=tmpMap["nFiles"],
            outDS=options.outDS,
            file_list=tmpMap["files"],
        )
        taskParamMap["jobParameters"] += dictItem
        inputMap[streamName] = tmpDsName
    dictItem = {
        "type": "constant",
        "value": f"-m \"${{{','.join([tmpMap['streamName'] for tmpMap in options.secondaryDSs.values()])}/T}}\"",
    }
    taskParamMap["jobParameters"] += [dictItem]
    taskParamMap["reuseSecOnDemand"] = True

# output
if options.addNthFieldOfInDSToLFN != "" or options.addNthFieldOfInFileToLFN != "":
    descriptionInLFN = "${MIDDLENAME}"
else:
    descriptionInLFN = ""
outMap, tmpParamList = AthenaUtils.convertConfToOutput(
    runConfig,
    options.extOutFile,
    options.outDS,
    destination=options.destSE,
    spaceToken=options.spaceToken,
    descriptionInLFN=descriptionInLFN,
    allowNoOutput=options.allowNoOutput,
    appendFileFieldToStreamName=options.appendStrToExtStream or options.useFileFieldAsStream,
    useEXTStreamName=not options.useFileFieldAsStream,
    extOutStreams=options.outputStreamNames,
)
taskParamMap["jobParameters"] += [
    {"type": "constant", "value": f'-o "{outMap}" '},
]
taskParamMap["jobParameters"] += tmpParamList
# output files allowed not to be produced
taskParamMap["jobParameters"] += get_allow_no_output_job_params(tmpParamList)
for tmpPatt in get_unmatched_allow_no_output_patterns(options.allowNoOutput, tmpParamList):
    tmpLog.warning(
        f"--allowNoOutput pattern '{tmpPatt}' matches none of output filename templates "
        f"{[item['value'] for item in tmpParamList if item.get('param_type') == 'output']}"
    )


# jobO parameter
if not options.trf:
    tmpJobO = jobO
    # replace full-path jobOs
    for tmpFullName in AthenaUtils.fullPathJobOs:
        tmpLocalName = AthenaUtils.fullPathJobOs[tmpFullName]
        tmpJobO = re.sub(tmpFullName, tmpLocalName, tmpJobO)
    # modify one-liner for G4 random seeds
    if runConfig.other.G4RandomSeeds > 0:
        if options.singleLine != "":
            tmpJobO = re.sub(
                f'-c "{options.singleLine}" ',
                '-c "%s;from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' % options.singleLine,
                tmpJobO,
            )
        else:
            tmpJobO = '-c "from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' + tmpJobO
        dictItem = {
            "type": "template",
            "param_type": "number",
            "value": "${RNDMSEED}",
            "hidden": True,
            "offset": runConfig.other.G4RandomSeeds,
        }
        taskParamMap["jobParameters"] += [dictItem]
else:
    # replace parameters for TRF
    tmpJobO = jobO
    # output : basenames are in outMap['IROOT'] trough extOutFile
    tmpOutMap = []
    if "IROOT" in outMap:
        for tmpName, tmpLFN in outMap["IROOT"]:
            tmpJobO = tmpJobO.replace("%OUT." + tmpName, tmpName)
    # replace DBR
    tmpJobO = re.sub("%DB=[^ '\";]+", "${DBR}", tmpJobO)
# set jobO parameter
taskParamMap["jobParameters"] += [
    {
        "type": "constant",
        "value": '-j "',
        "padding": False,
    },
]
if options.secondaryDSs:
    extra_in_list = [tmpMap["streamName"] for tmpMap in options.secondaryDSs.values()]
else:
    extra_in_list = []
taskParamMap["jobParameters"] += PsubUtils.convertParamStrToJediParam(
    tmpJobO,
    inputMap,
    options.outDS[:-1],
    True,
    False,
    usePfnList,
    extra_in_list=extra_in_list,
)
taskParamMap["jobParameters"] += [
    {
        "type": "constant",
        "value": '"',
    },
]

# use local IO for trf or BS
if options.forceStaged or ((options.trf or runConfig.input.inBS) and not options.forceDirectIO) or options.transferType == "file":
    taskParamMap["useLocalIO"] = 1
elif options.useDirectIOSites or options.transferType == "direct":
    taskParamMap["allowInputLAN"] = "only"

# use AMI to get the number of events per file
if options.useAMIEventLevelSplit == True:
    taskParamMap["getNumEventsInMetadata"] = True

# build step
build_job_params = None
if use_build_step(options):
    build_job_params = "-i ${IN} -o ${OUT} --sourceURL ${SURL} "
    # no compile
    if options.noCompile:
        build_job_params += "--noCompile "
    # use CMake
    if AthenaUtils.useCMake() or options.containerImage != "":
        build_job_params += "--useCMake "
    # debug parameters
    if options.queueData != "":
        build_job_params += f"--overwriteQueuedata={options.queueData} "
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
    if not options.noBuild:
        merge_job_params += "-l ${LIB} "
    else:
        merge_job_params += f"-a {archiveName} "
        merge_job_params += "--sourceURL ${SURL} "
    merge_job_params += "--useAthenaPackages "
    if AthenaUtils.useCMake() or options.containerImage != "":
        merge_job_params += "--useCMake "
    set_merge_task_params(options, taskParamMap, merge_job_params, tmpLog)


#####################################################################
# submission

exitCode = 0
dumpList = []

# submit task
for iSubmission, ioItem in enumerate(ioList):
    if options.verbose:
        print("== parameters ==")
        print(f"Site       : {options.site}")
        print(f"Athena     : {athenaVer}")
        if groupArea != "":
            print(f"Group Area : {groupArea}")
        if cacheVer != "":
            print(f"ProdCache  : {cacheVer[1:]}")
        if nightVer != "":
            print(f"Nightly    : {nightVer[1:]}")
        print(f"cmtConfig  : {AthenaUtils.getCmtConfigImg(athenaVer, cacheVer, nightVer, options.cmtConfig)}")
        print(f"RunDir     : {runDir}")
        print(f"jobO       : {jobO.lstrip()}")

    if len(ioList) == 1:
        newTaskParamMap = taskParamMap
    else:
        # replace input and output
        options.inDS = ioItem["inDS"]
        options.outDS = ioItem["outDS"]
        newTaskParamMap = PsubUtils.replaceInputOutput(taskParamMap, ioItem["inDS"], ioItem["outDS"], iSubmission)
    taskID = None
    # check outDS format
    if not PsubUtils.checkOutDsName(options.outDS, options.official, nickName, options.mergeOutput, options.verbose):
        tmpLog.error(f"invalid output datasetname:{options.outDS}")
        sys.exit(MiscUtils.EC_Config)
    # check task parameters
    exitCode, tmpStr = PsubUtils.checkTaskParam(newTaskParamMap, options.unlimitNumOutputs)
    if exitCode != 0 and len(ioList) == 1:
        sys.exit(exitCode)
    if options.noSubmit and options.verbose:
        PsubUtils.print_task_params(newTaskParamMap, tmpLog)
    if not options.noSubmit and exitCode == 0:
        exitCode, tmpStr, taskID = PsubUtils.submit_task(newTaskParamMap, options, tmpLog)
    dumpList.append(PsubUtils.make_dump_item(options, exitCode, tmpStr, taskID, iSubmission, len(ioList)))

# go back to current dir
os.chdir(currentDir)
# dump
if options.dumpJson is not None:
    with open(options.dumpJson, "w") as f:
        json.dump(dumpList, f)
# succeeded
sys.exit(0)
