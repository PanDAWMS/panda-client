import copy
import datetime
import gzip
import os
import platform
import re
import shutil
import sys
import time
from urllib.parse import quote

from . import Client, MiscUtils, PLogger
from .MiscUtils import commands_get_status_output, commands_get_status_output_with_env

cacheProxyStatus = None
cacheVomsStatus = None
cacheActimeStatus = None
cacheVomsFQAN = ""
cacheActime = ""
cacheLastUpdate = None
cacheVomsInfo = None


# reset cache values
def resetCacheValues():
    global cacheProxyStatus
    global cacheVomsStatus
    global cacheActimeStatus
    global cacheVomsFQAN
    global cacheActime
    global cacheLastUpdate
    global cacheVomsInfo
    timeNow = datetime.datetime.utcnow()
    if cacheLastUpdate is None or (timeNow - cacheLastUpdate) > datetime.timedelta(minutes=60):
        cacheLastUpdate = timeNow
        cacheProxyStatus = None
        cacheVomsStatus = None
        cacheActimeStatus = None
        cacheVomsFQAN = ""
        cacheActime = ""
        cacheVomsInfo = None


# get proxy info
def get_proxy_info(force, verbose):
    global cacheVomsInfo
    if force or cacheVomsInfo is None:
        # get logger
        tmpLog = PLogger.getPandaLogger()
        if Client.use_x509_no_grid():
            if "PANDA_NICKNAME" not in os.environ:
                status = 1
                nickname = ""
                tmpLog.error("PANDA_NICKNAME is not defined")
            else:
                status = 0
                nickname = os.environ["PANDA_NICKNAME"]
            cacheVomsInfo = (status, (nickname,))
        elif not Client.use_oidc():
            # check grid-proxy
            grid_setup_command = Client.build_grid_setup_command()
            com = f"{grid_setup_command} voms-proxy-info --all --e"
            if verbose:
                tmpLog.debug(com)
            status, out = commands_get_status_output_with_env(com)
            if verbose:
                tmpLog.debug(status % 255)
                tmpLog.debug(out)
            cacheVomsInfo = status, out
        else:
            # OIDC
            uid, groups, nickname = Client.get_user_name_from_token()
            if uid is None:
                status = 1
            else:
                status = 0
            if "PANDA_NICKNAME_REPLACE" in os.environ:
                # remove separator from <sep>src<sep>dst<sep>
                src, dst = os.environ["PANDA_NICKNAME_REPLACE"][1:-1].split(os.environ["PANDA_NICKNAME_REPLACE"][0])
                nickname = re.sub(src, dst, nickname)
            cacheVomsInfo = (status, (uid, groups, nickname))
    return cacheVomsInfo


# new proxy check
def check_proxy(verbose, voms_role, refresh_info=False, generate_new=True):
    status, out = get_proxy_info(refresh_info, verbose)
    if status == 0:
        if voms_role is None:
            return True
        # check role
        for tmpItem in out.split("\n"):
            if not tmpItem.startswith("attribute"):
                continue
            role = voms_role.split(":")[-1]
            if role in tmpItem:
                return True
    if not generate_new or Client.use_oidc() or Client.use_x509_no_grid():
        return False
    # generate proxy
    import getpass

    tmpLog = PLogger.getPandaLogger()
    tmpLog.info("Need to generate a grid proxy")
    gridPassPhrase = getpass.getpass("Enter GRID pass phrase for this identity:\n").replace("$", r"\$").replace('"', r"\"")
    grid_setup_command = Client.build_grid_setup_command()
    com = f'{grid_setup_command} echo "{gridPassPhrase}" | voms-proxy-init -pwstdin '
    com_msg = f'{grid_setup_command} echo "*****" | voms-proxy-init -pwstdin '
    if voms_role is None:
        com += "-voms atlas"
        com_msg += "-voms atlas"
    else:
        com += f"-voms {voms_role}"
        com_msg += f"-voms {voms_role}"
    if verbose:
        tmpLog.debug(com_msg)
    status, output = commands_get_status_output_with_env(com)
    if status != 0:
        tmpLog.error(output)
        tmpLog.error("Could not generate a grid proxy")
        sys.exit(MiscUtils.EC_Config)
    else:
        tmpLog.info("Succeeded")
    return check_proxy(verbose, voms_role, refresh_info=True, generate_new=False)


# get nickname
def getNickname(verbose=False):
    nickName = ""
    status, output = get_proxy_info(False, verbose)
    # OIDC
    if Client.use_oidc():
        return output[2]
    # x509 without grid
    if Client.use_x509_no_grid():
        return output[0]
    # X509
    for line in output.split("\n"):
        if line.startswith("attribute"):
            match = re.search(r"nickname =\s*([^\s]+)\s*\(.*\)", line)
            if match is not None:
                nickName = match.group(1)
                break
    # check
    if nickName == "":
        # get logger
        tmpLog = PLogger.getPandaLogger()
        wMessage = "Could not get nickname by using voms-proxy-info which gave\n\n"
        wMessage += output
        wMessage += "\nPlease contact VO Admins using project-lcg-vo-atlas-admin@cern.ch mailing list to register nickname in IAM"
        print("")
        tmpLog.warning(wMessage)
        print("")
    return nickName


# set Rucio accounting
def setRucioAccount(account, appid, forceSet):
    if forceSet or "RUCIO_ACCOUNT" not in os.environ:
        os.environ["RUCIO_ACCOUNT"] = account
    if forceSet or "RUCIO_APPID" not in os.environ:
        os.environ["RUCIO_APPID"] = appid


# invalid character check
def check_invalid_char(s, is_file=False):
    ng_list = [
        "%",
        "|",
        ";",
        ">",
        "<",
        "?",
        "'",
        '"',
        "(",
        ")",
        "$",
        "@",
        ":",
        "=",
        "&",
        "^",
        "#",
        "\\",
        "@",
        "[",
        "]",
        "{",
        "}",
        "`",
    ]

    # wildcard is allowed in filename
    if not is_file:
        ng_list += ["*"]

    for tmp_char in ng_list:
        if tmp_char in s:
            return tmp_char
    return None


# check name of output dataset
def checkOutDsName(outDS, official, nickName="", mergeOutput=False, verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check NG chars for SE
    checked = check_invalid_char(outDS)
    if checked is not None:
        error_message = f'invalid character "{checked}" is used in --outDS'
        tmpLog.error(error_message)
        return False
    # official dataset
    if official:
        if "PANDA_PRODUCTION_ROLE" in os.environ:
            role_name = os.environ["PANDA_PRODUCTION_ROLE"]
        else:
            role_name = "production"
        status, output = get_proxy_info(False, verbose)
        # extract production role
        prodGroups = []
        if Client.use_oidc():
            if verbose:
                print("== check roles ==")
            for tmpLine in output[1]:
                if verbose:
                    print(tmpLine)
                tmpItems = tmpLine.split("/")
                if len(tmpItems) != 2:
                    continue
                tmpVO, tmpRole = tmpLine.split("/")
                if tmpRole == role_name:
                    prodGroups.append(tmpVO)
        else:
            for tmpLine in output.split("\n"):
                match = re.search("/([^/]+)/Role=production", tmpLine)
                if match is not None:
                    # ignore atlas production role
                    if match.group(1) not in ["atlas"]:
                        prodGroups.append(match.group(1))
        # no production role
        if not prodGroups:
            error_message = f"The --official option requires {role_name} role. "
            if not Client.use_oidc():
                error_message += (
                    "Please use the --voms option to set production role;\n"
                    "  e.g.,  --voms atlas:/atlas/phys-higgs/Role=production\n"
                    "If you don't have production role for the group please request it in ATLAS VO first"
                )
            else:
                error_message += f"Please request to add you in the {role_name} group in IAM"
            tmpLog.error(error_message)
            return False
        # loop over all prefixes
        allowedPrefix = ["group"]
        for tmpPrefix in allowedPrefix:
            for tmpGroup in prodGroups:
                tmpPattO = "^" + tmpPrefix + r"\d{2}" + r"\." + tmpGroup + r"\."
                tmpPattN = "^" + tmpPrefix + r"\." + tmpGroup + r"\."
                if re.search(tmpPattO, outDS) is not None or re.search(tmpPattN, outDS) is not None:
                    return True
        # didn't match
        error_message = "You are allowed to produce official datasets with the following prefix\n"
        for tmpPrefix in allowedPrefix:
            for tmpGroup in prodGroups:
                tmpPattN = f"{tmpPrefix}.{tmpGroup}"
                error_message += f"          {tmpPattN}\n"
        if not Client.use_oidc():
            error_message += (
                "If you have production role for another group please use the --voms option to set the role\n"
                "  e.g.,  --voms atlas:/atlas/phys-higgs/Role=production\n"
            )
        tmpLog.error(error_message)
        return False
    # check output dataset format
    matStrN = r"^user\." + nickName + r"\."
    if nickName == "" or re.match(matStrN, outDS) is None:
        if nickName == "":
            error_message = "Could not get nickname from voms proxy\n"
        else:
            outDsPrefixN = f"user.{nickName}"
            error_message = f"outDS must be '{outDsPrefixN}.<user-controlled string...>'\n        e.g., {outDsPrefixN}.test1234"
        tmpLog.error(error_message)
        return False
    # check length. 200=255-55. 55 is reserved for Panda-internal (_subXYZ etc)
    maxLength = 200
    if mergeOutput:
        maxLengthCont = 120
    else:
        maxLengthCont = 132
    if outDS.endswith("/"):
        # container
        if len(outDS) > maxLengthCont:
            tmpErrStr = f"The name of the output dataset container is too long ({len(outDS)}). The length must be less than {maxLengthCont} "
            if mergeOutput:
                tmpErrStr += "when --mergeOutput is used. "
            else:
                tmpErrStr += ". "
            tmpErrStr += "Please note that the limit on the name length is tighter for containers than datasets"
            tmpLog.error(tmpErrStr)
            return False
    else:
        # dataset
        if len(outDS) > maxLength:
            tmpLog.error(f"output datasetname is too long ({len(outDS)}). The length must be less than {maxLength}")
            return False
    return True


# convert sys.argv to string
def convSysArgv(argv=None):
    if argv is None:
        argv = sys.argv
    # job params
    if "PANDA_EXEC_STRING" in os.environ:
        paramStr = os.environ["PANDA_EXEC_STRING"]
    else:
        paramStr = argv[0].split("/")[-1]
    for item in argv[1:]:
        # remove option
        match = re.search("(^-[^=]+=)(.+)", item)
        noSpace = False
        if match is not None:
            paramStr += f" {match.group(1)}"
            item = match.group(2)
            noSpace = True
        if not noSpace:
            paramStr += " "
        match = re.search(r"(\*| |')", item)
        if match is None:
            # normal parameters
            paramStr += f"{item}"
        else:
            # quote string
            paramStr += f'"{item}"'
    # return
    return paramStr


# compare version
def isLatestVersion(latestVer):
    # extract local version numbers
    import PandaToolsPkgInfo

    match = re.search(r"^(\d+)\.(\d+)\.(\d+)$", PandaToolsPkgInfo.release_version)
    if match is None:
        return True
    localMajorVer = int(match.group(1))
    localMinorVer = int(match.group(2))
    localBugfixVer = int(match.group(3))
    # extract local version numbers
    match = re.search(r"^(\d+)\.(\d+)\.(\d+)$", latestVer)
    if match is None:
        return True
    latestMajorVer = int(match.group(1))
    latestMinorVer = int(match.group(2))
    latestBugfixVer = int(match.group(3))
    # compare
    if latestMajorVer > localMajorVer:
        return False
    if latestMajorVer < localMajorVer:
        return True
    if latestMinorVer > localMinorVer:
        return False
    if latestMinorVer < localMinorVer:
        return True
    if latestBugfixVer > localBugfixVer:
        return False
    # latest or higher
    return True


# function for path completion
def completePathFunc(text, status):
    # remove white spaces
    text = text.strip()
    # convert ~
    useTilde = False
    if text.startswith("~"):
        useTilde = True
        # keep original
        origText = text
        # convert
        text = os.path.expanduser(text)
    # put / to directories
    if (not text.endswith("/")) and os.path.isdir(text):
        text += "/"
    # list dirs/files
    lsStat, output = commands_get_status_output(f"ls -d {text}*")
    results = []
    if lsStat == 0:
        for tmpItem in output.split("\n"):
            # ignore current and parent dirs
            if tmpItem in [".", ".."]:
                continue
            # put /
            if os.path.isdir(tmpItem) and not tmpItem.endswith("/"):
                tmpItem += "/"
            # recover ~
            if useTilde:
                tmpItem = re.sub(f"^{os.path.expanduser(origText)}", origText, tmpItem)
            # append
            results.append(tmpItem)
        # sort
        results.sort()
    # return
    return results[status]


# read dataset names from text
def readDsFromFile(txtName):
    dsList = ""
    try:
        # read lines
        txt = open(txtName)
        for tmpLine in txt:
            # remove \n and white spaces
            tmpLine = tmpLine.replace("\n", "")
            tmpLine = tmpLine.strip()
            # skip comment or empty
            if tmpLine.startswith("#") or tmpLine == "":
                continue
            # append
            dsList += f"{tmpLine},"
        # close file
        txt.close()
        # remove the last comma
        dsList = dsList[:-1]
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(f"cannot read datasets from {txtName} due to {errType}:{errValue}")
        sys.exit(MiscUtils.EC_Config)
    return dsList


# convert param string to JEDI params
def convertParamStrToJediParam(
    encStr,
    inputMap,
    outNamePrefix,
    encode,
    padding,
    usePfnList=False,
    includeIO=True,
    extra_in_list=None,
):
    # list of placeholders for input
    inList = [
        "IN",
        "CAVIN",
        "MININ",
        "LOMBIN",
        "HIMBIN",
        "BHIN",
        "BGIN",
        "BGHIN",
        "BGCIN",
        "BGOIN",
    ]
    if extra_in_list:
        inList += extra_in_list
    # placeholder for seq_number
    seqHolder = "SEQNUMBER"
    # placeholder for output
    outHolder = "SN"
    # placeholders with extension
    digExList = ["SEQNUMBER", "FIRSTEVENT"]
    allExList = digExList + ["DBR"]
    # mapping of client and JEDI placeholders
    holders = {
        "SEQNUMBER": "RNDM",
        "DBR": "DB",
        "SKIPEVENTS": "SKIPEVENTS",
        "FIRSTEVENT": None,
        "MAXEVENTS": None,
        "SEGMENT_NAME": None,
    }
    # replace %XYZ with ${XYZ}
    if includeIO:
        for tmpH in inList:
            # \b so that e.g. %IN doesn't match the beginning of %INPUT
            encStr = re.sub(rf"%{tmpH}\b", f"${{{tmpH}}}", encStr)
    # replace %XYZ with ${newXYZ}
    extensionMap = {}
    for newH in holders:
        oldH = holders[newH]
        # JEDI-only placeholders
        if oldH is None:
            oldH = newH
        oldH = "%" + oldH
        # with extension
        if newH in allExList:
            if newH in digExList:
                oldH += r"(:|=)(\d+)%{0,1}"
            else:
                oldH += "(:|=)([^ '\"\\}]+)"
            # look for extension
            tmpM = re.search(oldH, encStr)
            if tmpM is not None:
                extensionMap[newH] = tmpM.group(2)
            newH = "${" + newH + "}"
        else:
            newH = "${" + newH + "}"
        encStr = re.sub(oldH, newH, encStr)
    # replace %OUT to outDS${SN}
    if includeIO:
        encStr = re.sub("%OUT", outNamePrefix + ".${" + outHolder + "}", encStr)
    # make pattern for split
    patS = "("
    allKeys = list(holders)
    if includeIO:
        allKeys += inList
        allKeys += [outHolder]
    for tmpH in allKeys:
        patS += "[^=,\"' \\(\\{;]*\\$\\{" + tmpH + "[^\\}]*\\}[^,\"' \\)\\};]*|"
    patS = patS[:-1]
    patS += ")"
    # split
    tmpItems = re.split(patS, encStr)
    # make parameters
    jobParams = []
    for tmpItem in tmpItems:
        # check if a placeholder
        matchP = re.search(r"\$\{([^:\}]+)", tmpItem)
        if re.search(patS, tmpItem) is not None and matchP is not None:
            tmpHolder = matchP.group(1)
            # set attributes
            if tmpHolder in inList:
                # use constant since it is templated in another option e.g., -i
                tmpDict = {"type": "constant"}
                if encode:
                    tmpDict["value"] = "${" + tmpHolder + "/E}"
                else:
                    tmpDict["value"] = tmpItem
                # set dataset if PFN list is not used or the stream is not primary
                if not usePfnList or tmpHolder not in ["IN"]:
                    tmpDict["param_type"] = "input"
                    tmpDict["dataset"] = inputMap[tmpHolder]
            elif tmpHolder == seqHolder:
                tmpDict = {"type": "template", "value": tmpItem, "param_type": "pseudo_input", "dataset": "seq_number"}
                if tmpHolder in extensionMap:
                    try:
                        tmpDict["offset"] = int(extensionMap[tmpHolder])
                    except Exception:
                        pass
            elif tmpHolder == outHolder:
                tmpDict = {"type": "template", "value": tmpItem, "param_type": "output", "dataset": outNamePrefix + tmpItem.split("}")[-1] + "/"}
                tmpDict["container"] = tmpDict["dataset"]
            else:
                tmpDict = {"type": "template", "value": tmpItem, "param_type": "number"}
                if tmpHolder in extensionMap:
                    try:
                        tmpDict["offset"] = int(extensionMap[tmpHolder])
                    except Exception:
                        pass
        else:
            # constant
            tmpDict = {"type": "constant"}
            if encode:
                tmpDict["value"] = quote(tmpItem)
            else:
                tmpDict["value"] = tmpItem
        # no padding
        if not padding:
            tmpDict["padding"] = False
        # append
        jobParams.append(tmpDict)
    # return
    return jobParams


# split comma-concatenated list items
def splitCommaConcatenatedItems(oldList):
    if isinstance(oldList, str):
        oldList = [oldList]
    newList = []
    for oldItem in oldList:
        temItems = oldItem.split(",")
        for tmpItem in temItems:
            tmpItem = tmpItem.strip()
            # remove empty
            if tmpItem == "":
                continue
            if tmpItem not in newList:
                newList.append(tmpItem)
    return newList


# upload gzipped file
def uploadGzippedFile(origFileName, currentDir, tmpLog, delFilesOnExit, nosubmit, verbose):
    # open original file
    if origFileName.startswith("/"):
        # absolute path
        tmpIn = open(origFileName, "rb")
    else:
        # relative path
        tmpIn = open(f"{currentDir}/{origFileName}", "rb")
    # use unique name for gzip
    newFileName = f"pre_{MiscUtils.wrappedUuidGen()}.dat"
    gzipFullPath = f"{currentDir}/{newFileName}.gz"
    delFilesOnExit.append(gzipFullPath)
    # make gzip
    tmpOut = gzip.open(gzipFullPath, "wb")
    tmpOut.writelines(tmpIn)
    tmpOut.close()
    tmpIn.close()
    # upload
    if not nosubmit:
        tmpLog.info("uploading data file for preprocessing")
        status, out = Client.putFile(gzipFullPath, verbose, useCacheSrv=True, reuseSandbox=False)
        if status != 0 or out != "True":
            # failed
            print(out)
            tmpLog.error(f"Failed with {status}")
            sys.exit(MiscUtils.EC_Post)
    # delete
    os.remove(gzipFullPath)
    # return new filename
    return newFileName


# get PFN list
def getListPFN(pfnFile):
    rFile = open(pfnFile)
    inputFileList = []
    for line in rFile:
        line = line.replace("\n", "")
        line = line.strip()
        if line != "" and not line.startswith("#"):
            inputFileList.append(line)
    rFile.close()
    inputFileList.sort()
    if len(inputFileList) == 0:
        # get logger
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(f"{pfnFile} doesn't contain any PFNs")
        sys.exit(MiscUtils.EC_Config)
    return inputFileList


# check task parameters
def checkTaskParam(taskParamMap, unlimitNumOutputs):
    # check output dataset names
    maxLengthCont = 132
    maxNumOutputs = 10
    nOutputs = 0
    dict_list = taskParamMap["jobParameters"] + [taskParamMap["log"]] if "log" in taskParamMap else taskParamMap["jobParameters"]
    for tmpDict in dict_list:
        if tmpDict["type"] == "template" and tmpDict["param_type"] in ["output", "log"]:
            if tmpDict["param_type"] == "output":
                nOutputs += 1
            tmpErrStr = None
            # check length of dataset name
            if len(tmpDict["dataset"]) > maxLengthCont:
                tmpErrStr = (
                    f"The name of an output or log dataset container ({tmpDict['dataset']}) is too long ({len(tmpDict['dataset'])}). "
                    f"The length must be less than {maxLengthCont} following DDM definition. "
                    "Please note that one dataset container is created per output/log type and "
                    "each name is <outDS>_<extension made from the output filename>/ or <outDS>.log/. "
                )
            # check non-ascii characters
            if not tmpErrStr:
                try:
                    tmpDict["value"].encode("ascii")
                except Exception:
                    tmpErrStr = f"Output name {tmpDict['value']} contains non-ascii characters that are forbidden since they screw up the storage"
            if not tmpErrStr:
                try:
                    tmpDict["dataset"].encode("ascii")
                except Exception:
                    tmpErrStr = f"Dataset name {tmpDict['dataset']} contains non-ascii characters that are forbidden since they screw up the storage"
            if tmpErrStr:
                tmpLog = PLogger.getPandaLogger()
                tmpLog.error(tmpErrStr)
                return MiscUtils.EC_Config, tmpErrStr
    if not unlimitNumOutputs and nOutputs > maxNumOutputs:
        error_message = (
            f"Too many output files (={nOutputs}) per job. The default limit is {maxNumOutputs}. "
            "You can remove the constraint by using the --unlimitNumOutputs option. "
            "But please note that having too many outputs per job causes a severe load on the system. "
            "You may be banned if you carelessly use the option"
        )
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(error_message)
        return (MiscUtils.EC_Config, error_message)
    return (0, None)


# replace input and output
def replaceInputOutput(taskParamMap, inDS, outDS, seqNum):
    newTaskParamMap = copy.deepcopy(taskParamMap)
    if inDS != "":
        oldInDS = taskParamMap["dsForIN"]
        subInDSbefore = quote("%DATASET_IN")
        subInDSafter = quote(inDS)
        newTaskParamMap["dsForIN"] = inDS
        for tmpDict in newTaskParamMap["jobParameters"]:
            if "dataset" in tmpDict:
                if tmpDict["dataset"] == oldInDS:
                    tmpDict["dataset"] = inDS
            elif tmpDict["type"] == "constant":
                tmpDict["value"] = re.sub(subInDSbefore, subInDSafter, tmpDict["value"])
    outDS = re.sub("/$", "", outDS)
    oldOutDS = taskParamMap["taskName"]
    oldOutDS = re.sub("/$", "", oldOutDS)
    subOutDSbefore = quote("%DATASET_OUT")
    subOutDSafter = quote(outDS)
    subSeqBefore = quote("%BULKSEQNUMBER")
    subSeqAfter = str(seqNum)
    newTaskParamMap["taskName"] = outDS
    newTaskParamMap["log"]["dataset"] = re.sub(oldOutDS, outDS, taskParamMap["log"]["dataset"])
    newTaskParamMap["log"]["container"] = re.sub(oldOutDS, outDS, taskParamMap["log"]["container"])
    newTaskParamMap["log"]["value"] = re.sub(oldOutDS, outDS, taskParamMap["log"]["value"])
    for tmpDict in newTaskParamMap["jobParameters"]:
        if "dataset" in tmpDict:
            if tmpDict["dataset"].startswith(oldOutDS):
                tmpDict["dataset"] = re.sub(oldOutDS, outDS, tmpDict["dataset"])
                tmpDict["container"] = re.sub(oldOutDS, outDS, tmpDict["container"])
                tmpDict["value"] = re.sub(oldOutDS, outDS, tmpDict["value"])
        elif tmpDict["type"] == "constant":
            tmpDict["value"] = re.sub(subOutDSbefore, subOutDSafter, tmpDict["value"])
            tmpDict["value"] = re.sub(subSeqBefore, subSeqAfter, tmpDict["value"])
            tmpDict["value"] = re.sub(oldOutDS, outDS, tmpDict["value"])
    return newTaskParamMap


# get OS information
def get_os_information():
    return platform.platform()


# extract voms proxy user name
def extract_voms_proxy_username():
    username = None
    status, output = get_proxy_info(False, False)
    if Client.use_oidc():
        return output[0]
    if status != 0:
        return None
    for line in output.split("\n"):
        if line.startswith("subject"):
            subj = line.split(":", 1)[-1].lstrip()
            user_dn = re.sub(r"(/CN=\d+)+$", "", subj.replace("/CN=proxy", ""))
            username = user_dn.split("=")[-1]
            username = re.sub(r"[ |_]\d+", "", username)
            username = re.sub("[()']", "", username)
            break
    name_wo_email = re.sub(r" [a-z][\w\.-]+@[\w\.-]+(?:\.\w+)+", "", username).strip()
    if " " in name_wo_email:
        username = name_wo_email
    return username


# warning message when PQ is specified
def get_warning_for_pq(site, excluded_site, tmp_log):
    if site not in ["AUTO", None] or excluded_site:
        tmp_log.warning(
            "The grid queue names could change due to consolidation, migration, etc. "
            "Please check with the command listAnalyPQ to use only online/valid queues "
            "when site and/or excludedSite options are specified."
        )
    return ""


# warning message for memory
def get_warning_for_memory(memory, is_confirmed, n_core, tmp_log):
    if memory > 4000:
        tmp_str = (
            f"You are requesting {memory} MB/core which severely restricts the available resources to run on. "
            "Your task will take longer or may not run at all. Check if you really need this. "
            "Maybe improve the code"
        )
        if n_core <= 1:
            tmp_str += " or if the payload supports MT/MP then use --nCore=8 to reduce the memory per core"
        tmp_str += "."
        tmp_log.warning(tmp_str)
        if not is_confirmed:
            return MiscUtils.query_yes_no("\nAre you sure with the memory requirement? ")
    return True


def check_sandbox_symlinks(archive_name, tmp_log):
    """Warn about symlinks in the sandbox which point to absolute paths, since they are not resolved on WNs

    Exits with EC_Archive when the sandbox cannot be listed.

    args:
        archive_name: gzipped sandbox tarball
        tmp_log: logger
    """
    tmp_log.info("checking sandbox")
    # retry a few times in case listing fails
    for _ in range(5):
        status, out = MiscUtils.commands_get_status_output(f"tar tvfz {archive_name}")
        if status == 0:
            break
        time.sleep(5)
    if status != 0:
        tmp_log.error(f"Failed to expand sandbox. {out}")
        sys.exit(MiscUtils.EC_Archive)
    symlinks = []
    for line in out.split("\n"):
        items = line.split()
        if len(items) > 0 and items[0].startswith("l") and items[-1].startswith("/"):
            symlinks.append(line)
    if symlinks:
        tmp_log.warning("Found some unresolved symlinks which may cause a problem\n   Please ignore if you believe they are harmless")
        for symlink in symlinks:
            print(f"  {symlink}")


def use_existing_sandbox(options, tmp_dir):
    """Use a sandbox given with --tarBallViaDDM or --inTarBall instead of archiving the work area

    Changes the working directory to tmp_dir.

    args:
        options: parsed options namespace
        tmp_dir: temporary directory where the sandbox is copied
    returns:
        name of the sandbox, and its full path, which is None for a sandbox in DDM
    """
    # go to tmp dir
    os.chdir(tmp_dir)
    # use sandbox pre-uploaded to DDM. --tarBallViaDDM is datasetName:fileName
    if options.tarBallViaDDM:
        return options.tarBallViaDDM.split(":")[-1], None
    # use a saved copy. Without a build step it contains only job options
    if not (options.noBuild and not options.noCompile):
        archive_name = f"sources.{MiscUtils.wrappedUuidGen()}.tar"
    else:
        archive_name = f"jobO.{MiscUtils.wrappedUuidGen()}.tar"
    archive_full_name = f"{tmp_dir}/{archive_name}"
    # make copy to avoid name duplication
    shutil.copy(options.inTarBall, archive_full_name)
    return archive_name, archive_full_name


def upload_sandbox(options, archive_name, cur_dir, tmp_log, del_files_on_exit, use_cache_srv=True):
    """Save the sandbox for --outTarBall, and upload it and the good run list to the PanDA cache server

    Nothing is uploaded with --noSubmit or --tarBallViaDDM. Exits with EC_Post when the upload fails.

    args:
        options: parsed options namespace, goodRunListXML is replaced with the name of the uploaded file
        archive_name: sandbox tarball in the working directory
        cur_dir: directory where the command was run, to resolve the good run list path
        tmp_log: logger
        del_files_on_exit: list of files deleted on exit, extended with the gzipped good run list
        use_cache_srv: upload to the cache server instead of the main server
    returns:
        name of the sandbox on the server, which differs when an identical sandbox is reused
    """
    # save
    if options.outTarBall != "":
        shutil.copy(archive_name, options.outTarBall)

    if options.noSubmit or options.tarBallViaDDM:
        return archive_name

    # upload sources via HTTP POST
    tmp_log.info("upload sandbox")
    status, out = Client.putFile(archive_name, options.verbose, useCacheSrv=use_cache_srv, reuseSandbox=True)
    if out.startswith("NewFileName:"):
        # found the same input sandbox to reuse
        archive_name = out.split(":")[-1]
    elif out != "True":
        print(out)
        tmp_log.error(f"failed to upload sandbox with {status}")
        sys.exit(MiscUtils.EC_Post)
    # good run list
    if options.goodRunListXML != "":
        options.goodRunListXML = uploadGzippedFile(
            options.goodRunListXML,
            cur_dir,
            tmp_log,
            del_files_on_exit,
            options.noSubmit,
            options.verbose,
        )
    return archive_name


def print_task_params(task_param_map, tmp_log):
    """Print the task parameters sorted by key

    args:
        task_param_map: task parameter dict
        tmp_log: logger
    """
    tmp_log.debug("==== taskParams ====")
    for tmp_key in sorted(task_param_map):
        print(f"{tmp_key} : {task_param_map[tmp_key]}")


def submit_task(task_param_map, options, tmp_log):
    """Submit the task parameters to the PanDA server

    args:
        task_param_map: task parameter dict
        options: parsed options namespace
        tmp_log: logger
    returns:
        exit code (0 or EC_Submit), message from the server or the error, and jediTaskID or None
    """
    tmp_log.info(f"submit {options.outDS}")
    status, tmp_out = Client.insertTaskParams(
        task_param_map,
        options.verbose,
        properErrorCode=True,
        parent_tid=options.parentTaskID,
    )
    # communication error
    if status != 0:
        tmp_str = f"task submission failed with {status}"
        tmp_log.error(tmp_str)
        return MiscUtils.EC_Submit, tmp_str, None
    # return codes 0 and 3 from the server mean that the task was accepted
    if tmp_out[0] not in [0, 3]:
        tmp_str = f"task submission failed. {tmp_out[1]}"
        tmp_log.error(tmp_str)
        return MiscUtils.EC_Submit, tmp_str, None
    tmp_str = tmp_out[1]
    tmp_log.info(tmp_str)
    task_id = None
    match = re.search(r"jediTaskID=(\d+)", tmp_str)
    if match is not None:
        task_id = int(match.group(1))
    return 0, tmp_str, task_id


def make_dump_item(options, exit_code, tmp_str, task_id, i_submission, n_submissions):
    """Make the record of one submission for --dumpJson

    args:
        options: parsed options namespace
        exit_code: exit code of the submission
        tmp_str: message from the server or the error
        task_id: jediTaskID or None
        i_submission: index of the submission
        n_submissions: number of submissions, more than one with --inOutDsJson
    returns:
        dict with the options and the result of the submission
    """
    dump_item = copy.deepcopy(vars(options))
    dump_item["returnCode"] = exit_code
    dump_item["returnOut"] = tmp_str
    dump_item["jediTaskID"] = task_id
    if n_submissions > 1:
        dump_item["bulkSeqNumber"] = i_submission
    return dump_item
