import os
import re
import sys

from . import Client, MiscUtils, PLogger
from .MiscUtils import (
    commands_fail_on_non_zero_exit_status,
    commands_get_output,
    commands_get_output_with_env,
)

# error code
EC_Config = 10
EC_Archive = 60


# get CMT projects
def getCmtProjects(dir="."):
    # cmt or cmake
    if not useCMake():
        # keep current dir
        curdir = os.getcwd()
        # change dir
        os.chdir(dir)
        # get projects
        out = commands_get_output_with_env("cmt show projects")
        lines = out.split("\n")
        # remove CMT warnings
        tupLines = tuple(lines)
        lines = []
        for line in tupLines:
            if "CMTUSERCONTEXT" in line:
                continue
            if not line.startswith("#"):
                lines.append(line)
        # back to the current dir
        os.chdir(curdir)
        # return
        return lines, out
    else:
        lines = []
        # use current dir as test area
        tmpStr = "(in {0})".format(os.getcwd())
        lines.append(tmpStr)
        # AtlasProject
        if not "AtlasProject" in os.environ:
            return [], "AtlasProject is not defined in runtime environment"
        tmpStr = "{0} (in {0}/{1})".format(os.environ["AtlasProject"], os.environ["AtlasVersion"])
        lines.append(tmpStr)
        if "AtlasOffline_VERSION" in os.environ:
            tmpStr = "{0} (in {0}/{1})".format("AtlasOffline", os.environ["AtlasOffline_VERSION"])
            lines.append(tmpStr)
        prodVerStr = "{0}_VERSION".format(os.environ["AtlasProject"])
        if prodVerStr in os.environ:
            tmpStr = "{0} (in {0}/{1})".format(os.environ["AtlasProject"], os.environ[prodVerStr])
            lines.append(tmpStr)
        return lines, ""


# check if ath release
def isAthRelease(cacheVer):
    try:
        if "AthAnalysis" in cacheVer or re.search("Ath[a-zA-Z]+Base", cacheVer) is not None:
            return True
    except Exception:
        pass
    return False


# get Athena version
def getAthenaVer(verbose=True):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get project parameters
    lines, out = getCmtProjects()
    if len(lines) < 2:
        # make a tmp dir to execute cmt
        tmpDir = "cmttmp.%s" % MiscUtils.wrappedUuidGen()
        os.mkdir(tmpDir)
        # try cmt under a subdir since it doesn't work in top dir
        lines, tmpOut = getCmtProjects(tmpDir)
        # delete the tmp dir
        commands_get_output("rm -rf %s" % tmpDir)
        if len(lines) < 2:
            if verbose:
                print(out)
                tmpLog.error("cmt gave wrong info")
            return False, {}
    # private work area
    res = re.search("\(in ([^\)]+)\)", lines[0])
    if res == None:
        print(lines[0])
        tmpLog.error("no TestArea. could not get path to private work area")
        return False, {}
    workArea = os.path.realpath(res.group(1))
    # get Athena version and group area
    athenaVer = ""
    groupArea = ""
    cacheVer = ""
    nightVer = ""
    cmtConfig = ""
    for line in lines[1:]:
        res = re.search("\(in ([^\)]+)\)", line)
        if res is not None:
            items = line.split()
            if (
                items[0].startswith("Athena")
                or items[0].startswith("Analysis")
                or items[0] in ["AthDerivations", "AnalysisBase", "AthSimulation", "AthDerivation", "AthAnalysis", "AthGeneration", "AtlasStats"]
            ):
                isGitBase = True
            else:
                isGitBase = False
            # base release
            if items[0] in ("dist", "AtlasRelease", "AtlasOffline", "AtlasAnalysis", "AtlasTrigger", "AtlasReconstruction") or isGitBase:
                # Atlas release
                if "AtlasBuildStamp" in os.environ and ("AtlasReleaseType" not in os.environ or os.environ["AtlasReleaseType"] != "stable"):
                    athenaVer = os.environ["AtlasBuildStamp"]
                    useBuildStamp = True
                else:
                    athenaVer = os.path.basename(res.group(1))
                    useBuildStamp = False
                # nightly
                if athenaVer.startswith("rel") or useBuildStamp or isGitBase:
                    # extract base release
                    if not useCMake():
                        tmpMatch = re.search("/([^/]+)(/rel_\d+)*/Atlas[^/]+/rel_\d+", line)
                        if tmpMatch is None:
                            tmpLog.error("unsupported nightly %s" % line)
                            return False, {}
                        # set athenaVer and cacheVer
                        cacheVer = "-AtlasOffline_%s" % athenaVer
                        athenaVer = tmpMatch.group(1)
                    else:
                        if athenaVer.startswith("rel"):
                            tmpLog.error("Nightlies with AFS setup are unsupported on the grid. Setup with CVMFS")
                            return False, {}
                        if isGitBase:
                            cacheVer = "-{0}_{1}".format(items[0], athenaVer)
                        else:
                            cacheVer = "-AtlasOffline_%s" % athenaVer
                        if useBuildStamp:
                            athenaVer = os.environ["AtlasBuildBranch"]
                break
            # cache or analysis projects
            elif (
                items[0] in ["AtlasProduction", "AtlasPoint1", "AtlasTier0", "AtlasP1HLT", "AtlasDerivation", "TrigMC"]
                or isAthRelease(items[0])
                or items[1].count(".") >= 4
            ):
                # cache is used
                if cacheVer != "":
                    continue
                # production cache
                cacheTag = os.path.basename(res.group(1))
                if items[0] == "AtlasProduction" and cacheTag.startswith("rel"):
                    # nightlies for cache
                    tmpMatch = re.search("/([^/]+)(/rel_\d+)*/Atlas[^/]+/rel_\d+", line)
                    if tmpMatch is None:
                        tmpLog.error("unsupported nightly %s" % line)
                        return False, {}
                    cacheVer = "-AtlasOffline_%s" % cacheTag
                    athenaVer = tmpMatch.group(1)
                    break
                elif items[0] == "TrigMC" and cacheTag.startswith("rel"):
                    # nightlies for cache
                    tmpMatch = re.search("/([^/]+)(/rel_\d+)*/[^/]+/rel_\d+", line)
                    if tmpMatch is None:
                        tmpLog.error("unsupported nightly %s" % line)
                        return False, {}
                    cacheVer = "-%s_%s" % (items[0], cacheTag)
                    athenaVer = tmpMatch.group(1)
                    break
                elif isAthRelease(items[0]):
                    cacheVer = "-%s_%s" % (items[0], cacheTag)
                else:
                    # doesn't use when it is a base release since it is not installed in EGEE
                    if re.search("^\d+\.\d+\.\d+$", cacheTag) is None:
                        cacheVer = "-%s_%s" % (items[0], cacheTag)
                # no more check for AthAnalysis
                if isAthRelease(items[0]):
                    break
            else:
                # group area
                groupArea = os.path.realpath(res.group(1))
    # cmtconfig
    if "CMTCONFIG" in os.environ:
        cmtConfig = os.environ["CMTCONFIG"]
    # last resort
    if athenaVer == "":
        if "AtlasProject" in os.environ and "AtlasBuildBranch" in os.environ:
            prodVerStr = "{0}_VERSION".format(os.environ["AtlasProject"])
            if prodVerStr in os.environ:
                athenaVer = os.environ["AtlasBuildBranch"]
                cacheVer = "-{0}_{1}".format(os.environ["AtlasProject"], os.environ[prodVerStr])
                groupArea = ""
    # pack return values
    retVal = {
        "workArea": workArea,
        "athenaVer": athenaVer,
        "groupArea": groupArea,
        "cacheVer": cacheVer,
        "nightVer": nightVer,
        "cmtConfig": cmtConfig,
    }
    # check error
    if athenaVer == "" and not isAthRelease(cacheVer):
        tmpStr = ""
        for line in lines:
            tmpStr += line + "\n"
        tmpLog.info("cmt showed\n" + tmpStr)
        tmpLog.error("could not get Athena version. perhaps your requirements file doesn't have ATLAS_TEST_AREA")
        return False, retVal
    # return
    return True, retVal


# wrapper for attribute access
class ConfigAttr(dict):
    # override __getattribute__ for dot access
    def __getattribute__(self, name):
        if name in dict.__dict__.keys():
            return dict.__getattribute__(self, name)
        if name.startswith("__"):
            return dict.__getattribute__(self, name)
        if name in dict.keys(self):
            return dict.__getitem__(self, name)
        return False

    def __setattr__(self, name, value):
        if name in dict.__dict__.keys():
            dict.__setattr__(self, name, value)
        else:
            dict.__setitem__(self, name, value)


# extract run configuration
def extractRunConfig(jobO, supStream, shipinput, trf, verbose=False, useAMI=False, inDS="", tmpDir=".", one_liner=""):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    outputConfig = ConfigAttr()
    inputConfig = ConfigAttr()
    otherConfig = ConfigAttr()
    statsCode = True
    if trf:
        pass
    else:
        # use AMI
        amiJobO = ""
        if useAMI:
            amiJobO = getJobOtoUseAmiForAutoConf(inDS, tmpDir)
        baseName = os.environ["PANDA_SYS"] + "/etc/panda/share"
        if " - " in jobO:
            jobO = re.sub(" - ", " %s/ConfigExtractor.py - " % baseName, jobO)
        else:
            jobO = jobO + " %s/ConfigExtractor.py " % baseName
        com = "athena.py "
        if one_liner:
            com += '-c "%s" ' % one_liner
        com += "%s %s/FakeAppMgr.py %s" % (amiJobO, baseName, jobO)
        if verbose:
            tmpLog.debug(com)
        # run ConfigExtractor for normal jobO
        out = commands_get_output_with_env(com)
        failExtractor = True
        outputConfig["alloutputs"] = []
        skipOutName = False
        for line in out.split("\n"):
            match = re.findall("^ConfigExtractor > (.+)", line)
            if len(match):
                # suppress some streams
                if match[0].startswith("Output="):
                    tmpSt0 = "NoneNoneNone"
                    tmpSt1 = "NoneNoneNone"
                    tmpSt2 = "NoneNoneNone"
                    try:
                        tmpSt0 = match[0].replace("=", " ").split()[1].upper()
                    except Exception:
                        pass
                    try:
                        tmpSt1 = match[0].replace("=", " ").split()[-1].upper()
                    except Exception:
                        pass
                    try:
                        tmpSt2 = match[0].replace("=", " ").split()[2].upper()
                    except Exception:
                        pass
                    toBeSuppressed = False
                    # normal check
                    if tmpSt0 in supStream or tmpSt1 in supStream or tmpSt2 in supStream:
                        toBeSuppressed = True
                    # wild card check
                    if not toBeSuppressed:
                        for tmpPatt in supStream:
                            if "*" in tmpPatt:
                                tmpPatt = "^" + tmpPatt.replace("*", ".*")
                                tmpPatt = tmpPatt.upper()
                                if re.search(tmpPatt, tmpSt0) is not None or re.search(tmpPatt, tmpSt1) is not None or re.search(tmpPatt, tmpSt2) is not None:
                                    toBeSuppressed = True
                                    break
                    # suppressed
                    if toBeSuppressed:
                        tmpLog.info("%s is suppressed" % line)
                        # set skipOutName to ignore output filename in the next loop
                        skipOutName = True
                        continue
                failExtractor = False
                # AIDA HIST
                if match[0].startswith("Output=HIST"):
                    outputConfig["outHist"] = True
                # AIDA NTuple
                if match[0].startswith("Output=NTUPLE"):
                    if "outNtuple" not in outputConfig:
                        outputConfig["outNtuple"] = []
                    tmpItems = match[0].split()
                    outputConfig["outNtuple"].append(tmpItems[1])
                # RDO
                if match[0].startswith("Output=RDO"):
                    outputConfig["outRDO"] = match[0].split()[1]
                # ESD
                if match[0].startswith("Output=ESD"):
                    outputConfig["outESD"] = match[0].split()[1]
                # AOD
                if match[0].startswith("Output=AOD"):
                    outputConfig["outAOD"] = match[0].split()[1]
                # TAG output
                if match[0] == "Output=TAG":
                    outputConfig["outTAG"] = True
                # TAGCOM
                if match[0].startswith("Output=TAGX"):
                    if "outTAGX" not in outputConfig:
                        outputConfig["outTAGX"] = []
                    tmpItems = match[0].split()
                    outputConfig["outTAGX"].append(tuple(tmpItems[1:]))
                # AANT
                if match[0].startswith("Output=AANT"):
                    if "outAANT" not in outputConfig:
                        outputConfig["outAANT"] = []
                    tmpItems = match[0].split()
                    outputConfig["outAANT"].append(tuple(tmpItems[1:]))
                # THIST
                if match[0].startswith("Output=THIST"):
                    if "outTHIST" not in outputConfig:
                        outputConfig["outTHIST"] = []
                    tmpItems = match[0].split()
                    if not tmpItems[1] in outputConfig["outTHIST"]:
                        outputConfig["outTHIST"].append(tmpItems[1])
                # IROOT
                if match[0].startswith("Output=IROOT"):
                    if "outIROOT" not in outputConfig:
                        outputConfig["outIROOT"] = []
                    tmpItems = match[0].split()
                    outputConfig["outIROOT"].append(tmpItems[1])
                # Stream1
                if match[0].startswith("Output=STREAM1"):
                    outputConfig["outStream1"] = match[0].split()[1]
                # Stream2
                if match[0].startswith("Output=STREAM2"):
                    outputConfig["outStream2"] = match[0].split()[1]
                # ByteStream output
                if match[0] == "Output=BS":
                    outputConfig["outBS"] = True
                # General Stream
                if match[0].startswith("Output=STREAMG"):
                    tmpItems = match[0].split()
                    outputConfig["outStreamG"] = []
                    for tmpNames in tmpItems[1].split(","):
                        outputConfig["outStreamG"].append(tmpNames.split(":"))
                # Metadata
                if match[0].startswith("Output=META"):
                    if "outMeta" not in outputConfig:
                        outputConfig["outMeta"] = []
                    tmpItems = match[0].split()
                    outputConfig["outMeta"].append(tuple(tmpItems[1:]))
                # UserDataSvc
                if match[0].startswith("Output=USERDATA"):
                    if "outUserData" not in outputConfig:
                        outputConfig["outUserData"] = []
                    tmpItems = match[0].split()
                    outputConfig["outUserData"].append(tmpItems[-1])
                # MultipleStream
                if match[0].startswith("Output=MS"):
                    if "outMS" not in outputConfig:
                        outputConfig["outMS"] = []
                    tmpItems = match[0].split()
                    outputConfig["outMS"].append(tuple(tmpItems[1:]))
                # No input
                if match[0] == "No Input":
                    inputConfig["noInput"] = True
                # ByteStream input
                if match[0] == "Input=BS":
                    inputConfig["inBS"] = True
                # selected ByteStream
                if match[0].startswith("Output=SelBS"):
                    tmpItems = match[0].split()
                    inputConfig["outSelBS"] = tmpItems[1]
                # TAG input
                if match[0] == "Input=COLL":
                    inputConfig["inColl"] = True
                # POOL references
                if match[0].startswith("Input=COLLREF"):
                    tmpRef = match[0].split()[-1]
                    if tmpRef == "Input=COLLREF":
                        # use default token when ref is empty
                        tmpRef = "Token"
                    elif tmpRef != "Token" and (not tmpRef.endswith("_ref")):
                        # append _ref
                        tmpRef += "_ref"
                    inputConfig["collRefName"] = tmpRef
                # TAG Query
                if match[0].startswith("Input=COLLQUERY"):
                    tmpQuery = re.sub("Input=COLLQUERY", "", match[0])
                    tmpQuery = tmpQuery.strip()
                    inputConfig["tagQuery"] = tmpQuery
                # Minimum bias
                if match[0] == "Input=MINBIAS":
                    inputConfig["inMinBias"] = True
                # Cavern input
                if match[0] == "Input=CAVERN":
                    inputConfig["inCavern"] = True
                # Beam halo
                if match[0] == "Input=BEAMHALO":
                    inputConfig["inBeamHalo"] = True
                # Beam gas
                if match[0] == "Input=BEAMGAS":
                    inputConfig["inBeamGas"] = True
                # Back navigation
                if match[0] == "BackNavigation=ON":
                    inputConfig["backNavi"] = True
                # Random stream
                if match[0].startswith("RndmStream"):
                    if "rndmStream" not in otherConfig:
                        otherConfig["rndmStream"] = []
                    tmpItems = match[0].split()
                    otherConfig["rndmStream"].append(tmpItems[1])
                # Generator file
                if match[0].startswith("RndmGenFile"):
                    if "rndmGenFile" not in otherConfig:
                        otherConfig["rndmGenFile"] = []
                    tmpItems = match[0].split()
                    otherConfig["rndmGenFile"].append(tmpItems[-1])
                # G4 Random seeds
                if match[0].startswith("G4RandomSeeds"):
                    otherConfig["G4RandomSeeds"] = True
                # input files for direct input
                if match[0].startswith("InputFiles"):
                    if shipinput:
                        tmpItems = match[0].split()
                        otherConfig["inputFiles"] = tmpItems[1:]
                    else:
                        continue
                # condition file
                if match[0].startswith("CondInput"):
                    if "condInput" not in otherConfig:
                        otherConfig["condInput"] = []
                    tmpItems = match[0].split()
                    otherConfig["condInput"].append(tmpItems[-1])
                # collect all outputs
                if match[0].startswith(" Name:"):
                    # skipped output
                    if skipOutName:
                        skipOutName = False
                        continue
                    outputConfig["alloutputs"].append(match[0].split()[-1])
                    continue
                tmpLog.info(line)
                skipOutName = False
        # extractor failed
        if failExtractor:
            print(out)
            tmpLog.error("Could not parse jobOptions")
            statsCode = False
    # return
    retConfig = ConfigAttr()
    retConfig["input"] = inputConfig
    retConfig["other"] = otherConfig
    retConfig["output"] = outputConfig
    return statsCode, retConfig


# extPoolRefs for old releases which don't contain CollectionTools
athenaStuff = []

# jobO files with full path names
fullPathJobOs = {}


# convert fullPathJobOs to str
def convFullPathJobOsToStr():
    tmpStr = ""
    for fullJobO in fullPathJobOs:
        localName = fullPathJobOs[fullJobO]
        tmpStr += "%s:%s," % (fullJobO, localName)
    tmpStr = tmpStr[:-1]
    return tmpStr


# convert str to fullPathJobOs
def convStrToFullPathJobOs(tmpStr):
    retMap = {}
    for tmpItem in tmpStr.split(","):
        fullJobO, localName = tmpItem.split(":")
        retMap[fullJobO] = localName
    return retMap


# copy some athena specific files and full-path jobOs
def copyAthenaStuff(currentDir):
    baseName = os.environ["PANDA_SYS"] + "/etc/panda/share"
    for tmpFile in athenaStuff:
        com = "cp -p %s/%s %s" % (baseName, tmpFile, currentDir)
        commands_get_output(com)
    for fullJobO in fullPathJobOs:
        localName = fullPathJobOs[fullJobO]
        com = "cp -p %s %s/%s" % (fullJobO, currentDir, localName)
        commands_get_output(com)


# delete some athena specific files and copied jobOs
def deleteAthenaStuff(currentDir):
    for tmpFile in athenaStuff:
        com = "rm -f %s/%s" % (currentDir, tmpFile)
        commands_get_output(com)
    for tmpFile in fullPathJobOs.values():
        com = "rm -f %s/%s" % (currentDir, tmpFile)
        commands_get_output(com)


# set extFile
extFile = []


def setExtFile(v_extFile):
    global extFile
    extFile = v_extFile


# set excludeFile
excludeFile = []


def setExcludeFile(strExcludeFile):
    # empty
    if strExcludeFile == "":
        strExcludeFile = "jobReport.json,jobReport.txt,jobReportExtract.pickle"
    else:
        strExcludeFile += ",jobReport.json,jobReport.txt,jobReportExtract.pickle"
    # convert to list
    global excludeFile
    for tmpItem in strExcludeFile.split(","):
        tmpItem = tmpItem.strip()
        if tmpItem == "":
            continue
        # change . to \. for regexp
        tmpItem = tmpItem.replace(".", "\.")
        # change * to .* for regexp
        tmpItem = tmpItem.replace("*", ".*")
        # append
        excludeFile.append(tmpItem)


# matching for extFiles
def matchExtFile(fileName):
    # check exclude files
    for tmpPatt in excludeFile:
        if re.search(tmpPatt, fileName) is not None:
            return False
    # gather files with special extensions
    for tmpExtention in [".py", ".dat", ".C", ".xml", "Makefile", ".cc", ".cxx", ".h", ".hh", ".sh", ".cpp", ".hpp"]:
        if fileName.endswith(tmpExtention):
            return True
    # check filename
    baseName = fileName.split("/")[-1]
    for patt in extFile:
        if patt.find("*") == -1:
            # regular matching
            if patt == baseName:
                return True
            # patt may contain / for sub dir
            if patt != "" and re.search(patt, fileName) is not None:
                return True
        else:
            # use regex for *
            tmpPatt = patt.replace("*", ".*")
            if re.search(tmpPatt, baseName) is not None:
                return True
            # patt may contain / for sub dir
            if patt != "" and re.search(tmpPatt, fileName) is not None:
                return True
    # not matched
    return False


# extended extra stream name
useExtendedExtStreamName = False


# use extended extra stream name
def enableExtendedExtStreamName():
    global useExtendedExtStreamName
    useExtendedExtStreamName = True


# get extended extra stream name
def getExtendedExtStreamName(sIndex, sName, enableExtension):
    tmpBaseExtName = "EXT%s" % sIndex
    if not useExtendedExtStreamName or not enableExtension:
        return tmpBaseExtName
    # change * to X and add .tgz
    if sName.find("*") != -1:
        sName = sName.replace("*", "XYZ")
        sName = "%s.tgz" % sName
    # use extended extra stream name
    tmpItems = sName.split(".")
    if len(tmpItems) > 0:
        tmpBaseExtName += "_%s" % tmpItems[0]
    return tmpBaseExtName


# special files to be treated carefully
specialFilesForAthena = ["dblookup.xml"]


# archive source files
def archiveSourceFiles(workArea, runDir, currentDir, tmpDir, verbose, gluePackages=[], dereferenceSymLinks=False, archiveName=""):
    # archive sources
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info("archiving source files")

    #####################################################################
    # subroutines

    # scan InstallArea to get a list of local packages
    def getFileList(dir, files, forPackage, readLink=True):
        try:
            list = os.listdir(dir)
        except Exception:
            return
        for item in list:
            # skip if doc or .svn
            if item in ["doc", ".svn", "_CPack_Packages"]:
                continue
            fullName = dir + "/" + item
            if os.path.isdir(fullName):
                # ignore symlinked dir just under InstallArea/include
                # they are created for g77
                if os.path.islink(fullName) and re.search("/InstallArea/include$", dir) is not None:
                    pass
                elif os.path.islink(fullName) and readLink and forPackage:
                    # resolve symlink
                    getFileList(os.readlink(fullName), files, forPackage, readLink)
                else:
                    getFileList(fullName, files, forPackage, readLink)
            else:
                if os.path.islink(fullName):
                    if readLink:
                        tmpLink = os.readlink(fullName)
                        # put base dir when relative path
                        if not tmpLink.startswith("/"):
                            tmpLink = dir + "/" + tmpLink
                            tmpLink = os.path.abspath(tmpLink)
                        appFileName = tmpLink
                    else:
                        appFileName = os.path.abspath(fullName)
                else:
                    appFileName = os.path.abspath(fullName)
                # remove redundant //
                appFilename = re.sub("//", "/", appFileName)
                # append
                files.append(appFileName)

    # get package list
    def getPackages(_workArea, gluePackages=[]):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # special packages
        specialPackages = {"External/Lhapdf": "external/MCGenerators/lhapdf"}
        # get file list
        installFiles = []
        getFileList(_workArea + "/InstallArea", installFiles, True)
        # get list of packages
        cmt_config = os.environ["CMTCONFIG"]
        _packages = []
        for iFile in installFiles:
            # ignore InstallArea stuff
            if re.search("/InstallArea/", iFile):
                continue
            # converted to real path
            file = os.path.realpath(iFile)
            # remove special characters
            sString = re.sub("[\+]", ".", os.path.realpath(_workArea))
            # look for /share/ , /python/, /i686-slc3-gcc323-opt/, .h
            for target in ("share/", "python/", cmt_config + "/", "[^/]+\.h"):
                res = re.search(sString + "/(.+)/" + target, file)
                if res:
                    # append
                    pName = res.group(1)
                    if target in ["[^/]+\.h"]:
                        # convert PackageDir/PackageName/PackageName to PackageDir/PackageName
                        pName = re.sub("/[^/]+$", "", pName)
                    if pName not in _packages:
                        if os.path.isdir(_workArea + "/" + pName):
                            _packages.append(pName)
                    break
            # check special packages just in case
            for pName in specialPackages:
                pPath = specialPackages[pName]
                if pName not in _packages:
                    # look for path pattern
                    if re.search(pPath, file) is not None:
                        if os.path.isdir(_workArea + "/" + pName):
                            # check structured style
                            tmpDirList = os.listdir(_workArea + "/" + pName)
                            useSS = False
                            for tmpDir in tmpDirList:
                                if re.search("-\d+-\d+-\d+$", tmpDir) is not None:
                                    _packages.append(pName + "/" + tmpDir)
                                    useSS = True
                                    break
                            # normal structure
                            if not useSS:
                                _packages.append(pName)
                            # delete since no needs anymore
                            del specialPackages[pName]
                            break
        # check glue packages
        for pName in gluePackages:
            if pName not in _packages:
                if os.path.isdir(_workArea + "/" + pName):
                    # check structured style
                    tmpDirList = os.listdir(_workArea + "/" + pName)
                    useSS = False
                    for tmpDir in tmpDirList:
                        if re.search("-\d+-\d+-\d+$", tmpDir) is not None:
                            fullPName = pName + "/" + tmpDir
                            if fullPName not in _packages:
                                _packages.append(fullPName)
                            useSS = True
                            break
                    # normal structure
                    if not useSS:
                        _packages.append(pName)
                else:
                    tmpLog.warning("glue package %s not found under %s" % (pName, _workArea))
        # return
        return _packages

    # archive files
    def archiveFiles(_workArea, _packages, _archiveFullName):
        excludePattern = ".svn"
        for tmpPatt in excludeFile:
            # reverse regexp change
            tmpPatt = tmpPatt.replace(".*", "*")
            tmpPatt = tmpPatt.replace("\.", ".")
            excludePattern += " --exclude '%s'" % tmpPatt
        _curdir = os.getcwd()
        # change dir
        os.chdir(_workArea)
        for pack in _packages:
            # archive subdirs
            list = os.listdir(pack)
            for item in list:
                # ignore libraries
                if (
                    item.startswith("i686")
                    or item.startswith("i386")
                    or item.startswith("x86_64")
                    or item == "pool"
                    or item == "pool_plugins"
                    or item == "doc"
                    or item == ".svn"
                ):
                    continue
                # check exclude files
                excludeFileFlag = False
                for tmpPatt in excludeFile:
                    if re.search(tmpPatt, "%s/%s" % (pack, item)) is not None:
                        excludeFileFlag = True
                        break
                if excludeFileFlag:
                    continue
                # run dir
                if item == "run":
                    files = []
                    getFileList("%s/%s/run" % (_workArea, pack), files, False)
                    # not resolve symlink (appending instead of replacing for backward compatibility)
                    tmpFiles = []
                    getFileList("%s/%s/run" % (_workArea, pack), tmpFiles, False, False)
                    for tmpFile in tmpFiles:
                        if tmpFile not in files:
                            files.append(tmpFile)
                    for iFile in files:
                        # converted to real path
                        file = os.path.realpath(iFile)
                        # archive .py/.dat/.C files only
                        if matchExtFile(file):
                            # remove special characters
                            sString = re.sub("[\+]", ".", os.path.realpath(_workArea))
                            relPath = re.sub("^%s/" % sString, "", file)
                            # if replace is failed or the file is symlink, try non-converted path names
                            if relPath.startswith("/") or os.path.islink(iFile):
                                sString = re.sub("[\+]", ".", workArea)
                                relPath = re.sub(sString + "/", "", iFile)
                            if os.path.islink(iFile):
                                comStr = "tar -rh '%s' -f '%s' --exclude '%s'" % (relPath, _archiveFullName, excludePattern)
                            else:
                                comStr = "tar rf '%s' '%s' --exclude '%s'" % (_archiveFullName, relPath, excludePattern)
                            if verbose:
                                print(relPath)

                            commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")
                    continue
                # else
                if dereferenceSymLinks:
                    comStr = "tar rfh '%s' '%s/%s' --exclude '%s'" % (_archiveFullName, pack, item, excludePattern)
                else:
                    comStr = "tar rf '%s' '%s/%s' --exclude '%s'" % (_archiveFullName, pack, item, excludePattern)

                if verbose:
                    print("%s/%s" % (pack, item))

                commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")
        # back to previous dir
        os.chdir(_curdir)

    #####################################################################
    # execute

    # get packages in private area
    packages = getPackages(workArea, gluePackages)
    # check TestRelease since it doesn't create any links in InstallArea
    if os.path.exists("%s/TestRelease" % workArea):
        # the TestRelease could be created by hand
        packages.append("TestRelease")

    if verbose:
        tmpLog.debug("== private packages ==")
        for pack in packages:
            print(pack)
        tmpLog.debug("== private files ==")

    # create archive
    if archiveName == "":
        archiveName = "sources.%s.tar" % MiscUtils.wrappedUuidGen()
    archiveFullName = "%s/%s" % (tmpDir, archiveName)
    # archive private area
    archiveFiles(workArea, packages, archiveFullName)
    # archive current (run) dir
    files = []
    os.chdir(workArea)
    getFileList("%s/%s" % (workArea, runDir), files, False, False)
    for file in files:
        # remove special characters
        sString = re.sub("[\+]", ".", os.path.realpath(workArea))
        relPath = re.sub(sString + "/", "", os.path.realpath(file))
        # if replace is failed or the file is symlink, try non-converted path names
        if relPath.startswith("/") or os.path.islink(file):
            sString = re.sub("[\+]", ".", workArea)
            relPath = re.sub(sString + "/", "", file)
        # archive .py/.dat/.C/.xml files only
        if not matchExtFile(relPath):
            continue
        # ignore InstallArea
        if relPath.startswith("InstallArea"):
            continue
        # check special files
        spBaseName = relPath
        if re.search("/", spBaseName) is not None:
            spBaseName = spBaseName.split("/")[-1]
        if spBaseName in specialFilesForAthena:
            warStr = "%s in the current dir is sent to remote WNs, which might cause a database problem. " % spBaseName
            warStr += "If this is intentional please ignore this WARNING"
            tmpLog.warning(warStr)
        # check if already archived
        alreadyFlag = False
        for pack in packages:
            if relPath.startswith(pack):
                alreadyFlag = True
                break
        # archive
        if not alreadyFlag:
            if os.path.islink(file):
                comStr = "tar -rh '%s' -f '%s'" % (relPath, archiveFullName)
            else:
                comStr = "tar rf '%s' '%s'" % (archiveFullName, relPath)
            if verbose:
                print(relPath)

            commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")
    # back to current dir
    os.chdir(currentDir)
    # return
    return archiveName, archiveFullName


# archive jobO files
def archiveJobOFiles(workArea, runDir, currentDir, tmpDir, verbose, archiveName=""):
    # archive jobO files
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info("archiving jobOs and modules")

    # get real jobOs
    def getJobOs(dir, files):
        list = os.listdir(dir)
        for item in list:
            if item in ["_CPack_Packages"]:
                continue
            fullName = dir + "/" + item
            if os.path.isdir(fullName):
                # skip symlinks in include since they cause full scan on releases
                if os.path.islink(fullName) and re.search("InstallArea/include$", dir) is not None:
                    continue
                # dir
                getJobOs(fullName, files)
            else:
                # python and other extFiles
                if matchExtFile(fullName):
                    files.append(fullName)

    # get jobOs
    files = []
    os.chdir(workArea)
    getJobOs("%s" % workArea, files)
    # create archive
    if archiveName == "":
        archiveName = "jobO.%s.tar" % MiscUtils.wrappedUuidGen()
    archiveFullName = "%s/%s" % (tmpDir, archiveName)
    # archive
    if verbose:
        tmpLog.debug("== py files ==")
    for file in files:
        # remove special characters
        sString = re.sub("[\+]", ".", workArea)
        relPath = re.sub(sString + "/", "", file)
        # append
        comStr = "tar -rh '%s' -f '%s'" % (relPath, archiveFullName)
        if verbose:
            print(relPath)

        commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")

    # return
    return archiveName, archiveFullName


# archive InstallArea
def archiveInstallArea(workArea, groupArea, archiveName, archiveFullName, tmpDir, nobuild, verbose):
    # archive jobO files
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info("archiving InstallArea")

    # get file list
    def getFiles(dir, files, ignoreLib, ignoreSymLink):
        if verbose:
            tmpLog.debug("  getFiles(%s)" % dir)
        try:
            list = os.listdir(dir)
        except Exception:
            return
        for item in list:
            if ignoreLib and (item.startswith("i686") or item.startswith("i386") or item.startswith("x86_64")):
                continue
            fullName = dir + "/" + item
            if os.path.isdir(fullName):
                # ignore symlinked dir just under InstallArea/include
                if ignoreSymLink and os.path.islink(fullName) and re.search("InstallArea/include$", dir) is not None:
                    continue
                # dir
                getFiles(fullName, files, False, ignoreSymLink)
            else:
                files.append(fullName)

    # get cmt files
    def getCMTFiles(dir, files):
        list = os.listdir(dir)
        for item in list:
            fullName = dir + "/" + item
            if os.path.isdir(fullName):
                # dir
                getCMTFiles(fullName, files)
            else:
                if re.search("cmt/requirements$", fullName) is not None:
                    files.append(fullName)

    # get files
    areaList = []
    # workArea must be first
    areaList.append(workArea)
    if groupArea != "":
        areaList.append(groupArea)
    # groupArea archive
    groupFileName = re.sub("^sources", "groupArea", archiveName)
    groupFullName = "%s/%s" % (tmpDir, groupFileName)
    allFiles = []
    for areaName in areaList:
        # archive
        if verbose:
            tmpLog.debug("== InstallArea under %s ==" % areaName)
        files = []
        cmtFiles = []
        os.chdir(areaName)
        if areaName == workArea:
            if not nobuild:
                # ignore i686 and include for workArea
                getFiles("InstallArea", files, True, True)
            else:
                # ignore include for workArea
                getFiles("InstallArea", files, False, True)
        else:
            # groupArea
            if not os.path.exists("InstallArea"):
                if verbose:
                    print("  Doesn't exist. Skip")
                continue
            getFiles("InstallArea", files, False, False)
            # cmt/requirements is needed for non-release packages
            for itemDir in os.listdir(areaName):
                if itemDir != "InstallArea" and os.path.isdir(itemDir) and (not os.path.islink(itemDir)):
                    getCMTFiles(itemDir, cmtFiles)
        # remove special characters
        sString = re.sub("[\+]", ".", os.path.realpath(areaName))
        # archive files if they are under the area
        for file in files + cmtFiles:
            relPath = re.sub(sString + "/", "", os.path.realpath(file))
            # check exclude files
            excludeFileFlag = False
            for tmpPatt in excludeFile:
                if re.search(tmpPatt, relPath) is not None:
                    excludeFileFlag = True
                    break
            if excludeFileFlag:
                continue
            if not relPath.startswith("/"):
                # use files in private InstallArea instead of group InstallArea
                if file not in allFiles:
                    # append
                    if file in files:
                        comStr = "tar -rh '%s' -f '%s'" % (file, archiveFullName)
                    else:
                        # requirements files
                        comStr = "tar -rh '%s' -f '%s'" % (file, groupFullName)
                    allFiles.append(file)
                    if verbose:
                        print(file)

                    commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")

    # append groupArea to sources
    if groupArea != "" and (not nobuild):
        os.chdir(tmpDir)
        if os.path.exists(groupFileName):
            comStr = "tar -rh '%s' -f '%s'" % (groupFileName, archiveFullName)
            commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")

            commands_get_output("rm -rf %s" % groupFullName)


# archive with cpack
def archiveWithCpack(withSource, tmpDir, verbose):
    tmpLog = PLogger.getPandaLogger()
    # define archive name
    if withSource:
        tmpLog.info("archiving source files with cpack")
        archiveName = "sources.%s" % MiscUtils.wrappedUuidGen()
    else:
        tmpLog.info("archiving jobOs and modules with cpack")
        archiveName = "jobO.%s" % MiscUtils.wrappedUuidGen()
    archiveFullName = "%s/%s" % (tmpDir, archiveName)
    # extract build dir
    buildDir = os.environ["CMAKE_PREFIX_PATH"]
    buildDir = os.path.dirname(buildDir.split(":")[0])
    _curdir = os.getcwd()
    os.chdir(buildDir)
    tmpLog.info("the build directory is {0}".format(buildDir))
    check_file = "CPackConfig.cmake"
    if os.path.exists(os.path.join(buildDir, check_file)):
        use_cpack = True
        comStr = "cpack -B {0} -D CPACK_PACKAGE_FILE_NAME={1} -G TGZ ".format(tmpDir, archiveName)
        comStr += '-D CPACK_PACKAGE_NAME="" -D CPACK_PACKAGE_VERSION="" -D CPACK_PACKAGE_VERSION_MAJOR="" '
        comStr += '-D CPACK_PACKAGE_VERSION_MINOR="" -D CPACK_PACKAGE_VERSION_PATCH="" '
        comStr += '-D CPACK_PACKAGE_DESCRIPTION="" '

        commands_fail_on_non_zero_exit_status(comStr, EC_Config, verbose_cmd=verbose, verbose_output=verbose, logger=tmpLog, error_log_msg="cpack failed")

    else:
        use_cpack = False
        tmpLog.info("skip cpack since {0} is missing in the build directory".format(check_file))

    archiveName += ".tar"
    archiveFullName += ".tar"
    os.chdir(tmpDir)
    if use_cpack:
        # recreate tar to allow appending other files in the subsequent steps, as gzip decompress is not enough
        comStr = "tar xfz {0}.gz; tar cf {0} usr > /dev/null 2>&1; rm -rf usr _CPack_Packages {0}.gz".format(archiveName)
    else:
        comStr = "tar cf {0} -T /dev/null > /dev/null 2>&1".format(archiveName)

    commands_fail_on_non_zero_exit_status(comStr, EC_Archive, logger=tmpLog, error_log_msg="tarball creation failed")

    os.chdir(_curdir)
    return archiveName, archiveFullName


# convert runConfig to outMap
def convertConfToOutput(runConfig, extOutFile, original_outDS, destination="", spaceToken="", descriptionInLFN="", allowNoOutput=None):
    outMap = {}
    paramList = []
    # add IROOT
    if "IROOT" not in outMap:
        outMap["IROOT"] = []
    # remove /
    outDSwoSlash = re.sub("/$", "", original_outDS)
    outDsNameBase = outDSwoSlash
    tmpMatch = re.search("^([^\.]+)\.([^\.]+)\.", original_outDS)
    if tmpMatch is not None and original_outDS.endswith("/"):
        outDSwoSlash = "%s.%s" % (tmpMatch.group(1), tmpMatch.group(2))
        if descriptionInLFN != "":
            outDSwoSlash += descriptionInLFN
        outDSwoSlash += ".$JEDITASKID"
    # start conversion
    if runConfig.output.outNtuple:
        for sName in runConfig.output.outNtuple:
            lfn = "%s.%s._${SN/P}.root" % (outDSwoSlash, sName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            if "ntuple" not in outMap:
                outMap["ntuple"] = []
            outMap["ntuple"].append((sName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outHist:
        lfn = "%s.hist._${SN/P}.root" % outDSwoSlash
        tmpSuffix = "_HIST"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["hist"] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outRDO:
        lfn = "%s.RDO._${SN/P}.pool.root" % outDSwoSlash
        tmpSuffix = "_RDO"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["IROOT"].append((runConfig.output.outRDO, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outESD:
        lfn = "%s.ESD._${SN/P}.pool.root" % outDSwoSlash
        tmpSuffix = "_ESD"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["IROOT"].append((runConfig.output.outESD, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outAOD:
        lfn = "%s.AOD._${SN/P}.pool.root" % outDSwoSlash
        tmpSuffix = "_AOD"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["IROOT"].append((runConfig.output.outAOD, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outTAG:
        lfn = "%s.TAG._${SN/P}.coll.root" % outDSwoSlash
        tmpSuffix = "_TAG"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["TAG"] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outAANT:
        sNameList = []
        fsNameMap = {}
        for aName, sName, fName in runConfig.output.outAANT:
            # use first sName when multiple streams write to the same file
            realStreamName = sName
            if fName in fsNameMap:
                sName = fsNameMap[fName]
            else:
                fsNameMap[fName] = sName
            lfn = "%s.%s._${SN/P}.root" % (outDSwoSlash, sName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            if sName not in sNameList:
                sNameList.append(sName)
            if "AANT" not in outMap:
                outMap["AANT"] = []
            outMap["AANT"].append((aName, realStreamName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outTHIST:
        for sName in runConfig.output.outTHIST:
            lfn = "%s.%s._${SN/P}.root" % (outDSwoSlash, sName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            if "THIST" not in outMap:
                outMap["THIST"] = []
            outMap["THIST"].append((sName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outIROOT:
        for sIndex, sName in enumerate(runConfig.output.outIROOT):
            lfn = "%s.iROOT%s._${SN/P}.%s" % (outDSwoSlash, sIndex, sName)
            tmpSuffix = "_iROOT%s" % sIndex
            dataset = outDsNameBase + tmpSuffix + "/"
            if "IROOT" not in outMap:
                outMap["IROOT"] = []
            outMap["IROOT"].append((sName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if extOutFile:
        for sIndex, sName in enumerate(extOutFile):
            # change * to X and add .tgz
            origSName = sName
            if sName.find("*") != -1:
                sName = sName.replace("*", "XYZ")
                sName = "%s.tgz" % sName
            tmpExtStreamName = getExtendedExtStreamName(sIndex, sName, False)
            lfn = "%s.%s._${SN/P}.%s" % (outDSwoSlash, tmpExtStreamName, sName)
            tmpSuffix = "_%s" % tmpExtStreamName
            dataset = outDsNameBase + tmpSuffix + "/"
            if "IROOT" not in outMap:
                outMap["IROOT"] = []
            outMap["IROOT"].append((origSName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outTAGX:
        for sName, oName in runConfig.output.outTAGX:
            lfn = "%s.%s._${SN/P}.%s" % (outDSwoSlash, sName, oName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            if "IROOT" not in outMap:
                outMap["IROOT"] = []
            outMap["IROOT"].append((oName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outStream1:
        lfn = "%s.Stream1._${SN/P}.pool.root" % outDSwoSlash
        tmpSuffix = "_Stream1"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["IROOT"].append((runConfig.output.outStream1, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outStream2:
        lfn = "%s.Stream2._${SN/P}.pool.root" % outDSwoSlash
        tmpSuffix = "_Stream2"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["IROOT"].append((runConfig.output.outStream2, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outBS:
        lfn = "%s.BS._${SN/P}.data" % outDSwoSlash
        tmpSuffix = "_BS"
        dataset = outDsNameBase + tmpSuffix + "/"
        outMap["BS"] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outSelBS:
        lfn = "%s.%s._${SN/P}.data" % (outDSwoSlash, runConfig.output.outSelBS)
        tmpSuffix = "_SelBS"
        dataset = outDsNameBase + tmpSuffix + "/"
        if "IROOT" not in outMap:
            outMap["IROOT"] = []
        outMap["IROOT"].append(("%s.*.data" % runConfig.output.outSelBS, lfn))
        paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outStreamG:
        for sName, sOrigFileName in runConfig.output.outStreamG:
            lfn = "%s.%s._${SN/P}.pool.root" % (outDSwoSlash, sName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            outMap["IROOT"].append((sOrigFileName, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outMeta:
        iMeta = 0
        for sName, sAsso in runConfig.output.outMeta:
            foundLFN = ""
            if sAsso == "None":
                # non-associated metadata
                lfn = "%s.META%s._${SN/P}.root" % (outDSwoSlash, iMeta)
                tmpSuffix = "_META%s" % iMeta
                dataset = outDsNameBase + tmpSuffix + "/"
                paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
                iMeta += 1
                foundLFN = lfn
            elif sAsso in outMap:
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ["StreamESD", "StreamAOD"]:
                # ESD,AOD
                stKey = re.sub("^Stream", "", sAsso)
                if stKey in outMap:
                    foundLFN = outMap[stKey]
                else:
                    # check StreamG when ESD/AOD are not defined as algorithms
                    if "StreamG" in outMap:
                        for tmpStName, tmpLFN in outMap["StreamG"]:
                            if tmpStName == sAsso:
                                foundLFN = tmpLFN
            elif sAsso == "StreamRDO" and "StreamRDO" in outMap:
                # RDO
                stKey = re.sub("^Stream", "", sAsso)
                if stKey in outMap:
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if "StreamG" in outMap:
                    for tmpStName, tmpLFN in outMap["StreamG"]:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != "":
                if "Meta" not in outMap:
                    outMap["Meta"] = []
                outMap["Meta"].append((sName, foundLFN))
    if runConfig.output.outMS:
        for sName, sAsso in runConfig.output.outMS:
            lfn = "%s.%s._${SN/P}.pool.root" % (outDSwoSlash, sName)
            tmpSuffix = "_%s" % sName
            dataset = outDsNameBase + tmpSuffix + "/"
            if "IROOT" not in outMap:
                outMap["IROOT"] = []
            outMap["IROOT"].append((sAsso, lfn))
            paramList += MiscUtils.makeJediJobParam(lfn, dataset, "output", hidden=True, allowNoOutput=allowNoOutput)
    if runConfig.output.outUserData:
        for sAsso in runConfig.output.outUserData:
            # look for associated LFN
            foundLFN = ""
            if sAsso in outMap:
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ["StreamRDO", "StreamESD", "StreamAOD"]:
                # RDO,ESD,AOD
                stKey = re.sub("^Stream", "", sAsso)
                if stKey in outMap:
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if "StreamG" in outMap:
                    for tmpStName, tmpLFN in outMap["StreamG"]:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != "":
                if "UserData" not in outMap:
                    outMap["UserData"] = []
                outMap["UserData"].append(foundLFN)
    # remove IROOT if unnecessary
    if "IROOT" in outMap and outMap["IROOT"] == []:
        del outMap["IROOT"]
    # set destination
    if destination != "":
        for tmpParam in paramList:
            tmpParam["destination"] = destination
    # set token
    if spaceToken != "":
        for tmpParam in paramList:
            tmpParam["token"] = spaceToken
    # return
    return outMap, paramList


# get CMTCONFIG + IMG
def getCmtConfigImg(athenaVer=None, cacheVer=None, nightVer=None, cmtConfig=None, verbose=False, architecture=None):
    # get CMTCONFIG
    cmt_config = ""
    spec_str = ""
    if architecture:
        tmp_m = re.search("^[^@&#]+", architecture)
        if tmp_m:
            cmt_config = tmp_m.group(0)
            spec_str = architecture.replace(cmt_config, "")
        else:
            tmp_m = re.search("[@&#].+$", architecture)
            if tmp_m:
                spec_str = tmp_m.group(0)
    if not cmt_config:
        cmt_config = getCmtConfig(athenaVer, cacheVer, nightVer, cmtConfig, verbose)
    # get base platform + HW specs
    if spec_str:
        pass
    elif "ALRB_USER_PLATFORM" in os.environ:
        # base platform + HW specs from ALRB
        spec_str = "@" + os.environ["ALRB_USER_PLATFORM"]
    else:
        # architecture w/o base platform or even empty architecture
        spec_str = architecture
    # append base platform + HW specs if any
    if spec_str:
        if cmt_config is None:
            cmt_config = ""
        cmt_config = cmt_config + spec_str
    return cmt_config


# get CMTCONFIG
def getCmtConfig(athenaVer=None, cacheVer=None, nightVer=None, cmtConfig=None, verbose=False):
    # use user-specified cmtconfig
    if cmtConfig:
        return cmtConfig
    # local settting
    if "CMTCONFIG" in os.environ:
        return os.environ["CMTCONFIG"]
    # undefined in Athena
    if athenaVer or cacheVer:
        # get logger
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error("environment variable CMTCONFIG is undefined. Please set --cmtConfig")
        sys.exit(EC_Config)
    return None


# check CMTCONFIG
def checkCmtConfig(localCmtConfig, userCmtConfig, noBuild):
    # didn't specify CMTCONFIG
    if userCmtConfig in ["", None]:
        return True
    # CVMFS version format
    if re.search("-gcc\d+\.\d+$", userCmtConfig) is not None:
        return True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # CMTCONFIG is undefined locally
    if localCmtConfig in ["", None]:
        return True
    # user-specified CMTCONFIG is inconsitent with local CMTCONFIG
    if userCmtConfig != localCmtConfig and noBuild:
        errStr = "You cannot use --noBuild when --cmtConfig=%s is inconsistent with local CMTCONFIG=%s " % (userCmtConfig, localCmtConfig)
        errStr += "since you need re-compile source files on remote worker-node. Please remove --noBuild"
        tmpLog.error(errStr)
        return False
    # return OK
    return True


# use AMI for AutoConf
def getJobOtoUseAmiForAutoConf(inDS, tmpDir):
    # no input
    if inDS == "":
        return ""
    # use first one
    amiURL = "ami://%s" % inDS.split(",")[0]
    # remove /
    if amiURL.endswith("/"):
        amiURL = amiURL[:-1]
    inputFiles = [amiURL]
    # create jobO fragment
    optFileName = tmpDir + "/" + MiscUtils.wrappedUuidGen() + ".py"
    oFile = open(optFileName, "w")
    oFile.write(
        """
try:
    import AthenaCommon.AthenaCommonFlags
    
    def _dummyFilesInput(*argv):
        return %s

    AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except Exception:
    pass

try:
    import AthenaCommon.AthenaCommonFlags
    
    def _dummyGet_Value(*argv):
        return %s

    for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
        import re
        if re.search('^(Pool|BS).*Input$',tmpAttr) is not None:
            try:
                getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value
            except Exception:
                pass
except Exception:
    pass
"""
        % (inputFiles, inputFiles)
    )
    oFile.close()
    # reutrn file name
    return optFileName


# use CMake
def useCMake():
    return "CMAKE_PREFIX_PATH" in os.environ


# parse athenaTag
def parse_athena_tag(athena_tag, verbose, tmp_log):
    athenaVer = ""
    cacheVer = ""
    nightVer = ""
    # loop over all tags
    items = athena_tag.split(",")
    usingNightlies = False
    for item in items:
        # releases
        match = re.search(r"^(\d+\.\d+\.\d+)", item)
        if match:
            athenaVer = "Atlas-%s" % match.group(1)
            # cache
            cmatch = re.search(r"^(\d+\.\d+\.\d+\.\d+\.*\d*)$", item)
            if cmatch is not None:
                cacheVer += "_%s" % cmatch.group(1)
            else:
                cacheVer += "_%s" % match.group(1)
            continue
        # nightlies
        match = re.search(r"^(\d+\.\d+\.X|\d+\.X\.\d+)$", item)
        if match:
            athenaVer = "Atlas-%s" % match.group(1)
            continue
        # master or XX.YY
        match = re.search(r"^\d+\.\d+$", item)
        if item.startswith("master") or match or item == "main":
            athenaVer = "Atlas-%s" % item
            continue
        # old nightlies
        if item.startswith("rel_"):
            usingNightlies = True
            if "dev" in items:
                athenaVer = "Atlas-dev"
            elif "devval" in items:
                athenaVer = "Atlas-devval"
            cacheVer = "-AtlasOffline_%s" % item
            continue
        # nightlies
        if item in ["latest", "r27"] or re.search(r"^\d{4}-\d{2}-\d{2}T\d{4}$", item):
            cacheVer += "_%s" % item
            continue
        # CMTCONFIG
        if item in ["32", "64"]:
            tmp_log.error("%s in --athenaTag is unsupported. Please use --cmtConfig instead" % item)
            sys.exit(EC_Config)
        # ignoring AtlasOffline
        if item in ["AtlasOffline"]:
            continue
        # regarded as project
        cacheVer = "-" + item + cacheVer
    # check cache
    if re.search(r"^-.+_.+$", cacheVer) is None:
        if re.search(r"^_\d+\.\d+\.\d+\.\d+$", cacheVer) is not None:
            # use AtlasProduction
            cacheVer = "-AtlasProduction" + cacheVer
        elif "AthAnalysisBase" in cacheVer or "AthAnalysis" in cacheVer:
            # AthAnalysis
            cacheVer = cacheVer + "_%s" % athenaVer
            athenaVer = ""
        else:
            # unknown
            cacheVer = ""
    # use dev nightlies
    if usingNightlies and athenaVer == "":
        athenaVer = "Atlas-dev"
    return athenaVer, cacheVer, nightVer
