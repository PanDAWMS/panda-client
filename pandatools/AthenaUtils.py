import os
import re
import sys
import commands

import MiscUtils
import PLogger
import Client

# error code
EC_Config    = 10


# replace parameter with compact LFNs
def replaceParam(patt,inList,tmpJobO,useNewTRF=False):
    # remove attempt numbers
    compactLFNs = []
    for tmpLFN in inList:
        compactLFNs.append(re.sub('\.\d+$','',tmpLFN))
    # sort
    compactLFNs.sort()
    # replace parameters
    if len(compactLFNs) < 2:
        # replace for single input
        if not useNewTRF:
            tmpJobO = tmpJobO.replace(patt,compactLFNs[0])
        else:
            # use original filename with attempt number in new trf
            tmpJobO = tmpJobO.replace(patt,inList[0])
    else:
        # find head and tail to convert file.1.pool,file.2.pool,file.4.pool to file.[1,2,4].pool
        tmpHead = ''
        tmpTail = ''
        tmpLFN0 = compactLFNs[0]
        tmpLFN1 = compactLFNs[1]
        fullLFNList = ''
        for i in range(len(tmpLFN0)):
            match = re.search('^(%s)' % tmpLFN0[:i],tmpLFN1)
            if match:
                tmpHead = match.group(1)
            match = re.search('(%s)$' % tmpLFN0[-i:],tmpLFN1)
            if match:
                tmpTail = match.group(1)
        # remove numbers : ABC_00,00_XYZ -> ABC_,_XYZ
        tmpHead = re.sub('\d*$','',tmpHead)
        tmpTail = re.sub('^\d*','',tmpTail)
        # create compact paramter
        compactPar = '%s[' % tmpHead
        for tmpLFN in compactLFNs:
            # keep full LFNs
            fullLFNList += '%s,' % tmpLFN
            # extract number
            tmpLFN = re.sub('^%s' % tmpHead,'',tmpLFN)
            tmpLFN = re.sub('%s$' % tmpTail,'',tmpLFN)
            compactPar += '%s,' % tmpLFN
        compactPar = compactPar[:-1]
        compactPar += ']%s' % tmpTail
        fullLFNList = fullLFNList[:-1]
        # check contents in []
        conMatch = re.search('\[([^\]]+)\]',compactPar)
        if conMatch != None and re.search('^[\d,]+$',conMatch.group(1)) != None:
            # replace with compact format
            tmpJobO = tmpJobO.replace(patt,compactPar)
        else:
            # replace with full format since [] contains non digits
            tmpJobO = tmpJobO.replace(patt,fullLFNList)
    # return
    return tmpJobO


# get references from collection
def getGUIDfromColl(athenaVer,inputColls,directory,refName='Token',verbose=False):
    allrefs = []
    refs = {}
    # supported with 14.4.0 and onward
    if athenaVer != 'dev' and athenaVer < '14.4.0':
        print "WARNING : getGUIDfromColl is not supported in %s" \
              % athenaVer
        return refs,allrefs
    # extract refereces
    for inputColl in inputColls:
        refs[inputColl] = []
        com = "CollListFileGUID.exe -queryopt %s -src PFN:%s/%s RootCollection" % \
              (refName,directory,inputColl)
        if verbose:
            print com
        status,out = commands.getstatusoutput(com)
        if verbose or status != 0:
            print status,out
            if status != 0:
                raise RuntimeError,"ERROR : failed to run %s" % com
        # get GUIDs
        for line in out.split('\n'):
            items = line.split()
            # confirm GUID format
            guid = items[-1]
            if re.search('^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$',guid):
                if not refs.has_key(guid):
                    refs[guid] = inputColl
                if not guid in allrefs:
                    allrefs.append(guid)
    # return
    return refs,allrefs


# convert list of files to compact format : header[body]tailer[attemptNr]
def convToCompact(fList):
    # no conversion
    if len(fList) == 0:
        return ''
    elif len(fList) == 1:
        return '%s' % fList[0]
    # get header
    header = fList[0]
    for item in fList:
        # look for identical sub-string
        findStr = False
        for idx in range(len(item)):
            if idx == 0:
                subStr = item
            else:
                subStr = item[:-idx]
            # compare
            if re.search('^%s' % subStr, header) != None:
                # set header
                header = subStr
                findStr = True
                break
        # not found
        if not findStr:
            header = ''
            break
    # get body and attemptNr
    bodies = []
    attNrs = []
    for item in fList:
        body  = re.sub('^%s' % header,'',item)
        attNr = ''
        # look for attNr
        match = re.search('(.+)(\.\d+)$',body)
        if match != None:
            body  = match.group(1)
            attNr = match.group(2)
        # append    
        bodies.append(body)
        attNrs.append(attNr)
    # get tailer
    tailer = bodies[0]
    for item in bodies:
        # look for identical sub-string
        findStr = False
        for idx in range(len(item)):
            subStr = item[idx:]
            # compare
            if re.search('%s$' % subStr, tailer) != None:
                # set tailer
                tailer = subStr
                findStr = True
                break
        # not found
        if not findStr:
            tailer = ''
            break
    # remove tailer from bodies
    realBodies = []
    for item in bodies:
        realBody = re.sub('%s$' % tailer,'',item)
        realBodies.append(realBody)
    bodies = realBodies    
    # convert to string
    retStr = "%s%s%s%s" % (header,bodies,tailer,attNrs)
    # remove whitespaces and '
    retStr = re.sub('( |\'|\")','',retStr)
    return retStr


# get CMT projects
def getCmtProjects(dir='.'):
    # keep current dir
    curdir = os.getcwd()
    # change dir
    os.chdir(dir)
    # get projects
    out = commands.getoutput('cmt show projects')
    lines = out.split('\n')
    # remove CMT warnings
    tupLines = tuple(lines)
    lines = []
    for line in tupLines:
        if 'CMTUSERCONTEXT' in line:
            continue
        if not line.startswith('#'):
            lines.append(line)
    # back to the current dir
    os.chdir(curdir)
    # return
    return lines,out         
    
    
# get Athena version
def getAthenaVer():
    # get logger
    tmpLog = PLogger.getPandaLogger()            
    # get project parameters
    lines,out = getCmtProjects()
    if len(lines)<2:
        # make a tmp dir to execute cmt
        tmpDir = 'cmttmp.%s' % MiscUtils.wrappedUuidGen()
        os.mkdir(tmpDir)
        # try cmt under a subdir since it doesn't work in top dir
        lines,tmpOut = getCmtProjects(tmpDir)
        # delete the tmp dir
        commands.getoutput('rm -rf %s' % tmpDir)
        if len(lines)<2:
            print out
            tmpLog.error("cmt gave wrong info")
            return False,{}
    # private work area
    res = re.search('\(in ([^\)]+)\)',lines[0])
    if res==None:
        print lines[0]
        tmpLog.error("could not get path to private work area")
        return False,{}
    workArea = os.path.realpath(res.group(1))
    # get Athena version and group area
    athenaVer = ''
    groupArea = ''
    cacheVer  = ''
    nightVer  = ''
    cmtConfig = ''
    for line in lines[1:]:
        res = re.search('\(in ([^\)]+)\)',line)
        if res != None:
            items = line.split()
            if items[0] in ('dist','AtlasRelease','AtlasOffline','AtlasAnalysis','AtlasTrigger',
                            'AtlasReconstruction'):
                # Atlas release
                athenaVer = os.path.basename(res.group(1))
                # nightly
                if athenaVer.startswith('rel'):
                    # extract base release
                    tmpMatch = re.search('/([^/]+)(/rel_\d+)*/Atlas[^/]+/rel_\d+',line)
                    if tmpMatch == None:
                        tmpLog.error("unsupported nightly %s" % line)
                        return False,{}
                    # set athenaVer and cacheVer
                    cacheVer  = '-AtlasOffline_%s' % athenaVer
                    athenaVer = tmpMatch.group(1)
                break
            # cache or analysis projects
            elif items[0] in ['AtlasProduction','AtlasPoint1','AtlasTier0','AtlasP1HLT'] or \
                 items[1].count('.') >= 4:  
                # tailside cache is used
                if cacheVer != '':
                    continue
                # production cache
                cacheTag = os.path.basename(res.group(1))
                if items[0] == 'AtlasProduction' and cacheTag.startswith('rel'):
                    # nightlies for cache
                    tmpMatch = re.search('/([^/]+)(/rel_\d+)*/Atlas[^/]+/rel_\d+',line)
                    if tmpMatch == None:
                        tmpLog.error("unsupported nightly %s" % line)
                        return False,{}
                    cacheVer  = '-AtlasOffline_%s' % cacheTag
                    athenaVer = tmpMatch.group(1)
                    break
                else:
                    # doesn't use when it is a base release since it is not installed in EGEE
                    if re.search('^\d+\.\d+\.\d+$',cacheTag) == None:
                        cacheVer = '-%s_%s' % (items[0],cacheTag)
            else:
                # group area
                groupArea = os.path.realpath(res.group(1))
    # cmtconfig
    if os.environ.has_key('CMTCONFIG'):
        cmtConfig = os.environ['CMTCONFIG']
    # pack return values
    retVal = {
        'workArea' : workArea,
        'athenaVer': athenaVer,
        'groupArea': groupArea,
        'cacheVer' : cacheVer,
        'nightVer' : nightVer,
        'cmtConfig': cmtConfig,
           }
    # check error
    if athenaVer == '':
        tmpStr = ''
        for line in lines:
            tmpStr += (line+'\n')
        tmpLog.info('cmt showed\n'+tmpStr)
        tmpLog.error("could not get Athena version. perhaps your requirements file doesn't have ATLAS_TEST_AREA")
        return False,retVal
    # return
    return True,retVal


# wrapper for attribute access
class ConfigAttr(dict):
    # override __getattribute__ for dot access
    def __getattribute__(self,name):
        if name in dict.__dict__.keys():
            return dict.__getattribute__(self,name)
        if name.startswith('__'):
            return dict.__getattribute__(self,name)
        if name in dict.keys(self):
            return dict.__getitem__(self,name)
        return False

    def __setattr__(self,name,value):
        if name in dict.__dict__.keys():
            dict.__setattr__(self,name,value)
        else:
            dict.__setitem__(self,name,value)
                        

# extract run configuration
def extractRunConfig(jobO,supStream,useAIDA,shipinput,trf,verbose=False,useAMI=False,
                     inDS='',tmpDir='.'):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    outputConfig = ConfigAttr()
    inputConfig  = ConfigAttr()
    otherConfig  = ConfigAttr()
    statsCode = True
    if trf:
        pass
    else:
        # use AMI
        amiJobO = ''
        if useAMI:
            amiJobO = getJobOtoUseAmiForAutoConf(inDS,tmpDir)
        baseName = os.environ['PANDA_SYS'] + "/etc/panda/share"
        com = 'athena.py %s %s/FakeAppMgr.py %s %s/ConfigExtractor.py' % \
              (amiJobO,baseName,jobO,baseName)
        if verbose:
            tmpLog.debug(com)
        # run ConfigExtractor for normal jobO
        out = commands.getoutput(com)
        failExtractor = True
        outputConfig['alloutputs'] = []
        skipOutName = False
        for line in out.split('\n'):
            match = re.findall('^ConfigExtractor > (.+)',line)
            if len(match):
                # suppress some streams
                if match[0].startswith("Output="):
                    tmpSt0 = "NoneNoneNone"
                    tmpSt1 = "NoneNoneNone"
                    tmpSt2 = "NoneNoneNone"
                    try:
                        tmpSt0 = match[0].replace('=',' ').split()[1].upper()
                    except:
                        pass
                    try:
                        tmpSt1 = match[0].replace('=',' ').split()[-1].upper()
                    except:
                        pass
                    try:
                        tmpSt2 = match[0].replace('=',' ').split()[2].upper()
                    except:
                        pass
                    toBeSuppressed = False
                    # normal check
                    if tmpSt0 in supStream or tmpSt1 in supStream or tmpSt2 in supStream:
                        toBeSuppressed = True
                    # wild card check
                    if not toBeSuppressed:
                        for tmpPatt in supStream:
                            if '*' in tmpPatt:
                                tmpPatt = '^' + tmpPatt.replace('*','.*')
                                tmpPatt = tmpPatt.upper()
                                if re.search(tmpPatt,tmpSt0) != None or \
                                       re.search(tmpPatt,tmpSt1) != None or \
                                       re.search(tmpPatt,tmpSt2) != None:
                                    toBeSuppressed = True
                                    break
                    # suppressed            
                    if toBeSuppressed:            
                        tmpLog.info('%s is suppressed' % line)
                        # set skipOutName to ignore output filename in the next loop
                        skipOutName = True
                        continue
                failExtractor = False
                # AIDA HIST
                if match[0].startswith('Output=HIST'):
                    if useAIDA:
                        tmpLog.info('%s is suppressed. Please use --useAIDA if needed' % line)
                        continue
                    else:
                        outputConfig['outHist'] = True
                # AIDA NTuple
                if match[0].startswith('Output=NTUPLE'):
                    if useAIDA:
                        tmpLog.info('%s is suppressed. Please use --useAIDA if needed' % line)
                        continue
                    else:
                        if not outputConfig.has_key('outNtuple'):
                            outputConfig['outNtuple'] = []
                        tmpItems = match[0].split()
                        outputConfig['outNtuple'].append(tmpItems[1])
                # RDO
                if match[0].startswith('Output=RDO'):
                    outputConfig['outRDO'] = match[0].split()[1]
                # ESD
                if match[0].startswith('Output=ESD'):
                    outputConfig['outESD'] = match[0].split()[1]
                # AOD
                if match[0].startswith('Output=AOD'):
                    outputConfig['outAOD'] = match[0].split()[1]
                # TAG output
                if match[0]=='Output=TAG':            
                    outputConfig['outTAG'] = True
                # TAGCOM
                if match[0].startswith('Output=TAGX'):
                    if not outputConfig.has_key('outTAGX'):
                        outputConfig['outTAGX'] = []
                    tmpItems = match[0].split()
                    outputConfig['outTAGX'].append(tuple(tmpItems[1:]))
                # AANT
                if match[0].startswith('Output=AANT'):
                    if not outputConfig.has_key('outAANT'):
                        outputConfig['outAANT'] = []
                    tmpItems = match[0].split()
                    outputConfig['outAANT'].append(tuple(tmpItems[1:]))
                # THIST
                if match[0].startswith('Output=THIST'):            
                    if not outputConfig.has_key('outTHIST'):
                        outputConfig['outTHIST'] = []
                    tmpItems = match[0].split()
                    if not tmpItems[1] in outputConfig['outTHIST']:
                        outputConfig['outTHIST'].append(tmpItems[1])
                # IROOT
                if match[0].startswith('Output=IROOT'):            
                    if not outputConfig.has_key('outIROOT'):
                        outputConfig['outIROOT'] = []
                    tmpItems = match[0].split()
                    outputConfig['outIROOT'].append(tmpItems[1])
                # Stream1
                if match[0].startswith('Output=STREAM1'):
                    outputConfig['outStream1'] = match[0].split()[1]
                # Stream2
                if match[0].startswith('Output=STREAM2'):                        
                    outputConfig['outStream2'] = match[0].split()[1]
                # ByteStream output
                if match[0]=='Output=BS':            
                    outputConfig['outBS'] = True
                # General Stream
                if match[0].startswith('Output=STREAMG'):            
                    tmpItems = match[0].split()
                    outputConfig['outStreamG'] = []
                    for tmpNames in tmpItems[1].split(','):
                        outputConfig['outStreamG'].append(tmpNames.split(':'))
                # Metadata
                if match[0].startswith('Output=META'):            
                    if not outputConfig.has_key('outMeta'):
                        outputConfig['outMeta'] = []
                    tmpItems = match[0].split()
                    outputConfig['outMeta'].append(tuple(tmpItems[1:]))
                # UserDataSvc
                if match[0].startswith('Output=USERDATA'):            
                    if not outputConfig.has_key('outUserData'):
                        outputConfig['outUserData'] = []
                    tmpItems = match[0].split()
                    outputConfig['outUserData'].append(tmpItems[-1])
                # MultipleStream
                if match[0].startswith('Output=MS'):
                    if not outputConfig.has_key('outMS'):
                        outputConfig['outMS'] = []
                    tmpItems = match[0].split()            
                    outputConfig['outMS'].append(tuple(tmpItems[1:]))
                # No input
                if match[0]=='No Input':
                    inputConfig['noInput'] = True
                # ByteStream input
                if match[0]=='Input=BS':                        
                    inputConfig['inBS'] = True
                # selected ByteStream
                if match[0].startswith('Output=SelBS'):            
                    tmpItems = match[0].split()
                    inputConfig['outSelBS'] = tmpItems[1]
                # TAG input
                if match[0]=='Input=COLL':                        
                    inputConfig['inColl'] = True
                # POOL references
                if match[0].startswith('Input=COLLREF'):
                    tmpRef = match[0].split()[-1]
                    if tmpRef == 'Input=COLLREF':
                        # use default token when ref is empty
                        tmpRef = 'Token'
                    elif tmpRef != 'Token' and (not tmpRef.endswith('_ref')):
                        # append _ref
                        tmpRef += '_ref'
                    inputConfig['collRefName'] = tmpRef
                # TAG Query
                if match[0].startswith('Input=COLLQUERY'):
                    tmpQuery = re.sub('Input=COLLQUERY','',match[0])
                    tmpQuery = tmpQuery.strip()
                    inputConfig['tagQuery'] = tmpQuery
                # Minimum bias
                if match[0]=='Input=MINBIAS':
                    inputConfig['inMinBias'] = True
                # Cavern input
                if match[0]=='Input=CAVERN':
                    inputConfig['inCavern'] = True
                # Beam halo
                if match[0]=='Input=BEAMHALO':
                    inputConfig['inBeamHalo'] = True
                # Beam gas
                if match[0]=='Input=BEAMGAS':
                    inputConfig['inBeamGas'] = True
                # Back navigation
                if match[0]=='BackNavigation=ON':                        
                    inputConfig['backNavi'] = True
                # Random stream
                if match[0].startswith('RndmStream'):
                    if not otherConfig.has_key('rndmStream'):
                        otherConfig['rndmStream'] = []
                    tmpItems = match[0].split()
                    otherConfig['rndmStream'].append(tmpItems[1])
                # Generator file
                if match[0].startswith('RndmGenFile'):
                    if not otherConfig.has_key('rndmGenFile'):
                        otherConfig['rndmGenFile'] = []
                    tmpItems = match[0].split()
                    otherConfig['rndmGenFile'].append(tmpItems[-1])
                # G4 Random seeds
                if match[0].startswith('G4RandomSeeds'):
                    otherConfig['G4RandomSeeds'] = True
                # input files for direct input
                if match[0].startswith('InputFiles'):
                    if shipinput:
                        tmpItems = match[0].split()
                        otherConfig['inputFiles'] = tmpItems[1:]
                    else:
                        continue
                # condition file
                if match[0].startswith('CondInput'):
                    if not otherConfig.has_key('condInput'):
                        otherConfig['condInput'] = []
                    tmpItems = match[0].split()
                    otherConfig['condInput'].append(tmpItems[-1])
                # collect all outputs
                if match[0].startswith(' Name:'):
                    # skipped output
                    if skipOutName:
                        skipOutName = False
                        continue
                    outputConfig['alloutputs'].append(match[0].split()[-1])
                    continue
                tmpLog.info(line)
                skipOutName = False
        # extractor failed
        if failExtractor:
            print out
            tmpLog.error("Could not parse jobOptions")
            statsCode = False
    # return
    retConfig = ConfigAttr()
    retConfig['input']  = inputConfig
    retConfig['other']  = otherConfig
    retConfig['output'] = outputConfig
    return statsCode,retConfig


# extPoolRefs for old releases which don't contain CollectionTools
athenaStuff = ['extPoolRefs.C']

# jobO files with full path names
fullPathJobOs = {}


# convert fullPathJobOs to str
def convFullPathJobOsToStr():
    tmpStr = ''
    for fullJobO,localName in fullPathJobOs.iteritems():
        tmpStr += '%s:%s,' % (fullJobO,localName)
    tmpStr = tmpStr[:-1]
    return tmpStr


# convert str to fullPathJobOs
def convStrToFullPathJobOs(tmpStr):
    retMap = {}
    for tmpItem in tmpStr.split(','):
        fullJobO,localName = tmpItem.split(':')
        retMap[fullJobO] = localName
    return retMap

        
# copy some athena specific files and full-path jobOs
def copyAthenaStuff(currentDir):
    baseName = os.environ['PANDA_SYS'] + "/etc/panda/share"
    for tmpFile in athenaStuff:
        com = 'cp -p %s/%s %s' % (baseName,tmpFile,currentDir)
        commands.getoutput(com)
    for fullJobO,localName in fullPathJobOs.iteritems():
        com = 'cp -p %s %s/%s' % (fullJobO,currentDir,localName)
        commands.getoutput(com)


# delete some athena specific files and copied jobOs
def deleteAthenaStuff(currentDir):
    for tmpFile in athenaStuff:
        com = 'rm -f %s/%s' % (currentDir,tmpFile)
        commands.getoutput(com)
    for tmpFile in fullPathJobOs.values():
        com = 'rm -f %s/%s' % (currentDir,tmpFile)
        commands.getoutput(com)


# set extFile
extFile = []
def setExtFile(v_extFile):
    global extFile
    extFile = v_extFile


# set excludeFile
excludeFile = []
def setExcludeFile(strExcludeFile):
    # empty
    if strExcludeFile == '':
        return
    # convert to list
    global excludeFile
    for tmpItem in strExcludeFile.split(','):
        # change . to \. for regexp
        tmpItem = tmpItem.replace('.','\.')        
        # change * to .* for regexp
        tmpItem = tmpItem.replace('*','.*')
        # append
        excludeFile.append(tmpItem)
    
                
# matching for extFiles
def matchExtFile(fileName):
    # check exclude files
    for tmpPatt in excludeFile:
        if re.search(tmpPatt,fileName) != None:
            return False
    # gather files with special extensions
    for tmpExtention in ['.py','.dat','.C','.xml','Makefile',
                         '.cc','.cxx','.h','.hh','.sh','.cpp',
                         '.hpp']:
        if fileName.endswith(tmpExtention):
            return True
    # check filename
    baseName = fileName.split('/')[-1]
    for patt in extFile:
        if patt.find('*') == -1:
            # regular matching
            if patt == baseName:
                return True
            # patt may contain / for sub dir
            if patt != '' and re.search(patt+'$',fileName) != None:
                return True
        else:
            # use regex for *
            tmpPatt = patt.replace('*','.*')
            if re.search(tmpPatt,baseName) != None:
                return True
            # patt may contain / for sub dir
            if patt != '' and re.search(tmpPatt+'$',fileName) != None:
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
def getExtendedExtStreamName(sIndex,sName,enableExtension):
    tmpBaseExtName = 'EXT%s' % sIndex
    if not useExtendedExtStreamName or not enableExtension:
        return tmpBaseExtName
    # change * to X and add .tgz
    if sName.find('*') != -1:
        sName = sName.replace('*','XYZ')
        sName = '%s.tgz' % sName
    # use extended extra stream name
    tmpItems = sName.split('.')
    if len(tmpItems) > 0:
        tmpBaseExtName += '_%s' % tmpItems[0]
    return tmpBaseExtName
    

# special files to be treated carefully
specialFilesForAthena = ['dblookup.xml']

# archive source files
def archiveSourceFiles(workArea,runDir,currentDir,tmpDir,verbose,gluePackages=[],dereferenceSymLinks=False):
    # archive sources
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('archiving source files')

    #####################################################################
    # subroutines

    # scan InstallArea to get a list of local packages
    def getFileList(dir,files,forPackage,readLink=True):
        try:
            list = os.listdir(dir)
        except:
            return
        for item in list:
            # skip if doc or .svn
            if item in ['doc','.svn']:
                continue
            fullName=dir+'/'+item
            if os.path.isdir(fullName):
                # ignore symlinked dir just under InstallArea/include
                # they are created for g77
                if os.path.islink(fullName) and re.search('/InstallArea/include$',dir) != None:
                    pass
                elif os.path.islink(fullName) and readLink and forPackage:
                    # resolve symlink
                    getFileList(os.readlink(fullName),files,forPackage,readLink)
                else:
                    getFileList(fullName,files,forPackage,readLink)
            else:
                if os.path.islink(fullName):
                    if readLink:
                        tmpLink = os.readlink(fullName)
                        # put base dir when relative path
                        if not tmpLink.startswith('/'):
                            tmpLink = dir+'/'+tmpLink
                            tmpLink = os.path.abspath(tmpLink)
                        appFileName = tmpLink
                    else:
                        appFileName = os.path.abspath(fullName)                        
                else:
                    appFileName = os.path.abspath(fullName)
                # remove redundant //
                appFilename = re.sub('//','/',appFileName)
                # append
                files.append(appFileName)

    # get package list
    def getPackages(_workArea,gluePackages=[]):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        # special packages
        specialPackages = {'External/Lhapdf':'external/MCGenerators/lhapdf'}
        # get file list
        installFiles = []
        getFileList(_workArea+'/InstallArea',installFiles,True)
        # get list of packages
        cmt_config = os.environ['CMTCONFIG']
        _packages = []
        for iFile in installFiles:
            # ignore InstallArea stuff
            if re.search('/InstallArea/',iFile):
                continue
            # converted to real path
            file = os.path.realpath(iFile)
            # remove special characters
            sString=re.sub('[\+]','.',os.path.realpath(_workArea))
            # look for /share/ , /python/, /i686-slc3-gcc323-opt/, .h
            for target in ('share/','python/',cmt_config+'/','[^/]+\.h'):
                res = re.search(sString+'/(.+)/'+target,file)
                if res:
                    # append
                    pName = res.group(1)
                    if target in ['[^/]+\.h']:
                        # convert PackageDir/PackageName/PackageName to PackageDir/PackageName
                        pName = re.sub('/[^/]+$','',pName)
                    if not pName in _packages:
                        if os.path.isdir(_workArea+'/'+pName):
                            _packages.append(pName)
                    break
            # check special packages just in case
            for pName,pPath in specialPackages.iteritems():
                if not pName in _packages:
                    # look for path pattern
                    if re.search(pPath,file) != None:
                        if os.path.isdir(_workArea+'/'+pName):
                            # check structured style
                            tmpDirList = os.listdir(_workArea+'/'+pName)
                            useSS = False
                            for tmpDir in tmpDirList:
                                if re.search('-\d+-\d+-\d+$',tmpDir) != None:
                                    _packages.append(pName+'/'+tmpDir)
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
            if not pName in _packages:
                if os.path.isdir(_workArea+'/'+pName):
                    # check structured style
                    tmpDirList = os.listdir(_workArea+'/'+pName)
                    useSS = False
                    for tmpDir in tmpDirList:
                        if re.search('-\d+-\d+-\d+$',tmpDir) != None:
                            fullPName = pName+'/'+tmpDir
                            if not fullPName in _packages:
                                _packages.append(fullPName)
                            useSS = True
                            break
                    # normal structure
                    if not useSS:
                        _packages.append(pName)
                else:
                    tmpLog.warning('glue package %s not found under %s' % (pName,_workArea))
        # return 
        return _packages


    # archive files
    def archiveFiles(_workArea,_packages,_archiveFullName):
        excludePattern = '.svn'
        for tmpPatt in excludeFile:
            # reverse regexp change
            tmpPatt = tmpPatt.replace('.*', '*')
            tmpPatt = tmpPatt.replace('\.', '.')
            excludePattern += " --exclude '%s'" % tmpPatt
        _curdir = os.getcwd()
        # change dir
        os.chdir(_workArea)
        for pack in _packages:
            # archive subdirs
            list = os.listdir(pack)
            for item in list:
                # ignore libraries
                if item.startswith('i686') or item.startswith('i386') or item.startswith('x86_64') \
                       or item=='pool' or item =='pool_plugins' or item == 'doc' or item == '.svn':
                    continue
                # check exclude files
                excludeFileFlag = False
                for tmpPatt in excludeFile:
                    if re.search(tmpPatt,'%s/%s' % (pack,item)) != None:
                        excludeFileFlag = True
                        break
                if excludeFileFlag:
                    continue
                # run dir
                if item=='run':
                    files = []
                    getFileList('%s/%s/run' % (_workArea,pack),files,False)
                    # not resolve symlink (appending instead of replacing for backward compatibility)
                    tmpFiles = []
                    getFileList('%s/%s/run' % (_workArea,pack),tmpFiles,False,False)
                    for tmpFile in tmpFiles:
                        if not tmpFile in files:
                            files.append(tmpFile)
                    for iFile in files:
                        # converted to real path
                        file = os.path.realpath(iFile)
                        # archive .py/.dat/.C files only
                        if matchExtFile(file):
                            # remove special characters                    
                            sString=re.sub('[\+]','.',os.path.realpath(_workArea))
                            relPath = re.sub('^%s/' % sString, '', file)
                            # if replace is failed or the file is symlink, try non-converted path names
                            if relPath.startswith('/') or os.path.islink(iFile):
                                sString=re.sub('[\+]','.',workArea)
                                relPath = re.sub(sString+'/','',iFile)
                            if os.path.islink(iFile):
                                cmd = "tar -uh '%s' -f '%s' --exclude '%s'" % (relPath,_archiveFullName,excludePattern)
                                out = commands.getoutput(cmd)
                            else:
                                cmd = "tar uf '%s' '%s' --exclude '%s'" % (_archiveFullName,relPath,excludePattern)
                                out = commands.getoutput(cmd)
                            if verbose:
                                print relPath
                                if out != '':    
                                    print out
                    continue
                # else
                if dereferenceSymLinks:
                    cmd = "tar ufh '%s' '%s/%s' --exclude '%s'" % (_archiveFullName,pack,item,excludePattern)
                else:
                    cmd = "tar uf '%s' '%s/%s' --exclude '%s'" % (_archiveFullName,pack,item,excludePattern)
                out = commands.getoutput(cmd)
                if verbose:
                    print "%s/%s" % (pack,item)
                    if out != '':    
                        print out
        # back to previous dir
        os.chdir(_curdir)

    #####################################################################
    # execute

    # get packages in private area 
    packages = getPackages(workArea,gluePackages)
    # check TestRelease since it doesn't create any links in InstallArea
    if os.path.exists('%s/TestRelease' % workArea):
        # the TestRelease could be created by hand
        packages.append('TestRelease')

    if verbose:
        tmpLog.debug("== private packages ==")
        for pack in packages:
            print pack
        tmpLog.debug("== private files ==")

    # create archive
    archiveName     = 'sources.%s.tar' % MiscUtils.wrappedUuidGen()
    archiveFullName = "%s/%s" % (tmpDir,archiveName)
    # archive private area
    archiveFiles(workArea,packages,archiveFullName)
    # archive current (run) dir
    files = []
    os.chdir(workArea)
    getFileList('%s/%s' % (workArea,runDir),files,False,False)
    for file in files:
        # remove special characters                    
        sString=re.sub('[\+]','.',os.path.realpath(workArea))
        relPath = re.sub(sString+'/','',os.path.realpath(file))
        # if replace is failed or the file is symlink, try non-converted path names
        if relPath.startswith('/') or os.path.islink(file):
            sString=re.sub('[\+]','.',workArea)
            relPath = re.sub(sString+'/','',file)
        # archive .py/.dat/.C/.xml files only
        if not matchExtFile(relPath):
            continue
        # ignore InstallArea
        if relPath.startswith('InstallArea'):
            continue
        # check special files
        spBaseName = relPath
        if re.search('/',spBaseName) != None:
            spBaseName = spBaseName.split('/')[-1]
        if spBaseName in specialFilesForAthena:
            warStr  = '%s in the current dir is sent to remote WNs, which might cause a database problem. ' % spBaseName
            warStr += 'If this is intentional please ignore this WARNING'
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
                out = commands.getoutput("tar -uh '%s' -f '%s'" % (relPath,archiveFullName))                
            else:
                out = commands.getoutput("tar uf '%s' '%s'" % (archiveFullName,relPath))                
            if verbose:
                print relPath
                if out != '':    
                    print out
    # back to current dir            
    os.chdir(currentDir)
    # return
    return archiveName,archiveFullName
    

# archive jobO files
def archiveJobOFiles(workArea,runDir,currentDir,tmpDir,verbose):
    # archive jobO files    
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('archiving jobOs and modules')

    # get real jobOs
    def getJobOs(dir,files):
        list = os.listdir(dir)
        for item in list:
            fullName=dir+'/'+item
            if os.path.isdir(fullName):
                # skip symlinks in include since they cause full scan on releases
                if os.path.islink(fullName) and re.search('InstallArea/include$',dir) != None:
                    continue
                # dir
                getJobOs(fullName,files)
            else:
                # python and other extFiles
                if matchExtFile(fullName):
                    files.append(fullName)

    # get jobOs
    files = []
    os.chdir(workArea)
    getJobOs('%s' % workArea,files)
    # create archive
    archiveName     = 'jobO.%s.tar' % MiscUtils.wrappedUuidGen()
    archiveFullName = "%s/%s" % (tmpDir,archiveName)
    # archive
    if verbose:
        tmpLog.debug("== py files ==")
    for file in files:
        # remove special characters                    
        sString=re.sub('[\+]','.',workArea)
        relPath = re.sub(sString+'/','',file)
        # append
        out = commands.getoutput("tar -uh '%s' -f '%s'" % (relPath,archiveFullName))
        if verbose:
            print relPath
            if out != '':    
                print out
    # return
    return archiveName,archiveFullName


# archive InstallArea
def archiveInstallArea(workArea,groupArea,archiveName,archiveFullName,
                       tmpDir,nobuild,verbose):
    # archive jobO files    
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('archiving InstallArea')

    # get file list
    def getFiles(dir,files,ignoreLib,ignoreSymLink):
        if verbose:
            tmpLog.debug("  getFiles(%s)" % dir)
        try:    
            list = os.listdir(dir)
        except:
            return
        for item in list:
            if ignoreLib and (item.startswith('i686') or item.startswith('i386') or
                              item.startswith('x86_64')):
                continue
            fullName=dir+'/'+item
            if os.path.isdir(fullName):
                # ignore symlinked dir just under InstallArea/include
                if ignoreSymLink and os.path.islink(fullName) and re.search('InstallArea/include$',dir) != None:
                    continue
                # dir
                getFiles(fullName,files,False,ignoreSymLink)
            else:
                files.append(fullName)

    # get cmt files
    def getCMTFiles(dir,files):
        list = os.listdir(dir)
        for item in list:
            fullName=dir+'/'+item
            if os.path.isdir(fullName):
                # dir
                getCMTFiles(fullName,files)
            else:
                if re.search('cmt/requirements$',fullName) != None:
                    files.append(fullName)

    # get files
    areaList = []
    # workArea must be first
    areaList.append(workArea)
    if groupArea != '':
        areaList.append(groupArea)
    # groupArea archive    
    groupFileName = re.sub('^sources','groupArea',archiveName)
    groupFullName = "%s/%s" % (tmpDir,groupFileName)
    allFiles = []    
    for areaName in areaList:    
        # archive
        if verbose:
            tmpLog.debug("== InstallArea under %s ==" % areaName)
        files = []
        cmtFiles = []
        os.chdir(areaName)
        if areaName==workArea:
            if not nobuild:
                # ignore i686 and include for workArea
                getFiles('InstallArea',files,True,True)
            else:
                # ignore include for workArea
                getFiles('InstallArea',files,False,True)
        else:
            # groupArea
            if not os.path.exists('InstallArea'):
                if verbose:
                    print "  Doesn't exist. Skip"
                continue
            getFiles('InstallArea',files,False,False)
            # cmt/requirements is needed for non-release packages
            for itemDir in os.listdir(areaName):
                if itemDir != 'InstallArea' and os.path.isdir(itemDir) and \
                       (not os.path.islink(itemDir)):
                    getCMTFiles(itemDir,cmtFiles)
        # remove special characters                    
        sString=re.sub('[\+]','.',os.path.realpath(areaName))
        # archive files if they are under the area
        for file in files+cmtFiles:
            relPath = re.sub(sString+'/','',os.path.realpath(file))
            # check exclude files
            excludeFileFlag = False
            for tmpPatt in excludeFile:
                if re.search(tmpPatt,relPath) != None:
                    excludeFileFlag = True
                    break
            if excludeFileFlag:
                continue
            if not relPath.startswith('/'):
                # use files in private InstallArea instead of group InstallArea
                if not file in allFiles:
                    # append
                    if file in files:
                        out = commands.getoutput("tar -rh '%s' -f '%s'" % (file,archiveFullName))
                    else:
                        # requirements files
                        out = commands.getoutput("tar -rh '%s' -f '%s'" % (file,groupFullName))
                    allFiles.append(file)
                    if verbose:
                        print file
                        if out != '':    
                            print out
    # append groupArea to sources
    if groupArea != '' and (not nobuild):
        os.chdir(tmpDir)
        if os.path.exists(groupFileName):
            out = commands.getoutput("tar -rh '%s' -f '%s'" % (groupFileName,archiveFullName))
            if out != '':    
                print out
            commands.getoutput('rm -rf %s' % groupFullName)    



# index of output files
indexHIST    = 0
indexRDO     = 0
indexESD     = 0
indexAOD     = 0
indexTAG     = 0
indexStream1 = 0
indexStream2 = 0
indexBS      = 0
indexSelBS   = 0
indexNT      = 0
indexTHIST   = 0
indexAANT    = 0
indexIROOT   = 0
indexEXT     = 0
indexTAGX    = 0
indexStreamG = 0
indexMeta    = 0
indexMS      = 0

# global serial number for LFN
globalSerialnumber = None


# set initial index of outputs
def setInitOutputIndex(runConfig,outDS,individualOutDS,extOutFile,outputIndvDSlist,verbose,descriptionInLFN=''):
    import Client
    # use global
    global indexHIST
    global indexRDO
    global indexESD
    global indexAOD
    global indexTAG
    global indexStream1
    global indexStream2
    global indexBS
    global indexSelBS
    global indexNT
    global indexTHIST
    global indexAANT
    global indexIROOT
    global indexTAGX
    global indexEXT
    global indexStreamG
    global indexMeta
    global indexMS
    global globalSerialnumber
    # get logger
    tmpLog = PLogger.getPandaLogger()

    # remove /
    origOutDS = outDS
    outDS = re.sub('/$','',outDS)
    
    # get maximum index
    def getIndex(list,pattern):
        maxIndex = 0
        for item in list:
            match = re.match(pattern,item)
            if match != None:
                if len(match.groups()) == 1:
                    # old convention : FullDatasetName.XYZ
                    tmpIndex = int(match.group(1))
                else:
                    # new convention : user.nick.XYZ
                    tmpIndex = int(match.group(2))
                if maxIndex < tmpIndex:
                    maxIndex = tmpIndex
                    # set global serial number for LFN
                    if len(match.groups()) == 2:
                        global globalSerialnumber
                        if globalSerialnumber == None or \
                           int(globalSerialnumber.split('_')[-1]) < int(match.group(1).split('_')[-1]):
                            globalSerialnumber = match.group(1)
        return maxIndex

    # get files for individualOutDS
    def getFilesWithSuffix(fileMap,suffix):
        tmpDsName = "%s_%s" % (outDS,suffix)
        if origOutDS.endswith('/'):
            tmpDsName += '/'
        if not outputIndvDSlist.has_key(tmpDsName):
            return
        tmpLog.info("query files in %s" % tmpDsName)
        tmpList = Client.queryFilesInDataset(tmpDsName,verbose)
        for tmpLFN,tmpVal in tmpList.iteritems():
            if not tmpLFN in fileMap:
                fileMap[tmpLFN] = tmpVal
        # query files in dataset from Panda
        status,tmpMap = Client.queryLastFilesInDataset([tmpDsName],verbose)
        for tmpLFN in tmpMap[tmpDsName]:
            if not tmpLFN in tmpList:
                fileMap[tmpLFN] = None

    # query files in dataset from DDM
    tmpLog.info("query files in %s" % origOutDS)
    tmpList = Client.queryFilesInDataset(origOutDS,verbose)
    # query files in dataset from Panda
    status,tmpMap = Client.queryLastFilesInDataset([origOutDS],verbose)
    for tmpLFN in tmpMap[origOutDS]:
        if not tmpLFN in tmpList:
            tmpList[tmpLFN] = None
    # query files for individualOutDS:
    if individualOutDS:
        if runConfig.output.outHist:
            getFilesWithSuffix(tmpList,'HIST')
        if runConfig.output.outRDO:
            getFilesWithSuffix(tmpList,'RDO')
        if runConfig.output.outEDS:
            getFilesWithSuffix(tmpList,'ESD')
        if runConfig.output.outAOD:
            getFilesWithSuffix(tmpList,'AOD')
        if runConfig.output.outTAG:
            getFilesWithSuffix(tmpList,'TAG')
        if runConfig.output.outStream1:
            getFilesWithSuffix(tmpList,'Stream1')
        if runConfig.output.outStream2:
            getFilesWithSuffix(tmpList,'Stream2')
        if runConfig.output.outBS:
            getFilesWithSuffix(tmpList,'BS')
        if runConfig.output.outSelBS:    
            getFilesWithSuffix(tmpList,'SelBS')
        if runConfig.output.outNtuple:    
            for sName in runConfig.output.outNtuple:
                getFilesWithSuffix(tmpList,sName)
        if runConfig.output.outTHIST:
            for sName in runConfig.output.outTHIST:
                getFilesWithSuffix(tmpList,sName)
        if runConfig.output.outAANT:
            for aName,sName,fName in runConfig.output.outAANT:
                getFilesWithSuffix(tmpList,sName)
        if runConfig.output.outIROOT:
            for sIndex,sName in enumerate(runConfig.output.outIROOT):
                getFilesWithSuffix(tmpList,'iROOT%s' % sIndex)
        if runConfig.output.extOutFile:
            for sIndex,sName in enumerate(extOutFile):
                tmpExtStreamName = getExtendedExtStreamName(sIndex,sName,True)
                getFilesWithSuffix(tmpList,tmpExtStreamName)
        if runConfig.output.outStreamG:
            for sName,fName in runConfig.output.outStreamG:
                getFilesWithSuffix(tmpList,sName)                
        if runConfig.output.outMeta:
            iMeta = 0
            for sName,sAsso in runConfig.output.outMeta:
                getFilesWithSuffix(tmpList,'META%s' % iMeta)
                iMeta += 1
        if runConfig.output.outMS:
            for sName,sAsso in runConfig.output.outMS:
                getFilesWithSuffix(tmpList,sName)
                
    # set index
    tmpMatch = re.search('^([^\.]+)\.([^\.]+)\.',outDS)
    if tmpMatch != None and origOutDS.endswith('/'):
        shortPrefix = '^%s\.%s\.([_\d]+)' % (tmpMatch.group(1),tmpMatch.group(2))
        if descriptionInLFN != '':
            shortPrefix += '\.%s' % descriptionInLFN
    else:
        shortPrefix = outDS        
    indexHIST    = getIndex(tmpList,"%s\.hist\._(\d+)\.root" % shortPrefix)
    indexRDO     = getIndex(tmpList,"%s\.RDO\._(\d+)\.pool\.root" % shortPrefix)    
    indexESD     = getIndex(tmpList,"%s\.ESD\._(\d+)\.pool\.root" % shortPrefix)
    indexAOD     = getIndex(tmpList,"%s\.AOD\._(\d+)\.pool\.root" % shortPrefix)
    indexTAG     = getIndex(tmpList,"%s\.TAG\._(\d+)\.coll\.root" % shortPrefix)
    indexStream1 = getIndex(tmpList,"%s\.Stream1\._(\d+)\.pool\.root" % shortPrefix)
    indexStream2 = getIndex(tmpList,"%s\.Stream2\._(\d+)\.pool\.root" % shortPrefix)
    indexBS      = getIndex(tmpList,"%s\.BS\._(\d+)\.data" % shortPrefix)
    if runConfig.output.outSelBS:
        indexSelBS   = getIndex(tmpList,"%s\.%s\._(\d+)\.data" % (shortPrefix,runConfig.output.outSelBS))
    if runConfig.output.outNtuple:
        for sName in runConfig.output.outNtuple:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (shortPrefix,sName))
            if tmpIndex > indexNT:
                indexNT  = tmpIndex
    if runConfig.output.outTHIST:            
        for sName in runConfig.output.outTHIST:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (shortPrefix,sName))
            if tmpIndex > indexTHIST:
                indexTHIST  = tmpIndex
    if runConfig.output.outAANT:            
        for aName,sName,fName in runConfig.output.outAANT:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (shortPrefix,sName))
            if tmpIndex > indexAANT:
                indexAANT  = tmpIndex
    if runConfig.output.outTAGX:            
        for sName,oName in runConfig.output.outTAGX:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.%s" % (shortPrefix,sName,oName))
            if tmpIndex > indexTAGX:
                indexTAGX  = tmpIndex
    if runConfig.output.outIROOT:            
        for sIndex,sName in enumerate(runConfig.output.outIROOT):
            tmpIndex = getIndex(tmpList,"%s\.iROOT%s\._(\d+)\.%s" % (shortPrefix,sIndex,sName))
            if tmpIndex > indexIROOT:
                indexIROOT  = tmpIndex
    if runConfig.output.extOutFile: 
        for sIndex,sName in enumerate(runConfig.output.extOutFile):
            # change * to X and add .tgz
            if sName.find('*') != -1:
                sName = sName.replace('*','XYZ')
                sName = '%s.tgz' % sName
            tmpExtStreamName = getExtendedExtStreamName(sIndex,sName,False)
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.%s" % (shortPrefix,tmpExtStreamName,sName))
            if tmpIndex > indexEXT:
                indexEXT  = tmpIndex
    if runConfig.output.outStreamG:
        for sName,sOrigFileName in runConfig.output.outStreamG:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.pool\.root" % (shortPrefix,sName))
            if tmpIndex > indexStreamG:
                indexStreamG = tmpIndex
    if runConfig.output.outMeta:            
        for sName,sAsso in runConfig.output.outMeta:
            iMeta = 0
            if sAsso == 'None':
                tmpIndex = getIndex(tmpList,"%s\.META%s\._(\d+)\.root" % (shortPrefix,iMeta))
                iMeta += 1
                if tmpIndex > indexMeta:
                    indexMeta = tmpIndex
    if runConfig.output.outMS:                
        for sName,sAsso in runConfig.output.outMS:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.pool\.root" % (shortPrefix,sName))
            if tmpIndex > indexMS:
                indexMS = tmpIndex


# convert runConfig to outMap
def convertConfToOutput(runConfig,extOutFile,original_outDS,destination='',spaceToken=''):
    outMap = {}
    paramList = []
    # add IROOT
    if not outMap.has_key('IROOT'):
        outMap['IROOT'] = []
    # remove /
    outDSwoSlash = re.sub('/$','',original_outDS)
    outDsNameBase = outDSwoSlash
    tmpMatch = re.search('^([^\.]+)\.([^\.]+)\.',original_outDS)
    if tmpMatch != None and original_outDS.endswith('/'):
        outDSwoSlash = '%s.%s.' % (tmpMatch.group(1),tmpMatch.group(2))
        outDSwoSlash += '$JEDITASKID'
    # start conversion
    if runConfig.output.outNtuple:
        for sName in runConfig.output.outNtuple:
            lfn  = '%s.%s._${SN/P}.root' % (outDSwoSlash,sName)
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('ntuple'):
                outMap['ntuple'] = []
            outMap['ntuple'].append((sName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outHist:
        lfn  = '%s.hist._${SN/P}.root' % outDSwoSlash
        tmpSuffix = '_HIST'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['hist'] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outRDO:
        lfn  = '%s.RDO._${SN/P}.pool.root' % outDSwoSlash
        tmpSuffix = '_RDO'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['IROOT'].append((runConfig.output.outRDO,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)        
    if runConfig.output.outESD:
        lfn  = '%s.ESD._${SN/P}.pool.root' % outDSwoSlash
        tmpSuffix = '_ESD'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['IROOT'].append((runConfig.output.outESD,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outAOD:
        lfn  = '%s.AOD._${SN/P}.pool.root' % outDSwoSlash
        tmpSuffix = '_AOD'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['IROOT'].append((runConfig.output.outAOD,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outTAG:
        lfn  = '%s.TAG._${SN/P}.coll.root' % outDSwoSlash
        tmpSuffix = '_TAG'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['TAG'] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outAANT:
        sNameList = []
        fsNameMap = {}
        for aName,sName,fName in runConfig.output.outAANT:
            # use first sName when multiple streams write to the same file
            realStreamName = sName
            if fsNameMap.has_key(fName):
                sName = fsNameMap[fName]
            else:
                fsNameMap[fName] = sName
            lfn  = '%s.%s._${SN/P}.root' % (outDSwoSlash,sName)       
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not sName in sNameList:
                sNameList.append(sName)
            if not outMap.has_key('AANT'):
                outMap['AANT'] = []
            outMap['AANT'].append((aName,realStreamName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outTHIST:
        for sName in runConfig.output.outTHIST:
            lfn  = '%s.%s._${SN/P}.root' % (outDSwoSlash,sName)
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('THIST'):
                outMap['THIST'] = []
            outMap['THIST'].append((sName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outIROOT:
        for sIndex,sName in enumerate(runConfig.output.outIROOT):
            lfn  = '%s.iROOT%s._${SN/P}.%s' % (outDSwoSlash,sIndex,sName)
            tmpSuffix = '_iROOT%s' % sIndex
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((sName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if extOutFile:
        for sIndex,sName in enumerate(extOutFile):
            # change * to X and add .tgz
            origSName = sName
            if sName.find('*') != -1:
                sName = sName.replace('*','XYZ')
                sName = '%s.tgz' % sName
            tmpExtStreamName = getExtendedExtStreamName(sIndex,sName,False)
            lfn  = '%s.%s._${SN/P}.%s' % (outDSwoSlash,tmpExtStreamName,sName)
            tmpSuffix = '_%s' % tmpExtStreamName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((origSName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outTAGX:
        for sName,oName in runConfig.output.outTAGX:
            lfn  = '%s.%s._${SN/P}.%s' % (outDSwoSlash,sName,oName)
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((oName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outStream1:
        lfn  = '%s.Stream1._${SN/P}.pool.root' % outDSwoSlash
        tmpSuffix = '_Stream1'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['IROOT'].append((runConfig.output.outStream1,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)                
    if runConfig.output.outStream2:
        lfn  = '%s.Stream2._${SN/P}.pool.root' % outDSwoSlash
        tmpSuffix = '_Stream2'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['IROOT'].append((runConfig.output.outStream2,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outBS:
        lfn  = '%s.BS._${SN/P}.data' % outDSwoSlash
        tmpSuffix = '_BS'
        dataset = outDsNameBase + tmpSuffix + '/'
        outMap['BS'] = lfn
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outSelBS:
        lfn  = '%s.%s._${SN/P}.data' % (outDSwoSlash,runConfig.output.outSelBS)
        tmpSuffix = '_SelBS'
        dataset = outDsNameBase + tmpSuffix + '/'
        if not outMap.has_key('IROOT'):
            outMap['IROOT'] = []
        outMap['IROOT'].append(('%s.*.data' % runConfig.output.outSelBS,lfn))
        paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outStreamG:
        for sName,sOrigFileName in runConfig.output.outStreamG:
            lfn  = '%s.%s._${SN/P}.pool.root' % (outDSwoSlash,sName)
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            outMap['IROOT'].append((sOrigFileName,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)                
    if runConfig.output.outMeta:
        iMeta = 0
        for sName,sAsso in runConfig.output.outMeta:
            foundLFN = ''
            if sAsso == 'None':
                # non-associated metadata
                lfn  = '%s.META%s._${SN/P}.root' % (outDSwoSlash,iMeta)
                tmpSuffix = '_META%s' % iMeta
                dataset = outDsNameBase + tmpSuffix + '/'
                paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
                iMeta += 1
                foundLFN = lfn
            elif outMap.has_key(sAsso):
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ['StreamESD','StreamAOD']:
                # ESD,AOD
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
                else:
                    # check StreamG when ESD/AOD are not defined as algorithms
                    if outMap.has_key('StreamG'):
                        for tmpStName,tmpLFN in outMap['StreamG']:
                            if tmpStName == sAsso:
                                foundLFN = tmpLFN
            elif sAsso == 'StreamRDO' and outMap.has_key('StreamRDO'):
                # RDO
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if outMap.has_key('StreamG'):
                    for tmpStName,tmpLFN in outMap['StreamG']:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != '':
                if not outMap.has_key('Meta'):
                    outMap['Meta'] = []
                outMap['Meta'].append((sName,foundLFN))
    if runConfig.output.outMS:
        for sName,sAsso in runConfig.output.outMS:
            lfn  = '%s.%s._${SN/P}.pool.root' % (outDSwoSlash,sName)
            tmpSuffix = '_%s' % sName
            dataset = outDsNameBase + tmpSuffix + '/'
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((sAsso,lfn))
            paramList += MiscUtils.makeJediJobParam(lfn,dataset,'output',hidden=True)
    if runConfig.output.outUserData:
        for sAsso in runConfig.output.outUserData:
            # look for associated LFN
            foundLFN = ''            
            if outMap.has_key(sAsso):
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ['StreamRDO','StreamESD','StreamAOD']:
                # RDO,ESD,AOD
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if outMap.has_key('StreamG'):
                    for tmpStName,tmpLFN in outMap['StreamG']:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != '':
                if not outMap.has_key('UserData'):
                    outMap['UserData'] = []
                outMap['UserData'].append(foundLFN)
    # remove IROOT if unnecessary
    if outMap.has_key('IROOT') and outMap['IROOT'] == []:
        del outMap['IROOT'] 
    # set destination
    if destination != '':
        for tmpParam in paramList:
            tmpParam['destination'] = destination
    # set token
    if spaceToken != '':
        for tmpParam in paramList:
            tmpParam['token'] = spaceToken
    # return
    return outMap,paramList



# convert runConfig to outMap
def convertConfToOutputOld(runConfig,jobR,outMap,individualOutDS,extOutFile,original_outDS='',descriptionInLFN=''):
    from taskbuffer.FileSpec import FileSpec    
    # use global to increment index
    global indexHIST
    global indexRDO
    global indexESD
    global indexAOD
    global indexTAG
    global indexTAGX    
    global indexStream1
    global indexStream2
    global indexBS
    global indexSelBS
    global indexNT
    global indexTHIST
    global indexAANT
    global indexIROOT
    global indexEXT
    global indexStreamG
    global indexMeta
    global indexMS
    # add IROOT
    if not outMap.has_key('IROOT'):
        outMap['IROOT'] = []
    # remove /
    outDSwoSlash = re.sub('/$','',original_outDS)
    tmpMatch = re.search('^([^\.]+)\.([^\.]+)\.',original_outDS)
    if tmpMatch != None and original_outDS.endswith('/'):
        outDSwoSlash = '%s.%s.' % (tmpMatch.group(1),tmpMatch.group(2))
        if globalSerialnumber == None:
            if outDSwoSlash.startswith('group'):
                outDSwoSlash += "$GROUPJOBSN_"
            outDSwoSlash += '$JOBSETID'
        else:
            outDSwoSlash += globalSerialnumber
        if descriptionInLFN != '':
            outDSwoSlash += '.%s' % descriptionInLFN
    # start conversion
    if runConfig.output.outNtuple:
        indexNT += 1
        for sName in runConfig.output.outNtuple:
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.root' % (outDSwoSlash,sName,indexNT)                
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexNT)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('ntuple'):
                outMap['ntuple'] = []
            outMap['ntuple'].append((sName,file.lfn))
    if runConfig.output.outHist:
        indexHIST += 1
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.hist._%05d.root' % (outDSwoSlash,indexHIST)            
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.hist._%05d.root' % (jobR.destinationDBlock,indexHIST)
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_HIST'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:                    
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['hist'] = file.lfn
    if runConfig.output.outRDO:
        indexRDO += 1        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.RDO._%05d.pool.root' % (outDSwoSlash,indexRDO)                    
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.RDO._%05d.pool.root' % (jobR.destinationDBlock,indexRDO)        
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_RDO'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['IROOT'].append((runConfig.output.outRDO,file.lfn))        
    if runConfig.output.outESD:
        indexESD += 1        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.ESD._%05d.pool.root' % (outDSwoSlash,indexESD)        
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.ESD._%05d.pool.root' % (jobR.destinationDBlock,indexESD)        
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_ESD'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['IROOT'].append((runConfig.output.outESD,file.lfn))
    if runConfig.output.outAOD:
        indexAOD += 1                
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.AOD._%05d.pool.root' % (outDSwoSlash,indexAOD)        
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.AOD._%05d.pool.root' % (jobR.destinationDBlock,indexAOD)        
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_AOD'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['IROOT'].append((runConfig.output.outAOD,file.lfn))
    if runConfig.output.outTAG:
        indexTAG += 1                        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.TAG._%05d.coll.root' % (outDSwoSlash,indexTAG)                
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.TAG._%05d.coll.root' % (jobR.destinationDBlock,indexTAG)                
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_TAG'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['TAG'] = file.lfn
    if runConfig.output.outAANT:
        indexAANT += 1
        sNameList = []
        fsNameMap = {}
        for aName,sName,fName in runConfig.output.outAANT:
            # use first sName when multiple streams write to the same file
            realStreamName = sName
            if fsNameMap.has_key(fName):
                sName = fsNameMap[fName]
            else:
                fsNameMap[fName] = sName
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.root' % (outDSwoSlash,sName,indexAANT)       
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexAANT)       
                file.dataset = jobR.destinationDBlock        
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            if not sName in sNameList:
                sNameList.append(sName)
                jobR.addFile(file)
            if not outMap.has_key('AANT'):
                outMap['AANT'] = []
            outMap['AANT'].append((aName,realStreamName,file.lfn))
    if runConfig.output.outTHIST:
        indexTHIST += 1
        for sName in runConfig.output.outTHIST:
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.root' % (outDSwoSlash,sName,indexTHIST)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexTHIST)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('THIST'):
                outMap['THIST'] = []
            outMap['THIST'].append((sName,file.lfn))
    if runConfig.output.outIROOT:
        indexIROOT += 1
        for sIndex,sName in enumerate(runConfig.output.outIROOT):
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.iROOT%s._%05d.%s' % (outDSwoSlash,sIndex,indexIROOT,sName)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.iROOT%s._%05d.%s' % (jobR.destinationDBlock,sIndex,indexIROOT,sName)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_iROOT%s' % sIndex
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((sName,file.lfn))
    if extOutFile:
        indexEXT += 1
        for sIndex,sName in enumerate(extOutFile):
            # change * to X and add .tgz
            origSName = sName
            if sName.find('*') != -1:
                sName = sName.replace('*','XYZ')
                sName = '%s.tgz' % sName
            file = FileSpec()
            file.type = 'output'
            tmpExtStreamName = getExtendedExtStreamName(sIndex,sName,False)
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.%s' % (outDSwoSlash,tmpExtStreamName,indexEXT,sName)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.%s' % (jobR.destinationDBlock,tmpExtStreamName,indexEXT,sName)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpExtStreamName = getExtendedExtStreamName(sIndex,sName,True)
                tmpSuffix = '_%s' % tmpExtStreamName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((origSName,file.lfn))
    if runConfig.output.outTAGX:
        indexTAGX += 1
        for sName,oName in runConfig.output.outTAGX:
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.%s' % (outDSwoSlash,sName,indexTAGX,oName)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.%s' % (jobR.destinationDBlock,sName,indexTAGX,oName)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((oName,file.lfn))
    if runConfig.output.outStream1:
        indexStream1 += 1                                        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.Stream1._%05d.pool.root' % (outDSwoSlash,indexStream1)
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.Stream1._%05d.pool.root' % (jobR.destinationDBlock,indexStream1)
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_Stream1'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['IROOT'].append((runConfig.output.outStream1,file.lfn))                
    if runConfig.output.outStream2:
        indexStream2 += 1                                        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.Stream2._%05d.pool.root' % (outDSwoSlash,indexStream2)
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.Stream2._%05d.pool.root' % (jobR.destinationDBlock,indexStream2)
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_Stream2'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['IROOT'].append((runConfig.output.outStream2,file.lfn))
    if runConfig.output.outBS:
        indexBS += 1                                        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.BS._%05d.data' % (outDSwoSlash,indexBS)
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.BS._%05d.data' % (jobR.destinationDBlock,indexBS)
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_BS'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['BS'] = file.lfn
    if runConfig.output.outSelBS:
        indexSelBS += 1                                        
        file = FileSpec()
        file.type = 'output'
        if original_outDS.endswith('/'):
            file.lfn  = '%s.%s._%05d.data' % (outDSwoSlash,runConfig.output.outSelBS,indexSelBS)
            file.dataset = original_outDS
        else:
            file.lfn  = '%s.%s._%05d.data' % (jobR.destinationDBlock,runConfig.output.outSelBS,indexSelBS)
            file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_SelBS'
            if original_outDS.endswith('/'):
                file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
            else:
                file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        if not outMap.has_key('IROOT'):
            outMap['IROOT'] = []
        outMap['IROOT'].append(('%s.*.data' % runConfig.output.outSelBS,file.lfn))
    if runConfig.output.outStreamG:
        indexStreamG += 1
        for sName,sOrigFileName in runConfig.output.outStreamG:
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.pool.root' % (outDSwoSlash,sName,indexStreamG)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.pool.root' % (jobR.destinationDBlock,sName,indexStreamG)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            outMap['IROOT'].append((sOrigFileName,file.lfn))                
    if runConfig.output.outMeta:
        iMeta = 0
	indexMeta += 1
        for sName,sAsso in runConfig.output.outMeta:
            foundLFN = ''
            if sAsso == 'None':
                # non-associated metadata
                file = FileSpec()
                file.type = 'output'
                if original_outDS.endswith('/'):
                    file.lfn  = '%s.META%s._%05d.root' % (outDSwoSlash,iMeta,indexMeta)
                    file.dataset = original_outDS
                else:
                    file.lfn  = '%s.META%s._%05d.root' % (jobR.destinationDBlock,iMeta,indexMeta)
                    file.dataset = jobR.destinationDBlock
                file.destinationDBlock = jobR.destinationDBlock
                if individualOutDS:
                    tmpSuffix = '_META%s' % iMeta
                    if original_outDS.endswith('/'):
                        file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                    else:
                        file.dataset += tmpSuffix
                    file.destinationDBlock += tmpSuffix
                file.destinationSE = jobR.destinationSE
                jobR.addFile(file)
                iMeta += 1
                foundLFN = file.lfn
            elif outMap.has_key(sAsso):
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ['StreamESD','StreamAOD']:
                # ESD,AOD
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
                else:
                    # check StreamG when ESD/AOD are not defined as algorithms
                    if outMap.has_key('StreamG'):
                        for tmpStName,tmpLFN in outMap['StreamG']:
                            if tmpStName == sAsso:
                                foundLFN = tmpLFN
            elif sAsso == 'StreamRDO' and outMap.has_key('StreamRDO'):
                # RDO
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if outMap.has_key('StreamG'):
                    for tmpStName,tmpLFN in outMap['StreamG']:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != '':
                if not outMap.has_key('Meta'):
                    outMap['Meta'] = []
                outMap['Meta'].append((sName,foundLFN))
    if runConfig.output.outMS:
	indexMS += 1
        for sName,sAsso in runConfig.output.outMS:
            file = FileSpec()
            file.type = 'output'
            if original_outDS.endswith('/'):
                file.lfn  = '%s.%s._%05d.pool.root' % (outDSwoSlash,sName,indexMS)
                file.dataset = original_outDS
            else:
                file.lfn  = '%s.%s._%05d.pool.root' % (jobR.destinationDBlock,sName,indexMS)
                file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                if original_outDS.endswith('/'):
                    file.dataset = re.sub('/$','%s/' % tmpSuffix,file.dataset)
                else:
                    file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((sAsso,file.lfn))
    if runConfig.output.outUserData:
        for sAsso in runConfig.output.outUserData:
            # look for associated LFN
            foundLFN = ''            
            if outMap.has_key(sAsso):
                # Stream1,2
                foundLFN = outMap[sAsso]
            elif sAsso in ['StreamRDO','StreamESD','StreamAOD']:
                # RDO,ESD,AOD
                stKey = re.sub('^Stream','',sAsso)
                if outMap.has_key(stKey):
                    foundLFN = outMap[stKey]
            else:
                # general stream
                if outMap.has_key('StreamG'):
                    for tmpStName,tmpLFN in outMap['StreamG']:
                        if tmpStName == sAsso:
                            foundLFN = tmpLFN
            if foundLFN != '':
                if not outMap.has_key('UserData'):
                    outMap['UserData'] = []
                outMap['UserData'].append(foundLFN)
    # log
    file = FileSpec()
    if original_outDS.endswith('/'):
        file.lfn  = '%s._$PANDAID.log.tgz' % outDSwoSlash
    else:
        file.lfn  = '%s._$PANDAID.log.tgz' % jobR.destinationDBlock
    file.type = 'log'
    if original_outDS.endswith('/'):
        file.dataset = original_outDS
    else:
        file.dataset = jobR.destinationDBlock    
    file.destinationDBlock = jobR.destinationDBlock
    if individualOutDS:
        # use original outDS for log, which guarantees location registration and shadow tracing
        pass
    file.destinationSE = jobR.destinationSE
    jobR.addFile(file)
    # remove IROOT if unnecessary
    if outMap.has_key('IROOT') and outMap['IROOT'] == []:
        del outMap['IROOT'] 



# get CMTCONFIG
def getCmtConfig(athenaVer=None,cacheVer=None,nightVer=None,cmtConfig=None,verbose=False):
    # use user-specified cmtconfig
    if cmtConfig != None:
        return cmtConfig
    # nightlies
    if cacheVer != None and re.search('_rel_\d+$',cacheVer) != None:
        # use local cmtconfig if it is available
        if os.environ.has_key('CMTCONFIG'):
            return os.environ['CMTCONFIG']
        # get cmtconfig for nightlies
        if athenaVer != None:
            # remove prefix
            verStr = re.sub('^[^-]+-','',athenaVer)
            # dev nightlies
            if verStr in ['dev','devval']:
                return 'x86_64-slc6-gcc47-opt'
            # extract version numbers
            match = re.search('(\d+)\.([^\.+])\.',verStr)
            # major,miner
            maVer = int(match.group(1))
            miVer = match.group(2)
            # use x86_64-slc5-gcc43-opt for 17.X.0 or higher                
            if maVer > 17 or (maVer == 17 and miVer == 'X'):
                return 'x86_64-slc5-gcc43-opt'
            # use i686-slc5-gcc43-opt by default
            return 'i686-slc5-gcc43-opt'
    # get default cmtconfig according to Atlas release
    if athenaVer != None:
        # remove prefix
        verStr = re.sub('^[^-]+-','',athenaVer)
        # get cmtconfig list
        cmtConfigList = Client.getCmtConfigList(athenaVer,verbose)
        if len(cmtConfigList) == 1:
            # no choice
            return cmtConfigList[0]
        elif len(cmtConfigList) > 1:
            # use local cmtconfig if it is available
            if os.environ['CMTCONFIG'] in cmtConfigList:
                return os.environ['CMTCONFIG']
            # use the latest one
            cmtConfigList.sort()
            return cmtConfigList[-1]
        # extract version numbers
        match = re.search('(\d+)\.(\d+)\.(\d+)',verStr)
        if match == None:
            return None
        # major,miner,rev
        maVer = int(match.group(1))
        miVer = int(match.group(2))
        reVer = int(match.group(3))
        # use x86_64-slc5-gcc43-opt for 17.5.0 or higher
        if maVer > 17 or (maVer == 17 and miVer > 5) or (maVer == 17 and miVer == 5 and reVer >= 0):
            return 'x86_64-slc5-gcc43-opt'
        # use i686-slc5-gcc43-opt for 15.6.3 or higher
        if maVer > 15 or (maVer == 15 and miVer > 6) or (maVer == 15 and miVer == 6 and reVer >= 3):
            return 'i686-slc5-gcc43-opt'
        # use i686-slc4-gcc34-opt by default
        return 'i686-slc4-gcc34-opt'
    return None
    

# check CMTCONFIG
def checkCmtConfig(localCmtConfig,userCmtConfig,noBuild):
    # didn't specify CMTCONFIG
    if userCmtConfig in ['',None]:
        return True
    # CVMFS version format
    if re.search('-gcc\d+\.\d+$',userCmtConfig) != None:
        return True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check if valid cmtconfig
    validCmtCofnigList = ['i686-slc4-gcc34-opt','i686-slc5-gcc43-opt','x86_64-slc5-gcc43-opt','x86_64-slc6-gcc46-opt',
                          'x86_64-slc6-gcc47-opt','x86_64-slc6-gcc48-opt']
    if not userCmtConfig in validCmtCofnigList:
        errStr = '%s is not a valid CMTCONFIG distributed on the grid. The following CMTCONFIGs are allowed:\n' % userCmtConfig
        for tmpC in validCmtCofnigList:
            errStr += '   %s\n' % tmpC
        errStr = errStr[:-1]
        tmpLog.error(errStr)
        return False
    # CMTCONFIG is undefined locally
    if localCmtConfig in ['',None]:
        return True
    # user-specified CMTCONFIG is inconsitent with local CMTCONFIG
    if userCmtConfig != localCmtConfig and noBuild:
        errStr  = 'You cannot use --noBuild when --cmtConfig=%s is inconsistent with local CMTCONFIG=%s ' % (userCmtConfig,localCmtConfig)
        errStr += 'since you need re-compile source files on remote worker-node. Please remove --noBuild'
        tmpLog.error(errStr)
        return False
    # return OK
    return True
            

# check configuration tag
def checkConfigTag(oldDSs,newDS):
    try:
        extPatt = '([a-zA-Z]+)(\d+)(_)*([a-zA-Z]+)*(\d+)*'
        # extract new tag 
        newTag = newDS.split('.')[5]
        matchN = re.search(extPatt,newTag)
        # loop over all DSs
        for oldDS in oldDSs:
            # extract old tag
            oldTag = oldDS.split('.')[5]
            matchO = re.search(extPatt,oldTag)
            # check tag consistency beforehand
            if matchO.group(1) != matchN.group(1):
                return None
            if matchO.group(4) != matchN.group(4):
                return None
        # use the first DS since they have the same tag        
        oldTag = oldDSs[0].split('.')[5]
        matchO = re.search(extPatt,oldTag)
        # check version
        verO = int(matchO.group(2))
        verN = int(matchN.group(2))
        if verO > verN:
            return False
        if verO < verN:
            return True
        # check next tag
        if matchO.group(3) == None:
            # no next tag 
            return None
        # check version
        verO = int(matchO.group(5))
        verN = int(matchN.group(5))
        if verO > verN:
            return False
        if verO < verN:
            return True
        # same tag
        return None
    except:
        return None


# convert GoodRunListXML to datasets
def convertGoodRunListXMLtoDS(goodRunListXML,goodRunDataType='',goodRunProdStep='',
                              goodRunListDS='',verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info('trying to convert GoodRunListXML to list of datasets')  
    # import pyAMI
    try:
        from pyAMI import pyAMI
    except:
        try:
            from pyAMI.client import AMIClient
        except:
            errType,errValue = sys.exc_info()[:2]
            print "%s %s" % (errType,errValue)
            tmpLog.error('cannot import pyAMI module')
            return False,'',''
    # read XML
    try:
        gl_xml = open(goodRunListXML)
    except:
        tmpLog.error('cannot open %s' % goodRunListXML)
        return False,'',''
    # parse XML to get run/lumi
    runLumiMap = {}
    import xml.dom.minidom
    rootDOM = xml.dom.minidom.parse(goodRunListXML)
    for tmpLumiBlock in rootDOM.getElementsByTagName('LumiBlockCollection'):
        for tmpRunNode in tmpLumiBlock.getElementsByTagName('Run'):
            tmpRunNum  = long(tmpRunNode.firstChild.data)
            for tmpLBRange in tmpLumiBlock.getElementsByTagName('LBRange'):
                tmpLBStart = long(tmpLBRange.getAttribute('Start'))
                tmpLBEnd   = long(tmpLBRange.getAttribute('End'))        
                # append
                if not runLumiMap.has_key(tmpRunNum):
                    runLumiMap[tmpRunNum] = []
                runLumiMap[tmpRunNum].append((tmpLBStart,tmpLBEnd))
    # make arguments
    amiArgv = []
    amiArgv.append("GetGoodDatasetList")
    amiArgv.append("goodRunList="+gl_xml.read())
    gl_xml.close()
    if goodRunDataType != '':
        amiArgv.append('dataType=%s' % goodRunDataType)
    if goodRunProdStep != '':    
        amiArgv.append('prodStep=%s' % goodRunProdStep)
    if verbose:
        tmpLog.debug(amiArgv)
    # convert for wildcard
    goodRunListDS = goodRunListDS.replace('*','.*')
    # list of datasets
    if goodRunListDS == '':
        goodRunListDS = []
    else:
        goodRunListDS = goodRunListDS.split(',')
    # execute
    try:
        amiclient=pyAMI.AMI()
        amiOut = amiclient.execute(amiArgv)
    except:
        try:
            amiclient = AMIClient()
            amiOut = amiclient.execute(amiArgv)
        except:
            errType,errValue = sys.exc_info()[:2]
            tmpLog.error("%s %s" % (errType,errValue))
            tmpLog.error('pyAMI failed')
            return False,'',''
    # get dataset map
    try:
        amiOutDict = amiOut.getDict()
    except:
        amiOutDict = amiOut.to_dict()
    if verbose:
        tmpLog.debug(amiOutDict)
    if not amiOutDict.has_key('goodDatasetList'):
        tmpLog.error("output from pyAMI doesn't contain goodDatasetList")
        return False,'',''
    amiDsDict = amiOutDict['goodDatasetList']
    # parse
    import Client    
    datasetMapFromAMI = {}
    for tmpKey,tmpVal in amiDsDict.iteritems():
        if tmpVal.has_key('logicalDatasetName'):
            dsName = str(tmpVal['logicalDatasetName'])
            runNumber = long(tmpVal['runNumber'])
            # check dataset names
            if goodRunListDS == []:    
                matchFlag = True
            else:
                matchFlag = False
                for tmpPatt in goodRunListDS:
                    if re.search(tmpPatt,dsName) != None:
                        matchFlag = True
            if not matchFlag:
                continue
            # check with DQ2 since AMI doesn't store /
            dsmap = {}
            try:
                dsmap = Client.getDatasets(dsName,verbose,True,onlyNames=True)
            except:
                pass
            if not dsmap.has_key(dsName):
                dsName += '/'
            # check duplication for the run number
            if matchFlag:
                newFlag = True
                if datasetMapFromAMI.has_key(runNumber):
                    # check configuration tag to use new one
                    newConfigTag = checkConfigTag(datasetMapFromAMI[runNumber],
                                                  dsName)
                    if newConfigTag == True:
                        del datasetMapFromAMI[runNumber]
                    elif newConfigTag == False:
                        # keep existing one
                        newFlag = False
                # append        
                if newFlag:
                    if not datasetMapFromAMI.has_key(runNumber):
                        datasetMapFromAMI[runNumber] = []
                    datasetMapFromAMI[runNumber].append(dsName)
    # make string
    amiRunNumList = datasetMapFromAMI.keys()
    amiRunNumList.sort()
    datasets = ''
    filesStr = []
    for tmpRunNum in amiRunNumList:
        datasetListFromAMI = datasetMapFromAMI[tmpRunNum]
        for dsName in datasetListFromAMI:
            datasets += '%s,' % dsName
            # get files in the dataset
            tmpFilesStr = []
            tmpFileList = Client.queryFilesInDataset(dsName,verbose)
            tmpLFNList = tmpFileList.keys()
            tmpLFNList.sort()
            for tmpLFN in tmpLFNList:
                # extract LBs
                tmpItems = tmpLFN.split('.')
                # sort format
                if len(tmpItems) < 7:
                    tmpFilesStr.append(tmpLFN)
                    continue
                tmpLBmatch = re.search('_lb(\d+)-lb(\d+)',tmpLFN)
                # _lbXXX-lbYYY not found
                if tmpLBmatch != None:
                    LBstart_LFN = long(tmpLBmatch.group(1))
                    LBend_LFN   = long(tmpLBmatch.group(2))
                else:
                    # try ._lbXYZ.
                    tmpLBmatch = re.search('\._lb(\d+)\.',tmpLFN)
                    if tmpLBmatch != None:
                        LBstart_LFN = long(tmpLBmatch.group(1))
                        LBend_LFN   = LBstart_LFN
                    else:
                        tmpFilesStr.append(tmpLFN)                    
                        continue
                # check range
                if not runLumiMap.has_key(tmpRunNum):
                    tmpLog.error('AMI gives a wrong run number (%s) which is not contained in %s' % \
                                 (tmpRunNum,goodRunListXML))
                    return False,'',''
                inRange = False
                for LBstartXML,LBendXML in runLumiMap[tmpRunNum]:
                    if (LBstart_LFN >= LBstartXML and LBstart_LFN <= LBendXML) or \
                       (LBend_LFN >= LBstartXML and LBend_LFN <= LBendXML) or \
                       (LBstart_LFN >= LBstartXML and LBend_LFN <= LBendXML) or \
                       (LBstart_LFN <= LBstartXML and LBend_LFN >= LBendXML):
                        inRange = True
                        break
                if inRange:
                    tmpFilesStr.append(tmpLFN)
            # check if files are found
            if tmpFilesStr == '':
                tmpLog.error('found no files with corresponding LBs in %s' % dsName)
                return False,'',''
            filesStr += tmpFilesStr    
    datasets = datasets[:-1]
    if verbose:
        tmpLog.debug('converted to DS:%s LFN:%s' % (datasets,str(filesStr)))
    # return        
    return True,datasets,filesStr
        


# use AMI for AutoConf
def getJobOtoUseAmiForAutoConf(inDS,tmpDir):
    # no input
    if inDS == '':
        return ''
    # use first one
    amiURL = 'ami://%s' % inDS.split(',')[0]
    # remove /
    if amiURL.endswith('/'):
        amiURL = amiURL[:-1]
    inputFiles = [amiURL]
    # create jobO fragment
    optFileName = tmpDir + '/' + MiscUtils.wrappedUuidGen() + '.py'
    oFile = open(optFileName,'w')
    oFile.write("""
try:
    import AthenaCommon.AthenaCommonFlags
    
    def _dummyFilesInput(*argv):
        return %s

    AthenaCommon.AthenaCommonFlags.FilesInput.__call__ = _dummyFilesInput
except:
    pass

try:
    import AthenaCommon.AthenaCommonFlags
    
    def _dummyGet_Value(*argv):
        return %s

    for tmpAttr in dir (AthenaCommon.AthenaCommonFlags):
        import re
        if re.search('^(Pool|BS).*Input$',tmpAttr) != None:
            try:
                getattr(AthenaCommon.AthenaCommonFlags,tmpAttr).get_Value = _dummyGet_Value
            except:
                pass
except:
    pass
""" % (inputFiles,inputFiles))
    oFile.close()
    # reutrn file name
    return optFileName


# convert GoodRunListXML to datasets
def listFilesUsingAMI(inDsStr,verbose):
    retMetadataMap = {}
    # get logger
    tmpLog = PLogger.getPandaLogger()
    if verbose:
        tmpLog.debug('trying to get file metadata from AMI for %s' % inDsStr) 
    # import pyAMI
    try:
        from pyAMI import pyAMI
    except:
        try:
            from pyAMI.client import AMIClient
        except:
            errType,errValue = sys.exc_info()[:2]
            print "%s %s" % (errType,errValue)
            tmpLog.error('cannot import pyAMI module')
            sys.exit(EC_Config)
    # loop over all datasets
    for inDS in inDsStr.split(','):
        # make arguments
        tmpInDS = re.sub('/$','',inDS)
        nLookUp = 1000
        iLookUp = 0
        while True:
            amiArgv = ["ListFiles"]
            amiArgv.append("logicalDatasetName="+tmpInDS)
            amiArgv.append("limit=%s,%s" % (iLookUp,iLookUp+nLookUp))
            if verbose:
                tmpLog.debug(amiArgv)
            try:
                amiclient=pyAMI.AMI()
                amiOut = amiclient.execute(amiArgv)
            except:
                try:
                    amiclient = AMIClient()
                    amiOut = amiclient.execute(amiArgv)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    tmpLog.error("%s %s" % (errType,errValue))
                    errStr  = 'pyAMI failed. '
                    errStr += 'If %s is not an official dataset, ' % inDS
                    errStr += 'please manually set --nEventsPerFile '
                    errStr += 'since metadata is unavailable in AMI'
                    tmpLog.error(errStr)
                    sys.exit(EC_Config)
            # get file metadata
            try:
                amiOutDict = amiOut.getDict()
            except:
                amiOutDict = amiOut.to_dict()
            if verbose:
                tmpLog.debug(amiOutDict)
            # no more files
            if amiOutDict == {}:
                break
            for tmpEleKey,tmpEleVal in amiOutDict.iteritems():
                for tmpRowKey,tmpRowVal in tmpEleVal.iteritems():
                    tmpLFN = str(tmpRowVal['LFN'])
                    tmpNumEvents = long(tmpRowVal['events'])
                    # append
                    retMetadataMap[tmpLFN] = {'nEvents':tmpNumEvents}
            # increment
            iLookUp += nLookUp
    if verbose:
        tmpLog.debug('metadata from AMI : %s' % str(retMetadataMap))
    # return        
    return retMetadataMap


# use TAG in TRF 
def checkUseTagInTrf(jobO,useTagInTRF):
    if 'inputTAGFile=' in jobO:
        return True
    if useTagInTRF:
        return True
    return False
