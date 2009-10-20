import os
import re
import sys
import commands

import PLogger

# replace parameter with compact LFNs
def replaceParam(patt,inList,tmpJobO):
    # remove attempt numbers
    compactLFNs = []
    for tmpLFN in inList:
        compactLFNs.append(re.sub('\.\d+$','',tmpLFN))
    # sort
    compactLFNs.sort()
    # replace parameters
    if len(compactLFNs) < 2:
        # replace for single input
        tmpJobO = tmpJobO.replace(patt,compactLFNs[0])
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
                refs[guid] = inputColl
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
        tmpDir = 'cmttmp.%s' % commands.getoutput('uuidgen')
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
                   if re.search('/bugfix',line) != None:
                      nightVer  = '/bugfix'
                   elif re.search('/dev',line) != None:
                      nightVer  = '/dev'
                   else:
                      tmpLog.error("unsupported nightly %s" % line)
                      return False,{}
                break
            elif items[0] in ['AtlasProduction','AtlasPoint1','AtlasTier0','AtlasP1HLT']:
                # production cache
                cacheTag = os.path.basename(res.group(1))
                # doesn't use when it is a base release since it is not installed in EGEE
                if re.search('^\d+\.\d+\.\d+$',cacheTag) == None:
                    cacheVer = '-%s_%s' % (items[0],cacheTag)
            else:
                # group area
                groupArea = os.path.realpath(res.group(1))
    # pack return values
    retVal = {
        'workArea' : workArea,
        'athenaVer': athenaVer,
        'groupArea': groupArea,
        'cacheVer' : cacheVer,
        'nightVer' : nightVer,
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
def extractRunConfig(jobO,supStream,useAIDA,shipinput,trf):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    outputConfig = ConfigAttr()
    inputConfig  = ConfigAttr()
    otherConfig  = ConfigAttr()
    statsCode = True
    if trf:
        pass
    else:
        baseName = os.environ['PANDA_SYS'] + "/etc/panda/share"
        com = 'athena.py %s/FakeAppMgr.py %s %s/ConfigExtractor.py' % \
              (baseName,jobO,baseName)          
        # run ConfigExtractor for normal jobO
        out = commands.getoutput(com)
        failExtractor = True
        outputConfig['alloutputs'] = []
        for line in out.split('\n'):
            match = re.findall('^ConfigExtractor > (.+)',line)
            if len(match):
                # suppress some streams
                if match[0].startswith("Output="):
                    tmpSt = match[0].replace('=',' ').split()[-1]
                    if tmpSt.upper() in supStream:
                        tmpLog.info('%s is suppressed' % line)
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
                if match[0]=='Output=RDO':
                    outputConfig['outRDO'] = True
                # ESD
                if match[0]=='Output=ESD':
                    outputConfig['outESD'] = True
                # AOD
                if match[0]=='Output=AOD':
                    outputConfig['outAOD'] = True
                # TAG output
                if match[0]=='Output=TAG':            
                    outputConfig['outTAG'] = True
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
                    outputConfig['outTHIST'].append(tmpItems[1])
                # IROOT
                if match[0].startswith('Output=IROOT'):            
                    if not outputConfig.has_key('outIROOT'):
                        outputConfig['outIROOT'] = []
                    tmpItems = match[0].split()
                    outputConfig['outIROOT'].append(tmpItems[1])
                # Stream1
                if match[0].startswith('Output=STREAM1'):
                    outputConfig['outStream1'] = True
                # Stream2
                if match[0]=='Output=STREAM2':                        
                    outputConfig['outStream2'] = True
                # ByteStream output
                if match[0]=='Output=BS':            
                    outputConfig['outBS'] = True
                # General Stream
                if match[0].startswith('Output=STREAMG'):            
                    tmpItems = match[0].split()
                    outputConfig['outStreamG'] = tmpItems[1].split(',')
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
                    outputConfig['alloutputs'].append(match[0].split()[-1])
                    continue
                tmpLog.info(line)
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

# copy some athena specific files and full-path jobOs
def copyAthenaStuff(currentDir):
    baseName = os.environ['PANDA_SYS'] + "/etc/panda/share"
    for tmpFile in athenaStuff:
        com = 'cp %s/%s %s' % (baseName,tmpFile,currentDir)
        commands.getoutput(com)
    for fullJobO,localName in fullPathJobOs.iteritems():
        com = 'cp %s %s/%s' % (fullJobO,currentDir,localName)
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

# matching for extFiles
def matchExtFile(fileName):
    # gather files with special extensions
    for tmpExtention in ['.py','.dat','.C','.xml','Makefile',
                         '.cc','.cxx','.h','.hh','.sh']:
        if fileName.endswith(tmpExtention):
            return True
    # check filename
    baseName = fileName.split('/')[-1]
    for patt in extFile:
        if patt.find('*') == -1:
            # regular matching
            if patt == baseName:
                return True
        else:
            # use regex for *
            tmpPatt = patt.replace('*','.*')
            if re.search(tmpPatt,baseName) != None:
                return True
    # not matched
    return False


# special files to be treated carefully
specialFilesForAthena = ['dblookup.xml']

# archive source files
def archiveSourceFiles(workArea,runDir,currentDir,tmpDir,verbose,gluePackages=[]):
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
        _curdir = os.getcwd()
        # change dir
        os.chdir(_workArea)
        for pack in _packages:
            # archive subdirs
            list = os.listdir(pack)
            for item in list:
                # ignore libraries
                if item.startswith('i686') or item.startswith('i386') or item.startswith('x86_64') \
                       or item=='dict' or item=='pool' or item =='pool_plugins':
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
                                out = commands.getoutput('tar -rh %s -f %s' % (relPath,_archiveFullName))                
                            else:
                                out = commands.getoutput('tar rf %s %s' % (_archiveFullName,relPath))                
                            if verbose:
                                print relPath
                                if out != '':    
                                    print out
                    continue
                # else
                out = commands.getoutput('tar rf %s %s/%s' % (_archiveFullName,pack,item))
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
    archiveName     = 'sources.%s.tar' % commands.getoutput('uuidgen')
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
                out = commands.getoutput('tar -rh %s -f %s' % (relPath,archiveFullName))                
            else:
                out = commands.getoutput('tar rf %s %s' % (archiveFullName,relPath))                
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
    archiveName     = 'jobO.%s.tar' % commands.getoutput('uuidgen')
    archiveFullName = "%s/%s" % (tmpDir,archiveName)
    # archive
    if verbose:
        tmpLog.debug("== py files ==")
    for file in files:
        # remove special characters                    
        sString=re.sub('[\+]','.',workArea)
        relPath = re.sub(sString+'/','',file)
        # append
        out = commands.getoutput('tar -rh %s -f %s' % (relPath,archiveFullName))
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
            if not relPath.startswith('/'):
                # use files in private InstallArea instead of group InstallArea
                if not file in allFiles:
                    # append
                    if file in files:
                        out = commands.getoutput('tar -rh %s -f %s' % (file,archiveFullName))
                    else:
                        # requirements files
                        out = commands.getoutput('tar -rh %s -f %s' % (file,groupFullName))
                    allFiles.append(file)
                    if verbose:
                        print file
                        if out != '':    
                            print out
    # append groupArea to sources
    if groupArea != '' and (not nobuild):
        os.chdir(tmpDir)
        if os.path.exists(groupFileName):
            out = commands.getoutput('tar -rh %s -f %s' % (groupFileName,archiveFullName))
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
indexStreamG = 0
indexMeta    = 0
indexMS      = 0


# set initial index of outputs
def setInitOutputIndex(runConfig,outDS,individualOutDS,extOutFile,outputIndvDSlist,verbose):
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
    global indexEXT
    global indexStreamG
    global indexMeta
    global indexMS
    # get logger
    tmpLog = PLogger.getPandaLogger()

    # get maximum index
    def getIndex(list,pattern):
        maxIndex = 0
        for item in list:
            match = re.match(pattern,item)
            if match != None:
                tmpIndex = int(match.group(1))
                if maxIndex < tmpIndex:
                    maxIndex = tmpIndex
        return maxIndex

    # get files for individualOutDS
    def getFilesWithSuffix(fileMap,suffix):
        tmpDsName = "%s_%s" % (outDS,suffix)
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
    tmpLog.info("query files in %s" % outDS)
    tmpList = Client.queryFilesInDataset(outDS,verbose)
    # query files in dataset from Panda
    status,tmpMap = Client.queryLastFilesInDataset([outDS],verbose)
    for tmpLFN in tmpMap[outDS]:
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
            for sName in runConfig.output.outAANT:
                getFilesWithSuffix(tmpList,sName)
        if runConfig.output.outIROOT:
            for sIndex,sName in enumerate(runConfig.output.outIROOT):
                getFilesWithSuffix(tmpList,'iROOT%s' % sIndex)
        if runConfig.output.extOutFile:
            for sIndex,sName in enumerate(extOutFile):
                getFilesWithSuffix(tmpList,'EXT%s' % sIndex)
        if runConfig.output.outStreamG:
            for sName in runConfig.output.outStreamG:
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
    indexHIST    = getIndex(tmpList,"%s\.hist\._(\d+)\.root" % outDS)
    indexRDO     = getIndex(tmpList,"%s\.RDO\._(\d+)\.pool\.root" % outDS)    
    indexESD     = getIndex(tmpList,"%s\.ESD\._(\d+)\.pool\.root" % outDS)
    indexAOD     = getIndex(tmpList,"%s\.AOD\._(\d+)\.pool\.root" % outDS)
    indexTAG     = getIndex(tmpList,"%s\.TAG\._(\d+)\.coll\.root" % outDS)
    indexStream1 = getIndex(tmpList,"%s\.Stream1\._(\d+)\.pool\.root" % outDS)
    indexStream2 = getIndex(tmpList,"%s\.Stream2\._(\d+)\.pool\.root" % outDS)
    indexBS      = getIndex(tmpList,"%s\.BS\._(\d+)\.data" % outDS)
    if runConfig.output.outSelBS:
        indexSelBS   = getIndex(tmpList,"%s\.%s\._(\d+)\.data" % (outDS,runConfig.output.outSelBS))
    if runConfig.output.outNtuple:
        for sName in runConfig.output.outNtuple:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (outDS,sName))
            if tmpIndex > indexNT:
                indexNT  = tmpIndex
    if runConfig.output.outTHIST:            
        for sName in runConfig.output.outTHIST:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (outDS,sName))
            if tmpIndex > indexTHIST:
                indexTHIST  = tmpIndex
    if runConfig.output.outAANT:            
        for aName,sName in runConfig.output.outAANT:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.root" % (outDS,sName))
            if tmpIndex > indexAANT:
                indexAANT  = tmpIndex
    if runConfig.output.outIROOT:            
        for sIndex,sName in enumerate(runConfig.output.outIROOT):
            tmpIndex = getIndex(tmpList,"%s\.iROOT%s\._(\d+)\.%s" % (outDS,sIndex,sName))
            if tmpIndex > indexIROOT:
                indexIROOT  = tmpIndex
    if runConfig.output.extOutFile: 
        for sIndex,sName in enumerate(runConfig.output.extOutFile):
            # change * to X and add .tgz
            if sName.find('*') != -1:
                sName = sName.replace('*','XYZ')
                sName = '%s.tgz' % sName
            tmpIndex = getIndex(tmpList,"%s\.EXT%s\._(\d+)\.%s" % (outDS,sIndex,sName))
            if tmpIndex > indexEXT:
                indexEXT  = tmpIndex
    if runConfig.output.outStreamG:            
        for sName in runConfig.output.outStreamG:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.pool\.root" % (outDS,sName))
            if tmpIndex > indexStreamG:
                indexStreamG = tmpIndex
    if runConfig.output.outMeta:            
        for sName,sAsso in runConfig.output.outMeta:
            iMeta = 0
            if sAsso == 'None':
                tmpIndex = getIndex(tmpList,"%s\.META%s\._(\d+)\.root" % (outDS,iMeta))
                iMeta += 1
                if tmpIndex > indexMeta:
                    indexMeta = tmpIndex
    if runConfig.output.outMS:                
        for sName,sAsso in runConfig.output.outMS:
            tmpIndex = getIndex(tmpList,"%s\.%s\._(\d+)\.pool\.root" % (outDS,sName))
            if tmpIndex > indexMS:
                indexMS = tmpIndex


    
# convert runConfig to outMap
def convertConfToOutput(runConfig,jobR,outMap,individualOutDS,extOutFile):
    from taskbuffer.FileSpec import FileSpec    
    # use global to increment index
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
    global indexEXT
    global indexStreamG
    global indexMeta
    global indexMS
    # start conversion
    if runConfig.output.outNtuple:
        indexNT += 1
        for sName in runConfig.output.outNtuple:
            file = FileSpec()
            file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexNT)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
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
        file.lfn  = '%s.hist._%05d.root' % (jobR.destinationDBlock,indexHIST)
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_HIST'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['hist'] = file.lfn
    if runConfig.output.outRDO:
        indexRDO += 1        
        file = FileSpec()
        file.lfn  = '%s.RDO._%05d.pool.root' % (jobR.destinationDBlock,indexRDO)        
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_RDO'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['RDO'] = file.lfn
    if runConfig.output.outESD:
        indexESD += 1        
        file = FileSpec()
        file.lfn  = '%s.ESD._%05d.pool.root' % (jobR.destinationDBlock,indexESD)        
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_ESD'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['ESD'] = file.lfn
    if runConfig.output.outAOD:
        indexAOD += 1                
        file = FileSpec()
        file.lfn  = '%s.AOD._%05d.pool.root' % (jobR.destinationDBlock,indexAOD)        
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_AOD'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['AOD'] = file.lfn
    if runConfig.output.outTAG:
        indexTAG += 1                        
        file = FileSpec()
        file.lfn  = '%s.TAG._%05d.coll.root' % (jobR.destinationDBlock,indexTAG)                
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_TAG'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['TAG'] = file.lfn
    if runConfig.output.outAANT:
        indexAANT += 1
        sNameList = []
        for aName,sName in runConfig.output.outAANT:
            file = FileSpec()
            file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexAANT)       
            file.type = 'output'
            file.dataset = jobR.destinationDBlock        
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            if not sName in sNameList:
                sNameList.append(sName)
                jobR.addFile(file)
            if not outMap.has_key('AANT'):
                outMap['AANT'] = []
            outMap['AANT'].append((aName,sName,file.lfn))
    if runConfig.output.outTHIST:
        indexTHIST += 1
        for sName in runConfig.output.outTHIST:
            file = FileSpec()
            file.lfn  = '%s.%s._%05d.root' % (jobR.destinationDBlock,sName,indexTHIST)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
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
            file.lfn  = '%s.iROOT%s._%05d.%s' % (jobR.destinationDBlock,sIndex,indexIROOT,sName)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_iROOT%s' % sIndex
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
            file.lfn  = '%s.EXT%s._%05d.%s' % (jobR.destinationDBlock,sIndex,indexEXT,sName)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_EXT%s' % sIndex
                file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('IROOT'):
                outMap['IROOT'] = []
            outMap['IROOT'].append((origSName,file.lfn))
    if runConfig.output.outStream1:
        indexStream1 += 1                                        
        file = FileSpec()
        file.lfn  = '%s.Stream1._%05d.pool.root' % (jobR.destinationDBlock,indexStream1)
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_Stream1'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['Stream1'] = file.lfn
    if runConfig.output.outStream2:
        indexStream2 += 1                                        
        file = FileSpec()
        file.lfn  = '%s.Stream2._%05d.pool.root' % (jobR.destinationDBlock,indexStream2)
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_Stream2'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['Stream2'] = file.lfn
    if runConfig.output.outBS:
        indexBS += 1                                        
        file = FileSpec()
        file.lfn  = '%s.BS._%05d.data' % (jobR.destinationDBlock,indexBS)
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_BS'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        outMap['BS'] = file.lfn
    if runConfig.output.outSelBS:
        indexSelBS += 1                                        
        file = FileSpec()
        file.lfn  = '%s.%s._%05d.data' % (jobR.destinationDBlock,runConfig.output.outSelBS,indexSelBS)
        file.type = 'output'
        file.dataset = jobR.destinationDBlock        
        file.destinationDBlock = jobR.destinationDBlock
        if individualOutDS:
            tmpSuffix = '_SelBS'
            file.dataset += tmpSuffix
            file.destinationDBlock += tmpSuffix
        file.destinationSE = jobR.destinationSE
        jobR.addFile(file)
        if not outMap.has_key('IROOT'):
            outMap['IROOT'] = []
        outMap['IROOT'].append(('%s.*.data' % runConfig.output.outSelBS,file.lfn))
    if runConfig.output.outStreamG:
        indexStreamG += 1
        for sName in runConfig.output.outStreamG:
            file = FileSpec()
            file.lfn  = '%s.%s._%05d.pool.root' % (jobR.destinationDBlock,sName,indexStreamG)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
                file.dataset += tmpSuffix
                file.destinationDBlock += tmpSuffix
            file.destinationSE = jobR.destinationSE
            jobR.addFile(file)
            if not outMap.has_key('StreamG'):
                outMap['StreamG'] = []
            outMap['StreamG'].append((sName,file.lfn))
    if runConfig.output.outMeta:
        iMeta = 0
	indexMeta += 1
        for sName,sAsso in runConfig.output.outMeta:
            foundLFN = ''
            if sAsso == 'None':
                # non-associated metadata
                file = FileSpec()
                file.lfn  = '%s.META%s._%05d.root' % (jobR.destinationDBlock,iMeta,indexMeta)
                file.type = 'output'
                file.dataset = jobR.destinationDBlock
                file.destinationDBlock = jobR.destinationDBlock
                if individualOutDS:
                    tmpSuffix = '_META%s' % iMeta
                    file.dataset += tmpSuffix
                    file.destinationDBlock += tmpSuffix
                file.destinationSE = jobR.destinationSE
                jobR.addFile(file)
                iMeta += 1
                foundLFN = file.lfn
            elif outMap.has_key(sAsso):
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
                if not outMap.has_key('Meta'):
                    outMap['Meta'] = []
                outMap['Meta'].append((sName,foundLFN))
    if runConfig.output.outMS:
	indexMS += 1
        for sName,sAsso in runConfig.output.outMS:
            file = FileSpec()
            file.lfn  = '%s.%s._%05d.pool.root' % (jobR.destinationDBlock,sName,indexMS)
            file.type = 'output'
            file.dataset = jobR.destinationDBlock
            file.destinationDBlock = jobR.destinationDBlock
            if individualOutDS:
                tmpSuffix = '_%s' % sName
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
    file.lfn  = '%s._$PANDAID.log.tgz' % jobR.destinationDBlock
    file.type = 'log'
    file.dataset = jobR.destinationDBlock    
    file.destinationDBlock = jobR.destinationDBlock
    if individualOutDS:
        # use original outDS for log, which guarantees location registration and shadow tracing
        pass
    file.destinationSE = jobR.destinationSE
    jobR.addFile(file)
