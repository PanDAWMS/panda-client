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
            # extract number
            tmpLFN = re.sub('^%s' % tmpHead,'',tmpLFN)
            tmpLFN = re.sub('%s$' % tmpTail,'',tmpLFN)
            compactPar += '%s,' % tmpLFN
        compactPar = compactPar[:-1]
        compactPar += ']%s' % tmpTail
        # replace
        tmpJobO = tmpJobO.replace(patt,compactPar)
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
            print "ERROR : cmt show projects"
            return False,{}

    # private work area
    res = re.search('\(in ([^\)]+)\)',lines[0])
    if res==None:
        print lines[0]
        print "ERROR : could not get path to private work area"
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
            if items[0] in ('dist','AtlasRelease','AtlasOffline'):
                # Atlas release
                athenaVer = os.path.basename(res.group(1))
                # nightly
                if athenaVer.startswith('rel'):
                   if re.search('/bugfix',line) != None:
                      nightVer  = '/bugfix'
                   elif re.search('/dev',line) != None:
                      nightVer  = '/dev'
                   else:
                      print "ERROR : unsupported nightly %s" % line
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
    # .py/.dat/.C/.xml
    for tmpExtention in ['.py','.dat','.C','.xml']:
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


# archive source files
def archiveSourceFiles(workArea,runDir,currentDir,tmpDir,verbose):
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
            # skip if doc
            if item == 'doc':
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
    def getPackages(_workArea):
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
    packages = getPackages(workArea)
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
        if areaName==workArea and not nobuild:
        # ignore i686 for workArea
            getFiles('InstallArea',files,True,True)
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
    if groupArea != '':
        os.chdir(tmpDir)
        if os.path.exists(groupFileName):
            out = commands.getoutput('tar -rh %s -f %s' % (groupFileName,archiveFullName))
            if out != '':    
                print out
            commands.getoutput('rm -rf %s' % groupFullName)    
