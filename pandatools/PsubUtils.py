import os
import re
import sys
import time
try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote
import datetime
import gzip
import copy
import platform

from . import Client
from . import MiscUtils
from . import PLogger

from .MiscUtils import commands_get_status_output, commands_get_output, commands_get_status_output_with_env
try:
    long()
except Exception:
    long = int

# error code
EC_Config    = 10
EC_Post      = 11


cacheProxyStatus = None
cacheVomsStatus = None
cacheActimeStatus = None
cacheVomsFQAN = ''
cacheActime = ''
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
    if cacheLastUpdate is None or (timeNow-cacheLastUpdate) > datetime.timedelta(minutes=60):
        cacheLastUpdate = timeNow
        cacheProxyStatus = None
        cacheVomsStatus = None
        cacheActimeStatus = None
        cacheVomsFQAN = ''
        cacheActime = ''
        cacheVomsInfo = None


# get proxy info
def get_proxy_info(force, verbose):
    global cacheVomsInfo
    if force or cacheVomsInfo is None:
        # get logger
        tmpLog = PLogger.getPandaLogger()
        if Client.use_x509_no_grid():
            if 'PANDA_NICKNAME' not in os.environ:
                status = 1
                nickname = ''
                tmpLog.error('PANDA_NICKNAME is not defined')
            else:
                status = 0
                nickname = os.environ['PANDA_NICKNAME']
            cacheVomsInfo = (status, (nickname,))
        elif not Client.use_oidc():
            # check grid-proxy
            gridSrc = Client._getGridSrc()
            com = '%s voms-proxy-info --all --e' % gridSrc
            if verbose:
                tmpLog.debug(com)
            status,out = commands_get_status_output_with_env(com)
            if verbose:
                tmpLog.debug(status % 255)
                tmpLog.debug(out)
            cacheVomsInfo = status,out
        else:
            # OIDC
            uid, groups, nickname = Client.get_user_name_from_token()
            if uid is None:
                status = 1
            else:
                status = 0
            cacheVomsInfo = (status, (uid, groups, nickname))
    return cacheVomsInfo


# new proxy check
def check_proxy(verbose, voms_role, refresh_info=False, generate_new=True):
    status, out = get_proxy_info(refresh_info, verbose)
    if status == 0:
        if voms_role is None:
            return True
        # check role
        for tmpItem in out.split('\n'):
            if not tmpItem.startswith('attribute'):
                continue
            role = voms_role.split(':')[-1]
            if role in tmpItem:
                return True
    if not generate_new or Client.use_oidc() or Client.use_x509_no_grid():
        return False
    # generate proxy
    import getpass
    tmpLog = PLogger.getPandaLogger()
    tmpLog.info("Need to generate a grid proxy")
    gridPassPhrase = getpass.getpass('Enter GRID pass phrase for this identity:\n').replace('$', '\$').replace('"', r'\"')
    gridSrc = Client._getGridSrc()
    com = '%s echo "%s" | voms-proxy-init -pwstdin ' % (gridSrc, gridPassPhrase)
    com_msg = '%s echo "*****" | voms-proxy-init -pwstdin ' % gridSrc
    if voms_role is None:
        com += '-voms atlas'
        com_msg += '-voms atlas'
    else:
        com += '-voms %s' % voms_role
        com_msg += '-voms %s' % voms_role
    if verbose:
        tmpLog.debug(com_msg)
    status, output = commands_get_status_output_with_env(com)
    if status != 0:
        tmpLog.error(output)
        tmpLog.error("Could not generate a grid proxy")
        sys.exit(EC_Config)
    return check_proxy(verbose, voms_role, refresh_info=True, generate_new=False)



# get nickname
def getNickname(verbose=False):
    nickName = ''
    status, output = get_proxy_info(False, verbose)
    # OIDC
    if Client.use_oidc():
        return output[2]
    # x509 without grid
    if Client.use_x509_no_grid():
        return output[0]
    # X509
    for line in output.split('\n'):
        if line.startswith('attribute'):
            match = re.search('nickname =\s*([^\s]+)\s*\(atlas\)',line)
            if match is not None:
                nickName = match.group(1)
                break
    # check
    if nickName == '':
        # get logger
        tmpLog = PLogger.getPandaLogger()
        wMessage =  'Could not get nickname by using voms-proxy-info which gave\n\n'
        wMessage += output
        wMessage += '\nPlease register nickname to ATLAS VO via\n\n'
        wMessage += '   https://lcg-voms2.cern.ch:8443/voms/atlas/vomrs\n'
        wMessage += '      [Member Info] -> [Edit Personal Info]'
        print('')
        tmpLog.warning(wMessage)
        print('')
    return nickName


# set Rucio accounting
def setRucioAccount(account,appid,forceSet):
    if forceSet or 'RUCIO_ACCOUNT' not in os.environ:
        os.environ['RUCIO_ACCOUNT'] = account
    if forceSet or 'RUCIO_APPID' not in os.environ:
        os.environ['RUCIO_APPID'] = appid


# check name of output dataset
def checkOutDsName(outDS,official,nickName='',mergeOutput=False,verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check NG chars for SE
    for tmpChar in ['%','|',';','>','<','?','\'','"','(',')','$','@','*',':',
                    '=','&','^','#','\\','@','[',']','{','}','`']:
        if tmpChar in outDS:
            errStr = 'invalid character "%s" is used in --outDS' % tmpChar
            tmpLog.error(errStr)
            return False
    # official dataset
    if official:
        status, output = get_proxy_info(False, verbose)
        # extract production role
        prodGroups = []
        for tmpLine in output.split('\n'):
            match = re.search('/([^/]+)/Role=production',tmpLine)
            if match is not None:
                # ignore atlas production role
                if not match.group(1) in ['atlas']:
                    prodGroups.append(match.group(1))
        # no production role
        if prodGroups == []:
            errStr  = "The --official option requires production role. Please use the --voms option to set production role;\n"
            errStr += "  e.g.,  --voms atlas:/atlas/phys-higgs/Role=production\n"
            errStr += "If you don't have production role for the group please request it in ATLAS VO first"
            tmpLog.error(errStr)
            return False
        # loop over all prefixes
        allowedPrefix = ['group']
        for tmpPrefix in allowedPrefix:
            for tmpGroup in prodGroups:
                tmpPattO = '^'+tmpPrefix+'\d{2}'+'\.'+tmpGroup+'\.'
                tmpPattN = '^'+tmpPrefix+'\.'+tmpGroup+'\.'
                if re.search(tmpPattO,outDS) is not None or re.search(tmpPattN,outDS) is not None:
                    return True
        # didn't match
        errStr  = "Your proxy is allowed to produce official datasets\n"
        errStr += "        with the following prefix\n"
        for tmpPrefix in allowedPrefix:
            for tmpGroup in prodGroups:
                tmpPattO = '%s%s.%s' % (tmpPrefix,time.strftime('%y',time.gmtime()),tmpGroup)
                errStr += "          %s\n" % tmpPattO
                tmpPattN = '%s.%s' % (tmpPrefix,tmpGroup)
                errStr += "          %s\n" % tmpPattN
        errStr += "If you have production role for another group please use the --voms option to set the role\n"
        errStr += "  e.g.,  --voms atlas:/atlas/phys-higgs/Role=production\n"
        tmpLog.error(errStr)
        return False
    # check output dataset format
    matStrN = '^user\.'+nickName+'\.'
    if nickName == '' or re.match(matStrN,outDS) is None:
        if nickName == '':
            errStr = "Could not get nickname from voms proxy\n"
        else:
            outDsPrefixN = 'user.%s' % nickName
            errStr  = "outDS must be '%s.<user-controlled string...>'\n" % outDsPrefixN
            errStr += "        e.g., %s.test1234" % outDsPrefixN
        tmpLog.error(errStr)
        return False
    # check length. 200=255-55. 55 is reserved for Panda-internal (_subXYZ etc)
    maxLength = 200
    if mergeOutput:
        maxLengthCont = 120
    else:
        maxLengthCont = 132
    if outDS.endswith('/'):
        # container
        if len(outDS) > maxLengthCont:
            tmpErrStr  = "The name of the output dataset container is too long (%s). " % len(outDS)
            tmpErrStr += "The length must be less than %s " % maxLengthCont
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
            tmpLog.error("output datasetname is too long (%s). The length must be less than %s" % \
                         (len(outDS),maxLength))
            return False
    return True


# convert sys.argv to string
def convSysArgv():
    # job params
    if 'PANDA_EXEC_STRING' in os.environ:
        paramStr = os.environ['PANDA_EXEC_STRING']
    else:
        paramStr = sys.argv[0].split('/')[-1]
    for item in sys.argv[1:]:
        # remove option
        match = re.search('(^-[^=]+=)(.+)',item)
        noSpace = False
        if match is not None:
            paramStr += ' %s' % match.group(1)
            item = match.group(2)
            noSpace = True
        if not noSpace:
            paramStr += ' '
        match = re.search('(\*| |\')',item)
        if match is None:
            # normal parameters
            paramStr += '%s' % item
        else:
            # quote string
            paramStr += '"%s"' % item
    # return
    return paramStr


# compare version
def isLatestVersion(latestVer):
    # extract local version numbers
    import PandaToolsPkgInfo
    match = re.search('^(\d+)\.(\d+)\.(\d+)$',PandaToolsPkgInfo.release_version)
    if match is None:
        return True
    localMajorVer  = int(match.group(1))
    localMinorVer  = int(match.group(2))
    localBugfixVer = int(match.group(3))
    # extract local version numbers
    match = re.search('^(\d+)\.(\d+)\.(\d+)$',latestVer)
    if match is None:
        return True
    latestMajorVer  = int(match.group(1))
    latestMinorVer  = int(match.group(2))
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


# check panda-client version
def checkPandaClientVer(verbose):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get latest version number
    vStatus,latestVer = Client.getPandaClientVer(verbose)
    if vStatus == 0:
        # check version
        if not isLatestVersion(latestVer):
            warStr = "A newer version of panda-client is available at https://twiki.cern.ch/twiki/bin/view/Atlas/PandaTools."
            if os.environ['PANDA_SYS'].startswith('/afs/cern.ch/atlas/offline/external/GRID/DA/panda-client'):
                # if the user uses CERN AFS
                warStr += " Please execute 'source /afs/cern.ch/atlas/offline/external/GRID/DA/panda-client/latest/etc/panda/panda_setup.[c]sh"
            else:
                warStr += " Please execute '%s --update' if you installed the package locally" % sys.argv[0].split('/')[-1]
            print('')
            tmpLog.warning(warStr+'\n')


# function for path completion
def completePathFunc(text, status):
    # remove white spaces
    text = text.strip()
    # convert ~
    useTilde = False
    if text.startswith('~'):
        useTilde = True
        # keep original
        origText = text
        # convert
        text = os.path.expanduser(text)
    # put / to directories
    if (not text.endswith('/')) and os.path.isdir(text):
        text += '/'
    # list dirs/files
    lsStat,output = commands_get_status_output('ls -d %s*' % text)
    results = []
    if lsStat == 0:
        for tmpItem in output.split('\n'):
            # ignore current and parent dirs
            if tmpItem in ['.','..']:
                continue
            # put /
            if os.path.isdir(tmpItem) and not tmpItem.endswith('/'):
                tmpItem += '/'
            # recover ~
            if useTilde:
                tmpItem = re.sub('^%s' % os.path.expanduser(origText),
                                 origText,tmpItem)
            # append
            results.append(tmpItem)
        # sort
        results.sort()
    # return
    return results[status]


# update package
def updatePackage(verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get the latest version number
    tmpLog.info('start version check')
    status,output = Client.getPandaClientVer(verbose)
    if status != 0:
        tmpLog.error(output)
        tmpLog.error('failed to get the latest version number : %s' % status)
        return False
    # extract version
    latestVer = output
    # check version
    if isLatestVersion(latestVer):
        tmpLog.info('you are already using the latest version')
        return True
    import PandaToolsPkgInfo
    tmpLog.info('update to %s from %s' % (latestVer,PandaToolsPkgInfo.release_version))
    # set readline for auto-complete
    import readline
    readline.parse_and_bind("tab: complete")
    readline.set_completer(completePathFunc)
    readline.parse_and_bind('set show-all-if-ambiguous On')
    # remove +/~ from delimiters
    curDelimter = readline.get_completer_delims()
    curDelimter = re.sub('\+|/|~','',curDelimter)
    readline.set_completer_delims(curDelimter)
    # installation type
    rpmInstall = False
    newPrefix = os.environ['PANDA_SYS']
    print('')
    print("Please specify type of installation")
    print("   PANDA_SYS=%s" % os.environ['PANDA_SYS'])
    print(" 1. Install to $PANDA_SYS")
    print("      all files in $PANDA_SYS will be erased first and new ones will")
    print("      be installed to the same dir")
    print(" 2. Install to a new dir")
    print("      new files will be installed to somewhere else than $PANDA_SYS")
    print(" 3. Patch (not recommended)")
    print("      existing files in $PANDA_SYS will be patched with new ones")
    print(" 4. RPM installation")
    print("      install RPM. sudo is required")
    print('')
    while True:
        str = input("Enter 1-4 : ")
        if str == '1':
            cleanInstall = True
            break
        if str == '2':
            cleanInstall = False
            # set default
            def startupHookPath():
                defPath = os.environ['PANDA_SYS']
                # remove /
                defPath = re.sub('/+$','',defPath)
                # use one dir up
                defPath = re.sub('/[^/]+$','',defPath)
                # add /
                if not defPath.endswith('/'):
                    defPath += '/'
                # set
                readline.insert_text(defPath)
            # set hook
            readline.set_startup_hook(startupHookPath)
            # get location
            while True:
                newPrefix = input("Enter new location (TAB for autocomplete): ")
                if newPrefix != '':
                    break
            # unset hook
            readline.set_startup_hook(None)
            break
        if str == '3':
            cleanInstall = False
            break
        if str == '4':
            rpmInstall = True
            break
    # get tarball
    tmpLog.info('get panda-client-%s' % latestVer)
    if not rpmInstall:
        packageName = 'panda-client-%s.tar.gz' % latestVer
    else:
        packageName = 'panda-client-%s-1.noarch.rpm' % latestVer
    com = 'wget --no-check-certificate --timeout 120 https://atlpan.web.cern.ch/atlpan/panda-client/%s' \
          % packageName
    status = os.system(com)
    status %= 255
    if status != 0:
        tmpLog.error('failed to download tarball : %s' % status)
        # delete tarball just in case
        commands_get_output('rm %s' % packageName)
        return False
    # install
    if not rpmInstall:
        # expand
        status,output = commands_get_status_output('tar xvfz %s' % packageName)
        status %= 255
        if verbose:
            tmpLog.debug(status)
            tmpLog.debug(output)
        if status != 0:
            tmpLog.error('failed to expand tarball : %s' % status)
            # delete dirs just in case
            commands_get_output('rm -rf panda-client-%s' % latestVer)
            return False
        # delete tarball
        commands_get_output('rm %s' % packageName)
        # save current dir
        currentDir = os.path.realpath(os.getcwd())
        # keep old release
        if cleanInstall:
            tmpLog.info('keep old version in %s.back' % os.environ['PANDA_SYS'])
            backUpDir = '%s.back' % os.environ['PANDA_SYS']
            status,output = commands_get_status_output('rm -rf %s; mv %s %s' % \
                                                     (backUpDir,os.environ['PANDA_SYS'],backUpDir))
            if status != 0:
                tmpLog.error(output)
                tmpLog.error('failed to keep old version')
                # delete dirs
                commands_get_output('rm -rf panda-client-%s' % latestVer)
                return False
        # install
        result = True
        os.chdir('panda-client-%s' % latestVer)
        status,output = commands_get_status_output('python setup.py install --prefix=%s' % newPrefix)
        if verbose:
            tmpLog.debug(output)
            tmpLog.debug(status)
        os.chdir(currentDir)
        status %= 255
        if status != 0:
            tmpLog.error('failed to install panda-client : %s' % status)
            # recover old one
            commands_get_output('rm -rf %s' % os.environ['PANDA_SYS'])
            commands_get_output('mv %s.back %s' % (os.environ['PANDA_SYS'],os.environ['PANDA_SYS']))
            result = False
        # cleanup
        commands_get_output('rm -rf panda-client-%s' % latestVer)
    else:
        # rpm install
        result = True
        newPrefix = ''
        com = 'sudo rpm -Uvh %s' % packageName
        print(com)
        status = os.system(com)
        status %= 255
        if status != 0:
            tmpLog.error('failed to install rpm : %s' % status)
            result = False
        # cleanup
        commands_get_output('rm -rf %s' % packageName)
    # return
    if result:
        tmpLog.info('completed')
        tmpLog.info("please do 'source %s/etc/panda/panda_setup.[c]sh'" % newPrefix)
    return result


# check unmerge dataset
def checkUnmergedDataset(inDS,secDS):
    dsList = []
    if inDS != '':
        dsList += inDS.split(',')
    if secDS != '':
        dsList += secDS.split(',')
    # pattern for unmerged datasets
    unPatt = '^mc.+\.recon\.AOD\.'
    unMergedDs = ''
    for tmpDs in dsList:
        # check dataset name
        if re.search(unPatt,tmpDs) is not None:
            unMergedDs += '%s,' % tmpDs
    unMergedDs = unMergedDs[:-1]
    # return
    if unMergedDs != '':
        # get logger
        tmpLog = PLogger.getPandaLogger()
        msg = "%s is unmerged AOD dataset which is not distributed. Please use mc*.merge.AOD.* instead\n" % unMergedDs
        print('')
        tmpLog.warning(msg)
    return


# read dataset names from text
def readDsFromFile(txtName):
    dsList = ''
    try:
        # read lines
        txt = open(txtName)
        for tmpLine in txt:
            # remove \n
            tmpLine = re.sub('\n','',tmpLine)
            # remove white spaces
            tmpLine = tmpLine.strip()
            # skip comment or empty
            if tmpLine.startswith('#') or tmpLine == '':
                continue
            # append
            dsList += '%s,' % tmpLine
        # close file
        txt.close()
        # remove the last comma
        dsList = dsList[:-1]
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error('cannot read datasets from %s due to %s:%s' \
                     % (txtName,errType,errValue))
        sys.exit(EC_Config)
    return dsList


# convert param string to JEDI params
def convertParamStrToJediParam(encStr,inputMap,outNamePrefix,encode,padding,usePfnList=False,includeIO=True):
    # list of place holders for input
    inList = ['IN','CAVIN','MININ','LOMBIN','HIMBIN','BHIN','BGIN','BGHIN','BGCIN','BGOIN']
    # place holder for output
    outHolder = 'SN'
    # place holders with extension
    digExList = ['RNDMSEED','FIRSTEVENT']
    allExList = digExList + ['DBR']
    # mapping of client and JEDI place holders
    holders = {'RNDMSEED'  : 'RNDM',
               'DBR'       : 'DB',
               'SKIPEVENTS': 'SKIPEVENTS',
               'FIRSTEVENT': None,
               'MAXEVENTS' : None,
               'SEGMENT_NAME': None
               }
    # replace %XYZ with ${XYZ}
    if includeIO:
        for tmpH in inList:
            encStr = re.sub('%'+tmpH,'${'+tmpH+'}',encStr)
    # replace %XYZ with ${newXYZ}
    extensionMap = {}
    for newH in holders:
        oldH = holders[newH]
        # JEDI-only place holders
        if oldH is None:
            oldH = newH
        oldH = '%' + oldH
        # with extension
        if newH in allExList:
            if newH in digExList:
                oldH += '(:|=)(\d+)%{0,1}'
            else:
                oldH += '(:|=)([^ \'\"\}]+)'
            # look for extension
            tmpM = re.search(oldH,encStr)
            if tmpM is not None:
                extensionMap[newH] = tmpM.group(2)
            newH = '${' + newH + '}'
        else:
            newH = '${' + newH + '}'
        encStr = re.sub(oldH,newH,encStr)
    # replace %OUT to outDS${SN}
    if includeIO:
        encStr = re.sub('%OUT',outNamePrefix+'.${'+outHolder+'}',encStr)
    # make pattern for split
    patS  = "("
    allKeys = list(holders)
    if includeIO:
        allKeys += inList
        allKeys += [outHolder]
    for tmpH in allKeys:
        patS += '[^=,\"\' \(\{;]*\$\{' + tmpH + '[^\}]*\}[^,\"\' \)\};]*|'
    patS  = patS[:-1]
    patS += ")"
    # split
    tmpItems = re.split(patS,encStr)
    # make parameters
    jobParams = []
    for tmpItem in tmpItems:
        # check if a place holder
        matchP = re.search('\$\{([^:\}]+)',tmpItem)
        if re.search(patS,tmpItem) is not None and matchP is not None:
            tmpHolder = matchP.group(1)
            # set attributes
            if tmpHolder in inList:
                # use constant since it is templated in another option e.g., -i
                tmpDict = {'type':'constant'}
                if encode:
                    tmpDict['value'] = '${' + tmpHolder + '/E}'
                else:
                    tmpDict['value'] = tmpItem
                # set dataset if PFN list is not used or the stream is not primary
                if not usePfnList or tmpHolder not in ['IN']:
                    tmpDict['param_type'] = 'input'
                    tmpDict['dataset'] = inputMap[tmpHolder]
            elif tmpHolder == outHolder:
                tmpDict = {'type':'template'}
                tmpDict['value'] = tmpItem
                tmpDict['param_type'] = 'output'
                tmpDict['dataset'] = outNamePrefix + tmpItem.split('}')[-1] + '/'
                tmpDict['container'] = tmpDict['dataset']
            else:
                tmpDict = {'type':'template'}
                tmpDict['value'] = tmpItem
                tmpDict['param_type'] = 'number'
                if tmpHolder in extensionMap:
                    try:
                        tmpDict['offset'] = long(extensionMap[tmpHolder])
                    except Exception:
                        pass
        else:
            # constant
            tmpDict = {'type':'constant'}
            if encode:
                tmpDict['value'] = quote(tmpItem)
            else:
                tmpDict['value'] = tmpItem
        # no padding
        if not padding:
            tmpDict['padding'] = False
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
        temItems = oldItem.split(',')
        for tmpItem in temItems:
            tmpItem = tmpItem.strip()
            # remove empty
            if tmpItem == '':
                continue
            if tmpItem not in newList:
                newList.append(tmpItem)
    return newList


# upload gzipped file
def uploadGzippedFile(origFileName,currentDir,tmpLog,delFilesOnExit,nosubmit,verbose):
    # open original file
    if origFileName.startswith('/'):
        # absolute path
        tmpIn = open(origFileName)
    else:
        # relative path
        tmpIn = open('%s/%s' % (currentDir,origFileName))
    # use unique name for gzip
    newFileName = 'pre_%s.dat' % MiscUtils.wrappedUuidGen()
    gzipFullPath = '%s/%s.gz' % (currentDir,newFileName)
    delFilesOnExit.append(gzipFullPath)
    # make gzip
    tmpOut = gzip.open(gzipFullPath,'wb')
    tmpOut.writelines(tmpIn)
    tmpOut.close()
    tmpIn.close()
    # upload
    if not nosubmit:
        tmpLog.info("uploading data file for preprocessing")
        status,out = Client.putFile(gzipFullPath,verbose,useCacheSrv=True,reuseSandbox=False)
        if status != 0 or out != 'True':
            # failed
            print(out)
            tmpLog.error("Failed with %s" % status)
            sys.exit(EC_Post)
    # delete
    os.remove(gzipFullPath)
    # return new filename
    return newFileName


# get PFN list
def getListPFN(pfnFile):
    rFile = open(pfnFile)
    inputFileList = []
    for line in rFile:
        line = re.sub('\n','',line)
        line.strip()
        if line != '' and not line.startswith('#'):
            inputFileList.append(line)
    rFile.close()
    inputFileList.sort()
    if len(inputFileList) == 0:
        # get logger
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error("{0} doesn't contain any PFNs".format(pfnFile))
        sys.exit(EC_Config)
    return inputFileList


# check task parameters
def checkTaskParam(taskParamMap,unlimitNumOutputs):
    # check output dataset names
    maxLengthCont = 132
    maxNumOutputs = 10
    nOutputs = 0
    for tmpDict in taskParamMap['jobParameters']+[taskParamMap['log']]:
        if tmpDict['type'] == 'template' and tmpDict['param_type'] in ['output','log']:
            if tmpDict['param_type'] == 'output':
                nOutputs += 1
            if len(tmpDict['dataset']) > maxLengthCont:
                tmpErrStr  = "The name of an output or log dataset container (%s) is too long (%s). " % (tmpDict['dataset'],len(tmpDict['dataset']))
                tmpErrStr += "The length must be less than %s following DDM definition. " % maxLengthCont
                tmpErrStr += "Please note that one dataset container is creted per output/log type and "
                tmpErrStr += "each name is <outDS>_<extension made from the output filename>/ or <outDS>.log/. "
                # get logger
                tmpLog = PLogger.getPandaLogger()
                tmpLog.error(tmpErrStr)
                return (EC_Config, tmpErrStr)
    if not unlimitNumOutputs and nOutputs > maxNumOutputs:
        errStr  ='Too many output files (=%s) per job. The default limit is %s. ' % (nOutputs,maxNumOutputs)
        errStr += 'You can remove the constraint by using the --unlimitNumOutputs option. '
        errStr += 'But please note that having too many outputs per job causes a severe load on the system. '
        errStr += 'You may be banned if you carelessly use the option'
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(errStr)
        return(EC_Config, errStr)
    return (0, None)


# replace input and output
def replaceInputOutput(taskParamMap, inDS, outDS, seqNum):
    newTaskParamMap = copy.deepcopy(taskParamMap)
    if inDS != '':
        oldInDS = taskParamMap['dsForIN']
        subInDSbefore = quote('%DATASET_IN')
        subInDSafter = quote(inDS)
        newTaskParamMap['dsForIN'] = inDS
        for tmpDict in newTaskParamMap['jobParameters']:
            if 'dataset' in tmpDict:
                if tmpDict['dataset'] == oldInDS:
                    tmpDict['dataset'] = inDS
            elif tmpDict['type'] == 'constant':
                tmpDict['value'] = re.sub(subInDSbefore, subInDSafter, tmpDict['value'])
    outDS = re.sub('/$', '', outDS)
    oldOutDS = taskParamMap['taskName']
    oldOutDS = re.sub('/$', '', oldOutDS)
    subOutDSbefore = quote('%DATASET_OUT')
    subOutDSafter = quote(outDS)
    subSeqBefore = quote('%BULKSEQNUMBER')
    subSeqAfter = str(seqNum)
    newTaskParamMap['taskName'] = outDS
    newTaskParamMap['log']['dataset'] = re.sub(oldOutDS, outDS, taskParamMap['log']['dataset'])
    newTaskParamMap['log']['container'] = re.sub(oldOutDS, outDS, taskParamMap['log']['container'])
    newTaskParamMap['log']['value'] = re.sub(oldOutDS, outDS, taskParamMap['log']['value'])
    for tmpDict in newTaskParamMap['jobParameters']:
        if 'dataset' in tmpDict:
            if tmpDict['dataset'].startswith(oldOutDS):
                tmpDict['dataset'] = re.sub(oldOutDS, outDS, tmpDict['dataset'])
                tmpDict['container'] = re.sub(oldOutDS, outDS, tmpDict['container'])
                tmpDict['value'] = re.sub(oldOutDS, outDS, tmpDict['value'])
        elif tmpDict['type'] == 'constant':
            tmpDict['value'] = re.sub(subOutDSbefore, subOutDSafter, tmpDict['value'])
            tmpDict['value'] = re.sub(subSeqBefore, subSeqAfter, tmpDict['value'])
            tmpDict['value'] = re.sub(oldOutDS, outDS, tmpDict['value'])
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
    for line in output.split('\n'):
        if line.startswith('subject'):
            subj = line.split(':', 1)[-1].lstrip()
            user_dn = re.sub(r'(/CN=\d+)+$', '', subj.replace('/CN=proxy', ''))
            username = user_dn.split('=')[-1]
            username = re.sub('[ |_]\d+', '', username)
            username = re.sub("[()']", '', username)
            break
    return username


# warning message when PQ is specified
def get_warning_for_pq(site, excluded_site, tmp_log):
    if site not in ['AUTO', None] or excluded_site:
        tmp_log.warning("The grid queue names could change due to consolidation, migration, etc. "
                "Please check with the command listAnalyPQ to use only online/valid queues "
                "when site and/or excludedSite options are specified.")
    return ''
