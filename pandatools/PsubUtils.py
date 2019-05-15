import os

if os.environ.has_key('PANDA_DEBUG'):
    print "DEBUG : importing %s" % __name__

import re
import sys
import time
import random
import commands
import urllib
import pickle
import datetime
import tempfile
import gzip
import copy
import platform

import Client
import MyproxyUtils
import MiscUtils
import PLogger
import AthenaUtils

# error code
EC_Config    = 10
EC_Post      = 11


cacheProxyStatus = None
cacheVomsStatus = None
cacheActimeStatus = None
cacheVomsFQAN = ''
cacheActime = ''
cacheLastUpdate = None


# reset cache values
def resetCacheValues():
    global cacheProxyStatus
    global cacheVomsStatus
    global cacheActimeStatus
    global cacheVomsFQAN
    global cacheActime
    global cacheLastUpdate
    timeNow = datetime.datetime.utcnow()
    if cacheLastUpdate == None or (timeNow-cacheLastUpdate) > datetime.timedelta(minutes=60):
        cacheLastUpdate = timeNow
        cacheProxyStatus = None
        cacheVomsStatus = None
        cacheActimeStatus = None
        cacheVomsFQAN = ''
        cacheActime = ''



# check proxy
def checkGridProxy(gridPassPhrase='',enforceEnter=False,verbose=False,vomsRoles=None,useCache=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check grid-proxy
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        sys.exit(EC_Config)
    # reset cache values
    if useCache:
        resetCacheValues()
    global cacheProxyStatus
    global cacheVomsStatus
    global cacheActimeStatus
    global cacheVomsFQAN
    global cacheActime
    # use grid-proxy-info first becaus of bug #36573 of voms-proxy-info
    if useCache and cacheProxyStatus == 0:
        status,out = 0,''
    else:
        com = '%s voms-proxy-info -e' % gridSrc
        if verbose:
            tmpLog.debug(com)
        status,out = commands.getstatusoutput(com)
        if verbose:
            tmpLog.debug(status % 255)
            tmpLog.debug(out)
        cacheProxyStatus = status
    # check VOMS extension
    vomsFQAN = ''
    if status == 0:
        # with VOMS extension 
        if useCache and cacheVomsStatus == 0:
            status,out = 0,cacheVomsFQAN
        else:
            com = '%s voms-proxy-info -fqan -exists' % gridSrc    
            if verbose:
                tmpLog.debug(com)
            status,out = commands.getstatusoutput(com)
            if verbose:
                tmpLog.debug(status % 255)
                tmpLog.debug(out)
            cacheVomsStatus,cacheVomsFQAN = status,out
        if status == 0:
            vomsFQAN = out
            # check actime
            if useCache and cacheActimeStatus == 0:
                acstatus,acout = 0,cacheActime
            else:
                com = '%s voms-proxy-info -actimeleft' % gridSrc    
                if verbose:
                    tmpLog.debug(com)
                acstatus,acout = commands.getstatusoutput(com)
                if verbose:
                    tmpLog.debug(acstatus % 255)
                    tmpLog.debug(acout)
                cacheActimeStatus,cacheActime = acstatus,acout
            if acstatus == 0:
                # get actime
                acTimeLeft = 0
                try:
                    acTimeLeft = int(acout.split('\n')[-1])
                except:
                    pass
                if acTimeLeft < 2*60:
                    # set status to regenerate proxy with roles
                    status = -1
                    out = ''
        # check roles
        if vomsRoles != None:
            hasAttr = True
            # remove redundant messages
            outVomsList = []
            for tmpItem in out.split('\n'):
                if tmpItem.startswith('/'):
                    outVomsList.append(tmpItem)
            for indexAttr,attrItem in enumerate(vomsRoles.split(',')):
                vomsCommand = attrItem.split(':')[-1]
                if not '/Role' in vomsCommand:
                    vomsCommand += '/Role'
                if not vomsCommand in out:
                    hasAttr = False
                    break
                # check order
                try:
                    if not vomsCommand in outVomsList[indexAttr]:
                        hasAttr = False
                        break
                except:
                        hasAttr = False
                        break
            if not hasAttr:
                # set status to regenerate proxy with roles
                status = -1
                out = ''
    # generate proxy
    if (status != 0 and out.find('Error: Cannot verify AC signature') == -1) or \
           out.find('Error: VOMS extension not found') != -1 or enforceEnter:
        # GRID pass phrase
        if gridPassPhrase == '':
            import getpass
            tmpLog.info("Need to generate a grid proxy")
            if commands.getstatusoutput('%s which grid-cert-info' % gridSrc)[0] == 0:
                print "Your identity: " + commands.getoutput('%s grid-cert-info -subject' % gridSrc)
            else:
                print "Your identity: " + re.sub('subject= ','',
                                                 commands.getoutput('%s openssl x509 -in ~/.globus/usercert.pem -noout -subject' % gridSrc))
            if sys.stdin.isatty():
                gridPassPhrase = getpass.getpass('Enter GRID pass phrase for this identity:')
            else:
                sys.stdout.write('Enter GRID pass phrase for this identity:')
                sys.stdout.flush()
                gridPassPhrase = sys.stdin.readline().rstrip()
                print
            gridPassPhrase = gridPassPhrase.replace('$','\$')
        # with VOMS extension
        if vomsRoles != None:
            com = '%s echo "%s" | voms-proxy-init -pwstdin' % (gridSrc,gridPassPhrase)
            for attrItem in vomsRoles.split(','):
                com += ' -voms %s' % attrItem
        else:
            com = '%s echo "%s" | voms-proxy-init -voms atlas -pwstdin' % (gridSrc,gridPassPhrase)            
        if verbose:
            tmpLog.debug(re.sub(gridPassPhrase,"*****",com))
        status = os.system(com)
        if status != 0:
            tmpLog.error("Could not generate a grid proxy")
            sys.exit(EC_Config)
        # get FQAN
        com = '%s voms-proxy-info -fqan' % gridSrc
        if verbose:
            tmpLog.debug("Getting FQAN")
        status,out = commands.getstatusoutput(com)        
        if (status != 0 and out.find('Error: Cannot verify AC signature') == -1) \
               or out.find('Error: VOMS extension not found') != -1:
            tmpLog.error("Could not get FQAN after voms-proxy-init")
            sys.exit(EC_Config)
        vomsFQAN = out
    # return
    return gridPassPhrase,vomsFQAN
    


# get cloud according to country FQAN
def getCloudUsingFQAN(defaultCloud,verbose=False,randomCloud=[]):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get FQAN
    vomsFQAN = ''
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        return ''
    com = '%s voms-proxy-info -fqan -exists' % gridSrc    
    if verbose:
	tmpLog.debug(com)
    status,out = commands.getstatusoutput(com)
    if verbose:
	tmpLog.debug(status % 255)
	tmpLog.debug(out)
    if status == 0:
        vomsFQAN = out
    cloud = None
    countryAttStr = ''
    # remove cloud not used for analysis
    validCloudList = []
    for tmpCloud,spec in Client.PandaClouds.iteritems():
        for tmpSiteID,tmpSiteSpec in Client.PandaSites.iteritems():
            if tmpCloud == tmpSiteSpec['cloud']:
                if not tmpCloud in validCloudList:
                    validCloudList.append(tmpCloud)
                break
    # check countries
    for tmpCloud,spec in Client.PandaClouds.iteritems():            
        # loop over all FQANs
        for tmpFQAN in vomsFQAN.split('\n'):
            # look for matching country
            for tmpCountry in spec['countries'].split(','):
                # skip blank
                if tmpCountry == '':
                    continue
                # look for /atlas/xyz/
                if re.search('^/atlas/%s/' % tmpCountry, tmpFQAN) != None:
                    # set cloud
                    cloud = tmpCloud
                    countryAttStr = re.sub('/Capability=NULL','',tmpFQAN)
                    if verbose:
                        tmpLog.debug("  match %s %s %s" % (tmpCloud,tmpCountry,tmpFQAN))
                    break
            # escape
            if cloud != None:
                break
        # escape
        if cloud != None:
            break
    # set default
    if randomCloud != []:
        # choose one cloud from the list
        cloud = random.choice(randomCloud)
    elif cloud == None or not cloud in validCloudList:
        # use a cloud randomly
        cloud = random.choice(validCloudList)
    else:
        pass
    if verbose:    
        tmpLog.debug("use %s as default cloud" % cloud)
    # return
    return cloud


# convert DQ2 ID to Panda siteid 
def convertDQ2toPandaID(site, getAll=False):
    return Client.convertDQ2toPandaID(site, getAll=getAll)

# get DN
def getDN():
    shortName = ''
    distinguishedName = ''
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        return ''
    output = commands.getoutput('%s voms-proxy-info -identity' % gridSrc)
    for line in output.split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = re.sub('\.','',distinguishedName)
            distinguishedName = re.sub('\(','',distinguishedName)
            distinguishedName = re.sub('\)','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) != None:
                # look for full name
                distinguishedName = distinguishedName.replace(' ','')
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # remove _
    distinguishedName = re.sub('_$','',distinguishedName)
    # remove ' & "
    distinguishedName = re.sub('[\'\"]','',distinguishedName)
    # check
    if distinguishedName == '':
        # get logger
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error('could not get DistinguishedName from %s' % output)
    return distinguishedName


# get nickname
def getNickname():
    nickName = ''
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        return ''
    # get generic attribute from voms proxy
    output = commands.getoutput('%s voms-proxy-info -all' % gridSrc)
    for line in output.split('\n'):
        if line.startswith('attribute'):
            match = re.search('nickname =\s*([^\s]+)\s*\(atlas\)',line)
            if match != None:
                nickName = match.group(1)
                break
    # check
    if nickName == '':
        # get logger
        tmpLog = PLogger.getPandaLogger()
        wMessage =  'Could not get nickname by using voms-proxy-info which gave\n\n'
        wMessage += output
        wMessage += '\nPlease register nickname to ATLAS VO via\n\n'
        wMessage += '   https://lcg-voms.cern.ch:8443/vo/atlas/vomrs\n'
        wMessage += '      [Member Info] -> [Edit Personal Info]'
        print
        tmpLog.warning(wMessage)
        print
    return nickName
        

# set Rucio accounting
def setRucioAccount(account,appid,forceSet):
    if forceSet or not os.environ.has_key('RUCIO_ACCOUNT'):
        os.environ['RUCIO_ACCOUNT'] = account
    if forceSet or not os.environ.has_key('RUCIO_APPID'):
        os.environ['RUCIO_APPID'] = appid

        
# check if valid cloud
def checkValidCloud(cloud):
    # check cloud
    for tmpID,spec in Client.PandaSites.iteritems():
        if cloud == spec['cloud']:
            return True
    return False    


# check name of output dataset
def checkOutDsName(outDS,distinguishedName,official,nickName='',site='',vomsFQAN='',mergeOutput=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check NG chars for SE
    for tmpChar in ['%','|',';','>','<','?','\'','"','(',')','$','@','*',':',
                    '=','&','^','#','\\','@','[',']','{','}','`']:
        if tmpChar in outDS:
            errStr = 'invalid character "%s" is used in --outDS' % tmpChar
            tmpLog.error(errStr)
            return False
    # don't check if DQ2-free
    if Client.isDQ2free(site):
        return True
    # official dataset
    if official:
        # extract production role
        prodGroups = []
        for tmpLine in vomsFQAN.split('\n'):
            match = re.search('/([^/]+)/Role=production',tmpLine)
            if match != None:
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
                if re.search(tmpPattO,outDS) != None or re.search(tmpPattN,outDS) != None:
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
    matStrO = '^user' + '\d{2}' + '\.' + distinguishedName + '\.'
    matStrN = '^user\.'+nickName+'\.'
    if re.match(matStrO,outDS) == None and (nickName == '' or re.match(matStrN,outDS) == None):
        if nickName == '':
            errStr = "Could not get nickname from voms proxy\n"
        else:
            outDsPrefixO = 'user%s.%s' % (time.strftime('%y',time.gmtime()),distinguishedName)
            outDsPrefixN = 'user.%s' % nickName
            errStr  = "outDS must be '%s.<user-controlled string...>'\n" % outDsPrefixN
            errStr += "        e.g., %s.test1234" % outDsPrefixN
        tmpLog.error(errStr)
        return False
    # check convention
    if re.match(matStrO,outDS) != None:
        outDsPrefixO = 'user%s.%s' % (time.strftime('%y',time.gmtime()),distinguishedName)        
        tmpStr  = "You are still using the old naming convention for --outDS (%s.XYZ), " % outDsPrefixO
        tmpStr += "which is not allowed any more. "
        tmpStr += "Please use user.nickname.XYZ instead. If you don't know your nickname, "
        tmpStr += "see https://savannah.cern.ch/forum/forum.php?forum_id=1259"
        print
        tmpLog.error(tmpStr)
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


# get suffix to split job list
def getSuffixToSplitJobList(sIndex):
    if sIndex == 0:
        return ''
    else:
        return '_%s' % sIndex
    

# split job list by the number of output files
def splitJobsNumOutputFiles(jobList,buildInLastChunk=False):
    nJobs = 0
    tmpJobList = []
    splitJobList = []
    numFilesMap = {}
    # max
    maxNumFiles   = 10000
    maxNumJobs    = 4000
    maxNumTotJobs = 100000
    # count the number of files per dataset
    for tmpJob in jobList[:maxNumTotJobs]:
        # loop over all files
        for tmpFile in tmpJob.Files:
            if tmpFile.type in ['output','log']:
                # count the number of files per output dataset                
                if not numFilesMap.has_key(tmpFile.destinationDBlock):
                    numFilesMap[tmpFile.destinationDBlock] = 0
                # increment
                numFilesMap[tmpFile.destinationDBlock] += 1
            else:
                # count the number of input files for shadow dataset
                if tmpFile.lfn.endswith('lib.tgz'):
                    continue
                if tmpFile.lfn.startswith('DBRelease'):
                    continue
                # use dummy dataset for input
                if not numFilesMap.has_key('input'):
                    numFilesMap['input'] = 0
                # increment
                numFilesMap['input'] += 1
        # check the number of files
        newBunch = False
        for tmpDBlock,tmpNumFiles in numFilesMap.iteritems():
            if tmpNumFiles > maxNumFiles:
                # append
                if tmpJobList != []:
                    splitJobList.append(tmpJobList)
                # reset
                nJobs = 0
                tmpJobList = []
                numFilesMap = {}
                newBunch = True
                break
        # check the number of jobs    
        if not newBunch and nJobs+1 > maxNumJobs:
            # append
            if tmpJobList != []:
                splitJobList.append(tmpJobList)
            # reset
            nJobs = 0
            tmpJobList = []
            numFilesMap = {}
            newBunch = True
        # count again since map was reset
        if newBunch:
            for tmpFile in tmpJob.Files:
                if tmpFile.type in ['output','log']:
                    if not numFilesMap.has_key(tmpFile.destinationDBlock):
                        numFilesMap[tmpFile.destinationDBlock] = 0
                    # increment
                    numFilesMap[tmpFile.destinationDBlock] += 1
                else:
                    if tmpFile.lfn.endswith('lib.tgz'):
                        continue
                    if tmpFile.lfn.startswith('DBRelease'):
                        continue
                    if not numFilesMap.has_key('input'):
                        numFilesMap['input'] = 0
                    # increment
                    numFilesMap['input'] += 1
        # increment
        nJobs += 1
        # append
        tmpJobList.append(tmpJob)
    # remaining
    if tmpJobList != []:
        splitJobList.append(tmpJobList)
    # change dataset names
    for serIndex,tmpJobList in enumerate(splitJobList):
        # don't change the first bunch
        if serIndex == 0:
            continue
        # loop over all jobs
        for tmpJob in tmpJobList:
            for tmpFile in tmpJob.Files:
                if tmpFile.type in ['output','log']:
                    tmpFile.destinationDBlock += getSuffixToSplitJobList(serIndex)
    # move build job to the last chunk
    if buildInLastChunk:
        firstChunk = splitJobList[0]
        lastChunk  = splitJobList[-1]
        # check prodSourceLabel of the fist job 
        if firstChunk[0].prodSourceLabel == 'panda':
            tmpJob = firstChunk.pop(0)
            lastChunk.insert(0,tmpJob)
    # return
    return splitJobList


# get maximum index in a dataset
def getMaxIndex(list,pattern,shortLFN=False):
    maxIndex = 0
    maxJobsetID = None
    for item in list:
        match = re.match(pattern,item)
        if match != None:
            if not shortLFN:
                # old format : Dataset.XYZ
                tmpIndex = int(match.group(1))
            else:
                # short format : user.nickname.XYZ
                tmpIndex = int(match.group(2))
                tmpJobsetID = match.group(1)
            if maxIndex < tmpIndex:
                maxIndex = tmpIndex
                # check jobsetID in LFN
                if shortLFN:
                    if maxJobsetID == None or int(maxJobsetID.split('_')[-1]) < int(tmpJobsetID.split('_')[-1]):
                        maxJobsetID = tmpJobsetID
    if shortLFN:
        return maxIndex,maxJobsetID
    return maxIndex


# upload proxy
def uploadProxy(site,myproxy,gridPassPhrase,pilotownerDN,verbose=False):
    # non-proxy delegation
    if not Client.PandaSites[site]['glexec'] in ['uid']:
        return True
    # delegation
    if Client.PandaSites[site]['glexec'] == 'uid':
        # get proxy key
        status,proxyKey = Client.getProxyKey(verbose)
        if status != 0:
            print proxyKey
            print "ERROR : could not get proxy key"
            return False
        gridSrc = Client._getGridSrc()
        # check if the proxy is valid in MyProxy
        mypIF = MyproxyUtils.MyProxyInterface()
        mypIF.pilotownerDN = pilotownerDN
        mypIF.servername = myproxy
        proxyValid = False
        # check existing key
        if proxyKey != {}:
            proxyValid = mypIF.check(proxyKey['credname'],verbose)
        # expired
        if not proxyValid:
            # upload proxy
            newkey = mypIF.delegate(gridPassPhrase,verbose)
            # register proxykey
            status,retO = Client.registerProxyKey(newkey,commands.getoutput('hostname -f'),
                                                  myproxy,verbose)
        if status != 0:
            print retO
            print "ERROR : could not register proxy key"
            return False
        # return
        return True


# convet sys.argv to string
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
        if match != None:
            paramStr += ' %s' % match.group(1)
            item = match.group(2)
            noSpace = True
        if not noSpace:
            paramStr += ' '
        match = re.search('(\*| |\')',item)
        if match == None:
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
    if match == None:
        return True
    localMajorVer  = int(match.group(1))
    localMinorVer  = int(match.group(2))
    localBugfixVer = int(match.group(3))
    # extract local version numbers
    match = re.search('^(\d+)\.(\d+)\.(\d+)$',latestVer)
    if match == None:
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
            print
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
    lsStat,output = commands.getstatusoutput('ls -d %s*' % text)
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
    print
    print "Please specify type of installation"
    print "   PANDA_SYS=%s" % os.environ['PANDA_SYS']
    print " 1. Install to $PANDA_SYS"
    print "      all files in $PANDA_SYS will be erased first and new ones will"
    print "      be installed to the same dir"
    print " 2. Install to a new dir"
    print "      new files will be installed to somewhere else than $PANDA_SYS"
    print " 3. Patch (not recommended)"
    print "      existing files in $PANDA_SYS will be patched with new ones"
    print " 4. RPM installation"
    print "      install RPM. sudo is required"
    print
    while True:
        str = raw_input("Enter 1-4 : ")
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
                newPrefix = raw_input("Enter new location (TAB for autocomplete): ")
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
    # get tarballa
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
        commands.getoutput('rm %s' % packageName)    
        return False
    # install
    if not rpmInstall:
        # expand
        status,output = commands.getstatusoutput('tar xvfz %s' % packageName)
        status %= 255    
        if verbose:
            tmpLog.debug(status)
            tmpLog.debug(output)
        if status != 0:
            tmpLog.error('failed to expand tarball : %s' % status)        
            # delete dirs just in case
            commands.getoutput('rm -rf panda-client-%s' % latestVer)    
            return False
        # delete tarball
        commands.getoutput('rm %s' % packageName)
        # save current dir
        currentDir = os.path.realpath(os.getcwd())
        # keep old release
        if cleanInstall:
            tmpLog.info('keep old version in %s.back' % os.environ['PANDA_SYS'])
            backUpDir = '%s.back' % os.environ['PANDA_SYS']
            status,output = commands.getstatusoutput('rm -rf %s; mv %s %s' % \
                                                     (backUpDir,os.environ['PANDA_SYS'],backUpDir))
            if status != 0:
                tmpLog.error(output)
                tmpLog.error('failed to keep old version')
                # delete dirs
                commands.getoutput('rm -rf panda-client-%s' % latestVer)    
                return False
        # install
        result = True
        os.chdir('panda-client-%s' % latestVer)
        status,output = commands.getstatusoutput('python setup.py install --prefix=%s' % newPrefix)
        if verbose:
            tmpLog.debug(output)
            tmpLog.debug(status)
        os.chdir(currentDir)
        status %= 255
        if status != 0:
            tmpLog.error('failed to install panda-client : %s' % status)
            # recover old one
            commands.getoutput('rm -rf %s' % os.environ['PANDA_SYS'])
            commands.getoutput('mv %s.back %s' % (os.environ['PANDA_SYS'],os.environ['PANDA_SYS']))        
            result = False
        # cleanup
        commands.getoutput('rm -rf panda-client-%s' % latestVer)
    else:
        # rpm install
        result = True
        newPrefix = ''
        com = 'sudo rpm -Uvh %s' % packageName
        print com
        status = os.system(com)
        status %= 255    
        if status != 0:
            tmpLog.error('failed to install rpm : %s' % status)
            result = False
        # cleanup
        commands.getoutput('rm -rf %s' % packageName)
    # return
    if result:
        tmpLog.info('completed')
        tmpLog.info("please do 'source %s/etc/panda/panda_setup.[c]sh'" % newPrefix)
    return result
                    

# check direct access
def isDirectAccess(site,usingRAW=False,usingTRF=False,usingARA=False):
    return Client.isDirectAccess(site,usingRAW,usingTRF,usingARA)


# check destination SE
def checkDestSE(destSEs,dsName,verbose):
    # check destSE
    if destSEs == '':
        return True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check length
    maxLength = 250
    if len(destSEs) > maxLength:
        tmpLog.error("destSE is too long (%s) and must be less than %s" % (len(destSEs),maxLength))
        return False
    # get DN
    tmpBufferFile,tmpBufferName = tempfile.mkstemp()
    commands.getoutput('%s grid-proxy-info -identity > %s 2>/dev/null' % \
                               (Client._getGridSrc(),tmpBufferName))
    tmpBufferFH = os.fdopen(tmpBufferFile,'r')
    tmpDN = tmpBufferFH.read()
    tmpBufferFH.close()
    os.remove(tmpBufferName)
    # set X509_CERT_DIR
    if not os.environ.has_key('X509_CERT_DIR') or os.environ['X509_CERT_DIR'] == '':
        os.environ['X509_CERT_DIR'] = Client._x509_CApath() 
    # check with DaTRI
    from datriHandler import datriHandler
    tmpDaHandler = datriHandler(type='pathena')
    for destSE in destSEs.split(','):
        tmpDaHandler.setParameters(data_pattern=dsName,
                                   site=destSE,
                                   userid=tmpDN)
        tmpLog.info("checking with DaTRI for --destSE=%s" % destSE)
        sStat,dOut = tmpDaHandler.checkData()
        if sStat != 0:
            errMsg  = "%s\n" % dOut
            errMsg += "ErrorCode=%s parameters=(DS:%s,site:%s,DN:%s)" % \
                      (sStat,dsName,destSE,tmpDN)
            tmpLog.error(errMsg)
            return False
        if verbose:
            tmpLog.debug("%s %s" % (sStat,dOut))
    # return
    return True


# disable redundant transfer
def disableRedundantTransfer(job,transferredDS):
    # no pattern
    if transferredDS == '':
        return
    # DQ2 free
    if job.destinationSE == 'local':
        return
    # get patterns
    patterns = []
    for tmpItem in transferredDS.split(','):
        if tmpItem != '':
            # wild card
            tmpItem = tmpItem.replace('*','.*')
            # append
            patterns.append(tmpItem)
    # change destinationSE
    for tmpFile in job.Files:
        if tmpFile.type in ['log','output']:
            # check patterns
            matchFlag = False
            for tmpPatt in patterns:
                if re.search(tmpPatt,tmpFile.dataset) != None:
                    matchFlag = True
                    break
            # disable
            if not matchFlag:
                tmpFile.destinationSE = job.computingSite
    # return            
    return

    
# run pathena recursively
def runPathenaRec(runConfig,missList,tmpDir,fullExecString,nfiles,inputFileMap,site,crossSite,archiveName,
                  removedDS,inDS,goodRunListXML,eventPickEvtList,devidedByGUID,dbRelease,jobsetID,trfStr,
                  singleLine,isMissing,eventPickRunEvtDat,useTagParentLookup,verbose):
    anotherTry = True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # keep original args
    if not '--panda_origFullExecString=' in fullExecString:
        fullExecString += (" --panda_origFullExecString=" + urllib.quote(fullExecString))
    # nfiles
    if nfiles != 0:
        if nfiles > len(inputFileMap):
            fullExecString = re.sub('--nFiles\s*=*\d+',
                                    '--nFiles=%s' % (nfiles-len(inputFileMap)),
                                    fullExecString)
        else:
            anotherTry = False
    # nSkipFiles : set 0 since files are skipped in the first try
    fullExecString = re.sub('--nSkipFiles\s*=*\d+',
                            '--nSkipFiles=0',
                            fullExecString)
    # decrement crossSite counter
    fullExecString = re.sub(' --crossSite\s*=*\d+','',fullExecString)
    fullExecString += ' --crossSite=%s' % crossSite
    # don't reuse site
    match = re.search('--excludedSite\s*=*[^ "]+',fullExecString)
    if match != None:
        # append site
        fullExecString = re.sub(match.group(0),'%s,%s' % (match.group(0),site),fullExecString)
    else:
        # add option
        fullExecString += ' --excludedSite=%s' % site
    # remove datasets
    if removedDS != []:
        match = re.search('--removedDS\s*=*[^ "]+',fullExecString)
        if match != None:
            # remove the option
            fullExecString = re.sub('"*--removedDS\s*=*[^ "]+"*','',fullExecString)
        # add option
        tmpDsStr = ''
        for tmpItem in removedDS:
            tmpDsStr += '%s,' % tmpItem
        tmpDsStr = tmpDsStr[:-1]    
        fullExecString += ' --removedDS=%s' % tmpDsStr
    # remove --fileList
    fullExecString = re.sub('"*--fileList\s*=*[^ "]+"*','',fullExecString)
    # set list of input files
    inputTmpfile = '%s/intmp.%s' % (tmpDir,MiscUtils.wrappedUuidGen())
    iFile = open(inputTmpfile,'w')
    for tmpMiss in missList:
        iFile.write(tmpMiss+'\n')
    iFile.close()
    fullExecString = re.sub(' "*--inputFileList\s*=*[^ "]+"*','',fullExecString)
    fullExecString += ' --inputFileList=%s' % inputTmpfile
    # set inDS to avoid redundant AMI lookup for GRL
    if inDS != '' and goodRunListXML != '' and not '--panda_inDS' in fullExecString:
        fullExecString += ' --panda_inDS=%s' % inDS
    # set inDS to avoid redundant ELSSI lookup for event picking
    if inDS != '' and eventPickEvtList != '' and not '--panda_inDSForEP' in fullExecString:
        fullExecString += ' --panda_inDSForEP=%s' % inDS
        if eventPickRunEvtDat != '' and not '--panda_eventPickRunEvtDat' in fullExecString:
            fullExecString += ' --panda_eventPickRunEvtDat=%s' % eventPickRunEvtDat
    # set DBR
    if dbRelease != '' and not '--panda_dbRelease' in fullExecString:
        fullExecString += ' --panda_dbRelease=%s' % dbRelease
    # suppress repetitive message
    if not '--panda_suppressMsg' in fullExecString:
        fullExecString += ' --panda_suppressMsg'
    # source name
    if not '--panda_srcName' in fullExecString:
        fullExecString += ' --panda_srcName=%s' % archiveName
    # server URL
    if not '--panda_srvURL' in fullExecString:
        fullExecString += ' --panda_srvURL=%s,%s' % (Client.baseURL,Client.baseURLSSL)
    if '--panda_cacheSrvURL' in fullExecString:
        fullExecString = re.sub('"*--panda_cacheSrvURL=[^ ]+','',fullExecString)
    fullExecString += ' --panda_cacheSrvURL=%s,%s' % (Client.baseURLCSRV,Client.baseURLCSRVSSL)
    # devidedByGUID
    if devidedByGUID and not '--panda_devidedByGUID' in fullExecString:
        fullExecString += ' --panda_devidedByGUID'
    # run config
    conTmpfile = ''
    if not '--panda_runConfig' in fullExecString:
        conTmpfile = '%s/conftmp.%s' % (tmpDir,MiscUtils.wrappedUuidGen())
        cFile = open(conTmpfile,'w')
        pickle.dump(runConfig,cFile)
        cFile.close()
        fullExecString += ' --panda_runConfig=%s' % conTmpfile
    # set jobsetID
    if not '--panda_jobsetID' in fullExecString and not jobsetID in [None,'NULL',-1]:
        fullExecString += ' --panda_jobsetID=%s' % jobsetID

    # TAG parent
    if useTagParentLookup and not '--panda_tagParentFile' in fullExecString:
        tmpTagParentFile = dumpTagParentInfo(tmpDir)
        fullExecString += ' --panda_tagParentFile=%s' % tmpTagParentFile
    # trf string
    if not '--panda_trf' in fullExecString and trfStr != '':
        fullExecString += ' --panda_trf=%s' % urllib.quote(trfStr)
    # one liner
    if not '--panda_singleLine' in fullExecString and singleLine != '':
        fullExecString += ' --panda_singleLine=%s' % urllib.quote(singleLine)
    # jobOs with fullpath
    if not '--panda_fullPathJobOs' in fullExecString and AthenaUtils.fullPathJobOs != {}:
        fullExecString += ' --panda_fullPathJobOs=%s' % AthenaUtils.convFullPathJobOsToStr()
    # run pathena
    if anotherTry:
        if isMissing:
            tmpLog.info("trying other sites for the missing files")
        com = 'pathena ' + fullExecString
        if verbose:
            tmpLog.debug(com)
        status = os.system(com)
        # delete tmp files
        commands.getoutput('rm -f %s' % inputTmpfile)
        commands.getoutput('rm -f %s' % conTmpfile)            
        # exit
        sys.exit(status)
    # exit
    sys.exit(0)
    
    
# run prun recursively
def runPrunRec(missList,tmpDir,fullExecString,nFiles,inputFileMap,site,crossSite,archiveName,
               removedDS,inDS,goodRunListXML,eventPickEvtList,dbRelease,jobsetID,
               bexecStr,execStr,verbose):
    anotherTry = True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # keep original args
    if not '--panda_origFullExecString=' in fullExecString:
        fullExecString += (" --panda_origFullExecString=" + urllib.quote(fullExecString))
    # nfiles
    if nFiles != 0:
        if nFiles > len(inputFileMap):
            fullExecString = re.sub('--nFiles\s*=*\d+',
                                    '--nFiles=%s' % (nFiles-len(inputFileMap)),
                                    fullExecString)
        else:
            anotherTry = False
    # nSkipFiles : set 0 since files are skipped in the first try
    fullExecString = re.sub('--nSkipFiles\s*=*\d+',
                            '--nSkipFiles=0',
                            fullExecString)
    # decrement crossSite counter
    fullExecString = re.sub(' --crossSite\s*=*\d+','',fullExecString)
    fullExecString += ' --crossSite=%s' % crossSite
    # don't reuse site
    match = re.search('--excludedSite\s*=*[^ "]+',fullExecString)
    if match != None:
        # append site
        fullExecString = re.sub(match.group(0),'%s,%s' % (match.group(0),site),fullExecString)
    else:
        # add option
        fullExecString += ' --excludedSite=%s' % site
    # remove datasets
    if removedDS != []:
        match = re.search('--removedDS\s*=*[^ "]+',fullExecString)
        if match != None:
            # remove the option
            fullExecString = re.sub('"*--removedDS\s*=*[^ "]+"*','',fullExecString)
        # add option
        tmpDsStr = ''
        for tmpItem in removedDS:
            tmpDsStr += '%s,' % tmpItem
        tmpDsStr = tmpDsStr[:-1]    
        fullExecString += ' --removedDS=%s' % tmpDsStr
    # set list of input files
    inputTmpfile = '%s/intmp.%s' % (tmpDir,MiscUtils.wrappedUuidGen())
    iFile = open(inputTmpfile,'w')
    for tmpMiss in missList:
        iFile.write(tmpMiss+'\n')
    iFile.close()
    fullExecString = re.sub(' "*--inputFileList\s*=*[^ "]+"*','',fullExecString)
    fullExecString += ' --inputFileList=%s' % inputTmpfile
    # set inDS to avoid redundant AMI lookup for GRL
    if inDS != '' and goodRunListXML != '' and not '--panda_inDS' in fullExecString:
        fullExecString += ' --panda_inDS=%s' % inDS
    # set inDS to avoid redundant ELSSI lookup for event picking
    if inDS != '' and eventPickEvtList != '' and not '--panda_inDSForEP' in fullExecString:
        fullExecString += ' --panda_inDSForEP=%s' % inDS
    # suppress repetitive message
    if not '--panda_suppressMsg' in fullExecString:
        fullExecString += ' --panda_suppressMsg'
    # source name
    if archiveName != '' and not '--panda_srcName' in fullExecString:
        fullExecString += ' --panda_srcName=%s' % archiveName
    # server URL
    if not '--panda_srvURL' in fullExecString:
        fullExecString += ' --panda_srvURL=%s,%s' % (Client.baseURL,Client.baseURLSSL)
    if '--panda_cacheSrvURL' in fullExecString:
        fullExecString = re.sub('"*--panda_cacheSrvURL=[^ ]+','',fullExecString)
    fullExecString += ' --panda_cacheSrvURL=%s,%s' % (Client.baseURLCSRV,Client.baseURLCSRVSSL)
    # set DBR
    if dbRelease != '' and not '--panda_dbRelease' in fullExecString:
        fullExecString += ' --panda_dbRelease=%s' % dbRelease
    # set jobsetID
    if not '--panda_jobsetID' in fullExecString and not jobsetID in [None,'NULL',-1]:
                fullExecString += ' --panda_jobsetID=%s' % jobsetID
    # bexec string
    if not '--panda_bexec' in fullExecString and bexecStr != '':
        fullExecString += ' --panda_bexec=%s' % urllib.quote(bexecStr)
    # exec string
    if not '--panda_exec' in fullExecString and execStr != '':
        fullExecString += ' --panda_exec=%s' % urllib.quote(execStr)
    # run prun
    if anotherTry:
        tmpLog.info("trying other sites for the missing files")
        com = 'prun ' + fullExecString
        if verbose:
            tmpLog.debug(com)
        status = os.system(com)
        # delete tmp files
        commands.getoutput('rm -f %s' % inputTmpfile)
        # exit
        sys.exit(status)
    # exit
    sys.exit(0)



# run brokerage for composite site
def runBrokerageForCompSite(siteIDs,releaseVer,cacheVer,verbose,cmtConfig=None,
                            memorySize=0,useDirectIO=False,siteGroup=None,
                            maxCpuCount=-1,rootVer=''):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # run brokerage
    status,outMap = Client.runBrokerage(siteIDs,releaseVer,verbose=verbose,trustIS=True,cacheVer=cacheVer,loggingFlag=True,
                                        cmtConfig=cmtConfig,memorySize=memorySize,useDirectIO=useDirectIO,
                                        siteGroup=siteGroup,maxCpuCount=maxCpuCount,rootVer=rootVer)
    if status != 0:
        tmpLog.error('failed to run brokerage for composite site: %s' % outMap)
        sys.exit(EC_Config)
    out = outMap['site']    
    if out.startswith('ERROR :'):
        tmpLog.error(out + '\nbrokerage failed')
        sys.exit(EC_Config)
    if not Client.PandaSites.has_key(out):
        tmpLog.error('brokerage gave wrong PandaSiteID:%s' % out)
        sys.exit(EC_Config)
    return out,outMap['logInfo']

    
# get list of datasets and files by list of runs/events
def getDSsFilesByRunsEvents(curDir,runEventTxt,dsType,streamName,dsPatt='',verbose=False,amiTag=""):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # set X509_USER_PROXY
    if not os.environ.has_key('X509_USER_PROXY') or os.environ['X509_USER_PROXY'] == '':
        os.environ['X509_USER_PROXY'] = Client._x509()
    # set 
    from eventLookupClient import eventLookupClient
    elssiIF = eventLookupClient()
    elssiIF.debug = verbose
    # open run/event txt
    if '/' in runEventTxt:
        tmpLog.error('%s must be in the current directory' % runEventTxt.split('/')[-1])
        sys.exit(EC_Config)
    runevttxt = open('%s/%s' % (curDir,runEventTxt))
    # convert dsType to Athena stream ref
    if dsType == 'AOD':
        streamRef = 'StreamAOD_ref'
    elif dsType == 'ESD':
        streamRef = 'StreamESD_ref'
    elif dsType == 'RAW':
        streamRef = 'StreamRAW_ref'
    else:
        errStr  = 'invalid data type %s for event picking. ' % dsType
        errStr += ' Must be one of AOD,ESD,RAW'
        tmpLog.error(errStr)
        sys.exit(EC_Config)
    tmpLog.info('getting dataset names and LFNs from Event Lookup service')
    # read
    runEvtList = []
    guids = []
    guidRunEvtMap = {}
    runEvtGuidMap = {}    
    for line in runevttxt:
        items = line.split()
        if len(items) != 2:
            continue
        runNr,evtNr = items
        runEvtList.append([runNr,evtNr])
    # close
    runevttxt.close()
    # bulk lookup
    nEventsPerLoop = 500
    iEventsTotal = 0
    while iEventsTotal < len(runEvtList):
        tmpRunEvtList = runEvtList[iEventsTotal:iEventsTotal+nEventsPerLoop]
        iEventsTotal += nEventsPerLoop
        for tmpItem in tmpRunEvtList:
            sys.stdout.write('.')
        sys.stdout.flush()
        # check with ELSSI
        if streamName == '':
            guidListELSSI = elssiIF.doLookup(tmpRunEvtList,tokens=streamRef,
                                             amitag=amiTag,extract=True)
        else:
            guidListELSSI = elssiIF.doLookup(tmpRunEvtList,stream=streamName,tokens=streamRef,
                                             amitag=amiTag,extract=True)
        if guidListELSSI == None or len(guidListELSSI) == 0:
            if not verbose:
                print
            errStr = ''    
            for tmpLine in elssiIF.output:
                errStr += tmpLine + '\n'
            tmpLog.error(errStr)    
            errStr = "failed to get GUID from Event Lookup service"
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        # check attribute
        attrNames, attrVals = guidListELSSI
        def getAttributeIndex(attr):
            for tmpIdx,tmpAttrName in enumerate(attrNames):
                if tmpAttrName.strip() == attr:
                    return tmpIdx
            tmpLog.error("cannot find attribute=%s in %s provided by Event Lookup service" % \
                         (attr,str(attrNames)))
            sys.exit(EC_Config)
        # get index
        indexEvt = getAttributeIndex('EventNumber')
        indexRun = getAttributeIndex('RunNumber')
        indexTag = getAttributeIndex(streamRef)
        # check events
        for runNr,evtNr in tmpRunEvtList:
            paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,streamName)
            if verbose:
                tmpLog.debug(paramStr)
            # collect GUIDs    
            tmpguids = []
            for attrVal in attrVals:
                if runNr == attrVal[indexRun] and evtNr == attrVal[indexEvt]:
                    tmpGuid = attrVal[indexTag]
                    # check non existing
                    if tmpGuid == 'NOATTRIB':
                        continue
                    if not tmpGuid in tmpguids:
                        tmpguids.append(tmpGuid)
            # not found            
            if tmpguids == []:
                if not verbose:
                    print
                errStr = "no GUIDs were found in Event Lookup service for %s" % paramStr
                tmpLog.error(errStr)
                sys.exit(EC_Config)
            # append
            for tmpguid in tmpguids:
                if not tmpguid in guids:
                    guids.append(tmpguid)
                    guidRunEvtMap[tmpguid] = []
                guidRunEvtMap[tmpguid].append((runNr,evtNr))
            runEvtGuidMap[(runNr,evtNr)] = tmpguids
            if verbose:
                tmpLog.debug("   GUID:%s" % str(tmpguids))
    if not verbose:
        print
    # convert to dataset names and LFNs
    dsLFNs,allDSMap = Client.listDatasetsByGUIDs(guids,dsPatt,verbose)
    if verbose:
        tmpLog.debug(dsLFNs)
    # check duplication
    for runNr,evtNr in runEvtGuidMap.keys():
        tmpLFNs = []
        tmpAllDSs = {}
        for tmpguid in runEvtGuidMap[(runNr,evtNr)]:
            if dsLFNs.has_key(tmpguid):
                tmpLFNs.append(dsLFNs[tmpguid])
            else:
                tmpAllDSs[tmpguid] = allDSMap[tmpguid]
                if guidRunEvtMap.has_key(tmpguid):
                    del guidRunEvtMap[tmpguid]
        # empty        
        if tmpLFNs == []:
            paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,streamName)                        
            errStr = "--eventPickDS='%s' didn't pick up a file for %s\n" % (dsPatt,paramStr)
            for tmpguid,tmpAllDS in tmpAllDSs.iteritems():
                errStr += "    GUID:%s dataset:%s\n" % (tmpguid,str(tmpAllDS))
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        # duplicated    
        if len(tmpLFNs) != 1:
            paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,streamName)            
            errStr = "multiple LFNs %s were found in ELSSI for %s. Please set --eventPickDS and/or --eventPickStreamName and/or --eventPickAmiTag correctly" \
                     % (str(tmpLFNs),paramStr)
            tmpLog.error(errStr)
            sys.exit(EC_Config)
    # return
    return dsLFNs,guidRunEvtMap


# get mapping between TAG and parent GUIDs
mapTAGandParentGUIDs = {}
def getMapTAGandParentGUIDs(dsName,tagQuery,streamRef,verbose=False):
    # remove _tidXYZ
    dsNameForLookUp = re.sub('_tid\d+(_\d+)*$','',dsName)
    # reuse
    if mapTAGandParentGUIDs.has_key(dsNameForLookUp):
        return mapTAGandParentGUIDs[dsNameForLookUp]
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # set X509_USER_PROXY
    if not os.environ.has_key('X509_USER_PROXY') or os.environ['X509_USER_PROXY'] == '':
        os.environ['X509_USER_PROXY'] = Client._x509()
    # set
    from countGuidsClient import countGuidsClient
    tagIF = countGuidsClient()
    tagIF.debug = verbose
    tagResults = tagIF.countGuids(dsNameForLookUp,tagQuery,streamRef+',StreamTAG_ref')
    if tagResults == None:
        if not verbose:
            print
        errStr = ''    
        for tmpLine in tagIF.output:
            if tmpLine == '\n':
                continue
            errStr += tmpLine
        tmpLog.error(errStr)    
        errStr2  = "invalid return from Event Lookup service. "
        if "No collection in the catalog matches the dataset name" in errStr:
            errStr2 += "Note that only merged TAG is uploaded to the TAG DB, "
            errStr2 += "so you need to use merged TAG datasets (or container) for inDS. "
            errStr2 += "If this is already the case please contact atlas-event-metadata@cern.ch"            
        tmpLog.error(errStr2)
        sys.exit(EC_Config)
    # empty
    if not tagResults[0]:
        errStr = "No GUIDs found for %s" % dsName
        tmpLog.error(errStr)
        sys.exit(EC_Config)
    # collect
    retMap = {}
    for guidCount,guids in tagResults[1]:
        if verbose:
            print guidCount,guids
        parentGUID,tagGUID = guids
        # append TAG GUID
        if not retMap.has_key(tagGUID):
            retMap[tagGUID] = {}
        # append parent GUID and the number of selected events
        if retMap[tagGUID].has_key(parentGUID):
            errStr = "GUIDs=%s is duplicated" % parentGUID
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        retMap[tagGUID][parentGUID] = long(guidCount)
    # keep to avoid redundant lookup    
    mapTAGandParentGUIDs[dsNameForLookUp] = retMap
    # return
    return retMap


# get TAG files and parent DS/files using TAG query
tagParentInfo = {}
parentLfnToTagMap = {}
def getTagParentInfoUsingTagQuery(tagDsStr,tagQuery,streamRef,verbose):
    # avoid redundant lookup
    global tagParentInfo
    global parentLfnToTagMap
    if tagParentInfo != {}:
        return tagParentInfo,parentLfnToTagMap
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # set empty if Query is undefined
    if tagQuery == False:
        tagQuery = ''
    # loop over all tags
    tmpLog.info('getting parent dataset names and LFNs from TAG DB using EventSelector.Query="%s"' % tagQuery)
    for tagDS in tagDsStr.split(','):
        if tagDS.endswith('/'):
            # get elements in container
            elements = Client.getElementsFromContainer(tagDS,verbose)
        else:
            elements = [tagDS,]
        # loop over all elemets
        for dsName in elements:
            if not verbose:
                sys.stdout.write('.')
                sys.stdout.flush()
            else:
                tmpLog.debug("DS=%s Query=%s Ref:%s" % (dsName,tagQuery,streamRef))
            guidMap = getMapTAGandParentGUIDs(dsName,tagQuery,streamRef,verbose)
            # convert TAG GUIDs to LFNs
            tmpRetMap,tmpAllMap = Client.listDatasetsByGUIDs(guidMap.keys(),'',verbose)
            for tagGUID in guidMap.keys():
                # not found
                if not tmpRetMap.has_key(tagGUID):
                    errStr = 'TAG GUID=%s not found in DQ2' % tagGUID 
                    tmpLog.error(errStr)
                    sys.exit(EC_Config)
                # append
                tagElementDS,tagLFN = tmpRetMap[tagGUID]
                # convert parent GUIDs to LFNs
                tmpParentRetMap,tmpParentAllMap = Client.listDatasetsByGUIDs(guidMap[tagGUID].keys(),'',verbose)
                for parentGUID in guidMap[tagGUID].keys():
                    # not found
                    if not tmpParentRetMap.has_key(parentGUID):
                        errStr = '%s GUID=%s not found in DQ2' % (re.sub('_ref$','',streamRef),parentGUID)
                        tmpLog.error(errStr)
                        sys.exit(EC_Config)
                    # append parent dataset
                    tmpParentDS,tmpParentLFN = tmpParentRetMap[parentGUID] 
                    if not tagParentInfo.has_key(tmpParentDS):
                        tagParentInfo[tmpParentDS] = {'tagToParentLFNmap':{},'tagDS':tagDS,'tagElementDS':tagElementDS}
                    # append tag LFN
                    if not tagParentInfo[tmpParentDS]['tagToParentLFNmap'].has_key(tagLFN):
                        tagParentInfo[tmpParentDS]['tagToParentLFNmap'][tagLFN] = []
                    # append parent LFN
                    if not tmpParentLFN in tagParentInfo[tmpParentDS]['tagToParentLFNmap'][tagLFN]:
                        tagParentInfo[tmpParentDS]['tagToParentLFNmap'][tagLFN].append(tmpParentLFN)
                    # append parent/TAG LFN map
                    parentLfnToTagMap[tmpParentLFN] = {'lfn':tagLFN,'tagDS':tagDS,'nEvents':guidMap[tagGUID][parentGUID]}
        if not verbose:
            print
    # empty
    if tagParentInfo == {}:
        errStr = 'No events selected from TAG DB. Please make sure that you use proper EventSelector.Query and inDS'
        tmpLog.error(errStr)
        sys.exit(EC_Config)
    # return
    return tagParentInfo,parentLfnToTagMap


# dump TAG parent Info
def dumpTagParentInfo(tmpDir):
    tmpFileName = '%s/tagparenttmp.%s' % (tmpDir,MiscUtils.wrappedUuidGen())
    cFile = open(tmpFileName,'w')
    pickle.dump((tagParentInfo,parentLfnToTagMap),cFile)
    cFile.close()
    return tmpFileName


# load TAG parent Info
def loadTagParentInfo(tmpFileName):
    global tagParentInfo
    global parentLfnToTagMap
    cFile = open(tmpFileName)
    tagParentInfo,parentLfnToTagMap = pickle.load(cFile)
    cFile.close()
    return tagParentInfo,parentLfnToTagMap


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
        if re.search(unPatt,tmpDs) != None:
            unMergedDs += '%s,' % tmpDs
    unMergedDs = unMergedDs[:-1]
    # return
    if unMergedDs != '':
        # get logger
        tmpLog = PLogger.getPandaLogger()
        msg = "%s is unmerged AOD dataset which is not distributed. Please use mc*.merge.AOD.* instead\n" % unMergedDs
        print
        tmpLog.warning(msg)
    return 


# check location consistency between outDS and libDS 
def checkLocationConsistency(outDSlocations,libDSlocations):
    convOutDS = []
    # convert DQ2 ID to aggregated name
    for outDSlocation in outDSlocations:
        tmpLocation = Client.convSrmV2ID(outDSlocation)
        convOutDS.append(tmpLocation)
    # loop over all libDS locations
    for libDSlocation in libDSlocations:
        tmpLocation = Client.convSrmV2ID(libDSlocation)
        if tmpLocation in convOutDS:
            return
    # inconsistent    
    tmpConvIDsOut = []
    for outDSlocation in outDSlocations:
        tmpConvIDsOut += Client.convertDQ2toPandaIDList(outDSlocation)
    tmpConvIDsLib = []
    for libDSlocation in libDSlocations:
        tmpConvIDsLib += Client.convertDQ2toPandaIDList(libDSlocation)
    tmpConvIDsOut.sort()
    tmpConvIDsLib.sort()    
    tmpLog = PLogger.getPandaLogger()
    msg = "Location mismatch. outDS exists at %s while libDS exist at %s" % \
          (tmpConvIDsOut[0],tmpConvIDsLib[0])
    tmpLog.error(msg)
    sys.exit(EC_Config)    
    return 


# get real dataset name by ignoring case sensitivty
def getRealDatasetName(outDS,tmpDatasets):
    for tmpDataset in tmpDatasets.keys():
        if outDS.lower() == tmpDataset.lower():
            return tmpDataset
    return outDS    


limit_maxNumInputs = 200
limit_maxLfnLength = 150

isFirstJobSpecForCheck = True

# check job spec
def checkJobSpec(job):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    global isFirstJobSpecForCheck
    # loop over all files
    nInputFiles = 0
    for tmpFile in job.Files:
        # the number of input files
        if tmpFile.type == 'input' and (not tmpFile.lfn.endswith('.lib.tgz')) \
               and re.search('DBRelease-.*\.tar\.gz$',tmpFile.lfn) == None:
            nInputFiles += 1
        # check LFN length    
        if tmpFile.type == 'output' and len(tmpFile.lfn) > limit_maxLfnLength:
            errMsg =  "Filename %s is too long (%s chars). It must be less than %s. Please use a shorter name" \
                     % (tmpFile.lfn,len(tmpFile.lfn),limit_maxLfnLength)
            tmpLog.error(errMsg)
            sys.exit(EC_Config)
        # check NG chars     
        if isFirstJobSpecForCheck:
            if tmpFile.type == 'output':
                # $ is allowed for $PANDAID/$JOBSETID
                for tmpChar in ['%','|',';','>','<','?','\'','"','(',')','@','*',':',
                                '=','&','^','#','\\','@','[',']','{','}','`']:
                    if tmpChar in tmpFile.lfn:
                        errMsg = 'invalid character "%s" is used in output LFN %s' % (tmpChar,tmpFile.lfn)
                        tmpLog.error(errMsg)
                        sys.exit(EC_Config)
    # check the number of input files                                                    
    maxNumInputs = limit_maxNumInputs
    if nInputFiles > maxNumInputs:
        errMsg =  "Too many input files (%s files) in a sub job. " % nInputFiles
        errMsg += "Please reduce that to less than %s. " % maxNumInputs
        errMsg += "If you are using prun you may try --maxNFilesPerJob and --writeInputToTxt"
        tmpLog.error(errMsg)
        sys.exit(EC_Config)
    # NG char check with the first JobSpec is enough
    if job.prodSourceLabel == 'user':
        isFirstJobSpecForCheck = False


# get prodDBlock
def getProdDBlock(job,inDS):
    # no input
    if inDS == '':
        return 'NULL'
    # single dataset
    if re.search(',',inDS) == None:
        return inDS
    # max lenght
    maxLength = 200
    # less than max
    if len(inDS) < maxLength:
        return inDS
    # get real input datasets
    inDSList = []
    for tmpFile in job.Files:
        # only input
        if tmpFile.type != 'input':
            continue
        # ignore lib.tgz
        if tmpFile.lfn.endswith('.lib.tgz'):
            continue
        # ignore DBR
        if tmpFile.lfn.startswith('DBRelease'):
            continue
        # append
        if not tmpFile.dataset in inDSList:
            inDSList.append(tmpFile.dataset)
    # concatenate
    strInDS = ''
    for tmpInDS in inDSList:
        strInDS += "%s," % tmpInDS
    strInDS = strInDS[:-1]    
    # truncate    
    if len(strInDS) > maxLength:
        strInDS = strInDS[:maxLength] + '...'
    # empty
    if strInDS == '':
        return 'NULL'
    return strInDS


# get token for CHIRP
tokenForCHIRP = None
def getTokenForCHIRP(fileName,serverName,useCachedToken=True,useGO=False):
    global tokenForCHIRP
    if useCachedToken and tokenForCHIRP != None:
        return tokenForCHIRP
    if not useGO:
        tmpToken = 'chirp^%s^/%s^-d chirp' % (serverName,fileName.split('.')[1])
    else:
        tmpToken = 'globusonline^%s^/%s^-d globusonline' % (serverName,fileName.split('.')[1])
    if useCachedToken:
        tokenForCHIRP = tmpToken
    return tmpToken    
    

# set CHIRP token
def setCHIRPtokenToOutput(job,serverName,useGO=False):
    if serverName == '':
        return
    token = None
    for tmpFile in job.Files:
        if tmpFile.type in ['output','log']:
            # get token
            if token == None:
                token = getTokenForCHIRP(tmpFile.lfn,serverName,useGO=useGO)
            # set token
            tmpFile.dispatchDBlockToken = token
            
                          
# execute pathena/prun with modify command-line paramters
def execWithModifiedParams(jobs,newOpts,verbose,newSite=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # remove --
    tmpOpts = {}
    for tmpKey in newOpts.keys():
        newKey = re.sub('^--','',tmpKey)
        tmpOpts[newKey] = newOpts[tmpKey]
    newOpts = tmpOpts
    # use new site or not
    if newSite:
        # look for excludedSite
        matchEx = re.search('--excludedSite[ =]+\s*([^ "]+)',jobs[0].metadata)
        # set excludedSite
        if newOpts.has_key('excludedSite'):
            newOpts['excludedSite'] += ',%s' % jobs[0].computingSite
        else:
            if matchEx != None:
                newOpts['excludedSite'] = '%s,%s' % (matchEx.group(1),jobs[0].computingSite)
            else:        
                newOpts['excludedSite'] = '%s' % jobs[0].computingSite
    else:
        # send to the old site
        newOpts['site'] = jobs[0].computingSite
    # set provenanceID
    newOpts['provenanceID'] = jobs[0].jobExecutionID
    # get inputs
    inDSs = []
    inFiles = []
    for job in jobs:
        for tmpFile in job.Files:
            if tmpFile.type == 'input' and (not tmpFile.lfn.endswith('.lib.tgz')) \
               and re.search('^DBRelease-.*\.tar\.gz$',tmpFile.lfn) == None:
                # ignore seconday files
                if tmpFile.destinationDBlockToken == 'noshadow':
                    continue
                if not tmpFile.dataset in inDSs:
                    inDSs.append(tmpFile.dataset)
                if not tmpFile.lfn in inFiles:
                    inFiles.append(tmpFile.lfn)
    # modify command-line params
    commandOps = jobs[0].metadata
    # remove opts which conflict with --inDS
    for removedOpt in ['goodRunListXML','eventPickEvtList','inputFileList',
                       'inDS','retryID','site']:
        commandOps = re.sub('\"*--%s[ =]+\s*[^ ]+' % removedOpt," ",commandOps)
    # set inDS
    inputTmpfileName = ''
    if inDSs != []:
        strInDS = ''
        for inDS in inDSs:
            strInDS += '%s,' % inDS
        strInDS = strInDS[:-1]    
        commandOps += ' --inDS %s' % strInDS
        # set inputFileList
        if not newOpts.has_key('inputFileList'):
            inputTmpfileName = 'intmp.%s' % MiscUtils.wrappedUuidGen()
            inputTmpfile = open(inputTmpfileName,'w')
            for inFile in inFiles:
                inputTmpfile.write(inFile+'\n')
            inputTmpfile.close()
            commandOps += ' --inputFileList %s' % inputTmpfileName
    # modify options
    for tmpOpt,tmpArg in newOpts.iteritems():
        if tmpArg in ['',None]:
            if len(tmpOpt) == 1:
                commandOps += ' -%s' % tmpOpt
            else:
                commandOps += ' --%s' % tmpOpt
        elif re.search("--%s( |=)" % tmpOpt,commandOps) == None:
            commandOps += ' --%s %s' % (tmpOpt,tmpArg)
        else:    
            commandOps = re.sub("\"*--%s[ =]+\s*[^ ]+" % tmpOpt,"--%s %s" % (tmpOpt,tmpArg),commandOps)
    if verbose:
        commandOps += ' -v'
    newCommand = "%s %s" %  (jobs[0].processingType,commandOps)
    if verbose:
        tmpLog.debug(newCommand)
    # execute
    comStat = os.system(newCommand)
    # remove
    if inputTmpfileName != '':
        commands.getoutput('rm -f %s' % inputTmpfileName)
    # return
    return comStat


# read jobID
def readJobDefID():
    jobDefinitionID = 1
    jobid_file = '%s/pjobid.dat' % os.environ['PANDA_CONFIG_ROOT']
    if os.path.exists(jobid_file):
        try:
            # read line
            tmpJobIdFile = open(jobid_file)
            tmpID = tmpJobIdFile.readline()
            tmpJobIdFile.close()
            # remove \n
            tmpID = tmpID.replace('\n','')
            # convert to int
            jobDefinitionID = long(tmpID) + 1
        except:
            pass
    return jobDefinitionID
    

# write jobID
def writeJobDefID(jobID):
    # create dir for DB
    dbdir = os.path.expanduser(os.environ['PANDA_CONFIG_ROOT'])
    if not os.path.exists(dbdir):
        os.makedirs(dbdir)
    # record jobID
    jobid_file = '%s/pjobid.dat' % os.environ['PANDA_CONFIG_ROOT']
    tmpJobIdFile = open(jobid_file,'w')
    tmpJobIdFile.write(str(jobID))
    tmpJobIdFile.close()


# calculate the number of subjobs
def calculateNumSplit(nFilesPerJob,nGBPerJob,nEventsPerJob,nEventsPerFile,
                      maxTotalSize,dbrDsSize,safetySize,useTagParentLookup,
                      inputFileList,inputFileMap,tagFileList,parentLfnToTagMap):
    # count total size for inputs
    totalSize = 0 
    for fileName in inputFileList:
        try:
            vals = inputFileMap[fileName]
            totalSize += long(vals['fsize'])
        except:
            pass
    #@ If number of jobs is not defined then....
    #@ For splitting by files case
    if nEventsPerJob == -1 and nGBPerJob == -1:
        if nFilesPerJob > 0:
            defaultNFile = nFilesPerJob
        else:
            defaultNFile = 50
        tmpNSplit,tmpMod = divmod(len(inputFileList),defaultNFile)
        if tmpMod != 0:
            tmpNSplit += 1
        # check size limit
        if totalSize/tmpNSplit > maxTotalSize-dbrDsSize-safetySize or useTagParentLookup:
            # reset to meet the size limit
            tmpNSplit,tmpMod = divmod(totalSize,maxTotalSize-dbrDsSize-safetySize)
            if tmpMod != 0:
                tmpNSplit += 1
            # calculate N files
            divF,modF = divmod(len(inputFileList),tmpNSplit)
            if divF == 0:
                divF = 1
            # take TAG into account
            if useTagParentLookup:
                tmpTagList = []
                tmpTagSize = 0
                for fileName in inputFileList[:divF]:
                    tmpTagFileName = parentLfnToTagMap[fileName]['lfn']
                    if not tmpTagFileName in tmpTagList:
                        tmpTagList.append(tmpTagFileName)
                        tmpTagSize += long(tagFileList[tmpTagFileName]['fsize'])
                # recalculate
                tmpNSplit,tmpMod = divmod(totalSize,maxTotalSize-dbrDsSize-safetySize-tmpTagSize)
                if tmpMod != 0:
                    tmpNSplit += 1
                # calculate N files
                divF,modF = divmod(len(inputFileList),tmpNSplit)
                if divF == 0:
                    divF = 1
                if divF > defaultNFile:
                    divF = defaultNFile
            # reset tmpNSplit
            tmpNSplit,tmpMod = divmod(len(inputFileList),divF)
            if tmpMod != 0:
                tmpNSplit += 1
            # check again just in case
            iDiv = 0
            subTotal = 0
            for fileName in inputFileList:
                vals = inputFileMap[fileName]
                try:
                    subTotal += long(vals['fsize'])
                except:
                    pass
                iDiv += 1
                if iDiv >= divF:
                    # check
                    if subTotal > maxTotalSize-dbrDsSize-safetySize:
                        # recalcurate
                        if divF != 1:
                            divF -= 1
                        tmpNSplit,tmpMod = divmod(len(inputFileList),divF)
                        if tmpMod != 0:
                            tmpNSplit += 1
                        break
                    # reset
                    iDiv = 0
                    subTotal = 0
        # set            
        split = tmpNSplit
    #@ For splitting by events case
    elif nGBPerJob == -1:
        #@ split by number of events defined
        defaultNFile=1 #Each job has one input file in this case
        #@ tmpNSplit - number of jobs per file in case of splitting by event number
        tmpNSplit, tmpMod = divmod(nEventsPerFile, nEventsPerJob)
        if tmpMod != 0:
            tmpNSplit +=1
        #@ Number of Jobs calculated here:
        split = tmpNSplit*len(inputFileList)
    else:
        # calcurate number of jobs for nGBPerJob
        split = 0
        tmpSubTotal = 0
        tmpSubNumFiles = 0
        for fileName in inputFileList:
            if inputFileMap.has_key(fileName):
                vals = inputFileMap[fileName]
                tmpSize = long(vals['fsize'])
            else:
                tmpSize = 0
            tmpSubNumFiles += 1    
            singleLargeFile = False
            if tmpSubTotal+tmpSize > nGBPerJob-dbrDsSize-safetySize \
                   or tmpSubNumFiles > limit_maxNumInputs:
                split += 1
                tmpSubNumFiles = 0
                # single large file uses one job
                if tmpSubTotal == 0:
                    singleLargeFile = True
                tmpSubTotal = 0
            if not singleLargeFile:
                tmpSubTotal += tmpSize
        # remaining
        if tmpSubTotal != 0:
            split += 1    
    # return
    return split


# calculate the number of subjobs when EventsPerJob is used
def calculateNumSplitEvent(nEventsPerJob,inputFileList,shipinput,currentDir,nEventsPerFile,nFilesPerJob,inDS,verbose):
    # get the number of events per file
    if nEventsPerFile == 0:
        nEventsPerFile = Client.nEvents(inDS,verbose,(not shipinput),inputFileList,currentDir)
    # use file-boundaries since the number of events per job is larger than the number of events per file
    if nEventsPerJob > nEventsPerFile:
        tmpDiv,tmpMod = divmod(nEventsPerJob,nEventsPerFile)
        # set nFilesPerJob
        nFilesPerJob = tmpDiv
        if tmpMod != 0:
            nFilesPerJob += 1
        # reset    
        nEventsPerJob = -1
    # return
    return nEventsPerFile,nFilesPerJob,nEventsPerJob


# calculate the number of subjobs when TAG parent is used
def calculateNumSplitTAG(nEventsPerJob,inputFileList,parentLfnToTagMap,nFilesPerJob=-1):
    # the number of events is avaliable via TAG DB
    if nFilesPerJob < 0:
        tmpTotalEvents = 0
        for fileName in inputFileList:
            tmpTotalEvents += parentLfnToTagMap[fileName]['nEvents']
        split,tmpMod = divmod(tmpTotalEvents,nEventsPerJob)
        if tmpMod != 0:
            split += 1
    else:
        # the number of files is specified
        split = 0
        nEventUsed = 0
        indexFile = 0
        while indexFile < len(inputFileList):
            nFilesInChunk = 0
            for fileName in inputFileList[indexFile:]:
                # events remain in the same file
                if parentLfnToTagMap[fileName]['nEvents'] - nEventUsed > nEventsPerJob:
                    nEventUsed += nEventsPerJob
                    split += 1
                    break
                else:
                    # go to next file
                    nFilesInChunk += 1
                    # the number of files is exceeded
                    if nFilesInChunk >= nFilesPerJob:
                        split += 1
                        nEventUsed = 0
                        break
                    # no more files
                    if indexFile + nFilesInChunk >= len(inputFileList):
                        split += 1
                        break
                    # set nEventUsed
                    nEventUsed = nEventUsed - parentLfnToTagMap[fileName]['nEvents']
            indexFile += nFilesInChunk
    # return
    return split
    

# group files by dataset
def groupFilesByDataset(inDS,inputDsString,inputFileList,verbose):
    if inputDsString == '':
        # normal inDS
        dsString = inDS
    else:
        # wildcard and/or comma is used in inDS
        dsString = inputDsString
    # loop over all datasets
    retMap = {}
    for tmpDS in dsString.split(','):
        if tmpDS.endswith('/'):
            # get elements in container
            elements = Client.getElementsFromContainer(tmpDS,verbose)
        else:
            elements = [tmpDS,]
        # loop over all elements
        for element in elements:
            # get file map
            tmpFileMap = Client.queryFilesInDataset(element,verbose)
            # collect files
            tmpFileList = []
            for tmpLFN in tmpFileMap.keys():
                # append
                if tmpLFN in inputFileList:
                    tmpFileList.append(tmpLFN)
            # sort
            tmpFileList.sort()
            # append to return map
            if tmpFileList != []:
                retMap[element] = tmpFileList
    # return
    return retMap


# splitter for prun
def calculateNumSplitPrun(nFilesPerJob,nGBPerJob,inputFileList,inputFileMap,maxTotalSize,dbrDsSize,safetySize):
    nFilesEachSubJob = []
    if nFilesPerJob == None and nGBPerJob < 0:
        # count total size for inputs
        totalSize = 0
        for tmpLFN in inputFileList:
            vals = inputFileMap[tmpLFN]
            try:
                totalSize += long(vals['fsize'])
            except:
                pass
        # the number of files per max total
        tmpNSplit,tmpMod = divmod(totalSize,maxTotalSize-dbrDsSize-safetySize)
        if tmpMod != 0:
            tmpNSplit += 1
        tmpNFiles,tmpMod = divmod(len(inputFileList),tmpNSplit)
        # set upper limit
        upperLimitOnFiles = 50
        if tmpNFiles > upperLimitOnFiles:
            tmpNFiles = upperLimitOnFiles
        # check again just in case
        iDiv = 0
        subTotal = 0
        for tmpLFN in inputFileList:
            vals =inputFileMap[tmpLFN]
            try:
                subTotal += long(vals['fsize'])
            except:
                pass
            iDiv += 1
            if iDiv >= tmpNFiles:
                # check
                if subTotal > maxTotalSize-dbrDsSize-safetySize:
                    # decrement
                    tmpNFiles -= 1
                    break
                # reset
                iDiv = 0
                subTotal = 0
        # set
        nFilesPerJob = tmpNFiles
    else:
        # nGBPerJob
        subNFiles = 0
        subTotal = dbrDsSize+safetySize
        for tmpLFN in inputFileList:
            vals = inputFileMap[tmpLFN]
            fsize = long(vals['fsize'])
            if (subTotal+fsize > maxTotalSize) or (subNFiles+1) > limit_maxNumInputs:
                if subNFiles == 0:
                    nFilesEachSubJob.append(1)
                    subNFiles = 0
                else:
                    nFilesEachSubJob.append(subNFiles)
                    subNFiles = 1
                    subTotal = dbrDsSize+safetySize+fsize
            else:
                subNFiles += 1
                subTotal += fsize
        # remain
        if subNFiles != 0:
            nFilesEachSubJob.append(subNFiles)
    # return
    return nFilesPerJob,nFilesEachSubJob
    

# extract Nth field from dataset name
def extractNthFieldFromDS(datasetName,nth):
    items = datasetName.split('.')
    if len(items) < (nth-1):
        # get logger
        tmpLog = PLogger.getPandaLogger()
        errStr = "%s has only %s fields < --useNthFieldForLFN=%s" % (datasetName,len(items),nth)
        tmpLog.error(errStr)
        sys.exit(EC_Config)
    # return
    return items[nth-1]


# info about user brokerage
def getUserBrokerageInfo(val,optType,infoList):
    if optType == 'site':
        msgBody = 'action=use site=%s reason=useroption - site set by user' % val
        infoList.append(msgBody)
        # remove excluded info since --site overrides --excludedSite
        for tmpInfo in tuple(infoList):
            if 'action=exclude' in tmpInfo and ('site=%s' % val) in tmpInfo:
                infoList.remove(tmpInfo)
    elif optType == 'cloud':
        msgBody = 'action=use cloud=%s reason=useroption - cloud set by user' % val
        infoList.append(msgBody)        
    elif optType == 'libDS':
        msgBody = 'action=use site=%s reason=libds - libDS exists' % val
        infoList.append(msgBody)        
    elif optType == 'outDS':
        msgBody = 'action=use site=%s reason=outds - outDS exists' % val
        infoList.append(msgBody)        
    return infoList


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
    except:
        errType,errValue = sys.exc_info()[:2]
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error('cannot read datasets from %s due to %s:%s' \
                     % (txtName,errType,errValue))
        sys.exit(EC_Config)    
    return dsList


# get list of datasets checked for the brokerage
def getDsListCheckedForBrokerage(dsUsedDsMap):
    dsList = []
    for tmpDsUsedDsMapKey,tmpDsUsedDsVal in dsUsedDsMap.iteritems():
        for tmpDsUsedDsValItem in tmpDsUsedDsVal:
            if not tmpDsUsedDsValItem in dsList:
                dsList.append(tmpDsUsedDsValItem)
    # sort
    dsList.sort()
    # make return string
    retStr = ''
    for tmpDS in dsList:
        retStr += '%s,' % tmpDS
    retStr = retStr[:-1]
    # return
    return retStr


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
               'MAXEVENTS' : None
               }
    # replace %XYZ with ${XYZ}
    if includeIO:
        for tmpH in inList:
            encStr = re.sub('%'+tmpH,'${'+tmpH+'}',encStr)
    # replace %XYZ with ${newXYZ}
    extensionMap = {}
    for newH,oldH in holders.iteritems():
        # JEDI-only place holders
        if oldH == None:
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
            if tmpM != None:
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
    allKeys = holders.keys()
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
        if re.search(patS,tmpItem) != None and matchP != None:
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
                if not usePfnList or not tmpHolder in ['IN']:
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
                if extensionMap.has_key(tmpHolder):
                    try:
                        tmpDict['offset'] = long(extensionMap[tmpHolder])
                    except:
                        pass
        else:
            # constant
            tmpDict = {'type':'constant'}
            if encode:
                tmpDict['value'] = urllib.quote(tmpItem)
            else:
                tmpDict['value'] = tmpItem
        # no padding
        if not padding:
            tmpDict['padding'] = False
        # append
        jobParams.append(tmpDict)
    # return
    return jobParams


# give warning for maxCpuCount
def giveWarningForMaxCpuCount(defMaxCpuCount,maxCpuCount,tmpLog):
    # only when the default value is used
    if defMaxCpuCount == maxCpuCount:
        msg  = 'Time limit for each subjob is set to %ssec. ' % maxCpuCount
        msg += 'Use --maxCpuCount if your job requires longer execution time.' 
        tmpLog.warning(msg)


# split comma-concatenated list items
def splitCommaConcatenatedItems(oldList):
    newList = []
    for oldItem in oldList:
        temItems = oldItem.split(',')
        for tmpItem in temItems:
            tmpItem = tmpItem.strip()
            # remove empty
            if tmpItem == '':
                continue
            if not tmpItem in newList:
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
            print out
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
        subInDSbefore = urllib.quote('%DATASET_IN')
        subInDSafter = urllib.quote(inDS)
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
    subOutDSbefore = urllib.quote('%DATASET_OUT')
    subOutDSafter = urllib.quote(outDS)
    subSeqBefore = urllib.quote('%BULKSEQNUMBER')
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

if os.environ.has_key('PANDA_DEBUG'):
    print "DEBUG : imported %s" % __name__    
