import re
import os
import sys
import time
import random
import commands
import urllib
import pickle

import Client
import MyproxyUtils
import PLogger

# error code
EC_Config    = 10


# check proxy
def checkGridProxy(gridPassPhrase='',enforceEnter=False,verbose=False,vomsRoles=None):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # check grid-proxy
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        sys.exit(EC_Config)
    # use grid-proxy-info first becaus of bug #36573 of voms-proxy-info
    com = '%s grid-proxy-info -e' % gridSrc
    if verbose:
        tmpLog.debug(com)
    status,out = commands.getstatusoutput(com)
    if verbose:
        tmpLog.debug(status % 255)
        tmpLog.debug(out)
    # check VOMS extension
    vomsFQAN = ''
    if status == 0:
        # with VOMS extension 
        com = '%s voms-proxy-info -fqan -exists' % gridSrc    
        if verbose:
            tmpLog.debug(com)
        status,out = commands.getstatusoutput(com)
        if verbose:
            tmpLog.debug(status % 255)
            tmpLog.debug(out)
        if status == 0:
            vomsFQAN = out
        # check roles
        if vomsRoles != None:
            hasAttr = True
            for indexAttr,attrItem in enumerate(vomsRoles.split(',')):
                vomsCommand = attrItem.split(':')[-1]
                if not '/Role' in vomsCommand:
                    vomsCommand += '/Role'
                if not vomsCommand in out:
                    hasAttr = False
                    break
                # check order
                try:
                    if not vomsCommand in out.split('\n')[indexAttr]:
                        hasAttr = False
                        break
                except:
                        hasAttr = False
                        break
            if not hasAttr:
                # set status to regenerate proxy with roles
                status = -1
    # generate proxy
    if (status != 0 and out.find('Error: Cannot verify AC signature') == -1) or \
           out.find('Error: VOMS extension not found') != -1 or enforceEnter:
        # GRID pass phrase
        if gridPassPhrase == '':
            import getpass
            tmpLog.info("Need to generate a grid proxy")            
            print "Your identity: " + commands.getoutput('%s grid-cert-info -subject' % gridSrc)
            gridPassPhrase = getpass.getpass('Enter GRID pass phrase for this identity:')
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
        #tmpLog.info("use %s as default cloud" % cloud)
    elif cloud == None:
        # use a cloud randomly
        cloud = random.choice(Client.PandaClouds.keys())
        #tmpLog.info("use %s as default cloud" % cloud)
    else:
        #tmpLog.info("use %s as default cloud due to VOMS:%s" % (cloud,countryAttStr))
        pass
    # return
    return cloud


# convert DQ2 ID to Panda siteid 
def convertDQ2toPandaID(site):
    return Client.convertDQ2toPandaID(site)


# get DN
def getDN():
    shortName = ''
    distinguishedName = ''
    gridSrc = Client._getGridSrc()
    if gridSrc == False:
        return ''
    output = commands.getoutput('%s grid-proxy-info -identity' % gridSrc)
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
        wMessage =  'Could not get nickname from voms proxy\n'
        wMessage += 'Please register nickname to ATLAS VO via\n\n'
        wMessage += '   https://lcg-voms.cern.ch:8443/vo/atlas/vomrs\n'
        wMessage += '      [Member Info] -> [Edit Personal Info]\n\n'
        wMessage += 'Then you can use new naming convention "user.nickname" for --outDS. '
        wMessage += 'Note that as of May 31st 2010 old convention '
        wMessage += '"userXY.FirstLastname" will be terminated.\n'
        wMessage += 'See the announcement : https://savannah.cern.ch/forum/forum.php?forum_id=1259\n'
        tmpLog.warning(wMessage)
    return nickName
        

# check if valid cloud
def checkValidCloud(cloud):
    # check cloud
    for tmpID,spec in Client.PandaSites.iteritems():
        if cloud == spec['cloud']:
            return True
    return False    


# check name of output dataset
def checkOutDsName(outDS,distinguishedName,official,nickName='',site='',vomsFQAN=''):
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
        outDsPrefixO = 'user%s.%s' % (time.strftime('%y',time.gmtime()),distinguishedName)
        if nickName != '':
            outDsPrefixN = 'user.%s' % nickName
        errStr  = "outDS must be '%s.<user-controlled string...'\n" % outDsPrefixO
        if nickName != '':
            errStr += "           or '%s.<user-controlled string...'\n" % outDsPrefixN
        errStr += "        e.g., %s.test1234" % outDsPrefixO
        if nickName != '':        
            errStr += "\n              %s.test1234" % outDsPrefixN        
        tmpLog.error(errStr)
        return False
    # check length. 200=255-55. 55 is reserved for Panda-internal (_subXYZ etc)
    maxLength = 200
    if len(outDS) > maxLength:
        tmpLog.error("output datasetname is too long (%s). The length must be less than %s" % \
                     (len(outDS),maxLength))
        return False
    return True


# get maximum index in a dataset
def getMaxIndex(list,pattern):
    maxIndex = 0
    for item in list:
        match = re.match(pattern,item)
        if match != None:
            tmpIndex = int(match.group(1))
            if maxIndex < tmpIndex:
                maxIndex = tmpIndex
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
    paramStr = ''
    for item in sys.argv[1:]:
        match = re.search('(\*| |\'|=)',item)
        if match == None:
            # normal parameters
            paramStr += '%s ' % item
        else:
            # quote string
            paramStr += '"%s" ' % item
    # return
    return paramStr[:-1]


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
    if latestMinorVer > localMinorVer:
        return False
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
    com = 'wget --timeout 120 https://twiki.cern.ch/twiki/pub/Atlas/PandaTools/%s' \
          % packageName
    status = os.system(com)
    status %= 255    
    if status != 0:
        tmpLog.error('failed to download tarball : %s' % status)
        # delete tarball just in case
        commands.getoutput('rm %' % packageName)    
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
    # unknown site
    if not Client.PandaSites.has_key(site):
        return False
    # parse copysetup
    params = Client.PandaSites[site]['copysetup'].split('^')
    # doesn't use special parameters
    if len(params) < 5:
        return False
    # directIn
    directIn = params[4]
    if directIn != 'True':
        return False
    # xrootd uses local copy for RAW
    newPrefix = params[2]
    if newPrefix.startswith('root:'):
        if usingRAW:
            return False
    # official TRF doesn't work with direct dcap/xrootd
    if usingTRF and (not usingARA):
        if newPrefix.startswith('root:') or newPrefix.startswith('dcap:') or \
           newPrefix.startswith('dcache:'):
            return False
    # return
    return True


# check destination SE
def checkDestSE(destSE,dsName,verbose):
    # check destSE
    if destSE == '':
        return True
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get DN
    tmpDN = commands.getoutput('%s grid-proxy-info -identity' % Client._getGridSrc())
    # set X509_CERT_DIR
    if not os.environ.has_key('X509_CERT_DIR') or os.environ['X509_CERT_DIR'] == '':
        os.environ['X509_CERT_DIR'] = Client._x509_CApath() 
    # check with DaTRI
    from datriHandler import datriHandler
    tmpDaHandler = datriHandler(type='pathena')
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


# run pathena recursively
def runPathenaRec(runConfig,missList,tmpDir,fullExecString,nfiles,inputFileMap,site,crossSite,archiveName,
                  removedDS,inDS,goodRunListXML,eventPickEvtList,verbose):
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
    inputTmpfile = '%s/intmp.%s' % (tmpDir,commands.getoutput('uuidgen'))
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
    # source name
    if not '--panda_srcName' in fullExecString:
        fullExecString += ' --panda_srcName=%s' % archiveName
    # server URL
    if not '--panda_srvURL' in fullExecString:
        fullExecString += ' --panda_srvURL=%s,%s' % (Client.baseURL,Client.baseURLSSL)
    # run config
    conTmpfile = ''
    if not '--panda_runConfig' in fullExecString:
        conTmpfile = '%s/conftmp.%s' % (tmpDir,commands.getoutput('uuidgen'))
        cFile = open(conTmpfile,'w')
        pickle.dump(runConfig,cFile)
        cFile.close()
        fullExecString += ' --panda_runConfig=%s' % conTmpfile

    # run pathena
    if anotherTry:
        tmpLog.info("trying other sites for the missing files")
        com = 'pathena ' + fullExecString
        if verbose:
            tmpLog.debug(com)
        status = os.system(com)
        # delete tmp files
        commands.getoutput('\rm -f %s' % inputTmpfile)
        commands.getoutput('\rm -f %s' % conTmpfile)            
        # exit
        sys.exit(status)
    # exit
    sys.exit(0)
    
    
# run prun recursively
def runPrunRec(missList,tmpDir,fullExecString,nFiles,inputFileMap,site,crossSite,archiveName,
               removedDS,inDS,goodRunListXML,eventPickEvtList,verbose):
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
    inputTmpfile = '%s/intmp.%s' % (tmpDir,commands.getoutput('uuidgen'))
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
    # source name
    if archiveName != '' and not '--panda_srcName' in fullExecString:
        fullExecString += ' --panda_srcName=%s' % archiveName
    # server URL
    if not '--panda_srvURL' in fullExecString:
        fullExecString += ' --panda_srvURL=%s,%s' % (Client.baseURL,Client.baseURLSSL)
    # run prun
    if anotherTry:
        tmpLog.info("trying other sites for the missing files")
        com = 'prun ' + fullExecString
        if verbose:
            tmpLog.debug(com)
        status = os.system(com)
        # delete tmp files
        commands.getoutput('\rm -f %s' % inputTmpfile)
        # exit
        sys.exit(status)
    # exit
    sys.exit(0)



# run brokerage for composite site
def runBrokerageForCompSite(siteIDs,releaseVer,cacheVer,verbose):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # run brokerage
    status,out = Client.runBrokerage(siteIDs,releaseVer,verbose=verbose,trustIS=True,cacheVer=cacheVer)
    if status != 0:
        tmpLog.error('failed to run brokerage for composite site: %s' % out)
        sys.exit(EC_Config)
    if out.startswith('ERROR :'):
        tmpLog.error(out + '\nbrokerage failed')
        sys.exit(EC_Config)
    if not Client.PandaSites.has_key(out):
        tmpLog.error('brokerage gave wrong PandaSiteID:%s' % out)
        sys.exit(EC_Config)
    return out

    
# get list of datasets and files by list of runs/events
def getDSsFilesByRunsEvents(curDir,runEventTxt,dsType,streamName,dsPatt='',verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # set 
    from eventLookup import pyELSSI
    elssiIF = pyELSSI()
    # set X509_USER_PROXY
    if not os.environ.has_key('X509_USER_PROXY') or os.environ['X509_USER_PROXY'] == '':
        os.environ['X509_USER_PROXY'] = Client._x509()
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
    tmpLog.info('getting dataset names and LFNs from ELSSI for event picking')
    # read
    runEvtList = []
    guids = []
    guidRunEvtMap = {}
    for line in runevttxt:
        items = line.split()
        if len(items) != 2:
            continue
        runNr,evtNr = items
        paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,streamName)
        if verbose:
            tmpLog.debug(paramStr)
        else:
            sys.stdout.write('.')
            sys.stdout.flush()
        # check with ELSSI
        if streamName == '':
            guidListELSSI = elssiIF.eventLookup(runNr,evtNr,[streamRef],verbose=verbose)
        else:
            guidListELSSI = elssiIF.eventLookup(runNr,evtNr,[streamRef],streamName,verbose=verbose)            
        if guidListELSSI == []:
            if not verbose:
                print
            errStr = "GUID was not found in ELSSI for %s" % paramStr    
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        # check duplication
        tmpguids = []
        for tmpGuid, in guidListELSSI:
            if tmpGuid == 'NOATTRIB':
                continue
            if not tmpGuid in tmpguids:
                tmpguids.append(tmpGuid)
        if tmpguids == []:
            if not verbose:
                print
            errStr = "no GUIDs were found in ELSSI for %s" % paramStr
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        if len(tmpguids) != 1:        
            if not verbose:
                print
            errStr = "multiple GUIDs %s were found in ELSSI for %s. Please set --eventPickStreamName" % (str(),paramStr)
            tmpLog.error(errStr)
            sys.exit(EC_Config)
        # append
        if not tmpguids[0] in guids:
            guids.append(tmpguids[0])
            guidRunEvtMap[tmpguids[0]] = []
        guidRunEvtMap[tmpguids[0]].append((runNr,evtNr))
    # close
    runevttxt.close()
    if not verbose:
        print
    # convert to dataset names and LFNs
    dsLFNs = Client.listDatasetsByGUIDs(guids,dsPatt,verbose)
    if verbose:
        tmpLog.debug(dsLFNs)
    # return
    return dsLFNs,guidRunEvtMap


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

