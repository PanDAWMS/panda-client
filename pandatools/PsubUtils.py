import re
import os
import sys
import time
import random
import commands

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
def getCloudUsingFQAN(defaultCloud,verbose=False):
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
    if cloud == None:
        # use a cloud randomly
        cloud = random.choice(Client.PandaClouds.keys())
        tmpLog.info("use %s as default cloud" % cloud)
    else:
        tmpLog.info("use %s as default cloud due to VOMS:%s" % (cloud,countryAttStr))
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
        print 'could not get DistinguishedName from %s' % output
    return distinguishedName


# check if valid cloud
def checkValidCloud(cloud):
    # check cloud
    for tmpID,spec in Client.PandaSites.iteritems():
        if cloud == spec['cloud']:
            return True
    return False    


# check name of output dataset
def checkOutDsName(outDS,distinguishedName,official):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # official dataset
    if official:
        allowedPrefix = ['group']
        for tmpPrefix in allowedPrefix:
            if outDS.startswith(tmpPrefix):
                return True
        # didn't match
        errStr  = "End-users are allowed to produce official datasets\n"
        errStr += "        with the following prefix\n"
        for tmpPrefix in allowedPrefix:
            errStr += "          %s%s\n" % (tmpPrefix,time.strftime('%y',time.gmtime()))
        errStr += "If you need to use another prefix please request it to Panda Savannah"
        tmpLog.error(errStr)
        return False
    # check output dataset format
    matStr = '^user' + ('%s' % time.strftime('%y',time.gmtime())) + '\.' + distinguishedName + '\.'
    if re.match(matStr,outDS) == None:
        errStr  = "outDS must be 'user%s.%s.<user-controlled string...'\n" % \
                 (time.strftime('%y',time.gmtime()),distinguishedName)
        errStr += "        e.g., user%s.%s.test1234\n" % \
                  (time.strftime('%y',time.gmtime()),distinguishedName)
        errStr += "        Please use 'user%s.' instead of 'user.' to follow ATL-GEN-INT-2007-001" % \
                  time.strftime('%y',time.gmtime())
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


# check panda-client version
def checkPandaClientVer(verbose):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get latest version number
    vStatus,latestVer = Client.getPandaClientVer(verbose)
    if vStatus == 0:
        # check version
        import PandaToolsPkgInfo
        if latestVer > PandaToolsPkgInfo.release_version:
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
    import PandaToolsPkgInfo
    if latestVer <= PandaToolsPkgInfo.release_version:
        tmpLog.info('you are already using the latest version')
        return True
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
            status,output = commands.getstatusoutput('mv %s %s.back' % \
                                                     (os.environ['PANDA_SYS'],os.environ['PANDA_SYS']))
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
                    

