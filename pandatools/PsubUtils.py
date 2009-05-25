import re
import os
import sys
import time
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
        cloud = defaultCloud
        if verbose:
            tmpLog.debug("  use default %s" % cloud)
    if verbose:
        tmpLog.debug("set cloud=%s" % cloud)
    # return
    return cloud


# convert DQ2 ID to Panda siteid 
def convertDQ2toPandaID(site):
    keptSite = ''
    for tmpID,tmpSpec in Client.PandaSites.iteritems():
        # # exclude long,xrootd,local queues
        if Client.isExcudedSite(tmpID):
            continue
        # get list of DQ2 IDs
        srmv2ddmList = []
        for tmpDdmID in tmpSpec['setokens'].values():
            srmv2ddmList.append(Client.convSrmV2ID(tmpDdmID))
        # use Panda sitename
        if Client.convSrmV2ID(site) in srmv2ddmList:
            keptSite = tmpID
            # keep non-online site just in case
            if tmpSpec['status']=='online':
                return keptSite
    return keptSite


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
    # check length
    maxLength = 128
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


# update package
def updatePackage(verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # get the latest version number
    tmpLog.info('start update check')
    com = 'curl -m 120 --silent https://twiki.cern.ch/twiki/bin/view/Atlas/PandaTools'
    if verbose:
        tmpLog.debug(com)
    status,output = commands.getstatusoutput(com)
    status %= 255
    if verbose:
        tmpLog.debug(status)
        tmpLog.debug(output)
    if status != 0:
        tmpLog.error('failed to get the latest version number : %s' % status)
        return False
    # extract version
    match = re.search('Current version of panda-client : (\d+\.\d+\.\d+) ',output)
    if match == None:
        tmpLog.error('failed to extract the latest version number')
        return False
    latestVer = match.group(1)
    # check version
    import PandaToolsPkgInfo
    if latestVer == PandaToolsPkgInfo.release_version:
        tmpLog.info('you are already using the latest version')
        return True
    # get tarball
    tmpLog.info('get panda-client-%s' % latestVer)
    com = 'wget --timeout 120 https://twiki.cern.ch/twiki/pub/Atlas/PandaTools/panda-client-%s.tar.gz' \
          % latestVer
    status = os.system(com)
    status %= 255    
    if status != 0:
        tmpLog.error('failed to download tarball : %s' % status)
        # delete tarball just in case
        commands.getoutput('rm panda-client-%s.tar.gz' % latestVer)    
        return False
    tmpLog.info('update to %s from %s' % (latestVer,PandaToolsPkgInfo.release_version))
    # expand
    status,output = commands.getstatusoutput('tar xvfz panda-client-%s.tar.gz' % latestVer)
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
    commands.getoutput('rm panda-client-%s.tar.gz' % latestVer)
    # installation type
    print "\nPlease specify installation type"
    print " 1 : Clean install : all files under %s will be erased first (recommended)" % os.environ['PANDA_SYS']
    print " 2 : Overwrite : existing files under %s will be replaced with new ones" % os.environ['PANDA_SYS']
    while True:
        str = raw_input("Enter 1 or 2 : ")
        if str == '1':
            cleanInstall = True
            break
        if str== '2':
            cleanInstall = False
            break
    # save current dir
    currentDir = os.path.realpath(os.getcwd())
    # keep old release
    if cleanInstall:
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
    status,output = commands.getstatusoutput('python setup.py install --prefix=%s' % os.environ['PANDA_SYS'])
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
    if cleanInstall:
        commands.getoutput('rm -rf %s.back' % os.environ['PANDA_SYS'])
    # return
    if result:
        tmpLog.info('completed')
        tmpLog.info("please do 'source %s/etc/panda/panda_setup.[c]sh'" % os.environ['PANDA_SYS'])
    return result
