import re
import commands

SWLISTURL='https://atlpan.web.cern.ch/atlpan/swlist/'

# wrapper for uuidgen
def wrappedUuidGen():
    # check if uuidgen is available
    tmpSt,tmpOut = commands.getstatusoutput('which uuidgen')
    if tmpSt == 0:
        # use uuidgen
        return commands.getoutput('uuidgen 2>/dev/null')
    else:
        # use python uuidgen
        try:
            import uuid
        except:
            raise ImportError,'uuidgen and uuid.py are unavailable on your system. Please install one of them'
        return str(uuid.uuid4())


# get mana setup parameters
def getManaSetupParam(paramName):
    comStr = 'hwaf show setup'
    tmpSt,tmpOut = commands.getstatusoutput(comStr)
    if tmpSt != 0:
        return False,"'%s' failed : %s" % (comStr,tmpOut)
    # look for param
    for line in tmpOut.split('\n'):
        items = line.split('=')
        if len(items) != 2:
            continue
        # found
        if items[0] == paramName:
            return True,items[1]
    # not found
    return False,"%s not found in the following output from '%s'\n%s" % \
           (paramName,comStr,tmpOut)


# get mana version
def getManaVer():
    # get projects
    getS,getO = getManaSetupParam('projects')
    if not getS:
        return getS,getO
    # look for mana-core/XYZ
    match = re.search('mana-core/(\d+)/',getO)
    # not found
    if match == None:
        return False,"mana version number not found in '%s'" % getO
    # found
    return True,match.group(1)


# check mana version
def checkManaVersion(verStr,cmtConfig):
    if verStr == '':
        return True,'',verStr,cmtConfig
    # get list
    import urllib2
    req = urllib2.Request(url=SWLISTURL+'mana')
    f = urllib2.urlopen(req)
    listStr = f.read()
    tmpSwList = listStr.split('\n')
    # remove short format
    swList = []
    for tmpSW in tmpSwList:
        if re.search('^\d+$',tmpSW) != None:
            continue
        # append
        swList.append(tmpSW)
    # check
    retVal = False
    if verStr in swList:
        retVal = True
        retVer = verStr
    else:
        # add cmtConfig to short format version number
        if re.search('^\d+$',verStr) != None:
            # make search string
            if not cmtConfig in ['',None]:
                verStr += '-%s' % cmtConfig
        # look for pattern
        for tmpItem in swList:
            if re.search('^%s' % verStr,tmpItem) != None:
                retVal = True
                retVer = tmpItem
                # use default cmtConfig if available
                if 'x86_64-slc5-gcc43-opt' in retVer:
                    break
    # not found
    if not retVal:
        errStr  = "mana version %s is unavailable on CVMFS. " % verStr
        errStr += "Must be one of the following versions\n"
        errStr += listStr
        return False,errStr,None,None
    # extract cmtConfig
    if cmtConfig in ['',None]:
        cmtConfig = re.sub('^\d+-','',re.sub('-python.+$','',retVer))
    # return
    return True,'',retVer,cmtConfig
    
