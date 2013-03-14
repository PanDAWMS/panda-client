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


# check mana version
def checkManaVersion(verStr):
    if verStr == '':
        return True,''
    # get list
    import urllib2
    req = urllib2.Request(url=SWLISTURL+'mana')
    f = urllib2.urlopen(req)
    listStr = f.read()
    swList = listStr.split('\n')
    # check
    if verStr in swList:
        return True,''
    # not found
    errStr  = "mana version %s is unavailable on CVMFS. " % verStr
    errStr += "Must be one of the following versions\n"
    errStr += listStr
    return False,errStr
