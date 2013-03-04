import commands

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
def getMataSetupParam(paramName):
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
