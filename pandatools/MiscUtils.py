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
