import os
import re
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser

sectionName = 'book'
confFile = os.path.expanduser('%s/panda.cfg' % os.environ['PANDA_CONFIG_ROOT'])


# create config or add section when missing
parser=ConfigParser.ConfigParser()
newFlag = False
if not os.path.exists(confFile):
    # create new config
    newFlag = True
    # make dir
    try:
        os.makedirs(os.environ['PANDA_CONFIG_ROOT'])
    except Exception:
        pass
else:
    # add section
    parser.read(confFile)
    if not parser.has_section(sectionName):
        newFlag = True
# new file or missing section
if newFlag:
    # add section
    parser.add_section(sectionName)
    # set dummy time
    parser.set(sectionName,'last_synctime','')
    # keep old config just in case
    try:
        os.rename(confFile, '%s.back' % confFile)
    except Exception:
        pass
    # write
    confFH = open(confFile,'w')
    parser.write(confFH)
    confFH.close()


# get config
def getConfig():
    # instantiate parser
    parser=ConfigParser.ConfigParser()
    parser.read(confFile)
    # config class
    class _bookConfig:
        pass
    bookConf = _bookConfig()
    # expand sequencer section
    for key,val in parser.items(sectionName):
        # convert int/bool
        if re.search('^\d+$',val) is not None:
            val = int(val)
        elif re.search('true',val,re.I) is not None:
            val = True
        elif re.search('false',val,re.I) is not None:
            val = False
        # set attributes    
        setattr(bookConf,key,val)
    # return
    return bookConf


# update
def updateConfig(bookConf):
    # instantiate parser
    parser=ConfigParser.ConfigParser()
    parser.read(confFile)
    # set new values
    for attr in dir(bookConf):
        if not attr.startswith('_'):
            val = getattr(bookConf,attr)
            if val is not None:
                parser.set(sectionName,attr,val)
    # keep old config
    try:
        os.rename(confFile, '%s.back' % confFile)
    except Exception as e:
        print("WARNING : cannot make backup for %s with %s" % (confFile, str(e)))
        return
    # update conf
    conFH = open(confFile,'w')
    parser.write(conFH)
    # close
    conFH.close()
