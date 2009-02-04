import os
import re
import commands
import ConfigParser

sectionName = 'global'
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
    except:
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
    # set grid source file
    if os.environ.has_key('PATHENA_GRID_SETUP_SH'):
        parser.set(sectionName,'grid_src',os.environ['PATHENA_GRID_SETUP_SH'])        
    else:
        parser.set(sectionName,'grid_src','')
    # keep old config just in case
    status,out = commands.getstatusoutput('mv %s %s.back' % (confFile,confFile))
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
    class _globalConfig:
        pass
    globalConf = _globalConfig()
    # expand sequencer section
    for key,val in parser.items(sectionName):
        # convert int/bool
        if re.search('^\d+$',val) != None:
            val = int(val)
        elif re.search('true',val,re.I) != None:
            val = True
        elif re.search('false',val,re.I) != None:
            val = False
        # set attributes    
        setattr(globalConf,key,val)
    # return
    return globalConf


# update
def updateConfig(globalConf):
    # instantiate parser
    parser=ConfigParser.ConfigParser()
    parser.read(confFile)
    # set new values
    for attr in dir(globalConf):
        if not attr.startswith('_'):
            val = getattr(globalConf,attr)
            if val != None:
                parser.set(sectionName,attr,val)
    # keep old config
    status,out = commands.getstatusoutput('mv %s %s.back' % (confFile,confFile))
    if status != 0:
        print "WARNING : cannot make backup for %s" % confFile
        return
    # update conf
    conFH = open(confFile,'w')
    parser.write(conFH)
    # close
    conFH.close()
