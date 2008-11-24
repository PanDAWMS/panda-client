import os
import re
import commands
import ConfigParser

sectionName = 'sequencer'
confFile = os.path.expanduser('%s/panda.cfg' % os.environ['PANDA_CONFIG_ROOT'])

# check file existence
if not os.path.exists(confFile):
    raise RuntimeError, "config file : %s doesn't exist" % confFile

# instantiate parser
parser=ConfigParser.ConfigParser()
parser.read(confFile)

# config class
class _seqConfig:
    pass
seqConf = _seqConfig()
del _seqConfig

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
    setattr(seqConf,key,val)


# update
def updateConfig():
    # set UIDL/UID
    attrs = ['pop_uidl','imap_uid']
    for attr in attrs:
        if hasattr(seqConf,attr):
            val = getattr(seqConf,attr)
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
    conFH.close()
    os.chmod(confFile,0600)
