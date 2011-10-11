import os
import re
import commands
import ConfigParser


# config class for application
class AppConfig:

    # constructor
    def __init__(self,sectionName):
        self.sectionName = sectionName


    # get config
    def getConfig(self):
        # config class
        class _appConfig:
            pass
        appConf = {}
        # config file
        confFile = os.path.expanduser('%s/panda.cfg' % os.environ['PANDA_CONFIG_ROOT'])
        if os.path.exists(confFile):
            try:
                # instantiate parser
                parser = ConfigParser.ConfigParser()
                parser.optionxform = str
                parser.read(confFile)
                # expand sequencer section
                for key,val in parser.items(self.sectionName):
                    # convert int/bool
                    if re.search('^\d+$',val) != None:
                        val = int(val)
                    elif re.search('true',val,re.I) != None:
                        val = True
                    elif re.search('false',val,re.I) != None:
                        val = False
                    # set attributes    
                    appConf[key] = val
            except ConfigParser.NoSectionError:
                pass
        # return
        return appConf
