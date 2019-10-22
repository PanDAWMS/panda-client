# extraction from myproxyUtils

import re
import sys
import getpass
from . import Client
from . import MiscUtils
from .MiscUtils import commands_get_status_output, commands_get_output


"""Classes with methods to interact with a myproxy server. 

   * Class MyProxyInterface attributes:
       - servername      = name of the myproxy server
       - vomsattributes  = voms attributes, needed to use glexec
       - userDN          = DN of the user
       - pilotownerDN    = DN of the pilot owner 
                           who will be allowed to retrieve the proxy
       - proxypath       = location for the retrieved proxy
       - pilotproxypath  = location of the pilot owner proxy

       - command         = command, for testing purposes      

       - Two dates, in seconds since Epoch, to compare with
         the version of the  myproxy tools installed on the WN 

          - myproxyinit_refdate  = 1181620800 = 12 Jun 2007
          - myproxylogon_refdate = 1181620800 = 12 Jun 2007 

       - automatic_renewal = True/False to determine if the proxy 
                             has to be periodically retrieved
       - time_threshold    = time threshold to trigger a new retrieval
                              

   * Class MyProxyInterface methods:
      - delegate: to delegate a valid proxy using command myproxy-init 
      - retrieve: to recover the stored proxy using command myproxy-logon
      - delproxy: to delete the retrieved proxy

   Proxies must to have voms attributes, to be used by glexec.
   
   user's DN is used as username. This DN can be got
   from the job's schema, defined in JobSpec.py file,
   and passed by a dictionary variable named param[]

   Proxies are stored without password.

   Only the pilot onwer is allowed to retrieve the proxy.
   His DN is specified when the proxy is uploaded, 
   using the -Z option.

   
   * Class MyProxyError is implemented to throw exceptions
   when something is going wrong.      


   * Functions to manage the proxy retrieval and the gLExec preparation
     have been moved from SimplePilot.py to here for clarity
   

   Author:         Jose Caballero (Brookhaven National Laboratory)
   E-mail:         jcaballero (at) bnl.gov
   Last revision:  5/22/2008 
   Version:        1.5
"""

class MyProxyError(Exception):
    """class to throw exceptions when something is going wrong
    during the delegation/retrieval of the proxy credentials
    to/from a myproxy server
    """

    #constructor
    def __init__(self, index, message=''):
        """index is the key of a dictionary 
        with strings explaining what happened

        message is the output returned from the application which failed        
        """
        self.__index = index      
        self.__message = message

        self.__err={}

        self.__err[2100] = ' server name not specified\n'
        self.__err[2101] = ' voms attributes not specified\n'
        self.__err[2102] = ' user DN not specified\n'
        self.__err[2103] = ' pilot owner DN not specified\n'
        self.__err[2104] = ' invalid path for the delegated proxy\n'
        self.__err[2105] = ' invalid pilot proxy path\n'
        self.__err[2106] = ' no path to delegated proxy specified\n'

        self.__err[2200] = ' myproxy-init not available in PATH\n'
        self.__err[2201] = ' myproxy-logon not available in PATH\n'

        self.__err[2202] = ' myproxy-init version not valid\n'
        self.__err[2203] = ' myproxy-logon version not valid\n'

        self.__err[2300] = ' proxy delegation failed\n'
        self.__err[2301] = ' proxy retrieval failed\n'

        self.__err[2400] = ' security violation. Logname and DN do not match\n'

        # output message
        self.__output = '\n>>> MyProxyError %s:' % self.__index + self.__err[self.__index]

        if self.__message != '':
           self.__output += '\n>>> MyProxyError: Begin of error message\n'
           self.__output += self.__message
           self.__output += '\n>>> MyProxyError: End of error message\n'

    def __str__(self):
        return self.__output

    # method to read the index
    def getIndex(self):
        return self.__index
    index = property(getIndex)


class MyProxyInterface(object):
    """class with basic features to delegate/retrieve 
    a valid proxy credential to/from a myproxy server
    """

    # constructor
    def __init__(self):
        self.__servername = ''      # name of the myproxy server
        self.__vomsattributes = 'atlas'  # voms attributes, needed to use glexec
        self.__userDN = ''          # DN of the user
        self.__pilotownerDN = ''    # DN of the pilot owner 
                                    # who will be allowed to retrieve the proxy
        self.__proxypath = ''       # location for the retrieved proxy
        self.__pilotproxypath = ''  # location of the pilot owner proxy
        
        self.__command = ''         # command, for testing purposes

        # dates, in seconds since Epoch, used as reference for myproxy tools
        # to compare with version installed on the WN
        # Reference value is 12 Jun 2007
        # which corresponds with 1181620800 seconds
        self.__myproxyinit_refdate  = 1181620800
        self.__myproxylogon_refdate = 1181620800

        # variables to manage the periodically retrieval
        # by a separate script
        self.__automatic_retrieval = 0  # by default, the option is disabled. 
        self.__time_threshold = 3600    # by default, 1 hour
        # src file to setup grid runtime
        self.srcFile = Client._getGridSrc()

    # methods to write/read the name of the myproxy server
    def setServerName(self,value):
        self.__servername = value
    def getServerName(self):
        return self.__servername
    servername = property( getServerName, setServerName )
    
    # methods to write/read the voms attributes 
    def setVomsAttributes(self,value):
        self.__vomsattributes = value
    def getVomsAttributes(self):
        return self.__vomsattributes
    vomsattributes = property( getVomsAttributes, setVomsAttributes )

    # methods to write/read the DN of the user
    # This value can be got from dictionary param
    # implementing the job schema (file JobSpec.py) 

    def __processDN_xtrastrings(self,value):
        # delegated proxies do not include strings like "/CN=proxy" or 
        # "/CN=12345678" in the DN. Extra strings have to be removed
        if value.count('/CN=') > 1:
            first_index = value.find('/CN=')
            second_index = value.find('/CN=', first_index+1)
            value = value[0:second_index]

        return value

    def __processDN_whitespaces(self,value):
        # usually the last part of the DN is like 
        #   '/CN=<Name> <LastName> <RandNumber>'
        # with white spaces. It must be 
        #   '/CN=<Name>\ <LastName>\ <RandNumber>'
        # to scape the white spaces
        pattern = re.compile('[^\\\]\s') # a whitespace preceded 
                                         # by anything but \
        if pattern.search(value):
            value = value.replace(' ', '\ ')

        return value

    def __processDN_parenthesis(self,value):

        # the DN could contain parenthesis characteres
        # They have to be preceded by a backslash also
        pattern = re.compile( '[^\\\]\(' ) # a parenthesis "(" preceded
                                           # by anything but \
        if pattern.search(value):
            value = value.replace( '(', '\(' )

        pattern = re.compile( '[^\\\]\)' ) # a parenthesis ")" preceded
                                           # by anything but \
        if pattern.search(value):
            value = value.replace( ')', '\)' )

        return value

    def __processDN(self,value):
      
        value = self.__processDN_xtrastrings(value)
        value = self.__processDN_whitespaces(value)
        value = self.__processDN_parenthesis(value)
        
        return value


    def setUserDN(self,value):
        self.__userDN = self.__processDN(value) 
    def getUserDN(self):
        return self.__userDN
    userDN = property( getUserDN, setUserDN )

    # methods to write/read the DN of the pilot owner 
    def setPilotOwnerDN(self,value):
        self.__pilotownerDN = value
    def getPilotOwnerDN(self):
        return self.__pilotownerDN
    pilotownerDN = property( getPilotOwnerDN, setPilotOwnerDN )

    # methods to write/read the location of the retrieved proxy 
    def setProxyPath(self,value):
        # checking the path is valid. 
        # It has to be a file in the /tmp directory:
        if value.startswith('/tmp'):
            self.__proxypath = value
        else:
            raise MyProxyError(2104)

    def getProxyPath(self):
        # A file in the /tmp directory is created if no
        # other path has been set already  
        if self.__proxypath == '':
            self.__proxypath = commands_get_output( 'mktemp' )

        return self.__proxypath

    proxypath = property( getProxyPath, setProxyPath )

    # methods to write/read the path to the pilot owner proxy 
    def setPilotProxyPath(self,value):
        self.__pilotproxypath = value

    def getPilotProxyPath(self):
        # checking the path is valid
        st,out = commands_get_status_output('grid-proxy-info -exists -file %s'
                                                   % self.__pilotproxypath)
        if st != 0: #invalid path
            print("\nError: not valid proxy in path %" % self.__pilotproxypath)
            print("\nUsing the already existing proxy to get the path")
            st, path = commands_get_status_output('grid-proxy-info -path')
            if st == 0:
                self.__pilotproxypath = path
            else:
                raise MyProxyError(2105)

        if self.__pilotproxypath == '':
            print("\nWarning: not valid pilot proxy path specified already")
            print("\nUsing the already existing proxy to get the path")
            st, path = commands_get_status_output('grid-proxy-info -path')
            if st == 0:
                self.__pilotproxypath = path
            else:
                raise MyProxyError(2105)

        return self.__pilotproxypath

    pilotproxypath = property( getPilotProxyPath, setPilotProxyPath )

    # method to read the command (read-only variable)
    # for testing purposes
    def getCommand(self):
         return self.__command
    command = property( getCommand ) 

    # methods to read the reference dates 
    # (read-only variables)
    def getMyproxyinitRefdate(self):
         return self.__myproxyinit_refdate
    myproxyinit_refdate = property( getMyproxyinitRefdate )

    def getMyproxylogonRefdate(self):
         return self.__myproxylogon_refdate
    myproxylogon_refdate = property( getMyproxylogonRefdate )
 
    # methods to write/read the value of automatic_retrieval variable 
    def setAutomaticRetrieval(self,value):
        if value==0 or value==1:
          self.__automatic_retrieval = value
    def getAutomaticRetrieval(self):
        return self.__automatic_retrieval
    automatic_retrieval = property( getAutomaticRetrieval, setAutomaticRetrieval )
    

    # methods to write/read the number of seconds used as time threshold 
    # to the automatic retrieval 
    def setTimeThreshold(self,value):
        self.__time_threshold = value
    def getTimeThreshold(self):
        return self.__time_threshold
    time_threshold = property( getTimeThreshold, setTimeThreshold )
    

    def delegate(self,gridPassPhrase='',verbose=False):
        """method to upload a valid proxy credential in a myproxy server.
           The proxy must to have voms attributes.
           Only the pilot owner will be allowed to retrieve the proxy,
           specified by -Z option.
           The DN of the user is used as username.
        """

        if self.servername == '' :
            raise MyProxyError(2100)
        if self.vomsattributes == '' :
            raise MyProxyError(2101)
        if self.pilotownerDN == '' :
            raise MyProxyError(2103)    

        cmd  = 'myproxy-init'

        # credname
        credname = re.sub('-','',MiscUtils.wrappedUuidGen())

        print("=== upload proxy for glexec")
        # command options
        cmd += ' -s %s'       % self.servername     # myproxy sever name
        cmd += " -x -Z '%s'"  % self.pilotownerDN   # only user with this DN 
                                                    # is allowed to retrieve it
        cmd += ' --voms %s'   % self.vomsattributes # voms attributes
        cmd += ' -d'                                # uses DN as username
        cmd += ' --credname %s' % credname
        if gridPassPhrase == '':
            if sys.stdin.isatty():  
                gridPassPhrase = getpass.getpass('Enter GRID pass phrase:')
            else:
                sys.stdout.write('Enter GRID pass phrase:')
                sys.stdout.flush()
                gridPassPhrase = sys.stdin.readline().rstrip()
                print('')
            gridPassPhrase = gridPassPhrase.replace('$','\$')
        cmd = 'echo "%s" | %s -S' % (gridPassPhrase,cmd)
        cmd = self.srcFile + ' unset GT_PROXY_MODE; ' + cmd   
            
        self.__command = cmd  # for testing purpose

        if verbose:
            cmdStr = cmd
            if gridPassPhrase != '':
                cmdStr = re.sub(gridPassPhrase,'*****',cmd)
            print(cmdStr)
        status,out = commands_get_status_output( cmd )
        if verbose:
            print(out)
        if status != 0:
            if out.find('Warning: your certificate and proxy will expire') == -1:
                if not verbose:
                    print(out)
                raise MyProxyError(2300)
        return credname


    # check proxy
    def check(self,credname,verbose=False):
        # construct command to get MyProxy credentials 
        cmd = self.srcFile + ' myproxy-info -d '
        cmd += '-s %s' % self.servername
        if verbose:
            print(cmd)
        status,output = commands_get_status_output(cmd)
        if verbose:
            print(output)
        # check timeleft
        credSector = False
        for line in output.split('\n'):
            # look for name:
            match = re.search('^\s+name:\s+([a-zA-Z0-9]+)',line)
            if match is not None:
                if match.group(1) == credname:
                    credSector = True
                else:
                    credSector = False
                continue
            if not credSector:
                continue
            # look for timeleft:
            match = re.search('^\s+timeleft:\s+([0-9:]+)',line)
            if match is not None:
                hour = match.group(1).split(':')[0]
                hour = int(hour)
                # valid more than 3 days
                if hour > 24*3:
                    return True
                return False
        # not found
        return False
