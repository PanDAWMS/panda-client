"""
DaTRI Handler for external applications (curl, python ver. >= 2.4)
CERN, ATLAS Distributed Computing (March 2010)

@author: Mikhail Titov
@contact: mikhail.titov@cern.ch
@data: February 03, 2012
@version: 0.95.2
"""

import os
import subprocess
import urllib

HTTPS_PORT = 25943
PANDAMON_HOST = 'panda.cern.ch'
PANDAMON_URI = '/server/pandamon/query'

PARAMS_LIST = ['mode', 'action', 'dpat', 'site', 'userid']
MODE = {
    'pathena': 'ddm_pathenareq',
    'ganga': 'ddm_gangareq',
    'group': 'ddm_groupreq'
}

RETRY_NUM = 2


def execute(params):

    """Returns tuple (out, err)

        @param params (@type list)
            shell command (1st parameter) and its options
    """
    try:
        p = subprocess.Popen(params, stdout=subprocess.PIPE)
    except (OSError, ValueError), e:
        return '', 'SubprocessException: %s' % e
    else:
        return p.communicate()


class datriHandler(object):

    """Class datriHandler
    """

    def __init__(self, type='pathena'):

        """Initialization

            @param type (@type str)
                has one of the next values: 'pathena'|'ganga'|'group'
        """
        self.__init_attrs()
        self.curl = datriCurl()
        self.info['mode'] = MODE.get(type, '')
        if not self.info['mode']:
            self.err_message = 'datriHandler: mode is incorrect'

    def __init_attrs(self):
        self.info = {}
        self.curl = None
        self.err_message = ''

    def __del__(self):
        self.info.clear()
        self.curl = None
        self.err_message = ''

    def hasParams(self):

        """Check that parameters are defined and are not null

            @return (@type bool)
                True/False
        """
        for p in PARAMS_LIST:
            if not self.info.get(p, None):
                return False
        return True

    def setParameters(self, data_pattern, site, userid, **kwargs):

        """Define request parameters

            @param data_pattern (@type str)
                dataset | container | pattern
            @param site (@type str)
                destination site (see AGIS/TiersOfAtlas)
            @param userid (@type str)
                unique user identification (certificate dn | email)
        """
        if data_pattern and site and userid:
            self.info.update({'dpat': data_pattern, 'site': site, 'userid': userid})
            if kwargs:
                self.info.update(kwargs)
        else:
            self.err_message = 'datriHandler: required data are not defined'

    def checkData(self):

        """Check request data (send "Check"-request)

            @return (@type typle: int, str)
                returns status code and info (error) message
        """
        if not self.err_message:
            self.info['action'] = 'Check'
            if self.hasParams():
                return self.curl.get(**self.info)
            else:
                self.err_message = 'datriHandler: required data are not defined'
        return 4, self.err_message

    def sendRequest(self):

        """Send request to DaTRI (send "Submit"-request)

            @return (@type typle: int, str)
                returns status code and info (error) message
        """
        if not self.err_message:
            self.info['action'] = 'Submit'
            if self.hasParams():
                return self.curl.get(**self.info)
            else:
                self.err_message = 'datriHandler: required data are not defined'
        return 4, self.err_message

# - Class for https-request definition -

class datriCurl(object):

    """Class datriCurl for curl-command creation
    """

    def __init__(self):
        self.err_message = ''
        self.cmd_params = ['curl'] + \
            ['-s', '-G', '--user-agent', 'datricurl', '--max-redirs', '7', '-m', '90']
        self._user_proxy()
        self._ca_path()
        # - url definition -
        self.url = 'https://' + PANDAMON_HOST + ':' + str(HTTPS_PORT) + PANDAMON_URI

    def _user_proxy(self):
        if os.environ.get('X509_USER_PROXY', ''):
            self.cmd_params.extend(['-E', os.environ['X509_USER_PROXY']])
        else:
            try:
                x509_user_proxy = '/tmp/x509up_u%s' % os.getuid()
            except:
                pass
            else:
                if os.access(x509_user_proxy, os.R_OK):
                    self.cmd_params.extend(['-E', x509_user_proxy])
                    return
            self.err_message += 'User proxy certificate is not defined; '
        return

    def _ca_path(self):
        if os.environ.get('X509_CERT_DIR', ''):
            self.cmd_params.extend(['--capath', os.environ['X509_CERT_DIR']])
            return
        self.err_message += 'CA-path is not defined; '

    # - method GET -
    def get(self, **kwargs):

        """Returns status code and response message

            @param kwargs (@type dict)
                parameters for DaTRI request definition (see PARAMS_LIST)
            @return (@type typle: int, str)
                returns status code and info (error) message
        """
        if not self.err_message:
            if not kwargs:
                return 2, 'datriCurl: input parameters are not defined'
            o, e = '', '<retry_number> is not defined'
            # - several attempts for cmd execution - begin -
            cmd_params = self.cmd_params + ['--url', self.url + '?' + urllib.urlencode(kwargs)]
            for i in range(RETRY_NUM):
                o, e = execute(cmd_params)
                if o and not e:
                    return (0, o) if o.startswith('OK.') else (1, o)
            # - several attempts for cmd execution - end -
            return 3, 'datriCurl: execution error (output=%s, error=%s)' % (o, e)
        return 5, 'datriCurl: %s' % self.err_message


#######################################################################################
# datriHandler - Status code definition:                                              #
#                                                                                     #
# 0 - DaTRI request - CREATED SUCCESSFULLY                                            #
#                                                                                     #
# 1 - DaTRI request - NOT CREATED [due to incorrect input data]                       #
#     datriHandler - EXECUTED SUCCESSFULLY                                            #
#                                                                                     #
# 2 - DaTRI request - NOT CREATED                                                     #
#     datriHandler - FAILED [due to lack of input data at datriCurl.get]              #
#                                                                                     #
# 3 - DaTRI request - NOT CREATED                                                     #
#     datriHandler - FAILED [due to failure at datriCurl.get]                         #
#                                                                                     #
# 4 - DaTRI request - NOT CREATED                                                     #
#     datriHandler - FAILED [due to lack of input data at datriHandler.setParameters] #
#                                                                                     #
# 5 - DaTRI request - NOT CREATED                                                     #
#     datriHandler - FAILED [due to failure at datriCurl]                             #
#######################################################################################
