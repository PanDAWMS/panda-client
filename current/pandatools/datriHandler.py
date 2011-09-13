"""
DaTRI Handler (uses curl)
CERN, ATLAS Distributed Computing (March 2010)

@author: Mikhail Titov
@contact: mikhail.titov@cern.ch
@data: April 27, 2010
@version: 0.7
"""

import commands
import os
import re
import urllib

HTTPS_PORT = 25943
PANDAMON_HOST = 'panda.cern.ch'
PANDAMON_URI = '/server/pandamon/query'


class datriHandler(object):

    """
    Class datriHandler
    """

    info = {}
    err_message = ''
    curl = None

    def __init__(self, type='external'):

        """
        Init definition
        
        @type: 'external'|'pathena'|'ganga'
        """

        self.curl = pandamonCurl()
        self.info['mode'] = ''
        if (type == 'pathena'):
            self.info['mode'] = 'ddm_pathenareq'
        elif (type == 'ganga'):
            self.info['mode'] = 'ddm_gangareq'
        elif (type == 'external'):
            self.info['mode'] = 'ddm_exreq'

    def __del__(self):
        self.info.clear()
        self.err_message = ''
        self.curl = None

    def setParameters(self, data_pattern, site, userid):

        """
        Define request parameters
        
        @data_pattern: dataset | container | pattern
        @site: destination site (see TiersOfAtlas)
        @userid: unique user identification (cert dn | email)
        """

        if (data_pattern and site and userid):
            self.info.update({'dpat': data_pattern, 'site': site, 'userid': userid})
        else:
            self.err_message = 'datriHandler: not all data is defined'

    def checkData(self):

        """
        Check request data (send Check request)
        
        Returns status code and info-message
        """

        if not self.err_message:
            self.info['action'] = 'Check'
            return self.curl.get(**self.info)
        return 4, self.err_message


    def sendRequest(self):

        """
        Send request to DaTRI (send Submit request)
        
        Returns status code and info-message
        """

        if not self.err_message:
            self.info['action'] = 'Submit'
            return self.curl.get(**self.info)
        return 4, self.err_message


# - Class for https-request definition -

class pandamonCurl(object):

    """
    Class pandamonCurl for curl-command creation
    """

    def __init__(self):
        self.err_message = ''
        self.cmd = 'curl --user-agent "dqcurl" '
        self._user_proxy()
        self._ca_path()
        # - url definition -
        self.url = 'https://%(host)s:%(port)s%(uri)s' % {'host': PANDAMON_HOST, 'port': HTTPS_PORT, 'uri': PANDAMON_URI}

    def _user_proxy(self):
        if (os.environ.has_key('X509_USER_PROXY') and os.environ['X509_USER_PROXY']):
            self.cmd += '--cert $X509_USER_PROXY --key $X509_USER_PROXY '
            return
        # see the default place
        try:
            user_proxy = '/tmp/x509up_u%s' % os.getuid()
        except:
            pass
        else:
            if os.access(user_proxy, os.R_OK):
                self.cmd += '--cert %(proxy)s --key %(proxy)s ' % {'proxy': user_proxy}
                return
        self.err_message += 'User certificate is not defined; '

    def _ca_path(self):
        if (os.environ.has_key('X509_CERT_DIR') and os.environ['X509_CERT_DIR']):
            self.cmd += '--capath %s ' % os.environ['X509_CERT_DIR']
            return
        self.err_message += 'CA-path is not defined; '

    # - method GET -
    def get(self, **kwargs):

        """
        Returns status code and response message
        """

        cmd = '%s --silent --get ' % self.cmd

        if kwargs:
            params = urllib.urlencode(kwargs)
            cmd += '--url "%(url)s?%(params)s" ' % {'url': self.url, 'params': params}
        else:
            return 2, 'pandamonCurl: input parameters are not defined'

        (s, o) = commands.getstatusoutput(cmd)
        if not s:
            if ('OK.' in o):
                return s, o
            return 1, o
        else:
            if o:
                o = ' (' + o + ')'
            else:
                if self.err_message:
                    o = ' (' + self.err_message.strip() + ')'
                else:
                    o = ''
            return 3, 'pandamonCurl: executing error%s' % o


