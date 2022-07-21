'''
client methods

'''

import os
import re
import sys
import ssl
import stat
import json
import gzip
import string
import traceback
try:
    from urllib import urlencode, unquote_plus
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError
except ImportError:
    from urllib.parse import urlencode, unquote_plus, urlparse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
import struct
try:
    import cPickle as pickle
except ImportError:
    import pickle
import socket
import random
import tempfile

from . import MiscUtils
from .MiscUtils import commands_get_status_output, commands_get_output, pickle_loads
from . import PLogger
from . import openidc_utils

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except Exception:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except Exception:
    baseURLSSL = 'https://pandaserver.cern.ch/server/panda'

if 'PANDACACHE_URL' in os.environ:
    baseURLCSRVSSL = os.environ['PANDACACHE_URL']
else:
    baseURLCSRVSSL = "https://pandacache.cern.ch/server/panda"

# exit code
EC_Failed = 255

# limit on maxCpuCount
maxCpuCountLimit = 1000000000

# resolve panda cache server's name
if 'PANDA_BEHIND_REAL_LB' not in os.environ:
    netloc = urlparse(baseURLCSRVSSL)
    tmp_host = socket.getfqdn(random.choice(socket.getaddrinfo(netloc.hostname, netloc.port))[-1][0])
    if netloc.port:
        baseURLCSRVSSL = '%s://%s:%s%s' % (netloc.scheme, tmp_host, netloc.port, netloc.path)
    else:
        baseURLCSRVSSL = '%s://%s%s' % (netloc.scheme, tmp_host, netloc.path)


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except Exception:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    if 'PANDA_AUTH' in os.environ and os.environ['PANDA_AUTH'] == 'oidc':
        pass
    else:
        print("No valid grid proxy certificate found")
    return ''


# look for a CA certificate directory
def _x509_CApath():
    if 'X509_CERT_DIR' not in os.environ or os.environ['X509_CERT_DIR'] == '':
        com = "{0} echo $X509_CERT_DIR".format(_getGridSrc())
        output = commands_get_output(com)
        output = output.split('\n')[-1]
        if output == '':
            output = '/etc/grid-security/certificates'
        os.environ['X509_CERT_DIR'] = output
    return os.environ['X509_CERT_DIR']

# keep list of tmp files for cleanup
globalTmpDir = ''


# use OIDC
def use_oidc():
    return 'PANDA_AUTH' in os.environ and os.environ['PANDA_AUTH'] == 'oidc'


# use X509 without grid middleware
def use_x509_no_grid():
    return 'PANDA_AUTH' in os.environ and os.environ['PANDA_AUTH'] == 'x509_no_grid'


# string decode for python 2 and 3
def str_decode(data):
    if hasattr(data, 'decode'):
        try:
            return data.decode()
        except Exception:
            return data.decode('utf-8')
    return data


# check if https
def is_https(url):
    return url.startswith('https://')


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        if 'PANDA_VERIFY_HOST' in os.environ and os.environ['PANDA_VERIFY_HOST'] == 'off':
            self.verifyHost = False
        else:
            self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # auth mode
        self.idToken = None
        self.authVO = None
        if use_oidc():
            self.authMode = 'oidc'
            self.authVO = os.environ['PANDA_AUTH_VO']
        else:
            self.authMode = 'voms'
        # verbose
        self.verbose = False

    # run auth flow
    def get_oidc(self, tmp_log):
        parsed = urlparse(baseURLSSL)
        if parsed.port:
            auth_url = '{0}://{1}:{2}/auth/{3}_auth_config.json'.format(parsed.scheme, parsed.hostname, parsed.port,
                                                                        self.authVO)
        else:
            auth_url = '{0}://{1}/auth/{3}_auth_config.json'.format(parsed.scheme, parsed.hostname, parsed.port,
                                                                    self.authVO)
        oidc = openidc_utils.OpenIdConnect_Utils(auth_url, log_stream=tmp_log, verbose=self.verbose)
        return oidc

    # get ID token
    def get_id_token(self):
        tmp_log = PLogger.getPandaLogger()
        oidc = self.get_oidc(tmp_log)
        s, o = oidc.run_device_authorization_flow()
        if not s:
            tmp_log.error(o)
            sys.exit(EC_Failed)
        self.idToken = o
        return True

    # get token
    def get_token_info(self):
        tmp_log = PLogger.getPandaLogger()
        oidc = self.get_oidc(tmp_log)
        s, o = oidc.run_device_authorization_flow()
        if not s:
            tmp_log.error(o)
            return False, None
        s, o, token_info = oidc.check_token()
        return token_info

    # randomize IP
    def randomize_ip(self, url):
        # not to resolve IP when panda server is running behind real load balancer than DNS LB
        if 'PANDA_BEHIND_REAL_LB' in os.environ:
            return url
        # parse URL
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port
        if port is None:
            if parsed.scheme == 'http':
                port = 80
            else:
                port = 443
        host_names = [socket.getfqdn(vv) for vv in set(
            [v[-1][0] for v in socket.getaddrinfo(host, port, socket.AF_INET)])]
        return url.replace(host, random.choice(host_names))


    # GET method
    def get(self, url, data, rucioAccount=False, via_file=False):
        use_https = is_https(url)
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost or not use_https:
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.authMode == 'oidc':
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey  = _x509()
            com += ' --key %s' % self.sslKey
        # max time of 10 min
        com += ' -m 600'
        # add rucio account info
        if rucioAccount:
            if 'RUCIO_ACCOUNT' in os.environ:
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if 'RUCIO_APPID' in os.environ:
                data['appid'] = os.environ['RUCIO_APPID']
            data['client_version'] = '2.4.1'
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urlencode({key:data[key]})
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD,tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD,tmpName = tempfile.mkstemp()
        os.write(tmpFD, strData.encode())
        os.close(tmpFD)
        tmpNameOut = '{0}.out'.format(tmpName)
        com += ' --config %s' % tmpName
        if via_file:
            com += ' -o {0}'.format(tmpNameOut)
        com += ' %s' % self.randomize_ip(url)
        # execute
        if self.verbose:
            print(com)
            print(strData[:-1])
        s,o = commands_get_status_output(com)
        if o != '\x00':
            try:
                tmpout = unquote_plus(o)
                o = eval(tmpout)
            except Exception:
                pass
        if via_file:
            with open(tmpNameOut, 'rb') as f:
                ret = (s, f.read())
            os.remove(tmpNameOut)
        else:
            ret = (s, o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # POST method
    def post(self,url,data,rucioAccount=False, is_json=False, via_file=False, compress_body=False):
        use_https = is_https(url)
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not use_https:
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.authMode == 'oidc':
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += ' --key %s' % self.sslKey
        if compress_body:
            com += ' -H "Content-Type: application/json"'
        if is_json:
            com += ' -H "Accept: application/json"'
        # max time of 10 min
        com += ' -m 600'
        # add rucio account info
        if rucioAccount:
            if 'RUCIO_ACCOUNT' in os.environ:
                data['account'] = os.environ['RUCIO_ACCOUNT']
            if 'RUCIO_APPID' in os.environ:
                data['appid'] = os.environ['RUCIO_APPID']
            data['client_version'] = '2.4.1'
        # write data to temporary config file
        if globalTmpDir != '':
            tmpFD, tmpName = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmpFD, tmpName = tempfile.mkstemp()
        # data
        strData = ''
        if not compress_body:
            for key in data.keys():
                strData += 'data="%s"\n' % urlencode({key:data[key]})
            os.write(tmpFD, strData.encode('utf-8'))
        else:
            f = os.fdopen(tmpFD, "wb")
            with gzip.GzipFile(fileobj=f, mode='wb') as f_gzip:
                f_gzip.write(json.dumps(data).encode())
            f.close()
        try:
            os.close(tmpFD)
        except Exception:
            pass
        tmpNameOut = '{0}.out'.format(tmpName)
        if not compress_body:
            com += ' --config %s' % tmpName
        else:
            com += ' --data-binary @{}'.format(tmpName)
        if via_file:
            com += ' -o {0}'.format(tmpNameOut)
        com += ' %s' % self.randomize_ip(url)
        # execute
        if self.verbose:
            print(com)
            for key in data:
                print('{}={}'.format(key, data[key]))
        s,o = commands_get_status_output(com)
        if o != '\x00':
            try:
                if is_json:
                    o = json.loads(o)
                else:
                    tmpout = unquote_plus(o)
                    o = eval(tmpout)
            except Exception:
                pass
        if via_file:
            with open(tmpNameOut, 'rb') as f:
                ret = (s, f.read())
            os.remove(tmpNameOut)
        else:
            ret = (s, o)
        # remove temporary file
        os.remove(tmpName)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret

    # PUT method
    def put(self, url, data):
        use_https = is_https(url)
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost or not use_https:
            com += ' --insecure'
        else:
            tmp_x509_CApath = _x509_CApath()
            if tmp_x509_CApath != '':
                com += ' --capath %s' % tmp_x509_CApath
        if self.compress:
            com += ' --compressed'
        if self.authMode == 'oidc':
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % self.randomize_ip(url)
        if self.verbose:
            print(com)
        # execute
        ret = commands_get_status_output(com)
        ret = self.convRet(ret)
        if self.verbose:
            print(ret)
        return ret


    # convert return
    def convRet(self,ret):
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        # add messages to silent errors
        if ret[0] == 35:
            ret = (ret[0],'SSL connect error. The SSL handshaking failed. Check grid certificate/proxy.')
        elif ret[0] == 7:
            ret = (ret[0],'Failed to connect to host.')
        elif ret[0] == 55:
            ret = (ret[0],'Failed sending network data.')
        elif ret[0] == 56:
            ret = (ret[0],'Failure in receiving network data.')
        return ret


class _NativeCurl(_Curl):

    def http_method(self, url, data, header, rdata=None, compress_body=False, is_json=False):
        try:
            use_https = is_https(url)
            url = self.randomize_ip(url)
            if header is None:
                header = {}
            if self.authMode == 'oidc':
                self.get_id_token()
                header['Authorization'] = 'Bearer {0}'.format(self.idToken)
                header['Origin'] = self.authVO
            if compress_body:
                header['Content-Type'] = 'application/json'
            if is_json:
                header['Accept'] = 'application/json'
            if rdata is None:
                if not compress_body:
                    rdata = urlencode(data).encode()
                else:
                    rdata = gzip.compress(json.dumps(data).encode())
            req = Request(url, rdata, headers=header)
            context = ssl._create_unverified_context()
            if use_https and self.authMode != 'oidc':
                if not self.sslCert:
                    self.sslCert = _x509()
                if not self.sslKey:
                    self.sslKey = _x509()
                context.load_cert_chain(certfile=self.sslCert, keyfile=self.sslKey)
            if self.verbose:
                print('url = {}'.format(url))
                print('header = {}'.format(str(header)))
                print('data = {}'.format(str(data)))
            conn = urlopen(req, context=context)
            code = conn.getcode()
            if code == 200:
                code = 0
            text = conn.read()
            if self.verbose:
                print(code, text)
            return code, text
        except Exception as e:
            if self.verbose:
                print (traceback.format_exc())
            errMsg = str(e)
            if hasattr(e, 'fp'):
                errMsg += '. {0}'.format(e.fp.read().decode())
            return 1, errMsg

    # GET method
    def get(self, url, data, rucioAccount=False, via_file=False, output_name=None):
        if data:
            url = '{}?{}'.format(url, urlencode(data))
        code, text = self.http_method(url, {}, {})
        if code == 0 and output_name:
            with open(output_name, 'wb') as f:
                f.write(text)
            text = True
        return code, text

    # POST method
    def post(self,url,data,rucioAccount=False, is_json=False, via_file=False, compress_body=False):
        code, text = self.http_method(url, data, {}, compress_body=True, is_json=is_json)
        if is_json and code == 0:
            text = json.loads(text)
        return code, text

    # PUT method
    def put(self, url, data):
        boundary = ''.join(random.choice(string.ascii_letters) for ii in range(30 + 1))
        body = b''
        for k in data:
            lines = ['--' + boundary,
                     'Content-Disposition: form-data; name="%s"; filename="%s"' % (k, data[k]),
                     'Content-Type: application/octet-stream',
                     '']
            body += '\r\n'.join(lines).encode()
            body += b'\r\n'
            body += open(data[k], 'rb').read()
            body += b'\r\n'
        lines = ['--%s--' % boundary, '']
        body += '\r\n'.join(lines).encode()
        headers = {'content-type': 'multipart/form-data; boundary=' + boundary,
                   'content-length': str(len(body))}
        return self.http_method(url, None, headers, body)


if 'PANDA_USE_NATIVE_HTTPLIB' in os.environ:
    _Curl = _NativeCurl


# dump log
def dump_log(func_name, exception_obj, output):
    print(traceback.format_exc())
    print(output)
    err_str = "{} failed : {}".format(func_name, str(exception_obj))
    tmp_log = PLogger.getPandaLogger()
    tmp_log.error(err_str)
    return err_str


'''
public methods

'''

# submit jobs
def submitJobs(jobs,verbose=False):
    # set hostname
    hostname = commands_get_output('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs, protocol=0)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/submitJobs'
    data = {'jobs':strJobs}
    status,output = curl.post(url, data, via_file=True)
    if status != 0:
        print(output)
        return status,None
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("submitJobs", e, output)
        return EC_Failed,None


# get job statuses
def getJobStatus(ids, verbose=False):
    """Get status of jobs

       args:
           ids: a list of PanDA IDs
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           a list of job specs, or None if failed
    """
    # serialize
    strIDs = pickle.dumps(ids, protocol=0)
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url, data, via_file=True)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getJobStatus", e, output)
        return EC_Failed,None


# kill jobs
def killJobs(ids,verbose=False):
    """Kill jobs

       args:
           ids: a list of PanDA IDs
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           a list of server responses, or None if failed
    """
    # serialize
    strIDs = pickle.dumps(ids, protocol=0)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/killJobs'
    data = {'ids':strIDs}
    status,output = curl.post(url, data, via_file=True)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("killJobs", e, output)
        return EC_Failed,None


# kill task
def killTask(jediTaskID,verbose=False):
    """Kill a task
       args:
          jediTaskID: jediTaskID of the task to be killed
          verbose: True to see debug messages
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          tuple of return code and diagnostic message, or None if failed
             0: request is registered
             1: server error
             2: task not found
             3: permission denied
             4: irrelevant task status
           100: non SSL connection
           101: irrelevant taskID
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/killTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("killTask", e, output)
        return EC_Failed,None


# finish task
def finishTask(jediTaskID,soft=False,verbose=False):
    """finish a task
       args:
          jediTaskID: jediTaskID of the task to finish
          soft: True to wait until running jobs are done
          verbose: True to see debug messages
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          tuple of return code and diagnostic message, or None if failed
             0: request is registered
             1: server error
             2: task not found
             3: permission denied
             4: irrelevant task status
           100: non SSL connection
           101: irrelevant taskID
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/finishTask'
    data = {'jediTaskID':jediTaskID}
    if soft:
        data['soft'] = True
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("finishTask", e, output)
        return EC_Failed,None


# retry task
def retryTask(jediTaskID,verbose=False,properErrorCode=False,newParams=None):
    """retry a task
       args:
          jediTaskID: jediTaskID of the task to retry
          verbose: True to see debug messages
          newParams: a dictionary of task parameters to overwrite
          properErrorCode: True to get a detailed error code
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          tuple of return code and diagnostic message, or None if failed
             0: request is registered
             1: server error
             2: task not found
             3: permission denied
             4: irrelevant task status
           100: non SSL connection
           101: irrelevant taskID
    """
    if newParams is None:
        newParams = {}
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/retryTask'
    data = {'jediTaskID':jediTaskID,
            'properErrorCode':properErrorCode}
    if newParams != {}:
        data['newParams'] = json.dumps(newParams)
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("retryTask", e, output)
        return EC_Failed,None



# put file
def putFile(file,verbose=False,useCacheSrv=False,reuseSandbox=False):
    """Upload a file with the size limit on 10 MB
       args:
          file: filename to be uploaded
          verbose: True to see debug messages
          useCacheSrv: True to use a dedicated cache server separated from the PanDA server
          reuseSandbox: True to avoid uploading the same sandbox files
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          diagnostic message
    """
    # size check for noBuild
    sizeLimit = 10*1024*1024
    fileSize = os.stat(file)[stat.ST_SIZE]
    if not os.path.basename(file).startswith('sources.'):
        if fileSize > sizeLimit:
            errStr  = 'Exceeded size limit (%sB >%sB). ' % (fileSize,sizeLimit)
            errStr += 'Your working directory contains too large files which cannot be put on cache area. '
            errStr += 'Please submit job without --noBuild/--libDS so that your files will be uploaded to SE'
            # get logger
            tmpLog = PLogger.getPandaLogger()
            tmpLog.error(errStr)
            return EC_Failed,'False'
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # check duplication
    if reuseSandbox:
        # get CRC
        fo = open(file, 'rb')
        fileContent = fo.read()
        fo.close()
        footer = fileContent[-8:]
        checkSum, i_size = struct.unpack("II",footer)
        # check duplication
        url = baseURLSSL + '/checkSandboxFile'
        data = {'fileSize':fileSize,'checkSum':checkSum}
        status, output = curl.post(url,data)
        output = str_decode(output)
        if status != 0:
            return EC_Failed,'ERROR: Could not check sandbox duplication with %s' % output
        elif output.startswith('FOUND:'):
            # found reusable sandbox
            hostName,reuseFileName = output.split(':')[1:]
            # set cache server hostname
            setCacheServer(hostName)
            # return reusable filename
            return 0,"NewFileName:%s" % reuseFileName
    # execute
    if not useCacheSrv:
        global baseURLCSRVSSL
        baseURLCSRVSSL = baseURLSSL
    url = baseURLCSRVSSL + '/putFile'
    data = {'file':file}
    s,o = curl.put(url,data)
    return s, str_decode(o)


# get file
def getFile(filename, output_path=None, verbose=False):
    """Get a file
       args:
          filename: filename to be downloaded
          output_path: output path. set to filename if unspecified
          verbose: True to see debug messages
       returns:
          status code
             0: communication succeeded to the panda server
             1: communication failure
          True if succeeded. diagnostic message otherwise
    """
    if not output_path:
        output_path = filename
    # instantiate curl
    curl = _NativeCurl()
    curl.verbose = verbose
    # execute
    netloc = urlparse(baseURLCSRVSSL)
    url = '%s://%s' % (netloc.scheme, netloc.hostname)
    if netloc.port:
        url += ':%s' % netloc.port
    url = url + '/cache/' + filename
    s, o = curl.get(url, {}, output_name=output_path)
    return s, o


# get grid source file
def _getGridSrc():
    if 'PATHENA_GRID_SETUP_SH' in os.environ:
        gridSrc = os.environ['PATHENA_GRID_SETUP_SH']
    else:
        gridSrc = '/dev/null'
    gridSrc = 'source %s > /dev/null;' % gridSrc
    # some grid_env.sh doen't correct PATH/LD_LIBRARY_PATH
    gridSrc = "unset LD_LIBRARY_PATH; unset PYTHONPATH; unset MANPATH; export PATH=/usr/local/bin:/bin:/usr/bin; %s" % \
              gridSrc
    return gridSrc


# get DN
def getDN(origString):
    shortName = ''
    distinguishedName = ''
    for line in origString.split('/'):
        if line.startswith('CN='):
            distinguishedName = re.sub('^CN=','',line)
            distinguishedName = re.sub('\d+$','',distinguishedName)
            distinguishedName = re.sub('\.','',distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(' ',distinguishedName) is not None:
                # look for full name
                distinguishedName = distinguishedName.replace(' ','')
                break
            elif shortName == '':
                # keep short name
                shortName = distinguishedName
            distinguishedName = ''
    # use short name
    if distinguishedName == '':
        distinguishedName = shortName
    # return
    return distinguishedName


# use dev server
def useDevServer():
    global baseURL
    baseURL = 'http://aipanda007.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://aipanda007.cern.ch:25443/server/panda'
    global baseURLCSRVSSL
    baseURLCSRVSSL = 'https://aipanda007.cern.ch:25443/server/panda'


# use INTR server
def useIntrServer():
    global baseURL
    baseURL = 'http://aipanda059.cern.ch:25080/server/panda'
    global baseURLSSL
    baseURLSSL = 'https://aipanda059.cern.ch:25443/server/panda'
    global baseURLCSRVSSL
    baseURLCSRVSSL = baseURLSSL


# set cache server
def setCacheServer(host_name):
    global baseURLCSRVSSL
    netloc = urlparse(baseURLCSRVSSL)
    if netloc.port:
        baseURLCSRVSSL = '%s://%s:%s%s' % (netloc.scheme, host_name, netloc.port, netloc.path)
    else:
        baseURLCSRVSSL = '%s://%s%s' % (netloc.scheme, host_name, netloc.path)


# register proxy key
def registerProxyKey(credname,origin,myproxy,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    curl.verifyHost = True
    # execute
    url = baseURLSSL + '/registerProxyKey'
    data = {'credname': credname,
            'origin'  : origin,
            'myproxy' : myproxy
            }
    return curl.post(url,data)


# get proxy key
def getProxyKey(verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getProxyKey'
    status,output = curl.post(url,{})
    if status!=0:
        print(output)
        return status,None
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getProxyKey", e, output)
        return EC_Failed,None


# get JobIDs and jediTasks in a time range
def getJobIDsJediTasksInTimeRange(timeRange, dn=None, minTaskID=None, verbose=False, task_type='user'):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getJediTasksInTimeRange'
    data = {'timeRange': timeRange,
            'fullFlag': True,
            'task_type': task_type}
    if dn is not None:
        data['dn'] = dn
    if minTaskID is not None:
        data['minTaskID'] = minTaskID
    status,output = curl.post(url, data, via_file=True)
    if status!=0:
        print(output)
        return status, None
    try:
        jediTaskDicts = pickle_loads(output)
        return 0, jediTaskDicts
    except Exception as e:
        dump_log("getJediTasksInTimeRange", e, output)
        return EC_Failed, None


# get details of jedi task
def getJediTaskDetails(taskDict,fullFlag,withTaskInfo,verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getJediTaskDetails'
    data = {'jediTaskID':taskDict['jediTaskID'],
            'fullFlag':fullFlag,
            'withTaskInfo':withTaskInfo}
    status,output = curl.post(url,data)
    if status != 0:
        print(output)
        return status,None
    try:
        tmpDict = pickle_loads(output)
        # server error
        if tmpDict == {}:
            print("ERROR getJediTaskDetails got empty")
            return EC_Failed,None
        # copy
        for tmpKey in tmpDict:
            tmpVal = tmpDict[tmpKey]
            taskDict[tmpKey] = tmpVal
        return 0,taskDict
    except Exception as e:
        dump_log("getJediTaskDetails", e, output)
        return EC_Failed,None


# get full job status
def getFullJobStatus(ids, verbose=False):
    """Get detailed status of jobs

       args:
           ids: a list of PanDA IDs
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           a list of job specs, or None if failed
    """
    # serialize
    strIDs = pickle.dumps(ids, protocol=0)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getFullJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url, data, via_file=True)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getFullJobStatus", e, output)
        return EC_Failed,None


# set debug mode
def setDebugMode(pandaID,modeOn,verbose):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/setDebugMode'
    data = {'pandaID':pandaID,'modeOn':modeOn}
    status,output = curl.post(url,data)
    try:
        return status,output
    except Exception as e:
        errStr = dump_log("setDebugMode", e, output)
        return EC_Failed,errStr


# set tmp dir
def setGlobalTmpDir(tmpDir):
    global globalTmpDir
    globalTmpDir = tmpDir


# get client version
def getPandaClientVer(verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getPandaClientVer'
    status,output = curl.get(url,{})
    output = str_decode(output)
    # failed
    if status != 0:
        return status,output
    # check format
    if re.search('^\d+\.\d+\.\d+$',output) is None:
        return EC_Failed,"invalid version '%s'" % output
    # return
    return status,output


# get list of cache prefix
# OBSOLETE to be removed in a future release
def getCachePrefixes(verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getCachePrefixes'
    status,output = curl.get(url,{})
    # failed
    if status != 0:
        print(output)
        errStr = "cannot get the list of Athena projects"
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    try:
        tmpList = pickle_loads(output)
        tmpList.append('AthAnalysisBase')
        return tmpList
    except Exception as e:
        dump_log("getCachePrefixes", e, output)
        sys.exit(EC_Failed)


# get list of cmtConfig
# OBSOLETE to be removed in a future release
def getCmtConfigList(athenaVer,verbose):
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getCmtConfigList'
    data = {}
    data['relaseVer'] = athenaVer
    status,output = curl.get(url,data)
    # failed
    if status != 0:
        print(output)
        errStr = "cannot get the list of cmtconfig for %s" % athenaVer
        tmpLog = PLogger.getPandaLogger()
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return
    try:
        return pickle_loads(output)
    except Exception as e:
        dump_log("getCmtConfigList", e, output)
        sys.exit(EC_Failed)


# request EventPicking
def requestEventPicking(eventPickEvtList,eventPickDataType,eventPickStreamName,
                        eventPickDS,eventPickAmiTag,fileList,fileListName,outDS,
                        lockedBy,params,eventPickNumSites,eventPickWithGUID,ei_api,
                        verbose=False):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # list of input files
    strInput = ''
    for tmpInput in fileList:
        if tmpInput != '':
            strInput += '%s,' % tmpInput
    if fileListName != '':
        for tmpLine in open(fileListName):
            tmpInput = re.sub('\n','',tmpLine)
            if tmpInput != '':
                strInput += '%s,' % tmpInput
    strInput = strInput[:-1]
    # make dataset name
    userDatasetName = '%s.%s.%s/' % tuple(outDS.split('.')[:2]+[MiscUtils.wrappedUuidGen()])
    # open run/event number list
    evpFile = open(eventPickEvtList)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/putEventPickingRequest'
    data = {'runEventList'        : evpFile.read(),
            'eventPickDataType'   : eventPickDataType,
            'eventPickStreamName' : eventPickStreamName,
            'eventPickDS'         : eventPickDS,
            'eventPickAmiTag'     : eventPickAmiTag,
            'userDatasetName'     : userDatasetName,
            'lockedBy'            : lockedBy,
            'giveGUID'            : eventPickWithGUID,
            'params'              : params,
            'inputFileList'       : strInput,
            }
    if eventPickNumSites > 1:
        data['eventPickNumSites'] = eventPickNumSites
    if ei_api:
        data['ei_api'] = ei_api
    evpFile.close()
    status,output = curl.post(url,data)
    # failed
    if status != 0 or output is not True: 
        print(output)
        errStr = "failed to request EventPicking"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return user dataset name
    return True,userDatasetName


# submit task
def insertTaskParams(taskParams, verbose=False, properErrorCode=False, parent_tid=None):
    """Insert task parameters

       args:
           taskParams: a dictionary of task parameters
           verbose: True to see verbose messages
           properErrorCode: True to get a detailed error code
           parent_tid: ID of the parent task
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           tuple of return code, message from the server, and taskID if successful, or error message if failed
                 0: request is processed
                 1: duplication in DEFT
                 2: duplication in JEDI
                 3: accepted for incremental execution
                 4: server error
    """
    # serialize
    taskParamsStr = json.dumps(taskParams)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/insertTaskParams'
    data = {'taskParams':taskParamsStr,
            'properErrorCode':properErrorCode}
    if parent_tid:
        data['parent_tid'] = parent_tid
    status,output = curl.post(url,data)
    try:
        loaded_output = pickle_loads(output)
        # got error message from the server
        if loaded_output == output:
            print(output)
            return EC_Failed, output
        loaded_output = list(loaded_output)
        # extract taskID
        try:
            m = re.search('jediTaskID=(\d+)', loaded_output[-1])
            taskID = int(m.group(1))
        except Exception:
            taskID = None
        loaded_output.append(taskID)
        return status, loaded_output
    except Exception as e:
        errStr = dump_log("insertTaskParams", e, output)
        return EC_Failed, output+'\n'+errStr


# get PanDA IDs with TaskID
def getPandaIDsWithTaskID(jediTaskID,verbose=False):
    """Get PanDA IDs with TaskID

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of PanDA IDs, or error message if failed
    """
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getPandaIDsWithTaskID'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getPandaIDsWithTaskID", e, output)
        return EC_Failed,output+'\n'+errStr


# reactivate task
def reactivateTask(jediTaskID,verbose=False):
    """Reactivate task

       args:
           jediTaskID: jediTaskID of the task to be reactivated
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message, or error message if failed
                 0: unknown task
                 1: succeeded
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/reactivateTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("reactivateTask", e, output)
        return EC_Failed,output+'\n'+errStr

# resume task
def resumeTask(jediTaskID,verbose=False):
    """Resume task

       args:
           jediTaskID: jediTaskID of the task to be resumed
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message, or error message if failed
                 0: request is registered
                 1: server error
                 2: task not found
                 3: permission denied
                 4: irrelevant task status
                 100: non SSL connection
                 101: irrelevant taskID
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/resumeTask'
    data = {'jediTaskID': jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle.loads(output)
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        errStr = "ERROR resumeTask : %s %s" % (errtype, errvalue)
        return EC_Failed,output+'\n'+errStr

# get task status TaskID
def getTaskStatus(jediTaskID,verbose=False):
    """Get task status

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
           verbose: True to see verbose messages
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the status string, or error message if failed
    """
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/getTaskStatus'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getTaskStatus", e, output)
        return EC_Failed,output+'\n'+errStr


# get taskParamsMap with TaskID
def getTaskParamsMap(jediTaskID):
    """Get task parameters

       args:
           jediTaskID: jediTaskID of the task to get taskParamsMap
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tuple of return code and taskParamsMap, or error message if failed
                 1: logical error
                 0: success
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getTaskParamsMap'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("getTaskParamsMap", e, output)
        return EC_Failed,output+'\n'+errStr


# get user job metadata
def getUserJobMetadata(task_id, verbose=False):
    """Get metadata of all jobs in a task
       args:
          jediTaskID: jediTaskID of the task
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a list of job metadata dictionaries, or error message if failed
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getUserJobMetadata'
    data = {'jediTaskID': task_id}
    status,output = curl.post(url, data, is_json=True)
    try:
        return (0, output)
    except Exception as e:
        errStr = dump_log("getUserJobMetadata", e, output)
        return EC_Failed, errStr


# hello
def hello(verbose=False):
    """Health check with the PanDA server
       args:
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          diagnostic message
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/isAlive'
    try:
        status, output = curl.post(url, {})
        output = str_decode(output)
        if status != 0:
            msg = "Not good. " + output
            tmp_log.error(msg)
            return EC_Failed, msg
        elif output != "alive=yes":
            msg = "Not good. " + output
            tmp_log.error(msg)
            return EC_Failed, msg
        else:
            msg = "OK"
            tmp_log.info(msg)
            return 0, msg
    except Exception as e:
        msg = "Too bad. {}".format(str(e))
        tmp_log.error(msg)
        return EC_Failed, msg


# get certificate attributes
def get_cert_attributes(verbose=False):
    """Get certificate attributes from the PanDA server
       args:
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a dictionary of attributes or diagnostic message
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/getAttr'
    try:
        status, output = curl.post(url, {})
        output = str_decode(output)
        if status != 0:
            msg = "Not good. " + output
            tmp_log.error(msg)
            return EC_Failed, msg
        else:
            d = dict()
            for l in output.split('\n'):
                if ':' not in l:
                    continue
                l = l.encode('utf-8')
                print(l)
                if not l.startswith('GRST_CRED'):
                    continue
                items = l.split(':')
                d[items[0].strip()] = items[1].strip()
            return 0, d
    except Exception as e:
        msg = "Too bad. {}".format(str(e))
        tmp_log.error(msg)
        print(traceback.format_exc())
        return EC_Failed, msg


# get user name from token
def get_user_name_from_token():
    """Extract user name and groups from ID token

       returns:
          a tuple of username and groups
    """
    curl = _Curl()
    token_info = curl.get_token_info()
    try:
        return token_info['name'], token_info['groups'], token_info['preferred_username']
    except Exception:
        return None, None


# call idds command
def call_idds_command(command_name, args=None, kwargs=None, dumper=None, verbose=False, compress=False,
                      manager=False, loader=None):
    """Call an iDDS command through PanDA
       args:
          command_name: command name
          args: a list of positional arguments
          kwargs: a dictionary of keyword arguments
          dumper: function object for json.dumps
          verbose: True to see verbose message
          compress: True to compress request body
          manager: True to use ClientManager
          loader: function object for json.loads
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True, response from iDDS), or (False, diagnostic message) if failed
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/relay_idds_command'
    try:
        data = dict()
        data['command_name'] = command_name
        if args:
            if dumper is None:
                data['args'] = json.dumps(args)
            else:
                data['args'] = dumper(args)
        if kwargs:
            if dumper is None:
                data['kwargs'] = json.dumps(kwargs)
            else:
                data['kwargs'] = dumper(kwargs)
        if manager:
            data['manager'] = True
        status, output = curl.post(url, data, compress_body=compress)
        if status != 0:
            return EC_Failed, output
        else:
            try:
                if loader:
                    return 0, loader(output)
                else:
                    return 0, json.loads(output)
            except Exception:
                return EC_Failed, output
    except Exception as e:
        msg = "Failed with {}".format(str(e))
        print (traceback.format_exc())
        return EC_Failed, msg


# call idds user workflow command
def call_idds_user_workflow_command(command_name, kwargs=None, verbose=False):
    """Call an iDDS workflow user command
       args:
          command_name: command name
          kwargs: a dictionary of keyword arguments
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True, response from iDDS), or (False, diagnostic message) if failed
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/execute_idds_workflow_command'
    try:
        data = dict()
        data['command_name'] = command_name
        if kwargs:
            data['kwargs'] = json.dumps(kwargs)
        status, output = curl.post(url, data)
        if status != 0:
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = "Failed with {}".format(str(e))
        print (traceback.format_exc())
        return EC_Failed, msg


# send file recovery request
def send_file_recovery_request(task_id, dry_run=False, verbose=False):
    """Send a file recovery request
       args:
          task_id: task ID
          dry_run: True to run in the dry run mode
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True/False and diagnostic message). True if the request was accepted
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    output = None
    url = baseURLSSL + '/put_file_recovery_request'
    try:
        data = {'jediTaskID': task_id}
        if dry_run:
            data['dryRun'] = True
        status, output = curl.post(url, data)
        if status != 0:
            output = str_decode(output)
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = '{}.'.format(str(e))
        if output:
            msg += ' raw output="{}"'.format(str(output))
        tmp_log.error(msg)
        return EC_Failed, msg


# send workflow request
def send_workflow_request(params, relay_host=None, check=False, verbose=False):
    """Send a workflow request
       args:
          params: a workflow request dictionary
          relay_host: relay hostname to send request
          check: only check the workflow description
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True/False and diagnostic message). True if the request was accepted
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    output = None
    if relay_host:
        url = 'https://{}:25443/server/panda'.format(relay_host)
    else:
        url = baseURLSSL
    url += '/put_workflow_request'
    try:
        data = {'data': json.dumps(params)}
        if check:
            data['check'] = True
        status, output = curl.post(url, data, compress_body=True, is_json=True)
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, output
    except Exception as e:
        msg = '{}.'.format(str(e))
        if output:
            msg += ' raw output="{}"'.format(str(output))
        tmp_log.error(msg)
        return EC_Failed, msg


# set user secret
def set_user_secert(key, value, verbose=False):
    """Set a user secret
       args:
          key: secret name. None to delete all secrets
          value: secret value. None to delete the secret
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True/False and diagnostic message). True if the request was accepted
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/set_user_secret'
    try:
        data = dict()
        if key:
            data['key'] = key
            if value:
                data['value'] = value
        status, output = curl.post(url, data)
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = '{}.'.format(str(e))
        if output:
            msg += ' raw output="{}"'.format(str(output))
        tmp_log.error(msg)
        return EC_Failed, msg


# get user secret
def get_user_secerts(verbose=False):
    """Get user secrets
       args:
          verbose: True to see verbose message
       returns:
          status code
             0: communication succeeded to the panda server
           255: communication failure
          a tuple of (True/False and a dict of secrets). True if the request was accepted
    """
    tmp_log = PLogger.getPandaLogger()
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/get_user_secrets'
    try:
        status, output = curl.post(url, {})
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            output = json.loads(output)
            if not output[0]:
                return 0, output
            if not output[1]:
                return 0, (output[0], {})
            return 0, (output[0], json.loads(output[1]))
    except Exception as e:
        msg = '{}.'.format(str(e))
        if output:
            msg += ' raw output="{}"'.format(str(output))
        tmp_log.error(msg)
        return EC_Failed, msg


# increase attempt numbers to retry failed jobs
def increase_attempt_nr(task_id, increase=3, verbose=False):
    """increase attempt numbers to retry failed jobs
       args:
          task_id: jediTaskID of the task
          increase: increase for attempt numbers
          verbose: True to see verbose message
       returns:
          status code
                0: communication succeeded to the panda server
                255: communication failure
          return code
                0: succeeded
                1: unknown task
                2: invalid task status
                3: permission denied
                4: wrong parameter
                None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/increaseAttemptNrPanda'
    data = {'jediTaskID':task_id,
            'increasedNr':increase}
    status,output = curl.post(url, data)
    try:
        return 0, pickle_loads(output)
    except Exception as e:
        errStr = dump_log("increaseAttemptNrPanda", e, output)
        return EC_Failed, errStr
