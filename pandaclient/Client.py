"""
client methods

"""

import gzip
import inspect
import json
import os
import re
import ssl
import stat
import string
import sys
import time
import traceback

from datetime import datetime

try:
    # python 2
    from urllib import unquote_plus, urlencode
    from urllib2 import HTTPError, Request, urlopen
    from urlparse import urlparse
except ImportError:
    # python 3
    from urllib.parse import urlencode, unquote_plus, urlparse
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError

import struct

try:
    import cPickle as pickle
except ImportError:
    import pickle

import random
import socket
import tempfile
from io import BytesIO

from . import MiscUtils, PLogger, openidc_utils
from .MiscUtils import commands_get_output, commands_get_status_output, pickle_loads

# configuration
try:
    baseURL = os.environ["PANDA_URL"]
    parsed = urlparse(baseURL)
    server_base_path = "{0}://{1}/api/v1".format(parsed.scheme, parsed.netloc)
except Exception:
    baseURL = "http://pandaserver.cern.ch:25080/server/panda"
    server_base_path = "http://pandaserver.cern.ch:25080/api/v1"

try:
    baseURLSSL = os.environ["PANDA_URL_SSL"]
    parsed = urlparse(baseURLSSL)
    server_base_path_ssl = "{0}://{1}/api/v1".format(parsed.scheme, parsed.netloc)
except Exception:
    baseURLSSL = "https://pandaserver.cern.ch/server/panda"
    server_base_path_ssl = "https://pandaserver.cern.ch/api/v1"

if "PANDACACHE_URL" in os.environ:
    baseURLCSRVSSL = os.environ["PANDACACHE_URL"]
    parsed = urlparse(baseURLCSRVSSL)
    cache_base_path_ssl = "{0}://{1}/api/v1".format(parsed.scheme, parsed.netloc)
else:
    baseURLCSRVSSL = "https://pandacache.cern.ch/server/panda"
    cache_base_path_ssl = "https://pandacache.cern.ch/api/v1"

# exit code
EC_Failed = 255

# limit on maxCpuCount
maxCpuCountLimit = 1000000000

# resolve panda cache server's name
if "PANDA_BEHIND_REAL_LB" not in os.environ:
    netloc = urlparse(baseURLCSRVSSL)
    tmp_host = socket.getfqdn(random.choice(socket.getaddrinfo(netloc.hostname, netloc.port))[-1][0])
    if netloc.port:
        baseURLCSRVSSL = "%s://%s:%s%s" % (netloc.scheme, tmp_host, netloc.port, netloc.path)
    else:
        baseURLCSRVSSL = "%s://%s%s" % (netloc.scheme, tmp_host, netloc.path)

    parsed = urlparse(baseURLCSRVSSL)
    cache_base_path_ssl = "{0}://{1}/api/v1".format(parsed.scheme, parsed.netloc)


# Decoder: check marker and convert back
def decode_special_cases(obj):
    if "__datetime__" in obj:
        return datetime.fromisoformat(obj["__datetime__"])
    return obj


def curl_request_decorator(endpoint, method="post", via_file=False, json_out=False, output_mode='basic'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Extract arguments
            try:
                sig = inspect.signature(func)
                default_verbose = sig.parameters['verbose'].default
            except Exception:
                default_verbose = False

            verbose = kwargs.get("verbose", default_verbose)
            data = func(*args, **kwargs)

            # Instantiate curl
            curl = _Curl()
            curl.sslCert = _x509()
            curl.sslKey = _x509()
            curl.verbose = verbose

            # Execute request
            url = "{0}/{1}".format(server_base_path_ssl, endpoint)
            if method == "post":
                status, output = curl.post(url, data, via_file=via_file, json_out=json_out)
            elif method == "get":
                status, output = curl.get(url, data, via_file=via_file, json_out=json_out, repeating_keys=True)
            else:
                raise ValueError("Unsupported HTTP method")

            # Handle response
            if isinstance(output, str) or not isinstance(output, dict):
                dump_log(func.__name__, None, output)
                return EC_Failed, None

            # Let the caller handle full output if requested
            if output_mode == 'full':
                return status, output

            success = output.get("success")
            if not success:
                # dump_log(func.__name__, None, output.get("message"))
                if output_mode == 'extended':
                    if verbose:
                        output_status = output.get("data")
                    else:
                        output_status = False
                    return status, (output_status, output.get("message"))

                return EC_Failed, None

            if output_mode == 'extended':
                if verbose:
                    output_status = output.get("data")
                else:
                    output_status = True
                return status, (output_status, output.get("data"))

            # output_mode == 'basic'
            return status, output.get("data")
        return wrapper
    return decorator


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ["X509_USER_PROXY"]
    except Exception:
        pass
    # see the default place
    x509 = "/tmp/x509up_u%s" % os.getuid()
    if os.access(x509, os.R_OK):
        return x509
    # no valid proxy certificate
    if "PANDA_AUTH" in os.environ and os.environ["PANDA_AUTH"] == "oidc":
        pass
    else:
        print("No valid grid proxy certificate found")
    return ""


# look for a CA certificate directory
def _x509_CApath():
    if "X509_CERT_DIR" not in os.environ or os.environ["X509_CERT_DIR"] == "":
        com = "{0} echo $X509_CERT_DIR".format(_getGridSrc())
        output = commands_get_output(com)
        output = output.split("\n")[-1]
        if output == "":
            output = "/etc/grid-security/certificates"
        os.environ["X509_CERT_DIR"] = output
    return os.environ["X509_CERT_DIR"]


# keep list of tmp files for cleanup
globalTmpDir = ""


# use OIDC
def use_oidc():
    return "PANDA_AUTH" in os.environ and os.environ["PANDA_AUTH"] == "oidc"


# use X509 without grid middleware
def use_x509_no_grid():
    return "PANDA_AUTH" in os.environ and os.environ["PANDA_AUTH"] == "x509_no_grid"


# string decode for python 2 and 3
def str_decode(data):
    if hasattr(data, "decode"):
        try:
            return data.decode()
        except Exception:
            return data.decode("utf-8")
    return data


# check if https
def is_https(url):
    return url.startswith("https://")


# hide sensitive info
def hide_sensitive_info(com):
    com = re.sub("Bearer [^\"']+", '***"', str(com))
    return com


# get token string
def get_token_string(tmp_log, verbose):
    if "PANDA_AUTH_ID_TOKEN" in os.environ:
        if verbose:
            tmp_log.debug("use $PANDA_AUTH_ID_TOKEN")
        return os.environ["PANDA_AUTH_ID_TOKEN"]
    if "OIDC_AUTH_TOKEN_FILE" in os.environ:
        if verbose:
            tmp_log.debug("use $OIDC_AUTH_TOKEN_FILE")
        with open(os.environ["OIDC_AUTH_TOKEN_FILE"]) as f:
            return f.read()
    if "OIDC_AUTH_ID_TOKEN" in os.environ:
        if verbose:
            tmp_log.debug("use $OIDC_AUTH_ID_TOKEN")
        return os.environ["OIDC_AUTH_ID_TOKEN"]
    return None


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl --user-agent "dqcurl" '
        # verification of the host certificate
        if "PANDA_VERIFY_HOST" in os.environ and os.environ["PANDA_VERIFY_HOST"] == "off":
            self.verifyHost = False
        else:
            self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ""
        self.sslKey = ""
        # auth mode
        self.idToken = None
        self.authVO = None
        if use_oidc():
            self.authMode = "oidc"
            if "PANDA_AUTH_VO" in os.environ:
                self.authVO = os.environ["PANDA_AUTH_VO"]
            elif "OIDC_AUTH_VO" in os.environ:
                self.authVO = os.environ["OIDC_AUTH_VO"]
        else:
            self.authMode = "voms"
        # verbose
        self.verbose = False

    # run auth flow
    def get_oidc(self, tmp_log):
        parsed = urlparse(baseURLSSL)
        if parsed.port:
            auth_url = "{0}://{1}:{2}/auth/{3}_auth_config.json".format(parsed.scheme, parsed.hostname, parsed.port, self.authVO)
        else:
            auth_url = "{0}://{1}/auth/{3}_auth_config.json".format(parsed.scheme, parsed.hostname, parsed.port, self.authVO)
        oidc = openidc_utils.OpenIdConnect_Utils(auth_url, log_stream=tmp_log, verbose=self.verbose)
        return oidc

    # get ID token
    def get_id_token(self, force_new=False):
        tmp_log = PLogger.getPandaLogger()
        token_str = get_token_string(tmp_log, self.verbose)
        if token_str:
            self.idToken = token_str
            return True
        oidc = self.get_oidc(tmp_log)
        if force_new:
            oidc.cleanup()
        s, o = oidc.run_device_authorization_flow()
        if not s:
            tmp_log.error(o)
            sys.exit(EC_Failed)
        self.idToken = o
        return True

    # get token
    def get_token_info(self):
        tmp_log = PLogger.getPandaLogger()
        token_str = get_token_string(tmp_log, self.verbose)
        if token_str:
            return openidc_utils.decode_id_token(token_str)
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
        if "PANDA_BEHIND_REAL_LB" in os.environ:
            return url
        # parse URL
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port
        if port is None:
            if parsed.scheme == "http":
                port = 80
            else:
                port = 443
        host_names = [socket.getfqdn(vv) for vv in set([v[-1][0] for v in socket.getaddrinfo(host, port, socket.AF_INET)])]
        return url.replace(host, random.choice(host_names))

    # GET method
    def get(self, url, data, rucio_account=False, via_file=False, n_try=1, json_out=False, repeating_keys=False):
        use_https = is_https(url)
        # make command
        com = "%s --silent --get" % self.path
        if not self.verifyHost or not use_https:
            com += " --insecure"
        else:
            tmp_x509_ca_path = _x509_CApath()
            if tmp_x509_ca_path != "":
                com += " --capath %s" % tmp_x509_ca_path

        if self.compress:
            com += " --compressed"

        if self.authMode == "oidc":
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += " --cert %s" % self.sslCert
            com += " --cacert %s" % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += " --key %s" % self.sslKey

        if json_out:
            com += ' -H "Accept: application/json"'

        # max time of 10 min
        com += " -m 600"

        # add rucio account info
        if rucio_account:
            if "RUCIO_ACCOUNT" in os.environ:
                data["account"] = os.environ["RUCIO_ACCOUNT"]
            if "RUCIO_APPID" in os.environ:
                data["appid"] = os.environ["RUCIO_APPID"]
            data["client_version"] = "2.4.1"

        # data
        data_string = ""
        for key in data.keys():
            value = data[key]
            if repeating_keys and isinstance(value, list):
                for element in value:
                    data_string += 'data="%s"\n' % urlencode({key: element})
            else:
                data_string += 'data="%s"\n' % urlencode({key: value})

        # write data to temporary config file
        if globalTmpDir != "":
            tmp_file_descriptor, tmp_name = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmp_file_descriptor, tmp_name = tempfile.mkstemp()
        os.write(tmp_file_descriptor, data_string.encode())
        os.close(tmp_file_descriptor)
        tmp_name_out = "{0}.out".format(tmp_name)
        com += " --config %s" % tmp_name
        if via_file:
            com += " -o {0}".format(tmp_name_out)
        com += " %s" % self.randomize_ip(url)
        # execute
        if self.verbose:
            print(hide_sensitive_info(com))
            print(data_string[:-1])
        for i_try in range(n_try):
            s, o = commands_get_status_output(com)
            if s == 0 or i_try + 1 == n_try:
                break
            time.sleep(1)
        if o != "\x00":
            try:
                tmp_out = unquote_plus(o)
                o = eval(tmp_out)
            except Exception:
                pass

        if via_file:
            with open(tmp_name_out, "rb") as f:
                ret = (s, f.read())
            os.remove(tmp_name_out)
        else:
            ret = (s, o)
        # remove temporary file
        os.remove(tmp_name)

        # when specified, convert to json
        if json_out:
            try:
                ret = (ret[0], json.loads(ret[1], object_hook=decode_special_cases))
            except Exception:
                ret = (ret[0], ret[1])

        ret = self.convert_return(ret)

        if self.verbose:
            print(ret)
        return ret

    # POST method
    def post(self, url, data, rucio_account=False, is_json=False, via_file=False, compress_body=False, n_try=1, json_out=False):
        use_https = is_https(url)
        # make command
        com = "%s --silent" % self.path
        if not self.verifyHost or not use_https:
            com += " --insecure"
        else:
            tmp_x509_ca_path = _x509_CApath()
            if tmp_x509_ca_path != "":
                com += " --capath %s" % tmp_x509_ca_path
        if self.compress:
            com += " --compressed"
        if self.authMode == "oidc":
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += " --cert %s" % self.sslCert
            com += " --cacert %s" % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += " --key %s" % self.sslKey

        if compress_body or json_out:
            com += ' -H "Content-Type: application/json"'

        if is_json or json_out:
            com += ' -H "Accept: application/json"'

        # max time of 10 min
        com += " -m 600"
        # add rucio account info
        if rucio_account:
            if "RUCIO_ACCOUNT" in os.environ:
                data["account"] = os.environ["RUCIO_ACCOUNT"]
            if "RUCIO_APPID" in os.environ:
                data["appid"] = os.environ["RUCIO_APPID"]
            data["client_version"] = "2.4.1"
        # write data to temporary config file
        if globalTmpDir != "":
            tmp_file_descriptor, tmp_name = tempfile.mkstemp(dir=globalTmpDir)
        else:
            tmp_file_descriptor, tmp_name = tempfile.mkstemp()
        # data
        data_string = ""
        if not compress_body:
            if not json_out:
                for key in data.keys():
                    data_string += 'data="%s"\n' % urlencode({key: data[key]})
                os.write(tmp_file_descriptor, data_string.encode("utf-8"))
            else:
                # json data
                data_string = json.dumps(data)
                os.write(tmp_file_descriptor, data_string.encode("utf-8"))
        else:
            f = os.fdopen(tmp_file_descriptor, "wb")
            with gzip.GzipFile(fileobj=f, mode="wb") as f_gzip:
                f_gzip.write(json.dumps(data).encode())
            f.close()
        try:
            os.close(tmp_file_descriptor)
        except Exception:
            pass
        tmp_name_out = "{0}.out".format(tmp_name)
        if not compress_body:
            if not json_out:
                com += " --config %s" % tmp_name
            else:
                com += " --data @{}".format(tmp_name)
        else:
            com += " --data-binary @{}".format(tmp_name)
        if via_file:
            com += " -o {0}".format(tmp_name_out)

        # The new API requires POST method for json
        if json_out:
            com += " -X POST"

        com += " %s" % self.randomize_ip(url)
        # execute
        if self.verbose:
            print(hide_sensitive_info(com))
            for key in data:
                print("{}={}".format(key, data[key]))
        for i_try in range(n_try):
            s, o = commands_get_status_output(com)
            if s == 0 or i_try + 1 == n_try:
                break
            time.sleep(1)
        if o != "\x00":
            try:
                if is_json or json_out:
                    o = json.loads(o, object_hook=decode_special_cases)
                else:
                    tmp_out = unquote_plus(o)
                    o = eval(tmp_out)
            except Exception:
                pass
        if via_file:
            with open(tmp_name_out, "rb") as f:
                if not json_out:
                    ret = (s, f.read())
                else:
                    ret = (s, json.loads(f.read(), object_hook=decode_special_cases))
            os.remove(tmp_name_out)
        else:
            ret = (s, o)

        # remove temporary file
        os.remove(tmp_name)
        ret = self.convert_return(ret)
        if self.verbose:
            print(ret)
        return ret

    # PUT method
    def put(self, url, data, n_try=1, json_out=False):
        use_https = is_https(url)
        # make command
        com = "%s --silent" % self.path
        if not self.verifyHost or not use_https:
            com += " --insecure"
        else:
            tmp_x509_ca_path = _x509_CApath()
            if tmp_x509_ca_path != "":
                com += " --capath %s" % tmp_x509_ca_path
        if self.compress:
            com += " --compressed"
        if self.authMode == "oidc":
            self.get_id_token()
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += " --cert %s" % self.sslCert
            com += " --cacert %s" % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += " --key %s" % self.sslKey

        if json_out:
            # we accept json response, but the body is form data
            com += ' -H "Accept: application/json"'

        # emulate PUT
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key, data[key])
        com += " %s" % self.randomize_ip(url)
        if self.verbose:
            print(hide_sensitive_info(com))
        # execute
        for i_try in range(n_try):
            ret = commands_get_status_output(com)
            if ret[0] == 0 or i_try + 1 == n_try:
                break
            time.sleep(1)

        if json_out:
            try:
                ret = (ret[0], json.loads(ret[1], object_hook=decode_special_cases))
            except Exception:
                ret = (ret[0], ret[1])

        ret = self.convert_return(ret)
        if self.verbose:
            print(ret)

        return ret

    # convert return
    def convert_return(self, ret):

        code = ret[0] % 255
        data = ret[1]

        # add datas to silent errors
        if code == 35:
            data = "SSL connect error. The SSL handshaking failed. Check grid certificate/proxy."
        elif code == 7:
            data = "Failed to connect to host."
        elif code == 55:
            data = "Failed sending network data."
        elif code == 56:
            data = "Failure in receiving network data."

        return code, data


class _NativeCurl(_Curl):
    def http_method(self, url, data, header, rdata=None, compress_body=False, is_json=False, json_out=False, repeating_keys=False, method=None, file_upload=False):
        try:
            use_https = is_https(url)
            url = self.randomize_ip(url)
            if header is None:
                header = {}
            if self.authMode == "oidc":
                self.get_id_token()
                header["Authorization"] = "Bearer {0}".format(self.idToken)
                header["Origin"] = self.authVO
            if not file_upload and (compress_body or json_out):
                header["Content-Type"] = "application/json"

            if is_json or json_out:
                header["Accept"] = "application/json"

            if rdata is None:
                if not compress_body and not json_out:
                    rdata = urlencode(data, doseq=repeating_keys).encode()
                elif compress_body:
                    rdata_out = BytesIO()
                    with gzip.GzipFile(fileobj=rdata_out, mode="w") as f_gzip:
                        f_gzip.write(json.dumps(data).encode())
                    rdata = rdata_out.getvalue()
                else:
                    rdata = json.dumps(data).encode("utf-8")

            req = Request(url, rdata, headers=header, method=method)
            context = ssl._create_unverified_context()
            if use_https and self.authMode != "oidc":
                if not self.sslCert:
                    self.sslCert = _x509()
                if not self.sslKey:
                    self.sslKey = _x509()
                context.load_cert_chain(certfile=self.sslCert, keyfile=self.sslKey)
            if self.verbose:
                print("url = {}".format(url))
                print("header = {}".format(hide_sensitive_info(header)))
                print("data = {}".format(str(data)))
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
                print(traceback.format_exc())
            error_message = str(e)
            if hasattr(e, "fp"):
                error_message += ". {0}".format(e.fp.read().decode())
            return 1, error_message

    # GET method
    def get(self, url, data, rucio_account=False, via_file=False, output_name=None, n_try=1, json_out=False, repeating_keys=False):
        if data:
            url = "{}?{}".format(url, urlencode(data, doseq=repeating_keys))

        method = None
        if json_out:
            method = "GET"

        for i_try in range(n_try):
            code, text = self.http_method(url, {}, {}, is_json=json_out, json_out=json_out, repeating_keys=False, method=method)
            if code in [0, 403, 404] or i_try + 1 == n_try:
                break
            time.sleep(1)

        if json_out and code == 0:
            text = json.loads(text, object_hook=decode_special_cases)

        if code == 0 and output_name:
            with open(output_name, "wb") as f:
                f.write(text)
            text = True
        return code, text

    # POST method
    def post(self, url, data, rucio_account=False, is_json=False, via_file=False, compress_body=False, n_try=1, json_out=False):

        method = None
        if json_out:
            method = "POST"

        for i_try in range(n_try):
            code, text = self.http_method(url, data, {}, compress_body=compress_body, is_json=is_json, json_out=json_out, method=method)
            if code in [0, 403, 404] or i_try + 1 == n_try:
                break
            time.sleep(1)

        if code == 0 and (is_json or json_out):
            text = json.loads(text, object_hook=decode_special_cases)

        return code, text

    # PUT method
    def put(self, url, data, n_try=1, json_out=False):
        boundary = "".join(random.choice(string.ascii_letters) for ii in range(30 + 1))
        body = b""
        for k in data:
            lines = ["--" + boundary, 'Content-Disposition: form-data; name="%s"; filename="%s"' % (k, data[k]), "Content-Type: application/octet-stream", ""]
            body += "\r\n".join(lines).encode()
            body += b"\r\n"
            body += open(data[k], "rb").read()
            body += b"\r\n"
        lines = ["--%s--" % boundary, ""]
        body += "\r\n".join(lines).encode()
        headers = {"content-type": "multipart/form-data; boundary=" + boundary, "content-length": str(len(body))}

        for i_try in range(n_try):
            code, text = self.http_method(url, None, headers, body, json_out=json_out, file_upload=True)
            if code in [0, 403, 404] or i_try + 1 == n_try:
                break
            time.sleep(1)

        if code == 0 and json_out:
            text = json.loads(text, object_hook=decode_special_cases)

        return code, text


if "PANDA_USE_NATIVE_HTTPLIB" in os.environ:
    _Curl = _NativeCurl


# dump log
def dump_log(func_name, exception_obj, output):
    print(traceback.format_exc())
    print(output)
    err_str = "{} failed : {}".format(func_name, str(exception_obj))
    tmp_log = PLogger.getPandaLogger()
    tmp_log.error(err_str)
    return err_str


"""
public methods

"""

@curl_request_decorator(endpoint="job/submit", method="post", json_out=True)
def submitJobs_internal(jobs, verbose=False, no_pickle=False):
    return {"jobs": jobs}

def submitJobs(jobs, verbose=False, no_pickle=False):
    """Submit jobs

    args:
        jobs: a list of job specs
        verbose: True to see verbose messages
        no_pickle: True to use json instead of pickle
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        a list of PandaIDs, or None if failed
    """
    # set hostname
    hostname = commands_get_output("hostname")
    for job in jobs:
        job.creationHost = hostname

    jobs_serialized = MiscUtils.dump_jobs_json(jobs)
    return submitJobs_internal(jobs_serialized, verbose, no_pickle)


@curl_request_decorator(endpoint="job/get_description", method="get", json_out=True)
def getJobStatus_internal(ids, verbose=False, no_pickle=False):
    return {"job_ids": ids}

def getJobStatus(ids, verbose=False, no_pickle=False):
    """Get status of jobs

    args:
        ids: a list of PanDA IDs
        verbose: True to see verbose messages
        no_pickle: obsolete parameter left for backwards compatibility
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        a list of job specs, or None if failed
    """
    status, jobs = getJobStatus_internal(ids, verbose=verbose, no_pickle=no_pickle)
    if status != 0:
        return status, jobs

    try:
        return status, MiscUtils.load_jobs(jobs)
    except Exception as e:
        dump_log("getJobStatus", e, jobs)
        return EC_Failed, None


@curl_request_decorator(endpoint="job/kill", method="post", json_out=True)
def killJobs(ids, verbose=False):
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
    return {"job_ids": ids}


@curl_request_decorator(endpoint="task/kill", method="post", json_out=True, output_mode='extended')
def killTask(jediTaskID, verbose=False):
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
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/finish", method="post", json_out=True, output_mode='extended')
def finishTask(jediTaskID, soft=False, verbose=False):
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
    data = {"task_id": jediTaskID}
    if soft:
        data["soft"] = True
    return data


@curl_request_decorator(endpoint="task/retry", method="post", json_out=True, output_mode='extended')
def retryTask(jediTaskID, verbose=False, properErrorCode=False, newParams=None):
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
    data = {"task_id": jediTaskID}
    if newParams:
        data["new_parameters"] = json.dumps(newParams)
    return data


def putFile(file, verbose=False, useCacheSrv=False, reuseSandbox=False, n_try=1):
    """Upload a file with the size limit on 10 MB
    args:
       file: filename to be uploaded
       verbose: True to see debug messages
       useCacheSrv: True to use a dedicated cache server separated from the PanDA server
       reuseSandbox: True to avoid uploading the same sandbox files
       n_try: number of tries
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       diagnostic message
    """
    # size check for noBuild
    size_limit = 10 * 1024 * 1024
    file_size = os.stat(file)[stat.ST_SIZE]
    if not os.path.basename(file).startswith("sources."):
        if file_size > size_limit:
            error_message = "Exceeded size limit (%sB >%sB). " % (file_size, size_limit)
            error_message += "Your working directory contains too large files which cannot be put on cache area. "
            error_message += "Please submit job without --noBuild/--libDS so that your files will be uploaded to SE"
            tmp_logger = PLogger.getPandaLogger()
            tmp_logger.error(error_message)
            return EC_Failed, "False"

    # Instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    curl.verbose = verbose

    # check duplication
    if reuseSandbox:
        # get CRC
        fo = open(file, "rb")
        file_content = fo.read()
        fo.close()
        footer = file_content[-8:]
        checksum, i_size = struct.unpack("II", footer)

        # Execute request
        endpoint = "file_server/validate_cache_file"
        data = {"file_size": i_size, "checksum": checksum}
        url = "{0}/{1}".format(server_base_path_ssl, endpoint)
        status, output = curl.post(url, data, via_file=True, json_out=True)
        if status != 0:
            return EC_Failed, "ERROR: Could not check sandbox duplication with %s" % output

        success = output.get("success", False)
        message = output.get("message", "")

        if message.startswith("FOUND:"):
            # found reusable sandbox
            host_name, reusable_file_name = output.split(":")[1:]
            # set cache server hostname
            setCacheServer(host_name)
            # return reusable filename
            return 0, "NewFileName:{0}".format(reusable_file_name)

    if not useCacheSrv:
        global cache_base_path_ssl
        cache_base_path_ssl = server_base_path_ssl

    url = "{0}/{1}".format(cache_base_path_ssl, "file_server/upload_cache_file")
    data = {"file": file}
    s, o = curl.put(url, data, n_try=n_try, json_out=True)

    if s != 0:
        return s, "ERROR: Could not upload file with %s" % str_decode(o)

    success = o.get("success", False)
    if success:
        message = "True"
    else:
        message = o.get("message", "False")

    return s, message


# get file
def getFile(filename, output_path=None, verbose=False, n_try=1):
    """Get a file
    args:
       filename: filename to be downloaded
       output_path: output path. Set to filename if unspecified
       verbose: True to see debug messages
       n_try: number of tries
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
    url = "%s://%s" % (netloc.scheme, netloc.hostname)
    if netloc.port:
        url += ":%s" % netloc.port
    url = url + "/cache/" + filename
    s, o = curl.get(url, {}, output_name=output_path, n_try=n_try)
    return s, o


# get grid source file
def _getGridSrc():
    if "PATHENA_GRID_SETUP_SH" in os.environ:
        gridSrc = os.environ["PATHENA_GRID_SETUP_SH"]
    else:
        gridSrc = "/dev/null"
    gridSrc = "source %s > /dev/null;" % gridSrc
    # some grid_env.sh doen't correct PATH/LD_LIBRARY_PATH
    gridSrc = "unset LD_LIBRARY_PATH; unset PYTHONPATH; unset MANPATH; export PATH=/usr/local/bin:/bin:/usr/bin; %s" % gridSrc
    return gridSrc


# get DN
def getDN(origString):
    shortName = ""
    distinguishedName = ""
    for line in origString.split("/"):
        if line.startswith("CN="):
            distinguishedName = re.sub("^CN=", "", line)
            distinguishedName = re.sub("\d+$", "", distinguishedName)
            distinguishedName = re.sub("\.", "", distinguishedName)
            distinguishedName = distinguishedName.strip()
            if re.search(" ", distinguishedName) is not None:
                # look for full name
                distinguishedName = distinguishedName.replace(" ", "")
                break
            elif shortName == "":
                # keep short name
                shortName = distinguishedName
            distinguishedName = ""
    # use short name
    if distinguishedName == "":
        distinguishedName = shortName
    # return
    return distinguishedName


# use dev server
def useDevServer():
    global baseURL
    baseURL = "http://aipanda007.cern.ch:25080/server/panda"
    global baseURLSSL
    baseURLSSL = "https://aipanda007.cern.ch:25443/server/panda"
    global baseURLCSRVSSL
    baseURLCSRVSSL = "https://aipanda007.cern.ch:25443/server/panda"


# use INTR server
def useIntrServer():
    global baseURL
    baseURL = "http://aipanda123.cern.ch:25080/server/panda"
    global baseURLSSL
    baseURLSSL = "https://aipanda123.cern.ch:25443/server/panda"
    global baseURLCSRVSSL
    baseURLCSRVSSL = baseURLSSL


# set cache server
def setCacheServer(host_name):
    global baseURLCSRVSSL
    global cache_base_path_ssl

    netloc = urlparse(baseURLCSRVSSL)
    if netloc.port:
        baseURLCSRVSSL = "%s://%s:%s%s" % (netloc.scheme, host_name, netloc.port, netloc.path)
    else:
        baseURLCSRVSSL = "%s://%s%s" % (netloc.scheme, host_name, netloc.path)

    parsed = urlparse(baseURLCSRVSSL)
    cache_base_path_ssl = "{0}://{1}/api/v1".format(parsed.scheme, parsed.netloc)


# register proxy key
# THIS API DOES NOT EXIST IN PANDA SERVER?!?
def registerProxyKey(credname, origin, myproxy, verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    curl.verbose = verbose
    curl.verifyHost = True
    # execute
    url = baseURLSSL + "/registerProxyKey"
    data = {"credname": credname, "origin": origin, "myproxy": myproxy}
    return curl.post(url, data)


# get proxy key
# THIS API DOES NOT EXIST IN PANDA SERVER?!?
def getProxyKey(verbose=False):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + "/getProxyKey"
    status, output = curl.post(url, {})
    if status != 0:
        print(output)
        return status, None
    try:
        return status, pickle_loads(output)
    except Exception as e:
        dump_log("getProxyKey", e, output)
        return EC_Failed, None


@curl_request_decorator(endpoint="task/get_tasks_modified_since", method="get", json_out=True)
def getJobIDsJediTasksInTimeRange(timeRange, dn=None, minTaskID=None, verbose=False, task_type="user"):
    return {"since": timeRange, "dn": dn, "full": True, "min_task_id": minTaskID, "prod_source_label": task_type}


@curl_request_decorator(endpoint="task/get_details", method="get", json_out=True)
def getJediTaskDetails_internal(taskDict, fullFlag, withTaskInfo, verbose=False):
    return {"task_id": taskDict["jediTaskID"], "include_parameters": fullFlag, "include_status": withTaskInfo}


def getJediTaskDetails(taskDict, fullFlag, withTaskInfo, verbose=False):
    status, tmp_dictionary = getJediTaskDetails_internal(taskDict, fullFlag, withTaskInfo, verbose)
    if status == 0:
        taskDict.update(tmp_dictionary)
        return status, taskDict

    return status, tmp_dictionary


@curl_request_decorator(endpoint="job/get_description_incl_archive", method="get", json_out=True)
def getFullJobStatus_internal(ids, verbose=False):
    return {"job_ids": ids}

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
    status, jobs = getFullJobStatus_internal(ids, verbose=verbose)
    if status != 0:
        return status, jobs

    try:
        return status, MiscUtils.load_jobs(jobs)
    except Exception as e:
        dump_log("getFullJobStatus", e, jobs)
        return EC_Failed, None


@curl_request_decorator(endpoint="job/set_debug_mode", method="post", json_out=True)
def setDebugMode(pandaID, modeOn, verbose):
    return {"job_id": pandaID, "mode": modeOn}


# set tmp dir
def setGlobalTmpDir(tmpDir):
    global globalTmpDir
    globalTmpDir = tmpDir


# request EventPicking
def requestEventPicking(
    eventPickEvtList,
    eventPickDataType,
    eventPickStreamName,
    eventPickDS,
    eventPickAmiTag,
    fileList,
    fileListName,
    outDS,
    lockedBy,
    params,
    eventPickNumSites,
    eventPickWithGUID,
    ei_api,
    verbose=False,
):
    # get logger
    tmpLog = PLogger.getPandaLogger()
    # list of input files
    strInput = ""
    for tmpInput in fileList:
        if tmpInput != "":
            strInput += "%s," % tmpInput
    if fileListName != "":
        for tmpLine in open(fileListName):
            tmpInput = re.sub("\n", "", tmpLine)
            if tmpInput != "":
                strInput += "%s," % tmpInput
    strInput = strInput[:-1]
    # make dataset name
    userDatasetName = "%s.%s.%s/" % tuple(outDS.split(".")[:2] + [MiscUtils.wrappedUuidGen()])
    # open run/event number list
    evpFile = open(eventPickEvtList)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + "/putEventPickingRequest"
    data = {
        "runEventList": evpFile.read(),
        "eventPickDataType": eventPickDataType,
        "eventPickStreamName": eventPickStreamName,
        "eventPickDS": eventPickDS,
        "eventPickAmiTag": eventPickAmiTag,
        "userDatasetName": userDatasetName,
        "lockedBy": lockedBy,
        "giveGUID": eventPickWithGUID,
        "params": params,
        "inputFileList": strInput,
    }
    if eventPickNumSites > 1:
        data["eventPickNumSites"] = eventPickNumSites
    if ei_api:
        data["ei_api"] = ei_api
    evpFile.close()
    status, output = curl.post(url, data)
    # failed
    if status != 0 or output is not True:
        print(output)
        errStr = "failed to request EventPicking"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)
    # return user dataset name
    return True, userDatasetName


@curl_request_decorator(endpoint="task/submit", method="post", json_out=True, output_mode='full')
def insertTaskParams_internal(taskParams, verbose=False, properErrorCode=False, parent_tid=None):
    return {"task_parameters": taskParams}

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

    status, output = insertTaskParams_internal(taskParams, verbose=verbose, properErrorCode=properErrorCode, parent_tid=parent_tid)

    if status != 0:
        return status, output

    try:
        if not output['success']:
            # [error code, message]
            return status, (output['data'], output['message'])

        # [0, message including task ID]
        return status, (0, output['message'])

    except Exception:
        return EC_Failed, "Impossible to parse server response. Output: {}".format(output)


@curl_request_decorator(endpoint="task/get_job_ids", method="get", json_out=True)
def getPandaIDsWithTaskID(jediTaskID, verbose=False):
    """Get PanDA IDs with TaskID

    args:
        jediTaskID: jediTaskID of the task to get lit of PanDA IDs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        the list of PanDA IDs, or error message if failed
    """
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/reactivate", method="post", json_out=True, output_mode='extended')
def reactivateTask(jediTaskID, verbose=True):
    """Reactivate task

    args:
        jediTaskID: jediTaskID of the task to be reactivated
        verbose: True to see verbose messages
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message, or error message if failed
              0: unknown task
              1: succeeded
              None: database error
    """
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/resume", method="post", json_out=True, output_mode='extended')
def resumeTask(jediTaskID, verbose=True):
    """Resume task

    args:
        jediTaskID: jediTaskID of the task to be resumed
        verbose: True to see verbose messages
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message, or error message if failed
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
              100: non SSL connection
              101: irrelevant taskID
              None: database error
    """
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/pause", method="post", json_out=True, output_mode='extended')
def pauseTask(jediTaskID, verbose=True):
    """Pause task

    args:
        jediTaskID: jediTaskID of the task to pause
        verbose: True to see verbose messages
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message, or error message if failed
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
              100: non SSL connection
              101: irrelevant taskID
              None: database error
    """
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/get_status", method="get", json_out=True)
def getTaskStatus(jediTaskID, verbose=False):
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

    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="task/get_task_parameters", method="get", json_out=True)
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
    return {"task_id": jediTaskID}


@curl_request_decorator(endpoint="job/get_metadata_for_analysis_jobs", method="get", json_out=True)
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
    return {"task_id": task_id}

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
    curl.sslKey = _x509()
    curl.verbose = verbose
    # execute
    url = server_base_path_ssl + "/system/is_alive"
    try:
        status, response = curl.get(url, {}, json_out=True)

        # Communication issue with PanDA server
        if status != 0:
            tmp_message = "Communication issue. " + response
            tmp_log.error(tmp_message)
            return EC_Failed, tmp_message

        response = str_decode(response)
        success = response.get("success", False)
        message = response.get("message", "")
        if not success:
            tmp_message = "Problem with is_alive. " + message
            tmp_log.error(tmp_message)
            return EC_Failed, tmp_message

        tmp_log.info("Done with success={0} and message='{1}'".format(success, message))
        return 0, message

    except Exception as e:
        tmp_message = "Exception. {}".format(str(e))
        tmp_log.error(tmp_message)
        return EC_Failed, tmp_message


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
    curl.sslKey = _x509()
    curl.verbose = verbose

    # execute
    url = baseURLSSL + "/getAttr"
    try:
        status, output = curl.post(url, {})
        output = str_decode(output)
        if status != 0:
            msg = "Not good. " + output
            tmp_log.error(msg)
            return EC_Failed, msg
        else:
            d = dict()
            for line in output.split("\n"):
                if ":" not in line:
                    continue
                print(line)
                if not line.startswith("GRST_CRED"):
                    continue
                items = line.split(":")
                d[items[0].strip()] = items[1].strip()
            return 0, d
    except Exception as e:
        msg = "Too bad. {}".format(str(e))
        tmp_log.error(msg)
        print(traceback.format_exc())
        return EC_Failed, msg


@curl_request_decorator(endpoint="system/get_attributes", method="get", json_out=True)
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
    return {}

# get username from token
def get_user_name_from_token():
    """Extract username and groups from ID token

    returns:
       a tuple of username, groups, and preferred username
    """
    curl = _Curl()
    token_info = curl.get_token_info()
    try:
        name = " ".join([t[:1].upper() + t[1:].lower() for t in str(token_info["name"]).split()])
        groups = token_info.get("groups", None)
        preferred_username = token_info.get("preferred_username", None)
        return name, groups, preferred_username
    except Exception:
        return None, None, None


# get new token
def get_new_token():
    """Get new ID token

    returns: a string of ID token. None if failed

    """
    curl = _Curl()
    if curl.get_id_token(force_new=True):
        return curl.idToken
    return None


# call idds command
def call_idds_command(
    command_name, args=None, kwargs=None, dumper=None, verbose=False, compress=False, manager=False, loader=None, json_outputs=False, n_try=1
):
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
       json_outputs: True to use json outputs
       n_try: number of tries
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
    curl.sslKey = _x509()
    curl.verbose = verbose

    # execute
    url = baseURLSSL + "/relay_idds_command"
    try:
        data = dict()
        data["command_name"] = command_name
        if args:
            if dumper is None:
                data["args"] = json.dumps(args)
            else:
                data["args"] = dumper(args)
        if kwargs:
            if dumper is None:
                data["kwargs"] = json.dumps(kwargs)
            else:
                data["kwargs"] = dumper(kwargs)
        if manager:
            data["manager"] = True
        if json_outputs:
            data["json_outputs"] = True
        status, output = curl.post(url, data, compress_body=compress, n_try=n_try)
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
        print(traceback.format_exc())
        return EC_Failed, msg


# call idds user workflow command
def call_idds_user_workflow_command(command_name, kwargs=None, verbose=False, json_outputs=False, n_try=1):
    """Call an iDDS workflow user command
    args:
       command_name: command name
       kwargs: a dictionary of keyword arguments
       verbose: True to see verbose message
       json_outputs: True to use json outputs
       n_try: number of tries
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a tuple of (True, response from iDDS), or (False, diagnostic message) if failed
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    curl.verbose = verbose

    # execute
    url = baseURLSSL + "/execute_idds_workflow_command"
    try:
        data = dict()
        data["command_name"] = command_name
        if kwargs:
            data["kwargs"] = json.dumps(kwargs)
        if json_outputs:
            data["json_outputs"] = True
        status, output = curl.post(url, data, n_try=n_try)
        if status != 0:
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = "Failed with {}".format(str(e))
        print(traceback.format_exc())
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
    curl.sslKey = _x509()
    curl.verbose = verbose

    # execute
    output = None
    url = baseURLSSL + "/put_file_recovery_request"
    try:
        data = {"jediTaskID": task_id}
        if dry_run:
            data["dryRun"] = True
        status, output = curl.post(url, data)
        if status != 0:
            output = str_decode(output)
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = "{}.".format(str(e))
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
    curl.sslKey = _x509()
    curl.verbose = verbose

    # execute
    output = None
    if relay_host:
        url = "https://{}:25443/server/panda".format(relay_host)
    else:
        url = baseURLSSL
    url += "/put_workflow_request"
    try:
        data = {"data": json.dumps(params)}
        if check:
            data["check"] = True
        else:
            data["sync"] = True
        status, output = curl.post(url, data, compress_body=True, is_json=True)
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, output
    except Exception as e:
        msg = "{}.".format(str(e))
        if output:
            msg += ' raw output="{}"'.format(str(output))
        tmp_log.error(msg)
        return EC_Failed, msg


@curl_request_decorator(endpoint="creds/set_user_secrets", method="post", json_out=True, output_mode="extended")
def set_user_secret(key, value, verbose=False):
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
    return {"key": key, "value": value}


@curl_request_decorator(endpoint="creds/get_user_secrets", method="get", json_out=True, output_mode="extended")
def get_user_secrets_internal(verbose=False):
    return {}


def get_user_secrets(verbose=False):
    """Get user secrets
    args:
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a tuple of (True/False and a dict of secrets). True if the request was accepted
    """
    status, output = get_user_secrets_internal(verbose=verbose)

    if status != 0:
        return status, output

    success, data = output

    if success:
        return status, (success, json.loads(data))

    # data should just be an error message
    return status, (success, data)

@curl_request_decorator(endpoint="task/increase_attempts", method="post", json_out=True, output_mode="extended")
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
    return {"task_id": task_id, "increase": increase}


@curl_request_decorator(endpoint="task/reload_input", method="post", json_out=True, output_mode='extended')
def reload_input(task_id, verbose=True):
    """Retry task
    args:
        task_id: jediTaskID of the task to reload and retry
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """
    return {"task_id": task_id}


@curl_request_decorator(endpoint="task/get_datasets_and_files", method="get", json_out=True)
def get_files_in_datasets_internal(task_id, dataset_types, verbose=False):
    return {"task_id": task_id, "dataset_types": dataset_types}

def get_files_in_datasets(task_id, dataset_types="input,pseudo_input", verbose=False):
    """Get files in datasets
    args:
       task_id: jediTaskID of the datasets
       dataset_types: a comma-separated string to specify dataset types
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a list of dataset dictionaries including file info, or error message if failed
    """
    dataset_types_list = dataset_types.split(",")
    return get_files_in_datasets_internal(task_id, dataset_types_list)


@curl_request_decorator(endpoint="event/get_event_range_statuses", method="get", json_out=True)
def get_events_status(ids, verbose=False):
    """Get status of events
    args:
       ids: a list of {'task_id': ..., 'panda_id': ...}
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a dictionary of {panda_id: [{event_range_id: status}, ...], ...}
    """
    return {"job_task_ids": ids}


@curl_request_decorator(endpoint="event/update_event_ranges", method="post", json_out=True)
def update_events(events, verbose=False):
    """Update events
    args:
       events: a list of {'eventRangeID': ..., 'eventStatus': ...,
                          'errorCode': <optional>, 'errorDiag': <optional, < 500chars>}
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a dictionary of {'Returns': a list of returns when updating events, 'Command': commands to jobs, 'StatusCode': 0 for OK})
    """
    return {"event_ranges": events}