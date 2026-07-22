"""
client methods

Low-level client library for the PanDA server's REST API: job submission and
control, task control, file cache upload/download, and relays to iDDS/workflow
services. This is the layer pbook/pathena/prun sit on top of.

Most public functions are thin wrappers built with the @http_request_decorator,
which POSTs/GETs to f"{server_base_path_ssl}/{endpoint}" and unwraps the JSON
response envelope ({"success": ..., "message": ..., "data": ...}) according to
output_mode. Functions with unusual request/response shapes (file upload/
download, event picking, health checks, iDDS relays, ...) call _HttpClient
directly instead.

_HttpClient (httpx-based) is the shared HTTP layer underneath both paths. It
supports two auth modes, selected via $PANDA_AUTH:
  - "voms" (default): client cert/key from a grid proxy (see _x509_proxy_path),
    verified against the CA directory from _x509_ca_path.
  - "oidc": bearer token obtained via openidc_utils' device-authorization flow,
    or supplied directly through $PANDA_AUTH_ID_TOKEN/$OIDC_AUTH_TOKEN_FILE/
    $OIDC_AUTH_ID_TOKEN (see get_token_string).

Server locations come from $PANDA_URL/$PANDA_URL_SSL/$PANDACACHE_URL, falling
back to the ATLAS production servers at CERN when unset.
"""

import gzip
import inspect
import json
import os
import random
import re
import socket
import ssl
import stat
import struct
import sys
import time
import traceback
from datetime import datetime
from io import BytesIO
from urllib.parse import urlencode, urlparse

import httpx

from . import MiscUtils, PLogger, openidc_utils
from .MiscUtils import commands_get_output

# configuration
try:
    baseURL = os.environ["PANDA_URL"]
    parsed = urlparse(baseURL)
    server_base_path = f"{parsed.scheme}://{parsed.netloc}/api/v1"
except Exception:
    baseURL = "http://pandaserver.cern.ch:25080/server/panda"
    server_base_path = "http://pandaserver.cern.ch:25080/api/v1"

try:
    baseURLSSL = os.environ["PANDA_URL_SSL"]
    parsed = urlparse(baseURLSSL)
    server_base_path_ssl = f"{parsed.scheme}://{parsed.netloc}/api/v1"
except Exception:
    baseURLSSL = "https://pandaserver.cern.ch/server/panda"
    server_base_path_ssl = "https://pandaserver.cern.ch/api/v1"

if "PANDACACHE_URL" in os.environ:
    baseURLCSRVSSL = os.environ["PANDACACHE_URL"]
    parsed = urlparse(baseURLCSRVSSL)
    cache_base_path_ssl = f"{parsed.scheme}://{parsed.netloc}/api/v1"
else:
    baseURLCSRVSSL = "https://pandacache.cern.ch/server/panda"
    cache_base_path_ssl = "https://pandacache.cern.ch/api/v1"

# exit code
EC_Failed = 255

# limit on maxCpuCount
maxCpuCountLimit = 1000000000

# limits on file sizes
NO_BUILD_LIMIT = 10 * 1024 * 1024
SOURCES_LIMIT = 768 * 1024 * 1024

# resolve panda cache server's name
if "PANDA_BEHIND_REAL_LB" not in os.environ:
    netloc = urlparse(baseURLCSRVSSL)
    tmp_host = socket.getfqdn(random.choice(socket.getaddrinfo(netloc.hostname, netloc.port))[-1][0])
    if netloc.port:
        baseURLCSRVSSL = f"{netloc.scheme}://{tmp_host}:{netloc.port}{netloc.path}"
    else:
        baseURLCSRVSSL = f"{netloc.scheme}://{tmp_host}{netloc.path}"

    parsed = urlparse(baseURLCSRVSSL)
    cache_base_path_ssl = f"{parsed.scheme}://{parsed.netloc}/api/v1"


def decode_special_cases(obj):
    """json.loads object_hook that reconstructs values the server marked as a special case

    Currently handles datetime values serialized as {"__datetime__": "<isoformat>"}.

    args:
       obj: a dict decoded from a JSON object
    returns:
       obj, or the reconstructed value if obj carries a recognized marker
    """
    if "__datetime__" in obj:
        return datetime.fromisoformat(obj["__datetime__"])
    return obj


def http_request_decorator(endpoint, method="post", json_out=False, output_mode="basic"):
    """Turn a function that builds a request payload into a call against a PanDA server REST endpoint

    The decorated function should return the request payload dict. This decorator sends it
    to f"{server_base_path_ssl}/{endpoint}" via _HttpClient, then unwraps the JSON response
    envelope ({"success": ..., "message": ..., "data": ...}) according to output_mode:
       "basic": (status, data) if success else (EC_Failed, None)
       "extended": (status, (success, data)) on success, (status, (False, message)) on failure
       "full": (status, the raw parsed JSON dict), regardless of success

    args:
       endpoint: REST API path, appended to server_base_path_ssl
       method: "post" or "get"
       json_out: True to request/parse a JSON response body
       output_mode: "basic", "extended", or "full" (see above)
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            # Extract verbose flag from kwargs, args, or function signature
            verbose = None
            if "verbose" in kwargs:
                verbose = kwargs.get("verbose")
            else:
                sig = inspect.signature(func)
                for arg, param_name in zip(args, sig.parameters):
                    if param_name == "verbose":
                        verbose = arg
                        break
                if verbose is None and "verbose" in sig.parameters:
                    verbose = sig.parameters["verbose"].default

            if verbose is None:
                verbose = False
            data = func(*args, **kwargs)

            # Instantiate the HTTP client
            client = _HttpClient()
            client.ssl_certificate = _x509_proxy_path()
            client.ssl_key = _x509_proxy_path()
            client.verbose = verbose

            # Execute request
            url = f"{server_base_path_ssl}/{endpoint}"
            if method == "post":
                status, output = client.post(url, data, json_out=json_out)
            elif method == "get":
                status, output = client.get(url, data, json_out=json_out, repeating_keys=True)
            else:
                raise ValueError("Unsupported HTTP method")

            # Handle response
            if isinstance(output, str) or not isinstance(output, dict):
                dump_log(func.__name__, None, output)
                return EC_Failed, None

            # Let the caller handle full output if requested
            if output_mode == "full":
                return status, output

            success = output.get("success")
            if not success:
                # dump_log(func.__name__, None, output.get("message"))
                if output_mode == "extended":
                    output_status = False
                    return status, (output_status, output.get("message"))

                return EC_Failed, None

            if output_mode == "extended":
                output_status = True
                return status, (output_status, output.get("data"))

            # output_mode == 'basic'
            return status, output.get("data")

        return wrapper

    return decorator


def _x509_proxy_path():
    """Get the path to the user's grid proxy certificate

    Prefers $X509_USER_PROXY, falling back to the default proxy location. Prints a
    warning if neither exists and OIDC auth isn't in use (voms auth needs a proxy).

    returns:
       path to the proxy certificate, or "" if none was found
    """
    # see X509_USER_PROXY
    x509 = os.environ.get("X509_USER_PROXY")
    if x509:
        return x509

    # see the default place
    x509 = f"/tmp/x509up_u{os.getuid()}"
    if os.access(x509, os.R_OK):
        return x509

    # no valid proxy certificate
    if use_oidc():
        pass
    else:
        print("No valid grid proxy certificate found")

    return ""


def _x509_ca_path():
    """Get the CA certificate directory, caching the result in $X509_CERT_DIR

    Resolves via the grid environment setup script (see build_grid_setup_command), falling back
    to the standard grid-security path if that doesn't yield one.

    returns:
       path to the CA certificate directory
    """
    if not os.environ.get("X509_CERT_DIR"):
        com = f"{build_grid_setup_command()} echo $X509_CERT_DIR"
        output = commands_get_output(com).split("\n")[-1]
        os.environ["X509_CERT_DIR"] = output or "/etc/grid-security/certificates"
    return os.environ["X509_CERT_DIR"]


def use_oidc():
    """Whether $PANDA_AUTH selects OIDC bearer-token authentication"""
    return "PANDA_AUTH" in os.environ and os.environ["PANDA_AUTH"] == "oidc"


def use_x509_no_grid():
    """Whether $PANDA_AUTH selects a plain X.509 cert without the grid middleware"""
    return "PANDA_AUTH" in os.environ and os.environ["PANDA_AUTH"] == "x509_no_grid"


def is_https(url):
    """Whether url uses the https scheme"""
    return url.startswith("https://")


def hide_sensitive_info(com):
    """Redact a bearer token from a string, e.g. before printing verbose request info"""
    com = re.sub("Bearer [^\"']+", '***"', str(com))
    return com


def get_token_string(tmp_log, verbose):
    """Get a pre-supplied OIDC ID token, so callers can skip the interactive device-authorization flow

    Checks, in order: a token string in $PANDA_AUTH_ID_TOKEN, a file path in
    $OIDC_AUTH_TOKEN_FILE, or a token string in $OIDC_AUTH_ID_TOKEN.
    """
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


class _HttpClient:
    """HTTP client for the PanDA server's REST API, built on httpx

    Supports the two auth modes described in the module docstring (voms client-cert
    or OIDC bearer-token), retries, and a couple of PanDA-specific quirks (DNS-based
    load spreading via randomize_ip, gzip-compressed JSON bodies). Instances are
    cheap and stateless enough to create per call; see http_request_decorator and
    the various module-level functions that instantiate one directly.
    """

    def __init__(self):
        # verification of the host certificate
        if "PANDA_VERIFY_HOST" in os.environ and os.environ["PANDA_VERIFY_HOST"] == "off":
            self.verify_host = False
        else:
            self.verify_host = True

        # SSL cert/key
        self.ssl_certificate = ""
        self.ssl_key = ""

        # auth mode
        self.id_token = None
        self.auth_vo = None
        if use_oidc():
            self.auth_mode = "oidc"
            if "PANDA_AUTH_VO" in os.environ:
                self.auth_vo = os.environ["PANDA_AUTH_VO"]
            elif "OIDC_AUTH_VO" in os.environ:
                self.auth_vo = os.environ["OIDC_AUTH_VO"]
        else:
            self.auth_mode = "voms"

        # verbose
        self.verbose = False

    def get_oidc(self, tmp_log):
        """Build an OpenIdConnect_Utils helper pointed at this server's auth config for self.auth_vo

        args:
           tmp_log: logger passed through to OpenIdConnect_Utils
        returns:
           an openidc_utils.OpenIdConnect_Utils instance
        """
        parsed = urlparse(baseURLSSL)
        if parsed.port:
            auth_url = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}/auth/{self.auth_vo}_auth_config.json"
        else:
            auth_url = f"{parsed.scheme}://{parsed.hostname}/auth/{self.auth_vo}_auth_config.json"
        oidc = openidc_utils.OpenIdConnect_Utils(auth_url, log_stream=tmp_log, verbose=self.verbose)
        return oidc

    def get_id_token(self, force_new=False):
        """Populate self.id_token, from a pre-supplied token or by running the device-authorization flow

        args:
           force_new: True to discard any cached token and force a fresh device-authorization flow
        returns:
           True on success; exits the process if the device-authorization flow fails
        """
        tmp_log = PLogger.getPandaLogger()
        token_str = get_token_string(tmp_log, self.verbose)
        if token_str:
            self.id_token = token_str
            return True
        oidc = self.get_oidc(tmp_log)
        if force_new:
            oidc.cleanup()
        s, o = oidc.run_device_authorization_flow()
        if not s:
            tmp_log.error(o)
            sys.exit(EC_Failed)
        self.id_token = o
        return True

    def get_token_info(self):
        """Get the decoded claims of the current OIDC ID token

        Uses a pre-supplied token if available, otherwise runs the device-authorization flow.

        returns:
           the decoded token claims dict, or (False, None) if the device-authorization flow fails
        """
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

    def randomize_ip(self, url):
        """Replace url's hostname with a randomly-picked FQDN of one of its resolved IPs

        Used to spread load across backend hosts sharing one DNS name, on deployments
        without a real load balancer in front of the server (see $PANDA_BEHIND_REAL_LB).

        args:
           url: the request URL
        returns:
           url unchanged if $PANDA_BEHIND_REAL_LB is set, otherwise with the hostname replaced
        """
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
        host_names = [socket.getfqdn(vv) for vv in {v[-1][0] for v in socket.getaddrinfo(host, port, socket.AF_INET)}]
        return url.replace(host, random.choice(host_names))

    def _build_ssl_context(self, use_https):
        """Build the SSL context for host verification and (for voms auth) client-cert authentication

        args:
           use_https: whether the request URL uses https; non-https requests need no SSL context
        returns:
           False for a plain http request, otherwise an ssl.SSLContext configured per self.verify_host
           and self.auth_mode
        """
        if not use_https:
            return False
        if self.auth_mode != "oidc":
            if not self.ssl_certificate:
                self.ssl_certificate = _x509_proxy_path()
            if not self.ssl_key:
                self.ssl_key = _x509_proxy_path()
        if not self.verify_host:
            context = ssl._create_unverified_context()
        else:
            context = ssl.create_default_context(capath=_x509_ca_path() or None)
            if self.auth_mode != "oidc":
                # the grid proxy is typically self-issued, so it is also trusted as a CA
                context.load_verify_locations(cafile=self.ssl_certificate)
        if self.auth_mode != "oidc":
            context.load_cert_chain(certfile=self.ssl_certificate, keyfile=self.ssl_key)
        return context

    def _auth_headers(self):
        """Build the Authorization/Origin headers for OIDC auth, refreshing self.id_token as needed

        returns:
           a headers dict; empty for voms auth, since that authenticates via client cert instead
        """
        headers = {}
        if self.auth_mode == "oidc":
            self.get_id_token()
            headers["Authorization"] = f"Bearer {self.id_token}"
            headers["Origin"] = self.auth_vo
        return headers

    def _send(self, method, url, headers=None, content=None, files=None, verify=True):
        """Issue one HTTP request via httpx and normalize the result to (code, content)

        No exception escapes this method: network/SSL errors are caught and turned into
        (1, str(error)) so callers can treat every response uniformly.

        args:
           method: "GET", "POST", ...
           url: target URL
           headers: request headers
           content: raw request body bytes, for POST
           files: multipart files dict, for the file-upload PUT path
           verify: SSL verification, as returned by _build_ssl_context
        returns:
           (code, content) where code is 0 on HTTP 200, the raw HTTP status code otherwise,
           or 1 on a network/SSL-level failure (content is the error message string in that case)
        """
        try:
            if self.verbose:
                print(f"{method} {url}")
                print(f"headers = {hide_sensitive_info(headers)}")
            with httpx.Client(verify=verify, timeout=600) as client:
                response = client.request(method, url, headers=headers, content=content, files=files)
            ret = (0 if response.status_code == 200 else response.status_code, response.content)
        except Exception as e:
            if self.verbose:
                print(traceback.format_exc())
            ret = (1, str(e))
        if self.verbose:
            print(ret)
        return ret

    def get(self, url, data, n_try=1, json_out=False, repeating_keys=False, output_name=None):
        """Issue a GET request, with data sent as query-string parameters

        args:
           url: target URL
           data: dict of query parameters
           n_try: number of attempts; retries on any code other than 0/403/404
           json_out: True to request and parse a JSON response body
           repeating_keys: True to encode list-valued entries in data as repeated keys
           output_name: if set, write the response body to this file instead of returning it
        returns:
           (code, content): content is the parsed JSON body if json_out, True if output_name
           was used, or the raw response bytes otherwise
        """
        use_https = is_https(url)
        url = self.randomize_ip(url)
        if data:
            url = f"{url}?{urlencode(data, doseq=repeating_keys)}"
        verify = self._build_ssl_context(use_https)
        headers = self._auth_headers()
        if json_out:
            headers["Content-Type"] = "application/json"
            headers["Accept"] = "application/json"

        for i_try in range(n_try):
            code, content = self._send("GET", url, headers=headers, verify=verify)
            if code in (0, 403, 404) or i_try + 1 == n_try:
                break
            time.sleep(1)

        if json_out and code == 0:
            content = json.loads(content, object_hook=decode_special_cases)

        if code == 0 and output_name:
            with open(output_name, "wb") as f:
                f.write(content)
            content = True

        return code, content

    def post(self, url, data, is_json=False, compress_body=False, n_try=1, json_out=False):
        """Issue a POST request, with data sent as the request body

        args:
           url: target URL
           data: dict of form fields, sent url-encoded (or JSON if json_out/compress_body)
           is_json: True to request a JSON response body (without changing the request body encoding)
           compress_body: True to gzip-compress the JSON-encoded body
           n_try: number of attempts; retries on any code other than 0/403/404
           json_out: True to JSON-encode the request body and request/parse a JSON response
        returns:
           (code, content): content is the parsed JSON body if is_json/json_out, otherwise the raw
           response bytes
        """
        use_https = is_https(url)
        url = self.randomize_ip(url)
        verify = self._build_ssl_context(use_https)
        headers = self._auth_headers()
        if compress_body or json_out:
            headers["Content-Type"] = "application/json"
        if is_json or json_out:
            headers["Accept"] = "application/json"

        if compress_body:
            body_out = BytesIO()
            with gzip.GzipFile(fileobj=body_out, mode="w") as f_gzip:
                f_gzip.write(json.dumps(data).encode())
            body = body_out.getvalue()
        elif json_out:
            body = json.dumps(data).encode("utf-8")
        else:
            body = urlencode(data).encode()

        for i_try in range(n_try):
            code, content = self._send("POST", url, headers=headers, content=body, verify=verify)
            if code in (0, 403, 404) or i_try + 1 == n_try:
                break
            time.sleep(1)

        if code == 0 and (is_json or json_out):
            content = json.loads(content, object_hook=decode_special_cases)

        return code, content

    def put(self, url, data, n_try=1, json_out=False):
        """Upload one or more files as a multipart/form-data request

        Despite the name, this sends an HTTP POST (matching the server endpoint's
        expectation), not a PUT.

        args:
           url: target URL
           data: dict of {field_name: local_file_path}; each file is read and uploaded
           n_try: number of attempts; retries on any code other than 0/403/404
           json_out: True to request and parse a JSON response body
        returns:
           (code, content): content is the parsed JSON body if json_out and parsing succeeds,
           otherwise the raw response bytes
        """
        use_https = is_https(url)
        url = self.randomize_ip(url)
        verify = self._build_ssl_context(use_https)
        headers = self._auth_headers()
        if json_out:
            headers["Accept"] = "application/json"

        code, content = 1, "not attempted"
        for i_try in range(n_try):
            open_files = {k: open(v, "rb") for k, v in data.items()}
            try:
                files = {k: (v, open_files[k], "application/octet-stream") for k, v in data.items()}
                code, content = self._send("POST", url, headers=headers, files=files, verify=verify)
            finally:
                for fh in open_files.values():
                    fh.close()
            if code in (0, 403, 404) or i_try + 1 == n_try:
                break
            time.sleep(1)

        if code == 0 and json_out:
            try:
                content = json.loads(content, object_hook=decode_special_cases)
            except Exception:
                pass

        return code, content


def dump_log(func_name, exception_obj, output):
    """Log an unparseable/unexpected server response, e.g. when JSON decoding fails

    args:
       func_name: name of the calling function, for the log message
       exception_obj: the exception that was raised, if any (may be None)
       output: the raw response that couldn't be handled
    returns:
       the formatted error string that was logged
    """
    print(traceback.format_exc())
    print(output)
    err_str = f"{func_name} failed : {str(exception_obj)}"
    tmp_log = PLogger.getPandaLogger()
    tmp_log.error(err_str)
    return err_str


"""
public methods

"""


@http_request_decorator(endpoint="job/submit", method="post", json_out=True)
def submitJobs_internal(jobs, verbose=False):
    """Build the request payload for job/submit; see submitJobs for the public API"""
    return {"jobs": jobs}


def submitJobs(jobs, verbose=False):
    """Submit jobs

    args:
        jobs: a list of job specs
        verbose: True to see verbose messages
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
    return submitJobs_internal(jobs_serialized, verbose)


@http_request_decorator(endpoint="job/get_description", method="post", json_out=True)
def getJobStatus_internal(ids, verbose=False):
    """Build the request payload for job/get_description; see getJobStatus for the public API"""
    return {"job_ids": ids}


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
    status, jobs = getJobStatus_internal(ids, verbose=verbose)
    if status != 0:
        return status, jobs

    try:
        return status, MiscUtils.load_jobs(jobs)
    except Exception as e:
        dump_log("getJobStatus", e, jobs)
        return EC_Failed, None


@http_request_decorator(endpoint="job/kill", method="post", json_out=True)
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


@http_request_decorator(endpoint="task/kill", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/finish", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/retry", method="post", json_out=True, output_mode="full")
def retryTask_internal(jediTaskID, verbose, properErrorCode, newParams):
    """Build the request payload for task/retry; see retryTask for the public API"""
    data = {"task_id": jediTaskID}
    if newParams:
        data["new_parameters"] = newParams
    return data


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
    status, output = retryTask_internal(jediTaskID, verbose, properErrorCode, newParams)

    if status != 0:
        return status, output

    message = output["message"]
    data = output["data"]

    return status, (data, message)


def putFile(file, verbose=False, useCacheSrv=False, reuseSandbox=False, n_try=1):
    """Upload a file with the size limits: 10 MB for noBuild files, 768 MB for sources (Sandbox) files
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
    # size checks
    file_size = os.stat(file)[stat.ST_SIZE]
    exceeded_limit = False
    error_message = ""
    if os.path.basename(file).startswith("sources.") and file_size > SOURCES_LIMIT:
        error_message = (
            f"Exceeded size limit for sandbox files ({file_size}B >{SOURCES_LIMIT}B). "
            "Your working directory contains too large files which cannot be put on cache area. "
        )
        exceeded_limit = True

    elif not os.path.basename(file).startswith("sources.") and file_size > NO_BUILD_LIMIT:
        error_message = (
            f"Exceeded size limit ({file_size}B >{NO_BUILD_LIMIT}B). "
            "Your working directory contains too large files which cannot be put on cache area. "
            "Please submit job without --noBuild/--libDS so that your files will be uploaded to SE"
        )
        exceeded_limit = True

    if exceeded_limit:
        tmp_logger = PLogger.getPandaLogger()
        tmp_logger.error(error_message)
        return EC_Failed, "False"

    # Instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose

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
        data = {"file_size": file_size, "checksum": checksum}
        url = f"{server_base_path_ssl}/{endpoint}"
        status, output = client.post(url, data, json_out=True)
        if status != 0:
            return EC_Failed, f"ERROR: Could not check sandbox duplication with {output}"

        success = output.get("success", False)
        message = output.get("message", "")

        if message.startswith("FOUND:"):
            # found reusable sandbox
            rest = message.split(":", 1)[1]  # strip only the first "FOUND:"
            host_name, reusable_file_name = rest.rsplit(":", 1)
            # set cache server hostname
            setCacheServer(host_name)
            # return reusable filename
            return 0, f"NewFileName:{reusable_file_name}"

    if not useCacheSrv:
        global cache_base_path_ssl
        cache_base_path_ssl = server_base_path_ssl

    url = f"{cache_base_path_ssl}/file_server/upload_cache_file"
    data = {"file": file}
    s, o = client.put(url, data, n_try=n_try, json_out=True)

    # Status error
    if s != 0:
        return s, f"ERROR: Could not upload file with {o}"

    # Status OK, but somehow not a json response
    if isinstance(o, str):
        tmp_logger = PLogger.getPandaLogger()
        tmp_logger.error(f"{s}, {o}")
        return s, o

    success = o.get("success", False)
    if success:
        message = "True"
    else:
        message = o.get("message", "False")

    return s, message


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
    # instantiate the HTTP client
    client = _HttpClient()
    client.verbose = verbose
    # execute
    netloc = urlparse(baseURLCSRVSSL)
    url = f"{netloc.scheme}://{netloc.hostname}"
    if netloc.port:
        url += f":{netloc.port}"
    url = url + "/cache/" + filename
    s, o = client.get(url, {}, output_name=output_path, n_try=n_try)
    return s, o


def build_grid_setup_command():
    """Build a shell prefix that sources the grid environment setup script before a command

    Used to run grid commands (voms-proxy-info, echo $X509_CERT_DIR, ...) with the right
    environment via commands_get_output/commands_get_status_output_with_env.

    returns:
       shell command prefix, e.g. "unset ...; export PATH=...; source <script> > /dev/null;"
    """
    if "PATHENA_GRID_SETUP_SH" in os.environ:
        grid_setup_command = os.environ["PATHENA_GRID_SETUP_SH"]
    else:
        grid_setup_command = "/dev/null"
    grid_setup_command = f"source {grid_setup_command} > /dev/null;"
    # some grid_env.sh doesn't correct PATH/LD_LIBRARY_PATH
    grid_setup_command = f"unset LD_LIBRARY_PATH; unset PYTHONPATH; unset MANPATH; export PATH=/usr/local/bin:/bin:/usr/bin; {grid_setup_command}"
    return grid_setup_command


def getDN(dn):
    """Extract a display name from an X.509 distinguished name string

    Parses the "/CN=..." components of dn, preferring a full name (one
    containing a space) over a short name, and strips trailing digits/dots that
    grid DNs commonly append (e.g. numeric suffixes for renewed certs).

    args:
       dn: an X.509 DN, e.g. "/DC=ch/DC=cern/OU=.../CN=John Doe/CN=proxy"
    returns:
       the extracted name, or "" if no CN component was found
    """
    short_name = ""
    name = ""
    # a DN can carry several CN= components (e.g. ".../CN=John Doe/CN=proxy");
    # the first one containing a space is taken as the full name and wins
    # outright, otherwise the first space-less one is kept as a fallback
    for component in dn.split("/"):
        if component.startswith("CN="):
            name = re.sub("^CN=", "", component)
            name = re.sub(r"\d+$", "", name)
            name = re.sub(r"\.", "", name)
            name = name.strip()
            if re.search(" ", name) is not None:
                # look for full name
                name = name.replace(" ", "")
                break
            elif short_name == "":
                # keep short name
                short_name = name
            # reset so a later, non-full CN doesn't override the fallback
            name = ""
    # use short name
    if name == "":
        name = short_name
    # return
    return name


def useDevServer():
    """Point baseURL/baseURLSSL/baseURLCSRVSSL at the PanDA development server"""
    global baseURL
    baseURL = "http://aipanda007.cern.ch:25080/server/panda"
    global baseURLSSL
    baseURLSSL = "https://aipanda007.cern.ch:25443/server/panda"
    global baseURLCSRVSSL
    baseURLCSRVSSL = "https://aipanda007.cern.ch:25443/server/panda"


def useIntrServer():
    """Point baseURL/baseURLSSL/baseURLCSRVSSL/server_base_path[_ssl] at the PanDA INTR server"""
    global baseURL
    baseURL = "http://aipanda123.cern.ch:25080/server/panda"
    global baseURLSSL
    baseURLSSL = "https://aipanda123.cern.ch:25443/server/panda"
    global baseURLCSRVSSL
    baseURLCSRVSSL = baseURLSSL
    global server_base_path
    server_base_path = "http://aipanda123.cern.ch:25080/api/v1"
    global server_base_path_ssl
    server_base_path_ssl = "https://aipanda123.cern.ch:25443/api/v1"


# set cache server
def setCacheServer(host_name):
    """Point baseURLCSRVSSL/cache_base_path_ssl at a specific cache server host

    Used e.g. by putFile when the server reports that a reusable sandbox already
    exists on a specific cache server.

    args:
       host_name: hostname of the cache server to use
    """
    global baseURLCSRVSSL
    global cache_base_path_ssl

    parsed_url = urlparse(baseURLCSRVSSL)
    network_location = f"{host_name}:{parsed_url.port}" if parsed_url.port else host_name

    baseURLCSRVSSL = f"{parsed_url.scheme}://{network_location}{parsed_url.path}"
    cache_base_path_ssl = f"{parsed_url.scheme}://{network_location}/api/v1"


@http_request_decorator(endpoint="task/get_tasks_modified_since", method="get", json_out=True)
def getJobIDsJediTasksInTimeRange(timeRange, dn=None, minTaskID=None, verbose=False, task_type="user"):
    """Get task/job IDs modified since a given time

    args:
       timeRange: lower bound on modificationTime, in the format '%Y-%m-%d %H:%M:%S'
       dn: restrict to tasks owned by this DN; None for the caller's own tasks
       minTaskID: only return tasks with jediTaskID greater than this value
       verbose: True to see verbose messages
       task_type: prodSourceLabel to filter on, e.g. "user"
    returns:
       status code
          0: communication succeeded to the panda server
          255: communication failure
       a dictionary of {jediTaskID: [PandaID, ...], ...}, or None if failed
    """
    return {"since": timeRange, "dn": dn, "full": True, "min_task_id": minTaskID, "prod_source_label": task_type}


@http_request_decorator(endpoint="task/get_tasks_detailed_info_since", method="get", json_out=True)
def get_tasks_detailed_info_since(since=None, filters=None, n_tasks=500, verbose=False):
    """Get detailed info of tasks owned by the user from the PanDA server.

    args:
       since: lower bound on modificationTime in the format '%Y-%m-%d %H:%M:%S'.
              When None the time-window cap is removed so tasks of any age are returned,
              which is useful for by-jediTaskID / by-reqID lookups.
       filters: dict of {JediTaskSpec attribute: value} equality conditions, e.g.
              {"jediTaskID": "123"}, {"reqID": "45"}, {"status": "done"}. Values are
              sent as strings. Note that the server applies '|' patterns as python regex
              after truncating to n_tasks, so callers should expand OR-values into
              separate queries rather than passing a single '|'-joined value.
       n_tasks: maximum number of tasks to retrieve
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a list of task detail dictionaries, or None if failed
    """
    data = {"n_tasks": n_tasks}
    if since is not None:
        data["since"] = since
    if filters:
        data["filters"] = json.dumps(filters)
    return data


@http_request_decorator(endpoint="task/get_details", method="get", json_out=True)
def getJediTaskDetails_internal(taskDict, fullFlag, withTaskInfo, verbose=False):
    """Build the request payload for task/get_details; see getJediTaskDetails for the public API"""
    return {"task_id": taskDict["jediTaskID"], "include_parameters": fullFlag, "include_status": withTaskInfo}


def getJediTaskDetails(taskDict, fullFlag, withTaskInfo, verbose=False):
    """Get detailed info of a task, merged into the given task dictionary

    args:
       taskDict: a dict containing at least "jediTaskID"; updated in place with the task details
       fullFlag: True to include task parameters
       withTaskInfo: True to include task status info
       verbose: True to see verbose messages
    returns:
       status code
          0: communication succeeded to the panda server
          255: communication failure
       taskDict, updated with the retrieved details, or the raw error output if failed
    """
    status, tmp_dictionary = getJediTaskDetails_internal(taskDict, fullFlag, withTaskInfo, verbose)
    if status == 0:
        taskDict.update(tmp_dictionary)
        return status, taskDict

    return status, tmp_dictionary


@http_request_decorator(endpoint="job/get_description_incl_archive", method="post", json_out=True)
def getFullJobStatus_internal(ids, verbose=False):
    """Build the request payload for job/get_description_incl_archive; see getFullJobStatus for the public API"""
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


@http_request_decorator(endpoint="job/set_debug_mode", method="post", json_out=True)
def setDebugMode(pandaID, modeOn, verbose):
    """Turn debug mode on/off for a job

    args:
       pandaID: PanDA ID of the job
       modeOn: True to turn debug mode on, False to turn it off
       verbose: True to see verbose messages
    returns:
       status code
          0: communication succeeded to the panda server
          255: communication failure
       server diagnostic message, or None if failed
    """
    return {"job_id": pandaID, "mode": modeOn}


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
    """Request the server to build a new dataset out of specific events picked from specific files

    args:
       eventPickEvtList: path to a file listing the run/event numbers to pick
       eventPickDataType: data type of the events to pick
       eventPickStreamName: stream name of the events to pick
       eventPickDS: dataset(s) to pick events from
       eventPickAmiTag: AMI tag of the input data
       fileList: list of extra input file names to search, in addition to fileListName
       fileListName: path to a file with one extra input file name per line; "" to skip
       outDS: output dataset name; only the first two dot-separated fields are kept, with a
              generated unique suffix appended
       lockedBy: identifies the requester/system that locked this request
       params: extra parameters passed through to the server
       eventPickNumSites: number of sites to spread the output across, if > 1
       eventPickWithGUID: True to include file GUIDs in the run/event list sent to the server
       ei_api: event-index API version/selector
       verbose: True to see debug messages
    returns:
       (True, userDatasetName) on success; exits the process on failure
    """
    tmpLog = PLogger.getPandaLogger()

    # list of input files
    strInput = ""
    for tmpInput in fileList:
        if tmpInput != "":
            strInput += f"{tmpInput},"
    if fileListName != "":
        for tmpLine in open(fileListName):
            tmpInput = re.sub("\n", "", tmpLine)
            if tmpInput != "":
                strInput += f"{tmpInput},"
    strInput = strInput[:-1]

    # make dataset name
    ds_prefix, ds_suffix = outDS.split(".")[:2]
    userDatasetName = f"{ds_prefix}.{ds_suffix}.{MiscUtils.wrappedUuidGen()}/"

    # open run/event number list
    evpFile = open(eventPickEvtList)

    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose

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
    status, output = client.post(url, data)

    # failed
    if status != 0 or output is not True:
        print(output)
        errStr = "failed to request EventPicking"
        tmpLog.error(errStr)
        sys.exit(EC_Failed)

    # return user dataset name
    return True, userDatasetName


@http_request_decorator(endpoint="task/submit", method="post", json_out=True, output_mode="full")
def insertTaskParams_internal(taskParams, verbose=False, properErrorCode=False, parent_tid=None):
    """Build the request payload for task/submit; see insertTaskParams for the public API"""
    return {"task_parameters": taskParams, "parent_tid": parent_tid}


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
        if not output["success"]:
            # [error code, message]
            return status, (output["data"], output["message"])

        # [0, message including task ID]
        return status, (0, output["message"])

    except Exception:
        return EC_Failed, f"Impossible to parse server response. Output: {output}"


@http_request_decorator(endpoint="task/get_job_ids", method="get", json_out=True)
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


@http_request_decorator(endpoint="task/reactivate", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/resume", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/pause", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/get_status", method="get", json_out=True)
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


@http_request_decorator(endpoint="task/get_task_parameters", method="get", json_out=True)
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


@http_request_decorator(endpoint="job/get_metadata_for_analysis_jobs", method="get", json_out=True)
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


@http_request_decorator(endpoint="task/get_job_descriptions", method="get", json_out=True)
def get_job_descriptions(task_id, unsuccessful_only=False, verbose=False):
    """Get job descriptions for a task.

    args:
       task_id: jediTaskID of the task
       unsuccessful_only: True to return only failed, cancelled, or closed jobs
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a list of job description dictionaries, or None if failed
    """
    return {"task_id": task_id, "unsuccessful_only": unsuccessful_only}


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
    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose
    # execute
    url = server_base_path_ssl + "/system/is_alive"
    try:
        status, response = client.get(url, {}, json_out=True)

        # Communication issue with PanDA server
        if status != 0:
            tmp_message = f"Communication issue: {response}"
            tmp_log.error(tmp_message)
            return EC_Failed, tmp_message

        success = response.get("success", False)
        message = response.get("message", "")
        if not success:
            tmp_message = f"Problem with is_alive: {message}"
            tmp_log.error(tmp_message)
            return EC_Failed, tmp_message

        tmp_log.info(f"Done with success={success} and message='{message}'")
        return 0, message

    except Exception as e:
        tmp_message = f"Exception. {e}"
        tmp_log.error(tmp_message)
        return EC_Failed, tmp_message


@http_request_decorator(endpoint="system/get_attributes", method="get", json_out=True, output_mode="extended")
def get_cert_attributes_internal(verbose=False):
    """Build the (empty) request payload for system/get_attributes; see get_cert_attributes for the public API"""
    return {}


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
    status, output = get_cert_attributes_internal(verbose=verbose)

    if status != 0:
        return status, output

    success, data = output
    if success:
        # Print all the environment seen server side
        for k, v in data["environment"].items():
            print(f"{k}: {v}")

        # Return the certificate attributes
        cert_attributes = {k: v for k, v in data["environment"].items() if k.startswith("GRST_CRED")}

        return status, cert_attributes

    # data should just be an error message
    return status, "Could not retrieve certificate attributes"


# get username from token
def get_user_name_from_token():
    """Extract username and groups from ID token

    returns:
       a tuple of username, groups, and preferred username
    """
    client = _HttpClient()
    token_info = client.get_token_info()
    try:
        name = " ".join([t[:1].upper() + t[1:].lower() for t in str(token_info["name"]).split()])
        groups = token_info.get("groups", None)
        preferred_username = token_info.get("preferred_username", None)
        return name, groups, preferred_username
    except Exception:
        return None, None, None


def get_new_token(verbose=False):
    """Get new ID token

    args:
      verbose: True to see verbose message

    returns:
      a string of ID token. None if failed

    """
    client = _HttpClient()
    client.verbose = verbose
    if client.get_id_token(force_new=True):
        return client.id_token
    return None


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

    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose

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
        status, output = client.post(url, data, compress_body=compress, n_try=n_try)
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
        msg = f"Failed with {e}"
        print(traceback.format_exc())
        return EC_Failed, msg


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
    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose

    # execute
    url = baseURLSSL + "/execute_idds_workflow_command"
    try:
        data = dict()
        data["command_name"] = command_name
        if kwargs:
            data["kwargs"] = json.dumps(kwargs)
        if json_outputs:
            data["json_outputs"] = True
        status, output = client.post(url, data, n_try=n_try)
        if status != 0:
            return EC_Failed, output
        else:
            return 0, json.loads(output)
    except Exception as e:
        msg = f"Failed with {e}"
        print(traceback.format_exc())
        return EC_Failed, msg


@http_request_decorator(endpoint="file_server/upload_file_recovery_request", method="post", json_out=True, output_mode="extended")
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
    return {"task_id": task_id, "dry_run": dry_run}


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

    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose

    # execute
    output = None
    if relay_host:
        url = f"https://{relay_host}:25443/server/panda"
    else:
        url = baseURLSSL
    url += "/put_workflow_request"
    try:
        data = {"data": json.dumps(params)}
        if check:
            data["check"] = True
        else:
            data["sync"] = True
        status, output = client.post(url, data, compress_body=True, is_json=True)
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, output
    except Exception as e:
        msg = f"Failed with {e}."
        if output:
            msg += f' raw output="{str(output)}"'
        tmp_log.error(msg)
        return EC_Failed, msg


def submit_workflow_tmp(params, relay_host=None, check=False, verbose=False):
    """Temporary method to submit a PanDA native workflow
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
    # instantiate the HTTP client
    client = _HttpClient()
    client.ssl_certificate = _x509_proxy_path()
    client.ssl_key = _x509_proxy_path()
    client.verbose = verbose
    # execute
    output = None
    # if relay_host:
    #     url = "https://{}:25443/server/panda".format(relay_host)
    # else:
    #     url = baseURLSSL
    url = f"{server_base_path_ssl}/workflow/submit_workflow_raw_request"
    try:
        data = {"params": json.dumps(params)}
        # data = {"params": params}
        # if check:
        #     data["check"] = True
        # else:
        #     data["sync"] = True
        status, output = client.post(url, data, compress_body=False, is_json=True)
        if status != 0:
            tmp_log.error(output)
            return EC_Failed, output
        else:
            return 0, output
    except Exception as e:
        msg = f"Failed with {e}."
        if output:
            msg += f' raw output="{str(output)}"'
        tmp_log.error(msg)
        return EC_Failed, msg


@http_request_decorator(endpoint="workflow/submit_workflow_raw_request", method="post", json_out=True, output_mode="extended")
def submit_workflow(params, **kwargs):
    """Submit a PanDA native workflow
    args:
       params: a workflow definition dictionary
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a tuple of (True/False and diagnostic message). True if the request was accepted
    """
    return {"params": params}


@http_request_decorator(endpoint="creds/set_user_secrets", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="creds/get_user_secrets", method="get", json_out=True, output_mode="extended")
def get_user_secrets_internal(verbose=False):
    """Build the (empty) request payload for creds/get_user_secrets; see get_user_secrets for the public API"""
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


@http_request_decorator(endpoint="task/increase_attempts", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/reload_input", method="post", json_out=True, output_mode="extended")
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


@http_request_decorator(endpoint="task/get_datasets_and_files", method="get", json_out=True)
def get_files_in_datasets_internal(task_id, dataset_types, verbose=False):
    """Build the request payload for task/get_datasets_and_files; see get_files_in_datasets for the public API"""
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


@http_request_decorator(endpoint="event/get_event_range_statuses", method="get", json_out=True)
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


@http_request_decorator(endpoint="event/update_event_ranges", method="post", json_out=True)
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


@http_request_decorator(endpoint="task/get_detailed_info", method="get", json_out=True, output_mode="extended")
def get_task_details_json(task_id, verbose=False):
    """Get detailed info of a task in JSON format
    args:
       task_id: jediTaskID of the task
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a list of job metadata dictionaries, or error message if failed
    """
    return {"task_id": task_id}


@http_request_decorator(endpoint="task/get_parent_detailed_info", method="get", json_out=True)
def get_parent_detailed_info(task_id, verbose=False):
    """Get detailed info of the parent task for a given child task.

    args:
       task_id: jediTaskID of the child task
       verbose: True to see verbose message
    returns:
       status code
          0: communication succeeded to the panda server
        255: communication failure
       a dictionary with parent task details, or None if failed
    """
    return {"task_id": task_id}
