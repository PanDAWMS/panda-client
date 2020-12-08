import os
import sys
import ssl
import uuid
import json
import time
import glob
import base64
import datetime

try:
    from urllib import urlencode
    from urllib2 import urlopen, Request, HTTPError
except ImportError:
    from urllib.request import urlopen, Request
    from urllib.parse import urlencode
    from urllib.error import HTTPError
    raw_input = input


TOKEN_BASENAME = '.token'
CACHE_PREFIX = '.page_cache_'


class OpenIdConnect_Utils:

    # constructor
    def __init__(self, auth_config_url, token_dir=None, log_stream=None, verbose=False):
        self.auth_config_url = auth_config_url
        if token_dir is None:
            token_dir = os.environ['PANDA_CONFIG_ROOT']
        self.token_dir = os.path.expanduser(token_dir)
        if not os.path.exists(token_dir):
            os.makedirs(token_dir)
        self.log_stream = log_stream
        self.verbose = verbose

    # get token path
    def get_token_path(self):
        return os.path.join(self.token_dir, TOKEN_BASENAME)

    # get device code
    def get_device_code(self, device_auth_endpoint, client_id, audience):
        if self.verbose:
            self.log_stream.debug('getting device code')
        data = {'client_id': client_id,
                'scope': "openid profile email offline_access",
                'audience': audience}
        rdata = urlencode(data).encode()
        req = Request(device_auth_endpoint, rdata)
        req.add_header('content-type', 'application/x-www-form-urlencoded')
        try:
            conn = urlopen(req)
            text = conn.read()
            if self.verbose:
                self.log_stream.debug(text)
            return True, json.loads(text)
        except HTTPError as e:
            return False, 'code={0}. reason={1}. description={2}'.format(e.code, e.reason, e.read())
        except Exception as e:
            return False, str(e)

    # get ID token
    def get_id_token(self, token_endpoint, client_id, client_secret, device_code, interval, expires_in):
        self.log_stream.info('Ready to get ID token?')
        while True:
            sys.stdout.write("[y/n] \n")
            choice = raw_input().lower()
            if choice == 'y':
                break
            elif choice == 'n':
                return False, "aborted"
        if self.verbose:
            self.log_stream.debug('getting ID token')
        startTime = datetime.datetime.utcnow()
        data = {'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
                'device_code': device_code}
        rdata = urlencode(data).encode()
        req = Request(token_endpoint, rdata)
        req.add_header('content-type', 'application/x-www-form-urlencoded')
        while datetime.datetime.utcnow() - startTime < datetime.timedelta(seconds=expires_in):
            try:
                conn = urlopen(req)
                text = conn.read().decode()
                if self.verbose:
                    self.log_stream.debug(text)
                id_token = json.loads(text)['id_token']
                with open(self.get_token_path(), 'w') as f:
                    f.write(text)
                return True, id_token
            except HTTPError as e:
                text = e.read()
                try:
                    description = json.loads(text)
                    # pending
                    if description['error'] == "authorization_pending":
                        time.sleep(interval + 1)
                        continue
                except Exception:
                    pass
                return False, 'code={0}. reason={1}. description={2}'.format(e.code, e.reason, text)
            except Exception as e:
                return False, str(e)

    # refresh token
    def refresh_token(self, token_endpoint, client_id, client_secret, refresh_token_string):
        if self.verbose:
            self.log_stream.debug('refreshing token')
        data = {'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token_string}
        rdata = urlencode(data).encode()
        req = Request(token_endpoint, rdata)
        req.add_header('content-type', 'application/x-www-form-urlencoded')
        try:
            conn = urlopen(req)
            text = conn.read()
            if self.verbose:
                self.log_stream.debug(text)
            id_token = json.loads(text)['id_token']
            with open(self.get_token_path(), 'w') as f:
                f.write(text)
            return True, id_token
        except HTTPError as e:
            return False, 'code={0}. reason={1}. description={2}'.format(e.code, e.reason, e.read())
        except Exception as e:
            return False, str(e)

    # fetch page
    def fetch_page(self, url):
        path = os.path.join(self.token_dir, CACHE_PREFIX + str(uuid.uuid5(uuid.NAMESPACE_URL, str(url))))
        if os.path.exists(path) and \
                datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path)) < \
                datetime.timedelta(hours=1):
            try:
                with open(path) as f:
                    return True, json.load(f)
            except Exception as e:
                self.log_stream.debug('cached {0} is corrupted: {1}'.format(os.path.basename(url), str(e)))
        if self.verbose:
            self.log_stream.debug('fetching {0}'.format(url))
        try:
            context = ssl._create_unverified_context()
            conn = urlopen(url, context=context)
            text = conn.read().decode()
            if self.verbose:
                self.log_stream.debug(text)
            with open(path, 'w') as f:
                f.write(text)
            with open(path) as f:
                return True, json.load(f)
        except HTTPError as e:
            return False, 'code={0}. reason={1}. description={2}'.format(e.code, e.reason, e.read())
        except Exception as e:
            return False, str(e)

    # check token expiry
    def check_token(self):
        token_file = self.get_token_path()
        if os.path.exists(token_file):
            with open(token_file) as f:
                if self.verbose:
                    self.log_stream.debug('check {0}'.format(token_file))
                try:
                    # decode ID token
                    data = json.load(f)
                    enc = data['id_token'].split('.')[1]
                    enc += '=' * (-len(enc) % 4)
                    dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
                    exp_time = datetime.datetime.fromtimestamp(dec['exp'])
                    delta = exp_time - datetime.datetime.now()
                    if self.verbose:
                        self.log_stream.debug('token expiration time : {0}'.\
                                              format(exp_time.strftime("%Y-%m-%d %H:%M:%S")))
                    # check expiration time
                    if delta < datetime.timedelta(minutes=10):
                        # return refresh token
                        if 'refresh_token' in data:
                            if self.verbose:
                                self.log_stream.debug('to refresh token')
                            return False, data['refresh_token'], dec
                    else:
                        # return valid token
                        if self.verbose:
                            self.log_stream.debug('valid token is available')
                        return True, data['id_token'], dec
                except Exception as e:
                    self.log_stream.error('failed to decode cached token with {0}'.format(e))
        if self.verbose:
            self.log_stream.debug('cached token unavailable')
        return False, None, None

    # run device authorization flow
    def run_device_authorization_flow(self):
        # check toke expiry
        s, o, dec = self.check_token()
        if s:
            # still valid
            return True, o
        refresh_token_string = o
        # get auth config
        s, o = self.fetch_page(self.auth_config_url)
        if not s:
            return False, "Failed to get Auth configuration: " + o
        auth_config = o
        # get endpoint config
        s, o = self.fetch_page(auth_config['oidc_config_url'])
        if not s:
            return False, "Failed to get endpoint configuration: " + o
        endpoint_config = o
        # refresh token
        if refresh_token_string is not None:
            s, o = self.refresh_token(endpoint_config['token_endpoint'], auth_config['client_id'],
                                 auth_config['client_secret'], refresh_token_string)
            # refreshed
            if s:
                return True, o
        # get device code
        s, o = self.get_device_code(endpoint_config['device_authorization_endpoint'], auth_config['client_id'],
                                    auth_config['audience'])
        if not s:
            return False, 'Failed to get device code: ' + o
        # get ID token
        self.log_stream.info(("Please go to {0} and sign in. "
                         "Waiting until authentication is completed").format(o['verification_uri_complete']))
        if 'interval' in o:
            interval = o['interval']
        else:
            interval = 5
        s, o = self.get_id_token(endpoint_config['token_endpoint'], auth_config['client_id'],
                                 auth_config['client_secret'], o['device_code'], interval, o['expires_in'])
        if not s:
            return False, "Failed to get ID token: " + o
        self.log_stream.info('All set')
        return True, o

    # cleanup
    def cleanup(self):
        for patt in [TOKEN_BASENAME, CACHE_PREFIX]:
            for f in glob.glob(os.path.join(self.token_dir, patt + '*')):
                os.remove(f)
