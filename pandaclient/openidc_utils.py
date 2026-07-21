import base64
import datetime
import glob
import json
import os
import ssl
import sys
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TOKEN_BASENAME = ".token"
CACHE_PREFIX = ".page_cache_"


# get an SSL context with a usable CA trust store
def _get_ssl_context():
    context = ssl.create_default_context()
    # python.org macOS builds ship an empty default trust store; fall back to
    # the bundled certifi roots so OIDC works without setting SSL_CERT_FILE.
    if not context.get_ca_certs():
        try:
            import certifi

            context.load_verify_locations(cafile=certifi.where())
        except Exception:
            pass
    return context


# decode ID token
def decode_id_token(enc):
    enc = enc.split(".")[1]
    enc += "=" * (-len(enc) % 4)
    dec = json.loads(base64.urlsafe_b64decode(enc.encode()))
    return dec


# utility class
class OpenIdConnect_Utils:
    # constructor
    def __init__(self, auth_config_url, token_dir=None, log_stream=None, verbose=False):
        self.auth_config_url = auth_config_url
        if token_dir is None:
            token_dir = os.environ.get("PANDA_CONFIG_ROOT", ".")
        self.token_dir = os.path.expanduser(token_dir)
        if not os.path.exists(token_dir):
            os.makedirs(token_dir)
        self.log_stream = log_stream
        self.verbose = verbose

    # get token path
    def get_token_path(self):
        return os.path.join(self.token_dir, TOKEN_BASENAME)

    # get device code
    def get_device_code(self, device_auth_endpoint, client_id, audience, jwt_profile):
        if self.verbose:
            self.log_stream.debug("getting device code")
        scopes = "openid profile email offline_access "
        if jwt_profile == "wlcg":
            scopes += "wlcg wlcg.groups "
        data = {"client_id": client_id, "scope": scopes, "audience": audience}  # iam",
        rdata = urlencode(data).encode()
        if self.verbose:
            self.log_stream.debug(f"request url: {device_auth_endpoint} data: {rdata}")
        req = Request(device_auth_endpoint, rdata)
        req.add_header("content-type", "application/x-www-form-urlencoded")
        try:
            conn = urlopen(req, context=_get_ssl_context())
            text = conn.read()
            if self.verbose:
                self.log_stream.debug(text)
            return True, json.loads(text)
        except HTTPError as e:
            return False, f"code={e.code}. reason={e.reason}. description={e.read()}"
        except Exception as e:
            return False, str(e)

    # get ID token
    def get_id_token(self, token_endpoint, client_id, client_secret, device_code, interval, expires_in):
        self.log_stream.info("Ready to get ID token?")
        while True:
            sys.stdout.write("[y/n] \n")
            choice = input().lower()
            if choice == "y":
                break
            elif choice == "n":
                return False, "aborted"
        if self.verbose:
            self.log_stream.debug("getting ID token")
        startTime = datetime.datetime.utcnow()
        data = {
            "client_id": client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": device_code,
        }
        if client_secret:
            data["client_secret"] = client_secret
        rdata = urlencode(data).encode()
        req = Request(token_endpoint, rdata)
        req.add_header("content-type", "application/x-www-form-urlencoded")
        while datetime.datetime.utcnow() - startTime < datetime.timedelta(seconds=expires_in):
            try:
                conn = urlopen(req, context=_get_ssl_context())
                text = conn.read().decode()
                if self.verbose:
                    self.log_stream.debug(text)
                id_token = json.loads(text)["id_token"]
                with open(self.get_token_path(), "w") as f:
                    f.write(text)
                return True, id_token
            except HTTPError as e:
                text = e.read()
                try:
                    description = json.loads(text)
                    # pending
                    if description["error"] == "authorization_pending":
                        time.sleep(interval + 1)
                        continue
                except Exception:
                    pass
                return False, f"code={e.code}. reason={e.reason}. description={text}"
            except Exception as e:
                return False, str(e)

    # refresh token
    def refresh_token(self, token_endpoint, client_id, client_secret, refresh_token_string):
        if self.verbose:
            self.log_stream.debug("refreshing token")
        data = {"client_id": client_id, "client_secret": client_secret, "grant_type": "refresh_token", "refresh_token": refresh_token_string}
        rdata = urlencode(data).encode()
        req = Request(token_endpoint, rdata)
        req.add_header("content-type", "application/x-www-form-urlencoded")
        try:
            conn = urlopen(req, context=_get_ssl_context())
            text = conn.read().decode()
            if self.verbose:
                self.log_stream.debug(text)
            id_token = json.loads(text)["id_token"]
            with open(self.get_token_path(), "w") as f:
                f.write(text)
            return True, id_token
        except HTTPError as e:
            return False, f"code={e.code}. reason={e.reason}. description={e.read()}"
        except Exception as e:
            return False, str(e)

    # fetch page
    def fetch_page(self, url):
        path = os.path.join(self.token_dir, CACHE_PREFIX + str(uuid.uuid5(uuid.NAMESPACE_URL, str(url))))
        if os.path.exists(path) and datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path)) < datetime.timedelta(hours=1):
            try:
                with open(path) as f:
                    return True, json.load(f)
            except Exception as e:
                self.log_stream.debug(f"cached {os.path.basename(url)} is corrupted: {str(e)}")
        if self.verbose:
            self.log_stream.debug(f"fetching {url}")
        try:
            context = ssl._create_unverified_context()
            conn = urlopen(url, context=context)
            text = conn.read().decode()
            if self.verbose:
                self.log_stream.debug(text)
            with open(path, "w") as f:
                f.write(text)
            with open(path) as f:
                return True, json.load(f)
        except HTTPError as e:
            return False, f"code={e.code}. reason={e.reason}. description={e.read()}"
        except Exception as e:
            return False, str(e)

    # check token expiry
    def check_token(self):
        token_file = self.get_token_path()
        if os.path.exists(token_file):
            with open(token_file) as f:
                if self.verbose:
                    self.log_stream.debug(f"check {token_file}")
                try:
                    # decode ID token
                    data = json.load(f)
                    dec = decode_id_token(data["id_token"])
                    exp_time = datetime.datetime.utcfromtimestamp(dec["exp"])
                    delta = exp_time - datetime.datetime.utcnow()
                    if self.verbose:
                        self.log_stream.debug("token expiration time : {} UTC".format(exp_time.strftime("%Y-%m-%d %H:%M:%S")))
                    # check expiration time
                    if delta < datetime.timedelta(minutes=5):
                        # return refresh token
                        if "refresh_token" in data:
                            if self.verbose:
                                self.log_stream.debug("to refresh token")
                            return False, data["refresh_token"], dec
                    else:
                        # return valid token
                        if self.verbose:
                            self.log_stream.debug("valid token is available")
                        return True, data["id_token"], dec
                except Exception as e:
                    self.log_stream.error(f"failed to decode cached token with {e}")
        if self.verbose:
            self.log_stream.debug("cached token unavailable")
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
        s, o = self.fetch_page(auth_config["oidc_config_url"])
        if not s:
            return False, "Failed to get endpoint configuration: " + o
        endpoint_config = o
        # refresh token
        if refresh_token_string is not None:
            s, o = self.refresh_token(endpoint_config["token_endpoint"], auth_config["client_id"], auth_config["client_secret"], refresh_token_string)
            # refreshed
            if s:
                return True, o
            else:
                if self.verbose:
                    self.log_stream.debug(f"failed to refresh token: {o}")
        # get device code
        jwt_profile = auth_config.get("jwt_profile")
        s, o = self.get_device_code(endpoint_config["device_authorization_endpoint"], auth_config["client_id"], auth_config["audience"], jwt_profile)
        if not s:
            return False, "Failed to get device code: " + o
        # get ID token
        self.log_stream.info(("Please go to {} and sign in. " "Waiting until authentication is completed").format(o["verification_uri_complete"]))
        if "interval" in o:
            interval = o["interval"]
        else:
            interval = 5
        s, o = self.get_id_token(
            endpoint_config["token_endpoint"], auth_config["client_id"], auth_config["client_secret"], o["device_code"], interval, o["expires_in"]
        )
        if not s:
            return False, "Failed to get ID token: " + o
        self.log_stream.info("All set")
        return True, o

    # cleanup
    def cleanup(self):
        for patt in [TOKEN_BASENAME, CACHE_PREFIX]:
            for f in glob.glob(os.path.join(self.token_dir, patt + "*")):
                os.remove(f)
