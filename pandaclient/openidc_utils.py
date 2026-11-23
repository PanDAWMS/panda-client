import base64
import datetime
import glob
import json
import os
import ssl
import time
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# the token endpoint response (ID and refresh tokens) is kept in $PANDA_TOKEN_DIR/token, next to the
# cached auth config and discovery pages
TOKEN_DIR_ENV = "PANDA_TOKEN_DIR"
DEFAULT_TOKEN_DIR = "~/.token/panda"
TOKEN_BASENAME = "token"
CACHE_PREFIX = ".page_cache_"
# where versions before PANDA_TOKEN_DIR kept the token
LEGACY_TOKEN_BASENAME = ".token"


# path of the token file used by older versions, or None if $PANDA_CONFIG_ROOT is not set
def _legacy_token_path():
    config_root = os.environ.get("PANDA_CONFIG_ROOT")
    if not config_root:
        return None
    return os.path.join(os.path.abspath(os.path.expanduser(config_root)), LEGACY_TOKEN_BASENAME)


# write a file readable by the owner only
def _write_private(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        # the mode above only applies to a new file: also fix one left world-readable by older versions
        os.fchmod(f.fileno(), 0o600)
        f.write(text)


# create a directory and its missing parents, each readable by the owner only. Existing
# directories are left as they are, since they may be shared
def _makedirs_private(path):
    missing = []
    while not os.path.isdir(path):
        missing.append(path)
        parent = os.path.dirname(path)
        if parent == path:
            break
        path = parent
    for directory in reversed(missing):
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            # created concurrently by another process
            pass


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
        self.log_stream = log_stream
        self.verbose = verbose
        explicit_dir = token_dir is not None
        if not explicit_dir:
            token_dir = os.environ.get(TOKEN_DIR_ENV) or DEFAULT_TOKEN_DIR
        self.token_dir = os.path.abspath(os.path.expanduser(token_dir))
        _makedirs_private(self.token_dir)
        if not explicit_dir:
            self._migrate_legacy_token()

    # get token path
    def get_token_path(self):
        return os.path.join(self.token_dir, TOKEN_BASENAME)

    # move a token left in $PANDA_CONFIG_ROOT by older versions, so the user does not have to log in again
    def _migrate_legacy_token(self):
        legacy_path = _legacy_token_path()
        token_path = self.get_token_path()
        if legacy_path is None or os.path.exists(token_path) or not os.path.isfile(legacy_path):
            return
        try:
            os.replace(legacy_path, token_path)
            os.chmod(token_path, 0o600)
            if self.verbose:
                self.log_stream.debug(f"moved {legacy_path} to {token_path}")
        except OSError as e:
            # not fatal: without the old token the user goes through the device flow again
            if self.verbose:
                self.log_stream.debug(f"failed to move {legacy_path} to {token_path}: {e}")

    # write the token endpoint response, which holds the refresh token
    def write_token(self, text):
        _write_private(self.get_token_path(), text)

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

    # get ID token, polling the token endpoint until the user has signed in or the device code expires
    def get_id_token(self, token_endpoint, client_id, client_secret, device_code, interval, expires_in):
        if self.verbose:
            self.log_stream.debug("getting ID token")
        deadline = time.monotonic() + expires_in
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
        try:
            while time.monotonic() < deadline:
                try:
                    conn = urlopen(req, context=_get_ssl_context())
                    text = conn.read().decode()
                    if self.verbose:
                        self.log_stream.debug(text)
                    id_token = json.loads(text)["id_token"]
                    self.write_token(text)
                    return True, id_token
                except HTTPError as e:
                    text = e.read()
                    try:
                        error = json.loads(text)["error"]
                    except Exception:
                        error = None
                    # the user has not signed in yet
                    if error == "authorization_pending":
                        time.sleep(interval + 1)
                        continue
                    # polling too often: wait 5 seconds longer from now on (RFC 8628)
                    if error == "slow_down":
                        interval += 5
                        time.sleep(interval + 1)
                        continue
                    return False, f"code={e.code}. reason={e.reason}. description={text}"
                except Exception as e:
                    return False, str(e)
        except KeyboardInterrupt:
            return False, "aborted"
        return False, f"sign-in was not completed within {round(expires_in / 60)} minutes"

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
            self.write_token(text)
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
                self.log_stream.debug(f"cached {os.path.basename(url)} is corrupted: {e!s}")
        if self.verbose:
            self.log_stream.debug(f"fetching {url}")
        try:
            context = ssl._create_unverified_context()
            conn = urlopen(url, context=context)
            text = conn.read().decode()
            if self.verbose:
                self.log_stream.debug(text)
            # the auth config holds the client secret
            _write_private(path, text)
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
                    if self.verbose:
                        exp_time = datetime.datetime.fromtimestamp(dec["exp"], datetime.timezone.utc)
                        self.log_stream.debug(f"token expiration time : {exp_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
                    # check expiration time
                    if dec["exp"] - time.time() < 5 * 60:
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
            return False, f"Failed to get Auth configuration: {o}"
        auth_config = o
        # get endpoint config
        s, o = self.fetch_page(auth_config["oidc_config_url"])
        if not s:
            return False, f"Failed to get endpoint configuration: {o}"
        endpoint_config = o
        # refresh token
        if refresh_token_string is not None:
            s, o = self.refresh_token(endpoint_config["token_endpoint"], auth_config["client_id"], auth_config["client_secret"], refresh_token_string)
            # refreshed
            if s:
                return True, o
            if self.verbose:
                self.log_stream.debug(f"failed to refresh token: {o}")
        # get device code
        jwt_profile = auth_config.get("jwt_profile")
        s, o = self.get_device_code(endpoint_config["device_authorization_endpoint"], auth_config["client_id"], auth_config["audience"], jwt_profile)
        if not s:
            return False, f"Failed to get device code: {o}"
        # get ID token
        self.log_stream.info(f"Please go to {o['verification_uri_complete']} and sign in. Waiting until authentication is completed (Ctrl-C to abort)")
        if "interval" in o:
            interval = o["interval"]
        else:
            interval = 5
        s, o = self.get_id_token(
            endpoint_config["token_endpoint"], auth_config["client_id"], auth_config["client_secret"], o["device_code"], interval, o["expires_in"]
        )
        if not s:
            return False, f"Failed to get ID token: {o}"
        self.log_stream.info("All set")
        return True, o

    # cleanup
    def cleanup(self):
        # also a token left by older versions, which would otherwise be moved back in by the next run
        for path in [self.get_token_path(), _legacy_token_path()] + glob.glob(os.path.join(self.token_dir, f"{CACHE_PREFIX}*")):
            if path is not None and os.path.isfile(path):
                os.remove(path)
