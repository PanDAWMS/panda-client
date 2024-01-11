import datetime
import json
import os
import re
import ssl
import sys
import time

try:
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen
except ImportError:
    from urllib import urlencode

    from urllib2 import HTTPError, Request, URLError, urlopen

try:
    baseMonURL = os.environ["PANDAMON_URL"]
except Exception:
    baseMonURL = "https://bigpanda.cern.ch"

HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


def query_tasks(
    jeditaskid=None, username=None, limit=10000, taskname=None, status=None, superstatus=None, reqid=None, days=None, metadata=False, sync=False, verbose=False
):
    timestamp = int(time.time())
    parmas = {
        "json": 1,
        "datasets": True,
        "limit": limit,
    }
    if jeditaskid:
        parmas["jeditaskid"] = jeditaskid
    if username:
        parmas["username"] = username
    if taskname:
        parmas["taskname"] = taskname
    if status:
        parmas["status"] = status
    if superstatus:
        parmas["superstatus"] = superstatus
    if reqid:
        parmas["reqid"] = reqid
    if days is not None:
        parmas["days"] = days
    if metadata:
        parmas["extra"] = "metastruct"
    if sync:
        parmas["timestamp"] = timestamp
    url = baseMonURL + "/tasks/?{0}".format(urlencode(parmas))
    if verbose:
        sys.stderr.write("query url = {0}\n".format(url))
        sys.stderr.write("headers   = {0}\n".format(json.dumps(HEADERS)))
    try:
        req = Request(url, headers=HEADERS)
        try:
            # Skip SSL verification
            context = ssl._create_unverified_context()
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            rep = urlopen(req)
        else:
            rep = urlopen(req, context=context)
        if verbose:
            sys.stderr.write("time UTC  = {0}\n".format(datetime.datetime.utcnow()))
        rec = rep.getcode()
        if verbose:
            sys.stderr.write("resp code = {0}\n".format(rec))
        res = rep.read().decode("utf-8")
        if verbose:
            sys.stderr.write("data = {0}\n".format(res))
        ret = json.loads(res)
        return timestamp, url, ret
    except Exception as e:
        err_str = "{0} : {1}".format(e.__class__.__name__, e)
        sys.stderr.write("{0}\n".format(err_str))
        raise


def datetime_parser(d):
    for k, v in d.items():
        if isinstance(v, str) and re.search("^\d{4}-\d{2}-\d{2}(\W|T)\d{2}:\d{2}:\d{2}", v):
            try:
                d[k] = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                try:
                    d[k] = datetime.datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass
    return d


def query_jobs(jeditaskid, limit=10000, drop=True, verbose=False):
    parmas = {
        "json": 1,
        "limit": limit,
    }
    parmas["jeditaskid"] = jeditaskid
    if not drop:
        parmas["mode"] = "nodrop"
    url = baseMonURL + "/jobs/?{0}".format(urlencode(parmas))
    if verbose:
        sys.stderr.write("query url = {0}\n".format(url))
        sys.stderr.write("headers   = {0}\n".format(json.dumps(HEADERS)))
    try:
        req = Request(url, headers=HEADERS)
        try:
            # Skip SSL verification
            context = ssl._create_unverified_context()
        except AttributeError:
            # Legacy Python that doesn't verify HTTPS certificates by default
            rep = urlopen(req)
        else:
            rep = urlopen(req, context=context)
        if verbose:
            sys.stderr.write("time UTC  = {0}\n".format(datetime.datetime.utcnow()))
        rec = rep.getcode()
        if verbose:
            sys.stderr.write("resp code = {0}\n".format(rec))
        res = rep.read().decode("utf-8")
        if verbose:
            sys.stderr.write("data = {0}\n".format(res))
        ret = json.loads(res, object_hook=datetime_parser)
        return url, ret
    except Exception as e:
        err_str = "{0} : {1}".format(e.__class__.__name__, e)
        sys.stderr.write("{0}\n".format(err_str))
        raise
