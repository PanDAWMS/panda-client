import time
import json

try:
    from urllib.parmase import urlencode
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
except ImportError:
    from urllib import urlencode
    from urllib2 import urlopen, Request, HTTPError


HEADERS = {'Accept': 'application/json', 'Content-Type':'application/json'}


def query_tasks(jeditaskid=None, username=None, limit=10000, taskname=None, days=None, metadata=False, sync=False):
    timestamp = int(time.time())
    parmas = {  'json': 1,
                'datasets': True,
                'limit':limit,
                }
    if jeditaskid:
        parmas['jeditaskid'] = jeditaskid
    if username:
        parmas['username'] = username
    if taskname:
        parmas['taskname'] = taskname
    if days is not None:
        parmas['days'] = days
    if metadata:
        parmas['extra'] = 'metastruct'
    if sync:
        parmas['timestamp'] = timestamp
    url = 'https://bigpanda.cern.ch/tasks/?{0}'.format(urlencode(parmas))
    req = Request(url, headers=HEADERS)
    res = urlopen(req).read().decode('utf-8')
    ret = json.loads(res)
    return timestamp, url, ret
