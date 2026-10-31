"""Unwired gateway-namespace health helper for a fixed synthetic topology.

No runtime caller exists. The operator must first independently enforce gateway
and backend network/container identity; DNS name alone is never authentication.
Only main() is supported: its alarm covers stdin and network calls; the private
_probe() is not independently deadline-bounded. A hard deadline is not assured
for uninterruptible kernel work. No Python runtime is qualified in the current
nginx image, and no network identity or executable/container wiring exists.
The NodeODM API token is passed on stdin, never in helper argv or output. Its
upstream query string can still appear in engine/proxy/network logs; log policy
must be separately qualified. This helper never authorizes maintenance.
"""
import json
import os
import signal
import socket
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import build_opener, ProxyHandler, HTTPRedirectHandler, Request

_MAX_INPUT=8192
_MAX_RESPONSE=65536
_DEADLINE=5.0
_ENDPOINTS=('http://nodeodm:3000/info','http://nodeodm:3000/task/list')

class Refused(ValueError):pass

def _pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result:raise Refused('duplicate_key')
        result[key]=value
    return result

def _bad_constant(_):raise Refused('nonfinite')

def _json(raw):
    try:return json.loads(raw.decode('utf-8'),object_pairs_hook=_pairs,parse_constant=_bad_constant)
    except (UnicodeError,ValueError,TypeError,RecursionError):raise Refused('invalid_json') from None

class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*_args,**_kwargs):raise Refused('redirect')

def _probe(raw, *, opener=None, clock=time.monotonic):
    """Internal only: main supplies a POSIX alarm around input and this call."""
    if type(raw) is not bytes or len(raw)>_MAX_INPUT:raise Refused('input')
    request=_json(raw)
    if type(request) is not dict or set(request)!={'token','engine'} or request['engine'] not in ('odm','odx'):
        raise Refused('input')
    token=request['token']
    if type(token) is not str or len(token)>4096 or any(ord(c)<33 or ord(c)>126 for c in token):
        raise Refused('input')
    opener=opener or build_opener(ProxyHandler({}),_NoRedirect())
    deadline=clock()+_DEADLINE
    values=[]
    for endpoint in _ENDPOINTS:
        remaining=deadline-clock()
        if remaining<=0:raise Refused('deadline')
        # Existing NodeODM compatibility uses a query token. Do not log URL.
        url=endpoint+('?' + urlencode({'token':token}) if token else '')
        try:
            with opener.open(Request(url,method='GET',headers={'Accept':'application/json'}),timeout=remaining) as response:
                if response.status!=200:raise Refused('response')
                body=response.read(_MAX_RESPONSE+1)
                if len(body)>_MAX_RESPONSE:raise Refused('response')
                values.append(_json(body))
        except (OSError,URLError,HTTPError,socket.timeout):raise Refused('transport') from None
        if clock()>deadline:raise Refused('deadline')
    info,tasks=values
    if (type(info) is not dict or info.get('engine')!=request['engine'] or
        type(info.get('taskQueueCount')) is not int or info['taskQueueCount']!=0 or
        type(tasks) is not list or tasks!=[]):raise Refused('not_idle')
    return {'healthObserved':True,'archiveAuthorized':False,'maintenanceAuthorized':False}

def _alarm(_signum,_frame):raise Refused('deadline')

def main():
    try:
        if os.name!='posix' or not hasattr(signal,'setitimer'):
            raise Refused('bounded_posix_required')
        signal.signal(signal.SIGALRM,_alarm)
        signal.setitimer(signal.ITIMER_REAL,_DEADLINE)
        raw=sys.stdin.buffer.read(_MAX_INPUT+1)
        result=_probe(raw)
    except Exception:
        # Do not print input, URL, exceptions, or backend data.
        print('{"healthObserved":false,"maintenanceAuthorized":false}')
        return 1
    finally:
        if hasattr(signal,'setitimer'):signal.setitimer(signal.ITIMER_REAL,0)
    print(json.dumps(result,separators=(',',':')))
    return 0

if __name__=='__main__':sys.exit(main())
