"""Inactive local Unix peer-credential capacity protocol experiment.

Caller owns accepted socket, listener and lifecycle. No listener is installed.
Only a fixed trusted-UID peer may name an existing server-side immutable plan.
There is no remote authentication, coordinator integration, physical quota,
mount fence, or automatic cleanup. Status is journal state, not an admission
capability. Trusted plan-provider execution itself is not deadline bounded.
"""
import json
import os
import socket
import struct
import time

from .crossfs_admission_candidate import CrossFsHeld
from .local_capacity_authority_candidate import LocalCapacityAuthority

LOCAL_CAPACITY_PROTOCOL_QUALIFIED=False
MAX_MESSAGE=4096


def _unique(items):
    result={}
    for key,value in items:
        if key in result:raise CrossFsHeld('duplicate request field')
        result[key]=value
    return result


def _read_exact(sock,length,deadline):
    out=bytearray()
    while len(out)<length:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise CrossFsHeld('local request deadline exceeded')
        sock.settimeout(remaining)
        chunk=sock.recv(length-len(out))
        if not chunk:raise CrossFsHeld('short local request')
        out.extend(chunk)
    return bytes(out)


class LocalCapacityProtocol:
    def __init__(self, authority, *, trusted_uid, plans):
        if LOCAL_CAPACITY_PROTOCOL_QUALIFIED is not True:
            raise CrossFsHeld('local capacity protocol not qualified')
        if (type(authority) is not LocalCapacityAuthority or type(trusted_uid) is not int
                or trusted_uid<0 or type(plans) is not dict or not plans):
            raise CrossFsHeld('fixed authority, peer UID and plans required')
        self.authority=authority
        self.trusted_uid=trusted_uid
        # Deep-copy into the service's private memory; no request supplies a plan.
        self.plans=json.loads(json.dumps(plans,sort_keys=True,allow_nan=False))
        if any(type(key) is not str or not key or type(value) is not dict
               or value.get('attempt_id')!=key for key,value in self.plans.items()):
            raise CrossFsHeld('bound server-side plans required')

    def serve_one(self, sock, *, timeout_seconds=2):
        if (type(sock) is not socket.socket or sock.family!=socket.AF_UNIX
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
            raise CrossFsHeld('bounded local socket required')
        try:
            raw=sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i'))
            pid,uid,gid=struct.unpack('3i',raw)
            if pid<=0 or uid!=self.trusted_uid:raise CrossFsHeld('untrusted local peer')
            deadline=time.monotonic()+timeout_seconds
            size=struct.unpack('!I',_read_exact(sock,4,deadline))[0]
            if not 0<size<=MAX_MESSAGE:raise CrossFsHeld('local request too large')
            request=json.loads(_read_exact(sock,size,deadline),object_pairs_hook=_unique,
                               parse_constant=lambda _: (_ for _ in ()).throw(CrossFsHeld('invalid request')))
            if (type(request) is not dict or set(request)!={'op','attemptId'}
                    or request['op'] not in ('reserve','pin','status')
                    or type(request['attemptId']) is not str or request['attemptId'] not in self.plans):
                raise CrossFsHeld('unknown fixed request')
            attempt=request['attemptId']
            if request['op'] in ('reserve','pin'):
                if request['op']=='pin':
                    self.authority._check()
                    with self.authority._gate._db() as db:
                        row=db.execute('SELECT state FROM attempts WHERE id=?',(attempt,)).fetchone()
                    if row is None or row[0]!='admitted':
                        raise CrossFsHeld('no admitted reservation to recheck')
                result=self.authority.acquire(**self.plans[attempt])
                response={'attemptId':attempt,'state':result['state']}
            else:
                self.authority._check()
                with self.authority._gate._db() as db:
                    row=db.execute('SELECT state FROM attempts WHERE id=?',(attempt,)).fetchone()
                self.authority._check()
                response={'attemptId':attempt,'state':row[0] if row else 'absent'}
            data=json.dumps(response,sort_keys=True,separators=(',',':')).encode()
            remaining=deadline-time.monotonic()
            if remaining<=0:raise CrossFsHeld('local request deadline exceeded')
            sock.settimeout(remaining)
            sock.sendall(struct.pack('!I',len(data))+data)
        except (OSError,ValueError,TypeError,KeyError,struct.error) as error:
            raise CrossFsHeld('local capacity request held') from error
