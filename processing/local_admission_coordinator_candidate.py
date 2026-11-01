"""Disabled local-only coordinator intent/receipt experiment.

No listener, real worker hook, mTLS, remote NAS, or physical space reservation.
SO_PEERCRED authenticates a local UID, not an individual process or remote host.
A recorded receipt is historical evidence, never an ongoing capacity guarantee.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import stat
import struct
import time

from ops.reference_worker_daemon import DaemonStateLock
from .crossfs_admission_candidate import CrossFsHeld
from .local_capacity_protocol_candidate import MAX_MESSAGE, _read_exact, _unique

LOCAL_CAPACITY_COORDINATOR_QUALIFIED=False


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


class UnixCapacityConnector:
    """Bounded local connector; same-UID is not host/process authentication."""
    def __init__(self, path, expected_uid):
        self.path=Path(path).absolute()
        if self.path.resolve()!=self.path or type(expected_uid) is not int or expected_uid<0:
            raise CrossFsHeld('fixed local socket path required')
        self.uid=expected_uid

    def open(self, deadline):
        info=self.path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or (os.name!='nt' and info.st_uid!=self.uid):
            raise CrossFsHeld('local socket identity refused')
        remaining=deadline-time.monotonic()
        if remaining<=0:raise CrossFsHeld('local connection deadline exceeded')
        sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        try:
            sock.settimeout(remaining);sock.connect(str(self.path))
            after=self.path.lstat()
            if (info.st_dev,info.st_ino)!=(after.st_dev,after.st_ino):raise CrossFsHeld('local socket changed')
            pid,uid,gid=struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
            if pid<=0 or uid!=self.uid:raise CrossFsHeld('local authority peer refused')
            return sock
        except BaseException:
            sock.close();raise


class LocalAdmissionCoordinator:
    def __init__(self, *, owner, journal, expected_uid, expected_authority):
        if LOCAL_CAPACITY_COORDINATOR_QUALIFIED is not True:
            raise CrossFsHeld('local admission coordinator not qualified')
        if (type(owner) is not DaemonStateLock or type(expected_uid) is not int or expected_uid<0
                or type(expected_authority) is not str or not re.fullmatch('[a-f0-9]{32}',expected_authority)):
            raise CrossFsHeld('fixed local authority required')
        self.owner=owner;self.root=owner.root;self.owner.check(self.root)
        self.path=Path(journal).absolute()
        if self.path.parent!=self.root or self.path.resolve()!=self.path:
            raise CrossFsHeld('owned coordinator journal required')
        info=self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
            raise CrossFsHeld('private coordinator journal required')
        self._identity=(info.st_dev,info.st_ino)
        self.uid=expected_uid;self.authority=expected_authority;self.pid=os.getpid()
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS local_admission (attempt TEXT PRIMARY KEY, authority TEXT NOT NULL, plan_sha TEXT NOT NULL, state TEXT NOT NULL, receipt TEXT)')
            db.commit()

    def _check(self):
        if self.pid!=os.getpid():raise CrossFsHeld('coordinator process changed')
        self.owner.check(self.root)
        st=self.path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or (st.st_dev,st.st_ino)!=self._identity:
            raise CrossFsHeld('coordinator journal changed')

    def _db(self):
        from contextlib import contextmanager
        @contextmanager
        def open_db():
            self._check()
            db=sqlite3.connect(self.path,timeout=5,isolation_level=None)
            try:
                self._check();db.execute('PRAGMA synchronous=FULL')
                yield db
            finally:db.close()
        return open_db()

    def reserve(self, *, attempt_id, expected_plan, connector, timeout_seconds=3):
        if (type(attempt_id) is not str or not attempt_id or len(attempt_id)>128
                or type(expected_plan) is not dict or expected_plan.get('attempt_id')!=attempt_id
                or type(connector) is not UnixCapacityConnector or connector.uid!=self.uid or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
            raise CrossFsHeld('fixed admission request required')
        plan_sha=hashlib.sha256(_canonical(expected_plan)).hexdigest()
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                row=db.execute('SELECT authority,plan_sha,state,receipt FROM local_admission WHERE attempt=?',(attempt_id,)).fetchone()
                if row and row[:2]!=(self.authority,plan_sha):raise CrossFsHeld('admission identity changed')
                if row is None:
                    db.execute("INSERT INTO local_admission VALUES(?,?,?,'intent',NULL)",(attempt_id,self.authority,plan_sha))
                elif row[2]=='receipt':
                    expected={'attemptId':attempt_id,'state':'admitted','authorityId':self.authority,'planSha256':plan_sha}
                    if row[3]!=_canonical(expected).decode():raise CrossFsHeld('admission receipt changed')
                    db.commit();return {'state':'receipt_recorded','receipt':expected}
                elif row[2]!='intent' or row[3] is not None:raise CrossFsHeld('admission journal changed')
                db.commit()
            except BaseException:db.rollback();raise
        # Any connection loss leaves the same durable intent for exact replay.
        expected=self._request('reserve',attempt_id,plan_sha,connector,timeout_seconds)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                self._check()
                current=db.execute('SELECT authority,plan_sha,state,receipt FROM local_admission WHERE attempt=?',(attempt_id,)).fetchone()
                if current!=(self.authority,plan_sha,'intent',None):raise CrossFsHeld('admission intent changed')
                db.execute("UPDATE local_admission SET state='receipt',receipt=? WHERE attempt=?",(_canonical(expected).decode(),attempt_id))
                db.commit()
            except BaseException:db.rollback();raise
        return {'state':'receipt_recorded','receipt':expected}

    def _request(self, operation, attempt_id, plan_sha, connector, timeout_seconds):
        deadline=time.monotonic()+timeout_seconds
        sock=None
        try:
            sock=connector.open(deadline)
            remaining=deadline-time.monotonic()
            if remaining<=0:raise CrossFsHeld('local connection deadline exceeded')
            request=_canonical({'op':operation,'attemptId':attempt_id})
            sock.settimeout(remaining);sock.sendall(struct.pack('!I',len(request))+request)
            size=struct.unpack('!I',_read_exact(sock,4,deadline))[0]
            if not 0<size<=MAX_MESSAGE:raise CrossFsHeld('authority reply size changed')
            result=json.loads(_read_exact(sock,size,deadline),object_pairs_hook=_unique)
            expected={'attemptId':attempt_id,'state':'admitted','authorityId':self.authority,'planSha256':plan_sha}
            if result!=expected:raise CrossFsHeld('authority receipt changed')
            return expected
        except (OSError,ValueError,TypeError,struct.error) as error:
            raise CrossFsHeld('local admission requires reconciliation') from error
        finally:
            if sock is not None:sock.close()

    def observe_pin(self, *, attempt_id, expected_plan, connector, timeout_seconds=3):
        """Fresh peer observation only; not an authority to transfer."""
        if (type(expected_plan) is not dict or expected_plan.get('attempt_id')!=attempt_id
                or type(connector) is not UnixCapacityConnector or connector.uid!=self.uid
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
            raise CrossFsHeld('fixed pin request required')
        plan_sha=hashlib.sha256(_canonical(expected_plan)).hexdigest()
        with self._db() as db:
            row=db.execute('SELECT authority,plan_sha,state,receipt FROM local_admission WHERE attempt=?',(attempt_id,)).fetchone()
        expected={'attemptId':attempt_id,'state':'admitted','authorityId':self.authority,'planSha256':plan_sha}
        if row!=(self.authority,plan_sha,'receipt',_canonical(expected).decode()):
            raise CrossFsHeld('matching recorded receipt required')
        self._request('pin',attempt_id,plan_sha,connector,timeout_seconds)
        return {'state':'pin_observed','receipt':expected}
