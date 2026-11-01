"""Inactive mTLS multi-host observation journal; not a transfer authority.

Only trusted fixed endpoints and plans. Sequential pin replies are not atomic;
physical disk and host liveness can change immediately. A qualifying distributed
lease/barrier, release/reconciliation, mount fence and actual remote deployment
are absent. Caller owns the exclusive coordinator state lock.
"""
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import ssl
import stat
import time

from ops.reference_worker_daemon import DaemonStateLock
from .crossfs_admission_candidate import CrossFsHeld
from .remote_capacity_session_candidate import request_capacity_session

MULTIHOST_CAPACITY_QUALIFIED=False


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


class PinnedCapacityConnector:
    """Fixed literal IP, CA/hostname-verified TLS, pinned certificate and ID."""
    def __init__(self, *, host, port, context, server_pin, authority_id):
        try:self.host=str(ipaddress.ip_address(host))
        except ValueError:raise CrossFsHeld('literal remote host required') from None
        if (type(port) is not int or not 1<=port<=65535 or not isinstance(context,ssl.SSLContext)
                or not context.check_hostname or context.verify_mode!=ssl.CERT_REQUIRED
                or type(server_pin) is not str or not re.fullmatch('[a-f0-9]{64}',server_pin)
                or type(authority_id) is not str or not re.fullmatch('[a-f0-9]{32}',authority_id)):
            raise CrossFsHeld('fixed pinned remote authority required')
        self.port,self.context,self.server_pin,self.authority_id=port,context,server_pin,authority_id

    def observe(self, *, operation, attempt, plan, timeout_seconds=4):
        if type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5:
            raise CrossFsHeld('bounded remote timeout required')
        deadline=time.monotonic()+timeout_seconds;raw=None;tls=None
        try:
            raw=socket.create_connection((self.host,self.port),timeout=max(.001,deadline-time.monotonic()))
            raw.settimeout(max(.001,deadline-time.monotonic()))
            tls=self.context.wrap_socket(raw,server_hostname=self.host)
            raw=None
            remaining=deadline-time.monotonic()
            if remaining<=0:raise CrossFsHeld('remote authority deadline exceeded')
            return request_capacity_session(tls,operation=operation,attempt=attempt,plan=plan,
                authority_id=self.authority_id,server_pin=self.server_pin,timeout_seconds=remaining)
        except (OSError,ValueError,ssl.SSLError) as error:
            raise CrossFsHeld('remote authority requires reconciliation') from error
        finally:
            if tls is not None:tls.close()
            elif raw is not None:raw.close()


class MultiHostCapacityJournal:
    def __init__(self, *, owner, journal, authorities):
        if MULTIHOST_CAPACITY_QUALIFIED is not True:raise CrossFsHeld('multi-host capacity not qualified')
        if type(owner) is not DaemonStateLock or type(authorities) is not dict or not authorities:
            raise CrossFsHeld('owned fixed authorities required')
        if any(type(k) is not str or not k or type(v) is not PinnedCapacityConnector for k,v in authorities.items()):
            raise CrossFsHeld('fixed capacity authorities required')
        if len({v.authority_id for v in authorities.values()})!=len(authorities) or len({v.server_pin for v in authorities.values()})!=len(authorities):
            raise CrossFsHeld('distinct host authority identities required')
        self.owner,self.root,self.authorities,self.pid=owner,owner.root,dict(authorities),os.getpid()
        self.owner.check(self.root);self.path=Path(journal).absolute()
        if self.path.parent!=self.root or self.path.resolve()!=self.path:raise CrossFsHeld('owned admission journal required')
        info=self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode&0o077))):raise CrossFsHeld('private admission journal required')
        self.identity=(info.st_dev,info.st_ino)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS multi_attempts (id TEXT PRIMARY KEY, plan TEXT NOT NULL, state TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS multi_receipts (attempt TEXT NOT NULL, host TEXT NOT NULL, receipt TEXT NOT NULL, PRIMARY KEY(attempt,host))')
            db.commit()

    def _check(self):
        if self.pid!=os.getpid():raise CrossFsHeld('coordinator process changed')
        self.owner.check(self.root);st=self.path.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or (st.st_dev,st.st_ino)!=self.identity:
            raise CrossFsHeld('admission journal changed')

    def _db(self):
        from contextlib import contextmanager
        @contextmanager
        def opened():
            self._check();db=sqlite3.connect(self.path,timeout=5,isolation_level=None)
            try:
                self._check();db.execute('PRAGMA synchronous=FULL');yield db
            finally:db.close()
        return opened()

    def observe(self, *, attempt_id, plans):
        if (type(attempt_id) is not str or not attempt_id or len(attempt_id)>128 or type(plans) is not dict
                or set(plans)!=set(self.authorities)):
            raise CrossFsHeld('exact multi-host plans required')
        # Freeze all caller-owned nested objects before journaling or requests.
        frozen=json.loads(_json(plans))
        if any(type(plan) is not dict or plan.get('attempt_id')!=attempt_id for plan in frozen.values()):
            raise CrossFsHeld('attempt plan changed')
        binding={name:dict(authorityId=connector.authority_id,serverPin=connector.server_pin,
                           host=connector.host,port=connector.port,plan=frozen[name])
                 for name,connector in self.authorities.items()}
        encoded=_json(binding)
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                old=db.execute('SELECT plan,state FROM multi_attempts WHERE id=?',(attempt_id,)).fetchone()
                if old and old[0]!=encoded:raise CrossFsHeld('multi-host attempt identity changed')
                if old is None:db.execute("INSERT INTO multi_attempts VALUES(?,?,'collecting')",(attempt_id,encoded))
                elif old[1] not in ('collecting','observed'):raise CrossFsHeld('multi-host journal changed')
                db.commit()
            except BaseException:db.rollback();raise
        def check_endpoint(name):
            connector=self.authorities[name];expected=binding[name]
            if (connector.host,connector.port,connector.server_pin,connector.authority_id)!=(expected['host'],expected['port'],expected['serverPin'],expected['authorityId']):
                raise CrossFsHeld('remote authority identity changed')
            return connector
        for name in sorted(self.authorities):
            connector=check_endpoint(name)
            with self._db() as db:row=db.execute('SELECT receipt FROM multi_receipts WHERE attempt=? AND host=?',(attempt_id,name)).fetchone()
            plan_sha=hashlib.sha256(_json(frozen[name]).encode()).hexdigest()
            if row is None:
                # Intent is durable before remote call. Unknown response repeats
                # the same exact per-host attempt and plan; never compensates.
                observed=connector.observe(operation='reserve',attempt=attempt_id,plan=frozen[name])
                check_endpoint(name)
                receipt=observed['receipt']
                if (receipt.get('attemptId')!=attempt_id or receipt.get('authorityId')!=connector.authority_id
                        or receipt.get('planSha256')!=plan_sha or receipt.get('state')!='admitted'
                        or type(receipt.get('holdSha256')) is not str or not re.fullmatch('[a-f0-9]{64}',receipt['holdSha256'])):
                    raise CrossFsHeld('remote receipt changed')
                with self._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    try:
                        db.execute('INSERT INTO multi_receipts VALUES(?,?,?)',(attempt_id,name,_json(receipt)));db.commit()
                    except BaseException:db.rollback();raise
            else:
                receipt=json.loads(row[0])
                if (receipt.get('attemptId')!=attempt_id or receipt.get('authorityId')!=connector.authority_id
                        or receipt.get('planSha256')!=plan_sha or receipt.get('state')!='admitted'
                        or type(receipt.get('holdSha256')) is not str or not re.fullmatch('[a-f0-9]{64}',receipt['holdSha256'])):
                    raise CrossFsHeld('stored remote receipt changed')
        for name in sorted(self.authorities):
            connector=check_endpoint(name)
            pin=connector.observe(operation='pin',attempt=attempt_id,plan=frozen[name])
            check_endpoint(name)
            with self._db() as db:
                row=db.execute('SELECT receipt FROM multi_receipts WHERE attempt=? AND host=?',(attempt_id,name)).fetchone()
            if row is None or pin['receipt']['holdSha256']!=json.loads(row[0]).get('holdSha256'):
                raise CrossFsHeld('remote held generation changed')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                if db.execute('SELECT COUNT(*) FROM multi_receipts WHERE attempt=?',(attempt_id,)).fetchone()[0]!=len(self.authorities):
                    raise CrossFsHeld('multi-host receipt count changed')
                db.execute("UPDATE multi_attempts SET state='observed' WHERE id=? AND plan=?",(attempt_id,encoded));db.commit()
            except BaseException:db.rollback();raise
        return {'attemptId':attempt_id,'state':'observed_only'}
