"""Disabled node-local NodeODM mutation broker; no listener or live wiring.

One signed coordinator epoch and one non-expiring NodeGate reservation are
bound to an exact physical node and task. A same-process Queue authority witness
is required; remote brokers are deliberately unsupported. Lost responses retain
started intents. This cooperative prototype does not fence local root, Docker
administrators, direct backend connections, or arbitrary code in this process.

Recovery prerequisite limitations: creation interrupted between journal and
manifest leaves a manual, fail-closed recovery case; there is no migration of
older journals. Inspection uses pathname-based SQLite and gate state reads,
which do not pin all parent/file paths against replacement or ABA. SQLite
read-only WAL access may create shared-memory files. Do not use this snapshot
as a no-follow guarantee, admission barrier, or deployable recovery protocol.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
import uuid
from types import SimpleNamespace
from contextlib import contextmanager

from .engine_ingress_guard import BackendPin, EngineIngressGuard, local_backend_identity, local_engine_identity
from .node_gate import NodeGate, canonical_gate_path
from .reference_protocol import decode_submission

QUALIFIED = False
_FIELDS = {'authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId',
           'environment','gateGeneration','operationId','operation','bodySha256','contentType'}
_DDL = {
    'binding': 'CREATE TABLE binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), digest TEXT NOT NULL)',
    'reservation': 'CREATE TABLE reservation (singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL)',
    'operations': 'CREATE TABLE operations (id TEXT PRIMARY KEY, operation TEXT NOT NULL UNIQUE, grant_hash TEXT NOT NULL, state TEXT NOT NULL, reply TEXT)',
}


class BrokerHeld(ValueError): pass


class LocalCoordinatorAuthority:
    """Exact same-process Queue owner; never a remote authority assertion."""
    def __init__(self, queue):
        from .prototype import Queue
        from .authority_lease import AuthorityLease
        if type(queue) is not Queue or queue._creator_pid != os.getpid() or queue._lock.closed:
            raise BrokerHeld('local coordinator owner required')
        lease=getattr(queue,'authority_lease',None)
        if type(lease) is not AuthorityLease or lease.authority!=queue.authority_id or lease._descriptor is None:
            raise BrokerHeld('installed authority lease required')
        self.queue=queue; self.pid=os.getpid();self.epoch=queue.startup_id;self.authority=queue.authority_id
        self.authority_lease=lease
        info=os.fstat(queue._lock.fileno());self.lock_identity=(info.st_dev,info.st_ino)
        with lease.held(time.monotonic()+2):self._check()

    def _check(self):
        queue=self.queue
        if (os.getpid()!=self.pid or queue._creator_pid!=self.pid or queue._lock.closed
                or queue.startup_id!=self.epoch or queue.authority_id!=self.authority
                or getattr(queue,'authority_lease',None) is not self.authority_lease
                or self.authority_lease.authority!=self.authority or self.authority_lease._descriptor is None):
            raise BrokerHeld('local coordinator authority unavailable')
        from .authority_lease import LeaseUnavailable
        try:self.authority_lease.check_held()
        except LeaseUnavailable:raise BrokerHeld('installed authority lease unavailable') from None
        try:
            held=os.fstat(queue._lock.fileno());name=(queue.path.parent/'coordinator.lock').lstat()
            if ((held.st_dev,held.st_ino)!=self.lock_identity or (name.st_dev,name.st_ino)!=self.lock_identity
                    or not stat.S_ISREG(name.st_mode) or name.st_nlink!=1):
                raise BrokerHeld('local coordinator lease changed')
        except OSError:raise BrokerHeld('local coordinator lease unavailable') from None

    @contextmanager
    def hold(self, grant, timeout, *, body=None, signature=None):
        from .prototype import VIEWER_PROFILE
        from .authority_lease import MARKER_DDL, LeaseUnavailable
        deadline=time.monotonic()+timeout
        if os.getpid()!=self.pid or not self.queue._lifecycle_lock.acquire(timeout=timeout):
            raise BrokerHeld('local coordinator owner unavailable')
        try:
            try:
                with self.authority_lease.held(deadline):
                    self._check()
                    if (grant['authorityId'],grant['coordinatorEpoch'])!=(self.authority,self.epoch):
                        raise BrokerHeld('local coordinator epoch differs')
                    with self.queue._db() as db:
                        db.execute('BEGIN')
                        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
                        if (schema is None or schema[0]!=MARKER_DDL or
                                db.execute('SELECT COUNT(*) FROM coordinator_authority_lease WHERE singleton=1 AND required=1').fetchone()[0]!=1):
                            raise BrokerHeld('authority lease marker unavailable')
                        job=db.execute('SELECT * FROM jobs WHERE id=?',(grant['jobId'],)).fetchone()
                        tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(grant['jobId'],)).fetchall()
                        active=db.execute("SELECT id FROM jobs WHERE status IN ('running','held')").fetchall()
                        if (job is None or len(tasks)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                                or len(active)!=1 or active[0]['id']!=grant['jobId']
                                or job['worker_id']!='viewer-dispatch' or job['node_id']!=grant['nodeId']
                                or job['environment']!=grant['environment'] or not job['claim_hash']
                                or not self.queue._transfer_admission_clear(db)):
                            raise BrokerHeld('local active coordinator claim required')
                        task=tasks[0];token=task['claim_token']
                        if (task['removed'] or task['stage']!='running' or task['phase']!='init_pending'
                                or task['engine_uuid']!=grant['engineTaskId'] or task['node_id']!=grant['nodeId']
                                or task['environment']!=grant['environment'] or type(token) is not str
                                or not 32<=len(token)<=128 or not token.isascii()
                                or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(token.encode()).hexdigest())):
                            raise BrokerHeld('local current task required')
                    # Queue.close uses an RLock and can otherwise be called
                    # reentrantly by the owning thread during the exchange.
                    self.queue._database_users+=1
                    try:
                        yield
                        self._check()
                    finally:self.queue._database_users-=1
            except LeaseUnavailable:raise BrokerHeld('installed authority lease unavailable') from None
        finally:self.queue._lifecycle_lock.release()

    def check(self):
        self._check()


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('ascii')


def _uuid(value):
    try: return type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError): return False


def sign_grant(grant, secret):
    if type(secret) is not bytes or len(secret) < 32: raise BrokerHeld('invalid signing key')
    return hmac.new(secret, b'ledgetop-node-mutation-v1\0' + _json(grant), hashlib.sha256).hexdigest()


def _reply(value, task):
    if (type(value) is not dict or value.get('uuid') != task
            or ('success' in value and value['success'] is not True) or 'error' in value
            or len(_json(value)) > 4096):
        raise BrokerHeld('backend response uncertain')
    return value


def issue_init_grant(queue, *, job_id, claim_token, engine_id, gate_generation,
                     operation_id, body, content_type, secret, _authority_required=False):
    """Bind a grant to the currently observed coordinator-owned Viewer row.

    The observation is not revocation or a durable authority fence: this
    experimental grant must not be used on an operational node yet.
    """
    from .prototype import Queue, VIEWER_PROFILE
    if QUALIFIED is not True or type(queue) is not Queue or queue._creator_pid != os.getpid() or queue._lock.closed:
        raise BrokerHeld('actual coordinator owner required')
    if (not _uuid(job_id) or not _uuid(operation_id) or type(body) is not bytes or len(body)>65536
            or type(claim_token) is not str or not 1<=len(claim_token)<=128
            or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
            or type(gate_generation) is not int or gate_generation < 1
            or type(content_type) is not str or not re.fullmatch(r'[ -~]{1,256}',content_type)):
        raise BrokerHeld('invalid grant request')
    with queue._db() as db:
        db.execute('BEGIN')
        if _authority_required:
            from .authority_lease import MARKER_DDL
            marker=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
            if (marker is None or marker['sql']!=MARKER_DDL or
                    db.execute('SELECT COUNT(*) FROM coordinator_authority_lease WHERE singleton=1 AND required=1').fetchone()[0]!=1):
                raise BrokerHeld('authority lease marker unavailable')
        job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchall()
        if (job is None or len(tasks)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                or job['worker_id']!='viewer-dispatch' or job['node_id'] not in ('cluster','main')
                or not job['claim_hash'] or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(claim_token.encode()).hexdigest())
                or not queue._transfer_admission_clear(db)):
            raise BrokerHeld('active coordinator claim required')
        task=tasks[0]
        if (task['removed'] or task['stage']!='running' or task['phase']!='init_pending'
                or task['node_id']!=job['node_id'] or task['environment']!=job['environment']
                or task['claim_token']!=claim_token or not _uuid(task['engine_uuid'])):
            raise BrokerHeld('exact Viewer initialization required')
        grant=dict(authorityId=queue.authority_id,nodeId=job['node_id'],engineId=engine_id,
                   coordinatorEpoch=queue.startup_id,jobId=job_id,engineTaskId=task['engine_uuid'],
                   environment=job['environment'],gateGeneration=gate_generation,operationId=operation_id,
                   operation='init',bodySha256=hashlib.sha256(body).hexdigest(),contentType=content_type)
        return grant,sign_grant(grant,secret)


class LoopbackNodeODM:
    """One fixed-loopback exchange bounded by a single monotonic deadline."""
    def __init__(self, engine_id, backend_pin, backend_token, *, deadline_seconds=10):
        if (type(backend_pin) is not BackendPin or type(backend_token) is not str
                or not backend_token.isascii() or any(ord(c)<33 or ord(c)>126 for c in backend_token)
                or type(deadline_seconds) not in (int,float) or not 0<deadline_seconds<=10):
            raise BrokerHeld('exact local backend required')
        self.engine_id, self.pin, self.token, self.deadline_seconds = engine_id, backend_pin, backend_token, deadline_seconds

    def exchange(self, grant, body):
        deadline = time.monotonic() + self.deadline_seconds
        if local_engine_identity(deadline) != self.engine_id: raise BrokerHeld('local engine changed')
        local_backend_identity(self.pin, deadline)
        route = '/task/new/init' if grant['operation'] == 'init' else '/task/new/commit/' + grant['engineTaskId']
        transport=SimpleNamespace(binding=SimpleNamespace(backend_port=self.pin.backend_port,backend_secret=self.token))
        status,raw=EngineIngressGuard._exchange(transport,'POST',route,body,
            {'content-type': grant['contentType'], 'set-uuid': grant['engineTaskId']},deadline,omit_empty_token=True)
        if status!=200 or len(raw)>4096:raise BrokerHeld('backend response uncertain')
        value = decode_submission(raw)
        return _reply(value,grant['engineTaskId'])


def inspect_prior_node_state(engine_id):
    """Cooperative, local snapshot only; never adopts an epoch or permits work.

    Requires the existing node gate and identity manifest. A busy, missing or
    inconsistent node is unknown. This is not an authenticated network endpoint,
    a continuing fence, or evidence against direct backend writers.
    """
    if os.name == 'nt':raise BrokerHeld('local inspection requires POSIX locking')
    import fcntl
    root=canonical_gate_path(engine_id); state_root=root/'mutation-broker'
    lockfd=None; manifestfd=None; db=None
    try:
        rootstat=root.lstat(); brokerstat=state_root.lstat()
        if (not stat.S_ISDIR(rootstat.st_mode) or not stat.S_ISDIR(brokerstat.st_mode)
                or root.resolve()!=root.absolute() or state_root.resolve()!=state_root.absolute()
                or rootstat.st_uid!=os.geteuid() or brokerstat.st_uid!=os.geteuid()
                or rootstat.st_mode&0o077 or brokerstat.st_mode&0o077):
            raise BrokerHeld('node state requires inspection')
        lockfd=os.open(root/'gate.lock',os.O_RDWR|os.O_NOFOLLOW)
        lock=os.fstat(lockfd)
        if not stat.S_ISREG(lock.st_mode) or lock.st_uid!=os.geteuid() or lock.st_mode&0o077 or lock.st_nlink!=1:
            raise BrokerHeld('node lock requires inspection')
        fcntl.flock(lockfd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        manifestfd=os.open(state_root/'identity.json',os.O_RDONLY|os.O_NOFOLLOW)
        info=os.fstat(manifestfd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_size>8192:
            raise BrokerHeld('node identity requires inspection')
        manifest=json.loads(os.read(manifestfd,8193))
        fields={'version','nodeId','engineId','authorityId','coordinatorOrigin','generation','coordinatorEpoch','gateRoot','gateLock','journal'}
        if (type(manifest) is not dict or set(manifest)!=fields or manifest['version']!=1
                or manifest['engineId']!=engine_id or not _uuid(manifest['authorityId'])
                or not _uuid(manifest['coordinatorEpoch']) or manifest['nodeId'] not in ('main','cluster')
                or type(manifest['generation']) is not int or manifest['generation']<1
                or manifest['gateRoot']!=[rootstat.st_dev,rootstat.st_ino]
                or manifest['gateLock']!=[lock.st_dev,lock.st_ino]):
            raise BrokerHeld('node identity changed')
        journal=state_root/'node-broker.sqlite3';js=journal.lstat()
        if (not stat.S_ISREG(js.st_mode) or js.st_nlink!=1 or js.st_uid!=os.geteuid() or js.st_mode&0o077
                or manifest['journal']!=[js.st_dev,js.st_ino]):raise BrokerHeld('node journal changed')
        gate=NodeGate(engine_id)
        # The existing lock is already held. Do not call NodeGate.__enter__,
        # which creates files and adjusts permissions.
        gate._stream=os.fdopen(os.dup(lockfd),'rb')
        try:state=gate.snapshot()
        finally:gate._stream.close();gate._stream=None
        for key in ('nodeId','engineId','authorityId','coordinatorOrigin','generation'):
            if state.get(key)!=manifest[key]:raise BrokerHeld('node gate identity changed')
        db=sqlite3.connect(journal.as_uri()+'?mode=ro',uri=True,timeout=2,isolation_level=None)
        db.row_factory=sqlite3.Row;db.execute('BEGIN')
        for name,ddl in _DDL.items():
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
            if row is None or row['sql']!=ddl:raise BrokerHeld('node journal schema changed')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone():raise BrokerHeld('node journal schema changed')
        binding=hashlib.sha256(_json([manifest[k] for k in ('nodeId','engineId','authorityId','coordinatorEpoch','coordinatorOrigin','generation')])).hexdigest()
        row=db.execute('SELECT digest FROM binding WHERE singleton=1').fetchone()
        if row is None or row['digest']!=binding:raise BrokerHeld('node journal binding changed')
        reservation=db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
        operations=[dict(row) for row in db.execute('SELECT id,operation,state FROM operations ORDER BY id')]
        if any(row['state'] not in ('started','complete') or row['operation'] not in ('init','commit') for row in operations):
            raise BrokerHeld('node operation state unknown')
        after=journal.lstat()
        if (after.st_dev,after.st_ino)!=(js.st_dev,js.st_ino):raise BrokerHeld('node journal changed')
        return {'identity':manifest,'gateReservation':state['reservation'],
                'journalReservation':json.loads(reservation['identity']) if reservation else None,
                'operations':operations,'authorizesAdmission':False}
    except BrokerHeld:raise
    except (OSError,ValueError,TypeError,KeyError,sqlite3.Error):
        raise BrokerHeld('node state unavailable') from None
    finally:
        if db is not None:db.close()
        if manifestfd is not None:os.close(manifestfd)
        if lockfd is not None:os.close(lockfd)


class NodeMutationBroker:
    def __init__(self, root, *, node_id, engine_id, authority_id, coordinator_epoch,
                 coordinator_origin, gate_generation, secret, adapter, authority_witness):
        if QUALIFIED is not True: raise BrokerHeld('node broker is not qualified')
        from .node_authority_session import RemoteAuthoritySession
        if (node_id not in ('cluster','main') or not _uuid(authority_id) or not _uuid(coordinator_epoch)
                or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
                or type(gate_generation) is not int or gate_generation < 1
                or type(secret) is not bytes or len(secret) < 32 or type(adapter) is not LoopbackNodeODM
                or adapter.engine_id != engine_id or type(authority_witness) not in (LocalCoordinatorAuthority,RemoteAuthoritySession)
                or (authority_witness.authority,authority_witness.epoch)!=(authority_id,coordinator_epoch)):
            raise BrokerHeld('invalid local broker binding')
        self.root=Path(root).absolute();self.node=node_id;self.engine=engine_id;self.authority=authority_id
        self.epoch=coordinator_epoch;self.origin=coordinator_origin;self.generation=gate_generation
        self.secret=secret;self.adapter=adapter;self.witness=authority_witness
        self._mutex=threading.Lock();self._pid=os.getpid();self._closed=False
        if (self.root != canonical_gate_path(engine_id) / 'mutation-broker'
                or not self.root.is_dir() or self.root.is_symlink() or self.root.resolve()!=self.root
                or self.root.stat().st_uid!=os.geteuid() or self.root.stat().st_mode&0o077):
            raise BrokerHeld('private existing node state required')
        self.path=self.root/'node-broker.sqlite3'
        if self.path.is_symlink(): raise BrokerHeld('invalid broker database')
        existed=self.path.exists()
        if existed:
            details=self.path.lstat()
            if (not stat.S_ISREG(details.st_mode) or details.st_nlink!=1
                    or details.st_uid!=os.geteuid() or details.st_mode&0o077):
                raise BrokerHeld('private broker database required')
        else:
            descriptor=os.open(self.path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW,0o600)
            try:os.fsync(descriptor)
            finally:os.close(descriptor)
            directory=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(directory)
            finally:os.close(directory)
        if existed:
            # Do not open a historical journal for writing until its existing
            # manifest matches this exact broker and node gate.
            with NodeGate(self.engine) as gate:self._identity_manifest(gate,False)
        self.db=sqlite3.connect(self.path, timeout=2, isolation_level=None, check_same_thread=False)
        root_info=self.root.stat();db_info=self.path.lstat()
        self._root_identity=(root_info.st_dev,root_info.st_ino)
        self._database_identity=(db_info.st_dev,db_info.st_ino)
        self.db.row_factory=sqlite3.Row
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('BEGIN IMMEDIATE')
        try:
            for name, ddl in _DDL.items():
                existing=self.db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
                if existing is None:
                    if existed:raise BrokerHeld('incomplete node broker schema')
                    self.db.execute(ddl)
                elif existing['sql']!=ddl:raise BrokerHeld('node broker schema changed')
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone():
                raise BrokerHeld('unexpected node broker trigger')
            binding=hashlib.sha256(_json([node_id,engine_id,authority_id,coordinator_epoch,coordinator_origin,gate_generation])).hexdigest()
            old=self.db.execute('SELECT digest FROM binding WHERE singleton=1').fetchone()
            if old is None:
                if existed:raise BrokerHeld('existing node broker binding missing')
                self.db.execute('INSERT INTO binding VALUES(1,?)',(binding,))
            elif old['digest'] != binding:raise BrokerHeld('node broker binding changed')
            self.db.commit()
        except BaseException:self.db.rollback();self.db.close();raise
        try:
            with NodeGate(self.engine) as gate:self._identity_manifest(gate,not existed)
        except BaseException:
            self.db.close()
            raise

    def _identity_manifest(self, gate, created):
        state=gate.snapshot()
        expected={'nodeId':self.node,'engineId':self.engine,'authorityId':self.authority,
                  'coordinatorOrigin':self.origin,'generation':self.generation}
        if any(state.get(k)!=v for k,v in expected.items()):
            raise BrokerHeld('node gate identity changed')
        root=gate.root.stat(); lock=os.fstat(gate._stream.fileno()); journal=self.path.lstat()
        manifest={'version':1,**expected,'coordinatorEpoch':self.epoch,
                  'gateRoot':[root.st_dev,root.st_ino], 'gateLock':[lock.st_dev,lock.st_ino],
                  'journal':[journal.st_dev,journal.st_ino]}
        path=self.root/'identity.json'; raw=_json(manifest)
        flags=os.O_NOFOLLOW | (os.O_WRONLY|os.O_CREAT|os.O_EXCL if created else os.O_RDONLY)
        try:
            fd=os.open(path,flags,0o600)
            try:
                info=os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode&0o077:
                    raise BrokerHeld('invalid node identity manifest')
                if created:
                    with os.fdopen(fd,'wb',closefd=False) as out:
                        out.write(raw);out.flush();os.fsync(fd)
                elif info.st_size>8192 or os.read(fd,8193)!=raw:
                    raise BrokerHeld('node identity manifest changed')
            finally:os.close(fd)
            if created:
                fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
                try:os.fsync(fd)
                finally:os.close(fd)
        except OSError:raise BrokerHeld('node identity manifest unavailable') from None

    def _check_owner(self):
        if self._closed or self._pid!=os.getpid():raise BrokerHeld('node broker owner changed')
        try:
            root=self.root.lstat();database=self.path.lstat()
            if (not stat.S_ISDIR(root.st_mode) or not stat.S_ISREG(database.st_mode)
                    or (root.st_dev,root.st_ino)!=self._root_identity
                    or (database.st_dev,database.st_ino)!=self._database_identity
                    or root.st_uid!=os.geteuid() or database.st_uid!=os.geteuid()
                    or root.st_mode&0o077 or database.st_mode&0o077 or database.st_nlink!=1):
                raise BrokerHeld('node broker state changed')
        except OSError:raise BrokerHeld('node broker state unavailable') from None

    def close(self):
        with self._mutex:
            if not self._closed:
                self._closed=True
                self.db.close()

    def mutate(self, grant, signature, body):
        if type(grant) is not dict or set(grant)!=_FIELDS or type(body) is not bytes or len(body)>65536:
            raise BrokerHeld('invalid bounded grant')
        if (any(type(grant[k]) is not str or not 1 <= len(grant[k]) <= 256 for k in _FIELDS-{'gateGeneration'})
                or type(grant['gateGeneration']) is not int or type(signature) is not str
                or not re.fullmatch('[a-f0-9]{64}',signature)):
            raise BrokerHeld('invalid bounded grant')
        if not hmac.compare_digest(sign_grant(grant,self.secret),signature):
            raise BrokerHeld('grant authentication failed')
        if (grant['nodeId']!=self.node or grant['engineId']!=self.engine or grant['authorityId']!=self.authority
                or grant['coordinatorEpoch']!=self.epoch or grant['gateGeneration']!=self.generation
                or any(not _uuid(grant[k]) for k in ('jobId','engineTaskId','operationId'))
                or grant['environment'] not in ('production','staging') or grant['operation'] not in ('init','commit')
                or type(grant['contentType']) is not str or not re.fullmatch(r'[ -~]{1,256}',grant['contentType'])
                or grant['bodySha256']!=hashlib.sha256(body).hexdigest()):raise BrokerHeld('grant binding changed')
        # Upload is not implemented; never commit an empty or partially uploaded
        # NodeODM task while that stage lacks a durable completion contract.
        if grant['operation']=='commit':raise BrokerHeld('upload stage not qualified')
        digest=hashlib.sha256(_json(grant)).hexdigest();identity=uuid.UUID(grant['jobId']).hex
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration')]).decode()
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        with self._mutex, self.witness.hold(grant,10,body=body,signature=signature), NodeGate(self.engine) as gate:
            self._check_owner()
            self._identity_manifest(gate,False)
            state=gate.snapshot();expected={'nodeId':self.node,'coordinatorOrigin':self.origin,'authorityId':self.authority,'generation':self.generation}
            if any(state[k]!=v for k,v in expected.items()):raise BrokerHeld('node ownership changed')
            reservation=state['reservation']
            if reservation is None:
                if grant['operation']!='init':raise BrokerHeld('initial reservation required')
                gate.reserve(self.node,self.origin,self.authority,self.generation,identity);gate.bind(identity,job)
            elif reservation['receiptId']!=identity or reservation['job']!=job or reservation['completionDigest'] is not None:
                raise BrokerHeld('another reservation holds node')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                stored=self.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
                if stored is None:
                    if grant['operation']!='init':raise BrokerHeld('initial reservation required')
                    self.db.execute('INSERT INTO reservation VALUES(1,?)',(durable_identity,))
                elif stored['identity']!=durable_identity:raise BrokerHeld('durable reservation changed')
                old=self.db.execute('SELECT * FROM operations WHERE id=?',(grant['operationId'],)).fetchone()
                if old:
                    if old['grant_hash']!=digest or old['state']!='complete':raise BrokerHeld('operation requires inspection')
                    reply=_reply(json.loads(old['reply']),grant['engineTaskId']);self.db.commit();return reply
                if self.db.execute("SELECT 1 FROM operations WHERE state!='complete' LIMIT 1").fetchone():
                    raise BrokerHeld('previous operation requires inspection')
                if grant['operation']=='commit' and not self.db.execute("SELECT 1 FROM operations WHERE operation='init' AND state='complete'").fetchone():
                    raise BrokerHeld('confirmed initialization required')
                if self.db.execute('SELECT 1 FROM operations WHERE operation=?',(grant['operation'],)).fetchone():
                    raise BrokerHeld('operation already exists')
                self.db.execute('INSERT INTO operations VALUES(?,?,?,?,NULL)',(grant['operationId'],grant['operation'],digest,'started'));self.db.commit()
            except BaseException:self.db.rollback();raise
            reply=_reply(self.adapter.exchange(grant,body),grant['engineTaskId'])
            self._check_owner()
            self._identity_manifest(gate,False)
            self.witness.check()
            raw=_json(reply)
            if len(raw)>4096:raise BrokerHeld('backend response exceeds bound')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                changed=self.db.execute("UPDATE operations SET state='complete',reply=? WHERE id=? AND grant_hash=? AND state='started'",(raw.decode(),grant['operationId'],digest)).rowcount
                if changed!=1:raise BrokerHeld('operation identity changed')
                self.db.commit()
            except BaseException:self.db.rollback();raise
            return reply
