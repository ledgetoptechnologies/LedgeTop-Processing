"""Disabled node-local NodeODM mutation broker; no listener or live wiring.

One signed coordinator epoch and one non-expiring NodeGate reservation are
bound to an exact physical node and task. A same-process Queue authority witness
is required; remote brokers are deliberately unsupported. Lost responses retain
started intents. This cooperative prototype does not fence local root, Docker
administrators, direct backend connections, or arbitrary code in this process.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import stat
import threading
import time
import uuid
from types import SimpleNamespace
from contextlib import contextmanager

from .engine_ingress_guard import (BackendPin, EngineIngressGuard, MAX_STREAM_BYTES, _file_multipart,
                                   local_backend_identity, local_engine_identity)
from .node_gate import GateError, NodeGate, canonical_gate_path
from .reference_protocol import decode_submission

QUALIFIED = False
_FIELDS = {'authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId',
           'environment','gateGeneration','taskGeneration','operationId','operation','bodySha256','contentType'}
_UPLOAD_FIELDS = _FIELDS | {'fileName','fileBytes'}
_ISSUER_DDL = ('CREATE TABLE node_broker_upload_issues ('
    'operation_id TEXT PRIMARY KEY, task_uuid TEXT NOT NULL, generation INTEGER NOT NULL, '
    'file_key TEXT NOT NULL, file_path TEXT NOT NULL, root_path TEXT NOT NULL, root_dev INTEGER NOT NULL, '
    'root_ino INTEGER NOT NULL, task_dir TEXT NOT NULL, task_dev INTEGER NOT NULL, task_ino INTEGER NOT NULL, '
    'grant_hash TEXT NOT NULL, UNIQUE(task_uuid,generation,file_key))')
_DDL = {
    'binding': 'CREATE TABLE binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), digest TEXT NOT NULL)',
    'reservation': 'CREATE TABLE reservation (singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL)',
    'operations': 'CREATE TABLE operations (id TEXT PRIMARY KEY, operation TEXT NOT NULL, file_key TEXT NOT NULL, grant_hash TEXT NOT NULL, state TEXT NOT NULL, reply TEXT, UNIQUE(operation,file_key))',
    'node_grants': 'CREATE TABLE node_grants (operation_id TEXT PRIMARY KEY NOT NULL, grant_json TEXT NOT NULL, signature TEXT NOT NULL)',
    'engine_observations': 'CREATE TABLE engine_observations (operation_id TEXT PRIMARY KEY NOT NULL, grant_hash TEXT NOT NULL, reply_json TEXT NOT NULL)',
}
_NODE_TRIGGERS = {
    'node_grant_insert_duplicate': "CREATE TRIGGER node_grant_insert_duplicate BEFORE INSERT ON node_grants WHEN EXISTS (SELECT 1 FROM node_grants WHERE operation_id=NEW.operation_id) BEGIN SELECT RAISE(ABORT,'node grant evidence exists'); END",
    'engine_observation_insert_duplicate': "CREATE TRIGGER engine_observation_insert_duplicate BEFORE INSERT ON engine_observations WHEN EXISTS (SELECT 1 FROM engine_observations WHERE operation_id=NEW.operation_id) BEGIN SELECT RAISE(ABORT,'engine observation exists'); END",
    'node_grant_update': "CREATE TRIGGER node_grant_update BEFORE UPDATE ON node_grants BEGIN SELECT RAISE(ABORT,'node grant evidence immutable'); END",
    'node_grant_delete': "CREATE TRIGGER node_grant_delete BEFORE DELETE ON node_grants BEGIN SELECT RAISE(ABORT,'node grant evidence immutable'); END",
    'engine_observation_update': "CREATE TRIGGER engine_observation_update BEFORE UPDATE ON engine_observations BEGIN SELECT RAISE(ABORT,'engine observation immutable'); END",
    'engine_observation_delete': "CREATE TRIGGER engine_observation_delete BEFORE DELETE ON engine_observations BEGIN SELECT RAISE(ABORT,'engine observation immutable'); END",
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
    def hold(self, grant, timeout, *, create=False, body=None, signature=None):
        from .prototype import VIEWER_PROFILE
        from .authority_lease import MARKER_DDL, LeaseUnavailable
        deadline=time.monotonic()+timeout
        if os.getpid()!=self.pid:
            raise BrokerHeld('local coordinator owner unavailable')
        with self.queue._lifecycle_lock:
            if self.queue._lock.closed: raise BrokerHeld('local coordinator owner unavailable')
            self.queue._database_users+=1
        try:
            try:
                with self.authority_lease.held(deadline):
                    self._check()
                    if (grant['authorityId'],grant['coordinatorEpoch'])!=(self.authority,self.epoch):
                        raise BrokerHeld('local coordinator epoch differs')
                    with self.queue._db() as db:
                        db.execute('BEGIN IMMEDIATE')
                        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
                        if (schema is None or schema[0]!=MARKER_DDL or
                                db.execute('SELECT COUNT(*) FROM coordinator_authority_lease WHERE singleton=1 AND required=1').fetchone()[0]!=1):
                            raise BrokerHeld('authority lease marker unavailable')
                        from .broker_intent_fence import attest, IntentFenceError
                        try: attest(db)
                        except IntentFenceError: raise BrokerHeld('broker intent fence unavailable') from None
                        digest=hashlib.sha256(_json(grant)).hexdigest()
                        allowed=(grant['operationId'],grant['jobId'],digest)
                        job=db.execute('SELECT * FROM jobs WHERE id=?',(grant['jobId'],)).fetchone()
                        tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(grant['jobId'],)).fetchall()
                        active=db.execute("SELECT id FROM jobs WHERE status IN ('running','held')").fetchall()
                        if (job is None or len(tasks)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                                or len(active)!=1 or active[0]['id']!=grant['jobId']
                                or job['worker_id']!='viewer-dispatch' or job['node_id']!=grant['nodeId']
                                or job['environment']!=grant['environment'] or not job['claim_hash']
                                or not self.queue._transfer_admission_clear(db,broker_allowed=allowed)):
                            raise BrokerHeld('local active coordinator claim required')
                        task=tasks[0];token=task['claim_token']
                        expected_phase={'init':'init_pending','upload':'upload_pending'}.get(grant.get('operation'))
                        if (expected_phase is None or task['removed'] or task['stage']!='running' or task['phase']!=expected_phase
                                or task['generation']!=grant.get('taskGeneration')
                                or task['engine_uuid']!=grant['engineTaskId'] or task['node_id']!=grant['nodeId']
                                or task['environment']!=grant['environment'] or type(token) is not str
                                or not 32<=len(token)<=128 or not token.isascii()
                                or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(token.encode()).hexdigest())):
                            raise BrokerHeld('local current task required')
                        if create:
                            # Preserve the exact signed public grant atomically
                            # with the started intent, before node forwarding.
                            # This is evidence only, never authority to release.
                            raw=_json(grant)
                            if len(raw)>16384 or type(signature) is not str or not re.fullmatch('[a-f0-9]{64}',signature):
                                raise BrokerHeld('signed broker grant evidence required')
                            evidence=db.execute('SELECT grant_json,signature FROM broker_grant_evidence WHERE operation_id=?',(grant['operationId'],)).fetchone()
                            if evidence is None:
                                if db.execute('SELECT 1 FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone():
                                    raise BrokerHeld('prior broker grant evidence missing')
                                db.execute('INSERT INTO broker_grant_evidence VALUES(?,?,?)',(grant['operationId'],raw.decode('ascii'),signature))
                            elif tuple(evidence)!=(raw.decode('ascii'),signature):
                                raise BrokerHeld('broker grant evidence changed')
                            row=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
                            if row is None:
                                db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(grant['operationId'],grant['jobId'],digest))
                            elif tuple(row)==(grant['jobId'],digest,'complete'):
                                db.execute("UPDATE broker_intents SET state='started' WHERE operation_id=? AND state='complete'",(grant['operationId'],))
                            elif tuple(row)!=(grant['jobId'],digest,'started'):
                                raise BrokerHeld('broker intent identity changed')
                    yield
                    self._check()
            except LeaseUnavailable:raise BrokerHeld('installed authority lease unavailable') from None
        finally:
            with self.queue._lifecycle_lock:
                self.queue._database_users-=1

    def completed(self, grant):
        from .broker_intent_fence import attest
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');attest(db)
            changed=db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=? AND job_id=? AND grant_hash=? AND state='started'",(grant['operationId'],grant['jobId'],digest)).rowcount
            if changed!=1:
                row=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
                if row is None or tuple(row)!=(grant['jobId'],digest,'complete'):
                    raise BrokerHeld('broker intent completion changed')


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('ascii')


def _uuid(value):
    try: return type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError): return False


def sign_grant(grant, secret):
    if type(secret) is not bytes or len(secret) < 32: raise BrokerHeld('invalid signing key')
    return hmac.new(secret, b'ledgetop-node-mutation-v1\0' + _json(grant), hashlib.sha256).hexdigest()


def verify_grant_evidence(grant_json, signature, secret):
    """Authenticate canonical public grant bytes; never authorize recovery.

    Callers must separately qualify the signing key, complete node/coordinator
    identities, epoch, reservation and engine state. No claim token is needed.
    """
    if (type(grant_json) is not str or not grant_json.isascii() or len(grant_json)>16384
            or type(signature) is not str or not re.fullmatch('[a-f0-9]{64}',signature)):
        raise BrokerHeld('invalid signed grant evidence')
    def unique(pairs):
        output={}
        for key,value in pairs:
            if key in output:raise ValueError('duplicate grant field')
            output[key]=value
        return output
    try:
        grant=json.loads(grant_json,object_pairs_hook=unique,
                         parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if type(grant) is not dict:raise ValueError()
        operation=grant.get('operation')
        expected=_UPLOAD_FIELDS if operation=='upload' else _FIELDS
        if (operation not in ('init','upload','commit') or set(grant)!=expected
                or not _uuid(grant.get('operationId')) or _json(grant).decode('ascii')!=grant_json
                or not hmac.compare_digest(sign_grant(grant,secret),signature)):
            raise ValueError()
        return grant
    except (ValueError,TypeError,AttributeError,RecursionError,BrokerHeld):
        raise BrokerHeld('signed grant evidence authentication failed') from None


def inspect_coordinator_held_grant(queue, witness, operation_id, secret, *, timeout_seconds=3):
    """Read exact authenticated old-epoch evidence under the new local lease.

    This is an inactive coordinator-side diagnostic. It does not contact a
    node, inspect an engine, prove exclusive access, adopt an epoch or release
    work. The caller must keep signing keys private and qualify them separately.
    """
    from .prototype import Queue, VIEWER_PROFILE
    from .authority_lease import MARKER_DDL, LeaseUnavailable
    from .broker_intent_fence import attest, IntentFenceError
    if (QUALIFIED is not True or type(queue) is not Queue or type(witness) is not LocalCoordinatorAuthority
            or witness.queue is not queue or not _uuid(operation_id)
            or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
        raise BrokerHeld('coordinator inspection unavailable')
    deadline=time.monotonic()+timeout_seconds
    try:
        with queue.pin_transfer(deadline),witness.authority_lease.held(deadline):
            witness._check()
            with queue._db() as db:
                db.execute('BEGIN')
                marker=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='coordinator_authority_lease'").fetchone()
                if (marker is None or marker['sql']!=MARKER_DDL or
                        db.execute('SELECT COUNT(*) FROM coordinator_authority_lease WHERE singleton=1 AND required=1').fetchone()[0]!=1):
                    raise BrokerHeld('authority marker unavailable')
                try:attest(db)
                except IntentFenceError:raise BrokerHeld('broker intent fence unavailable') from None
                evidence=db.execute('SELECT grant_json,signature FROM broker_grant_evidence WHERE operation_id=?',(operation_id,)).fetchone()
                intent=db.execute('SELECT job_id,grant_hash,state FROM broker_intents WHERE operation_id=?',(operation_id,)).fetchone()
                if evidence is None or intent is None:raise BrokerHeld('held grant evidence unavailable')
                grant=verify_grant_evidence(evidence['grant_json'],evidence['signature'],secret)
                if (grant['operationId']!=operation_id or grant['authorityId']!=queue.authority_id
                        or grant['coordinatorEpoch']==queue.startup_id or not _uuid(grant['coordinatorEpoch'])
                        or intent['job_id']!=grant['jobId'] or intent['state']!='started'
                        or intent['grant_hash']!=hashlib.sha256(_json(grant)).hexdigest()):
                    raise BrokerHeld('held grant identity differs')
                job=db.execute('SELECT * FROM jobs WHERE id=?',(grant['jobId'],)).fetchone()
                tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(grant['jobId'],)).fetchall()
                if (job is None or len(tasks)!=1 or job['status']!='held' or job['held_reason']!='coordinator_restart'
                        or job['profile']!=VIEWER_PROFILE or job['node_id']!=grant['nodeId']
                        or job['environment']!=grant['environment'] or job['worker_id']!='viewer-dispatch'):
                    raise BrokerHeld('held job identity differs')
                task=tasks[0]
                if (task['engine_uuid']!=grant['engineTaskId']
                        or task['node_id']!=grant['nodeId'] or task['environment']!=grant['environment']
                        or task['generation']!=grant['taskGeneration']):
                    raise BrokerHeld('held task identity differs')
                token=task['claim_token']
                if (type(token) is not str or not token.isascii() or not 32<=len(token)<=128
                        or type(job['claim_hash']) is not str
                        or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(token.encode()).hexdigest())):
                    raise BrokerHeld('held task claim differs')
                witness._check()
                return {'grant':grant,'intentState':'started','jobState':'held',
                        'nodeState':'unverified','engineState':'unverified','authorizesRelease':False}
    except LeaseUnavailable:raise BrokerHeld('coordinator lease unavailable') from None


def match_coordinator_node_evidence(queue, witness, operation_id, envelope, nonce, secret):
    """Match a nonce-bound shared-key report to actual held coordinator state.

    This inactive diagnostic does not authenticate a physical node, establish
    a continuously held gate, check engine task state or authorize release.
    It is not a network protocol. The caller must generate a fresh nonce.
    """
    if (type(nonce) is not str or not re.fullmatch('[a-f0-9]{64}',nonce)
            or type(envelope) is not dict or set(envelope)!={'report','mac'}
            or type(envelope['report']) is not dict or type(envelope['mac']) is not str
            or not re.fullmatch('[a-f0-9]{64}',envelope['mac'])
            or type(secret) is not bytes or len(secret)<32):
        raise BrokerHeld('node evidence unavailable')
    report=envelope['report']
    fields={'nonce','grant','grantSignature','operationState','reservationId','observation','engineTaskState','engineTaskObservation','nodeOrigin'}
    if set(report)!=fields or report['nonce']!=nonce:raise BrokerHeld('node evidence differs')
    try:
        raw_report=_json(report)
        if len(raw_report)>32768:raise ValueError('oversized report')
        mac=hmac.new(secret,b'ledgetop-node-held-evidence-v1\0'+raw_report,hashlib.sha256).hexdigest()
    except (ValueError,TypeError,RecursionError):raise BrokerHeld('node evidence differs') from None
    if not hmac.compare_digest(mac,envelope['mac']):raise BrokerHeld('node evidence authentication failed')
    coordinator=inspect_coordinator_held_grant(queue,witness,operation_id,secret)
    grant=verify_grant_evidence(_json(report['grant']).decode('ascii'),report['grantSignature'],secret)
    if (grant!=coordinator['grant'] or report['operationState']!='started'
            or report['engineTaskState']!='unverified' or report['nodeOrigin']!='shared-key-only'
            or report['reservationId']!=uuid.UUID(grant['jobId']).hex):
        raise BrokerHeld('node and coordinator evidence differ')
    task=report['engineTaskObservation']
    if (type(task) is not dict or set(task)!={'uuid','statusCode'} or task['uuid']!=grant['engineTaskId']
            or type(task['statusCode']) is not int or task['statusCode'] not in (10,20,30,40,50)):
        raise BrokerHeld('node task observation differs')
    observation=report['observation']
    if observation is not None:
        if grant['operation']!='init':raise BrokerHeld('unsupported node observation')
        _reply(observation,grant['engineTaskId'])
    return {'operationId':operation_id,'coordinatorState':'held','nodeState':'reported-held',
            'engineTaskState':'unverified','engineTaskObservation':task,
            'nodeIdentity':'not-independently-authenticated',
            'observationPresent':observation is not None,'authorizesRelease':False}


def _sender_confirmation(grant, secret):
    return hmac.new(secret, b'ledgetop-node-upload-verified-v1\0' + _json(grant), hashlib.sha256).hexdigest()


def _reply(value, task):
    if (type(value) is not dict or value.get('uuid') != task
            or ('success' in value and value['success'] is not True) or 'error' in value
            or len(_json(value)) > 4096):
        raise BrokerHeld('backend response uncertain')
    return value


def _upload_reply(value):
    if (type(value) is not dict or value.get('success') is not True or 'error' in value
            or len(_json(value)) > 4096):
        raise BrokerHeld('backend upload response uncertain')
    return value


def issue_init_grant(queue, *, job_id, claim_token, engine_id, gate_generation,
                     operation_id, body, content_type, secret, _authority_required=False, _broker_allowed=None):
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
            if (marker is None or marker[0]!=MARKER_DDL or db.execute('SELECT COUNT(*) FROM coordinator_authority_lease WHERE singleton=1 AND required=1').fetchone()[0]!=1):
                raise BrokerHeld('authority lease marker unavailable')
        job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchall()
        if (job is None or len(tasks)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                or job['worker_id']!='viewer-dispatch' or job['node_id'] not in ('cluster','main')
                or not job['claim_hash'] or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(claim_token.encode()).hexdigest())
                or not queue._transfer_admission_clear(db,broker_allowed=_broker_allowed)):
            raise BrokerHeld('active coordinator claim required')
        task=tasks[0]
        if (task['removed'] or task['stage']!='running' or task['phase']!='init_pending'
                or task['node_id']!=job['node_id'] or task['environment']!=job['environment']
                or task['claim_token']!=claim_token or not _uuid(task['engine_uuid'])):
            raise BrokerHeld('exact Viewer initialization required')
        grant=dict(authorityId=queue.authority_id,nodeId=job['node_id'],engineId=engine_id,
                   coordinatorEpoch=queue.startup_id,jobId=job_id,engineTaskId=task['engine_uuid'],
                   environment=job['environment'],gateGeneration=gate_generation,taskGeneration=task['generation'],operationId=operation_id,
                   operation='init',bodySha256=hashlib.sha256(body).hexdigest(),contentType=content_type)
        return grant,sign_grant(grant,secret)


def install_upload_issuer_schema(queue):
    """Explicit offline preparation on an idle, exclusively owned Queue."""
    from .prototype import Queue
    if QUALIFIED is not True or type(queue) is not Queue or queue._creator_pid!=os.getpid() or queue._lock.closed:
        raise BrokerHeld('actual coordinator owner required')
    with queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():
            raise BrokerHeld('idle coordinator required')
        old=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='node_broker_upload_issues'").fetchone()
        if old is None:db.execute(_ISSUER_DDL)
        elif old['sql']!=_ISSUER_DDL:raise BrokerHeld('upload issuer schema changed')


def _source_scope(store, row):
    try:
        root=store.root;directory=store._directory(row)
        first=root.lstat();second=directory.lstat()
        if (not stat.S_ISDIR(first.st_mode) or not stat.S_ISDIR(second.st_mode)
                or first.st_uid!=os.geteuid() or second.st_uid!=os.geteuid()
                or first.st_mode&0o077 or second.st_mode&0o077):raise ValueError()
        return (str(root),first.st_dev,first.st_ino,row['directory'],second.st_dev,second.st_ino)
    except (OSError,ValueError):raise BrokerHeld('source directory requires inspection') from None


def issue_upload_grant(queue, *, store, job_id, claim_token, engine_id, gate_generation,
                       operation_id, file_key, secret):
    """Record and sign exact input metadata from the active Queue transaction."""
    from .prototype import Queue, VIEWER_PROFILE
    from .viewer_tasks import TaskStore
    if (QUALIFIED is not True or type(queue) is not Queue or type(store) is not TaskStore or store.queue is not queue
            or queue._creator_pid!=os.getpid() or queue._lock.closed):
        raise BrokerHeld('actual coordinator owner required')
    if (not _uuid(job_id) or not _uuid(operation_id) or type(claim_token) is not str or not 1<=len(claim_token)<=128
            or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
            or type(gate_generation) is not int or gate_generation<1 or type(file_key) is not str
            or TaskStore._filename(file_key)!=file_key):raise BrokerHeld('invalid upload request')
    with store._lock, queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='node_broker_upload_issues'").fetchone()
        if schema is None or schema['sql']!=_ISSUER_DDL:raise BrokerHeld('offline upload issuer migration required')
        job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        tasks=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(job_id,)).fetchall()
        if (job is None or len(tasks)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                or job['worker_id']!='viewer-dispatch' or job['node_id'] not in ('cluster','main')
                or not job['claim_hash'] or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(claim_token.encode()).hexdigest())
                or not queue._transfer_admission_clear(db)):raise BrokerHeld('active coordinator claim required')
        task=tasks[0]
        file=db.execute('SELECT * FROM viewer_files WHERE uuid=? AND name_key=?',(task['uuid'],file_key)).fetchone()
        if (task['removed'] or task['stage']!='running' or task['phase']!='upload_pending'
                or task['node_id']!=job['node_id'] or task['environment']!=job['environment']
                or task['claim_token']!=claim_token or not _uuid(task['engine_uuid']) or file is None
                or TaskStore._filename(file['name'])!=file_key or file['is_photo']!=1 or type(file['bytes']) is not int
                or type(file['path']) is not str or not re.fullmatch('input-[0-9a-f]{32}',file['path'])
                or not 0<file['bytes']<=MAX_STREAM_BYTES-4096 or type(file['sha256']) is not str
                or not re.fullmatch('[a-f0-9]{64}',file['sha256'])):raise BrokerHeld('exact stored upload required')
        grant=dict(authorityId=queue.authority_id,nodeId=job['node_id'],engineId=engine_id,
                   coordinatorEpoch=queue.startup_id,jobId=job_id,engineTaskId=task['engine_uuid'],
                   environment=job['environment'],gateGeneration=gate_generation,taskGeneration=task['generation'],operationId=operation_id,
                   operation='upload',bodySha256=file['sha256'],contentType='application/octet-stream',
                   fileName=file['name'],fileBytes=file['bytes'])
        digest=hashlib.sha256(_json(grant)).hexdigest()
        scope=_source_scope(store,task)
        existing=db.execute('SELECT * FROM node_broker_upload_issues WHERE operation_id=?',(operation_id,)).fetchone()
        if existing is None:
            if db.execute('SELECT COUNT(*) FROM node_broker_upload_issues').fetchone()[0]>=100000:
                raise BrokerHeld('upload issuer capacity reached')
            if db.execute('SELECT 1 FROM node_broker_upload_issues WHERE task_uuid=? AND generation=? AND file_key=?',
                          (task['uuid'],task['generation'],file_key)).fetchone():raise BrokerHeld('file issue already exists')
            db.execute('INSERT INTO node_broker_upload_issues VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                       (operation_id,task['uuid'],task['generation'],file_key,file['path'],*scope,digest))
        elif (existing['task_uuid'],existing['generation'],existing['file_key'],existing['file_path'],
              existing['root_path'],existing['root_dev'],existing['root_ino'],existing['task_dir'],
              existing['task_dev'],existing['task_ino'],existing['grant_hash']) != (task['uuid'],task['generation'],file_key,file['path'],*scope,digest):
            raise BrokerHeld('upload issue changed')
        return grant,sign_grant(grant,secret)


class LocalUploadSender:
    """Owned descriptor sender for same-process integration tests, not RPC."""
    def __init__(self):self._unreaped=[]

    @staticmethod
    def _issued_item(store, grant, claim_token):
        from .prototype import Queue, VIEWER_PROFILE
        from .viewer_tasks import TaskStore
        if (type(store) is not TaskStore or type(store.queue) is not Queue
                or store.queue._creator_pid!=os.getpid() or store.queue._lock.closed
                or type(claim_token) is not str or not 1<=len(claim_token)<=128):
            raise BrokerHeld('actual coordinator store required')
        queue=store.queue
        try:file_key=TaskStore._filename(grant['fileName'])
        except (ValueError,KeyError,TypeError):raise BrokerHeld('invalid stored file identity') from None
        with store._lock, queue._db() as db:
            db.execute('BEGIN')
            schema=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='node_broker_upload_issues'").fetchone()
            if schema is None or schema['sql']!=_ISSUER_DDL:raise BrokerHeld('upload issuer schema changed')
            job=db.execute('SELECT * FROM jobs WHERE id=?',(grant['jobId'],)).fetchone()
            rows=db.execute('SELECT * FROM viewer_tasks WHERE job_id=?',(grant['jobId'],)).fetchall()
            if (job is None or len(rows)!=1 or job['status']!='running' or job['profile']!=VIEWER_PROFILE
                    or job['worker_id']!='viewer-dispatch' or job['node_id']!=grant['nodeId']
                    or job['environment']!=grant['environment'] or grant['authorityId']!=queue.authority_id
                    or grant['coordinatorEpoch']!=queue.startup_id or not job['claim_hash']
                    or not hmac.compare_digest(job['claim_hash'],hashlib.sha256(claim_token.encode()).hexdigest())
                    or not queue._transfer_admission_clear(db,broker_allowed=(grant['operationId'],grant['jobId'],hashlib.sha256(_json(grant)).hexdigest()))):raise BrokerHeld('active coordinator claim required')
            row=rows[0]
            if (row['removed'] or row['stage']!='running' or row['phase']!='upload_pending'
                    or row['generation']!=grant['taskGeneration'] or row['engine_uuid']!=grant['engineTaskId']
                    or row['node_id']!=job['node_id'] or row['environment']!=job['environment']
                    or row['claim_token']!=claim_token):raise BrokerHeld('Viewer upload binding changed')
            issued=db.execute('SELECT * FROM node_broker_upload_issues WHERE operation_id=?',(grant['operationId'],)).fetchone()
            digest=hashlib.sha256(_json(grant)).hexdigest()
            if (issued is None or (issued['task_uuid'],issued['generation'],issued['file_key'],issued['grant_hash'])
                    != (row['uuid'],row['generation'],file_key,digest)):
                raise BrokerHeld('durable upload issue changed')
            if tuple(issued[k] for k in ('root_path','root_dev','root_ino','task_dir','task_dev','task_ino')) != _source_scope(store,row):
                raise BrokerHeld('stored input directory changed')
            task=store._task(db,row)
            files=[item for item in task['files'] if item['name_key']==file_key]
            if (len(files)!=1 or files[0]['name']!=grant['fileName'] or files[0]['bytes']!=grant['fileBytes']
                    or files[0]['sha256']!=grant['bodySha256'] or not files[0]['is_photo']
                    or Path(files[0]['path']).name!=issued['file_path']):
                raise BrokerHeld('exact stored input required')
            return files[0]

    def send(self, broker, grant, signature, *, store, claim_token, secret, deadline_seconds=10):
        from .guarded_native_engine import start_guarded_sender
        if (QUALIFIED is not True or type(broker) is not NodeMutationBroker or self._unreaped
                or type(grant) is not dict
                or type(broker.witness) is not LocalCoordinatorAuthority
                or getattr(store,'queue',None) is not broker.witness.queue
                or type(secret) is not bytes or not hmac.compare_digest(sign_grant(grant,secret),signature)
                or type(deadline_seconds) not in (int,float) or not 0<deadline_seconds<=900):
            raise BrokerHeld('exact owned upload sender required')
        item=self._issued_item(store,grant,claim_token)
        prior=broker.lookup_upload(grant,signature)
        if prior is not None:
            return prior
        deadline=time.monotonic()+deadline_seconds
        completion=start_guarded_sender(item,grant['operationId'],deadline,self)
        try:
            result,replayed=broker.upload(grant,signature,completion.source,_with_replay=True)
            if replayed:
                return result
            completion.verify(deadline)
            if self._issued_item(store,grant,claim_token)!=item:
                raise BrokerHeld('stored input identity changed')
            broker.confirm_upload(grant,signature,_sender_confirmation(grant,secret))
            return result
        finally:completion.stop()


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

    def inspect_task(self, task_id, deadline):
        """Read sanitized point-in-time task status; no missing=idle inference."""
        if not _uuid(task_id):raise BrokerHeld('invalid engine task identity')
        if local_engine_identity(deadline)!=self.engine_id:raise BrokerHeld('local engine changed')
        local_backend_identity(self.pin,deadline)
        transport=SimpleNamespace(binding=SimpleNamespace(backend_port=self.pin.backend_port,backend_secret=self.token))
        try:
            status,raw=EngineIngressGuard._exchange(transport,'GET','/task/'+task_id+'/info',b'',{},deadline,omit_empty_token=True)
        except Exception:
            # Never surface transport details, including the backend token or URL.
            raise BrokerHeld('engine task status unavailable') from None
        if status!=200 or len(raw)>65536:raise BrokerHeld('engine task status unavailable')
        try:
            value=decode_submission(raw)
            if (type(value) is not dict or value.get('uuid')!=task_id or type(value.get('status')) is not dict
                    or type(value['status'].get('code')) is not int or value['status']['code'] not in (10,20,30,40,50)):
                raise ValueError()
            return {'uuid':task_id,'statusCode':value['status']['code']}
        except (ValueError,TypeError,KeyError,RecursionError):
            raise BrokerHeld('engine task status unavailable') from None

    def exchange_upload(self, grant, source):
        deadline=time.monotonic()+self.deadline_seconds
        if local_engine_identity(deadline)!=self.engine_id:raise BrokerHeld('local engine changed')
        local_backend_identity(self.pin,deadline)
        boundary,prefix,suffix=_file_multipart(grant['operationId'],grant['fileName'])
        if grant['fileBytes']+len(prefix)+len(suffix)>MAX_STREAM_BYTES:raise BrokerHeld('upload exceeds bound')
        transport=SimpleNamespace(binding=SimpleNamespace(backend_port=self.pin.backend_port,backend_secret=self.token))
        status,raw=EngineIngressGuard._exchange(transport,'POST','/task/new/upload/'+grant['engineTaskId'],b'',
            {'content-type':'multipart/form-data; boundary='+boundary,'set-uuid':grant['engineTaskId']},deadline,
            source=source,source_length=grant['fileBytes'],expected_sha=grant['bodySha256'],
            file_prefix=prefix,file_suffix=suffix,forbidden=b'--'+boundary.encode('ascii'),omit_empty_token=True)
        if status!=200 or len(raw)>4096:raise BrokerHeld('backend response uncertain')
        return _upload_reply(decode_submission(raw))


def inspect_prior_node_state(engine_id, operation_id, secret):
    """Cooperative, local snapshot only; never adopts an epoch or permits work.

    Requires the existing node gate and identity manifest. A busy, missing or
    inconsistent node is unknown. This is not an authenticated network endpoint,
    a continuing fence, or evidence against direct backend writers. No mTLS
    endpoint is exposed: provisioned certificate identity is not yet bound.
    """
    if (not _uuid(operation_id) or type(secret) is not bytes or len(secret)<32):
        raise BrokerHeld('invalid recovery evidence request')
    if os.name == 'nt':raise BrokerHeld('local inspection requires POSIX locking')
    import fcntl
    root=canonical_gate_path(engine_id); state_root=root/'mutation-broker'
    rootfd=None; statefd=None; lockfd=None; manifestfd=None; journalfd=None; gatefd=None; db=None
    try:
        rootstat=root.lstat(); brokerstat=state_root.lstat()
        if (not stat.S_ISDIR(rootstat.st_mode) or not stat.S_ISDIR(brokerstat.st_mode)
                or root.resolve()!=root.absolute() or state_root.resolve()!=state_root.absolute()
                or rootstat.st_uid!=os.geteuid() or brokerstat.st_uid!=os.geteuid()
                or rootstat.st_mode&0o077 or brokerstat.st_mode&0o077):
            raise BrokerHeld('node state requires inspection')
        rootfd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        if (os.fstat(rootfd).st_dev,os.fstat(rootfd).st_ino)!=(rootstat.st_dev,rootstat.st_ino):raise BrokerHeld('node root changed')
        statefd=os.open('mutation-broker',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=rootfd)
        if (os.fstat(statefd).st_dev,os.fstat(statefd).st_ino)!=(brokerstat.st_dev,brokerstat.st_ino):raise BrokerHeld('node state changed')
        lockfd=os.open('gate.lock',os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=rootfd)
        lock=os.fstat(lockfd)
        if not stat.S_ISREG(lock.st_mode) or lock.st_uid!=os.geteuid() or lock.st_mode&0o077 or lock.st_nlink!=1:
            raise BrokerHeld('node lock requires inspection')
        fcntl.flock(lockfd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        manifestfd=os.open('identity.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=statefd)
        info=os.fstat(manifestfd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_size>8192:
            raise BrokerHeld('node identity requires inspection')
        manifest=json.loads(os.read(manifestfd,8193))
        fields={'version','nodeId','engineId','authorityId','coordinatorOrigin','generation','coordinatorEpoch','gateRoot','gateLock','journal'}
        if (type(manifest) is dict and manifest.get('version')==2):fields=fields|{'nodeCertificateSha256'}
        if (type(manifest) is not dict or set(manifest)!=fields or type(manifest['version']) is not int or manifest['version'] not in (1,2)
                or (manifest['version']==2 and (type(manifest['nodeCertificateSha256']) is not str
                    or not re.fullmatch('[a-f0-9]{64}',manifest['nodeCertificateSha256'])))
                or manifest['engineId']!=engine_id or not _uuid(manifest['authorityId'])
                or not _uuid(manifest['coordinatorEpoch']) or manifest['nodeId'] not in ('main','cluster')
                or type(manifest['generation']) is not int or manifest['generation']<1
                or manifest['gateRoot']!=[rootstat.st_dev,rootstat.st_ino]
                or manifest['gateLock']!=[lock.st_dev,lock.st_ino]):
            raise BrokerHeld('node identity changed')
        # No recovery journals or WAL are interpreted from a pathname. Even a
        # stale sidecar makes this snapshot unknown and requires inspection.
        for suffix in ('-journal','-wal','-shm'):
            try:os.stat('node-broker.sqlite3'+suffix,dir_fd=statefd,follow_symlinks=False)
            except FileNotFoundError:continue
            raise BrokerHeld('node journal sidecar requires inspection')
        journalfd=os.open('node-broker.sqlite3',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=statefd)
        js=os.fstat(journalfd)
        if (not stat.S_ISREG(js.st_mode) or js.st_nlink!=1 or js.st_uid!=os.geteuid() or js.st_mode&0o077
                or not 100<=js.st_size<=16*1024*1024 or manifest['journal']!=[js.st_dev,js.st_ino]):
            raise BrokerHeld('node journal changed')
        gatefd=os.open('gate.json',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=rootfd)
        gs=os.fstat(gatefd)
        if not stat.S_ISREG(gs.st_mode) or gs.st_nlink!=1 or gs.st_uid!=os.geteuid() or gs.st_mode&0o077 or gs.st_size>16384:
            raise BrokerHeld('node gate state changed')
        from .node_gate import _binding
        state=decode_submission(os.read(gatefd,16385))
        if (type(state) is not dict or set(state)!={'schema','engineId','nodeId','coordinatorOrigin','authorityId','generation','reservation'}
                or type(state['schema']) is not int or state['schema']!=1
                or any(state.get(k)!=v for k,v in _binding(state['nodeId'],state['coordinatorOrigin'],state['authorityId']).items())):
            raise BrokerHeld('node gate identity changed')
        for key in ('nodeId','engineId','authorityId','coordinatorOrigin','generation'):
            if state.get(key)!=manifest[key]:raise BrokerHeld('node gate identity changed')
        chunks=[];remaining=js.st_size
        while remaining:
            chunk=os.read(journalfd,min(remaining,65536))
            if not chunk:raise BrokerHeld('node journal changed')
            chunks.append(chunk);remaining-=len(chunk)
        data=b''.join(chunks)
        if data[:16]!=b'SQLite format 3\x00' or data[18:20]!=b'\x01\x01':
            raise BrokerHeld('node journal mode requires inspection')
        # Deserialize the pinned bytes in memory. SQLite never opens the node
        # journal path or creates a rollback, WAL, or shared-memory file.
        db=sqlite3.connect(':memory:',isolation_level=None)
        db.deserialize(data);db.execute('PRAGMA query_only=ON');db.row_factory=sqlite3.Row
        for name,ddl in _DDL.items():
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
            if row is None or row['sql']!=ddl:raise BrokerHeld('node journal schema changed')
        for name,ddl in _NODE_TRIGGERS.items():
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
            if row is None or row['sql']!=ddl:raise BrokerHeld('node journal schema changed')
        if {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}!=set(_NODE_TRIGGERS):
            raise BrokerHeld('node journal schema changed')
        binding=hashlib.sha256(_json([manifest[k] for k in ('nodeId','engineId','authorityId','coordinatorEpoch','coordinatorOrigin','generation')])).hexdigest()
        row=db.execute('SELECT digest FROM binding WHERE singleton=1').fetchone()
        if row is None or row['digest']!=binding:raise BrokerHeld('node journal binding changed')
        reservation=db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
        op=db.execute('SELECT operation,file_key,grant_hash,state FROM operations WHERE id=?',(operation_id,)).fetchone()
        evidence=db.execute('SELECT grant_json,signature FROM node_grants WHERE operation_id=?',(operation_id,)).fetchone()
        observation=db.execute('SELECT grant_hash,reply_json FROM engine_observations WHERE operation_id=?',(operation_id,)).fetchone()
        if reservation is None or op is None or evidence is None:raise BrokerHeld('node held evidence unavailable')
        grant=verify_grant_evidence(evidence['grant_json'],evidence['signature'],secret)
        digest=hashlib.sha256(_json(grant)).hexdigest()
        expected=[grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':manifest['nodeId'],'authorityId':manifest['authorityId']}
        held=state['reservation']
        if (grant['operationId']!=operation_id or grant['operation']!='init'
                or [grant[k] for k in ('nodeId','engineId','authorityId','coordinatorEpoch','gateGeneration')]
                   !=[manifest[k] for k in ('nodeId','engineId','authorityId','coordinatorEpoch','generation')]
                or reservation['identity']!=_json(expected).decode('ascii')
                or tuple(op)!=('init','',digest,'started') or held is None
                or held['receiptId']!=uuid.UUID(grant['jobId']).hex or held['job']!=job
                or held['completionDigest'] is not None):
            raise BrokerHeld('node held evidence differs')
        reply=None
        if observation is not None:
            if observation['grant_hash']!=digest:raise BrokerHeld('node engine evidence differs')
            reply=_reply(decode_submission(observation['reply_json'].encode('utf-8')),grant['engineTaskId'])
        after=os.fstat(journalfd)
        if (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)!=(js.st_dev,js.st_ino,js.st_size,js.st_mtime_ns):raise BrokerHeld('node journal changed')
        return {'identity':manifest,'grant':grant,'operationState':'started',
                'observationPresent':reply is not None,'authorizesRelease':False}
    except BrokerHeld:raise
    except (OSError,ValueError,TypeError,KeyError,sqlite3.Error,GateError):
        raise BrokerHeld('node state unavailable') from None
    finally:
        if db is not None:db.close()
        for fd in (gatefd,journalfd,manifestfd,lockfd,statefd,rootfd):
            if fd is not None:os.close(fd)


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
            for name,ddl in _NODE_TRIGGERS.items():
                existing=self.db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
                if existing is None:
                    if existed:raise BrokerHeld('incomplete node evidence fence')
                    self.db.execute(ddl)
                elif existing['sql']!=ddl:raise BrokerHeld('node evidence fence changed')
            names={row['name'] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
            if names!=set(_NODE_TRIGGERS):raise BrokerHeld('unexpected node broker trigger')
            binding=hashlib.sha256(_json([node_id,engine_id,authority_id,coordinator_epoch,coordinator_origin,gate_generation])).hexdigest()
            old=self.db.execute('SELECT digest FROM binding WHERE singleton=1').fetchone()
            if old is None:self.db.execute('INSERT INTO binding VALUES(1,?)',(binding,))
            elif old['digest'] != binding:raise BrokerHeld('node broker binding changed')
            self.db.commit()
        except BaseException:self.db.rollback();self.db.close();raise

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

    def inspect_held_evidence(self, grant, signature, nonce, *, timeout_seconds=3):
        """Observe this live old-epoch broker and held gate without mutation.

        Requires its own node-local daemon probes; a shared-key MAC is not
        independent node identity or a fence. Engine task state is not queried.
        """
        if (type(nonce) is not str or not re.fullmatch('[a-f0-9]{64}',nonce)
                or type(timeout_seconds) not in (int,float) or not 0<timeout_seconds<=5):
            raise BrokerHeld('node evidence unavailable')
        self._authenticate(grant,signature,'init')
        deadline=time.monotonic()+timeout_seconds
        if not self._mutex.acquire(timeout=timeout_seconds):raise BrokerHeld('node broker busy')
        try:
            with NodeGate(self.engine) as gate:
                self._check_owner()
                def check_gate():
                    opened=os.fstat(gate._stream.fileno());named=(gate.root/'gate.lock').lstat()
                    if (not stat.S_ISREG(named.st_mode) or named.st_nlink!=1
                            or (opened.st_dev,opened.st_ino)!=(named.st_dev,named.st_ino)):
                        raise BrokerHeld('node gate lock changed')
                check_gate()
                if local_engine_identity(deadline)!=self.engine:raise BrokerHeld('local engine changed')
                local_backend_identity(self.adapter.pin,deadline)
                state=gate.snapshot();reservation=state['reservation']
                expected={'nodeId':self.node,'coordinatorOrigin':self.origin,'authorityId':self.authority,'generation':self.generation}
                identity=uuid.UUID(grant['jobId']).hex
                job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
                if (any(state[k]!=v for k,v in expected.items()) or reservation is None
                        or reservation['receiptId']!=identity or reservation['job']!=job
                        or reservation['completionDigest'] is not None):
                    raise BrokerHeld('held node reservation differs')
                self.db.execute('BEGIN')
                try:
                    for name,ddl in _DDL.items():
                        row=self.db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone()
                        if row is None or row['sql']!=ddl:raise BrokerHeld('node evidence schema changed')
                    for name,ddl in _NODE_TRIGGERS.items():
                        row=self.db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
                        if row is None or row['sql']!=ddl:raise BrokerHeld('node evidence fence changed')
                    names={row['name'] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
                    if names!=set(_NODE_TRIGGERS):raise BrokerHeld('unexpected node evidence trigger')
                    raw=_json(grant).decode('ascii');digest=hashlib.sha256(raw.encode('ascii')).hexdigest()
                    binding=hashlib.sha256(_json([self.node,self.engine,self.authority,self.epoch,self.origin,self.generation])).hexdigest()
                    old=self.db.execute('SELECT digest FROM binding WHERE singleton=1').fetchone()
                    stored=self.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
                    op=self.db.execute('SELECT operation,file_key,grant_hash,state FROM operations WHERE id=?',(grant['operationId'],)).fetchone()
                    evidence=self.db.execute('SELECT grant_json,signature FROM node_grants WHERE operation_id=?',(grant['operationId'],)).fetchone()
                    observation=self.db.execute('SELECT grant_hash,reply_json FROM engine_observations WHERE operation_id=?',(grant['operationId'],)).fetchone()
                    durable=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
                    if (old is None or old['digest']!=binding or stored is None or stored['identity']!=durable
                            or op is None or tuple(op)!=('init','',digest,'started')
                            or evidence is None or tuple(evidence)!=(raw,signature)):
                        raise BrokerHeld('held node evidence differs')
                    reply=None
                    if observation is not None:
                        if observation['grant_hash']!=digest:raise BrokerHeld('engine observation differs')
                        reply=_reply(json.loads(observation['reply_json']),grant['engineTaskId'])
                    self.db.commit()
                except BaseException:self.db.rollback();raise
                # This GET occurs while this cooperative gate is held. A
                # missing/malformed response stays held; no absence inference.
                task_observation=self.adapter.inspect_task(grant['engineTaskId'],deadline)
                self._check_owner();check_gate()
                if local_engine_identity(deadline)!=self.engine:raise BrokerHeld('local engine changed')
                local_backend_identity(self.adapter.pin,deadline)
                report={'nonce':nonce,'grant':grant,'grantSignature':signature,'operationState':'started',
                        'reservationId':identity,'observation':reply,'engineTaskState':'unverified',
                        'engineTaskObservation':task_observation,'nodeOrigin':'shared-key-only'}
                mac=hmac.new(self.secret,b'ledgetop-node-held-evidence-v1\0'+_json(report),hashlib.sha256).hexdigest()
                return {'report':report,'mac':mac}
        finally:self._mutex.release()

    def close(self):
        with self._mutex:
            if not self._closed:
                self._closed=True
                self.db.close()

    def _authenticate(self, grant, signature, operation):
        fields=_UPLOAD_FIELDS if operation=='upload' else _FIELDS
        if type(grant) is not dict or set(grant)!=fields:
            raise BrokerHeld('invalid bounded grant')
        integers={'gateGeneration','taskGeneration','fileBytes'} if operation=='upload' else {'gateGeneration','taskGeneration'}
        if (any(type(grant[k]) is not str or not 1 <= len(grant[k]) <= 256 for k in fields-integers)
                or type(grant['gateGeneration']) is not int or type(grant['taskGeneration']) is not int
                or grant['taskGeneration']<1 or type(signature) is not str
                or (operation=='upload' and type(grant['fileBytes']) is not int)
                or not re.fullmatch('[a-f0-9]{64}',signature)):
            raise BrokerHeld('invalid bounded grant')
        if not hmac.compare_digest(sign_grant(grant,self.secret),signature):
            raise BrokerHeld('grant authentication failed')
        if (grant['nodeId']!=self.node or grant['engineId']!=self.engine or grant['authorityId']!=self.authority
                or grant['coordinatorEpoch']!=self.epoch or grant['gateGeneration']!=self.generation
                or any(not _uuid(grant[k]) for k in ('jobId','engineTaskId','operationId'))
                or grant['environment'] not in ('production','staging') or grant['operation']!=operation
                or type(grant['contentType']) is not str or not re.fullmatch(r'[ -~]{1,256}',grant['contentType'])
                or not re.fullmatch('[a-f0-9]{64}',grant['bodySha256'])):raise BrokerHeld('grant binding changed')

    def mutate(self, grant, signature, body):
        if type(body) is not bytes or len(body)>65536:raise BrokerHeld('invalid bounded grant')
        operation=grant.get('operation') if type(grant) is dict else None
        if operation not in ('init','commit'):raise BrokerHeld('invalid mutation')
        self._authenticate(grant,signature,operation)
        if grant['bodySha256']!=hashlib.sha256(body).hexdigest():raise BrokerHeld('grant binding changed')
        # Upload is not implemented; never commit an empty or partially uploaded
        # NodeODM task while that stage lacks a durable completion contract.
        if grant['operation']=='commit':raise BrokerHeld('upload stage not qualified')
        return self._execute(grant,'',lambda:self.adapter.exchange(grant,body),body=body,signature=signature)

    def _validate_upload(self, grant, signature):
        from .viewer_tasks import TaskStore
        self._authenticate(grant,signature,'upload')
        if (type(grant['fileBytes']) is not int or not 0<grant['fileBytes']<=MAX_STREAM_BYTES-4096
                or grant['contentType']!='application/octet-stream'):
            raise BrokerHeld('invalid bounded upload')
        try:return TaskStore._filename(grant['fileName'])
        except ValueError:raise BrokerHeld('invalid upload filename') from None

    def lookup_upload(self, grant, signature):
        """Return exact completed replay without creating a producer or writer."""
        if type(self.witness) is not LocalCoordinatorAuthority:raise BrokerHeld('remote upload not qualified')
        file_key=self._validate_upload(grant,signature)
        identity=uuid.UUID(grant['jobId']).hex
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._mutex, self.witness.hold(grant,10), NodeGate(self.engine) as gate:
            self._check_owner();state=gate.snapshot();reservation=state['reservation']
            if (any(state[k]!=v for k,v in {'nodeId':self.node,'coordinatorOrigin':self.origin,
                    'authorityId':self.authority,'generation':self.generation}.items()) or not reservation
                    or reservation['receiptId']!=identity or reservation['job']!=job
                    or reservation['completionDigest'] is not None):raise BrokerHeld('node ownership changed')
            self.db.execute('BEGIN')
            try:
                current=self.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
                if current is None or current['identity']!=durable_identity:raise BrokerHeld('durable reservation changed')
                row=self.db.execute('SELECT * FROM operations WHERE id=?',(grant['operationId'],)).fetchone()
                if row is None:
                    if (self.db.execute("SELECT 1 FROM operations WHERE state!='complete' LIMIT 1").fetchone()
                            or self.db.execute("SELECT 1 FROM operations WHERE operation='upload' AND file_key=?",(file_key,)).fetchone()):
                        raise BrokerHeld('operation requires inspection')
                    self.db.commit();return None
                if (row['operation']!='upload' or row['file_key']!=file_key or row['grant_hash']!=digest
                        or row['state']!='complete'):raise BrokerHeld('operation requires inspection')
                reply=_upload_reply(json.loads(row['reply']));self.db.commit();return reply
            except BaseException:self.db.rollback();raise

    def upload(self, grant, signature, source, *, _with_replay=False):
        """Consume and close one dedicated socket carrying exactly one file."""
        if type(self.witness) is not LocalCoordinatorAuthority:raise BrokerHeld('remote upload not qualified')
        if type(source) is not socket.socket:raise BrokerHeld('dedicated socket required')
        try:
            file_key=self._validate_upload(grant,signature)
            return self._execute(grant,file_key,lambda:self.adapter.exchange_upload(grant,source),with_replay=_with_replay,signature=signature)
        finally:source.close()

    def confirm_upload(self, grant, signature, confirmation):
        if type(self.witness) is not LocalCoordinatorAuthority:raise BrokerHeld('remote upload not qualified')
        file_key=self._validate_upload(grant,signature)
        if (type(confirmation) is not str or not re.fullmatch('[a-f0-9]{64}',confirmation)
                or not hmac.compare_digest(_sender_confirmation(grant,self.secret),confirmation)):
            raise BrokerHeld('sender confirmation required')
        identity=uuid.UUID(grant['jobId']).hex
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._mutex, self.witness.hold(grant,10), NodeGate(self.engine) as gate:
            self._check_owner();state=gate.snapshot();reservation=state['reservation']
            if (any(state[k]!=v for k,v in {'nodeId':self.node,'coordinatorOrigin':self.origin,
                    'authorityId':self.authority,'generation':self.generation}.items()) or not reservation
                    or reservation['receiptId']!=identity or reservation['job']!=job
                    or reservation['completionDigest'] is not None):raise BrokerHeld('node ownership changed')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                current=self.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()
                if current is None or current['identity']!=durable_identity:raise BrokerHeld('durable reservation changed')
                row=self.db.execute('SELECT grant_hash,state,file_key FROM operations WHERE id=? AND operation=?',
                                    (grant['operationId'],'upload')).fetchone()
                if row is None or row['grant_hash']!=digest or row['file_key']!=file_key:raise BrokerHeld('upload identity changed')
                if row['state']=='complete':
                    self.db.commit();self.witness.completed(grant);return
                if row['state']!='awaiting_sender':raise BrokerHeld('upload confirmation unavailable')
                self.db.execute("UPDATE operations SET state='complete' WHERE id=? AND state='awaiting_sender'",(grant['operationId'],))
                self.db.commit()
            except BaseException:self.db.rollback();raise
            self.witness.completed(grant)

    def _execute(self, grant, file_key, forward, *, with_replay=False, body=None, signature=None):
        digest=hashlib.sha256(_json(grant)).hexdigest();identity=uuid.UUID(grant['jobId']).hex
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        with self._mutex, self.witness.hold(grant,10,create=True,body=body,signature=signature), NodeGate(self.engine) as gate:
            self._check_owner()
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
                raw_grant=_json(grant).decode('ascii')
                if len(raw_grant)>16384 or type(signature) is not str or not re.fullmatch('[a-f0-9]{64}',signature):
                    raise BrokerHeld('signed node grant evidence required')
                evidence=self.db.execute('SELECT grant_json,signature FROM node_grants WHERE operation_id=?',(grant['operationId'],)).fetchone()
                if evidence is None:
                    if old is not None:raise BrokerHeld('prior node grant evidence missing')
                    self.db.execute('INSERT INTO node_grants VALUES(?,?,?)',(grant['operationId'],raw_grant,signature))
                elif tuple(evidence)!=(raw_grant,signature):raise BrokerHeld('node grant evidence changed')
                if old:
                    if (old['grant_hash']!=digest or old['operation']!=grant['operation']
                            or old['file_key']!=file_key or old['state']!='complete'):
                        raise BrokerHeld('operation requires inspection')
                    reply=json.loads(old['reply'])
                    reply=_upload_reply(reply) if grant['operation']=='upload' else _reply(reply,grant['engineTaskId'])
                    self.db.commit()
                    if not isinstance(self.witness,LocalCoordinatorAuthority):self.witness.check()
                    self.witness.completed(grant);return (reply,True) if with_replay else reply
                if self.db.execute("SELECT 1 FROM operations WHERE state!='complete' LIMIT 1").fetchone():
                    raise BrokerHeld('previous operation requires inspection')
                if grant['operation']=='upload' and not self.db.execute("SELECT 1 FROM operations WHERE operation='init' AND state='complete'").fetchone():
                    raise BrokerHeld('confirmed initialization required')
                if self.db.execute('SELECT 1 FROM operations WHERE operation=? AND file_key=?',(grant['operation'],file_key)).fetchone():
                    raise BrokerHeld('operation already exists')
                if self.db.execute('SELECT COUNT(*) FROM operations').fetchone()[0]>=100000:
                    raise BrokerHeld('node operation capacity reached')
                self.db.execute('INSERT INTO operations VALUES(?,?,?,?,?,NULL)',(grant['operationId'],grant['operation'],file_key,digest,'started'));self.db.commit()
            except BaseException:self.db.rollback();raise
            reply=forward()
            reply=_upload_reply(reply) if grant['operation']=='upload' else _reply(reply,grant['engineTaskId'])
            self._check_owner()
            # A received response is evidence, not permission to release. The
            # operation remains started until the witness also confirms it.
            observed=_json(reply).decode('ascii')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                old_observation=self.db.execute('SELECT grant_hash,reply_json FROM engine_observations WHERE operation_id=?',(grant['operationId'],)).fetchone()
                if old_observation is None:
                    self.db.execute('INSERT INTO engine_observations VALUES(?,?,?)',(grant['operationId'],digest,observed))
                elif tuple(old_observation)!=(digest,observed):raise BrokerHeld('engine observation changed')
                self.db.commit()
            except BaseException:self.db.rollback();raise
            if isinstance(self.witness,LocalCoordinatorAuthority):self.witness._check()
            else:self.witness.check()
            raw=_json(reply)
            if len(raw)>4096:raise BrokerHeld('backend response exceeds bound')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                next_state='awaiting_sender' if grant['operation']=='upload' else 'complete'
                changed=self.db.execute("UPDATE operations SET state=?,reply=? WHERE id=? AND grant_hash=? AND state='started'",(next_state,raw.decode(),grant['operationId'],digest)).rowcount
                if changed!=1:raise BrokerHeld('operation identity changed')
                self.db.commit()
            except BaseException:self.db.rollback();raise
            if grant['operation']=='init':self.witness.completed(grant)
            return (reply,False) if with_replay else reply
