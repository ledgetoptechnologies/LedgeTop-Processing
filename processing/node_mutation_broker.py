"""Disabled node-local NodeODM mutation broker; no listener or live wiring.

One signed coordinator epoch and one non-expiring NodeGate reservation are
bound to an exact physical node and task. Lost responses retain started intents.
This cooperative prototype does not fence local root, Docker administrators,
direct backend connections, or a coordinator that retains the signing key.
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

from .engine_ingress_guard import (BackendPin, EngineIngressGuard, MAX_STREAM_BYTES, _file_multipart,
                                   local_backend_identity, local_engine_identity)
from .node_gate import NodeGate, canonical_gate_path
from .reference_protocol import decode_submission

QUALIFIED = False
_FIELDS = {'authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId',
           'environment','gateGeneration','taskGeneration','operationId','operation','bodySha256','contentType'}
_UPLOAD_FIELDS = _FIELDS | {'fileName','fileBytes'}
_ISSUER_DDL = ('CREATE TABLE node_broker_upload_issues ('
    'operation_id TEXT PRIMARY KEY, task_uuid TEXT NOT NULL, generation INTEGER NOT NULL, '
    'file_key TEXT NOT NULL, file_path TEXT NOT NULL, grant_hash TEXT NOT NULL, UNIQUE(task_uuid,generation,file_key))')
_DDL = {
    'binding': 'CREATE TABLE binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), digest TEXT NOT NULL)',
    'reservation': 'CREATE TABLE reservation (singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL)',
    'operations': 'CREATE TABLE operations (id TEXT PRIMARY KEY, operation TEXT NOT NULL, file_key TEXT NOT NULL, grant_hash TEXT NOT NULL, state TEXT NOT NULL, reply TEXT, UNIQUE(operation,file_key))',
}


class BrokerHeld(ValueError): pass


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')).encode('ascii')


def _uuid(value):
    try: return type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError): return False


def sign_grant(grant, secret):
    if type(secret) is not bytes or len(secret) < 32: raise BrokerHeld('invalid signing key')
    return hmac.new(secret, b'ledgetop-node-mutation-v1\0' + _json(grant), hashlib.sha256).hexdigest()


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
                     operation_id, body, content_type, secret):
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


def issue_upload_grant(queue, *, job_id, claim_token, engine_id, gate_generation,
                       operation_id, file_key, secret):
    """Record and sign exact input metadata from the active Queue transaction."""
    from .prototype import Queue, VIEWER_PROFILE
    from .viewer_tasks import TaskStore
    if QUALIFIED is not True or type(queue) is not Queue or queue._creator_pid!=os.getpid() or queue._lock.closed:
        raise BrokerHeld('actual coordinator owner required')
    if (not _uuid(job_id) or not _uuid(operation_id) or type(claim_token) is not str or not 1<=len(claim_token)<=128
            or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
            or type(gate_generation) is not int or gate_generation<1 or type(file_key) is not str
            or TaskStore._filename(file_key)!=file_key):raise BrokerHeld('invalid upload request')
    with queue._db() as db:
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
        existing=db.execute('SELECT * FROM node_broker_upload_issues WHERE operation_id=?',(operation_id,)).fetchone()
        if existing is None:
            if db.execute('SELECT COUNT(*) FROM node_broker_upload_issues').fetchone()[0]>=100000:
                raise BrokerHeld('upload issuer capacity reached')
            if db.execute('SELECT 1 FROM node_broker_upload_issues WHERE task_uuid=? AND generation=? AND file_key=?',
                          (task['uuid'],task['generation'],file_key)).fetchone():raise BrokerHeld('file issue already exists')
            db.execute('INSERT INTO node_broker_upload_issues VALUES(?,?,?,?,?,?)',
                       (operation_id,task['uuid'],task['generation'],file_key,file['path'],digest))
        elif (existing['task_uuid'],existing['generation'],existing['file_key'],existing['file_path'],existing['grant_hash']) != (task['uuid'],task['generation'],file_key,file['path'],digest):
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
                    or not queue._transfer_admission_clear(db)):raise BrokerHeld('active coordinator claim required')
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
            result=broker.upload(grant,signature,completion.source)
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


class NodeMutationBroker:
    def __init__(self, root, *, node_id, engine_id, authority_id, coordinator_epoch,
                 coordinator_origin, gate_generation, secret, adapter):
        if QUALIFIED is not True: raise BrokerHeld('node broker is not qualified')
        if (node_id not in ('cluster','main') or not _uuid(authority_id) or not _uuid(coordinator_epoch)
                or type(engine_id) is not str or not re.fullmatch('[A-Za-z0-9:._-]{1,255}',engine_id)
                or type(gate_generation) is not int or gate_generation < 1
                or type(secret) is not bytes or len(secret) < 32 or type(adapter) is not LoopbackNodeODM
                or adapter.engine_id != engine_id): raise BrokerHeld('invalid broker binding')
        self.root=Path(root).absolute();self.node=node_id;self.engine=engine_id;self.authority=authority_id
        self.epoch=coordinator_epoch;self.origin=coordinator_origin;self.generation=gate_generation
        self.secret=secret;self.adapter=adapter;self._mutex=threading.Lock();self._pid=os.getpid();self._closed=False
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
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone():
                raise BrokerHeld('unexpected node broker trigger')
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
        return self._execute(grant,'',lambda:self.adapter.exchange(grant,body))

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
        file_key=self._validate_upload(grant,signature)
        identity=uuid.UUID(grant['jobId']).hex
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._mutex, NodeGate(self.engine) as gate:
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

    def upload(self, grant, signature, source):
        """Consume and close one dedicated socket carrying exactly one file."""
        if type(source) is not socket.socket:raise BrokerHeld('dedicated socket required')
        try:
            file_key=self._validate_upload(grant,signature)
            return self._execute(grant,file_key,lambda:self.adapter.exchange_upload(grant,source))
        finally:source.close()

    def confirm_upload(self, grant, signature, confirmation):
        file_key=self._validate_upload(grant,signature)
        if (type(confirmation) is not str or not re.fullmatch('[a-f0-9]{64}',confirmation)
                or not hmac.compare_digest(_sender_confirmation(grant,self.secret),confirmation)):
            raise BrokerHeld('sender confirmation required')
        identity=uuid.UUID(grant['jobId']).hex
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._mutex, NodeGate(self.engine) as gate:
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
                if row['state']=='complete':self.db.commit();return
                if row['state']!='awaiting_sender':raise BrokerHeld('upload confirmation unavailable')
                self.db.execute("UPDATE operations SET state='complete' WHERE id=? AND state='awaiting_sender'",(grant['operationId'],))
                self.db.commit()
            except BaseException:self.db.rollback();raise

    def _execute(self, grant, file_key, forward):
        digest=hashlib.sha256(_json(grant)).hexdigest();identity=uuid.UUID(grant['jobId']).hex
        durable_identity=_json([grant[k] for k in ('authorityId','nodeId','engineId','coordinatorEpoch','jobId','engineTaskId','environment','gateGeneration','taskGeneration')]).decode()
        job={'id':grant['jobId'],'workerId':'node-broker','nodeId':self.node,'authorityId':self.authority}
        with self._mutex, NodeGate(self.engine) as gate:
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
                if old:
                    if (old['grant_hash']!=digest or old['operation']!=grant['operation']
                            or old['file_key']!=file_key or old['state']!='complete'):
                        raise BrokerHeld('operation requires inspection')
                    reply=json.loads(old['reply'])
                    reply=_upload_reply(reply) if grant['operation']=='upload' else _reply(reply,grant['engineTaskId'])
                    self.db.commit();return reply
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
            raw=_json(reply)
            if len(raw)>4096:raise BrokerHeld('backend response exceeds bound')
            self.db.execute('BEGIN IMMEDIATE')
            try:
                next_state='awaiting_sender' if grant['operation']=='upload' else 'complete'
                changed=self.db.execute("UPDATE operations SET state=?,reply=? WHERE id=? AND grant_hash=? AND state='started'",(next_state,raw.decode(),grant['operationId'],digest)).rowcount
                if changed!=1:raise BrokerHeld('operation identity changed')
                self.db.commit()
            except BaseException:self.db.rollback();raise
            return reply
