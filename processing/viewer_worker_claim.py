"""Experimental coordinator-only Viewer claim; never authorizes node dispatch.

The protected worker API supplies its authenticated principal. This isolated
draft binds persisted Viewer metadata, not current input bytes or a physical
NodeGate reservation. Runtime activation is independently disabled.
"""
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import stat
import threading
from types import MappingProxyType
import uuid

from . import reference_protocol as p
from .maintenance import Conflict
from .viewer_tasks import PHOTO_SUFFIXES, VIEWER_PROFILE, canonical
from .reference_binary import BinaryResponse
from .direct_output_slot import _identity
from .direct_outbox_contract import validate_binding, direct_pipeline_digest
from pathlib import Path


DDL = '''CREATE TABLE viewer_worker_claims (
 job_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, node_id TEXT NOT NULL,
 worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, environment TEXT NOT NULL,
 binding TEXT NOT NULL, token TEXT NOT NULL)'''
FILES_DDL = '''CREATE TABLE viewer_worker_inventory (
 job_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0 AND ordinal<100000),
 object_id TEXT NOT NULL CHECK(length(object_id)=64),
 name TEXT NOT NULL CHECK(length(CAST(name AS BLOB))<=180),
 bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>0),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64), is_photo INTEGER NOT NULL CHECK(is_photo IN (0,1)),
 source_name TEXT NOT NULL, source_device INTEGER NOT NULL, source_inode INTEGER NOT NULL,
 source_mtime INTEGER NOT NULL, source_ctime INTEGER NOT NULL, source_mount INTEGER NOT NULL,
 PRIMARY KEY(job_id,ordinal), UNIQUE(job_id,object_id))'''
ENROLL_DDL = '''CREATE TABLE viewer_worker_enrollments (
 job_id TEXT PRIMARY KEY, attestation TEXT NOT NULL, receipt TEXT NOT NULL)'''
PLAN_DDL = '''CREATE TABLE viewer_worker_plans (job_id TEXT PRIMARY KEY, plan TEXT NOT NULL, task_name TEXT NOT NULL)'''
GRANT_DDL = '''CREATE TABLE viewer_worker_grants (job_id TEXT PRIMARY KEY, request TEXT NOT NULL, grant_value TEXT NOT NULL)'''
FENCE_DDL = '''CREATE TABLE viewer_worker_fences (job_id TEXT PRIMARY KEY, stop_requested INTEGER NOT NULL CHECK(stop_requested IN (0,1)))'''
PERMIT_DDL = '''CREATE TABLE viewer_worker_permits (job_id TEXT NOT NULL, operation_id TEXT NOT NULL, operation TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','finished')), PRIMARY KEY(job_id,operation_id))'''
RESULT_DDL = '''CREATE TABLE viewer_worker_result_metadata (job_id TEXT PRIMARY KEY, payload TEXT NOT NULL, receipt TEXT NOT NULL)'''
SPOOL_CONFIG_DDL = '''CREATE TABLE viewer_worker_spool_config (singleton INTEGER PRIMARY KEY CHECK(singleton=1), policy TEXT NOT NULL)'''
SPOOL_DDL = '''CREATE TABLE viewer_worker_spool (job_id TEXT PRIMARY KEY, metadata_sha TEXT NOT NULL, slot_key TEXT NOT NULL, state TEXT NOT NULL, slot_identity TEXT, receipt TEXT)'''
CHUNK_DDL = '''CREATE TABLE viewer_worker_chunk_sessions (job_id TEXT PRIMARY KEY, metadata_sha TEXT NOT NULL, slot_key TEXT NOT NULL, slot_identity TEXT, state TEXT NOT NULL CHECK(state IN ('creating','ready','sealed')))'''
COMPLETION_DDL = '''CREATE TABLE viewer_worker_completions (job_id TEXT PRIMARY KEY, observation TEXT NOT NULL, spool_receipt TEXT NOT NULL, receipt TEXT NOT NULL)'''
TRIGGER_DDL = '''CREATE TRIGGER viewer_worker_active_permit_hold BEFORE UPDATE OF status ON jobs WHEN OLD.status='running' AND NEW.status!='running' AND EXISTS(SELECT 1 FROM viewer_worker_permits WHERE job_id=OLD.id AND state='active') BEGIN SELECT RAISE(ABORT,'viewer active permit requires drain'); END'''
PAGE_SIZE=100
MAX_INPUT_READERS=2


class _InputStream:
    def __init__(self,fd,directory_fd,name,expected,permit):
        self.fd,self.directory_fd,self.name,self.expected,self.permit=fd,directory_fd,name,expected,permit
        self.closed=False

    def _check(self):
        info=os.fstat(self.fd);named=os.stat(self.name,dir_fd=self.directory_fd,follow_symlinks=False)
        identity=(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or identity!=self.expected[:5]
                or (named.st_dev,named.st_ino)!=(info.st_dev,info.st_ino) or _identity(self.fd)[2]!=self.expected[6]):
            raise Conflict('Viewer source identity changed')

    def chunks(self):
        digest=hashlib.sha256();offset=0;size=self.expected[2]
        while offset<size:
            self._check();data=os.pread(self.fd,min(1024*1024,size-offset),offset)
            if not data:raise Conflict('Viewer source truncated')
            offset+=len(data);digest.update(data);self._check()
            if offset==size and digest.hexdigest()!=self.expected[5]:raise Conflict('Viewer source hash changed')
            yield data

    def close(self):
        if not self.closed:
            self.closed=True
            try:os.close(self.fd)
            finally:
                try:os.close(self.directory_fd)
                finally:self.permit.release()


class ViewerWorkerClaims:
    def __init__(self, queue, viewer_store, *, local_max_photos, local_max_input_bytes, node_environments):
        if viewer_store.queue is not queue: raise ValueError('shared Viewer queue required')
        if type(local_max_photos) is not int or local_max_photos<1 or type(local_max_input_bytes) is not int or local_max_input_bytes<1:
            raise ValueError('positive routing limits required')
        self.queue,self.store=queue,viewer_store
        self.max_photos,self.max_bytes=local_max_photos,local_max_input_bytes
        self._input_readers=threading.BoundedSemaphore(MAX_INPUT_READERS)
        if (type(node_environments) is not dict or set(node_environments)!=set(queue.node_ids)
                or any(type(values) not in (tuple,list) or not values or len(set(values))!=len(values)
                       or not set(values)<={'production','staging'} for values in node_environments.values())):
            raise ValueError('explicit node environment policy required')
        self.node_environments=MappingProxyType({node:frozenset(values) for node,values in node_environments.items()})
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            objects=db.execute("SELECT type,name,sql FROM sqlite_master WHERE name IN ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_plans','viewer_worker_grants','viewer_worker_fences','viewer_worker_permits','viewer_worker_result_metadata','viewer_worker_spool_config','viewer_worker_spool','viewer_worker_chunk_sessions','viewer_worker_completions','viewer_worker_active_permit_hold')").fetchall()
            if not objects:
                for sql in (DDL,FILES_DDL,ENROLL_DDL,PLAN_DDL,GRANT_DDL,FENCE_DDL,PERMIT_DDL,RESULT_DDL,SPOOL_CONFIG_DDL,SPOOL_DDL,CHUNK_DDL,COMPLETION_DDL,TRIGGER_DDL):db.execute(sql)
            self._schema(db)

    @staticmethod
    def _schema(db):
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_plans','viewer_worker_grants','viewer_worker_fences','viewer_worker_permits','viewer_worker_result_metadata','viewer_worker_spool_config','viewer_worker_spool','viewer_worker_chunk_sessions','viewer_worker_completions') OR name='viewer_worker_active_permit_hold' ORDER BY type,name"
        expected=sqlite3.connect(':memory:')
        try:
            expected.execute('CREATE TABLE jobs (id TEXT,status TEXT)')
            for sql in (DDL,FILES_DDL,ENROLL_DDL,PLAN_DDL,GRANT_DDL,FENCE_DDL,PERMIT_DDL,RESULT_DDL,SPOOL_CONFIG_DDL,SPOOL_DDL,CHUNK_DDL,COMPLETION_DDL,TRIGGER_DDL):expected.execute(sql)
            if [tuple(row) for row in db.execute(query)] != [tuple(row) for row in expected.execute(query)]:
                raise Conflict('Viewer claim schema changed')
        finally:expected.close()

    def _binding(self,db,job,task,identity,engine_uuid,*,include_manifest=False):
        if (task['removed'] or task['job_id']!=job['id'] or task['environment']!=job['environment']
                or task['generation']<1):raise Conflict('Viewer identity changed')
        # TaskStore verifies the persisted option evidence and paths within the
        # same DB transaction; hashes below remain upload-time metadata only.
        item=self.store._task(db,task)
        if not item['files'] or len(item['files'])>100000 or item['images_count']<1 or item['input_bytes']>p.MAX_SAFE_INTEGER:
            raise Conflict('Viewer inputs unavailable')
        raw_files=db.execute('SELECT name_key,name,is_photo FROM viewer_files WHERE uuid=?',(task['uuid'],)).fetchall()
        try:
            if any(row['name_key']!=self.store._filename(row['name']) or type(row['is_photo']) is not int
                   or row['is_photo'] not in (0,1)
                   or bool(row['is_photo'])!=(Path(row['name']).suffix.lower() in PHOTO_SUFFIXES)
                   for row in raw_files):raise ValueError()
        except (ValueError,TypeError):raise Conflict('Viewer input metadata invalid') from None
        manifest=[];source=[]
        for entry in sorted(item['files'],key=lambda value:value['name'].casefold()):
            path=entry['path']
            try: info=os.stat(path,follow_symlinks=False)
            except OSError:raise Conflict('Viewer input unavailable') from None
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size!=entry['bytes']
                    or type(entry['bytes']) is not int or not 1<=entry['bytes']<=p.MAX_SAFE_INTEGER
                    or not isinstance(entry['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',entry['sha256'])):
                raise Conflict('Viewer input metadata invalid')
            manifest.append(dict(name=entry['name'],bytes=entry['bytes'],sha256=entry['sha256'],isPhoto=entry['is_photo']))
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK)
            try:
                opened=os.fstat(fd)
                if (opened.st_dev,opened.st_ino,opened.st_size,opened.st_mtime_ns,opened.st_ctime_ns)!=(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns):
                    raise Conflict('Viewer input identity changed')
                mount=_identity(fd)[2]
            finally:os.close(fd)
            source.append((Path(path).name,info.st_dev,info.st_ino,info.st_mtime_ns,info.st_ctime_ns,mount))
        # This conservative route never sends a small job to main merely
        # because the small node is offline. Readiness is a separate gate.
        small=item['images_count']<=self.max_photos and item['input_bytes']<=self.max_bytes
        if identity['node_id']!=('cluster' if small else 'main'):raise Conflict('Viewer routing mismatch')
        settings=canonical({'options':item['options'],'outputs':item['outputs']}).encode()
        if len(settings)>65536:raise Conflict('Viewer plan too large')
        if direct_pipeline_digest(item['options'],item['outputs'])!=hashlib.sha256(settings).hexdigest():
            raise Conflict('Viewer plan invalid')
        value=dict(schemaVersion=1,authorityId=self.queue.authority_id,jobId=job['id'],viewerTaskId=task['uuid'],viewerGeneration=task['generation'],
            environment=task['environment'],nodeId=identity['node_id'],workerId=identity['worker_id'],
            workerInstanceId=identity['worker_instance_id'],engineTaskId=engine_uuid,
            inputInventorySha256=p.digest(manifest),pipelineSha256=hashlib.sha256(settings).hexdigest(),taskNameSha256=p.digest(item['name']),
            imageCount=item['images_count'],objectCount=len(manifest),inputBytes=item['input_bytes'],state='claimed_unenrolled')
        return (value,manifest,source,settings.decode(),item['name']) if include_manifest else value

    def claim(self,payload,identity):
        p._closed(payload,('jobId','claimRequestId','environment'),'Viewer worker claim')
        p._uuid(payload['jobId'],'job ID');p._slug(payload['claimRequestId'],'claim request ID')
        p._environment(payload['environment'])
        if payload['environment'] not in self.node_environments.get(identity['node_id'],()):
            raise Conflict('worker environment is not enabled')
        q=self.queue
        with q._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._schema(db)
            gate=getattr(q,'admission_drain',None)
            if gate is not None and gate._closed(db):raise Conflict('admission closed')
            previous=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=? OR request_id=?',
                                (payload['jobId'],payload['claimRequestId'])).fetchall()
            job=db.execute('SELECT * FROM jobs WHERE id=?',(payload['jobId'],)).fetchone()
            task=db.execute('SELECT * FROM viewer_tasks WHERE job_id=? AND removed=0',(payload['jobId'],)).fetchall()
            if not job or len(task)!=1 or job['profile']!=VIEWER_PROFILE:raise Conflict('Viewer claim unavailable')
            task=task[0]
            if payload['environment']!=job['environment']:raise Conflict('Viewer environment mismatch')
            if previous:
                if len(previous)!=1:raise Conflict('Viewer claim identity conflict')
                old=previous[0]
                if ((old['job_id'],old['request_id'],old['node_id'],old['worker_id'],old['instance_id'],old['environment'])!=
                    (job['id'],payload['claimRequestId'],identity['node_id'],identity['worker_id'],identity['worker_instance_id'],payload['environment'])
                    or job['status'] not in ('running','held') or (job['status']=='held' and job['held_reason']!='coordinator_restart')):
                    raise Conflict('Viewer claim replay conflict')
                saved=json.loads(old['binding'])
                if (task['stage']!='running' or task['phase']!='worker_claimed'
                        or task['node_id']!=old['node_id'] or task['engine_uuid']!=saved.get('engineTaskId')
                        or task['claim_token']!=old['token'] or job['worker_id']!=old['worker_id']
                        or job['node_id']!=old['node_id']
                        or job['claim_hash']!=hashlib.sha256(old['token'].encode()).hexdigest()):
                    raise Conflict('Viewer claim ownership changed')
                current=self._binding(db,job,task,identity,saved['engineTaskId'])
                if p.canonical_json(saved)!=p.canonical_json(current):raise Conflict('Viewer claim binding changed')
                return {**saved,'claimRequestId':old['request_id'],'claimToken':old['token']}
            q._refresh_nodes(db)
            if q._node(db,identity['node_id'])['state'] not in ('active','scheduled'):
                raise Conflict('node unavailable')
            if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():
                raise Conflict('execution slot occupied')
            head=db.execute("SELECT id FROM jobs WHERE status='queued' ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1").fetchone()
            if not head or head['id']!=job['id'] or job['status']!='queued':raise Conflict('Viewer queue head changed')
            if task['stage']!='queued' or task['phase'] not in ('queued','waiting_node') or any(task[k] is not None for k in ('node_id','engine_uuid','claim_token')):
                raise Conflict('Viewer task is not unclaimed')
            engine_uuid=str(uuid.uuid4());value,manifest,source,settings,task_name=self._binding(db,job,task,identity,engine_uuid,include_manifest=True)
            token=secrets.token_urlsafe(32);encoded=p.canonical_json(value).decode()
            db.execute('INSERT INTO viewer_worker_claims VALUES(?,?,?,?,?,?,?,?)',
                (job['id'],payload['claimRequestId'],identity['node_id'],identity['worker_id'],identity['worker_instance_id'],job['environment'],encoded,token))
            db.execute('INSERT INTO viewer_worker_plans VALUES(?,?,?)',(job['id'],settings,task_name))
            db.executemany('INSERT INTO viewer_worker_inventory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(
                (job['id'],index,p.digest(dict(jobId=job['id'],generation=task['generation'],ordinal=index,entry=entry)),
                 entry['name'],entry['bytes'],entry['sha256'],int(entry['isPhoto']),*source[index]) for index,entry in enumerate(manifest)))
            db.execute("UPDATE jobs SET status='running',worker_id=?,node_id=?,claim_hash=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (identity['worker_id'],identity['node_id'],hashlib.sha256(token.encode()).hexdigest(),job['id']))
            db.execute("UPDATE viewer_tasks SET stage='running',phase='worker_claimed',node_id=?,engine_uuid=?,claim_token=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                (identity['node_id'],engine_uuid,token,task['uuid']))
            return {**value,'claimRequestId':payload['claimRequestId'],'claimToken':token}

    def _authorize_inventory(self,db,job_id,identity,claim_token,*,require_enrollment=True,allow_fence_hold=False):
        if type(claim_token) is not str or not claim_token.isascii() or not 1<=len(claim_token)<=128:
            raise Conflict('Viewer claim token required')
        self._schema(db)
        row=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(job_id,)).fetchone()
        job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
        task=db.execute('SELECT * FROM viewer_tasks WHERE job_id=? AND removed=0',(job_id,)).fetchall()
        if (not row or not job or len(task)!=1 or
            (row['node_id'],row['worker_id'],row['instance_id'])!=(identity['node_id'],identity['worker_id'],identity['worker_instance_id'])
            or row['environment'] not in self.node_environments.get(identity['node_id'],())
            or not hmac.compare_digest(row['token'],claim_token)
            or job['claim_hash']!=hashlib.sha256(claim_token.encode()).hexdigest()
            or job['status'] not in ('running','held') or (job['status']=='held' and job['held_reason']!='coordinator_restart'
                and not (allow_fence_hold and job['held_reason']=='worker_fence_hold'))):
            raise Conflict('Viewer inventory claim unavailable')
        saved=json.loads(row['binding']);task=task[0]
        if (p.canonical_json(saved).decode()!=row['binding'] or saved['state']!='claimed_unenrolled'
            or (saved['authorityId'],saved['jobId'],saved['nodeId'],saved['workerId'],saved['workerInstanceId'],saved['environment'])!=
               (self.queue.authority_id,job_id,row['node_id'],row['worker_id'],row['instance_id'],row['environment'])
            or job['profile']!=VIEWER_PROFILE or job['environment']!=row['environment']
            or task['uuid']!=saved['viewerTaskId'] or task['generation']!=saved['viewerGeneration']
            or task['environment']!=row['environment'] or task['stage']!='running' or task['phase']!='worker_claimed'
            or task['node_id']!=row['node_id'] or task['engine_uuid']!=saved['engineTaskId']
            or task['claim_token']!=row['token'] or job['node_id']!=row['node_id'] or job['worker_id']!=row['worker_id']):
            raise Conflict('Viewer inventory ownership changed')
        if 'objectCount' not in saved:raise Conflict('Viewer claim object count unavailable')
        p._integer(saved['objectCount'],'input object count')
        p._hash(saved['inputInventorySha256'],'input inventory digest');p._hash(saved['taskNameSha256'],'task name digest')
        if require_enrollment:
            receipt=db.execute('SELECT attestation,receipt FROM viewer_worker_enrollments WHERE job_id=?',(job_id,)).fetchone()
            if not receipt or not hmac.compare_digest(receipt['receipt'],p.digest(json.loads(receipt['attestation']))):
                raise Conflict('Viewer node enrollment unavailable')
        return saved,task

    def plan(self,job_id,identity,claim_token):
        p._uuid(job_id,'job ID')
        with self.queue._db() as db:
            db.execute('BEGIN');saved,_=self._authorize_inventory(db,job_id,identity,claim_token)
            row=db.execute('SELECT plan FROM viewer_worker_plans WHERE job_id=?',(job_id,)).fetchone()
            if not row or not isinstance(row['plan'],str) or len(row['plan'].encode())>65536:
                raise Conflict('Viewer plan unavailable')
            try:
                value=json.loads(row['plan']);p._closed(value,('options','outputs'),'Viewer plan')
                if canonical(value)!=row['plan'] or direct_pipeline_digest(value['options'],value['outputs'])!=saved['pipelineSha256']:
                    raise ValueError()
            except (ValueError,TypeError,RecursionError):raise Conflict('Viewer plan changed') from None
            return dict(jobId=job_id,viewerGeneration=saved['viewerGeneration'],pipelineSha256=saved['pipelineSha256'],plan=value)

    def execution_grant(self,job_id,payload,identity,claim_token):
        """Prototype grant after a trusted worker attests complete local inputs."""
        p._uuid(job_id,'job ID')
        p._closed(payload,('enrollmentReceipt','inputInventorySha256','pipelineSha256','inputCount','inputBytes'),'execution grant request')
        for field in ('enrollmentReceipt','inputInventorySha256','pipelineSha256'):p._hash(payload[field],field)
        for field in ('inputCount','inputBytes'):p._integer(payload[field],field)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');saved,_=self._authorize_inventory(db,job_id,identity,claim_token)
            enrollment=db.execute('SELECT receipt FROM viewer_worker_enrollments WHERE job_id=?',(job_id,)).fetchone()
            plan=db.execute('SELECT plan,task_name FROM viewer_worker_plans WHERE job_id=?',(job_id,)).fetchone()
            job=db.execute('SELECT status FROM jobs WHERE id=?',(job_id,)).fetchone()
            inventory=db.execute('SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM viewer_worker_inventory WHERE job_id=?',(job_id,)).fetchone()
            if (not enrollment or not plan or not job or job['status']!='running' or type(plan['task_name']) is not str or len(plan['task_name'])>256
                or any(ord(char)<32 for char in plan['task_name']) or p.digest(plan['task_name'])!=saved['taskNameSha256']
                or payload['enrollmentReceipt']!=enrollment['receipt'] or payload['inputInventorySha256']!=saved['inputInventorySha256']
                or payload['pipelineSha256']!=saved['pipelineSha256'] or payload['inputBytes']!=saved['inputBytes']
                or tuple(inventory)!=(payload['inputCount'],payload['inputBytes'])):
                raise Conflict('execution grant binding changed')
            try:
                value=json.loads(plan['plan']);p._closed(value,('options','outputs'),'Viewer plan')
                if canonical(value)!=plan['plan'] or direct_pipeline_digest(value['options'],value['outputs'])!=saved['pipelineSha256']:raise ValueError()
            except (ValueError,TypeError,RecursionError):raise Conflict('Viewer plan changed') from None
            request=p.canonical_json(payload).decode()
            grant=dict(schemaVersion=1,kind='synthetic_execution_grant',authorityId=saved['authorityId'],jobId=job_id,viewerTaskId=saved['viewerTaskId'],
                viewerGeneration=saved['viewerGeneration'],environment=saved['environment'],nodeId=saved['nodeId'],
                workerId=saved['workerId'],workerInstanceId=saved['workerInstanceId'],engineTaskId=saved['engineTaskId'],
                taskName=plan['task_name'],taskNameSha256=saved['taskNameSha256'],inputInventorySha256=saved['inputInventorySha256'],pipelineSha256=saved['pipelineSha256'],
                enrollmentReceipt=enrollment['receipt'],inputCount=payload['inputCount'],inputBytes=payload['inputBytes'])
            encoded=p.canonical_json(grant).decode();old=db.execute('SELECT request,grant_value FROM viewer_worker_grants WHERE job_id=?',(job_id,)).fetchone()
            if old:
                if (old['request'],old['grant_value'])!=(request,encoded):raise Conflict('execution grant changed')
            else:db.execute('INSERT INTO viewer_worker_grants VALUES(?,?,?)',(job_id,request,encoded))
            return grant

    def mutation_permit(self,job_id,payload,identity,claim_token):
        p._closed(payload,('operationId','operation'),'mutation permit');p._slug(payload['operationId'],'operation ID')
        if payload['operation'] not in ('init','upload','commit'):raise Conflict('mutation invalid')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._authorize_inventory(db,job_id,identity,claim_token)
            job=db.execute('SELECT status FROM jobs WHERE id=?',(job_id,)).fetchone()
            grant=db.execute('SELECT 1 FROM viewer_worker_grants WHERE job_id=?',(job_id,)).fetchone()
            fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(job_id,)).fetchone()
            if not grant or not job or job['status']!='running' or (fence and fence['stop_requested']):raise Conflict('mutation admission closed')
            existing=db.execute('SELECT operation,state FROM viewer_worker_permits WHERE job_id=? AND operation_id=?',(job_id,payload['operationId'])).fetchone()
            if existing:
                if (existing['operation'],existing['state'])!=(payload['operation'],'active'):raise Conflict('mutation permit changed')
            else:
                if db.execute("SELECT 1 FROM viewer_worker_permits WHERE job_id=? AND state='active'",(job_id,)).fetchone():raise Conflict('mutation already active')
                db.execute('INSERT INTO viewer_worker_permits VALUES(?,?,?,?)',(job_id,payload['operationId'],payload['operation'],'active'))
            return dict(jobId=job_id,operationId=payload['operationId'],operation=payload['operation'],state='active')

    def mutation_finish(self,job_id,payload,identity,claim_token):
        p._closed(payload,('operationId','operation'),'mutation finish');p._slug(payload['operationId'],'operation ID')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._authorize_inventory(db,job_id,identity,claim_token,allow_fence_hold=True)
            row=db.execute('SELECT operation,state FROM viewer_worker_permits WHERE job_id=? AND operation_id=?',(job_id,payload['operationId'])).fetchone()
            if not row or row['operation']!=payload['operation'] or row['state'] not in ('active','finished'):raise Conflict('mutation permit changed')
            db.execute("UPDATE viewer_worker_permits SET state='finished' WHERE job_id=? AND operation_id=?",(job_id,payload['operationId']))
            fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(job_id,)).fetchone()
            if fence and fence['stop_requested']:
                db.execute("UPDATE jobs SET status='held',held_reason='worker_fence_hold' WHERE id=? AND status='running'",(job_id,))
            return dict(jobId=job_id,operationId=payload['operationId'],operation=payload['operation'],state='finished')

    def mutation_hold(self,job_id,identity,claim_token):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._authorize_inventory(db,job_id,identity,claim_token,allow_fence_hold=True)
            db.execute('INSERT INTO viewer_worker_fences VALUES(?,1) ON CONFLICT(job_id) DO UPDATE SET stop_requested=1',(job_id,))
            active=db.execute("SELECT 1 FROM viewer_worker_permits WHERE job_id=? AND state='active'",(job_id,)).fetchone()
            if not active:db.execute("UPDATE jobs SET status='held',held_reason='worker_fence_hold' WHERE id=? AND status='running'",(job_id,))
            return dict(jobId=job_id,state='draining' if active else 'held')

    def result_metadata(self,job_id,payload,identity,claim_token):
        p._uuid(job_id,'job ID')
        p._closed(payload,('schemaVersion','kind','bindingSha256','jobId','viewerTaskId','viewerGeneration',
            'engineTaskId','environment','archiveBytes','archiveSha256','allocation'),'local result intent')
        if type(payload['schemaVersion']) is not int or payload['schemaVersion']!=1 or payload['kind']!='viewer-local-result-intent':
            raise Conflict('result metadata invalid')
        p._hash(payload['bindingSha256'],'binding digest');p._hash(payload['archiveSha256'],'archive digest')
        p._integer(payload['archiveBytes'],'archive size')
        p._integer(payload['viewerGeneration'],'Viewer generation');p._environment(payload['environment'])
        for field in ('jobId','viewerTaskId','engineTaskId'):p._uuid(payload[field],field)
        if (type(payload['allocation']) is not list or len(payload['allocation'])!=3
                or any(type(value) is not int or value<0 for value in payload['allocation'])):
            raise Conflict('result allocation invalid')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');saved,_=self._authorize_inventory(db,job_id,identity,claim_token,allow_fence_hold=True)
            enrollment=db.execute('SELECT attestation,receipt FROM viewer_worker_enrollments WHERE job_id=?',(job_id,)).fetchone()
            grant=db.execute('SELECT grant_value FROM viewer_worker_grants WHERE job_id=?',(job_id,)).fetchone()
            commit=db.execute("SELECT state,operation FROM viewer_worker_permits WHERE job_id=? AND operation_id='commit'",(job_id,)).fetchone()
            if not enrollment or not grant or not commit or (commit['state'],commit['operation'])!=('finished','commit'):
                raise Conflict('result metadata unavailable')
            attestation=json.loads(enrollment['attestation']);binding=attestation['binding'];grant_value=json.loads(grant['grant_value'])
            if (payload['bindingSha256']!=validate_binding(binding) or payload['jobId']!=job_id
                or payload['viewerTaskId']!=saved['viewerTaskId'] or payload['viewerGeneration']!=saved['viewerGeneration']
                or payload['engineTaskId']!=saved['engineTaskId'] or payload['environment']!=saved['environment']
                or payload['allocation']!=attestation['slotIdentity']['allocation']
                or payload['archiveBytes']>binding['outputBudgetBytes'] or grant_value['jobId']!=job_id
                or grant_value['engineTaskId']!=saved['engineTaskId'] or grant_value['enrollmentReceipt']!=enrollment['receipt']):
                raise Conflict('result metadata binding changed')
            encoded=p.canonical_json(payload).decode();digest=p.digest(payload)
            receipt=dict(schemaVersion=1,kind='viewer-result-metadata-receipt',jobId=job_id,payloadSha256=digest,
                         archiveSha256=payload['archiveSha256'],archiveBytes=payload['archiveBytes'])
            result=p.canonical_json(receipt).decode()
            old=db.execute('SELECT payload,receipt FROM viewer_worker_result_metadata WHERE job_id=?',(job_id,)).fetchone()
            if old:
                if (old['payload'],old['receipt'])!=(encoded,result):raise Conflict('result metadata changed')
            else:db.execute('INSERT INTO viewer_worker_result_metadata VALUES(?,?,?)',(job_id,encoded,result))
            return receipt

    def completion_attestation(self,job_id,payload,identity,claim_token,verified_spool):
        p._closed(payload,('observation','spoolReceipt'),'completion attestation')
        observation=payload['observation'];spool_receipt=payload['spoolReceipt']
        p._closed(observation,('schemaVersion','kind','bindingSha256','grantSha256','jobId','viewerTaskId',
                              'viewerGeneration','engineTaskId','environment','engineStatus'),'completion observation')
        if (type(observation['schemaVersion']) is not int or observation['schemaVersion']!=1
                or observation['kind']!='viewer-engine-completion-observation' or type(observation['engineStatus']) is not int
                or observation['engineStatus']!=40):raise Conflict('completion observation invalid')
        p._hash(observation['bindingSha256'],'binding digest');p._hash(observation['grantSha256'],'grant digest')
        p._integer(observation['viewerGeneration'],'Viewer generation');p._environment(observation['environment'])
        for key in ('jobId','viewerTaskId','engineTaskId'):p._uuid(observation[key],key)
        if p.canonical_json(spool_receipt)!=p.canonical_json(verified_spool):raise Conflict('spool receipt changed')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');saved,_=self._authorize_inventory(db,job_id,identity,claim_token)
            job=db.execute('SELECT status FROM jobs WHERE id=?',(job_id,)).fetchone()
            fence=db.execute('SELECT stop_requested FROM viewer_worker_fences WHERE job_id=?',(job_id,)).fetchone()
            active=db.execute("SELECT 1 FROM viewer_worker_permits WHERE job_id=? AND state='active'",(job_id,)).fetchone()
            grant=db.execute('SELECT grant_value FROM viewer_worker_grants WHERE job_id=?',(job_id,)).fetchone()
            enrollment=db.execute('SELECT attestation FROM viewer_worker_enrollments WHERE job_id=?',(job_id,)).fetchone()
            spool=db.execute('SELECT state,receipt FROM viewer_worker_spool WHERE job_id=?',(job_id,)).fetchone()
            commit=db.execute("SELECT state,operation FROM viewer_worker_permits WHERE job_id=? AND operation_id='commit'",(job_id,)).fetchone()
            if (not job or job['status']!='running' or (fence and fence['stop_requested']) or active or not grant or not enrollment
                or not commit or (commit['state'],commit['operation'])!=('finished','commit')
                or not spool or spool['state']!='sealed' or spool['receipt']!=p.canonical_json(spool_receipt).decode()):
                raise Conflict('completion publication unavailable')
            grant_value=json.loads(grant['grant_value']);enrollment_value=json.loads(enrollment['attestation'])
            if (observation['bindingSha256']!=validate_binding(enrollment_value['binding'])
                or observation['grantSha256']!=p.digest(grant_value) or observation['jobId']!=job_id
                or observation['viewerTaskId']!=saved['viewerTaskId'] or observation['viewerGeneration']!=saved['viewerGeneration']
                or observation['engineTaskId']!=saved['engineTaskId'] or observation['environment']!=saved['environment']):
                raise Conflict('completion identity changed')
            observed=p.canonical_json(observation).decode();spooled=p.canonical_json(spool_receipt).decode()
            receipt=dict(schemaVersion=1,kind='viewer-completion-attestation-receipt',jobId=job_id,
                         observationSha256=p.digest(observation),spoolReceiptSha256=p.digest(spool_receipt))
            encoded=p.canonical_json(receipt).decode()
            old=db.execute('SELECT observation,spool_receipt,receipt FROM viewer_worker_completions WHERE job_id=?',(job_id,)).fetchone()
            if old:
                if (old['observation'],old['spool_receipt'],old['receipt'])!=(observed,spooled,encoded):raise Conflict('completion attestation changed')
            else:db.execute('INSERT INTO viewer_worker_completions VALUES(?,?,?,?)',(job_id,observed,spooled,encoded))
            return receipt

    def enroll(self,job_id,payload,identity,claim_token):
        p._uuid(job_id,'job ID');p._closed(payload,('binding','slotIdentity'),'node enrollment')
        binding=payload['binding'];validate_binding(binding)
        p._closed(payload['slotIdentity'],('root','allocation','metadata'),'output slot identity')
        for key in ('root','allocation','metadata'):
            value=payload['slotIdentity'][key]
            if type(value) is not list or len(value)!=3 or any(type(v) is not int or v<0 for v in value):
                raise Conflict('node slot identity invalid')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');saved,_=self._authorize_inventory(db,job_id,identity,claim_token,require_enrollment=False)
            if any(binding[a]!=saved[b] for a,b in (('authorityId','authorityId'),('nodeId','nodeId'),('workerId','workerId'),
                    ('environment','environment'),('viewerTaskId','viewerTaskId'),('viewerGeneration','viewerGeneration'),
                    ('jobId','jobId'),('engineTaskId','engineTaskId'),('inputSetSha256','inputInventorySha256'),('pipelineSha256','pipelineSha256'))):
                raise Conflict('node enrollment claim mismatch')
            text=p.canonical_json(payload).decode();receipt=p.digest(payload)
            old=db.execute('SELECT attestation,receipt FROM viewer_worker_enrollments WHERE job_id=?',(job_id,)).fetchone()
            if old:
                if (old['attestation'],old['receipt'])!=(text,receipt):raise Conflict('node enrollment changed')
            else:db.execute('INSERT INTO viewer_worker_enrollments VALUES(?,?,?)',(job_id,text,receipt))
            return dict(jobId=job_id,enrollmentReceipt=receipt,state='enrolled_no_dispatch')

    def inventory_page(self,job_id,cursor,identity,claim_token):
        """Bounded metadata page; does not serve or verify any input bytes."""
        p._uuid(job_id,'job ID')
        if type(cursor) is not int or not 0<=cursor<=100000:raise ValueError('invalid inventory cursor')
        with self.queue._db() as db:
            db.execute('BEGIN');saved,task=self._authorize_inventory(db,job_id,identity,claim_token)
            rows=db.execute('SELECT * FROM viewer_worker_inventory WHERE job_id=? AND ordinal>=? ORDER BY ordinal LIMIT ?',
                            (job_id,cursor,PAGE_SIZE+1)).fetchall()
            entries=[]
            for index,item in enumerate(rows[:PAGE_SIZE],start=cursor):
                if item['ordinal']!=index:raise Conflict('Viewer inventory sequence changed')
                try:
                    self.store._filename(item['name']);p._hash(item['sha256'],'input hash');p._hash(item['object_id'],'object identity')
                    p._integer(item['bytes'],'input size')
                except ValueError:raise Conflict('Viewer inventory record changed') from None
                entry=dict(name=item['name'],bytes=item['bytes'],sha256=item['sha256'],isPhoto=bool(item['is_photo']))
                if item['is_photo'] not in (0,1) or item['object_id']!=p.digest(dict(jobId=job_id,generation=task['generation'],ordinal=index,entry=entry)):
                    raise Conflict('Viewer inventory record changed')
                entries.append(dict(ordinal=index,objectId=item['object_id'],**entry))
            if not entries:raise Conflict('Viewer inventory cursor unavailable')
            return dict(jobId=job_id,viewerGeneration=task['generation'],inputInventorySha256=saved['inputInventorySha256'],
                        start=cursor,entries=entries,nextCursor=cursor+PAGE_SIZE if len(rows)>PAGE_SIZE else None)

    def input_object(self,job_id,object_id,identity,claim_token):
        """Open one bounded authenticated frozen source, retaining a reader permit."""
        p._uuid(job_id,'job ID');p._hash(object_id,'object identity')
        if not self._input_readers.acquire(blocking=False):raise Conflict('Viewer source read capacity unavailable')
        fd=directory_fd=None
        try:
            with self.queue._db() as db:
                db.execute('BEGIN');saved,task=self._authorize_inventory(db,job_id,identity,claim_token)
                row=db.execute('SELECT * FROM viewer_worker_inventory WHERE job_id=? AND object_id=?',(job_id,object_id)).fetchone()
                if not row:raise Conflict('Viewer source unavailable')
                entry=dict(name=row['name'],bytes=row['bytes'],sha256=row['sha256'],isPhoto=bool(row['is_photo']))
                if (row['is_photo'] not in (0,1) or object_id!=p.digest(dict(jobId=job_id,generation=task['generation'],ordinal=row['ordinal'],entry=entry))
                        or Path(row['source_name']).name!=row['source_name']):raise Conflict('Viewer source identity invalid')
                directory=self.store._directory(task)
                directory_fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
                fd=os.open(row['source_name'],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=directory_fd)
                expected=(row['source_device'],row['source_inode'],row['bytes'],row['source_mtime'],row['source_ctime'],row['sha256'],row['source_mount'])
                stream=_InputStream(fd,directory_fd,row['source_name'],expected,self._input_readers)
                stream._check()
                result=BinaryResponse(stream,headers={'Content-Type':'application/octet-stream','Content-Length':str(row['bytes'])})
                fd=directory_fd=None
                return result
        except BaseException:
            if fd is not None:os.close(fd)
            if directory_fd is not None:os.close(directory_fd)
            self._input_readers.release()
            raise
