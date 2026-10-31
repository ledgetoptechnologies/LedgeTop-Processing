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
from types import MappingProxyType
import uuid

from . import reference_protocol as p
from .maintenance import Conflict
from .viewer_tasks import PHOTO_SUFFIXES, VIEWER_PROFILE, canonical
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
 PRIMARY KEY(job_id,ordinal), UNIQUE(job_id,object_id))'''
PAGE_SIZE=100


class ViewerWorkerClaims:
    def __init__(self, queue, viewer_store, *, local_max_photos, local_max_input_bytes, node_environments):
        if viewer_store.queue is not queue: raise ValueError('shared Viewer queue required')
        if type(local_max_photos) is not int or local_max_photos<1 or type(local_max_input_bytes) is not int or local_max_input_bytes<1:
            raise ValueError('positive routing limits required')
        self.queue,self.store=queue,viewer_store
        self.max_photos,self.max_bytes=local_max_photos,local_max_input_bytes
        if (type(node_environments) is not dict or set(node_environments)!=set(queue.node_ids)
                or any(type(values) not in (tuple,list) or not values or len(set(values))!=len(values)
                       or not set(values)<={'production','staging'} for values in node_environments.values())):
            raise ValueError('explicit node environment policy required')
        self.node_environments=MappingProxyType({node:frozenset(values) for node,values in node_environments.items()})
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            objects=db.execute("SELECT type,name,sql FROM sqlite_master WHERE name IN ('viewer_worker_claims','viewer_worker_inventory')").fetchall()
            if not objects:db.execute(DDL);db.execute(FILES_DDL)
            self._schema(db)

    @staticmethod
    def _schema(db):
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN ('viewer_worker_claims','viewer_worker_inventory') ORDER BY type,name"
        expected=sqlite3.connect(':memory:')
        try:
            expected.execute(DDL);expected.execute(FILES_DDL)
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
        manifest=[]
        for entry in sorted(item['files'],key=lambda value:value['name'].casefold()):
            path=entry['path']
            try: info=os.stat(path,follow_symlinks=False)
            except OSError:raise Conflict('Viewer input unavailable') from None
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_size!=entry['bytes']
                    or type(entry['bytes']) is not int or not 1<=entry['bytes']<=p.MAX_SAFE_INTEGER
                    or not isinstance(entry['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',entry['sha256'])):
                raise Conflict('Viewer input metadata invalid')
            manifest.append(dict(name=entry['name'],bytes=entry['bytes'],sha256=entry['sha256'],isPhoto=entry['is_photo']))
        # This conservative route never sends a small job to main merely
        # because the small node is offline. Readiness is a separate gate.
        small=item['images_count']<=self.max_photos and item['input_bytes']<=self.max_bytes
        if identity['node_id']!=('cluster' if small else 'main'):raise Conflict('Viewer routing mismatch')
        settings=canonical({'options':item['options'],'outputs':item['outputs']}).encode()
        value=dict(schemaVersion=1,authorityId=self.queue.authority_id,jobId=job['id'],viewerTaskId=task['uuid'],viewerGeneration=task['generation'],
            environment=task['environment'],nodeId=identity['node_id'],workerId=identity['worker_id'],
            workerInstanceId=identity['worker_instance_id'],engineTaskId=engine_uuid,
            inputInventorySha256=p.digest(manifest),pipelineSha256=hashlib.sha256(settings).hexdigest(),
            imageCount=item['images_count'],inputBytes=item['input_bytes'],state='claimed_unenrolled')
        return (value,manifest) if include_manifest else value

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
            engine_uuid=str(uuid.uuid4());value,manifest=self._binding(db,job,task,identity,engine_uuid,include_manifest=True)
            token=secrets.token_urlsafe(32);encoded=p.canonical_json(value).decode()
            db.execute('INSERT INTO viewer_worker_claims VALUES(?,?,?,?,?,?,?,?)',
                (job['id'],payload['claimRequestId'],identity['node_id'],identity['worker_id'],identity['worker_instance_id'],job['environment'],encoded,token))
            db.executemany('INSERT INTO viewer_worker_inventory VALUES(?,?,?,?,?,?,?)',(
                (job['id'],index,p.digest(dict(jobId=job['id'],generation=task['generation'],ordinal=index,entry=entry)),
                 entry['name'],entry['bytes'],entry['sha256'],int(entry['isPhoto'])) for index,entry in enumerate(manifest)))
            db.execute("UPDATE jobs SET status='running',worker_id=?,node_id=?,claim_hash=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (identity['worker_id'],identity['node_id'],hashlib.sha256(token.encode()).hexdigest(),job['id']))
            db.execute("UPDATE viewer_tasks SET stage='running',phase='worker_claimed',node_id=?,engine_uuid=?,claim_token=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                (identity['node_id'],engine_uuid,token,task['uuid']))
            return {**value,'claimRequestId':payload['claimRequestId'],'claimToken':token}

    def inventory_page(self,job_id,cursor,identity,claim_token):
        """Bounded metadata page; does not serve or verify any input bytes."""
        p._uuid(job_id,'job ID')
        if type(cursor) is not int or not 0<=cursor<=100000:raise ValueError('invalid inventory cursor')
        if type(claim_token) is not str or not claim_token.isascii() or not 1<=len(claim_token)<=128:
            raise Conflict('Viewer claim token required')
        with self.queue._db() as db:
            db.execute('BEGIN');self._schema(db)
            row=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(job_id,)).fetchone()
            job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
            task=db.execute('SELECT * FROM viewer_tasks WHERE job_id=? AND removed=0',(job_id,)).fetchall()
            if (not row or not job or len(task)!=1 or
                (row['node_id'],row['worker_id'],row['instance_id'])!=(identity['node_id'],identity['worker_id'],identity['worker_instance_id'])
                or row['environment'] not in self.node_environments.get(identity['node_id'],())
                or not hmac.compare_digest(row['token'],claim_token)
                or job['claim_hash']!=hashlib.sha256(claim_token.encode()).hexdigest()
                or job['status'] not in ('running','held') or (job['status']=='held' and job['held_reason']!='coordinator_restart')):
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
            p._hash(saved['inputInventorySha256'],'input inventory digest')
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
