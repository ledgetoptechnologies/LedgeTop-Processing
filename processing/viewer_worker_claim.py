"""Experimental coordinator-only Viewer claim; never authorizes node dispatch.

The protected worker API supplies its authenticated principal. This isolated
draft binds persisted Viewer metadata, not current input bytes or a physical
NodeGate reservation. Runtime activation is independently disabled.
"""
import hashlib
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
            objects=db.execute("SELECT type,sql FROM sqlite_master WHERE name='viewer_worker_claims'").fetchall()
            if not objects:db.execute(DDL)
            elif len(objects)!=1 or tuple(objects[0])!=('table',DDL):raise Conflict('Viewer claim schema changed')
            self._schema(db)

    @staticmethod
    def _schema(db):
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='viewer_worker_claims' ORDER BY type,name"
        expected=sqlite3.connect(':memory:')
        try:
            expected.execute(DDL)
            if [tuple(row) for row in db.execute(query)] != [tuple(row) for row in expected.execute(query)]:
                raise Conflict('Viewer claim schema changed')
        finally:expected.close()

    def _binding(self,db,job,task,identity,engine_uuid):
        if (task['removed'] or task['job_id']!=job['id'] or task['environment']!=job['environment']
                or task['generation']<1):raise Conflict('Viewer identity changed')
        # TaskStore verifies the persisted option evidence and paths within the
        # same DB transaction; hashes below remain upload-time metadata only.
        item=self.store._task(db,task)
        if not item['files'] or item['images_count']<1:raise Conflict('Viewer inputs unavailable')
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
                    or type(entry['bytes']) is not int or entry['bytes']<=0
                    or not isinstance(entry['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',entry['sha256'])):
                raise Conflict('Viewer input metadata invalid')
            manifest.append(dict(name=entry['name'],bytes=entry['bytes'],sha256=entry['sha256'],isPhoto=entry['is_photo']))
        # This conservative route never sends a small job to main merely
        # because the small node is offline. Readiness is a separate gate.
        small=item['images_count']<=self.max_photos and item['input_bytes']<=self.max_bytes
        if identity['node_id']!=('cluster' if small else 'main'):raise Conflict('Viewer routing mismatch')
        settings=canonical({'options':item['options'],'outputs':item['outputs']}).encode()
        return dict(schemaVersion=1,authorityId=self.queue.authority_id,jobId=job['id'],viewerTaskId=task['uuid'],viewerGeneration=task['generation'],
            environment=task['environment'],nodeId=identity['node_id'],workerId=identity['worker_id'],
            workerInstanceId=identity['worker_instance_id'],engineTaskId=engine_uuid,
            inputInventorySha256=p.digest(manifest),pipelineSha256=hashlib.sha256(settings).hexdigest(),
            imageCount=item['images_count'],inputBytes=item['input_bytes'],state='claimed_unenrolled')

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
            engine_uuid=str(uuid.uuid4());value=self._binding(db,job,task,identity,engine_uuid)
            token=secrets.token_urlsafe(32);encoded=p.canonical_json(value).decode()
            db.execute('INSERT INTO viewer_worker_claims VALUES(?,?,?,?,?,?,?,?)',
                (job['id'],payload['claimRequestId'],identity['node_id'],identity['worker_id'],identity['worker_instance_id'],job['environment'],encoded,token))
            db.execute("UPDATE jobs SET status='running',worker_id=?,node_id=?,claim_hash=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (identity['worker_id'],identity['node_id'],hashlib.sha256(token.encode()).hexdigest(),job['id']))
            db.execute("UPDATE viewer_tasks SET stage='running',phase='worker_claimed',node_id=?,engine_uuid=?,claim_token=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                (identity['node_id'],engine_uuid,token,task['uuid']))
            return {**value,'claimRequestId':payload['claimRequestId'],'claimToken':token}
