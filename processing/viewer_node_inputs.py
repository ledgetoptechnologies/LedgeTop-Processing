"""Disabled direct-Viewer input transfer prototype; no execution or cleanup."""
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import threading
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener

from . import reference_protocol as p
from .direct_output_slot import _identity, open_slot
from .direct_outbox_contract import direct_pipeline_digest
from .node_gate import NodeGate
from .reference_transfer import _DeadlineHTTPSHandler, _TransferWatchdog
from .viewer_node_enrollment import ViewerNodeEnrollment, _NoRedirect
from .viewer_tasks import PHOTO_SUFFIXES, TaskStore


class InputHeld(RuntimeError):pass


POLICY_DDL='CREATE TABLE policy (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL, mac_key TEXT NOT NULL)'
MANIFEST_DDL='CREATE TABLE manifest (singleton INTEGER PRIMARY KEY CHECK(singleton=1), phase TEXT NOT NULL, digest TEXT NOT NULL, count INTEGER, total INTEGER)'
OBJECT_DDL='''CREATE TABLE objects (ordinal INTEGER PRIMARY KEY, object_id TEXT NOT NULL UNIQUE, entry TEXT NOT NULL,
 mac TEXT NOT NULL, phase TEXT NOT NULL, slot_key TEXT, slot_identity TEXT)'''
PLAN_DDL='CREATE TABLE plan (singleton INTEGER PRIMARY KEY CHECK(singleton=1), value TEXT NOT NULL)'
GRANT_DDL='CREATE TABLE execution_grant (singleton INTEGER PRIMARY KEY CHECK(singleton=1), request TEXT NOT NULL, value TEXT NOT NULL)'
SUBMISSION_DDL='CREATE TABLE synthetic_submission (singleton INTEGER PRIMARY KEY CHECK(singleton=1), grant_value TEXT NOT NULL, origin TEXT NOT NULL, phase TEXT NOT NULL, ordinal INTEGER NOT NULL, object_id TEXT)'


class ViewerNodeInputs:
    """One authenticated enrollment, bounded metadata and same-inode inputs."""
    def __init__(self,enrollment,job_id,request_id,environment,*,input_root,input_root_identity,max_total,max_object):
        if type(enrollment) is not ViewerNodeEnrollment:raise InputHeld('enrollment_required')
        if type(max_total) is not int or type(max_object) is not int or not 1<=max_object<=max_total<=p.MAX_SAFE_INTEGER:
            raise InputHeld('input_budget_invalid')
        enrollment.enroll(job_id,request_id,environment)
        self.worker=enrollment;self._pid=os.getpid();self._lock=threading.RLock();self._verified_session=False
        with enrollment._db() as db:
            row=db.execute('SELECT request,claim,binding,slot_phase,slot_identity,receipt FROM intent WHERE singleton=1').fetchone()
        if not row or row[0]!=p.canonical_json(dict(jobId=job_id,claimRequestId=request_id,environment=environment)).decode() or row[3]!='committed':
            raise InputHeld('enrollment_unavailable')
        self.request=row[0];self.claim=json.loads(row[1]);self.binding=json.loads(row[2]);self.receipt=row[5]
        self.binding_sha=p.digest(self.binding)
        self.policy=json.loads(p.canonical_json(dict(jobId=job_id,requestId=request_id,environment=environment,
            bindingSha256=self.binding_sha,claimSha256=p.digest(self.claim),inputRoot=str(input_root),
            inputRootIdentity=input_root_identity,maxTotal=max_total,maxObject=max_object)))
        self.path=enrollment.owner.root/'viewer-inputs.sqlite'
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        try:
            info=os.fstat(fd);named=self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or stat.S_IMODE(info.st_mode)!=0o600 or (info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino)):
                raise InputHeld('input_journal_identity_invalid')
            self._identity=(info.st_dev,info.st_ino);os.fsync(fd)
        finally:os.close(fd)
        rootfd=os.open(enrollment.owner.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(rootfd)
        finally:os.close(rootfd)
        with self._db(bootstrap=True) as db:
            objects=db.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").fetchall()
            if not objects:db.execute(POLICY_DDL);db.execute(MANIFEST_DDL);db.execute(OBJECT_DDL);db.execute(PLAN_DDL);db.execute(GRANT_DDL);db.execute(SUBMISSION_DDL)
            self._schema(db);value=p.canonical_json(self.policy).decode()
            existing=db.execute('SELECT value,mac_key FROM policy WHERE singleton=1').fetchone()
            if existing and existing[0]!=value:raise InputHeld('input_policy_changed')
            if not existing:
                if db.execute('SELECT 1 FROM manifest UNION ALL SELECT 1 FROM objects UNION ALL SELECT 1 FROM plan UNION ALL SELECT 1 FROM execution_grant UNION ALL SELECT 1 FROM synthetic_submission LIMIT 1').fetchone():
                    raise InputHeld('input_policy_missing')
                db.execute('INSERT INTO policy VALUES(1,?,?)',(value,secrets.token_hex(32)))

    @staticmethod
    def _schema(db):
        query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        expected=sqlite3.connect(':memory:')
        try:
            for sql in (POLICY_DDL,MANIFEST_DDL,OBJECT_DDL,PLAN_DDL,GRANT_DDL,SUBMISSION_DDL):expected.execute(sql)
            if db.execute(query).fetchall()!=expected.execute(query).fetchall():raise InputHeld('input_schema_changed')
        finally:expected.close()

    def _sidecars(self):
        for suffix in ('-journal','-wal','-shm'):
            path=Path(str(self.path)+suffix)
            try:info=path.lstat()
            except FileNotFoundError:continue
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>128*1024*1024):
                raise InputHeld('input_sidecar_invalid')

    @contextmanager
    def _db(self,*,bootstrap=False):
        if os.getpid()!=self._pid:raise InputHeld('input_process_changed')
        self.worker.owner.check();info=self.path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>64*1024*1024
                or (info.st_dev,info.st_ino)!=self._identity):raise InputHeld('input_journal_changed')
        self._sidecars();db=sqlite3.connect(self.path,timeout=5,isolation_level=None)
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if bootstrap and db.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise InputHeld('input_journal_invalid')
            if not bootstrap:
                self._schema(db);rows=db.execute('SELECT singleton,value,mac_key FROM policy').fetchall()
                if (len(rows)!=1 or rows[0][0]!=1 or rows[0][1]!=p.canonical_json(self.policy).decode()
                        or type(rows[0][2]) is not str or len(rows[0][2])!=64):raise InputHeld('input_policy_changed')
            yield db
            self.worker.owner.check();after=self.path.lstat()
            if (after.st_dev,after.st_ino)!=self._identity:raise InputHeld('input_journal_changed')
            self._sidecars();db.commit()
        except BaseException:db.rollback();raise
        finally:db.close()

    def _local_claim(self):
        with self.worker._db() as db:
            row=db.execute('SELECT request,receipt,claim,binding,slot_phase,slot_identity FROM intent WHERE singleton=1').fetchone()
        if (not row or row[0]!=self.request or row[1]!=self.receipt or row[2]!=p.canonical_json(self.claim).decode()
                or row[3]!=p.canonical_json(self.binding).decode() or row[4]!='committed' or not isinstance(row[5],str)):
            raise InputHeld('enrollment_changed')
        return row[5]

    @contextmanager
    def _gate(self):
        recorded=self._local_claim();c=self.worker.config
        with NodeGate(c['engineId']) as gate:
            state=self.worker._check_gate(gate);reservation=state['reservation']
            expected=dict(id=self.claim['jobId'],workerId=c['workerId'],nodeId=c['nodeId'],authorityId=c['authorityId'])
            if (not reservation or reservation['receiptId']!=self.receipt or reservation['job']!=expected
                    or reservation['completionDigest'] is not None):raise InputHeld('input_gate_changed')
            slot=open_slot(c['outputRoot'],c['outputRootIdentity'],self.binding_sha,c['outputBudget'],
                           max_budget=c['maxBudget'],create=False)
            try:
                actual=p.canonical_json(dict(root=_identity(slot._root),allocation=_identity(slot._fd),
                                             metadata=_identity(slot._meta))).decode()
                if actual!=recorded:raise InputHeld('output_slot_identity_changed')
                slot._check()
                yield gate
                slot._check()
                if self._local_claim()!=recorded:raise InputHeld('enrollment_changed')
                state=self.worker._check_gate(gate)
                if state['reservation']!=reservation:raise InputHeld('input_gate_changed')
            finally:slot.close()

    def _get(self,path,*,max_bytes,consumer=None):
        url=self.worker.config['origin']+path;watchdog=_TransferWatchdog(30 if consumer is None else 3600)
        request=Request(url,headers={'Authorization':'Bearer '+self.worker._bearer,
            'X-Claim-Token':self.claim['claimToken'],'Accept-Encoding':'identity'})
        try:
            opener=build_opener(ProxyHandler({}),_NoRedirect(),_DeadlineHTTPSHandler(watchdog,context=self.worker._ssl_context)).open
            response=opener(request,timeout=min(10,watchdog.remaining()))
        except HTTPError as error:
            watchdog.close();error.close();raise InputHeld('input_request_rejected') from None
        except Exception:
            watchdog.close();raise InputHeld('input_request_unavailable') from None
        try:
            if (response.status!=200 or response.geturl()!=url or any(len(response.headers.get_all(name,[]))>1
                    for name in ('Content-Length','Content-Type','Content-Encoding','Transfer-Encoding'))
                    or response.headers.get('Content-Encoding','identity')!='identity'
                    or response.headers.get('Transfer-Encoding')):raise InputHeld('input_response_invalid')
            expected_mime='application/json' if consumer is None else 'application/octet-stream'
            if response.headers.get('Content-Type','').split(';',1)[0].strip().lower()!=expected_mime:
                raise InputHeld('input_response_invalid')
            length=response.headers.get('Content-Length')
            if length is None or not length.isascii() or not length.isdecimal() or len(length)>16 or int(length)>max_bytes:
                raise InputHeld('input_length_invalid')
            length=int(length);count=0;chunks=[]
            while True:
                watchdog.remaining();data=response.read1(min(1024*1024,max_bytes-count+1))
                if not data:break
                count+=len(data)
                if count>max_bytes:raise InputHeld('input_length_invalid')
                if consumer is None:chunks.append(data)
                else:consumer(data)
            if count!=length:raise InputHeld('input_length_invalid')
            return length,b''.join(chunks) if consumer is None else None
        except InputHeld:raise
        except Exception:raise InputHeld('input_response_uncertain') from None
        finally:response.close();watchdog.close()

    def _prefix(self):return '/ltds/worker/v1/viewer-claims/'+self.claim['jobId']

    def fetch_plan(self):
        """Fetch and persist claim-bound options; never dispatch an engine."""
        with self._lock,self._gate():
            _,raw=self._get(self._prefix()+'/plan',max_bytes=131072)
            response=p.decode_submission(raw)
            p._closed(response,('jobId','viewerGeneration','pipelineSha256','plan'),'Viewer plan response')
            if (response['jobId']!=self.claim['jobId'] or type(response['viewerGeneration']) is not int
                or response['viewerGeneration']!=self.claim['viewerGeneration']
                or response['pipelineSha256']!=self.claim['pipelineSha256']):raise InputHeld('plan_identity_changed')
            plan=response['plan'];p._closed(plan,('options','outputs'),'Viewer plan')
            if direct_pipeline_digest(plan['options'],plan['outputs'])!=self.claim['pipelineSha256']:
                raise InputHeld('plan_digest_changed')
            encoded=p.canonical_json(plan).decode()
            with self._db() as db:
                saved=db.execute('SELECT value FROM plan WHERE singleton=1').fetchone()
                if saved and saved[0]!=encoded:raise InputHeld('plan_replay_changed')
                if not saved:db.execute('INSERT INTO plan VALUES(1,?)',(encoded,))
            return plan

    def request_execution_grant(self):
        """Request a synthetic grant only after rechecking every retained input."""
        with self._lock:
            self.fetch_plan();self.freeze_inventory()
            with self._gate():
                with self._db() as db:
                    manifest=db.execute('SELECT phase,digest,count,total FROM manifest WHERE singleton=1').fetchone()
                    self._verify_inventory_rows(db,manifest)
                    rows=db.execute('SELECT entry,phase,slot_key,slot_identity FROM objects ORDER BY ordinal').fetchall()
                for encoded,phase,key,recorded in rows:
                    entry=p.decode_submission(encoded)
                    expected_key=p.digest(dict(purpose='viewer-input-slot-v1',bindingSha256=self.binding_sha,objectId=entry['objectId']))
                    if phase!='verified' or key!=expected_key or not isinstance(recorded,str):raise InputHeld('input_not_verified')
                    slot=open_slot(self.policy['inputRoot'],self.policy['inputRootIdentity'],key,entry['bytes'],
                                   max_budget=self.policy['maxObject'],create=False)
                    try:
                        actual=p.canonical_json(dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))).decode()
                        if actual!=recorded or self._hash_slot(slot,entry['bytes'])!=entry['sha256']:
                            raise InputHeld('input_identity_or_hash_changed')
                    finally:slot.close()
                slot_identity=json.loads(self._local_claim())
                request=dict(enrollmentReceipt=p.digest(dict(binding=self.binding,slotIdentity=slot_identity)),
                    inputInventorySha256=self.claim['inputInventorySha256'],pipelineSha256=self.claim['pipelineSha256'],
                    inputCount=manifest[2],inputBytes=manifest[3])
                grant=self.worker._request(request,job_id=self.claim['jobId'],claim_token=self.claim['claimToken'],kind='execution-grant')
                expected=dict(schemaVersion=1,kind='synthetic_execution_grant',authorityId=self.claim['authorityId'],jobId=self.claim['jobId'],
                    viewerTaskId=self.claim['viewerTaskId'],viewerGeneration=self.claim['viewerGeneration'],
                    environment=self.claim['environment'],nodeId=self.claim['nodeId'],workerId=self.claim['workerId'],
                    workerInstanceId=self.claim['workerInstanceId'],engineTaskId=self.claim['engineTaskId'],
                    inputInventorySha256=request['inputInventorySha256'],pipelineSha256=request['pipelineSha256'],taskNameSha256=self.claim['taskNameSha256'],
                    enrollmentReceipt=request['enrollmentReceipt'],inputCount=request['inputCount'],inputBytes=request['inputBytes'])
                p._closed(grant,(*expected.keys(),'taskName'),'execution grant')
                if (any(grant[k]!=v or type(grant[k]) is not type(v) for k,v in expected.items()) or type(grant['taskName']) is not str
                        or len(grant['taskName'])>256 or any(ord(char)<32 for char in grant['taskName'])
                        or p.digest(grant['taskName'])!=self.claim['taskNameSha256']):
                    raise InputHeld('execution_grant_changed')
                encoded=p.canonical_json(grant).decode();request_text=p.canonical_json(request).decode()
                with self._db() as db:
                    old=db.execute('SELECT request,value FROM execution_grant WHERE singleton=1').fetchone()
                    if old and old!=(request_text,encoded):raise InputHeld('execution_grant_replay_changed')
                    if not old:db.execute('INSERT INTO execution_grant VALUES(1,?,?)',(request_text,encoded))
                return grant

    def _verify_inventory_rows(self,db,manifest):
        key=bytes.fromhex(db.execute('SELECT mac_key FROM policy WHERE singleton=1').fetchone()[0])
        digest=hashlib.sha256();digest.update(b'[');count=total=photos=0
        for ordinal,object_id,encoded,mac in db.execute('SELECT ordinal,object_id,entry,mac FROM objects ORDER BY ordinal'):
            if ordinal!=count or not hmac.compare_digest(hmac.new(key,encoded.encode(),hashlib.sha256).hexdigest(),mac):
                raise InputHeld('input_metadata_changed')
            item=p.decode_submission(encoded);p._closed(item,('ordinal','objectId','name','bytes','sha256','isPhoto'),'inventory entry')
            if item['ordinal']!=ordinal or item['objectId']!=object_id:raise InputHeld('input_metadata_changed')
            entry={name:item[name] for name in ('name','bytes','sha256','isPhoto')}
            if count:digest.update(b',')
            digest.update(p.canonical_json(entry));count+=1;total+=item['bytes'];photos+=int(item['isPhoto'])
        digest.update(b']')
        if (count!=self.claim['objectCount'] or count!=manifest[2] or total!=manifest[3] or total!=self.claim['inputBytes'] or photos!=self.claim['imageCount']
                or digest.hexdigest()!=self.claim['inputInventorySha256']):raise InputHeld('inventory_digest_mismatch')

    def freeze_inventory(self):
        """Authenticate all pages and commit a full ordered digest before use."""
        if os.getpid()!=self._pid:raise InputHeld('input_process_changed')
        with self._lock,self._gate():
            with self._db() as db:
                current=db.execute('SELECT phase,digest,count,total FROM manifest WHERE singleton=1').fetchone()
                if current:
                    if current[0]!='verified' or current[1]!=self.claim['inputInventorySha256']:
                        raise InputHeld('inventory_requires_inspection')
                    self._verify_inventory_rows(db,current);self._verified_session=True
                    return dict(state='inventory_verified_no_dispatch',count=current[2],total=current[3])
                db.execute('INSERT INTO manifest VALUES(1,?,?,NULL,NULL)',('fetching',self.claim['inputInventorySha256']))
                key=bytes.fromhex(db.execute('SELECT mac_key FROM policy WHERE singleton=1').fetchone()[0])
            digest=hashlib.sha256();digest.update(b'[');first=True;cursor=0;count=0;total=0;photos=0
            while cursor is not None:
                _,raw=self._get(self._prefix()+'/inventory/'+str(cursor),max_bytes=65536)
                page=p.decode_submission(raw)
                p._closed(page,('jobId','viewerGeneration','inputInventorySha256','start','entries','nextCursor'),'inventory page')
                if (page['jobId']!=self.claim['jobId'] or page['viewerGeneration']!=self.claim['viewerGeneration']
                        or page['inputInventorySha256']!=self.claim['inputInventorySha256'] or page['start']!=cursor
                        or type(page['entries']) is not list or not 1<=len(page['entries'])<=100):
                    raise InputHeld('inventory_page_invalid')
                rows=[]
                for item in page['entries']:
                    p._closed(item,('ordinal','objectId','name','bytes','sha256','isPhoto'),'inventory entry')
                    if type(item['ordinal']) is not int or item['ordinal']!=count or type(item['isPhoto']) is not bool:
                        raise InputHeld('inventory_sequence_invalid')
                    TaskStore._filename(item['name']);p._integer(item['bytes'],'input bytes');p._hash(item['sha256'],'input hash');p._hash(item['objectId'],'input object')
                    if (item['bytes']>self.policy['maxObject'] or item['isPhoto']!=(Path(item['name']).suffix.lower() in PHOTO_SUFFIXES)):
                        raise InputHeld('inventory_entry_invalid')
                    entry={name:item[name] for name in ('name','bytes','sha256','isPhoto')}
                    if item['objectId']!=p.digest(dict(jobId=self.claim['jobId'],generation=self.claim['viewerGeneration'],ordinal=count,entry=entry)):
                        raise InputHeld('inventory_identity_invalid')
                    if not first:digest.update(b',')
                    first=False;digest.update(p.canonical_json(entry));total+=item['bytes'];photos+=int(item['isPhoto'])
                    if total>self.policy['maxTotal'] or count>=100000:raise InputHeld('inventory_budget_exceeded')
                    encoded=p.canonical_json(item).decode();mac=hmac.new(key,encoded.encode(),hashlib.sha256).hexdigest()
                    rows.append((count,item['objectId'],encoded,mac,'new',None,None));count+=1
                next_cursor=page['nextCursor']
                if next_cursor is not None and (type(next_cursor) is not int or next_cursor!=count or len(page['entries'])!=100):
                    raise InputHeld('inventory_cursor_invalid')
                with self._db() as db:db.executemany('INSERT INTO objects VALUES(?,?,?,?,?,?,?)',rows)
                cursor=next_cursor
            digest.update(b']')
            if (count!=self.claim['objectCount'] or digest.hexdigest()!=self.claim['inputInventorySha256'] or total!=self.claim['inputBytes']
                    or photos!=self.claim['imageCount']):raise InputHeld('inventory_digest_mismatch')
            with self._db() as db:
                db.execute("UPDATE manifest SET phase='verified',count=?,total=? WHERE singleton=1 AND phase='fetching'",(count,total))
                self._verify_inventory_rows(db,('verified',self.claim['inputInventorySha256'],count,total))
            self._verified_session=True
            return dict(state='inventory_verified_no_dispatch',count=count,total=total)

    def transfer(self,object_id):
        """Verify one source into its own allocated inode; uncertain writes hold."""
        if os.getpid()!=self._pid:raise InputHeld('input_process_changed')
        p._hash(object_id,'object identity')
        with self._lock,self._gate():
            if not self._verified_session:raise InputHeld('verified_inventory_required')
            with self._db() as db:
                manifest=db.execute('SELECT phase,digest FROM manifest WHERE singleton=1').fetchone()
                row=db.execute('SELECT ordinal,entry,mac,phase,slot_key,slot_identity FROM objects WHERE object_id=?',(object_id,)).fetchone()
                key=bytes.fromhex(db.execute('SELECT mac_key FROM policy WHERE singleton=1').fetchone()[0])
                if not manifest or manifest!=('verified',self.claim['inputInventorySha256']) or not row:
                    raise InputHeld('verified_inventory_required')
                if not hmac.compare_digest(hmac.new(key,row[1].encode(),hashlib.sha256).hexdigest(),row[2]):
                    raise InputHeld('input_metadata_changed')
                entry=p.decode_submission(row[1]);slot_key=p.digest(dict(purpose='viewer-input-slot-v1',bindingSha256=self.binding_sha,objectId=object_id))
                if entry['objectId']!=object_id or row[4] not in (None,slot_key):raise InputHeld('input_identity_changed')
                if row[3]=='new':
                    db.execute("UPDATE objects SET phase='creating',slot_key=? WHERE object_id=?",(slot_key,object_id));create=True
                elif row[3]=='verified':create=False
                else:raise InputHeld('input_requires_inspection')
                recorded=row[5]
            slot=open_slot(self.policy['inputRoot'],self.policy['inputRootIdentity'],slot_key,entry['bytes'],
                           max_budget=self.policy['maxObject'],create=create)
            try:
                actual=p.canonical_json(dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))).decode()
                if not create:
                    if recorded!=actual:raise InputHeld('input_slot_identity_changed')
                    if self._hash_slot(slot,entry['bytes'])!=entry['sha256']:raise InputHeld('input_hash_changed')
                    return dict(state='input_verified_no_dispatch',objectId=object_id)
                with self._db() as db:
                    if db.execute("UPDATE objects SET phase='fetching',slot_identity=? WHERE object_id=? AND phase='creating'",(actual,object_id)).rowcount!=1:
                        raise InputHeld('input_state_changed')
                digest=hashlib.sha256();offset=0
                def write(data):
                    nonlocal offset
                    slot.write_at(offset,data);digest.update(data);offset+=len(data)
                length,_=self._get(self._prefix()+'/objects/'+object_id,max_bytes=entry['bytes'],consumer=write)
                if length!=entry['bytes'] or offset!=entry['bytes'] or digest.hexdigest()!=entry['sha256']:
                    raise InputHeld('input_hash_mismatch')
                slot.sync()
                if self._hash_slot(slot,entry['bytes'])!=entry['sha256']:raise InputHeld('input_hash_mismatch')
                with self._db() as db:
                    if db.execute("UPDATE objects SET phase='verified' WHERE object_id=? AND phase='fetching' AND slot_identity=?",(object_id,actual)).rowcount!=1:
                        raise InputHeld('input_state_changed')
                slot._check()
                return dict(state='input_verified_no_dispatch',objectId=object_id)
            finally:slot.close()

    @staticmethod
    def _hash_slot(slot,length):
        slot._check();digest=hashlib.sha256();offset=0
        while offset<length:
            data=os.pread(slot._fd,min(1024*1024,length-offset),offset)
            if not data:raise InputHeld('input_file_incomplete')
            digest.update(data);offset+=len(data)
        slot._check();return digest.hexdigest()
