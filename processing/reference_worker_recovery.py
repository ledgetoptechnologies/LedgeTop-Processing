"""Explicit original-claim recovery; no engine or scheduling operations.

Durable requests reconcile exact receipts before same-ID replay. A historical
receipt never authorizes execution: only a fresh ProductionParentGuard can.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import Request

from . import reference_protocol as p
from .reference_parent_guard import ProductionParentGuard,ClaimStateClient,DriverClaimTokens,_private,_readonly_sqlite,_private_json,_existing_gate,exact
from .reference_pipeline import PipelineJournal
from .reference_recovery import STATE_PROTOCOL,ClaimRecoveryService
from .reference_sender import decode_receipt
from .scratch_policy import ScratchLedger


class WorkerRecoveryHeld(ValueError):pass


def encoded(value):return p.canonical_json(value).decode()
def detached(value):return json.loads(p.canonical_json(value))


@dataclass(frozen=True)
class RecoveryPolicy:
    per_job_attempts:int=8
    global_attempts:int=64
    max_row_bytes:int=8192
    max_serialized_bytes:int=131072
    max_db_pages:int=64
    preparation_allowance:int=0

    def __post_init__(self):
        for key,value in asdict(self).items():p._integer(value,'bounded recovery policy',allow_zero=key=='preparation_allowance')
        if (not 1<=self.per_job_attempts<=self.global_attempts<=64 or not 16<=self.max_db_pages<=256
                or self.global_attempts>self.max_db_pages or not 4096<=self.max_row_bytes<=16384
                or not self.max_row_bytes<=self.max_serialized_bytes<=self.max_db_pages*2048):raise WorkerRecoveryHeld('bounded_recovery_policy_required')
        p._integer(self.control_allowance,'total recovery control allowance')
    @property
    def sha256(self):return p.digest(asdict(self))
    @property
    def control_allowance(self):return 4*self.max_db_pages*4096+65536+self.preparation_allowance


SCHEMA=(
 'CREATE TABLE worker_recovery_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
 '''CREATE TABLE worker_recovery_attempts(request_id TEXT PRIMARY KEY,job TEXT NOT NULL,epoch TEXT NOT NULL,revision INTEGER NOT NULL,
    body TEXT NOT NULL,evidence TEXT NOT NULL,phase TEXT NOT NULL,receipt TEXT,resolved_epoch TEXT,
    UNIQUE(job,epoch,revision))''')


class OriginalClaimRecovery:
    def __init__(self,path,*,parent_guard,policy=RecoveryPolicy()):
        from ops.reference_worker_daemon import DaemonStateLock
        if type(parent_guard) is not ProductionParentGuard or type(policy) is not RecoveryPolicy:raise WorkerRecoveryHeld('actual_parent_guard_required')
        guard=parent_guard
        if (type(guard.journal) is not PipelineJournal or type(guard.ledger) is not ScratchLedger or type(guard.owner) is not DaemonStateLock
                or type(guard.client) is not ClaimStateClient or type(guard.tokens) is not DriverClaimTokens):raise WorkerRecoveryHeld('actual_original_claim_dependencies_required')
        self.guard,self.journal,self.ledger,self.owner,self.client,self.tokens=guard,guard.journal,guard.ledger,guard.owner,guard.client,guard.tokens
        self.policy=policy
        self.path=Path(path).absolute();self.root=self.path.parent;self.lock_path=self.path.with_name(self.path.name+'.lock')
        guard._check();_private(self.root,directory=True)
        if self.root!=self.journal.root or self.root.stat().st_dev!=self.ledger.root.stat().st_dev:raise WorkerRecoveryHeld('shared_recovery_capacity_domain_required')
        if self.journal.policy.recovery_control_budget!=policy.control_allowance:raise WorkerRecoveryHeld('exact_additive_recovery_control_budget_required')
        known=(self.journal.path,self.journal.lock_path,Path(self.ledger.path).absolute(),self.tokens.path,self.root/'worker-daemon.lock')
        if any(path in known or path in {Path(str(k)+s) for k in known for s in ('-journal','-wal','-shm','.lock')} for path in (self.path,self.lock_path)):raise WorkerRecoveryHeld('dedicated_recovery_paths_required')
        self._dependencies=(guard,self.journal,self.ledger,self.owner,self.client,self.tokens)
        self._pid=os.getpid();self._mutex=threading.Lock();self._busy=False;self._initialized=False
        self._root_identity=_private(self.root,directory=True)[1];self._files={}
        self._scope=dict(protocol='ltds-original-claim-recovery-worker-v2',parentPolicy=self.guard.binding,policy=asdict(policy),
            transportPolicy=self.client.policy.sha256,root=str(self.root),rootIdentity=list(self._root_identity),path=str(self.path),lockPath=str(self.lock_path),pipelineStoragePageCap=self.journal.storage_page_cap)
        self.scope_sha256=p.digest(self._scope)
        publications=self.ledger.control_scope_publications(self.scope_sha256,self.policy.global_attempts)
        try:
            if self.path.exists() or self.lock_path.exists():self._attach(readonly=True)
            elif publications:raise WorkerRecoveryHeld('published_recovery_journal_missing')
        except WorkerRecoveryHeld:raise
        except Exception:raise WorkerRecoveryHeld('recovery_journal_attach_held') from None

    def _attach(self,*,new_charge=None,readonly=False):
        publications=self.ledger.control_scope_publications(self.scope_sha256,self.policy.global_attempts)
        new=new_charge is not None and not self.path.exists() and not self.lock_path.exists()
        if readonly and new:raise WorkerRecoveryHeld('readonly_recovery_attach_required')
        if new and publications:raise WorkerRecoveryHeld('cannot_replace_published_recovery_scope')
        for path in (self.path,self.lock_path):
            if path.resolve()!=path:raise WorkerRecoveryHeld('private_recovery_file_required')
            if new:
                fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
                try:self._files[path]=_private(path)[1];os.fsync(fd)
                finally:os.close(fd)
            else:self._files[path]=_private(path)[1]
        self._disk_census()
        if new:
            anchor=new_charge['reservationId']
            self._binding=encoded(dict(scope=self._scope,files={str(k):list(v) for k,v in self._files.items()},anchorReservation=anchor,anchorChargeId=new_charge['chargeId']))
            with self._db() as db:
                for sql in SCHEMA:db.execute(sql)
                self._schema(db);db.execute('INSERT INTO worker_recovery_binding VALUES(1,?)',(self._binding,))
        else:
            with _readonly_sqlite(self.path) as db:
                self._format(db);self._schema(db);self._census(db)
                row=db.execute('SELECT value FROM worker_recovery_binding WHERE id=1').fetchone()
                if row is None or len(row[0].encode())>self.policy.max_row_bytes:raise WorkerRecoveryHeld('exact_recovery_binding_required')
                binding=_private_json(row[0]);p._closed(binding,('scope','files','anchorReservation','anchorChargeId'),'recovery control owner')
                expected=dict(scope=self._scope,files={str(k):list(v) for k,v in self._files.items()},anchorReservation=binding['anchorReservation'],anchorChargeId=binding['anchorChargeId'])
                if not exact(binding,expected):raise WorkerRecoveryHeld('recovery_scope_inode_or_policy_changed')
                self._binding=encoded(expected)
            anchor=self.ledger.read_control_charge(binding['anchorReservation'])
            if anchor is None or anchor['chargeId']!=binding['anchorChargeId'] or anchor['scopeSha256']!=self.scope_sha256:raise WorkerRecoveryHeld('typed_recovery_anchor_required')
            if not publications:
                with _readonly_sqlite(self.path) as db:
                    if db.execute('SELECT COUNT(*) FROM worker_recovery_attempts').fetchone()[0]:raise WorkerRecoveryHeld('unpublished_recovery_history_not_resettable')
        if any(value!=p.digest(_private_json(self._binding)) for value in publications):raise WorkerRecoveryHeld('prior_recovery_publication_identity_changed')
        if not readonly:self._sync_directories()
        self._initialized=True

    def _sync_directories(self):
        for path in (self.root,self.root.parent):
            fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)

    def _check(self):
        if self._pid!=os.getpid():raise WorkerRecoveryHeld('recovery_owner_process_changed')
        if (any(a is not b for a,b in zip((self.guard,self.journal,self.ledger,self.owner,self.client,self.tokens),self._dependencies))
                or self.guard.journal is not self.journal or self.guard.ledger is not self.ledger or self.guard.owner is not self.owner
                or self.guard.client is not self.client or self.guard.tokens is not self.tokens
                or self._scope['policy']!=asdict(self.policy) or p.digest(self._scope)!=self.scope_sha256
                or _private(self.root,directory=True)[1]!=self._root_identity
                or any(_private(path)[1]!=identity for path,identity in self._files.items())):raise WorkerRecoveryHeld('recovery_owner_changed')
        self.guard._check()

    @staticmethod
    def _schema(db):
        expected=[('index','sqlite_autoindex_worker_recovery_attempts_'+str(number),'worker_recovery_attempts',None) for number in (1,2)]
        expected += [('table','worker_recovery_binding','worker_recovery_binding',SCHEMA[0]),('table','worker_recovery_attempts','worker_recovery_attempts',SCHEMA[1])]
        actual=[tuple(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        if actual!=sorted(expected,key=lambda row:(row[0],row[1])):raise WorkerRecoveryHeld('dedicated_recovery_schema_required')

    @contextmanager
    def _operation(self):
        self._check()
        if not self._mutex.acquire(blocking=False):raise WorkerRecoveryHeld('recovery_operation_active')
        fd=None
        try:
            if self._initialized:
                fd=os.open(self.lock_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                import fcntl
                if (os.fstat(fd).st_dev,os.fstat(fd).st_ino)!=self._files[self.lock_path]:raise WorkerRecoveryHeld('recovery_lock_changed')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise WorkerRecoveryHeld('recovery_operation_active') from None
            with self.journal._operation() as lease:
                self._lease=lease
                try:yield;self._check()
                finally:self._lease=None
        finally:
            if fd is not None:os.close(fd)
            self._mutex.release()

    _ROW_BYTES=" + ".join("length(CAST(COALESCE("+name+",'') AS BLOB))" for name in ('request_id','job','epoch','revision','body','evidence','phase','receipt','resolved_epoch'))

    def _census(self,db,*,job=None,extra_rows=0,extra_bytes=0):
        size,count,largest=db.execute('SELECT COALESCE(SUM('+self._ROW_BYTES+'),0),COUNT(*),COALESCE(MAX('+self._ROW_BYTES+'),0) FROM worker_recovery_attempts').fetchone()
        binding=db.execute('SELECT COALESCE(SUM(length(CAST(value AS BLOB))),0),COALESCE(MAX(length(CAST(value AS BLOB))),0) FROM worker_recovery_binding').fetchone()
        if count+extra_rows>self.policy.global_attempts or size+binding[0]+extra_bytes>self.policy.max_serialized_bytes or max(largest,binding[1])>self.policy.max_row_bytes:raise WorkerRecoveryHeld('recovery_metadata_cap_reached')
        if job is not None and db.execute('SELECT COUNT(*) FROM worker_recovery_attempts WHERE job=?',(job,)).fetchone()[0]+extra_rows>self.policy.per_job_attempts:raise WorkerRecoveryHeld('recovery_job_attempt_cap_reached')

    def _format(self,db):
        if db.execute('PRAGMA page_size').fetchone()[0]!=4096 or db.execute('PRAGMA page_count').fetchone()[0]>self.policy.max_db_pages:raise WorkerRecoveryHeld('recovery_page_cap_exceeded')

    @contextmanager
    def _db(self,*,readonly=False):
        self._check()
        if self.path.stat().st_size:
            with _readonly_sqlite(self.path):pass
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True) if readonly else sqlite3.connect(self.path,timeout=5)
        db.row_factory=sqlite3.Row
        try:
            if readonly:db.execute('BEGIN')
            else:
                db.execute('PRAGMA page_size=4096')
                if db.execute('PRAGMA max_page_count='+str(self.policy.max_db_pages)).fetchone()[0]!=self.policy.max_db_pages:raise WorkerRecoveryHeld('recovery_page_cap_exceeded')
                db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if getattr(self,'_initialized',False):
                self._schema(db);row=db.execute('SELECT value FROM worker_recovery_binding WHERE id=1').fetchone()
                if not row or row[0]!=self._binding:raise WorkerRecoveryHeld('recovery_binding_changed')
                self._census(db)
            self._format(db)
            yield db;self._check();self._census(db)
            if not readonly:db.commit()
        except BaseException:
            if not readonly:db.rollback()
            raise
        finally:db.close()

    def _charge_identity(self,parent):
        return dict(reservation_id=parent['reservationId'],charge_id='recovery-control-'+p.digest(dict(scope=self.scope_sha256,parent=parent))[:32],
            scope_sha256=self.scope_sha256,parent_binding_sha256=p.digest(parent))

    def _prepared(self,parent):
        receipt=self.ledger.read_control_charge(parent['reservationId']);expected=self._charge_identity(parent)
        if (not self._initialized or receipt is None or any(receipt[key]!=value for key,value in (('chargeId',expected['charge_id']),('scopeSha256',self.scope_sha256),('parentBindingSha256',p.digest(parent)),('allowanceBytes',self.policy.control_allowance)))
                or receipt['publicationSha256']!=p.digest(_private_json(self._binding))):raise WorkerRecoveryHeld('published_preexecution_recovery_control_required')
        if any(value!=receipt['publicationSha256'] for value in self.ledger.control_scope_publications(self.scope_sha256,self.policy.global_attempts)):raise WorkerRecoveryHeld('recovery_scope_publication_disagrees')
        self._disk_census()
        return receipt

    def _disk_census(self):
        if self.path.stat().st_size>self.policy.max_db_pages*4096:raise WorkerRecoveryHeld('recovery_file_cap_exceeded')
        rollback=Path(str(self.path)+'-journal')
        if rollback.exists():
            _private(rollback)
            if rollback.stat().st_size>2*self.policy.max_db_pages*4096:raise WorkerRecoveryHeld('recovery_rollback_cap_exceeded')
        if any(Path(str(self.path)+suffix).exists() for suffix in ('-wal','-shm')):raise WorkerRecoveryHeld('rollback_recovery_journal_required')

    def prepare(self,parent_binding):
        """Explicit pre-stage typed charge and exact metadata publication."""
        try:
            with self._operation():
                parent=self.guard._parent(parent_binding)
                # A published exact preparation is historical and idempotent.
                old=self.ledger.read_control_charge(parent['reservationId'])
                if old is not None and old['publicationSha256'] is not None:
                    self._prepared(parent);return old
                _,_,stages,_,_,_=self.journal._read(parent['jobId'])
                if any(stage['state']!='new' or stage['intent'] is not None or stage['proof'] is not None for stage in stages.values()):raise WorkerRecoveryHeld('recovery_preparation_before_any_stage_required')
                self.guard.check_preexecution_recovery(parent,lease=self._lease)
                args=self._charge_identity(parent)
                with _existing_gate(parent['engineId']) as gate:
                    self.guard._fence(gate,parent);self.guard._reservation(parent,mutation=True)
                    receipt=self.ledger.prepare_control_charge(**args,expected_allocated=parent['inputBytes'],allowance_bytes=self.policy.control_allowance,maximum_scope_charges=self.policy.global_attempts)
                    if not self._initialized:self._attach(new_charge=receipt)
                    self._disk_census()
                    self._sync_directories()
                    receipt=self.ledger.publish_control_charge(**args,publication_descriptor=_private_json(self._binding))
                self._prepared(parent);return receipt
        except WorkerRecoveryHeld:raise
        except Exception:raise WorkerRecoveryHeld('preexecution_recovery_preparation_held') from None

    @contextmanager
    def _facts(self,parent):
        """Real existing physical lease; read-only reservation and original token."""
        self._check()
        with _readonly_sqlite(Path(self.ledger.path).absolute()) as db:
            row=db.execute('SELECT state FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone()
        preexecution=row is not None and row[0]=='reserved'
        if preexecution:self.guard.check_preexecution_recovery(parent,lease=self._lease)
        with _existing_gate(parent['engineId']) as gate:
            self.guard._fence(gate,parent);self.guard._reservation(parent,mutation=False,recovering=not preexecution)
            token=self.tokens.token(parent)
            with _readonly_sqlite(self.tokens.path) as db:
                row=db.execute('SELECT data FROM driver_jobs WHERE job=?',(parent['jobId'],)).fetchone()
                if row is None:raise WorkerRecoveryHeld('original_transfer_required')
                transfer=_private_json(row[0])['reservation']
            p._closed(transfer,('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce'),'original transfer')
            p._slug(transfer['reservationId'],'original reservation');p._slug(transfer['nonce'],'original nonce')
            p._integer(transfer['generation'],'original generation');p._integer(transfer['byteBudget'],'original byte budget');p._string(transfer['expiresAt'],'original expiry',40)
            if (not exact(transfer['generation'],parent['generation']) or transfer['reservationId']!=parent['reservationId'] or transfer['manifestSha256']!=parent['manifestSha256']):raise WorkerRecoveryHeld('original_transfer_binding_changed')
            with _readonly_sqlite(Path(self.ledger.path).absolute()) as db:
                reserved=dict(db.execute('SELECT * FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone())
            evidence=dict(schemaVersion=1,protocol='ltds-local-recovery-evidence-v1',parentBindingSha256=p.digest(parent),
                physicalFence=gate.snapshot(),reservation={key:reserved[key] for key in ('id','job','generation','environment','state','allocated','remaining','expires','terminal_at')},
                settingsSha256=p.digest(_private_json(reserved['settings'])),ownerRootIdentity=list(self._root_identity))
            yield evidence,token,p.digest(transfer)
            self._check();self.guard._fence(gate,parent);self.guard._reservation(parent,mutation=False,recovering=not preexecution)

    def _readonly_guard(self,parent):
        try:
            if self.guard.check(parent,mutation=False) is not None:raise WorkerRecoveryHeld('actual_readonly_parent_required')
        except Exception:
            self.guard.check_preexecution_recovery(parent,lease=self._lease)

    def _http(self,method,parent,operation,token,body=None,*,missing=False):
        self._check();policy=self.client.policy
        url=policy.origin+'/ltds/worker/v1/jobs/'+parent['jobId']+'/'+operation
        payload=None if body is None else p.canonical_json(body)
        if payload is not None and len(payload)>policy.max_response_bytes:raise WorkerRecoveryHeld('bounded_recovery_request_required')
        request=Request(url,data=payload,method=method,headers={'Authorization':'Bearer '+self.client.credential._secret,'X-Claim-Token':token,
            'Content-Type':'application/json','Accept':'application/json','Accept-Encoding':'identity','Cache-Control':'no-store','Pragma':'no-cache'})
        response=None;deadline=time.monotonic()+policy.timeout_seconds
        try:
            try:response=self.client._open(request,timeout=policy.timeout_seconds)
            except HTTPError as error:response=error
            if (response.geturl()!=url or response.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json'
                    or response.headers.get('Content-Encoding','identity').lower()!='identity' or response.headers.get('Cache-Control','').lower()!='no-store'):raise WorkerRecoveryHeld('recovery_response_unavailable')
            lengths=response.headers.get_all('Content-Length',[])
            if len(lengths)!=1 or not lengths[0].isascii() or not lengths[0].isdecimal() or int(lengths[0])>policy.max_response_bytes:raise WorkerRecoveryHeld('bounded_recovery_response_required')
            length=int(lengths[0]);chunks=[];total=0;read=getattr(response,'read1',response.read)
            while total<length:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise WorkerRecoveryHeld('recovery_response_deadline')
                wire=response.fp if isinstance(response,HTTPError) else response
                sock=getattr(getattr(getattr(wire,'fp',None),'raw',None),'_sock',None)
                if sock is None:raise WorkerRecoveryHeld('bounded_recovery_response_required')
                sock.settimeout(remaining);chunk=read(min(16384,length-total))
                if not chunk:raise WorkerRecoveryHeld('truncated_recovery_response')
                chunks.append(chunk);total+=len(chunk)
            if time.monotonic()>=deadline:raise WorkerRecoveryHeld('recovery_response_deadline')
            value=decode_receipt(b''.join(chunks));self._check()
            if missing and response.status==404 and value=={'error':'receipt_unavailable'}:return None
            if response.status!=200:raise WorkerRecoveryHeld('recovery_request_rejected')
            return value
        except WorkerRecoveryHeld:raise
        except Exception:raise WorkerRecoveryHeld('recovery_transport_unavailable') from None
        finally:
            if response is not None:response.close()

    def _state(self,parent,token,transfer):
        value=self._http('GET',parent,'recovery-state',token)
        p._closed(value,('schemaVersion','protocol','authorityId','coordinatorEpoch','claim','physicalBinding','reservation','node','eligible'),'recovery state')
        if type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['protocol']!=STATE_PROTOCOL or value['authorityId']!=parent['authorityId'] or type(value['eligible']) is not bool:raise WorkerRecoveryHeld('recovery_state_binding_changed')
        p._uuid(value['coordinatorEpoch'],'recovery epoch')
        claim=value['claim']
        from .reference_parent_guard import validate_claim_fields
        validate_claim_fields(claim)
        expected=dict(schemaVersion=1,protocol='ltds-parent-claim-v1',**{key:parent[key] for key in ('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')})
        if any(not exact(claim[k],v) for k,v in expected.items()) or claim['executionPhase']!='processing':raise WorkerRecoveryHeld('recovery_claim_changed')
        physical=dict(node_id=parent['nodeId'],engine_id=parent['engineId'],authority_id=parent['authorityId'],coordinator_origin=parent['coordinatorOrigin'],gate_generation=parent['gateGeneration'],output_budget=parent['outputBudget'])
        if not exact(value['physicalBinding'],physical) or not exact(value['reservation'],dict(reservationId=parent['reservationId'],transferRequestSha256=transfer,inputBytes=parent['inputBytes'])):raise WorkerRecoveryHeld('recovery_original_reservation_changed')
        node=value['node'];p._closed(node,('id','state','revision','planId'),'recovery node');p._integer(node['revision'],'node revision',allow_zero=True)
        if node['id']!=parent['nodeId'] or node['state'] not in ('active','scheduled','draining','updating','held','failed') or node['planId'] is not None and type(node['planId']) is not str:raise WorkerRecoveryHeld('recovery_node_state_changed')
        if value['eligible'] and (claim['status']!='held' or claim['heldReason']!='coordinator_restart' or node['state'] not in ('active','scheduled','draining')):raise WorkerRecoveryHeld('recovery_eligibility_changed')
        return value

    def _row(self,parent):
        with self._db(readonly=True) as db:
            self._census(db,job=parent['jobId'])
            rows=[dict(row) for row in db.execute('SELECT * FROM worker_recovery_attempts WHERE job=? LIMIT ?',(parent['jobId'],self.policy.per_job_attempts+1))]
        pending=[]
        for row in rows:
            request=self._request(row,parent)
            if row['phase']=='issued':
                if row['receipt'] is not None or row['resolved_epoch'] is not None:raise WorkerRecoveryHeld('unresolved_recovery_record_changed')
                pending.append(row)
            elif row['phase']=='receipted':
                if row['resolved_epoch'] is not None or row['receipt']!=encoded(ClaimRecoveryService._make_receipt(request)):raise WorkerRecoveryHeld('historical_recovery_receipt_changed')
            elif row['phase'] in ('absent_prior_epoch','absent_prior_revision'):
                if row['receipt'] is not None:raise WorkerRecoveryHeld('retired_recovery_record_changed')
                resolution=_private_json(row['resolved_epoch']);p._closed(resolution,('coordinatorEpoch','nodeRevision','eligible','claimStatus','heldReason'),'recovery retirement')
                p._uuid(resolution['coordinatorEpoch'],'retirement epoch');p._integer(resolution['nodeRevision'],'retirement revision',allow_zero=True)
                if resolution['eligible'] is not True or resolution['claimStatus']!='held' or resolution['heldReason']!='coordinator_restart':raise WorkerRecoveryHeld('retirement_did_not_fence_old_attempt')
                if row['phase']=='absent_prior_epoch' and resolution['coordinatorEpoch']==request['coordinatorEpoch']:raise WorkerRecoveryHeld('retirement_epoch_not_advanced')
                if row['phase']=='absent_prior_revision' and (resolution['coordinatorEpoch']!=request['coordinatorEpoch'] or resolution['nodeRevision']<=request['expectedNodeRevision']):raise WorkerRecoveryHeld('retirement_revision_not_advanced')
            else:raise WorkerRecoveryHeld('recovery_attempt_phase_changed')
        if len(pending)>1:raise WorkerRecoveryHeld('multiple_unresolved_recovery_attempts')
        return pending[0] if pending else None

    def _request(self,row,parent):
        body=ClaimRecoveryService._request(_private_json(row['body']));evidence=_private_json(row['evidence'])
        if (row['job']!=parent['jobId'] or row['request_id']!=body['requestId'] or not exact(row['revision'],body['expectedNodeRevision'])
                or row['epoch']!=body['coordinatorEpoch'] or not exact(body['parentBinding'],parent) or body['workerEvidenceSha256']!=p.digest(evidence)
                or evidence['parentBindingSha256']!=p.digest(parent)):raise WorkerRecoveryHeld('durable_recovery_attempt_changed')
        return body

    def _receipt(self,request,receipt):
        if not exact(receipt,ClaimRecoveryService._make_receipt(request)):raise WorkerRecoveryHeld('exact_recovery_receipt_required')
        with self._db() as db:db.execute("UPDATE worker_recovery_attempts SET phase='receipted',receipt=? WHERE request_id=? AND phase='issued'",(encoded(receipt),request['requestId']))

    def _executable(self,parent):
        # Receipt history is never executable permission, even after local save.
        if self.guard.check(parent,mutation=True) is not None:raise WorkerRecoveryHeld('fresh_executable_parent_required')
        return dict(state='original_claim_running',executable=True,jobId=parent['jobId'],generation=parent['generation'])

    def recover(self,parent_binding):
        """One bounded reconcile/recovery operation; no background retry loop."""
        try:
            with self._operation():
                parent=self.guard._parent(parent_binding);self._prepared(parent);pending=self._row(parent)
                with self._facts(parent) as (facts,token,transfer):
                    request=self._request(pending,parent) if pending else None
                    receipt=self._http('GET',parent,'recovery-receipts/'+request['requestId'],token,missing=True) if request else None
                    if receipt is not None:self._receipt(request,receipt)
                    state=self._state(parent,token,transfer)
                if request and receipt is None:
                    if request['transferRequestSha256']!=transfer:raise WorkerRecoveryHeld('durable_original_transfer_changed')
                    # A racing original POST may commit after the missing
                    # lookup. Fresh running state permits only Guard evaluation;
                    # retain the unresolved intent for later receipt lookup.
                    if state['claim']['status']=='running' and state['claim']['heldReason'] is None:return self._executable(parent)
                    if not state['eligible']:raise WorkerRecoveryHeld('unresolved_recovery_hold')
                    advanced_epoch=request['coordinatorEpoch']!=state['coordinatorEpoch']
                    advanced_revision=not advanced_epoch and state['node']['revision']>request['expectedNodeRevision']
                    if advanced_epoch or advanced_revision:
                        resolution=dict(coordinatorEpoch=state['coordinatorEpoch'],nodeRevision=state['node']['revision'],eligible=True,claimStatus='held',heldReason='coordinator_restart')
                        with self._db() as db:db.execute("UPDATE worker_recovery_attempts SET phase=?,resolved_epoch=? WHERE request_id=? AND phase='issued'",('absent_prior_epoch' if advanced_epoch else 'absent_prior_revision',encoded(resolution),request['requestId']))
                        request=None
                    elif state['node']['revision']!=request['expectedNodeRevision']:raise WorkerRecoveryHeld('recovery_node_revision_regressed')
                else:request=None
                if state['claim']['status']=='running' and state['claim']['heldReason'] is None:return self._executable(parent)
                if not state['eligible']:raise WorkerRecoveryHeld('original_claim_not_recoverable')
                self._readonly_guard(parent)
                if request is None:
                    with self._facts(parent) as (facts,token,transfer):
                        body=dict(schemaVersion=1,requestId=str(uuid.uuid4()),coordinatorEpoch=state['coordinatorEpoch'],expectedNodeRevision=state['node']['revision'],
                            parentBinding=parent,transferRequestSha256=transfer,workerEvidenceSha256=p.digest(facts))
                        ClaimRecoveryService._request(body)
                        with self._db() as db:
                            self._census(db,job=parent['jobId'],extra_rows=1,extra_bytes=self.policy.max_row_bytes)
                            if len(encoded(body).encode())+len(encoded(facts).encode())+512>self.policy.max_row_bytes:raise WorkerRecoveryHeld('bounded_recovery_intent_required')
                            db.execute('INSERT INTO worker_recovery_attempts VALUES(?,?,?,?,?,?,?,NULL,NULL)',(body['requestId'],parent['jobId'],body['coordinatorEpoch'],body['expectedNodeRevision'],encoded(body),encoded(facts),'issued'))
                        request=body
                self._readonly_guard(parent)
                with self._facts(parent) as (_,token,transfer):
                    if request['transferRequestSha256']!=transfer:raise WorkerRecoveryHeld('original_transfer_changed_before_resume')
                    receipt=self._http('POST',parent,'recover',token,request)
                    self._receipt(request,receipt)
                return self._executable(parent)
        except WorkerRecoveryHeld:raise
        except Exception:raise WorkerRecoveryHeld('original_claim_recovery_held') from None


def validate_published_control(guard,parent,receipt):
    """Concrete read-only engine-admission boundary, never a cached grant.

    Paths are checked against the already-owned root before constructing the
    existing-state reader. No caller callback, file creation or SQLite write.
    """
    if type(guard) is not ProductionParentGuard:raise WorkerRecoveryHeld('actual_published_control_guard_required')
    guard._check();descriptor=receipt.get('publicationDescriptor')
    if type(descriptor) is not dict or receipt['publicationSha256']!=p.digest(descriptor):raise WorkerRecoveryHeld('full_control_descriptor_required')
    scope=descriptor['scope'];root=guard.journal.root;path=Path(scope['path']);lock=Path(scope['lockPath'])
    if (scope['root']!=str(root) or scope['rootIdentity']!=list(_private(root,directory=True)[1])
            or path.parent!=root or lock!=path.with_name(path.name+'.lock') or not path.is_absolute()
            or path.resolve()!=path or lock.resolve()!=lock or scope['parentPolicy']!=guard.binding
            or scope['transportPolicy']!=guard.client.policy.sha256 or scope['pipelineStoragePageCap']!=guard.journal.storage_page_cap):raise WorkerRecoveryHeld('published_control_scope_changed')
    policy=RecoveryPolicy(**scope['policy'])
    reader=OriginalClaimRecovery(path,parent_guard=guard,policy=policy)
    if not reader._initialized or reader._binding!=encoded(descriptor):raise WorkerRecoveryHeld('published_control_owner_changed')
    current=reader._prepared(parent)
    if not exact(current,receipt):raise WorkerRecoveryHeld('published_control_receipt_changed')
    # Share the client's read-only current-parent history validation as well as
    # its storage checks; a valid SQLite schema does not prove valid records.
    reader._row(parent)
    reader._check()
