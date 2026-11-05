"""Explicit original-claim continuation, separate from execution and claiming.

Authenticated worker attestation is not remote hardware attestation. A worker
must prove its actual local fence/reservation and check ParentGuard again before
any engine mutation. Historical receipts never constitute current permission.
"""
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType

from . import reference_protocol as p
from .maintenance import Conflict
from .node_gate import ENGINE,_binding
from .reference_parent_guard import PARENT_FIELDS

STATE_PROTOCOL='ltds-claim-recovery-state-v1'
RECEIPT_PROTOCOL='ltds-claim-recovery-receipt-v1'
REQUEST_FIELDS=('schemaVersion','requestId','coordinatorEpoch','expectedNodeRevision',
                'parentBinding','transferRequestSha256','workerEvidenceSha256')
# A v2 source fetch has no gate receipt or control charge yet, so it binds the
# original claim and its persisted transfer rather than a physical parent.
SOURCE_STATE_PROTOCOL='ltds-source-fetch-recovery-state-v1'
SOURCE_RECEIPT_PROTOCOL='ltds-source-fetch-recovery-receipt-v1'
SOURCE_TRANSFER_MARK='"protocol":"source-range-v2"'
SOURCE_TRANSFER_FIELDS=('protocol','reservationId','generation','expiresAt','manifestSha256','sourceVersion',
                        'objectIds','chunkBytes','requestBudget','byteBudget','nonce','attemptBudget')
SOURCE_REQUEST_FIELDS=('schemaVersion','requestId','coordinatorEpoch','expectedNodeRevision','claimBinding',
                       'transferRequestSha256','sourceClosureSha256','grantSha256','renewalSequence',
                       'committedRanges','committedBytes','workerEvidenceSha256')
SOURCE_CLAIM_FIELDS=('jobId','generation','environment','nodeId','workerId','workerInstanceId','authorityId',
                     'claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest','reservationId','inputBytes')
# The transfer wire lists every object, so it scales with the manifest and is
# capped by the same bounds as the submission. Requests and receipts carry only
# digests and counters, so their size does not depend on the object count.
SOURCE_TRANSFER_MAX_BYTES=p.MAX_BODY_BYTES
SOURCE_TRANSFER_MAX_OBJECTS=p.MAX_OBJECTS
SCHEMA='''CREATE TABLE IF NOT EXISTS reference_recovery_receipts (
 request_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,epoch_id TEXT NOT NULL,
 request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)'''


def detached(value):return json.loads(p.canonical_json(value))
def exact(left,right):return p.canonical_json(left)==p.canonical_json(right)


@dataclass(frozen=True)
class RecoveryNodeBinding:
    node_id:str
    engine_id:str
    authority_id:str
    coordinator_origin:str
    gate_generation:int
    output_budget:int

    def __post_init__(self):
        p._slug(self.node_id,'recovery node');p._uuid(self.authority_id,'recovery authority')
        if type(self.engine_id) is not str or not ENGINE.fullmatch(self.engine_id):raise ValueError('fixed recovery engine required')
        if _binding(self.node_id,self.coordinator_origin,self.authority_id)['coordinatorOrigin']!=self.coordinator_origin:raise ValueError('canonical gate origin required')
        p._integer(self.gate_generation,'gate generation');p._integer(self.output_budget,'worker output reservation')


class ClaimRecoveryService:
    def __init__(self,store,bindings):
        from .reference_store import ReferenceStore
        from .prototype import Queue
        if os.name!='posix' or type(store) is not ReferenceStore or type(store.queue) is not Queue or type(bindings) is not dict or not bindings:
            raise ValueError('actual configured recovery dependencies required')
        queue=store.queue
        for node,binding in bindings.items():
            queue._known_node(node)
            if type(binding) is not RecoveryNodeBinding or binding.node_id!=node or binding.authority_id!=queue.authority_id:
                raise ValueError('fixed recovery physical bindings required')
        self.store,self.queue=store,queue;self.bindings=MappingProxyType(dict(bindings))
        self._pid=os.getpid();self._epoch=queue.startup_id;self._authority=queue.authority_id
        self._initial=(store,queue,queue._lock)
        self._binding_sha=p.digest({node:asdict(value) for node,value in bindings.items()})
        self._paths={}
        for path in (Path(queue.path).absolute(),Path(queue.path).absolute().parent/'coordinator.lock'):
            info=path.lstat()
            if (path.resolve()!=path or any(part.is_symlink() for part in path.parents) or not stat.S_ISREG(info.st_mode)
                    or info.st_uid!=os.geteuid() or info.st_nlink!=1):raise ValueError('owned recovery authority required')
            self._paths[path]=(info.st_dev,info.st_ino)
        self._check()
        # Explicit feature startup, before its listener is available. Epoch and
        # restart overlay commit together; reconstructing the API in the SAME
        # Queue ownership period does not re-hold a running job.
        with queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._check()
            db.execute('''CREATE TABLE IF NOT EXISTS reference_recovery_startup (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),epoch_id TEXT NOT NULL,
                authority_id TEXT NOT NULL,binding_sha256 TEXT NOT NULL)''')
            db.execute(SCHEMA)
            old=db.execute('SELECT * FROM reference_recovery_startup WHERE singleton=1').fetchone()
            if old and old['epoch_id']==self._epoch:
                if old['authority_id']!=self._authority or old['binding_sha256']!=self._binding_sha:raise Conflict('recovery configuration changed within startup')
            else:
                db.execute("UPDATE jobs SET status='held',held_reason='coordinator_restart',updated_at=CURRENT_TIMESTAMP WHERE status='running'")
                db.execute('INSERT INTO reference_recovery_startup VALUES(1,?,?,?) ON CONFLICT(singleton) DO UPDATE SET epoch_id=excluded.epoch_id,authority_id=excluded.authority_id,binding_sha256=excluded.binding_sha256',
                    (self._epoch,self._authority,self._binding_sha))
            self._check(db)

    def _check(self,db=None):
        if (self._pid!=os.getpid() or self.store is not self._initial[0] or self.queue is not self._initial[1]
                or self.store.queue is not self.queue or self.queue._lock is not self._initial[2] or self.queue._lock.closed
                or self.queue.startup_id!=self._epoch or self.queue.authority_id!=self._authority
                or self._binding_sha!=p.digest({node:asdict(value) for node,value in self.bindings.items()})):
            raise Conflict('recovery authority unavailable')
        for path,identity in self._paths.items():
            info=path.lstat()
            if (path.resolve()!=path or not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid()
                    or info.st_nlink!=1 or (info.st_dev,info.st_ino)!=identity):raise Conflict('recovery authority file changed')
        observed=os.fstat(self.queue._lock.fileno())
        if (observed.st_dev,observed.st_ino)!=self._paths[Path(self.queue.path).absolute().parent/'coordinator.lock']:
            raise Conflict('recovery authority lease changed')
        if db is not None:
            authority=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            epoch=db.execute('SELECT * FROM reference_recovery_startup WHERE singleton=1').fetchone()
            if (not authority or authority[0]!=self._authority or not epoch or epoch['epoch_id']!=self._epoch
                    or epoch['authority_id']!=self._authority or epoch['binding_sha256']!=self._binding_sha):raise Conflict('recovery startup changed')

    def _state(self,db,job_id,identity,claim_token):
        self._check(db)
        claim=self.store._claim_snapshot(db,job_id,**identity,claim_token=claim_token)
        node_id=identity['node_id']
        if node_id not in self.bindings:raise Conflict('recovery node is unconfigured')
        row=self.store._row(db,job_id);node=self.queue._node(db,node_id)
        reservation=None;source=None
        wire=row['transfer_json']
        source_range=type(wire) is str and SOURCE_TRANSFER_MARK in wire
        if source_range:
            reservation=self._source_reservation(row,claim)
            source=self._source_transfer(db,job_id,claim)
        elif wire is not None:
            if type(wire) is not str or len(wire.encode())>4096:raise Conflict('recovery reservation unavailable')
            transfer=p.decode_submission(wire.encode())
            p._closed(transfer,('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce'),'original transfer reservation')
            p._slug(transfer['reservationId'],'reservation');p._slug(transfer['nonce'],'nonce')
            p._integer(transfer['generation'],'generation');p._integer(transfer['byteBudget'],'byte budget')
            p._hash(transfer['manifestSha256'],'original manifest');p._string(transfer['expiresAt'],'original expiry',40)
            if (p.canonical_json(transfer).decode()!=wire or transfer['generation']!=claim['generation']
                    or transfer['manifestSha256']!=claim['manifestSha256']):raise Conflict('recovery reservation changed')
            submission=json.loads(row['input_json'])
            reservation=dict(reservationId=transfer['reservationId'],transferRequestSha256=p.digest(transfer),
                inputBytes=submission['manifest']['totalBytes'])
        eligible=(claim['status']=='held' and claim['heldReason']=='coordinator_restart'
            and claim['executionPhase']==('fetching' if source_range else 'processing')
            and row['error'] is None and row['result_json'] is None and row['terminal_failure_json'] is None
            and reservation is not None and node['state'] in ('active','scheduled','draining')
            and (source is not None or not source_range))
        state=dict(schemaVersion=1,protocol=STATE_PROTOCOL,authorityId=self._authority,coordinatorEpoch=self._epoch,
            claim=claim,physicalBinding=asdict(self.bindings[node_id]),reservation=reservation,
            node=dict(id=node_id,state=node['state'],revision=node['revision'],planId=node['plan_id']),eligible=eligible)
        if source_range:state.update(schemaVersion=2,protocol=SOURCE_STATE_PROTOCOL,sourceTransfer=source)
        return state

    def _source_reservation(self,row,claim):
        wire=row['transfer_json']
        if len(wire.encode())>SOURCE_TRANSFER_MAX_BYTES:raise Conflict('recovery reservation unavailable')
        transfer=p.decode_submission(wire.encode())
        p._closed(transfer,SOURCE_TRANSFER_FIELDS,'original source transfer')
        p._slug(transfer['reservationId'],'reservation');p._slug(transfer['nonce'],'nonce')
        if (transfer['protocol']!='source-range-v2' or p.canonical_json(transfer).decode()!=wire
                or type(transfer['objectIds']) is not list or len(transfer['objectIds'])>SOURCE_TRANSFER_MAX_OBJECTS
                or transfer['generation']!=claim['generation'] or transfer['manifestSha256']!=claim['manifestSha256']):
            raise Conflict('recovery reservation changed')
        return dict(reservationId=transfer['reservationId'],transferRequestSha256=p.digest(transfer),
            inputBytes=json.loads(row['input_json'])['manifest']['totalBytes'])

    def _source_transfer(self,db,job_id,claim):
        """None, not an error: a refused job stays held with everything retained."""
        if claim['executionPhase']!='fetching':return None
        freeze=db.execute('SELECT phase FROM coordinator_freeze_state WHERE singleton=1').fetchone()
        if freeze is not None and freeze['phase']!='open':return None
        try:return self.store._source_fetch_closure(db,job_id)
        except (Conflict,ValueError,KeyError,TypeError,AttributeError):return None

    def state(self,job_id,*,identity,claim_token):
        self._check()
        with self.queue._db() as db:
            db.execute('BEGIN');return self._state(db,job_id,identity,claim_token)

    @staticmethod
    def _request(value):
        if type(value) is dict and type(value.get('schemaVersion')) is int and value['schemaVersion']==2:
            return ClaimRecoveryService._source_request(value)
        p._closed(value,REQUEST_FIELDS,'claim recovery request')
        if type(value['schemaVersion']) is not int or value['schemaVersion']!=1:raise ValueError('recovery version required')
        p._uuid(value['requestId'],'recovery request');p._uuid(value['coordinatorEpoch'],'startup epoch')
        p._integer(value['expectedNodeRevision'],'maintenance revision',allow_zero=True)
        for key in ('transferRequestSha256','workerEvidenceSha256'):p._hash(value[key],key)
        parent=value['parentBinding'];p._closed(parent,PARENT_FIELDS,'original parent binding')
        for key in ('jobId','authorityId'):p._uuid(parent[key],key)
        for key in ('generation','gateGeneration','inputBytes','outputBudget'):p._integer(parent[key],key)
        for key in ('nodeId','workerId','workerInstanceId','claimRequestId','reservationId'):p._slug(parent[key],key)
        for key in ('claimTokenSha256','manifestSha256','pipelineDigest'):p._hash(parent[key],key)
        p._environment(parent['environment'])
        if (type(parent['engineId']) is not str or not ENGINE.fullmatch(parent['engineId'])
                or type(parent['gateReceiptId']) is not str or not re.fullmatch('[a-f0-9]{32}',parent['gateReceiptId'])):raise ValueError('physical parent binding required')
        if _binding(parent['nodeId'],parent['coordinatorOrigin'],parent['authorityId'])['coordinatorOrigin']!=parent['coordinatorOrigin']:raise ValueError('canonical parent origin required')
        return detached(value)

    @staticmethod
    def _source_request(value):
        p._closed(value,SOURCE_REQUEST_FIELDS,'source fetch recovery request')
        p._uuid(value['requestId'],'recovery request');p._uuid(value['coordinatorEpoch'],'startup epoch')
        for key in ('expectedNodeRevision','renewalSequence','committedRanges','committedBytes'):
            p._integer(value[key],key,allow_zero=True)
        for key in ('transferRequestSha256','sourceClosureSha256','grantSha256','workerEvidenceSha256'):p._hash(value[key],key)
        claim=value['claimBinding'];p._closed(claim,SOURCE_CLAIM_FIELDS,'original claim binding')
        for key in ('jobId','authorityId'):p._uuid(claim[key],key)
        for key in ('generation','inputBytes'):p._integer(claim[key],key)
        for key in ('nodeId','workerId','workerInstanceId','claimRequestId','reservationId'):p._slug(claim[key],key)
        for key in ('claimTokenSha256','manifestSha256','pipelineDigest'):p._hash(claim[key],key)
        p._environment(claim['environment'])
        return detached(value)

    @staticmethod
    def _source_attestation(request,source):
        """A worker ahead of the coordinator means the coordinator lost history.

        Committed ranges were all served, so they cannot exceed what was admitted.
        Worker attempts can legitimately exceed the coordinator's: they are
        charged before a GET the coordinator may never see, so they are not sent.
        """
        sequence=request['renewalSequence'];latest=source['renewalSequence'];acked=latest-int(source['renewalPending'])
        known=(request['grantSha256']==source['grantSha256'] and sequence==acked
            or request['grantSha256']==source['predecessorGrantSha256'] and not source['renewalPending'] and sequence==latest-1)
        if (not known or request['committedRanges']>source['requestsAdmitted']
                or request['committedBytes']>source['bytesAdmitted']):raise Conflict('worker source state exceeds the coordinator')

    def receipt(self,job_id,request_id,*,identity,claim_token):
        p._uuid(request_id,'recovery request');self._check()
        with self.queue._db() as db:
            db.execute('BEGIN');state=self._state(db,job_id,identity,claim_token)
            row=db.execute('SELECT * FROM reference_recovery_receipts WHERE request_id=?',(request_id,)).fetchone()
            if row is None:return None
            if row['job_id']!=job_id:raise Conflict('recovery request belongs to another job')
            return self._receipt(row,state['claim'])

    @staticmethod
    def _make_receipt(request):
        digest=p.digest(request)
        if request['schemaVersion']==2:
            claim=request['claimBinding']
            return dict(schemaVersion=2,protocol=SOURCE_RECEIPT_PROTOCOL,receiptId='recovery-'+digest[:32],requestId=request['requestId'],
                requestSha256=digest,coordinatorEpoch=request['coordinatorEpoch'],authorityId=claim['authorityId'],jobId=claim['jobId'],
                generation=claim['generation'],environment=claim['environment'],claimBindingSha256=p.digest(claim),
                workerEvidenceSha256=request['workerEvidenceSha256'],transferRequestSha256=request['transferRequestSha256'],
                sourceClosureSha256=request['sourceClosureSha256'],grantSha256=request['grantSha256'],
                renewalSequence=request['renewalSequence'],committedRanges=request['committedRanges'],
                committedBytes=request['committedBytes'],nodeRevision=request['expectedNodeRevision'],
                priorStatus='held',resumedStatus='running',executionPhase='fetching')
        parent=request['parentBinding']
        return dict(schemaVersion=1,protocol=RECEIPT_PROTOCOL,receiptId='recovery-'+digest[:32],requestId=request['requestId'],
            requestSha256=digest,coordinatorEpoch=request['coordinatorEpoch'],authorityId=parent['authorityId'],jobId=parent['jobId'],
            generation=parent['generation'],environment=parent['environment'],parentBindingSha256=p.digest(parent),
            workerEvidenceSha256=request['workerEvidenceSha256'],transferRequestSha256=request['transferRequestSha256'],
            nodeRevision=request['expectedNodeRevision'],priorStatus='held',resumedStatus='running',executionPhase='processing')

    @staticmethod
    def _receipt(row,claim):
        try:
            request=ClaimRecoveryService._request(p.decode_submission(row['request_json'].encode()))
            receipt=p.decode_submission(row['receipt_json'].encode())
            parent=request['claimBinding'] if request['schemaVersion']==2 else request['parentBinding']
            if (p.digest(request)!=row['request_sha256'] or request['requestId']!=row['request_id']
                    or request['coordinatorEpoch']!=row['epoch_id'] or parent['jobId']!=row['job_id']
                    or any(not exact(parent[key],claim[key]) for key in ('jobId','generation','environment','nodeId','workerId',
                        'workerInstanceId','authorityId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest'))
                    or not exact(receipt,ClaimRecoveryService._make_receipt(request))):raise ValueError()
            return receipt
        except Exception:raise Conflict('stored recovery receipt changed') from None

    def resume(self,job_id,payload,*,identity,claim_token):
        self._check();request=self._request(payload);encoded=p.canonical_json(request).decode();digest=p.digest(request)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');state=self._state(db,job_id,identity,claim_token)
            old=db.execute('SELECT * FROM reference_recovery_receipts WHERE request_id=?',(request['requestId'],)).fetchone()
            if old:
                if old['job_id']!=job_id or old['request_sha256']!=digest or old['request_json']!=encoded:raise Conflict('recovery request differs')
                # Historical read only: never clear a later restart/operator hold.
                return self._receipt(old,state['claim'])
            if not state['eligible'] or request['coordinatorEpoch']!=self._epoch or request['expectedNodeRevision']!=state['node']['revision']:
                raise Conflict('recovery startup or maintenance state changed')
            occupied=db.execute("SELECT id FROM jobs WHERE node_id=? AND status IN ('running','held')",
                (identity['node_id'],)).fetchall()
            if len(occupied)!=1 or occupied[0]['id']!=job_id:raise Conflict('recovery node slot changed')
            claim=state['claim'];physical=self.bindings[identity['node_id']];reservation=state['reservation']
            expected={key:claim[key] for key in ('jobId','generation','environment','nodeId','workerId','workerInstanceId',
                'authorityId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')}
            expected.update(reservationId=reservation['reservationId'],inputBytes=reservation['inputBytes'])
            source='sourceTransfer' in state
            if source:
                if request['schemaVersion']!=2 or request['sourceClosureSha256']!=state['sourceTransfer']['closureSha256']:
                    raise Conflict('original recovery binding differs')
                self._source_attestation(request,state['sourceTransfer'])
            elif request['schemaVersion']!=1:raise Conflict('original recovery binding differs')
            else:expected.update(engineId=physical.engine_id,coordinatorOrigin=physical.coordinator_origin,
                gateGeneration=physical.gate_generation,outputBudget=physical.output_budget)
            parent=request['claimBinding'] if source else request['parentBinding']
            if (any(not exact(parent[key],value) for key,value in expected.items())
                    or request['transferRequestSha256']!=reservation['transferRequestSha256']):raise Conflict('original recovery binding differs')
            receipt=self._make_receipt(request)
            wire=p.canonical_json(receipt).decode()
            self.store._quota(db,sum(len(value.encode()) for value in (request['requestId'],job_id,self._epoch,digest,encoded,wire)))
            changed=db.execute("UPDATE jobs SET status='running',held_reason=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=? AND status='held' AND held_reason='coordinator_restart' AND claim_hash=?",
                (job_id,claim['claimTokenSha256']))
            if changed.rowcount!=1:raise Conflict('original recovery claim changed')
            db.execute('INSERT INTO reference_recovery_receipts VALUES(?,?,?,?,?,?)',(request['requestId'],job_id,self._epoch,digest,encoded,wire))
            self._check(db)
            return receipt
