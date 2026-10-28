"""Unintegrated ODM→original-LiDAR stage journal, not a queue or engine adapter.

Intermediate adapters must never publish a parent outbox result or release its
claim/gate. This module records intent before start and reconciles only through
explicit read-only lookup. It creates no child job or rewritten submanifest.
"""
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from typing import Protocol
import uuid

from . import reference_protocol as p
from .node_gate import _binding as physical_binding,ENGINE
from .reference_failure import make_failure
from .treedar_admission import AdmissionPolicy,_provenance

VERSION='reference-sequential-v1'
STAGES=('odm','treedar')
RESULT_ROLES=frozenset(('pointcloud','mesh','orthophoto','dem','report','tree_inventory','odm_archive'))
RAW_SUBMISSION=('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')
SCHEMA=(
    'CREATE TABLE IF NOT EXISTS pipeline_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
    '''CREATE TABLE IF NOT EXISTS pipeline_parents(job TEXT PRIMARY KEY,identity TEXT NOT NULL,
       submission TEXT NOT NULL,plan TEXT NOT NULL,state TEXT NOT NULL,result TEXT,failure TEXT)''',
    '''CREATE TABLE IF NOT EXISTS pipeline_stages(job TEXT NOT NULL,stage TEXT NOT NULL,state TEXT NOT NULL,
       intent TEXT,proof TEXT,PRIMARY KEY(job,stage))''')


class PipelineHeld(ValueError):pass
class PipelineBusy(PipelineHeld):pass


class PipelineOperationLease:
    """Live local operation capability; never a serialized execution grant."""
    def __init__(self,journal,descriptor):
        self.journal,self.descriptor=journal,descriptor;self.pid=os.getpid();self.thread=threading.get_ident()
    def check(self,journal):
        if (self.journal is not journal or self.pid!=os.getpid() or self.thread!=threading.get_ident()
                or getattr(journal,'_active_lease',None) is not self or not journal._busy):raise PipelineHeld('live_pipeline_operation_required')
        journal._check()
        if journal._file(journal.lock_path,self.descriptor)!=journal._lock_identity:raise PipelineHeld('pipeline_operation_identity_changed')


def encoded(value):return p.canonical_json(value).decode()
def detached(value):return json.loads(p.canonical_json(value))


@dataclass(frozen=True)
class PipelinePolicy:
    worker_id:str
    worker_instance_id:str
    authority_id:str
    engine_id:str
    coordinator_origin:str
    gate_generation:int
    odm_version:str
    treedar_version:str
    odm_runtime_sha256:str
    treedar_runtime_sha256:str
    provenance_policy_sha256:str
    odm_output_budget:int
    treedar_output_budget:int
    node_id:str='main'
    allowed_environments:tuple[str,...]=('production','staging')
    recovery_control_budget:int=0

    def __post_init__(self):
        if self.node_id!='main':raise PipelineHeld('sequential_pipeline_requires_main')
        for value in (self.worker_id,self.worker_instance_id):p._slug(value,'fixed worker identity')
        if len(self.worker_id)>64 or type(self.engine_id) is not str or not ENGINE.fullmatch(self.engine_id):raise PipelineHeld('fixed_physical_worker_required')
        p._uuid(self.authority_id,'authority');p._integer(self.gate_generation,'gate generation')
        if physical_binding(self.node_id,self.coordinator_origin,self.authority_id)['coordinatorOrigin']!=self.coordinator_origin:
            raise PipelineHeld('canonical_coordinator_origin_required')
        for version in (self.odm_version,self.treedar_version):
            if type(version) is not str or not 1<=len(version)<=128 or not all(0x21<=ord(char)<=0x7e for char in version):
                raise PipelineHeld('fixed_engine_versions_required')
        for sha in (self.odm_runtime_sha256,self.treedar_runtime_sha256,self.provenance_policy_sha256):p._hash(sha,'runtime/policy digest')
        for size in (self.odm_output_budget,self.treedar_output_budget):p._integer(size,'stage output budget')
        p._integer(self.recovery_control_budget,'recovery control budget',allow_zero=True)
        p._integer(self.output_budget,'parent output budget')
        if (type(self.allowed_environments) is not tuple or not self.allowed_environments
                or len(set(self.allowed_environments))!=len(self.allowed_environments)):raise PipelineHeld('fixed_environments_required')
        for environment in self.allowed_environments:p._environment(environment)

    @property
    def output_budget(self):return self.odm_output_budget+self.treedar_output_budget+self.recovery_control_budget


class ParentGuard(Protocol):
    binding:dict
    def check(self,parent_binding:dict,*,mutation:bool)->None:
        """Prove exact live Queue claim, scratch reservation and physical fence."""


class IntermediateAdapter(Protocol):
    binding:dict
    def start(self,intent:dict)->dict|None:
        """One mutation attempt; no parent outbox/ACK/gate release side effects."""
    def lookup(self,intent:dict)->dict|None:
        """Read only the exact intent's durable terminal evidence; never start."""
    def verify(self,intent:dict,proof:dict)->None:
        """Verify actual terminal engine identity and durable owned output bytes."""


def seal_stage_proof(value):
    """Hash helper only; does not authenticate engine state or output files."""
    if type(value) is not dict or 'proofSha256' in value:raise PipelineHeld('unsealed_stage_proof_required')
    return detached(dict(value,proofSha256=p.digest(value)))


def _submission(value,environment):
    raw={key:value[key] for key in RAW_SUBMISSION}
    checked=p.validate_submission(raw,environment=environment,capability_fingerprint=raw['capabilityFingerprint'],
        available_engines=('odm','treedar'),options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'],
        allowed_odm_options=set(raw['pipeline']['odm']['options']))
    if checked!=value:raise PipelineHeld('normalized_parent_submission_required')
    manifest,pipeline=checked['manifest'],checked['pipeline']
    images=[obj for obj in manifest['objects'] if obj['kind']=='image']
    lidar=[obj for obj in manifest['objects'] if obj['kind']=='lidar']
    if (pipeline['odm']['requested'] is not True or pipeline['treedar']['requested'] is not True or not images or len(lidar)!=1
            or len(images)+1!=len(manifest['objects']) or pipeline['treedar']['inputObjectIds']!=[lidar[0]['id']]):
        raise PipelineHeld('images_then_one_original_lidar_required')
    return checked


def _result_template(policy,parent,objects):
    return dict(schemaVersion=1,jobId=parent['jobId'],generation=parent['generation'],environment=parent['environment'],
        pipelineDigest=parent['pipelineDigest'],engineVersions=dict(odm=policy.odm_version,treedar=policy.treedar_version),
        objects=objects,resultManifestSha256='0'*64)


def _output_limits(policy,parent):
    # Reserve both stage arrays plus their separating comma in the final wire
    # manifest before either adapter starts. A valid first-stage proof must not
    # consume the metadata space needed to close the whole parent later.
    header_bytes=len(p.canonical_json(_result_template(policy,parent,[])))
    return dict(outputObjectLimit=p.MAX_OBJECTS//len(STAGES),
        outputMetadataBudget=(p.MAX_BODY_BYTES-header_bytes-(len(STAGES)-1))//len(STAGES))


def _stage(policy,parent,submission,stage_id):
    return dict(stageId=stage_id,engineVersion=getattr(policy,stage_id+'_version'),
        runtimeSha256=getattr(policy,stage_id+'_runtime_sha256'),outputBudget=getattr(policy,stage_id+'_output_budget'),
        **_output_limits(policy,parent),
        inputObjects=[obj for obj in submission['manifest']['objects'] if obj['kind']==('image' if stage_id=='odm' else 'lidar')],
        options=submission['pipeline']['odm'] if stage_id=='odm' else None,
        provenanceReceiptId=submission['pipeline']['treedar']['provenanceReceiptId'] if stage_id=='treedar' else None)


def _plan(policy,parent,submission):
    value=dict(schemaVersion=1,protocol=VERSION,parentBinding=parent,parentBindingSha256=p.digest(parent),
        submissionSha256=p.digest(submission),policySha256=p.digest(asdict(policy)),
        stages=[_stage(policy,parent,submission,stage) for stage in STAGES])
    return dict(value,planSha256=p.digest(value))


def validate_stage_proof(value,*,intent,policy):
    try:
        fields=('schemaVersion','planSha256','intentSha256','jobId','generation','environment','inputManifestSha256','pipelineDigest',
            'stageId','engineVersion','runtimeSha256','inputObjectIds','engineTerminal','outcome','objects','errorCode','proofSha256')
        p._closed(value,fields,'stage proof');value=detached(value)
        parent,stage=intent['parentBinding'],intent['stage']
        if (type(policy) is not PipelinePolicy or stage['stageId'] not in STAGES
                or stage['engineVersion']!=getattr(policy,stage['stageId']+'_version')
                or stage['runtimeSha256']!=getattr(policy,stage['stageId']+'_runtime_sha256')
                or stage['outputBudget']!=getattr(policy,stage['stageId']+'_output_budget')
                or any(stage[key]!=item for key,item in _output_limits(policy,parent).items())):raise ValueError()
        expected=dict(planSha256=intent['planSha256'],intentSha256=intent['intentSha256'],jobId=parent['jobId'],generation=parent['generation'],
            environment=parent['environment'],inputManifestSha256=parent['manifestSha256'],pipelineDigest=parent['pipelineDigest'],
            stageId=stage['stageId'],engineVersion=stage['engineVersion'],runtimeSha256=stage['runtimeSha256'],
            inputObjectIds=[obj['id'] for obj in stage['inputObjects']])
        if (type(value['schemaVersion']) is not int or value['schemaVersion']!=1 or value['engineTerminal'] is not True
                or any(p.canonical_json(value[key])!=p.canonical_json(item) for key,item in expected.items())
                or value['proofSha256']!=p.digest({key:item for key,item in value.items() if key!='proofSha256'})
                or len(p.canonical_json(value))>p.MAX_BODY_BYTES):raise ValueError()
        if value['outcome']=='succeeded':
            if value['errorCode'] is not None:raise ValueError()
            result=dict(schemaVersion=1,jobId=parent['jobId'],generation=parent['generation'],environment=parent['environment'],
                pipelineDigest=parent['pipelineDigest'],engineVersions={stage['stageId']:stage['engineVersion']},objects=value['objects'])
            result['resultManifestSha256']=p.digest(result)
            p.validate_result_manifest(result,job_id=parent['jobId'],generation=parent['generation'],environment=parent['environment'],pipeline_digest=parent['pipelineDigest'])
            archives=sum(obj['role']=='odm_archive' for obj in value['objects'])
            if (archives!=(1 if stage['stageId']=='odm' else 0)
                    or any(obj['role'] not in RESULT_ROLES or obj['byteSize']==0 and obj['role']!='report' for obj in value['objects'])
                    or len(value['objects'])>stage['outputObjectLimit']
                    or len(p.canonical_json(value['objects']))-2>stage['outputMetadataBudget']
                    or any(not obj['id'].startswith(stage['stageId']+'-') for obj in value['objects'])
                    or sum(obj['byteSize'] for obj in value['objects'])>stage['outputBudget']):raise ValueError()
        elif value['outcome'] in ('failed','cancelled'):
            if value['objects']!=[] or type(value['errorCode']) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,79}',value['errorCode']):raise ValueError()
        else:raise ValueError()
        return value
    except Exception:raise PipelineHeld('exact_terminal_stage_proof_required') from None


class PipelineJournal:
    def __init__(self,path,policy,*,verify_provenance):
        if os.name!='posix' or type(policy) is not PipelinePolicy or not callable(verify_provenance):raise PipelineHeld('fixed_linux_stage_policy_required')
        self.policy,self.verify_provenance=policy,verify_provenance
        self.policy_sha256=p.digest(asdict(policy));self._pid=os.getpid();self._mutex=threading.RLock();self._busy=False
        self.path=Path(path).absolute();self.root=self.path.parent
        if self.path.resolve()!=self.path or any(part.is_symlink() for part in (self.path,*self.path.parents)):
            raise PipelineHeld('private_stage_journal_required')
        self.lock_path=self.path.with_name(self.path.name+'.lock')
        # The worker factory owns durable directory publication and its lifetime
        # lease. Never create a missing ancestor whose entry could disappear
        # after an engine mutation despite the leaf journal's FULL commits.
        try:root=self.root.stat()
        except OSError:raise PipelineHeld('preexisting_private_stage_root_required') from None
        if not stat.S_ISDIR(root.st_mode) or root.st_uid!=os.geteuid() or stat.S_IMODE(root.st_mode)!=0o700:
            raise PipelineHeld('private_stage_journal_required')
        self._root_identity=(root.st_dev,root.st_ino)
        self._path_identity=self._create(self.path);self._lock_identity=self._create(self.lock_path)
        self._binding=encoded(dict(protocol=VERSION,policy=asdict(policy),path=str(self.path),rootIdentity=self._root_identity,
            databaseIdentity=self._path_identity,lockIdentity=self._lock_identity))
        with self._operation(),self._db() as db:
            names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names-{'pipeline_binding','pipeline_parents','pipeline_stages'}:raise PipelineHeld('dedicated_pipeline_journal_required')
            for sql in SCHEMA:db.execute(sql)
            self._schema(db)
            old=db.execute('SELECT value FROM pipeline_binding WHERE id=1').fetchone()
            if old and old[0]!=self._binding:raise PipelineHeld('pipeline_policy_or_owner_changed')
            if not old and (db.execute('SELECT 1 FROM pipeline_parents').fetchone() or db.execute('SELECT 1 FROM pipeline_stages').fetchone()):
                raise PipelineHeld('unbound_pipeline_rows')
            db.execute('INSERT OR IGNORE INTO pipeline_binding VALUES(1,?)',(self._binding,))
        for directory in (self.root,self.root.parent):
            fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
        self._initialized=True

    def _create(self,path):
        fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
        try:return self._file(path,fd)
        finally:os.close(fd)

    def _file(self,path,fd=None):
        before=path.lstat();observed=os.fstat(fd) if fd is not None else before
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600
                or (before.st_dev,before.st_ino)!=(observed.st_dev,observed.st_ino)):raise PipelineHeld('owned_stage_file_required')
        return (before.st_dev,before.st_ino)

    def _check(self):
        if self._pid!=os.getpid():raise PipelineHeld('pipeline_process_changed')
        root=self.root.stat()
        if (p.digest(asdict(self.policy))!=self.policy_sha256 or self.root.resolve()!=self.root or stat.S_IMODE(root.st_mode)!=0o700
                or root.st_uid!=os.geteuid() or (root.st_dev,root.st_ino)!=self._root_identity
                or self._file(self.path)!=self._path_identity or self._file(self.lock_path)!=self._lock_identity):
            raise PipelineHeld('pipeline_identity_changed')

    @contextmanager
    def _operation(self):
        self._check()
        with self._mutex:
            if self._busy:raise PipelineBusy('pipeline_operation_active')
            self._check();fd=os.open(self.lock_path,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
            try:
                import fcntl
                if self._file(self.lock_path,fd)!=self._lock_identity:raise PipelineHeld('pipeline_lock_changed')
                try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise PipelineBusy('pipeline_operation_active') from None
                self._busy=True;self._active_lease=PipelineOperationLease(self,fd)
                yield self._active_lease;self._check()
            finally:self._active_lease=None;self._busy=False;os.close(fd)

    @contextmanager
    def _db(self):
        self._check();db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row
        try:
            if db.execute('PRAGMA journal_mode').fetchone()[0]!='delete':raise PipelineHeld('pipeline_journal_mode_changed')
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if getattr(self,'_initialized',False):self._schema(db)
            yield db;self._check();self._commit(db)
        except BaseException:db.rollback();raise
        finally:db.close()

    def _commit(self,db):db.commit()

    def _schema(self,db):
        def inventory(connection):
            return [[*row[:3],re.sub(r'\s+','',row[3]).lower() if row[3] is not None else None]
                for row in connection.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        memory=sqlite3.connect(':memory:')
        try:
            for sql in SCHEMA:memory.execute(sql)
            expected=inventory(memory)
        finally:memory.close()
        if inventory(db)!=expected:raise PipelineHeld('dedicated_pipeline_schema_required')

    def _parent(self,claim,claim_request_id,reservation_id,gate_receipt_id):
        p._uuid(claim['jobId'],'parent job');p._integer(claim['generation'],'generation');p._environment(claim['environment'])
        for item in (claim_request_id,reservation_id):p._slug(item,'parent reservation/claim request')
        if (claim['nodeId']!='main' or claim['status']!='running' or claim['environment'] not in self.policy.allowed_environments
                or type(gate_receipt_id) is not str or not re.fullmatch(r'[a-f0-9]{32}',gate_receipt_id)
                or type(claim['claimToken']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{32,256}',claim['claimToken'])):
            raise PipelineHeld('exact_running_parent_claim_required')
        submission=_submission(claim['submission'],claim['environment'])
        if (submission['manifestSha256'],submission['pipelineDigest'])!=(claim['manifestSha256'],claim['pipelineDigest']):raise PipelineHeld('parent_digest_changed')
        parent=dict(jobId=claim['jobId'],generation=claim['generation'],environment=claim['environment'],nodeId=self.policy.node_id,
            workerId=self.policy.worker_id,workerInstanceId=self.policy.worker_instance_id,authorityId=self.policy.authority_id,
            engineId=self.policy.engine_id,coordinatorOrigin=self.policy.coordinator_origin,gateGeneration=self.policy.gate_generation,
            manifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],claimRequestId=claim_request_id,
            claimTokenSha256=hashlib.sha256(claim['claimToken'].encode()).hexdigest(),reservationId=reservation_id,
            gateReceiptId=gate_receipt_id,inputBytes=submission['manifest']['totalBytes'],
            outputBudget=self.policy.output_budget)
        return parent,submission

    def bind_parent(self,claim,*,claim_request_id,reservation_id,gate_receipt_id,guard):
        parent,submission=self._parent(claim,claim_request_id,reservation_id,gate_receipt_id)
        plan=_plan(self.policy,parent,submission)
        with self._operation(),self._db() as db:
            # Binding observes an existing claim; control-enabled execution is
            # gated only after pre-stage preparation can publish its receipt.
            self._guard(guard,parent,mutation=self.policy.recovery_control_budget==0)
            old=db.execute('SELECT identity,submission,plan FROM pipeline_parents WHERE job=?',(parent['jobId'],)).fetchone()
            expected=(encoded(parent),encoded(submission),encoded(plan))
            if old:
                if tuple(old)!=expected:raise PipelineHeld('immutable_parent_binding_changed')
            else:
                db.execute("INSERT INTO pipeline_parents VALUES(?,?,?,?,'bound',NULL,NULL)",(parent['jobId'],*expected))
                for stage in STAGES:db.execute("INSERT INTO pipeline_stages VALUES(?,?,'new',NULL,NULL)",(parent['jobId'],stage))
        return detached(plan)

    def _read(self,job_id):
        p._uuid(job_id,'parent job')
        with self._db() as db:
            bound=db.execute('SELECT value FROM pipeline_binding WHERE id=1').fetchone()
            row=db.execute('SELECT * FROM pipeline_parents WHERE job=?',(job_id,)).fetchone()
            stages=[dict(item) for item in db.execute('SELECT * FROM pipeline_stages WHERE job=?',(job_id,))]
        if bound is None or bound[0]!=self._binding or row is None:raise PipelineHeld('unknown_or_changed_pipeline')
        row=dict(row);parent=json.loads(row['identity']);submission=_submission(json.loads(row['submission']),parent['environment'])
        plan=_plan(self.policy,parent,submission)
        if parent['jobId']!=job_id or row['plan']!=encoded(plan) or set(item['stage'] for item in stages)!=set(STAGES):raise PipelineHeld('pipeline_plan_changed')
        stages={item['stage']:item for item in stages}
        prior=None
        for stage_id in STAGES:
            stage=stages[stage_id]
            if stage['state'] not in ('new','issued','succeeded','failed','cancelled'):raise PipelineHeld('stage_state_changed')
            if stage_id=='treedar' and stage['state']!='new' and stages['odm']['state']!='succeeded':raise PipelineHeld('stage_order_changed')
            if stage['state']=='new':
                if stage['intent'] is not None or stage['proof'] is not None:raise PipelineHeld('stage_evidence_changed')
                continue
            intent=json.loads(stage['intent']);self._intent(intent,plan,stage_id,prior)
            if stage['state']=='issued':
                if stage['proof'] is not None:raise PipelineHeld('stage_evidence_changed')
            else:
                proof=validate_stage_proof(json.loads(stage['proof']),intent=intent,policy=self.policy)
                if proof['outcome']!=stage['state']:raise PipelineHeld('stage_evidence_changed')
                prior=proof['proofSha256']
        state,result,failure=self._closure(parent,stages)
        if (row['state'],row['result'],row['failure'])!=(state,encoded(result) if result else None,encoded(failure) if failure else None):
            raise PipelineHeld('pipeline_closure_changed')
        return parent,plan,stages,state,result,failure

    def _intent(self,intent,plan,stage_id,prior):
        p._closed(intent,('schemaVersion','protocol','planSha256','parentBinding','stage','precedingProofSha256','attemptId','provenance','intentSha256'),'stage intent')
        if (type(intent['schemaVersion']) is not int or intent['schemaVersion']!=1 or intent['protocol']!=VERSION
                or intent['planSha256']!=plan['planSha256'] or intent['parentBinding']!=plan['parentBinding']
                or intent['stage']!=plan['stages'][STAGES.index(stage_id)] or intent['precedingProofSha256']!=prior
                or intent['intentSha256']!=p.digest({key:value for key,value in intent.items() if key!='intentSha256'})):
            raise PipelineHeld('stage_intent_binding_changed')
        p._uuid(intent['attemptId'],'stage attempt')
        if stage_id=='odm' and intent['provenance'] is not None:raise PipelineHeld('odm_does_not_create_lidar_provenance')
        if stage_id=='treedar' and (type(intent['provenance']) is not dict or intent['provenance'].get('receiptId')!=intent['stage']['provenanceReceiptId']):
            raise PipelineHeld('original_lidar_provenance_required')

    def _closure(self,parent,stages):
        for stage_id in STAGES:
            stage=stages[stage_id]
            if stage['state'] in ('failed','cancelled'):
                proof=json.loads(stage['proof'])
                return stage['state'],None,make_failure(job_id=parent['jobId'],generation=parent['generation'],environment=parent['environment'],
                    input_manifest_sha256=parent['manifestSha256'],pipeline_digest=parent['pipelineDigest'],terminal_state=stage['state'],error_code=proof['errorCode'])
        if all(stages[stage]['state']=='succeeded' for stage in STAGES):
            result=_result_template(self.policy,parent,
                [obj for stage in STAGES for obj in json.loads(stages[stage]['proof'])['objects']])
            result.pop('resultManifestSha256')
            result['resultManifestSha256']=p.digest(result)
            p.validate_result_manifest(result,job_id=parent['jobId'],generation=parent['generation'],environment=parent['environment'],pipeline_digest=parent['pipelineDigest'])
            if len(p.canonical_json(result))>p.MAX_BODY_BYTES:raise PipelineHeld('final_closure_exceeds_metadata_limit')
            return 'ready_final',result,None
        state='executing' if any(item['state']=='issued' for item in stages.values()) else 'waiting_treedar' if stages['odm']['state']=='succeeded' else 'bound'
        return state,None,None

    def guard_binding(self):
        policy=self.policy
        return dict(policySha256=self.policy_sha256,nodeId=policy.node_id,workerId=policy.worker_id,workerInstanceId=policy.worker_instance_id,
            authorityId=policy.authority_id,engineId=policy.engine_id,coordinatorOrigin=policy.coordinator_origin,gateGeneration=policy.gate_generation)

    def adapter_binding(self,stage_id):
        if stage_id not in STAGES:raise PipelineHeld('unknown_stage')
        return dict(policySha256=self.policy_sha256,stageId=stage_id,engineVersion=getattr(self.policy,stage_id+'_version'),runtimeSha256=getattr(self.policy,stage_id+'_runtime_sha256'))

    def _guard(self,guard,parent,*,mutation=False):
        if getattr(guard,'binding',None)!=self.guard_binding() or not callable(getattr(guard,'check',None)):
            raise PipelineHeld('trusted_parent_guard_required')
        try:
            if guard.check(detached(parent),mutation=mutation) is not None:raise ValueError()
        except Exception:raise PipelineHeld('exact_parent_fence_unavailable') from None

    def _adapter(self,adapter,stage_id):
        if (getattr(adapter,'binding',None)!=self.adapter_binding(stage_id)
                or any(not callable(getattr(adapter,name,None)) for name in ('start','lookup','verify'))):raise PipelineHeld('exact_intermediate_adapter_required')

    def _provenance(self,plan,receipt):
        parent=plan['parentBinding'];stage=plan['stages'][1];obj=stage['inputObjects'][0]
        if type(receipt) is not dict or receipt.get('receiptId')!=stage['provenanceReceiptId']:raise PipelineHeld('original_lidar_provenance_required')
        try:return _provenance(receipt,self.verify_provenance,job_id=parent['jobId'],generation=parent['generation'],environment=parent['environment'],
            object_id=obj['id'],expected_sha256=obj['sha256'],expected_bytes=obj['byteSize'],allow_synthetic=False,policy=AdmissionPolicy())
        except Exception:raise PipelineHeld('authenticated_original_lidar_required') from None

    def advance(self,job_id,stage_id,adapter,guard,*,provenance=None):
        with self._operation():
            parent,plan,stages,state,result,failure=self._read(job_id)
            self._adapter(adapter,stage_id);self._guard(guard,parent,mutation=stages[stage_id]['state']=='new')
            stage=stages[stage_id]
            if stage['state']=='issued':raise PipelineHeld('stage_intent_requires_readonly_reconciliation')
            if stage['state']!='new':return self._public(parent,stages,state,result,failure)
            if failure or stage_id=='treedar' and stages['odm']['state']!='succeeded':raise PipelineHeld('preceding_successful_stage_required')
            receipt=self._provenance(plan,provenance) if stage_id=='treedar' else None
            prior=json.loads(stages['odm']['proof'])['proofSha256'] if stage_id=='treedar' else None
            intent=dict(schemaVersion=1,protocol=VERSION,planSha256=plan['planSha256'],parentBinding=parent,
                stage=plan['stages'][STAGES.index(stage_id)],precedingProofSha256=prior,attemptId=str(uuid.uuid4()),provenance=receipt)
            intent['intentSha256']=p.digest(intent)
            with self._db() as db:
                db.execute("UPDATE pipeline_stages SET state='issued',intent=? WHERE job=? AND stage=? AND state='new'",(encoded(intent),job_id,stage_id))
                if db.execute('SELECT changes()').fetchone()[0]!=1:raise PipelineHeld('stage_state_changed')
                db.execute("UPDATE pipeline_parents SET state='executing' WHERE job=?",(job_id,))
            self._guard(guard,parent,mutation=True);self._adapter(adapter,stage_id)
            if stage_id=='treedar':self._provenance(plan,receipt)
            try:proof=adapter.start(detached(intent))
            except Exception:raise PipelineHeld('stage_mutation_outcome_unknown') from None
            return self._accept(job_id,stage_id,adapter,guard,intent,proof)

    def reconcile(self,job_id,stage_id,adapter,guard):
        with self._operation():
            parent,plan,stages,state,result,failure=self._read(job_id)
            self._adapter(adapter,stage_id);self._guard(guard,parent)
            stage=stages[stage_id]
            if stage['state']=='new':raise PipelineHeld('no_prior_stage_intent')
            if stage['state']!='issued':return self._public(parent,stages,state,result,failure)
            intent=json.loads(stage['intent'])
            try:proof=adapter.lookup(detached(intent))
            except Exception:raise PipelineHeld('stage_readonly_proof_unavailable') from None
            return self._accept(job_id,stage_id,adapter,guard,intent,proof)

    def _accept(self,job_id,stage_id,adapter,guard,intent,proof):
        parent,plan,stages,state,result,failure=self._read(job_id)
        self._guard(guard,parent);self._adapter(adapter,stage_id)
        if proof is None:return self._public(parent,stages,state,result,failure)
        checked=validate_stage_proof(proof,intent=intent,policy=self.policy)
        try:
            if adapter.verify(detached(intent),detached(checked)) is not None:raise ValueError()
        except Exception:raise PipelineHeld('stage_engine_or_artifacts_unverified') from None
        self._guard(guard,parent);self._adapter(adapter,stage_id)
        stages[stage_id].update(state=checked['outcome'],proof=encoded(checked))
        state,result,failure=self._closure(parent,stages)
        with self._db() as db:
            db.execute("UPDATE pipeline_stages SET state=?,proof=? WHERE job=? AND stage=? AND state='issued' AND intent=?",
                (checked['outcome'],encoded(checked),job_id,stage_id,encoded(intent)))
            if db.execute('SELECT changes()').fetchone()[0]!=1:raise PipelineHeld('stage_state_changed')
            db.execute('UPDATE pipeline_parents SET state=?,result=?,failure=? WHERE job=?',
                (state,encoded(result) if result else None,encoded(failure) if failure else None,job_id))
        return self._public(parent,stages,state,result,failure)

    def _public(self,parent,stages,state,result,failure):
        return dict(jobId=parent['jobId'],generation=parent['generation'],environment=parent['environment'],state=state,
            stages={stage:stages[stage]['state'] for stage in STAGES},resultManifestSha256=result['resultManifestSha256'] if result else None,
            failureSha256=failure['failureSha256'] if failure else None)

    def status(self,job_id):
        with self._operation():
            parent,plan,stages,state,result,failure=self._read(job_id)
            return self._public(parent,stages,state,result,failure)

    def final_result(self,job_id):
        with self._operation():
            result=self._read(job_id)[4]
            if result is None:raise PipelineHeld('whole_pipeline_closure_required')
            return detached(result)

    def terminal_failure(self,job_id):
        with self._operation():
            failure=self._read(job_id)[5]
            if failure is None:raise PipelineHeld('whole_pipeline_failure_unavailable')
            return detached(failure)
