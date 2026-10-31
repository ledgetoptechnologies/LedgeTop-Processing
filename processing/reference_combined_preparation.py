"""Concrete original-parent preparation only; no combined engine/delivery mode.

This is explicitly installed by ReferenceDriver's trusted constructor policy.
It uses the actual claim/download journal and preserves one physical receipt.
Metadata/scientific/final-v2 authority remains unavailable and work stays held.
"""
from contextlib import closing
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3

from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_pipeline import PipelinePolicy,PipelineJournal
from .reference_parent_guard import ClaimStatePolicy,ClaimStateClient,WorkerCredential,DriverClaimTokens,ProductionParentGuard,_private,_readonly_sqlite,_private_json,exact,tls_trust_sha256
from .reference_worker_recovery import RecoveryPolicy,OriginalClaimRecovery
from .reference_odm_stage import NativeRuntimePolicy,NativeRuntimeProbe


class CombinedPreparationHeld(ValueError):pass


@dataclass(frozen=True)
class CombinedPreparationPolicy:
    pipeline:PipelinePolicy
    recovery:RecoveryPolicy
    native:NativeRuntimePolicy
    driver_page_cap:int=4096
    pipeline_page_cap:int=4096
    max_preparations:int=64
    max_intent_bytes:int=8192

    def __post_init__(self):
        if type(self.pipeline) is not PipelinePolicy or type(self.recovery) is not RecoveryPolicy or type(self.native) is not NativeRuntimePolicy:raise CombinedPreparationHeld('concrete_combined_policy_required')
        for value in (self.driver_page_cap,self.pipeline_page_cap):
            if type(value) is not int or not 64<=value<=16384:raise CombinedPreparationHeld('bounded_preparation_storage_required')
        if type(self.max_preparations) is not int or not 1<=self.max_preparations<=self.recovery.global_attempts or type(self.max_intent_bytes) is not int or not 2048<=self.max_intent_bytes<=8192:raise CombinedPreparationHeld('bounded_gate_history_required')
        if self.recovery.preparation_allowance!=3*(self.driver_page_cap+self.pipeline_page_cap)*4096+65536:raise CombinedPreparationHeld('explicit_full_preparation_allowance_required')
        if (self.pipeline.recovery_control_budget!=self.recovery.control_allowance or self.pipeline.odm_runtime_sha256!=self.native.sha256
                or self.pipeline.engine_id!=self.native.engine_id or self.pipeline.odm_version!=self.native.engine_version):raise CombinedPreparationHeld('combined_budget_runtime_binding_changed')
    @property
    def sha256(self):return p.digest(dict(mode='combined-prepare-only-v1',policy=asdict(self)))


TABLE='''CREATE TABLE combined_parent_preparations (
 job TEXT PRIMARY KEY,identity TEXT NOT NULL,phase TEXT NOT NULL)'''
PHASES=('reserve_pending','reserved','bind_pending','bound','prepared')


class CombinedParentPreparation:
    def __init__(self,driver,policy,*,runtime_probe=None):
        from .reference_driver import ReferenceDriver
        from ops.reference_worker_daemon import DaemonStateLock
        if type(driver) is not ReferenceDriver or type(policy) is not CombinedPreparationPolicy or type(driver._state_owner) is not DaemonStateLock:raise CombinedPreparationHeld('actual_owned_driver_required')
        self.driver,self.policy,self.owner=driver,policy,driver._state_owner;self.root=driver.path.parent
        self.owner.check(self.root);_private(self.root,directory=True);self._pid=os.getpid();self._policy_sha=policy.sha256
        self._driver_identity=_private(driver.path)[1];self._driver_path=driver.path
        core=policy.pipeline;worker=driver.worker
        from .reference_engine_worker import ReferenceEngineWorker
        from .scratch_policy import ScratchLedger
        from .viewer_service import NativeEngine
        if type(worker) is not ReferenceEngineWorker or type(driver.ledger) is not ScratchLedger or type(worker.engine) is not NativeEngine:raise CombinedPreparationHeld('concrete_preparation_components_required')
        if (driver.node_id!='main' or driver.output_budget!=core.output_budget or driver.worker_instance_id!=core.worker_instance_id
                or (driver.worker_id,worker.engine_id,worker.authority_id,worker.origin,worker.gate_generation,worker.version)!=(core.worker_id,core.engine_id,core.authority_id,core.coordinator_origin,core.gate_generation,core.odm_version)
                or worker.engine.origin!=policy.native.engine_origin or set(driver.allowed_environments)!=set(core.allowed_environments)
                or self.root.stat().st_dev!=driver.ledger.root.stat().st_dev):raise CombinedPreparationHeld('actual_combined_driver_binding_changed')
        transport=ClaimStatePolicy(driver.origin,core.authority_id,'main',core.worker_id,core.worker_instance_id,'combined-main',tls_trust_sha256(driver._ssl_context))
        self.client=ClaimStateClient(transport,WorkerCredential('combined-main',driver._bearer),ssl_context=driver._ssl_context)
        self.probe=runtime_probe or NativeRuntimeProbe(policy.native)
        if type(self.probe) is not NativeRuntimeProbe or self.probe.policy!=policy.native:raise CombinedPreparationHeld('actual_combined_runtime_probe_required')
        self._components=(driver,driver.worker,driver.ledger,driver._state_owner,driver._ssl_context,self.probe,self.client,driver.worker.engine,driver.worker.engine.client,driver.outbox,driver.failure_outbox)
        self._probe_runner=self.probe.runner;self._probe_policy=self.probe.policy;self._bearer=driver._bearer;self._engine_token=worker.engine.token
        self._engine_methods={name:getattr(worker.engine,name).__func__ for name in ('read','route','initialize','upload','action','download')}
        self._driver_origin=driver.origin;self._broker=driver.broker;self._broker_origin=driver.broker.origin;self._broker_open=driver.broker._open
        self._driver_methods={name:getattr(driver,name).__func__ for name in ('_request','_validate_job','_refresh_options','_save','_kind','_bind_claim','_storage')}
        self._broker_method=driver.broker.download_object.__func__;self._broker_clock=driver.broker.clock;self._broker_timeout=driver.broker.timeout
        self._native_client_methods={name:getattr(worker.engine.client,name).__func__ for name in ('request','json')}
        self._native_client_config=(worker.engine.client.origin,worker.engine.client.parsed,worker.engine.client.token,worker.engine.client.node)
        self.journal=self.guard=self.recovery=None
        # A new profile cannot adopt standalone execution/delivery history.
        for component,table in ((worker,'executions'),(driver.outbox,'completions'),(driver.failure_outbox,'completions')):
            with _readonly_sqlite(component.path) as db:
                if db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone():raise CombinedPreparationHeld('standalone_history_requires_separate_qualified_migration')

    def _check(self,lease):
        from .reference_driver import DriverStepLease
        if type(lease) is not DriverStepLease:raise CombinedPreparationHeld('actual_driver_step_lease_required')
        lease.check(self.driver);self.owner.check(self.root);self.client.check()
        driver=self.driver;worker=driver.worker;core=self.policy.pipeline
        if (self._pid!=os.getpid() or self.policy.sha256!=self._policy_sha or driver._combined_policy is not self.policy or driver.path!=self._driver_path or _private(driver.path)[1]!=self._driver_identity
                or any(a is not b for a,b in zip((driver,worker,driver.ledger,driver._state_owner,driver._ssl_context,self.probe,self.client,worker.engine,worker.engine.client,driver.outbox,driver.failure_outbox),self._components))
                or driver._open is not driver._initial_open or driver._bearer!=self._bearer or worker.engine.token!=self._engine_token
                or driver.origin!=self._driver_origin or driver.broker is not self._broker or driver.broker.origin!=self._broker_origin or driver.broker._open is not self._broker_open
                or driver.broker.clock is not self._broker_clock or driver.broker.timeout!=self._broker_timeout
                or getattr(driver.broker.download_object,'__func__',None) is not self._broker_method or getattr(driver.broker.download_object,'__self__',None) is not self._broker
                or any(getattr(getattr(driver,name),'__func__',None) is not function or getattr(getattr(driver,name),'__self__',None) is not driver for name,function in self._driver_methods.items())
                or any(getattr(getattr(worker.engine.client,name),'__func__',None) is not function or getattr(getattr(worker.engine.client,name),'__self__',None) is not worker.engine.client for name,function in self._native_client_methods.items())
                or (worker.engine.client.origin,worker.engine.client.parsed,worker.engine.client.token,worker.engine.client.node)!=self._native_client_config
                or self.probe.runner is not self._probe_runner or self.probe.policy is not self._probe_policy
                or worker.engine.origin!=self.policy.native.engine_origin or worker.engine.client.origin!=worker.engine.origin
                or (driver.node_id,driver.worker_id,driver.worker_instance_id,driver.output_budget,worker.engine_id,worker.authority_id,worker.origin,worker.gate_generation,worker.version)!=('main',core.worker_id,core.worker_instance_id,core.output_budget,core.engine_id,core.authority_id,core.coordinator_origin,core.gate_generation,core.odm_version)
                or driver.execution_router is not None or driver.provenance_client is not None or driver.engine_cleanup is not None or driver.cleanup_executor is not None
                or any(getattr(getattr(worker.engine,name),'__func__',None) is not function or getattr(getattr(worker.engine,name),'__self__',None) is not worker.engine for name,function in self._engine_methods.items())):raise CombinedPreparationHeld('combined_preparer_owner_changed')

    def before_step(self,lease):
        self._check(lease)
        allowed=('claim_pending','reservation_pending','awaiting_grant','fetching_pending','fetching','processing_pending','execution_pending','combined_prepared_held')
        with _readonly_sqlite(self.driver.path) as db:
            if db.execute('SELECT 1 FROM driver_jobs WHERE stage NOT IN ('+','.join('?' for _ in allowed)+') LIMIT 1',allowed).fetchone():raise CombinedPreparationHeld('legacy_driver_lifecycle_not_permitted_in_prepare_profile')
        for component,table in ((self.driver.worker,'executions'),(self.driver.outbox,'completions'),(self.driver.failure_outbox,'completions')):
            with _readonly_sqlite(component.path) as db:
                if db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone():raise CombinedPreparationHeld('standalone_history_requires_separate_qualified_migration')

    def is_combined(self,job):
        with _readonly_sqlite(self.driver.path) as db:
            size=db.execute('SELECT length(CAST(data AS BLOB)) FROM driver_jobs WHERE job=?',(job,)).fetchone()
            if size is None:return False
            if size[0]>2*p.MAX_BODY_BYTES:raise CombinedPreparationHeld('bounded_private_driver_row_required')
            data=_private_json(db.execute('SELECT data FROM driver_jobs WHERE job=?',(job,)).fetchone()[0])
        claim=data.get('claim')
        return type(claim) is dict and self.driver._pipeline_kind(claim['submission'])=='combined'

    def _record(self,job):
        with _readonly_sqlite(self.driver.path) as db:
            limit=db.execute('SELECT stage,length(CAST(data AS BLOB)) AS size FROM driver_jobs WHERE job=?',(job,)).fetchone()
            if limit is None or limit['size']>2*p.MAX_BODY_BYTES or limit['stage'] not in ('execution_pending','combined_prepared_held'):raise CombinedPreparationHeld('actual_fetched_combined_record_required')
            row=db.execute('SELECT claim_request,stage,data FROM driver_jobs WHERE job=?',(job,)).fetchone()
        data=_private_json(row['data']);claim=data['claim'];self.driver._validate_job(claim)
        if self.driver._pipeline_kind(claim['submission'])!='combined' or set(data['inputPaths'])!={obj['id'] for obj in claim['submission']['manifest']['objects']}:raise CombinedPreparationHeld('complete_original_combined_inputs_required')
        return dict(row),data,claim

    def _parent(self,row,data,claim,receipt):
        cfg=self.policy.pipeline
        return dict(jobId=claim['jobId'],generation=claim['generation'],environment=claim['environment'],nodeId='main',workerId=cfg.worker_id,workerInstanceId=cfg.worker_instance_id,
            authorityId=cfg.authority_id,engineId=cfg.engine_id,coordinatorOrigin=cfg.coordinator_origin,gateGeneration=cfg.gate_generation,
            manifestSha256=claim['manifestSha256'],pipelineDigest=claim['pipelineDigest'],claimRequestId=row['claim_request'],claimTokenSha256=hashlib.sha256(claim['claimToken'].encode()).hexdigest(),
            reservationId=data['reservation']['reservationId'],gateReceiptId=receipt,inputBytes=claim['submission']['manifest']['totalBytes'],outputBudget=cfg.output_budget)

    def _fresh(self,parent,claim):
        observed=self.client.snapshot(parent['jobId'],claim['claimToken'])
        if any(not exact(observed[key],parent[key]) for key in ('jobId','generation','environment','nodeId','workerId','workerInstanceId','authorityId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')) or observed['status']!='running' or observed['heldReason'] is not None or observed['executionPhase']!='processing':raise CombinedPreparationHeld('fresh_running_original_claim_required')
        self.probe.check();self.driver._refresh_options();self.driver._validate_job(claim)
        info=self.driver.worker.engine.read('/info');tasks=self.driver.worker.engine.read('/task/list')
        if type(info) is not dict or type(info.get('taskQueueCount')) is not int or info['taskQueueCount']!=0 or type(tasks) is not list or len(tasks)>100:raise CombinedPreparationHeld('idle_original_engine_required')
        seen=set()
        for item in tasks:
            identifier=item.get('uuid');p._uuid(identifier,'retained native task')
            if identifier in seen or identifier==parent['jobId']:raise CombinedPreparationHeld('native_task_requires_separate_reconciliation')
            seen.add(identifier);status=self.driver.worker.engine.read('/task/'+identifier+'/info').get('status')
            if type(status) is not dict or type(status.get('code')) is not int or status['code'] not in (30,40,50):raise CombinedPreparationHeld('native_engine_not_idle')

    def _inputs(self,parent,data,claim):
        for obj in claim['submission']['manifest']['objects']:self.driver.worker._file(parent['reservationId'],data['inputPaths'][obj['id']],obj['byteSize'],obj['sha256'])
        with _readonly_sqlite(self.driver.ledger.path) as db:
            self.driver.ledger.validate_accounting(db);reserved=db.execute('SELECT * FROM reservations WHERE id=?',(parent['reservationId'],)).fetchone()
        if not reserved:raise CombinedPreparationHeld('original_parent_reservation_required')
        settings=_private_json(reserved['settings'])
        if ((reserved['job'],reserved['generation'],reserved['environment'],settings['manifest_sha256'],settings['input_bytes'],settings['output_budget'])!=(parent['jobId'],parent['generation'],parent['environment'],parent['manifestSha256'],parent['inputBytes'],parent['outputBudget'])
                or reserved['state']!='reserved' or reserved['allocated']<parent['inputBytes'] or reserved['expires']<=self.driver.clock()):raise CombinedPreparationHeld('unstarted_full_original_reservation_required')

    def _table(self,db,*,create=False):
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='combined_parent_preparations'").fetchone()
        if row is None:
            if not create:return False
            db.execute(TABLE)
        elif row[0]!=TABLE:raise CombinedPreparationHeld('combined_preparation_schema_changed')
        inventory=[tuple(value) for value in db.execute("SELECT type,name,sql FROM sqlite_master WHERE tbl_name='combined_parent_preparations' ORDER BY type,name")]
        if inventory!=[('index','sqlite_autoindex_combined_parent_preparations_1',None),('table','combined_parent_preparations',TABLE)]:raise CombinedPreparationHeld('combined_preparation_schema_changed')
        count,size,largest=db.execute('SELECT COUNT(*),COALESCE(SUM(length(CAST(identity AS BLOB))),0),COALESCE(MAX(length(CAST(identity AS BLOB))),0) FROM combined_parent_preparations').fetchone()
        if count>self.policy.max_preparations or size>self.policy.max_preparations*self.policy.max_intent_bytes or largest>self.policy.max_intent_bytes:raise CombinedPreparationHeld('combined_preparation_history_cap')
        return True

    def _phase(self,job,phase):
        with self.driver._db() as db:
            self._table(db);db.execute('UPDATE combined_parent_preparations SET phase=? WHERE job=?',(phase,job))

    @staticmethod
    def _gate_base(state,parent):
        expected=dict(engineId=parent['engineId'],nodeId=parent['nodeId'],authorityId=parent['authorityId'],coordinatorOrigin=parent['coordinatorOrigin'],generation=parent['gateGeneration'])
        if any(not exact(state[key],value) for key,value in expected.items()):raise CombinedPreparationHeld('original_physical_gate_changed')

    def _core(self,parent,claim):
        if self.journal is None:
            journal=PipelineJournal(self.root/'combined-pipeline.sqlite',self.policy.pipeline,verify_provenance=self._metadata_unavailable,storage_page_cap=self.policy.pipeline_page_cap)
            tokens=DriverClaimTokens(self.driver.path,journal=journal,policy=self.client.policy,state_owner=self.owner)
            guard=ProductionParentGuard(journal=journal,ledger=self.driver.ledger,client=self.client,tokens=tokens,state_owner=self.owner)
            recovery=OriginalClaimRecovery(self.root/'combined-recovery.sqlite',parent_guard=guard,policy=self.policy.recovery)
            self.journal,self.guard,self.recovery=journal,guard,recovery
        with _readonly_sqlite(self.journal.path) as db:exists=db.execute('SELECT 1 FROM pipeline_parents WHERE job=?',(parent['jobId'],)).fetchone()
        if exists:
            with self.journal._operation():_,plan,_,_,_,_=self.journal._read(parent['jobId'])
        else:plan=self.journal.bind_parent(claim,claim_request_id=parent['claimRequestId'],reservation_id=parent['reservationId'],gate_receipt_id=parent['gateReceiptId'],guard=self.guard)
        if plan['parentBinding']!=parent:raise CombinedPreparationHeld('original_combined_parent_changed')
        self.recovery.prepare(parent)
        self.recovery.recover(parent)

    @staticmethod
    def _metadata_unavailable(_):return False

    def prepare(self,job,*,lease):
        self._check(lease);row,data,claim=self._record(job)
        with self.driver._db() as db:
            exists=self._table(db)
            local=db.execute('SELECT identity,phase FROM combined_parent_preparations WHERE job=?',(job,)).fetchone() if exists else None
        if local:
            identity=_private_json(local['identity']);parent=identity['parentBinding'];phase=local['phase']
            expected=dict(schemaVersion=1,protocol='ltds-parent-gate-preparation-v1',policySha256=self.policy.sha256,parentBinding=self._parent(row,data,claim,parent['gateReceiptId']),transferSha256=p.digest(data['reservation']),inputPathsSha256=p.digest(data['inputPaths']))
            if not exact(identity,expected) or phase not in PHASES:raise CombinedPreparationHeld('original_gate_preparation_changed')
        else:
            parent=self._parent(row,data,claim,secrets.token_hex(16));phase='reserve_pending';self._inputs(parent,data,claim);self._fresh(parent,claim)
            identity=dict(schemaVersion=1,protocol='ltds-parent-gate-preparation-v1',policySha256=self.policy.sha256,parentBinding=parent,transferSha256=p.digest(data['reservation']),inputPathsSha256=p.digest(data['inputPaths']))
            if len(p.canonical_json(identity))>self.policy.max_intent_bytes:raise CombinedPreparationHeld('bounded_gate_intent_required')
            with self.driver._db() as db:
                self._table(db,create=True)
                if db.execute('SELECT COUNT(*) FROM combined_parent_preparations').fetchone()[0]>=self.policy.max_preparations:raise CombinedPreparationHeld('combined_preparation_history_cap')
                db.execute('INSERT INTO combined_parent_preparations VALUES(?,?,?)',(job,p.canonical_json(identity).decode(),phase))
            with NodeGate(parent['engineId']) as gate:
                self._check(lease);state=gate.snapshot();self._gate_base(state,parent)
                if state['reservation'] is not None:raise CombinedPreparationHeld('gate_occupied_before_original_reserve')
                self._fresh(parent,claim)
                gate.reserve('main',parent['coordinatorOrigin'],parent['authorityId'],parent['gateGeneration'],parent['gateReceiptId'])
            self._phase(job,'reserved');phase='reserved'
        # Snapshot reconciliation proves a committed local mutation, never mere
        # job-name equality. Absent/conflicting unknowns remain held.
        with NodeGate(parent['engineId']) as gate:
            self._check(lease);state=gate.snapshot();self._gate_base(state,parent);held=state['reservation']
            expected_job=dict(id=job,workerId=parent['workerId'],nodeId='main',authorityId=parent['authorityId'])
            if not held or held['receiptId']!=parent['gateReceiptId'] or held['completionDigest'] is not None or held['job'] not in (None,expected_job):raise CombinedPreparationHeld('unknown_original_gate_outcome_held')
            if held['job']==expected_job:
                self._phase(job,'bound');phase='bound'
            elif phase in ('reserve_pending','reserved'):
                self._phase(job,'bind_pending');self._fresh(parent,claim)
                gate.bind(parent['gateReceiptId'],expected_job);self._phase(job,'bound');phase='bound'
            else:raise CombinedPreparationHeld('unknown_bind_outcome_held')
        self._inputs(parent,data,claim)
        self._core(parent,claim);self._phase(job,'prepared')
        return dict(state='combined_prepared_held',jobId=job,reason='combined_metadata_and_final_transport_unqualified')
