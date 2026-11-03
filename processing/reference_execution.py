"""Durable per-job execution selection; not a queue or readiness advertisement.

Bind a validated claim before reserving/transferring input or mutating an engine.
ODM numeric/bool discovery and photo uploads stay with the actual ODM worker.
Combined ODM→TreeDar still needs one sequential stage plan and final ACK contract
before public TreeDar availability may be enabled; it is deliberately rejected.
There is no automatic legacy-execution adoption: a new route must precede engine
intent. The composing driver must refuse enablement with unbound legacy claims.
"""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import stat

from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_engine_worker import ReferenceEngineWorker
from .reference_failure import FailureOutbox, validate_failure
from .treedar_delivery import TreeDarDelivery
from .viewer_service import NativeEngine


class RouteConflict(ValueError):
    """Unknown or immutable execution binding cannot safely be selected."""


def _migration_rows(path, query):
    """Read a quiescent journal without opening a component's write context."""
    path=Path(path).absolute()
    if not os.path.lexists(path):return []
    if path.resolve()!=path or any(item.is_symlink() for item in (path,*path.parents)):
        raise RouteConflict('migration_journal_unavailable')
    info=path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
            or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))
            or any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal'))):
        raise RouteConflict('migration_journal_unavailable')
    db=sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)
    db.row_factory=sqlite3.Row
    try:return [dict(row) for row in db.execute(query)]
    finally:db.close()


class ExecutionRouter:
    @staticmethod
    def migration_census(path, *, driver_path, physical_binding, odm_worker, failure_outbox, delivery_db, current_envelope):
        """Closed read-only driver/route census under external exclusive owners."""
        from .reference_driver import ReferenceDriver
        drivers=ReferenceDriver.migration_census(driver_path,physical_binding=physical_binding)
        rows=_migration_rows(path,'SELECT * FROM execution_routes ORDER BY job')
        routes={row['job']:row for row in rows}
        if len(routes)!=len(rows) or set(routes)!=set(drivers['jobs']):
            raise RouteConflict('migration_driver_route_set_changed')
        bindings=_migration_rows(path,'SELECT value FROM router_binding WHERE id=1')
        if rows and len(bindings)!=1:raise RouteConflict('migration_router_binding_unavailable')
        if bindings:
            stored=json.loads(bindings[0]['value'])
            if any(stored.get(key)!=physical_binding[key] for key in
                    ('nodeId','workerId','engineId','authorityId','origin','gateGeneration')):
                raise RouteConflict('migration_router_binding_changed')
        projections={row['job']:dict(row) for row in delivery_db.execute('SELECT * FROM executions')}
        odm_rows={row['job']:row for row in _migration_rows(odm_worker.path,'SELECT * FROM executions')}
        expected_odm={job for job,row in routes.items() if row['kind']=='odm'}
        if set(odm_rows)!=expected_odm:raise RouteConflict('migration_orphan_odm_intent')
        tree_jobs={job for job,row in routes.items() if row['kind']=='treedar'}
        if set(projections)!=tree_jobs:raise RouteConflict('migration_orphan_tree_projection')
        storage=dict(reservations=_migration_rows(odm_worker.ledger.path,'SELECT * FROM reservations'),
            success=_migration_rows(odm_worker.outbox.path,'SELECT * FROM completions'),
            failure=_migration_rows(failure_outbox.path,'SELECT * FROM completions'),
            successReceipts=_migration_rows(odm_worker.outbox.path,'SELECT * FROM durable_receipts'),
            failureReceipts=_migration_rows(failure_outbox.path,'SELECT * FROM durable_receipts'))
        for reserved in storage['reservations']:
            if reserved['state']=='expired':continue
            driver=drivers['jobs'].get(reserved['job'])
            if (driver is None or reserved['state']!='terminal'
                    or (reserved['generation'],reserved['environment'])!=(
                        driver['binding']['generation'],driver['binding']['environment'])
                    or json.loads(reserved['settings'])['manifest_sha256']!=driver['binding']['manifestSha256']):
                raise RouteConflict('migration_orphan_scratch_reservation')
        for key,field in (('success','outboxId'),('failure','failureOutboxId')):
            expected={driver[field] for driver in drivers['jobs'].values() if driver[field] is not None}
            if {record['id'] for record in storage[key]}!=expected:
                raise RouteConflict('migration_orphan_terminal_outbox')
            if {receipt['outbox'] for receipt in storage[key+'Receipts']}!=expected:
                raise RouteConflict('migration_orphan_durable_receipt')
            for record in storage[key]:
                driver=drivers['jobs'].get(record['job'])
                if (driver is None or record['id']!=driver[field] or record['state']!='acknowledged'
                        or (record['generation'],record['environment'])!=(
                            driver['binding']['generation'],driver['binding']['environment'])):
                    raise RouteConflict('migration_terminal_outbox_not_closed')
        jobs={}
        for job_id,route in routes.items():
            driver=drivers['jobs'][job_id]
            binding=json.loads(route['binding']);runtime=json.loads(route['runtime'])
            if binding!=driver['binding'] or route['kind']!=driver['kind']:
                raise RouteConflict('migration_route_claim_changed')
            if route['kind']=='odm':
                ExecutionRouter._migration_odm_closed(route,driver,odm_rows[job_id],odm_worker,storage)
                continue
            if route['kind']!='treedar':raise RouteConflict('migration_pipeline_unavailable')
            p._closed(runtime,('engineVersion','imageId','sourceSha256','workerJournal','controllerJournal'),'runtime')
            local=projections[job_id]
            if (local['phase']!='delivered' or json.loads(local['identity'])['job']!=binding
                    or local['outcome'] not in ('succeeded','failed')):
                raise RouteConflict('migration_tree_not_delivered')
            succeeded=local['outcome']=='succeeded'
            if (driver['outboxId']!=local['outbox_id'] or driver['failureOutboxId']!=local['failure_outbox_id']
                    or (driver['stage']=='awaiting_acceptance' and not succeeded)
                    or (driver['stage']=='awaiting_failure_acceptance' and succeeded)
                    or (succeeded and driver['failureSha256'] is not None)
                    or (not succeeded and driver['failureSha256']!=p.digest(json.loads(local['failure_json'])))):
                raise RouteConflict('migration_tree_driver_seal_changed')
            jobs[job_id]=dict(binding=binding,runtime=runtime,stage=driver['stage'],
                outboxId=driver['outboxId'],failureOutboxId=driver['failureOutboxId'])
        return dict(schemaVersion=1,jobs=jobs,driverSha256=drivers['sha256'],routerSha256=p.digest(rows))

    @staticmethod
    def _migration_odm_closed(route,driver,local,worker,storage):
        """Validate terminal ODM metadata without updating outbox clock state."""
        binding=json.loads(route['binding']);identity=json.loads(local['identity'])
        if (any(identity['job'].get(key)!=value for key,value in binding.items())
                or local['phase'] not in ('delivered','failure_delivered')):
            raise RouteConflict('migration_odm_not_delivered')
        reserved=[row for row in storage['reservations']
                  if row['id']==identity['reservation']]
        if (len(reserved)!=1 or reserved[0]['state']!='terminal'
                or (reserved[0]['job'],reserved[0]['generation'],reserved[0]['environment'])!=(
                    binding['jobId'],binding['generation'],binding['environment'])):
            raise RouteConflict('migration_odm_ledger_not_terminal')
        succeeded=local['phase']=='delivered'
        if ((driver['stage']=='awaiting_acceptance' and not succeeded)
                or (driver['stage']=='awaiting_failure_acceptance' and succeeded)
                or (succeeded and (driver['failureOutboxId'] is not None or driver['failureSha256'] is not None))
                or (not succeeded and driver['outboxId'] is not None)):
            raise RouteConflict('migration_driver_outcome_changed')
        identifier=driver['outboxId'] if succeeded else driver['failureOutboxId']
        records=[row for row in storage['success' if succeeded else 'failure'] if row['id']==identifier]
        if len(records)!=1:raise RouteConflict('migration_odm_ack_unavailable')
        record=records[0];payload=json.loads(record['payload'])
        if (record['state']!='acknowledged' or not record['receipt'] or record['attempts']<1
                or record['digest']!=p.digest(payload)):
            raise RouteConflict('migration_odm_ack_unavailable')
        receipt=json.loads(record['receipt'])
        durable=[row for row in storage['successReceipts' if succeeded else 'failureReceipts'] if row['outbox']==identifier]
        if len(durable)!=1 or durable[0]['payload']!=record['receipt']:
            raise RouteConflict('migration_durable_ack_unavailable')
        if succeeded:
            from .worker_outbox import _id
            result=p.validate_result_manifest(json.loads(local['result']),job_id=binding['jobId'],
                generation=binding['generation'],environment=binding['environment'],pipeline_digest=binding['pipelineDigest'])
            expected=dict(jobId=binding['jobId'],generation=binding['generation'],environment=binding['environment'],
                inputManifestSha256=binding['manifestSha256'],pipelineDigest=binding['pipelineDigest'],
                resultManifestSha256=result['resultManifestSha256'])
            required={'type','durable','receiptId','storageReceiptId','outboxId','payloadSha256','acceptedObjectIds',*expected}
            _id(receipt.get('receiptId'));_id(receipt.get('storageReceiptId'))
            if (set(receipt)!=required or type(receipt.get('generation')) is not int
                    or durable[0]['id']!=receipt['receiptId'] or durable[0]['storage_id']!=receipt['storageReceiptId']
                    or local['outbox_id']!=identifier or identifier!=p.digest(expected)
                    or payload!=dict(**expected,resultManifest=result,terminalState='succeeded')
                    or result['engineVersions']!={'odm':json.loads(route['runtime'])['engineVersion']}
                    or receipt.get('type')!='durable-result-receipt' or receipt.get('durable') is not True
                    or receipt.get('outboxId')!=identifier or receipt.get('payloadSha256')!=record['digest']
                    or any(receipt.get(key)!=value for key,value in expected.items())
                    or sorted(receipt.get('acceptedObjectIds',[]))!=sorted(obj['id'] for obj in result['objects'])):
                raise RouteConflict('migration_odm_ack_binding_changed')
        else:
            from .reference_failure import _envelope,_receipt
            envelope=dict(outboxId=identifier,payloadSha256=record['digest'],payload=payload)
            failure=_envelope(envelope)
            validate_failure(failure,job_id=binding['jobId'],generation=binding['generation'],
                environment=binding['environment'],input_manifest_sha256=binding['manifestSha256'],pipeline_digest=binding['pipelineDigest'])
            if (p.canonical_json(receipt)!=p.canonical_json(_receipt(envelope,failure))
                    or durable[0]['id']!=receipt.get('receiptId') or durable[0]['storage_id']!=receipt.get('receiptId')
                    or driver['failureSha256']!=p.digest(failure)):
                raise RouteConflict('migration_failure_ack_binding_changed')

    def __init__(self,path,odm_worker,treedar_worker=None,*,failure_outbox):
        if (not isinstance(odm_worker,ReferenceEngineWorker) or isinstance(odm_worker,TreeDarDelivery)
                or not isinstance(odm_worker.engine,NativeEngine) or not isinstance(failure_outbox,FailureOutbox)):
            raise ValueError('actual ODM worker and explicit shared failure outbox required')
        self.odm_worker,self.treedar_worker=odm_worker,treedar_worker
        self.ledger,self.outbox,self.failure_outbox=odm_worker.ledger,odm_worker.outbox,failure_outbox
        for name in ('node_id','worker_id','engine_id','authority_id','origin','gate_generation'):
            setattr(self,name,getattr(odm_worker,name))
        if treedar_worker is not None:
            if (not isinstance(treedar_worker,TreeDarDelivery) or self.node_id!='main'
                    or treedar_worker.ledger is not self.ledger or treedar_worker.outbox is not self.outbox
                    or treedar_worker.failure_outbox is not failure_outbox
                    or any(getattr(treedar_worker,name)!=getattr(self,name) for name in
                           ('node_id','worker_id','engine_id','authority_id','origin','gate_generation'))):
                raise ValueError('TreeDar must share the exact main-host durable and physical binding')
        self._physical_check()
        self.path=Path(path).absolute()
        self.path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        if any(item.is_symlink() for item in (self.path,*self.path.parents)):
            raise ValueError('private route journal required')
        parent=self.path.parent.stat()
        if os.name!='nt' and (parent.st_uid!=os.geteuid() or parent.st_mode & 0o077):
            raise ValueError('owned private route journal required')
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
        try:
            info,current=os.fstat(fd),self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                    or (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino)
                    or (os.name!='nt' and info.st_uid!=os.geteuid())):
                raise ValueError('owned route journal file required')
            if os.name!='nt':
                os.fchmod(fd,0o600)
        finally:
            os.close(fd)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS router_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS execution_routes (
                job TEXT PRIMARY KEY,binding TEXT NOT NULL,kind TEXT NOT NULL,runtime TEXT NOT NULL)''')
            # Versions belong to individual jobs. ODM retains accepted reads;
            # managed TreeDar epochs retain sealed historical projections.
            # Unmanaged TreeDar journals continue to pin their complete runtime.
            encoded=p.canonical_json(dict(nodeId=self.node_id,workerId=self.worker_id,engineId=self.engine_id,
                authorityId=self.authority_id,origin=self.origin,gateGeneration=self.gate_generation,
                ledger=str(self.ledger.path),outbox=str(self.outbox.path),failureOutbox=str(failure_outbox.path),
                odmOrigin=odm_worker.engine.origin)).decode()
            old=db.execute('SELECT value FROM router_binding WHERE id=1').fetchone()
            if old and old[0]!=encoded:
                raise RouteConflict('router_physical_binding_changed')
            db.execute('INSERT OR IGNORE INTO router_binding VALUES(1,?)',(encoded,))

    @contextmanager
    def _db(self):
        db=sqlite3.connect(self.path,timeout=20)
        db.row_factory=sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            db.close()

    def _physical_check(self):
        with NodeGate(self.engine_id) as gate:
            state=gate.snapshot()
            if (state['nodeId'],state['authorityId'],state['coordinatorOrigin'],state['generation'])!=(
                    self.node_id,self.authority_id,self.origin.rstrip('/'),self.gate_generation):
                raise RouteConflict('physical_gate_binding_changed')

    def _runtime(self,kind):
        worker=self.odm_worker if kind=='odm' else self.treedar_worker
        if worker is None:
            raise RouteConflict('requested_execution_not_configured')
        if kind=='treedar' and worker.runtime_manager is not None:
            return worker.runtime_descriptor()
        p._string(worker.version,'actual engine version',128)
        if kind=='odm':
            return dict(engineVersion=worker.version,engineOrigin=worker.engine.origin,workerJournal=str(worker.path))
        return dict(engineVersion=worker.version,imageId=worker.controller.config.image_id,
                    sourceSha256=worker.controller.config.source_sha256,workerJournal=str(worker.path),
                    controllerJournal=str(worker.controller.path))

    def _route(self,job_id):
        p._slug(job_id,'jobId')
        with self._db() as db:
            row=db.execute('SELECT * FROM execution_routes WHERE job=?',(job_id,)).fetchone()
        if row is None:
            raise RouteConflict('unknown_execution_route')
        if row['kind'] not in ('odm','treedar'):
            raise RouteConflict('invalid_execution_route_kind')
        return dict(row)

    @staticmethod
    def _identity(job):
        p._uuid(job['jobId'],'jobId'); p._integer(job['generation'],'generation')
        p._environment(job['environment'])
        for key in ('manifestSha256','pipelineDigest'):
            p._hash(job[key],key)
        return {key:job[key] for key in ('jobId','generation','environment','manifestSha256','pipelineDigest','nodeId')}

    def bind(self,job):
        self._physical_check()
        binding=self._identity(job)
        if binding['nodeId']!=self.node_id:
            raise RouteConflict('execution_node_mismatch')
        submission=job['submission']
        pipeline,manifest=submission['pipeline'],submission['manifest']
        if (p.digest(pipeline)!=job['pipelineDigest'] or p.manifest_sha256(manifest)!=job['manifestSha256']
                or manifest['sha256']!=job['manifestSha256'] or pipeline['gaussian_splat']['requested'] is not False):
            raise RouteConflict('execution_submission_binding_changed')
        odm,tree=pipeline['odm']['requested'],pipeline['treedar']['requested']
        if type(odm) is not bool or type(tree) is not bool or odm==tree:
            raise RouteConflict('combined_or_empty_pipeline_requires_stage_plan')
        kind='odm' if odm else 'treedar'
        if kind=='odm':
            if any(obj['kind']!='image' for obj in manifest['objects']):
                raise RouteConflict('odm_image_inputs_required')
        elif (self.node_id!='main' or self.treedar_worker is None
                or len(manifest['objects'])!=1 or manifest['objects'][0]['kind']!='lidar'
                or pipeline['treedar']['inputObjectIds']!=[manifest['objects'][0]['id']]):
            raise RouteConflict('configured_main_lidar_execution_required')
        encoded=p.canonical_json(binding).decode()
        with self._db() as db:
            old=db.execute('SELECT * FROM execution_routes WHERE job=?',(job['jobId'],)).fetchone()
            if old:
                if old['binding']!=encoded or old['kind']!=kind:
                    raise RouteConflict('execution_route_rebound')
            else:
                if job.get('status')!='running':
                    raise RouteConflict('new_route_requires_claimed_job')
                selected=self.odm_worker if kind=='odm' else self.treedar_worker
                with selected._db() as worker_db:
                    if worker_db.execute('SELECT 1 FROM executions WHERE job=?',(job['jobId'],)).fetchone():
                        raise RouteConflict('legacy_execution_requires_explicit_adoption')
                if kind=='treedar':
                    if selected.runtime_manager is not None:
                        selected.runtime_manager.assert_active(self._runtime(kind))
                    with selected.controller._db() as engine_db:
                        if engine_db.execute('SELECT 1 FROM treedar_executions WHERE job=?',(job['jobId'],)).fetchone():
                            raise RouteConflict('legacy_execution_requires_explicit_adoption')
                db.execute('INSERT INTO execution_routes VALUES(?,?,?,?)',
                    (job['jobId'],encoded,kind,p.canonical_json(self._runtime(kind)).decode()))
        self.worker_for(job['jobId'])
        return self.describe(job['jobId'])

    def describe(self,job_id):
        row=self._route(job_id)
        return dict(**json.loads(row['binding']),executionKind=row['kind'],runtimeBinding=json.loads(row['runtime']))

    def kind_for(self,job_id):
        return self._route(job_id)['kind']

    def _local_row(self,route,worker):
        local=worker._row(route['job'])
        identity=json.loads(local['identity'])
        expected=json.loads(route['binding'])
        if any(identity['job'].get(key)!=value for key,value in expected.items()):
            raise RouteConflict('worker_execution_binding_changed')
        if local['result']:
            result=p.validate_result_manifest(json.loads(local['result']),job_id=route['job'],
                generation=expected['generation'],environment=expected['environment'],pipeline_digest=expected['pipelineDigest'])
            if result['engineVersions']!={route['kind']:json.loads(route['runtime'])['engineVersion']}:
                raise RouteConflict('retained_result_runtime_binding_changed')
        return local,identity

    def _retained(self,route,worker):
        try:
            local,identity=self._local_row(route,worker)
            job=json.loads(route['binding'])
            with self.ledger._transaction() as db:
                reserved=self.ledger._reservation(db,identity['reservation'])
                if reserved['state']!='terminal':
                    return False
            if local['result'] and local['outbox_id']:
                completion=self.outbox.get(local['outbox_id'])
                result=json.loads(local['result'])
                binding=dict(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],
                    inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],
                    resultManifestSha256=result['resultManifestSha256'])
                payload=dict(**binding,resultManifest=result,terminalState='succeeded')
                return (completion['state']=='acknowledged' and completion['receipt'] is not None
                    and local['outbox_id']==p.digest(binding) and completion['digest']==p.digest(payload)
                    and p.canonical_json(completion['payload'])==p.canonical_json(payload))
            if local['phase'] not in ('delivered','failure_delivered'):
                return False
            with self.failure_outbox._transaction() as db:
                failure=db.execute('SELECT id FROM completions WHERE job=? AND generation=?',(job['jobId'],job['generation'])).fetchone()
            if failure is None:
                return False
            completion=self.failure_outbox.get(failure['id'])
            validate_failure(completion['payload']['failure'],job_id=job['jobId'],generation=job['generation'],
                environment=job['environment'],input_manifest_sha256=job['manifestSha256'],pipeline_digest=job['pipelineDigest'])
            return completion['state']=='acknowledged' and completion['digest']==p.digest(completion['payload'])
        except Exception:
            return False

    def worker_for(self,job_id,*,for_execution=False):
        """Historical pins may serve retained data, never authorize a new runtime."""
        route=self._route(job_id)
        worker=self.odm_worker if route['kind']=='odm' else self.treedar_worker
        if worker is None:
            raise RouteConflict('routed_worker_unavailable')
        if json.loads(route['runtime'])!=self._runtime(route['kind']):
            if for_execution:
                raise RouteConflict('pending_execution_runtime_binding_changed')
            if route['kind']=='treedar' and worker.runtime_manager is not None:
                worker=worker.retained_view(job_id,json.loads(route['runtime']))
            if not self._retained(route,worker):
                raise RouteConflict('pending_execution_runtime_binding_changed')
        if for_execution and route['kind']=='treedar' and worker.runtime_manager is not None:
            worker.runtime_manager.assert_active(json.loads(route['runtime']))
        return worker

    def _row(self,job_id):
        route=self._route(job_id)
        local,_=self._local_row(route,self.worker_for(job_id))
        return local

    def _file(self,reservation,relative,expected_size=None,expected_sha=None):
        with self.ledger._transaction() as db:
            reserved=self.ledger._reservation(db,reservation)
            settings=json.loads(reserved['settings'])
        route=self._route(reserved['job'])
        job=json.loads(route['binding'])
        if (reserved['generation'],reserved['environment'],settings['manifest_sha256'])!=(
                job['generation'],job['environment'],job['manifestSha256']):
            raise RouteConflict('reservation_route_binding_changed')
        return self.worker_for(reserved['job'])._file(reservation,relative,expected_size,expected_sha)

    def apply_viewer_acceptance(self,job_id,receipts,*,require_verified_copy=False):
        self._row(job_id)
        return self.worker_for(job_id).apply_viewer_acceptance(job_id,receipts,require_verified_copy=require_verified_copy)
