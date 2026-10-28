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


class ExecutionRouter:
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
            # Router versions belong to individual jobs. Idle ODM upgrades can
            # retain accepted reads; TreeDar's controller/delivery journals still
            # pin their full runtime and require a future migration design.
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
            if for_execution or not self._retained(route,worker):
                raise RouteConflict('pending_execution_runtime_binding_changed')
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

    def apply_viewer_acceptance(self,job_id,receipts):
        self._row(job_id)
        return self.worker_for(job_id).apply_viewer_acceptance(job_id,receipts)
