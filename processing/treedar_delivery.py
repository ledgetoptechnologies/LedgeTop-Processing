"""TreeDar terminal-proof bridge into the shared delivery/acceptance machinery.

No coordinator transport, scheduler, availability registration, or engine launch
policy lives here. The host driver selects a TreeDar-only job and authenticates
coordinator responses before passing their exact receipts to acknowledge_receipt.
Coordinator metadata ACK releases the physical execution gate, never output files.
"""
import json
import os
from pathlib import Path
import sqlite3
import stat

from . import reference_protocol as p
from .node_gate import NodeGate, _digest as gate_completion_digest
from .reference_engine_worker import ReferenceEngineWorker, WorkerHeld
from .reference_failure import make_failure, BINDING as FAILURE_BINDING


class TreeDarDelivery(ReferenceEngineWorker):
    """ReferenceResults-compatible worker view of one durable TreeDar controller.

    Generic _file, _row, _phase, and apply_viewer_acceptance reuse the established
    worker implementation. start/poll/acknowledge never use its NodeODM execution
    code. A separate private journal holds delivery projections, not a second queue.
    """
    def __init__(self, path, *, controller, outbox, failure_outbox):
        self.controller = controller
        self.ledger, self.outbox, self.failure_outbox = controller.ledger, outbox, failure_outbox
        config = controller.config
        self.engine = controller
        self.engine_id, self.node_id, self.worker_id = config.engine_id, config.node_id, config.worker_id
        self.authority_id, self.origin = config.authority_id, config.coordinator_origin
        self.gate_generation, self.version = config.gate_generation, config.engine_version
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(item.is_symlink() for item in (self.path, *self.path.parents)):
            raise WorkerHeld('owned_private_delivery_journal_required')
        parent = self.path.parent.stat()
        if os.name != 'nt' and (parent.st_uid != os.geteuid() or parent.st_mode & 0o077):
            raise WorkerHeld('private_delivery_journal_permissions_required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os,'O_NOFOLLOW',0), 0o600)
        try:
            info, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or (info.st_dev,info.st_ino) != (current.st_dev,current.st_ino)
                    or (os.name != 'nt' and info.st_uid != os.geteuid())):
                raise WorkerHeld('owned_delivery_journal_required')
            if os.name != 'nt':
                os.fchmod(descriptor,0o600)
        finally:
            os.close(descriptor)
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS executions (
                job TEXT PRIMARY KEY, identity TEXT NOT NULL, receipt TEXT NOT NULL,
                phase TEXT NOT NULL, result TEXT, outbox_id TEXT,
                proof TEXT NOT NULL, outcome TEXT NOT NULL,
                failure_json TEXT, failure_outbox_id TEXT)''')
            db.execute('CREATE TABLE IF NOT EXISTS delivery_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            binding = p.canonical_json(dict(controllerPath=str(controller.path),nodeId=self.node_id,
                workerId=self.worker_id,engineId=self.engine_id,authorityId=self.authority_id,
                origin=self.origin,gateGeneration=self.gate_generation,imageId=config.image_id,
                sourceSha256=config.source_sha256,engineVersion=self.version,
                ledgerPath=str(self.ledger.path),outboxPath=str(outbox.path),failureOutboxPath=str(failure_outbox.path))).decode()
            old = db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()
            if old and old[0] != binding:
                raise WorkerHeld('delivery_controller_binding_changed')
            db.execute('INSERT OR IGNORE INTO delivery_binding VALUES(1,?)',(binding,))

    def start(self, job, *, reservation_id, input_paths, provenance, allow_synthetic=False):
        pipeline = job['submission']['pipeline']
        if (pipeline['odm']['requested'] or not pipeline['treedar']['requested']
                or pipeline['gaussian_splat']['requested']
                or len(pipeline['treedar']['inputObjectIds']) != 1
                or set(input_paths) != set(pipeline['treedar']['inputObjectIds'])):
            raise WorkerHeld('treedar_only_job_required')
        object_id = pipeline['treedar']['inputObjectIds'][0]
        obj = next((obj for obj in job['submission']['manifest']['objects'] if obj['id']==object_id),None)
        if obj is None:
            raise WorkerHeld('treedar_input_object_unavailable')
        return self.controller.start(job,reservation_id=reservation_id,input_object=obj,
            input_relative_path=input_paths[object_id],provenance=provenance,allow_synthetic=allow_synthetic)

    def _project(self, job_id, proof):
        execution = self.controller._row(job_id)
        source = json.loads(execution['identity'])
        if (execution['proof'] is None or p.canonical_json(json.loads(execution['proof'])) != p.canonical_json(proof)
                or proof.get('job') != source['job'] or proof.get('engineTerminal') is not True
                or proof.get('containerId') != execution['container']
                or proof.get('imageId') != self.controller.config.image_id
                or proof.get('status') not in ('succeeded','failed')):
            raise WorkerHeld('exact_terminal_controller_proof_required')
        job = dict(source['job'],nodeId=self.node_id)
        identity = dict(job=job,reservation=source['reservationId'],proofSha256=p.digest(proof))
        result = None
        if proof['status']=='succeeded':
            result = p.validate_result_manifest(proof['resultManifest'],job_id=job_id,
                generation=job['generation'],environment=job['environment'],pipeline_digest=job['pipelineDigest'])
            if (result['engineVersions'] != {'treedar':self.version}
                    or set(proof['localObjects']) != {obj['id'] for obj in result['objects']}
                    or any(obj['role']=='odm_archive' for obj in result['objects'])):
                raise WorkerHeld('treedar_result_pipeline_or_version_changed')
            expected_prefix = source['outputRelativePath']+'/run/'
            for obj in result['objects']:
                relative = proof['localObjects'][obj['id']]
                if (not isinstance(relative,str) or not relative.startswith(expected_prefix)
                        or relative==source['inputRelativePath']):
                    raise WorkerHeld('treedar_result_path_binding_changed')
                self.ledger._owned_path(source['reservationId'],relative)
        encoded, proof_json = p.canonical_json(identity).decode(),p.canonical_json(proof).decode()
        with self._db() as db:
            old = db.execute('SELECT * FROM executions WHERE job=?',(job_id,)).fetchone()
            if old:
                if (old['identity'],old['receipt'],old['proof'],old['outcome'],old['result']) != (
                        encoded,execution['receipt'],proof_json,proof['status'],p.canonical_json(result).decode() if result else None):
                    raise WorkerHeld('sealed_delivery_projection_changed')
            else:
                db.execute("INSERT INTO executions VALUES(?,?,?,'proof_durable',?,NULL,?,?,NULL,NULL)",
                    (job_id,encoded,execution['receipt'],p.canonical_json(result).decode() if result else None,
                     proof_json,proof['status']))
        return self._row(job_id)

    @staticmethod
    def _completion(row):
        if row['outcome']=='succeeded':
            return dict(status='succeeded',result={'resultManifestSha256':json.loads(row['result'])['resultManifestSha256']})
        failure = json.loads(row['failure_json'])
        return dict(status='failed',result={key:failure[key] for key in ('failureSha256','terminalState','errorCode')})

    def _outbox(self,row):
        return (self.outbox,row['outbox_id']) if row['outcome']=='succeeded' else (self.failure_outbox,row['failure_outbox_id'])

    def _bound_outbox(self,row):
        box,identifier=self._outbox(row)
        if not identifier:
            raise WorkerHeld('terminal_outbox_not_durable')
        identity=json.loads(row['identity'])
        job=identity['job']
        if row['outcome']=='succeeded':
            result=json.loads(row['result'])
            binding=dict(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],
                inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],
                resultManifestSha256=result['resultManifestSha256'])
            expected=dict(**binding,resultManifest=result,terminalState='succeeded')
            local=dict(ownedPrefix='jobs/'+identity['reservation'],localObjects=json.loads(row['proof'])['localObjects'])
        else:
            failure=json.loads(row['failure_json'])
            binding={key:failure[key] for key in FAILURE_BINDING}
            expected=dict(failure=failure,engineTerminal=True)
            local={}
        completion=box.get(identifier)
        if (identifier!=p.digest(binding) or completion['digest']!=p.digest(expected)
                or p.canonical_json(completion['payload'])!=p.canonical_json(expected)
                or completion['local_metadata']!=local):
            raise WorkerHeld('terminal_outbox_binding_changed')
        return box,identifier,completion

    def poll(self, job_id):
        """Idempotently project terminal proof, register results, and queue metadata."""
        proof = self.controller.poll(job_id)
        if proof is None:
            return None
        row = self._project(job_id,proof)
        if row['phase']=='delivered':
            return self._bound_outbox(row)[1]
        identity = json.loads(row['identity'])
        job,reservation = identity['job'],identity['reservation']
        if row['outcome']=='succeeded':
            result = json.loads(row['result'])
            self.ledger.register_result(reservation,job_id=job_id,generation=job['generation'],
                manifest_sha256=job['manifestSha256'],pipeline_sha256=job['pipelineDigest'],
                result_manifest_sha256=result['resultManifestSha256'],objects=[dict(object_id=obj['id'],
                    bytes=obj['byteSize'],sha256=obj['sha256'],relative_path=proof['localObjects'][obj['id']]) for obj in result['objects']])
            outbox_id = self.outbox.persist_terminal(job_id=job_id,generation=job['generation'],environment=job['environment'],
                input_manifest_sha256=job['manifestSha256'],pipeline_digest=job['pipelineDigest'],
                result_manifest_sha256=result['resultManifestSha256'],result_manifest=result,
                owned_prefix='jobs/'+reservation,local_objects=proof['localObjects'])
            with self._db() as db:
                db.execute("UPDATE executions SET outbox_id=?,phase='awaiting_delivery' WHERE job=?",(outbox_id,job_id))
        else:
            failure = make_failure(job_id=job_id,generation=job['generation'],environment=job['environment'],
                input_manifest_sha256=job['manifestSha256'],pipeline_digest=job['pipelineDigest'],
                terminal_state='failed',error_code=proof['errorCode'])
            if row['failure_json'] and p.canonical_json(json.loads(row['failure_json']))!=p.canonical_json(failure):
                raise WorkerHeld('sealed_failure_changed')
            with self._db() as db:
                db.execute('UPDATE executions SET failure_json=? WHERE job=?',(p.canonical_json(failure).decode(),job_id))
            outbox_id = self.failure_outbox.persist(failure,engine_terminal=True)
            with self._db() as db:
                db.execute("UPDATE executions SET failure_outbox_id=?,phase='awaiting_delivery' WHERE job=?",(outbox_id,job_id))
        row = self._row(job_id)
        box,identifier,completion = self._bound_outbox(row)
        if completion['state']=='acknowledged':
            self.acknowledge_delivery(job_id)
        else:
            with NodeGate(self.engine_id) as gate:
                self.controller._gate(gate,row['receipt'],job_id)
                gate.prepare_completion(row['receipt'],self._completion(row))
        return outbox_id

    def completion(self, job_id):
        row = self._row(job_id)
        _,identifier,_ = self._bound_outbox(row)
        return dict(kind='result' if row['outcome']=='succeeded' else 'failure',outboxId=identifier)

    def acknowledge_receipt(self,job_id,receipt):
        """Trusted transport supplies the existing exact coordinator receipt."""
        row = self._row(job_id)
        box,identifier,_ = self._bound_outbox(row)
        box.acknowledge(identifier,receipt)  # FULL commit before any gate release.
        self.acknowledge_delivery(job_id)

    def acknowledge_delivery(self,job_id):
        row = self._row(job_id)
        box,identifier,completion_record = self._bound_outbox(row)
        if completion_record['state']!='acknowledged' or not completion_record['receipt']:
            raise WorkerHeld('exact_coordinator_metadata_ack_required')
        box.acknowledge(identifier,completion_record['receipt'])
        if row['phase']=='delivered':
            return
        completion = self._completion(row)
        with NodeGate(self.engine_id) as gate:
            self.controller._gate(gate)
            state = gate.snapshot()
            acknowledgment = dict(id=job_id,workerId=self.worker_id,nodeId=self.node_id,
                authorityId=self.authority_id,**completion)
            tombstone=dict(receiptId=row['receipt'],generation=self.gate_generation,kind='completion',
                job={key:acknowledgment[key] for key in ('id','workerId','nodeId','authorityId')},
                completionDigest=gate_completion_digest(completion))
            # A lost response after gate release reuses only its exact tombstone.
            if state.get('lastAcknowledgment')!=tombstone:
                self.controller._gate(gate,row['receipt'],job_id)
                gate.prepare_completion(row['receipt'],completion)
                gate.acknowledge(row['receipt'],acknowledgment)
        self._phase(job_id,'delivered')
