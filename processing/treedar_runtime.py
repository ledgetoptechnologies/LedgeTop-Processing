"""Explicit, idle-only TreeDar runtime migration; historical epochs are data.

This is not an executable registry. Only the supplied current controller can
mutate an engine. Retired controller journals are read as evidence, never opened
as TreeDarEngine instances. Publication requires a factory state-owner lease,
the exact ReaderSession, an idle physical NodeGate, and a trusted local driver /
router census. Pending Viewer acceptance is deliberately independent of engine
metadata delivery. No files, containers, images or receipts are removed here.
"""
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

from . import reference_protocol as p
from .node_gate import NodeGate
from .reference_engine_worker import WorkerHeld
from .reference_readers import ReaderSession
from .treedar_admission import AdmissionPolicy
from .treedar_engine import TreeDarEngine


def encoded(value):
    return p.canonical_json(value).decode()


def runtime_descriptor(controller, delivery_path):
    c = controller.config
    return dict(engineVersion=c.engine_version,imageId=c.image_id,
        sourceSha256=c.source_sha256,workerJournal=str(Path(delivery_path).absolute()),
        controllerJournal=str(controller.path))


def _private_file(path):
    path = Path(path).absolute()
    if any(part.is_symlink() for part in (path,*path.parents)):
        raise WorkerHeld('runtime_journal_identity_changed')
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or (os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077))):
        raise WorkerHeld('runtime_journal_identity_changed')
    return path


@contextmanager
def _readonly(path):
    path = _private_file(path)
    db = sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=10)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        yield db
    finally:
        db.close()


class TreeDarRuntimeManager:
    """Stable delivery binding and immutable job epochs, explicitly opted in.

    ``census(db,currentEnvelope)`` is a trusted in-process factory dependency;
    never accept a network-supplied census. It verifies the complete driver and
    router sets under the owner lock, and reads delivery rows via the passed
    transaction. Missing or ambiguous legacy evidence requires reconciliation.
    """
    def __init__(self,delivery_path,*,ledger,outbox,failure_outbox,physical_binding,
                 allow_legacy_v1=False):
        self.path = Path(delivery_path).absolute()
        self.ledger,self.outbox,self.failure_outbox = ledger,outbox,failure_outbox
        self.allow_legacy_v1 = allow_legacy_v1 is True
        keys = {'nodeId','workerId','engineId','authorityId','origin','gateGeneration'}
        if not isinstance(physical_binding,dict) or set(physical_binding)!=keys or physical_binding['nodeId']!='main':
            raise WorkerHeld('exact_main_physical_binding_required')
        self.physical = dict(physical_binding)
        self.binding = dict(self.physical,ledgerPath=str(ledger.path),outboxPath=str(outbox.path),
                            failureOutboxPath=str(failure_outbox.path))
        self.path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        if any(part.is_symlink() for part in (self.path,*self.path.parents)):
            raise WorkerHeld('private_delivery_journal_required')
        parent = self.path.parent.stat()
        if os.name!='nt' and (parent.st_uid!=os.geteuid() or parent.st_mode & 0o077):
            raise WorkerHeld('private_delivery_journal_required')
        fd = os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600)
        os.close(fd)
        with self._db() as db:
            for statement in (
                'CREATE TABLE IF NOT EXISTS delivery_binding_v2 (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS treedar_legacy_binding_evidence (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS treedar_runtime_epochs (epoch TEXT PRIMARY KEY,envelope TEXT NOT NULL,state TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS treedar_runtime_active (id INTEGER PRIMARY KEY CHECK(id=1),epoch TEXT NOT NULL,revision INTEGER NOT NULL)',
                'CREATE TABLE IF NOT EXISTS treedar_runtime_publications (revision INTEGER PRIMARY KEY,epoch TEXT NOT NULL,census TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS treedar_job_runtimes (job TEXT PRIMARY KEY,epoch TEXT NOT NULL,runtime_sha TEXT NOT NULL,binding TEXT NOT NULL)'):
                db.execute(statement)
            old = db.execute('SELECT value FROM delivery_binding_v2 WHERE id=1').fetchone()
            if old and old[0]!=encoded(self.binding):
                raise WorkerHeld('delivery_physical_binding_changed')
            # Legacy binding is immutable evidence; never rewrite it to candidate.
            if self._has(db,'delivery_binding'):
                legacy = db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()
                if legacy:
                    value = json.loads(legacy[0])
                    if any(value.get(key)!=val for key,val in self.binding.items()):
                        raise WorkerHeld('legacy_delivery_physical_binding_changed')
                    if 'runtimeManagementVersion' in value:
                        evidence=db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()
                        pointer=db.execute('SELECT 1 FROM treedar_runtime_active WHERE id=1').fetchone()
                        original=json.loads(evidence[0]) if evidence else None
                        if (value['runtimeManagementVersion']!=1 or evidence is None or pointer is None
                                or value!=dict(original if original is not None else self.binding,runtimeManagementVersion=1)):
                            raise WorkerHeld('legacy_runtime_management_fence_changed')
            db.execute('INSERT OR IGNORE INTO delivery_binding_v2 VALUES(1,?)',(encoded(self.binding),))
            self._validate_fence(db)

    @staticmethod
    def _has(db,table):
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone() is not None

    @contextmanager
    def _db(self):
        _private_file(self.path)
        db = sqlite3.connect(self.path,timeout=20)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            self._commit(db)
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _commit(self,db):
        db.commit()

    def controller_path(self,config,policy=None):
        digest = p.digest(dict(config=asdict(config),policy=asdict(policy or AdmissionPolicy())))
        return self.path.parent/'treedar-runtimes'/digest/'controller.sqlite'

    def _legacy(self,db):
        if not self.allow_legacy_v1 or not self._has(db,'delivery_binding'):
            return None
        row = db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()
        if not row:
            return None
        value = json.loads(row[0])
        if 'runtimeManagementVersion' in value:
            raise WorkerHeld('runtime_management_fence_without_pointer')
        with _readonly(value['controllerPath']) as old:
            config_row = old.execute('SELECT value FROM configuration WHERE id=1').fetchone()
            if not config_row:
                raise WorkerHeld('legacy_configuration_missing')
            configuration = json.loads(config_row[0])
        c = configuration['config']
        expected = dict(nodeId=c['node_id'],workerId=c['worker_id'],engineId=c['engine_id'],
            authorityId=c['authority_id'],origin=c['coordinator_origin'],gateGeneration=c['gate_generation'],
            engineVersion=c['engine_version'],imageId=c['image_id'],sourceSha256=c['source_sha256'])
        if any(value.get(key)!=val for key,val in expected.items()):
            raise WorkerHeld('legacy_configuration_binding_changed')
        runtime = dict(engineVersion=value['engineVersion'],imageId=value['imageId'],sourceSha256=value['sourceSha256'],
            workerJournal=str(self.path),controllerJournal=value['controllerPath'])
        envelope = dict(schemaVersion=1,runtime=runtime,controllerBindingSha256=p.digest(configuration))
        return dict(epochId=p.digest(envelope),revision=0,envelope=envelope,runtime=runtime,legacy=True)

    def _snapshot(self,db):
        self._validate_fence(db)
        row = db.execute('SELECT epoch,revision FROM treedar_runtime_active WHERE id=1').fetchone()
        if row is None:
            return self._legacy(db)
        epoch = db.execute('SELECT envelope,state FROM treedar_runtime_epochs WHERE epoch=?',(row['epoch'],)).fetchone()
        if epoch is None or epoch['state']!='active':
            raise WorkerHeld('runtime_pointer_corrupt')
        envelope = json.loads(epoch['envelope'])
        if p.digest(envelope)!=row['epoch']:
            raise WorkerHeld('runtime_epoch_digest_changed')
        return dict(epochId=row['epoch'],revision=row['revision'],envelope=envelope,runtime=envelope['runtime'])

    def _validate_fence(self,db):
        pointer=db.execute('SELECT 1 FROM treedar_runtime_active WHERE id=1').fetchone()
        row=db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone() if self._has(db,'delivery_binding') else None
        value=json.loads(row[0]) if row else None
        marker=isinstance(value,dict) and 'runtimeManagementVersion' in value
        if not pointer and not marker:
            return
        evidence=db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()
        original=json.loads(evidence[0]) if evidence else None
        if (not pointer or not marker or type(value['runtimeManagementVersion']) is not int
                or value['runtimeManagementVersion']!=1 or evidence is None
                or value!=dict(original if original is not None else self.binding,runtimeManagementVersion=1)
                or any(value.get(key)!=val for key,val in self.binding.items())):
            raise WorkerHeld('legacy_runtime_management_fence_changed')

    def snapshot(self):
        with self._db() as db:
            return self._snapshot(db)

    def assert_active(self,runtime):
        snapshot = self.snapshot()
        if snapshot is None or snapshot.get('legacy') or snapshot['runtime']!=runtime:
            raise WorkerHeld('active_treedar_runtime_changed')
        return snapshot

    @staticmethod
    def job_binding(job):
        return {key:job[key] for key in ('jobId','generation','environment','manifestSha256','pipelineDigest','nodeId')}

    def pin_job(self,job,runtime):
        binding = self.job_binding(job)
        with self._db() as db:
            snapshot = self._snapshot(db)
            if snapshot is None or snapshot.get('legacy') or snapshot['runtime']!=runtime:
                raise WorkerHeld('active_treedar_runtime_changed')
            old = db.execute('SELECT * FROM treedar_job_runtimes WHERE job=?',(job['jobId'],)).fetchone()
            values = (snapshot['epochId'],p.digest(runtime),encoded(binding))
            if old and (old['epoch'],old['runtime_sha'],old['binding'])!=values:
                raise WorkerHeld('job_runtime_binding_changed')
            db.execute('INSERT OR IGNORE INTO treedar_job_runtimes VALUES(?,?,?,?)',(job['jobId'],*values))

    def runtime_for_job(self,job_id):
        with self._db() as db:
            self._validate_fence(db)
            row = db.execute('SELECT * FROM treedar_job_runtimes WHERE job=?',(job_id,)).fetchone()
            if row is None:
                raise WorkerHeld('job_runtime_not_pinned')
            epoch = db.execute('SELECT envelope FROM treedar_runtime_epochs WHERE epoch=?',(row['epoch'],)).fetchone()
            if epoch is None:
                raise WorkerHeld('job_runtime_epoch_missing')
            envelope = json.loads(epoch[0])
            if p.digest(envelope)!=row['epoch'] or p.digest(envelope['runtime'])!=row['runtime_sha']:
                raise WorkerHeld('job_runtime_binding_changed')
            return envelope['runtime'],json.loads(row['binding'])

    def _capabilities(self,owner,reader_session,gate):
        # A caller-provided callable is not an exclusive ownership capability.
        from ops.reference_worker_daemon import DaemonStateLock
        owner_type = type(owner)
        canonical = owner_type is DaemonStateLock
        main = sys.modules.get('__main__')
        if not canonical and main is not None:
            canonical = (getattr(main,'DaemonStateLock',None) is owner_type
                and Path(getattr(main,'__file__','')).resolve()==Path(sys.modules[DaemonStateLock.__module__].__file__).resolve())
        if not canonical or not isinstance(reader_session,ReaderSession) or reader_session.ledger is not self.ledger:
            raise WorkerHeld('exclusive_runtime_owner_required')
        owner.check(self.path.parent)
        reader_session.check()
        if not isinstance(gate,NodeGate) or gate.engine_id!=self.physical['engineId'] or gate._stream is None:
            raise WorkerHeld('held_physical_gate_required')
        gate._check_lock()
        state = gate.snapshot()
        if (state['engineId'],state['nodeId'],state['authorityId'],state['coordinatorOrigin'],state['generation']) != (
                self.physical['engineId'],'main',self.physical['authorityId'],self.physical['origin'].rstrip('/'),self.physical['gateGeneration']):
            raise WorkerHeld('physical_runtime_gate_changed')
        if state['reservation'] is not None:
            raise WorkerHeld('runtime_migration_requires_idle_gate')

    def _controller_rows(self,envelope):
        with _readonly(envelope['runtime']['controllerJournal']) as db:
            config = db.execute('SELECT value FROM configuration WHERE id=1').fetchone()
            if not config or p.digest(json.loads(config[0]))!=envelope['controllerBindingSha256']:
                raise WorkerHeld('historical_controller_binding_changed')
            return [dict(row) for row in db.execute('SELECT * FROM treedar_executions')]

    def validate_retained_outbox(self,row):
        """Read-only exact ACK proof; no resend, re-ACK, or clock observations."""
        from .treedar_delivery import TreeDarDelivery
        from .worker_outbox import BINDING,_id,_int
        from .reference_failure import _envelope,_receipt
        view=object.__new__(TreeDarDelivery)
        view.outbox,view.failure_outbox=self.outbox,self.failure_outbox
        box,identifier=view._outbox(row)
        with _readonly(box.path) as db:
            stored=db.execute('SELECT * FROM completions WHERE id=?',(identifier,)).fetchone()
            if stored is None:
                raise WorkerHeld('historical_metadata_ack_required')
            completion=dict(stored)
            for key in ('payload','local_metadata','receipt'):
                completion[key]=json.loads(stored[key]) if stored[key] else None
            view._bound_outbox(row,completion)
            receipt=completion['receipt']
            if completion['state']!='acknowledged' or not receipt or completion['attempts']<1:
                raise WorkerHeld('historical_metadata_ack_required')
            if row['outcome']=='succeeded':
                payload=completion['payload']
                required={'type','durable','receiptId','storageReceiptId','outboxId','payloadSha256','acceptedObjectIds',*BINDING}
                if (set(receipt)!=required or receipt['type']!='durable-result-receipt' or receipt['durable'] is not True
                        or receipt['outboxId']!=identifier or receipt['payloadSha256']!=completion['digest']
                        or any(receipt[key]!=payload[key] for key in BINDING)
                        or not isinstance(receipt['acceptedObjectIds'],list)
                        or any(type(value) is not str for value in receipt['acceptedObjectIds'])
                        or sorted(receipt['acceptedObjectIds'])!=sorted(obj['id'] for obj in payload['resultManifest']['objects'])):
                    raise WorkerHeld('historical_metadata_ack_changed')
                _id(receipt['receiptId']);_id(receipt['storageReceiptId']);_int(receipt['generation'],1)
                storage=receipt['storageReceiptId']
            else:
                envelope=dict(outboxId=identifier,payloadSha256=completion['digest'],payload=completion['payload'])
                record=_envelope(envelope)
                if receipt!=_receipt(envelope,record) or type(receipt.get('generation')) is not int or receipt.get('durable') is not True:
                    raise WorkerHeld('historical_metadata_ack_changed')
                storage=receipt['receiptId']
            durable=db.execute('SELECT * FROM durable_receipts WHERE outbox=?',(identifier,)).fetchone()
            if durable is None or (durable['id'],durable['storage_id'],durable['payload'])!=(receipt['receiptId'],storage,encoded(receipt)):
                raise WorkerHeld('historical_metadata_receipt_not_durable')
        return completion

    def _closed(self,db,census,current):
        evidence = census(db,current['envelope'] if current else None)
        if (not isinstance(evidence,dict) or set(evidence)!={'schemaVersion','jobs','driverSha256','routerSha256'}
                or evidence['schemaVersion']!=1 or not isinstance(evidence['jobs'],dict)):
            raise WorkerHeld('exact_runtime_census_required')
        p._hash(evidence['driverSha256'],'driver census'); p._hash(evidence['routerSha256'],'router census')
        rows = {row['job']:dict(row) for row in db.execute('SELECT * FROM executions')} if self._has(db,'executions') else {}
        epochs = {row['epoch']:json.loads(row['envelope']) for row in db.execute('SELECT epoch,envelope FROM treedar_runtime_epochs')}
        if current:
            epochs[current['epochId']] = current['envelope']
        controls = {}
        for epoch,envelope in epochs.items():
            if p.digest(envelope)!=epoch:
                raise WorkerHeld('runtime_epoch_digest_changed')
            for row in self._controller_rows(envelope):
                if row['job'] in controls or row['phase'] not in ('terminal_proof','container_removed') or not row['proof']:
                    raise WorkerHeld('unsettled_historical_controller_intent')
                controls[row['job']] = (epoch,envelope,row)
        pins = {row['job']:dict(row) for row in db.execute('SELECT * FROM treedar_job_runtimes')}
        if set(rows)!=set(controls) or set(rows)!=set(evidence['jobs']) or set(pins)-set(rows):
            raise WorkerHeld('runtime_job_census_incomplete')
        for job_id,row in rows.items():
            epoch,envelope,control = controls[job_id]
            item = evidence['jobs'][job_id]
            identity,source,proof = json.loads(row['identity']),json.loads(control['identity']),json.loads(row['proof'])
            binding = identity['job']
            if (not isinstance(item,dict) or set(item)!={'binding','runtime','stage','outboxId','failureOutboxId'}
                    or item['binding']!=binding or item['runtime']!=envelope['runtime']
                    or item['stage'] not in ('completed','awaiting_acceptance','awaiting_failure_acceptance')
                    or (item['stage']!='completed' and item['stage']!=('awaiting_acceptance' if row['outcome']=='succeeded' else 'awaiting_failure_acceptance'))
                    or item['outboxId']!=row['outbox_id'] or item['failureOutboxId']!=row['failure_outbox_id']
                    or row['phase']!='delivered' or row['receipt']!=control['receipt']
                    or identity['reservation']!=source['reservationId']
                    or identity['proofSha256']!=p.digest(proof) or proof!=json.loads(control['proof'])
                    or proof.get('engineTerminal') is not True or proof.get('job')!=source['job']
                    or proof.get('imageId')!=envelope['runtime']['imageId']
                    or dict(source['job'],nodeId='main')!=binding or proof.get('containerId')!=control['container']
                    or row['outcome']!=proof.get('status')):
                raise WorkerHeld('historical_terminal_evidence_changed')
            if row['outcome']=='succeeded':
                result=p.validate_result_manifest(json.loads(row['result']),job_id=job_id,generation=binding['generation'],
                    environment=binding['environment'],pipeline_digest=binding['pipelineDigest'])
                if result!=proof.get('resultManifest') or result['engineVersions']!={'treedar':envelope['runtime']['engineVersion']}:
                    raise WorkerHeld('historical_result_version_changed')
            elif row['outcome']=='failed':
                from .reference_failure import make_failure
                expected_failure=make_failure(job_id=job_id,generation=binding['generation'],environment=binding['environment'],
                    input_manifest_sha256=binding['manifestSha256'],pipeline_digest=binding['pipelineDigest'],
                    terminal_state='failed',error_code=proof['errorCode'])
                if json.loads(row['failure_json'])!=expected_failure:
                    raise WorkerHeld('historical_failure_binding_changed')
            else:
                raise WorkerHeld('historical_terminal_outcome_unknown')
            self.validate_retained_outbox(row)
            other=self.failure_outbox if row['outcome']=='succeeded' else self.outbox
            with _readonly(other.path) as other_db:
                if other_db.execute('SELECT 1 FROM completions WHERE job=? AND generation=?',(job_id,binding['generation'])).fetchone():
                    raise WorkerHeld('mixed_historical_terminal_outboxes')
            with _readonly(self.ledger.path) as ledger_db:
                reservation=ledger_db.execute('SELECT * FROM reservations WHERE id=?',(identity['reservation'],)).fetchone()
                if (not reservation or reservation['state']!='terminal' or reservation['job']!=job_id
                        or reservation['generation']!=binding['generation'] or reservation['environment']!=binding['environment']
                        or json.loads(reservation['settings'])['manifest_sha256']!=binding['manifestSha256']):
                    raise WorkerHeld('historical_reservation_not_terminal')
                if row['outcome']=='succeeded':
                    sealed=ledger_db.execute('SELECT identity FROM results WHERE reservation=?',(identity['reservation'],)).fetchone()
                    expected_result=dict(job_id=job_id,generation=binding['generation'],
                            manifest_sha256=binding['manifestSha256'],pipeline_sha256=binding['pipelineDigest'],
                            result_manifest_sha256=result['resultManifestSha256'],objects=sorted([
                                dict(object_id=obj['id'],bytes=obj['byteSize'],sha256=obj['sha256'],
                                     relative_path=proof['localObjects'][obj['id']]) for obj in result['objects']],key=lambda obj:obj['object_id']))
                    if not sealed or json.loads(sealed[0])!=expected_result:
                        raise WorkerHeld('historical_result_registration_changed')
                    objects=[dict(object_id=obj['id'],bytes=obj['bytes'],sha256=obj['sha'],relative_path=obj['path'])
                        for obj in ledger_db.execute('SELECT * FROM objects WHERE reservation=? ORDER BY id',(identity['reservation'],))]
                    if objects!=expected_result['objects']:
                        raise WorkerHeld('historical_result_objects_changed')
            expected=(epoch,p.digest(envelope['runtime']),encoded(binding))
            old=pins.get(job_id)
            if old:
                if (old['epoch'],old['runtime_sha'],old['binding'])!=expected:
                    raise WorkerHeld('historical_job_pin_changed')
            elif not current or not current.get('legacy') or epoch!=current['epochId']:
                raise WorkerHeld('historical_job_pin_missing')
            else:
                db.execute('INSERT INTO treedar_job_runtimes VALUES(?,?,?,?)',(job_id,*expected))
        return evidence

    def activate_runtime(self,controller,*,expected_epoch,expected_revision,owner,reader_session,gate,census):
        if not isinstance(controller,TreeDarEngine) or controller.ledger is not self.ledger:
            raise WorkerHeld('exact_current_controller_required')
        c=controller.config
        actual=dict(nodeId=c.node_id,workerId=c.worker_id,engineId=c.engine_id,authorityId=c.authority_id,
                    origin=c.coordinator_origin,gateGeneration=c.gate_generation)
        if actual!=self.physical or controller.path!=self.controller_path(c,controller.policy):
            raise WorkerHeld('versioned_runtime_controller_required')
        envelope=dict(schemaVersion=1,runtime=runtime_descriptor(controller,self.path),
                      controllerBindingSha256=p.digest(dict(config=asdict(c),policy=asdict(controller.policy))))
        epoch=p.digest(envelope)
        self._capabilities(owner,reader_session,gate)
        controller._host()  # Read-only candidate image/source/socket qualification.
        target=None
        try:
            with self._db() as db:
                current=self._snapshot(db)
                if (current['epochId'] if current else None)!=expected_epoch or (current['revision'] if current else 0)!=expected_revision:
                    raise WorkerHeld('runtime_activation_compare_and_swap_failed')
                if (current is None and self._has(db,'delivery_binding')
                        and db.execute('SELECT 1 FROM delivery_binding WHERE id=1').fetchone()):
                    raise WorkerHeld('explicit_legacy_runtime_adoption_required')
                evidence=self._closed(db,census,current)
                known=db.execute('SELECT envelope FROM treedar_runtime_epochs WHERE epoch=?',(epoch,)).fetchone()
                candidate_rows=self._controller_rows(envelope)
                if known is None and candidate_rows:
                    raise WorkerHeld('unregistered_candidate_controller_intent')
                if current and current['epochId']==epoch and not current.get('legacy'):
                    return current
                revision=expected_revision+1
                target=dict(epochId=epoch,revision=revision,envelope=envelope,runtime=envelope['runtime'])
                if current:
                    db.execute("INSERT OR IGNORE INTO treedar_runtime_epochs VALUES(?,?,'retired')",
                               (current['epochId'],encoded(current['envelope'])))
                    db.execute("UPDATE treedar_runtime_epochs SET state='retired' WHERE epoch=?",(current['epochId'],))
                old=db.execute('SELECT envelope FROM treedar_runtime_epochs WHERE epoch=?',(epoch,)).fetchone()
                if old and old[0]!=encoded(envelope):
                    raise WorkerHeld('runtime_epoch_binding_changed')
                db.execute("INSERT OR IGNORE INTO treedar_runtime_epochs VALUES(?,?,'active')",(epoch,encoded(envelope)))
                db.execute("UPDATE treedar_runtime_epochs SET state='active' WHERE epoch=?",(epoch,))
                db.execute('INSERT INTO treedar_runtime_active VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch,revision=excluded.revision',(epoch,revision))
                db.execute('INSERT INTO treedar_runtime_publications VALUES(?,?,?)',(revision,epoch,encoded(evidence)))
                # An old installed daemon knows only the exact v1 global binding.
                # Preserve that original as evidence, then make its immutable
                # equality check refuse this managed journal on downgrade.
                db.execute('CREATE TABLE IF NOT EXISTS delivery_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
                legacy=db.execute('SELECT value FROM delivery_binding WHERE id=1').fetchone()
                value=json.loads(legacy[0]) if legacy else None
                if value is None or 'runtimeManagementVersion' not in value:
                    original=legacy[0] if legacy else 'null'  # Explicit absence, never a fabricated v1 history.
                    preserved=db.execute('SELECT value FROM treedar_legacy_binding_evidence WHERE id=1').fetchone()
                    if preserved and preserved[0]!=original:
                        raise WorkerHeld('legacy_binding_evidence_changed')
                    db.execute('INSERT OR IGNORE INTO treedar_legacy_binding_evidence VALUES(1,?)',(original,))
                    db.execute('INSERT INTO delivery_binding VALUES(1,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                        (encoded(dict(value if value is not None else self.binding,runtimeManagementVersion=1)),))
                self._capabilities(owner,reader_session,gate)
        except BaseException:
            # Unknown commit result: inspect the exact durable pointer, never
            # blindly repeat a publish or override a newer actor's revision.
            with _readonly(self.path) as db:
                observed=self._snapshot(db)
            if target is not None and observed==target:
                return observed
            raise
        return target
