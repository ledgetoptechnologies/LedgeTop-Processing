"""Success-only, exact-UUID native engine cache removal after durable acceptance.

Worker files are never removed. First admission needs original validated Viewer
receipts; retries reuse the persisted proof and only observe the exact UUID.
An unknown mutation outcome never authorizes another remove request.
"""
from contextlib import contextmanager
import json
import ipaddress
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import urlsplit

from . import reference_protocol as p
from .node_gate import NodeGate
from .viewer_service import NativeEngine


class _Held(ValueError):
    pass


class EngineCacheCleanup:
    def __init__(self, worker, *, path, max_census_tasks=100, viewer_receipt_keys=None):
        if not isinstance(worker.engine, NativeEngine):
            raise ValueError('fixed native engine required')
        origin = worker.engine.origin
        parsed = urlsplit(origin)
        if (parsed.scheme not in ('http','https') or not ipaddress.ip_address(parsed.hostname).is_loopback
                or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/')
                or parsed.query or parsed.fragment or parsed.port == 0
                or worker.engine.client.origin != origin.rstrip('/')):
            raise ValueError('fixed literal loopback native engine required')
        if type(max_census_tasks) is not int or not 1 <= max_census_tasks <= p.MAX_OBJECTS:
            raise ValueError('invalid engine census bound')
        from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
        try:
            self.viewer_receipt_keys = validate_trusted_viewer_receipt_keys(
                {} if viewer_receipt_keys is None else viewer_receipt_keys)
        except ValueError:
            raise ValueError('invalid pinned Viewer receipt keys') from None
        self.worker, self.maximum = worker, max_census_tasks
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(part.is_symlink() for part in (self.path, *self.path.parents)):
            raise ValueError('private engine cleanup journal required')
        if os.name != 'nt':
            parent = self.path.parent.stat()
            if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
                raise ValueError('private owned cleanup journal parent required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0)
                             | getattr(os, 'O_NONBLOCK', 0), 0o600)
        try:
            observed, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('owned regular cleanup journal required')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        self.binding = dict(engineId=worker.engine_id, engineOrigin=worker.engine.origin,
            nodeId=worker.node_id, workerId=worker.worker_id, authorityId=worker.authority_id,
            gateOrigin=worker.origin.rstrip('/'), gateGeneration=worker.gate_generation)
        encoded = p.canonical_json(self.binding).decode()
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS binding (singleton INTEGER PRIMARY KEY,value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS removals (
              job TEXT PRIMARY KEY,identity TEXT NOT NULL,proof TEXT NOT NULL,phase TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,checked INTEGER NOT NULL DEFAULT 0)''')
            if 'checked' not in {row['name'] for row in db.execute('PRAGMA table_info(removals)')}:
                db.execute('ALTER TABLE removals ADD COLUMN checked INTEGER NOT NULL DEFAULT 0')
            old = db.execute('SELECT value FROM binding WHERE singleton=1').fetchone()
            if old and old[0] != encoded:
                raise ValueError('engine cleanup ownership changed')
            db.execute('INSERT OR IGNORE INTO binding VALUES(1,?)', (encoded,))

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            db.close()

    def _phase(self, job_id, phase):
        with self._db() as db:
            db.execute('UPDATE removals SET phase=?,updated_at=CURRENT_TIMESTAMP WHERE job=?', (phase, job_id))

    def _success(self, job_id):
        from .reference_engine_worker import _source_intake_version
        row = self.worker._row(job_id)
        if not row['result'] or not row['outbox_id']:
            raise _Held('sealed_success_required')
        source = json.loads(row['identity'])
        job, reservation = source['job'], source['reservation']
        if job['jobId'] != job_id or job['nodeId'] != self.binding['nodeId']:
            raise _Held('local_job_binding_changed')
        result = p.validate_result_manifest(json.loads(row['result']), job_id=job_id, generation=job['generation'],
            environment=job['environment'], pipeline_digest=job['pipelineDigest'])
        manifest = job['submission']['manifest']
        if manifest['sha256'] != job['manifestSha256'] or p.manifest_sha256(manifest) != job['manifestSha256']:
            raise _Held('input_manifest_changed')
        completion = self.worker.outbox.get(row['outbox_id'])
        payload, receipt = completion['payload'], completion['receipt']
        keys = ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')
        if type(receipt) is not dict:
            raise _Held('durable_metadata_ack_required')
        p._closed(receipt, (*keys,'type','durable','receiptId','storageReceiptId','outboxId','payloadSha256','acceptedObjectIds'), 'metadata receipt')
        p._closed(payload, (*keys,'resultManifest','terminalState'), 'terminal success payload')
        p._integer(receipt['generation'], 'metadata receipt generation')
        for field in ('receiptId','storageReceiptId'):
            p._slug(receipt[field], field)
        ids = receipt['acceptedObjectIds']
        if type(ids) is not list or any(type(value) is not str for value in ids) or len(set(ids)) != len(ids):
            raise _Held('durable_metadata_ack_required')
        expected = (job_id,job['generation'],job['environment'],job['manifestSha256'],job['pipelineDigest'],result['resultManifestSha256'])
        if (completion['state'] != 'acknowledged' or type(receipt) is not dict
                or receipt.get('type') != 'durable-result-receipt' or receipt.get('durable') is not True
                or payload.get('terminalState') != 'succeeded' or p.digest(payload) != completion['digest']
                or p.digest({key: payload[key] for key in keys}) != row['outbox_id']
                or tuple(payload[key] for key in keys) != expected or tuple(receipt.get(key) for key in keys) != expected
                or receipt.get('outboxId') != row['outbox_id'] or receipt.get('payloadSha256') != completion['digest']
                or set(receipt.get('acceptedObjectIds', [])) != {obj['id'] for obj in result['objects']}
                or p.canonical_json(payload['resultManifest']) != p.canonical_json(result)):
            raise _Held('durable_metadata_ack_required')
        inputs = []
        if set(source['paths']) != {obj['id'] for obj in manifest['objects']}:
            raise _Held('retained_input_binding_changed')
        for obj in manifest['objects']:
            relative = source['paths'][obj['id']]
            try:
                self.worker.ledger._owned_path(reservation, relative)
            except Exception:
                raise _Held('retained_inputs_not_verified') from None
            inputs.append(dict(id=obj['id'], byteSize=obj['byteSize'], sha256=obj['sha256'], relativePath=relative))
        identity = dict(self.binding, jobId=job_id, taskUUID=job_id, generation=job['generation'],
            environment=job['environment'], inputManifestSha256=job['manifestSha256'], pipelineDigest=job['pipelineDigest'],
            resultManifestSha256=result['resultManifestSha256'], outboxId=row['outbox_id'],
            coordinatorReceiptSha256=p.digest(receipt), reservationId=reservation, inputs=inputs)
        try:
            source_intake_version = _source_intake_version(job, source['job'], source)
        except ValueError:
            raise _Held('source_acceptance_binding_unavailable') from None
        if source_intake_version == 2:
            try:
                from .source_acceptance_binding import validate_source_acceptance_binding
                source_binding = validate_source_acceptance_binding(source['job'].get('sourceAcceptanceBinding'), job)
                journal_binding = validate_source_acceptance_binding(source.get('sourceAcceptanceBinding'), job)
                if source_binding != journal_binding:
                    raise ValueError('persisted source binding copies differ')
                if source_binding['coordinatorAuthorityId'] != self.worker.authority_id:
                    raise ValueError('source authority differs from worker authority')
            except (ImportError, TypeError, ValueError, KeyError):
                raise _Held('source_acceptance_binding_unavailable') from None
            identity.update(sourceIntakeVersion=2, sourceAcceptanceBinding=source_binding)
        return identity, result

    def _verify_inputs(self, identity):
        try:
            for obj in identity['inputs']:
                self.worker._file(identity['reservationId'], obj['relativePath'], obj['byteSize'], obj['sha256'])
        except Exception:
            raise _Held('retained_inputs_not_verified') from None

    def _proof(self, identity, result, receipts):
        if not self.viewer_receipt_keys:
            raise _Held('viewer_receipt_keys_not_configured')
        if type(receipts) is not list or not 0 < len(receipts) <= p.MAX_OBJECTS:
            raise _Held('full_viewer_receipts_required')
        metadata_present = 'sourceIntakeVersion' in identity or 'sourceAcceptanceBinding' in identity
        source_intake_version = None
        source_binding = identity.get('sourceAcceptanceBinding')
        if metadata_present:
            if (type(identity.get('sourceIntakeVersion')) is not int or identity['sourceIntakeVersion'] != 2
                    or type(source_binding) is not dict):
                raise _Held('source_acceptance_binding_unavailable')
            source_intake_version = 2
        if source_intake_version == 2:
            try:
                from .source_acceptance_binding import validate_source_acceptance_binding
                source_binding = validate_source_acceptance_binding(source_binding,
                    {'jobId': identity['jobId'], 'generation': identity['generation'],
                     'environment': identity['environment'], 'manifestSha256': identity['inputManifestSha256'],
                     'pipelineDigest': identity['pipelineDigest'], 'sourceIntakeVersion': 2,
                     'submission': {'sourceIntakeVersion': 2}})
                if source_binding['coordinatorAuthorityId'] != self.worker.authority_id:
                    raise ValueError('source authority differs from worker authority')
            except (ImportError, TypeError, ValueError, KeyError):
                raise _Held('source_acceptance_binding_unavailable') from None
        elif source_intake_version is not None or source_binding is not None:
            raise _Held('source_acceptance_binding_unavailable')
        from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
        checked, seen, accepted, receipt_owners = [], {}, set(), {}
        expected = dict(jobId=identity['jobId'], generation=identity['generation'],
            environment=identity['environment'], inputManifestSha256=identity['inputManifestSha256'],
            pipelineDigest=identity['pipelineDigest'], resultManifestSha256=identity['resultManifestSha256'])
        for envelope in receipts:
            if type(envelope) is not dict or set(envelope) != {'acceptance', 'viewerReceipt'}:
                # Legacy processing receipts remain valid job-state metadata,
                # but never add cleanup coverage without the signed sibling.
                if source_intake_version != 2 and type(envelope) is dict and 'viewerReceipt' not in envelope:
                    try:
                        legacy = p.validate_acceptance(envelope, result_manifest=result,
                            input_manifest_sha256=identity['inputManifestSha256'])
                    except (TypeError, ValueError):
                        raise _Held('viewer_receipt_binding_invalid') from None
                    legacy_ids = {legacy['receiptId'], legacy['storageReceiptId']}
                    if len(legacy_ids) != 2 or any(owner != legacy['receiptId'] for key in legacy_ids
                            if (owner := receipt_owners.get(key)) is not None):
                        raise _Held('viewer_receipt_identity_collision')
                    for key in legacy_ids:
                        receipt_owners[key] = legacy['receiptId']
                    continue
                raise _Held('signed_viewer_receipt_required')
            value = p.validate_acceptance(envelope['acceptance'], result_manifest=result,
                input_manifest_sha256=identity['inputManifestSha256'])
            try:
                if source_intake_version == 2:
                    from .viewer_copy_receipt_v2 import verify_viewer_copy_receipt_v2
                    expected = dict(source_binding, resultManifestSha256=identity['resultManifestSha256'])
                    signed = verify_viewer_copy_receipt_v2(envelope['viewerReceipt'],
                        trusted_keys=self.viewer_receipt_keys, expected=expected,
                        result_manifest=result, acceptance=value).receipt
                else:
                    signed = verify_viewer_acceptance_receipt(envelope['viewerReceipt'],
                        trusted_keys=self.viewer_receipt_keys, expected=expected,
                        result_manifest=result, accepted_object_ids=value['acceptedObjectIds']).receipt
            except (TypeError, ValueError):
                raise _Held('viewer_receipt_signature_or_binding_invalid') from None
            sibling_fields = ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')
            if (any(value[field] != signed[field] for field in sibling_fields)
                    or value['acceptedObjectIds'] != signed['acceptedObjectIds']
                    or {value['receiptId'], value['storageReceiptId']} & {signed['receiptId'], signed['storageReceiptId']}):
                raise _Held('viewer_receipt_sibling_binding_mismatch')
            encoded = p.canonical_json(dict(acceptance=value, viewerReceipt=signed))
            receipt_ids = {value['receiptId'], value['storageReceiptId'], signed['receiptId'], signed['storageReceiptId']}
            if len(receipt_ids) != 4 or any(owner != value['receiptId'] for key in receipt_ids
                    if (owner := receipt_owners.get(key)) is not None):
                raise _Held('viewer_receipt_identity_collision')
            for key in receipt_ids:
                receipt_owners[key] = value['receiptId']
            if value['receiptId'] in seen and seen[value['receiptId']] != encoded:
                raise _Held('viewer_receipt_changed')
            if value['receiptId'] not in seen:
                checked.append(dict(acceptance=value, viewerReceipt=signed))
            seen[value['receiptId']] = encoded
            accepted.update(value['acceptedObjectIds'])
        if accepted != {obj['id'] for obj in result['objects']}:
            raise _Held('full_viewer_acceptance_required')
        with self.worker.ledger._transaction() as db:
            reservation = self.worker.ledger._reservation(db, identity['reservationId'])
            if (reservation['job'], reservation['generation'], reservation['environment'], reservation['state']) != (
                    identity['jobId'], identity['generation'], identity['environment'], 'terminal'):
                raise _Held('terminal_scratch_binding_required')
            registered = db.execute('SELECT identity FROM results WHERE reservation=?', (identity['reservationId'],)).fetchone()
            if registered is None:
                raise _Held('registered_result_required')
            registered = json.loads(registered[0])
            if tuple(registered[key] for key in ('job_id','generation','manifest_sha256','pipeline_sha256','result_manifest_sha256')) != (
                    identity['jobId'],identity['generation'],identity['inputManifestSha256'],identity['pipelineDigest'],identity['resultManifestSha256']):
                raise _Held('registered_result_changed')
            stored = list(db.execute('SELECT id,accepted,accepted_copy_verified FROM objects WHERE reservation=?', (identity['reservationId'],)))
            if ({obj['id'] for obj in stored} != accepted or any(obj['accepted'] is None
                    or obj['accepted_copy_verified'] != 1 for obj in stored)):
                raise _Held('durable_viewer_acceptance_required')
            for envelope in checked:
                value = envelope['acceptance']
                old = db.execute('SELECT payload,viewer_receipt,verified_copy FROM receipts WHERE id=?',
                    (value['receiptId'],)).fetchone()
                expected = dict(reservation_id=identity['reservationId'], receipt_id=value['receiptId'],
                    storage_receipt_id=value['storageReceiptId'], job_id=identity['jobId'], generation=identity['generation'],
                    manifest_sha256=identity['inputManifestSha256'], pipeline_sha256=identity['pipelineDigest'],
                    result_manifest_sha256=identity['resultManifestSha256'], accepted_object_ids=sorted(value['acceptedObjectIds']))
                if (old is None or json.loads(old['payload']) != expected or old['verified_copy'] != 1
                        or old['viewer_receipt'] != p.canonical_json(envelope['viewerReceipt']).decode()):
                    raise _Held('viewer_receipt_not_durable_locally')
        return sorted(checked, key=lambda value: value['acceptance']['receiptId'])

    def _census(self, target):
        info = self.worker.engine.read('/info')
        tasks = self.worker.engine.read('/task/list')
        if (type(info) is not dict or info.get('engineVersion') != self.worker.version
                or type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0
                or type(tasks) is not list or len(tasks) > self.maximum):
            raise _Held('engine_not_confirmed_idle')
        seen, target_status = set(), None
        for task in tasks:
            if type(task) is not dict:
                raise _Held('engine_census_unavailable')
            identifier = task.get('uuid')
            p._uuid(identifier, 'engine UUID')
            if identifier in seen:
                raise _Held('engine_census_unavailable')
            seen.add(identifier)
            current = self.worker.engine.read('/task/' + identifier + '/info', missing=True)
            if current is None and identifier == target:
                continue
            if (type(current) is not dict or current.get('uuid') != identifier or type(current.get('status')) is not dict
                    or type(current['status'].get('code')) is not int or current['status']['code'] not in (30,40,50)):
                raise _Held('engine_has_active_or_uncertain_work')
            if identifier == target:
                target_status = current['status']['code']
        # Probe the exact UUID even when its list entry is absent.
        if target not in seen:
            current = self.worker.engine.read('/task/' + target + '/info', missing=True)
            if current is not None:
                raise _Held('engine_census_identity_changed')
        return target_status

    def _policy(self, identity):
        with self.worker.ledger._transaction() as db:
            reservation = self.worker.ledger._reservation(db, identity['reservationId'])
            objects = list(db.execute('SELECT accepted FROM objects WHERE reservation=?', (identity['reservationId'],)))
            now = self.worker.ledger._now(db)
            if (reservation['state'] != 'terminal' or reservation['terminal_at'] is None
                    or any(obj['accepted'] is None or max(obj['accepted'], reservation['terminal_at']) > now - self.worker.ledger.grace for obj in objects)):
                raise _Held('acceptance_grace_not_elapsed')
            if db.execute("SELECT 1 FROM pins WHERE reservation=? AND state!='released' LIMIT 1", (identity['reservationId'],)).fetchone():
                raise _Held('active_or_uncertain_result_pins')

    def pending(self, *, max_count=100):
        """Bounded path-free IDs with persisted proof awaiting policy/reconciliation."""
        if type(max_count) is not int or not 1 <= max_count <= 1000:
            raise ValueError('invalid cleanup pending bound')
        with self._db() as db:
            return [row[0] for row in db.execute("""SELECT job FROM removals WHERE phase IN
              ('waiting_policy','remove_pending','uncertain') ORDER BY checked,updated_at,job LIMIT ?""", (max_count,))]

    def reconcile_one(self):
        """One fair, bounded retry; uncertain mutations are always read-only."""
        with self._db() as db:
            row = db.execute("""SELECT job FROM removals WHERE phase IN
              ('waiting_policy','remove_pending','uncertain') ORDER BY checked,updated_at,job LIMIT 1""").fetchone()
            if row is None:
                return {'state':'idle'}
            db.execute('UPDATE removals SET checked=(SELECT COALESCE(MAX(checked),0)+1 FROM removals) WHERE job=?', (row[0],))
        return self.remove(row[0])

    def remove(self, job_id, *, viewer_receipts=None):
        """Admit one exact removal or read-only reconcile an existing intent.

        First call requires the raw full Viewer receipt list already applied to
        the ledger. Reconciliation can omit it and use the committed proof.
        Removed state never grants permission to remove worker inputs/artifacts.
        """
        try:
            p._uuid(job_id, 'job UUID')
        except ValueError:
            return {'state':'held', 'reason':'invalid_job_id'}
        try:
            if self.worker.engine.origin != self.binding['engineOrigin']:
                raise _Held('engine_origin_changed')
            identity, result = self._success(job_id)
            encoded = p.canonical_json(identity).decode()
            with self._db() as db:
                old = db.execute('SELECT * FROM removals WHERE job=?', (job_id,)).fetchone()
            if old and old['identity'] != encoded:
                raise _Held('cleanup_identity_changed')
            if viewer_receipts is None and old:
                viewer_receipts = json.loads(old['proof'])
            proof = p.canonical_json(self._proof(identity, result, viewer_receipts)).decode()
            # This proof checkpoint authorizes no engine mutation. It permits
            # deferred grace/pin/gate checks without polling Viewer receipts.
            with self._db() as db:
                db.execute("INSERT OR IGNORE INTO removals(job,identity,proof,phase) VALUES(?,?,?,'waiting_policy')", (job_id, encoded, proof))
                old = db.execute('SELECT * FROM removals WHERE job=?', (job_id,)).fetchone()
                if old['identity'] != encoded or old['proof'] != proof:
                    raise _Held('viewer_receipt_proof_changed')
            with NodeGate(self.binding['engineId']) as gate:
                state = gate.snapshot()
                if (state['engineId'],state['nodeId'],state['authorityId'],state['coordinatorOrigin'],state['generation']) != (
                        self.binding['engineId'],self.binding['nodeId'],self.binding['authorityId'],self.binding['gateOrigin'],self.binding['gateGeneration']):
                    raise _Held('physical_gate_binding_changed')
                if state['reservation'] is not None:
                    raise _Held('physical_gate_has_reserved_work')
                with self._db() as db:
                    old = db.execute('SELECT * FROM removals WHERE job=?', (job_id,)).fetchone()
                if old['phase'] not in ('waiting_policy','remove_pending','uncertain','removed'):
                    raise _Held('cleanup_journal_phase_unavailable')
                self._policy(identity)
                status = self._census(job_id)
                if status is None:
                    if old['phase'] == 'waiting_policy':
                        raise _Held('missing_engine_task_without_intent')
                    self._verify_inputs(identity)
                    self._phase(job_id, 'removed')
                    return {'state':'removed', 'jobId':job_id}
                if status != 40:
                    raise _Held('successful_engine_task_required')
                if old['phase'] != 'waiting_policy':
                    if old['phase'] == 'removed':
                        raise _Held('engine_task_reappeared_after_removal')
                    self._phase(job_id, 'uncertain')
                    return {'state':'uncertain', 'jobId':job_id, 'reason':'remove_outcome_requires_reconciliation'}
                self._verify_inputs(identity)
                self._policy(identity)
                self.worker._verify_local_engine()
                with self._db() as db:
                    db.execute("UPDATE removals SET phase='remove_pending',updated_at=CURRENT_TIMESTAMP WHERE job=?", (job_id,))
                try:
                    acknowledgement = self.worker.engine.action('remove', job_id)
                except BaseException:
                    self._phase(job_id, 'uncertain')
                    return {'state':'uncertain', 'jobId':job_id, 'reason':'remove_response_unknown'}
                if (type(acknowledgement) is not dict or acknowledgement.get('success') is not True
                        or ('uuid' in acknowledgement and acknowledgement['uuid'] != job_id)):
                    self._phase(job_id, 'uncertain')
                    return {'state':'uncertain', 'jobId':job_id, 'reason':'explicit_remove_ack_missing'}
                try:
                    current = self.worker.engine.read('/task/' + job_id + '/info', missing=True)
                except Exception:
                    self._phase(job_id, 'uncertain')
                    return {'state':'uncertain', 'jobId':job_id, 'reason':'engine_absence_not_confirmed'}
                if current is not None:
                    self._phase(job_id, 'uncertain')
                    return {'state':'uncertain', 'jobId':job_id, 'reason':'engine_absence_not_confirmed'}
                self._phase(job_id, 'removed')
                return {'state':'removed', 'jobId':job_id}
        except _Held as error:
            return {'state':'held', 'jobId':job_id, 'reason':str(error)}
        except Exception:
            return {'state':'held', 'jobId':job_id, 'reason':'engine_cleanup_unavailable'}
