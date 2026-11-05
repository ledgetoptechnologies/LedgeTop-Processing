"""Private NodeODM execution controller using the shared physical-engine gate.

Inputs must already be fetched into a capacity-reserved private job directory.
No daemon or engine is started by import. Uncertain mutations are never replayed.
The injected engine implements NativeEngine; only configured local origins are
supported by the deployment driver (not supplied by submissions).
"""
from contextlib import contextmanager
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import re
import sqlite3
import stat
import time
import zipfile
from urllib.parse import urlsplit

from . import reference_protocol as p
from .node_gate import NodeGate, GateError
from .scratch_policy import LeaseLapsed
from .engine_ingress_guard import local_engine_identity
from . import reference_native_archive as native_archive


class WorkerHeld(ValueError):
    pass


def _source_intake_version(job, *binding_records):
    submission = job.get('submission') if type(job) is dict else None
    records = (job, submission, *binding_records)
    marker_present = any(type(record) is dict and 'sourceIntakeVersion' in record for record in records)
    binding_present = any(type(record) is dict and 'sourceAcceptanceBinding' in record for record in records)
    if not marker_present and not binding_present:
        return None
    if (type(job) is not dict or type(submission) is not dict
            or type(job.get('sourceIntakeVersion')) is not int or job['sourceIntakeVersion'] != 2
            or type(submission.get('sourceIntakeVersion')) is not int or submission['sourceIntakeVersion'] != 2
            or any(type(record) is not dict or 'sourceAcceptanceBinding' not in record
                   or type(record['sourceAcceptanceBinding']) is not dict for record in binding_records)):
        raise ValueError('incomplete V2 source-intake binding')
    return 2


class ReferenceEngineWorker:
    def __init__(self, path, *, ledger, outbox, engine, engine_id, node_id,
                 authority_id, coordinator_origin, gate_generation, worker_id,
                 engine_version, allowed_odm_options=(), viewer_receipt_keys=None,
                 engine_probe=local_engine_identity):
        self.path = Path(path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(x.is_symlink() for x in (self.path, *self.path.parents)):
            raise ValueError('private journal path required')
        if os.name != 'nt':
            parent = self.path.parent.stat()
            if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
                raise ValueError('owned private journal directory required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        try:
            observed, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('owned regular journal required')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        try:
            origin = engine.origin
            parsed = urlsplit(origin)
            local = ipaddress.ip_address(parsed.hostname).is_loopback
            valid = (type(origin) is str and parsed.scheme in ('http', 'https') and local
                     and parsed.username is None and parsed.password is None
                     and parsed.path in ('', '/') and not parsed.query and not parsed.fragment
                     and parsed.port != 0 and not any(ord(char) <= 32 for char in origin))
        except (ValueError, TypeError, AttributeError):
            valid = False
        if not valid:
            raise ValueError('engine must have a fixed literal loopback origin')
        if (type(engine_version) is not str or not 1 <= len(engine_version) <= 128
                or not all(0x21 <= ord(char) <= 0x7e for char in engine_version)):
            raise ValueError('invalid engine version')
        self.ledger, self.outbox, self.engine = ledger, outbox, engine
        self.engine_id, self.node_id = engine_id, node_id
        if not callable(engine_probe):
            raise ValueError('local Docker Engine identity probe required')
        self.engine_probe = engine_probe
        self.authority_id, self.origin = authority_id, coordinator_origin
        self.gate_generation, self.worker_id = gate_generation, worker_id
        from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
        try:
            self.viewer_receipt_keys = validate_trusted_viewer_receipt_keys(
                {} if viewer_receipt_keys is None else viewer_receipt_keys)
        except ValueError:
            raise ValueError('invalid pinned Viewer receipt keys') from None
        self.version, self.allowed_options = engine_version, frozenset(allowed_odm_options)
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS executions (
                job TEXT PRIMARY KEY, identity TEXT NOT NULL, receipt TEXT NOT NULL,
                phase TEXT NOT NULL, result TEXT, outbox_id TEXT)''')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(executions)')}
            if 'archive_size' not in columns:
                db.execute('ALTER TABLE executions ADD COLUMN archive_size INTEGER')
            if 'archive_sha' not in columns:
                db.execute('ALTER TABLE executions ADD COLUMN archive_sha TEXT')
            for name in ('part_dev', 'part_ino', 'archive_expected_size'):
                if name not in columns:
                    db.execute('ALTER TABLE executions ADD COLUMN ' + name + ' TEXT')

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

    def _row(self, job_id):
        with self._db() as db:
            row = db.execute('SELECT * FROM executions WHERE job=?', (job_id,)).fetchone()
            if row is None:
                raise WorkerHeld('unknown local execution')
            return dict(row)

    def _phase(self, job_id, phase):
        with self._db() as db:
            db.execute('UPDATE executions SET phase=? WHERE job=?', (phase, job_id))

    def log_metadata(self, job_id):
        """Resolve only this worker's persisted execution; no caller engine UUID."""
        p._slug(job_id, 'job ID')
        row = self._row(job_id)
        job = json.loads(row['identity'])['job']
        if job['jobId'] != job_id or job['nodeId'] != self.node_id:
            raise WorkerHeld('execution_logs_unavailable')
        submission = job['submission']
        if submission['pipeline']['treedar']['requested']:
            raise WorkerHeld('execution_logs_unsupported')
        return dict(jobId=job_id, attemptId=submission['submissionId'], generation=job['generation'],
                    environment=job['environment'], nodeId=self.node_id)

    def read_logs(self, job_id, *, generation, environment, attempt_id, after, limit):
        """Read bounded NodeODM output under the existing physical engine gate."""
        from . import reference_logs as logs
        logs.bounds(generation, after, limit)
        with NodeGate(self.engine_id) as gate:
            state = gate.snapshot()
            if (state['nodeId'], state['coordinatorOrigin'], state['authorityId'], state['generation']) != (
                    self.node_id, self.origin, self.authority_id, self.gate_generation):
                raise WorkerHeld('execution_logs_unavailable')
            job = self.log_metadata(job_id)
            if (job['generation'], job['environment'], job['attemptId']) != (generation, environment, attempt_id):
                raise WorkerHeld('execution_logs_binding_mismatch')
            self._verify_local_engine()
            info = self.engine.read('/task/' + job_id + '/info', limit=64 * 1024)
            if type(info) is not dict or info.get('uuid') != job_id:
                raise WorkerHeld('execution_logs_unavailable')
            output, has_more = self.engine.output_page(job_id, line=after, limit=limit)
            if (type(output) is not list or any(type(line) is not str for line in output)
                    or type(has_more) is not bool or len(output) > limit
                    or len(p.canonical_json(output)) > logs.MAX_LOG_BYTES):
                raise WorkerHeld('execution_logs_unavailable')
            self._verify_local_engine()
            lines = []
            for text in output[:limit]:
                candidate = lines + [dict(level='info', message=text[:4000])]
                if logs.encoded_size(logs.page(job, candidate, after, has_more or len(output) > len(candidate))) > logs.MAX_LOG_BYTES:
                    break
                lines = candidate
            return logs.validate(logs.page(job, lines, after, has_more or len(output) > len(lines)), job, after, limit)

    def _verify_local_engine(self):
        try:
            observed = self.engine_probe(time.monotonic() + 2)
        except Exception:
            raise WorkerHeld('physical_engine_identity_unavailable') from None
        if observed != self.engine_id:
            raise WorkerHeld('physical_engine_identity_changed')

    def _part_fd(self, row, part, *, create=False):
        try:return native_archive.part_fd(row,part,create=create)
        except native_archive.ArchiveHeld as error:raise WorkerHeld(str(error)) from None

    def _cache_checkpoint(self,job,device,inode,expected):
        with self._db() as db:
            db.execute("UPDATE executions SET phase='archive_pending',part_dev=?,part_ino=?,archive_expected_size=? WHERE job=?",
                (device,inode,expected,job))

    def _download_archive(self, row, reservation, part, budget):
        try:return native_archive.download_archive(self.engine,row,reservation,part,budget,
            relative='jobs/'+reservation+'/.archive.part',file=self._file,phase=self._phase,
            checkpoint=self._cache_checkpoint,read_row=self._row)
        except native_archive.ArchiveHeld as error:raise WorkerHeld(str(error)) from None

    def _file(self, reservation, relative, expected_size=None, expected_sha=None):
        try:return native_archive.owned_file(self.ledger,reservation,relative,expected_size,expected_sha)
        except native_archive.ArchiveHeld as error:raise WorkerHeld(str(error)) from None

    def start(self, job, *, reservation_id, input_paths):
        """A ready, already claimed job and externally admitted scratch are required."""
        submission = job['submission']
        source_binding = None
        try:
            source_intake_version = _source_intake_version(job, job)
        except ValueError:
            raise WorkerHeld('source_acceptance_binding_unavailable') from None
        if source_intake_version == 2:
            try:
                from .source_acceptance_binding import validate_source_acceptance_binding
                source_binding = validate_source_acceptance_binding(job.get('sourceAcceptanceBinding'), job)
                if source_binding['coordinatorAuthorityId'] != self.authority_id:
                    raise ValueError('source authority differs from worker authority')
            except (ImportError, TypeError, ValueError, KeyError):
                raise WorkerHeld('source_acceptance_binding_unavailable') from None
        elif source_intake_version is not None:
            raise WorkerHeld('source_acceptance_binding_unavailable')
        from .reference_source_intake_canonical import pipeline_digest_for_job
        manifest, pipeline = submission['manifest'], submission['pipeline']
        if (job['status'] != 'running' or job['nodeId'] != self.node_id
                or not pipeline['odm']['requested'] or pipeline['treedar']['requested']
                or pipeline['gaussian_splat']['requested']
                or set(pipeline['odm']['options']) - self.allowed_options
                or set(input_paths) != {obj['id'] for obj in manifest['objects']}):
            raise WorkerHeld('job is unsupported or lacks an active local claim')
        from .reference_odm_inputs import validate_odm_input_closure
        try:
            validate_odm_input_closure(job, authority_id=self.authority_id)
        except ValueError:
            raise WorkerHeld('job is unsupported or lacks an active local claim') from None
        if (p.manifest_sha256(manifest) != job['manifestSha256']
                or manifest['sha256'] != job['manifestSha256']
                or pipeline_digest_for_job(job) != job['pipelineDigest']):
            raise WorkerHeld('job metadata integrity changed')
        with self.ledger._transaction() as db:
            reserved = self.ledger._reservation(db, reservation_id)
            settings = json.loads(reserved['settings'])
        if ((reserved['job'], reserved['generation'], reserved['environment'], settings['manifest_sha256'], settings['input_bytes'])
                != (job['jobId'], job['generation'], job['environment'], job['manifestSha256'], manifest['totalBytes'])):
            raise WorkerHeld('scratch reservation does not bind this input')
        outputs = {'jobs/' + reservation_id + '/' + name for name in ('all.zip', '.archive.part')}
        if set(input_paths.values()) & outputs:
            raise WorkerHeld('input aliases reserved archive paths')
        inputs = []
        for obj in manifest['objects']:
            path, _, _ = self._file(reservation_id, input_paths[obj['id']], obj['byteSize'], obj['sha256'])
            inputs.append(dict(path=str(path), bytes=obj['byteSize'], sha256=obj['sha256'], name=obj['name']))
        identity = dict(job=job, reservation=reservation_id, paths=input_paths)
        if source_binding is not None:
            identity['sourceAcceptanceBinding'] = source_binding
        encoded = p.canonical_json(identity).decode()
        try:
            # Nothing below may commit for a lease that set_engine_state would refuse.
            self.ledger.claim_engine_lease(reservation_id)
        except LeaseLapsed:
            raise WorkerHeld('reservation_expired') from None
        with NodeGate(self.engine_id) as gate:
            with self._db() as db:
                old = db.execute('SELECT * FROM executions WHERE job=?', (job['jobId'],)).fetchone()
                if old:
                    if old['identity'] != encoded:
                        raise WorkerHeld('local execution identity changed')
                    # Already committed processing may be polled; pending intent
                    # means an unknown HTTP outcome and never permits a retry.
                    return old['phase']
                # Same lease test as set_engine_state, before the gate is taken.
                with self.ledger._transaction() as lease:
                    now = self.ledger._now(lease)
                    self.ledger._expire(lease, now)
                    current = self.ledger._reservation(lease, reservation_id)
                    expired = current['state'] == 'expired' or (current['state'] == 'reserved' and current['expires'] <= now)
                if expired:
                    raise WorkerHeld('reservation_expired')
                receipt = secrets.token_hex(16)
                gate.reserve(self.node_id, self.origin, self.authority_id, self.gate_generation, receipt)
                gate.bind(receipt, dict(id=job['jobId'], workerId=self.worker_id,
                    nodeId=self.node_id, authorityId=self.authority_id))
                # Gate persistence precedes this commit. A crash between them
                # leaves an orphan hold requiring operator reconciliation.
                db.execute('INSERT INTO executions(job,identity,receipt,phase) VALUES(?,?,?,\'prepared\')',
                           (job['jobId'], encoded, receipt))
            self.ledger.set_engine_state(reservation_id, 'engine_started')
            self._phase(job['jobId'], 'initialize_pending')
            self._verify_local_engine()
            self.engine.initialize(dict(name=job['jobId'], engine_uuid=job['jobId'], outputs=None,
                options=[dict(name=k, value=v) for k, v in pipeline['odm']['options'].items()]))
            for item in inputs:
                self._phase(job['jobId'], 'upload_pending')
                self._verify_local_engine()
                self.engine.upload(job['jobId'], item)
            self._phase(job['jobId'], 'commit_pending')
            self._verify_local_engine()
            self.engine.action('commit', job['jobId'])
            self._phase(job['jobId'], 'processing')
            return 'processing'

    def task_observation(self, job_id):
        """Read-only: ('present', info), ('missing', None) or ('unreachable', None).

        Present needs the exact task and a known status code. Missing needs an
        info read that found nothing (404, or NativeEngine's failure on 200
        {error}) AND a well-formed /task/list census without the task. Anything
        else, such as a malformed 200, is unknown and proves neither.
        """
        try:
            info = self.engine.read('/task/' + job_id + '/info', missing=True)
        except Exception:
            info = None
        if info is not None:
            status = info.get('status') if type(info) is dict else None
            code = status.get('code') if type(status) is dict else None
            if type(info) is dict and info.get('uuid') == job_id and type(code) is int and code in (10, 20, 30, 40, 50):
                return 'present', info
            return 'unreachable', None
        try:
            tasks = self.engine.read('/task/list')
        except Exception:
            return 'unreachable', None
        if (type(tasks) is not list or len(tasks) > p.MAX_OBJECTS
                or any(type(task) is not dict or type(task.get('uuid')) is not str or task['uuid'] == job_id for task in tasks)):
            return 'unreachable', None
        return 'missing', None

    def poll(self, job_id):
        """Read-only engine reconciliation; terminal files stay until Viewer ACK."""
        with NodeGate(self.engine_id) as gate:
            row = self._row(job_id)
            if row['outbox_id']:
                return row['outbox_id']
            if row['phase'] == 'downloading':
                raise WorkerHeld('uncertain archive download is retained for reconciliation')
            if row['phase'] not in ('commit_pending', 'processing', 'archive_open_pending', 'archive_pending', 'archive_ready'):
                raise WorkerHeld('uncertain precommit engine mutation requires reconciliation')
            info = self.engine.read('/task/' + job_id + '/info') if row['phase'] != 'archive_ready' else {'uuid': job_id, 'status': {'code': 40}}
            if (not isinstance(info, dict) or info.get('uuid') != job_id
                    or not isinstance(info.get('status'), dict)):
                raise WorkerHeld('engine status identity unavailable')
            code = info['status'].get('code')
            if type(code) is not int or code not in (10, 20, 30, 40, 50):
                raise WorkerHeld('engine status unavailable')
            if code in (10, 20):
                return None
            if code != 40:
                raise WorkerHeld('failed or cancelled engine requires failure reconciliation')
            identity = json.loads(row['identity'])
            job, reservation = identity['job'], identity['reservation']
            with self.ledger._transaction() as db:
                settings = json.loads(self.ledger._reservation(db, reservation)['settings'])
            directory = self.ledger.prepare_job_directory(reservation)
            self.ledger._owned_path(reservation, 'jobs/' + reservation + '/all.zip')
            archive = directory / 'all.zip'
            part = directory / '.archive.part'
            self.ledger._owned_path(reservation, 'jobs/' + reservation + '/.archive.part')
            if row['phase'] != 'archive_ready':
                if archive.exists():
                    raise WorkerHeld('unexpected archive artifacts require reconciliation')
                size, sha = self._download_archive(row, reservation, part, settings['output_budget'])
                # Persist complete artifact proof before publication. Unknown
                # outcomes retain/reuse only the same durably owned cache.
                with self._db() as db:
                    db.execute("UPDATE executions SET phase='archive_ready',archive_size=?,archive_sha=? WHERE job=?", (size, sha, job_id))
                row = self._row(job_id)
            if part.exists() and archive.exists():
                if not os.path.samefile(part, archive):
                    raise WorkerHeld('archive publication identity changed')
                part.unlink()
            elif not archive.exists():
                self._file(reservation, 'jobs/' + reservation + '/.archive.part', row['archive_size'], row['archive_sha'])
                os.link(part, archive, follow_symlinks=False)
                part.unlink()
            if os.name != 'nt':
                fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            path, size, sha = self._file(reservation, str(archive.relative_to(self.ledger.root)).replace('\\', '/'), row['archive_size'], row['archive_sha'])
            if not size or size > settings['output_budget'] or not zipfile.is_zipfile(path):
                raise WorkerHeld('invalid or oversized engine archive')
            result = dict(schemaVersion=1, jobId=job_id, generation=job['generation'],
                environment=job['environment'], pipelineDigest=job['pipelineDigest'],
                engineVersions={'odm': self.version}, objects=[dict(id='odm-archive', role='odm_archive',
                    byteSize=size, sha256=sha)])
            result['resultManifestSha256'] = p.digest(result)
            relative = 'jobs/' + reservation + '/all.zip'
            self.ledger.register_result(reservation, job_id=job_id, generation=job['generation'],
                manifest_sha256=job['manifestSha256'], pipeline_sha256=job['pipelineDigest'],
                result_manifest_sha256=result['resultManifestSha256'], objects=[dict(
                    object_id='odm-archive', bytes=size, sha256=sha, relative_path=relative)])
            self.ledger.set_engine_state(reservation, 'terminal', terminal_attested=True)
            outbox_id = self.outbox.persist_terminal(job_id=job_id, generation=job['generation'],
                environment=job['environment'], input_manifest_sha256=job['manifestSha256'],
                pipeline_digest=job['pipelineDigest'], result_manifest_sha256=result['resultManifestSha256'],
                result_manifest=result, owned_prefix='jobs/' + reservation,
                local_objects={'odm-archive': relative})
            if not gate.resolved(row['receipt'], job_id):
                gate.prepare_completion(row['receipt'], dict(status='succeeded',
                    result={'resultManifestSha256': result['resultManifestSha256']}))
            with self._db() as db:
                db.execute('UPDATE executions SET phase=\'awaiting_delivery\', result=?,outbox_id=? WHERE job=?',
                           (p.canonical_json(result).decode(), outbox_id, job_id))
            return outbox_id

    def acknowledge_delivery(self, job_id):
        """Free only the engine fence after exact coordinator metadata receipt.

        Worker files and acceptance pins remain; this is not Viewer cleanup.
        """
        with NodeGate(self.engine_id) as gate:
            row = self._row(job_id)
            if not row['outbox_id']:
                raise WorkerHeld('terminal outbox is not durable')
            completion = self.outbox.get(row['outbox_id'])
            if completion['state'] != 'acknowledged':
                raise WorkerHeld('coordinator has not acknowledged the completion')
            job_ack = dict(id=job_id, workerId=self.worker_id, nodeId=self.node_id,
                authorityId=self.authority_id, status='succeeded',
                result={'resultManifestSha256': json.loads(row['result'])['resultManifestSha256']})
            if not gate.resolved(row['receipt'], job_id):
                gate.acknowledge(row['receipt'], job_ack)
            self._phase(job_id, 'delivered')

    def apply_viewer_acceptance(self, job_id, receipts):
        """Apply exact relayed Viewer receipts; never delete or bypass pins/grace.

        Transport authenticates the coordinator. A relay's fullyAccepted flag is
        never evidence: derive coverage from locally registered receipt subsets.
        """
        if type(receipts) is not list or len(receipts) > p.MAX_OBJECTS:
            raise WorkerHeld('Viewer acceptance must be a bounded receipt list')
        row = self._row(job_id)
        if not row['result'] or not row['outbox_id']:
            raise WorkerHeld('Viewer acceptance requires a durable local result')
        identity = json.loads(row['identity'])
        job, reservation = identity['job'], identity['reservation']
        try:
            source_intake_version = _source_intake_version(job, job, identity)
        except ValueError:
            raise WorkerHeld('source_acceptance_binding_unavailable') from None
        source_binding = identity.get('sourceAcceptanceBinding')
        if source_intake_version == 2:
            try:
                from .source_acceptance_binding import validate_source_acceptance_binding
                job_binding = validate_source_acceptance_binding(job.get('sourceAcceptanceBinding'), job)
                source_binding = validate_source_acceptance_binding(source_binding, job)
                if source_binding != job_binding:
                    raise ValueError('persisted source binding copies differ')
                if source_binding['coordinatorAuthorityId'] != self.authority_id:
                    raise ValueError('source authority differs from worker authority')
            except (ImportError, TypeError, ValueError, KeyError):
                raise WorkerHeld('source_acceptance_binding_unavailable') from None
        elif source_intake_version is not None or source_binding is not None:
            raise WorkerHeld('source_acceptance_binding_unavailable')
        result = p.validate_result_manifest(json.loads(row['result']), job_id=job_id,
            generation=job['generation'], environment=job['environment'], pipeline_digest=job['pipelineDigest'])
        checked, identities, signed_identities, receipt_owners = [], {}, {}, {}
        for envelope in receipts:
            viewer_receipt = None
            receipt = envelope
            if type(envelope) is dict and set(envelope) == {'acceptance', 'viewerReceipt'}:
                if not self.viewer_receipt_keys:
                    raise WorkerHeld('pinned Viewer receipt keys are not configured')
                receipt, viewer_receipt = envelope['acceptance'], envelope['viewerReceipt']
            if source_intake_version == 2 and viewer_receipt is None:
                raise WorkerHeld('signed V2 Viewer accepted-copy receipt required')
            value = p.validate_acceptance(receipt, result_manifest=result,
                input_manifest_sha256=job['manifestSha256'])
            if viewer_receipt is not None:
                try:
                    if source_intake_version == 2:
                        from .viewer_copy_receipt_v2 import verify_viewer_copy_receipt_v2
                        expected = dict(source_binding, resultManifestSha256=result['resultManifestSha256'])
                        signed = verify_viewer_copy_receipt_v2(viewer_receipt,
                            trusted_keys=self.viewer_receipt_keys, expected=expected,
                            result_manifest=result, acceptance=value).receipt
                    else:
                        from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
                        expected = dict(jobId=job_id, generation=job['generation'], environment=job['environment'],
                            inputManifestSha256=job['manifestSha256'], pipelineDigest=job['pipelineDigest'],
                            resultManifestSha256=result['resultManifestSha256'])
                        signed = verify_viewer_acceptance_receipt(viewer_receipt,
                            trusted_keys=self.viewer_receipt_keys, expected=expected,
                            result_manifest=result, accepted_object_ids=value['acceptedObjectIds']).receipt
                except (TypeError, ValueError):
                    raise WorkerHeld('Viewer accepted-copy signature or binding is invalid') from None
                sibling_fields = ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')
                if (any(value[field] != signed[field] for field in sibling_fields)
                        or value['acceptedObjectIds'] != signed['acceptedObjectIds']
                        or {value['receiptId'], value['storageReceiptId']} & {signed['receiptId'], signed['storageReceiptId']}):
                    raise WorkerHeld('Viewer and processing acceptance bindings disagree')
            encoded = p.canonical_json(value)
            if value['receiptId'] in identities and identities[value['receiptId']] != encoded:
                raise WorkerHeld('relayed receipt identity changed')
            identities[value['receiptId']] = encoded
            receipt_ids = {value['receiptId'], value['storageReceiptId']}
            if viewer_receipt is not None:
                signed_encoded = p.canonical_json(signed)
                if value['receiptId'] in signed_identities and signed_identities[value['receiptId']] != signed_encoded:
                    raise WorkerHeld('relayed signed Viewer receipt changed')
                signed_identities[value['receiptId']] = signed_encoded
                receipt_ids.update((signed['receiptId'], signed['storageReceiptId']))
            if len(receipt_ids) != (4 if viewer_receipt is not None else 2):
                raise WorkerHeld('relayed acceptance identities are not distinct')
            if any(owner != value['receiptId'] for identity in receipt_ids
                   if (owner := receipt_owners.get(identity)) is not None):
                raise WorkerHeld('relayed acceptance identity collision')
            for identity in receipt_ids:
                receipt_owners[identity] = value['receiptId']
            checked.append((value, signed if viewer_receipt is not None else None))
        for receipt, signed_viewer_receipt in checked:
            self.ledger.accept_receipt(reservation, receipt_id=receipt['receiptId'],
                storage_receipt_id=receipt['storageReceiptId'], job_id=job_id, generation=receipt['generation'],
                manifest_sha256=receipt['inputManifestSha256'], pipeline_sha256=receipt['pipelineDigest'],
                result_manifest_sha256=receipt['resultManifestSha256'], accepted_object_ids=receipt['acceptedObjectIds'],
                viewer_receipt=signed_viewer_receipt, verified_copy=signed_viewer_receipt is not None)
        with self.ledger._transaction() as db:
            if source_intake_version == 2:
                accepted = {obj['id'] for obj in db.execute(
                    'SELECT id FROM objects WHERE reservation=? AND accepted IS NOT NULL AND accepted_copy_verified=1',
                    (reservation,))}
            else:
                accepted = {obj['id'] for obj in db.execute(
                    'SELECT id FROM objects WHERE reservation=? AND accepted IS NOT NULL', (reservation,))}
        return {'acceptedObjectIds': sorted(accepted),
                'fullyAccepted': accepted == {obj['id'] for obj in result['objects']}}
