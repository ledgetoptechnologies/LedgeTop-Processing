"""Private NodeODM execution controller using the shared physical-engine gate.

Inputs must already be fetched into a capacity-reserved private job directory.
No daemon or engine is started by import. Uncertain mutations are never replayed.
The injected engine implements NativeEngine; only configured local origins are
supported by the deployment driver (not supplied by submissions).
"""
from contextlib import contextmanager, ExitStack
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import re
import sqlite3
import stat
import zipfile
from urllib.parse import urlsplit

from . import reference_protocol as p
from .node_gate import NodeGate, GateError
from . import reference_native_archive as native_archive


class WorkerHeld(ValueError):
    pass


class ReferenceEngineWorker:
    def __init__(self, path, *, ledger, outbox, engine, engine_id, node_id,
                 authority_id, coordinator_origin, gate_generation, worker_id,
                 engine_version, allowed_odm_options=(), auxiliary_budget=None):
        self.path = Path(path).absolute()
        from .reference_auxiliary_budget import reject_unbudgeted
        reject_unbudgeted(self.path,auxiliary_budget)
        if auxiliary_budget is not None and (getattr(ledger,'auxiliary_budget',None) is not auxiliary_budget
                or getattr(outbox,'auxiliary_budget',None) is not auxiliary_budget):
            raise WorkerHeld('auxiliary_writer_composition_required')
        self.auxiliary_budget=auxiliary_budget
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
        self.authority_id, self.origin = authority_id, coordinator_origin
        self.gate_generation, self.worker_id = gate_generation, worker_id
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
        from .reference_auxiliary_budget import writer_scope
        scope=ExitStack()
        configure=scope.enter_context(writer_scope(self.path,self.auxiliary_budget))
        try:
            db = sqlite3.connect(self.path, timeout=10)
            scope.callback(db.close)
            configure(db)
        except BaseException:
            scope.close();raise
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
        finally:
            scope.close()

    def _row(self, job_id):
        with self._db() as db:
            row = db.execute('SELECT * FROM executions WHERE job=?', (job_id,)).fetchone()
            if row is None:
                raise WorkerHeld('unknown local execution')
            return dict(row)

    def _phase(self, job_id, phase):
        with self._db() as db:
            db.execute('UPDATE executions SET phase=? WHERE job=?', (phase, job_id))

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
        manifest, pipeline = submission['manifest'], submission['pipeline']
        if (job['status'] != 'running' or job['nodeId'] != self.node_id
                or not pipeline['odm']['requested'] or pipeline['treedar']['requested']
                or pipeline['gaussian_splat']['requested']
                or any(obj['kind'] != 'image' for obj in manifest['objects'])
                or set(pipeline['odm']['options']) - self.allowed_options
                or set(input_paths) != {obj['id'] for obj in manifest['objects']}):
            raise WorkerHeld('job is unsupported or lacks an active local claim')
        if (p.manifest_sha256(manifest) != job['manifestSha256']
                or manifest['sha256'] != job['manifestSha256']
                or p.digest(pipeline) != job['pipelineDigest']):
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
        encoded = p.canonical_json(identity).decode()
        with NodeGate(self.engine_id) as gate:
            with self._db() as db:
                old = db.execute('SELECT * FROM executions WHERE job=?', (job['jobId'],)).fetchone()
                if old:
                    if old['identity'] != encoded:
                        raise WorkerHeld('local execution identity changed')
                    # Already committed processing may be polled; pending intent
                    # means an unknown HTTP outcome and never permits a retry.
                    return old['phase']
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
            self.engine.initialize(dict(name=job['jobId'], engine_uuid=job['jobId'], outputs=None,
                options=[dict(name=k, value=v) for k, v in pipeline['odm']['options'].items()]))
            for item in inputs:
                self._phase(job['jobId'], 'upload_pending')
                self.engine.upload(job['jobId'], item)
            self._phase(job['jobId'], 'commit_pending')
            self.engine.action('commit', job['jobId'])
            self._phase(job['jobId'], 'processing')
            return 'processing'

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
            directory = self.ledger.root / 'jobs' / reservation
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
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
        result = p.validate_result_manifest(json.loads(row['result']), job_id=job_id,
            generation=job['generation'], environment=job['environment'], pipeline_digest=job['pipelineDigest'])
        checked, identities = [], {}
        for receipt in receipts:
            value = p.validate_acceptance(receipt, result_manifest=result,
                input_manifest_sha256=job['manifestSha256'])
            encoded = p.canonical_json(value)
            if value['receiptId'] in identities and identities[value['receiptId']] != encoded:
                raise WorkerHeld('relayed receipt identity changed')
            identities[value['receiptId']] = encoded
            checked.append(value)
        for receipt in checked:
            self.ledger.accept_receipt(reservation, receipt_id=receipt['receiptId'],
                storage_receipt_id=receipt['storageReceiptId'], job_id=job_id, generation=receipt['generation'],
                manifest_sha256=receipt['inputManifestSha256'], pipeline_sha256=receipt['pipelineDigest'],
                result_manifest_sha256=receipt['resultManifestSha256'], accepted_object_ids=receipt['acceptedObjectIds'])
        with self.ledger._transaction() as db:
            accepted = {obj['id'] for obj in db.execute(
                'SELECT id FROM objects WHERE reservation=? AND accepted IS NOT NULL', (reservation,))}
        return {'acceptedObjectIds': sorted(accepted),
                'fullyAccepted': accepted == {obj['id'] for obj in result['objects']}}
