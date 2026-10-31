"""Main-host TreeDar controller; no scheduler, registration, pulls, or activation.

All Docker writers must share this OS user's canonical NodeGate. Docker access
and installed source are trusted configuration. Unknown mutations never replay.
The engine container receives no socket, credentials, network, or caller code.
Outputs and stopped containers remain retained; the driver owns completion ACK
and gate release. Filesystem fsync requests durability, not hardware guarantees.
"""
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import subprocess
import sys
import time

from . import reference_protocol as p
from .node_gate import NodeGate, _binding as _gate_binding
from .treedar_admission import AdmissionPolicy, _provenance, validate_lidar


SOURCE_FILES = ('__init__.py', 'reference_protocol.py', 'node_gate.py',
                'treedar_admission.py', 'treedar_engine.py')
REQUIRED_OUTPUTS = {'dtm.tif','chm.tif','chm_raw.tif','crowns.tif','trees.csv','tree_features.csv',
                    'summary.json','manifest.json','report.html','tree_map.geojson','areas.png','tree_map.png'}
INSPECT = ('{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},'
           '"status":{{json .State.Status}},"exitCode":{{json .State.ExitCode}},'
           '"binding":{{json (index .Config.Labels "ltds.treedar.binding")}}}')


class TreeDarHeld(ValueError):
    """Bounded reason only; unknown engine state retains the physical fence."""


@dataclass(frozen=True)
class TreeDarConfig:
    docker_executable: str
    docker_socket: str
    image_id: str
    engine_id: str
    authority_id: str
    coordinator_origin: str
    gate_generation: int
    worker_id: str
    source_root: str
    source_sha256: str
    node_id: str = 'main'
    uid: int = 1000
    gid: int = 1000
    engine_version: str = '0.0.1'
    timeout_seconds: int | None = None
    output_max_files: int = 64

    def __post_init__(self):
        if self.node_id != 'main' or type(self.uid) is not int or type(self.gid) is not int or self.uid < 1 or self.gid < 1:
            raise ValueError('configured nonroot main host required')
        for value in (self.docker_executable, self.docker_socket, self.source_root):
            if not isinstance(value, str) or not Path(value).is_absolute() or any(c in value for c in '\n\r,'):
                raise ValueError('fixed absolute local configuration required')
        if not re.fullmatch(r'sha256:[a-f0-9]{64}', self.image_id):
            raise ValueError('exact cached image ID required')
        p._hash(self.source_sha256, 'installed source digest')
        p._uuid(self.authority_id, 'authorityId'); p._slug(self.worker_id, 'workerId')
        _gate_binding(self.node_id, self.coordinator_origin, self.authority_id)
        if not isinstance(self.engine_version, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]{0,79}', self.engine_version):
            raise ValueError('bounded configured engine version required')
        p._integer(self.gate_generation, 'gate generation')
        if self.timeout_seconds is not None and (type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= p.MAX_SAFE_INTEGER):
            raise ValueError('explicit positive timeout or none required')
        if type(self.output_max_files) is not int or not 16 <= self.output_max_files <= 100:
            raise ValueError('bounded output file count required')


def source_digest(root):
    """Installer/configuration helper; never derive a trusted pin from submission."""
    root = Path(root)
    return p.digest({name: hashlib.sha256((root / 'processing' / name).read_bytes()).hexdigest()
                     for name in SOURCE_FILES})


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _entries(path, maximum):
    """Bound enumeration itself, including unexpected files, before materializing."""
    entries = []
    with os.scandir(path) as stream:
        for entry in stream:
            if len(entries) >= maximum:
                raise TreeDarHeld('output_entry_budget_exceeded')
            entries.append(Path(entry.path))
    return entries


def _write_new(path, payload):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), 'wb') as stream:
        stream.write(p.canonical_json(payload))
        stream.flush()
        os.fsync(stream.fileno())
    _sync_directory(path.parent)


def _file(path, maximum, *, expected_bytes=None, expected_sha=None):
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise TreeDarHeld('owned_regular_file_required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or not 0 <= before.st_size <= maximum:
            raise TreeDarHeld('file_budget_or_identity_changed')
        digest = hashlib.sha256()
        count = 0
        while chunk := os.read(fd, min(1024**2, maximum - count + 1)):
            count += len(chunk)
            if count > maximum:
                raise TreeDarHeld('file_budget_exceeded')
            digest.update(chunk)
        os.fsync(fd)
        after, current = os.fstat(fd), path.lstat()
        snapshot = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        if snapshot(before) != snapshot(after) or snapshot(after) != snapshot(current):
            raise TreeDarHeld('file_changed_during_verification')
        sha = digest.hexdigest()
        if (expected_bytes is not None and count != expected_bytes) or (expected_sha is not None and sha != expected_sha):
            raise TreeDarHeld('file_integrity_changed')
        return count, sha
    finally:
        os.close(fd)


class TreeDarEngine:
    def __init__(self, path, *, ledger, config, verify_provenance, policy=None,
                 runner=None, clock=None):
        if not sys.platform.startswith('linux'):
            raise TreeDarHeld('linux_host_required')
        if os.geteuid() != config.uid or os.getegid() != config.gid:
            raise TreeDarHeld('configured_host_user_required')
        self.path, self.ledger, self.config = Path(path).absolute(), ledger, config
        self.policy = policy or AdmissionPolicy()
        self.verifier, self.runner, self.clock = verify_provenance, runner, clock or time.time
        if not callable(verify_provenance):
            raise ValueError('trusted provenance verifier required')
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        self._directory(self.path.parent, private=True)
        if self.path.is_symlink():
            raise TreeDarHeld('private_journal_required')
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS configuration (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            db.execute('''CREATE TABLE IF NOT EXISTS treedar_executions (
                job TEXT PRIMARY KEY, identity TEXT NOT NULL, receipt TEXT NOT NULL,
                name TEXT NOT NULL UNIQUE, container TEXT, phase TEXT NOT NULL,
                deadline REAL, proof TEXT)''')
            encoded = p.canonical_json(dict(config=asdict(config), policy=asdict(self.policy))).decode()
            old = db.execute('SELECT value FROM configuration WHERE id=1').fetchone()
            if old and old[0] != encoded:
                raise TreeDarHeld('execution_configuration_changed')
            db.execute('INSERT OR IGNORE INTO configuration VALUES(1,?)', (encoded,))
        os.chmod(self.path, 0o600)
        _sync_directory(self.path.parent)

    def _directory(self, path, *, private=False):
        if any(item.is_symlink() for item in (path, *path.parents)):
            raise TreeDarHeld('owned_directory_required')
        info = path.stat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != self.config.uid
                or info.st_mode & (0o077 if private else 0o022)):
            raise TreeDarHeld('owned_directory_permissions_required')

    @contextmanager
    def _db(self):
        if self.path.exists():
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != self.config.uid:
                raise TreeDarHeld('owned_private_journal_required')
        db = sqlite3.connect(self.path, timeout=20)
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
            row = db.execute('SELECT * FROM treedar_executions WHERE job=?', (job_id,)).fetchone()
            if row is None:
                raise TreeDarHeld('unknown_execution')
            return dict(row)

    def _phase(self, job_id, phase, **values):
        with self._db() as db:
            if values:
                if set(values) != {'container'}:
                    raise ValueError('invalid execution update')
                db.execute('UPDATE treedar_executions SET phase=?,container=? WHERE job=?', (phase, values['container'], job_id))
            else:
                db.execute('UPDATE treedar_executions SET phase=? WHERE job=?', (phase, job_id))

    def _docker(self, arguments, *, timeout=20):
        command = [self.config.docker_executable, '--host', 'unix://' + self.config.docker_socket, *arguments]
        try:
            if self.runner:
                result = self.runner(command, timeout=timeout)
            else:
                result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL, timeout=timeout, check=False,
                    env={'PATH': str(Path(self.config.docker_executable).parent),
                         'HOME': str(self.path.parent), 'DOCKER_CONFIG': str(self.path.parent / 'no-registry-config')})
            output = result.stdout.decode() if isinstance(result.stdout, bytes) else result.stdout
            if result.returncode != 0 or not isinstance(output, str) or len(output) > 16384:
                raise TreeDarHeld('docker_control_outcome_unavailable')
            return output.strip()
        except TreeDarHeld:
            raise
        except Exception:
            raise TreeDarHeld('docker_control_outcome_uncertain') from None

    def _gate(self, gate, receipt=None, job_id=None):
        state = gate.snapshot()
        c = self.config
        if (state['nodeId'], state['authorityId'], state['coordinatorOrigin'], state['generation']) != (
                'main', c.authority_id, c.coordinator_origin.rstrip('/'), c.gate_generation):
            raise TreeDarHeld('physical_gate_binding_changed')
        if receipt is not None:
            reservation = state['reservation']
            expected = dict(id=job_id, workerId=c.worker_id, nodeId='main', authorityId=c.authority_id)
            if not reservation or reservation['receiptId'] != receipt or reservation['job'] != expected:
                raise TreeDarHeld('physical_gate_reservation_changed')

    def _host(self):
        if not stat.S_ISSOCK(Path(self.config.docker_socket).stat().st_mode):
            raise TreeDarHeld('local_unix_docker_socket_required')
        if self._docker(['info', '--format', '{{.ID}}']) != self.config.engine_id:
            raise TreeDarHeld('physical_docker_engine_changed')
        if self._docker(['image', 'inspect', '--format', '{{.Id}}', self.config.image_id]) != self.config.image_id:
            raise TreeDarHeld('cached_image_unavailable')
        if source_digest(self.config.source_root) != self.config.source_sha256:
            raise TreeDarHeld('installed_bootstrap_source_changed')
        for filename in SOURCE_FILES:
            source = Path(self.config.source_root) / 'processing' / filename
            if any(part.is_symlink() for part in (source, *source.parents)) or source.stat().st_mode & 0o022:
                raise TreeDarHeld('immutable_installed_source_required')

    def start(self, job, *, reservation_id, input_object, input_relative_path,
              provenance, allow_synthetic=False):
        """One launch attempt. Returned pending phases never authorize a retry."""
        p._uuid(job['jobId'], 'jobId')
        pipeline, manifest = job['submission']['pipeline'], job['submission']['manifest']
        if (job['status'] != 'running' or job['nodeId'] != 'main'
                or pipeline['odm']['requested'] or not pipeline['treedar']['requested']
                or pipeline['gaussian_splat']['requested']
                or manifest['objects'] != [input_object] or input_object['kind'] != 'lidar'
                or not input_object['name'].lower().endswith(('.las','.laz'))
                or pipeline['treedar']['inputObjectIds'] != [input_object['id']]
                or pipeline['treedar']['provenanceReceiptId'] != provenance.get('receiptId')
                or p.manifest_sha256(manifest) != job['manifestSha256']
                or p.digest(pipeline) != job['pipelineDigest']):
            raise TreeDarHeld('unsupported_or_unbound_treedar_job')
        checked = _provenance(provenance, self.verifier, job_id=job['jobId'],
            generation=job['generation'], environment=job['environment'], object_id=input_object['id'],
            expected_sha256=input_object['sha256'], expected_bytes=input_object['byteSize'],
            allow_synthetic=allow_synthetic, policy=self.policy)
        self.ledger._owned_path(reservation_id, input_relative_path)
        source = self.ledger.root / input_relative_path
        _file(source, self.policy.max_bytes, expected_bytes=input_object['byteSize'], expected_sha=input_object['sha256'])
        with self.ledger._transaction() as db:
            reserved = self.ledger._reservation(db, reservation_id)
            settings = json.loads(reserved['settings'])
        if (reserved['job'], reserved['generation'], reserved['environment'], settings['manifest_sha256'], settings['input_bytes']) != (
                job['jobId'], job['generation'], job['environment'], job['manifestSha256'], input_object['byteSize']):
            raise TreeDarHeld('scratch_reservation_binding_changed')
        output_relative = 'jobs/' + reservation_id + '/treedar-output'
        self.ledger._owned_path(reservation_id, output_relative)
        identity = dict(job={key: job[key] for key in ('jobId','generation','environment','manifestSha256','pipelineDigest')},
            reservationId=reservation_id, inputObject=input_object, inputRelativePath=input_relative_path,
            outputRelativePath=output_relative, provenance=checked, allowSynthetic=allow_synthetic is True,
            policy=asdict(self.policy), outputBudget=settings['output_budget'])
        encoded = p.canonical_json(identity).decode()
        with NodeGate(self.config.engine_id) as gate:
            self._gate(gate)
            with self._db() as db:
                old = db.execute('SELECT * FROM treedar_executions WHERE job=?', (job['jobId'],)).fetchone()
                if old:
                    if old['identity'] != encoded:
                        raise TreeDarHeld('execution_identity_changed')
                    self._gate(gate, old['receipt'], job['jobId'])
                    return old['phase']
                if reserved['state'] != 'reserved':
                    raise TreeDarHeld('fresh_scratch_reservation_required')
                self._host()
                # Input hashing and Docker/source probes can outlive the NAS
                # grant's freshness. Revalidate the frozen exact receipt at the
                # final admission boundary, before reserving or mutating Docker.
                _provenance(checked,self.verifier,job_id=job['jobId'],generation=job['generation'],
                    environment=job['environment'],object_id=input_object['id'],
                    expected_sha256=input_object['sha256'],expected_bytes=input_object['byteSize'],
                    allow_synthetic=allow_synthetic,policy=self.policy)
                receipt = secrets.token_hex(16)
                gate.reserve('main', self.config.coordinator_origin, self.config.authority_id, self.config.gate_generation, receipt)
                gate.bind(receipt, dict(id=job['jobId'], workerId=self.config.worker_id, nodeId='main', authorityId=self.config.authority_id))
                name = 'ltds-treedar-' + receipt
                db.execute("INSERT INTO treedar_executions VALUES(?,?,?,?,NULL,'prepare_pending',?,NULL)",
                    (job['jobId'], encoded, receipt, name,
                     self.clock() + self.config.timeout_seconds + 30 if self.config.timeout_seconds is not None else None))
            output = self.ledger.root / output_relative
            output.mkdir(mode=0o700)
            self._directory(output, private=True)
            _sync_directory(output.parent)
            control = self.path.parent / (receipt + '.json')
            task = identity | dict(sourceSha256=self.config.source_sha256, engineVersion=self.config.engine_version,
                timeoutSeconds=self.config.timeout_seconds, maxFiles=self.config.output_max_files)
            _write_new(control, task)
            self.ledger.set_engine_state(reservation_id, 'engine_started')
            binding = p.digest(identity)
            if any(any(char in str(path) for char in ',\n\r') for path in (source, output, control)):
                raise TreeDarHeld('unsafe_configured_mount_path')
            args = ['create', '--pull', 'never', '--name', name, '--label', 'ltds.treedar.binding=' + binding,
                '--network', 'none', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges',
                '--user', f'{self.config.uid}:{self.config.gid}',
                '--log-driver', 'none', '--tmpfs', '/tmp:rw,nosuid,nodev,mode=1777',
                '--env', 'PYTHONDONTWRITEBYTECODE=1', '--env', 'PYTHONPATH=/worker',
                '--mount', f'type=bind,source={source},target=/input/cloud.las,readonly',
                '--mount', f'type=bind,source={output},target=/output',
                '--mount', f'type=bind,source={control},target=/control/task.json,readonly']
            for filename in SOURCE_FILES:
                fixed = Path(self.config.source_root) / 'processing' / filename
                args += ['--mount', f'type=bind,source={fixed},target=/worker/processing/{filename},readonly']
            args += ['--entrypoint', '/usr/local/bin/python', self.config.image_id,
                     '-B', '-m', 'processing.treedar_engine', '--inside']
            self._phase(job['jobId'], 'create_pending')
            container = self._docker(args)
            if not re.fullmatch(r'[a-f0-9]{64}', container):
                raise TreeDarHeld('container_create_identity_uncertain')
            self._phase(job['jobId'], 'created', container=container)
            self._phase(job['jobId'], 'start_pending')
            if self._docker(['start', container]) != container:
                raise TreeDarHeld('container_start_outcome_uncertain')
            self._phase(job['jobId'], 'processing')
            return 'processing'

    def _inspect(self, row):
        observed = json.loads(self._docker(['inspect', '--format', INSPECT, row['container'] or row['name']]))
        expected_binding = p.digest(json.loads(row['identity']))
        if (set(observed) != {'id','image','running','status','exitCode','binding'}
                or not re.fullmatch(r'[a-f0-9]{64}', observed['id'])
                or (row['container'] and row['container'] != observed['id'])
                or observed['image'] != self.config.image_id or observed['binding'] != expected_binding
                or type(observed['running']) is not bool or type(observed['exitCode']) is not int):
            raise TreeDarHeld('container_inspection_binding_changed')
        return observed

    def poll(self, job_id):
        """Reconcile exact container only; never re-create or re-start after crash."""
        row = self._row(job_id)
        if row['proof']:
            proof = json.loads(row['proof'])
            self._settle(json.loads(row['identity']), proof)
            return proof
        with NodeGate(self.config.engine_id) as gate:
            self._gate(gate, row['receipt'], job_id)
            self._host()
            observed = self._inspect(row)
            if not row['container']:
                self._phase(job_id, row['phase'], container=observed['id'])
                row['container'] = observed['id']
            if observed['running']:
                if row['phase'] == 'stop_pending':
                    raise TreeDarHeld('container_stop_requires_reconciliation')
                if row['deadline'] is None or self.clock() < row['deadline']:
                    return None
                self._phase(job_id, 'stop_pending')
                self._docker(['stop', '--time', '10', row['container']], timeout=20)
                observed = self._inspect(row)
            if observed['running'] or observed['status'] != 'exited':
                raise TreeDarHeld('container_terminal_state_unconfirmed')
            identity = json.loads(row['identity'])
            output = self.ledger.root / identity['outputRelativePath']
            proof = dict(job=identity['job'], engineTerminal=True, containerId=observed['id'], imageId=self.config.image_id,
                status='failed', errorCode='treedar_execution_failed')
            allocated = identity['inputObject']['byteSize']
            if observed['exitCode'] == 0:
                result, paths, size = self._collect(identity, output)
                allocated += size
                proof = dict(job=identity['job'], engineTerminal=True, containerId=observed['id'], imageId=self.config.image_id,
                             status='succeeded', resultManifest=result, localObjects=paths, allocatedBytes=allocated)
            else:
                # No artifacts are removed or declared accepted on failure.
                proof['errorCode'] = 'treedar_container_terminal_failure'
            with self._db() as db:
                db.execute("UPDATE treedar_executions SET phase='terminal_proof',proof=? WHERE job=?",
                           (p.canonical_json(proof).decode(), job_id))
            self._settle(identity, proof)
            return proof

    def _settle(self, identity, proof):
        with self.ledger._transaction() as db:
            state = self.ledger._reservation(db, identity['reservationId'])['state']
        if state != 'terminal':
            if proof['status'] == 'succeeded':
                self.ledger.record_allocated(identity['reservationId'], proof['allocatedBytes'])
            self.ledger.set_engine_state(identity['reservationId'], 'terminal', terminal_attested=True)

    def remove_container(self, job_id):
        """Optional exact stopped-container removal, never output/image cleanup."""
        row = self._row(job_id)
        if not row['proof'] or row['phase'] == 'remove_pending':
            raise TreeDarHeld('container_removal_requires_reconciliation')
        if row['phase'] == 'container_removed':
            return
        with NodeGate(self.config.engine_id) as gate:
            self._gate(gate)
            self._host()
            observed = self._inspect(row)
            if observed['running'] or observed['status'] != 'exited':
                raise TreeDarHeld('container_terminal_state_unconfirmed')
            self._phase(job_id, 'remove_pending')
            if self._docker(['rm', row['container']]) != row['container']:
                raise TreeDarHeld('container_removal_outcome_uncertain')
            self._phase(job_id, 'container_removed')

    def _collect(self, identity, output):
        self._directory(output, private=True)
        proof_path = output / 'proof.json'
        _file(proof_path, 128 * 1024)
        proof = json.loads(proof_path.read_bytes())
        if (set(proof) != {'schemaVersion','job','engineVersion','admission','objects'}
                or proof['schemaVersion'] != 1 or type(proof['schemaVersion']) is not int
                or proof['job'] != identity['job'] or proof['engineVersion'] != self.config.engine_version
                or not isinstance(proof['objects'], list) or not 1 <= len(proof['objects']) <= self.config.output_max_files):
            raise TreeDarHeld('engine_output_proof_invalid')
        total, objects, paths = proof_path.stat().st_size, [], {}
        expected_admission = dict(jobId=identity['job']['jobId'], generation=identity['job']['generation'],
            environment=identity['job']['environment'], objectId=identity['inputObject']['id'],
            objectSha256=identity['inputObject']['sha256'], byteSize=identity['inputObject']['byteSize'],
            sourceKind=identity['provenance']['sourceKind'], verticalUnits='metre',
            verticalDatum=identity['provenance']['verticalDatum'])
        if not isinstance(proof['admission'], dict) or any(proof['admission'].get(key) != value for key, value in expected_admission.items()):
            raise TreeDarHeld('output_admission_binding_changed')
        for item in proof['objects']:
            p._closed(item, ('id','role','byteSize','sha256','relativePath'), 'TreeDar output proof')
            relative = item['relativePath']
            if not isinstance(relative, str) or not re.fullmatch(r'run/[a-zA-Z0-9_.-]{1,100}', relative):
                raise TreeDarHeld('engine_output_path_invalid')
            if item['id'] != 'treedar-' + hashlib.sha256(relative.encode()).hexdigest()[:32] or item['id'] in paths:
                raise TreeDarHeld('engine_output_identity_invalid')
            size, _ = _file(output / relative, identity['outputBudget'], expected_bytes=item['byteSize'], expected_sha=item['sha256'])
            total += size
            if total > identity['outputBudget']:
                raise TreeDarHeld('engine_output_budget_exceeded')
            objects.append({key: item[key] for key in ('id','role','byteSize','sha256')})
            paths[item['id']] = identity['outputRelativePath'] + '/' + relative
        if ({path.name for path in _entries(output, 2)} != {'proof.json','run'}
                or {path.name for path in _entries(output / 'run', self.config.output_max_files)} !=
                   {item['relativePath'].split('/')[1] for item in proof['objects']}
                or not REQUIRED_OUTPUTS <= {item['relativePath'].split('/')[1] for item in proof['objects']}):
            raise TreeDarHeld('unlisted_engine_output_retained')
        _sync_directory(output / 'run'); _sync_directory(output)
        result = dict(schemaVersion=1, jobId=identity['job']['jobId'], generation=identity['job']['generation'],
            environment=identity['job']['environment'], pipelineDigest=identity['job']['pipelineDigest'],
            engineVersions={'treedar': self.config.engine_version}, objects=objects)
        result['resultManifestSha256'] = p.digest(result)
        return p.validate_result_manifest(result, job_id=result['jobId'], generation=result['generation'],
            environment=result['environment'], pipeline_digest=result['pipelineDigest']), paths, total


def _inside():
    """Fixed installed-source bootstrap; no arbitrary executable/argv input."""
    import resource
    from treedar import __version__
    from treedar.runinfo import ROLES
    task = json.loads(Path('/control/task.json').read_bytes())
    if source_digest('/worker') != task['sourceSha256'] or __version__ != task['engineVersion']:
        return 1
    policy_values = task['policy']
    policy_values['approved_vertical_datums'] = tuple(policy_values['approved_vertical_datums'])
    policy = AdmissionPolicy(**policy_values)
    obj, job = task['inputObject'], task['job']
    admission = validate_lidar('/input/cloud.las', job_id=job['jobId'], generation=job['generation'],
        environment=job['environment'], object_id=obj['id'], expected_sha256=obj['sha256'],
        expected_bytes=obj['byteSize'], provenance=task['provenance'],
        verify_provenance=lambda candidate: candidate == task['provenance'], policy=policy,
        allow_synthetic=task['allowSynthetic'])
    # Conservative per-file write limit plus fixed CLI outputs. This is not a
    # filesystem quota; the host must retain scratch headroom for temporary files.
    per_file = max(1, task['outputBudget'] // task['maxFiles'])
    resource.setrlimit(resource.RLIMIT_FSIZE, (per_file, per_file))
    output = Path('/output')
    completed = subprocess.run(['/usr/local/bin/treedar','pipeline','/input/cloud.las','--out','/output/run',
        '--cell',str(policy.raster_cell_m),'--step','1','--preset','broadleaf'],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        timeout=task['timeoutSeconds'], check=False)
    if completed.returncode:
        return 1
    run = output / 'run'
    files = _entries(run, task['maxFiles'])
    if not REQUIRED_OUTPUTS <= {path.name for path in files} or len(files) > task['maxFiles']:
        return 1
    import numpy as np
    import rasterio
    reference = None
    for name in ('dtm.tif','chm.tif','chm_raw.tif','crowns.tif'):
        with rasterio.open(run / name) as raster:
            geometry = (raster.shape, raster.transform, raster.crs)
            if (raster.crs is None or raster.crs.to_string() != admission['crs']
                    or raster.width * raster.height > policy.max_grid_cells
                    or (reference is not None and geometry != reference)):
                return 1
            reference = geometry
            for _, window in raster.block_windows(1):
                if not np.isfinite(raster.read(1, window=window)).all():
                    return 1
    summary = json.loads((run / 'summary.json').read_bytes())
    native_manifest = json.loads((run / 'manifest.json').read_bytes())
    if (summary['n_points_used'] != admission['pointCount'] or summary['version'] != __version__
            or native_manifest['command'] != 'pipeline' or native_manifest['treedar_version'] != __version__):
        return 1
    objects = []
    total = 0
    for path in sorted(files):
        if path.name not in ROLES and path.name != 'manifest.json':
            return 1
        size, sha = _file(path, per_file)
        total += size
        if total > task['outputBudget']:
            return 1
        relative = 'run/' + path.name
        objects.append(dict(id='treedar-' + hashlib.sha256(relative.encode()).hexdigest()[:32],
            role=ROLES[path.name][0] if path.name in ROLES else 'engine_manifest',
            byteSize=size, sha256=sha, relativePath=relative))
    _sync_directory(run)
    _write_new(output / 'proof.json', dict(schemaVersion=1, job=job, engineVersion=__version__, admission=admission, objects=objects))
    return 0


if __name__ == '__main__':
    if sys.argv[1:] != ['--inside']:
        raise SystemExit('fixed bootstrap invocation required')
    try:
        raise SystemExit(_inside())
    except Exception:
        raise SystemExit(1) from None
