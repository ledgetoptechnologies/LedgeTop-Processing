"""Durable worker driver over fixed authenticated HTTPS boundaries.

ODM is the default; an explicit router binds supported engines per claimed job.
Engine mutations remain physically fenced. Delivery, Viewer acceptance and
optional exact-object cleanup continue after slot release.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import hashlib
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import ssl
import stat
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from . import reference_protocol as p
from .reference_engine_worker import WorkerHeld
from .reference_sender import ReferenceSender
from .reference_transfer import BrokerClient, TransferResponseUnavailable, validate_grant
from .viewer_service import NativeEngine
from .node_gate import NodeGate
from .reference_failure import FailureOutbox, make_failure
from .reference_terminal import validate_terminal_acceptance
from .reference_engine_cleanup import EngineCacheCleanup
from .options import _validate_options
from .reference_runtime import _UNSAFE
from .reference_execution import ExecutionRouter, RouteConflict
from .treedar_engine import TreeDarHeld


PREFIX = '/ltds/worker/v1'


class DriverHeld(ValueError):
    pass


class DriverWait(ValueError):
    pass


class DriverStepLease:
    def __init__(self,driver):self.driver=driver;self.pid=os.getpid();self.thread=threading.get_ident();self.lock=driver._lock
    def check(self,driver):
        if self.driver is not driver or self.pid!=os.getpid() or self.thread!=threading.get_ident() or getattr(driver,'_active_step',None) is not self or self.lock is not driver._lock or not self.lock.locked():raise DriverHeld('actual_driver_step_required')
        driver._state_owner.check(driver.path.parent)


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise DriverWait('coordinator_redirect_rejected')


class ReferenceDriver:
    @staticmethod
    def migration_census(path, *, physical_binding):
        """Read-only closed driver history; never instantiate or resume a driver."""
        path = Path(path).absolute()
        if not os.path.lexists(path):
            return {'jobs': {}, 'sha256': p.digest([])}
        if path.resolve() != path or any(item.is_symlink() for item in (path,*path.parents)):
            raise DriverHeld('migration_driver_journal_unavailable')
        info = path.stat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077))
                or any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal'))):
            raise DriverHeld('migration_driver_journal_unavailable')
        db = sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)
        db.row_factory = sqlite3.Row
        try:
            db.execute('BEGIN')
            rows = db.execute('SELECT job,stage,data FROM driver_jobs ORDER BY job').fetchall()
            binding = db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            if rows and not binding:
                raise DriverHeld('migration_driver_binding_unavailable')
            if binding:
                binding = json.loads(binding[0])
                expected = {key:physical_binding[key] for key in ('nodeId','workerId','engineId','authorityId','gateGeneration')}
                expected['gateOrigin'] = physical_binding['origin']
                if any(binding.get(key) != value for key,value in expected.items()):
                    raise DriverHeld('migration_driver_binding_changed')
            jobs = {}
            for row in rows:
                if row['stage'] not in ('completed','awaiting_acceptance','awaiting_failure_acceptance'):
                    raise DriverHeld('migration_driver_not_terminal')
                data = json.loads(row['data'])
                claim = data['claim']
                if claim['jobId'] != row['job']:
                    raise DriverHeld('migration_driver_identity_changed')
                identity = ExecutionRouter._identity(claim)
                if identity['nodeId'] != physical_binding['nodeId']:
                    raise DriverHeld('migration_driver_identity_changed')
                submission = claim['submission']
                raw = {key:submission[key] for key in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
                checked = p.validate_submission(raw, environment=identity['environment'],
                    capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm','treedar'),
                    allowed_odm_options=set(raw['pipeline']['odm']['options']))
                if (p.canonical_json(checked) != p.canonical_json(submission)
                        or checked['manifestSha256'] != identity['manifestSha256']
                        or checked['pipelineDigest'] != identity['pipelineDigest']):
                    raise DriverHeld('migration_driver_submission_changed')
                kind = ReferenceDriver._pipeline_kind(submission)
                if kind not in ('odm','treedar'):
                    raise DriverHeld('migration_driver_pipeline_unavailable')
                jobs[row['job']] = dict(binding=identity,kind=kind,stage=row['stage'],
                    outboxId=data.get('outboxId'),failureOutboxId=data.get('failureOutboxId'),
                    failureSha256=p.digest(data['failure']) if data.get('failure') is not None else None)
            return {'jobs':jobs,'sha256':p.digest(jobs)}
        finally:
            db.close()

    def __init__(self, worker, ledger, outbox, broker, coordinator_origin, worker_bearer,
                 *, state_path, ssl_context, output_budget, transfer_ttl, safety_floor,
                 failure_outbox, worker_instance_id, cleanup_executor=None, engine_cleanup=None,
                 allowed_environments=('production', 'staging'), clock=time.time,
                 execution_router=None, provenance_client=None, state_owner=None,combined_policy=None,combined_runtime_probe=None):
        self._combined_policy=combined_policy;self._combined_policy_initial=combined_policy;self.combined_preparation=None;self._combined_preparation_initial=None;self._active_step=None
        if combined_policy is not None:
            from .reference_combined_preparation import CombinedPreparationPolicy
            from ops.reference_worker_daemon import DaemonStateLock
            if type(combined_policy) is not CombinedPreparationPolicy or type(state_owner) is not DaemonStateLock or execution_router is not None or provenance_client is not None:raise ValueError('separate owned combined preparation profile required')
        if state_owner is not None:
            state_owner.check(Path(state_path).absolute().parent)
        self._state_owner=state_owner
        if not isinstance(worker.engine, NativeEngine) or not isinstance(broker, BrokerClient):
            raise ValueError('actual native engine and broker client required')
        if worker.ledger is not ledger or worker.outbox is not outbox or not isinstance(failure_outbox, FailureOutbox):
            raise ValueError('driver durable components differ')
        if engine_cleanup is not None and (not isinstance(engine_cleanup, EngineCacheCleanup)
                                           or engine_cleanup.worker is not worker):
            raise ValueError('engine cleanup ownership differs')
        if execution_router is not None and (not isinstance(execution_router, ExecutionRouter)
                or execution_router.odm_worker is not worker or execution_router.failure_outbox is not failure_outbox):
            raise ValueError('execution router ownership differs')
        if provenance_client is not None:
            from .reference_provenance import LidarProvenanceClient
            tree = execution_router.treedar_worker if execution_router is not None else None
            if (not isinstance(provenance_client, LidarProvenanceClient) or provenance_client.broker is not broker
                    or provenance_client.origin != broker.origin or tree is None
                    or tree.controller.verifier != provenance_client.verify):
                raise ValueError('provenance client execution binding differs')
        self.execution_router, self.provenance_client = execution_router, provenance_client
        for value in (output_budget, transfer_ttl):
            p._integer(value, 'driver budget')
        if transfer_ttl > 86400 or type(safety_floor) is not int or safety_floor < 0 or ledger.floor != safety_floor:
            raise ValueError('scratch safety floor or transfer TTL differs')
        if type(allowed_environments) not in (tuple, list) or not allowed_environments:
            raise ValueError('configured worker environments required')
        for environment in allowed_environments:
            p._environment(environment)
        p._slug(worker_instance_id, 'worker instance ID')
        self.output_budget, self.transfer_ttl = output_budget, transfer_ttl
        self.failure_outbox, self.cleanup_executor = failure_outbox, cleanup_executor
        self.engine_cleanup = engine_cleanup
        self.allowed_environments = frozenset(allowed_environments)
        self.worker_instance_id = worker_instance_id
        self._option_definitions = {}
        node_id, worker_id = worker.node_id, worker.worker_id
        parsed = urlsplit(coordinator_origin)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in ('', '/') or parsed.query
                or parsed.fragment or parsed.port == 0 or '\\' in coordinator_origin
                or any(ord(char) <= 32 for char in coordinator_origin)):
            raise ValueError('configured HTTPS coordinator origin required')
        if (not isinstance(ssl_context, ssl.SSLContext) or not ssl_context.check_hostname
                or ssl_context.verify_mode != ssl.CERT_REQUIRED):
            raise ValueError('verified worker TLS context required')
        if (type(worker_bearer) is not str or not 16 <= len(worker_bearer) <= 4096
                or not all(0x21 <= ord(char) <= 0x7e for char in worker_bearer)):
            raise ValueError('invalid private worker credential')
        self.worker, self.ledger, self.outbox, self.broker = worker, ledger, outbox, broker
        self.origin, self._bearer = coordinator_origin.rstrip('/'), worker_bearer
        self.node_id, self.worker_id, self.clock = node_id, worker_id, clock
        self._lock = threading.Lock()
        self.path = Path(state_path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(path.is_symlink() for path in (self.path, *self.path.parents)):
            raise ValueError('private worker journal required')
        if os.name != 'nt':
            parent = self.path.parent.stat()
            if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
                raise ValueError('private owned worker journal parent required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        try:
            observed, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('owned regular worker journal required')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        self._open = build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=ssl_context)).open
        self._initial_open=self._open;self._ssl_context=ssl_context
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS driver_jobs (
              created INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT UNIQUE NOT NULL,
              claim_request TEXT UNIQUE NOT NULL, stage TEXT NOT NULL, data TEXT NOT NULL)''')
            db.execute('CREATE TABLE IF NOT EXISTS readiness_tasks (uuid TEXT PRIMARY KEY)')
            if 'background_checked' not in {row['name'] for row in db.execute('PRAGMA table_info(driver_jobs)')}:
                db.execute('ALTER TABLE driver_jobs ADD COLUMN background_checked REAL NOT NULL DEFAULT 0')
            db.execute('CREATE TABLE IF NOT EXISTS driver_binding (singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS driver_engine_pin (singleton INTEGER PRIMARY KEY,pin TEXT NOT NULL)')
            settings = dict(origin=self.origin, brokerOrigin=broker.origin, nodeId=node_id, workerId=worker_id,
                workerInstanceId=worker_instance_id, authorityId=worker.authority_id, engineOrigin=worker.engine.origin,
                engineId=worker.engine_id, gateOrigin=worker.origin, gateGeneration=worker.gate_generation,
                optionPolicy='discovered-numeric-bool-v1',
                outputBudget=output_budget, transferTtl=transfer_ttl, safetyFloor=safety_floor,
                environments=sorted(self.allowed_environments))
            if execution_router is not None:
                settings['executionRouting'] = dict(mode='per-job-v1', path=str(execution_router.path),
                    provenancePath=str(provenance_client.path) if provenance_client is not None else None)
            if combined_policy is not None:
                from dataclasses import asdict
                settings['combinedPreparation']=dict(mode='combined-prepare-only-v1',policy=asdict(combined_policy))
            binding = p.canonical_json(settings).decode()
            old = db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            if execution_router is not None and old is None and db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone():
                raise ValueError('unbound legacy driver claims require reconciliation')
            if old and old[0] != binding:
                previous = json.loads(old[0])
                if combined_policy is not None and 'combinedPreparation' not in previous and previous=={key:value for key,value in settings.items() if key!='combinedPreparation'} and not db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone():
                    db.execute('UPDATE driver_binding SET binding=? WHERE singleton=1',(binding,))
                elif (execution_router is None or 'executionRouting' in previous
                        or previous != {key:value for key,value in settings.items() if key != 'executionRouting'}
                        or db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone()):
                    raise ValueError('worker driver ownership changed')
                else:db.execute('UPDATE driver_binding SET binding=? WHERE singleton=1', (binding,))
            if execution_router is not None:
                # A route must precede any engine intent. Enabling routing may
                # not silently adopt an existing legacy worker execution.
                adapters = [worker] + ([execution_router.treedar_worker] if execution_router.treedar_worker is not None else [])
                for adapter in adapters:
                    sources = [(adapter, 'executions')]
                    if adapter is not worker:
                        sources.append((adapter.controller, 'treedar_executions'))
                    for source, table in sources:
                        with source._db() as journal:
                            ids = [row[0] for row in journal.execute('SELECT job FROM '+table)]
                        for job_id in ids:
                            execution_router.kind_for(job_id)
            db.execute('INSERT OR IGNORE INTO driver_binding VALUES(1,?)', (binding,))
            old_pin = db.execute('SELECT pin FROM driver_engine_pin WHERE singleton=1').fetchone()
            if old_pin and old_pin[0] != worker.version:
                active = db.execute("""SELECT 1 FROM driver_jobs WHERE stage NOT IN
                  ('completed','awaiting_acceptance','awaiting_failure_acceptance') LIMIT 1""").fetchone()
                with NodeGate(worker.engine_id) as gate:
                    gate_empty = gate.snapshot()['reservation'] is None
                if active or not gate_empty:
                    raise ValueError('engine pin change requires reconciled idle execution')
                db.execute('UPDATE driver_engine_pin SET pin=? WHERE singleton=1', (worker.version,))
            db.execute('INSERT OR IGNORE INTO driver_engine_pin VALUES(1,?)', (worker.version,))
        self.sender = ReferenceSender(outbox, self.origin, bearer_token=worker_bearer,
            claim_binding_provider=self._claim_binding, ssl_context=ssl_context, timeout=10)
        if combined_policy is not None:
            from .reference_combined_preparation import CombinedParentPreparation
            self.combined_preparation=CombinedParentPreparation(self,combined_policy,runtime_probe=combined_runtime_probe)
            self._combined_preparation_initial=self.combined_preparation

    def _storage(self):
        if self._combined_policy is not self._combined_policy_initial:raise DriverHeld('combined_profile_identity_changed')
        if self._combined_policy is None:return
        maximum=self._combined_policy.driver_page_cap*4096
        if self.path.exists() and self.path.stat().st_size>maximum:raise DriverHeld('combined_driver_storage_cap')
        if any(os.path.lexists(str(self.path)+suffix) for suffix in ('-wal','-shm')):raise DriverHeld('combined_driver_rollback_required')
        rollback=Path(str(self.path)+'-journal')
        if os.path.lexists(rollback):
            info=rollback.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>2*maximum:raise DriverHeld('combined_driver_rollback_cap')

    @contextmanager
    def _db(self):
        self._storage()
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            if self._combined_policy is not None:
                cap=self._combined_policy.driver_page_cap
                if db.execute('PRAGMA page_size').fetchone()[0]!=4096 or db.execute('PRAGMA page_count').fetchone()[0]>cap:raise DriverHeld('combined_driver_storage_format')
                if db.execute('PRAGMA max_page_count='+str(cap)).fetchone()[0]!=cap:raise DriverHeld('combined_driver_storage_cap')
            with db:
                db.execute('BEGIN IMMEDIATE')
                yield db
                self._storage()
        finally:
            db.close()

    def _save(self, job_id, stage, data):
        with self._db() as db:
            db.execute('UPDATE driver_jobs SET stage=?,data=? WHERE job=?',
                       (stage, p.canonical_json(data).decode(), job_id))

    def _request(self, method, suffix, payload=None, *, claim_token=None):
        url = self.origin + PREFIX + suffix
        headers = {'Authorization': 'Bearer ' + self._bearer, 'Accept': 'application/json',
                   'Accept-Encoding': 'identity'}
        if claim_token is not None:
            headers['X-Claim-Token'] = claim_token
        body = p.canonical_json(payload) if payload is not None else None
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = Request(url, data=body, method=method, headers=headers)
        try:
            response = self._open(request, timeout=10)
        except HTTPError as error:
            status = error.code
            error.close()
            if status in (400, 401, 403, 404):
                raise DriverHeld('control_request_rejected') from None
            if status == 409:
                return status, None
            raise DriverWait('coordinator_unavailable') from None
        except Exception:
            raise DriverWait('coordinator_unavailable') from None
        try:
            if (response.status != 200 or response.geturl() != url
                    or response.headers.get('Content-Type', '').split(';')[0] != 'application/json'
                    or response.headers.get('Content-Encoding', 'identity') != 'identity'):
                raise DriverWait('invalid_control_response')
            raw = response.read(p.MAX_BODY_BYTES + 1)
            if len(raw) > p.MAX_BODY_BYTES:
                raise DriverWait('control_response_too_large')
            expected = response.headers.get('Content-Length')
            if expected is not None and (not expected.isascii() or not expected.isdecimal() or int(expected) != len(raw)):
                raise DriverWait('truncated_control_response')
            payload = p.decode_submission(raw)
            if type(payload) is not dict:
                raise DriverWait('invalid_control_response')
            return 200, payload
        except DriverWait:
            raise
        except Exception:
            raise DriverWait('control_response_unavailable') from None
        finally:
            response.close()

    def _validate_job(self, job):
        submission = job['submission']
        raw = {key: submission[key] for key in ('schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        kind = self._pipeline_kind(submission)
        checked = p.validate_submission(raw, environment=job['environment'],
            capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar') if self.execution_router or self._combined_policy else ('odm',),
            allowed_odm_options=self.worker.allowed_options)
        if (job['environment'] not in self.allowed_environments or job['nodeId'] != self.node_id
                or p.canonical_json(checked) != p.canonical_json(submission)
                or checked['manifestSha256'] != job['manifestSha256']
                or checked['pipelineDigest'] != job['pipelineDigest']
                or (kind == 'odm' and any(obj['kind'] != 'image' for obj in submission['manifest']['objects']))):
            raise DriverHeld('unsupported_worker_submission')
        for name, value in raw['pipeline']['odm']['options'].items():
            definition = self._option_definitions.get(name)
            if definition is None:
                raise DriverHeld('engine_option_no_longer_available')
            kind = definition['type']
            valid = ((kind in ('bool', 'boolean') and type(value) is bool)
                     or (kind in ('int', 'integer') and type(value) is int and abs(value) <= p.MAX_SAFE_INTEGER)
                     or (kind in ('float', 'double', 'number') and type(value) in (int, float)
                         and abs(value) <= p.MAX_SAFE_INTEGER and math.isfinite(value)))
            if not valid:
                raise DriverHeld('engine_option_type_mismatch')
        if self._combined_policy is not None and self._pipeline_kind(submission)!='combined':raise DriverHeld('combined_prepare_profile_only')
        if self._combined_policy is not None:
            from .reference_pipeline import _submission
            _submission(submission,job['environment'])

    @staticmethod
    def _pipeline_kind(submission):
        odm, tree = (submission['pipeline'][key]['requested'] for key in ('odm', 'treedar'))
        if type(odm) is not bool or type(tree) is not bool or not (odm or tree):
            raise DriverHeld('unsupported_execution_pipeline')
        return 'combined' if odm and tree else 'odm' if odm else 'treedar'

    def _advice(self, head, hint):
        fields = ('jobId', 'generation', 'environment', 'manifestSha256', 'pipelineDigest')
        if type(hint) is not dict or set(hint) != {'kind', *fields}:
            raise DriverHeld('execution_advice_required')
        if any(hint[key] != head[key] for key in fields):
            raise DriverHeld('execution_advice_binding_changed')
        p._integer(hint['generation'], 'generation')
        for key in ('manifestSha256', 'pipelineDigest'):
            p._hash(hint[key], key)
        kind = hint['kind']
        if self.combined_preparation is not None:
            if kind=='combined':return hint
            raise DriverHeld('combined_prepare_profile_only')
        if kind not in ('odm', 'treedar'):
            raise DriverHeld('unsupported_execution_pipeline')
        if kind == 'treedar' and (self.execution_router.treedar_worker is None or self.provenance_client is None):
            raise DriverHeld('treedar_execution_not_configured')
        return hint

    def _adapter(self, job_id, *, for_execution=False):
        return self.execution_router.worker_for(job_id, for_execution=for_execution) if self.execution_router else self.worker

    def _kind(self, job_id):
        if self.combined_preparation is not None and self.combined_preparation.is_combined(job_id):return 'combined'
        return self.execution_router.kind_for(job_id) if self.execution_router else 'odm'

    def _apply_acceptance(self, job_id, relayed, data):
        # Only an explicit false relay keeps legacy metadata completion. A
        # missing or malformed policy, or one this driver already saw required,
        # waits for locally verified signed coverage; the requirement is sticky.
        required = data.get('viewerCopyRequired') is True or relayed.get('viewerCopyRequired') is not False
        if required and data.get('viewerCopyRequired') is not True:
            data['viewerCopyRequired'] = True
            self._save(job_id, 'awaiting_acceptance', data)
        source = self.execution_router if self.execution_router is not None else self.worker
        return source.apply_viewer_acceptance(job_id, relayed['receipts'], require_verified_copy=required)

    def _bind_claim(self, claim, data):
        self._validate_job(claim)
        if self._pipeline_kind(claim['submission'])=='combined' and self.combined_preparation is not None:
            self._advice(claim,data.get('execution'));return
        if self.execution_router is not None:
            hint = self._advice(claim, data.get('execution'))
            if hint['kind'] != self._pipeline_kind(claim['submission']):
                raise DriverHeld('execution_advice_pipeline_changed')
            self.execution_router.bind(claim)

    def _fetch_provenance(self, claim, grant):
        try:
            client = self.provenance_client
            tree = self.execution_router.treedar_worker
            if (client is None or client.broker is not self.broker or client.origin != self.broker.origin
                    or tree.controller.verifier != client.verify):
                raise ValueError('provenance binding changed')
            receipt = client.fetch(claim, grant)
            if client.verify(receipt) is not True:
                raise ValueError('provenance verification unavailable')
            return receipt
        except Exception:
            raise DriverHeld('lidar_provenance_unavailable') from None

    def _refresh_options(self):
        definitions = _validate_options(self.worker.engine.read('/options', limit=262144))
        if self.worker.engine.token and self.worker.engine.token in p.canonical_json(definitions).decode():
            raise DriverHeld('invalid_engine_option_catalog')
        selected = {}
        for option in definitions:
            name = option['name']
            if (re.fullmatch(r'[a-z][a-z0-9-]{0,79}', name)
                    and option.get('type') in ('bool','boolean','int','integer','float','double','number')
                    and not _UNSAFE.search(p.canonical_json(option).decode())):
                selected[name] = option
        self._option_definitions = selected
        self.worker.allowed_options = frozenset(selected)

    def readiness_report(self):
        """Local advisory only; the real scratch reservation is still required."""
        if not self._lock.acquire(blocking=False):
            return None
        try:
            if self._state_owner is not None:
                self._state_owner.check(self.path.parent)
            with self._db() as db:
                if db.execute("""SELECT 1 FROM driver_jobs WHERE stage NOT IN
                    ('completed','awaiting_acceptance','awaiting_failure_acceptance') LIMIT 1""").fetchone():
                    return None
            if not self._ready():
                return None
            info = self.worker.engine.read('/info')
            maximum = info.get('maxImages') if type(info) is dict else False
            if (type(info) is not dict or 'maxImages' not in info
                    or info.get('engineVersion') != self.worker.version
                    or type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0
                    or (maximum is not None and (type(maximum) is not int or maximum < 1))):
                return None
            return {'ready': True, 'taskQueueCount': 0, 'maxImages': maximum,
                    'availableScratchBytes': self.ledger.available_scratch(),
                    'outputBudgetBytes': self.output_budget}
        except Exception:
            return None
        finally:
            self._lock.release()

    def _ready(self):
        _, authority = self._request('GET', '/authority')
        if authority.get('authorityId') != self.worker.authority_id:
            raise DriverHeld('coordinator_authority_changed')
        with NodeGate(self.worker.engine_id) as gate:
            state = gate.snapshot()
            if (state['authorityId'], state['nodeId'], state['coordinatorOrigin'], state['generation']) != (
                    self.worker.authority_id, self.node_id, self.worker.origin.rstrip('/'), self.worker.gate_generation):
                raise DriverHeld('physical_gate_binding_changed')
            if state['reservation'] is not None:
                return False
        info = self.worker.engine.read('/info')
        tasks = self.worker.engine.read('/task/list')
        if (type(info) is not dict or info.get('engineVersion') != self.worker.version
                or type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0
                or type(tasks) is not list or len(tasks) > p.MAX_OBJECTS):
            return False
        self._refresh_options()
        identifiers = []
        for task in tasks:
            if type(task) is not dict:
                return False
            p._uuid(task.get('uuid'), 'engine task UUID')
            identifiers.append(task['uuid'])
        if len(set(identifiers)) != len(identifiers):
            return False
        with self._db() as db:
            known = {row[0] for row in db.execute('SELECT uuid FROM readiness_tasks')}
        pending = [identifier for identifier in identifiers if identifier not in known]
        # NodeODM /task/list supplies UUIDs, not terminal status. Spread unknown
        # retained task checks across bounded polls; /info remains fresh each time.
        for identifier in pending[:4]:
            task = self.worker.engine.read('/task/' + identifier + '/info')
            if (type(task) is not dict or task.get('uuid') != identifier or
                    type(task.get('status')) is not dict or type(task['status'].get('code')) is not int
                    or task['status']['code'] not in (30, 40, 50)):
                return False
            with self._db() as db:
                db.execute('INSERT OR IGNORE INTO readiness_tasks VALUES(?)', (identifier,))
        return len(pending) <= 4

    def _claim_binding(self, binding):
        with self._db() as db:
            row = db.execute('SELECT * FROM driver_jobs WHERE job=?', (binding['jobId'],)).fetchone()
        if row is None:
            raise DriverHeld('unknown_worker_claim')
        data = json.loads(row['data'])
        job = data['claim']
        if tuple(binding[key] for key in ('generation', 'environment', 'inputManifestSha256', 'pipelineDigest')) != (
                job['generation'], job['environment'], job['manifestSha256'], job['pipelineDigest']):
            raise DriverHeld('worker_claim_binding_changed')
        return {'claimRequestId': row['claim_request'], 'claimToken': job['claimToken']}

    def step(self):
        """Perform one bounded stage; never return private metadata or paths."""
        if self._state_owner is not None:
            try:self._state_owner.check(self.path.parent)
            except Exception:return {'state':'held','reason':'worker_state_owner_unavailable'}
        if not self._lock.acquire(blocking=False):
            return {'state': 'busy'}
        try:
            if self._state_owner is not None:
                try:self._state_owner.check(self.path.parent)
                except Exception:return {'state':'held','reason':'worker_state_owner_unavailable'}
            self._active_step=DriverStepLease(self)
            self._hold_legacy_guarded_failures()
            if self._combined_policy_initial is not None:
                if self.combined_preparation is not self._combined_preparation_initial or self._combined_policy is not self._combined_policy_initial:raise DriverHeld('combined_profile_identity_changed')
                self.combined_preparation.before_step(self._active_step)
                background={'state':'background_disabled_in_preparation_profile'}
            else:
                try:
                    background = self._background()
                except Exception:
                    background = {'state': 'background_retry'}
            result = self._step()
            return background if result['state'] == 'idle' and background['state'] in ('completed', 'awaiting_acceptance') else result
        except DriverHeld as error:
            return {'state': 'held', 'reason': str(error)}
        except DriverWait as error:
            return {'state': 'retry_wait', 'reason': str(error)}
        except (RouteConflict, TreeDarHeld):
            return {'state': 'held', 'reason': 'execution_reconciliation_required'}
        except Exception as error:
            if self.combined_preparation is not None:
                from .reference_combined_preparation import CombinedPreparationHeld
                if isinstance(error,CombinedPreparationHeld):return {'state':'held','reason':str(error)}
            return {'state': 'retry_wait', 'reason': 'driver_step_unavailable'}
        finally:
            self._active_step=None
            self._lock.release()

    def _hold_legacy_guarded_failures(self):
        # Older builds could progress a guarded failure after marking scratch
        # terminal without physical-stop evidence. Fence those rows before
        # background acceptance, generic cleanup, or another claim can run.
        with self._db() as db:
            rows = db.execute("""SELECT job, stage, data FROM driver_jobs WHERE stage IN
                ('failure_delivering','awaiting_failure_acceptance','completed')""").fetchall()
        for row in rows:
            if self._kind(row['job']) != 'odm':
                continue
            execution = self.worker._row(row['job'])
            if not execution['host_guarded']:
                continue
            # The stage itself is proof this is a failure flow even if its
            # payload is absent or damaged; do not run background first.
            if row['stage'] != 'completed':
                self._hold_guarded_failure(execution)
                raise DriverHeld('guarded_terminal_stop_unverified')
            data = json.loads(row['data'])
            # A completed row is safe to pass only when the worker explicitly
            # records successful delivery and no failure payload remains.
            if execution['phase'] != 'delivered' or data.get('failure') is not None:
                self._hold_guarded_failure(execution)
                raise DriverHeld('guarded_terminal_stop_unverified')

    def _hold_guarded_failure(self, execution):
        identity = json.loads(execution['identity'])
        job = identity['job']
        self.ledger.hold_guarded_failure_cleanup(identity['reservation'],
            job_id=job['jobId'], generation=job['generation'])

    def _background(self):
        """One retained acknowledgement plus at most one separately fenced cleanup."""
        with self._db() as db:
            row = db.execute("""SELECT * FROM driver_jobs WHERE stage IN
              ('awaiting_acceptance','awaiting_failure_acceptance') ORDER BY background_checked,created LIMIT 1""").fetchone()
            if row is not None:
                db.execute('UPDATE driver_jobs SET background_checked=? WHERE job=?', (self.clock(), row['job']))
        observed = {'state': 'background_idle'}
        if row is not None:
            data = json.loads(row['data'])
            claim = data['claim']
            kind = 'acceptance' if row['stage'] == 'awaiting_acceptance' else 'terminal-acceptance'
            status, relayed = self._request('GET', '/jobs/' + row['job'] + '/' + kind, claim_token=claim['claimToken'])
            if kind == 'acceptance':
                # Keep a still-pending success observable when no other work runs.
                observed = {'state': 'awaiting_acceptance', 'jobId': row['job']}
            if status != 409:
                if kind == 'acceptance':
                    accepted = self._apply_acceptance(row['job'], relayed, data)['fullyAccepted']
                else:
                    receipt = relayed.get('receipt')
                    accepted = receipt is not None
                    if accepted:
                        data['terminalAcceptance'] = validate_terminal_acceptance(receipt, failure=data['failure'])
                if accepted:
                    if kind == 'acceptance' and self.engine_cleanup is not None and self._kind(row['job']) == 'odm':
                        # Persist exact success proof even if grace, pins or a
                        # newer task defer removal. Failure ACKs accept no data.
                        self.engine_cleanup.remove(row['job'], viewer_receipts=relayed['receipts'])
                    self._save(row['job'], 'completed', data)
                    observed = {'state': 'completed', 'jobId': row['job']}
        if self.engine_cleanup is not None:
            self.engine_cleanup.reconcile_one()
        if self.cleanup_executor is not None:
            for item in self.ledger.cleanup_candidates(max_count=1, max_bytes=self.output_budget):
                self.cleanup_executor.execute(item['reservation'], item['id'], generation=item['generation'],
                    sha256=item['sha'], byte_size=item['bytes'], relative_path=item['path'])
        return observed

    def _seal_failure(self, job_id, data, code):
        # A NodeODM terminal status alone does not prove that the physical
        # process stopped, especially after a cancellation. Guarded work must
        # retain its journal and scratch reservation until independent stop
        # evidence and a durable failure handoff are implemented.
        if self.worker._row(job_id)['host_guarded']:
            self._hold_guarded_failure(self.worker._row(job_id))
            raise DriverHeld('guarded_terminal_stop_unverified')
        claim = data['claim']
        failure = make_failure(job_id=job_id, generation=claim['generation'], environment=claim['environment'],
            input_manifest_sha256=claim['manifestSha256'], pipeline_digest=claim['pipelineDigest'],
            terminal_state='failed' if code == 30 else 'cancelled',
            error_code='engine_failed' if code == 30 else 'engine_cancelled')
        outbox_id = self.failure_outbox.persist(failure, engine_terminal=True)
        data.update(failure=failure, failureOutboxId=outbox_id)
        # The failure outbox precedes ledger terminal state and physical completion.
        # A crash before the driver checkpoint safely re-derives this exact record.
        self._save(job_id, 'failure_delivering', data)
        self._prepare_failure(job_id, data)

    @staticmethod
    def _failure_completion(failure):
        return {'status': 'failed', 'result': {key: failure[key] for key in ('failureSha256', 'terminalState', 'errorCode')}}

    def _prepare_failure(self, job_id, data):
        # Also fence records created by an older version before it can mark
        # scratch terminal or release a physical reservation.
        if self.worker._row(job_id)['host_guarded']:
            self._hold_guarded_failure(self.worker._row(job_id))
            raise DriverHeld('guarded_terminal_stop_unverified')
        self.ledger.set_engine_state(data['reservation']['reservationId'], 'terminal', terminal_attested=True)
        local = self.worker._row(job_id)
        if local['phase'] == 'failure_delivered' and self.failure_outbox.get(data['failureOutboxId'])['state'] == 'acknowledged':
            return
        with NodeGate(self.worker.engine_id) as gate:
            if self._gate_ack_matches(gate.snapshot(), local['receipt'], job_id, self._failure_completion(data['failure'])):
                return
            if gate.snapshot()['reservation'] is None:
                if self.failure_outbox.get(data['failureOutboxId'])['state'] != 'acknowledged':
                    raise DriverHeld('terminal_gate_ownership_unavailable')
                gate.acknowledge(self.worker._row(job_id)['receipt'], dict(id=job_id,
                    workerId=self.worker_id, nodeId=self.node_id, authorityId=self.worker.authority_id,
                    **self._failure_completion(data['failure'])))
                return
            gate.prepare_completion(self.worker._row(job_id)['receipt'], self._failure_completion(data['failure']))

    def _gate_ack_matches(self, state, receipt, job_id, completion):
        expected = dict(receiptId=receipt, generation=self.worker.gate_generation, kind='completion',
            job=dict(id=job_id, workerId=self.worker_id, nodeId=self.node_id, authorityId=self.worker.authority_id),
            completionDigest=p.digest(completion))
        return state.get('lastAcknowledgment') == expected

    def _deliver_failure(self, job_id, request_id, data):
        tree = self._kind(job_id) == 'treedar'
        adapter = self._adapter(job_id)
        if not tree:
            self._prepare_failure(job_id, data)
        completion = self.failure_outbox.get(data['failureOutboxId'])
        if completion['state'] != 'acknowledged':
            delivery = self.failure_outbox.claim_next()
            if delivery is None:
                return {'state': 'failure_delivering', 'jobId': job_id}
            try:
                failure = delivery['payload']['failure']
                binding = self._claim_binding(failure)
                status, receipt = self._request('POST', '/terminal-outcomes', dict(**binding,
                    delivery={key: delivery[key] for key in ('outboxId','payloadSha256','payload')}))
                if status != 200:
                    raise DriverWait('terminal_receipt_unavailable')
                self.failure_outbox.acknowledge(delivery['outboxId'], receipt)
            except Exception:
                try:
                    self.failure_outbox.failed_attempt(delivery['outboxId'], delivery['claimToken'])
                except Exception:
                    pass
                return {'state': 'failure_delivering', 'jobId': job_id}
        # claim_next is global to the outbox. A successful attempt may have
        # acknowledged a different job; only this exact job can release its
        # physical gate or advance its driver state.
        completion = self.failure_outbox.get(data['failureOutboxId'])
        if completion['state'] != 'acknowledged':
            return {'state': 'failure_delivering', 'jobId': job_id}
        if tree:
            adapter.acknowledge_delivery(job_id)
            self._save(job_id, 'awaiting_failure_acceptance', data)
            return {'state': 'awaiting_failure_acceptance', 'jobId': job_id}
        local = self.worker._row(job_id)
        if local['phase'] != 'failure_delivered':
            with NodeGate(self.worker.engine_id) as gate:
                completion = self._failure_completion(data['failure'])
                ack = dict(id=job_id, workerId=self.worker_id, nodeId=self.node_id,
                    authorityId=self.worker.authority_id, **completion)
                if not self._gate_ack_matches(gate.snapshot(), local['receipt'], job_id, completion):
                    gate.acknowledge(local['receipt'], ack)
        self.worker._phase(job_id, 'failure_delivered')
        self._save(job_id, 'awaiting_failure_acceptance', data)
        return {'state': 'awaiting_failure_acceptance', 'jobId': job_id}

    def _step(self):
        with self._db() as db:
            row = db.execute("SELECT * FROM driver_jobs WHERE stage NOT IN ('completed','awaiting_acceptance','awaiting_failure_acceptance') ORDER BY created LIMIT 1").fetchone()
        if row is None:
            if not self._ready():
                return {'state': 'not_ready'}
            _, next_job = self._request('GET', '/next')
            head = next_job.get('job')
            if head is None:
                return {'state': 'idle'}
            if head['environment'] not in self.allowed_environments:
                raise DriverHeld('worker_environment_not_enabled')
            data = {}
            if self.execution_router is not None or self.combined_preparation is not None:
                data['execution'] = self._advice(head, next_job.get('execution'))
            job_id = head['jobId']
            p._slug(job_id, 'job ID')
            request_id = 'driver-' + secrets.token_hex(16)
            with self._db() as db:
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claim_pending',?)",
                           (job_id, request_id, p.canonical_json(data).decode()))
            return {'state': 'claim_pending', 'jobId': job_id}
        job_id, request_id, stage = row['job'], row['claim_request'], row['stage']
        data = json.loads(row['data'])
        if stage == 'claim_pending':
            if self.execution_router is not None or self.combined_preparation is not None:
                self._advice(data.get('execution', {}), data.get('execution'))
            if not self._ready():
                return {'state': 'not_ready', 'jobId': job_id}
            status, claim = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
            if status == 409:
                return {'state': 'claim_pending', 'jobId': job_id}
            if claim['status'] != 'running':
                raise DriverHeld('unstarted_claim_requires_reconciliation')
            self._bind_claim(claim, data)
            if self.combined_preparation is not None:
                if 'reservation' in data and 'originalCombinedClaim' not in data:raise DriverHeld('combined_claim_intent_binding_missing')
                original={key:claim[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest')}
                original['claimTokenSha256']=hashlib.sha256(claim['claimToken'].encode()).hexdigest()
                if 'originalCombinedClaim' in data and p.canonical_json(data['originalCombinedClaim'])!=p.canonical_json(original):raise DriverHeld('original_combined_claim_response_changed')
                data['originalCombinedClaim']=original
                if 'reservation' not in data:
                    data['reservation']=dict(reservationId='driver-'+secrets.token_hex(16),generation=claim['generation'],expiresAt=datetime.fromtimestamp(self.clock()+self.transfer_ttl,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z'),manifestSha256=claim['manifestSha256'],byteBudget=min(p.MAX_SAFE_INTEGER,3*claim['submission']['manifest']['totalBytes']),nonce=secrets.token_hex(16))
                    if len(p.canonical_json(data))>self._combined_policy.max_intent_bytes:raise DriverHeld('bounded_combined_claim_intent_required')
                    self._save(job_id,'claim_pending',data)
                self.ledger.reserve(job_id=job_id,generation=claim['generation'],reservation_id=data['reservation']['reservationId'],manifest_sha256=claim['manifestSha256'],input_bytes=claim['submission']['manifest']['totalBytes'],output_budget=self.output_budget,environment=claim['environment'],ttl_seconds=self.transfer_ttl,worker_ready=True)
                data['claim']=claim;self._save(job_id,'reservation_pending',data)
                return {'state':'reservation_pending','jobId':job_id}
            data['claim'] = claim
            self._save(job_id, 'claimed', data)
            return {'state': 'claimed', 'jobId': job_id}
        claim = data['claim']
        token = claim['claimToken']
        suffix = '/jobs/' + job_id
        if stage in ('claimed', 'reservation_pending'):
            if stage == 'claimed':
                reservation = 'driver-' + secrets.token_hex(16)
                data['reservation'] = dict(reservationId=reservation, generation=claim['generation'],
                    expiresAt=datetime.fromtimestamp(self.clock() + self.transfer_ttl, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                    manifestSha256=claim['manifestSha256'], byteBudget=min(p.MAX_SAFE_INTEGER, 3 * claim['submission']['manifest']['totalBytes']), nonce=secrets.token_hex(16))
                self._save(job_id, 'reservation_pending', data)
            request = data['reservation']
            self.ledger.reserve(job_id=job_id, generation=claim['generation'], reservation_id=request['reservationId'],
                manifest_sha256=claim['manifestSha256'], input_bytes=claim['submission']['manifest']['totalBytes'], output_budget=self.output_budget,
                environment=claim['environment'], ttl_seconds=self.transfer_ttl, worker_ready=True)
            status, public = self._request('POST', suffix + '/reservation', {'claimToken': token, 'transferRequest': request})
            if status == 409:
                return {'state': 'reservation_pending', 'jobId': job_id}
            claim.update(public)
            self._save(job_id, 'awaiting_grant', data)
            return {'state': 'awaiting_grant', 'jobId': job_id}
        if stage == 'awaiting_grant':
            status, grant = self._request('GET', suffix + '/transfer-grant', claim_token=token)
            if status == 409:
                return {'state': 'awaiting_grant', 'jobId': job_id}
            validated = validate_grant(grant, job=claim, clock=self.clock)
            if self._kind(job_id) == 'treedar':
                data['provenance'] = self._fetch_provenance(claim, validated)
            data['grant'] = grant
            self._save(job_id, 'fetching_pending', data)
            return {'state': 'fetching_pending', 'jobId': job_id}
        if stage in ('fetching_pending', 'processing_pending'):
            if stage == 'processing_pending' and self._kind(job_id) == 'odm':
                # Input transfer is complete, but engine start is not yet
                # acknowledged. Keep the coordinator phase at fetching.
                self._save(job_id, 'execution_pending', data)
                return {'state': 'execution_pending', 'jobId': job_id}
            phase = 'fetching' if stage == 'fetching_pending' else 'processing'
            status, public = self._request('POST', suffix + '/phase', {'claimToken': token, 'phase': phase})
            if status == 409:
                return {'state': stage, 'jobId': job_id}
            claim.update(public)
            self._save(job_id, 'fetching' if phase == 'fetching' else 'execution_pending', data)
            return {'state': 'fetching' if phase == 'fetching' else 'execution_pending', 'jobId': job_id}
        if stage == 'fetching':
            grant = validate_grant(data['grant'], job=claim, clock=self.clock)
            reservation = data['reservation']['reservationId']
            directory = self.ledger.ensure_job_directory(reservation)
            paths = dict(data.get('inputPaths', {}))
            for obj in claim['submission']['manifest']['objects']:
                if obj['id'] in paths:
                    continue
                relative = 'jobs/' + reservation + '/' + obj['id'] + '.input'
                self.ledger._owned_path(reservation, relative)
                target = self.ledger.root / relative
                if os.path.lexists(target):
                    self.worker._file(reservation, relative, obj['byteSize'], obj['sha256'])
                else:
                    attempts = data.setdefault('fetchAttempts', {}).get(obj['id'], 0)
                    if attempts >= 3:
                        raise DriverHeld('worker_fetch_attempt_budget_exhausted')
                    data['fetchAttempts'][obj['id']] = attempts + 1
                    self._save(job_id, stage, data)
                    try:
                        self.broker.download_object(grant, obj['id'], target)
                    except TransferResponseUnavailable:
                        # Attempt and budget remain consumed/uncertain; retain the
                        # existing cap and grant semantics. No automatic renewal.
                        raise DriverWait('broker_response_unavailable') from None
                paths[obj['id']] = relative
                data['inputPaths'] = paths
                self.ledger.record_allocated(reservation, sum(item['byteSize'] for item in claim['submission']['manifest']['objects'] if item['id'] in paths))
                self._save(job_id, 'fetching', data)
                return {'state': 'fetching', 'jobId': job_id}
            self.ledger.record_allocated(reservation, claim['submission']['manifest']['totalBytes'])
            data['inputPaths'] = paths
            self._save(job_id, 'processing_pending', data)
            return {'state': 'processing_pending', 'jobId': job_id}
        if stage in ('execution_pending','combined_prepared_held') and self._kind(job_id)=='combined':
            result=self.combined_preparation.prepare(job_id,lease=self._active_step)
            self._save(job_id,'combined_prepared_held',data);return result
        if stage == 'execution_pending':
            adapter = self._adapter(job_id, for_execution=True)
            exists = False
            try:
                # TreeDar's delivery projection exists only after terminal
                # proof. Its controller intent must fence a second launch.
                if self._kind(job_id) == 'treedar':
                    adapter.controller._row(job_id)
                else:
                    adapter._row(job_id)
                exists = True
            except (WorkerHeld, TreeDarHeld):
                pass
            if not exists:
                status, fresh = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
                if status == 409 or fresh['status'] != 'running':
                    raise DriverHeld('unstarted_claim_requires_reconciliation')
                self._refresh_options()
                self._bind_claim(fresh, data)
                data['claim'] = fresh
                if self._kind(job_id) == 'treedar':
                    try:
                        grant = validate_grant(data['grant'], job=fresh, clock=self.clock)
                    except ValueError:
                        raise DriverHeld('lidar_transfer_grant_unavailable') from None
                    data['provenance'] = self._fetch_provenance(fresh, grant)
                self._save(job_id, 'execution_pending', data)
                arguments = dict(reservation_id=data['reservation']['reservationId'], input_paths=data['inputPaths'])
                if self._kind(job_id) == 'treedar':
                    arguments['provenance'] = data['provenance']
                adapter.start(fresh, **arguments)
            if self._kind(job_id) == 'odm':
                # A durable local processing phase follows the engine commit
                # acknowledgment. Pending/uncertain commits remain unreported.
                if adapter._row(job_id)['phase'] != 'processing':
                    raise DriverHeld('engine_start_unconfirmed')
                observed = self.worker.engine.read('/task/' + job_id + '/info')
                if (type(observed) is not dict or observed.get('uuid') != job_id
                        or type(observed.get('status')) is not dict
                        or type(observed['status'].get('code')) is not int
                        or observed['status']['code'] not in (20, 30, 40, 50)):
                    return {'state': 'execution_pending', 'jobId': job_id}
                status, public = self._request('POST', suffix + '/phase',
                    {'claimToken': token, 'phase': 'processing'})
                if status == 409:
                    return {'state': 'execution_pending', 'jobId': job_id}
                claim.update(public)
            self._save(job_id, 'executing', data)
            return {'state': 'executing', 'jobId': job_id}
        if stage == 'executing':
            adapter = self._adapter(job_id, for_execution=True)
            tree = self._kind(job_id) == 'treedar'
            local = None if tree else adapter._row(job_id)
            if not tree and local['phase'] in ('commit_pending', 'processing'):
                terminal = self.worker.engine.read('/task/' + job_id + '/info')
                if (type(terminal) is not dict or terminal.get('uuid') != job_id
                        or type(terminal.get('status')) is not dict):
                    raise DriverWait('engine_terminal_identity_unavailable')
                code = terminal['status'].get('code')
                if type(code) is int and code in (30, 50):
                    self._seal_failure(job_id, data, code)
                    return {'state': 'failure_delivering', 'jobId': job_id}
            try:
                outbox_id = adapter.poll(job_id)
            except WorkerHeld:
                if not tree and adapter._row(job_id)['phase'] in ('archive_pending', 'archive_open_pending'):
                    return {'state': 'retry_wait', 'jobId': job_id}
                raise DriverHeld('engine_reconciliation_required') from None
            if outbox_id is None:
                return {'state': 'executing', 'jobId': job_id}
            if tree and adapter.completion(job_id)['kind'] == 'failure':
                completion = self.failure_outbox.get(outbox_id)
                data.update(failureOutboxId=outbox_id, failure=completion['payload']['failure'])
                self._save(job_id, 'failure_delivering', data)
                return {'state': 'failure_delivering', 'jobId': job_id}
            data['outboxId'] = outbox_id
            self._save(job_id, 'delivering', data)
            return {'state': 'delivering', 'jobId': job_id, 'outboxId': outbox_id}
        if stage == 'delivering':
            adapter = self._adapter(job_id)
            completion = self.outbox.get(data['outboxId'])
            if completion['state'] != 'acknowledged':
                sent = self.sender.send_next()
                if sent['state'] != 'acknowledged':
                    return {'state': 'delivering', 'jobId': job_id, 'deliveryState': sent['state']}
            # A shared sender can acknowledge another retained result first.
            # Never treat that as this job's coordinator receipt.
            completion = self.outbox.get(data['outboxId'])
            if completion['state'] != 'acknowledged':
                return {'state': 'delivering', 'jobId': job_id, 'deliveryState': 'other_acknowledged'}
            if self._kind(job_id) == 'treedar':
                adapter.acknowledge_delivery(job_id)
                self._save(job_id, 'awaiting_acceptance', data)
                return {'state': 'awaiting_acceptance', 'jobId': job_id}
            local = self.worker._row(job_id)
            if local['phase'] != 'delivered':
                completion = dict(status='succeeded', result={'resultManifestSha256': json.loads(local['result'])['resultManifestSha256']})
                with NodeGate(self.worker.engine_id) as gate:
                    acknowledged = self._gate_ack_matches(gate.snapshot(), local['receipt'], job_id, completion)
                if acknowledged:
                    self.worker._phase(job_id, 'delivered')
                else:
                    self.worker.acknowledge_delivery(job_id)
            self._save(job_id, 'awaiting_acceptance', data)
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        if stage == 'failure_delivering':
            return self._deliver_failure(job_id, request_id, data)
        if stage == 'awaiting_acceptance':
            status, relayed = self._request('GET', suffix + '/acceptance', claim_token=token)
            if status == 409:
                return {'state': 'awaiting_acceptance', 'jobId': job_id}
            observed = self._apply_acceptance(job_id, relayed, data)
            if observed['fullyAccepted']:
                self._save(job_id, 'completed', data)
                return {'state': 'completed', 'jobId': job_id}
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        raise DriverHeld('unknown_worker_driver_stage')
