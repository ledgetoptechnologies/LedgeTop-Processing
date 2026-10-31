"""Durable worker driver over fixed authenticated HTTPS boundaries.

ODM is the default; an explicit router binds supported engines per claimed job.
Engine mutations remain physically fenced. Delivery, Viewer acceptance and
optional exact-object cleanup continue after slot release.
"""
from contextlib import contextmanager
from dataclasses import dataclass
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
from .reference_transfer import BrokerClient, validate_grant
from .viewer_service import NativeEngine
from .node_gate import NodeGate
from .reference_failure import FailureOutbox, make_failure
from .reference_terminal import validate_terminal_acceptance
from .reference_engine_cleanup import EngineCacheCleanup
from .options import _validate_options
from .reference_runtime import _UNSAFE
from .reference_execution import ExecutionRouter, RouteConflict
from .treedar_engine import TreeDarHeld
from .scratch_policy import ScratchLedger, PolicyConflict, INPUT_COPY_ACCOUNTING_VERSION


PREFIX = '/ltds/worker/v1'
_INDEXED_FIXED_CLAIM = ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')
_INDEXED_MUTABLE_CLAIM = ('phase','status','error','transferRequest','transferGrantAcknowledgement','resultManifestSha256','executionPhase')


def validate_indexed_fetch_projection(projection, *, reservation, identity):
    """Pure existing hot-fetch control guard, shared with pre-recovery census."""
    from .reference_indexed_fetch import require
    require(not set(projection)-{'claim','reservation','inputAccounting','indexedGrant','execution'}
            and projection.get('indexedGrant') is True
            and p.canonical_json(projection.get('reservation'))==p.canonical_json(reservation)
            and p.canonical_json(projection.get('inputAccounting'))==p.canonical_json(dict(schemaVersion=1,
                binding=identity,state='bound',next=0,byteCount=0)), 'indexed_progress_projection_changed')



class DriverHeld(ValueError):
    pass


class DriverWait(ValueError):
    pass


class DriverStepLease:
    def __init__(self,driver):self.driver=driver;self.pid=os.getpid();self.thread=threading.get_ident();self.lock=driver._lock
    def check(self,driver):
        if self.driver is not driver or self.pid!=os.getpid() or self.thread!=threading.get_ident() or getattr(driver,'_active_step',None) is not self or self.lock is not driver._lock or not self.lock.locked() or not driver._step_guard.locked() or driver._readiness_rpc is not None:raise DriverHeld('actual_driver_step_required')
        driver._state_owner.check(driver.path.parent)


@dataclass(frozen=True)
class CoordinatorReadinessLease:
    driver:object
    step:object
    phase:str
    job_id:str|None=None
    claim_request_id:str|None=None
    def __post_init__(self):
        if type(self.step) is not DriverStepLease or self.phase not in ('discovery','initial_claim'):raise DriverHeld('actual_readiness_rpc_required')
        self.step.check(self.driver)
        if self.phase=='initial_claim':
            p._slug(self.job_id,'readiness job');p._slug(self.claim_request_id,'readiness claim request')
        elif self.job_id is not None or self.claim_request_id is not None:raise DriverHeld('actual_readiness_rpc_required')
    def check_observer(self,driver):
        step=self.step
        if (self.driver is not driver or step.driver is not driver or step.pid!=os.getpid() or step.thread==threading.get_ident()
                or driver._active_step is not step or driver._readiness_rpc is not self
                or step.lock is not driver._lock or not driver._lock.locked() or not driver._step_guard.locked()):raise DriverHeld('actual_readiness_observer_required')
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
        from .reference_indexed_fetch import reject_indexed_footprint, IndexedFetchHeld
        try:reject_indexed_footprint(path)
        except IndexedFetchHeld:raise DriverHeld('indexed_migration_census_not_qualified') from None
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
                 execution_router=None, provenance_client=None, state_owner=None,combined_policy=None,combined_runtime_probe=None,
                 require_option_evidence=False, input_copy_accounting=False, indexed_fetch_context=None):
        from .reference_indexed_fetch import IndexedFetchContext
        if indexed_fetch_context is not None and (type(indexed_fetch_context) is not IndexedFetchContext
                or not input_copy_accounting or combined_policy is not None
                or execution_router is not None and execution_router.treedar_worker is not None):
            raise ValueError('separate fresh indexed ODM profile required')
        self.indexed_fetch_context=indexed_fetch_context
        self._indexed_context_initial=indexed_fetch_context
        self._indexed=None;self._indexed_session=None;self._indexed_progress_proof=None;self._indexed_background_due=0
        if type(input_copy_accounting) is not bool:
            raise ValueError('explicit driver input-copy profile required')
        if input_copy_accounting != getattr(ledger, 'input_copy_accounting', False):
            raise ValueError('driver and ledger input-copy profiles differ')
        if input_copy_accounting:
            if not isinstance(ledger, ScratchLedger):
                raise ValueError('actual input-copy ledger required')
            with ledger._transaction() as db:
                if ledger.validate_accounting(db).get('inputCopyAccountingVersion') != INPUT_COPY_ACCOUNTING_VERSION:
                    raise ValueError('input-copy ledger must be explicitly enabled first')
        self.input_copy_accounting = input_copy_accounting
        self._input_copy_accounting_initial = input_copy_accounting
        self._input_ledger_initial = ledger
        self._input_fetch_cache = None
        if type(require_option_evidence) is not bool:
            raise ValueError('explicit original option evidence profile required')
        if require_option_evidence and (combined_policy is not None or execution_router is not None and execution_router.treedar_worker is not None):
            raise ValueError('original option evidence profile is ODM-only')
        self.require_option_evidence = require_option_evidence
        self._require_option_evidence_initial = require_option_evidence
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
        self._step_guard=threading.Lock()
        self._readiness_rpc=None
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
        from .reference_indexed_fetch import IndexedFetchJournal, reject_indexed_footprint
        if indexed_fetch_context is None:
            reject_indexed_footprint(self.path)
        else:
            self._indexed=IndexedFetchJournal(self.path,indexed_fetch_context,state_owner,clock=clock)
            from .reference_indexed_fetch import IndexedCapacityProbe
            if (type(ledger.probe) is not IndexedCapacityProbe or ledger.probe.context!=indexed_fetch_context
                    or ledger.probe.owner is not state_owner or indexed_fetch_context.driver_path!=str(self.path)):
                raise ValueError('indexed capacity composition differs')
            context_binding=json.loads(indexed_fetch_context.binding_json)
            physical=context_binding['physical']
            if (any(physical.get(key)!=value for key,value in dict(nodeId=node_id,workerId=worker_id,
                    workerInstanceId=worker_instance_id,authorityId=worker.authority_id,engineId=worker.engine_id,
                    gateGeneration=worker.gate_generation).items())
                    or context_binding['stateRoot']!=str(self.path.parent)
                    or context_binding['scratchRoot']!=str(ledger.root)):
                raise ValueError('indexed physical context differs')
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
            if require_option_evidence:
                settings['optionEvidence'] = dict(schemaVersion=1)
            if input_copy_accounting:
                settings['inputCopyAccounting'] = dict(schemaVersion=1)
            if indexed_fetch_context is not None:
                from .reference_indexed_fetch import compiled_profile_descriptor
                settings['indexedFetchJournal']=compiled_profile_descriptor(indexed_fetch_context.policy.policy_id)
            if combined_policy is not None:
                from dataclasses import asdict
                settings['combinedPreparation']=dict(mode='combined-prepare-only-v1',policy=asdict(combined_policy))
            binding = p.canonical_json(settings).decode()
            old = db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            if input_copy_accounting and old is None and db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone():
                raise ValueError('unbound legacy input claims require reconciliation')
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
        if self._indexed is not None:self._indexed.driver_binding=binding
        self.sender = ReferenceSender(outbox, self.origin, bearer_token=worker_bearer,
            claim_binding_provider=self._claim_binding, ssl_context=ssl_context, timeout=10)
        if combined_policy is not None:
            from .reference_combined_preparation import CombinedParentPreparation
            self.combined_preparation=CombinedParentPreparation(self,combined_policy,runtime_probe=combined_runtime_probe)
            self._combined_preparation_initial=self.combined_preparation

    def _storage(self):
        if self.indexed_fetch_context is not self._indexed_context_initial:
            raise DriverHeld('indexed_profile_changed')
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
                if self._indexed is not None:self._indexed.check(db)
                yield db
                self._storage()
        finally:
            db.close()

    def _save(self, job_id, stage, data):
        with self._db() as db:
            if self._indexed is not None:
                data=self._indexed_projection(db,job_id,data)
            db.execute('UPDATE driver_jobs SET stage=?,data=? WHERE job=?',
                       (stage, p.canonical_json(data).decode(), job_id))

    def _request(self, method, suffix, payload=None, *, claim_token=None):
        if self.require_option_evidence is not self._require_option_evidence_initial:
            raise DriverHeld('original_option_evidence_profile_changed')
        url = self.origin + PREFIX + suffix
        headers = {'Authorization': 'Bearer ' + self._bearer, 'Accept': 'application/json',
                   'Accept-Encoding': 'identity'}
        if self.require_option_evidence and suffix in ('/next', '/claim'):
            headers['X-LTDS-Option-Evidence'] = '1'
        if claim_token is not None:
            headers['X-LTDS-Claim-Token' if suffix.endswith('/option-evidence') else 'X-Claim-Token'] = claim_token
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
            from .reference_option_evidence import MAX_EVIDENCE_BYTES
            limit = MAX_EVIDENCE_BYTES if suffix.endswith('/option-evidence') else p.MAX_BODY_BYTES
            raw = response.read(limit + 1)
            if len(raw) > limit:
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

    def _apply_acceptance(self, job_id, receipts):
        source = self.execution_router if self.execution_router is not None else self.worker
        return source.apply_viewer_acceptance(job_id, receipts)

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
            if self._indexed is not None:
                job=self._indexed.header(db,binding['jobId'])
                if tuple(binding[key] for key in ('generation','environment','inputManifestSha256','pipelineDigest')) != (
                        job['generation'],job['environment'],job['manifestSha256'],job['pipelineDigest']):
                    raise DriverHeld('worker_claim_binding_changed')
                return dict(claimRequestId=job['claimRequestId'],claimToken=job['claimToken'])
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
        if (self.input_copy_accounting is not self._input_copy_accounting_initial
                or self.ledger is not self._input_ledger_initial
                or getattr(self.ledger, 'input_copy_accounting', False) is not self._input_copy_accounting_initial):
            return {'state': 'held', 'reason': 'input_copy_profile_changed'}
        if self.indexed_fetch_context is not self._indexed_context_initial:
            return {'state':'held','reason':'indexed_profile_changed'}
        self._indexed_progress_proof=None
        if self.require_option_evidence is not self._require_option_evidence_initial:
            return {'state':'held','reason':'original_option_evidence_profile_changed'}
        if self._state_owner is not None:
            try:self._state_owner.check(self.path.parent)
            except Exception:return {'state':'held','reason':'worker_state_owner_unavailable'}
        if not self._step_guard.acquire(blocking=False):
            return {'state': 'busy'}
        if not self._lock.acquire(blocking=False):
            self._step_guard.release();return {'state':'busy'}
        try:
            if self._state_owner is not None:
                try:self._state_owner.check(self.path.parent)
                except Exception:return {'state':'held','reason':'worker_state_owner_unavailable'}
            self._active_step=DriverStepLease(self)
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
            return background if result['state'] == 'idle' and background['state'] == 'completed' else result
        except DriverHeld as error:
            return {'state': 'held', 'reason': str(error)}
        except DriverWait as error:
            return {'state': 'retry_wait', 'reason': str(error)}
        except (RouteConflict, TreeDarHeld):
            return {'state': 'held', 'reason': 'execution_reconciliation_required'}
        except Exception as error:
            from .reference_indexed_fetch import IndexedFetchHeld
            if isinstance(error,IndexedFetchHeld):return {'state':'held','reason':str(error)}
            from .reference_transfer import TransferError
            if self._indexed is not None and isinstance(error,TransferError):return {'state':'held','reason':'indexed_transfer_unavailable'}
            if self.combined_preparation is not None:
                from .reference_combined_preparation import CombinedPreparationHeld
                if isinstance(error,CombinedPreparationHeld):return {'state':'held','reason':str(error)}
            return {'state': 'retry_wait', 'reason': 'driver_step_unavailable'}
        finally:
            self._active_step=None
            self._lock.release()
            self._step_guard.release()

    @contextmanager
    def _readiness_window(self,phase,job_id=None,claim_request_id=None):
        if self._state_owner is None or self._combined_policy is not None:
            yield;return
        lease=CoordinatorReadinessLease(self,self._active_step,phase,job_id,claim_request_id)
        self._readiness_rpc=lease
        self._lock.release()
        try:yield
        finally:
            self._lock.acquire()
            self._readiness_rpc=None

    def _background(self):
        """One retained acknowledgement plus at most one separately fenced cleanup."""
        if self._indexed is not None and time.monotonic()<self._indexed_background_due:
            return {'state':'background_idle'}
        if self._indexed is not None:self._indexed_background_due=time.monotonic()+5
        with self._db() as db:
            fields='*' if self._indexed is None else "created,job,claim_request,stage,background_checked,CASE WHEN typeof(data)='text' AND length(CAST(data AS BLOB))<=8192 THEN data ELSE NULL END AS data"
            row = db.execute("""SELECT """+fields+""" FROM driver_jobs WHERE stage IN
              ('awaiting_acceptance','awaiting_failure_acceptance') ORDER BY background_checked,created LIMIT 1""").fetchone()
            if row is not None:
                if self._indexed is not None:
                    from .reference_indexed_fetch import require
                    require(row['data'] is not None,'indexed_progress_bound')
                db.execute('UPDATE driver_jobs SET background_checked=? WHERE job=?', (self.clock(), row['job']))
        observed = {'state': 'background_idle'}
        if row is not None:
            if self._indexed is None:data=json.loads(row['data'])
            else:
                from .reference_indexed_fetch import require
                require(len(row['data'].encode())<=8192)
                data=json.loads(row['data'])
                require(type(data.get('claim')) is dict and set(_INDEXED_FIXED_CLAIM)<=set(data['claim']) and not set(data['claim'])-set(_INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM),
                    'indexed_claim_projection_changed')
                with self._db() as db:header=self._indexed.header(db,row['job'])
                require(all(type(data['claim'][key]) is type(header[key]) and data['claim'][key]==header[key]
                    for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')),
                    'indexed_original_claim_changed')
            claim = data['claim']
            kind = 'acceptance' if row['stage'] == 'awaiting_acceptance' else 'terminal-acceptance'
            status, relayed = self._request('GET', '/jobs/' + row['job'] + '/' + kind, claim_token=claim['claimToken'])
            if status != 409:
                if kind == 'acceptance':
                    accepted = self._apply_acceptance(row['job'], relayed['receipts'])['fullyAccepted']
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

    def _indexed_projection(self,db,job_id,data):
        """Seal large original records once; retain only bounded mutable control."""
        from .reference_indexed_fetch import require, LIMITS
        claim=data.get('claim')
        if claim is not None:
            old=db.execute('SELECT 1 FROM indexed_claims WHERE job=?',(job_id,)).fetchone()
            if old is None:
                row=db.execute('SELECT claim_request FROM driver_jobs WHERE job=?',(job_id,)).fetchone()
                require(row is not None)
                self._indexed.seal_claim(db,job_id,row[0],claim)
            else:
                header=self._indexed.header(db,job_id)
                require(all(type(claim[key]) is type(header[key]) and claim[key]==header[key]
                    for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')),
                    'indexed_original_claim_changed')
        if 'reservation' in data:self._indexed.seal_reservation(db,job_id,data['reservation'])
        if 'grant' in data:self._indexed.seal_grant(db,job_id,data['grant'])
        small={key:value for key,value in data.items() if key not in ('claim','grant','inputPaths','fetchAttempts')}
        if claim is not None:small['claim']={key:value for key,value in claim.items() if key in _INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM}
        if 'grant' in data:small['indexedGrant']=True
        require(len(p.canonical_json(small))<=LIMITS['progressBytes'],'indexed_progress_bound')
        return small

    def _load_driver_data(self,row):
        if self._indexed is None:return json.loads(row['data'])
        from .reference_indexed_fetch import require, LIMITS
        require(len(row['data'].encode())<=LIMITS['progressBytes'],'indexed_progress_bound')
        data=json.loads(row['data'])
        if 'claim' not in data:return data
        with self._db() as db:
            original=self._indexed.claim(db,row['job']);header=self._indexed.header(db,row['job'])
            require(type(data['claim']) is dict and set(_INDEXED_FIXED_CLAIM)<=set(data['claim']) and not set(data['claim'])-set(_INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM),
                'indexed_claim_projection_changed')
            require(all(type(data['claim'][key]) is type(header[key]) and data['claim'][key]==header[key]
                for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')),
                'indexed_original_claim_changed')
            reservation=db.execute("SELECT record FROM indexed_reservations WHERE job=? AND typeof(record)='text' AND length(CAST(record AS BLOB))<=4096",(row['job'],)).fetchone()
            expected=None if reservation is None else json.loads(reservation[0])
            require(('reservation' not in data and expected is None) or p.canonical_json(data.get('reservation'))==p.canonical_json(expected),'indexed_reservation_changed')
            transfer=data['claim'].get('transferRequest')
            require((transfer is None and row['stage'] in ('claimed','reservation_pending')) or p.canonical_json(transfer)==p.canonical_json(expected),
                'indexed_transfer_request_changed')
            for key in _INDEXED_MUTABLE_CLAIM:
                if key in data['claim']:original[key]=data['claim'][key]
            data['claim']=original
            if data.pop('indexedGrant',False):data['grant']=self._indexed.grant(db,row['job'])
            progress=db.execute('SELECT next,bytes FROM indexed_fetch WHERE job=?',(row['job'],)).fetchone()
            if progress is not None and data.get('inputAccounting',{}).get('state')=='bound':
                objects=original['submission']['manifest']['objects'];reservation=data['reservation']['reservationId']
                data['inputPaths']={obj['id']:'jobs/'+reservation+'/'+obj['id']+'.input' for obj in objects[:progress[0]]}
                data['inputAccounting'].update(next=progress[0],byteCount=progress[1])
        return data

    def immediate_fetch_progress(self,result):
        """Concrete service scheduling proof, never a caller's JSON progress flag."""
        if self._indexed is None:return False
        if self.indexed_fetch_context is not self._indexed_context_initial:return False
        proof=self._indexed_progress_proof
        if proof is None or proof[0] is not result:return False
        self._state_owner.check(self.path.parent)
        with self._db() as db:return self._indexed.progress(db,proof[1])==proof[2]

    def indexed_empty_claim_pending(self,job,claim_request,lease):
        if self._indexed is None:return False
        if (self.indexed_fetch_context is not self._indexed_context_initial
                or type(lease) is not CoordinatorReadinessLease or lease.phase!='initial_claim'
                or (job,claim_request)!=(lease.job_id,lease.claim_request_id)):return False
        lease.check_observer(self)
        from .reference_indexed_fetch import read_only_profile
        from ops.reference_worker_daemon import _health_sql
        context=self.indexed_fetch_context
        if read_only_profile(self.path,bootstrap_id=context.bootstrap_id,binding=json.loads(context.binding_json),owner=self._state_owner)!=context:return False
        rows=_health_sql(self.path,"SELECT data FROM driver_jobs WHERE job=? AND claim_request=? AND stage='claim_pending' AND typeof(data)='text' AND length(CAST(data AS BLOB))<=8192",(job,claim_request),indexed_context=self._indexed_context_initial)
        if len(rows)!=1 or set(json.loads(rows[0][0]))-{'execution'}:return False
        claimed=_health_sql(self.path,'SELECT 1 FROM indexed_claims WHERE job=? UNION ALL SELECT 1 FROM indexed_fetch WHERE job=? LIMIT 1',(job,job),indexed_context=self._indexed_context_initial)
        lease.check_observer(self)
        return not claimed

    def _indexed_fetch_step(self):
        from .reference_indexed_fetch import require
        from .reference_transfer import validate_indexed_grant
        with self._db() as db:
            head=db.execute("SELECT job,stage,typeof(data) AS data_type,length(CAST(data AS BLOB)) AS data_bytes FROM driver_jobs WHERE stage NOT IN ('completed','awaiting_acceptance','awaiting_failure_acceptance') ORDER BY created LIMIT 1").fetchone()
            if head is None:return None
            require(head['data_type']=='text' and head['data_bytes']<=8192,'indexed_progress_bound')
            if head['stage']!='fetching':return None
            row=db.execute("SELECT job,claim_request,stage,data FROM driver_jobs WHERE job=? AND typeof(data)='text' AND length(CAST(data AS BLOB))<=8192",(head['job'],)).fetchone()
            require(row is not None,'indexed_progress_bound')
        job_id=row['job'];session=self._indexed_session
        if session is None or session['job']!=job_id:
            data=self._load_driver_data(row);claim=data['claim']
            self._refresh_options();self._validate_job(claim)
            with self._db() as db:
                self._indexed.validate_cold(db,job_id,claim,grant=data['grant'])
                progress=self._indexed.progress(db,job_id)
                attempts=list(db.execute('SELECT seq,MAX(attempt) FROM indexed_attempts WHERE job=? GROUP BY seq LIMIT 10001',(job_id,)))
                known_attempts=self._indexed.attempts_for(db,job_id,progress['next']) if progress['next']<len(claim['submission']['manifest']['objects']) else []
            require(self.clock()>=progress['clock'],'indexed_clock_rollback')
            data['fetchAttempts']={claim['submission']['manifest']['objects'][seq]['id']:count for seq,count in attempts}
            cached=self._input_cache(job_id,claim,data)
            handle=validate_indexed_grant(data['grant'],job=claim,clock=self.clock)
            session=dict(job=job_id,claim_request=row['claim_request'],data=data,claim=claim,cached=cached,progress=progress,grant=handle,known_attempts=known_attempts)
            self._indexed_session=session
        claim=session['claim'];cached=session['cached'];progress=session['progress'];index=progress['next']
        session['grant'].check_expiry(clock=self.clock)
        require(len(row['data'].encode())<=8192)
        projection=json.loads(row['data'])
        require(type(projection.get('claim')) is dict and set(_INDEXED_FIXED_CLAIM)<=set(projection['claim']) and not set(projection['claim'])-set(_INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM),
            'indexed_claim_projection_changed')
        require(row['claim_request']==session['claim_request'],'indexed_claim_request_changed')
        validate_indexed_fetch_projection(projection,reservation=session['data']['reservation'],identity=cached['identity'])
        require(all(type(projection['claim'][key]) is type(claim[key]) and projection['claim'][key]==claim[key]
            for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken')),
            'indexed_original_claim_changed')
        reservation=session['data']['reservation']['reservationId']
        with self._db() as db:
            require(self._indexed.progress(db,job_id)==progress,'indexed_cursor_changed')
        if index==len(cached['objects']):
            with self._db() as db:self._indexed.validate_attempt_closure(db,job_id,len(cached['objects']),progress)
            self._check_input_rows(reservation,cached['identity'])
            data=session['data'];data['inputPaths']={obj['object_id']:obj['relative_path'] for obj in cached['objects']}
            data['inputAccounting'].update(next=index,byteCount=progress['bytes'])
            self._save(job_id,'processing_pending',data)
            self._indexed_session=None;self._input_fetch_cache=None
            return {'state':'processing_pending','jobId':job_id}
        expected=cached['objects'][index]
        with self._db() as db:
            descriptor=self._indexed.object(db,job_id,index)
            require((descriptor['id'],descriptor['byteSize'],descriptor['sha256'])==
                    (expected['object_id'],expected['byte_size'],expected['sha256']),'indexed_object_changed')
            actual_attempts=self._indexed.attempts_for(db,job_id,index)
            require(actual_attempts==session['known_attempts'],'indexed_attempt_predecessor_changed')
            attempt=None if not actual_attempts else actual_attempts[-1]
        object_id=expected['object_id'];relative=expected['relative_path'];target=self.ledger.root/relative
        with self.ledger._transaction() as db:
            self.ledger._require_inputs(db)
            copy=db.execute('SELECT path,sha,bytes,cleaned FROM input_copies WHERE reservation=? AND id=?',(reservation,object_id)).fetchone()
            require(copy is not None and tuple(copy)==(relative,expected['sha256'],expected['byte_size'],None),'indexed_scratch_copy_changed')
        self.ledger._owned_path(reservation,relative);target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        receiver=getattr(self.ledger,'allocated_receiver',False)
        if receiver and (attempt is not None or os.path.lexists(target)):
            raise DriverHeld('allocated_input_recovery_required')
        if os.path.lexists(target):
            require(attempt is not None,'indexed_unowned_input_file')
        else:
            with self._db() as db:progress,intent=self._indexed.begin_attempt(db,job_id,index,progress,previous_attempts=session['known_attempts'])
            session['known_attempts']=[*session['known_attempts'],dict(attempt=len(session['known_attempts'])+1,intent=intent,state='started')]
            session['progress']=progress;attempt=dict(intent=intent,state='started')
            if receiver:
                from .reference_allocated_receiver import receive
                identity=dict(jobId=claim['jobId'],generation=claim['generation'],environment=claim['environment'],
                    manifestSha256=claim['manifestSha256'],path=relative,sha256=expected['sha256'],byteSize=expected['byte_size'])
                receive(self.ledger,self.broker,session['grant'],reservation,object_id,intent,identity,
                        max(cached['allocated'],progress['bytes']+expected['byte_size']))
            else:
                self.broker.download_indexed_object(session['grant'],object_id,target)
        self.worker._file(reservation,relative,expected['byte_size'],expected['sha256'])
        self.ledger.confirm_input_copy(reservation,object_id,byte_size=expected['byte_size'],sha256=expected['sha256'])
        allocated=max(cached['allocated'],progress['bytes']+expected['byte_size'])
        self.ledger.record_allocated(reservation,allocated);cached['allocated']=allocated
        with self._db() as db:
            progress=self._indexed.advance(db,job_id,index,expected['byte_size'],progress,attempt['intent'])
        session['progress']=progress
        session['known_attempts']=[]
        result={'state':'fetching','jobId':job_id}
        self._indexed_progress_proof=(result,job_id,progress)
        return result

    def _input_closure(self, job_id, claim, data):
        """One bounded manifest-derived identity, never journal-supplied paths."""
        manifest = claim['submission']['manifest']
        p._integer(claim['generation'], 'input generation')
        p._environment(claim['environment'])
        objects = manifest['objects']
        if (claim['jobId'] != job_id or type(objects) is not list or not 0 < len(objects) <= p.MAX_OBJECTS
                or p.manifest_sha256(manifest) != claim['manifestSha256']
                or manifest['sha256'] != claim['manifestSha256']
                or p.digest(claim['submission']['pipeline']) != claim['pipelineDigest']):
            raise DriverHeld('input_copy_manifest_changed')
        reservation = data['reservation']['reservationId']
        normalized = []
        for obj in objects:
            p._slug(obj['id'], 'input object'); p._integer(obj['byteSize'], 'input bytes'); p._hash(obj['sha256'], 'input hash')
            normalized.append(dict(object_id=obj['id'], byte_size=obj['byteSize'], sha256=obj['sha256'],
                relative_path='jobs/' + reservation + '/' + obj['id'] + '.input'))
        if (len({obj['object_id'] for obj in normalized}) != len(normalized)
                or sum(obj['byte_size'] for obj in normalized) != manifest['totalBytes']):
            raise DriverHeld('input_copy_manifest_changed')
        identity = dict(jobId=job_id, generation=claim['generation'], environment=claim['environment'],
            inputManifestSha256=claim['manifestSha256'], pipelineDigest=claim['pipelineDigest'],
            inputsSha256=self.ledger._closure_sha256(normalized))
        return reservation, normalized, identity

    def _prepare_input_binding(self, job_id, claim, data):
        """Persist this driver's registration intent before creating any copy rows."""
        reservation, objects, identity = self._input_closure(job_id, claim, data)
        expected = dict(schemaVersion=1, binding=identity, state='binding_pending', next=0, byteCount=0)
        checkpoint = data.get('inputAccounting')
        if checkpoint is None:
            with self.ledger._transaction() as db:
                if db.execute('SELECT 1 FROM input_bindings WHERE reservation=?', (reservation,)).fetchone():
                    raise DriverHeld('unowned_input_binding_requires_reconciliation')
            if data.get('inputPaths'):
                raise DriverHeld('legacy_input_paths_require_reconciliation')
            data['inputAccounting'] = expected
            self._save(job_id, 'reservation_pending', data)
        elif p.canonical_json(checkpoint) not in (p.canonical_json(expected), p.canonical_json(dict(expected, state='bound'))):
            raise DriverHeld('input_copy_binding_checkpoint_changed')
        try:
            if data['inputAccounting']['state'] == 'binding_pending':
                self.ledger.register_input_binding(reservation, job_id=job_id, generation=claim['generation'],
                    environment=claim['environment'], manifest_sha256=claim['manifestSha256'],
                    pipeline_sha256=claim['pipelineDigest'], objects=objects)
            self._check_input_rows(reservation, identity)
        except PolicyConflict:
            raise DriverHeld('input_copy_ledger_binding_changed') from None
        data['inputAccounting'] = dict(expected, state='bound')
        self._save(job_id, 'reservation_pending', data)

    def _check_input_rows(self, reservation, identity):
        with self.ledger._transaction() as db:
            self.ledger._require_inputs(db)
            row = db.execute('SELECT identity FROM input_bindings WHERE reservation=?', (reservation,)).fetchone()
            if (row is None or row[0] != json.dumps(identity, sort_keys=True, separators=(',', ':'))
                    or not self.ledger._closure_matches(db, reservation, identity)):
                raise DriverHeld('input_copy_ledger_binding_changed')
            reserved = self.ledger._reservation(db, reservation)
            settings = json.loads(reserved['settings'])
            if (reserved['job'], reserved['generation'], reserved['environment']) != (
                    identity['jobId'], identity['generation'], identity['environment']) or settings['manifest_sha256'] != identity['inputManifestSha256'] or reserved['state'] not in ('reserved','uncertain'):
                raise DriverHeld('input_copy_reservation_changed')
            return dict(reserved)

    def _input_cache(self, job_id, claim, data):
        if (type(claim['generation']) is not int or claim['generation'] < 1
                or p.digest(claim['submission']['pipeline']) != claim['pipelineDigest']):
            raise DriverHeld('input_copy_manifest_changed')
        checkpoint = data.get('inputAccounting')
        if (type(checkpoint) is not dict or set(checkpoint) != {'schemaVersion','binding','state','next','byteCount'}
                or type(checkpoint['schemaVersion']) is not int or checkpoint['schemaVersion'] != 1
                or type(checkpoint['next']) is not int or type(checkpoint['byteCount']) is not int):
            raise DriverHeld('input_copy_binding_checkpoint_changed')
        key = (job_id, data['reservation']['reservationId'], claim['generation'], claim['environment'],
               claim['manifestSha256'], claim['pipelineDigest'])
        cached = self._input_fetch_cache
        if cached is None or cached['key'] != key:
            reservation, objects, identity = self._input_closure(job_id, claim, data)
            if (type(checkpoint) is not dict or set(checkpoint) != {'schemaVersion','binding','state','next','byteCount'}
                    or type(checkpoint['schemaVersion']) is not int or checkpoint['schemaVersion'] != 1 or checkpoint['binding'] != identity or checkpoint['state'] != 'bound'
                    or type(checkpoint['next']) is not int or not 0 <= checkpoint['next'] <= len(objects)
                    or type(checkpoint['byteCount']) is not int):
                raise DriverHeld('input_copy_binding_checkpoint_changed')
            reserved = self._check_input_rows(reservation, identity)
            if reserved['allocated'] > sum(obj['byte_size'] for obj in objects):
                raise DriverHeld('input_copy_allocation_changed')
            index = checkpoint['next']; paths = data.get('inputPaths', {})
            expected = {obj['object_id']: obj['relative_path'] for obj in objects[:index]}
            if paths != expected or checkpoint['byteCount'] != sum(obj['byte_size'] for obj in objects[:index]):
                raise DriverHeld('input_copy_journal_prefix_changed')
            attempts = data.get('fetchAttempts', {})
            ids = {obj['object_id'] for obj in objects}
            if (type(attempts) is not dict or set(attempts) - ids
                    or any(type(count) is not int or not 1 <= count <= 3 for count in attempts.values())):
                raise DriverHeld('input_copy_attempt_history_changed')
            with self.ledger._transaction() as db:
                rows = {row['id']: dict(row) for row in db.execute(
                    'SELECT id,fetched,cleaned FROM input_copies WHERE reservation=? LIMIT ?', (reservation, p.MAX_OBJECTS + 1))}
            for number, obj in enumerate(objects):
                row = rows[obj['object_id']]
                if row['cleaned'] is not None or (number < index and row['fetched'] is None) or (number > index and row['fetched'] is not None):
                    raise DriverHeld('input_copy_fetch_history_changed')
                if number < index:
                    self.worker._file(reservation, obj['relative_path'], obj['byte_size'], obj['sha256'])
                    self.ledger.confirm_input_copy(reservation, obj['object_id'], byte_size=obj['byte_size'], sha256=obj['sha256'])
            cached = dict(key=key, objects=objects, identity=identity, next=index,
                          byteCount=checkpoint['byteCount'], allocated=reserved['allocated'])
            self._input_fetch_cache = cached
        if checkpoint != dict(schemaVersion=1, binding=cached['identity'], state='bound', next=cached['next'], byteCount=cached['byteCount']):
            raise DriverHeld('input_copy_journal_prefix_changed')
        if len(data.get('inputPaths', {})) != cached['next']:
            raise DriverHeld('input_copy_journal_prefix_changed')
        return cached

    def _fetch_input_copy(self, job_id, claim, data, grant):
        """One object per step; no release authority or cleanup is supplied here."""
        try:
            cached = self._input_cache(job_id, claim, data)
            reservation = data['reservation']['reservationId']; index = cached['next']
            if index == len(cached['objects']):
                expected = {obj['object_id']: obj['relative_path'] for obj in cached['objects']}
                if data.get('inputPaths', {}) != expected:
                    raise DriverHeld('input_copy_journal_prefix_changed')
                self._check_input_rows(reservation, cached['identity'])
                self._save(job_id, 'processing_pending', data)
                self._input_fetch_cache = None
                return {'state': 'processing_pending', 'jobId': job_id}
            obj = cached['objects'][index]; relative = obj['relative_path']; object_id = obj['object_id']
            with self.ledger._transaction() as db:
                self.ledger._require_inputs(db)
                binding = db.execute('SELECT identity FROM input_bindings WHERE reservation=?', (reservation,)).fetchone()
                row = db.execute('SELECT path,sha,bytes,cleaned FROM input_copies WHERE reservation=? AND id=?', (reservation, object_id)).fetchone()
                if (binding is None or binding[0] != json.dumps(cached['identity'], sort_keys=True, separators=(',', ':'))
                        or row is None or (row['path'],row['sha'],row['bytes'],row['cleaned']) != (relative,obj['sha256'],obj['byte_size'],None)):
                    raise DriverHeld('input_copy_ledger_binding_changed')
            if object_id in data.get('inputPaths', {}):
                raise DriverHeld('input_copy_journal_prefix_changed')
            self.ledger._owned_path(reservation, relative)
            target = self.ledger.root / relative
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            attempts = data.setdefault('fetchAttempts', {}).get(object_id, 0)
            if os.path.lexists(target):
                if not attempts:
                    raise DriverHeld('unowned_input_file_requires_reconciliation')
            else:
                if attempts >= 3:
                    raise DriverHeld('worker_fetch_attempt_budget_exhausted')
                data['fetchAttempts'][object_id] = attempts + 1
                self._save(job_id, 'fetching', data)
                self.broker.download_object(grant, object_id, target)
            self.worker._file(reservation, relative, obj['byte_size'], obj['sha256'])
            self.ledger.confirm_input_copy(reservation, object_id, byte_size=obj['byte_size'], sha256=obj['sha256'])
            byte_count = cached['byteCount'] + obj['byte_size']
            allocated = max(cached['allocated'], byte_count)
            self.ledger.record_allocated(reservation, allocated)
            cached['allocated'] = allocated
            data.setdefault('inputPaths', {})[object_id] = relative
            data['inputAccounting'] = dict(schemaVersion=1, binding=cached['identity'], state='bound', next=index + 1, byteCount=byte_count)
            self._save(job_id, 'fetching', data)
            cached.update(next=index + 1, byteCount=byte_count)
            return {'state': 'fetching', 'jobId': job_id}
        except PolicyConflict:
            raise DriverHeld('input_copy_ledger_binding_changed') from None
        except WorkerHeld:
            raise DriverHeld('input_copy_integrity_changed') from None

    def _step(self):
        if self._indexed is not None:
            fast=self._indexed_fetch_step()
            if fast is not None:return fast
        with self._db() as db:
            row = db.execute("SELECT * FROM driver_jobs WHERE stage NOT IN ('completed','awaiting_acceptance','awaiting_failure_acceptance') ORDER BY created LIMIT 1").fetchone()
        if row is None:
            if not self._ready():
                return {'state': 'not_ready'}
            with self._readiness_window('discovery'):
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
            auxiliary=getattr(self.ledger,'auxiliary_budget',None)
            if auxiliary is not None:
                auxiliary.require_lifecycle_qualification()
                auxiliary.reserve_job(job_id,request_id)
            with self._db() as db:
                if self._indexed is not None:self._indexed.reserve_slot(db,job_id,ledger=self.ledger)
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claim_pending',?)",
                           (job_id, request_id, p.canonical_json(data).decode()))
            return {'state': 'claim_pending', 'jobId': job_id}
        job_id, request_id, stage = row['job'], row['claim_request'], row['stage']
        data = self._load_driver_data(row)
        if stage == 'claim_pending':
            if self.execution_router is not None or self.combined_preparation is not None:
                self._advice(data.get('execution', {}), data.get('execution'))
            if not self._ready():
                return {'state': 'not_ready', 'jobId': job_id}
            with self._readiness_window('initial_claim',job_id,request_id):
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
            if self.input_copy_accounting:
                self._prepare_input_binding(job_id, claim, data)
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
            phase = 'fetching' if stage == 'fetching_pending' else 'processing'
            status, public = self._request('POST', suffix + '/phase', {'claimToken': token, 'phase': phase})
            if status == 409:
                return {'state': stage, 'jobId': job_id}
            claim.update(public)
            self._save(job_id, 'fetching' if phase == 'fetching' else 'execution_pending', data)
            return {'state': 'fetching' if phase == 'fetching' else 'execution_pending', 'jobId': job_id}
        if stage == 'fetching':
            if self.input_copy_accounting:
                try:
                    grant = validate_grant(data['grant'], job=claim, clock=self.clock)
                except ValueError:
                    raise DriverHeld('input_copy_grant_changed') from None
                return self._fetch_input_copy(job_id, claim, data, grant)
            grant = validate_grant(data['grant'], job=claim, clock=self.clock)
            reservation = data['reservation']['reservationId']
            directory = self.ledger.root / 'jobs' / reservation
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
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
                    self.broker.download_object(grant, obj['id'], target)
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
                if self.input_copy_accounting:
                    original = data['claim']
                    if (any(type(fresh[key]) is not type(original[key]) or fresh[key] != original[key]
                            for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken'))
                            or p.canonical_json(fresh['submission']) != p.canonical_json(original['submission'])):
                        raise DriverHeld('input_copy_original_claim_changed')
                    reservation, objects, identity = self._input_closure(job_id, original, data)
                    self._check_input_rows(reservation, identity)
                    if data.get('inputPaths') != {obj['object_id']:obj['relative_path'] for obj in objects}:
                        raise DriverHeld('input_copy_journal_prefix_changed')
                evidence = None
                if self.require_option_evidence:
                    # Authenticate with the ORIGINAL persisted token; an echoed
                    # newer claim cannot replace the request that owns inputs.
                    original = data['claim']
                    if (any(type(fresh[key]) is not type(original[key]) or fresh[key] != original[key]
                            for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest','claimToken'))
                            or p.canonical_json(fresh['submission']) != p.canonical_json(original['submission'])):
                        raise DriverHeld('original_option_evidence_claim_changed')
                    code, evidence = self._request('GET', '/jobs/' + job_id + '/option-evidence',
                        claim_token=original['claimToken'])
                    if code != 200:
                        raise DriverHeld('original_engine_option_evidence_unavailable')
                self._refresh_options()
                self._bind_claim(fresh, data)
                if self.require_option_evidence:
                    from .reference_option_evidence import validate_option_evidence, OptionEvidenceHeld
                    try:
                        encoded = p.canonical_json(evidence).decode()
                        if any(secret and secret in encoded for secret in (self._bearer, original['claimToken'], self.worker.engine.token)):
                            raise OptionEvidenceHeld('original_engine_option_evidence_unavailable')
                        validate_option_evidence(evidence, claim=original, claim_request_id=request_id,
                            authority_id=self.worker.authority_id, worker_id=self.worker_id,
                            worker_instance_id=self.worker_instance_id, fresh_definitions=self._option_definitions)
                    except OptionEvidenceHeld as error:
                        raise DriverHeld(str(error)) from None
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
            observed = self._apply_acceptance(job_id, relayed['receipts'])
            if observed['fullyAccepted']:
                self._save(job_id, 'completed', data)
                return {'state': 'completed', 'jobId': job_id}
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        raise DriverHeld('unknown_worker_driver_stage')
