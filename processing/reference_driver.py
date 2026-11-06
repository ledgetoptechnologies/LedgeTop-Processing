"""Durable worker driver over fixed authenticated HTTPS boundaries.

ODM is the default; an explicit router binds supported engines per claimed job.
Engine mutations remain physically fenced. Delivery, Viewer acceptance and
optional exact-object cleanup continue after slot release.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import hashlib
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
from uuid import uuid4

from . import reference_protocol as p
from .reference_engine_worker import WorkerHeld
from .reference_sender import ReferenceSender
from .reference_transfer import BrokerClient, _now, validate_grant
from .scratch_policy import LeaseLapsed, lease_lapsed
from .viewer_service import NativeEngine
from .node_gate import NodeGate
from .reference_failure import FailureOutbox, make_failure
from .reference_terminal import validate_terminal_acceptance
from .reference_engine_cleanup import EngineCacheCleanup
from .options import _validate_options
from .reference_option_policy import safe_option_definitions, validate_option_values
from .reference_execution import ExecutionRouter, RouteConflict
from .reference_parent_guard import validate_claim_fields
from .reference_worker_hold import HOLD_REASONS, OBSERVING_REASONS, make_request as make_hold_request, validate_ack as validate_hold_ack
from .scratch_policy import PolicyConflict
from .treedar_engine import TreeDarHeld


PREFIX = '/ltds/worker/v1'
SOURCE_RECOVERY_ATTEMPTS = 8
# Worker stage -> coordinator phases it can resume from. Any other pairing means
# the worker is ahead of the coordinator, whose history was lost, so it stays held.
SOURCE_RECOVERY_PHASES = {'reservation_pending': ('queued', 'waiting_capacity', 'awaiting_transfer'), 'awaiting_grant': ('awaiting_transfer',),
    'fetching_pending': ('awaiting_transfer', 'fetching'), 'fetching': ('fetching',),
    'processing_pending': ('fetching', 'processing'), 'execution_pending': ('processing',),
    'executing': ('processing',)}
# Exact original claims before reservation use the closed schema-4 transition.
SOURCE_UNRESERVED_PHASES = ('queued', 'waiting_capacity')
# Consecutive bad engine reads before they are reported; a good read resets both.
ENGINE_TASK_MISSING_READS = 10
ENGINE_SUSPECT_SECONDS = 1800
# Accepted-result cleanup can free scratch, so a shortage waits before reporting.
SCRATCH_SHORTAGE_SECONDS = 900
HOLDABLE_STAGES = frozenset(('claim_pending', 'claimed', 'reservation_pending', 'awaiting_grant', 'fetching_pending',
    'fetching', 'processing_pending', 'execution_pending', 'executing'))
# Not job evidence: a retry can clear these or the coordinator already stopped
# the job. Reporting would make a blip sticky and operator-only. An uncertain
# TreeDar start resumes after restart by design, so it stays local too.
LOCAL_HOLDS = frozenset(('unstarted_claim_requires_reconciliation', 'control_request_rejected',
    'execution_reconciliation_required',
    'coordinator_authority_changed', 'physical_gate_binding_changed', 'actual_driver_step_required',
    'lidar_provenance_unavailable', 'lidar_transfer_grant_unavailable'))
OVER_BUDGET_ARCHIVES = frozenset(('archive declared length exceeds output budget', 'archive exceeds scratch output budget'))
SCRATCH_CONFLICTS = {'insufficient unreserved scratch': 'scratch_capacity_unavailable',
    'reservation settings changed or lease expired': 'reservation_expired',
    'lease expired before engine mutation': 'reservation_expired', 'invalid engine transition': 'reservation_expired',
    'allocation outside reservation': 'reservation_expired', 'result exceeds output budget': 'result_over_budget'}
ENGINE_OBSERVATIONS = {'missing': 'engine_task_missing', 'unreachable': 'node_lost'}
# Closed reply states that prove one unanswered claim request can never admit.
CLAIM_RETIRED = {'dropped': frozenset(('running', 'held', 'succeeded', 'failed', 'cancelled')), 'refused': frozenset(('queued',)),
    'ended': frozenset(('succeeded', 'failed', 'cancelled'))}
# Any of these in the row means something already happened for this request.
CLAIM_EFFECTS = ('claim', 'reservation', 'originalCombinedClaim', 'grant', 'inputPaths', 'fetchAttempts',
    'failure', 'failureOutboxId', 'outboxId', 'hold', 'watch')


def _hold_code(reason):
    if reason in LOCAL_HOLDS:
        return None
    if reason in HOLD_REASONS or reason in OBSERVING_REASONS:
        return reason
    if reason.startswith('source_') or reason == 'worker_fetch_attempt_budget_exhausted':
        return 'source_transfer_unavailable'
    return 'worker_reconciliation_required'


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
                from .reference_source_intake_canonical import (
                    bind_validated_submission, pipeline_digest_for_job, submission_metadata_limit)
                raw = {key:submission[key] for key in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
                checked = p.validate_submission(raw, environment=identity['environment'],
                    capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm','treedar'),
                    allowed_odm_options=set(raw['pipeline']['odm']['options']),
                    max_body_bytes=submission_metadata_limit(submission))
                checked = bind_validated_submission(checked, submission)
                if pipeline_digest_for_job(claim) != identity['pipelineDigest']:
                    raise DriverHeld('migration_driver_submission_changed')
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
                 source_range_v2_enabled=False):
        self._combined_policy=combined_policy;self._combined_policy_initial=combined_policy;self.combined_preparation=None;self._combined_preparation_initial=None;self._active_step=None;self._active_job=None
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
        if type(source_range_v2_enabled) is not bool:
            raise ValueError('explicit source-range-v2 enablement required')
        self.source_range_v2_enabled = source_range_v2_enabled
        self.source_range_client = None
        if source_range_v2_enabled:
            from .source_range_transfer import SourceRangeClient
            self.source_range_client = SourceRangeClient(broker.origin,
                self.path.parent / 'source-range-progress.sqlite',
                ssl_context=ssl_context, clock=clock, renewal_origin=self.origin,
                renewal_headers=self._source_renewal_headers)
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
            if source_range_v2_enabled:
                settings['sourceTransfer'] = dict(protocol='source-range-v2',
                    progressPath=str(self.path.parent / 'source-range-progress.sqlite'))
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
        if stage == 'completed' and 'reservation' in data:
            # Nothing can write for a completed row, and only this row could ever
            # end its fence, so however it ended a never-started reservation ends too.
            self.ledger.settle_unstarted(data['reservation']['reservationId'], transfer_ended=True)
        if stage == 'completed':
            # Before the row: a death here costs a replay, the other order leaks an entry.
            with NodeGate(self.worker.engine_id) as gate:
                gate.retire_job(job_id)
        with self._db() as db:
            db.execute('UPDATE driver_jobs SET stage=?,data=? WHERE job=?',
                       (stage, p.canonical_json(data).decode(), job_id))

    def _source_renewal_headers(self, _route, payload):
        """Renewals prove this worker and, for a new grant, its exact claim."""
        headers = {'Authorization': 'Bearer ' + self._bearer}
        previous = payload.get('previousGrant')
        if type(previous) is dict:
            with self._db() as db:
                row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (previous.get('jobId'),)).fetchone()
            if row is None:
                raise DriverHeld('source_renewal_journal_invalid')
            headers['X-Claim-Token'] = json.loads(row['data'])['claim']['claimToken']
        return headers

    def _source_range_grant(self, job_id, claim, data):
        """Renew an expired v2 grant through a durable request/reply/ack journal.

        Time alone is never terminal here. The reservation deadline can pass,
        and a replayed successor can have expired during an outage. The ACK then
        still closes that journal step and the next renewal starts from it.
        """
        from .source_range_transfer import (MAX_RENEWAL_SEQUENCE, SourceRenewalInvalid,
            SourceRenewalUnavailable, validate_source_grant)

        sequence = data.get('sourceRenewalSequence', 0)
        if type(sequence) is not int or not 0 <= sequence <= MAX_RENEWAL_SEQUENCE:
            raise DriverHeld('source_renewal_sequence_invalid')
        raw_grant = data.get('grant')
        pending = data.get('sourceRenewal')
        expected_pending_fields = {'renewalId', 'sequence', 'previousGrantSha256', 'request', 'phase'}
        def journaled(grant):
            return validate_source_grant(grant, job=claim, clock=self.clock,
                                         allow_expired=True, past_reservation=True)

        try:
            previous = journaled(raw_grant)
        except Exception:
            raise DriverHeld('source_transfer_grant_unavailable') from None
        if pending is not None:
            if (type(pending) is not dict or set(pending) not in
                    (expected_pending_fields, expected_pending_fields | {'successorGrant', 'grantSha256', 'ackRequest'})
                    or type(pending.get('sequence')) is not int or pending.get('sequence') != sequence + 1
                    or type(pending.get('renewalId')) is not str
                    or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', pending['renewalId'])
                    or type(pending.get('previousGrantSha256')) is not str
                    or not re.fullmatch(r'[a-f0-9]{64}', pending['previousGrantSha256'])):
                raise DriverHeld('source_renewal_journal_invalid')
            request = pending.get('request')
            if (type(request) is not dict or set(request) !=
                    {'protocol', 'renewalId', 'sequence', 'previousGrantSha256', 'previousGrant'}
                    or request.get('protocol') != 'source-range-v2-renewal'
                    or request.get('renewalId') != pending['renewalId']
                    or request.get('sequence') != pending['sequence']
                    or request.get('previousGrantSha256') != pending['previousGrantSha256']
                    or request.get('previousGrant') != raw_grant
                    or hashlib.sha256(p.canonical_json(raw_grant)).hexdigest() != pending['previousGrantSha256']):
                raise DriverHeld('source_renewal_journal_invalid')
        else:
            if previous.expires > _now(self.clock):
                return validate_source_grant(raw_grant, job=claim, clock=self.clock, past_reservation=sequence > 0)
            if sequence >= MAX_RENEWAL_SEQUENCE:
                raise DriverHeld('source_renewal_sequence_exhausted')
            previous_digest = hashlib.sha256(p.canonical_json(raw_grant)).hexdigest()
            renewal_id = str(uuid4())
            request = {'protocol': 'source-range-v2-renewal', 'renewalId': renewal_id,
                'sequence': sequence + 1, 'previousGrantSha256': previous_digest,
                'previousGrant': raw_grant}
            pending = {'renewalId': renewal_id, 'sequence': sequence + 1,
                'previousGrantSha256': previous_digest, 'request': request, 'phase': 'request_pending'}
            data['sourceRenewal'] = pending
            self._save(job_id, 'fetching', data)

        if pending['phase'] == 'request_pending':
            if set(pending) != expected_pending_fields:
                raise DriverHeld('source_renewal_journal_invalid')
            try:
                status, response = self.source_range_client.renew_grant(request)
            except SourceRenewalUnavailable:
                raise DriverWait('source_renewal_endpoint_unavailable') from None
            except SourceRenewalInvalid:
                raise DriverHeld('source_renewal_response_invalid') from None
            if status != 200:
                code = response.get('error') if type(response) is dict else None
                if status >= 500 or code in ('renewal_authority_unavailable', 'grant_not_expired'):
                    raise DriverWait('source_renewal_not_ready')
                raise DriverHeld('source_renewal_rejected')
            if (type(response) is not dict or set(response) !=
                    {'protocol', 'renewalId', 'sequence', 'previousGrantSha256', 'grant'}
                    or response.get('protocol') != 'source-range-v2-renewal'
                    or response.get('renewalId') != pending['renewalId']
                    or response.get('sequence') != pending['sequence']
                    or response.get('previousGrantSha256') != pending['previousGrantSha256']):
                raise DriverHeld('source_renewal_response_invalid')
            try:
                renewed = journaled(response['grant'])
            except Exception:
                raise DriverHeld('source_renewal_grant_invalid') from None
            successor = response['grant']
            if (renewed.expires <= previous.expires
                    or (renewed.identity, renewed.objects, renewed.chunk_bytes, renewed.request_budget,
                        renewed.byte_budget, renewed.attempt_budget) !=
                       (previous.identity, previous.objects, previous.chunk_bytes, previous.request_budget,
                        previous.byte_budget, previous.attempt_budget)):
                raise DriverHeld('source_renewal_grant_invalid')
            grant_digest = hashlib.sha256(p.canonical_json(successor)).hexdigest()
            ack = {'protocol': 'source-range-v2-renewal-ack', 'renewalId': pending['renewalId'],
                'sequence': pending['sequence'], 'grantSha256': grant_digest, 'token': successor['token']}
            pending.update({'phase': 'ack_pending', 'successorGrant': successor,
                'grantSha256': grant_digest, 'ackRequest': ack})
            # Save the exact successor and ack before acknowledging it remotely.
            self._save(job_id, 'fetching', data)

        if pending.get('phase') != 'ack_pending' or set(pending) != expected_pending_fields | {'successorGrant', 'grantSha256', 'ackRequest'}:
            raise DriverHeld('source_renewal_journal_invalid')
        successor = pending['successorGrant']
        if (type(successor) is not dict or type(pending['grantSha256']) is not str
                or hashlib.sha256(p.canonical_json(successor)).hexdigest() != pending['grantSha256']
                or pending['ackRequest'] != {'protocol': 'source-range-v2-renewal-ack',
                    'renewalId': pending['renewalId'], 'sequence': pending['sequence'],
                    'grantSha256': pending['grantSha256'], 'token': successor.get('token')}):
            raise DriverHeld('source_renewal_journal_invalid')
        try:
            old = journaled(raw_grant)
            new = journaled(successor)
        except Exception:
            raise DriverHeld('source_renewal_grant_invalid') from None
        if (new.expires <= old.expires
                or (new.identity, new.objects, new.chunk_bytes, new.request_budget, new.byte_budget,
                    new.attempt_budget) !=
                   (old.identity, old.objects, old.chunk_bytes, old.request_budget, old.byte_budget,
                    old.attempt_budget)):
            raise DriverHeld('source_renewal_grant_invalid')
        try:
            status, response = self.source_range_client.acknowledge_grant_renewal(pending['ackRequest'])
        except SourceRenewalUnavailable:
            raise DriverWait('source_renewal_ack_unavailable') from None
        except SourceRenewalInvalid:
            raise DriverHeld('source_renewal_ack_response_invalid') from None
        if status != 200:
            code = response.get('error') if type(response) is dict else None
            if status >= 500 or code == 'renewal_authority_unavailable':
                raise DriverWait('source_renewal_ack_unavailable')
            raise DriverHeld('source_renewal_ack_rejected')
        if (type(response) is not dict or set(response) != {'protocol', 'renewalId', 'sequence', 'grantSha256'}
                or response != {'protocol': 'source-range-v2-renewal-ack',
                    'renewalId': pending['renewalId'], 'sequence': pending['sequence'],
                    'grantSha256': pending['grantSha256']}):
            raise DriverHeld('source_renewal_ack_response_invalid')
        data['grant'] = successor
        data['sourceRenewalSequence'] = pending['sequence']
        data.pop('sourceRenewal', None)
        self._save(job_id, 'fetching', data)
        if new.expires <= _now(self.clock):
            return self._source_range_grant(job_id, claim, data)
        try:
            return validate_source_grant(data['grant'], job=claim, clock=self.clock, past_reservation=True)
        except Exception:
            raise DriverHeld('source_renewal_grant_invalid') from None

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
            from .reference_limits import PRIVATE_CONTROL_RESPONSE_BYTES
            raw = response.read(PRIVATE_CONTROL_RESPONSE_BYTES + 1)
            if len(raw) > PRIVATE_CONTROL_RESPONSE_BYTES:
                raise DriverWait('control_response_too_large')
            expected = response.headers.get('Content-Length')
            if expected is not None and (not expected.isascii() or not expected.isdecimal() or int(expected) != len(raw)):
                raise DriverWait('truncated_control_response')
            payload = p.decode_submission(raw, max_bytes=PRIVATE_CONTROL_RESPONSE_BYTES)
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
        from .reference_source_intake_canonical import (
            bind_validated_submission, pipeline_digest_for_job, submission_metadata_limit)
        raw = {key: submission[key] for key in ('schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        kind = self._pipeline_kind(submission)
        checked = p.validate_submission(raw, environment=job['environment'],
            capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar') if self.execution_router or self._combined_policy else ('odm',),
            allowed_odm_options=self.worker.allowed_options,
            max_body_bytes=submission_metadata_limit(submission))
        checked = bind_validated_submission(checked, submission)
        if pipeline_digest_for_job(job) != job['pipelineDigest']:
            raise DriverHeld('unsupported_worker_submission')
        if (job['environment'] not in self.allowed_environments or job['nodeId'] != self.node_id
                or p.canonical_json(checked) != p.canonical_json(submission)
                or checked['manifestSha256'] != job['manifestSha256']
                or checked['pipelineDigest'] != job['pipelineDigest']):
            raise DriverHeld('unsupported_worker_submission')
        if kind == 'odm':
            from .reference_odm_inputs import validate_odm_input_closure
            try:
                validate_odm_input_closure(job, authority_id=self.worker.authority_id)
            except ValueError:
                raise DriverHeld('unsupported_worker_submission') from None
        values = raw['pipeline']['odm']['options']
        if set(values) - self._option_definitions.keys():
            raise DriverHeld('engine_option_no_longer_available')
        try:
            validate_option_values(values, self._option_definitions)
        except (TypeError, ValueError, OverflowError):
            raise DriverHeld('engine_option_type_mismatch') from None
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

    def _new_transfer_request(self, claim):
        reservation_id = 'driver-' + secrets.token_hex(16)
        expires_at = datetime.fromtimestamp(self.clock() + self.transfer_ttl, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        nonce = secrets.token_hex(16)
        if self.source_range_v2_enabled:
            from .source_range_transfer import build_source_transfer_request
            return build_source_transfer_request(claim, reservation_id=reservation_id,
                expires_at=expires_at, nonce=nonce, clock=self.clock)
        return dict(reservationId=reservation_id, generation=claim['generation'],
            expiresAt=expires_at, manifestSha256=claim['manifestSha256'],
            byteBudget=min(p.MAX_SAFE_INTEGER, 3 * claim['submission']['manifest']['totalBytes']), nonce=nonce)

    def _apply_acceptance(self, job_id, receipts):
        source = self.execution_router if self.execution_router is not None else self.worker
        return source.apply_viewer_acceptance(job_id, receipts)

    def _observation_retry_state(self, data):
        attempts = data.get('acceptanceObservationAttempts', 0)
        if type(attempts) is not int or not 0 <= attempts <= 32:
            raise DriverHeld('acceptance_observation_retry_state_changed')
        return (_now(self.clock), _now(lambda: data.get('acceptanceObservationNextAttemptAt', 0)),
            _now(lambda: data.get('acceptanceObservationLastAttemptAt', 0)), attempts)

    def _observation_deferred(self, claim, data):
        if (claim.get('sourceIntakeVersion') != 2 or not data.get('acceptanceObservationRequest')
                or data.get('acceptanceObservationAck')):
            return False
        try:
            now, due, previous, _attempts = self._observation_retry_state(data)
            return now < due or now < previous
        except Exception:
            return True

    def _confirm_acceptance_observed(self, job_id, data, receipts):
        """Persist/replay local V2 verification before native cleanup/completion.

        Existing signed-copy scratch cleanup remains independent. This intent
        uses durable metadata only and never reads source or cleaned result bytes.
        """
        from .reference_acceptance_observed import make_request, validate_request, validate_ack
        claim = data['claim']
        source_version = claim.get('sourceIntakeVersion')
        submitted_version = claim.get('submission', {}).get('sourceIntakeVersion')
        if source_version is None and submitted_version is None and 'sourceAcceptanceBinding' not in claim:
            return True
        if type(source_version) is not int or source_version != 2 or submitted_version != 2:
            raise DriverHeld('source_acceptance_observation_binding_unavailable')
        try:
            result = self.outbox.get(data['outboxId'])['payload']['resultManifest']
            intent = data.get('acceptanceObservationRequest')
            if intent is None:
                request = make_request(claim, result, receipts, str(uuid4()))
                intent = p.canonical_json(request).decode('ascii')
                data['acceptanceObservationRequest'] = intent
                self._save(job_id, 'awaiting_acceptance', data)
            else:
                if type(intent) is not str or len(intent.encode('utf-8')) > p.MAX_BODY_BYTES:
                    raise ValueError('invalid persisted observation')
                request = validate_request(p.decode_submission(intent.encode('utf-8')))
                expected = make_request(claim, result, receipts, request['observationId'])
                if (any(request[key] != expected[key] for key in expected if key != 'receipts')
                        or any(pair not in expected['receipts'] for pair in request['receipts'])
                        or p.canonical_json(request).decode('ascii') != intent):
                    raise ValueError('persisted observation changed')
            identity = dict(node_id=self.node_id, worker_id=self.worker_id, worker_instance_id=self.worker_instance_id)
            ack = data.get('acceptanceObservationAck')
            if ack is None:
                now, due, previous, attempts = self._observation_retry_state(data)
                if now < previous:
                    raise DriverWait('acceptance_observation_clock_rollback')
                if now < due:
                    raise DriverWait('acceptance_observation_backoff')
                # Persist the bounded retry window before contacting the
                # coordinator, so restart cannot turn an outage into a loop.
                attempts = min(32, attempts + 1)
                data.update(acceptanceObservationAttempts=attempts,
                    acceptanceObservationLastAttemptAt=now,
                    acceptanceObservationNextAttemptAt=now + min(300, 2 ** min(attempts, 9)))
                self._save(job_id, 'awaiting_acceptance', data)
                status, ack = self._request('POST', '/jobs/' + job_id + '/acceptance-observed',
                    request, claim_token=claim['claimToken'])
                if status != 200:
                    raise DriverWait('acceptance_observation_unavailable')
                ack = validate_ack(ack, request, identity)
                data['acceptanceObservationAck'] = ack
                self._save(job_id, 'awaiting_acceptance', data)
            else:
                validate_ack(ack, request, identity)
            return True
        except (DriverHeld, DriverWait):
            raise
        except Exception:
            raise DriverWait('acceptance_observation_reconciliation_required') from None

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
        selected = safe_option_definitions(definitions)
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
            return self._hold(str(error))
        except DriverWait as error:
            return {'state': 'retry_wait', 'reason': str(error)}
        except (RouteConflict, TreeDarHeld):
            return self._hold('execution_reconciliation_required')
        except PolicyConflict as error:
            return self._scratch_conflict(str(error))
        except Exception as error:
            if self.combined_preparation is not None:
                from .reference_combined_preparation import CombinedPreparationHeld
                if isinstance(error,CombinedPreparationHeld):return {'state':'held','reason':str(error)}
            return {'state': 'retry_wait', 'reason': 'driver_step_unavailable'}
        finally:
            self._active_step=self._active_job=None
            self._lock.release()

    def _active_row(self):
        if self._active_job is None:
            return None
        with self._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (self._active_job,)).fetchone()
        return (row['stage'], json.loads(row['data'])) if row is not None and row['stage'] in HOLDABLE_STAGES else None

    def _lease_lapsed(self, data):
        """The ledger's own durable word; a refusal message alone proves nothing."""
        reservation = (data.get('reservation') or {}).get('reservationId')
        if reservation is None:
            return False
        try:
            with self.ledger._transaction() as db:
                now = self.ledger._now(db)
                self.ledger._expire(db, now)
                current = self.ledger._reservation(db, reservation)
        except PolicyConflict:
            return False
        return current['state'] == 'expired' or lease_lapsed(current, now)

    def _hold(self, reason):
        """Journal irreversible evidence before contact; the row then stops advancing.

        Anything else is observed and the row keeps its original retry, so a job
        that recovers delivers as if nothing had happened.
        """
        held = {'state': 'held', 'reason': reason}
        code = _hold_code(reason)
        try:
            active = None if code is None or self.combined_preparation is not None else self._active_row()
            if active is None:
                return held
            stage, data = active
            if code == 'reservation_expired' and not self._lease_lapsed(data):
                code = 'worker_reconciliation_required'
            if code not in HOLD_REASONS:
                self._observe(self._active_job, stage, data, code)
                return held
            watch = data.pop('watch', None) or {}
            data.setdefault('hold', dict(holdId=watch.get('holdId') or str(uuid4()), reasonCode=code, reason=reason,
                sequence=watch.get('sequence', -1) + 1, observation=None, engineStatusCode=None, ack=None))
            data.pop('suspect', None)
            self._save(self._active_job, stage, data)
            self._report_hold(self._active_job, stage, data)
        except Exception:
            pass
        return held

    def _post_report(self, job_id, stage, data, key):
        """One bounded report of data[key]: acknowledged, refused or pending."""
        record, claim = data[key], data['claim']
        if record['ack'] is not None and record['ack']['sequence'] == record['sequence']:
            return 'acknowledged'
        with self._db() as db:
            request_id = db.execute('SELECT claim_request FROM driver_jobs WHERE job=?', (job_id,)).fetchone()[0]
        request = make_hold_request(claim, authority_id=self.worker.authority_id, claim_request_id=request_id, hold=record)
        try:
            status, ack = self._request('POST', '/jobs/' + job_id + '/hold', request, claim_token=claim['claimToken'])
            if status != 200:
                return 'refused'
            record['ack'] = validate_hold_ack(ack, request)
        except (DriverHeld, DriverWait, ValueError):
            return 'pending'
        self._save(job_id, stage, data)
        return 'acknowledged'

    def _report_hold(self, job_id, stage, data):
        hold = data['hold']
        return {'state': 'held', 'jobId': job_id, 'reason': hold['reason'], 'holdReason': hold['reasonCode'],
            'observation': hold['observation'], 'engineStatusCode': hold['engineStatusCode'],
            'holdReport': self._post_report(job_id, stage, data, 'hold')}

    def _held_step(self, job_id, stage, data):
        """Read-only while held: an operator's decision is honoured, a task only reported."""
        hold = data['hold']
        closed = self._operator_terminal(job_id, stage, data, hold)
        if closed is not None:
            return closed
        observed = (hold['observation'], hold['engineStatusCode'])
        try:
            if self._kind(job_id) == 'odm':
                self.worker._row(job_id)
                state, info = self.worker.task_observation(job_id)
                code = info['status']['code'] if info else None
                observed = ({'present': 'engine_task_present', 'missing': 'engine_task_missing',
                    'unreachable': 'engine_unreachable'}[state], code)
        except WorkerHeld:
            pass
        if observed != (hold['observation'], hold['engineStatusCode']):
            hold.update(observation=observed[0], engineStatusCode=observed[1], sequence=hold['sequence'] + 1)
            self._save(job_id, stage, data)
        return self._report_hold(job_id, stage, data)

    def _physically_live(self, job_id):
        """Whether this job may still own the node; only the gate can say it does not."""
        tree = getattr(self.execution_router, 'treedar_worker', None)
        if tree is not None:
            try:
                tree.controller._row(job_id)
                return True
            except (WorkerHeld, TreeDarHeld):
                pass
        with NodeGate(self.worker.engine_id) as gate:
            reservation = gate.snapshot()['reservation']
        # An unbound reservation may be this job's crash between reserve and bind.
        return reservation is not None and (reservation['job'] is None or reservation['job']['id'] == job_id)

    def _no_local_effect(self, job_id, data):
        """Absence census for an unstarted claim row; anything unknown is an effect."""
        if any(key in data for key in CLAIM_EFFECTS):
            return False
        try:
            with self.ledger._transaction() as db:
                reserved = db.execute('SELECT 1 FROM reservations WHERE job=? LIMIT 1', (job_id,)).fetchone()
            if reserved is not None or self._physically_live(job_id):
                return False
            self.worker._row(job_id)
        except WorkerHeld:
            return True
        except Exception:
            pass
        return False

    def _overtaken_reply(self, job_id, request_id):
        """Withdraw a request only once the queue offers this node something else.

        Waiting behind nothing is not starvation, and the request stays live. An
        admission that won the race stands and the next replay returns its claim.
        """
        _, offered = self._request('GET', '/next')
        head = offered.get('job')
        if head is None or head.get('jobId') == job_id:
            return None
        status, reply = self._request('POST', '/claim-withdrawal', {'jobId': job_id, 'claimRequestId': request_id})
        if status == 409 or (set(reply) == {'jobId', 'status', 'claimState'} and reply['claimState'] == 'admitted'
                and reply['jobId'] == job_id):
            return None
        return reply

    def _claim_retirable(self, job_id, data, reply):
        """Only the exact closed reply; a claim-shaped body proves nothing, whatever it says."""
        allowed = CLAIM_RETIRED.get(reply.get('claimState')) if type(reply.get('claimState')) is str else None
        return (set(reply) == {'jobId', 'status', 'claimState'} and allowed is not None and type(reply['status']) is str and reply['status'] in allowed
            and reply['jobId'] == job_id and self._no_local_effect(job_id, data))

    def _terminal_status(self, job_id, data):
        """Exact authenticated terminal word for this claim, else None (keep trying)."""
        try:
            state = self._claim_state(job_id, data['claim'])
        except (DriverHeld, DriverWait, ValueError):
            return None
        if state is None:
            return None
        return state['status'] if state['status'] in ('failed', 'cancelled') else None

    def _claim_state(self, job_id, claim):
        """Fresh coordinator view of this exact claim, else None."""
        status, state = self._request('GET', '/jobs/' + job_id + '/claim-state', claim_token=claim['claimToken'])
        if status != 200:
            return None
        validate_claim_fields(state)
        return state if self._claim_bound(job_id, claim, state) else None

    def _claim_bound(self, job_id, claim, state):
        with self._db() as db:
            request_id = db.execute('SELECT claim_request FROM driver_jobs WHERE job=?', (job_id,)).fetchone()[0]
        expected = dict(authorityId=self.worker.authority_id, jobId=job_id, generation=claim['generation'],
            environment=claim['environment'], nodeId=self.node_id, workerId=self.worker_id,
            workerInstanceId=self.worker_instance_id, claimRequestId=request_id,
            claimTokenSha256=hashlib.sha256(claim['claimToken'].encode()).hexdigest(),
            manifestSha256=claim['manifestSha256'], pipelineDigest=claim['pipelineDigest'])
        return all(state[key] == value for key, value in expected.items())

    def _delivery_owed(self, job_id, data):
        """Durable evidence only: poll and failure sealing commit before the row hears."""
        return (self.outbox.holds(job_id) or self.failure_outbox.holds(job_id)
            or self.ledger.result_registered((data.get('reservation') or {}).get('reservationId')))

    def _physical_fence(self, job_id, stage, data, record, result):
        """A live gate stays public and held; a cleared one retires its report. None once clear."""
        if self._physically_live(job_id):
            data.setdefault('fence', dict(holdId=record['holdId'], sequence=record['sequence'] + 1,
                reasonCode='physical_reservation_retained', observation=None, engineStatusCode=None, ack=None))
            self._save(job_id, stage, data)
            return dict(result, state='held', reconciliation='physical_reservation_retained',
                holdReport=self._post_report(job_id, stage, data, 'fence'))
        if 'fence' in data:
            fence = data['fence']
            if fence['reasonCode'] is not None:
                fence.update(reasonCode=None, sequence=fence['sequence'] + 1, ack=None)
                self._save(job_id, stage, data)
            if self._post_report(job_id, stage, data, 'fence') == 'pending':
                return dict(result, state='held', reconciliation='physical_reservation_clearing')
        return None

    def _operator_terminal(self, job_id, stage, data, record):
        """An operator ended this job: end its row, but never a live physical fence.

        While the gate still names the job the row stays and says so, publicly,
        so the node is fenced visibly. gate.resolve is the operator's physical
        step; scratch, engine files and any task are left exactly as they are.
        """
        if self._delivery_owed(job_id, data):
            return None
        status = self._terminal_status(job_id, data)
        if status is None:
            return None
        result = {'jobId': job_id, 'reason': record['reasonCode'], 'terminalStatus': status}
        fenced = self._physical_fence(job_id, stage, data, record, result)
        if fenced is not None:
            return fenced
        data['operatorTerminal'] = dict(status=status, reasonCode=record['reasonCode'], stage=stage)
        self._save(job_id, 'completed', data)
        return dict(result, state='operator_terminal')

    def _observe(self, job_id, stage, data, code, observation=None):
        """Show a transient condition; never changes the job or the row's retry."""
        if self.combined_preparation is not None:
            return None
        watch = data.get('watch') or dict(holdId=str(uuid4()), sequence=-1, reasonCode=None, observation=None,
            engineStatusCode=None, ack=None, stage=stage)
        if (watch['reasonCode'], watch['observation'], watch['stage']) != (code, observation, stage):
            watch.update(reasonCode=code, observation=observation, stage=stage, sequence=watch['sequence'] + 1, ack=None)
        data['watch'] = watch
        self._save(job_id, stage, data)
        return self._post_report(job_id, stage, data, 'watch')

    def _clear_watch(self, job_id, stage, data):
        """Retire a shown condition; a refusal means there is nothing left to retire."""
        watch = data.get('watch')
        if watch is None:
            return
        if watch['reasonCode'] is not None:
            watch.update(reasonCode=None, observation=None, engineStatusCode=None, sequence=watch['sequence'] + 1, ack=None)
            self._save(job_id, stage, data)
        if self._post_report(job_id, stage, data, 'watch') != 'pending':
            del data['watch']
            self._save(job_id, stage, data)

    def _watch_step(self, job_id, stage, data):
        watch = data['watch']
        if watch['reasonCode'] is None or watch['stage'] != stage:
            return self._clear_watch(job_id, stage, data)
        closed = self._operator_terminal(job_id, stage, data, watch)
        if closed is None:
            self._post_report(job_id, stage, data, 'watch')
        return closed

    def _suspect(self, job_id, stage, data, kind):
        """Persist one consecutive bad observation; returns (count, elapsed)."""
        now = self.clock()
        state = data.get('suspect') or dict(kind=kind, count=0, since=now)
        if state['kind'] != kind:
            state.update(kind=kind, count=0)
        state['count'] += 1
        data['suspect'] = state
        self._save(job_id, stage, data)
        return state['count'], max(0, now - state['since'])

    def _clear_suspect(self, job_id, stage, data):
        if data.pop('suspect', None) is not None:
            self._save(job_id, stage, data)

    def _engine_task(self, job_id, data):
        """The exact task, or a retry. An outage is only ever observed: a task that
        reappears carries on through the ordinary poll and outbox."""
        state, info = self.worker.task_observation(job_id)
        if state == 'present':
            self._clear_suspect(job_id, 'executing', data)
            if (data.get('watch') or {}).get('reasonCode') in ENGINE_OBSERVATIONS.values():
                self._clear_watch(job_id, 'executing', data)
            return info
        count, elapsed = self._suspect(job_id, 'executing', data, state)
        if elapsed >= ENGINE_SUSPECT_SECONDS or (state == 'missing' and count >= ENGINE_TASK_MISSING_READS):
            self._observe(job_id, 'executing', data, ENGINE_OBSERVATIONS[state],
                'engine_task_missing' if state == 'missing' else 'engine_unreachable')
        raise DriverWait('engine_task_not_found' if state == 'missing' else 'engine_terminal_identity_unavailable')

    def _scratch_conflict(self, message):
        reason = SCRATCH_CONFLICTS.get(message)
        active = self._active_row() if reason is not None and self.combined_preparation is None else None
        if active is None:
            return {'state': 'retry_wait', 'reason': 'driver_step_unavailable'}
        stage, data = active
        if reason == 'scratch_capacity_unavailable':
            if self._suspect(self._active_job, stage, data, reason)[1] >= SCRATCH_SHORTAGE_SECONDS:
                self._observe(self._active_job, stage, data, reason)
            return {'state': 'retry_wait', 'jobId': self._active_job, 'reason': reason}
        if reason == 'reservation_expired' and not self._lease_lapsed(data):
            self._observe(self._active_job, stage, data, 'worker_reconciliation_required')
            return {'state': 'retry_wait', 'jobId': self._active_job, 'reason': 'worker_reconciliation_required'}
        return self._hold(reason)

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
            deferred = kind == 'acceptance' and self._observation_deferred(claim, data)
            if deferred:
                status, relayed = 409, None
                observed = {'state': 'acceptance_observation_pending', 'jobId': row['job']}
            else:
                status, relayed = self._request('GET', '/jobs/' + row['job'] + '/' + kind, claim_token=claim['claimToken'])
            if status != 409:
                if kind == 'acceptance':
                    accepted = self._apply_acceptance(row['job'], relayed['receipts'])['fullyAccepted']
                    if accepted:
                        try:
                            accepted = self._confirm_acceptance_observed(row['job'], data, relayed['receipts'])
                        except (DriverHeld, DriverWait):
                            # Observation is a NAS-source-release prerequisite,
                            # not an extra retention rule for already verified
                            # result copies. Keep its intent and native task,
                            # but allow the existing scratch cleanup policy below.
                            accepted = False
                            observed = {'state': 'acceptance_observation_pending', 'jobId': row['job']}
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

    def _fetch_refusal(self, job_id, stage, data, source_range_v2):
        """Stop before a source attempt is charged if no GET would be admitted.

        The source budgets charge before the request, so asking a held job over
        and over would spend them with nothing served. A v1 broker is a separate
        origin that an unreachable coordinator says nothing about, so only a v2
        fetch waits for one. Returns the outcome to report, or None to fetch.
        """
        try:
            state = self._claim_state(job_id, data['claim'])
        except DriverWait:
            if source_range_v2:
                raise
            return None
        except ValueError:
            raise DriverHeld('source_fetch_not_admitted') from None
        if state is None:
            raise DriverHeld('source_fetch_not_admitted')
        if state['status'] in ('failed', 'cancelled'):
            return self._unstarted(job_id, stage, data, 'source_fetch_ended')
        recovered = False
        if source_range_v2 and self._restart_recoverable(state, data, stage):
            self._recover_source_fetch(job_id, stage, data)
            state = self._claim_state(job_id, data['claim'])
            if state is None:
                raise DriverHeld('source_fetch_not_admitted')
            recovered = True
        if state['status'] == 'running' and state['executionPhase'] == 'fetching':
            if recovered:
                self._clear_transfer_watch(job_id, stage, data)
            return None
        raise DriverHeld('source_fetch_held')

    @staticmethod
    def _restart_recoverable(state, data, stage):
        """A restart hold this stage can resume, or an unresolved intent from an earlier one."""
        return state['executionPhase'] in SOURCE_RECOVERY_PHASES.get(stage, ()) and (
            (state['status'], state['heldReason']) == ('held', 'coordinator_restart')
            or (data.get('sourceRecovery') or {}).get('pending') is not None)

    def _clear_transfer_watch(self, job_id, stage, data):
        """Retire a shown transfer problem once it is proven over; nothing else.

        Only a recovery that returned this claim to running, or an object that
        really arrived, proves that. Running state alone does not, or a failing
        source would flap. A journaled hold is never a watch, so it is untouched.
        """
        if (data.get('watch') or {}).get('reasonCode') == 'source_transfer_unavailable':
            self._clear_watch(job_id, stage, data)

    def _source_refused(self, job_id, stage, data):
        """A control call got 409 with no GET to gate: resume it if a restart held it."""
        if data['reservation'].get('protocol') != 'source-range-v2':
            return
        state = self._claim_state(job_id, data['claim'])
        if state is None:
            return
        if self._restart_recoverable(state, data, stage):
            self._recover_source_fetch(job_id, stage, data)

    def _source_recovery_journal(self, data):
        from .reference_recovery import ClaimRecoveryService
        journal = data.get('sourceRecovery', {'attempts': 0, 'pending': None})
        if (type(journal) is not dict or set(journal) != {'attempts', 'pending'} or type(journal['attempts']) is not int
                or not 0 <= journal['attempts'] <= SOURCE_RECOVERY_ATTEMPTS):
            raise DriverHeld('source_recovery_journal_invalid')
        pending = journal['pending']
        if pending is not None:
            try:
                if type(pending) is not dict or set(pending) != {'request', 'evidence'}:
                    raise ValueError()
                ClaimRecoveryService._source_request(pending['request'])
                if (pending['request'].get('schemaVersion')==4
                        and pending['request']['transferRequest']!=data.get('reservation')):
                    raise ValueError()
                if pending['request']['workerEvidenceSha256'] != p.digest(pending['evidence']):
                    raise ValueError()
            except (ValueError, TypeError, KeyError):
                raise DriverHeld('source_recovery_journal_invalid') from None
        return journal

    def _source_unstarted_proof(self, job_id, data, phase):
        """Local proof that no engine was ever told, or a hold; never an adoption.

        Any execution row, in any phase, is a start this path must not judge, and
        an occupied gate or a live task with no row is a start nobody recorded.
        An engine that cannot answer proves nothing, so it waits and spends nothing.
        """
        kind=(self._pipeline_kind(data['claim']['submission']) if phase in SOURCE_UNRESERVED_PHASES else self._kind(job_id))
        if kind != 'odm':
            raise DriverHeld('source_recovery_engine_unsupported')
        try:
            self.worker._row(job_id)
        except WorkerHeld:
            pass
        else:
            raise DriverHeld('source_recovery_execution_exists')
        reservation = data['reservation']['reservationId']
        fenced = self.ledger.transfer_fenced(reservation)
        if not fenced and self._lease_lapsed(data):
            raise DriverHeld('reservation_expired')
        with self.ledger._transaction() as db:
            current = self.ledger._reservation(db, reservation)
            state, allocated = current['state'], current['allocated']
            if phase in SOURCE_UNRESERVED_PHASES:
                settings=json.loads(current['settings'])
                expected_settings=dict(job_id=job_id,generation=data['claim']['generation'],reservation_id=reservation,
                    manifest_sha256=data['claim']['manifestSha256'],input_bytes=data['claim']['submission']['manifest']['totalBytes'],
                    output_budget=self.output_budget,environment=data['claim']['environment'],ttl_seconds=self.transfer_ttl)
                if settings!=expected_settings or current['job']!=job_id or current['generation']!=data['claim']['generation']:
                    raise DriverHeld('source_recovery_ledger_changed')
        # Before the transfer nothing is fenced or written; after it, all of it is.
        expected = (False, 0) if phase in ('awaiting_transfer',)+SOURCE_UNRESERVED_PHASES else (True, data['claim']['submission']['manifest']['totalBytes'])
        if (state != 'reserved' or (fenced, allocated) != expected or self.ledger.result_registered(reservation)
                or (phase in ('awaiting_transfer',)+SOURCE_UNRESERVED_PHASES and data.get('inputPaths'))):
            raise DriverHeld('source_recovery_ledger_changed')
        with NodeGate(self.worker.engine_id) as gate:
            if gate.snapshot()['reservation'] is not None:
                raise DriverHeld('source_recovery_gate_occupied')
        observed, _info = self.worker.task_observation(job_id)
        if observed == 'unreachable':
            raise DriverWait('source_recovery_engine_unreachable')
        if observed != 'missing':
            raise DriverHeld('source_recovery_engine_task_present')
        return dict(engineClass='unstarted', executionRow=False, ledgerState=state, allocatedBytes=allocated,
            gateReservation=None, engineTask=observed)

    def _source_started_proof(self, job_id, data):
        """Local proof of one committed task, or a refusal; polling carries on either way.

        The row, gate, ledger and engine must all name this exact start. Status 20
        and processing time prove nothing about a commit: only a row past the commit,
        or engine output for a row stuck at it, does. Earlier phases never get here,
        since poll refuses them, and nothing on this path can start an engine.
        """
        if self._kind(job_id) != 'odm':
            raise DriverHeld('source_recovery_engine_unsupported')
        reservation, claim = data['reservation']['reservationId'], data['claim']
        try:
            row = self.worker._row(job_id)
        except WorkerHeld:
            raise DriverHeld('source_recovery_execution_missing') from None
        identity = json.loads(row['identity'])
        if identity['reservation'] != reservation or any(identity['job'][key] != claim[key]
                for key in ('jobId', 'generation', 'environment', 'manifestSha256', 'pipelineDigest')):
            raise DriverHeld('source_recovery_execution_changed')
        with NodeGate(self.worker.engine_id) as gate:
            bound = gate.snapshot()['reservation']
        if (bound is None or bound['receiptId'] != row['receipt'] or bound['completionDigest'] is not None
                or (bound['job'] or {}).get('id') != job_id):
            raise DriverHeld('source_recovery_gate_changed')
        with self.ledger._transaction() as db:
            ledger = self.ledger._reservation(db, reservation)['state']
        if ledger != 'engine_started':
            raise DriverHeld('source_recovery_ledger_changed')
        observed, info = self.worker.task_observation(job_id)
        if observed == 'unreachable':
            raise DriverWait('source_recovery_engine_unreachable')
        if observed != 'present' or info['status']['code'] not in (10, 20):
            raise DriverHeld('source_recovery_engine_task_changed')
        if row['phase'] == 'commit_pending':
            output, _more = self.worker.engine.output_page(job_id, line=0, limit=1)
            if not output:
                raise DriverHeld('source_recovery_commit_unproven')
        elif row['phase'] != 'processing':
            raise DriverHeld('source_recovery_execution_changed')
        return dict(engineClass='started', executionPhase=row['phase'], ledgerState=ledger,
            gateReceipt=row['receipt'], engineTask=observed)

    def _source_started_visible(self, job_id, data):
        """Show a running engine as running after a restart. Never stops the poll."""
        if data['reservation'].get('protocol') != 'source-range-v2':
            return
        try:
            state = self._claim_state(job_id, data['claim'])
            if state is not None and self._restart_recoverable(state, data, 'executing'):
                self._recover_source_fetch(job_id, 'executing', data)
        except (DriverHeld, DriverWait, ValueError, KeyError, PolicyConflict):
            pass

    def _source_recovery_state(self, job_id, claim, data):
        from .reference_recovery import SOURCE_STATE_PROTOCOL, SOURCE_UNSTARTED_PHASES
        status, state = self._request('GET', '/jobs/' + job_id + '/recovery-state', claim_token=claim['claimToken'])
        if status != 200:
            raise DriverHeld('source_recovery_state_unavailable')
        try:
            p._closed(state, ('schemaVersion', 'protocol', 'authorityId', 'coordinatorEpoch', 'claim', 'physicalBinding',
                'reservation', 'node', 'eligible', 'sourceTransfer'), 'source recovery state')
            validate_claim_fields(state['claim'])
            p._uuid(state['coordinatorEpoch'], 'recovery epoch')
            p._integer(state['node']['revision'], 'node revision', allow_zero=True)
            expected = dict(reservationId=data['reservation']['reservationId'],
                transferRequestSha256=p.digest(data['reservation']),
                inputBytes=claim['submission']['manifest']['totalBytes'])
            unreserved=state['claim']['executionPhase'] in SOURCE_UNRESERVED_PHASES
            schema = 4 if unreserved else 3 if state['claim']['executionPhase'] in SOURCE_UNSTARTED_PHASES else 2
            if unreserved:expected=None
            if (state['schemaVersion'] != schema or state['protocol'] != SOURCE_STATE_PROTOCOL
                    or state['authorityId'] != self.worker.authority_id or type(state['eligible']) is not bool
                    or state['reservation'] != expected or not self._claim_bound(job_id, claim, state['claim'])
                    or state['eligible'] and type(state['sourceTransfer']) is not dict):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise DriverHeld('source_recovery_state_changed') from None
        return state

    def _recover_source_fetch(self, job_id, stage, data):
        """Ask the coordinator to lift a restart hold; the answer grants nothing.

        The intent is saved before its POST and retried exactly, so an outage or
        lost reply spends no attempt, and an exact receipt clears the count. The
        caller still needs fresh running state, and every range is checked again.
        A hold that stays keeps everything.
        """
        from .reference_recovery import ClaimRecoveryService
        from .source_range_transfer import validate_source_grant
        claim = data['claim']
        suffix = '/jobs/' + job_id + '/recover'
        token = claim['claimToken']
        journal = self._source_recovery_journal(data)
        state = self._source_recovery_state(job_id, claim, data)
        running = state['claim']['status'] == 'running' and state['claim']['heldReason'] is None
        phase = state['claim']['executionPhase']
        # Checked before any POST, a replay included: the intent proves nothing now.
        proof = None
        if phase != 'fetching' and state['eligible']:
            proof = (self._source_started_proof(job_id, data) if stage == 'executing'
                else self._source_unstarted_proof(job_id, data, phase))

        def settle(request):
            status, receipt = self._request('POST', suffix, request, claim_token=token)
            if status == 200 and receipt != ClaimRecoveryService._make_receipt(request):
                raise DriverHeld('source_recovery_receipt_changed')
            if status != 200 and not running:
                return False
            journal['pending'] = None
            if status == 200:
                # The cap counts unresolved attempts in a row, not restarts in a
                # job's life. Only this counter: every source budget runs on.
                journal['attempts'] = 0
            data['sourceRecovery'] = journal
            self._save(job_id, stage, data)
            return True

        pending = journal['pending']
        if pending is not None:
            request = pending['request']
            current = (request['coordinatorEpoch'], request['expectedNodeRevision'], request['sourceClosureSha256']) == (
                state['coordinatorEpoch'], state['node']['revision'], (state['sourceTransfer'] or {}).get('closureSha256'))
            if running or current:
                if settle(request):
                    return
                if not state['eligible']:
                    raise DriverHeld('source_recovery_ineligible')
                raise DriverWait('source_recovery_refused')
            if not state['eligible']:
                raise DriverHeld('source_recovery_ineligible')
            # The epoch, node or closure moved on, so that request can never commit
            # now, but it may have before. Its replay returns a stored receipt and
            # nothing else: that resolves it, and a refusal leaves it unresolved.
            if not settle(request):
                journal['pending'] = None
                data['sourceRecovery'] = journal
                self._save(job_id, stage, data)
        if running:
            return
        if not state['eligible']:
            raise DriverHeld('source_recovery_ineligible')
        reservation = data['reservation']['reservationId']
        fenced = self.ledger.transfer_fenced(reservation)
        # An unfenced lease that lapsed can never begin its transfer, so resuming
        # the claim would only move the same hold past a coordinator round trip.
        if not fenced and self._lease_lapsed(data):
            raise DriverHeld('reservation_expired')
        if journal['attempts'] >= SOURCE_RECOVERY_ATTEMPTS:
            raise DriverHeld('source_recovery_attempts_exhausted')
        ranges = committed = 0
        grant_sha = None
        if 'grant' in data:
            checked = validate_source_grant(data['grant'], job=claim, clock=self.clock,
                                            allow_expired=True, past_reservation=True)
            ranges, committed = self.source_range_client.committed_progress(checked.identity)
            grant_sha = p.digest(data['grant'])
        sequence = data.get('sourceRenewalSequence', 0)
        evidence = dict(schemaVersion=1, reservationId=reservation, transferFenced=fenced,
            grantSha256=grant_sha, renewalSequence=sequence, committedRanges=ranges, committedBytes=committed,
            inputObjects=len(data.get('inputPaths', {})), **(proof or {}))
        binding = {key: state['claim'][key] for key in ('jobId', 'generation', 'environment', 'nodeId', 'workerId',
            'workerInstanceId', 'authorityId', 'claimRequestId', 'claimTokenSha256', 'manifestSha256', 'pipelineDigest')}
        reservation_binding=state['reservation'] or dict(reservationId=data['reservation']['reservationId'],
            inputBytes=claim['submission']['manifest']['totalBytes'],transferRequestSha256=p.digest(data['reservation']))
        binding.update(reservationId=reservation_binding['reservationId'], inputBytes=reservation_binding['inputBytes'])
        request = dict(schemaVersion=2, requestId=str(uuid4()), coordinatorEpoch=state['coordinatorEpoch'],
            expectedNodeRevision=state['node']['revision'], claimBinding=binding,
            transferRequestSha256=reservation_binding['transferRequestSha256'],
            sourceClosureSha256=state['sourceTransfer']['closureSha256'], grantSha256=grant_sha,
            renewalSequence=sequence, committedRanges=ranges, committedBytes=committed,
            workerEvidenceSha256=p.digest(evidence))
        if proof is not None:
            request.update(schemaVersion=3, executionPhase=phase, engineClass=proof['engineClass'])
        if state['schemaVersion']==4:
            request.update(schemaVersion=4,transferRequest=data['reservation'])
        ClaimRecoveryService._source_request(request)
        journal.update(attempts=journal['attempts'] + 1, pending=dict(request=request, evidence=evidence))
        data['sourceRecovery'] = journal
        self._save(job_id, stage, data)
        if not settle(request):
            raise DriverWait('source_recovery_refused')

    def _unstarted(self, job_id, stage, data, reason):
        """A claim that can no longer start ends only on the coordinator's word.

        The step lock proves no transfer is running, and a reserved ledger row
        proves no engine was told, so the job is the one doubt left. Only the exact
        terminal word for this claim settles it; until then, outage included, the
        row stays held under its named reason and a fence keeps its capacity. An
        operator ends the job, then this ends the row, unless a crash left a live
        physical intent: gate.resolve is the operator's step, so until it happens
        the row, its capacity and the reported hold all stay. A started engine is
        not this path's to judge and keeps its own reconciliation.
        """
        reservation = data['reservation']['reservationId']
        # Saved first so a death after settling replays under this same reason.
        if data.get('unstartedHold') != reason:
            data['unstartedHold'] = reason
            self._save(job_id, stage, data)
        status = self._terminal_status(job_id, data)
        if status is None:
            raise DriverHeld(reason)
        if self.ledger.never_started(reservation):
            record = data.get('hold') or data.get('watch') or dict(holdId=str(uuid4()), sequence=-1)
            fenced = self._physical_fence(job_id, stage, data, record,
                {'jobId': job_id, 'reason': reason, 'terminalStatus': status})
            if fenced is not None:
                return fenced
        # The ledger decides, not this row: poll commits a result before the row hears.
        if self.ledger.settle_unstarted(reservation, transfer_ended=True) not in (None, 'expired', 'unstarted'):
            raise DriverHeld('engine_reconciliation_required')
        data['unstartedOutcome'] = dict(reason=reason, stage=stage, terminalStatus=status)
        self._save(job_id, 'completed', data)
        return {'state': 'unstarted_terminal', 'jobId': job_id, 'reason': reason}

    def reconcile_unstarted(self, job_id):
        """Operator entry: end one never-started claim row once its job has ended."""
        if not self._lock.acquire(blocking=False):
            return {'state': 'busy'}
        try:
            with self._db() as db:
                row = db.execute('SELECT * FROM driver_jobs WHERE job=?', (job_id,)).fetchone()
            data = json.loads(row['data']) if row is not None else {}
            if 'reservation' not in data or 'claim' not in data or row['stage'] == 'completed':
                raise DriverHeld('unstarted_reconciliation_unavailable')
            return self._unstarted(job_id, row['stage'], data, 'unstarted_claim_requires_reconciliation')
        except DriverHeld as error:
            return {'state': 'held', 'reason': str(error)}
        except DriverWait as error:
            return {'state': 'retry_wait', 'reason': str(error)}
        finally:
            self._lock.release()

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
            if (gate.resolved(local['receipt'], job_id)
                    or self._gate_ack_matches(gate.snapshot(), local['receipt'], job_id, self._failure_completion(data['failure']))):
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
                if not gate.resolved(local['receipt'], job_id) and not self._gate_ack_matches(gate.snapshot(), local['receipt'], job_id, completion):
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
        if 'claim' in data:
            self._active_job = job_id
        if 'hold' in data:
            return self._held_step(job_id, stage, data)
        if 'watch' in data:
            closed = self._watch_step(job_id, stage, data)
            if closed is not None:
                return closed
        if stage == 'claim_pending':
            if self.execution_router is not None or self.combined_preparation is not None:
                self._advice(data.get('execution', {}), data.get('execution'))
            if not self._ready():
                return {'state': 'not_ready', 'jobId': job_id}
            status, claim = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
            if status == 409:
                claim = self._overtaken_reply(job_id, request_id)
                if claim is None:
                    return {'state': 'claim_pending', 'jobId': job_id}
            if self._claim_retirable(job_id, data, claim):
                # The coordinator has made this request unadmittable and nothing
                # local exists. Without this the row would be selected first
                # forever and starve the next job. Timeouts never reach here.
                with self._db() as db:
                    db.execute("DELETE FROM driver_jobs WHERE job=? AND stage='claim_pending'", (job_id,))
                return {'state': 'claim_dropped', 'jobId': job_id}
            recovered_claim=False
            if 'claimState' in claim or claim['status'] != 'running':
                # Exact claim replay may return the original token after its
                # lost reply. Persist only that authenticated original identity;
                # schema-4 recovery, not this replay, can resume it.
                if ('claimState' in claim or not self.source_range_v2_enabled
                        or self.combined_preparation is not None or claim.get('sourceIntakeVersion')!=2
                        or self._pipeline_kind(claim['submission'])!='odm'):
                    raise DriverHeld('unstarted_claim_requires_reconciliation')
                state=self._claim_state(job_id,claim)
                if (state is None or (state['status'],state['heldReason'])!=('held','coordinator_restart')
                        or state['executionPhase'] not in SOURCE_UNRESERVED_PHASES
                        or claim.get('transferRequest') is not None):
                    raise DriverHeld('unstarted_claim_requires_reconciliation')
                self._validate_job(claim)
                recovered_claim=True
            if self.combined_preparation is None:
                # The coordinator shows running from here, so a bind refusal must be reportable.
                data['claim'] = claim
                self._save(job_id, 'claim_pending', data)
                self._active_job = job_id
            if recovered_claim:
                # New execution routes require running. Bind only after the
                # recovery CAS and fresh original-claim replay prove that state.
                self._save(job_id,'claimed',data)
                return {'state':'claimed','jobId':job_id}
            self._bind_claim(claim, data)
            if self.combined_preparation is not None:
                if 'reservation' in data and 'originalCombinedClaim' not in data:raise DriverHeld('combined_claim_intent_binding_missing')
                original={key:claim[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest')}
                original['claimTokenSha256']=hashlib.sha256(claim['claimToken'].encode()).hexdigest()
                if 'originalCombinedClaim' in data and p.canonical_json(data['originalCombinedClaim'])!=p.canonical_json(original):raise DriverHeld('original_combined_claim_response_changed')
                data['originalCombinedClaim']=original
                if 'reservation' not in data:
                    data['reservation']=self._new_transfer_request(claim)
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
        if 'reservation' in data and self.ledger.unstarted_settled(data['reservation']['reservationId']):
            # The ledger settled and the row never heard: nothing else can move
            # this row, and only the terminal word that settled it ends it here.
            return self._unstarted(job_id, stage, data,
                data.get('unstartedHold', 'unstarted_claim_requires_reconciliation'))
        if stage in ('claimed', 'reservation_pending'):
            if stage == 'claimed':
                data['reservation'] = self._new_transfer_request(claim)
                self._save(job_id, 'reservation_pending', data)
            request = data['reservation']
            self.ledger.reserve(job_id=job_id, generation=claim['generation'], reservation_id=request['reservationId'],
                manifest_sha256=claim['manifestSha256'], input_bytes=claim['submission']['manifest']['totalBytes'], output_budget=self.output_budget,
                environment=claim['environment'], ttl_seconds=self.transfer_ttl, worker_ready=True)
            self._clear_suspect(job_id, 'reservation_pending', data)
            self._clear_watch(job_id, 'reservation_pending', data)
            status, public = self._request('POST', suffix + '/reservation', {'claimToken': token, 'transferRequest': request})
            if status == 409:
                self._source_refused(job_id, 'reservation_pending', data)
                return {'state': 'reservation_pending', 'jobId': job_id}
            if claim['status']!='running':
                # A recovery may have committed before this worker died. Route
                # creation still waits for fresh exact original-claim state.
                status,fresh=self._request('POST','/claim',{'jobId':job_id,'claimRequestId':request_id})
                state=None if status!=200 else self._claim_state(job_id,fresh)
                if state is None or state['status']!='running' or state['executionPhase']!='awaiting_transfer':
                    raise DriverHeld('source_execution_not_admitted')
                self._bind_claim(fresh,data)
                claim=fresh
                data['claim']=claim
            claim.update(public)
            self._save(job_id, 'awaiting_grant', data)
            return {'state': 'awaiting_grant', 'jobId': job_id}
        if stage == 'awaiting_grant':
            status, grant = self._request('GET', suffix + '/transfer-grant', claim_token=token)
            if status == 409:
                self._source_refused(job_id, 'awaiting_grant', data)
                return {'state': 'awaiting_grant', 'jobId': job_id}
            if (claim.get('transferRequest') or {}).get('protocol') == 'source-range-v2':
                if self.source_range_client is None:
                    raise DriverHeld('source_range_client_not_enabled')
                from .source_range_transfer import validate_source_grant
                validated = validate_source_grant(grant, job=claim, clock=self.clock)
            else:
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
                self._source_refused(job_id, stage, data)
                return {'state': stage, 'jobId': job_id}
            claim.update(public)
            self._save(job_id, 'fetching' if phase == 'fetching' else 'execution_pending', data)
            return {'state': 'fetching' if phase == 'fetching' else 'execution_pending', 'jobId': job_id}
        if stage == 'fetching':
            source_range_v2 = (claim.get('transferRequest') or {}).get('protocol') == 'source-range-v2'
            if source_range_v2:
                if self.source_range_client is None:
                    raise DriverHeld('source_range_client_not_enabled')
                grant = None
            else:
                grant = validate_grant(data['grant'], job=claim, clock=self.clock)
            reservation = data['reservation']['reservationId']
            directory = self.ledger.prepare_job_directory(reservation)
            paths = dict(data.get('inputPaths', {}))
            try:
                self.ledger.begin_transfer(reservation)
            except LeaseLapsed:
                return self._unstarted(job_id, stage, data, 'reservation_expired')
            for obj in claim['submission']['manifest']['objects']:
                if obj['id'] in paths:
                    continue
                relative = 'jobs/' + reservation + '/' + obj['id'] + '.input'
                self.ledger._owned_path(reservation, relative)
                target = self.ledger.root / relative
                if os.path.lexists(target):
                    self.worker._file(reservation, relative, obj['byteSize'], obj['sha256'])
                else:
                    if source_range_v2:
                        refusal = self._fetch_refusal(job_id, stage, data, source_range_v2)
                        if refusal is not None:
                            return refusal
                        grant = self._source_range_grant(job_id, claim, data)
                        from .source_range_transfer import SourceRenewalInvalid, SourceRenewalUnavailable
                        def renew_during_transfer():
                            try:
                                return self._source_range_grant(job_id, claim, data)
                            except DriverWait:
                                raise SourceRenewalUnavailable('source renewal deferred') from None
                            except DriverHeld:
                                raise SourceRenewalInvalid('source renewal rejected') from None
                        try:
                            self.source_range_client.download_object(grant=grant, object_id=obj['id'],
                                destination=target, grant_provider=renew_during_transfer)
                        except SourceRenewalUnavailable:
                            raise DriverWait('source_renewal_endpoint_unavailable') from None
                        except SourceRenewalInvalid:
                            raise DriverHeld('source_renewal_response_invalid') from None
                    else:
                        attempts = data.setdefault('fetchAttempts', {}).get(obj['id'], 0)
                        if attempts >= 3:
                            return self._unstarted(job_id, stage, data, 'worker_fetch_attempt_budget_exhausted')
                        refusal = self._fetch_refusal(job_id, stage, data, source_range_v2)
                        if refusal is not None:
                            return refusal
                        data['fetchAttempts'][obj['id']] = attempts + 1
                        self._save(job_id, stage, data)
                        self.broker.download_object(grant, obj['id'], target)
                paths[obj['id']] = relative
                data['inputPaths'] = paths
                self.ledger.record_allocated(reservation, sum(item['byteSize'] for item in claim['submission']['manifest']['objects'] if item['id'] in paths))
                self._save(job_id, 'fetching', data)
                self._clear_transfer_watch(job_id, 'fetching', data)
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
                try:
                    self.ledger.claim_engine_lease(data['reservation']['reservationId'])
                except LeaseLapsed:
                    return self._unstarted(job_id, stage, data, 'reservation_expired')
                status, fresh = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
                if status != 409 and (claim.get('transferRequest') or {}).get('protocol') == 'source-range-v2':
                    state = self._claim_state(job_id, claim)
                    if state is None:
                        raise DriverHeld('source_execution_not_admitted')
                    if self._restart_recoverable(state, data, stage):
                        self._recover_source_fetch(job_id, stage, data)
                        return {'state': stage, 'jobId': job_id}
                    if state['status'] == 'running' and state['executionPhase'] != 'processing':
                        raise DriverHeld('source_execution_ahead_of_coordinator')
                if status == 409 or fresh['status'] != 'running':
                    return self._unstarted(job_id, stage, data, 'unstarted_claim_requires_reconciliation')
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
                try:
                    adapter.start(fresh, **arguments)
                except WorkerHeld as error:
                    if str(error) == 'reservation_expired':
                        raise DriverHeld('reservation_expired') from None
                    # Refused before any engine mutation is retried as before; an
                    # execution row left behind is what becomes init-uncertain.
                    self._observe(job_id, 'execution_pending', data, 'engine_start_refused')
                    raise
            self._save(job_id, 'executing', data)
            return {'state': 'executing', 'jobId': job_id}
        if stage == 'executing':
            adapter = self._adapter(job_id, for_execution=True)
            tree = self._kind(job_id) == 'treedar'
            local = None if tree else adapter._row(job_id)
            if not tree and local['phase'] in ('commit_pending', 'processing', 'archive_open_pending', 'archive_pending'):
                terminal = self._engine_task(job_id, data)
                code = terminal['status'].get('code')
                if type(code) is int and code in (30, 50) and local['phase'] in ('commit_pending', 'processing'):
                    self._seal_failure(job_id, data, code)
                    return {'state': 'failure_delivering', 'jobId': job_id}
            try:
                outbox_id = adapter.poll(job_id)
            except WorkerHeld as error:
                if str(error) in OVER_BUDGET_ARCHIVES:
                    raise DriverHeld('result_over_budget') from None
                if not tree and adapter._row(job_id)['phase'] in ('archive_pending', 'archive_open_pending'):
                    return {'state': 'retry_wait', 'jobId': job_id}
                if not tree and adapter._row(job_id)['phase'] in ('prepared', 'initialize_pending', 'upload_pending'):
                    raise DriverHeld('engine_init_uncertain') from None
                raise DriverHeld('engine_reconciliation_required') from None
            self._clear_watch(job_id, 'executing', data)
            if outbox_id is None:
                if not tree:
                    self._source_started_visible(job_id, data)
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
            if self._observation_deferred(claim, data):
                return {'state': 'awaiting_acceptance', 'jobId': job_id}
            status, relayed = self._request('GET', suffix + '/acceptance', claim_token=token)
            if status == 409:
                return {'state': 'awaiting_acceptance', 'jobId': job_id}
            observed = self._apply_acceptance(job_id, relayed['receipts'])
            if observed['fullyAccepted']:
                self._confirm_acceptance_observed(job_id, data, relayed['receipts'])
                if (data['claim'].get('sourceIntakeVersion') == 2 and self.engine_cleanup is not None
                        and self._kind(job_id) == 'odm'):
                    self.engine_cleanup.remove(job_id, viewer_receipts=relayed['receipts'])
                self._save(job_id, 'completed', data)
                return {'state': 'completed', 'jobId': job_id}
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        raise DriverHeld('unknown_worker_driver_stage')
