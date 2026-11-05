"""Explicit synthetic joint-fixture driver; never a production worker daemon.

Control metadata, broker reads and durable delivery use real verified HTTPS.
Only the injected engine is synthetic. State survives fixture/driver restarts;
uncertain engine mutations retain the controller's existing reconciliation hold.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import ssl
import stat
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

from processing import reference_protocol as p
from processing.reference_engine_worker import WorkerHeld
from processing.reference_sender import ReferenceSender
from processing.reference_transfer import BrokerClient, validate_grant
from processing.scratch_policy import LeaseLapsed


PREFIX = '/ltds/worker/v1'
INPUT_BYTES = 3
OUTPUT_BUDGET = 1024 * 1024
TTL_SECONDS = 240  # Synthetic fixture stays within broker's five-minute policy.
INPUT_SHA256 = hashlib.sha256(b'abc').hexdigest()


class DriverHeld(ValueError):
    pass


class DriverWait(ValueError):
    pass


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise DriverWait('coordinator_redirect_rejected')


class JointDriver:
    def __init__(self, store, worker_api, worker, ledger, outbox, broker,
                 coordinator_origin, worker_bearer, worker_id, node_id, ssl_context,
                 *, state_path, synthetic_enabled=False, clock=time.time):
        if (synthetic_enabled is not True or getattr(worker.engine, 'synthetic_fixture', False) is not True
                or not isinstance(broker, BrokerClient)):
            raise ValueError('explicit synthetic engine and actual broker client required')
        if (worker.node_id != node_id or worker.worker_id != worker_id
                or worker.ledger is not ledger or worker.outbox is not outbox):
            raise ValueError('joint driver fixed worker binding differs')
        parsed = urlsplit(coordinator_origin)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in ('', '/') or parsed.query
                or parsed.fragment or parsed.port == 0 or '\\' in coordinator_origin
                or any(ord(char) <= 32 for char in coordinator_origin)):
            raise ValueError('configured HTTPS coordinator origin required')
        if (not isinstance(ssl_context, ssl.SSLContext) or not ssl_context.check_hostname
                or ssl_context.verify_mode != ssl.CERT_REQUIRED):
            raise ValueError('verified fixture TLS context required')
        if (type(worker_bearer) is not str or not 16 <= len(worker_bearer) <= 4096
                or not all(0x21 <= ord(char) <= 0x7e for char in worker_bearer)):
            raise ValueError('invalid private fixture credential')
        # Accepted for fixture assembly/binding compatibility; step() never calls
        # these objects directly instead of using the actual HTTPS boundary.
        self.store, self.worker_api = store, worker_api
        self.worker, self.ledger, self.outbox, self.broker = worker, ledger, outbox, broker
        self.origin, self._bearer = coordinator_origin.rstrip('/'), worker_bearer
        self.node_id, self.worker_id, self.clock = node_id, worker_id, clock
        self._lock = threading.Lock()
        self.path = Path(state_path).absolute()
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if any(path.is_symlink() for path in (self.path, *self.path.parents)):
            raise ValueError('private fixture journal required')
        if os.name != 'nt':
            parent = self.path.parent.stat()
            if parent.st_uid != os.geteuid() or parent.st_mode & 0o077:
                raise ValueError('private owned fixture journal parent required')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        try:
            observed, current = os.fstat(descriptor), self.path.lstat()
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != 'nt' and observed.st_uid != os.geteuid())):
                raise ValueError('owned regular fixture journal required')
            if os.name != 'nt':
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        self._open = build_opener(ProxyHandler({}), _NoRedirects(), HTTPSHandler(context=ssl_context)).open
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS driver_jobs (
              created INTEGER PRIMARY KEY AUTOINCREMENT, job TEXT UNIQUE NOT NULL,
              claim_request TEXT UNIQUE NOT NULL, stage TEXT NOT NULL, data TEXT NOT NULL)''')
            db.execute('CREATE TABLE IF NOT EXISTS driver_binding (singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)')
            binding = p.canonical_json(dict(origin=self.origin, nodeId=node_id, workerId=worker_id)).decode()
            old = db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()
            if old and old[0] != binding:
                raise ValueError('fixture driver ownership changed')
            db.execute('INSERT OR IGNORE INTO driver_binding VALUES(1,?)', (binding,))
        self.sender = ReferenceSender(outbox, self.origin, bearer_token=worker_bearer,
            claim_binding_provider=self._claim_binding, ssl_context=ssl_context, timeout=10)

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

    @staticmethod
    def _synthetic(job):
        manifest = job['submission']['manifest']
        if (job['environment'] != 'staging' or manifest['totalBytes'] != INPUT_BYTES
                or len(manifest['objects']) != 1 or manifest['objects'][0]['kind'] != 'image'
                or manifest['objects'][0]['byteSize'] != INPUT_BYTES
                or manifest['objects'][0]['sha256'] != INPUT_SHA256):
            raise DriverHeld('fixture_requires_synthetic_abc_input')

    def _claim_binding(self, binding):
        with self._db() as db:
            row = db.execute('SELECT * FROM driver_jobs WHERE job=?', (binding['jobId'],)).fetchone()
        if row is None:
            raise DriverHeld('unknown_fixture_claim')
        data = json.loads(row['data'])
        job = data['claim']
        if tuple(binding[key] for key in ('generation', 'environment', 'inputManifestSha256', 'pipelineDigest')) != (
                job['generation'], job['environment'], job['manifestSha256'], job['pipelineDigest']):
            raise DriverHeld('fixture_claim_binding_changed')
        return {'claimRequestId': row['claim_request'], 'claimToken': job['claimToken']}

    def step(self):
        """Perform one bounded stage; never return private metadata or paths."""
        if not self._lock.acquire(blocking=False):
            return {'state': 'busy'}
        try:
            return self._step()
        except DriverHeld as error:
            return {'state': 'held', 'reason': str(error)}
        except DriverWait as error:
            return {'state': 'retry_wait', 'reason': str(error)}
        except Exception:
            return {'state': 'retry_wait', 'reason': 'driver_step_unavailable'}
        finally:
            self._lock.release()

    def _step(self):
        with self._db() as db:
            row = db.execute("SELECT * FROM driver_jobs WHERE stage!='completed' ORDER BY created LIMIT 1").fetchone()
        if row is None:
            _, next_job = self._request('GET', '/next')
            head = next_job.get('job')
            if head is None:
                return {'state': 'idle'}
            if head['environment'] != 'staging':
                raise DriverHeld('fixture_accepts_staging_only')
            job_id = head['jobId']
            p._slug(job_id, 'job ID')
            request_id = 'fixture-' + secrets.token_hex(16)
            with self._db() as db:
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claim_pending','{}')", (job_id, request_id))
            return {'state': 'claim_pending', 'jobId': job_id}
        job_id, request_id, stage = row['job'], row['claim_request'], row['stage']
        data = json.loads(row['data'])
        if stage == 'claim_pending':
            status, claim = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
            if status == 409:
                return {'state': 'claim_pending', 'jobId': job_id}
            self._synthetic(claim)
            if claim['status'] != 'running':
                raise DriverHeld('unstarted_claim_requires_reconciliation')
            data['claim'] = claim
            self._save(job_id, 'claimed', data)
            return {'state': 'claimed', 'jobId': job_id}
        claim = data['claim']
        token = claim['claimToken']
        suffix = '/jobs/' + job_id
        if stage in ('claimed', 'reservation_pending'):
            if stage == 'claimed':
                reservation = 'fixture-' + secrets.token_hex(16)
                data['reservation'] = dict(reservationId=reservation, generation=claim['generation'],
                    expiresAt=datetime.fromtimestamp(self.clock() + TTL_SECONDS, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                    manifestSha256=claim['manifestSha256'], byteBudget=3 * INPUT_BYTES, nonce=secrets.token_hex(16))
                self._save(job_id, 'reservation_pending', data)
            request = data['reservation']
            self.ledger.reserve(job_id=job_id, generation=claim['generation'], reservation_id=request['reservationId'],
                manifest_sha256=claim['manifestSha256'], input_bytes=INPUT_BYTES, output_budget=OUTPUT_BUDGET,
                environment='staging', ttl_seconds=TTL_SECONDS, worker_ready=True)
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
            validate_grant(grant, job=claim, clock=self.clock)
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
            grant = validate_grant(data['grant'], job=claim, clock=self.clock)
            reservation = data['reservation']['reservationId']
            directory = self.ledger.root / 'jobs' / reservation
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            paths = {}
            for obj in claim['submission']['manifest']['objects']:
                relative = 'jobs/' + reservation + '/' + obj['id'] + '.input'
                self.ledger._owned_path(reservation, relative)
                target = self.ledger.root / relative
                if os.path.lexists(target):
                    self.worker._file(reservation, relative, obj['byteSize'], obj['sha256'])
                else:
                    attempts = data.get('fetchAttempts', 0)
                    if attempts >= 3:
                        raise DriverHeld('fixture_fetch_attempt_budget_exhausted')
                    try:
                        self.ledger.begin_transfer(reservation)
                    except LeaseLapsed:
                        raise DriverHeld('reservation_expired') from None
                    data['fetchAttempts'] = attempts + 1
                    self._save(job_id, stage, data)
                    self.broker.download_object(grant, obj['id'], target)
                paths[obj['id']] = relative
            self.ledger.record_allocated(reservation, INPUT_BYTES)
            data['inputPaths'] = paths
            self._save(job_id, 'processing_pending', data)
            return {'state': 'processing_pending', 'jobId': job_id}
        if stage == 'execution_pending':
            try:
                self.worker._row(job_id)
            except WorkerHeld:
                status, fresh = self._request('POST', '/claim', {'jobId': job_id, 'claimRequestId': request_id})
                if status == 409 or fresh['status'] != 'running':
                    raise DriverHeld('unstarted_claim_requires_reconciliation')
                self._synthetic(fresh)
                data['claim'] = fresh
                self._save(job_id, 'execution_pending', data)
                self.worker.start(fresh, reservation_id=data['reservation']['reservationId'], input_paths=data['inputPaths'])
            self._save(job_id, 'executing', data)
            return {'state': 'executing', 'jobId': job_id}
        if stage == 'executing':
            try:
                outbox_id = self.worker.poll(job_id)
            except WorkerHeld:
                if self.worker._row(job_id)['phase'] in ('archive_pending', 'archive_open_pending'):
                    return {'state': 'retry_wait', 'jobId': job_id}
                raise DriverHeld('engine_reconciliation_required') from None
            if outbox_id is None:
                return {'state': 'executing', 'jobId': job_id}
            data['outboxId'] = outbox_id
            self._save(job_id, 'delivering', data)
            return {'state': 'delivering', 'jobId': job_id, 'outboxId': outbox_id}
        if stage == 'delivering':
            completion = self.outbox.get(data['outboxId'])
            if completion['state'] != 'acknowledged':
                sent = self.sender.send_next()
                if sent['state'] != 'acknowledged':
                    return {'state': 'delivering', 'jobId': job_id, 'deliveryState': sent['state']}
            self.worker.acknowledge_delivery(job_id)
            self._save(job_id, 'awaiting_acceptance', data)
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        if stage == 'awaiting_acceptance':
            status, relayed = self._request('GET', suffix + '/acceptance', claim_token=token)
            if status == 409:
                return {'state': 'awaiting_acceptance', 'jobId': job_id}
            observed = self.worker.apply_viewer_acceptance(job_id, relayed['receipts'])
            if observed['fullyAccepted']:
                self._save(job_id, 'completed', data)
                return {'state': 'completed', 'jobId': job_id}
            return {'state': 'awaiting_acceptance', 'jobId': job_id}
        raise DriverHeld('unknown_fixture_driver_stage')
