"""Durable reference metadata alongside the single authoritative jobs queue.

No file transfer, execution, cleanup or credential storage occurs here. Capacity
reservation is an external precondition of reserve(); uncertain reservations
require reconciliation by its caller, never an automatic engine-lock release.
"""
import json
import hashlib
import hmac
import os
from pathlib import Path
import re
import secrets
import stat
import time
import uuid
from datetime import datetime, timezone

from .maintenance import Conflict
from . import reference_protocol as protocol
from .viewer_result_manifest import derive_viewer_result_manifest, validate_viewer_acceptance

PROFILE = "reference-pipeline-v1"
PHASES = {
    "queued": {"waiting_capacity", "awaiting_transfer", "cancelled", "held"},
    "waiting_capacity": {"awaiting_transfer", "cancelled", "held"},
    "awaiting_transfer": {"fetching", "cancelled", "held"},
    "fetching": {"processing", "held"},
    "processing": {"waiting_for_delivery", "held"},
    "waiting_for_delivery": {"awaiting_acceptance", "held"},
    "awaiting_acceptance": {"completed", "held"},
    "held": set(), "completed": set(), "cancelled": set(),
}
_NORMALIZED = {"schemaVersion", "submissionId", "manifest", "pipeline", "capabilityFingerprint",
               "environment", "requestDigest", "manifestSha256", "pipelineDigest"}


class ViewerTransferJobConflict(Conflict):
    """A particular sealed result requires reconciliation before transfer."""


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


def _reason(value):
    if value is not None and (type(value) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", value)):
        raise ValueError("error must be a bounded reason code")


class ReferenceStore:
    def __init__(self, queue, max_queued_jobs=10_000, max_metadata_bytes=64 * 1024 * 1024,
                 *, local_max_photos=1000, local_max_input_bytes=20 * 1024**3,
                 main_node_id=None, viewer_required_environments=(),
                 node_inventory=None, node_readiness=None):
        if PROFILE not in queue.allowed_profiles:
            raise ValueError("reference queue profile is disabled")
        for value in (max_queued_jobs, max_metadata_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("metadata quotas must be positive integers")
        self.queue = queue
        self.node_inventory = tuple(node_inventory or ())
        self.node_readiness = node_readiness
        if self.node_inventory:
            if (not queue.per_node_slots or not callable(node_readiness)
                    or tuple(n['id'] for n in self.node_inventory) != queue.node_ids):
                raise ValueError('reference inventory requires matching per-node queue and readiness')
            for spec in self.node_inventory:
                limit = spec.get('maxPhotos')
                if (limit is not None and (type(limit) is not int or limit < 1)
                        or not isinstance(spec.get('capabilities'), (list, tuple))):
                    raise ValueError('invalid reference inventory capability or limit')
        if not isinstance(viewer_required_environments, (tuple, list, set, frozenset)):
            raise ValueError('invalid Viewer acceptance environments')
        self.viewer_required_environments = frozenset(viewer_required_environments)
        for environment in self.viewer_required_environments:
            protocol._environment(environment)
        self.max_queued_jobs = max_queued_jobs
        self.max_metadata_bytes = max_metadata_bytes
        for value in (local_max_photos, local_max_input_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("local node limits must be positive integers")
        self.local_max_photos = local_max_photos
        self.local_max_input_bytes = local_max_input_bytes
        self.main_node_id = main_node_id or next((n for n in queue.node_ids if n != 'cluster'), None)
        if self.main_node_id is not None and (self.main_node_id not in queue.node_ids or self.main_node_id == 'cluster'):
            raise ValueError("main node must be a configured node distinct from cluster")
        database = Path(queue.path).absolute()
        if database.resolve() != database or database.is_symlink():
            raise ValueError("owned regular queue database required")
        initial = database.lstat()
        if not stat.S_ISREG(initial.st_mode) or (os.name != "nt" and initial.st_uid != os.geteuid()):
            raise ValueError("owned regular queue database required")
        descriptor = os.open(database, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        try:
            observed, current = os.fstat(descriptor), database.lstat()
            if (not stat.S_ISREG(observed.st_mode)
                    or (observed.st_dev, observed.st_ino) != (current.st_dev, current.st_ino)
                    or (os.name != "nt" and observed.st_uid != os.geteuid())):
                raise ValueError("owned regular queue database required")
            if os.name != "nt":
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        with queue._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS reference_jobs (
                  job_id TEXT PRIMARY KEY REFERENCES jobs(id), submission_id TEXT UNIQUE NOT NULL,
                  request_digest TEXT NOT NULL, environment TEXT NOT NULL, input_json TEXT NOT NULL,
                  manifest_sha TEXT NOT NULL, pipeline_digest TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 1,
                  phase TEXT NOT NULL, node_id TEXT, transfer_json TEXT, result_json TEXT, error TEXT,
                  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_acceptances (
                  receipt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES reference_jobs(job_id),
                  digest TEXT NOT NULL, receipt_json TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_viewer_receipts (
                  receipt_id TEXT PRIMARY KEY, storage_receipt_id TEXT NOT NULL UNIQUE,
                  acceptance_receipt_id TEXT NOT NULL UNIQUE REFERENCES reference_acceptances(receipt_id),
                  job_id TEXT NOT NULL REFERENCES reference_jobs(job_id), digest TEXT NOT NULL,
                  receipt_json TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_viewer_transfers (
                  job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id),
                  receipt_id TEXT UNIQUE NOT NULL, acceptance_json TEXT NOT NULL,
                  viewer_manifest_json TEXT,
                  attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                  lease_token TEXT, lease_until REAL, last_error TEXT,
                  completed_at TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_outbox (
                  identity TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL,
                  digest TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                  acknowledged INTEGER NOT NULL DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_claims (
                  request_id TEXT PRIMARY KEY, job_id TEXT UNIQUE NOT NULL REFERENCES reference_jobs(job_id),
                  node_id TEXT NOT NULL, worker_id TEXT NOT NULL, instance_id TEXT NOT NULL,
                  token TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_worker_receipts (
                  outbox_id TEXT PRIMARY KEY, payload_sha TEXT NOT NULL,
                  job_id TEXT NOT NULL, receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reference_transfer_grants (
                  job_id TEXT PRIMARY KEY, grant_json TEXT NOT NULL, digest TEXT NOT NULL);
            """)
            transfer_columns = {row[1] for row in db.execute('PRAGMA table_info(reference_viewer_transfers)')}
            if 'viewer_manifest_json' not in transfer_columns:
                db.execute('ALTER TABLE reference_viewer_transfers ADD COLUMN viewer_manifest_json TEXT')
            if 'verified_full_coverage' not in transfer_columns:
                # Older completed transfer rows lack independently established
                # full Viewer coverage and require reconciliation.
                db.execute('ALTER TABLE reference_viewer_transfers ADD COLUMN verified_full_coverage INTEGER')
            job_columns = {row[1] for row in db.execute('PRAGMA table_info(reference_jobs)')}
            if 'viewer_receipt_required' not in job_columns:
                # Legacy NULL is deliberately ambiguous until reconciled.
                db.execute('ALTER TABLE reference_jobs ADD COLUMN viewer_receipt_required INTEGER')
            # Preserve execution phase across restart. Queue's held status is an
            # overlay; it neither authorizes replay nor erases worker progress.

    def _row(self, db, job_id):
        has_failure = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone()
        failure_columns = 'f.failure_json AS terminal_failure_json,f.delivery_state AS failure_delivery_state' if has_failure else 'NULL AS terminal_failure_json,NULL AS failure_delivery_state'
        failure_join = 'LEFT JOIN reference_worker_failures f ON f.job_id=r.job_id AND f.generation=r.generation' if has_failure else ''
        row = db.execute(f"""SELECT r.*,j.status,j.result AS queue_result,j.claim_hash,j.held_reason,c.token AS private_claim_token,
          {failure_columns},
          g.grant_json AS private_grant_json
          FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
          LEFT JOIN reference_claims c ON c.job_id=r.job_id
          LEFT JOIN reference_transfer_grants g ON g.job_id=r.job_id
          {failure_join}
          WHERE r.job_id=? AND j.profile=?""", (job_id, PROFILE)).fetchone()
        if row is None:
            raise Conflict("unknown reference job")
        return row

    def _public(self, row):
        result = json.loads(row["result_json"]) if row["result_json"] else None
        grant = json.loads(row['private_grant_json']) if row['private_grant_json'] else None
        recovering = row["status"] == "held" and row["held_reason"] == "coordinator_restart"
        phase, status, error = ('held' if recovering else row['phase']), row['status'], ('coordinator_restart' if recovering else row['error'])
        if row['terminal_failure_json'] and row['failure_delivery_state'] == 'awaiting_viewer_failure_acceptance' and status == 'failed':
            failure = json.loads(row['terminal_failure_json'])
            expected = {key: failure[key] for key in ('failureSha256', 'terminalState', 'errorCode')}
            if row['queue_result'] and json.loads(row['queue_result']) == expected:
                status, error = failure['terminalState'], failure['errorCode']
                phase = 'completed' if row['phase'] == 'completed' else 'awaiting_acceptance'
        return {"jobId": row["job_id"], "generation": row["generation"],
                "environment": row["environment"], "phase": phase,
                "manifestSha256": row["manifest_sha"], "pipelineDigest": row["pipeline_digest"],
                "status": status, "nodeId": row["node_id"],
                "transferRequest": json.loads(row["transfer_json"]) if row["transfer_json"] else None,
                "transferGrantAcknowledgement": self._transfer_grant_ack(grant) if grant else None,
                "error": error,
                "resultManifestSha256": result["resultManifestSha256"] if result else None}

    def _record(self, row, private):
        result = self._public(row)
        if private:
            result.update(submission=json.loads(row["input_json"]),
                          resultManifest=json.loads(row["result_json"]) if row["result_json"] else None,
                          claimToken=row["private_claim_token"], executionPhase=row["phase"])
        return result

    def _quota(self, db, extra_bytes=0):
        used = db.execute("""SELECT COALESCE(SUM(length(CAST(input_json AS BLOB))+
          COALESCE(length(CAST(transfer_json AS BLOB)),0)+COALESCE(length(CAST(result_json AS BLOB)),0)),0)
          FROM reference_jobs""").fetchone()[0]
        for table, column in (("reference_acceptances", "receipt_json"), ("reference_outbox", "payload_json"),
                              ("reference_viewer_receipts", "receipt_json"),
                              ("reference_viewer_transfers", "acceptance_json"),
                              ("reference_worker_receipts", "receipt_json"),
                              ("reference_transfer_grants", "grant_json")):
            used += db.execute(f"SELECT COALESCE(SUM(length(CAST({column} AS BLOB))),0) FROM {table}").fetchone()[0]
        transfer_columns = {row[1] for row in db.execute('PRAGMA table_info(reference_viewer_transfers)')}
        if 'viewer_manifest_json' in transfer_columns:
            used += db.execute('SELECT COALESCE(SUM(length(CAST(viewer_manifest_json AS BLOB))),0) FROM reference_viewer_transfers').fetchone()[0]
        used += db.execute("""SELECT COALESCE(SUM(length(request_id)+length(job_id)+length(node_id)+
          length(worker_id)+length(instance_id)+length(token)),0) FROM reference_claims""").fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone():
            used += db.execute('SELECT COALESCE(SUM(length(CAST(failure_json AS BLOB))+length(CAST(receipt_json AS BLOB))),0) FROM reference_worker_failures').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_terminal_acceptances'").fetchone():
            used += db.execute('SELECT COALESCE(SUM(length(CAST(receipt_json AS BLOB))),0) FROM reference_terminal_acceptances').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_recovery_receipts'").fetchone():
            used += db.execute('''SELECT COALESCE(SUM(length(request_id)+length(job_id)+length(epoch_id)+length(request_sha256)
                +length(CAST(request_json AS BLOB))+length(CAST(receipt_json AS BLOB))),0)
                FROM reference_recovery_receipts''').fetchone()[0]
        if used + extra_bytes > self.max_metadata_bytes:
            raise Conflict("reference metadata quota reached")

    def submit(self, validated):
        if type(validated) is not dict or set(validated) != _NORMALIZED:
            raise ValueError("expected a normalized reference submission")
        raw = {k: v for k, v in validated.items() if k not in
               ("environment", "requestDigest", "manifestSha256", "pipelineDigest")}
        checked = protocol.validate_submission(raw, environment=validated["environment"],
            capability_fingerprint=validated["capabilityFingerprint"], available_engines=("odm", "treedar"),
            allowed_odm_options=set(raw["pipeline"]["odm"]["options"]))
        if _json(checked) != _json(validated):
            raise ValueError("normalized submission identity or digest mismatch")
        encoded = _json(checked)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT job_id,request_digest,environment FROM reference_jobs WHERE submission_id=?",
                             (checked["submissionId"],)).fetchone()
            if old:
                if (old["request_digest"], old["environment"]) != (checked["requestDigest"], checked["environment"]):
                    raise Conflict("submissionId belongs to a different reference request")
                return self._public(self._row(db, old["job_id"])), False
            pending = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase NOT IN ('completed','cancelled')").fetchone()[0]
            if pending >= self.max_queued_jobs:
                raise Conflict("reference pending job quota reached")
            self._quota(db, len(encoded.encode("utf-8")))
            job_id = str(uuid.uuid4())
            # Namespaced request identity also prevents collisions with ordinary
            # multipart/legacy submission IDs in the shared jobs table.
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'queued')",
                       (job_id, "reference:" + checked["submissionId"], checked["environment"], PROFILE))
            db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
              manifest_sha,pipeline_digest,phase,viewer_receipt_required) VALUES(?,?,?,?,?,?,?,'queued',?)""",
              (job_id, checked["submissionId"], checked["requestDigest"], checked["environment"], encoded,
               checked["manifestSha256"], checked["pipelineDigest"],
               int(checked['environment'] in self.viewer_required_environments)))
            return self._public(self._row(db, job_id)), True

    def get(self, job_id, *, private=False):
        with self.queue._db() as db:
            return self._record(self._row(db, job_id), private)

    def find_submission_replay(self, submission_id, *, request_digest, environment):
        """Read-only exact replay; never creates a job for an unsupported shape."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._hash(request_digest, 'request digest')
        protocol._environment(environment)
        with self.queue._db() as db:
            db.execute('BEGIN')
            row=db.execute('SELECT job_id,request_digest,environment FROM reference_jobs WHERE submission_id=?', (submission_id,)).fetchone()
            # An absent, changed, or differently scoped identity is deliberately
            # indistinguishable; none may create a new unsupported job.
            if row is None or (row['request_digest'],row['environment']) != (request_digest,environment):
                return None
            return self._public(self._row(db,row['job_id']))

    def find_submission(self, submission_id, *, environment):
        """Recover a lost submit response without issuing another execution."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._environment(environment)
        with self.queue._db() as db:
            row = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=? AND environment=?',
                             (submission_id, environment)).fetchone()
            return self._public(self._row(db, row[0])) if row else None

    def list(self, *, private=False):
        with self.queue._db() as db:
            ids = [row[0] for row in db.execute("SELECT job_id FROM reference_jobs ORDER BY created_at,job_id")]
            return [self._record(self._row(db, job_id), private) for job_id in ids]

    @staticmethod
    def _transition(row, phase, expected_phase=None):
        if expected_phase is not None and row["phase"] != expected_phase:
            raise Conflict("reference phase changed")
        if phase not in PHASES or (phase != row["phase"] and phase not in PHASES[row["phase"]]):
            raise Conflict("invalid reference phase transition")

    def _required_node(self, submission):
        requires_main = (submission['pipeline']['treedar']['requested']
            or submission['manifest']['inputCounts']['images'] > self.local_max_photos
            or submission['manifest']['totalBytes'] > self.local_max_input_bytes)
        return self.main_node_id if requires_main else 'cluster'

    def _eligible_head(self, db, node_id):
        """Return the same eligible queue head for discovery and atomic claim.

        Caller holds a transaction and refreshes maintenance state first.  This
        is the legacy global-slot policy; inventory routing remains disabled.
        """
        if self.node_inventory:
            return self._inventory_eligible_head(db, node_id)
        if (self.queue._node(db, node_id)['state'] not in ('active', 'scheduled')
                or db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone()):
            return None
        head = db.execute("""SELECT id,profile FROM jobs WHERE status='queued'
            ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1""").fetchone()
        if not head or head['profile'] != PROFILE or PROFILE not in self.queue.node_profiles(node_id):
            return None
        row = self._row(db, head['id'])
        submission = json.loads(row['input_json'])
        if (self._required_node(submission) != node_id or
                (submission['pipeline']['treedar']['requested'] and node_id not in self.queue.treedar_node_ids)):
            return None
        return row

    def _inventory_eligible_head(self, db, node_id):
        """Pick one ordered eligible assignment under the queue write lock.

        Readiness must be a trusted, nonblocking local observation. A missing or
        malformed observation is ineligible; reservations are checked again in
        this transaction. This path is not enabled by runtime configuration.
        """
        occupied = {r['node_id'] for r in db.execute(
            "SELECT node_id FROM jobs WHERE status IN ('running','held')")}
        if not occupied.issubset(set(self.queue.node_ids)):
            return None
        available = []
        for spec in self.node_inventory:
            name = spec['id']
            if (name in occupied or self.queue._node(db, name)['state'] not in ('active', 'scheduled')
                    or PROFILE not in self.queue.node_profiles(name)):
                continue
            try:
                report = self.node_readiness(name)
            except Exception:
                continue
            if (not isinstance(report, dict) or report.get('ready') is not True
                    or type(report.get('taskQueueCount')) is not int or report['taskQueueCount'] != 0
                    or 'maxImages' not in report):
                continue
            limit = report['maxImages']
            if limit is not None and (type(limit) is not int or limit < 1):
                continue
            available.append((spec, limit))
        jobs = db.execute("""SELECT id,profile,environment FROM jobs WHERE status='queued'
            ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq""").fetchall()
        if not jobs:
            return None
        environment = jobs[0]['environment']
        if environment == 'staging' and db.execute("""SELECT 1 FROM jobs
                WHERE environment='production' AND status IN ('queued','running','held') LIMIT 1""").fetchone():
            return None
        for job in jobs:
            if job['environment'] != environment or job['profile'] != PROFILE:
                break
            row = self._row(db, job['id'])
            submission = json.loads(row['input_json'])
            photos = submission['manifest']['inputCounts']['images']
            for spec, limit in available:
                if ('odm' not in spec['capabilities'] or
                    (submission['pipeline']['treedar']['requested'] and spec['id'] not in self.queue.treedar_node_ids)):
                    continue
                if all(cap is None or photos <= cap for cap in (spec['maxPhotos'], limit)):
                    return row if spec['id'] == node_id else None
        return None

    def next_for_node(self, node_id):
        """PRIVATE worker discovery; advisory only, claim remains atomic."""
        self.queue._known_node(node_id)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.queue._refresh_nodes(db)
            row = self._eligible_head(db, node_id)
            return self._public(row) if row else None

    def claim(self, job_id, *, node_id, worker_id, worker_instance_id, claim_request_id):
        from contextlib import nullcontext
        from .admission_drain import AdmissionClosed
        gate = getattr(self.queue, 'admission_drain', None)
        try:
            with gate.admit() if gate is not None else nullcontext():
                return self._claim(job_id,node_id=node_id,worker_id=worker_id,
                                   worker_instance_id=worker_instance_id,claim_request_id=claim_request_id)
        except AdmissionClosed:
            # Existing claims are read-only replay and remain recoverable while
            # closing; a new claim may never cross the barrier.
            self.queue._known_node(node_id);self.queue.slug(worker_id)
            for value in (worker_instance_id,claim_request_id):protocol._slug(value,'claim identifier')
            if PROFILE not in self.queue.allowed_profiles:raise Conflict('reference profile disabled')
            with self.queue._db() as db:
                db.execute('BEGIN')
                row=self._row(db,job_id)
                old=db.execute('SELECT * FROM reference_claims WHERE request_id=?',(claim_request_id,)).fetchone()
                if old is None:return None
                if (old['job_id'],old['node_id'],old['worker_id'],old['instance_id'])!=(job_id,node_id,worker_id,worker_instance_id):
                    raise Conflict('claim request belongs to different worker binding')
                return self._record(row,True)

    def _claim(self, job_id, *, node_id, worker_id, worker_instance_id, claim_request_id):
        """PRIVATE worker API: atomically persist ownership and replay envelope.

        Exact replay returns the current state and original token, including
        terminal/held states. A worker MUST inspect state before executing.
        """
        self.queue._known_node(node_id)
        self.queue.slug(worker_id)
        for value in (worker_instance_id, claim_request_id):
            protocol._slug(value, "claim identifier")
        if PROFILE not in self.queue.allowed_profiles:
            raise Conflict("reference profile disabled")
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            old = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if old:
                if (old["job_id"], old["node_id"], old["worker_id"], old["instance_id"]) != (
                        job_id, node_id, worker_id, worker_instance_id):
                    raise Conflict("claim request belongs to different worker binding")
                return self._record(row, True)
            gate = getattr(self.queue, 'admission_drain', None)
            if gate is not None and gate._closed(db):
                return None
            self.queue._refresh_nodes(db)
            eligible = self._eligible_head(db, node_id)
            if not eligible or eligible['job_id'] != job_id:
                return None
            if row["phase"] not in ("queued", "waiting_capacity"):
                raise Conflict("reference phase cannot be claimed")
            token = secrets.token_urlsafe(32)
            self._quota(db, sum(map(len, (claim_request_id, job_id, node_id, worker_id, worker_instance_id, token))))
            db.execute("""UPDATE jobs SET status='running',worker_id=?,node_id=?,claim_hash=?,
              updated_at=CURRENT_TIMESTAMP WHERE id=?""",
              (worker_id, node_id, hashlib.sha256(token.encode()).hexdigest(), job_id))
            db.execute("UPDATE reference_jobs SET node_id=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (node_id, job_id))
            db.execute("""INSERT INTO reference_claims(request_id,job_id,node_id,worker_id,instance_id,token)
              VALUES(?,?,?,?,?,?)""", (claim_request_id, job_id, node_id, worker_id, worker_instance_id, token))
            return self._record(self._row(db, job_id), True)

    def claim_snapshot(self, job_id, *, node_id, worker_id, worker_instance_id, claim_token):
        """Observe one owned claim in a read transaction; never replay a claim.

        The private worker boundary authenticates the bearer and claim token.
        This envelope is evidence for a host guard, not execution authorization.
        In particular, a held or terminal parent cannot start another stage.
        """
        with self.queue._db() as db:
            db.execute('BEGIN')
            return self._claim_snapshot(db, job_id, node_id=node_id, worker_id=worker_id,
                worker_instance_id=worker_instance_id, claim_token=claim_token)

    def _claim_snapshot(self, db, job_id, *, node_id, worker_id, worker_instance_id, claim_token):
        """Same authenticated observation inside an existing transaction."""
        self.queue._known_node(node_id)
        protocol._uuid(job_id, 'jobId')
        protocol._slug(worker_id, 'worker ID')
        protocol._slug(worker_instance_id, 'worker instance ID')
        if (type(claim_token) is not str or not claim_token.isascii()
                or not 1 <= len(claim_token) <= 128):
            raise Conflict('owned worker claim required')
        supplied_hash = hashlib.sha256(claim_token.encode()).hexdigest()
        authority = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        row = db.execute('''SELECT r.*,j.status AS queue_status,j.environment AS queue_environment,
            j.node_id AS queue_node,j.worker_id AS queue_worker,j.claim_hash AS queue_claim_hash,
            j.held_reason,c.request_id AS claim_request,c.node_id AS claim_node,
            c.worker_id AS claim_worker,c.instance_id AS claim_instance,c.token AS claim_token
            FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
            JOIN reference_claims c ON c.job_id=r.job_id WHERE r.job_id=? AND j.profile=?''',
            (job_id, PROFILE)).fetchone()
        if (not authority or authority['authority_id'] != self.queue.authority_id or row is None
                or (row['node_id'], row['queue_node'], row['claim_node']) != (node_id,) * 3
                or (row['queue_worker'], row['claim_worker']) != (worker_id,) * 2
                or row['claim_instance'] != worker_instance_id
                or type(row['claim_token']) is not str
                or not row['claim_token'].isascii()
                or not hmac.compare_digest(row['claim_token'], claim_token)
                or type(row['queue_claim_hash']) is not str
                or not row['queue_claim_hash'].isascii()
                or not hmac.compare_digest(row['queue_claim_hash'], supplied_hash)
                or row['environment'] != row['queue_environment']):
            raise Conflict('owned worker claim required')
        try:
            protocol._integer(row['generation'], 'generation')
            protocol._environment(row['environment'])
            protocol._slug(row['claim_request'], 'claim request ID')
            protocol._hash(row['manifest_sha'], 'manifest digest')
            protocol._hash(row['pipeline_digest'], 'pipeline digest')
            _reason(row['held_reason'])
            submission = json.loads(row['input_json'])
            if (row['phase'] not in PHASES
                    or row['queue_status'] not in ('running', 'held', 'succeeded', 'failed', 'cancelled')
                    or row['queue_status'] != 'held' and row['held_reason'] is not None
                    or submission['environment'] != row['environment']
                    or submission['manifestSha256'] != row['manifest_sha']
                    or submission['pipelineDigest'] != row['pipeline_digest']
                    or protocol.manifest_sha256(submission['manifest']) != row['manifest_sha']
                    or protocol.digest(submission['pipeline']) != row['pipeline_digest']):
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise Conflict('persisted claim binding unavailable') from None
        return dict(schemaVersion=1, protocol='ltds-parent-claim-v1',
            authorityId=authority['authority_id'], jobId=job_id, generation=row['generation'],
            environment=row['environment'], nodeId=node_id, workerId=worker_id,
            workerInstanceId=worker_instance_id, claimRequestId=row['claim_request'],
            claimTokenSha256=supplied_hash, manifestSha256=row['manifest_sha'],
            pipelineDigest=row['pipeline_digest'], status=row['queue_status'],
            executionPhase=row['phase'], heldReason=row['held_reason'])

    def update_phase(self, job_id, phase, *, expected_phase=None, error=None):
        _reason(error)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            self._transition(row, phase, expected_phase)
            if phase in ("fetching", "processing") and row["status"] != "running":
                raise Conflict("execution requires a running shared queue claim")
            if phase == "awaiting_transfer":
                raise Conflict("use reserve after external capacity admission")
            if phase == "completed":
                raise Conflict("completion requires full result acceptance")
            if phase == "cancelled" and row["status"] in ("running", "held"):
                raise Conflict("active or uncertain engine requires operator resolution")
            db.execute("UPDATE reference_jobs SET phase=?,error=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?",
                       (phase, error, job_id))
            if phase == "cancelled":
                db.execute("UPDATE jobs SET status='cancelled',updated_at=CURRENT_TIMESTAMP WHERE id=?", (job_id,))
            if phase == "held" and row["status"] in ("queued", "running"):
                db.execute("UPDATE jobs SET status='held',held_reason=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                           (error or "reference_held", job_id))
            return self._public(self._row(db, job_id))

    def reserve(self, job_id, transfer_request, *, node_id):
        """Record externally admitted capacity for the claimed worker.

        Expiry does not reset this reservation or release uncertain ownership.
        A safe retry needs an explicit ScratchLedger expiry attestation proving
        no engine mutation, plus a future reconciliation API. Until then freeze.
        """
        protocol._closed(transfer_request, ("reservationId", "generation", "expiresAt", "manifestSha256", "byteBudget", "nonce"), "transfer request")
        for key in ("reservationId", "nonce"):
            protocol._slug(transfer_request[key], key)
        protocol._integer(transfer_request["generation"], "generation")
        protocol._integer(transfer_request["byteBudget"], "byteBudget")
        protocol._hash(transfer_request["manifestSha256"], "manifestSha256")
        protocol._string(transfer_request["expiresAt"], "expiresAt", 40)
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z', transfer_request['expiresAt']):
            raise ValueError('transfer expiry must use UTC millisecond wire format')
        try:
            expiry = datetime.fromisoformat(transfer_request["expiresAt"].replace("Z", "+00:00"))
            if expiry.utcoffset() is None:
                raise ValueError("missing offset")
        except ValueError:
            raise ValueError("transfer expiry must be an offset timestamp") from None
        self.queue._known_node(node_id)
        encoded = _json(transfer_request)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            if row["status"] != "running" or row["node_id"] != node_id or not row["private_claim_token"]:
                raise Conflict("reservation requires a running reference claim on this node")
            if row["transfer_json"] == encoded and row["node_id"] == node_id:
                return self._public(row)
            if row["phase"] not in ("queued", "waiting_capacity"):
                raise Conflict("reference job cannot be reserved in this phase")
            submission = json.loads(row["input_json"])
            total_bytes = submission['manifest']['totalBytes']
            if ((transfer_request['generation'], transfer_request['manifestSha256']) != (
                    row['generation'], row['manifest_sha']) or not total_bytes <= transfer_request['byteBudget'] <=
                    min(protocol.MAX_SAFE_INTEGER, 3 * total_bytes)):
                raise Conflict("transfer reservation identity mismatch")
            self._quota(db, len(encoded.encode("utf-8")))
            db.execute("UPDATE reference_jobs SET phase='awaiting_transfer',node_id=?,transfer_json=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?",
                       (node_id, encoded, job_id))
            return self._public(self._row(db, job_id))

    def set_result_manifest(self, job_id, result):
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            return self._seal_result(db, row, result, allow_recovery=False)

    def receive_transfer_grant(self, job_id, grant, *, clock=None):
        """Persist a private scoped grant; safe binding is the only public ACK.

        This never starts a transfer. Worker must revalidate expiry on use.
        Lost ACKs replay the same binding even after phase advance/expiry;
        unknown tokens are not auto-rotated.
        """
        from .reference_transfer import validate_grant
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            encoded, digest = _json(grant), protocol.digest(grant)
            old = db.execute('SELECT digest FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
            if old:
                if old[0] != digest:
                    raise Conflict('transfer grant already sealed')
                # The ACK means this exact canonical grant was sealed earlier,
                # not that its bearer is still usable or that work succeeded.
                # This idempotent read is safe after execution advances or the
                # bearer expires and closes the lost-POST-response window.
                return self._transfer_grant_ack(grant)
            if row['phase'] != 'awaiting_transfer' or row['status'] != 'running':
                raise Conflict('job is not awaiting a transfer grant')
            validate_grant(grant, job=self._record(row, True), clock=clock or time.time)
            if not old:
                self._quota(db, len(encoded.encode()))
                db.execute('INSERT INTO reference_transfer_grants VALUES(?,?,?)', (job_id, encoded, digest))
            return self._transfer_grant_ack(grant)

    @staticmethod
    def _transfer_grant_ack(grant):
        # Echo the canonical digest so an authenticated Viewer can reconcile a
        # lost POST response against this exact sealed grant, not only the
        # reservation fields (which would also match a changed bearer token).
        return ({key: grant[key] for key in
                 ('reservationId', 'generation', 'manifestSha256', 'nonce')} |
                {'accepted': True, 'grantSha256': protocol.digest(grant)})

    def private_transfer_grant(self, job_id):
        """PRIVATE worker boundary; never serialize as part of public status."""
        with self.queue._db() as db:
            self._row(db, job_id)
            row = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def _seal_result(self, db, row, result, *, allow_recovery):
        failure_table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone()
        if failure_table and db.execute('SELECT 1 FROM reference_worker_failures WHERE job_id=? AND generation=?',
                                       (row['job_id'], row['generation'])).fetchone():
            raise Conflict('execution already has a sealed failure')
        checked = protocol.validate_result_manifest(result, job_id=row["job_id"], generation=row["generation"],
            environment=row["environment"], pipeline_digest=row["pipeline_digest"])
        encoded = _json(checked)
        if row["result_json"]:
            if row["result_json"] != encoded:
                raise Conflict("result manifest already sealed")
            return self._public(row)
        recovering = row["status"] == "held" and row["held_reason"] == "coordinator_restart"
        if row["status"] != "running" and not (allow_recovery and recovering):
            raise Conflict("new result requires a running claim or authenticated restart recovery")
        if row["phase"] not in ("processing", "waiting_for_delivery"):
            raise Conflict("result is premature")
        self._quota(db, len(encoded.encode("utf-8")))
        db.execute("UPDATE reference_jobs SET result_json=?,phase='waiting_for_delivery',updated_at=CURRENT_TIMESTAMP WHERE job_id=?",
                   (encoded, row["job_id"]))
        return self._public(self._row(db, row["job_id"]))

    def record_worker_result(self, job_id, result, *, claim_token, node_id, worker_id,
                             worker_instance_id, claim_request_id):
        """Seal a returned terminal result, including a surviving worker's result.

        This proves persisted claim ownership, never authorizes engine execution.
        Queue.complete(token, terminal status) separately releases the shared
        slot; Viewer acceptance and cleanup remain independent later steps.
        """
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            claim = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if (not claim or (claim["job_id"], claim["node_id"], claim["worker_id"], claim["instance_id"]) !=
                    (job_id, node_id, worker_id, worker_instance_id)
                    or type(claim_token) is not str or not hmac.compare_digest(claim["token"], claim_token)):
                raise Conflict("worker result does not match persisted claim ownership")
            if row["status"] == "held" and row["held_reason"] != "coordinator_restart":
                raise Conflict("operator-held worker results require explicit resolution")
            return self._seal_result(db, row, result, allow_recovery=True)

    def accept(self, job_id, receipt, *, viewer_receipt=None, trusted_viewer_receipt_keys=None):
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            if not row["result_json"] or row["phase"] not in ("awaiting_acceptance", "completed"):
                raise Conflict("result is not ready for acceptance")
            result = json.loads(row["result_json"])
            transfer = db.execute('SELECT receipt_id,viewer_manifest_json,acceptance_json FROM reference_viewer_transfers WHERE job_id=?',
                                  (job_id,)).fetchone()
            if row['viewer_receipt_required'] not in (0, 1):
                raise Conflict('legacy Viewer acceptance policy requires reconciliation')
            if (row['viewer_receipt_required'] == 1 or transfer is not None) and viewer_receipt is None:
                raise Conflict('signed Viewer accepted-copy receipt required')
            accepted_manifest = result
            if (transfer and transfer['receipt_id'] == (receipt.get('receiptId') if type(receipt) is dict else None)
                    and transfer['viewer_manifest_json']):
                accepted_manifest = json.loads(transfer['viewer_manifest_json'])
            if accepted_manifest is result:
                # Preserve the original store API validation behavior for
                # ordinary worker receipts. Only Viewer transfer receipts use
                # the separate wire schema and conflict translation below.
                checked = protocol.validate_acceptance(receipt, result_manifest=result,
                    input_manifest_sha256=row['manifest_sha'])
            else:
                try:
                    checked = validate_viewer_acceptance(receipt, result_manifest=accepted_manifest,
                        input_manifest_sha256=row['manifest_sha'])
                except (TypeError, ValueError):
                    raise Conflict('acceptance does not match the persisted result manifest') from None
            if transfer and transfer['receipt_id'] == checked['receiptId']:
                if checked != json.loads(transfer['acceptance_json']):
                    raise Conflict('Viewer acceptance differs from durable transfer')
            identities = {checked['receiptId'], checked['storageReceiptId']}
            viewer_checked = None
            if viewer_receipt is not None:
                from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
                expected = dict(jobId=job_id, generation=row['generation'], environment=row['environment'],
                    inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                    resultManifestSha256=accepted_manifest['resultManifestSha256'])
                try:
                    verified = verify_viewer_acceptance_receipt(viewer_receipt,
                        trusted_keys=trusted_viewer_receipt_keys, expected=expected,
                        result_manifest=accepted_manifest, accepted_object_ids=checked['acceptedObjectIds'])
                except (TypeError, ValueError):
                    raise Conflict('Viewer accepted-copy signature or binding is invalid') from None
                viewer_checked = verified.receipt
                viewer_identities = {viewer_checked['receiptId'], viewer_checked['storageReceiptId']}
                if len(viewer_identities) != 2 or viewer_identities & identities:
                    raise Conflict('Viewer receipt identities must be independent')
            encoded, receipt_digest = _json(checked), protocol.receipt_tombstone_digest(checked)
            identities = {checked['receiptId'], checked['storageReceiptId']}
            for existing in db.execute('SELECT receipt_id,receipt_json FROM reference_acceptances'):
                prior = json.loads(existing['receipt_json'])
                if existing['receipt_id'] != checked['receiptId'] and identities & {prior['receiptId'], prior['storageReceiptId']}:
                    raise Conflict('acceptance identity already bound')
            for existing in db.execute('SELECT receipt_id,storage_receipt_id FROM reference_viewer_receipts'):
                if identities & {existing['receipt_id'], existing['storage_receipt_id']}:
                    raise Conflict('acceptance identity belongs to a Viewer receipt')
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_terminal_acceptances'").fetchone():
                for existing in db.execute('SELECT receipt_id,storage_receipt_id FROM reference_terminal_acceptances'):
                    if identities & {existing['receipt_id'], existing['storage_receipt_id']}:
                        raise Conflict('acceptance identity belongs to terminal metadata')
            old = db.execute("SELECT job_id,digest FROM reference_acceptances WHERE receipt_id=?", (checked["receiptId"],)).fetchone()
            if old and (old["job_id"], old["digest"]) != (job_id, receipt_digest):
                raise Conflict("receipt ID already belongs to a different acceptance")
            if not old:
                self._quota(db, len(encoded.encode("utf-8")))
                db.execute("INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)",
                           (checked["receiptId"], job_id, receipt_digest, encoded))
            if viewer_checked is not None:
                viewer_encoded = _json(viewer_checked)
                viewer_digest = protocol.digest(viewer_checked)
                existing_viewer = db.execute('SELECT * FROM reference_viewer_receipts WHERE acceptance_receipt_id=?',
                    (checked['receiptId'],)).fetchone()
                if existing_viewer and (existing_viewer['job_id'], existing_viewer['digest'], existing_viewer['receipt_json']) != (
                        job_id, viewer_digest, viewer_encoded):
                    raise Conflict('Viewer accepted-copy receipt changed on replay')
                if not existing_viewer:
                    identities = {viewer_checked['receiptId'], viewer_checked['storageReceiptId']}
                    for prior in db.execute('SELECT receipt_json FROM reference_acceptances'):
                        prior_value = json.loads(prior[0])
                        if identities & {prior_value['receiptId'], prior_value['storageReceiptId']}:
                            raise Conflict('Viewer receipt identity belongs to another acceptance')
                    for prior in db.execute('SELECT receipt_id,storage_receipt_id FROM reference_viewer_receipts'):
                        if identities & {prior['receipt_id'], prior['storage_receipt_id']}:
                            raise Conflict('Viewer receipt identity already bound')
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_terminal_acceptances'").fetchone():
                        for prior in db.execute('SELECT receipt_id,storage_receipt_id FROM reference_terminal_acceptances'):
                            if identities & {prior['receipt_id'], prior['storage_receipt_id']}:
                                raise Conflict('Viewer receipt identity belongs to terminal metadata')
                    self._quota(db, len(viewer_encoded.encode('utf-8')))
                    db.execute('INSERT INTO reference_viewer_receipts(receipt_id,storage_receipt_id,acceptance_receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?,?,?)',
                        (viewer_checked['receiptId'], viewer_checked['storageReceiptId'], checked['receiptId'],
                         job_id, viewer_digest, viewer_encoded))
            accepted = set()
            require_viewer = row['viewer_receipt_required'] == 1 or transfer is not None
            if require_viewer:
                from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
                prior_rows = db.execute("SELECT a.receipt_json,a.digest,v.receipt_json,v.digest,v.receipt_id,v.storage_receipt_id "
                    "FROM reference_acceptances a JOIN reference_viewer_receipts v "
                    "ON v.acceptance_receipt_id=a.receipt_id WHERE a.job_id=? AND v.job_id=a.job_id", (job_id,))
                for prior_json, prior_digest, signed_json, signed_digest, signed_id, signed_storage_id in prior_rows:
                    try:
                        prior = validate_viewer_acceptance(json.loads(prior_json), result_manifest=accepted_manifest,
                            input_manifest_sha256=row['manifest_sha'])
                        signed = json.loads(signed_json)
                        expected = dict(jobId=job_id, generation=row['generation'], environment=row['environment'],
                            inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                            resultManifestSha256=accepted_manifest['resultManifestSha256'])
                        verified = verify_viewer_acceptance_receipt(signed, trusted_keys=trusted_viewer_receipt_keys,
                            expected=expected, result_manifest=accepted_manifest,
                            accepted_object_ids=prior['acceptedObjectIds']).receipt
                        if (prior_digest != protocol.receipt_tombstone_digest(prior)
                                or signed_digest != protocol.digest(verified)
                                or signed_id != verified['receiptId'] or signed_storage_id != verified['storageReceiptId']):
                            raise ValueError('persisted Viewer receipt identity mismatch')
                    except (TypeError, ValueError, KeyError, OverflowError):
                        raise Conflict('persisted Viewer acceptance evidence is invalid') from None
                    accepted.update(prior['acceptedObjectIds'])
            else:
                for item in db.execute("SELECT receipt_json FROM reference_acceptances WHERE job_id=?", (job_id,)):
                    accepted.update(json.loads(item[0])["acceptedObjectIds"])
            fully = accepted == {obj["id"] for obj in result["objects"]}
            if fully:
                db.execute("UPDATE reference_jobs SET phase='completed',updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (job_id,))
            if transfer and transfer['receipt_id'] == checked['receiptId']:
                if viewer_checked is None or not fully:
                    raise Conflict('complete signed Viewer coverage required for transfer')
                db.execute('UPDATE reference_viewer_transfers SET completed_at=COALESCE(completed_at,CURRENT_TIMESTAMP),'
                           'verified_full_coverage=1,lease_token=NULL,lease_until=NULL,last_error=NULL WHERE job_id=?', (job_id,))
            return {"job": self._public(self._row(db, job_id)), "created": old is None,
                    "fullyAccepted": fully, "acceptedObjectIds": sorted(accepted)}

    def ensure_viewer_transfer(self, job_id, *, accepted_at=None):
        """Durably create one stable Viewer acceptance request for a sealed result."""
        accepted_at = accepted_at or datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        if type(accepted_at) is not str:
            raise ValueError('invalid acceptance time')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            if not row['result_json'] or row['phase'] not in ('awaiting_acceptance', 'completed'):
                raise Conflict('result is not ready for Viewer transfer')
            try:
                result = json.loads(row['result_json'])
                viewer_manifest = derive_viewer_result_manifest(result, job_id=job_id,
                    generation=row['generation'], environment=row['environment'],
                    pipeline_digest=row['pipeline_digest'])
            except (TypeError, ValueError):
                raise ViewerTransferJobConflict('result cannot be represented by the Viewer acceptance protocol') from None
            ordered_ids = [obj['id'] for obj in viewer_manifest['objects']]
            accepted_ids = ordered_ids if viewer_manifest['schemaVersion'] == 2 else sorted(ordered_ids)
            acceptance = dict(schemaVersion=1, receiptId=str(uuid.uuid4()),
                storageReceiptId=str(uuid.uuid4()), jobId=job_id, generation=row['generation'],
                environment=row['environment'], inputManifestSha256=row['manifest_sha'],
                pipelineDigest=row['pipeline_digest'], resultManifestSha256=viewer_manifest['resultManifestSha256'],
                acceptedObjectIds=accepted_ids,
                acceptedAt=accepted_at, validationVersion=protocol.VALIDATION_VERSION)
            try:
                checked = validate_viewer_acceptance(acceptance, result_manifest=viewer_manifest,
                    input_manifest_sha256=row['manifest_sha'])
            except (TypeError, ValueError):
                raise ViewerTransferJobConflict('Viewer acceptance manifest binding is invalid') from None
            encoded = _json(checked)
            manifest_encoded = _json(viewer_manifest)
            existing = db.execute('SELECT receipt_id,acceptance_json,viewer_manifest_json,attempts,completed_at FROM reference_viewer_transfers WHERE job_id=?',
                                  (job_id,)).fetchone()
            if existing:
                previous = json.loads(existing['acceptance_json'])
                previous_manifest = json.loads(existing['viewer_manifest_json']) if existing['viewer_manifest_json'] else None
                if previous_manifest is None:
                    # A never-leased candidate transfer may be upgraded in
                    # place. Once any send may have occurred, changing its
                    # wire binding requires explicit reconciliation.
                    if previous['resultManifestSha256'] != viewer_manifest['resultManifestSha256'] and (
                            existing['attempts'] or existing['completed_at']):
                        raise ViewerTransferJobConflict('Viewer transfer contract changed after an attempt; reconciliation required')
                    checked = dict(checked, receiptId=previous['receiptId'],
                        storageReceiptId=previous['storageReceiptId'], acceptedAt=previous['acceptedAt'])
                    checked = validate_viewer_acceptance(checked, result_manifest=viewer_manifest,
                        input_manifest_sha256=row['manifest_sha'])
                    encoded = _json(checked)
                    self._quota(db, max(0, len(encoded.encode('utf-8'))
                        + len(manifest_encoded.encode('utf-8'))
                        - len(existing['acceptance_json'].encode('utf-8'))))
                    db.execute('UPDATE reference_viewer_transfers SET acceptance_json=?,viewer_manifest_json=? WHERE job_id=?',
                        (encoded, manifest_encoded, job_id))
                    return checked
                if (previous['jobId'], previous['generation'], previous['environment'],
                    previous['inputManifestSha256'], previous['pipelineDigest'],
                    previous['resultManifestSha256'], previous['acceptedObjectIds']) != (
                    checked['jobId'], checked['generation'], checked['environment'],
                    checked['inputManifestSha256'], checked['pipelineDigest'],
                    checked['resultManifestSha256'], checked['acceptedObjectIds']):
                    raise ViewerTransferJobConflict('Viewer transfer binding changed')
                if previous_manifest != viewer_manifest:
                    raise ViewerTransferJobConflict('Viewer result manifest changed on durable transfer')
                return previous
            self._quota(db, len(encoded.encode('utf-8')) + len(manifest_encoded.encode('utf-8')))
            db.execute('INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json,viewer_manifest_json) VALUES(?,?,?,?)',
                       (job_id, checked['receiptId'], encoded, manifest_encoded))
            return checked

    def pending_viewer_transfer_jobs(self, *, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid Viewer transfer discovery limit')
        with self.queue._db() as db:
            phase, params = self._viewer_pending_phase()
            rows = db.execute('''SELECT r.job_id FROM reference_jobs r
                WHERE ''' + phase + ''' AND r.result_json IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM reference_viewer_transfers t WHERE t.job_id=r.job_id)
                ORDER BY r.created_at,r.job_id LIMIT ?''', (*params, limit)).fetchall()
            return [row['job_id'] for row in rows]

    def _viewer_pending_phase(self):
        return ("(r.viewer_receipt_required=1 AND (r.phase='awaiting_acceptance' OR "
                "(r.phase='completed' AND NOT " + self._viewer_full_coverage_sql() + ")))", ())

    @staticmethod
    def _viewer_full_coverage_sql():
        # The marker is written only in the same transaction as exact durable
        # acceptance and verified full signed coverage. Require those records
        # still to be bound to this transfer; a lone partial receipt is not proof.
        return ("EXISTS (SELECT 1 FROM reference_viewer_transfers t "
                "JOIN reference_acceptances a ON a.job_id=t.job_id AND a.receipt_id=t.receipt_id "
                "AND a.receipt_json=t.acceptance_json "
                "JOIN reference_viewer_receipts v ON v.job_id=t.job_id AND v.acceptance_receipt_id=a.receipt_id "
                "WHERE t.job_id=r.job_id AND t.completed_at IS NOT NULL AND t.verified_full_coverage=1)")

    def has_viewer_acceptance_reconciliation(self, *, trusted_keys=None):
        """Conservative one-page answer; unchecked pages require further audit."""
        return self.audit_viewer_acceptance_page(trusted_keys=trusted_keys)['state'] != 'clean'

    def audit_viewer_acceptance_page(self, *, trusted_keys=None, after_job=None):
        """Reverify a bounded page and identify whether more evidence remains."""
        if after_job is not None and (type(after_job) is not str or not after_job or len(after_job) > 128):
            raise ValueError('invalid Viewer acceptance audit cursor')
        with self.queue._db() as db:
            placeholders = ','.join('?' for _ in PHASES)
            invalid_phase = db.execute('''SELECT 1 FROM reference_jobs r
                WHERE (r.viewer_receipt_required IS NULL OR r.viewer_receipt_required != 0)
                AND (r.phase IS NULL OR r.phase NOT IN (''' + placeholders + ''')) LIMIT 1''',
                tuple(sorted(PHASES))).fetchone()
            if invalid_phase:
                return {'state': 'reconcile'}
            ambiguous = db.execute('''SELECT 1 FROM reference_jobs r
                WHERE r.phase IN ('awaiting_acceptance','completed')
                AND ((r.viewer_receipt_required IS NULL OR r.viewer_receipt_required NOT IN (0,1))
                  OR (r.viewer_receipt_required=1 AND r.result_json IS NULL)
                  OR (r.viewer_receipt_required=1 AND EXISTS
                      (SELECT 1 FROM reference_viewer_transfers t WHERE t.job_id=r.job_id
                       AND t.completed_at IS NOT NULL) AND NOT ''' + self._viewer_full_coverage_sql() + '''))
                LIMIT 1''').fetchone()
            if ambiguous:
                return {'state': 'reconcile'}
            rows = db.execute('''SELECT r.job_id,r.generation,r.environment,r.manifest_sha,r.pipeline_digest,r.result_json,
                t.receipt_id AS transfer_id,t.acceptance_json,t.viewer_manifest_json,
                a.receipt_json AS acceptance_json_stored,a.digest AS acceptance_digest,
                v.receipt_id AS viewer_id,v.storage_receipt_id AS viewer_storage_id,
                v.receipt_json AS viewer_json,v.digest AS viewer_digest
                FROM reference_jobs r JOIN reference_viewer_transfers t ON t.job_id=r.job_id
                LEFT JOIN reference_acceptances a ON a.job_id=r.job_id AND a.receipt_id=t.receipt_id
                LEFT JOIN reference_viewer_receipts v ON v.job_id=r.job_id AND v.acceptance_receipt_id=a.receipt_id
                WHERE r.viewer_receipt_required=1 AND t.completed_at IS NOT NULL
                AND (? IS NULL OR r.job_id > ?)
                ORDER BY r.job_id LIMIT 101''', (after_job, after_job)).fetchall()
            from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
            for row in rows[:100]:
                try:
                    result = json.loads(row['result_json'])
                    manifest = derive_viewer_result_manifest(result, job_id=row['job_id'],
                        generation=row['generation'], environment=row['environment'], pipeline_digest=row['pipeline_digest'])
                    if json.loads(row['viewer_manifest_json']) != manifest:
                        return {'state': 'reconcile'}
                    checked = validate_viewer_acceptance(json.loads(row['acceptance_json']), result_manifest=manifest,
                        input_manifest_sha256=row['manifest_sha'])
                    if (row['acceptance_json_stored'] != row['acceptance_json']
                            or row['transfer_id'] != checked['receiptId']
                            or row['acceptance_digest'] != protocol.receipt_tombstone_digest(checked)
                            or set(checked['acceptedObjectIds']) != {item['id'] for item in manifest['objects']}):
                        return {'state': 'reconcile'}
                    receipt = json.loads(row['viewer_json'])
                    expected = dict(jobId=row['job_id'], generation=row['generation'], environment=row['environment'],
                        inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                        resultManifestSha256=manifest['resultManifestSha256'])
                    verified = verify_viewer_acceptance_receipt(receipt, trusted_keys=trusted_keys,
                        expected=expected, result_manifest=manifest, accepted_object_ids=checked['acceptedObjectIds']).receipt
                    if (row['viewer_id'] != verified['receiptId']
                            or row['viewer_storage_id'] != verified['storageReceiptId']
                            or row['viewer_digest'] != protocol.digest(verified)):
                        return {'state': 'reconcile'}
                except (ValueError, TypeError, KeyError, OverflowError):
                    return {'state': 'reconcile'}
            if len(rows) > 100:
                return {'state': 'more', 'after': rows[99]['job_id']}
            return {'state': 'clean'}

    def pending_viewer_transfer_page(self, *, limit=20, after=None):
        """Return a bounded keyset page; callers may wrap after reaching its end."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid Viewer transfer discovery limit')
        if after is not None and (type(after) is not tuple or len(after) != 2
                                  or any(type(v) is not str or not v for v in after)):
            raise ValueError('invalid Viewer transfer discovery cursor')
        with self.queue._db() as db:
            phase, phase_params = self._viewer_pending_phase()
            params = list(phase_params)
            cursor = ''
            if after is not None:
                cursor = ' AND (r.created_at > ? OR (r.created_at = ? AND r.job_id > ?))'
                params.extend((after[0], after[0], after[1]))
            params.append(limit)
            rows = db.execute('''SELECT r.job_id,r.created_at FROM reference_jobs r
                WHERE ''' + phase + ''' AND r.result_json IS NOT NULL
                AND NOT EXISTS (SELECT 1 FROM reference_viewer_transfers t WHERE t.job_id=r.job_id)'''
                + cursor + ' ORDER BY r.created_at,r.job_id LIMIT ?', params).fetchall()
            return [(row['created_at'], row['job_id']) for row in rows]

    def claim_viewer_transfer(self, *, now, lease_seconds=120):
        """Lease one due Viewer transfer; expired leases are safe to resume."""
        if type(now) not in (int, float) or now < 0 or type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise ValueError('invalid Viewer transfer lease')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT job_id,receipt_id,acceptance_json,viewer_manifest_json,attempts FROM reference_viewer_transfers
                WHERE completed_at IS NULL AND next_attempt<=? AND (lease_until IS NULL OR lease_until<=?)
                ORDER BY next_attempt,created_at,job_id LIMIT 1''', (now, now)).fetchone()
            if row is None:
                return None
            token = secrets.token_urlsafe(24)
            db.execute('UPDATE reference_viewer_transfers SET attempts=attempts+1,lease_token=?,lease_until=? WHERE job_id=?',
                       (token, now + lease_seconds, row['job_id']))
            return {'jobId': row['job_id'], 'receiptId': row['receipt_id'],
                    'acceptance': json.loads(row['acceptance_json']), 'attempt': row['attempts'] + 1,
                    'viewerResultManifest': json.loads(row['viewer_manifest_json']) if row['viewer_manifest_json'] else None,
                    'leaseToken': token}

    def fail_viewer_transfer(self, job_id, lease_token, *, now, reason):
        """Schedule bounded exponential retry without storing transport detail."""
        _reason(reason)
        if type(now) not in (int, float) or now < 0:
            raise ValueError('invalid Viewer transfer time')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT attempts,lease_token,completed_at FROM reference_viewer_transfers WHERE job_id=?',
                             (job_id,)).fetchone()
            if not row or row['completed_at'] is not None or not lease_token or row['lease_token'] != lease_token:
                return False
            delay = min(3600, 2 ** min(row['attempts'], 12))
            db.execute('UPDATE reference_viewer_transfers SET next_attempt=?,lease_token=NULL,lease_until=NULL,last_error=? WHERE job_id=?',
                       (now + delay, reason, job_id))
            return True

    def viewer_transfer_status(self, job_id):
        """Secret-free durable status for tests and operator diagnostics."""
        with self.queue._db() as db:
            row = db.execute('SELECT receipt_id,attempts,next_attempt,last_error,completed_at FROM reference_viewer_transfers WHERE job_id=?',
                             (job_id,)).fetchone()
            if not row:
                return None
            return {'receiptId': row['receipt_id'], 'attempts': row['attempts'],
                    'nextAttempt': row['next_attempt'], 'lastError': row['last_error'],
                    'completed': row['completed_at'] is not None}

    def enqueue_outbox(self, kind, identity, payload):
        protocol._slug(kind, "outbox kind")
        protocol._slug(identity, "outbox identity")
        # Durable delivery is references only, never tokens, local paths or URLs.
        protocol._closed(payload, ("jobId", "generation", "environment", "receiptId"), "outbox payload")
        protocol._slug(payload["receiptId"], "outbox receipt ID")
        encoded = _json(payload)
        content_digest = protocol.digest({"kind": kind, "payload": payload})
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, payload["jobId"])
            protocol._integer(payload["generation"], "generation")
            if (row["generation"], row["environment"]) != (payload["generation"], payload["environment"]):
                raise Conflict("outbox identity mismatch")
            old = db.execute("SELECT digest FROM reference_outbox WHERE identity=?", (identity,)).fetchone()
            if old:
                if old[0] != content_digest:
                    raise Conflict("outbox identity already has different content")
                return False
            self._quota(db, len(encoded.encode("utf-8")))
            db.execute("INSERT INTO reference_outbox(identity,kind,payload_json,digest) VALUES(?,?,?,?)",
                       (identity, kind, encoded, content_digest))
            return True

    def acceptance_receipts(self, job_id):
        """PRIVATE worker relay; only signed Viewer receipts may prove accepted copies."""
        with self.queue._db() as db:
            row = self._row(db, job_id)
            receipts = []
            for item in db.execute('SELECT receipt_id,receipt_json FROM reference_acceptances WHERE job_id=? ORDER BY receipt_id', (job_id,)):
                acceptance = json.loads(item['receipt_json'])
                signed = db.execute('SELECT receipt_json FROM reference_viewer_receipts WHERE acceptance_receipt_id=?',
                    (item['receipt_id'],)).fetchone()
                receipts.append(dict(acceptance=acceptance, viewerReceipt=json.loads(signed[0])) if signed else acceptance)
            return {'receipts': receipts, 'fullyAccepted': row['phase'] == 'completed'}

    def pending_outbox(self, *, now=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("invalid outbox limit")
        now = time.time() if now is None else now
        with self.queue._db() as db:
            return [{"identity": row["identity"], "kind": row["kind"],
                     "payload": json.loads(row["payload_json"]), "attempts": row["attempts"]}
                    for row in db.execute("SELECT * FROM reference_outbox WHERE acknowledged=0 AND next_attempt<=? ORDER BY created_at,identity LIMIT ?", (now, limit))]

    def acknowledge_outbox(self, identity):
        with self.queue._db() as db:
            if not db.execute("UPDATE reference_outbox SET acknowledged=1 WHERE identity=?", (identity,)).rowcount:
                raise Conflict("unknown outbox identity")

    def retry_outbox(self, identity, *, now=None):
        now = time.time() if now is None else now
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT attempts,acknowledged FROM reference_outbox WHERE identity=?", (identity,)).fetchone()
            if not row or row["acknowledged"]:
                raise Conflict("outbox item unavailable")
            delay = min(3600, 2 ** min(row["attempts"], 12))
            db.execute("UPDATE reference_outbox SET attempts=attempts+1,next_attempt=? WHERE identity=?", (now + delay, identity))
            return delay
