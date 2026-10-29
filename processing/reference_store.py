"""Durable reference metadata alongside the single authoritative jobs queue.

No file transfer, execution, cleanup or credential storage occurs here. Capacity
reservation is an external precondition of reserve(); uncertain reservations
require reconciliation by its caller, never an automatic engine-lock release.
"""
import json
import math
import hashlib
import hmac
import os
from pathlib import Path
import re
import secrets
import stat
import time
import uuid
from datetime import datetime

from .maintenance import Conflict
from . import reference_protocol as protocol

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


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


def _reason(value):
    if value is not None and (type(value) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", value)):
        raise ValueError("error must be a bounded reason code")


class ReferenceStore:
    def __init__(self, queue, max_queued_jobs=10_000, max_metadata_bytes=64 * 1024 * 1024,
                 *, local_max_photos=1000, local_max_input_bytes=20 * 1024**3,
                 main_node_id=None, routing=None, initial_grant_policy=None):
        if PROFILE not in queue.allowed_profiles:
            raise ValueError("reference queue profile is disabled")
        for value in (max_queued_jobs, max_metadata_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("metadata quotas must be positive integers")
        if routing is not None:
            from .reference_routing import ReferenceRouting
            if type(routing) is not ReferenceRouting or routing.authority_id!=queue.authority_id:raise ValueError('concrete queue-bound reference routing required')
        self.routing=routing;self._routing_identity=routing
        self.catalog_admission=None
        self._catalog_identity=None
        self.queue = queue
        if initial_grant_policy!=getattr(queue,'initial_grant_policy',None):
            raise ValueError('queue/store initial grant policy differs')
        self.initial_grant_policy=initial_grant_policy
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
            if self.initial_grant_policy is not None:
                from .reference_initial_grant import check_policy_binding
                db.execute('BEGIN IMMEDIATE')
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_initial_policy'").fetchone():
                    self._quota(db,len(self.initial_grant_policy.canonical.encode())+64)
                check_policy_binding(db,self.initial_grant_policy,create=True)
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
                "transferGrantAcknowledgement": ({key: grant[key] for key in
                    ('reservationId', 'generation', 'manifestSha256', 'nonce')} | {'accepted': True}) if grant else None,
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
                              ("reference_worker_receipts", "receipt_json"),
                              ("reference_transfer_grants", "grant_json")):
            used += db.execute(f"SELECT COALESCE(SUM(length(CAST({column} AS BLOB))),0) FROM {table}").fetchone()[0]
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
        for table in ('reference_catalog_state','reference_option_evidence'):
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():
                used+=db.execute(f'SELECT COALESCE(SUM(length(CAST(payload AS BLOB))+length(digest)),0) FROM {table}').fetchone()[0]
                if table=='reference_catalog_state':used+=176*db.execute('SELECT COUNT(*) FROM reference_catalog_state').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_runtime_versions'").fetchone():
            used+=db.execute('SELECT COALESCE(SUM(length(CAST(payload AS BLOB))+length(digest)),0) FROM reference_runtime_versions').fetchone()[0]
            used+=db.execute('SELECT COALESCE(SUM(length(request_id)+length(node_id)+length(request_sha)+length(CAST(request_json AS BLOB))+length(CAST(receipt_json AS BLOB))+length(receipt_sha)+length(update_token_sha)+length(row_sha)+16),0) FROM reference_runtime_version_receipts').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_initial_policy'").fetchone():
            used+=db.execute('SELECT COALESCE(SUM(length(CAST(record AS BLOB))+length(sha)),0) FROM reference_initial_policy').fetchone()[0]
            used+=db.execute('SELECT COALESCE(SUM(length(job_id)+length(CAST(record AS BLOB))+length(sha)),0) FROM reference_initial_jobs').fetchone()[0]
            used+=db.execute('SELECT COALESCE(SUM(2*length(job_id)+length(body_sha)+length(CAST(record AS BLOB))+length(CAST(receipt AS BLOB))+64),0) FROM reference_initial_receipts').fetchone()[0]
        if used + extra_bytes > self.max_metadata_bytes:
            raise Conflict("reference metadata quota reached")

    def enable_catalog_admission(self, catalog):
        from .reference_catalog_cache import CatalogAdmission
        if type(catalog) is not CatalogAdmission or catalog.queue is not self.queue or self.catalog_admission is not None:
            raise ValueError('concrete original catalog required')
        self.catalog_admission=catalog;self._catalog_identity=catalog

    def submit(self, validated, *, catalog_sha=None):
        if self.catalog_admission is not self._catalog_identity:raise ValueError('catalog identity changed')
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
            if self.initial_grant_policy is not None:
                self.initial_grant_policy.transfer_policy(checked['manifest'])
            pending = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase NOT IN ('completed','cancelled')").fetchone()[0]
            if pending >= self.max_queued_jobs:
                raise Conflict("reference pending job quota reached")
            evidence=None
            if self.catalog_admission is not None:
                evidence=self.catalog_admission.admit(db,checked,catalog_sha)
            elif catalog_sha is not None:raise ValueError('catalog admission disabled')
            job_id = str(uuid.uuid4())
            initial_marker=None
            if self.initial_grant_policy is not None:
                initial_marker=dict(schemaVersion=1,jobId=job_id,generation=1,environment=checked['environment'],
                    requestDigest=checked['requestDigest'],manifestSha256=checked['manifestSha256'],pipelineDigest=checked['pipelineDigest'],
                    policySha256=protocol.digest(self.initial_grant_policy.payload()))
            self._quota(db,len(encoded.encode('utf-8'))+(len(_json(evidence).encode())+64 if evidence is not None else 0)
                +(len(_json(initial_marker).encode())+len(job_id)+64 if initial_marker is not None else 0))
            # Namespaced request identity also prevents collisions with ordinary
            # multipart/legacy submission IDs in the shared jobs table.
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'queued')",
                       (job_id, "reference:" + checked["submissionId"], checked["environment"], PROFILE))
            db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
              manifest_sha,pipeline_digest,phase) VALUES(?,?,?,?,?,?,?,'queued')""",
              (job_id, checked["submissionId"], checked["requestDigest"], checked["environment"], encoded,
               checked["manifestSha256"], checked["pipelineDigest"]))
            if evidence is not None:self.catalog_admission.insert_evidence(db,job_id,evidence)
            if initial_marker is not None:
                db.execute('INSERT INTO reference_initial_jobs VALUES(?,?,?)',(job_id,_json(initial_marker),protocol.digest(initial_marker)))
            return self._public(self._row(db, job_id)), True

    def option_evidence(self,job_id,*,node_id,worker_id,worker_instance_id,claim_token):
        if self.catalog_admission is None or self.catalog_admission is not self._catalog_identity:
            raise Conflict('option evidence unavailable')
        with self.queue._db() as db:
            db.execute('BEGIN')
            claim=self._claim_snapshot(db,job_id,node_id=node_id,worker_id=worker_id,worker_instance_id=worker_instance_id,claim_token=claim_token)
            if claim['status']!='running':raise Conflict('option evidence requires original running claim')
            row=self._row(db,job_id);submission=json.loads(row['input_json'])
            evidence=self.catalog_admission.read_evidence(db,job_id)
            if (evidence['capabilityFingerprint'],evidence['optionsFingerprint'])!=(submission['capabilityFingerprint'],submission['pipeline']['odm']['optionsFingerprint']):
                raise Conflict('original option binding changed')
            definitions=evidence['selectedDefinitions']
            if [o['name'] for o in definitions]!=sorted(submission['pipeline']['odm']['options']):raise Conflict('original selected definitions changed')
            fields=('authorityId','jobId','generation','environment','nodeId','workerId','workerInstanceId','claimRequestId','manifestSha256','pipelineDigest')
            return dict(schemaVersion=1,**{k:claim[k] for k in fields},**{k:evidence[k] for k in ('capabilityFingerprint','optionsFingerprint','catalogSha256','selectedDefinitions')})

    def get(self, job_id, *, private=False):
        with self.queue._db() as db:
            return self._record(self._row(db, job_id), private)

    def find_submission(self, submission_id, *, environment):
        """Recover a lost submit response without issuing another execution."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._environment(environment)
        with self.queue._db() as db:
            row = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=? AND environment=?',
                             (submission_id, environment)).fetchone()
            return self._public(self._row(db, row[0])) if row else None

    def reconcile_submission(self,raw,*,environment):
        """Exact original-envelope comparison only; never new admission."""
        protocol._closed(raw,('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint'),'original submission')
        try:
            checked=protocol.validate_submission(raw,environment=environment,
                capability_fingerprint=raw['capabilityFingerprint'],
                options_fingerprint=raw['pipeline']['odm']['optionsFingerprint'],
                allowed_odm_options=set(raw['pipeline']['odm']['options']),available_engines=('odm','treedar'))
        except (KeyError,TypeError):raise ValueError('malformed original submission') from None
        with self.queue._db() as db:
            db.execute('BEGIN')
            found=db.execute('SELECT job_id,input_json,request_digest FROM reference_jobs WHERE submission_id=? AND environment=?',
                (checked['submissionId'],environment)).fetchone()
            if found is None:return None
            if found['input_json']!=_json(checked) or found['request_digest']!=checked['requestDigest']:
                raise Conflict('original submission differs')
            return self._public(self._row(db,found['job_id']))

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

    def _route_sample(self):
        if self.routing is not self._routing_identity:raise ValueError('reference routing identity changed')
        if self.routing is None:return None
        # Avoid health traffic while the single slot is owned or no reference
        # head exists. A later head change is rechecked atomically and retries.
        with self.queue._db() as db:
            if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():return None
            head=db.execute("SELECT profile FROM jobs WHERE status='queued' ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1").fetchone()
            if head is None or head['profile']!=PROFILE:return None
        return self.routing.sample()

    def _required_node(self, submission, db=None, sample=None):
        if self.routing is not None:
            states={node:self.queue._node(db,node)['state'] for node in ('cluster','main')}
            return self.routing.choose(submission,states,sample,local_max_photos=self.local_max_photos,
                local_max_input_bytes=self.local_max_input_bytes,main_node_id=self.main_node_id,db=db)

        requires_main = (submission['pipeline']['treedar']['requested']
            or submission['manifest']['inputCounts']['images'] > self.local_max_photos)
        return self.main_node_id if requires_main else 'cluster'

    def next_for_node(self, node_id):
        """PRIVATE worker discovery; advisory only, claim remains atomic."""
        self.queue._known_node(node_id)
        sample=self._route_sample()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.queue._refresh_nodes(db)
            if (self.queue._node(db, node_id)['state'] not in ('active', 'scheduled')
                    or db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone()):
                return None
            head = db.execute("SELECT id,profile FROM jobs WHERE status='queued' ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1").fetchone()
            if not head or head['profile'] != PROFILE or PROFILE not in self.queue.node_profiles(node_id):
                return None
            row = self._row(db, head['id'])
            submission = json.loads(row['input_json'])
            if self._required_node(submission,db,sample) != node_id:
                return None
            if submission['pipeline']['treedar']['requested'] and node_id not in self.queue.treedar_node_ids:
                return None
            return self._public(row)

    def claim(self, job_id, *, node_id, worker_id, worker_instance_id, claim_request_id):
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
        sample=self._route_sample()
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            old = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if old:
                if (old["job_id"], old["node_id"], old["worker_id"], old["instance_id"]) != (
                        job_id, node_id, worker_id, worker_instance_id):
                    raise Conflict("claim request belongs to different worker binding")
                return self._record(row, True)
            self.queue._refresh_nodes(db)
            if self.queue._node(db, node_id)["state"] not in ("active", "scheduled"):
                return None
            if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():
                return None
            head = db.execute("""SELECT id,profile FROM jobs WHERE status='queued'
              ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq LIMIT 1""").fetchone()
            if (not head or head["id"] != job_id or head["profile"] != PROFILE
                    or PROFILE not in self.queue.node_profiles(node_id)):
                return None
            if row["phase"] not in ("queued", "waiting_capacity"):
                raise Conflict("reference phase cannot be claimed")
            submission = json.loads(row["input_json"])
            required_node = self._required_node(submission,db,sample)
            if node_id != required_node:
                return None
            if submission['pipeline']['treedar']['requested'] and node_id not in self.queue.treedar_node_ids:
                return None
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
        Lost ACKs replay the same binding; unknown tokens are not auto-rotated.
        """
        from .reference_transfer import validate_grant
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.initial_grant_policy is not None and db.execute('SELECT 1 FROM reference_initial_jobs WHERE job_id=?',(job_id,)).fetchone():
                raise Conflict('initial envelope required; raw grant fallback forbidden')
            row = self._row(db, job_id)
            if row['phase'] != 'awaiting_transfer' or row['status'] != 'running':
                raise Conflict('job is not awaiting a transfer grant')
            validate_grant(grant, job=self._record(row, True), clock=clock or time.time)
            encoded, digest = _json(grant), protocol.digest(grant)
            old = db.execute('SELECT digest FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
            if old and old[0] != digest:
                raise Conflict('transfer grant already sealed')
            if not old:
                self._quota(db, len(encoded.encode()))
                db.execute('INSERT INTO reference_transfer_grants VALUES(?,?,?)', (job_id, encoded, digest))
            return {key: grant[key] for key in ('reservationId', 'generation', 'manifestSha256', 'nonce')} | {'accepted': True}

    def private_transfer_grant(self, job_id):
        """PRIVATE worker boundary; never serialize as part of public status."""
        with self.queue._db() as db:
            self._row(db, job_id)
            row = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def receive_initial_grant(self,job_id,payload,*,environment,clock=None):
        """Atomic initial envelope + existing worker-consumable private grant.

        Only admission-marked jobs use this profile. Existing unmarked v1 jobs
        retain their old path. Exact historical replay is a receipt, not a new
        bearer/lease/custody/drain authority. Successor epochs are not exposed.
        """
        if self.initial_grant_policy is None:raise Conflict('initial delivery disabled')
        from .reference_initial_grant import validate_initial_envelope
        from .reference_grant_history import decode_history,MAX_NORMALIZED_BYTES
        protocol._slug(job_id,'job ID');protocol._environment(environment)
        body_sha=protocol.digest(payload)
        if len(protocol.canonical_json(payload))>protocol.MAX_BODY_BYTES:raise ValueError('initial body bound')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('''SELECT r.generation,r.environment,j.environment AS queue_environment,r.manifest_sha,r.pipeline_digest,
              r.phase,j.status,r.node_id,j.node_id AS queue_node,j.worker_id AS queue_worker,j.claim_hash,
              c.node_id AS claim_node,c.worker_id,c.instance_id,c.request_id AS claim_request,
              typeof(r.input_json) AS input_kind,length(CAST(r.input_json AS BLOB)) AS input_size,
              typeof(r.transfer_json) AS request_kind,length(CAST(r.transfer_json AS BLOB)) AS request_size,
              typeof(m.record) AS marker_kind,length(CAST(m.record AS BLOB)) AS marker_size,
              typeof(m.sha) AS marker_sha_kind,length(CAST(m.sha AS BLOB)) AS marker_sha_size,
              typeof(i.record) AS receipt_kind,length(CAST(i.record AS BLOB)) AS receipt_size,
              typeof(i.receipt) AS ack_kind,length(CAST(i.receipt AS BLOB)) AS ack_size,
              typeof(i.body_sha) AS body_kind,length(CAST(i.body_sha AS BLOB)) AS body_size,
              typeof(g.grant_json) AS grant_kind,length(CAST(g.grant_json AS BLOB)) AS grant_size,
              typeof(g.digest) AS grant_sha_kind,length(CAST(g.digest AS BLOB)) AS grant_sha_size
              FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
              LEFT JOIN reference_claims c ON c.job_id=r.job_id
              LEFT JOIN reference_initial_jobs m ON m.job_id=r.job_id
              LEFT JOIN reference_initial_receipts i ON i.job_id=r.job_id
              LEFT JOIN reference_transfer_grants g ON g.job_id=r.job_id
              WHERE r.job_id=? AND j.profile=? AND r.environment=? AND j.environment=?
                AND typeof(r.generation)='integer' AND r.generation BETWEEN 1 AND 9007199254740991
                AND typeof(r.manifest_sha)='text' AND length(CAST(r.manifest_sha AS BLOB))=64
                AND typeof(r.pipeline_digest)='text' AND length(CAST(r.pipeline_digest AS BLOB))=64
                AND typeof(r.phase)='text' AND length(CAST(r.phase AS BLOB))<=32
                AND typeof(j.status)='text' AND length(CAST(j.status AS BLOB))<=32
                AND typeof(r.node_id)='text' AND length(CAST(r.node_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(j.node_id)='text' AND length(CAST(j.node_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(j.worker_id)='text' AND length(CAST(j.worker_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(j.claim_hash)='text' AND length(CAST(j.claim_hash AS BLOB))=64
                AND typeof(c.node_id)='text' AND length(CAST(c.node_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(c.worker_id)='text' AND length(CAST(c.worker_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(c.instance_id)='text' AND length(CAST(c.instance_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(c.request_id)='text' AND length(CAST(c.request_id AS BLOB)) BETWEEN 1 AND 128
                AND typeof(c.token)='text' AND length(CAST(c.token AS BLOB)) BETWEEN 1 AND 128''',
              (job_id,PROFILE,environment,environment)).fetchone()
            if row is None or row['environment']!=environment or row['queue_environment']!=environment:
                raise Conflict('initial delivery unavailable')
            if (row['input_kind']!='text' or not 0<row['input_size']<=MAX_NORMALIZED_BYTES
                    or row['request_kind']!='text' or not 0<row['request_size']<=4096
                    or row['marker_kind']!='text' or not 0<row['marker_size']<=4096
                    or row['marker_sha_kind']!='text' or row['marker_sha_size']!=64
                    or (row['node_id'],row['queue_node'])!=(row['claim_node'],)*2
                    or row['worker_id']!=row['queue_worker']):
                raise Conflict('initial delivery unavailable')
            authority=db.execute("SELECT authority_id FROM coordinator_identity WHERE singleton=1 AND typeof(authority_id)='text' AND length(CAST(authority_id AS BLOB)) BETWEEN 1 AND 128").fetchone()
            token=db.execute('SELECT token FROM reference_claims WHERE job_id=?',(job_id,)).fetchone()[0]
            if (authority is None or authority[0]!=self.queue.authority_id or not token.isascii()
                    or not row['claim_hash'].isascii() or not hmac.compare_digest(row['claim_hash'],hashlib.sha256(token.encode()).hexdigest())):
                raise Conflict('initial parent claim differs')
            protocol._slug(row['instance_id'],'worker instance');protocol._slug(row['claim_request'],'claim request')
            parent_claim=dict(authorityId=authority[0],jobId=job_id,generation=row['generation'],environment=environment,
                nodeId=row['node_id'],workerId=row['worker_id'],workerInstanceId=row['instance_id'],
                claimRequestId=row['claim_request'],claimTokenSha256=row['claim_hash'])
            original,request=db.execute('SELECT input_json,transfer_json FROM reference_jobs WHERE job_id=?',(job_id,)).fetchone()
            submission=decode_history(original,limit=MAX_NORMALIZED_BYTES)
            reservation=decode_history(request,limit=4096)
            marker_raw,marker_sha=db.execute('SELECT record,sha FROM reference_initial_jobs WHERE job_id=?',(job_id,)).fetchone()
            expected_marker=dict(schemaVersion=1,jobId=job_id,generation=row['generation'],environment=environment,
                requestDigest=submission['requestDigest'],manifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],
                policySha256=protocol.digest(self.initial_grant_policy.payload()))
            if marker_raw!=_json(expected_marker) or marker_sha!=protocol.digest(expected_marker):raise Conflict('initial admission changed')
            job=dict(jobId=job_id,generation=row['generation'],environment=environment,manifestSha256=row['manifest_sha'],
                pipelineDigest=row['pipeline_digest'],submission=submission,transferRequest=reservation)
            replay=row['receipt_kind']!='null'
            if replay:
                if (row['receipt_kind']!='text' or not 0<row['receipt_size']<=16384
                        or row['ack_kind']!='text' or not 0<row['ack_size']<=4096
                        or row['body_kind']!='text' or row['body_size']!=64
                        or row['grant_kind']!='text' or not 0<row['grant_size']<=protocol.MAX_BODY_BYTES
                        or row['grant_sha_kind']!='text' or row['grant_sha_size']!=64):
                    raise Conflict('initial receipt unavailable')
                existing_sha,record_raw,ack_raw=db.execute('SELECT body_sha,record,receipt FROM reference_initial_receipts WHERE job_id=?',(job_id,)).fetchone()
                if existing_sha!=body_sha:raise Conflict('initial delivery differs')
                record,ack=validate_initial_envelope(payload,job=job,node_id=row['node_id'],worker_id=row['worker_id'],policy=self.initial_grant_policy)
                record['parentClaimSha256']=protocol.digest(parent_claim)
                grant_raw,grant_sha=db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',(job_id,)).fetchone()
                if (record_raw!=_json(record) or ack_raw!=_json(ack) or grant_raw!=_json(payload['grant'])
                        or grant_sha!=ack['grantSha256']):raise Conflict('initial delivery differs')
                return ack
            if row['phase']!='awaiting_transfer' or row['status']!='running' or row['grant_kind']!='null':
                raise Conflict('fresh owned initial reservation required')
            now=(clock or time.time)()
            if type(now) not in (int,float) or not math.isfinite(now):raise Conflict('initial clock unavailable')
            record,ack=validate_initial_envelope(payload,job=job,node_id=row['node_id'],worker_id=row['worker_id'],
                policy=self.initial_grant_policy,now_ms=int(now*1000))
            record['parentClaimSha256']=protocol.digest(parent_claim)
            grant_raw=_json(payload['grant']);record_raw=_json(record);ack_raw=_json(ack)
            self._quota(db,len(grant_raw.encode())+len(record_raw.encode())+len(ack_raw.encode())+2*len(job_id)+128)
            db.execute('INSERT INTO reference_transfer_grants VALUES(?,?,?)',(job_id,grant_raw,ack['grantSha256']))
            db.execute('INSERT INTO reference_initial_receipts VALUES(?,?,?,?)',(job_id,body_sha,record_raw,ack_raw))
            return ack

    def has_reference_scope(self, job_id, *, environment):
        """Scalar authorization probe; never select private/input/result JSON."""
        protocol._slug(job_id,'job ID');protocol._environment(environment)
        with self.queue._db() as db:
            return db.execute('''SELECT 1 FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
              WHERE r.job_id=? AND r.environment=? AND j.environment=? AND j.profile=? LIMIT 1''',
              (job_id,environment,environment,PROFILE)).fetchone() is not None

    def reconcile_transfer_grant(self, job_id, expected, *, environment):
        """Historical exact delivery ACK; no token, renewal or download authority.

        Expiry and later job phases do not erase a committed delivery. Failure
        is UNKNOWN, never evidence that reissuance, deletion or retry is safe.
        Public caller separately authenticates the Viewer environment.
        """
        fields=('schemaVersion','jobId','generation','environment','reservationId',
                'manifestSha256','nonce','grantSha256')
        protocol._closed(expected,fields,'grant delivery reconciliation')
        if type(expected['schemaVersion']) is not int or expected['schemaVersion']!=1:
            raise ValueError('unsupported grant reconciliation version')
        protocol._environment(environment);protocol._environment(expected['environment'])
        protocol._integer(expected['generation'],'generation')
        for key in ('jobId','reservationId','nonce'):protocol._slug(expected[key],key)
        for key in ('manifestSha256','grantSha256'):protocol._hash(expected[key],key)
        if expected['jobId']!=job_id or expected['environment']!=environment:
            raise Conflict('grant reconciliation binding differs')
        from .reference_grant_history import decode_history,validate_historical_grant,MAX_NORMALIZED_BYTES,HistoricalGrantHeld
        with self.queue._db() as db:
            db.execute('BEGIN')
            # Size/type probes precede materializing any private grant bytes.
            row=db.execute('''SELECT r.generation,r.environment,j.environment AS queue_environment,r.manifest_sha,r.pipeline_digest,
              typeof(r.input_json) AS input_kind,length(CAST(r.input_json AS BLOB)) AS input_size,
              typeof(r.transfer_json) AS request_kind,length(CAST(r.transfer_json AS BLOB)) AS request_size,
              typeof(g.grant_json) AS kind,length(CAST(g.grant_json AS BLOB)) AS size,
              typeof(g.digest) AS digest_kind,length(CAST(g.digest AS BLOB)) AS digest_size
              FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
              LEFT JOIN reference_transfer_grants g ON g.job_id=r.job_id
              WHERE r.job_id=? AND j.profile=?''',(job_id,PROFILE)).fetchone()
            if row is None or row['environment']!=environment or row['queue_environment']!=environment:
                raise Conflict('grant delivery unavailable')
            if (row['kind']!='text' or not 0<row['size']<=protocol.MAX_BODY_BYTES
                    or row['digest_kind']!='text' or row['digest_size']!=64
                    or row['input_kind']!='text' or not 0<row['input_size']<=MAX_NORMALIZED_BYTES
                    or row['request_kind']!='text' or not 0<row['request_size']<=4096):
                raise Conflict('grant delivery unavailable')
            raw,digest=db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',(job_id,)).fetchone()
            original,request=db.execute('SELECT input_json,transfer_json FROM reference_jobs WHERE job_id=?',(job_id,)).fetchone()
            try:
                grant=decode_history(raw,limit=protocol.MAX_BODY_BYTES)
                validate_historical_grant(grant,job=dict(jobId=job_id,generation=row['generation'],environment=row['environment'],
                    manifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],
                    submission=decode_history(original,limit=MAX_NORMALIZED_BYTES),transferRequest=decode_history(request,limit=4096)))
            except HistoricalGrantHeld:
                raise Conflict('grant delivery unavailable') from None
            if _json(grant)!=raw or protocol.digest(grant)!=digest or digest!=expected['grantSha256']:
                raise Conflict('grant delivery differs')
            if (type(row['generation']) is not int or row['generation']!=expected['generation']
                    or row['manifest_sha']!=expected['manifestSha256']):
                raise Conflict('grant delivery differs')
            for key in ('jobId','generation','environment','reservationId','manifestSha256','nonce'):
                if type(grant.get(key)) is not type(expected[key]) or grant[key]!=expected[key]:
                    raise Conflict('grant delivery differs')
            return dict(expected,receiptType='transfer_grant_reconciliation',accepted=True,mayDownload=False)

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

    def accept(self, job_id, receipt):
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            if not row["result_json"] or row["phase"] not in ("awaiting_acceptance", "completed"):
                raise Conflict("result is not ready for acceptance")
            result = json.loads(row["result_json"])
            checked = protocol.validate_acceptance(receipt, result_manifest=result,
                input_manifest_sha256=row["manifest_sha"])
            encoded, receipt_digest = _json(checked), protocol.receipt_tombstone_digest(checked)
            identities = {checked['receiptId'], checked['storageReceiptId']}
            for existing in db.execute('SELECT receipt_id,receipt_json FROM reference_acceptances'):
                prior = json.loads(existing['receipt_json'])
                if existing['receipt_id'] != checked['receiptId'] and identities & {prior['receiptId'], prior['storageReceiptId']}:
                    raise Conflict('acceptance identity already bound')
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
            accepted = set()
            for item in db.execute("SELECT receipt_json FROM reference_acceptances WHERE job_id=?", (job_id,)):
                accepted.update(json.loads(item[0])["acceptedObjectIds"])
            fully = accepted == {obj["id"] for obj in result["objects"]}
            if fully:
                db.execute("UPDATE reference_jobs SET phase='completed',updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (job_id,))
            return {"job": self._public(self._row(db, job_id)), "created": old is None,
                    "fullyAccepted": fully, "acceptedObjectIds": sorted(accepted)}

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
        """PRIVATE worker relay; metadata receipts grant no unverified deletion."""
        with self.queue._db() as db:
            row = self._row(db, job_id)
            receipts = [json.loads(item[0]) for item in db.execute(
                'SELECT receipt_json FROM reference_acceptances WHERE job_id=? ORDER BY receipt_id', (job_id,))]
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
