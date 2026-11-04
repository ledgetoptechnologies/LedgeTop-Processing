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
import sqlite3
import stat
import time
import uuid
from contextlib import closing
from datetime import datetime, timezone

from .maintenance import Conflict
from . import reference_protocol as protocol
from .prototype import VIEWER_PROFILE
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

_ACTIVATION_SCHEMA = """CREATE TABLE reference_viewer_activation (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), activation_id TEXT UNIQUE NOT NULL,
 principal_id TEXT NOT NULL, environment TEXT NOT NULL, viewer_attempt_id TEXT NOT NULL,
 binding_json TEXT NOT NULL, binding_sha256 TEXT NOT NULL,
 UNIQUE(principal_id, environment, viewer_attempt_id))"""
_OWNER_SCHEMA = """CREATE TABLE reference_viewer_submission_owners (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,
 environment TEXT NOT NULL, request_digest TEXT NOT NULL, protocol TEXT NOT NULL)"""
_SCHEMA_MARKER = """CREATE TABLE reference_viewer_activation_schema (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))"""
_ENVELOPE_V2_SCHEMA = """CREATE TABLE reference_viewer_envelopes_v2 (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,
 environment TEXT NOT NULL, activation_id TEXT UNIQUE NOT NULL, viewer_task_id TEXT NOT NULL,
 viewer_attempt_id TEXT NOT NULL, envelope_json TEXT NOT NULL, envelope_sha256 TEXT NOT NULL,
 UNIQUE(principal_id, environment, viewer_attempt_id))"""
_V3_COPY_HOLD_SCHEMA = """CREATE TABLE reference_viewer_v3_copy_hold (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), envelope_sha256 TEXT NOT NULL,
 payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL)"""
_V3_POLICY_SCHEMA = """CREATE TABLE reference_viewer_v3_policy (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), policy_json TEXT NOT NULL, policy_sha256 TEXT NOT NULL)"""
_ENVELOPE_V2_DOMAIN = b'ltds-viewer-prepared-envelope-v2\x00'
_PREPARED_PROTOCOL = 'ltds-viewer-prepared-submit-v1'


# Generation stays fixed across a release: it is bound into worker journals and
# Viewer receipts. A release is bounded per job, so its tombstones stay a small
# multiple of the job row and cannot exhaust the metadata quota by cycling.
MAX_CLAIM_RELEASES_PER_JOB = 3
# Any row for the job means a worker reported or the job left unstarted custody.
_RELEASE_BARRIER_TABLES = ('reference_worker_failures', 'reference_worker_receipts',
                           'reference_acceptances', 'reference_viewer_receipts',
                           'reference_viewer_transfers', 'reference_terminal_acceptances',
                           'reference_recovery_receipts')


class ViewerTransferJobConflict(Conflict):
    """A particular sealed result requires reconciliation before transfer."""


class ClaimReleased(Conflict):
    """This exact claim request was fenced; it can never become a claim again."""


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


def _reason(value):
    if value is not None and (type(value) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", value)):
        raise ValueError("error must be a bounded reason code")


class JCSRuntimeUnavailable(Conflict):
    pass


class ReferenceStore:
    def __init__(self, queue, max_queued_jobs=10_000, max_metadata_bytes=64 * 1024 * 1024,
                 *, local_max_photos=1000, local_max_input_bytes=20 * 1024**3,
                 main_node_id=None, viewer_required_environments=(),
                 node_inventory=None, node_readiness=None, jcs_runtime_enabled=False):
        if PROFILE not in queue.allowed_profiles:
            raise ValueError("reference queue profile is disabled")
        for value in (max_queued_jobs, max_metadata_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("metadata quotas must be positive integers")
        if type(jcs_runtime_enabled) is not bool:
            raise ValueError('explicit JCS runtime flag required')
        self.jcs_runtime_enabled = jcs_runtime_enabled
        self._receipt_namespace_gate = None
        if jcs_runtime_enabled:
            try:
                import rfc8785
                from importlib.metadata import version
                if version('rfc8785') != '0.1.4': raise ImportError('unexpected JCS version')
            except (ImportError, ModuleNotFoundError):
                raise JCSRuntimeUnavailable('pinned JCS runtime dependency unavailable') from None
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
        # Read-only compatibility preflight before chmod or schema writes.
        try:
            with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as preflight:
                owners = preflight.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_viewer_submission_owners'").fetchone()
                if owners and not jcs_runtime_enabled and preflight.execute("SELECT 1 FROM reference_viewer_submission_owners WHERE protocol='ltds-viewer-prepared-envelope-jcs-v3' LIMIT 1").fetchone():
                    raise JCSRuntimeUnavailable('JCS durable state requires enabled capable runtime')
        except sqlite3.Error:
            raise Conflict('JCS compatibility preflight unavailable') from None
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
                CREATE TABLE IF NOT EXISTS reference_released_claims (
                  request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, node_id TEXT NOT NULL,
                  worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, token_hash TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reference_worker_receipts (
                  outbox_id TEXT PRIMARY KEY, payload_sha TEXT NOT NULL,
                  job_id TEXT NOT NULL, receipt_json TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reference_transfer_grants (
                  job_id TEXT PRIMARY KEY, grant_json TEXT NOT NULL, digest TEXT NOT NULL);
            """)
            self._init_viewer_activation_schema(db)
            self._init_envelope_v2_schema(db)
            self._init_v3_policy_schema(db)
            self._init_v3_copy_hold_schema(db)
            self._check_jcs_capability(db)
            if db.execute("SELECT 1 FROM reference_viewer_submission_owners WHERE protocol='ltds-viewer-prepared-envelope-jcs-v3' LIMIT 1").fetchone():
                if not self.jcs_runtime_enabled:
                    raise JCSRuntimeUnavailable('JCS durable state requires enabled capable runtime')
                try: import rfc8785
                except ImportError: raise JCSRuntimeUnavailable('JCS runtime dependency unavailable') from None
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

    @staticmethod
    def _init_viewer_activation_schema(db):
        expected = {'reference_viewer_activation': _ACTIVATION_SCHEMA,
                    'reference_viewer_submission_owners': _OWNER_SCHEMA,
                    'reference_viewer_activation_schema': _SCHEMA_MARKER}
        existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        present = set(expected) & existing
        if present and present != set(expected):
            raise Conflict('partial Viewer activation schema requires reconciliation')
        if not present:
            db.execute('BEGIN IMMEDIATE')
            for sql in expected.values():
                db.execute(sql)
            db.execute('INSERT INTO reference_viewer_activation_schema VALUES(1,1)')
        normalize = lambda sql: ' '.join(sql.split())
        for name, sql in expected.items():
            row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
            if not row or normalize(row[0]) != normalize(sql):
                raise Conflict('incompatible Viewer activation schema')
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (name,)).fetchone():
                raise Conflict('unexpected Viewer activation trigger')
        for name, wanted in (
                ('reference_viewer_activation', {('job_id',), ('activation_id',), ('principal_id','environment','viewer_attempt_id')}),
                ('reference_viewer_submission_owners', {('job_id',)})):
            indexes = db.execute('PRAGMA index_list(' + name + ')').fetchall()
            actual = set()
            for index in indexes:
                if not index['unique'] or index['partial'] or index['origin'] not in ('pk', 'u'):
                    raise Conflict('unexpected Viewer activation index')
                actual.add(tuple(row['name'] for row in db.execute('PRAGMA index_info(' + index['name'] + ')')))
            if actual != wanted:
                raise Conflict('incompatible Viewer activation indexes')
            foreign = db.execute('PRAGMA foreign_key_list(' + name + ')').fetchall()
            if (len(foreign) != 1 or (foreign[0]['table'], foreign[0]['from'], foreign[0]['to'],
                    foreign[0]['on_update'], foreign[0]['on_delete'], foreign[0]['match']) !=
                    ('reference_jobs', 'job_id', 'job_id', 'NO ACTION', 'NO ACTION', 'NONE')):
                raise Conflict('incompatible Viewer activation foreign key')
        if [tuple(row) for row in db.execute('SELECT singleton,version FROM reference_viewer_activation_schema')] != [(1,1)]:
            raise Conflict('incompatible Viewer activation schema version')

    @staticmethod
    def _init_envelope_v2_schema(db):
        row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_envelopes_v2'").fetchone()
        if row is None:
            db.execute(_ENVELOPE_V2_SCHEMA)
            row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_envelopes_v2'").fetchone()
        if ' '.join(row[0].split()) != ' '.join(_ENVELOPE_V2_SCHEMA.split()):
            raise Conflict('incompatible prepared envelope schema')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='reference_viewer_envelopes_v2'").fetchone():
            raise Conflict('unexpected prepared envelope trigger')
        indexes = db.execute('PRAGMA index_list(reference_viewer_envelopes_v2)').fetchall()
        actual = set()
        for index in indexes:
            if not index['unique'] or index['partial'] or index['origin'] not in ('pk', 'u'):
                raise Conflict('unexpected prepared envelope index')
            actual.add(tuple(r['name'] for r in db.execute('PRAGMA index_info(' + index['name'] + ')')))
        if actual != {('job_id',), ('activation_id',), ('principal_id','environment','viewer_attempt_id')}:
            raise Conflict('incompatible prepared envelope indexes')
        fk = db.execute('PRAGMA foreign_key_list(reference_viewer_envelopes_v2)').fetchall()
        if len(fk) != 1 or (fk[0]['table'],fk[0]['from'],fk[0]['to'],fk[0]['on_update'],fk[0]['on_delete'],fk[0]['match']) != ('reference_jobs','job_id','job_id','NO ACTION','NO ACTION','NONE'):
            raise Conflict('incompatible prepared envelope foreign key')

    @staticmethod
    def _init_v3_policy_schema(db):
        existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_v3_policy'").fetchone()
        if existing is None:
            db.execute(_V3_POLICY_SCHEMA)
            existing = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_v3_policy'").fetchone()
        if ' '.join(existing[0].split()) != ' '.join(_V3_POLICY_SCHEMA.split()):
            raise Conflict('incompatible v3 policy schema')
        indexes=db.execute('PRAGMA index_list(reference_viewer_v3_policy)').fetchall()
        if len(indexes)!=1 or not indexes[0]['unique'] or indexes[0]['partial'] or tuple(x['name'] for x in db.execute('PRAGMA index_info('+indexes[0]['name']+')')) != ('job_id',):
            raise Conflict('incompatible v3 policy index')
        fks=db.execute('PRAGMA foreign_key_list(reference_viewer_v3_policy)').fetchall()
        if len(fks)!=1 or (fks[0]['table'],fks[0]['from'],fks[0]['to']) != ('reference_jobs','job_id','job_id'):
            raise Conflict('incompatible v3 policy foreign key')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='reference_viewer_v3_policy'").fetchone():
            raise Conflict('unexpected v3 policy trigger')

    @staticmethod
    def _init_v3_copy_hold_schema(db):
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_v3_copy_hold'").fetchone()
        if row is None:
            db.execute(_V3_COPY_HOLD_SCHEMA)
            row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_viewer_v3_copy_hold'").fetchone()
        if ' '.join(row[0].split())!=' '.join(_V3_COPY_HOLD_SCHEMA.split()): raise Conflict('v3 copy hold schema changed')
        indexes=db.execute('PRAGMA index_list(reference_viewer_v3_copy_hold)').fetchall()
        if len(indexes)!=1 or not indexes[0]['unique'] or indexes[0]['partial'] or tuple(x['name'] for x in db.execute('PRAGMA index_info('+indexes[0]['name']+')')) != ('job_id',): raise Conflict('v3 copy hold index changed')
        fks=db.execute('PRAGMA foreign_key_list(reference_viewer_v3_copy_hold)').fetchall()
        if len(fks)!=1 or (fks[0]['table'],fks[0]['from'],fks[0]['to'])!=('reference_jobs','job_id','job_id'): raise Conflict('v3 copy hold foreign key changed')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='reference_viewer_v3_copy_hold'").fetchone(): raise Conflict('v3 copy hold trigger changed')

    def _check_jcs_capability(self, db):
        if not self.jcs_runtime_enabled and db.execute(
            "SELECT 1 FROM reference_viewer_submission_owners WHERE protocol='ltds-viewer-prepared-envelope-jcs-v3' LIMIT 1").fetchone():
            raise JCSRuntimeUnavailable('JCS durable state requires enabled capable runtime')

    def _require_viewer_authority(self, db, expected):
        if expected is None: return
        protocol._uuid(expected, 'expected coordinator authority')
        table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_identity'").fetchone()
        if table is None:
            raise JCSRuntimeUnavailable('coordinator authority schema unavailable')
        columns={row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
        if not {'singleton','authority_id'}.issubset(columns):
            raise JCSRuntimeUnavailable('coordinator authority schema unavailable')
        values=db.execute('SELECT singleton,authority_id FROM coordinator_identity').fetchall()
        if len(values)!=1 or values[0]['singleton']!=1 or values[0]['authority_id']!=expected or self.queue.authority_id!=expected:
            raise JCSRuntimeUnavailable('coordinator authority missing or changed')

    def _row(self, db, job_id):
        self._check_jcs_capability(db)
        has_failure = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone()
        failure_columns = 'f.failure_json AS terminal_failure_json,f.delivery_state AS failure_delivery_state' if has_failure else 'NULL AS terminal_failure_json,NULL AS failure_delivery_state'
        failure_join = 'LEFT JOIN reference_worker_failures f ON f.job_id=r.job_id AND f.generation=r.generation' if has_failure else ''
        row = db.execute(f"""SELECT r.*,j.status,j.environment AS queue_environment,j.result AS queue_result,j.claim_hash,j.held_reason,c.token AS private_claim_token,
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

    def _owned_private_record(self, db, row):
        """Attach prepared-v3 attempt only from the checked activation row."""
        result=self._record(row, True)
        owner=self._submission_owner(db,row)
        if owner is not None and owner['protocol']=='ltds-viewer-prepared-envelope-jcs-v3':
            activation=self._activation(db,row)
            if activation is None: raise Conflict('Viewer activation required')
            result['sourceViewerAttemptId']=activation['viewerAttemptId']
        return result

    def _quota(self, db, extra_bytes=0):
        from .fixture_v2_copy_census_candidate import census
        try: _,fixture_bytes=census(db)
        except (ValueError,TypeError,KeyError,sqlite3.Error):raise Conflict('fixture v2 accounting unavailable') from None
        used = db.execute("""SELECT COALESCE(SUM(length(CAST(input_json AS BLOB))+
          COALESCE(length(CAST(transfer_json AS BLOB)),0)+COALESCE(length(CAST(result_json AS BLOB)),0)),0)
          FROM reference_jobs""").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(CAST(binding_json AS BLOB))+length(binding_sha256)),0) FROM reference_viewer_activation").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(principal_id)+length(environment)+length(request_digest)+length(protocol)),0) FROM reference_viewer_submission_owners").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(CAST(envelope_json AS BLOB))+512),0) FROM reference_viewer_envelopes_v2").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(CAST(policy_json AS BLOB))+64),0) FROM reference_viewer_v3_policy").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(CAST(payload_json AS BLOB))+128),0) FROM reference_viewer_v3_copy_hold").fetchone()[0]
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
        used += db.execute("""SELECT COALESCE(SUM(length(request_id)+length(job_id)+length(node_id)+
          length(worker_id)+length(instance_id)+length(token_hash)),0) FROM reference_released_claims""").fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone():
            used += db.execute('SELECT COALESCE(SUM(length(CAST(failure_json AS BLOB))+length(CAST(receipt_json AS BLOB))),0) FROM reference_worker_failures').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_terminal_acceptances'").fetchone():
            used += db.execute('SELECT COALESCE(SUM(length(CAST(receipt_json AS BLOB))),0) FROM reference_terminal_acceptances').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_recovery_receipts'").fetchone():
            used += db.execute('''SELECT COALESCE(SUM(length(request_id)+length(job_id)+length(epoch_id)+length(request_sha256)
                +length(CAST(request_json AS BLOB))+length(CAST(receipt_json AS BLOB))),0)
                FROM reference_recovery_receipts''').fetchone()[0]
        if used + fixture_bytes + extra_bytes > self.max_metadata_bytes:
            raise Conflict("reference metadata quota reached")

    def submit(self, validated):
        """Legacy environment-only submission; never assigns a Viewer owner."""
        return self._submit(validated, owner=None)

    def prepare_viewer_submission(self, validated, *, authenticated_principal, authenticated_environment):
        """Internal candidate: a future boundary must authenticate this principal.

        No network endpoint exposes this method. An environment-only bearer is
        not sufficient to establish the required independent principal identity.
        """
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        if type(validated) is not dict or validated.get('environment') != authenticated_environment:
            raise Conflict('Viewer submission environment differs')
        return self._submit(validated, owner=(authenticated_principal, authenticated_environment))

    def prepare_viewer_envelope_v2(self, envelope, *, authenticated_principal, authenticated_environment, trusted_policy):
        """Unmounted candidate; policy and identity must come from trusted configuration.

        The caller must obtain one coherent capability snapshot independently of
        all request data. This method does not authenticate either argument.
        """
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        protocol._closed(envelope, ('schemaVersion','protocol','submission','viewerTaskId','viewerAttemptId','activationId'), 'prepared envelope')
        if type(envelope['schemaVersion']) is not int or envelope['schemaVersion'] != 2 or envelope['protocol'] != 'ltds-viewer-prepared-envelope-v2':
            raise ValueError('unsupported prepared envelope')
        for field in ('viewerTaskId','viewerAttemptId','activationId'):
            protocol._uuid(envelope[field], field)
        # Lost-response recovery is a read of an already admitted immutable
        # request, not a new admission under today's capability policy.
        raw = envelope['submission']
        if type(raw) is not dict:
            raise ValueError('invalid submission')
        protocol._uuid(raw.get('submissionId'), 'submissionId')
        wire = _json(envelope)
        if len(wire.encode('utf-8')) > protocol.MAX_BODY_BYTES:
            raise ValueError('prepared envelope too large')
        envelope_hash = hashlib.sha256(_ENVELOPE_V2_DOMAIN + wire.encode('utf-8')).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            found = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=?', (raw['submissionId'],)).fetchone()
            if found is not None:
                row = self._row(db, found['job_id'])
                owner = self._submission_owner(db, row)
                prior = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (found['job_id'],)).fetchone()
                if (owner is None or prior is None or
                    (owner['principal_id'],owner['environment'],prior['envelope_json'],prior['envelope_sha256']) !=
                    (authenticated_principal,authenticated_environment,wire,envelope_hash)):
                    raise Conflict('prepared envelope differs')
                return self._public(row), False
        protocol._closed(trusted_policy, ('capabilityFingerprint','optionsFingerprint','allowedOdmOptions','availableEngines'), 'trusted policy')
        protocol._hash(trusted_policy['capabilityFingerprint'], 'trusted capability fingerprint')
        protocol._hash(trusted_policy['optionsFingerprint'], 'trusted options fingerprint')
        options, engines = trusted_policy['allowedOdmOptions'], trusted_policy['availableEngines']
        if (type(options) not in (list, tuple) or any(type(o) is not str or not o for o in options)
                or len(set(options)) != len(options) or type(engines) not in (list, tuple)
                or any(e not in ('odm', 'treedar') for e in engines) or len(set(engines)) != len(engines)):
            raise ValueError('invalid trusted capability policy')
        raw = envelope['submission']
        validated = protocol.validate_submission(raw, environment=authenticated_environment,
            capability_fingerprint=trusted_policy['capabilityFingerprint'],
            options_fingerprint=trusted_policy['optionsFingerprint'], available_engines=tuple(engines),
            allowed_odm_options=set(options))
        return self._submit(validated, owner=(authenticated_principal, authenticated_environment), envelope=(wire,envelope_hash,envelope['activationId'],envelope['viewerTaskId'],envelope['viewerAttemptId']))

    def prepare_viewer_envelope_jcs_v3(self, raw, *, authenticated_principal, authenticated_environment, trusted_policy, expected_authority_id=None):
        """Unmounted v3. Exact authenticated replay reads durable evidence first."""
        if not self.jcs_runtime_enabled:
            raise JCSRuntimeUnavailable('JCS runtime not enabled')
        try: import rfc8785
        except ImportError: raise JCSRuntimeUnavailable('JCS runtime dependency unavailable') from None
        from . import prepared_envelope_jcs_candidate as jcs
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        value = jcs._parse(raw)
        try: canonical = rfc8785.dumps(value)
        except (ValueError, TypeError, UnicodeError): raise ValueError('invalid JCS data') from None
        value = jcs._parse(canonical)
        protocol._closed(value, ('schemaVersion','protocol','submission','viewerTaskId','viewerAttemptId','activationId'), 'envelope')
        if type(value['schemaVersion']) is not int or value['schemaVersion'] != 3 or value['protocol'] != jcs.PROTOCOL:
            raise ValueError('unsupported envelope')
        for key in ('viewerTaskId','viewerAttemptId','activationId'): protocol._uuid(value[key], key)
        if type(value['submission']) is not dict: raise ValueError('invalid submission')
        protocol._uuid(value['submission'].get('submissionId'), 'submission ID')
        wire = canonical.decode('utf-8')
        digest = hashlib.sha256(jcs.DOMAIN + canonical).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            self._require_viewer_authority(db, expected_authority_id)
            existing = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=?', (value['submission']['submissionId'],)).fetchone()
            if existing is not None:
                row = self._row(db, existing['job_id'])
                owner = self._submission_owner(db, row)
                prior = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (row['job_id'],)).fetchone()
                if (owner is None or owner['protocol'] != jcs.PROTOCOL or prior is None or
                    (owner['principal_id'],owner['environment'],prior['envelope_json'],prior['envelope_sha256']) !=
                    (authenticated_principal,authenticated_environment,wire,digest)):
                    raise Conflict('prepared envelope differs')
                return self._public(row), False
        canonical2, digest2, normalized = jcs.validate(raw, trusted_policy=trusted_policy, environment=authenticated_environment)
        if canonical2 != canonical or digest2 != digest: raise Conflict('JCS identity changed')
        return self._submit(normalized, owner=(authenticated_principal,authenticated_environment),
            envelope=(wire,digest,value['activationId'],value['viewerTaskId'],value['viewerAttemptId']), envelope_protocol=jcs.PROTOCOL, policy_snapshot=trusted_policy, expected_authority_id=expected_authority_id)

    def _submit(self, validated, *, owner, envelope=None, envelope_protocol='ltds-viewer-prepared-envelope-v2', policy_snapshot=None, expected_authority_id=None):
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
        policy_wire = _json(policy_snapshot) if policy_snapshot is not None else None
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_jcs_capability(db)
            self._require_viewer_authority(db, expected_authority_id)
            old = db.execute("SELECT job_id,request_digest,environment FROM reference_jobs WHERE submission_id=?",
                             (checked["submissionId"],)).fetchone()
            if old:
                if (old["request_digest"], old["environment"]) != (checked["requestDigest"], checked["environment"]):
                    raise Conflict("submissionId belongs to a different reference request")
                recorded = self._submission_owner(db, self._row(db, old['job_id']))
                if (recorded is None) != (owner is None) or (recorded is not None and
                        (recorded['principal_id'], recorded['environment']) != owner):
                    raise Conflict('submission ownership differs')
                prior = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (old['job_id'],)).fetchone()
                if prior is not None and recorded['protocol'] != envelope_protocol: raise Conflict('prepared protocol differs')
                if (prior is None) != (envelope is None) or (prior is not None and
                        (prior['principal_id'],prior['environment'],prior['envelope_json'],prior['envelope_sha256'],prior['activation_id'],prior['viewer_task_id'],prior['viewer_attempt_id']) !=
                        (owner[0],owner[1],*envelope)):
                    raise Conflict('prepared envelope differs')
                return self._public(self._row(db, old["job_id"])), False
            pending = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase NOT IN ('completed','cancelled')").fetchone()[0]
            if pending >= self.max_queued_jobs:
                raise Conflict("reference pending job quota reached")
            self._quota(db, len(encoded.encode("utf-8")) + (512 if owner else 0) + (len(envelope[0].encode("utf-8")) + 512 if envelope else 0) + (len(policy_wire.encode("utf-8")) + 64 if policy_wire else 0))
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
            if owner is not None:
                db.execute('INSERT INTO reference_viewer_submission_owners VALUES(?,?,?,?,?)',
                    (job_id, owner[0], owner[1], checked['requestDigest'], envelope_protocol if envelope is not None else _PREPARED_PROTOCOL))
            if envelope is not None:
                collision = db.execute('SELECT 1 FROM reference_viewer_activation WHERE activation_id=? OR (principal_id=? AND environment=? AND viewer_attempt_id=?)',
                    (envelope[2],owner[0],owner[1],envelope[4])).fetchone()
                if collision:
                    raise Conflict('prepared envelope identity already activated')
                try:
                    db.execute('INSERT INTO reference_viewer_envelopes_v2 VALUES(?,?,?,?,?,?,?,?)',
                        (job_id, owner[0], owner[1], envelope[2], envelope[3], envelope[4], envelope[0], envelope[1]))
                except sqlite3.IntegrityError:
                    raise Conflict('prepared envelope identity already bound') from None
            if policy_wire is not None:
                db.execute('INSERT INTO reference_viewer_v3_policy VALUES(?,?,?)',
                    (job_id,policy_wire,hashlib.sha256(policy_wire.encode('utf-8')).hexdigest()))
            return self._public(self._row(db, job_id)), True

    def get(self, job_id, *, private=False):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
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
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            row = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=? AND environment=?',
                             (submission_id, environment)).fetchone()
            if not row:
                return None
            job = self._row(db, row[0])
            return None if self._submission_owner(db, job) is not None else self._public(job)

    def find_viewer_submission(self, submission_id, *, authenticated_principal, authenticated_environment):
        """Private candidate recovery; the caller must authenticate principal and environment."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            found = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=?', (submission_id,)).fetchone()
            if found is None:
                return None
            job = self._row(db, found[0])
            owner = self._submission_owner(db, job)
            if owner is None or (owner['principal_id'], owner['environment']) != (authenticated_principal, authenticated_environment):
                return None
            return dict(self._public(job), submissionId=job['submission_id'], requestDigest=job['request_digest'])

    @staticmethod
    def _scoped_prepared_job_id(db, submission_id, principal, environment, expected_protocol):
        """Filter only owner mapping before parsing any caller-inaccessible evidence."""
        return db.execute("""SELECT r.job_id FROM reference_jobs r
            JOIN reference_viewer_submission_owners o ON o.job_id=r.job_id
            WHERE r.submission_id=? AND o.principal_id=? AND o.environment=? AND o.protocol=?""",
            (submission_id,principal,environment,expected_protocol)).fetchone()

    def find_viewer_envelope_v2(self, submission_id, *, authenticated_principal, authenticated_environment, expected_protocol='ltds-viewer-prepared-envelope-v2', expected_authority_id=None):
        """Unmounted authenticated-boundary candidate, no secrets in response."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            self._require_viewer_authority(db, expected_authority_id)
            found = self._scoped_prepared_job_id(db, submission_id, authenticated_principal, authenticated_environment, expected_protocol)
            if found is None: return None
            row = self._row(db, found['job_id'])
            owner = self._submission_owner(db, row)
            if owner is None or owner['protocol'] != expected_protocol or (owner['principal_id'],owner['environment']) != (authenticated_principal,authenticated_environment):
                return None
            record = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (row['job_id'],)).fetchone()
            if record is None: return None
            return dict(self._public(row), submissionId=row['submission_id'],
                envelopeSha256=record['envelope_sha256'], viewerTaskId=record['viewer_task_id'],
                viewerAttemptId=record['viewer_attempt_id'], activationId=record['activation_id'])

    @staticmethod
    def _v3_result_contract_ready():
        # No verified end-to-end v3 source/worker/result custody contract yet.
        return False

    def read_viewer_v3_job(self, submission_id, *, authenticated_principal, authenticated_environment, view, expected_authority_id=None):
        """Inactive read model; never authorizes retrieval, acceptance or cleanup."""
        from .prepared_envelope_jcs_candidate import PROTOCOL as JCS_PROTOCOL
        protocol._uuid(submission_id, 'submission ID')
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        if view not in ('status','result-manifest','accepted-copies'): raise ValueError('unsupported view')
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            self._require_viewer_authority(db, expected_authority_id)
            found=self._scoped_prepared_job_id(db,submission_id,authenticated_principal,authenticated_environment,JCS_PROTOCOL)
            if found is None: return None
            row=self._row(db,found['job_id']);owner=self._submission_owner(db,row)
            if owner is None or owner['protocol']!=JCS_PROTOCOL or (owner['principal_id'],owner['environment']) != (authenticated_principal,authenticated_environment): return None
            envelope=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
            if envelope is None: raise Conflict('prepared evidence unavailable')
            activation=self._activation(db,row)
            binding=dict(submissionId=submission_id,jobId=row['job_id'],generation=row['generation'],environment=row['environment'],
                viewerTaskId=envelope['viewer_task_id'],viewerAttemptId=envelope['viewer_attempt_id'],
                activationId=envelope['activation_id'],envelopeSha256=envelope['envelope_sha256'])
            if view == 'status':
                return dict(binding, schemaVersion=1, view='status', status=row['status'], phase=row['phase'], activationRecorded=activation is not None, dispatchReady=False)
            # The current receipt tables accept unsigned legacy receipts and do
            # not carry the current principal-scoped verifier context. Never
            # represent them as verified v3 accepted copies.
            if view == 'accepted-copies':
                raise JCSRuntimeUnavailable('v3 accepted-copy recovery unavailable')
            if not self._v3_result_contract_ready():
                raise JCSRuntimeUnavailable('v3 result custody contract unavailable')
            if activation is None or row['status']!='succeeded' or row['phase'] not in ('awaiting_acceptance','completed') or not row['result_json']:
                raise Conflict('v3 result not ready')
            try:
                raw=json.loads(row['result_json'])
                result=protocol.validate_result_manifest(raw,job_id=row['job_id'],generation=row['generation'],
                    environment=row['environment'],pipeline_digest=row['pipeline_digest'])
                if _json(result)!=row['result_json']: raise ValueError('result evidence changed')
            except (ValueError,TypeError,KeyError,json.JSONDecodeError):
                raise Conflict('v3 result evidence invalid') from None
            return dict(binding,schemaVersion=1,view='result-manifest',resultManifest=result)

    @staticmethod
    def _held_copy_ids(db, *, exclude_job=None):
        from .reference_copy_hold_integrity import held_copy_ids
        try: return held_copy_ids(db,exclude_job=exclude_job)
        except ValueError: raise Conflict('held copy proof unavailable') from None

    @classmethod
    def _check_copy_identity_namespaces(cls, db, identities, *, exclude_job=None):
        if identities & cls._held_copy_ids(db,exclude_job=exclude_job): raise Conflict('receipt identity already held')
        from .reference_copy_hold_integrity import legacy_reserved_ids
        try: occupied=legacy_reserved_ids(db)
        except (ValueError,sqlite3.Error): raise Conflict('acceptance identity unavailable') from None
        if identities & occupied: raise Conflict('receipt identity already reserved')

    def configure_receipt_namespace_gate(self, gate):
        """Attach a candidate gate only after a read-only clean census."""
        from .receipt_namespace_authority_candidate import ReceiptNamespaceGate
        if type(gate) is not ReceiptNamespaceGate or gate.queue is not self.queue:
            raise JCSRuntimeUnavailable('shared receipt namespace unavailable')
        try: result=gate.census()
        except ValueError: raise JCSRuntimeUnavailable('shared receipt namespace unavailable') from None
        gate.v1._receipt_namespace_gate=gate
        gate.v2._receipt_namespace_gate=gate
        self._receipt_namespace_gate=gate
        return result

    @staticmethod
    def _receipt_namespace_writer_boundary_ready():
        # Configuring cooperative objects does not exclude older binaries,
        # unconfigured writers, or writes during sequential installation.
        return False

    def verify_viewer_v3_copy_held(self, submission_id, payload, *, authenticated_principal, authenticated_environment, expected_authority_id, trusted_keys):
        """Candidate verifies and journals only; it never records acceptance or cleanup authority."""
        from .prepared_envelope_jcs_candidate import PROTOCOL as JCS_PROTOCOL
        from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
        from .viewer_result_manifest import derive_viewer_result_manifest, validate_viewer_acceptance
        protocol._uuid(submission_id, 'submission ID')
        protocol._closed(payload, ('acceptance','viewerReceipt'), 'copy envelope')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE');self._check_jcs_capability(db);self._require_viewer_authority(db,expected_authority_id)
            found=self._scoped_prepared_job_id(db,submission_id,authenticated_principal,authenticated_environment,JCS_PROTOCOL)
            if found is None: return None
            if self._receipt_namespace_gate is None or not self._receipt_namespace_writer_boundary_ready():
                raise JCSRuntimeUnavailable('shared receipt namespace unavailable')
            try:self._receipt_namespace_gate.check_in_transaction(db)
            except ValueError:raise JCSRuntimeUnavailable('shared receipt namespace unavailable') from None
            row=self._row(db,found['job_id']);owner=self._submission_owner(db,row)
            if owner is None: raise Conflict('owner unavailable')
            prepared=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
            if prepared is None or self._activation(db,row) is None or not self._v3_result_contract_ready():
                raise JCSRuntimeUnavailable('v3 copy verification prerequisites unavailable')
            if row['status']!='succeeded' or row['phase'] not in ('awaiting_acceptance','completed') or not row['result_json']:
                raise Conflict('result is not ready')
            try:
                manifest=derive_viewer_result_manifest(json.loads(row['result_json']),job_id=row['job_id'],generation=row['generation'],environment=row['environment'],pipeline_digest=row['pipeline_digest'])
                acceptance=validate_viewer_acceptance(payload['acceptance'],result_manifest=manifest,input_manifest_sha256=row['manifest_sha'])
                if set(acceptance['acceptedObjectIds']) != {item['id'] for item in manifest['objects']}:
                    raise ValueError('incomplete accepted closure')
                expected=dict(jobId=row['job_id'],generation=row['generation'],environment=row['environment'],inputManifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest'],resultManifestSha256=manifest['resultManifestSha256'])
                signed=verify_viewer_acceptance_receipt(payload['viewerReceipt'],trusted_keys=trusted_keys,expected=expected,result_manifest=manifest,accepted_object_ids=acceptance['acceptedObjectIds']).receipt
                if len({acceptance['receiptId'],acceptance['storageReceiptId'],signed['receiptId'],signed['storageReceiptId']})!=4: raise ValueError('receipt IDs collide')
            except (ValueError,TypeError,KeyError): raise Conflict('v3 copy signature or binding invalid') from None
            identities={acceptance['receiptId'],acceptance['storageReceiptId'],signed['receiptId'],signed['storageReceiptId']}
            self._check_copy_identity_namespaces(db,identities,exclude_job=row['job_id'])
            wire=_json(dict(acceptance=acceptance,viewerReceipt=signed));sha=hashlib.sha256(wire.encode()).hexdigest()
            prior=db.execute('SELECT * FROM reference_viewer_v3_copy_hold WHERE job_id=?',(row['job_id'],)).fetchone()
            if prior:
                if (prior['envelope_sha256'],prior['payload_json'],prior['payload_sha256'])!=(prepared['envelope_sha256'],wire,sha): raise Conflict('v3 held proof differs')
                return {'state':'verified_held','created':False,'proofSha256':sha}
            self._quota(db,len(wire.encode())+128)
            db.execute('INSERT INTO reference_viewer_v3_copy_hold VALUES(?,?,?,?)',(row['job_id'],prepared['envelope_sha256'],wire,sha))
            return {'state':'verified_held','created':True,'proofSha256':sha}

    def get_legacy_job(self, job_id, environment, *, private=False):
        """Environment-only callers cannot inspect principal-owned prepared jobs."""
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            try:
                job = self._row(db, job_id)
            except Conflict:
                return None
            if job['environment'] != environment or self._submission_owner(db, job) is not None:
                return None
            return self._record(job, private)

    @staticmethod
    def _submission_owner(db, row):
        owner = db.execute('SELECT * FROM reference_viewer_submission_owners WHERE job_id=?', (row['job_id'],)).fetchone()
        if owner is None:
            # An ownerless row is not proof of a legacy submission. At minimum,
            # prepared evidence without its owner is inconsistent and must not
            # be silently reinterpreted as an unowned job. This does not prove
            # provenance if all evidence has disappeared; dispatch stays held.
            for table in ('reference_viewer_envelopes_v2', 'reference_viewer_activation',
                          'reference_viewer_v3_policy', 'reference_viewer_v3_copy_hold'):
                if db.execute('SELECT 1 FROM '+table+' WHERE job_id=? LIMIT 1', (row['job_id'],)).fetchone():
                    raise Conflict('Viewer submission provenance missing')
            return None
        try:
            protocol._slug(owner['principal_id'], 'Viewer principal')
            if (owner['protocol'] not in (_PREPARED_PROTOCOL, 'ltds-viewer-prepared-envelope-v2', 'ltds-viewer-prepared-envelope-jcs-v3') or owner['environment'] != row['environment']
                    or owner['environment'] != row['queue_environment'] or owner['request_digest'] != row['request_digest']):
                raise ValueError('submission owner binding changed')
        except (ValueError, TypeError, KeyError):
            raise Conflict('Viewer submission provenance invalid') from None
        prepared = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (row['job_id'],)).fetchone()
        if (prepared is not None) != (owner['protocol'] in ('ltds-viewer-prepared-envelope-v2', 'ltds-viewer-prepared-envelope-jcs-v3')):
            raise Conflict('Viewer prepared envelope provenance missing')
        if prepared is not None:
            try:
                value = json.loads(prepared['envelope_json'])
                if owner['protocol'] == 'ltds-viewer-prepared-envelope-jcs-v3':
                    import rfc8785
                    from .prepared_envelope_jcs_candidate import DOMAIN
                    canonical = rfc8785.dumps(value)
                    version = 3
                else:
                    canonical = _json(value).encode('utf-8')
                    DOMAIN = _ENVELOPE_V2_DOMAIN
                    version = 2
                if (canonical != prepared['envelope_json'].encode('utf-8') or
                    hashlib.sha256(DOMAIN + canonical).hexdigest() != prepared['envelope_sha256'] or
                    (prepared['principal_id'],prepared['environment'],prepared['activation_id'],prepared['viewer_task_id'],prepared['viewer_attempt_id']) !=
                    (owner['principal_id'],owner['environment'],value['activationId'],value['viewerTaskId'],value['viewerAttemptId']) or
                    value['submission']['submissionId'] != row['submission_id'] or
                    value['protocol'] != owner['protocol'] or value['schemaVersion'] != version or
                    value['submission'] != {k:v for k,v in json.loads(row['input_json']).items() if k not in ('environment','requestDigest','manifestSha256','pipelineDigest')}):
                    raise ValueError('prepared evidence changed')
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                raise Conflict('Viewer prepared envelope evidence invalid') from None
        return owner

    def list(self, *, private=False):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
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

    @staticmethod
    def _activation_bound_grant_ready():
        # There is no configured or implemented broker/worker v2 binding yet.
        # A caller must not turn this into a runtime override.
        return False

    @staticmethod
    def _activation_binding(value):
        fields = ('schemaVersion', 'protocol', 'activationId', 'principalId', 'submissionId',
                  'jobId', 'generation', 'environment', 'viewerTaskId', 'viewerAttemptId',
                  'requestDigest', 'manifestSha256', 'pipelineDigest')
        protocol._closed(value, fields, 'Viewer activation')
        if type(value['schemaVersion']) is not int or value['schemaVersion'] != 1 or value['protocol'] != 'ltds-viewer-activation-v1':
            raise ValueError('unsupported Viewer activation')
        for key in ('activationId', 'submissionId', 'jobId', 'viewerTaskId', 'viewerAttemptId'):
            protocol._uuid(value[key], key)
        protocol._slug(value['principalId'], 'Viewer principal')
        protocol._integer(value['generation'], 'generation')
        protocol._environment(value['environment'])
        for key in ('requestDigest', 'manifestSha256', 'pipelineDigest'):
            protocol._hash(value[key], key)
        return json.loads(protocol.canonical_json(value))

    def _activation(self, db, row):
        stored = db.execute('SELECT * FROM reference_viewer_activation WHERE job_id=?', (row['job_id'],)).fetchone()
        if stored is None:
            return None
        try:
            owner = self._submission_owner(db, row)
            if owner is None:
                raise ValueError('Viewer submission provenance missing')
            binding = self._activation_binding(json.loads(stored['binding_json']))
            if (_json(binding) != stored['binding_json'] or protocol.digest(binding) != stored['binding_sha256']
                    or (stored['activation_id'], stored['principal_id'], stored['environment'], stored['viewer_attempt_id'])
                    != (binding['activationId'], binding['principalId'], binding['environment'], binding['viewerAttemptId'])
                    or (binding['jobId'], binding['submissionId'], binding['generation'], binding['environment'],
                        binding['requestDigest'], binding['manifestSha256'], binding['pipelineDigest'])
                    != (row['job_id'], row['submission_id'], row['generation'], row['environment'],
                        row['request_digest'], row['manifest_sha'], row['pipeline_digest'])
                    or row['queue_environment'] != row['environment']
                    or binding['principalId'] != owner['principal_id']
                    or (owner['protocol'] in ('ltds-viewer-prepared-envelope-v2', 'ltds-viewer-prepared-envelope-jcs-v3') and
                        (lambda e: e is None or (e['activation_id'],e['viewer_task_id'],e['viewer_attempt_id']) !=
                          (binding['activationId'],binding['viewerTaskId'],binding['viewerAttemptId']))(
                            db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (row['job_id'],)).fetchone()))):
                raise ValueError('activation binding changed')
            return binding
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            raise Conflict('Viewer activation evidence invalid') from None

    def activate_viewer_binding(self, value, *, authenticated_principal, authenticated_environment):
        """Internal candidate only: caller must authenticate a distinct Viewer principal.

        No network route is mounted. An environment bearer alone is not a
        principal. A future boundary must authenticate this argument itself.
        """
        binding = self._activation_binding(value)
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        if (authenticated_principal, authenticated_environment) != (binding['principalId'], binding['environment']):
            raise Conflict('Viewer principal or environment differs')
        encoded = _json(binding)
        digest = protocol.digest(binding)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            row = self._row(db, binding['jobId'])
            owner = self._submission_owner(db, row)
            if owner is None or (owner['principal_id'], owner['environment']) != (
                    authenticated_principal, authenticated_environment):
                raise Conflict('Viewer submission provenance required')
            prepared = db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?', (binding['jobId'],)).fetchone()
            if prepared is not None:
                # No authenticated current-policy revalidation contract exists yet.
                raise Conflict('prepared v2 activation policy revalidation unavailable')
            reserved = db.execute('SELECT job_id FROM reference_viewer_envelopes_v2 WHERE (activation_id=? OR (principal_id=? AND environment=? AND viewer_attempt_id=?)) AND job_id!=?',
                (binding['activationId'], binding['principalId'], binding['environment'], binding['viewerAttemptId'], binding['jobId'])).fetchone()
            if reserved:
                raise Conflict('activation identity reserved by another prepared job')
            existing = self._activation(db, row)
            if existing is not None:
                if _json(existing) != encoded:
                    raise Conflict('Viewer activation differs')
                return {'activationId': binding['activationId'], 'bindingSha256': digest}
            if (row['status'] != 'queued' or row['phase'] not in ('queued', 'waiting_capacity')
                    or row['queue_environment'] != row['environment']
                    or db.execute('SELECT 1 FROM reference_claims WHERE job_id=?', (binding['jobId'],)).fetchone()):
                raise Conflict('legacy or active job cannot be activated')
            if (binding['submissionId'], binding['generation'], binding['environment'], binding['requestDigest'],
                    binding['manifestSha256'], binding['pipelineDigest']) != (
                    row['submission_id'], row['generation'], row['environment'], row['request_digest'],
                    row['manifest_sha'], row['pipeline_digest']):
                raise Conflict('Viewer activation job binding differs')
            self._quota(db, len(encoded.encode('utf-8')) + len(digest))
            try:
                db.execute('INSERT INTO reference_viewer_activation VALUES(?,?,?,?,?,?,?)',
                    (binding['jobId'], binding['activationId'], binding['principalId'], binding['environment'],
                     binding['viewerAttemptId'], encoded, digest))
            except sqlite3.IntegrityError:
                raise Conflict('Viewer activation identity already bound') from None
            return {'activationId': binding['activationId'], 'bindingSha256': digest}

    @staticmethod
    def _v3_source_contract_ready():
        # Dataset/file/GCP source snapshot contract is not implemented.
        return False

    def activate_viewer_envelope_jcs_v3(self, submission_id, request, *, authenticated_principal, authenticated_environment, trusted_policy, expected_authority_id=None):
        """Unmounted candidate; caller authenticates identity and authoritative policy."""
        from . import prepared_envelope_jcs_candidate as jcs
        protocol._uuid(submission_id, 'submission ID')
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        protocol._closed(request, ('activationId','viewerTaskId','viewerAttemptId','envelopeSha256'), 'v3 activation request')
        for key in ('activationId','viewerTaskId','viewerAttemptId'): protocol._uuid(request[key], key)
        protocol._hash(request['envelopeSha256'], 'envelope digest')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            self._require_viewer_authority(db, expected_authority_id)
            found=self._scoped_prepared_job_id(db,submission_id,authenticated_principal,authenticated_environment,jcs.PROTOCOL)
            if found is None: return None
            row=self._row(db,found['job_id'])
            owner=self._submission_owner(db,row)
            if owner is None or owner['protocol'] != jcs.PROTOCOL or (owner['principal_id'],owner['environment']) != (authenticated_principal,authenticated_environment): return None
            envelope=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
            snapshot=db.execute('SELECT * FROM reference_viewer_v3_policy WHERE job_id=?',(row['job_id'],)).fetchone()
            if envelope is None or snapshot is None:
                raise Conflict('v3 activation policy snapshot unavailable')
            wire=snapshot['policy_json']
            if (type(wire) is not str or hashlib.sha256(wire.encode()).hexdigest()!=snapshot['policy_sha256']
                or wire != _json(trusted_policy)):
                raise Conflict('v3 activation policy changed')
            canonical, digest, normalized=jcs.validate(envelope['envelope_json'].encode(),trusted_policy=trusted_policy,environment=authenticated_environment)
            if (digest != envelope['envelope_sha256'] or normalized['requestDigest'] != row['request_digest']
                or (request['activationId'],request['viewerTaskId'],request['viewerAttemptId'],request['envelopeSha256']) !=
                   (envelope['activation_id'],envelope['viewer_task_id'],envelope['viewer_attempt_id'],envelope['envelope_sha256'])):
                raise Conflict('v3 activation binding differs')
            binding=self._activation_binding(dict(schemaVersion=1,protocol='ltds-viewer-activation-v1',
                activationId=envelope['activation_id'],principalId=authenticated_principal,submissionId=submission_id,
                jobId=row['job_id'],generation=row['generation'],environment=row['environment'],
                viewerTaskId=envelope['viewer_task_id'],viewerAttemptId=envelope['viewer_attempt_id'],
                requestDigest=row['request_digest'],manifestSha256=row['manifest_sha'],pipelineDigest=row['pipeline_digest']))
            if not self._v3_source_contract_ready():
                raise JCSRuntimeUnavailable('v3 source provenance contract unavailable')
            prior=self._activation(db,row)
            encoded=_json(binding); digest=protocol.digest(binding)
            if prior is not None:
                if _json(prior)!=encoded: raise Conflict('v3 activation differs')
                return {'activationId':binding['activationId'],'bindingSha256':digest,'created':False}
            if row['status']!='queued' or row['phase'] not in ('queued','waiting_capacity') or db.execute('SELECT 1 FROM reference_claims WHERE job_id=?',(row['job_id'],)).fetchone():
                raise Conflict('v3 activation requires unclaimed queued job')
            self._quota(db,len(encoded.encode())+len(digest))
            try:
                db.execute('INSERT INTO reference_viewer_activation VALUES(?,?,?,?,?,?,?)',
                    (row['job_id'],binding['activationId'],authenticated_principal,authenticated_environment,binding['viewerAttemptId'],encoded,digest))
            except sqlite3.IntegrityError:
                raise Conflict('activation identity already bound') from None
            return {'activationId':binding['activationId'],'bindingSha256':digest,'created':True}

    def inspect_viewer_v3_source_grant(self, submission_id, grant, *, authenticated_principal, authenticated_environment, expected_authority_id):
        """Read-only candidate: reconcile durable evidence but never authorize fetching."""
        from . import prepared_envelope_jcs_candidate as jcs
        from . import viewer_source_v2_reconcile_candidate as source
        from .source_range_transfer import source_version_for_manifest
        protocol._uuid(submission_id, 'submission ID')
        protocol._slug(authenticated_principal, 'authenticated Viewer principal')
        protocol._environment(authenticated_environment)
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            if expected_authority_id is None: raise JCSRuntimeUnavailable('coordinator authority pin required')
            self._require_viewer_authority(db, expected_authority_id)
            found=self._scoped_prepared_job_id(db,submission_id,authenticated_principal,authenticated_environment,jcs.PROTOCOL)
            if found is None: return None
            row=self._row(db,found['job_id'])
            owner=self._submission_owner(db,row)
            if owner is None or owner['protocol']!=jcs.PROTOCOL or (owner['principal_id'],owner['environment'])!=(authenticated_principal,authenticated_environment):
                return None
            envelope=db.execute('SELECT * FROM reference_viewer_envelopes_v2 WHERE job_id=?',(row['job_id'],)).fetchone()
            snapshot=db.execute('SELECT * FROM reference_viewer_v3_policy WHERE job_id=?',(row['job_id'],)).fetchone()
            if envelope is None or snapshot is None or not row['transfer_json']:
                raise Conflict('durable source reservation unavailable')
            try:
                policy=json.loads(snapshot['policy_json'])
                if (_json(policy)!=snapshot['policy_json'] or
                        hashlib.sha256(snapshot['policy_json'].encode()).hexdigest()!=snapshot['policy_sha256']):
                    raise ValueError()
                canonical,digest,normalized=jcs.validate(envelope['envelope_json'].encode(),trusted_policy=policy,environment=authenticated_environment)
                if digest!=envelope['envelope_sha256'] or _json(normalized)!=row['input_json']:
                    raise ValueError()
                transfer=json.loads(row['transfer_json'])
                if _json(transfer)!=row['transfer_json']: raise ValueError()
                manifest=normalized['manifest']
                total=manifest['totalBytes']; minimum=sum((x['byteSize']+source.CHUNK-1)//source.CHUNK for x in manifest['objects'])
                budget={'chunkBytes':source.CHUNK,'requestBudget':minimum,'byteBudget':min(protocol.MAX_SAFE_INTEGER,total*3),
                        'attemptBudget':min(protocol.MAX_SAFE_INTEGER,minimum*3)}
                binding={'environment':row['environment'],'generation':row['generation'],'jobId':row['job_id'],
                         'manifestSha256':row['manifest_sha'],'reservationId':transfer['reservationId'],
                         'sourceVersion':source_version_for_manifest(manifest),'viewerAttemptId':envelope['viewer_attempt_id']}
                objects=[{k:item[k] for k in ('id','byteSize','sha256')} for item in manifest['objects']]
                source.reconcile(binding=binding,transfer=transfer,grant=grant,objects=objects,
                    trusted_manifest=manifest,trusted_policy=budget,now=time.time())
            except (ValueError, TypeError, KeyError, AttributeError):
                raise Conflict('durable source grant evidence differs') from None
            return {'state':'held','reason':'broker_authorization_unverified'}

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
            scratch, output = report.get('availableScratchBytes'), report.get('outputBudgetBytes')
            if (type(scratch) is not int or scratch < 0 or type(output) is not int or output < 0):
                continue
            available.append((spec, limit, scratch, output))
        jobs = db.execute("""SELECT id,profile,environment FROM jobs WHERE status='queued'
            ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq""").fetchall()
        if not jobs:
            return None
        environment = jobs[0]['environment']
        if environment == 'staging' and db.execute("""SELECT 1 FROM jobs
                WHERE environment='production' AND status IN ('queued','running','held') LIMIT 1""").fetchone():
            return None
        foreign_viewer_counts = []
        for job in jobs:
            if job['environment'] != environment:
                break
            if job['profile'] == VIEWER_PROFILE:
                # A Viewer job may be bypassed only on a node that provably
                # cannot accept its photo count. Missing or ambiguous Viewer
                # metadata is a barrier, not evidence of ineligibility.
                try:
                    viewer = db.execute('SELECT uuid FROM viewer_tasks WHERE job_id=? AND removed=0',
                                        (job['id'],)).fetchall()
                    if len(viewer) != 1:
                        return None
                    counts = db.execute('SELECT COUNT(*),SUM(CASE WHEN is_photo=1 THEN 1 ELSE 0 END),'
                                        'SUM(CASE WHEN is_photo NOT IN (0,1) OR is_photo IS NULL THEN 1 ELSE 0 END) '
                                        'FROM viewer_files WHERE uuid=?', (viewer[0]['uuid'],)).fetchone()
                    if not counts[0] or not counts[1] or counts[2]:
                        return None
                    foreign_viewer_counts.append(counts[1])
                except (KeyError, TypeError, ValueError, sqlite3.Error):
                    return None
                continue
            if job['profile'] != PROFILE:
                break
            row = self._row(db, job['id'])
            submission = json.loads(row['input_json'])
            photos = submission['manifest']['inputCounts']['images']
            for spec, limit, scratch, output in available:
                if any(not any(cap is not None and count > cap for cap in (spec['maxPhotos'], limit))
                       for count in foreign_viewer_counts):
                    continue
                if ((submission['pipeline']['odm']['requested'] and 'odm' not in spec['capabilities']) or
                    (submission['pipeline']['treedar']['requested'] and
                     ('treedar' not in spec['capabilities'] or spec['id'] not in self.queue.treedar_node_ids))):
                    continue
                if (submission['manifest']['totalBytes'] + output <= scratch
                        and all(cap is None or photos <= cap for cap in (spec['maxPhotos'], limit))):
                    return row if spec['id'] == node_id else None
        return None

    def next_for_node(self, node_id):
        """PRIVATE worker discovery; advisory only, claim remains atomic."""
        self.queue._known_node(node_id)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            self.queue._refresh_nodes(db)
            row = self._eligible_head(db, node_id)
            if row is None or self._activation(db, row) is None:
                return None
            if not self._activation_bound_grant_ready():
                return None
            return self._public(row)

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
                self._check_jcs_capability(db)
                row=self._row(db,job_id)
                self._fence_released_request(db,job_id,node_id,worker_id,worker_instance_id,claim_request_id)
                if self._activation(db,row) is None or not self._activation_bound_grant_ready():return None
                old=db.execute('SELECT * FROM reference_claims WHERE request_id=?',(claim_request_id,)).fetchone()
                if old is None:return None
                if (old['job_id'],old['node_id'],old['worker_id'],old['instance_id'])!=(job_id,node_id,worker_id,worker_instance_id):
                    raise Conflict('claim request belongs to different worker binding')
                return self._owned_private_record(db,row)

    @staticmethod
    def _fence_released_request(db, job_id, node_id, worker_id, worker_instance_id, claim_request_id):
        """Answer one released request identically on every claim path."""
        old = db.execute('SELECT * FROM reference_released_claims WHERE request_id=?',
                         (claim_request_id,)).fetchone()
        if old is None:
            return
        if (old['job_id'], old['node_id'], old['worker_id'], old['instance_id']) != (
                job_id, node_id, worker_id, worker_instance_id):
            raise Conflict('claim request belongs to different worker binding')
        raise ClaimReleased('claim request was released')

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
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            self._fence_released_request(db, job_id, node_id, worker_id, worker_instance_id, claim_request_id)
            if self._activation(db, row) is None:
                return None
            if not self._activation_bound_grant_ready():
                return None
            old = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if old:
                if (old["job_id"], old["node_id"], old["worker_id"], old["instance_id"]) != (
                        job_id, node_id, worker_id, worker_instance_id):
                    raise Conflict("claim request belongs to different worker binding")
                return self._owned_private_record(db,row)
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
            return self._owned_private_record(db,self._row(db, job_id))

    @staticmethod
    def _worker_reported(db, job_id):
        """Any durable worker or Viewer evidence forbids returning a job to queue."""
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if any(table in tables and db.execute(f'SELECT 1 FROM {table} WHERE job_id=? LIMIT 1', (job_id,)).fetchone()
               for table in _RELEASE_BARRIER_TABLES):
            return True
        # reference_outbox has no job column; a job UUID in its payload binds it.
        return db.execute('SELECT 1 FROM reference_outbox WHERE instr(payload_json,?)>0 LIMIT 1',
                          (job_id,)).fetchone() is not None

    def release_unstarted_claim(self, job_id, *, node_id, worker_id, worker_instance_id,
                                claim_request_id, claim_token, worker_attestation):
        """Held candidate: fence an authenticated claim before any transfer grant.

        This method has no HTTP route and is not connected to the worker. It
        does not prove the physical engine stayed idle; activation requires
        independently validated node fencing. The worker goes first: only its
        exact durable release attestation (ReferenceDriver.prepare_release) is
        accepted, and a job any worker reported on stays with its claim so that
        a sealed failure still ends failed instead of returning to the queue.
        """
        self.queue._known_node(node_id); self.queue.slug(worker_id)
        for value in (worker_instance_id, claim_request_id):
            protocol._slug(value, 'claim identifier')
        if type(claim_token) is not str or not claim_token.isascii():
            raise ValueError('invalid claim token')
        digest = hashlib.sha256(claim_token.encode()).hexdigest()
        binding = (job_id, node_id, worker_id, worker_instance_id, digest)
        expected = protocol.release_attestation(job_id=job_id, claim_request_id=claim_request_id,
            node_id=node_id, worker_id=worker_id, worker_instance_id=worker_instance_id)
        if (type(worker_attestation) is not dict
                or protocol.canonical_json(worker_attestation) != protocol.canonical_json(expected)):
            raise Conflict('matching worker release attestation required')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            authority = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            if not authority or authority['authority_id'] != self.queue.authority_id:
                raise Conflict('claim cannot be safely released')
            old = db.execute('SELECT * FROM reference_released_claims WHERE request_id=?',
                             (claim_request_id,)).fetchone()
            if old:
                if tuple(old[k] for k in ('job_id', 'node_id', 'worker_id', 'instance_id', 'token_hash')) != binding:
                    raise Conflict('released claim binding changed')
                return False
            row = self._row(db, job_id)
            claim = db.execute('SELECT * FROM reference_claims WHERE request_id=?',
                               (claim_request_id,)).fetchone()
            self._claim_snapshot(db, job_id, node_id=node_id, worker_id=worker_id,
                                 worker_instance_id=worker_instance_id, claim_token=claim_token)
            if (claim is None or tuple(claim[k] for k in ('job_id', 'node_id', 'worker_id', 'instance_id')) != binding[:4]
                    or not hmac.compare_digest(claim['token'], claim_token)
                    or row['status'] != 'running' or row['phase'] not in ('queued', 'waiting_capacity')
                    or row['node_id'] != node_id or type(row['claim_hash']) is not str
                    or not hmac.compare_digest(row['claim_hash'], digest)
                    or row['transfer_json'] is not None or row['result_json'] is not None
                    or row['private_grant_json'] is not None or row['queue_result'] is not None
                    or self._worker_reported(db, job_id)):
                raise Conflict('claim cannot be safely released')
            if (row['terminal_failure_json'] is not None or
                    db.execute('SELECT 1 FROM reference_worker_receipts WHERE job_id=? LIMIT 1', (job_id,)).fetchone() or
                    db.execute("""SELECT 1 FROM reference_outbox WHERE
                        json_extract(payload_json,'$.jobId')=? LIMIT 1""", (job_id,)).fetchone()):
                raise Conflict('claim has durable outcome evidence')
            if db.execute('SELECT COUNT(*) FROM reference_released_claims WHERE job_id=?',
                          (job_id,)).fetchone()[0] >= MAX_CLAIM_RELEASES_PER_JOB:
                raise Conflict('claim release budget exhausted')
            self._quota(db, sum(len(x) for x in (claim_request_id, *binding)))
            db.execute('INSERT INTO reference_released_claims VALUES(?,?,?,?,?,?)',
                       (claim_request_id, *binding))
            db.execute('DELETE FROM reference_claims WHERE request_id=?', (claim_request_id,))
            db.execute("UPDATE jobs SET status='queued',worker_id=NULL,node_id=NULL,claim_hash=NULL WHERE id=?", (job_id,))
            db.execute("UPDATE reference_jobs SET phase='waiting_capacity',node_id=NULL WHERE job_id=?", (job_id,))
            return True

    def claim_snapshot(self, job_id, *, node_id, worker_id, worker_instance_id, claim_token):
        """Observe one owned claim in a read transaction; never replay a claim.

        The private worker boundary authenticates the bearer and claim token.
        This envelope is evidence for a host guard, not execution authorization.
        In particular, a held or terminal parent cannot start another stage.
        """
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_jcs_capability(db)
            return self._claim_snapshot(db, job_id, node_id=node_id, worker_id=worker_id,
                worker_instance_id=worker_instance_id, claim_token=claim_token)

    def _claim_snapshot(self, db, job_id, *, node_id, worker_id, worker_instance_id, claim_token):
        """Same authenticated observation inside an existing transaction."""
        self._check_jcs_capability(db)
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
        if row is not None and self._activation(db, row) is None:
            raise Conflict('Viewer activation required for recovery')
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

    @staticmethod
    def _check_worker_claim(db, job_id, identity, token):
        if identity is None:
            return
        claim = db.execute('SELECT node_id,worker_id,instance_id,token FROM reference_claims WHERE job_id=?',
                           (job_id,)).fetchone()
        if (claim is None or type(token) is not str or not token.isascii()
                or tuple(claim[k] for k in ('node_id', 'worker_id', 'instance_id')) !=
                   (identity['node_id'], identity['worker_id'], identity['worker_instance_id'])
                or not hmac.compare_digest(claim['token'], token)):
            raise Conflict('worker claim changed')

    def update_phase(self, job_id, phase, *, expected_phase=None, error=None, worker_identity=None, claim_token=None):
        _reason(error)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_worker_claim(db, job_id, worker_identity, claim_token)
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            if phase in ('awaiting_transfer', 'fetching', 'processing'):
                if self._activation(db, row) is None:
                    raise Conflict('Viewer activation required')
                if not self._activation_bound_grant_ready():
                    raise Conflict('activation-bound source grant unavailable')
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

    def reserve(self, job_id, transfer_request, *, node_id, worker_identity=None, claim_token=None):
        """Record externally admitted capacity for the claimed worker.

        Expiry does not reset this reservation or release uncertain ownership.
        A safe retry needs an explicit ScratchLedger expiry attestation proving
        no engine mutation, plus a future reconciliation API. Until then freeze.
        """
        source_range_v2 = type(transfer_request) is dict and transfer_request.get('protocol') == 'source-range-v2'
        if source_range_v2:
            fields=("protocol", "reservationId", "generation", "expiresAt",
                "manifestSha256", "sourceVersion", "objectIds", "chunkBytes", "requestBudget",
                "byteBudget", "nonce", "attemptBudget")
            if 'viewerAttemptId' in transfer_request:
                fields=(*fields,'viewerAttemptId')
                from .source_range_transfer import _viewer_attempt
                _viewer_attempt(transfer_request['viewerAttemptId'])
            protocol._closed(transfer_request, fields, "source-range-v2 transfer request")
        else:
            protocol._closed(transfer_request, ("reservationId", "generation", "expiresAt", "manifestSha256", "byteBudget", "nonce"), "transfer request")
        for key in ("reservationId", "nonce"):
            protocol._slug(transfer_request[key], key)
        protocol._integer(transfer_request["generation"], "generation")
        protocol._integer(transfer_request["byteBudget"], "byteBudget")
        protocol._hash(transfer_request["manifestSha256"], "manifestSha256")
        if source_range_v2:
            from .source_range_transfer import CHUNK_BYTES, source_version_for_manifest
            protocol._string(transfer_request['sourceVersion'], 'sourceVersion', 256)
            protocol._integer(transfer_request['chunkBytes'], 'chunkBytes')
            protocol._integer(transfer_request['requestBudget'], 'requestBudget')
            protocol._integer(transfer_request['attemptBudget'], 'attemptBudget')
            protocol._closed({'objectIds': transfer_request['objectIds']}, ('objectIds',), 'source object closure')
            if (transfer_request['protocol'] != 'source-range-v2' or transfer_request['chunkBytes'] != CHUNK_BYTES
                    or not isinstance(transfer_request['objectIds'], list)
                    or any(type(value) is not str for value in transfer_request['objectIds'])
                    or len(transfer_request['objectIds']) != len(set(transfer_request['objectIds']))):
                raise ValueError('invalid source-range-v2 transfer request')
            for value in transfer_request['objectIds']:
                protocol._slug(value, 'source object ID')
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
            self._check_worker_claim(db, job_id, worker_identity, claim_token)
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            activation=self._activation(db, row)
            if activation is None:
                raise Conflict('Viewer activation required')
            if not self._activation_bound_grant_ready():
                raise Conflict('activation-bound source grant unavailable')
            owner=self._submission_owner(db,row)
            prepared_v3=owner is not None and owner['protocol']=='ltds-viewer-prepared-envelope-jcs-v3'
            if prepared_v3:
                if not source_range_v2 or transfer_request.get('viewerAttemptId')!=activation['viewerAttemptId']:
                    raise Conflict('prepared Viewer attempt binding differs')
            elif source_range_v2 and 'viewerAttemptId' in transfer_request:
                raise Conflict('Viewer attempt requires prepared v3 provenance')
            if row["status"] != "running" or row["node_id"] != node_id or not row["private_claim_token"]:
                raise Conflict("reservation requires a running reference claim on this node")
            if row["transfer_json"] == encoded and row["node_id"] == node_id:
                return self._public(row)
            # A replay only acknowledges the immutable stored request; it does not
            # renew the lease, grant or budget. Fresh reservations must be live.
            if source_range_v2 and expiry.timestamp() <= time.time():
                raise Conflict('source-range-v2 reservation already expired')
            if row["phase"] not in ("queued", "waiting_capacity"):
                raise Conflict("reference job cannot be reserved in this phase")
            submission = json.loads(row["input_json"])
            total_bytes = submission['manifest']['totalBytes']
            if source_range_v2:
                manifest = submission['manifest']
                required_ranges = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
                if (transfer_request['sourceVersion'] != source_version_for_manifest(manifest)
                        or set(transfer_request['objectIds']) != {item['id'] for item in manifest['objects']}
                        or transfer_request['requestBudget'] != required_ranges
                        or transfer_request['attemptBudget'] < required_ranges
                        or transfer_request['attemptBudget'] > 3 * required_ranges):
                    raise Conflict('source-range-v2 transfer identity mismatch')
                if prepared_v3 and (transfer_request['attemptBudget'] != min(protocol.MAX_SAFE_INTEGER, 3*required_ranges)
                                    or transfer_request['byteBudget'] != min(protocol.MAX_SAFE_INTEGER, 3*total_bytes)):
                    raise Conflict('prepared Viewer source policy differs')
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
            self._check_jcs_capability(db)
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
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            if self._activation(db, row) is None:
                raise Conflict('Viewer activation required')
            if not self._activation_bound_grant_ready():
                raise Conflict('activation-bound source grant unavailable')
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
            if json.loads(row['transfer_json'] or '{}').get('protocol') == 'source-range-v2':
                from .source_range_transfer import validate_source_grant
                validate_source_grant(grant, job=self._owned_private_record(db,row), clock=clock or time.time)
            else:
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

    def private_transfer_grant(self, job_id, *, worker_identity=None, claim_token=None):
        """PRIVATE worker boundary; never serialize as part of public status."""
        with self.queue._db() as db:
            db.execute('BEGIN')
            self._check_worker_claim(db, job_id, worker_identity, claim_token)
            job = self._row(db, job_id)
            if worker_identity is not None and (job['status'] != 'running' or job['phase'] != 'awaiting_transfer'):
                raise Conflict('worker grant is not available in this phase')
            self._check_jcs_capability(db)
            reference = self._row(db, job_id)
            if self._activation(db, reference) is None:
                raise Conflict('Viewer activation required')
            if not self._activation_bound_grant_ready():
                raise Conflict('activation-bound source grant unavailable')
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
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            claim = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if (not claim or (claim["job_id"], claim["node_id"], claim["worker_id"], claim["instance_id"]) !=
                    (job_id, node_id, worker_id, worker_instance_id)
                    or type(claim_token) is not str or not hmac.compare_digest(claim["token"], claim_token)):
                raise Conflict("worker result does not match persisted claim ownership")
            if row["status"] == "held" and row["held_reason"] != "coordinator_restart":
                raise Conflict("operator-held worker results require explicit resolution")
            return self._seal_result(db, row, result, allow_recovery=True)

    def _latch_viewer_copy_requirement(self, job_id):
        """Persist observed tightening before responding, including on rejection.

        NULL policy remains an explicit reconciliation case. A separate short
        transaction ensures rejecting an unsigned receipt cannot roll back an
        already observed requirement.
        """
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            if row['viewer_receipt_required'] not in (0, 1):
                raise Conflict('legacy Viewer acceptance policy requires reconciliation')
            required = (row['viewer_receipt_required'] == 1
                        or row['environment'] in self.viewer_required_environments
                        or db.execute('SELECT 1 FROM reference_viewer_transfers WHERE job_id=?', (job_id,)).fetchone())
            if required and row['viewer_receipt_required'] == 0:
                db.execute('UPDATE reference_jobs SET viewer_receipt_required=1,updated_at=CURRENT_TIMESTAMP WHERE job_id=?',
                           (job_id,))

    def accept(self, job_id, receipt, *, viewer_receipt=None, trusted_viewer_receipt_keys=None):
        self._latch_viewer_copy_requirement(job_id)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            if not row["result_json"] or row["phase"] not in ("awaiting_acceptance", "completed"):
                raise Conflict("result is not ready for acceptance")
            result = json.loads(row["result_json"])
            transfer = db.execute('SELECT receipt_id,viewer_manifest_json,acceptance_json FROM reference_viewer_transfers WHERE job_id=?',
                                  (job_id,)).fetchone()
            if row['viewer_receipt_required'] not in (0, 1):
                raise Conflict('legacy Viewer acceptance policy requires reconciliation')
            require_viewer = (row['viewer_receipt_required'] == 1 or transfer is not None
                              or row['environment'] in self.viewer_required_environments)
            if require_viewer and viewer_receipt is None:
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
            if (identities | (viewer_identities if viewer_checked is not None else set())) & self._held_copy_ids(db): raise Conflict('receipt identity held by v3 proof')
            from .reference_copy_hold_integrity import legacy_reserved_ids
            try: legacy_reserved_ids(db)
            except (ValueError,sqlite3.Error): raise Conflict('acceptance identity unavailable') from None
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
            if self._receipt_namespace_gate is not None:
                prior_viewer=db.execute('SELECT 1 FROM reference_viewer_receipts WHERE acceptance_receipt_id=?',(checked['receiptId'],)).fetchone() if viewer_checked is not None else None
                proposed=(identities if not old else set()) | (viewer_identities if viewer_checked is not None and not prior_viewer else set())
                try:self._receipt_namespace_gate.check_in_transaction(db,proposed=proposed)
                except ValueError:raise Conflict('shared receipt namespace unavailable') from None
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
            self._check_jcs_capability(db)
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
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
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
            self._check_jcs_capability(db)
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
            self._check_jcs_capability(db)
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
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
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
            self._check_jcs_capability(db)
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
        """PRIVATE worker relay; only signed Viewer receipts may prove accepted copies.

        viewerCopyRequired tells the worker whether completion needs verified
        signed coverage. Durable per-job policy, an existing Viewer transfer or
        the current environment target can each only tighten it; legacy NULL
        policy is never relayed as legacy-OK and requires reconciliation.
        """
        self._latch_viewer_copy_requirement(job_id)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            row = self._row(db, job_id)
            if row['viewer_receipt_required'] not in (0, 1):
                raise Conflict('legacy Viewer acceptance policy requires reconciliation')
            required = bool(row['viewer_receipt_required'] == 1 or row['environment'] in self.viewer_required_environments
                or db.execute('SELECT 1 FROM reference_viewer_transfers WHERE job_id=?', (job_id,)).fetchone())
            receipts = []
            signed_coverage = set()
            for item in db.execute('SELECT receipt_id,receipt_json FROM reference_acceptances WHERE job_id=? ORDER BY receipt_id', (job_id,)):
                acceptance = json.loads(item['receipt_json'])
                signed = db.execute('SELECT receipt_json FROM reference_viewer_receipts WHERE acceptance_receipt_id=?',
                    (item['receipt_id'],)).fetchone()
                receipts.append(dict(acceptance=acceptance, viewerReceipt=json.loads(signed[0])) if signed else acceptance)
                if signed:
                    signed_coverage.update(acceptance['acceptedObjectIds'])
            complete = row['phase'] == 'completed'
            if required and complete:
                # This is an advisory status only: the worker must still
                # verify each signature and its local object coverage. Old
                # unsigned completion cannot count as verified coverage.
                result = json.loads(row['result_json']) if row['result_json'] else None
                complete = bool(result and signed_coverage == {obj['id'] for obj in result['objects']})
            return {'receipts': receipts, 'fullyAccepted': complete, 'viewerCopyRequired': required}

    def pending_outbox(self, *, now=None, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("invalid outbox limit")
        now = time.time() if now is None else now
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            return [{"identity": row["identity"], "kind": row["kind"],
                     "payload": json.loads(row["payload_json"]), "attempts": row["attempts"]}
                    for row in db.execute("SELECT * FROM reference_outbox WHERE acknowledged=0 AND next_attempt<=? ORDER BY created_at,identity LIMIT ?", (now, limit))]

    def acknowledge_outbox(self, identity):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._check_jcs_capability(db)
            if not db.execute("UPDATE reference_outbox SET acknowledged=1 WHERE identity=?", (identity,)).rowcount:
                raise Conflict("unknown outbox identity")

    def retry_outbox(self, identity, *, now=None):
        now = time.time() if now is None else now
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._check_jcs_capability(db)
            row = db.execute("SELECT attempts,acknowledged FROM reference_outbox WHERE identity=?", (identity,)).fetchone()
            if not row or row["acknowledged"]:
                raise Conflict("outbox item unavailable")
            delay = min(3600, 2 ** min(row["attempts"], 12))
            db.execute("UPDATE reference_outbox SET attempts=attempts+1,next_attempt=? WHERE identity=?", (now + delay, identity))
            return delay
