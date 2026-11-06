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
import threading
import time
import uuid
from datetime import datetime, timezone

from .maintenance import Conflict
from . import reference_protocol as protocol
from .viewer_result_manifest import derive_viewer_result_manifest, validate_viewer_acceptance
from .prototype import PROFILE as TREEDAR_PROFILE, VIEWER_PROFILE
from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
from .reference_source_intake_store import ReferenceSourceIntakeMethods
from .reference_worker_hold import HOLD_REASONS, claim_refused, queue_status, stored_record

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
_SOURCE_INTAKE_FIELDS = {"schemaVersion", "submissionId", "providerTaskId", "sourceManifest",
                         "derivedManifest", "manifest", "pipeline", "capabilityFingerprint",
                         "environment", "requestDigest", "manifestSha256", "sourceManifestSha256",
                         "derivedManifestSha256", "pipelineDigest"}


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


def _reason(value):
    if value is not None and (type(value) is not str or not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", value)):
        raise ValueError("error must be a bounded reason code")


class ReferenceStore(ReferenceSourceIntakeMethods):
    def __init__(self, queue, max_queued_jobs=10_000, max_metadata_bytes=64 * 1024 * 1024,
                 *, local_max_photos=1000, local_max_input_bytes=20 * 1024**3,
                 main_node_id=None, node_limits=None, node_option_validator=None,
                 source_routing_keys=None):
        if PROFILE not in queue.allowed_profiles:
            raise ValueError("reference queue profile is disabled")
        for value in (max_queued_jobs, max_metadata_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("metadata quotas must be positive integers")
        self.queue = queue
        from .viewer_source_attestation import ValidatedSourceKeys, validate_trusted_keys
        self._source_routing_keys = (source_routing_keys if isinstance(source_routing_keys,ValidatedSourceKeys)
            else validate_trusted_keys(source_routing_keys) if source_routing_keys is not None else None)
        self.max_queued_jobs = max_queued_jobs
        self.max_metadata_bytes = max_metadata_bytes
        for value in (local_max_photos, local_max_input_bytes):
            if type(value) is not int or value < 1:
                raise ValueError("local node limits must be positive integers")
        self.local_max_photos = local_max_photos
        self.local_max_input_bytes = local_max_input_bytes
        self.main_node_id = main_node_id or next((n for n in queue.node_ids if n == 'main'), None)
        if self.main_node_id is not None and (self.main_node_id not in queue.node_ids or self.main_node_id == 'cluster'):
            raise ValueError("main node must be a configured node distinct from cluster")
        if node_limits is None:
            node_limits = {}
        if type(node_limits) is not dict or set(node_limits) - set(queue.node_ids):
            raise ValueError("per-node limits must match configured nodes")
        self._configured_node_limits = {}
        for node_id in queue.node_ids:
            raw = node_limits.get(node_id, {})
            if type(raw) is not dict or set(raw) - {"maxImages", "maxInputBytes"}:
                raise ValueError("invalid per-node capacity limits")
            images = raw.get("maxImages", local_max_photos if node_id == "cluster" else None)
            size = raw.get("maxInputBytes", local_max_input_bytes if node_id == "cluster" else None)
            if ((images is not None and (type(images) is not int or images < 1))
                    or (size is not None and (type(size) is not int or size < 1))):
                raise ValueError("per-node capacity limits must be positive integers")
            self._configured_node_limits[node_id] = {"maxImages": images, "maxInputBytes": size}
        if node_option_validator is not None and not callable(node_option_validator):
            raise ValueError("node option validator must be callable")
        self._node_option_validator = node_option_validator
        self._capacity_lock = threading.RLock()
        self._capacity_snapshot = {node_id: {"ready": True, "maxImages": None,
            "maxInputBytes": None, "taskQueueCount": 0} for node_id in queue.node_ids}
        self._node_option_snapshots = None
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
                CREATE TABLE IF NOT EXISTS reference_source_intakes (
                  job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id),
                  request_digest TEXT NOT NULL, source_manifest_json TEXT NOT NULL,
                  viewer_task_id TEXT, provider_task_id TEXT, source_attestation_json TEXT,
                  source_attestation_sha TEXT,
                  derived_manifest_json TEXT NOT NULL, source_manifest_sha TEXT NOT NULL,
                  derived_manifest_sha TEXT NOT NULL, pipeline_manifest_sha TEXT NOT NULL,
                  reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0),
                  source_receipt_json TEXT, source_receipt_sha TEXT,
                  derived_receipt_json TEXT, derived_receipt_sha TEXT,
                  source_state TEXT NOT NULL CHECK(source_state IN ('receiving','accepted','held')),
                  derived_state TEXT NOT NULL CHECK(derived_state IN ('receiving','accepted','held')),
                  created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
                CREATE TABLE IF NOT EXISTS reference_source_object_receipts (
                  job_id TEXT NOT NULL REFERENCES reference_source_intakes(job_id),
                  generation INTEGER NOT NULL, object_id TEXT NOT NULL,
                  object_kind TEXT NOT NULL CHECK(object_kind IN ('source','derived')),
                  storage_key TEXT NOT NULL, byte_size INTEGER NOT NULL, sha256 TEXT NOT NULL,
                  mount_id TEXT NOT NULL, receipt_json TEXT NOT NULL,
                  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                  PRIMARY KEY(job_id,generation,object_id));
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
                CREATE TABLE IF NOT EXISTS reference_source_grant_tokens (
                  token_sha256 TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES reference_jobs(job_id),
                  generation INTEGER NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
            """)
            intake_columns = {row[1] for row in db.execute('PRAGMA table_info(reference_source_intakes)')}
            if 'reserved_bytes' not in intake_columns:
                db.execute('ALTER TABLE reference_source_intakes ADD COLUMN reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0)')
                for intake in db.execute('SELECT job_id,source_manifest_json,derived_manifest_json FROM reference_source_intakes').fetchall():
                    source=json.loads(intake['source_manifest_json']);derived=json.loads(intake['derived_manifest_json'])
                    db.execute('UPDATE reference_source_intakes SET reserved_bytes=? WHERE job_id=?',
                        (source['totalBytes']+sum(item['byteSize'] for item in derived['objects']),intake['job_id']))
            for column in ('source_receipt_json','source_receipt_sha','derived_receipt_json','derived_receipt_sha'):
                if column not in intake_columns:
                    db.execute(f'ALTER TABLE reference_source_intakes ADD COLUMN {column} TEXT')
            for column in ('viewer_task_id','provider_task_id','source_attestation_json','source_attestation_sha'):
                if column not in intake_columns:
                    db.execute(f'ALTER TABLE reference_source_intakes ADD COLUMN {column} TEXT')
            transfer_columns = {row[1] for row in db.execute('PRAGMA table_info(reference_viewer_transfers)')}
            if 'viewer_manifest_json' not in transfer_columns:
                db.execute('ALTER TABLE reference_viewer_transfers ADD COLUMN viewer_manifest_json TEXT')
            from .source_intake_mode import initialize as initialize_source_mode
            if not db.in_transaction:
                db.execute('BEGIN IMMEDIATE')
            initialize_source_mode(db)
            # Preserve execution phase across restart. Queue's held status is an
            # overlay; it neither authorizes replay nor erases worker progress.
        # The coordinator lease permits one runtime owner per Queue process;
        # tests and service assembly may replace an unused store before use.
        queue._reference_scheduler = self

    def _row(self, db, job_id):
        has_failure = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone()
        failure_columns = 'f.failure_json AS terminal_failure_json,f.delivery_state AS failure_delivery_state' if has_failure else 'NULL AS terminal_failure_json,NULL AS failure_delivery_state'
        failure_join = 'LEFT JOIN reference_worker_failures f ON f.job_id=r.job_id AND f.generation=r.generation' if has_failure else ''
        row = db.execute(f"""SELECT r.*,j.status,j.result AS queue_result,j.claim_hash,j.held_reason,c.token AS private_claim_token,
          {failure_columns},
          g.grant_json AS private_grant_json,
          s.request_digest AS source_request_digest,s.viewer_task_id,s.provider_task_id,
          s.source_attestation_json,s.source_attestation_sha,s.source_manifest_json,s.derived_manifest_json,
          s.source_manifest_sha,s.derived_manifest_sha,s.pipeline_manifest_sha,
          s.source_state,s.derived_state,s.reserved_bytes,s.source_receipt_json,s.source_receipt_sha,
          s.derived_receipt_json,s.derived_receipt_sha,
          (SELECT json_group_array(json_object('objectId',sr.object_id,'objectKind',sr.object_kind,
              'byteSize',sr.byte_size,'sha256',sr.sha256,'generation',sr.generation,
              'manifestSha256',CASE WHEN sr.object_kind='source' THEN s.source_manifest_sha ELSE s.derived_manifest_sha END))
              FROM (SELECT * FROM reference_source_object_receipts
                    WHERE job_id=r.job_id AND generation=r.generation ORDER BY object_kind,object_id) sr
              ) AS source_object_receipts_json
          FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
          LEFT JOIN reference_claims c ON c.job_id=r.job_id
          LEFT JOIN reference_transfer_grants g ON g.job_id=r.job_id
          LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
          {failure_join}
          WHERE r.job_id=? AND j.profile=?""", (job_id, PROFILE)).fetchone()
        if row is None:
            raise Conflict("unknown reference job")
        return row

    def _public(self, row):
        result = json.loads(row["result_json"]) if row["result_json"] else None
        grant = json.loads(row['private_grant_json']) if row['private_grant_json'] else None
        recovering = row["status"] == "held" and row["held_reason"] == "coordinator_restart"
        # A resolved hold keeps its code: a claim alone never advances the phase.
        worker_hold = row['status'] in ('held', 'failed') and row['error'] in HOLD_REASONS
        phase, status, error = ('held' if recovering or worker_hold else row['phase']), row['status'], ('coordinator_restart' if recovering else row['error'])
        source_receiving = row['source_state'] is not None and (
            row['source_state'] != 'accepted' or row['derived_state'] != 'accepted')
        source_held = row['source_state'] == 'held' or row['derived_state'] == 'held'
        if not recovering and source_held:
            phase, status, error = 'held', 'held', 'source_receipt_held'
        elif not recovering and source_receiving:
            phase = 'source_receiving'
        if row['terminal_failure_json'] and row['failure_delivery_state'] == 'awaiting_viewer_failure_acceptance' and status == 'failed':
            failure = json.loads(row['terminal_failure_json'])
            expected = {key: failure[key] for key in ('failureSha256', 'terminalState', 'errorCode')}
            if row['queue_result'] and json.loads(row['queue_result']) == expected:
                status, error = failure['terminalState'], failure['errorCode']
                phase = 'completed' if row['phase'] == 'completed' else 'awaiting_acceptance'
        result_record = {"jobId": row["job_id"], "attemptId": row['submission_id'],
                "providerTaskId": row['job_id'] if json.loads(row['input_json'])['pipeline']['odm']['requested'] else None,
                "generation": row["generation"],
                "environment": row["environment"], "phase": phase,
                "manifestSha256": row["manifest_sha"], "pipelineDigest": row["pipeline_digest"],
                "status": status, "nodeId": row["node_id"],
                "transferRequest": json.loads(row["transfer_json"]) if row["transfer_json"] else None,
                "transferGrantAcknowledgement": self._transfer_grant_ack(grant) if grant else None,
                "error": error,
                "resultManifestSha256": result["resultManifestSha256"] if result else None}
        if row['source_state'] is not None:
            result_record.update(sourceIntakeVersion=2,
                sourceState='retained' if row['source_state']=='accepted' else row['source_state'],
                derivedState='retained' if row['derived_state']=='accepted' else row['derived_state'],
                sourceManifestSha256=row['source_manifest_sha'],
                derivedManifestSha256=row['derived_manifest_sha'],
                pipelineManifestSha256=row['pipeline_manifest_sha'],
                pipelineDigest=row['pipeline_digest'],
                providerTaskId=row['provider_task_id'],
                sourceReceipt=json.loads(row['source_receipt_json']) if row['source_receipt_json'] else None,
                sourceReceiptSha256=row['source_receipt_sha'],
                derivedReceipt=json.loads(row['derived_receipt_json']) if row['derived_receipt_json'] else None,
                derivedReceiptSha256=row['derived_receipt_sha'],
                sourceObjectReceipts=json.loads(row['source_object_receipts_json'] or '[]'),
                derivedObjectReceipts=[])
            if result_record['sourceObjectReceipts']:
                acks = result_record['sourceObjectReceipts']
                result_record['sourceObjectReceipts'] = [ack for ack in acks if ack['objectKind']=='source']
                result_record['derivedObjectReceipts'] = [ack for ack in acks if ack['objectKind']=='derived']
            if not recovering and source_held:
                result_record.update(queueReason='source_receipt_held',
                    queueMessage='Waiting for operator resolution of source custody.')
            elif not recovering and source_receiving:
                result_record.update(queueReason='source_receiving',
                    queueMessage='Waiting for durable source and derived input receipts.')
        result_record.update(queue_status(stored_record(row)) or {})
        return result_record

    def _record(self, row, private, db=None):
        result = self._public(row)
        if not private:
            # Here and not in _public: claim.update(public) would journal it, and
            # worker replies stay byte-unchanged.
            from .reference_presentation import latch, project
            presentation = project(result, latch(db, row['job_id']))
            if presentation is not None:
                result['presentation'] = presentation
        if private:
            result.update(submission=json.loads(row["input_json"]),
                          resultManifest=json.loads(row["result_json"]) if row["result_json"] else None,
                          claimToken=row["private_claim_token"], executionPhase=row["phase"])
            if row['source_state'] == 'accepted' and row['derived_state'] == 'accepted':
                result.update(authorityId=self.queue.authority_id,
                              sourceAcceptanceBinding=self._source_acceptance_binding(row))
        return result

    def _metadata_used_bytes(self, db):
        from .reference_source_readers import metadata_bytes as source_reader_metadata_bytes
        used = db.execute("""SELECT COALESCE(SUM(length(CAST(input_json AS BLOB))+
          COALESCE(length(CAST(transfer_json AS BLOB)),0)+COALESCE(length(CAST(result_json AS BLOB)),0)),0)
          FROM reference_jobs""").fetchone()[0]
        used += db.execute("""SELECT COALESCE(SUM(length(CAST(source_manifest_json AS BLOB))+
          length(CAST(derived_manifest_json AS BLOB))+
          COALESCE(length(CAST(source_attestation_json AS BLOB)),0)),0) FROM reference_source_intakes""").fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_source_intake_modes'").fetchone():
            from .source_intake_mode import _check_schema as check_source_mode_schema
            check_source_mode_schema(db)
            used += db.execute("""SELECT COALESCE(SUM(length(CAST(job_id AS BLOB))+
              length(CAST(mode AS BLOB))),0) FROM reference_source_intake_modes""").fetchone()[0]
        used += db.execute("SELECT COALESCE(SUM(length(CAST(receipt_json AS BLOB))),0) FROM reference_source_object_receipts").fetchone()[0]
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
        # Source range metadata is introduced additively and is accounted by
        # the existing source-intake mixin so all quota decisions share one
        # calculation, including range requests, custody pins and renewals.
        used += self._source_range_metadata_bytes(db)
        used += source_reader_metadata_bytes(db)
        from .reference_acceptance_observed import metadata_bytes as observation_metadata_bytes
        used += observation_metadata_bytes(db)
        from .node_inventory import metadata_bytes as inventory_metadata_bytes
        used += inventory_metadata_bytes(db)
        from .coordinator_freeze import metadata_bytes as freeze_metadata_bytes
        used += freeze_metadata_bytes(db)
        from .source_release import metadata_bytes as source_release_metadata_bytes
        used += source_release_metadata_bytes(db)
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_intake_reservations'").fetchone():
            columns={row['name'] for row in db.execute('PRAGMA table_info(direct_intake_reservations)')}
            if columns!={'reservation_id','job_id','attempt_id','generation','environment','authority_id',
                         'manifest_sha','derived_manifest_sha','node_id','worker_id','instance_id','state'}:
                raise Conflict('direct reservation schema requires inspection')
            used += db.execute('''SELECT COALESCE(SUM(length(reservation_id)+length(job_id)+length(attempt_id)
                +length(CAST(generation AS TEXT))+length(environment)+length(authority_id)+length(manifest_sha)+length(derived_manifest_sha)
                +length(node_id)+length(worker_id)+length(instance_id)+length(state)),0)
                FROM direct_intake_reservations''').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_negotiation_candidate'").fetchone():
            from .direct_negotiation_candidate import verify_schema, NegotiationHeld
            try: verify_schema(db)
            except NegotiationHeld: raise Conflict('direct negotiation schema requires inspection') from None
            used += db.execute('''SELECT COALESCE(SUM(length(CAST(principal AS BLOB))+
                length(CAST(operation AS BLOB))+length(request)+length(capability)+length(response)),0)
                FROM direct_negotiation_candidate''').fetchone()[0]
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='direct_intake_profile_bindings'").fetchone():
            from .direct_intake_reservation import verify_profile_schema
            verify_profile_schema(db)
            used += db.execute('''SELECT COALESCE(SUM(length(CAST(reservation_id AS BLOB))+
                length(CAST(principal AS BLOB))+length(CAST(operation AS BLOB))+
                length(CAST(profile AS BLOB))),0) FROM direct_intake_profile_bindings''').fetchone()[0]
        from .direct_handoff_census import verify_census as direct_handoff_census
        used += direct_handoff_census(db)
        from .direct_v2_handoff_journal import metadata_bytes as direct_v2_handoff_bytes
        used += direct_v2_handoff_bytes(db)
        from .direct_v2_reservation_schema import metadata_bytes as direct_v2_reservation_metadata
        used += direct_v2_reservation_metadata(db)
        from .direct_preparation_journal import metadata_bytes as direct_preparation_metadata
        used += direct_preparation_metadata(db)
        from .direct_snapshot_page_journal import metadata_bytes as direct_snapshot_metadata
        used += direct_snapshot_metadata(db)
        from .direct_snapshot_page_journal import selected_metadata_bytes
        used += selected_metadata_bytes(db)
        return used

    def _quota(self, db, extra_bytes=0):
        used = self._metadata_used_bytes(db)
        if used + extra_bytes > self.max_metadata_bytes:
            gate = getattr(self.queue, '_coordinator_freeze', None)
            if gate is not None and gate.reserve_startup_metadata(db, extra_bytes):
                return
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
              manifest_sha,pipeline_digest,phase) VALUES(?,?,?,?,?,?,?,'queued')""",
              (job_id, checked["submissionId"], checked["requestDigest"], checked["environment"], encoded,
               checked["manifestSha256"], checked["pipelineDigest"]))
            return self._public(self._row(db, job_id)), True

    def get(self, job_id, *, private=False):
        with self.queue._db() as db:
            return self._record(self._row(db, job_id), private, db)

    def find_submission(self, submission_id, *, environment):
        """Recover a lost submit response without issuing another execution."""
        protocol._uuid(submission_id, 'submission ID')
        protocol._environment(environment)
        with self.queue._db() as db:
            row = db.execute('SELECT job_id FROM reference_jobs WHERE submission_id=? AND environment=?',
                             (submission_id, environment)).fetchone()
            return self._record(self._row(db, row[0]), False, db) if row else None

    def list(self, *, private=False):
        with self.queue._db() as db:
            ids = [row[0] for row in db.execute("SELECT job_id FROM reference_jobs ORDER BY created_at,job_id")]
            return [self._record(self._row(db, job_id), private, db) for job_id in ids]

    @staticmethod
    def _transition(row, phase, expected_phase=None):
        if expected_phase is not None and row["phase"] != expected_phase:
            raise Conflict("reference phase changed")
        if phase not in PHASES or (phase != row["phase"] and phase not in PHASES[row["phase"]]):
            raise Conflict("invalid reference phase transition")

    def update_fleet_snapshot(self, capacities, node_options=None):
        """Replace the in-memory live capacity/option view atomically.

        Worker discovery calls this before /next and new /claim operations. A
        failed or incomplete probe must not leave an older usable snapshot.
        """
        if type(capacities) is not dict or set(capacities) != set(self.queue.node_ids):
            raise ValueError("capacity snapshot must cover the configured fleet")
        checked = {}
        for node_id, value in capacities.items():
            if (type(value) is not dict or set(value) -
                    {"ready", "maxImages", "maxInputBytes", "taskQueueCount"}
                    or type(value.get("ready")) is not bool):
                raise ValueError("invalid node capacity snapshot")
            for name in ("maxImages", "maxInputBytes"):
                number = value.get(name)
                if number is not None and (type(number) is not int or number < 1):
                    raise ValueError("invalid node capacity ceiling")
            queued = value.get("taskQueueCount")
            if type(queued) is not int or queued < 0:
                raise ValueError("invalid node queue observation")
            checked[node_id] = dict(value)
        if node_options is not None:
            if (type(node_options) not in (list, tuple)
                    or {node.get("role") for node in node_options if type(node) is dict} != set(self.queue.node_ids)
                    or len(node_options) != len(self.queue.node_ids)):
                raise ValueError("option snapshot must cover the configured fleet")
            options = {node["role"]: dict(node) for node in node_options}
        else:
            options = None
        with self._capacity_lock:
            self._capacity_snapshot = checked
            self._node_option_snapshots = options

    @staticmethod
    def _minimum_limit(first, second):
        if first is None:
            return second
        if second is None:
            return first
        return min(first, second)

    def _node_fits(self, submission, node_id):
        with self._capacity_lock:
            capacity = dict(self._capacity_snapshot[node_id])
            option_snapshot = (self._node_option_snapshots or {}).get(node_id)
        if not capacity["ready"] or capacity["taskQueueCount"] != 0:
            return False
        limits = self._configured_node_limits[node_id]
        max_images = self._minimum_limit(capacity["maxImages"], limits["maxImages"])
        max_bytes = self._minimum_limit(capacity["maxInputBytes"], limits["maxInputBytes"])
        manifest = submission["manifest"]
        images, total_bytes = manifest["inputCounts"]["images"], manifest["totalBytes"]
        if ((max_images is not None and images > max_images)
                or (max_bytes is not None and total_bytes > max_bytes)):
            return False
        if submission["pipeline"]["treedar"]["requested"] and node_id not in self.queue.treedar_node_ids:
            return False
        if option_snapshot is not None:
            if option_snapshot.get("available") is not True:
                return False
            if self._node_option_validator is not None:
                try:
                    if self._node_option_validator(submission, option_snapshot) is not True:
                        return False
                except Exception:
                    return False
        return True

    def _node_has_slot(self, db, node_id):
        if self.queue._node(db, node_id)["state"] not in ("active", "scheduled"):
            return False
        return db.execute("SELECT 1 FROM jobs WHERE node_id=? AND status IN ('running','held') LIMIT 1",
                          (node_id,)).fetchone() is None

    def _select_node(self, db, submission, profile=PROFILE, nodes=None):
        for node_id in self.queue.node_ids if nodes is None else nodes:
            if (profile not in self.queue.node_profiles(node_id)
                    or not self._node_has_slot(db, node_id)
                    or not self._node_fits(submission, node_id)):
                continue
            return node_id
        return None

    @staticmethod
    def _viewer_submission(db, job_id, environment):
        """Read the private Viewer queue metadata needed for size admission."""
        task = db.execute("SELECT uuid,options,removed,environment,stage,phase FROM viewer_tasks WHERE job_id=?",
                          (job_id,)).fetchone()
        if (task is None or task["removed"] or task["environment"] != environment
                or task["stage"] != "queued" or task["phase"] not in ("queued", "waiting_node")):
            return None
        files = db.execute("""SELECT COUNT(*) AS images,COALESCE(SUM(bytes),0) AS total_bytes,
                            COALESCE(SUM(is_photo),0) AS photo_count FROM viewer_files WHERE uuid=?""",
                           (task["uuid"],)).fetchone()
        try:
            raw_options = json.loads(task["options"])
            if (type(raw_options) is not list or any(type(item) is not dict
                    or set(item) != {"name", "value"} or type(item["name"]) is not str
                    for item in raw_options)):
                return None
            options = {item["name"]: item["value"] for item in raw_options}
            if len(options) != len(raw_options) or files["images"] != files["photo_count"]:
                return None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return {"manifest": {"inputCounts": {"images": files["images"]},
                             "totalBytes": files["total_bytes"]},
                "pipeline": {"odm": {"options": options},
                             "treedar": {"requested": False}}}

    def _candidate_any_row(self, db, node_id):
        """First globally ordered, profile-eligible job this free node can run.

        Reference and native Viewer schedulers share this selector inside their
        existing IMMEDIATE transactions. Profiles without trustworthy size
        metadata conservatively reserve the node's queue position.
        """
        self.queue._known_node(node_id)
        self.queue._refresh_nodes(db)
        if not self._node_has_slot(db, node_id):
            return None
        for environment in ("production", "staging"):
            nodes = None
            if environment == "staging":
                # Production on this node comes first. Staging then selects only
                # among fence-open nodes, so a closed small node cannot strand
                # it from a later open one.
                nodes = [node for node in self.queue.node_ids if self.queue._staging_open(db, node)]
                if node_id not in nodes:
                    return None
            rows = db.execute("SELECT id,profile,environment FROM jobs WHERE (status='queued' OR "
                              "(status='held' AND held_reason='direct_admission_pending')) AND environment=? "
                              "ORDER BY seq", (environment,)).fetchall()
            for item in rows:
                profile = item["profile"]
                if profile not in self.queue.node_profiles(node_id):
                    continue
                if profile == PROFILE:
                    row = self._row(db, item["id"])
                    if row["phase"] not in ("queued", "waiting_capacity"):
                        continue
                    submission = json.loads(row["input_json"])
                    if row['status']=='held' and row['held_reason']=='direct_admission_pending':
                        try:
                            if self._source_routing_keys is None:
                                raise ValueError('trusted direct source keys unavailable')
                            source=json.loads(row['source_manifest_json'])
                            derived=json.loads(row['derived_manifest_json'])
                            attestation=json.loads(row['source_attestation_json'])
                            from .viewer_source_attestation import proof_digest
                            proof=attestation['proof']
                            total=sum(obj['byteSize'] for manifest in (source,derived) for obj in manifest['objects'])
                            protocol._integer(total,'direct retained bytes',allow_zero=True)
                            original=dict(schemaVersion=2,submissionId=submission['submissionId'],
                                providerTaskId=row['provider_task_id'],viewerTaskId=row['viewer_task_id'],
                                sourceManifest=source,derivedManifest=derived,manifest=submission['manifest'],
                                pipeline=submission['pipeline'],capabilityFingerprint=submission['capabilityFingerprint'],
                                sourceAttestation=attestation)
                            if (sum(obj['byteSize'] for obj in source['objects'])!=source['totalBytes']
                                    or source['sha256']!=protocol.digest({k:v for k,v in source.items() if k!='sha256'})
                                    or derived['sha256']!=protocol.digest({k:v for k,v in derived.items() if k!='sha256'})
                                    or source['sha256']!=row['source_manifest_sha']
                                    or derived['sha256']!=row['derived_manifest_sha']
                                    or attestation['attestationSha256']!=row['source_attestation_sha']
                                    or attestation['attestationSha256']!=proof_digest(proof)
                                    or proof['sourceManifestSha256']!=source['sha256']
                                    or proof['derivedManifestSha256']!=derived['sha256']
                                    or proof['pipelineManifestSha256']!=row['manifest_sha']
                                    or proof['pipelineDigest']!=row['pipeline_digest']
                                    or protocol.digest(original)!=row['source_request_digest']):
                                raise ValueError('direct retained inventory changed')
                            from .viewer_source_attestation import validate_and_verify
                            validate_and_verify(attestation,trusted_keys=self._source_routing_keys,
                                authority_id=self.queue.authority_id,environment=row['environment'],
                                submission_id=submission['submissionId'],provider_task_id=row['provider_task_id'],
                                attempt_id=submission['submissionId'],viewer_task_id=row['viewer_task_id'],
                                source_manifest_sha256=source['sha256'],derived_manifest_sha256=derived['sha256'],
                                pipeline_manifest_sha256=row['manifest_sha'],pipeline_digest=row['pipeline_digest'])
                            submission['manifest']['totalBytes']=total
                        except (ValueError,TypeError,KeyError,AttributeError):
                            # Unknown size remains an ordering barrier; it is
                            # never a reason to hand the node to a later job.
                            return item
                    if self._select_node(db, submission, PROFILE, nodes) == node_id:
                        return item
                    continue
                if profile == VIEWER_PROFILE:
                    submission = self._viewer_submission(db, item["id"], item["environment"])
                    if submission is None:
                        # Missing/corrupt private metadata must not let a later
                        # profile bypass this job on a node that could run Viewer.
                        return item
                    if self._select_node(db, submission, VIEWER_PROFILE, nodes) == node_id:
                        return item
                    continue
                if profile == TREEDAR_PROFILE and node_id not in self.queue.treedar_node_ids:
                    continue
                # Legacy or future profiles have no shared size contract yet.
                # Their advertised profile membership is sufficient to hold this
                # node's place in FIFO until that contract is available.
                return item
        return None

    def _reference_input(self, db, job):
        """Return hash-verified (images, bytes, treedar), or None for doubtful metadata."""
        row = db.execute("SELECT environment,input_json,manifest_sha,pipeline_digest FROM reference_jobs WHERE job_id=?",
                         (job["id"],)).fetchone()
        if row is None or row["environment"] != job["environment"]:
            return None
        submission = json.loads(row["input_json"])
        manifest, pipeline = submission["manifest"], submission["pipeline"]
        images, total, objects = manifest["inputCounts"]["images"], manifest["totalBytes"], manifest["objects"]
        pipeline_digest = v2_pipeline_digest if submission.get("sourceIntakeVersion") == 2 else protocol.digest
        if (type(images) is not int or type(total) is not int or type(objects) is not list
                or images != sum(item["kind"] == "image" for item in objects)
                or total != sum(item["byteSize"] for item in objects)
                or row["manifest_sha"] != manifest["sha256"] or row["manifest_sha"] != protocol.manifest_sha256(manifest)
                or row["pipeline_digest"] != pipeline_digest(pipeline)):
            return None
        return images, total, pipeline["treedar"]["requested"] is True

    def _viewer_input(self, db, job):
        """Return Viewer (images, None, False); its bytes are not rechecked here."""
        tasks = db.execute("SELECT uuid FROM viewer_tasks WHERE job_id=?", (job["id"],)).fetchall()
        submission = self._viewer_submission(db, job["id"], job["environment"]) if len(tasks) == 1 else None
        if (submission is None or not submission["manifest"]["inputCounts"]["images"]
                or db.execute("SELECT 1 FROM viewer_files WHERE uuid=? AND is_photo NOT IN (0,1) LIMIT 1",
                              (tasks[0]["uuid"],)).fetchone()):
            return None
        return submission["manifest"]["inputCounts"]["images"], None, False

    def _production_cannot_use(self, db, job, node_id):
        """Prove queued production cannot run on this node; doubt proves nothing.

        Proof is a configured capability or photo cap only. Byte caps and scratch
        stay admission guards, not staging priority. Live engine reports (readiness,
        queue depth, reported maxImages) are not proof, so a node still being
        probed stays unknown, never busy.
        """
        if job["profile"] not in self.queue.node_profiles(node_id):
            return True
        readers = {PROFILE: self._reference_input, VIEWER_PROFILE: self._viewer_input}
        try:
            evidence = readers[job["profile"]](db, job) if job["profile"] in readers else None
            if evidence is None:
                return False
            images, _total, treedar = evidence
            limit = self._configured_node_limits[node_id]["maxImages"]
            return (limit is not None and images > limit) or (treedar and node_id not in self.queue.treedar_node_ids)
        except (sqlite3.Error, KeyError, TypeError, ValueError, AttributeError):
            return False

    def _candidate_row(self, db, node_id):
        candidate = self._candidate_any_row(db, node_id)
        if candidate is None or candidate['profile'] != PROFILE:
            return None
        row = self._row(db, candidate['id'])
        if row['status'] != 'queued':
            return None
        # Keep its position as a queue barrier, but never advertise it to an
        # ordinary worker before a direct-custody handoff exists.
        if not self._ordinary_source_execution_allowed(db, row):
            return None
        return row

    def _ordinary_source_execution_allowed(self, db, row):
        """Only spool-mode V2 sources may enter the ordinary claim path.

        Direct custody needs a separately qualified exact-worker transition;
        even an erroneously accepted/queued direct row remains a queue barrier.
        Missing or damaged source mode evidence fails closed.
        """
        from .source_intake_mode import _check_schema
        _check_schema(db)
        submission = json.loads(row['input_json'])
        if type(submission) is not dict or 'directIntakeVersion' in submission:
            return False
        mode = db.execute('SELECT mode FROM reference_source_intake_modes WHERE job_id=?',
                          (row['job_id'],)).fetchone()
        version = submission.get('sourceIntakeVersion')
        if mode is None:
            return version is None and row['source_state'] is None
        if (type(version) is not int or version != 2 or row['source_state'] is None
                or mode['mode'] not in ('spool-v1', 'direct-v2')):
            return False
        return mode['mode'] == 'spool-v1'

    def _required_node(self, submission):
        """Return the first configured capacity-compatible node, ignoring load."""
        for node_id in self.queue.node_ids:
            if (PROFILE in self.queue.node_profiles(node_id)
                    and self._node_fits(submission, node_id)):
                return node_id
        return None

    def next_for_node(self, node_id):
        """PRIVATE worker discovery; advisory only, claim remains atomic."""
        self.queue._known_node(node_id)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._candidate_row(db, node_id)
            return self._public(row) if row is not None else None

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
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            # Replays also confer the prior claim token. Never return that
            # authority to an ordinary worker for direct-mode or damaged V2
            # metadata, even if an old claim row is present.
            if not self._ordinary_source_execution_allowed(db, row):
                return None
            old = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
            if old:
                if (old["job_id"], old["node_id"], old["worker_id"], old["instance_id"]) != (
                        job_id, node_id, worker_id, worker_instance_id):
                    raise Conflict("claim request belongs to different worker binding")
                return self._record(row, True)
            if claim_refused(db, job_id, claim_request_id, node_id=node_id, worker_id=worker_id,
                             worker_instance_id=worker_instance_id):
                return None
            self.queue._refresh_nodes(db)
            if row["phase"] not in ("queued", "waiting_capacity"):
                raise Conflict("reference phase cannot be claimed")
            if row['source_state'] is not None and (row['source_state']!='accepted' or row['derived_state']!='accepted'):
                raise Conflict("reference source custody is incomplete")
            submission = json.loads(row["input_json"])
            # Future direct-intake records must not enter ordinary execution
            # before a separately qualified worker-custody transition exists.
            # V1/V2 do not carry this reserved marker and retain their behavior.
            candidate = self._candidate_row(db, node_id)
            if candidate is None or candidate["job_id"] != job_id:
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
            c.worker_id AS claim_worker,c.instance_id AS claim_instance,c.token AS claim_token,
            s.source_state,s.derived_state
            FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
            JOIN reference_claims c ON c.job_id=r.job_id
            LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
            WHERE r.job_id=? AND j.profile=?''',
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
                    or row['source_state'] is not None and (row['source_state']!='accepted' or row['derived_state']!='accepted')
                    or row['queue_status'] not in ('running', 'held', 'succeeded', 'failed', 'cancelled')
                    or row['queue_status'] != 'held' and row['held_reason'] is not None
                    or submission['environment'] != row['environment']
                    or submission['manifestSha256'] != row['manifest_sha']
                    or submission['pipelineDigest'] != row['pipeline_digest']
                    or protocol.manifest_sha256(submission['manifest']) != row['manifest_sha']):
                raise ValueError()
            if row['source_state'] is None:
                if protocol.digest(submission['pipeline']) != row['pipeline_digest']:
                    raise ValueError()
            else:
                from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
                if v2_pipeline_digest(submission['pipeline']) != row['pipeline_digest']:
                    raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise Conflict('persisted claim binding unavailable') from None
        result = dict(schemaVersion=1, protocol='ltds-parent-claim-v1',
            authorityId=authority['authority_id'], jobId=job_id, generation=row['generation'],
            environment=row['environment'], nodeId=node_id, workerId=worker_id,
            workerInstanceId=worker_instance_id, claimRequestId=row['claim_request'],
            claimTokenSha256=supplied_hash, manifestSha256=row['manifest_sha'],
            pipelineDigest=row['pipeline_digest'], status=row['queue_status'],
            executionPhase=row['phase'], heldReason=row['held_reason'])
        if row['source_state'] is not None:
            result['sourceIntakeVersion'] = 2
        return result

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
        encoded, source_range_v2 = self._validate_transfer_request(transfer_request)
        self.queue._known_node(node_id)
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._reserve_transfer(db, job_id, transfer_request, node_id=node_id,
                                          encoded=encoded, source_range_v2=source_range_v2)

    @staticmethod
    def _validate_transfer_request(transfer_request, *, allow_expired=False):
        source_range_v2 = type(transfer_request) is dict and transfer_request.get('protocol') == 'source-range-v2'
        if source_range_v2:
            protocol._closed(transfer_request, ("protocol", "reservationId", "generation", "expiresAt",
                "manifestSha256", "sourceVersion", "objectIds", "chunkBytes", "requestBudget",
                "byteBudget", "nonce", "attemptBudget"), "source-range-v2 transfer request")
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
        if source_range_v2 and not allow_expired and expiry.timestamp() <= time.time():
            raise Conflict('source-range-v2 reservation already expired')
        return _json(transfer_request), source_range_v2

    def _reserve_transfer(self, db, job_id, transfer_request, *, node_id, encoded,
                          source_range_v2, recovering=False):
        # Internal caller owns the writer transaction; recovery performs its
        # exact-claim/epoch/custody CAS and receipt in this SAME transaction.
        if source_range_v2:
            from .source_range_transfer import CHUNK_BYTES, source_version_for_manifest
        row = self._row(db, job_id)
        valid_status = row["status"] == "running" if not recovering else (row["status"], row["held_reason"]) == ("held", "coordinator_restart")
        if not valid_status or row["node_id"] != node_id or not row["private_claim_token"]:
            raise Conflict("reservation requires a running reference claim on this node")
        if row["transfer_json"] == encoded and row["node_id"] == node_id:
            return self._public(row)
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
            if json.loads(row['transfer_json'] or '{}').get('protocol') == 'source-range-v2':
                from .source_range_transfer import validate_source_grant
                validate_source_grant(grant, job=self._record(row, True), clock=clock or time.time)
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

    @staticmethod
    def _require_result_claim(db, job_id, *, claim_token, node_id, worker_id,
                              worker_instance_id, claim_request_id):
        claim = db.execute('SELECT * FROM reference_claims WHERE request_id=?', (claim_request_id,)).fetchone()
        if (not claim or (claim['job_id'], claim['node_id'], claim['worker_id'], claim['instance_id']) !=
                (job_id, node_id, worker_id, worker_instance_id)
                or type(claim_token) is not str or not claim_token.isascii()
                or type(claim['token']) is not str or not claim['token'].isascii()
                or not hmac.compare_digest(claim['token'], claim_token)):
            raise Conflict('worker result does not match persisted claim ownership')

    def authenticate_worker_result_claim(self, job_id, *, claim_token, node_id, worker_id,
                                         worker_instance_id, claim_request_id):
        """Authenticate without exposing job metadata or changing durable state."""
        with self.queue._db() as db:
            self._require_result_claim(db, job_id, claim_token=claim_token, node_id=node_id,
                worker_id=worker_id, worker_instance_id=worker_instance_id, claim_request_id=claim_request_id)

    def record_worker_result(self, job_id, result, *, claim_token, node_id, worker_id,
                             worker_instance_id, claim_request_id):
        """Seal a returned terminal result, including a surviving worker's result.

        This proves persisted claim ownership, never authorizes engine execution.
        Queue.complete(token, terminal status) separately releases the shared
        slot; Viewer acceptance and cleanup remain independent later steps.
        """
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._require_result_claim(db, job_id, claim_token=claim_token, node_id=node_id,
                worker_id=worker_id, worker_instance_id=worker_instance_id, claim_request_id=claim_request_id)
            row = self._row(db, job_id)
            if row["status"] == "held" and row["held_reason"] != "coordinator_restart":
                raise Conflict("operator-held worker results require explicit resolution")
            return self._seal_result(db, row, result, allow_recovery=True)

    def accept(self, job_id, receipt, *, viewer_receipt=None, trusted_viewer_receipt_keys=None):
        with self.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._row(db, job_id)
            if not row["result_json"] or row["phase"] not in ("awaiting_acceptance", "completed"):
                raise Conflict("result is not ready for acceptance")
            submission = json.loads(row['input_json'])
            # Durable ingress identity takes precedence over possibly damaged
            # source markers. Direct custody cannot borrow spool/legacy result
            # acceptance, even if the source row or marker has disappeared.
            from .source_intake_mode import _check_schema as check_source_mode_schema
            check_source_mode_schema(db)
            mode=db.execute('SELECT mode FROM reference_source_intake_modes WHERE job_id=?',(job_id,)).fetchone()
            if mode is not None and mode['mode'] != 'spool-v1':
                raise Conflict('source intake mode mismatch')
            source_v2 = 'sourceIntakeVersion' in submission or row['source_state'] is not None
            if mode is not None and not source_v2:
                raise Conflict('source intake metadata unavailable')
            if source_v2:
                if mode is None:
                    raise Conflict('source intake mode mismatch')
                if (type(submission.get('sourceIntakeVersion')) is not int
                        or submission['sourceIntakeVersion'] != 2 or row['source_state'] is None
                        or viewer_receipt is None):
                    raise Conflict('V2 result acceptance requires its signed source-bound copy receipt')
            result = json.loads(row["result_json"])
            transfer = db.execute('SELECT receipt_id,viewer_manifest_json FROM reference_viewer_transfers WHERE job_id=?',
                                  (job_id,)).fetchone()
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
            identities = {checked['receiptId'], checked['storageReceiptId']}
            viewer_checked = None
            if viewer_receipt is not None:
                expected = dict(jobId=job_id, generation=row['generation'], environment=row['environment'],
                    inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                    resultManifestSha256=accepted_manifest['resultManifestSha256'])
                try:
                    if source_v2:
                        from .viewer_copy_receipt_v2 import verify_viewer_copy_receipt_v2
                        expected = {**self._source_acceptance_binding(row),
                                    'resultManifestSha256': accepted_manifest['resultManifestSha256']}
                        verified = verify_viewer_copy_receipt_v2(viewer_receipt,
                            trusted_keys=trusted_viewer_receipt_keys, expected=expected,
                            result_manifest=accepted_manifest, acceptance=checked)
                    else:
                        from .viewer_acceptance_receipt import verify_viewer_acceptance_receipt
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
            for item in db.execute("SELECT receipt_json FROM reference_acceptances WHERE job_id=?", (job_id,)):
                accepted.update(json.loads(item[0])["acceptedObjectIds"])
            fully = accepted == {obj["id"] for obj in result["objects"]}
            if fully:
                db.execute("UPDATE reference_jobs SET phase='completed',updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (job_id,))
            if transfer and transfer['receipt_id'] == checked['receiptId']:
                db.execute('UPDATE reference_viewer_transfers SET completed_at=COALESCE(completed_at,CURRENT_TIMESTAMP),'
                           'lease_token=NULL,lease_until=NULL,last_error=NULL WHERE job_id=?', (job_id,))
            response = {"job": self._public(self._row(db, job_id)), "created": old is None,
                        "fullyAccepted": fully, "acceptedObjectIds": sorted(accepted)}
            if source_v2:
                response['viewerReceipt'] = viewer_checked
            return response

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
            submission = json.loads(row['input_json'])
            if (type(submission) is not dict or 'sourceIntakeVersion' in submission
                    or row['source_state'] is not None):
                raise Conflict('source-bound jobs use the Viewer pull acceptance path')
            result = json.loads(row['result_json'])
            try:
                viewer_manifest = derive_viewer_result_manifest(result, job_id=job_id,
                    generation=row['generation'], environment=row['environment'],
                    pipeline_digest=row['pipeline_digest'])
            except (TypeError, ValueError):
                raise Conflict('result cannot be represented by the Viewer acceptance protocol') from None
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
                raise Conflict('Viewer acceptance manifest binding is invalid') from None
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
                        raise Conflict('Viewer transfer contract changed after an attempt; reconciliation required')
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
                    raise Conflict('Viewer transfer binding changed')
                if previous_manifest != viewer_manifest:
                    raise Conflict('Viewer result manifest changed on durable transfer')
                return previous
            self._quota(db, len(encoded.encode('utf-8')) + len(manifest_encoded.encode('utf-8')))
            db.execute('INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json,viewer_manifest_json) VALUES(?,?,?,?)',
                       (job_id, checked['receiptId'], encoded, manifest_encoded))
            return checked

    def pending_viewer_transfer_jobs(self, *, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid Viewer transfer discovery limit')
        with self.queue._db() as db:
            rows = db.execute('''SELECT r.job_id FROM reference_jobs r
                WHERE r.phase='awaiting_acceptance' AND r.result_json IS NOT NULL
                AND CASE WHEN json_valid(r.input_json) THEN json_type(r.input_json, '$.sourceIntakeVersion') IS NULL ELSE 0 END
                AND NOT EXISTS (SELECT 1 FROM reference_source_intakes s WHERE s.job_id=r.job_id)
                AND NOT EXISTS (SELECT 1 FROM reference_viewer_transfers t WHERE t.job_id=r.job_id)
                ORDER BY r.created_at,r.job_id LIMIT ?''', (limit,)).fetchall()
            return [row['job_id'] for row in rows]

    def claim_viewer_transfer(self, *, now, lease_seconds=120):
        """Lease one due Viewer transfer; expired leases are safe to resume."""
        if type(now) not in (int, float) or now < 0 or type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise ValueError('invalid Viewer transfer lease')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT t.job_id,t.receipt_id,t.acceptance_json,t.viewer_manifest_json,t.attempts
                FROM reference_viewer_transfers t JOIN reference_jobs r ON r.job_id=t.job_id
                WHERE t.completed_at IS NULL AND t.next_attempt<=? AND (t.lease_until IS NULL OR t.lease_until<=?)
                AND CASE WHEN json_valid(r.input_json) THEN json_type(r.input_json, '$.sourceIntakeVersion') IS NULL ELSE 0 END
                AND NOT EXISTS (SELECT 1 FROM reference_source_intakes s WHERE s.job_id=r.job_id)
                ORDER BY t.next_attempt,t.created_at,t.job_id LIMIT 1''', (now, now)).fetchone()
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

    def observe_worker_acceptance(self, job_id, request, *, identity, claim_token):
        """Record local worker receipt verification, without authorizing cleanup."""
        from .reference_acceptance_observed import observe
        return observe(self, job_id, request, identity=identity, claim_token=claim_token)

    def worker_acceptance_observation(self, job_id):
        from .reference_acceptance_observed import lookup
        return lookup(self, job_id)

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
