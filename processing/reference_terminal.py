"""Viewer acknowledgement of sealed terminal metadata, never file acceptance.

Only authenticated-worker failures reconciled with the authoritative terminal
queue result can be exposed or acknowledged. Connection loss, held jobs, operator
resolution, and unconfirmed cancellation do not manufacture terminal outcomes.
"""
import json
from datetime import datetime

from . import reference_protocol as protocol
from .reference_failure import validate_failure
from .maintenance import Conflict


FIELDS = ("schemaVersion", "receiptId", "storageReceiptId", "jobId", "generation",
          "environment", "inputManifestSha256", "pipelineDigest", "failureSha256",
          "acceptedAt", "validationVersion")


def validate_terminal_acceptance(receipt, *, failure):
    protocol._closed(receipt, FIELDS, "terminal acceptance")
    protocol._require(type(receipt["schemaVersion"]) is int and receipt["schemaVersion"] == 1,
                      "unsupported terminal acceptance schemaVersion")
    for key in ("receiptId", "storageReceiptId", "jobId"):
        protocol._slug(receipt[key], key)
    protocol._integer(receipt["generation"], "generation")
    protocol._environment(receipt["environment"])
    for key in ("inputManifestSha256", "pipelineDigest", "failureSha256"):
        protocol._hash(receipt[key], key)
    for key in ("jobId", "generation", "environment", "inputManifestSha256", "pipelineDigest", "failureSha256"):
        if receipt[key] != failure[key]:
            raise Conflict(f"terminal acceptance {key} mismatch")
    protocol._require(receipt["validationVersion"] == protocol.VALIDATION_VERSION,
                      "unsupported terminal acceptance validation version")
    protocol._string(receipt["acceptedAt"], "acceptedAt", 40)
    try:
        parsed = datetime.fromisoformat(receipt["acceptedAt"].replace("Z", "+00:00"))
        protocol._require(parsed.utcoffset() is not None, "acceptedAt requires timezone")
    except ValueError:
        raise protocol.ProtocolError("invalid acceptedAt") from None
    return json.loads(protocol.canonical_json(receipt))


class TerminalService:
    def __init__(self, store):
        self.store = store
        with store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS reference_terminal_acceptances (
                receipt_id TEXT PRIMARY KEY, storage_receipt_id TEXT NOT NULL UNIQUE,
                job_id TEXT NOT NULL, generation INTEGER NOT NULL, digest TEXT NOT NULL,
                receipt_json TEXT NOT NULL, UNIQUE(job_id,generation))""")

    def _outcome(self, db, job_id, environment):
        protocol._slug(job_id, "jobId")
        protocol._environment(environment)
        row = self.store._row(db, job_id)
        if row["environment"] != environment:
            raise Conflict("terminal outcome environment mismatch")
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reference_worker_failures'").fetchone():
            raise Conflict("confirmed terminal failure is unavailable")
        sealed = db.execute("SELECT * FROM reference_worker_failures WHERE job_id=? AND generation=?",
                            (job_id, row["generation"])).fetchone()
        if sealed is None or sealed["delivery_state"] != "awaiting_viewer_failure_acceptance":
            raise Conflict("failure metadata is not ready for terminal acceptance")
        from .reference_store import StoredReferenceCorrupt
        try:
            failure = json.loads(sealed["failure_json"])
            failure = validate_failure(failure, job_id=job_id, generation=row["generation"],
                environment=environment, input_manifest_sha256=row["manifest_sha"], pipeline_digest=row["pipeline_digest"])
        except (ValueError, TypeError, KeyError, Conflict):
            raise StoredReferenceCorrupt('stored failure requires inspection') from None
        queue_row = db.execute("SELECT status,result FROM jobs WHERE id=?", (job_id,)).fetchone()
        expected_result = dict(failureSha256=failure["failureSha256"], terminalState=failure["terminalState"], errorCode=failure["errorCode"])
        try:
            queue_result = json.loads(queue_row['result']) if queue_row and queue_row['result'] else None
        except (ValueError, TypeError):
            raise StoredReferenceCorrupt('stored failure result requires inspection') from None
        if (not queue_row or queue_row["status"] != "failed" or queue_result != expected_result or row["result_json"]
                or sealed["failure_sha"] != failure["failureSha256"]):
            raise Conflict("queue has not confirmed this exact terminal engine failure")
        return row, failure

    def outcome(self, job_id, environment):
        with self.store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            _, failure = self._outcome(db, job_id, environment)
            return failure

    def accepted(self, job_id, environment):
        """Private relay. A terminal metadata ACK grants no artifact cleanup."""
        with self.store.queue._db() as db:
            _, failure = self._outcome(db, job_id, environment)
            row = db.execute('SELECT * FROM reference_terminal_acceptances WHERE job_id=? AND generation=?',
                             (job_id, failure['generation'])).fetchone()
            if row is None:
                return {'receipt': None, 'terminalAccepted': False}
            from .reference_store import StoredReferenceCorrupt
            try:
                receipt = validate_terminal_acceptance(json.loads(row['receipt_json']), failure=failure)
                if (receipt['receiptId'] != row['receipt_id'] or receipt['storageReceiptId'] != row['storage_receipt_id']
                        or receipt['jobId'] != row['job_id'] or receipt['generation'] != row['generation']
                        or protocol.canonical_json(receipt).decode() != row['receipt_json']
                        or protocol.digest(receipt) != row['digest']):
                    raise ValueError('stored terminal acceptance changed')
            except (ValueError, TypeError, KeyError, Conflict):
                raise StoredReferenceCorrupt('stored terminal acceptance requires inspection') from None
            return {'receipt': receipt, 'terminalAccepted': True}

    def _check_ids(self, db, checked, *, replay_id=None):
        from .reference_store import validated_collision_identities
        ids = {checked["receiptId"], checked["storageReceiptId"]}
        for kind, receipt_id, storage_id in validated_collision_identities(db):
            if kind == 'terminal' and receipt_id == replay_id:
                continue
            if ids & {receipt_id, storage_id}:
                raise Conflict("receipt identity already bound")

    def accept(self, job_id, receipt, environment):
        with self.store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            _, failure = self._outcome(db, job_id, environment)
            checked = validate_terminal_acceptance(receipt, failure=failure)
            encoded = protocol.canonical_json(checked).decode()
            receipt_digest = protocol.digest(checked)
            old = db.execute("SELECT * FROM reference_terminal_acceptances WHERE job_id=? AND generation=?",
                             (job_id, failure["generation"])).fetchone()
            self._check_ids(db, checked, replay_id=old['receipt_id'] if old else None)
            if old:
                if (old["receipt_id"], old["storage_receipt_id"], old["digest"], old["receipt_json"]) != (
                        checked["receiptId"], checked["storageReceiptId"], receipt_digest, encoded):
                    raise Conflict("terminal acceptance already sealed with different receipt")
            if not old:
                self.store._quota(db, len(encoded.encode()))
                db.execute("INSERT INTO reference_terminal_acceptances VALUES(?,?,?,?,?,?)",
                    (checked["receiptId"], checked["storageReceiptId"], job_id,
                     failure["generation"], receipt_digest, encoded))
            # Receipt and phase commit in one FULL queue transaction. A lost reply
            # replays the exact receipt; neither path acknowledges output bytes.
            db.execute("UPDATE reference_jobs SET phase='completed',updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (job_id,))
            return dict(job=self.store._public(self.store._row(db, job_id)),
                        created=old is None, terminalAccepted=True)
