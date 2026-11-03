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
        failure = json.loads(sealed["failure_json"])
        failure = validate_failure(failure, job_id=job_id, generation=row["generation"],
            environment=environment, input_manifest_sha256=row["manifest_sha"], pipeline_digest=row["pipeline_digest"])
        queue_row = db.execute("SELECT status,result FROM jobs WHERE id=?", (job_id,)).fetchone()
        expected_result = dict(failureSha256=failure["failureSha256"], terminalState=failure["terminalState"], errorCode=failure["errorCode"])
        if (queue_row["status"] != "failed" or not queue_row["result"]
                or json.loads(queue_row["result"]) != expected_result or row["result_json"]
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
            row = db.execute('SELECT receipt_json FROM reference_terminal_acceptances WHERE job_id=? AND generation=?',
                             (job_id, failure['generation'])).fetchone()
            if row is None:
                return {'receipt': None, 'terminalAccepted': False}
            receipt = validate_terminal_acceptance(json.loads(row[0]), failure=failure)
            return {'receipt': receipt, 'terminalAccepted': True}

    def _check_ids(self, db, checked):
        ids = {checked["receiptId"], checked["storageReceiptId"]}
        if ids & self.store._held_copy_ids(db): raise Conflict("receipt identity held by v3 proof")
        for row in db.execute("SELECT receipt_id,receipt_json FROM reference_acceptances"):
            old = json.loads(row["receipt_json"])
            if ids & {row["receipt_id"], old["receiptId"], old["storageReceiptId"]}:
                raise Conflict("receipt identity already belongs to success acceptance")
        for row in db.execute("SELECT receipt_id,storage_receipt_id FROM reference_terminal_acceptances"):
            if ids & {row["receipt_id"], row["storage_receipt_id"]}:
                raise Conflict("terminal receipt identity already bound")

    def accept(self, job_id, receipt, environment):
        with self.store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            _, failure = self._outcome(db, job_id, environment)
            checked = validate_terminal_acceptance(receipt, failure=failure)
            encoded = protocol.canonical_json(checked).decode()
            receipt_digest = protocol.digest(checked)
            old = db.execute("SELECT * FROM reference_terminal_acceptances WHERE job_id=? AND generation=?",
                             (job_id, failure["generation"])).fetchone()
            if old:
                if (old["receipt_id"], old["storage_receipt_id"], old["digest"], old["receipt_json"]) != (
                        checked["receiptId"], checked["storageReceiptId"], receipt_digest, encoded):
                    raise Conflict("terminal acceptance already sealed with different receipt")
            else:
                self._check_ids(db, checked)
                self.store._quota(db, len(encoded.encode()))
                db.execute("INSERT INTO reference_terminal_acceptances VALUES(?,?,?,?,?,?)",
                    (checked["receiptId"], checked["storageReceiptId"], job_id,
                     failure["generation"], receipt_digest, encoded))
            # Receipt and phase commit in one FULL queue transaction. A lost reply
            # replays the exact receipt; neither path acknowledges output bytes.
            db.execute("UPDATE reference_jobs SET phase='completed',updated_at=CURRENT_TIMESTAMP WHERE job_id=?", (job_id,))
            return dict(job=self.store._public(self.store._row(db, job_id)),
                        created=old is None, terminalAccepted=True)
