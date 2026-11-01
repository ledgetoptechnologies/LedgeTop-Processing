"""Private terminal-failure metadata protocol, durable outbox, and receipt barrier.

Failures never invent output archives. Coordinator persistence receipts prove
metadata registration and engine-slot release only; Viewer failure acceptance and
scratch cleanup require a separately agreed protocol and are not implemented here.
Transport must authenticate worker identity before invoking FailureReceiver.
"""
import hmac
import hashlib
import json
import re
import sqlite3

from . import reference_protocol as protocol
from .maintenance import Conflict
from .worker_outbox import WorkerOutbox, OutboxConflict


FAILURE_FIELDS = ("schemaVersion", "jobId", "generation", "environment",
                  "inputManifestSha256", "pipelineDigest", "terminalState",
                  "errorCode", "failureSha256")
BINDING = ("jobId", "generation", "environment", "inputManifestSha256",
           "pipelineDigest", "terminalState", "failureSha256")


def failure_sha256(record):
    return protocol.digest({key: value for key, value in record.items() if key != "failureSha256"})


def validate_failure(record, *, job_id, generation, environment,
                     input_manifest_sha256, pipeline_digest):
    protocol._slug(job_id, "expected jobId")
    protocol._integer(generation, "expected generation")
    protocol._environment(environment)
    protocol._hash(input_manifest_sha256, "expected inputManifestSha256")
    protocol._hash(pipeline_digest, "expected pipelineDigest")
    protocol._closed(record, FAILURE_FIELDS, "terminal failure")
    protocol._require(type(record["schemaVersion"]) is int and record["schemaVersion"] == 1,
                      "unsupported failure schemaVersion")
    protocol._slug(record["jobId"], "jobId")
    protocol._integer(record["generation"], "generation")
    protocol._environment(record["environment"])
    for key in ("inputManifestSha256", "pipelineDigest", "failureSha256"):
        protocol._hash(record[key], key)
    protocol._require(record["terminalState"] in ("failed", "cancelled"), "invalid terminal failure state")
    protocol._require(type(record["errorCode"]) is str and
        re.fullmatch(r"[a-z][a-z0-9_]{0,79}", record["errorCode"]), "invalid bounded failure code")
    protocol._require((record["jobId"], record["generation"], record["environment"],
        record["inputManifestSha256"], record["pipelineDigest"]) ==
        (job_id, generation, environment, input_manifest_sha256, pipeline_digest),
        "failure binding mismatch")
    protocol._require(record["failureSha256"] == failure_sha256(record), "failure digest mismatch")
    return json.loads(protocol.canonical_json(record))


def make_failure(*, job_id, generation, environment, input_manifest_sha256,
                 pipeline_digest, terminal_state, error_code):
    record = dict(schemaVersion=1, jobId=job_id, generation=generation,
        environment=environment, inputManifestSha256=input_manifest_sha256,
        pipelineDigest=pipeline_digest, terminalState=terminal_state, errorCode=error_code)
    record["failureSha256"] = failure_sha256(record)
    return validate_failure(record, job_id=job_id, generation=generation,
        environment=environment, input_manifest_sha256=input_manifest_sha256,
        pipeline_digest=pipeline_digest)


def _checked(record):
    protocol._closed(record, FAILURE_FIELDS, "terminal failure")
    return validate_failure(record, job_id=record["jobId"], generation=record["generation"],
        environment=record["environment"], input_manifest_sha256=record["inputManifestSha256"],
        pipeline_digest=record["pipelineDigest"])


def _envelope(envelope):
    protocol._closed(envelope, ("outboxId", "payloadSha256", "payload"), "failure delivery")
    for key in ("outboxId", "payloadSha256"):
        protocol._hash(envelope[key], key)
    payload = envelope["payload"]
    protocol._closed(payload, ("failure", "engineTerminal"), "failure payload")
    protocol._require(payload["engineTerminal"] is True, "authenticated terminal engine attestation required")
    record = _checked(payload["failure"])
    if (protocol.digest(payload) != envelope["payloadSha256"] or
            protocol.digest({key: record[key] for key in BINDING}) != envelope["outboxId"]):
        raise Conflict("failure delivery identity mismatch")
    return record


def _receipt(envelope, record):
    return dict(type="durable-failure-receipt", durable=True,
        receiptId="failure-" + record["failureSha256"], outboxId=envelope["outboxId"],
        payloadSha256=envelope["payloadSha256"], **{key: record[key] for key in BINDING})


class FailureOutbox(WorkerOutbox):
    """Reuse FULL SQLite/clock/claim/backoff machinery; retain failure metadata.

    No success manifest schema is reused. The payload contains a failure record
    and explicit engineTerminal attestation, never paths, credentials, or logs.
    """
    def persist_terminal(self, **kwargs):
        raise ValueError("failure outbox requires persist(failure, engine_terminal=True)")

    def persist(self, failure, *, engine_terminal):
        record = _checked(failure)
        protocol._require(engine_terminal is True, "engine must be confirmed terminal before persistence")
        binding = {key: record[key] for key in BINDING}
        outbox_id = protocol.digest(binding)
        payload = protocol.canonical_json(dict(failure=record, engineTerminal=True)).decode()
        digest = protocol.digest(dict(failure=record, engineTerminal=True))
        with self._transaction() as db:
            bound = db.execute("SELECT environment FROM job_environments WHERE job=?", (record["jobId"],)).fetchone()
            if bound and bound[0] != record["environment"]:
                raise OutboxConflict("failure job environment changed")
            old = db.execute("SELECT * FROM completions WHERE job=? AND generation=?",
                             (record["jobId"], record["generation"])).fetchone()
            if old:
                if old["id"] != outbox_id or old["payload"] != payload:
                    raise OutboxConflict("sealed terminal failure changed")
                return outbox_id
            db.execute("INSERT OR IGNORE INTO job_environments VALUES(?,?)", (record["jobId"], record["environment"]))
            db.execute("INSERT INTO completions VALUES(?,?,?,?,?,?,'pending',0,?,NULL,NULL,NULL,?)",
                (outbox_id, record["jobId"], record["generation"], record["environment"],
                 payload, digest, self._now(db), '{}'))
        return outbox_id

    def acknowledge(self, outbox_id, receipt):
        with self._transaction() as db:
            row = db.execute("SELECT * FROM completions WHERE id=?", (outbox_id,)).fetchone()
            if row is None:
                raise OutboxConflict("unknown failure delivery")
            envelope = dict(outboxId=outbox_id, payloadSha256=row["digest"], payload=json.loads(row["payload"]))
            record = _envelope(envelope)
            expected = _receipt(envelope, record)
            if type(receipt) is not dict or set(receipt) != set(expected) or receipt != expected:
                raise OutboxConflict("exact durable coordinator failure receipt required")
            if type(receipt["durable"]) is not bool or type(receipt["generation"]) is not int:
                raise OutboxConflict("failure receipt has invalid scalar types")
            encoded = protocol.canonical_json(receipt).decode()
            if row["attempts"] == 0 or (row["receipt"] and row["receipt"] != encoded):
                raise OutboxConflict("unsent or changed failure receipt")
            if not row["receipt"]:
                try:
                    db.execute("INSERT INTO durable_receipts VALUES(?,?,?,?)",
                        (receipt["receiptId"], receipt["receiptId"], outbox_id, encoded))
                except sqlite3.IntegrityError as exc:
                    raise OutboxConflict("failure receipt identity already bound") from exc
            db.execute("UPDATE completions SET state='acknowledged',receipt=?,claim_token=NULL,lease_until=NULL WHERE id=?", (encoded, outbox_id))


class FailureReceiver:
    def __init__(self, store):
        self.store = store
        with store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS reference_worker_failures (
                job_id TEXT NOT NULL, generation INTEGER NOT NULL,
                failure_sha TEXT NOT NULL UNIQUE, payload_sha TEXT NOT NULL,
                failure_json TEXT NOT NULL, receipt_json TEXT NOT NULL,
                delivery_state TEXT NOT NULL, PRIMARY KEY(job_id,generation))""")

    def _authenticate(self, db, record, *, node_id, worker_id, worker_instance_id,
                      claim_request_id, claim_token):
        row = self.store._row(db, record["jobId"])
        claim = db.execute("SELECT * FROM reference_claims WHERE request_id=?", (claim_request_id,)).fetchone()
        if (not claim or (claim["job_id"], claim["node_id"], claim["worker_id"], claim["instance_id"]) !=
                (record["jobId"], node_id, worker_id, worker_instance_id)
                or type(claim_token) is not str or not hmac.compare_digest(claim["token"], claim_token)
                or not row["claim_hash"] or not hmac.compare_digest(
                    hashlib.sha256(claim_token.encode()).hexdigest(), row["claim_hash"])):
            raise Conflict("failure does not match persisted worker claim")
        if row["status"] == "held" and row["held_reason"] != "coordinator_restart":
            raise Conflict("operator-held failure requires explicit resolution")
        validate_failure(record, job_id=row["job_id"], generation=row["generation"],
            environment=row["environment"], input_manifest_sha256=row["manifest_sha"],
            pipeline_digest=row["pipeline_digest"])
        return row

    def receive(self, envelope, *, node_id, worker_id, worker_instance_id,
                claim_request_id, claim_token):
        record = _envelope(envelope)
        identity = dict(node_id=node_id, worker_id=worker_id, worker_instance_id=worker_instance_id,
                        claim_request_id=claim_request_id, claim_token=claim_token)
        receipt = _receipt(envelope, record)
        encoded = protocol.canonical_json(record).decode()
        receipt_json = protocol.canonical_json(receipt).decode()
        with self.store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._authenticate(db, record, **identity)
            old = db.execute("SELECT * FROM reference_worker_failures WHERE job_id=? AND generation=?",
                             (record["jobId"], record["generation"])).fetchone()
            if row["result_json"]:
                raise Conflict("success result already sealed")
            if old:
                if (old["failure_json"], old["payload_sha"], old["receipt_json"]) != (encoded, envelope["payloadSha256"], receipt_json):
                    raise Conflict("terminal failure already sealed")
            else:
                recovering = row["status"] == "held" and row["held_reason"] == "coordinator_restart"
                if row["status"] != "running" and not recovering:
                    raise Conflict("new failure requires running or authenticated restart claim")
                self.store._quota(db, len(encoded.encode()) + len(receipt_json.encode()))
                db.execute("INSERT INTO reference_worker_failures VALUES(?,?,?,?,?,?,'pending_queue_completion')",
                    (record["jobId"], record["generation"], record["failureSha256"], envelope["payloadSha256"], encoded, receipt_json))
        # Failure metadata is FULL-committed first. Queue completion releases the
        # execution slot independently of later Viewer acknowledgement. Queue only
        # supports failed, so a cancelled engine remains explicit in sealed result.
        self.store.queue.complete(record["jobId"], dict(claimToken=claim_token, status="failed",
            result=dict(failureSha256=record["failureSha256"], terminalState=record["terminalState"], errorCode=record["errorCode"])))
        with self.store.queue._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._authenticate(db, record, **identity)
            db.execute("UPDATE reference_worker_failures SET delivery_state='awaiting_viewer_failure_acceptance' WHERE job_id=? AND generation=?",
                       (record["jobId"], record["generation"]))
        return receipt

    def get(self, job_id, generation):
        """Internal metadata accessor; no public route or Viewer ACK is installed."""
        with self.store.queue._db() as db:
            row = db.execute("SELECT * FROM reference_worker_failures WHERE job_id=? AND generation=?", (job_id, generation)).fetchone()
            if row is None:
                raise Conflict("unknown terminal failure")
            return dict(failure=json.loads(row["failure_json"]), deliveryState=row["delivery_state"])
