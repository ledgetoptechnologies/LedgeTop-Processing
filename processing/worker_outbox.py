"""Durable local terminal-result delivery. No network, file removal, or cleanup ACK.

Persist before sending. Send only a claimed immutable payload; a transport success
is not a durable receipt. Even acknowledged rows and manifests remain retained.
"""
from contextlib import contextmanager, ExitStack
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import random
import re
import secrets
import sqlite3
import time
import unicodedata

from processing.reference_protocol import canonical_json, validate_result_manifest


MAX_INTEGER = 2**53 - 1
BINDING = ("jobId", "generation", "environment", "inputManifestSha256",
           "pipelineDigest", "resultManifestSha256")


class OutboxConflict(ValueError):
    """Immutable identity, stale claim, or invalid durable receipt."""


def _json(value):
    return canonical_json(value).decode("utf-8")


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
        raise ValueError("invalid identity")
    return value


def _int(value, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_INTEGER:
        raise ValueError("invalid bounded integer")
    return value


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("invalid SHA256")
    return value


def _path(value):
    if not isinstance(value, str):
        raise ValueError("invalid owned path")
    parts = PurePosixPath(value).parts
    if (value != "/".join(parts) or not parts or parts[0] != "jobs"
            or len(parts) < 2 or any(p in (".", "..") for p in parts)
            or any(c in value for c in '\\:<>"|?*')):
        raise ValueError("path must be canonical scratch-relative jobs path")
    for part in parts:
        if (part.endswith((".", " ")) or any(ord(c) < 32 for c in part)
                or re.fullmatch(r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?", part, re.I)):
            raise ValueError("unsafe owned path")
    return value


class WorkerOutbox:
    def __init__(self, path, *, clock=None, random_source=None,
                 retry_base_seconds=5, retry_max_seconds=300, auxiliary_budget=None):
        self.path = Path(path)
        from .reference_auxiliary_budget import reject_unbudgeted
        reject_unbudgeted(self.path,auxiliary_budget)
        self.auxiliary_budget=auxiliary_budget
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.clock = clock or time.time
        self.random = random_source or random.random
        self.base = _int(retry_base_seconds, 1)
        self.maximum = _int(retry_max_seconds, self.base)
        with self._transaction(bootstrap=True) as db:
            db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS completions (
                id TEXT PRIMARY KEY, job TEXT NOT NULL, generation INTEGER NOT NULL,
                environment TEXT NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL,
                state TEXT NOT NULL, attempts INTEGER NOT NULL, next_at REAL NOT NULL,
                claim_token TEXT, lease_until REAL, receipt TEXT, local_metadata TEXT NOT NULL,
                UNIQUE(job,generation))""")
            db.execute("CREATE TABLE IF NOT EXISTS job_environments (job TEXT PRIMARY KEY,environment TEXT NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS durable_receipts (id TEXT PRIMARY KEY,storage_id TEXT NOT NULL UNIQUE,outbox TEXT NOT NULL UNIQUE,payload TEXT NOT NULL)")
            config = _json(dict(base=self.base, maximum=self.maximum))
            old = db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
            if old and old[0] != config:
                raise OutboxConflict("outbox retry configuration changed")
            db.execute("INSERT OR IGNORE INTO metadata VALUES('config',?)", (config,))
            self._observe(db)
        if os.name != 'nt':
            os.chmod(self.path, 0o600)

    @contextmanager
    def _transaction(self, *, bootstrap=False):
        from .reference_auxiliary_budget import writer_scope
        scope=ExitStack()
        configure=scope.enter_context(writer_scope(self.path,self.auxiliary_budget))
        try:
            db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            scope.callback(db.close)
            configure(db)
        except BaseException:
            scope.close();raise
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            if not bootstrap:
                self._observe(db)
                db.commit()
                db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            scope.close()

    def _observe(self, db):
        value = self.clock()
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("invalid clock")
        old = db.execute("SELECT value FROM metadata WHERE key='clock'").fetchone()
        value = max(value, float(old[0]) if old else 0)
        db.execute("INSERT INTO metadata VALUES('clock',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(value),))

    def _now(self, db):
        return float(db.execute("SELECT value FROM metadata WHERE key='clock'").fetchone()[0])

    def persist_terminal(self, *, job_id, generation, environment,
                         input_manifest_sha256, pipeline_digest,
                         result_manifest_sha256, result_manifest,
                         owned_prefix, local_objects, terminal_state="succeeded"):
        """Persist the exact reference-v1 result manifest and separate local paths.

        Caller verifies actual files and supplies its reserved owned_prefix. This
        module reuses protocol validation, never opens outputs. local_objects is
        {objectId: relativePath}, persisted locally and excluded from wire payload.
        """
        _id(job_id); _int(generation, 1)
        for digest in (input_manifest_sha256, pipeline_digest, result_manifest_sha256):
            _sha(digest)
        if environment not in ("production", "staging") or terminal_state not in ("succeeded", "failed", "cancelled"):
            raise ValueError("invalid environment or terminal state")
        _path(owned_prefix)
        manifest = validate_result_manifest(result_manifest, job_id=job_id,
            generation=generation, environment=environment, pipeline_digest=pipeline_digest)
        if manifest["resultManifestSha256"] != result_manifest_sha256:
            raise OutboxConflict("result manifest digest mismatch")
        if not isinstance(local_objects, dict) or set(local_objects) != {o["id"] for o in manifest["objects"]}:
            raise ValueError("local paths must bind every exact result object")
        paths = set()
        for object_id, relative_path in local_objects.items():
            _id(object_id); _path(relative_path)
            path_key = unicodedata.normalize("NFC", relative_path).casefold()
            if not relative_path.startswith(owned_prefix + "/") or path_key in paths:
                raise ValueError("duplicate or unowned result object")
            paths.add(path_key)
        local_metadata = _json(dict(ownedPrefix=owned_prefix, localObjects=local_objects))
        binding = dict(jobId=job_id, generation=generation, environment=environment,
            inputManifestSha256=input_manifest_sha256, pipelineDigest=pipeline_digest,
            resultManifestSha256=result_manifest_sha256)
        outbox_id = hashlib.sha256(_json(binding).encode()).hexdigest()
        payload = _json(binding | dict(resultManifest=manifest, terminalState=terminal_state))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        with self._transaction() as db:
            bound = db.execute("SELECT environment FROM job_environments WHERE job=?", (job_id,)).fetchone()
            if bound and bound[0] != environment:
                raise OutboxConflict("job environment changed")
            old = db.execute("SELECT * FROM completions WHERE job=? AND generation=?", (job_id, generation)).fetchone()
            if old:
                if old["payload"] != payload or old["local_metadata"] != local_metadata:
                    raise OutboxConflict("terminal completion changed")
                return old["id"]
            db.execute("INSERT OR IGNORE INTO job_environments VALUES(?,?)", (job_id, environment))
            db.execute("INSERT INTO completions VALUES(?,?,?,?,?,?,'pending',0,?,NULL,NULL,NULL,?)",
                       (outbox_id, job_id, generation, environment, payload, digest, self._now(db), local_metadata))
        return outbox_id

    def get(self, outbox_id):
        with self._transaction() as db:
            row = db.execute("SELECT * FROM completions WHERE id=?", (outbox_id,)).fetchone()
            if row is None:
                raise OutboxConflict("unknown completion")
            return dict(row) | dict(payload=json.loads(row["payload"]),
                local_metadata=json.loads(row["local_metadata"]), receipt=json.loads(row["receipt"]) if row["receipt"] else None)

    def claim_next(self, *, lease_seconds=60):
        """Only the holder may report failure. Expired claims are safely resendable."""
        _int(lease_seconds, 1)
        with self._transaction() as db:
            now = self._now(db)
            row = db.execute("SELECT * FROM completions WHERE (state='pending' AND next_at<=?) "
                "OR (state='sending' AND lease_until<=?) ORDER BY next_at,id LIMIT 1", (now, now)).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(32)
            _int(row["attempts"] + 1, 1)
            db.execute("UPDATE completions SET state='sending',attempts=attempts+1,claim_token=?,lease_until=? WHERE id=?",
                       (token, now + lease_seconds, row["id"]))
            return dict(outboxId=row["id"], payload=json.loads(row["payload"]),
                payloadSha256=row["digest"], claimToken=token, leaseExpires=now + lease_seconds,
                attempt=row["attempts"] + 1)

    def failed_attempt(self, outbox_id, claim_token):
        """Keep the immutable payload; schedule bounded exponential equal jitter."""
        with self._transaction() as db:
            row = db.execute("SELECT * FROM completions WHERE id=?", (outbox_id,)).fetchone()
            now = self._now(db)
            if (row is None or row["state"] != "sending" or row["claim_token"] != claim_token
                    or row["lease_until"] <= now):
                raise OutboxConflict("stale delivery claim")
            jitter = self.random()
            if type(jitter) not in (int, float) or not 0 <= jitter <= 1:
                raise ValueError("invalid jitter")
            ceiling = min(self.maximum, self.base * 2**min(row["attempts"]-1, 53))
            due = now + ceiling * (0.5 + jitter * 0.5)
            db.execute("UPDATE completions SET state='pending',next_at=?,claim_token=NULL,lease_until=NULL WHERE id=?", (due, outbox_id))
            return due

    def acknowledge(self, outbox_id, receipt):
        """Receipt is verified protocol metadata supplied by a trusted adapter.

        This is a coordinator persistence receipt, distinct from Viewer acceptance.
        Requires explicit durable receipt + entire immutable binding and accepted
        object set; HTTP status alone is insufficient. This grants no file cleanup.
        A valid late receipt can finish delivery despite an expired attempt lease.
        """
        required = {"type", "durable", "receiptId", "storageReceiptId", "outboxId", "payloadSha256", "acceptedObjectIds", *BINDING}
        if not isinstance(receipt, dict) or set(receipt) != required:
            raise OutboxConflict("explicit durable receipt required")
        if receipt["type"] != "durable-result-receipt" or receipt["durable"] is not True:
            raise OutboxConflict("receipt is not durable")
        _id(receipt["receiptId"]); _id(receipt["storageReceiptId"])
        _int(receipt["generation"], 1)
        if not isinstance(receipt["acceptedObjectIds"], list) or any(not isinstance(v, str) for v in receipt["acceptedObjectIds"]):
            raise OutboxConflict("invalid accepted object set")
        canonical = _json(receipt)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM completions WHERE id=?", (outbox_id,)).fetchone()
            if row is None:
                raise OutboxConflict("unknown completion")
            payload = json.loads(row["payload"])
            expected_ids = sorted(o["id"] for o in payload["resultManifest"]["objects"])
            if (receipt["outboxId"] != outbox_id or receipt["payloadSha256"] != row["digest"]
                    or any(receipt[key] != payload[key] for key in BINDING)
                    or sorted(receipt["acceptedObjectIds"]) != expected_ids):
                raise OutboxConflict("receipt binding mismatch")
            if row["receipt"] and row["receipt"] != canonical:
                raise OutboxConflict("durable receipt changed")
            if row["attempts"] == 0:
                raise OutboxConflict("completion has never been sent")
            if not row["receipt"]:
                try:
                    db.execute("INSERT INTO durable_receipts VALUES(?,?,?,?)",
                               (receipt["receiptId"], receipt["storageReceiptId"], outbox_id, canonical))
                except sqlite3.IntegrityError as exc:
                    raise OutboxConflict("durable receipt already bound") from exc
            db.execute("UPDATE completions SET state='acknowledged',receipt=?,claim_token=NULL,lease_until=NULL WHERE id=?", (canonical, outbox_id))
