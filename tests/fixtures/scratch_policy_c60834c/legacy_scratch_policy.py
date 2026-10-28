"""Internal durable scratch policy. No transfers, engine calls, or file deletion.

Capacity probes return *current free bytes*, already excluding allocated bytes.
Only remainingBytes is subtracted from that snapshot. Call record_allocated only
after writes consume disk; it moves bytes from remaining to allocated. Production
and staging are immutable reservation settings, not interchangeable namespaces.
Confirmed terminal attestation releases only unused reservation budget; allocated
files still occupy the probe's free-space snapshot until an executor removes them.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import time
import secrets
import unicodedata


MAX_INTEGER = 2**53 - 1


class PolicyConflict(ValueError):
    """Identity or state conflict (HTTP adapters may map to 409)."""


def _integer(value, name, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_INTEGER:
        raise ValueError(f"invalid {name}")
    return value


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
        raise ValueError("invalid identity")
    return value


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("invalid SHA256")
    return value


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class ScratchLedger:
    def __init__(self, path, storage_root, capacity_probe=None, clock=None,
                 safety_floor_bytes=1024**3, cleanup_grace_seconds=3600):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        original_root = Path(storage_root).absolute()
        if original_root.is_symlink() or any(p.is_symlink() for p in original_root.parents):
            raise ValueError("storage root must not contain symlinks")
        self.root = original_root.resolve()
        self.probe = capacity_probe or (lambda root: shutil.disk_usage(root).free)
        self.clock = clock or time.time
        self.floor = _integer(safety_floor_bytes, "safety floor")
        self.grace = _integer(cleanup_grace_seconds, "cleanup grace")
        with self._transaction(bootstrap=True) as db:
            schema = """
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS job_environments (job TEXT PRIMARY KEY, environment TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reservations (
                  id TEXT PRIMARY KEY, settings TEXT NOT NULL, job TEXT NOT NULL,
                  generation INTEGER NOT NULL, environment TEXT NOT NULL,
                  expires REAL NOT NULL, state TEXT NOT NULL,
                  remaining INTEGER NOT NULL, allocated INTEGER NOT NULL,
                  terminal_at REAL);
                DROP INDEX IF EXISTS one_generation;
                CREATE UNIQUE INDEX IF NOT EXISTS active_generation
                  ON reservations(job,generation) WHERE state!='expired';
                CREATE TABLE IF NOT EXISTS results (
                  reservation TEXT PRIMARY KEY, identity TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS objects (
                  reservation TEXT NOT NULL, id TEXT NOT NULL, bytes INTEGER NOT NULL,
                  sha TEXT NOT NULL, path TEXT NOT NULL UNIQUE, accepted REAL,
                  cleaned REAL, PRIMARY KEY(reservation,id));
                CREATE TABLE IF NOT EXISTS receipts (
                  id TEXT PRIMARY KEY, storage_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS pins (
                  reservation TEXT NOT NULL, object TEXT NOT NULL, reference TEXT NOT NULL,
                  kind TEXT NOT NULL, state TEXT NOT NULL,
                  PRIMARY KEY(reservation,object,reference));
                CREATE TABLE IF NOT EXISTS cleanup_intents (
                  reservation TEXT NOT NULL, object TEXT NOT NULL, token TEXT NOT NULL UNIQUE,
                  expires REAL NOT NULL, state TEXT NOT NULL,
                  PRIMARY KEY(reservation,object));
            """
            for statement in schema.split(";"):
                if statement.strip():
                    db.execute(statement)
            if db.execute("SELECT job FROM reservations GROUP BY job HAVING COUNT(DISTINCT environment)>1").fetchone():
                raise PolicyConflict("existing job has conflicting environments")
            db.execute("INSERT OR IGNORE INTO job_environments SELECT DISTINCT job,environment FROM reservations")
            config = _json(dict(root=str(self.root), floor=self.floor, grace=self.grace))
            previous = db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
            if previous and previous[0] != config:
                raise PolicyConflict("ledger root or policy settings changed")
            db.execute("INSERT OR IGNORE INTO metadata VALUES('config',?)", (config,))
            self._observe_clock(db)
        if os.name != "nt":
            os.chmod(self.path, 0o600)

    @contextmanager
    def _transaction(self, *, bootstrap=False):
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("BEGIN IMMEDIATE")
            if not bootstrap:
                # Persist observations before business validation can roll back.
                # Both phases hold SQLite's writer lock; a newer observation
                # between phases is picked up by _now in the business phase.
                self._expire(db, self._observe_clock(db))
                db.commit()
                db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _reservation(self, db, reservation_id):
        row = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
        if row is None:
            raise PolicyConflict("unknown reservation")
        return row

    def _observe_clock(self, db):
        """Persist a high water mark: wall-clock rollback cannot reopen expired leases."""
        value = self.clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("invalid clock")
        previous = db.execute("SELECT value FROM metadata WHERE key='clock'").fetchone()
        now = max(value, float(previous[0]) if previous else 0)
        db.execute("INSERT INTO metadata VALUES('clock',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(now),))
        return now

    def _now(self, db):
        # No fresh wall-clock observations in a transaction that may roll back.
        return float(db.execute("SELECT value FROM metadata WHERE key='clock'").fetchone()[0])

    def _expire(self, db, now):
        # Neither engine-started nor uncertain reservations may expire automatically.
        db.execute("UPDATE reservations SET state='expired', remaining=0 "
                   "WHERE state='reserved' AND expires<=? AND allocated=0", (now,))

    def reserve(self, *, job_id, generation, reservation_id, manifest_sha256,
                input_bytes, output_budget, environment, ttl_seconds, worker_ready):
        _id(job_id); _id(reservation_id); _sha(manifest_sha256)
        _integer(generation, "generation", 1); _integer(input_bytes, "input bytes")
        _integer(output_budget, "output budget"); _integer(ttl_seconds, "TTL", 1)
        total = _integer(input_bytes + output_budget, "total")
        if environment not in ("production", "staging"):
            raise ValueError("invalid environment")
        if worker_ready is not True:
            raise PolicyConflict("worker is not ready")
        settings = _json(dict(job_id=job_id, generation=generation,
            reservation_id=reservation_id, manifest_sha256=manifest_sha256,
            input_bytes=input_bytes, output_budget=output_budget,
            environment=environment, ttl_seconds=ttl_seconds))
        with self._transaction() as db:
            now = self._now(db)
            self._expire(db, now)
            binding = db.execute("SELECT environment FROM job_environments WHERE job=?", (job_id,)).fetchone()
            if binding and binding[0] != environment:
                raise PolicyConflict("job environment is immutable")
            old = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if old:
                if old["settings"] != settings or old["state"] == "expired":
                    raise PolicyConflict("reservation settings changed or lease expired")
                return dict(old)
            outstanding = db.execute("SELECT COALESCE(SUM(remaining),0) FROM reservations").fetchone()[0]
            free = _integer(self.probe(self.root), "free bytes")
            if free - outstanding - self.floor < total:
                raise PolicyConflict("insufficient unreserved scratch")
            try:
                db.execute("INSERT OR IGNORE INTO job_environments VALUES(?,?)", (job_id, environment))
                db.execute("INSERT INTO reservations VALUES(?,?,?,?,?,?,'reserved',?,0,NULL)",
                           (reservation_id, settings, job_id, generation, environment,
                            now + ttl_seconds, total))
            except sqlite3.IntegrityError as exc:
                raise PolicyConflict("job generation already reserved") from exc
            return dict(self._reservation(db, reservation_id))

    def record_allocated(self, reservation_id, allocated_bytes):
        """Set monotonic total allocation after disk writes; never a delta."""
        _integer(allocated_bytes, "allocated bytes")
        with self._transaction() as db:
            self._expire(db, self._now(db))
            row = self._reservation(db, reservation_id)
            delta = allocated_bytes - row["allocated"]
            if row["state"] not in ("reserved", "engine_started", "uncertain") or not 0 <= delta <= row["remaining"]:
                raise PolicyConflict("allocation outside reservation")
            db.execute("UPDATE reservations SET allocated=?,remaining=remaining-? WHERE id=?",
                       (allocated_bytes, delta, reservation_id))

    def set_engine_state(self, reservation_id, state, *, terminal_attested=False):
        """terminal_attested guarantees engine shutdown and no future allocation."""
        if state not in ("engine_started", "uncertain", "terminal"):
            raise ValueError("invalid engine state")
        if state == "terminal" and terminal_attested is not True:
            raise PolicyConflict("terminal attestation must guarantee no future allocation")
        with self._transaction() as db:
            now = self._now(db)
            self._expire(db, now)
            row = self._reservation(db, reservation_id)
            if row["state"] == "reserved" and row["expires"] <= now:
                raise PolicyConflict("lease expired before engine mutation")
            if row["state"] == "expired" or (row["state"] == "terminal" and state != "terminal"):
                raise PolicyConflict("invalid engine transition")
            db.execute("UPDATE reservations SET state=?,terminal_at=CASE WHEN ?='terminal' "
                       "THEN COALESCE(terminal_at,?) ELSE terminal_at END, "
                       "remaining=CASE WHEN ?='terminal' THEN 0 ELSE remaining END WHERE id=?",
                       (state, state, now, state, reservation_id))

    def _owned_path(self, reservation_id, relative_path):
        if not isinstance(relative_path, str) or "\\" in relative_path or ":" in relative_path:
            raise ValueError("invalid owned path")
        parts = PurePosixPath(relative_path).parts
        if (relative_path.startswith("/") or relative_path != "/".join(parts)
                or len(parts) < 3 or parts[:2] != ("jobs", reservation_id)
                or any(p in (".", "..") for p in parts)):
            raise ValueError("path must be canonical inside jobs/reservationId")
        for part in parts:
            if (part.endswith((".", " ")) or any(ord(c) < 32 or c in '<>"|?*' for c in part)
                    or re.fullmatch(r"(?:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?", part, re.IGNORECASE)):
                raise ValueError("unsafe Windows path component")
        candidate = self.root.joinpath(*parts)
        if any(p.is_symlink() for p in (candidate, *candidate.parents)):
            raise ValueError("symlink in owned path")
        if not candidate.resolve().is_relative_to(self.root):
            raise ValueError("path escapes scratch")
        return relative_path

    def _check_path_aliases(self, db, normalized):
        # Enforce portable Windows case/Unicode identity even on case-sensitive
        # development machines. Existing files also use filesystem inode identity
        # to detect hardlinks; all checks precede registration in one writer lock.
        known = [row[0] for row in db.execute("SELECT path FROM objects")]
        for obj in normalized:
            path = obj["relative_path"]
            candidate = self.root / path
            if candidate.exists() and not candidate.is_file():
                raise ValueError("output path is not a regular file")
            key = unicodedata.normalize("NFC", path).casefold()
            for previous in known:
                other = self.root / previous
                if (key == unicodedata.normalize("NFC", previous).casefold()
                        or (candidate.exists() and other.exists() and os.path.samefile(candidate, other))):
                    raise PolicyConflict("output path aliases another object")
            known.append(path)

    def register_result(self, reservation_id, *, job_id, generation, manifest_sha256,
                        pipeline_sha256, result_manifest_sha256, objects):
        """objects: list of {object_id, bytes, sha256, relative_path}."""
        _id(job_id); _integer(generation, "generation", 1)
        for digest in (manifest_sha256, pipeline_sha256, result_manifest_sha256):
            _sha(digest)
        normalized = []
        for obj in objects:
            normalized.append(dict(object_id=_id(obj["object_id"]), bytes=_integer(obj["bytes"], "object bytes"),
                sha256=_sha(obj["sha256"]), relative_path=self._owned_path(reservation_id, obj["relative_path"])))
        if len({o["object_id"] for o in normalized}) != len(normalized) or not normalized:
            raise ValueError("empty or duplicate objects")
        normalized.sort(key=lambda o: o["object_id"])
        identity = _json(dict(job_id=job_id, generation=generation, manifest_sha256=manifest_sha256,
            pipeline_sha256=pipeline_sha256, result_manifest_sha256=result_manifest_sha256, objects=normalized))
        with self._transaction() as db:
            row = self._reservation(db, reservation_id)
            settings = json.loads(row["settings"])
            if row["state"] not in ("engine_started", "uncertain", "terminal"):
                raise PolicyConflict("result requires a started or confirmed terminal engine")
            if (row["job"], row["generation"], settings["manifest_sha256"]) != (job_id, generation, manifest_sha256):
                raise PolicyConflict("stale result identity")
            if sum(o["bytes"] for o in normalized) > settings["output_budget"]:
                raise PolicyConflict("result exceeds output budget")
            old = db.execute("SELECT identity FROM results WHERE reservation=?", (reservation_id,)).fetchone()
            if old:
                if old[0] != identity:
                    raise PolicyConflict("result changed")
                return
            self._check_path_aliases(db, normalized)
            db.execute("INSERT INTO results VALUES(?,?)", (reservation_id, identity))
            for obj in normalized:
                db.execute("INSERT INTO objects VALUES(?,?,?,?,?,NULL,NULL)",
                    (reservation_id, obj["object_id"], obj["bytes"], obj["sha256"], obj["relative_path"]))

    def accept_receipt(self, reservation_id, *, receipt_id, storage_receipt_id,
                       job_id, generation, manifest_sha256, pipeline_sha256,
                       result_manifest_sha256, accepted_object_ids):
        _id(receipt_id); _id(storage_receipt_id)
        _id(job_id); _integer(generation, "generation", 1)
        ids = sorted(set(accepted_object_ids))
        for value in ids:
            _id(value)
        payload = _json(dict(reservation_id=reservation_id, receipt_id=receipt_id,
            storage_receipt_id=storage_receipt_id, job_id=job_id, generation=generation,
            manifest_sha256=manifest_sha256, pipeline_sha256=pipeline_sha256,
            result_manifest_sha256=result_manifest_sha256, accepted_object_ids=ids))
        with self._transaction() as db:
            result = db.execute("SELECT identity FROM results WHERE reservation=?", (reservation_id,)).fetchone()
            if not result:
                raise PolicyConflict("unknown result")
            identity = json.loads(result[0])
            supplied = json.loads(payload)
            for key in ("job_id", "generation", "manifest_sha256", "pipeline_sha256", "result_manifest_sha256"):
                if identity[key] != supplied[key]:
                    raise PolicyConflict("stale receipt")
            if not set(ids) <= {o["object_id"] for o in identity["objects"]}:
                raise PolicyConflict("receipt contains unknown object")
            old = db.execute("SELECT payload FROM receipts WHERE id=?", (receipt_id,)).fetchone()
            if old:
                if old[0] != payload:
                    raise PolicyConflict("receipt changed")
                return
            storage_old = db.execute("SELECT id FROM receipts WHERE storage_id=?", (storage_receipt_id,)).fetchone()
            if storage_old:
                raise PolicyConflict("storage receipt already bound")
            db.execute("INSERT INTO receipts VALUES(?,?,?)", (receipt_id, storage_receipt_id, payload))
            for object_id in ids:
                db.execute("UPDATE objects SET accepted=COALESCE(accepted,?) WHERE reservation=? AND id=?",
                           (self._now(db), reservation_id, object_id))

    def set_pin(self, reservation_id, object_id, reference, *, kind, state):
        _id(reference)
        if kind not in ("read", "worker", "downstream") or state not in ("active", "uncertain", "released"):
            raise ValueError("invalid pin")
        with self._transaction() as db:
            obj = db.execute("SELECT cleaned FROM objects WHERE reservation=? AND id=?",
                             (reservation_id, object_id)).fetchone()
            if obj is None or obj[0] is not None:
                raise PolicyConflict("unknown or cleaned object")
            if db.execute("SELECT 1 FROM cleanup_intents WHERE reservation=? AND object=?",
                          (reservation_id, object_id)).fetchone():
                raise PolicyConflict("object is fenced for cleanup")
            old = db.execute("SELECT kind FROM pins WHERE reservation=? AND object=? AND reference=?",
                             (reservation_id, object_id, reference)).fetchone()
            if old and old[0] != kind:
                raise PolicyConflict("pin kind changed")
            db.execute("INSERT INTO pins VALUES(?,?,?,?,?) ON CONFLICT(reservation,object,reference) "
                       "DO UPDATE SET state=excluded.state", (reservation_id, object_id, reference, kind, state))

    def _eligible(self, db, now):
        return db.execute("""SELECT o.*,r.generation FROM objects o JOIN reservations r
            ON r.id=o.reservation WHERE r.state='terminal' AND o.accepted IS NOT NULL
            AND o.cleaned IS NULL AND MAX(o.accepted,r.terminal_at)<=?
            AND NOT EXISTS(SELECT 1 FROM cleanup_intents c WHERE c.reservation=o.reservation AND c.object=o.id)
            AND NOT EXISTS(SELECT 1 FROM pins p WHERE p.reservation=o.reservation
              AND p.object=o.id AND p.state!='released') ORDER BY o.reservation,o.id""", (now-self.grace,))

    def cleanup_candidates(self, *, max_count=100, max_bytes=1024**3):
        _integer(max_count, "max count"); _integer(max_bytes, "max bytes")
        planned, total = [], 0
        with self._transaction() as db:
            for row in self._eligible(db, self._now(db)):
                if len(planned) >= max_count:
                    break
                if total + row["bytes"] > max_bytes:
                    continue
                self._owned_path(row["reservation"], row["path"])
                planned.append(dict(row))
                total += row["bytes"]
        return planned

    def begin_cleanup(self, reservation_id, object_id, *, generation, sha256, lease_seconds=60):
        """Atomically fence new consumers and return a durable executor intent.

        A lease expiry never removes the fence. Recovery must reconcile whether
        deletion occurred before confirming with the same token. No file is deleted.
        """
        _integer(generation, "generation", 1); _sha(sha256)
        _integer(lease_seconds, "cleanup lease", 1)
        with self._transaction() as db:
            now = self._now(db)
            row = next((r for r in self._eligible(db, now)
                        if r["reservation"] == reservation_id and r["id"] == object_id), None)
            if row is None or row["generation"] != generation or row["sha"] != sha256:
                raise PolicyConflict("cleanup candidate changed or is no longer eligible")
            self._owned_path(reservation_id, row["path"])
            token = secrets.token_hex(32)
            expires = now + lease_seconds
            db.execute("INSERT INTO cleanup_intents VALUES(?,?,?,?,'cleanup_pending')",
                       (reservation_id, object_id, token, expires))
            return dict(row) | dict(intent_token=token, lease_expires=expires, state="cleanup_pending")

    def cleanup_intent(self, reservation_id, object_id):
        """Recover a persisted fence; expired leases report uncertainty and retain it."""
        with self._transaction() as db:
            now = self._now(db)
            row = db.execute("""SELECT c.*,c.token AS intent_token,c.expires AS lease_expires,
                             o.path,o.sha,o.bytes,r.generation FROM cleanup_intents c
                             JOIN objects o ON o.reservation=c.reservation AND o.id=c.object
                             JOIN reservations r ON r.id=c.reservation
                             WHERE c.reservation=? AND c.object=?""",
                             (reservation_id, object_id)).fetchone()
            if row is None:
                raise PolicyConflict("unknown cleanup intent")
            if row["state"] == "cleanup_pending" and row["expires"] <= now:
                db.execute("UPDATE cleanup_intents SET state='uncertain' WHERE reservation=? AND object=?",
                           (reservation_id, object_id))
                return dict(row) | dict(state="uncertain")
            return dict(row)

    def mark_cleaned(self, reservation_id, object_id, *, generation, sha256,
                     intent_token, executor_attested):
        """Confirm the exact fenced intent after executor path/hash verified unlink.

        Expired intents may be confirmed only after executor reconciliation; the
        attestation promises confirmed removal, never inference from missing ACK.
        """
        _integer(generation, "generation", 1); _sha(sha256)
        if executor_attested is not True:
            raise PolicyConflict("cleanup executor attestation required")
        with self._transaction() as db:
            row = db.execute("SELECT o.*,r.generation FROM objects o JOIN reservations r "
                "ON r.id=o.reservation WHERE o.reservation=? AND o.id=?", (reservation_id, object_id)).fetchone()
            if row is None or row["generation"] != generation or row["sha"] != sha256:
                raise PolicyConflict("cleanup identity changed")
            intent = db.execute("SELECT * FROM cleanup_intents WHERE reservation=? AND object=?",
                                (reservation_id, object_id)).fetchone()
            if intent is None or not isinstance(intent_token, str) or not secrets.compare_digest(intent["token"], intent_token):
                raise PolicyConflict("cleanup intent token does not match")
            if row["cleaned"] is not None:
                return
            self._owned_path(reservation_id, row["path"])
            db.execute("UPDATE objects SET cleaned=? WHERE reservation=? AND id=?",
                       (self._now(db), reservation_id, object_id))
            db.execute("UPDATE cleanup_intents SET state='cleaned' WHERE reservation=? AND object=?",
                       (reservation_id, object_id))
