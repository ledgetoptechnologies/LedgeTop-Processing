"""Disposable v2 range-ledger model; not used by the Processing runtime.

The real broker must authenticate grants and range proofs, bind every range to
an authorized manifest object, and coordinate this ledger with source custody.
This module only mirrors the Viewer fixture for cross-language state tests.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3

MAX_SAFE_INTEGER = 2**53 - 1
_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,127}\Z")
_HASH = re.compile(r"[a-f0-9]{64}\Z")


class CandidateRejected(ValueError):
    """A synthetic candidate record violates its closed test contract."""


def _fail():
    raise CandidateRejected("candidate record rejected")


def _closed(value, fields):
    return type(value) is dict and set(value) == set(fields)


def _id(value):
    return type(value) is str and _ID.fullmatch(value) is not None


def _hash(value):
    return type(value) is str and _HASH.fullmatch(value) is not None


def _positive(value):
    return type(value) is int and 0 < value <= MAX_SAFE_INTEGER


def validate_reservation(reservation):
    if (not _closed(reservation, {"version", "environment", "jobId", "manifestSha256",
                                  "generation", "reservationId", "policy"})
            or type(reservation["version"]) is not int or reservation["version"] != 2
            or reservation["environment"] not in ("staging", "production")
            or not _id(reservation["jobId"]) or not _hash(reservation["manifestSha256"])
            or not _positive(reservation["generation"]) or not _id(reservation["reservationId"])):
        _fail()
    policy = reservation["policy"]
    if (not _closed(policy, {"version", "chunkBytes", "requestBudget", "byteBudget"})
            or type(policy["version"]) is not int or policy["version"] != 1
            or any(not _positive(policy[key]) for key in ("chunkBytes", "requestBudget", "byteBudget"))):
        _fail()


def validate_grant(grant, reservation):
    validate_reservation(reservation)
    if (not _closed(grant, {"version", "environment", "jobId", "manifestSha256",
                            "generation", "reservationId", "grantId", "policy"})
            or not _id(grant["grantId"])):
        _fail()
    for key in ("version", "environment", "jobId", "manifestSha256", "generation", "reservationId"):
        if grant[key] != reservation[key]:
            _fail()
    validate_reservation({**reservation, "policy": grant["policy"]})
    if grant["policy"] != reservation["policy"]:
        _fail()


def _identity(reservation):
    return json.dumps([reservation["environment"], reservation["jobId"],
                       reservation["manifestSha256"]], separators=(",", ":"))


def _policy_text(policy):
    return json.dumps(policy, sort_keys=True, separators=(",", ":"))


def _proof_bytes(identity, object_id, start, length, body):
    record = {
        "identity": identity,
        "objectId": object_id,
        "start": start,
        "length": length,
        "sha256": hashlib.sha256(body).hexdigest(),
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8")


class RangeLedger:
    """A disposable SQLite model of non-resetting range admission and proof."""

    def __init__(self, filename, proof_key):
        if type(proof_key) is not bytes or len(proof_key) < 16:
            _fail()
        self._proof_key = proof_key
        self._db = sqlite3.connect(filename, isolation_level=None, timeout=5)
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs(
                identity TEXT PRIMARY KEY,
                policy TEXT NOT NULL,
                requests INTEGER NOT NULL DEFAULT 0,
                bytes INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS ranges(
                identity TEXT NOT NULL,
                object_id TEXT NOT NULL,
                start INTEGER NOT NULL,
                length INTEGER NOT NULL,
                state TEXT NOT NULL,
                PRIMARY KEY(identity, object_id, start),
                FOREIGN KEY(identity) REFERENCES jobs(identity)
            );
        """)

    def close(self):
        self._db.close()

    def charge(self, reservation, grant, object_id, start, length):
        validate_grant(grant, reservation)
        policy = reservation["policy"]
        if (not _id(object_id) or type(start) is not int or start < 0
                or start > MAX_SAFE_INTEGER or not _positive(length)
                or length > policy["chunkBytes"] or start + length > MAX_SAFE_INTEGER):
            _fail()
        identity = _identity(reservation)
        policy_text = _policy_text(policy)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            row = self._db.execute("SELECT policy, requests, bytes FROM jobs WHERE identity=?",
                                   (identity,)).fetchone()
            if row and row[0] != policy_text:
                _fail()
            if not row:
                self._db.execute("INSERT INTO jobs(identity, policy) VALUES(?, ?)",
                                 (identity, policy_text))
                requests = used_bytes = 0
            else:
                requests, used_bytes = row[1], row[2]
            overlap = self._db.execute(
                "SELECT 1 FROM ranges WHERE identity=? AND object_id=? "
                "AND start < ? AND start + length > ? LIMIT 1",
                (identity, object_id, start + length, start),
            ).fetchone()
            if (overlap or requests >= policy["requestBudget"]
                    or length > policy["byteBudget"] - used_bytes):
                _fail()
            self._db.execute("UPDATE jobs SET requests=requests+1, bytes=bytes+? WHERE identity=?",
                             (length, identity))
            self._db.execute("INSERT INTO ranges VALUES(?, ?, ?, ?, 'uncertain')",
                             (identity, object_id, start, length))
            self._db.execute("COMMIT")
        except Exception:
            self._db.execute("ROLLBACK")
            raise
        return "charged"

    def verify(self, reservation, object_id, start, body, proof):
        validate_reservation(reservation)
        identity = _identity(reservation)
        row = self._db.execute(
            "SELECT length, state FROM ranges WHERE identity=? AND object_id=? AND start=?",
            (identity, object_id, start),
        ).fetchone()
        if (not row or row[1] != "uncertain" or type(body) is not bytes
                or len(body) != row[0] or not _hash(proof)):
            _fail()
        expected = hmac.new(self._proof_key,
                            _proof_bytes(identity, object_id, start, row[0], body),
                            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, proof):
            _fail()
        updated = self._db.execute(
            "UPDATE ranges SET state='verified' WHERE identity=? AND object_id=? "
            "AND start=? AND state='uncertain'", (identity, object_id, start),
        ).rowcount
        if updated != 1:
            _fail()

    def status(self, reservation, object_id, start):
        validate_reservation(reservation)
        row = self._db.execute(
            "SELECT state, length FROM ranges WHERE identity=? AND object_id=? AND start=?",
            (_identity(reservation), object_id, start),
        ).fetchone()
        return None if not row else {"state": row[0], "length": row[1]}

    def usage(self, reservation):
        validate_reservation(reservation)
        row = self._db.execute("SELECT requests, bytes FROM jobs WHERE identity=?",
                               (_identity(reservation),)).fetchone()
        return None if not row else {"requests": row[0], "bytes": row[1]}


def range_proof(key, reservation, object_id, start, body):
    """Test helper only; the real broker must issue authenticated evidence."""
    identity = _identity(reservation)
    data = _proof_bytes(identity, object_id, start, len(body), body)
    return hmac.new(key, data, hashlib.sha256).hexdigest()
