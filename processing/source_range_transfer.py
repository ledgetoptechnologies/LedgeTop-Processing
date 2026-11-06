"""Candidate-only resumable client for a future NAS source-range endpoint.

This module is deliberately not wired into the Processing runtime. It requires
the matching versioned endpoint contract and explicit operator activation.
The v2 queued job adds a closed ``transferRequest`` envelope containing
``protocol, reservationId, generation, expiresAt, manifestSha256, sourceVersion,
objectIds, chunkBytes, requestBudget, byteBudget, nonce, attemptBudget``. The short-lived v2
grant echoes the reservation identity and budgets, adds jobId, environment, and
token, and has a shorter ``expiresAt`` than the reservation lease. A journaled
renewal from the coordinator may outlive that lease, because the claim, not the
reservation deadline, keeps a transfer in progress; see ``past_reservation``. The
``attemptBudget`` caps all HTTP attempts, including retries, and is charged
durably before each request; ``requestBudget`` charges unique range intents.
This keeps reservation/source identity in the transfer envelope; v1 queued
records do not carry the sourceVersion fields required here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import ssl
import stat
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

from . import reference_protocol as p
from .reference_transfer import TransferError, _now

CHUNK_BYTES = 8 * 1024 * 1024
OBJECT_PREFIX = "/api/processing/source-range/v2/objects/"
RENEWAL_PATH = "/api/processing/source-range/v2/grants/renew"
RENEWAL_ACK_PATH = RENEWAL_PATH + "/ack"
REQUEST_HEADER = "X-Processing-Request-ID"
MAX_RENEWAL_SEQUENCE = 1000
MAX_RENEWAL_BODY_BYTES = p.MAX_BODY_BYTES


class SourceRenewalUnavailable(TransferError):
    """A renewal request may have been accepted; retry the durable intent."""


class SourceRenewalInvalid(TransferError):
    """A response could not be validated; preserve state and hold for review."""


def source_version_for_manifest(manifest):
    """Return the cross-service immutable identity for a Viewer dataset input."""
    if type(manifest) is not dict:
        raise TransferError("invalid source manifest identity")
    dataset_prefix = "dataset-"
    manifest_id = manifest.get("id")
    digest = manifest.get("sha256")
    if (type(manifest_id) is not str or type(digest) is not str
            or not re.fullmatch(r"[a-f0-9]{64}", digest)):
        raise TransferError("invalid source manifest identity")
    if manifest_id.startswith(dataset_prefix):
        dataset_id = manifest_id[len(dataset_prefix):]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", dataset_id):
            raise TransferError("invalid source manifest identity")
    else:
        # Viewer V2 freezes the dataset's canonical UUID directly in manifest.id.
        # Preserve existing prefixed identities; never rewrite a signed manifest.
        try:
            dataset_id = p._uuid(manifest_id, 'dataset ID')
        except ValueError:
            raise TransferError("invalid source manifest identity") from None
    return f"dataset:{dataset_id}:{digest}"


def build_source_transfer_request(job, *, reservation_id, expires_at, nonce, clock=time.time):
    """Build the immutable v2 reservation envelope persisted by the queue."""
    if type(job) is not dict or type(job.get('submission')) is not dict:
        raise TransferError('invalid source job')
    try:
        manifest = job['submission']['manifest']
        if (type(manifest) is not dict or set(manifest) != {
                'id', 'revision', 'sha256', 'totalBytes', 'objectCount', 'inputCounts', 'objects'}
                or type(manifest['objects']) is not list or not manifest['objects']
                or type(manifest['totalBytes']) is not int or manifest['totalBytes'] < 1
                or type(manifest['objectCount']) is not int or manifest['objectCount'] != len(manifest['objects'])
                or p.manifest_sha256(manifest) != manifest['sha256']):
            raise ValueError()
        for item in manifest['objects']:
            if (type(item) is not dict or set(item) != {'id', 'name', 'byteSize', 'sha256', 'kind', 'mediaType'}
                    or type(item['byteSize']) is not int or item['byteSize'] < 0):
                raise ValueError()
            p._slug(item['id'], 'source object ID')
            p._hash(item['sha256'], 'source object SHA-256')
            p._basename(item['name'])
        if sum(item['byteSize'] for item in manifest['objects']) != manifest['totalBytes']:
            raise ValueError()
        if (job['manifestSha256'] != manifest['sha256']
                or job['environment'] not in ('production', 'staging')
                or type(job['generation']) is not int or job['generation'] < 1):
            raise ValueError()
        source_version = source_version_for_manifest(manifest)
        p._slug(reservation_id, 'reservation ID')
        p._slug(nonce, 'source transfer nonce')
        p._string(expires_at, 'source transfer expiry', 24)
        expiry = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
        if (not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z', expires_at)
                or expiry.utcoffset() != timezone.utc.utcoffset(None)
                or expiry.isoformat(timespec='milliseconds').replace('+00:00', 'Z') != expires_at
                or expiry.timestamp() <= _now(clock)):
            raise ValueError()
        total_bytes = manifest['totalBytes']
        minimum_requests = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES
                               for item in manifest['objects'])
        if not minimum_requests:
            raise ValueError()
        byte_budget = min(p.MAX_SAFE_INTEGER, total_bytes * 3)
        attempt_budget = min(p.MAX_SAFE_INTEGER, minimum_requests * 3)
        return {
            'protocol': 'source-range-v2', 'reservationId': reservation_id,
            'generation': job['generation'], 'expiresAt': expires_at,
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version,
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': minimum_requests, 'byteBudget': byte_budget,
            'nonce': nonce, 'attemptBudget': attempt_budget,
        }
    except (KeyError, TypeError, ValueError, AttributeError):
        raise TransferError('invalid source transfer request') from None


@dataclass(frozen=True)
class SourceIdentity:
    environment: str
    job_id: str
    reservation_id: str
    generation: int
    manifest_sha256: str
    source_version: str

    def canonical(self):
        values = {
            "environment": self.environment, "jobId": self.job_id,
            "reservationId": self.reservation_id, "generation": self.generation,
            "manifestSha256": self.manifest_sha256, "sourceVersion": self.source_version,
        }
        if self.environment not in ("production", "staging"):
            raise TransferError("invalid source identity")
        for key in ("job_id", "reservation_id"):
            value = getattr(self, key)
            if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
                raise TransferError("invalid source identity")
        if type(self.generation) is not int or self.generation < 1:
            raise TransferError("invalid source identity")
        if type(self.manifest_sha256) is not str or not re.fullmatch(r"[a-f0-9]{64}", self.manifest_sha256):
            raise TransferError("invalid source identity")
        if type(self.source_version) is not str or not 1 <= len(self.source_version) <= 256 or any(ord(c) < 33 or ord(c) > 126 for c in self.source_version):
            raise TransferError("invalid source identity")
        return values


@dataclass(frozen=True)
class SourceObject:
    id: str
    byte_size: int
    sha256: str

    def validate(self):
        if (type(self.id) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", self.id)
                or type(self.byte_size) is not int or self.byte_size < 0
                or type(self.sha256) is not str or not re.fullmatch(r"[a-f0-9]{64}", self.sha256)):
            raise TransferError("invalid source object")


@dataclass(frozen=True)
class ValidatedSourceGrant:
    identity: SourceIdentity
    expires: float
    objects: tuple
    chunk_bytes: int
    request_budget: int
    byte_budget: int
    attempt_budget: int
    token: str = field(repr=False)


def validate_source_grant(grant, *, job, clock=time.time, allow_expired=False, past_reservation=False):
    """Validate a closed v2 grant against the immutable queued job envelope.

    ``past_reservation`` lifts only the reservation-deadline bound. Pass it for
    a grant the caller holds through the renewal journal, never for a first
    grant: the coordinator mints those strictly inside the reservation.
    """
    if type(allow_expired) is not bool or type(past_reservation) is not bool:
        raise TransferError("invalid source grant validation mode")
    fields = ("protocol", "reservationId", "generation", "expiresAt", "manifestSha256",
              "sourceVersion", "jobId", "environment", "objectIds", "nonce", "token", "chunkBytes",
              "requestBudget", "byteBudget", "attemptBudget")
    if type(grant) is not dict or set(grant) != set(fields):
        raise TransferError("invalid source grant shape")
    if type(job) is not dict or grant["protocol"] != "source-range-v2":
        raise TransferError("invalid source grant protocol")
    try:
        identity = SourceIdentity(grant["environment"], grant["jobId"], grant["reservationId"],
                                  grant["generation"], grant["manifestSha256"], grant["sourceVersion"])
        identity.canonical()
        transfer = job["transferRequest"]
        transfer_fields = ("protocol", "reservationId", "generation", "expiresAt", "manifestSha256",
                           "sourceVersion", "objectIds", "chunkBytes", "requestBudget", "byteBudget", "nonce")
        transfer_fields = (*transfer_fields, "attemptBudget")
        p._closed(transfer, transfer_fields, "source transfer request")
        if transfer["protocol"] != "source-range-v2":
            raise TransferError("unsupported queued source transfer protocol")
        if (job["jobId"], job["generation"], job["environment"], job["manifestSha256"],
                transfer["sourceVersion"], transfer["reservationId"], transfer["generation"],
                transfer["manifestSha256"]) != (
                identity.job_id, identity.generation, identity.environment, identity.manifest_sha256,
                identity.source_version, identity.reservation_id, identity.generation, identity.manifest_sha256):
            raise TransferError("source grant does not match queued job")
        p._slug(transfer["nonce"], "source transfer nonce")
        for key in transfer_fields:
            if key == "expiresAt":
                continue  # reservation deadline; grant expiry is shorter-lived and may rotate
            if grant.get(key) != transfer[key]:
                raise TransferError("source grant differs from queued transfer request")
        manifest = job["submission"]["manifest"]
        p._closed(manifest, ("id", "revision", "sha256", "totalBytes", "objectCount", "inputCounts", "objects"), "source manifest")
        p._slug(manifest["id"], "manifest.id")
        p._integer(manifest["revision"], "manifest.revision")
        p._integer(manifest["totalBytes"], "manifest.totalBytes")
        counts = manifest["inputCounts"]
        p._closed(counts, ("images", "lidarObjects", "points"), "manifest input counts")
        for count in counts.values():
            p._integer(count, "manifest input count", allow_zero=True)
        if (manifest["sha256"] != identity.manifest_sha256
                or p.manifest_sha256(manifest) != identity.manifest_sha256):
            raise TransferError("source grant manifest mismatch")
        if identity.source_version != source_version_for_manifest(manifest):
            raise TransferError("source grant revision mismatch")
    except (KeyError, TypeError, ValueError, AttributeError):
        raise TransferError("invalid queued source job") from None
    if type(grant["chunkBytes"]) is not int or grant["chunkBytes"] != CHUNK_BYTES:
        raise TransferError("invalid canonical source chunk size")
    for name in ("requestBudget", "byteBudget", "attemptBudget"):
        value = grant[name]
        if type(value) is not int or not 1 <= value <= p.MAX_SAFE_INTEGER:
            raise TransferError("invalid source transfer budget")
    manifest_objects, ids = manifest.get("objects"), grant["objectIds"]
    if (type(manifest_objects) is not list or type(ids) is not list
            or any(type(item) is not str for item in ids) or len(ids) != len(set(ids))):
        raise TransferError("invalid source object closure")
    objects = []
    for raw in manifest_objects:
        if type(raw) is not dict or set(raw) != {"id", "name", "byteSize", "sha256", "kind", "mediaType"}:
            raise TransferError("invalid source object closure")
        p._basename(raw["name"])
        if raw["kind"] not in ("image", "lidar", "auxiliary") or type(raw["mediaType"]) is not str:
            raise TransferError("invalid source object closure")
        obj = SourceObject(raw["id"], raw["byteSize"], raw["sha256"])
        obj.validate()
        objects.append(obj)
    if (len({obj.id for obj in objects}) != len(objects) or set(ids) != {obj.id for obj in objects}
            or sum(obj.byte_size for obj in objects) != manifest.get("totalBytes")
            or manifest.get("objectCount") != len(objects)
            or counts.get("images") != sum(1 for raw in manifest_objects if raw.get("kind") == "image")
            or counts.get("lidarObjects") != sum(1 for raw in manifest_objects if raw.get("kind") == "lidar")
            or grant["byteBudget"] < manifest["totalBytes"]):
        raise TransferError("source object closure or byte budget mismatch")
    minimum_requests = sum((obj.byte_size + grant["chunkBytes"] - 1) // grant["chunkBytes"] for obj in objects)
    if grant["requestBudget"] < minimum_requests:
        raise TransferError("source request budget is too small")
    if grant["attemptBudget"] < minimum_requests:
        raise TransferError("source attempt budget is too small")
    token = grant["token"]
    if (type(token) is not str or not 1 <= len(token) <= 4096
            or any(ord(char) < 33 or ord(char) > 126 for char in token)):
        raise TransferError("invalid source access grant")
    def timestamp(value, label):
        if type(value) is not str or len(value) > 40:
            raise TransferError("invalid " + label)
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.utcoffset() is None:
                raise ValueError()
            return parsed.timestamp()
        except (ValueError, OverflowError, OSError):
            raise TransferError("invalid " + label) from None
    expires = timestamp(grant["expiresAt"], "source grant expiry")
    lease_expires = timestamp(transfer["expiresAt"], "source reservation expiry")
    if expires <= _now(clock) and not allow_expired:
        raise TransferError("source grant expired")
    if not past_reservation and (expires > lease_expires or lease_expires <= _now(clock)):
        raise TransferError("source grant exceeds reservation lease")
    return ValidatedSourceGrant(identity, expires, tuple(objects), grant["chunkBytes"],
                                grant["requestBudget"], grant["byteBudget"], grant["attemptBudget"], token)


def _validator(obj):
    return '"sha256-' + obj.sha256 + '"'


def _request_id(identity_json, obj, start, length):
    # No ephemeral bearer grant/token participates in this stable identity.
    payload = json.dumps([identity_json, obj.id, obj.byte_size, obj.sha256, start, length],
                         separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()


class _RefuseRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise TransferError("source broker redirects are forbidden")


class SourceRangeClient:
    """Durably fetch canonical byte ranges and publish one verified object."""

    def __init__(self, origin, ledger_path, *, opener=None, ssl_context=None,
                 clock=time.time, timeout=60, renewal_origin=None, renewal_headers=None,
                 renewal_ssl_context=None):
        """``renewal_headers(route, payload)`` adds the worker's own credentials.

        They go only to ``renewal_origin``, the coordinator; range reads keep
        using just the grant token, so the broker origin never sees them.
        """
        for candidate in (origin, origin if renewal_origin is None else renewal_origin):
            parsed = urlsplit(candidate)
            if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                    or parsed.password is not None or parsed.path not in ("", "/")
                    or parsed.query or parsed.fragment or "\\" in candidate
                    or any(ord(c) <= 32 or ord(c) == 127 or c.isspace() for c in candidate)):
                raise ValueError("source broker must be a configured HTTPS origin")
        if renewal_headers is not None and not callable(renewal_headers):
            raise ValueError("invalid source renewal credentials")
        context = ssl_context or ssl.create_default_context()
        if not context.check_hostname or context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("source broker TLS verification is required")
        if renewal_ssl_context is not None and (not isinstance(renewal_ssl_context,ssl.SSLContext)
                or not renewal_ssl_context.check_hostname or renewal_ssl_context.verify_mode!=ssl.CERT_REQUIRED):
            raise ValueError('source coordinator TLS verification is required')
        if type(timeout) not in (int, float) or isinstance(timeout, bool) or not 0 < timeout <= 300:
            raise ValueError("invalid source transfer timeout")
        self.origin, self.clock, self.timeout = origin.rstrip("/"), clock, timeout
        self.renewal_origin = self.origin if renewal_origin is None else renewal_origin.rstrip("/")
        self.renewal_headers = renewal_headers
        self._open = opener or build_opener(_RefuseRedirects(), HTTPSHandler(context=context)).open
        self._renewal_open = (build_opener(_RefuseRedirects(),HTTPSHandler(context=renewal_ssl_context)).open
                              if renewal_ssl_context is not None else None)
        self.ledger_path = Path(ledger_path).absolute()
        if any(p.is_symlink() for p in (self.ledger_path.parent, *self.ledger_path.parent.parents)):
            raise ValueError("private ledger directory required")
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = self.ledger_path.parent.stat()
        if os.name != "nt" and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise ValueError("private ledger directory required")
        self._init_ledger()

    def _connect(self):
        db = sqlite3.connect(self.ledger_path, timeout=30, isolation_level=None)
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA journal_mode=DELETE")
        return db

    def _post_renewal_json(self, route, payload):
        """POST one bounded canonical renewal message without redirects."""
        if route not in (RENEWAL_PATH, RENEWAL_ACK_PATH):
            raise SourceRenewalInvalid("invalid source renewal route")
        try:
            body = p.canonical_json(payload)
        except Exception:
            raise SourceRenewalInvalid("invalid source renewal request") from None
        if len(body) > MAX_RENEWAL_BODY_BYTES:
            raise SourceRenewalInvalid("source renewal request exceeds protocol limit")
        url = self.renewal_origin + route
        headers = {"Accept": "application/json", "Accept-Encoding": "identity",
                   "Content-Type": "application/json"}
        if self.renewal_headers is not None:
            headers.update(self.renewal_headers(route, payload))
        request = Request(url, data=body, method="POST", headers=headers)
        try:
            try:
                response = (self._renewal_open or self._open)(request, timeout=self.timeout)
            except HTTPError as error:
                response = error
            except Exception:
                raise SourceRenewalUnavailable("source renewal endpoint unavailable") from None
            try:
                status = getattr(response, "status", None)
                if (type(status) is not int or response.geturl() != url
                        or response.headers.get("Content-Type", "").split(";", 1)[0].lower() != "application/json"
                        or response.headers.get("Content-Encoding", "identity").lower() != "identity"):
                    raise SourceRenewalInvalid("invalid source renewal response")
                raw = response.read(MAX_RENEWAL_BODY_BYTES + 1)
                if not isinstance(raw, bytes) or not raw or len(raw) > MAX_RENEWAL_BODY_BYTES:
                    raise SourceRenewalInvalid("invalid source renewal response size")
                expected = response.headers.get("Content-Length")
                if expected is not None and (not expected.isascii() or not expected.isdecimal()
                        or int(expected) != len(raw)):
                    raise SourceRenewalInvalid("truncated source renewal response")
                result = p.decode_submission(raw)
                if type(result) is not dict or p.canonical_json(result) != raw:
                    raise SourceRenewalInvalid("noncanonical source renewal response")
                return status, result
            except (SourceRenewalInvalid, SourceRenewalUnavailable):
                raise
            except Exception:
                raise SourceRenewalInvalid("invalid source renewal response") from None
            finally:
                response.close()
        except (SourceRenewalInvalid, SourceRenewalUnavailable):
            raise
        except Exception:
            raise SourceRenewalUnavailable("source renewal endpoint unavailable") from None

    def renew_grant(self, request):
        return self._post_renewal_json(RENEWAL_PATH, request)

    def acknowledge_grant_renewal(self, request):
        return self._post_renewal_json(RENEWAL_ACK_PATH, request)

    def committed_progress(self, identity):
        """Ranges and bytes durably committed; each one was served by the broker."""
        key, _identity = self._identity_key(identity)
        with closing(self._connect()) as db:
            return tuple(db.execute("""SELECT COUNT(*),COALESCE(SUM(length),0) FROM source_ranges
                WHERE identity_key=? AND state='committed'""", (key,)).fetchone())

    def _init_ledger(self):
        with closing(self._connect()) as db:
            db.executescript("""
              CREATE TABLE IF NOT EXISTS source_objects (
                identity_key TEXT NOT NULL, identity_json TEXT NOT NULL, object_id TEXT NOT NULL,
                byte_size INTEGER NOT NULL, object_sha256 TEXT NOT NULL, part_name TEXT NOT NULL,
                committed_bytes INTEGER NOT NULL DEFAULT 0, published INTEGER NOT NULL DEFAULT 0,
                part_device TEXT, part_inode TEXT,
                PRIMARY KEY(identity_key, object_id));
              CREATE TABLE IF NOT EXISTS source_ranges (
                identity_key TEXT NOT NULL, object_id TEXT NOT NULL, request_id TEXT NOT NULL,
                start INTEGER NOT NULL, length INTEGER NOT NULL, chunk_sha256 TEXT,
                state TEXT NOT NULL CHECK(state IN ('intent','committed')),
                PRIMARY KEY(identity_key, object_id, request_id),
                UNIQUE(identity_key, object_id, start));
              CREATE TABLE IF NOT EXISTS source_budgets (
                identity_key TEXT PRIMARY KEY, chunk_bytes INTEGER NOT NULL,
                request_budget INTEGER NOT NULL, byte_budget INTEGER NOT NULL,
                requests_used INTEGER NOT NULL DEFAULT 0, bytes_used INTEGER NOT NULL DEFAULT 0,
                attempt_budget INTEGER NOT NULL, attempts_used INTEGER NOT NULL DEFAULT 0);
            """)
        if os.name != "nt":
            os.chmod(self.ledger_path, 0o600)

    def _identity_key(self, identity):
        encoded = json.dumps(identity.canonical(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest(), encoded

    def _ensure_state(self, key, identity_json, obj, part, grant):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            budget = db.execute("SELECT chunk_bytes,request_budget,byte_budget,attempt_budget FROM source_budgets WHERE identity_key=?", (key,)).fetchone()
            settings = (grant.chunk_bytes, grant.request_budget, grant.byte_budget, grant.attempt_budget)
            if budget is None:
                db.execute("INSERT INTO source_budgets(identity_key,chunk_bytes,request_budget,byte_budget,attempt_budget) VALUES(?,?,?,?,?)", (key, *settings))
            elif budget != settings:
                db.rollback()
                raise TransferError("source transfer budget changed")
            row = db.execute("SELECT identity_json,byte_size,object_sha256,part_name,committed_bytes,published,part_device,part_inode FROM source_objects WHERE identity_key=? AND object_id=?", (key, obj.id)).fetchone()
            expected = (identity_json, obj.byte_size, obj.sha256, part.name)
            if row is None:
                db.execute("INSERT INTO source_objects(identity_key,identity_json,object_id,byte_size,object_sha256,part_name) VALUES(?,?,?,?,?,?)",
                           (key, identity_json, obj.id, *expected[1:]))
                db.commit()
                return 0, False, None, None
            if row[:4] != expected:
                db.rollback()
                raise TransferError("source transfer identity conflict")
            committed, published = row[4], bool(row[5])
            db.commit()
            return committed, published, row[6], row[7]

    def _bind_part(self, key, object_id, device, inode):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT part_device,part_inode FROM source_objects WHERE identity_key=? AND object_id=?",
                             (key, object_id)).fetchone()
            if not row or (row[0] is not None and row != (str(device), str(inode))):
                db.rollback()
                raise TransferError("source partial file identity conflict")
            db.execute("UPDATE source_objects SET part_device=?,part_inode=? WHERE identity_key=? AND object_id=?",
                       (str(device), str(inode), key, object_id))
            db.commit()

    def _record_intent(self, key, identity_json, obj, grant, request_id, start, length):
        if (type(start) is not int or type(length) is not int or start < 0 or length <= 0
                or start >= obj.byte_size or length != min(grant.chunk_bytes, obj.byte_size - start)
                or request_id != _request_id(identity_json, obj, start, length)):
            raise TransferError("noncanonical source range")
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT start,length,state FROM source_ranges WHERE identity_key=? AND object_id=? AND request_id=?",
                             (key, obj.id, request_id)).fetchone()
            if row:
                if row[:2] != (start, length):
                    db.rollback()
                    raise TransferError("source range request identity conflict")
                if row[2] == "committed":
                    db.commit()
                    return False
            else:
                previous = db.execute("SELECT start,length,state FROM source_ranges WHERE identity_key=? AND object_id=? ORDER BY start DESC LIMIT 1",
                                      (key, obj.id)).fetchone()
                if previous and (previous[2] != "committed" or previous[0] + previous[1] != start):
                    db.rollback()
                    raise TransferError("source ranges must be canonical and contiguous")
                if not previous and start != 0:
                    db.rollback()
                    raise TransferError("source ranges must start at zero")
                budget = db.execute("SELECT chunk_bytes,request_budget,byte_budget,requests_used,bytes_used,attempt_budget FROM source_budgets WHERE identity_key=?", (key,)).fetchone()
                if not budget or (budget[0], budget[1], budget[2], budget[5]) != (grant.chunk_bytes, grant.request_budget, grant.byte_budget, grant.attempt_budget):
                    db.rollback()
                    raise TransferError("source transfer budget changed")
                if budget[3] + 1 > budget[1] or budget[4] + length > budget[2]:
                    db.rollback()
                    raise TransferError("source transfer budget exhausted")
                db.execute("INSERT INTO source_ranges VALUES(?,?,?,?,?,NULL,'intent')",
                           (key, obj.id, request_id, start, length))
                db.execute("UPDATE source_budgets SET requests_used=requests_used+1,bytes_used=bytes_used+? WHERE identity_key=?",
                           (length, key))
            db.commit()
            return True

    def _record_attempt(self, key, object_id, request_id, grant):
        """Charge a network attempt before opening the request, including retries."""
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM source_ranges WHERE identity_key=? AND object_id=? AND request_id=?",
                             (key, object_id, request_id)).fetchone()
            budget = db.execute("SELECT attempt_budget,attempts_used FROM source_budgets WHERE identity_key=?", (key,)).fetchone()
            if not row or row[0] != "intent" or not budget:
                db.rollback()
                raise TransferError("source request intent is not pending")
            if budget[1] + 1 > budget[0] or budget[0] != grant.attempt_budget:
                db.rollback()
                raise TransferError("source attempt budget exhausted")
            db.execute("UPDATE source_budgets SET attempts_used=attempts_used+1 WHERE identity_key=?", (key,))
            db.commit()

    def _commit_range(self, key, object_id, request_id, start, length, chunk_hash):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT start,length,state,chunk_sha256 FROM source_ranges WHERE identity_key=? AND object_id=? AND request_id=?",
                             (key, object_id, request_id)).fetchone()
            state = db.execute("SELECT committed_bytes FROM source_objects WHERE identity_key=? AND object_id=?",
                               (key, object_id)).fetchone()
            if not row or not state or row[:2] != (start, length):
                db.rollback()
                raise TransferError("source range checkpoint missing")
            if row[2] == "committed":
                if row[3] != chunk_hash:
                    db.rollback()
                    raise TransferError("source range replay differs")
                db.commit()
                return
            if state[0] != start:
                db.rollback()
                raise TransferError("source range checkpoint is out of order")
            db.execute("UPDATE source_ranges SET state='committed',chunk_sha256=? WHERE identity_key=? AND object_id=? AND request_id=?",
                       (chunk_hash, key, object_id, request_id))
            db.execute("UPDATE source_objects SET committed_bytes=? WHERE identity_key=? AND object_id=?",
                       (start + length, key, object_id))
            db.commit()

    def _reconcile(self, key, obj, part, committed, expected_device, expected_inode):
        if not part.exists():
            if committed or expected_device is not None or expected_inode is not None:
                raise TransferError("committed source bytes are missing")
            fd = os.open(part, os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
            info = os.fstat(fd)
            os.close(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or (os.name != "nt" and info.st_uid != os.geteuid()):
                raise TransferError("source partial ownership is invalid")
            self._fsync_dir(part.parent)
            self._bind_part(key, obj.id, info.st_dev, info.st_ino)
            expected_device, expected_inode = str(info.st_dev), str(info.st_ino)
        if part.is_symlink():
            raise TransferError("source partial is not a regular file")
        fd = os.open(part, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (os.name != "nt" and info.st_uid != os.geteuid())
                or (expected_device, expected_inode) != (str(info.st_dev), str(info.st_ino))):
            os.close(fd)
            raise TransferError("source partial ownership or identity changed")
        with closing(self._connect()) as db:
            rows = db.execute("SELECT start,length,chunk_sha256 FROM source_ranges WHERE identity_key=? AND object_id=? AND state='committed' ORDER BY start",
                              (key, obj.id)).fetchall()
        cursor = 0
        with os.fdopen(fd, "r+b", buffering=0) as stream:
            for start, length, expected in rows:
                if start != cursor or length <= 0 or not expected:
                    raise TransferError("committed source ledger is inconsistent")
                stream.seek(start)
                data = stream.read(length)
                if len(data) != length or hashlib.sha256(data).hexdigest() != expected:
                    raise TransferError("committed source bytes are missing or corrupt")
                cursor += length
            if cursor != committed:
                raise TransferError("committed source byte count is inconsistent")
            if stream.seek(0, os.SEEK_END) != committed:
                stream.truncate(committed)
                stream.flush()
                os.fsync(stream.fileno())
        return str(info.st_dev), str(info.st_ino)

    def download_object(self, *, grant, object_id, destination, grant_provider=None):
        try:
            return self._download_object(grant=grant, object_id=object_id, destination=destination,
                                         grant_provider=grant_provider)
        except TransferError:
            raise
        except Exception:
            # Storage and HTTP exception strings may contain secrets or local paths.
            raise TransferError("source object transfer failed") from None

    def _download_object(self, *, grant, object_id, destination, grant_provider=None):
        if not isinstance(grant, ValidatedSourceGrant):
            raise TransferError("validated v2 source grant required")
        if grant_provider is not None and not callable(grant_provider):
            raise TransferError("invalid source grant provider")
        obj = next((item for item in grant.objects if item.id == object_id), None)
        if obj is None:
            raise TransferError("source object is outside grant")
        identity = grant.identity
        key, identity_json = self._identity_key(identity)
        lock_path = self.ledger_path.parent / (".source-lock-" + key[:32])
        with self._object_lock(lock_path):
            return self._download_locked(grant, identity, identity_json, key, obj, destination,
                                         grant_provider=grant_provider)

    @contextmanager
    def _object_lock(self, lock_path):
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        stream = os.fdopen(os.open(lock_path, flags, 0o600), "r+b", buffering=0)
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt
                stream.seek(0, os.SEEK_END)
                if stream.tell() == 0:
                    stream.write(b"\0")
                    stream.flush()
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            stream.close()
            raise TransferError("source object transfer is already active") from None
        try:
            yield
        finally:
            if acquired:
                try:
                    if os.name == "nt":
                        import msvcrt
                        stream.seek(0)
                        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
            stream.close()

    def _download_locked(self, grant, identity, identity_json, key, obj, destination, *, grant_provider=None):
        target = Path(destination).absolute()
        if target.name in ("", ".", "..") or target.parent.is_symlink() or not target.parent.is_dir():
            raise TransferError("private destination directory required")
        parent_info = target.parent.stat()
        if os.name != "nt" and (parent_info.st_uid != os.geteuid() or stat.S_IMODE(parent_info.st_mode) & 0o077):
            raise TransferError("private destination directory required")
        part = target.parent / (".source-range-" + key[:24] + "-" + hashlib.sha256(obj.id.encode()).hexdigest()[:24] + ".part")
        committed, published, part_device, part_inode = self._ensure_state(key, identity_json, obj, part, grant)
        if published:
            self._verify_target(target, obj)
            self._remove_published_partial(part, target, part_device, part_inode)
            return target
        if os.path.lexists(target):
            self._verify_target(target, obj)
            self._remove_published_partial(part, target, part_device, part_inode)
            self._mark_published(key, obj.id)
            return target
        part_device, part_inode = self._reconcile(key, obj, part, committed, part_device, part_inode)
        validator = _validator(obj)
        while committed < obj.byte_size:
            if grant.expires <= _now(self.clock) and grant_provider is not None:
                successor = grant_provider()
                old = grant
                if (not isinstance(successor, ValidatedSourceGrant)
                        or successor.identity != identity
                        or successor.expires <= _now(self.clock)
                        or successor.expires <= old.expires
                        or (successor.chunk_bytes, successor.request_budget, successor.byte_budget,
                            successor.attempt_budget, successor.objects) !=
                           (old.chunk_bytes, old.request_budget, old.byte_budget,
                            old.attempt_budget, old.objects)):
                    raise TransferError("invalid renewed source grant")
                grant = successor
            length = min(grant.chunk_bytes, obj.byte_size - committed)
            request_id = _request_id(identity_json, obj, committed, length)
            should_fetch = self._record_intent(key, identity_json, obj, grant, request_id, committed, length)
            if should_fetch:
                if grant.expires <= _now(self.clock):
                    raise TransferError("source grant expired")
                self._record_attempt(key, obj.id, request_id, grant)
                request = Request(self.origin + OBJECT_PREFIX + quote(obj.id, safe=""), method="GET", headers={
                    "Authorization": "Bearer " + grant.token,
                    "Range": "bytes=%d-%d" % (committed, committed + length - 1),
                    "If-Match": validator,
                    "Accept-Encoding": "identity",
                    REQUEST_HEADER: request_id,
                })
                try:
                    response = self._open(request, timeout=self.timeout)
                except HTTPError as error:
                    error.close()
                    raise TransferError("source range request rejected") from None
                except Exception:
                    raise TransferError("source range request failed") from None
                try:
                    if grant.expires <= _now(self.clock):
                        raise TransferError("source grant expired")
                    expected_range = "bytes %d-%d/%d" % (committed, committed + length - 1, obj.byte_size)
                    if response.status != 206 or response.geturl() != request.full_url:
                        raise TransferError("unexpected source range response")
                    headers = response.headers
                    if (headers.get("Content-Range") != expected_range
                            or headers.get("Content-Length") != str(length)
                            or headers.get("ETag") != validator
                            or headers.get(REQUEST_HEADER) != request_id
                            or headers.get("Content-Encoding", "identity").lower() != "identity"):
                        raise TransferError("source range response identity mismatch")
                    data = response.read(length + 1)
                    if not isinstance(data, bytes) or len(data) != length:
                        raise TransferError("source range body length mismatch")
                    if response.read(1):
                        raise TransferError("source range body exceeds declared length")
                    if grant.expires <= _now(self.clock):
                        raise TransferError("source grant expired")
                finally:
                    response.close()
                if target.parent.stat().st_ino != parent_info.st_ino or target.parent.stat().st_dev != parent_info.st_dev:
                    raise TransferError("destination directory changed")
                self._write_range_bytes(part, committed, data)
                self._commit_range(key, obj.id, request_id, committed, length, hashlib.sha256(data).hexdigest())
            committed += length
        part_device, part_inode = self._reconcile(key, obj, part, committed, part_device, part_inode)
        self._verify_file(part, obj)
        try:
            os.link(part, target, follow_symlinks=False)
        except FileExistsError:
            self._verify_target(target, obj)
        part.unlink()
        self._fsync_dir(target.parent)
        self._mark_published(key, obj.id)
        return target

    @staticmethod
    def _write_range_bytes(part, offset, data):
        """Write one range fully before it can be committed in the ledger."""
        fd = os.open(part, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        try:
            with os.fdopen(fd, "r+b", buffering=0) as stream:
                fd = None
                stream.seek(offset)
                written = stream.write(data)
                if written != len(data):
                    raise TransferError("short source range file write")
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            if fd is not None:
                os.close(fd)

    def _remove_published_partial(self, part, target, expected_device, expected_inode):
        """Finish cleanup after a crash between no-clobber publish and partial unlink."""
        try:
            part_info = os.lstat(part)
        except FileNotFoundError:
            return
        target_info = os.stat(target, follow_symlinks=False)
        if (not stat.S_ISREG(part_info.st_mode) or not stat.S_ISREG(target_info.st_mode)
                or part_info.st_nlink != 2
                or (str(part_info.st_dev), str(part_info.st_ino)) != (str(expected_device), str(expected_inode))
                or (part_info.st_dev, part_info.st_ino) != (target_info.st_dev, target_info.st_ino)):
            raise TransferError("published source partial identity changed")
        part.unlink()
        self._fsync_dir(part.parent)

    def _verify_file(self, path, obj):
        digest = hashlib.sha256()
        count = 0
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(CHUNK_BYTES)
                if not chunk:
                    break
                count += len(chunk)
                digest.update(chunk)
        if count != obj.byte_size or digest.hexdigest() != obj.sha256:
            raise TransferError("final source object integrity mismatch")

    def _verify_target(self, target, obj):
        if target.is_symlink() or not target.is_file():
            raise TransferError("published source object is not a regular file")
        self._verify_file(target, obj)

    def _mark_published(self, key, object_id):
        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE source_objects SET published=1 WHERE identity_key=? AND object_id=?", (key, object_id))
            db.commit()

    @staticmethod
    def _fsync_dir(directory):
        if os.name != "nt":
            fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
