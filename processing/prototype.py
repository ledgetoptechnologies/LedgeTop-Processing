"""Durable, single-slot queue for the isolated synthetic TreeDar prototype.

This is not a Viewer API or a production dataset admission path. A process lock
prevents a second coordinator from resetting another coordinator's live claims.
"""

import hashlib
from contextlib import contextmanager
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from .maintenance import Conflict, Maintenance


SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
PROFILE = "treedar-synthetic-v1"
PHOTO_PROFILE = "odx-photo-smoke-v1"
VIEWER_PROFILE = "odm-viewer-v1"
REFERENCE_PROFILE = "reference-pipeline-v1"


def _require_safe_idle_database_file(path):
    """Reject WAL and sidecars before any SQLite connection is opened."""
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise RuntimeError('production_hold_idle_state_unavailable')
    try:
        # WAL reads can create shared-memory files, and immutable SQLite reads
        # can silently ignore uncheckpointed WAL data. Until that case is
        # qualified, refuse it and any outstanding journal/sidecar outright.
        if any(os.path.lexists(str(path) + suffix) for suffix in ('-wal', '-shm', '-journal')):
            raise ValueError('database sidecars require inspection')
        with path.open('rb') as source:
            header = source.read(20)
        if len(header) != 20 or header[:16] != b'SQLite format 3\x00' or header[18:20] != b'\x01\x01':
            raise ValueError('unsupported database journal mode')
    except (OSError, ValueError):
        raise RuntimeError('production_hold_idle_state_unavailable') from None


def _require_idle_state(path):
    """Check held startup state under its flock, before any migration.

    This is a point-in-time check, not a fence on other volumes, direct
    engine clients, or noncooperating writers.
    """
    path = Path(path)
    _require_safe_idle_database_file(path)
    try:
        db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
        try:
            db.execute('BEGIN')
            if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('invalid database')
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'jobs', 'viewer_tasks'} <= tables:
                raise ValueError('missing state tables')
            if 'viewer_upload_intents' in tables:
                schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='viewer_upload_intents'").fetchone()
                indexes = db.execute('PRAGMA index_list(viewer_upload_intents)').fetchall()
                if (not schema or not isinstance(schema[0], str) or
                        hashlib.sha256(schema[0].encode()).hexdigest() != '7f7e1921a1402ffddb1fbcc80d97339a0413dce980592a8416a984bc81937cac' or
                        len(indexes) != 1 or indexes[0][1:5] != ('sqlite_autoindex_viewer_upload_intents_1', 1, 'pk', 0) or
                        db.execute('PRAGMA index_info(sqlite_autoindex_viewer_upload_intents_1)').fetchall() != [(0,0,'uuid'),(1,4,'name_key')] or
                        db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='viewer_upload_intents' LIMIT 1").fetchone() or
                        db.execute('SELECT 1 FROM viewer_upload_intents LIMIT 1').fetchone()):
                    raise ValueError('unresolved or invalid upload intents')
            if db.execute("SELECT 1 FROM jobs WHERE status IS NULL OR status NOT IN ('succeeded','failed','cancelled') LIMIT 1").fetchone():
                raise ValueError('nonterminal job')
            if db.execute("SELECT 1 FROM viewer_tasks WHERE stage IS NULL OR stage NOT IN ('succeeded','failed','cancelled') LIMIT 1").fetchone():
                raise ValueError('nonterminal task')
            if 'viewer_reservations' in tables and db.execute("""SELECT 1 FROM viewer_reservations r
                LEFT JOIN viewer_tasks t ON t.uuid=r.uuid
                WHERE r.state IS NULL OR r.state NOT IN ('reserved','cancelled')
                   OR (r.state='reserved' AND (t.uuid IS NULL OR t.stage IS NULL OR t.stage NOT IN ('succeeded','failed','cancelled')))
                LIMIT 1""").fetchone():
                raise ValueError('unresolved reservation')
        finally:
            db.close()
    except (sqlite3.Error, OSError, ValueError, TypeError, IndexError):
        raise RuntimeError('production_hold_idle_state_unavailable') from None


class Queue(Maintenance):
    def __init__(self, directory, photo_smoke_enabled=False, *, node_ids=('cluster',),
                 treedar_node_ids=None, viewer_enabled=False, reference_enabled=False, clock=None,
                 require_idle_existing=False):
        if (not isinstance(node_ids, (tuple, list)) or not node_ids
                or any(not isinstance(node, str) or not SLUG.fullmatch(node) for node in node_ids)
                or len(set(node_ids)) != len(node_ids) or 'cluster' not in node_ids):
            raise ValueError("node_ids must be unique configured slugs including cluster")
        self._creator_pid = os.getpid()
        self._lifecycle_lock = threading.RLock()
        self._database_users = 0
        self.node_ids = tuple(node_ids)
        if treedar_node_ids is None:
            # Isolated legacy fixtures retain their original node capabilities.
            treedar_node_ids = self.node_ids
        if (not isinstance(treedar_node_ids, (tuple, list)) or not treedar_node_ids
                or any(not isinstance(node, str) or node not in self.node_ids for node in treedar_node_ids)
                or len(set(treedar_node_ids)) != len(treedar_node_ids)):
            raise ValueError("treedar_node_ids must be unique configured node IDs")
        self.treedar_node_ids = tuple(treedar_node_ids)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.allowed_profiles = {PROFILE, PHOTO_PROFILE} if photo_smoke_enabled else {PROFILE}
        if viewer_enabled:
            self.allowed_profiles.add(VIEWER_PROFILE)
        if reference_enabled:
            self.allowed_profiles.add(REFERENCE_PROFILE)
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        self._lock = (root / "coordinator.lock").open("a+b")
        self._lock.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                if not self._lock.read(1):
                    self._lock.write(b"0")
                    self._lock.flush()
                self._lock.seek(0)
                msvcrt.locking(self._lock.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                self._lock.close()
                raise RuntimeError("prototype state already owned") from None
        else:
            import fcntl
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                self._lock.close()
                raise RuntimeError("prototype state already owned") from None
        # One ownership period, generated only after the OS lease is acquired.
        # Optional recovery persists this epoch with its restart overlay.
        self.startup_id = str(uuid.uuid4())
        self.path = root / "jobs.sqlite3"
        if require_idle_existing:
            try:
                _require_idle_state(self.path)
            except BaseException:
                self._lock.close()
                raise
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                request_id TEXT UNIQUE NOT NULL, environment TEXT NOT NULL,
                profile TEXT NOT NULL, status TEXT NOT NULL, worker_id TEXT,
                claim_hash TEXT, result TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
            if 'node_id' not in {row['name'] for row in db.execute('PRAGMA table_info(jobs)')}:
                db.execute('ALTER TABLE jobs ADD COLUMN node_id TEXT')
            if 'held_reason' not in {row['name'] for row in db.execute('PRAGMA table_info(jobs)')}:
                db.execute('ALTER TABLE jobs ADD COLUMN held_reason TEXT')
            db.execute('CREATE TABLE IF NOT EXISTS coordinator_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority_id TEXT NOT NULL)')
            db.execute('INSERT OR IGNORE INTO coordinator_identity VALUES(1, ?)', (str(uuid.uuid4()),))
            self.authority_id = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]
            db.execute("UPDATE jobs SET node_id='cluster' WHERE node_id IS NULL AND status IN ('running','held')")
            db.execute("UPDATE jobs SET status='held', held_reason='coordinator_restart', updated_at=CURRENT_TIMESTAMP WHERE status='running'")
            self._init_maintenance(db)

    def close(self):
        if os.getpid() != self._creator_pid:
            raise RuntimeError('coordinator process changed')
        with self._lifecycle_lock:
            if self._database_users:
                raise RuntimeError('coordinator transaction still active')
            self._lock.close()

    def node_profiles(self, node_id):
        return sorted(profile for profile in self.allowed_profiles
                      if profile != PROFILE or node_id in self.treedar_node_ids)

    def _node_public(self, row):
        return {**super()._node_public(row), "allowedProfiles": self.node_profiles(row['id'])}

    @contextmanager
    def _db(self):
        # Check the PID before acquiring an inherited mutex after fork. Keep
        # close() from releasing the authority lease until COMMIT/ROLLBACK ends.
        if os.getpid() != self._creator_pid:
            raise RuntimeError('coordinator process changed')
        with self._lifecycle_lock:
            if self._lock.closed:
                raise RuntimeError('coordinator ownership closed')
            db = sqlite3.connect(self.path, timeout=10)
            db.row_factory = sqlite3.Row
            self._database_users += 1
            try:
                db.execute("PRAGMA synchronous=FULL")
                with db:
                    yield db
                    if os.getpid() != self._creator_pid or self._lock.closed:
                        raise RuntimeError('coordinator ownership changed before commit')
            finally:
                try:
                    db.close()
                finally:
                    self._database_users -= 1

    def public(self, row):
        return {"id": row["id"], "requestId": row["request_id"],
                "environment": row["environment"], "profile": row["profile"],
                "status": row["status"], "workerId": row["worker_id"],
                "nodeId": row["node_id"],
                "authorityId": self.authority_id, "heldReason": row["held_reason"],
                "createdAt": row["created_at"], "updatedAt": row["updated_at"],
                "result": json.loads(row["result"]) if row["result"] else None}

    @staticmethod
    def fields(payload, expected):
        if not isinstance(payload, dict) or set(payload) != set(expected):
            raise ValueError("unexpected or missing fields")

    @staticmethod
    def slug(value):
        if not isinstance(value, str) or not SLUG.fullmatch(value):
            raise ValueError("identifier must be an ASCII slug of 1 to 64 characters")

    def submit(self, payload):
        self.fields(payload, ("requestId", "environment", "profile"))
        if payload['profile'] == REFERENCE_PROFILE:
            raise ValueError('reference submissions require the reference metadata API')
        self.slug(payload["requestId"])
        if (payload["environment"] not in ("production", "staging")
                or not isinstance(payload["profile"], str) or payload["profile"] not in self.allowed_profiles):
            raise ValueError("profile is not enabled or environment is invalid")
        if payload["profile"] == PHOTO_PROFILE and payload["environment"] != "staging":
            raise ValueError("the bounded photo smoke profile is staging-only")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM jobs WHERE request_id=?", (payload["requestId"],)).fetchone()
            if old:
                if old["environment"] != payload["environment"] or old["profile"] != payload["profile"]:
                    raise Conflict("requestId already belongs to a different submission")
                return self.public(old), False
            if db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] >= 1000:
                raise Conflict("prototype history limit reached")
            identifier = str(uuid.uuid4())
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'queued')",
                       (identifier, payload["requestId"], payload["environment"], payload["profile"]))
            return self.public(db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()), True

    def list(self):
        with self._db() as db:
            return [self.public(row) for row in db.execute("SELECT * FROM jobs ORDER BY seq")]

    def get(self, identifier):
        with self._db() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            return self.public(row) if row else None

    def claim(self, payload):
        from contextlib import nullcontext
        from .admission_drain import AdmissionClosed
        gate = getattr(self, 'admission_drain', None)
        try:
            with gate.admit() if gate is not None else nullcontext():
                return self._claim(payload)
        except AdmissionClosed:
            return None

    def _claim(self, payload):
        expected = ['workerId']
        if isinstance(payload, dict):
            expected.extend(key for key in ('profiles', 'nodeId', 'expectedJobId') if key in payload)
        self.fields(payload, expected)
        self.slug(payload["workerId"])
        node_id = payload.get('nodeId', 'cluster')
        self._known_node(node_id)
        expected_job = payload.get('expectedJobId')
        if expected_job is not None:
            self.slug(expected_job)
        profiles = payload.get("profiles", [PROFILE])
        if (not isinstance(profiles, list) or not 1 <= len(profiles) <= 3
                or any(not isinstance(p, str) or p not in self.allowed_profiles for p in profiles)
                or len(profiles) != len(set(profiles))):
            raise ValueError("worker must declare supported enabled profiles")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            gate = getattr(self, 'admission_drain', None)
            if gate is not None and gate._closed(db):
                return None
            self._refresh_nodes(db)
            if self._node(db, node_id)['state'] not in ('active', 'scheduled'):
                return None
            if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():
                return None
            row = db.execute("""SELECT * FROM jobs WHERE status='queued'
                ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END, seq LIMIT 1""").fetchone()
            if not row:
                return None
            if row['profile'] == REFERENCE_PROFILE:
                # Reference claims also persist an idempotent private envelope
                # and enforce input-dependent routing in ReferenceStore.claim.
                return None
            if expected_job is not None and row['id'] != expected_job:
                return None
            if row["profile"] not in profiles or row["profile"] not in self.node_profiles(node_id):
                # Do not bypass earlier work or production for a worker that
                # cannot run it. Legacy TreeDar workers never claim photo jobs.
                return None
            token = secrets.token_urlsafe(32)
            db.execute("UPDATE jobs SET status='running',worker_id=?,node_id=?,claim_hash=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                       (payload["workerId"], node_id, hashlib.sha256(token.encode()).hexdigest(), row["id"]))
            job = self.public(db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())
            return {**job, "claimToken": token}

    def complete(self, identifier, payload):
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._complete_in_transaction(db, identifier, payload)

    def _complete_in_transaction(self, db, identifier, payload):
        self.fields(payload, ("claimToken", "status", "result"))
        if (not isinstance(payload["claimToken"], str) or len(payload["claimToken"]) > 128
                or payload["status"] not in ("succeeded", "failed") or not isinstance(payload["result"], dict)):
            raise ValueError("invalid completion")
        result = json.dumps(payload["result"], sort_keys=True, allow_nan=False, separators=(",", ":"))
        if len(result.encode()) > 4096:
            raise ValueError("result exceeds prototype limit")
        if not db.in_transaction:
            raise ValueError('completion requires a transaction')
        row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
        if not row:
            raise Conflict("unknown job")
        supplied_hash = hashlib.sha256(payload["claimToken"].encode()).hexdigest()
        if not row["claim_hash"] or not hmac.compare_digest(supplied_hash, row["claim_hash"]):
            raise Conflict("invalid claim")
        if row["status"] in ("succeeded", "failed"):
            if row["status"] == payload["status"] and row["result"] == result:
                return self.public(row)
            raise Conflict("terminal completion differs")
        if row["status"] != "running" and not (row['status'] == 'held' and row['held_reason'] == 'coordinator_restart'):
            raise Conflict("job is held for operator inspection; completion cannot release it")
        db.execute("UPDATE jobs SET status=?,result=?,held_reason=NULL,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                   (payload["status"], result, identifier))
        return self.public(db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone())

    def operator(self, identifier, action, payload):
        """A separate human/operator credential is required by the HTTP layer.

        Holding never kills a worker; resolution is an explicit attestation after
        the operator independently verifies no old worker/container remains.
        """
        self.fields(payload, ("note", "workerStopped") if action == "resolve" else ("note",))
        if not isinstance(payload["note"], str) or not 1 <= len(payload["note"]) <= 256:
            raise ValueError("operator inspection note required")
        if action == "resolve" and payload["workerStopped"] is not True:
            raise ValueError("verify the worker is stopped before resolution")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            if not row:
                raise Conflict("unknown job")
            if action == "hold" and (row["status"] == "running" or
                                     (row["status"] == "held" and row['held_reason'] == 'coordinator_restart')):
                status = "held"
            elif action == "resolve" and row["status"] == "held":
                status = "failed"
            else:
                raise Conflict("operator transition not allowed")
            result = json.dumps({"environment": row["environment"], "operatorAction": action,
                                 "note": payload["note"], "workerStopped": payload.get("workerStopped", False)})
            db.execute("UPDATE jobs SET status=?,result=?,held_reason=?,updated_at=CURRENT_TIMESTAMP WHERE id=?",
                       (status, result, 'operator' if action == 'hold' else None, identifier))
            return self.public(db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone())
