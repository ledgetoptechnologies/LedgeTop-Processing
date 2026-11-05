"""Internal durable scratch policy. No transfers, engine calls, or file deletion.

Capacity probes return *current free bytes*, already excluding allocated bytes.
Remaining bytes plus retained precharged control holds are subtracted from that
snapshot. Control holds remain conservative after metadata materializes; no TTL
or ordinary terminal settlement releases them. Call record_allocated only
after writes consume disk; it moves bytes from remaining to allocated. Call
begin_transfer before the first input byte: a fence replaces the lease deadline in
the row itself with a marker, so the reservation keeps its whole remaining budget
and can no longer lapse at any clock. Every reader asks lease_lapsed, never the
timestamp, so that same reservation continues. A fenced claim that never reaches
an engine ends through settle_unstarted, never by time. The first fence latches
the config in its own transaction: older allocators refuse this ledger, and one
with no fence stays old-compatible. The latch is one-way: it outlives every fence,
so older images stay refused after fences end and rollback is not automatic.
Production and staging are immutable reservation settings, not interchangeable namespaces.
Confirmed terminal attestation releases only unused reservation budget; allocated
files still occupy the probe's free-space snapshot until an executor removes them.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import hashlib
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import sqlite3
import stat
import time
import secrets
import unicodedata


MAX_INTEGER = 2**53 - 1
ACCOUNTING_VERSION = 1
TRANSFER_FENCE_VERSION = 1
FENCED_LEASE = float(MAX_INTEGER)
CONTROL_SCHEMA='''CREATE TABLE IF NOT EXISTS control_charges (
 reservation TEXT PRIMARY KEY,charge_id TEXT NOT NULL UNIQUE,scope TEXT NOT NULL,
 identity TEXT NOT NULL CHECK(length(CAST(identity AS BLOB))<=4096),
 receipt TEXT NOT NULL CHECK(length(CAST(receipt AS BLOB))<=8192),
 bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>0),
 publication TEXT CHECK(publication IS NULL OR length(CAST(publication AS BLOB))<=16384))'''


class PolicyConflict(ValueError):
    """Identity or state conflict (HTTP adapters may map to 409)."""


class LeaseLapsed(PolicyConflict):
    """The lease ended with no fence: nothing may start, and no bytes were written for it."""


def lease_lapsed(row, now):
    """Whether a reserved row's deadline has passed; a fence's marker is no deadline.

    The marker is compared for identity, never order, so no clock can reach it.
    """
    return row["state"] == "reserved" and row["expires"] != FENCED_LEASE and row["expires"] <= now


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
    @staticmethod
    def _control_publication(value):
        """Closed bounded inert descriptor; concrete guard checks actual files."""
        def closed(item,keys):
            if type(item) is not dict or set(item)!=set(keys):raise PolicyConflict('closed control publication required')
        closed(value,('scope','files','anchorReservation','anchorChargeId'))
        scope=value['scope'];closed(scope,('protocol','parentPolicy','policy','transportPolicy','root','rootIdentity','path','lockPath','pipelineStoragePageCap'))
        _integer(scope['pipelineStoragePageCap'],'control pipeline page cap')
        if scope['protocol']!='ltds-original-claim-recovery-worker-v2':raise PolicyConflict('control publication protocol differs')
        closed(scope['policy'],('per_job_attempts','global_attempts','max_row_bytes','max_serialized_bytes','max_db_pages','preparation_allowance'))
        for key,number in scope['policy'].items():_integer(number,'control publication limit',0 if key=='preparation_allowance' else 1)
        closed(scope['parentPolicy'],('policySha256','nodeId','workerId','workerInstanceId','authorityId','engineId','coordinatorOrigin','gateGeneration'))
        for key,item in scope['parentPolicy'].items():
            if key=='gateGeneration':_integer(item,'control gate generation',1)
            elif type(item) is not str or len(item)>4096 or any(c in item for c in '\0\n\r'):raise PolicyConflict('bounded control parent policy required')
        _sha(scope['transportPolicy']);_sha(scope['parentPolicy']['policySha256'])
        for key in ('root','path','lockPath'):
            item=scope[key]
            if type(item) is not str or len(item)>4096 or not Path(item).is_absolute() or any(c in item for c in '\0\n\r'):raise PolicyConflict('literal control paths required')
        closed(value['files'],(scope['path'],scope['lockPath']))
        for identity in (scope['rootIdentity'],*value['files'].values()):
            if type(identity) is not list or len(identity)!=2:raise PolicyConflict('closed control inode identity required')
            for number in identity:_integer(number,'control inode identity')
        _id(value['anchorReservation']);_id(value['anchorChargeId'])
        serialized=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
        if len(serialized.encode())>16384:raise PolicyConflict('bounded control publication required')
        return serialized,hashlib.sha256(serialized.encode()).hexdigest()

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
                CREATE TABLE IF NOT EXISTS transfer_fences (reservation TEXT PRIMARY KEY, started_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS unstarted_settlements (reservation TEXT PRIMARY KEY, settled_at REAL NOT NULL);
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
                  cleaned REAL, accepted_copy_verified INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(reservation,id));
                CREATE TABLE IF NOT EXISTS receipts (
                  id TEXT PRIMARY KEY, storage_id TEXT NOT NULL UNIQUE, payload TEXT NOT NULL,
                  viewer_receipt TEXT, verified_copy INTEGER NOT NULL DEFAULT 0);
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
            for table, name, declaration in (
                ("objects", "accepted_copy_verified", "INTEGER NOT NULL DEFAULT 0"),
                ("receipts", "viewer_receipt", "TEXT"),
                ("receipts", "verified_copy", "INTEGER NOT NULL DEFAULT 0"),
            ):
                columns = {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}
                if name not in columns:
                    db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
            db.execute(CONTROL_SCHEMA)
            expected=sqlite3.connect(':memory:')
            try:
                expected.execute('PRAGMA synchronous=FULL');expected.execute('BEGIN IMMEDIATE')
                expected.execute(CONTROL_SCHEMA)
                query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='control_charges' ORDER BY type,name"
                if [tuple(row) for row in db.execute(query)]!=[tuple(row) for row in expected.execute(query)]:raise PolicyConflict('typed control schema changed')
            finally:expected.close()
            if db.execute("SELECT job FROM reservations GROUP BY job HAVING COUNT(DISTINCT environment)>1").fetchone():
                raise PolicyConflict("existing job has conflicting environments")
            db.execute("INSERT OR IGNORE INTO job_environments SELECT DISTINCT job,environment FROM reservations")
            config = _json(dict(root=str(self.root), floor=self.floor, grace=self.grace))
            previous = db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
            if previous is None and any(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() is not None for table in
                    ('metadata','job_environments','reservations','results','objects','receipts','pins','cleanup_intents','control_charges','transfer_fences','unstarted_settlements')):
                raise PolicyConflict('existing accounting configuration missing')
            db.execute("INSERT OR IGNORE INTO metadata VALUES('config',?)", (config,))
            self.validate_accounting(db)
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
                self.validate_accounting(db)
                # Persist observations before business validation can roll back.
                # Both phases hold SQLite's writer lock; a newer observation
                # between phases is picked up by _now in the business phase.
                self._expire(db, self._observe_clock(db))
                db.commit()
                db.execute("BEGIN IMMEDIATE")
                self.validate_accounting(db)
            yield db
            self.validate_accounting(db)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def validate_accounting(self,db):
        """Live exact config/receipt invariant, including read-only snapshots.

        No cached version flag: the first control charge latches its marker in
        the same transaction as the receipt, counter and retained capacity hold.
        The first fence latches its own marker the same way, beside either form.
        """
        base=dict(root=str(self.root),floor=self.floor,grace=self.grace)
        config=db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
        count=db.execute('SELECT COUNT(*) FROM control_charges').fetchone()[0]
        if config is None:raise PolicyConflict('accounting configuration missing')
        if db.execute('SELECT 1 FROM transfer_fences LIMIT 1').fetchone():
            base['transferFenceVersion']=TRANSFER_FENCE_VERSION
            if db.execute('SELECT 1 FROM transfer_fences f LEFT JOIN reservations r ON r.id=f.reservation WHERE r.id IS NULL OR r.expires!=? LIMIT 1',(FENCED_LEASE,)).fetchone():
                raise PolicyConflict('fenced lease changed')
        if config[0]==_json(base):
            if count:raise PolicyConflict('control accounting marker missing')
            return base
        versioned=dict(base,accountingVersion=ACCOUNTING_VERSION)
        if config[0]!=_json(versioned):raise PolicyConflict('unknown accounting version or ledger policy')
        if not count:raise PolicyConflict('latched control accounting records missing')
        # SQL checks precede materialization. Each yielded row has fixed bounded
        # fields; qualify all retained allowances before admission trusts SUM.
        oversized=db.execute('''SELECT 1 FROM control_charges WHERE
            typeof(reservation)!='text' OR length(CAST(reservation AS BLOB))>128 OR
            typeof(charge_id)!='text' OR length(CAST(charge_id AS BLOB))>128 OR
            typeof(scope)!='text' OR length(CAST(scope AS BLOB))!=64 OR
            typeof(identity)!='text' OR length(CAST(identity AS BLOB))>4096 OR
            typeof(receipt)!='text' OR length(CAST(receipt AS BLOB))>8192 OR
            (publication IS NOT NULL AND (typeof(publication)!='text' OR length(CAST(publication AS BLOB))>16384)) OR
            typeof(bytes)!='integer' OR bytes<=0 LIMIT 1''').fetchone()
        if oversized:raise PolicyConflict('bounded control accounting records required')
        cursor=db.execute('SELECT * FROM control_charges ORDER BY reservation')
        for row in cursor:
            reservation=self._reservation(db,row['reservation'])
            self._control(row,reservation)
        return versioned

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
        # Neither engine-started, uncertain nor fenced reservations may expire
        # automatically, whatever the clock says.
        db.execute("UPDATE reservations SET state='expired', remaining=0 "
                   "WHERE state='reserved' AND expires<=? AND allocated=0 "
                   "AND id NOT IN (SELECT reservation FROM transfer_fences)", (now,))

    @staticmethod
    def _lapsed(row, now):
        return row["state"] == "expired" or lease_lapsed(row, now)

    def _latch(self, db, **marker):
        # One-way on purpose: no path clears a marker, even once its records end.
        value = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        db.execute("UPDATE metadata SET value=? WHERE key='config'", (_json(dict(value, **marker)),))

    def _fence(self, db, reservation_id, now):
        """Fence, deadline and latch commit together, or none of them do.

        The deadline goes, so an older allocator's live process cannot release
        what it sees as unexpired. The latch stays after this fence ends: older
        images then keep refusing this ledger, which is what prevents rollback to
        them. Removing it would need a separately proven quiescent migration.
        """
        if db.execute("INSERT OR IGNORE INTO transfer_fences VALUES(?,?)", (reservation_id, now)).rowcount:
            db.execute("UPDATE reservations SET expires=? WHERE id=?", (FENCED_LEASE, reservation_id))
            self._latch(db, transferFenceVersion=TRANSFER_FENCE_VERSION)

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
            if now + ttl_seconds >= FENCED_LEASE:
                raise PolicyConflict("lease deadline outside supported range")
            binding = db.execute("SELECT environment FROM job_environments WHERE job=?", (job_id,)).fetchone()
            if binding and binding[0] != environment:
                raise PolicyConflict("job environment is immutable")
            old = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if old:
                if old["settings"] != settings or old["state"] == "expired":
                    raise PolicyConflict("reservation settings changed or lease expired")
                return dict(old)
            outstanding = db.execute("SELECT COALESCE(SUM(remaining),0) FROM reservations").fetchone()[0]
            # A precharged control allowance is not yet physical disk use. Keep
            # its full hold even after materialization: conservative until an
            # explicitly qualified control lifecycle can release it.
            outstanding += db.execute("SELECT COALESCE(SUM(bytes),0) FROM control_charges").fetchone()[0]
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

    def begin_transfer(self, reservation_id):
        """Durably fence input writes before the first byte; idempotent.

        Bytes in flight are not yet allocated, so the fence is what keeps them
        budgeted. An unfenced lease past its deadline refuses before any byte,
        allocated or not, because a fence must never revive a lapsed lease.
        """
        with self._transaction() as db:
            now = self._now(db)
            self._expire(db, now)
            row = self._reservation(db, reservation_id)
            if self._lapsed(row, now):
                raise LeaseLapsed("allocation outside reservation")
            if row["state"] not in ("reserved", "engine_started", "uncertain"):
                raise PolicyConflict("allocation outside reservation")
            self._fence(db, reservation_id, now)

    def claim_engine_lease(self, reservation_id):
        """Before any gate or prepared commit: refuse a lapsed lease with nothing
        committed, else fence a live one so it cannot lapse before engine_started."""
        with self._transaction() as db:
            now = self._now(db)
            self._expire(db, now)
            row = self._reservation(db, reservation_id)
            if self._lapsed(row, now):
                raise LeaseLapsed("lease expired before engine mutation")
            if row["state"] == "reserved":
                self._fence(db, reservation_id, now)

    def settle_unstarted(self, reservation_id, *, transfer_ended=False):
        """Durable end of a claim that never reached an engine; returns the state.

        Every start path sets engine_started before its first engine mutation, so
        reserved proves no engine was told. transfer_ended is the caller's promise
        that no writer remains; until someone can say so the fence keeps the
        budget. "unstarted" means this path ended the row, now or on an earlier
        call. Any other state is left alone and returned, and "terminal" is one of
        them: it is an engine outcome that may still owe a delivery.
        Input bytes on disk stay in the probe's free-space snapshot.
        """
        if transfer_ended is not True:
            raise PolicyConflict("settlement must guarantee no transfer remains")
        with self._transaction() as db:
            now = self._now(db)
            self._expire(db, now)
            row = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            if row is None:
                return None
            if row["state"] == "reserved":
                db.execute("UPDATE reservations SET state='terminal',terminal_at=?,remaining=0 WHERE id=?", (now, reservation_id))
                db.execute("INSERT INTO unstarted_settlements VALUES(?,?)", (reservation_id, now))
                return "unstarted"
            return "unstarted" if self._settled(db, row) else row["state"]

    @staticmethod
    def _settled(db, row):
        return row["state"] == "terminal" and db.execute(
            "SELECT 1 FROM unstarted_settlements WHERE reservation=?", (row["id"],)).fetchone() is not None

    def result_registered(self, reservation_id):
        with self._transaction() as db:
            return db.execute("SELECT 1 FROM results WHERE reservation=?", (reservation_id,)).fetchone() is not None

    def never_started(self, reservation_id):
        """Whether the ledger proves no engine outcome exists for this reservation."""
        with self._transaction() as db:
            row = db.execute("SELECT * FROM reservations WHERE id=?", (reservation_id,)).fetchone()
            return row is None or row["state"] in ("reserved", "expired") or self._settled(db, row)

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

    @staticmethod
    def _control(row,reservation):
        if row is None:return None
        identity=json.loads(row['identity']);receipt=json.loads(row['receipt'])
        if row['identity']!=_json(identity) or row['receipt']!=_json(receipt):raise PolicyConflict('canonical control receipt required')
        fields=('reservationId','chargeId','scopeSha256','parentBindingSha256','allocatedBefore','allowanceBytes','maximumScopeCharges')
        if set(identity)!=set(fields):raise PolicyConflict('control charge identity changed')
        for key in ('allocatedBefore','allowanceBytes','maximumScopeCharges'):_integer(identity[key],key,0 if key=='allocatedBefore' else 1)
        for key in ('scopeSha256','parentBindingSha256'):_sha(identity[key])
        _id(identity['chargeId']);_id(identity['reservationId'])
        expected=dict(schemaVersion=1,protocol='ltds-scratch-control-charge-v1',**identity,
            jobId=reservation['job'],generation=reservation['generation'],environment=reservation['environment'],
            settingsSha256=hashlib.sha256(reservation['settings'].encode()).hexdigest(),
            allocatedAfter=identity['allocatedBefore']+identity['allowanceBytes'])
        _integer(expected['allocatedAfter'],'control target')
        expected['receiptSha256']=hashlib.sha256(_json(expected).encode()).hexdigest()
        if (_json(receipt)!=_json(expected) or row['reservation']!=identity['reservationId'] or row['charge_id']!=identity['chargeId']
                or row['scope']!=identity['scopeSha256'] or row['bytes']!=identity['allowanceBytes']
                or reservation['allocated']<expected['allocatedAfter']):raise PolicyConflict('control charge receipt changed')
        publication=json.loads(row['publication']) if row['publication'] is not None else None
        if publication is not None:
            serialized,sha=ScratchLedger._control_publication(publication)
            if serialized!=row['publication']:raise PolicyConflict('canonical control publication required')
        else:sha=None
        return dict(receipt,publicationSha256=sha,publicationDescriptor=publication)

    def read_control_charge(self,reservation_id):
        """Historical typed receipt only; no clock update or executable grant."""
        _id(reservation_id)
        db=sqlite3.connect(self.path.absolute().as_uri()+'?mode=ro',uri=True)
        db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN');self.validate_accounting(db)
            row=db.execute('SELECT * FROM control_charges WHERE reservation=?',(reservation_id,)).fetchone()
            return self._control(row,self._reservation(db,reservation_id))
        finally:db.close()

    def control_scope_publications(self,scope_sha256,maximum):
        """Bounded historical scope census; no inference from allocated totals."""
        _sha(scope_sha256);_integer(maximum,'control scope cap',1)
        db=sqlite3.connect(self.path.absolute().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN');self.validate_accounting(db)
            count,largest=db.execute('SELECT COUNT(*),COALESCE(MAX(length(CAST(publication AS BLOB))),0) FROM control_charges WHERE scope=?',(scope_sha256,)).fetchone()
            if count>maximum or largest>16384:raise PolicyConflict('control scope cap exceeded')
            values=[row[0] for row in db.execute('SELECT publication FROM control_charges WHERE scope=? AND publication IS NOT NULL LIMIT ?',(scope_sha256,maximum+1))]
            result=[]
            for value in values:
                serialized,sha=self._control_publication(json.loads(value))
                if serialized!=value:raise PolicyConflict('canonical control publication required')
                result.append(sha)
            return result
        finally:db.close()

    def prepare_control_charge(self,reservation_id,*,charge_id,scope_sha256,parent_binding_sha256,expected_allocated,allowance_bytes,maximum_scope_charges):
        """Atomic exact-CAS pre-execution charge, receipt and retained hold.

        Caller must separately own/prove the parent journal before stage issue.
        Exact replay returns history; it never repeats the counter increment.
        """
        _id(reservation_id);_id(charge_id);_sha(scope_sha256);_sha(parent_binding_sha256)
        _integer(expected_allocated,'control baseline');_integer(allowance_bytes,'control allowance',1);_integer(maximum_scope_charges,'scope cap',1)
        target=_integer(expected_allocated+allowance_bytes,'control target')
        identity=dict(reservationId=reservation_id,chargeId=charge_id,scopeSha256=scope_sha256,parentBindingSha256=parent_binding_sha256,
            allocatedBefore=expected_allocated,allowanceBytes=allowance_bytes,maximumScopeCharges=maximum_scope_charges)
        with self._transaction() as db:
            reservation=self._reservation(db,reservation_id);old=db.execute('SELECT * FROM control_charges WHERE reservation=?',(reservation_id,)).fetchone()
            if old is not None:
                if old['identity']!=_json(identity):raise PolicyConflict('control charge replay differs')
                return self._control(old,reservation)
            settings=json.loads(reservation['settings'])
            if (reservation['state']!='reserved' or lease_lapsed(reservation,self._now(db)) or reservation['terminal_at'] is not None
                    or reservation['allocated']!=expected_allocated or expected_allocated!=settings['input_bytes'] or allowance_bytes>reservation['remaining']):raise PolicyConflict('fresh input-only control baseline required')
            if db.execute('SELECT COUNT(*) FROM control_charges WHERE scope=?',(scope_sha256,)).fetchone()[0]>=maximum_scope_charges:raise PolicyConflict('control scope cap reached')
            receipt=dict(schemaVersion=1,protocol='ltds-scratch-control-charge-v1',**identity,jobId=reservation['job'],generation=reservation['generation'],environment=reservation['environment'],
                settingsSha256=hashlib.sha256(reservation['settings'].encode()).hexdigest(),allocatedAfter=target)
            receipt['receiptSha256']=hashlib.sha256(_json(receipt).encode()).hexdigest()
            db.execute('UPDATE reservations SET allocated=?,remaining=remaining-? WHERE id=?',(target,allowance_bytes,reservation_id))
            db.execute('INSERT INTO control_charges VALUES(?,?,?,?,?,?,NULL)',(reservation_id,charge_id,scope_sha256,_json(identity),_json(receipt),allowance_bytes))
            self._latch(db,accountingVersion=ACCOUNTING_VERSION)
            return dict(receipt,publicationSha256=None,publicationDescriptor=None)

    def publish_control_charge(self,reservation_id,*,charge_id,scope_sha256,parent_binding_sha256,publication_descriptor):
        """CAS publication identity; never replaces a published control owner."""
        _id(reservation_id);_id(charge_id)
        for value in (scope_sha256,parent_binding_sha256):_sha(value)
        serialized,publication_sha256=self._control_publication(publication_descriptor)
        if hashlib.sha256(json.dumps(publication_descriptor['scope'],sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()!=scope_sha256:raise PolicyConflict('control publication scope differs')
        with self._transaction() as db:
            reservation=self._reservation(db,reservation_id);row=db.execute('SELECT * FROM control_charges WHERE reservation=?',(reservation_id,)).fetchone()
            receipt=self._control(row,reservation)
            if receipt is None or any(receipt[key]!=value for key,value in (('chargeId',charge_id),('scopeSha256',scope_sha256),('parentBindingSha256',parent_binding_sha256))):raise PolicyConflict('exact control receipt required')
            if receipt['publicationSha256'] is not None:
                if receipt['publicationSha256']!=publication_sha256 or row['publication']!=serialized:raise PolicyConflict('control publication differs')
                return receipt
            if reservation['state']!='reserved' or lease_lapsed(reservation,self._now(db)):raise PolicyConflict('pre-execution control publication required')
            db.execute('UPDATE control_charges SET publication=? WHERE reservation=? AND publication IS NULL',(serialized,reservation_id))
            return dict(receipt,publicationSha256=publication_sha256,publicationDescriptor=json.loads(serialized))

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
            if lease_lapsed(row, now):
                raise PolicyConflict("lease expired before engine mutation")
            if row["state"] == "expired" or (row["state"] == "terminal" and state != "terminal"):
                raise PolicyConflict("invalid engine transition")
            db.execute("UPDATE reservations SET state=?,terminal_at=CASE WHEN ?='terminal' "
                       "THEN COALESCE(terminal_at,?) ELSE terminal_at END, "
                       "remaining=CASE WHEN ?='terminal' THEN 0 ELSE remaining END WHERE id=?",
                       (state, state, now, state, reservation_id))

    def prepare_job_directory(self, reservation_id):
        """Create each owned directory privately, independently of process umask.

        pathlib's parents=True applies its mode only to the last component.
        Never repair an existing directory's permissions or follow its links.
        """
        self._owned_path(reservation_id, 'jobs/' + reservation_id + '/.directory-check')
        with self._transaction() as db:
            self._reservation(db, reservation_id)
        if os.name == 'nt':
            directory = self.root
            for component in ('jobs', reservation_id):
                directory = directory / component
                directory.mkdir(mode=0o700, exist_ok=True)
                if directory.is_symlink() or not directory.is_dir():
                    raise PolicyConflict('owned scratch directory required')
            return directory
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for component in (None, 'jobs', reservation_id):
                if component is not None:
                    try:
                        os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    else:
                        os.fsync(descriptor)
                    child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                    dir_fd=descriptor)
                    previous = descriptor
                    descriptor = child
                    os.close(previous)
                observed = os.fstat(descriptor)
                if (not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.geteuid()
                        or observed.st_mode & 0o022):
                    raise PolicyConflict('scratch directory must be worker-owned without other writers')
            return self.root / 'jobs' / reservation_id
        finally:
            os.close(descriptor)

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
                db.execute("INSERT INTO objects(reservation,id,bytes,sha,path,accepted,cleaned) VALUES(?,?,?,?,?,NULL,NULL)",
                    (reservation_id, obj["object_id"], obj["bytes"], obj["sha256"], obj["relative_path"]))

    def accept_receipt(self, reservation_id, *, receipt_id, storage_receipt_id,
                       job_id, generation, manifest_sha256, pipeline_sha256,
                       result_manifest_sha256, accepted_object_ids,
                       viewer_receipt=None, verified_copy=False):
        if type(verified_copy) is not bool or (verified_copy != (viewer_receipt is not None)):
            raise PolicyConflict("signed Viewer copy proof and verification state must be paired")
        if viewer_receipt is not None:
            if type(viewer_receipt) is not dict:
                raise PolicyConflict("signed Viewer copy proof required")
            viewer_receipt = _json(viewer_receipt)
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
            old = db.execute("SELECT payload,viewer_receipt,verified_copy FROM receipts WHERE id=?", (receipt_id,)).fetchone()
            if old:
                if old[0] != payload:
                    raise PolicyConflict("receipt changed")
                if old[2] and viewer_receipt is not None and old[1] != viewer_receipt:
                    raise PolicyConflict("signed Viewer receipt changed")
                if not old[2] and verified_copy:
                    db.execute("UPDATE receipts SET viewer_receipt=?,verified_copy=1 WHERE id=?",
                        (viewer_receipt, receipt_id))
                    for object_id in ids:
                        db.execute("UPDATE objects SET accepted_copy_verified=1 WHERE reservation=? AND id=?",
                            (reservation_id, object_id))
                return
            storage_old = db.execute("SELECT id FROM receipts WHERE storage_id=?", (storage_receipt_id,)).fetchone()
            if storage_old:
                raise PolicyConflict("storage receipt already bound")
            db.execute("INSERT INTO receipts(id,storage_id,payload,viewer_receipt,verified_copy) VALUES(?,?,?,?,?)",
                (receipt_id, storage_receipt_id, payload, viewer_receipt, int(verified_copy)))
            for object_id in ids:
                db.execute("UPDATE objects SET accepted=COALESCE(accepted,?),accepted_copy_verified=MAX(accepted_copy_verified,?) WHERE reservation=? AND id=?",
                           (self._now(db), int(verified_copy), reservation_id, object_id))

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
            AND o.accepted_copy_verified=1
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
