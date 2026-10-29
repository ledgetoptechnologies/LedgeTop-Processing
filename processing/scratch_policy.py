"""Internal durable scratch policy. No transfers, engine calls, or file deletion.

Capacity probes return *current free bytes*, already excluding allocated bytes.
Remaining bytes plus retained precharged control holds are subtracted from that
snapshot. Control holds remain conservative after metadata materializes; no TTL
or ordinary terminal settlement releases them. Call record_allocated only
after writes consume disk; it moves bytes from remaining to allocated. Production
and staging are immutable reservation settings, not interchangeable namespaces.
Confirmed terminal attestation releases only unused reservation budget; allocated
files still occupy the probe's free-space snapshot until an executor removes them.

Opt-in input-copy accounting (input_copy_accounting=True plus an explicit,
irreversible enable_input_copy_accounting()) registers fetched processing-input
copies separately from result objects. Release is a low-level primitive: it
needs a configured InputReleaseAuthority that independently resolves the compact
release record from concrete durable stores. Without one, nothing is released.
The latched marker and its schema footprint fence older and default-off writers
before any business mutation; arbitrary privileged DB edits are out of scope.
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
import time
import secrets
import unicodedata


MAX_INTEGER = 2**53 - 1
ACCOUNTING_VERSION = 1
INPUT_COPY_ACCOUNTING_VERSION = 1
MAX_INPUT_COPIES = 10000  # Matches the paired v1 manifest object ceiling.
MAX_RELEASE_RECORD_BYTES = 16384  # Compact: independent of closure size.
RELEASE_FIELDS = ('schemaVersion', 'purpose', 'scope', 'releaseId', 'authorityId', 'reservationId', 'jobId',
                  'generation', 'environment', 'inputManifestSha256', 'pipelineDigest', 'inputsSha256', 'outcome',
                  'outcomeSha256', 'terminalReceiptId', 'sourceCustodyReceiptId', 'sourceStorageReceiptId',
                  'pipelineClosureSha256')
INPUT_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS input_accounting (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
    "version INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS input_bindings (reservation TEXT PRIMARY KEY, identity TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS input_copies (reservation TEXT NOT NULL, id TEXT NOT NULL, "
    "bytes INTEGER NOT NULL, sha TEXT NOT NULL, path TEXT NOT NULL UNIQUE, path_key TEXT NOT NULL UNIQUE, "
    "fetched REAL, cleaned REAL, PRIMARY KEY(reservation,id))",
    "CREATE TABLE IF NOT EXISTS input_releases (reservation TEXT PRIMARY KEY, record TEXT NOT NULL, "
    "record_sha TEXT NOT NULL, authority TEXT NOT NULL, released REAL NOT NULL)",
    "CREATE TABLE IF NOT EXISTS input_pins (reservation TEXT NOT NULL, object TEXT NOT NULL, "
    "reference TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY(reservation,object,reference))",
    "CREATE TABLE IF NOT EXISTS input_cleanup_intents (reservation TEXT NOT NULL, object TEXT NOT NULL, "
    "token TEXT NOT NULL UNIQUE, expires REAL NOT NULL, state TEXT NOT NULL, PRIMARY KEY(reservation,object))",
)
INPUT_TABLES = ('input_accounting', 'input_bindings', 'input_copies', 'input_releases', 'input_pins',
                'input_cleanup_intents')
CONTROL_SCHEMA='''CREATE TABLE IF NOT EXISTS control_charges (
 reservation TEXT PRIMARY KEY,charge_id TEXT NOT NULL UNIQUE,scope TEXT NOT NULL,
 identity TEXT NOT NULL CHECK(length(CAST(identity AS BLOB))<=4096),
 receipt TEXT NOT NULL CHECK(length(CAST(receipt AS BLOB))<=8192),
 bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes>0),
 publication TEXT CHECK(publication IS NULL OR length(CAST(publication AS BLOB))<=16384))'''


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


def _path_key(path):
    """Portable Windows case/Unicode identity of an owned relative path."""
    return unicodedata.normalize("NFC", path).casefold()


class InputReleaseAuthority:
    """Trusted composition seam for releasing worker input copies.

    An implementation must resolve every referenced record itself from concrete
    durable stores and the authenticated issuer: positive retained-original-source
    custody covering every original object until its release ACK, the durable
    terminal outcome and its exact acceptance, and whole-parent pipeline finality.
    It returns the exact canonical record, or raises. Caller JSON, digests or
    booleans are never authority. No implementation ships with the ledger; without
    one, every input release is refused.
    """

    authority_id = None

    def resolve_input_release(self, record):
        raise NotImplementedError


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
                 safety_floor_bytes=1024**3, cleanup_grace_seconds=3600, input_copy_accounting=False,
                 release_authority=None):
        if type(input_copy_accounting) is not bool:
            raise ValueError("explicit input copy accounting profile required")
        if release_authority is not None and (not isinstance(release_authority, InputReleaseAuthority)
                or not input_copy_accounting):
            raise ValueError("input release authority requires the opt-in profile")
        if release_authority is not None:
            _id(release_authority.authority_id)
        self.input_copy_accounting = input_copy_accounting
        self.release_authority = release_authority
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
            present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if previous is None and any(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() is not None for table in
                    ('metadata','job_environments','reservations','results','objects','receipts','pins','cleanup_intents','control_charges')
                    + tuple(table for table in INPUT_TABLES if table in present)):
                raise PolicyConflict('existing accounting configuration missing')
            db.execute("INSERT OR IGNORE INTO metadata VALUES('config',?)", (config,))
            if self.validate_accounting(db).get('inputCopyAccountingVersion'):
                self._check_input_schema(db)
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
        """
        config=db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()
        count=db.execute('SELECT COUNT(*) FROM control_charges').fetchone()[0]
        if config is None:raise PolicyConflict('accounting configuration missing')
        shapes={_json(self._config(control=control,inputs=inputs)):(control,inputs) for control in (False,True) for inputs in (False,True)}
        if config[0] not in shapes:raise PolicyConflict('unknown accounting version or ledger policy')
        control,inputs=shapes[config[0]]
        footprint=db.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN (%s)"
            %",".join("'"+table+"'" for table in INPUT_TABLES)).fetchone()[0]
        if footprint and not inputs:raise PolicyConflict('input copy records exist without their latched marker')
        if inputs and (footprint!=len(INPUT_TABLES) or [tuple(row) for row in db.execute('SELECT singleton,version FROM input_accounting')]!=[(1,INPUT_COPY_ACCOUNTING_VERSION)]):
            raise PolicyConflict('latched input copy marker lacks its schema footprint')
        if inputs and not self.input_copy_accounting:raise PolicyConflict('latched input copy accounting requires its opt-in profile')
        versioned=self._config(control=control,inputs=inputs)
        if not control:
            if count:raise PolicyConflict('control accounting marker missing')
            return versioned
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

    def _config(self, *, control, inputs):
        config = dict(root=str(self.root), floor=self.floor, grace=self.grace)
        if control:
            config["accountingVersion"] = ACCOUNTING_VERSION
        if inputs:
            config["inputCopyAccountingVersion"] = INPUT_COPY_ACCOUNTING_VERSION
        return config

    def _latch(self, db, *, control=False, inputs=False):
        """Write the config with every already latched marker preserved."""
        # Read the stored markers directly: a control latch runs after its first
        # charge row exists, before the marker would validate.
        current = json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        config = self._config(control=control or "accountingVersion" in current,
                              inputs=inputs or "inputCopyAccountingVersion" in current)
        db.execute("UPDATE metadata SET value=? WHERE key='config'", (_json(config),))

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
            if (reservation['state']!='reserved' or reservation['expires']<=self._now(db) or reservation['terminal_at'] is not None
                    or reservation['allocated']!=expected_allocated or expected_allocated!=settings['input_bytes'] or allowance_bytes>reservation['remaining']):raise PolicyConflict('fresh input-only control baseline required')
            if db.execute('SELECT COUNT(*) FROM control_charges WHERE scope=?',(scope_sha256,)).fetchone()[0]>=maximum_scope_charges:raise PolicyConflict('control scope cap reached')
            receipt=dict(schemaVersion=1,protocol='ltds-scratch-control-charge-v1',**identity,jobId=reservation['job'],generation=reservation['generation'],environment=reservation['environment'],
                settingsSha256=hashlib.sha256(reservation['settings'].encode()).hexdigest(),allocatedAfter=target)
            receipt['receiptSha256']=hashlib.sha256(_json(receipt).encode()).hexdigest()
            db.execute('UPDATE reservations SET allocated=?,remaining=remaining-? WHERE id=?',(target,allowance_bytes,reservation_id))
            db.execute('INSERT INTO control_charges VALUES(?,?,?,?,?,?,NULL)',(reservation_id,charge_id,scope_sha256,_json(identity),_json(receipt),allowance_bytes))
            self._latch(db,control=True)
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
            if reservation['state']!='reserved' or reservation['expires']<=self._now(db):raise PolicyConflict('pre-execution control publication required')
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
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='input_copies'").fetchone():
            if any(db.execute("SELECT 1 FROM input_copies WHERE path_key=?", (_path_key(obj["relative_path"]),)).fetchone()
                   for obj in normalized):
                raise PolicyConflict("output path aliases an input copy")
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

    # ----- Opt-in processing-input copies -------------------------------------
    # Separate tables, pins, intents and executor namespace: result object APIs,
    # IDs and receipts are unchanged and never stand in for input copies.

    def _check_input_schema(self, db):
        expected = sqlite3.connect(':memory:')
        try:
            for statement in INPUT_SCHEMA:
                expected.execute(statement)
            query = ("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN (%s) ORDER BY type,name"
                     % ",".join("'" + table + "'" for table in INPUT_TABLES))
            if [tuple(row) for row in db.execute(query)] != [tuple(row) for row in expected.execute(query)]:
                raise PolicyConflict('input copy schema changed')
        finally:
            expected.close()

    def _require_inputs(self, db):
        if not self.validate_accounting(db).get("inputCopyAccountingVersion"):
            raise PolicyConflict("input copy accounting is not enabled")

    def enable_input_copy_accounting(self):
        """Explicitly and irreversibly latch input-copy accounting (idempotent)."""
        if not self.input_copy_accounting:
            raise PolicyConflict("input copy accounting requires its opt-in profile")
        with self._transaction() as db:
            for statement in INPUT_SCHEMA:
                db.execute(statement)
            db.execute("INSERT OR IGNORE INTO input_accounting VALUES(1,?)", (INPUT_COPY_ACCOUNTING_VERSION,))
            self._check_input_schema(db)
            self._latch(db, inputs=True)

    def _check_input_aliases(self, db, reservation_id, normalized):
        """Portable case/Unicode identity via the indexed path_key, plus single-link
        identity for any file already present; bounded per registration."""
        keys = [_path_key(obj["relative_path"]) for obj in normalized]
        if len(set(keys)) != len(keys):
            raise PolicyConflict("input path aliases another object")
        if any(db.execute("SELECT 1 FROM input_copies WHERE path_key=?", (key,)).fetchone() for key in keys):
            raise PolicyConflict("input path aliases another object")
        # Owned result paths sit under their own (ASCII) reservation directory.
        results = {_path_key(row[0]) for row in db.execute(
            "SELECT path FROM objects WHERE lower(reservation)=lower(?)", (reservation_id,))}
        if results & set(keys):
            raise PolicyConflict("input path aliases another object")
        for obj in normalized:
            candidate = self.root / obj["relative_path"]
            if candidate.exists():
                if not candidate.is_file():
                    raise ValueError("input path is not a regular file")
                if candidate.stat().st_nlink != 1:
                    raise PolicyConflict("input path aliases another object")

    def _closure_sha256(self, normalized):
        return hashlib.sha256(_json([dict(object_id=o["object_id"], byte_size=o["byte_size"], sha256=o["sha256"],
            relative_path=o["relative_path"]) for o in sorted(normalized, key=lambda o: o["object_id"])]).encode()).hexdigest()

    def register_input_binding(self, reservation_id, *, job_id, generation, environment, manifest_sha256,
                               pipeline_sha256, objects):
        """Bind the full fetched-input closure to its original reservation before any download.

        objects: list of {object_id, byte_size, sha256, relative_path}. Exact replay is
        idempotent at any later state; a new binding needs a live reservation.
        """
        _id(reservation_id); _id(job_id); _integer(generation, "generation", 1)
        _sha(manifest_sha256); _sha(pipeline_sha256)
        if environment not in ("production", "staging"):
            raise ValueError("invalid environment")
        if type(objects) is not list or not 0 < len(objects) <= MAX_INPUT_COPIES:
            raise ValueError("bounded non-empty input closure required")
        normalized = []
        for obj in objects:
            if type(obj) is not dict or set(obj) != {"object_id", "byte_size", "sha256", "relative_path"}:
                raise ValueError("closed input copy descriptor required")
            normalized.append(dict(object_id=_id(obj["object_id"]), byte_size=_integer(obj["byte_size"], "input copy bytes"),
                sha256=_sha(obj["sha256"]), relative_path=self._owned_path(reservation_id, obj["relative_path"])))
        normalized.sort(key=lambda o: o["object_id"])
        if len({o["object_id"] for o in normalized}) != len(normalized):
            raise ValueError("duplicate input copy ID")
        identity = _json(dict(jobId=job_id, generation=generation, environment=environment,
            inputManifestSha256=manifest_sha256, pipelineDigest=pipeline_sha256,
            inputsSha256=self._closure_sha256(normalized)))
        with self._transaction() as db:
            self._require_inputs(db)
            row = self._reservation(db, reservation_id)
            old = db.execute("SELECT identity FROM input_bindings WHERE reservation=?", (reservation_id,)).fetchone()
            if old:
                if old[0] != identity:
                    raise PolicyConflict("input binding changed")
                return
            settings = json.loads(row["settings"])
            if row["state"] not in ("reserved", "engine_started", "uncertain") or (
                    row["state"] == "reserved" and row["expires"] <= self._now(db)):
                raise PolicyConflict("input binding requires a live reservation")
            if ((row["job"], row["generation"], row["environment"], settings["manifest_sha256"])
                    != (job_id, generation, environment, manifest_sha256)):
                raise PolicyConflict("input binding does not match the original reservation")
            if sum(o["byte_size"] for o in normalized) != settings["input_bytes"]:
                raise PolicyConflict("input closure bytes differ from the reservation")
            self._check_input_aliases(db, reservation_id, normalized)
            db.execute("INSERT INTO input_bindings VALUES(?,?)", (reservation_id, identity))
            db.executemany("INSERT INTO input_copies VALUES(?,?,?,?,?,?,NULL,NULL)",
                [(reservation_id, o["object_id"], o["byte_size"], o["sha256"], o["relative_path"],
                  _path_key(o["relative_path"])) for o in normalized])

    def confirm_input_copy(self, reservation_id, object_id, *, byte_size, sha256):
        """Record that the caller verified this exact copy on disk; idempotent.

        Only confirmed copies can become cleanup candidates: missing or partial
        fetches never authorize deletion.
        """
        _id(object_id); _integer(byte_size, "input copy bytes"); _sha(sha256)
        with self._transaction() as db:
            self._require_inputs(db)
            row = self._reservation(db, reservation_id)
            copy = db.execute("SELECT bytes,sha,fetched,cleaned FROM input_copies WHERE reservation=? AND id=?",
                              (reservation_id, object_id)).fetchone()
            if copy is None or (copy["bytes"], copy["sha"]) != (byte_size, sha256):
                raise PolicyConflict("unknown or changed input copy")
            if copy["fetched"] is not None:
                return
            if copy["cleaned"] is not None or row["state"] not in ("reserved", "engine_started", "uncertain"):
                raise PolicyConflict("input copy confirmation requires a live reservation")
            db.execute("UPDATE input_copies SET fetched=? WHERE reservation=? AND id=?",
                       (self._now(db), reservation_id, object_id))

    def _release_identity(self, db, reservation_id, record):
        """Closed compact record bound to this ledger's own binding and rows.

        Necessary, never sufficient: the configured authority must still resolve it.
        """
        if type(record) is not dict or set(record) != set(RELEASE_FIELDS):
            raise PolicyConflict("closed input release record required")
        for key, value in (("schemaVersion", 1), ("purpose", "input-copy-release"),
                           ("scope", "worker_fetched_inputs_only")):
            if type(record[key]) is not type(value) or record[key] != value:
                raise PolicyConflict("unsupported input release record")
        try:
            for key in ("releaseId", "authorityId", "reservationId", "jobId", "terminalReceiptId",
                        "sourceCustodyReceiptId", "sourceStorageReceiptId"):
                _id(record[key])
            for key in ("inputManifestSha256", "pipelineDigest", "inputsSha256", "outcomeSha256", "pipelineClosureSha256"):
                _sha(record[key])
            _integer(record["generation"], "generation", 1)
        except ValueError:
            raise PolicyConflict("invalid input release record field") from None
        roles = [record[key] for key in ("releaseId", "terminalReceiptId", "sourceCustodyReceiptId", "sourceStorageReceiptId")]
        if len(set(roles)) != len(roles):
            raise PolicyConflict("input release receipt roles must be distinct")
        if record["outcome"] not in ("succeeded", "failed", "cancelled"):
            raise PolicyConflict("unsupported terminal outcome")
        stored = db.execute("SELECT identity FROM input_bindings WHERE reservation=?", (reservation_id,)).fetchone()
        if stored is None:
            raise PolicyConflict("unknown input binding")
        binding = json.loads(stored[0])
        row = self._reservation(db, reservation_id)
        if (record["reservationId"], record["jobId"], record["generation"], record["environment"],
                record["inputManifestSha256"], record["pipelineDigest"], record["inputsSha256"]) != (
                reservation_id, binding["jobId"], binding["generation"], binding["environment"],
                binding["inputManifestSha256"], binding["pipelineDigest"], binding["inputsSha256"]):
            raise PolicyConflict("input release record differs from the original binding")
        if row["state"] != "terminal" or (row["job"], row["generation"]) != (binding["jobId"], binding["generation"]):
            raise PolicyConflict("input release requires the original confirmed terminal reservation")
        source_roles = (record["sourceCustodyReceiptId"], record["sourceStorageReceiptId"], record["releaseId"])
        if any(db.execute("SELECT 1 FROM receipts WHERE id=? OR storage_id=?", (value, value)).fetchone()
               for value in source_roles):
            raise PolicyConflict("input release reuses a result receipt role")
        if record["outcome"] == "succeeded":
            result = db.execute("SELECT identity FROM results WHERE reservation=?", (reservation_id,)).fetchone()
            if result is None or json.loads(result[0])["result_manifest_sha256"] != record["outcomeSha256"]:
                raise PolicyConflict("succeeded release must name the registered result")
            if db.execute("SELECT 1 FROM objects WHERE reservation=? AND accepted IS NULL", (reservation_id,)).fetchone():
                raise PolicyConflict("full durable Viewer acceptance required")
            receipt = db.execute("SELECT payload FROM receipts WHERE id=?", (record["terminalReceiptId"],)).fetchone()
            if receipt is None or json.loads(receipt[0])["reservation_id"] != reservation_id:
                raise PolicyConflict("succeeded release must name a durable Viewer receipt")
        return binding

    def _resolve(self, record):
        authority = self.release_authority
        if authority is None:
            raise PolicyConflict("no input release authority is configured")
        if record["authorityId"] != authority.authority_id:
            raise PolicyConflict("input release names a different authority")
        try:
            resolved = authority.resolve_input_release(json.loads(_json(record)))
        except Exception:
            raise PolicyConflict("input release authority did not confirm the record") from None
        try:
            if _json(resolved) != _json(record):
                raise PolicyConflict("input release authority resolved a different record")
        except (TypeError, ValueError):
            raise PolicyConflict("input release authority resolved a different record") from None

    def release_input_copies(self, reservation_id, *, record):
        """Store an authority-resolved compact release record; returns copy count.

        record fields: RELEASE_FIELDS. The ledger checks it against its own binding,
        reservation, results and receipts, then requires the configured authority to
        resolve the identical record. Terminal metadata acknowledgement alone is
        never deletion authority. No file is deleted; pins and grace still apply.
        """
        _id(reservation_id)
        try:
            encoded = _json(record)
        except (TypeError, ValueError):
            raise PolicyConflict("canonical input release record required") from None
        if len(encoded.encode()) > MAX_RELEASE_RECORD_BYTES:
            raise PolicyConflict("bounded input release record required")
        with self._transaction() as db:
            self._require_inputs(db)
            old = db.execute("SELECT record FROM input_releases WHERE reservation=?", (reservation_id,)).fetchone()
            if old and old[0] != encoded:
                raise PolicyConflict("input release record changed")
            self._release_identity(db, reservation_id, record)
            self._resolve(record)
            if not old:
                db.execute("INSERT INTO input_releases VALUES(?,?,?,?,?)", (reservation_id, encoded,
                    hashlib.sha256(encoded.encode()).hexdigest(), record["authorityId"], self._now(db)))
            return db.execute("SELECT COUNT(*) FROM input_copies WHERE reservation=?", (reservation_id,)).fetchone()[0]

    def _stored_release(self, db, reservation_id):
        """Re-check the bounded canonical stored record against its digest, original
        identity and referenced rows; None retains every copy."""
        row = db.execute("SELECT record,record_sha,authority FROM input_releases WHERE reservation=?",
                         (reservation_id,)).fetchone()
        if row is None or type(row[0]) is not str or len(row[0].encode()) > MAX_RELEASE_RECORD_BYTES:
            return None
        try:
            record = json.loads(row[0])
            if (_json(record) != row[0] or hashlib.sha256(row[0].encode()).hexdigest() != row[1]
                    or record.get("authorityId") != row[2]):
                return None
            self._release_identity(db, reservation_id, record)
        except (PolicyConflict, ValueError, TypeError, KeyError, AttributeError):
            return None
        return record

    def set_input_pin(self, reservation_id, object_id, reference, *, kind, state):
        """read: readers; worker: active execution; downstream: dependent stages."""
        _id(reference)
        if kind not in ("read", "worker", "downstream") or state not in ("active", "uncertain", "released"):
            raise ValueError("invalid pin")
        with self._transaction() as db:
            self._require_inputs(db)
            copy = db.execute("SELECT cleaned FROM input_copies WHERE reservation=? AND id=?",
                              (reservation_id, object_id)).fetchone()
            if copy is None or copy[0] is not None:
                raise PolicyConflict("unknown or cleaned input copy")
            if db.execute("SELECT 1 FROM input_cleanup_intents WHERE reservation=? AND object=?",
                          (reservation_id, object_id)).fetchone():
                raise PolicyConflict("input copy is fenced for cleanup")
            old = db.execute("SELECT kind FROM input_pins WHERE reservation=? AND object=? AND reference=?",
                             (reservation_id, object_id, reference)).fetchone()
            if old and old[0] != kind:
                raise PolicyConflict("pin kind changed")
            db.execute("INSERT INTO input_pins VALUES(?,?,?,?,?) ON CONFLICT(reservation,object,reference) "
                       "DO UPDATE SET state=excluded.state", (reservation_id, object_id, reference, kind, state))

    def _input_eligible(self, db, now, reservation_id=None, object_id=None):
        """Yield eligible copies whose stored release record still validates."""
        exact = "" if reservation_id is None else " AND c.reservation=? AND c.id=?"
        rows = db.execute("""SELECT c.reservation,c.id,c.bytes,c.sha,c.path,r.generation,x.released
            FROM input_copies c JOIN reservations r ON r.id=c.reservation
            JOIN input_releases x ON x.reservation=c.reservation
            WHERE r.state='terminal' AND c.fetched IS NOT NULL AND c.cleaned IS NULL
            AND MAX(x.released,r.terminal_at)<=?
            AND NOT EXISTS(SELECT 1 FROM input_cleanup_intents i WHERE i.reservation=c.reservation AND i.object=c.id)
            AND NOT EXISTS(SELECT 1 FROM input_pins p WHERE p.reservation=c.reservation
              AND p.object=c.id AND p.state!='released')""" + exact + " ORDER BY x.released,c.reservation,c.id",
            (now - self.grace,) + (() if reservation_id is None else (reservation_id, object_id)))
        valid = {}
        for row in rows:
            if row["reservation"] not in valid:
                valid[row["reservation"]] = self._stored_release(db, row["reservation"])
            if valid[row["reservation"]] is not None:
                yield row

    def input_cleanup_candidates(self, *, max_count=100, max_bytes=1024**3):
        """Oldest releases first; count and byte limits are hard. A copy larger
        than max_bytes is deferred, never admitted: see oversized_input_copies."""
        _integer(max_count, "max count", 1); _integer(max_bytes, "max bytes")
        planned, total = [], 0
        with self._transaction() as db:
            self._require_inputs(db)
            for row in self._input_eligible(db, self._now(db)):
                if len(planned) >= max_count:
                    break
                if total + row["bytes"] > max_bytes:
                    continue
                self._owned_path(row["reservation"], row["path"])
                planned.append(dict(row))
                total += row["bytes"]
        return planned

    def oversized_input_copies(self, *, max_bytes, max_count=100):
        """Report otherwise eligible copies that no batch under max_bytes can admit.

        They stay retained until a reviewed private object-size policy covers them.
        """
        _integer(max_count, "max count", 1); _integer(max_bytes, "max bytes")
        found = []
        with self._transaction() as db:
            self._require_inputs(db)
            for row in self._input_eligible(db, self._now(db)):
                if row["bytes"] > max_bytes:
                    found.append(dict(row))
                    if len(found) >= max_count:
                        break
        return found

    def begin_input_cleanup(self, reservation_id, object_id, *, generation, sha256, lease_seconds=60):
        """Atomically fence one released input copy after the authority freshly
        re-resolves its stored record; never deletes, never expires the fence."""
        _integer(generation, "generation", 1); _sha(sha256)
        _integer(lease_seconds, "cleanup lease", 1)
        with self._transaction() as db:
            self._require_inputs(db)
            now = self._now(db)
            row = next(self._input_eligible(db, now, reservation_id, object_id), None)
            if row is None or row["generation"] != generation or row["sha"] != sha256:
                raise PolicyConflict("input cleanup candidate changed or is no longer eligible")
            self._resolve(self._stored_release(db, reservation_id))
            self._owned_path(reservation_id, row["path"])
            token = secrets.token_hex(32)
            expires = now + lease_seconds
            db.execute("INSERT INTO input_cleanup_intents VALUES(?,?,?,?,'cleanup_pending')",
                       (reservation_id, object_id, token, expires))
            return dict(row) | dict(intent_token=token, lease_expires=expires, state="cleanup_pending")

    def input_cleanup_intent(self, reservation_id, object_id):
        """Recover a persisted input fence; expired leases report uncertainty and retain it."""
        with self._transaction() as db:
            self._require_inputs(db)
            now = self._now(db)
            row = db.execute("""SELECT i.*,i.token AS intent_token,i.expires AS lease_expires,
                             c.path,c.sha,c.bytes,r.generation FROM input_cleanup_intents i
                             JOIN input_copies c ON c.reservation=i.reservation AND c.id=i.object
                             JOIN reservations r ON r.id=i.reservation
                             WHERE i.reservation=? AND i.object=?""",
                             (reservation_id, object_id)).fetchone()
            if row is None:
                raise PolicyConflict("unknown input cleanup intent")
            if row["state"] == "cleanup_pending" and row["expires"] <= now:
                db.execute("UPDATE input_cleanup_intents SET state='uncertain' WHERE reservation=? AND object=?",
                           (reservation_id, object_id))
                return dict(row) | dict(state="uncertain")
            return dict(row)

    def mark_input_cleaned(self, reservation_id, object_id, *, generation, sha256,
                           intent_token, executor_attested):
        """Confirm the exact fenced input intent after executor-verified unlink."""
        _integer(generation, "generation", 1); _sha(sha256)
        if executor_attested is not True:
            raise PolicyConflict("cleanup executor attestation required")
        with self._transaction() as db:
            self._require_inputs(db)
            row = db.execute("SELECT c.*,r.generation FROM input_copies c JOIN reservations r "
                "ON r.id=c.reservation WHERE c.reservation=? AND c.id=?", (reservation_id, object_id)).fetchone()
            if row is None or row["generation"] != generation or row["sha"] != sha256:
                raise PolicyConflict("input cleanup identity changed")
            intent = db.execute("SELECT * FROM input_cleanup_intents WHERE reservation=? AND object=?",
                                (reservation_id, object_id)).fetchone()
            if intent is None or not isinstance(intent_token, str) or not secrets.compare_digest(intent["token"], intent_token):
                raise PolicyConflict("input cleanup intent token does not match")
            if row["cleaned"] is not None:
                return
            self._owned_path(reservation_id, row["path"])
            db.execute("UPDATE input_copies SET cleaned=? WHERE reservation=? AND id=?",
                       (self._now(db), reservation_id, object_id))
            db.execute("UPDATE input_cleanup_intents SET state='cleaned' WHERE reservation=? AND object=?",
                       (reservation_id, object_id))
