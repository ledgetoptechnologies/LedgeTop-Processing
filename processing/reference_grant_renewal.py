"""Candidate renewable transfer contract and opt-in durable journal.

No routes, issuer authority, scratch lease extension or driver activation. The
caller owns/authenticates the original job, source custody and SQLite connection.
The journal conservatively charges a whole object BEFORE each attempted GET;
unknown attempts never return credits. Existing v1 grant validators are unchanged.
"""
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import hashlib
import json
import sqlite3

from . import reference_protocol as p
from .reference_transfer import validate_grant

PROFILE = 'ltds-renewable-grants-v1'
MAX_EPOCHS = 32
MAX_REQUEST_BYTES = 4096
MAX_GRANT_BYTES = 2 * 1024**2


class RenewalHeld(ValueError):
    """Secret-free refusal; never serialize private grants in errors."""


def require(value, reason='renewal_contract_held'):
    if not value:
        raise RenewalHeld(reason)


def closed(value, keys):
    require(type(value) is dict and set(value) == set(keys))


def integer(value, minimum=1, maximum=p.MAX_SAFE_INTEGER):
    require(type(value) is int and minimum <= value <= maximum)
    return value


def encoded(value):
    try:
        return p.canonical_json(value)
    except Exception:
        raise RenewalHeld('renewal_json_held') from None


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def milliseconds(value):
    require(type(value) is str)
    try:
        date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        result = round(date.timestamp() * 1000)
        require(date.isoformat(timespec='milliseconds').replace('+00:00', 'Z') == value)
        return integer(result)
    except (ValueError, OverflowError, OSError):
        raise RenewalHeld('renewal_timestamp_held') from None


def decode(raw):
    """Bounded wire decoder; reject duplicate keys at every nesting level."""
    require(isinstance(raw, bytes) and len(raw) <= MAX_GRANT_BYTES)
    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, 'renewal_duplicate_key')
            out[key] = value
        return out
    try:
        return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False))
    except (UnicodeError, ValueError, RecursionError):
        raise RenewalHeld('renewal_json_held') from None


def validate_policy(policy, *, total_bytes, object_count):
    closed(policy, ('profile', 'maxEpochs', 'maxLifetimeMs', 'byteBudget', 'attemptBudget'))
    require(policy['profile'] == PROFILE)
    integer(policy['maxEpochs'], 1, MAX_EPOCHS)
    integer(policy['maxLifetimeMs'])
    integer(policy['byteBudget'], total_bytes, min(p.MAX_SAFE_INTEGER, 3 * total_bytes))
    integer(policy['attemptBudget'], object_count, min(3 * object_count, object_count + 10))
    return copy.deepcopy(policy)


CAPABILITY_FIELDS = ('schemaVersion', 'profile', 'wholeObject200', 'maxResponseBytes',
    'maxConcurrentBytes', 'maxRequestsPerEpoch', 'maxEpochs', 'maxEpochTtlMs',
    'maxLifetimeMs', 'maxTotalRequests', 'maxTotalBytes')


def validate_compatibility(capabilities, *, policy, manifest, started_ms, original_expiry_ms):
    """Validate authenticated SERVER-owned limits, never escalate client values.

    Necessary compatibility only; TTL feasibility still needs measured throughput.
    The transport/config owner must authenticate and persist this observation.
    """
    closed(capabilities, CAPABILITY_FIELDS)
    require(type(capabilities['schemaVersion']) is int and capabilities['schemaVersion'] == 2
            and capabilities['profile'] == PROFILE and capabilities['wholeObject200'] is True)
    for key in CAPABILITY_FIELDS[3:]:integer(capabilities[key])
    integer(capabilities['maxEpochs'], 1, MAX_EPOCHS)
    largest = max(obj['byteSize'] for obj in manifest['objects'])
    require(largest <= capabilities['maxResponseBytes']
            and largest <= capabilities['maxConcurrentBytes'], 'renewal_whole_object_capability_held')
    require(policy['maxEpochs'] <= capabilities['maxEpochs']
            and policy['maxLifetimeMs'] <= capabilities['maxLifetimeMs']
            and policy['attemptBudget'] <= capabilities['maxTotalRequests']
            and policy['byteBudget'] <= capabilities['maxTotalBytes']
            and capabilities['maxRequestsPerEpoch'] * (policy['maxEpochs']+1) >= policy['attemptBudget'],
            'renewal_aggregate_capability_held')
    require(original_expiry_ms-started_ms <= capabilities['maxEpochTtlMs'], 'renewal_epoch_ttl_held')
    return copy.deepcopy(capabilities)


REQUEST_FIELDS = ('schemaVersion', 'profile', 'requestId', 'jobId', 'generation',
                  'environment', 'reservationId', 'manifestSha256', 'nodeId',
                  'workerId', 'epoch', 'predecessorSha256', 'expiresAt', 'nonce')
ENVELOPE_FIELDS = ('schemaVersion', 'profile', 'requestId', 'epoch',
                   'predecessorSha256', 'grant')


def validate_request(request, *, binding, policy, predecessor, now_ms):
    closed(request, REQUEST_FIELDS)
    require(len(encoded(request)) <= MAX_REQUEST_BYTES)
    require(type(request['schemaVersion']) is int and request['schemaVersion'] == 2)
    require(request['profile'] == PROFILE)
    try:
        p._uuid(request['requestId'], 'renewal request')
        p._slug(request['nonce'], 'renewal nonce')
        p._hash(request['predecessorSha256'], 'predecessor')
    except ValueError:
        raise RenewalHeld() from None
    for key in ('jobId', 'generation', 'environment', 'reservationId',
                'manifestSha256', 'nodeId', 'workerId'):
        require(type(request[key]) is type(binding[key]) and request[key] == binding[key], 'renewal_binding_held')
    integer(request['epoch'], 1, policy['maxEpochs'])
    require(request['epoch'] == predecessor['epoch'] + 1
            and request['predecessorSha256'] == predecessor['sha256'], 'renewal_chain_held')
    expiry = milliseconds(request['expiresAt'])
    require(expiry > predecessor['expiresMs'] and expiry > now_ms
            and expiry <= binding['startedMs'] + policy['maxLifetimeMs'], 'renewal_expiry_held')
    return copy.deepcopy(request)


SCHEMA = (
    "CREATE TABLE renewal_meta (id INTEGER PRIMARY KEY CHECK(id=1), binding TEXT NOT NULL CHECK(typeof(binding)='text' AND length(CAST(binding AS BLOB))<=8192), policy TEXT NOT NULL CHECK(typeof(policy)='text' AND length(CAST(policy AS BLOB))<=4096), capabilities TEXT NOT NULL CHECK(typeof(capabilities)='text' AND length(CAST(capabilities AS BLOB))<=4096), original_job TEXT NOT NULL CHECK(typeof(original_job)='text' AND length(CAST(original_job AS BLOB))<=4194304), original_grant TEXT NOT NULL CHECK(typeof(original_grant)='text' AND length(CAST(original_grant AS BLOB))<=2097152), clock INTEGER NOT NULL CHECK(typeof(clock)='integer' AND clock BETWEEN 1 AND 9007199254740991))",
    "CREATE TABLE renewal_requests (request_id TEXT PRIMARY KEY CHECK(typeof(request_id)='text' AND length(CAST(request_id AS BLOB))=36), epoch INTEGER UNIQUE NOT NULL CHECK(typeof(epoch)='integer' AND epoch BETWEEN 1 AND 32), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=4096), sha TEXT NOT NULL CHECK(typeof(sha)='text' AND length(CAST(sha AS BLOB))=64))",
    "CREATE TABLE renewal_epochs (epoch INTEGER PRIMARY KEY CHECK(typeof(epoch)='integer' AND epoch BETWEEN 0 AND 32), request_id TEXT UNIQUE CHECK(request_id IS NULL OR (typeof(request_id)='text' AND length(CAST(request_id AS BLOB))=36)), record TEXT NOT NULL CHECK(typeof(record)='text' AND length(CAST(record AS BLOB))<=2097152), sha TEXT NOT NULL CHECK(typeof(sha)='text' AND length(CAST(sha AS BLOB))=64), expires INTEGER NOT NULL CHECK(typeof(expires)='integer' AND expires BETWEEN 1 AND 9007199254740991))",
    "CREATE TABLE renewal_attempts (attempt_id TEXT PRIMARY KEY CHECK(typeof(attempt_id)='text' AND length(CAST(attempt_id AS BLOB))=36), object_id TEXT NOT NULL CHECK(typeof(object_id)='text' AND length(CAST(object_id AS BLOB)) BETWEEN 1 AND 128), epoch INTEGER NOT NULL CHECK(typeof(epoch)='integer' AND epoch BETWEEN 0 AND 32), bytes INTEGER NOT NULL CHECK(typeof(bytes)='integer' AND bytes BETWEEN 1 AND 9007199254740991))",
)
TRIGGERS = tuple(
    'CREATE TRIGGER '+table+'_'+action.lower()+' BEFORE '+action+' ON '+table+
    " BEGIN SELECT RAISE(ABORT,'immutable renewal history'); END"
    for table in ('renewal_requests', 'renewal_epochs', 'renewal_attempts')
    for action in ('UPDATE', 'DELETE')
) + (
    "CREATE TRIGGER renewal_requests_insert BEFORE INSERT ON renewal_requests WHEN EXISTS(SELECT 1 FROM renewal_requests WHERE request_id=NEW.request_id OR epoch=NEW.epoch) BEGIN SELECT RAISE(ABORT,'immutable renewal collision'); END",
    "CREATE TRIGGER renewal_epochs_insert BEFORE INSERT ON renewal_epochs WHEN EXISTS(SELECT 1 FROM renewal_epochs WHERE epoch=NEW.epoch OR request_id=NEW.request_id) BEGIN SELECT RAISE(ABORT,'immutable renewal collision'); END",
    "CREATE TRIGGER renewal_attempts_insert BEFORE INSERT ON renewal_attempts WHEN EXISTS(SELECT 1 FROM renewal_attempts WHERE attempt_id=NEW.attempt_id) BEGIN SELECT RAISE(ABORT,'immutable renewal collision'); END",
    "CREATE TRIGGER renewal_meta_insert BEFORE INSERT ON renewal_meta WHEN EXISTS(SELECT 1 FROM renewal_meta) BEGIN SELECT RAISE(ABORT,'immutable renewal collision'); END",
    "CREATE TRIGGER renewal_meta_delete BEFORE DELETE ON renewal_meta BEGIN SELECT RAISE(ABORT,'immutable renewal history'); END",
    "CREATE TRIGGER renewal_meta_binding BEFORE UPDATE OF id,binding,policy,capabilities,original_job,original_grant ON renewal_meta BEGIN SELECT RAISE(ABORT,'immutable renewal binding'); END",
    "CREATE TRIGGER renewal_meta_clock BEFORE UPDATE OF clock ON renewal_meta WHEN typeof(NEW.clock)!='integer' OR NEW.clock<OLD.clock BEGIN SELECT RAISE(ABORT,'immutable renewal clock'); END",
)


class RenewalJournal:
    """Explicitly enabled candidate journal, not an issuer or download capability.

    Connection must be exclusively caller-owned, private, DELETE/FULL, without
    attached/TEMP schemas. Integrators must separately budget its physical bytes,
    bind its file/root identity and enforce driver/scratch ownership. This class
    intentionally does not migrate existing v1/indexed DBs or configure daemons.
    """
    def __init__(self, db, *, enabled=False, job, original_grant, node_id,
                 worker_id, policy, started_ms, capabilities=None):
        require(enabled is True, 'renewal_disabled')
        require(type(db) is sqlite3.Connection and not db.in_transaction)
        require(db.execute('PRAGMA journal_mode').fetchone()[0] == 'delete')
        require(db.execute('PRAGMA synchronous').fetchone()[0] == 2)
        integer(started_ms)
        manifest = job['submission']['manifest']
        require(type(manifest['objects']) is list and 0 < len(manifest['objects']) <= p.MAX_OBJECTS,
                'renewal_object_count_held')
        require(len(encoded(job)) <= p.MAX_BODY_BYTES and len(encoded(original_grant)) <= MAX_GRANT_BYTES)
        try:
            p._slug(node_id, 'node'); p._slug(worker_id, 'worker')
            validate_grant(original_grant, job=job, clock=lambda: started_ms / 1000)
        except ValueError:
            raise RenewalHeld('renewal_original_held') from None
        self.policy = validate_policy(policy, total_bytes=manifest['totalBytes'],
                                      object_count=len(manifest['objects']))
        require(self.policy['byteBudget'] == original_grant['byteBudget'])
        integer(started_ms + self.policy['maxLifetimeMs'])
        require(started_ms < milliseconds(original_grant['expiresAt'])
                <= started_ms + self.policy['maxLifetimeMs'], 'renewal_lifetime_held')
        self.capabilities = validate_compatibility(capabilities, policy=self.policy,
            manifest=manifest, started_ms=started_ms,
            original_expiry_ms=milliseconds(original_grant['expiresAt']))
        self.binding = dict(jobId=job['jobId'], generation=job['generation'],
            environment=job['environment'], reservationId=original_grant['reservationId'],
            manifestSha256=job['manifestSha256'], nodeId=node_id, workerId=worker_id,
            startedMs=started_ms, objectClosureSha256=digest(manifest['objects']))
        self.job, self.original = copy.deepcopy(job), copy.deepcopy(original_grant)
        self.objects = {o['id']: o['byteSize'] for o in manifest['objects']}
        require(len(self.objects) == len(manifest['objects']))
        self.db = db
        with self.transaction():
            tables = self.db.execute("SELECT 1 FROM main.sqlite_master WHERE type='table' LIMIT 1").fetchone()
            if tables is None:
                for statement in SCHEMA + TRIGGERS:
                    db.execute(statement)
                db.execute('INSERT INTO renewal_meta VALUES(1,?,?,?,?,?,?)',
                    (encoded(self.binding).decode(), encoded(self.policy).decode(),
                     encoded(self.capabilities).decode(),
                     encoded(self.job).decode(), encoded(self.original).decode(), started_ms))
                db.execute('INSERT INTO renewal_epochs VALUES(0,NULL,?,?,?)',
                    (encoded(self.original).decode(), digest(self.original), milliseconds(original_grant['expiresAt'])))
            self.audit()

    @contextmanager
    def transaction(self):
        require(not self.db.in_transaction, 'renewal_transaction_owner_held')
        require(self.db.execute('SELECT count(*) FROM temp.sqlite_master').fetchone()[0] == 0)
        require([row[1] for row in self.db.execute('PRAGMA database_list')] == ['main', 'temp'])
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def audit(self):
        # Refuse an unknown/oversized catalog before fetching its SQL into Python.
        maximum_catalog = len(SCHEMA) + len(TRIGGERS)
        require(self.db.execute('SELECT 1 FROM main.sqlite_master WHERE sql IS NOT NULL LIMIT 1 OFFSET ?',
                                (maximum_catalog,)).fetchone() is None, 'renewal_schema_held')
        require(self.db.execute('SELECT 1 FROM main.sqlite_master WHERE sql IS NOT NULL AND length(CAST(sql AS BLOB))>8192 LIMIT 1').fetchone() is None, 'renewal_schema_held')
        actual = sorted(row[0] for row in self.db.execute("SELECT sql FROM main.sqlite_master WHERE sql IS NOT NULL LIMIT ?", (maximum_catalog+1,)))
        require(actual == sorted(SCHEMA + TRIGGERS), 'renewal_schema_held')
        self._bounded_rows()
        meta = self.db.execute('SELECT binding,policy,capabilities,original_job,original_grant,clock FROM renewal_meta WHERE id=1').fetchone()
        require(meta is not None and meta[:5] == tuple(encoded(x).decode() for x in
            (self.binding, self.policy, self.capabilities, self.job, self.original)), 'renewal_latch_held')
        integer(meta[5], self.binding['startedMs'], self.binding['startedMs']+self.policy['maxLifetimeMs'])
        epochs = self.db.execute('SELECT epoch,request_id,record,sha,expires FROM renewal_epochs ORDER BY epoch LIMIT ?',
                                 (self.policy['maxEpochs']+2,)).fetchall()
        require(1 <= len(epochs) <= self.policy['maxEpochs'] + 1)
        requests = self.db.execute('SELECT request_id,epoch,record,sha FROM renewal_requests ORDER BY epoch LIMIT ?',
                                   (self.policy['maxEpochs']+1,)).fetchall()
        require(len(requests) in (len(epochs)-1, len(epochs)))
        for seq, (epoch, request_id, raw, sha, expires) in enumerate(epochs):
            require(epoch == seq and hashlib.sha256(raw.encode()).hexdigest() == sha)
            integer(expires)
            if epoch == 0:
                require(request_id is None and raw == encoded(self.original).decode()
                        and expires == milliseconds(self.original['expiresAt']), 'renewal_original_expiry_held')
            else:
                require(request_id == requests[epoch-1][0] and decode(raw.encode())['requestId'] == request_id)
        nonces = {self.original['nonce']}
        for seq, (request_id, epoch, raw, sha) in enumerate(requests):
            require(epoch == seq+1 and hashlib.sha256(raw.encode()).hexdigest() == sha)
            request = decode(raw.encode())
            require(request['requestId'] == request_id)
            require(request['nonce'] not in nonces, 'renewal_nonce_reused')
            nonces.add(request['nonce'])
            validate_request(request, binding=self.binding, policy=self.policy,
                predecessor=dict(epoch=epoch-1, sha256=epochs[epoch-1][3], expiresMs=epochs[epoch-1][4]),
                now_ms=self.binding['startedMs'])
            if epoch < len(epochs):
                envelope = decode(epochs[epoch][2].encode())
                self._validate_envelope(envelope, request, now_ms=self.binding['startedMs'])
                require(epochs[epoch][4] == milliseconds(envelope['grant']['expiresAt']))
        attempts = self.db.execute('SELECT attempt_id,object_id,epoch,bytes FROM renewal_attempts LIMIT ?',
                                   (self.policy['attemptBudget']+1,)).fetchall()
        require(len(attempts) <= self.policy['attemptBudget'])
        counts = {}; epoch_counts = {}; charged = 0
        for attempt_id, object_id, epoch, size in attempts:
            try:p._uuid(attempt_id, 'attempt')
            except ValueError:raise RenewalHeld() from None
            require(object_id in self.objects and type(size) is int and size == self.objects[object_id]
                    and type(epoch) is int and 0 <= epoch < len(epochs))
            counts[object_id] = counts.get(object_id, 0) + 1; charged += size
            epoch_counts[epoch] = epoch_counts.get(epoch, 0) + 1
        require(all(n <= 3 for n in counts.values()) and charged <= self.policy['byteBudget']
                and all(n <= self.capabilities['maxRequestsPerEpoch'] for n in epoch_counts.values()))

    def _bounded_rows(self):
        """SQL-only type/length/count probes BEFORE materializing durable cells.

        CHECK constraints prevent ordinary writes; these probes also hold on a
        preexisting dirty snapshot produced with ignore_check_constraints=ON.
        """
        limits = dict(renewal_meta=1, renewal_epochs=self.policy['maxEpochs']+1,
                      renewal_requests=self.policy['maxEpochs'], renewal_attempts=self.policy['attemptBudget'])
        columns = dict(
            renewal_meta=dict(binding=8192,policy=4096,capabilities=4096,original_job=p.MAX_BODY_BYTES,original_grant=MAX_GRANT_BYTES),
            renewal_epochs=dict(record=MAX_GRANT_BYTES,sha=64),
            renewal_requests=dict(request_id=36,record=MAX_REQUEST_BYTES,sha=64),
            renewal_attempts=dict(attempt_id=36,object_id=128))
        for table, limit in limits.items():
            require(self.db.execute('SELECT 1 FROM '+table+' LIMIT 1 OFFSET ?', (limit,)).fetchone() is None,
                    'renewal_durable_row_limit')
            for column, maximum in columns[table].items():
                require(self.db.execute('SELECT 1 FROM '+table+' WHERE typeof('+column+")!='text' OR length(CAST("+column+' AS BLOB))>? LIMIT 1', (maximum,)).fetchone() is None,
                    'renewal_durable_cell_limit')
        for table, columns in (('renewal_meta',('clock',)),('renewal_epochs',('epoch','expires')),
                               ('renewal_requests',('epoch',)),('renewal_attempts',('epoch','bytes'))):
            for column in columns:
                require(self.db.execute('SELECT 1 FROM '+table+' WHERE typeof('+column+")!='integer' LIMIT 1").fetchone() is None,
                        'renewal_durable_type_limit')
        require(self.db.execute("SELECT 1 FROM renewal_epochs WHERE request_id IS NOT NULL AND (typeof(request_id)!='text' OR length(CAST(request_id AS BLOB))!=36) LIMIT 1").fetchone() is None,
                'renewal_durable_cell_limit')

    def tick(self, now_ms):
        integer(now_ms)
        old = self.db.execute('SELECT clock FROM renewal_meta WHERE id=1').fetchone()[0]
        require(now_ms >= old and now_ms <= self.binding['startedMs'] + self.policy['maxLifetimeMs'], 'renewal_clock_held')
        self.db.execute('UPDATE renewal_meta SET clock=? WHERE id=1', (now_ms,))

    def _validate_envelope(self, envelope, request, *, now_ms):
        closed(envelope, ENVELOPE_FIELDS)
        require(type(envelope['schemaVersion']) is int and envelope['schemaVersion'] == 2
                and envelope['profile'] == PROFILE and type(envelope['epoch']) is int)
        require(len(encoded(envelope)) <= MAX_GRANT_BYTES
                and envelope['epoch'] == request['epoch']
                and envelope['requestId'] == request['requestId']
                and envelope['predecessorSha256'] == request['predecessorSha256'])
        grant = envelope['grant']; job = copy.deepcopy(self.job)
        job['transferRequest'].update(expiresAt=request['expiresAt'], nonce=request['nonce'])
        try:validate_grant(grant, job=job, clock=lambda: now_ms/1000)
        except ValueError:raise RenewalHeld('renewal_grant_binding_held') from None
        require(grant['byteBudget'] == self.policy['byteBudget'])

    def request(self, request, *, now_ms, inflight_drained=False):
        """Caller must first acquire real worker ownership and drain all reads.

        This explicit gate is an integration precondition, not proof of ownership
        or authority. This candidate has no live readers or daemon wiring.
        """
        require(inflight_drained is True, 'renewal_inflight_drain_required')
        closed(request, REQUEST_FIELDS)
        with self.transaction():
            self.audit(); self.tick(now_ms)
            old = self.db.execute('SELECT record FROM renewal_requests WHERE request_id=?', (request.get('requestId'),)).fetchone()
            if old:
                require(old[0] == encoded(request).decode(), 'renewal_request_conflict')
                return {'requestId':request['requestId'], 'epoch':request['epoch'], 'requestSha256':digest(request)}
            last = self.db.execute('SELECT epoch,sha,expires FROM renewal_epochs ORDER BY epoch DESC LIMIT 1').fetchone()
            checked = validate_request(request, binding=self.binding, policy=self.policy,
                predecessor=dict(epoch=last[0], sha256=last[1], expiresMs=last[2]), now_ms=now_ms)
            require(milliseconds(checked['expiresAt'])-now_ms <= self.capabilities['maxEpochTtlMs'], 'renewal_epoch_ttl_held')
            require(not self.db.execute('SELECT 1 FROM renewal_requests WHERE epoch=?', (checked['epoch'],)).fetchone(), 'renewal_epoch_conflict')
            self.db.execute('INSERT INTO renewal_requests VALUES(?,?,?,?)',
                (checked['requestId'], checked['epoch'], encoded(checked).decode(), digest(checked)))
            self.audit()
            return {'requestId':checked['requestId'], 'epoch':checked['epoch'], 'requestSha256':digest(checked)}

    def accept(self, envelope, *, now_ms, inflight_drained=False):
        require(inflight_drained is True, 'renewal_inflight_drain_required')
        with self.transaction():
            self.audit(); self.tick(now_ms)
            closed(envelope, ENVELOPE_FIELDS)
            require(envelope['schemaVersion'] == 2 and type(envelope['schemaVersion']) is int and envelope['profile'] == PROFILE)
            require(len(encoded(envelope)) <= MAX_GRANT_BYTES)
            row = self.db.execute('SELECT record FROM renewal_requests WHERE request_id=?', (envelope['requestId'],)).fetchone()
            require(row is not None, 'renewal_request_missing')
            request = decode(row[0].encode())
            require(envelope['epoch'] == request['epoch'] and type(envelope['epoch']) is int
                    and envelope['predecessorSha256'] == request['predecessorSha256'])
            old = self.db.execute('SELECT record FROM renewal_epochs WHERE epoch=?', (request['epoch'],)).fetchone()
            if old:
                require(old[0] == encoded(envelope).decode(), 'renewal_grant_conflict')
            else:
                self._validate_envelope(envelope, request, now_ms=now_ms)
                grant = envelope['grant']
                self.db.execute('INSERT INTO renewal_epochs VALUES(?,?,?,?,?)',
                    (request['epoch'], request['requestId'], encoded(envelope).decode(), digest(envelope), milliseconds(grant['expiresAt'])))
            self.audit()
            return dict(requestId=request['requestId'], epoch=request['epoch'], grantSha256=digest(envelope), accepted=True)

    def charge_attempt(self, attempt_id, object_id, *, epoch, now_ms):
        """Durably charge full bytes BEFORE GET; a replay CANNOT start a GET.

        Only the first successful commit returns mayStartGet=True. If its return
        was lost, a fresh attempt ID/full charge is required. This receipt alone
        is not a transferable authority; actual driver ownership stays external.
        """
        try:p._uuid(attempt_id, 'attempt')
        except ValueError:raise RenewalHeld() from None
        require(object_id in self.objects)
        integer(epoch, 0, self.policy['maxEpochs'])
        with self.transaction():
            self.audit(); self.tick(now_ms)
            old = self.db.execute('SELECT object_id,epoch,bytes FROM renewal_attempts WHERE attempt_id=?', (attempt_id,)).fetchone()
            if old:
                require(old == (object_id, epoch, self.objects[object_id]), 'renewal_attempt_conflict')
                return dict(attemptId=attempt_id, chargedBytes=old[2], replay=True, mayStartGet=False)
            current = self.db.execute('SELECT epoch,expires FROM renewal_epochs ORDER BY epoch DESC LIMIT 1').fetchone()
            require(current[0] == epoch and current[1] > now_ms, 'renewal_current_epoch_required')
            require(not self.db.execute('SELECT 1 FROM renewal_requests WHERE epoch>?', (current[0],)).fetchone(), 'renewal_pending_read_fence')
            self.db.execute('INSERT INTO renewal_attempts VALUES(?,?,?,?)', (attempt_id, object_id, epoch, self.objects[object_id]))
            self.audit()
            return dict(attemptId=attempt_id, chargedBytes=self.objects[object_id], replay=False, mayStartGet=True)

    def reconcile(self, request_id):
        """Read exact pending/accepted receipt after an unknown reply, no token."""
        try:p._uuid(request_id, 'request')
        except ValueError:raise RenewalHeld() from None
        with self.transaction():
            self.audit()
            row = self.db.execute('SELECT epoch,sha FROM renewal_requests WHERE request_id=?', (request_id,)).fetchone()
            if row is None:return None
            accepted = self.db.execute('SELECT sha FROM renewal_epochs WHERE request_id=?', (request_id,)).fetchone()
            return dict(requestId=request_id, epoch=row[0], requestSha256=row[1],
                        accepted=accepted is not None, grantSha256=accepted[0] if accepted else None)
