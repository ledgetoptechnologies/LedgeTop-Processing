"""Unwired, unqualified task-plan identity experiment; grants no execution."""
import hashlib
import math
import re
import struct

from . import reference_protocol as ref
from .maintenance import Conflict
from .native_manifest_binding import NativeManifestBindings

PLAN_DOMAIN = b'ltds-viewer-task-plan-v1\0'
CATALOG_DOMAIN = b'ltds-viewer-selected-catalog-v1\0'
MAX_PLAN_BYTES = 65536
MAX_RECORDS = 1000
MAX_HISTORY_BYTES = 64 * 1024 * 1024
DDL = '''CREATE TABLE native_plan_bindings (uuid TEXT PRIMARY KEY, authority TEXT NOT NULL,
 environment TEXT NOT NULL, capability_hash TEXT NOT NULL, manifest_binding_id TEXT NOT NULL,
 plan_bytes BLOB NOT NULL, plan_sha TEXT NOT NULL, catalog_bytes BLOB NOT NULL, catalog_sha TEXT NOT NULL)'''
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,119}\Z')
HEX64 = re.compile(r'[0-9a-f]{64}\Z')
BITS = re.compile(r'[0-9a-f]{16}\Z')


def _text(value, maximum, *, controls=True):
    ref._require(type(value) is str and len(value) <= maximum, 'invalid plan text')
    ref._require(not any(0xd800 <= ord(c) <= 0xdfff or (controls and ord(c) < 32) for c in value), 'invalid plan text')


def validate(body):
    """Check proposed canonical plan bytes; this does not qualify engine semantics."""
    ref._require(type(body) is bytes and len(body) <= MAX_PLAN_BYTES, 'invalid plan bytes')
    value = ref.decode_submission(body)
    ref._require(ref.canonical_json(value) == body, 'noncanonical plan')
    ref._closed(value, ('schemaVersion','environment','taskUuid','generation','bindingId',
                        'manifestSha256','taskName','options','outputs','expectedCatalogDigest'), 'plan')
    ref._require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1, 'invalid plan version')
    ref._environment(value['environment']); ref._uuid(value['taskUuid'], 'taskUuid')
    ref._require(type(value['generation']) is int and value['generation'] == 1, 'invalid generation')
    for key in ('bindingId','manifestSha256','expectedCatalogDigest'):
        ref._require(type(value[key]) is str and HEX64.fullmatch(value[key]), 'invalid digest')
    _text(value['taskName'], 256)
    options = value['options']
    ref._require(type(options) is list and len(options) <= 1000, 'invalid options')
    names = set()
    for option in options:
        ref._closed(option, ('name','value'), 'option')
        name = option['name']; ref._require(type(name) is str and NAME.fullmatch(name) and name not in names, 'invalid option name'); names.add(name)
        scalar = option['value']; ref._require(type(scalar) is dict, 'invalid scalar')
        kind = scalar.get('type')
        if kind == 'null': ref._closed(scalar, ('type',), 'null scalar')
        elif kind == 'bool':
            ref._closed(scalar, ('type','value'), 'bool scalar'); ref._require(type(scalar['value']) is bool, 'invalid bool')
        elif kind == 'string':
            ref._closed(scalar, ('type','value'), 'string scalar'); _text(scalar['value'], MAX_PLAN_BYTES, controls=False)
        elif kind == 'number':
            ref._closed(scalar, ('type','bits'), 'number scalar'); bits = scalar['bits']
            ref._require(type(bits) is str and BITS.fullmatch(bits) and bits != '8000000000000000', 'invalid number bits')
            number = struct.unpack('>d', bytes.fromhex(bits))[0]
            ref._require(math.isfinite(number) and (not number.is_integer() or abs(number) <= ref.MAX_SAFE_INTEGER), 'invalid number')
        else: raise ref.ProtocolError('unsupported scalar')
    outputs = value['outputs']
    ref._require(outputs is None or (type(outputs) is list and len(outputs) <= 1000), 'invalid outputs')
    if outputs is not None:
        for output in outputs: _text(output, 256, controls=False)
    return value


def plan_sha(body):
    return hashlib.sha256(PLAN_DOMAIN + body).hexdigest()


def catalog_sha(body):
    return hashlib.sha256(CATALOG_DOMAIN + body).hexdigest()


class NativePlanBindings:
    """Internal-only record, never exposed through the application or a wire route."""
    def __init__(self, service, manifest_binding):
        from . import config
        if getattr(config, 'NATIVE_PLAN_BINDING_QUALIFIED', False) is not True:
            raise Conflict('native plan binding not qualified')
        if not isinstance(manifest_binding, NativeManifestBindings) or manifest_binding.store is not service.store:
            raise Conflict('manifest authority unavailable')
        self.service = service; self.manifest = manifest_binding; self.store = service.store; self.queue = self.store.queue
        self.manifest._authority()
        with self.store._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='native_plan_bindings'").fetchone()
            if row is None: db.execute(DDL)
            elif row[0] != DDL: raise Conflict('native plan schema changed')

    def _manifest_locked(self, db, identifier, environment, capability):
        reservation = self.manifest._check(db, identifier, environment, capability)
        row = db.execute('SELECT * FROM native_manifest_bindings WHERE uuid=?', (identifier,)).fetchone()
        if row is None: raise Conflict('manifest binding required')
        from . import native_input_manifest
        try:
            checked = native_input_manifest.validate(bytes(row['manifest_bytes']), expected_manifest_id=row['manifest_id'], max_input_bytes=row['accepted_limit'])
            receipt = self.manifest._receipt(identifier, environment, reservation, checked, row['accepted_limit'])
            if (row['environment'] != environment or row['authority'] != self.queue.authority_id or row['generation'] != 1
                or row['capability_hash'] != reservation['capability_hash'] or row['revision'] != 1
                or row['manifest_sha'] != checked['sha256'] or row['binding_id'] != receipt['bindingId']):
                raise Conflict('manifest binding changed')
        except (ValueError, TypeError, KeyError, OverflowError):
            raise Conflict('manifest binding changed') from None
        return reservation, receipt

    def _existing(self, db, plan, body, reservation, manifest_receipt):
        row = db.execute('SELECT * FROM native_plan_bindings WHERE uuid=?', (plan['taskUuid'],)).fetchone()
        if row is None: return None
        catalog = bytes(row['catalog_bytes'])
        if (row['authority'] != self.queue.authority_id or row['environment'] != plan['environment']
                or row['capability_hash'] != reservation['capability_hash'] or row['manifest_binding_id'] != manifest_receipt['bindingId']
                or bytes(row['plan_bytes']) != body or row['plan_sha'] != plan_sha(body)
                or row['catalog_sha'] != catalog_sha(catalog) or plan['expectedCatalogDigest'] != row['catalog_sha']
                or catalog != b'[]'):
            raise Conflict('native plan binding changed')
        return self._receipt(plan, row['plan_sha'], row['catalog_sha'])

    @staticmethod
    def _receipt(plan, digest, catalog):
        return {'protocol':'native-plan-v1-experiment', 'taskUuid':plan['taskUuid'], 'environment':plan['environment'],
                'generation':1, 'bindingId':plan['bindingId'], 'manifestSha256':plan['manifestSha256'],
                'planSha256':digest, 'catalogDigest':catalog, 'state':'identity_bound',
                'engineEvidence':{'state':'unavailable'}, 'capacityReady':False, 'dispatchReady':False}

    def catalog_digest(self):
        """Only the empty selection is supported; catalog health is still required."""
        snapshot = self.service._catalog()
        if snapshot.get('ready') is not True: raise Conflict('catalog unavailable')
        return catalog_sha(b'[]')

    def bind(self, identifier, environment, capability, body):
        plan = validate(body)
        if plan['taskUuid'] != identifier or plan['environment'] != environment: raise Conflict('plan identity mismatch')
        if plan['options']: raise Conflict('option domain profile not qualified')
        def check(db):
            reservation, manifest = self._manifest_locked(db, identifier, environment, capability)
            if plan['bindingId'] != manifest['bindingId'] or plan['manifestSha256'] != manifest['manifestSha256']:
                raise Conflict('plan manifest mismatch')
            return reservation, manifest
        with self.store._lock, self.queue._db() as db:
            db.execute('BEGIN'); reservation, manifest = check(db)
            existing = self._existing(db, plan, body, reservation, manifest)
            if existing is not None: return existing
        # Never use a fresh catalog to reinterpret an already bound plan.
        try: observed = self.catalog_digest()
        except Exception:
            with self.store._lock, self.queue._db() as db:
                db.execute('BEGIN'); reservation, manifest = check(db)
                existing = self._existing(db, plan, body, reservation, manifest)
                if existing is not None: return existing
            raise
        with self.store._lock, self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE'); reservation, manifest = check(db)
            existing = self._existing(db, plan, body, reservation, manifest)
            if existing is not None: return existing
            if observed != plan['expectedCatalogDigest']: raise Conflict('catalog changed')
            count, used = db.execute('SELECT COUNT(*),COALESCE(SUM(length(plan_bytes)+length(catalog_bytes)),0) FROM native_plan_bindings').fetchone()
            if count >= MAX_RECORDS or used + len(body) + 2 > MAX_HISTORY_BYTES: raise Conflict('native plan history limit reached')
            digest = plan_sha(body)
            db.execute('INSERT INTO native_plan_bindings VALUES(?,?,?,?,?,?,?,?,?)',
                       (identifier,self.queue.authority_id,environment,reservation['capability_hash'],manifest['bindingId'],body,digest,b'[]',observed))
            return self._receipt(plan, digest, observed)

    def status(self, identifier, environment, capability):
        with self.store._lock, self.queue._db() as db:
            db.execute('BEGIN')
            reservation, manifest = self._manifest_locked(db, identifier, environment, capability)
            row = db.execute('SELECT plan_bytes FROM native_plan_bindings WHERE uuid=?', (identifier,)).fetchone()
            if row is None: raise KeyError('unknown plan binding')
            body = bytes(row['plan_bytes'])
            try: plan = validate(body)
            except (ValueError, TypeError, KeyError, OverflowError): raise Conflict('native plan binding changed') from None
            if (plan['taskUuid'] != identifier or plan['environment'] != environment or plan['options']
                    or plan['bindingId'] != manifest['bindingId'] or plan['manifestSha256'] != manifest['manifestSha256']):
                raise Conflict('native plan binding changed')
            return self._existing(db, plan, body, reservation, manifest)
