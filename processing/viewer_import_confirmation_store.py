"""Inactive first-confirmation journal; no route or cleanup authorization.

The trusted loader is application-owned code. It must read an independently
authenticated, coherent binding and pinned public keys using the supplied
SQLite transaction, never values derived from the incoming confirmation. No
such production binding source is currently available. This candidate must not
be wired until the Viewer contract and source of those bindings are agreed.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from . import reference_protocol as protocol
from .viewer_import_confirmation import (
    ImportConfirmationRejected, MAX_INPUT_BYTES, _snapshot_inputs,
    verify_import_confirmation,
)

_TABLE = 'viewer_import_confirmations_candidate_v1'
_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z')
_HASH = re.compile(r'[a-f0-9]{64}\Z')
_CONTEXT = frozenset(('expected', 'result_manifest', 'accepted_envelopes',
                      'trusted_import_keys', 'trusted_copy_keys'))


class ImportConfirmationStoreHeld(ValueError):
    """Missing trust, an uncertain store, or conflicting replay retains data."""


@dataclass(frozen=True)
class RecordResult:
    state: str
    sha256: str


def _after_insert():
    """Test seam for a crash before COMMIT; never grants authorization."""


def _after_commit():
    """Test seam for a lost response after COMMIT."""


def _scope(value):
    if (type(value) is not dict or set(value) != {'jobId', 'generation', 'environment'}
            or type(value['jobId']) is not str or not _ID.fullmatch(value['jobId'])
            or type(value['generation']) is not int or not 0 < value['generation'] <= 9007199254740991
            or value['environment'] not in ('staging', 'production')):
        raise ImportConfirmationStoreHeld('invalid_trusted_scope')
    return value


def _reject_duplicate_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ImportConfirmationStoreHeld('stored_confirmation_invalid')
        result[key] = value
    return result


class ImportConfirmationStore:
    """Journal exact verifier output under a trusted job scope, first write wins.

    ``trusted_loader(db, scope)`` is trusted installation code and must read a
    coherent committed binding and its public-key pins from the same SQLite
    database through the supplied transaction. It returns exactly
    ``expected``, ``result_manifest``, ``accepted_envelopes``,
    ``trusted_import_keys`` and ``trusted_copy_keys``. It must not commit,
    mutate data, use confirmation fields, or read mutable external state. This
    API does not establish those conditions; production integration requires a
    separately reviewed trusted source. The DB parent must be private and
    exclusively controlled by the service identity, including on Windows.
    """

    def __init__(self, path, *, trusted_loader):
        if not callable(trusted_loader):
            raise ValueError('trusted loader required')
        self.loader = trusted_loader
        self.path = Path(path).absolute()
        parent = self.path.parent
        if not parent.is_dir() or parent.resolve() != parent or self.path.is_symlink():
            raise ImportConfirmationStoreHeld('private_store_directory_required')
        info = parent.stat()
        if os.name != 'nt' and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise ImportConfirmationStoreHeld('private_store_directory_required')
        self.parent_identity = (info.st_dev, info.st_ino)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_NOFOLLOW', 0), 0o600)
        except FileExistsError:
            pass
        else:
            os.close(fd)
        self.file_identity = self._check_file()
        with self._connect() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                db.execute(f'''CREATE TABLE IF NOT EXISTS {_TABLE} (
                    job_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL,
                    confirmation_id TEXT NOT NULL UNIQUE, binding_sha256 TEXT NOT NULL,
                    confirmation_sha256 TEXT NOT NULL, canonical_bytes BLOB NOT NULL,
                    PRIMARY KEY (job_id, generation, environment))''')
                self._check_schema(db)
                db.commit()
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise

    def _check_file(self):
        try:
            parent = self.path.parent.stat()
            info = self.path.lstat()
        except OSError:
            raise ImportConfirmationStoreHeld('store_path_unavailable') from None
        if (self.path.parent.resolve() != self.path.parent
                or (parent.st_dev, parent.st_ino) != self.parent_identity
                or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (os.name != 'nt' and (parent.st_uid != os.geteuid() or parent.st_mode & 0o077
                    or info.st_uid != os.geteuid() or info.st_mode & 0o077))):
            raise ImportConfirmationStoreHeld('private_store_required')
        return info.st_dev, info.st_ino

    @contextmanager
    def _connect(self):
        if self._check_file() != self.file_identity:
            raise ImportConfirmationStoreHeld('store_identity_changed')
        try:
            db = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True,
                                 timeout=5, isolation_level=None)
        except sqlite3.Error:
            raise ImportConfirmationStoreHeld('store_unavailable') from None
        try:
            if self._check_file() != self.file_identity:
                raise ImportConfirmationStoreHeld('store_identity_changed')
            db.row_factory = sqlite3.Row
            db.execute('PRAGMA synchronous=FULL')
            if db.execute('PRAGMA journal_mode').fetchone()[0].lower() not in ('delete', 'wal'):
                raise ImportConfirmationStoreHeld('durable_journal_required')
            yield db
        finally:
            db.close()

    @staticmethod
    def _check_schema(db):
        columns = [(row['name'], row['type'], row['notnull'], row['pk'])
                   for row in db.execute(f'PRAGMA table_info({_TABLE})')]
        if columns != [('job_id', 'TEXT', 1, 1), ('generation', 'INTEGER', 1, 2),
                       ('environment', 'TEXT', 1, 3), ('confirmation_id', 'TEXT', 1, 0),
                       ('binding_sha256', 'TEXT', 1, 0), ('confirmation_sha256', 'TEXT', 1, 0),
                       ('canonical_bytes', 'BLOB', 1, 0)]:
            raise ImportConfirmationStoreHeld('store_schema_changed')
        indexes = list(db.execute(f'PRAGMA index_list({_TABLE})'))
        unique_columns = set()
        for index in indexes:
            if index['unique'] and not index['partial']:
                # Index names come from SQLite schema, never from a request.
                index_name = index['name'].replace('"', '""')
                unique_columns.add(tuple(row['name'] for row in db.execute(f'PRAGMA index_info("{index_name}")')))
        if (('confirmation_id',) not in unique_columns
                or ('job_id', 'generation', 'environment') not in unique_columns
                or db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 1", (_TABLE,)).fetchone()):
            raise ImportConfirmationStoreHeld('store_schema_changed')

    def _trusted(self, db, scope):
        # The callback is trusted application code. A shallow scope copy prevents
        # accidental mutation; the returned object is snapshotted and bounded.
        read_actions = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION}
        if hasattr(sqlite3, 'SQLITE_RECURSIVE'):
            read_actions.add(sqlite3.SQLITE_RECURSIVE)
        db.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK if action in read_actions else sqlite3.SQLITE_DENY)
        try:
            loaded = self.loader(db, dict(scope))
        except Exception:
            raise ImportConfirmationStoreHeld('trusted_binding_unavailable') from None
        finally:
            db.set_authorizer(None)
        if not db.in_transaction:
            raise ImportConfirmationStoreHeld('trusted_transaction_lost')
        try:
            context, = _snapshot_inputs(loaded)
        except ImportConfirmationRejected:
            raise ImportConfirmationStoreHeld('trusted_binding_invalid') from None
        if type(context) is not dict or set(context) != _CONTEXT or type(context['expected']) is not dict:
            raise ImportConfirmationStoreHeld('trusted_binding_invalid')
        if any(type(context['expected'].get(field)) is not type(value)
               or context['expected'].get(field) != value for field, value in scope.items()):
            raise ImportConfirmationStoreHeld('trusted_scope_mismatch')
        # Pin registries may rotate; the binding records the trusted job/copy
        # identity, and every read/replay re-verifies against current pins.
        binding = {'expected': context['expected'], 'resultManifest': context['result_manifest'],
                   'acceptedEnvelopes': context['accepted_envelopes']}
        return context, protocol.digest(binding)

    @staticmethod
    def _row(db, scope):
        # Do not materialize an arbitrarily large/corrupt BLOB into Python.
        return db.execute(f'''SELECT
            CASE WHEN typeof(confirmation_id)='text' AND length(confirmation_id) BETWEEN 1 AND 128
                 THEN confirmation_id ELSE NULL END AS confirmation_id,
            CASE WHEN typeof(binding_sha256)='text' AND length(binding_sha256)=64
                 THEN binding_sha256 ELSE NULL END AS binding_sha256,
            CASE WHEN typeof(confirmation_sha256)='text' AND length(confirmation_sha256)=64
                 THEN confirmation_sha256 ELSE NULL END AS confirmation_sha256,
            CASE WHEN typeof(canonical_bytes)='blob' AND length(canonical_bytes) BETWEEN 1 AND ?
                 THEN canonical_bytes ELSE NULL END AS canonical_bytes
            FROM {_TABLE} WHERE job_id=? AND generation=? AND environment=?''',
            (MAX_INPUT_BYTES, scope['jobId'], scope['generation'], scope['environment'])).fetchone()

    @staticmethod
    def _verify(confirmation, context, prior=None):
        try:
            return verify_import_confirmation(confirmation, trusted_import_keys=context['trusted_import_keys'],
                trusted_copy_keys=context['trusted_copy_keys'], expected=context['expected'],
                result_manifest=context['result_manifest'], accepted_envelopes=context['accepted_envelopes'],
                prior_confirmation_sha256=prior)
        except (ImportConfirmationRejected, TypeError, ValueError):
            raise ImportConfirmationStoreHeld('confirmation_unverified_or_changed') from None

    @staticmethod
    def _compare(row, verified, binding_sha):
        raw = row['canonical_bytes']
        if (type(raw) is not bytes or len(raw) > MAX_INPUT_BYTES
                or type(row['confirmation_sha256']) is not str or not _HASH.fullmatch(row['confirmation_sha256'])
                or type(row['binding_sha256']) is not str or not _HASH.fullmatch(row['binding_sha256'])
                or not hmac.compare_digest(row['binding_sha256'], binding_sha)
                or not hmac.compare_digest(row['confirmation_sha256'], verified.sha256)
                or not hmac.compare_digest(raw, verified.canonical_bytes)
                or hashlib.sha256(raw).hexdigest() != row['confirmation_sha256']
                or row['confirmation_id'] != verified.confirmation['confirmationId']):
            raise ImportConfirmationStoreHeld('stored_confirmation_conflict')

    def record(self, scope, confirmation):
        """Verify raw decoded confirmation and commit first exact signed bytes.

        An identical replay is accepted only after reloading the trusted binding
        and verifying signatures with currently trusted keys. A caller-supplied
        VerifiedImportConfirmation object is not accepted as proof. This method
        never releases a cleanup hold or starts a worker.
        """
        try:
            scope, confirmation = _snapshot_inputs(scope, confirmation)
        except ImportConfirmationRejected:
            raise ImportConfirmationStoreHeld('input_invalid') from None
        _scope(scope)
        with self._connect() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                self._check_schema(db)
                context, binding_sha = self._trusted(db, scope)
                row = self._row(db, scope)
                prior = row['confirmation_sha256'] if row else None
                verified = self._verify(confirmation, context, prior=prior)
                if row:
                    self._compare(row, verified, binding_sha)
                    result = RecordResult('replayed', verified.sha256)
                else:
                    try:
                        db.execute(f'''INSERT INTO {_TABLE} VALUES (?,?,?,?,?,?,?)''',
                            (scope['jobId'], scope['generation'], scope['environment'],
                             verified.confirmation['confirmationId'], binding_sha, verified.sha256,
                             verified.canonical_bytes))
                    except sqlite3.IntegrityError:
                        raise ImportConfirmationStoreHeld('confirmation_identity_conflict') from None
                    _after_insert()
                    result = RecordResult('recorded', verified.sha256)
                db.commit()
                _after_commit()
                return result
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise

    def load_verified(self, scope):
        """Reverify a retained proof against current trusted state, or return None."""
        try:
            scope, = _snapshot_inputs(scope)
        except ImportConfirmationRejected:
            raise ImportConfirmationStoreHeld('input_invalid') from None
        _scope(scope)
        with self._connect() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                self._check_schema(db)
                context, binding_sha = self._trusted(db, scope)
                row = self._row(db, scope)
                if row is None:
                    db.commit()
                    return None
                raw = row['canonical_bytes']
                if type(raw) is not bytes or len(raw) > MAX_INPUT_BYTES:
                    raise ImportConfirmationStoreHeld('stored_confirmation_invalid')
                digest = row['confirmation_sha256']
                if (type(digest) is not str or not _HASH.fullmatch(digest)
                        or not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), digest)):
                    raise ImportConfirmationStoreHeld('stored_confirmation_conflict')
                try:
                    confirmation = json.loads(raw, object_pairs_hook=_reject_duplicate_pairs,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('invalid JSON number')))
                except (ValueError, TypeError, UnicodeError, RecursionError):
                    raise ImportConfirmationStoreHeld('stored_confirmation_invalid') from None
                verified = self._verify(confirmation, context, prior=row['confirmation_sha256'])
                self._compare(row, verified, binding_sha)
                db.commit()
                return verified
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
