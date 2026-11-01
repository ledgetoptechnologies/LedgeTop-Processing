"""Disabled experiment: atomically create a job and an attempt binding.

This is not an authenticated endpoint. The caller-provided principal is
synthetic and is NOT a proof of identity. A durable issuer, revocation,
anti-rollback, writer fencing, migration and v1 bypass controls are absent.
Never enable this module in a running system on the basis of these tests.
"""
import sqlite3
import uuid

from .maintenance import Conflict
from . import reference_protocol as protocol
from .reference_store import ReferenceStore, _json

QUALIFIED = False

_DDL = '''CREATE TABLE reference_owned_binding_candidate (
  binding_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL UNIQUE,
  submission_id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL UNIQUE,
  generation INTEGER NOT NULL, environment TEXT NOT NULL,
  manifest_sha TEXT NOT NULL, pipeline_digest TEXT NOT NULL,
  principal_id TEXT NOT NULL)'''
_PROFILE_DDL = '''CREATE TABLE reference_owned_profile_candidate (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), pipeline_digest TEXT NOT NULL)'''
_PROFILE_GUARDS = (
    "CREATE TRIGGER reference_owned_profile_no_update BEFORE UPDATE ON reference_owned_profile_candidate BEGIN SELECT RAISE(ABORT,'profile immutable'); END",
    "CREATE TRIGGER reference_owned_profile_no_delete BEFORE DELETE ON reference_owned_profile_candidate BEGIN SELECT RAISE(ABORT,'profile immutable'); END",
    "CREATE TRIGGER reference_owned_profile_no_insert BEFORE INSERT ON reference_owned_profile_candidate BEGIN SELECT RAISE(ABORT,'profile immutable'); END",
)


def _uuid4(value):
    if type(value) is not str:
        raise ValueError('canonical UUIDv4 required')
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        raise ValueError('canonical UUIDv4 required') from None
    if str(parsed) != value or parsed.version != 4 or parsed.variant != uuid.RFC_4122:
        raise ValueError('canonical UUIDv4 required')
    return value


def _enabled():
    if QUALIFIED is not True:
        raise Conflict('owned binding candidate is not qualified')


def install_empty_candidate(store, *, synthetic_pipeline_digest):
    """Test-only empty install. Not a trusted registry or live provisioning."""
    _enabled()
    if not isinstance(store, ReferenceStore):
        raise ValueError('reference store required')
    protocol._hash(synthetic_pipeline_digest, 'synthetic pipeline digest')
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute('SELECT 1 FROM reference_jobs LIMIT 1').fetchone():
            raise Conflict('candidate install requires empty reference state')
        db.execute(_DDL)
        db.execute(_PROFILE_DDL)
        db.execute('INSERT INTO reference_owned_profile_candidate VALUES (1,?)', (synthetic_pipeline_digest,))
        for guard in _PROFILE_GUARDS:
            db.execute(guard)


def submit_bound_candidate(store, validated, *, binding_id, attempt_id, principal_id):
    """Own the entire transaction; return only after commit, never expose db.

    `principal_id` is supplied by the test caller and is not authentication.
    No claim about Viewer active attempts or authority is made by this method.
    """
    _enabled()
    if not isinstance(store, ReferenceStore):
        raise ValueError('reference store required')
    binding_id, attempt_id, principal_id = map(_uuid4, (binding_id, attempt_id, principal_id))
    checked = store._validate_submission(validated)
    request = _json(checked)
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        schema = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                            ('reference_owned_binding_candidate',)).fetchone()
        if schema is None or schema[0] != _DDL:
            raise Conflict('candidate schema requires inspection')
        expected_schema = {'reference_owned_profile_candidate': _PROFILE_DDL}
        expected_schema.update({item.split()[2]: item for item in _PROFILE_GUARDS})
        rows = db.execute("SELECT name,sql FROM sqlite_master WHERE name IN (?,?,?,?)", tuple(expected_schema)).fetchall()
        if {row['name']: row['sql'] for row in rows} != expected_schema:
            raise Conflict('candidate profile schema requires inspection')
        profile = db.execute('SELECT pipeline_digest FROM reference_owned_profile_candidate WHERE singleton=1').fetchall()
        if len(profile) != 1 or profile[0][0] != checked['pipelineDigest']:
            raise Conflict('submission does not match the owned pipeline profile')
        # No exception from this method is caught while the transaction is
        # live: any failed validation or binding insert aborts all writes.
        job, created = store._submit_owned_transaction(db, checked)
        source = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (job['jobId'],)).fetchone()
        if source is None or source[0] != request or job['generation'] != 1:
            raise Conflict('reference job identity requires inspection')
        values = (binding_id, attempt_id, checked['submissionId'], job['jobId'], job['generation'],
                  checked['environment'], checked['manifestSha256'], checked['pipelineDigest'],
                  principal_id)
        old = db.execute('SELECT * FROM reference_owned_binding_candidate WHERE submission_id=?',
                         (checked['submissionId'],)).fetchone()
        if old is None:
            if not created:
                raise Conflict('existing unbound v1 submission requires inspection')
            try:
                db.execute('INSERT INTO reference_owned_binding_candidate VALUES (?,?,?,?,?,?,?,?,?)', values)
            except sqlite3.IntegrityError:
                raise Conflict('attempt or binding already belongs to another job') from None
        elif created or tuple(old) != values:
            raise Conflict('submission binding differs')
        return job, created
