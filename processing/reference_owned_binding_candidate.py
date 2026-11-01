"""Disabled experiment: atomically create a job and an attempt binding.

This is not an authenticated endpoint. The test-only credential registry is
not protected provisioning, an anti-rollback authority, a writer fence, or
an exclusion of v1 bypass paths. Never enable this module in a running system
on the basis of these tests.
"""
import hashlib
import hmac
import sqlite3
import uuid

from .maintenance import Conflict
from . import reference_protocol as protocol
from .reference_store import ReferenceStore, _json

QUALIFIED = False

_DDL = '''CREATE TABLE reference_owned_binding_candidate (
  binding_id TEXT PRIMARY KEY NOT NULL, attempt_id TEXT NOT NULL UNIQUE,
  submission_id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL UNIQUE,
  generation INTEGER NOT NULL, environment TEXT NOT NULL,
  manifest_sha TEXT NOT NULL, pipeline_digest TEXT NOT NULL,
  principal_id TEXT NOT NULL, first_revision INTEGER NOT NULL)'''
_PROFILE_DDL = '''CREATE TABLE reference_owned_profile_candidate (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), pipeline_digest TEXT NOT NULL)'''
_PRINCIPAL_DDL = '''CREATE TABLE reference_owned_principal_candidate (
  principal_id TEXT PRIMARY KEY NOT NULL, environment TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','revoked')))'''
_CREDENTIAL_DDL = '''CREATE TABLE reference_owned_credential_candidate (
  principal_id TEXT NOT NULL, revision INTEGER NOT NULL,
  token_sha256 BLOB NOT NULL UNIQUE, status TEXT NOT NULL CHECK(status IN ('active','revoked')),
  PRIMARY KEY(principal_id,revision))'''
_BINDING_GUARDS = (
    "CREATE TRIGGER reference_owned_binding_no_update BEFORE UPDATE ON reference_owned_binding_candidate BEGIN SELECT RAISE(ABORT,'binding immutable'); END",
    "CREATE TRIGGER reference_owned_binding_no_delete BEFORE DELETE ON reference_owned_binding_candidate BEGIN SELECT RAISE(ABORT,'binding immutable'); END",
    "CREATE TRIGGER reference_owned_binding_no_replacement BEFORE INSERT ON reference_owned_binding_candidate WHEN EXISTS (SELECT 1 FROM reference_owned_binding_candidate WHERE binding_id IS NEW.binding_id OR attempt_id IS NEW.attempt_id OR submission_id IS NEW.submission_id OR job_id IS NEW.job_id) BEGIN SELECT RAISE(ABORT,'binding immutable'); END",
)
_IDENTITY_GUARDS = (
    "CREATE TRIGGER reference_owned_principal_no_insert BEFORE INSERT ON reference_owned_principal_candidate BEGIN SELECT RAISE(ABORT,'principal provisioning disabled'); END",
    "CREATE TRIGGER reference_owned_principal_no_delete BEFORE DELETE ON reference_owned_principal_candidate BEGIN SELECT RAISE(ABORT,'principal immutable'); END",
    "CREATE TRIGGER reference_owned_principal_monotonic BEFORE UPDATE ON reference_owned_principal_candidate WHEN NOT (OLD.status IS 'active' AND NEW.status IS 'revoked' AND OLD.principal_id IS NEW.principal_id AND OLD.environment IS NEW.environment) BEGIN SELECT RAISE(ABORT,'principal immutable'); END",
    "CREATE TRIGGER reference_owned_credential_no_delete BEFORE DELETE ON reference_owned_credential_candidate BEGIN SELECT RAISE(ABORT,'credential immutable'); END",
    "CREATE TRIGGER reference_owned_credential_monotonic BEFORE UPDATE ON reference_owned_credential_candidate WHEN NOT (OLD.status IS 'active' AND NEW.status IS 'revoked' AND OLD.principal_id IS NEW.principal_id AND OLD.revision IS NEW.revision AND OLD.token_sha256 IS NEW.token_sha256) BEGIN SELECT RAISE(ABORT,'credential immutable'); END",
    "CREATE TRIGGER reference_owned_credential_order BEFORE INSERT ON reference_owned_credential_candidate WHEN NEW.status IS NOT 'active' OR typeof(NEW.revision) IS NOT 'integer' OR NEW.revision<1 OR NEW.revision>9007199254740991 OR typeof(NEW.token_sha256) IS NOT 'blob' OR length(NEW.token_sha256) IS NOT 32 OR NOT EXISTS (SELECT 1 FROM reference_owned_principal_candidate WHERE principal_id IS NEW.principal_id AND status IS 'active') OR NEW.revision IS NOT (SELECT COALESCE(MAX(revision),0)+1 FROM reference_owned_credential_candidate WHERE principal_id IS NEW.principal_id) OR EXISTS (SELECT 1 FROM reference_owned_credential_candidate WHERE token_sha256 IS NEW.token_sha256) BEGIN SELECT RAISE(ABORT,'credential immutable'); END",
)
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


def _credential_digest(token):
    if type(token) is not bytes or not 32 <= len(token) <= 128:
        raise ValueError('credential must be 32 to 128 secret bytes')
    return hashlib.sha256(token).digest()


def _revision(value):
    if type(value) is not int or not 1 <= value <= 9007199254740991:
        raise ValueError('invalid credential revision')
    return value


def install_empty_candidate(store, *, synthetic_pipeline_digest, synthetic_principal_id,
                            synthetic_environment, synthetic_credential):
    """Test-only empty install. Not a trusted registry or live provisioning."""
    _enabled()
    if not isinstance(store, ReferenceStore):
        raise ValueError('reference store required')
    protocol._hash(synthetic_pipeline_digest, 'synthetic pipeline digest')
    synthetic_principal_id = _uuid4(synthetic_principal_id)
    if synthetic_environment not in ('production', 'staging'):
        raise ValueError('invalid environment')
    digest = _credential_digest(synthetic_credential)
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        if db.execute('SELECT 1 FROM reference_jobs LIMIT 1').fetchone():
            raise Conflict('candidate install requires empty reference state')
        db.execute(_DDL)
        for guard in _BINDING_GUARDS:
            db.execute(guard)
        db.execute(_PROFILE_DDL)
        db.execute(_PRINCIPAL_DDL)
        db.execute(_CREDENTIAL_DDL)
        db.execute('INSERT INTO reference_owned_principal_candidate VALUES (?,?,?)',
                   (synthetic_principal_id, synthetic_environment, 'active'))
        db.execute('INSERT INTO reference_owned_credential_candidate VALUES (?,?,?,?)',
                   (synthetic_principal_id, 1, digest, 'active'))
        for guard in _IDENTITY_GUARDS:
            db.execute(guard)
        db.execute('INSERT INTO reference_owned_profile_candidate VALUES (1,?)', (synthetic_pipeline_digest,))
        for guard in _PROFILE_GUARDS:
            db.execute(guard)


def _check_schema(db):
    expected_schema = {'reference_owned_binding_candidate': _DDL,
                       'reference_owned_profile_candidate': _PROFILE_DDL,
                       'reference_owned_principal_candidate': _PRINCIPAL_DDL,
                       'reference_owned_credential_candidate': _CREDENTIAL_DDL}
    expected_schema.update({item.split()[2]: item for item in _PROFILE_GUARDS + _BINDING_GUARDS + _IDENTITY_GUARDS})
    marks = ','.join('?' for _ in expected_schema)
    rows = db.execute('SELECT name,sql FROM sqlite_master WHERE name IN ('+marks+')', tuple(expected_schema)).fetchall()
    if {row['name']: row['sql'] for row in rows} != expected_schema:
        raise Conflict('candidate schema requires inspection')


def submit_bound_candidate(store, validated, *, binding_id, attempt_id, credential):
    """Own the entire transaction; return only after commit, never expose db.

    Resolve the supplied secret to a server-side principal under the same
    transaction as creation. Registry and credentials are test-only; no claim
    about protected issuance, identity, Viewer active attempts or authority.
    """
    _enabled()
    if not isinstance(store, ReferenceStore):
        raise ValueError('reference store required')
    binding_id, attempt_id = map(_uuid4, (binding_id, attempt_id))
    digest = _credential_digest(credential)
    checked = store._validate_submission(validated)
    request = _json(checked)
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        _check_schema(db)
        cred = db.execute('''SELECT c.token_sha256,c.revision,c.status AS credential_status,
          p.principal_id,p.environment,p.status AS principal_status
          FROM reference_owned_credential_candidate c
          JOIN reference_owned_principal_candidate p ON p.principal_id=c.principal_id
          WHERE c.token_sha256=?''', (digest,)).fetchone()
        if (cred is None or not hmac.compare_digest(cred['token_sha256'], digest)
            or cred['credential_status'] != 'active' or cred['principal_status'] != 'active'
            or cred['environment'] != checked['environment']):
            raise Conflict('credential is not authorized')
        principal_id = cred['principal_id']
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
            first_revision = _revision(cred['revision'])
            try:
                db.execute('INSERT INTO reference_owned_binding_candidate VALUES (?,?,?,?,?,?,?,?,?,?)', values + (first_revision,))
            except sqlite3.IntegrityError:
                raise Conflict('attempt or binding already belongs to another job') from None
        else:
            if created or tuple(old)[:-1] != values:
                raise Conflict('submission binding differs')
            try:
                first_revision = _revision(old['first_revision'])
            except ValueError:
                raise Conflict('first credential revision requires inspection') from None
            if not db.execute('SELECT 1 FROM reference_owned_credential_candidate WHERE principal_id=? AND revision=?',
                              (principal_id, first_revision)).fetchone():
                raise Conflict('first credential revision requires inspection')
        binding = {'bindingId': binding_id, 'attemptId': attempt_id, 'jobId': job['jobId'],
                   'generation': job['generation'], 'environment': checked['environment'],
                   'inputManifestSha256': checked['manifestSha256'],
                   'pipelineDigest': checked['pipelineDigest'], 'principalId': principal_id,
                   'firstCredentialRevision': first_revision}
        return {'binding': binding, 'created': created, 'currentJob': job}


def rotate_candidate(store, *, principal_id, expected_revision, new_revision, new_credential):
    """Synthetic operator operation; new credential overlaps the old one."""
    _enabled()
    principal_id = _uuid4(principal_id)
    expected_revision, new_revision = _revision(expected_revision), _revision(new_revision)
    digest = _credential_digest(new_credential)
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        _check_schema(db)
        principal = db.execute('SELECT status FROM reference_owned_principal_candidate WHERE principal_id=?', (principal_id,)).fetchone()
        current = db.execute('SELECT MAX(revision) FROM reference_owned_credential_candidate WHERE principal_id=?', (principal_id,)).fetchone()[0]
        if principal is None or principal[0] != 'active' or current != expected_revision or new_revision != current + 1:
            raise Conflict('credential revision requires inspection')
        try:
            db.execute('INSERT INTO reference_owned_credential_candidate VALUES (?,?,?,?)',
                       (principal_id, new_revision, digest, 'active'))
        except sqlite3.IntegrityError:
            raise Conflict('credential or revision already used') from None


def revoke_candidate(store, *, principal_id, revision=None):
    """Synthetic operator revoke. Revoked principal is never reactivated here."""
    _enabled()
    principal_id = _uuid4(principal_id)
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        _check_schema(db)
        if revision is None:
            changed = db.execute("UPDATE reference_owned_principal_candidate SET status='revoked' WHERE principal_id=? AND status='active'", (principal_id,)).rowcount
        else:
            changed = db.execute("UPDATE reference_owned_credential_candidate SET status='revoked' WHERE principal_id=? AND revision=? AND status='active'", (principal_id, _revision(revision))).rowcount
        if changed != 1:
            raise Conflict('credential or principal requires inspection')
