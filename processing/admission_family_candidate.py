"""Unmounted admission-family evidence candidate; not a migration or runtime gate.

Caller must hold the actual single-writer authority and use its existing SQLite
write transaction. Markers and their checked rows are trusted only within that
boundary; a writer able to rewrite the database can forge them. No existing
jobs are classified by this module.
"""
import sqlite3

from . import reference_protocol as p

SCHEMA = """CREATE TABLE reference_admission_family_candidate (
 job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id),
 authority_id TEXT NOT NULL, generation INTEGER NOT NULL, environment TEXT NOT NULL,
 request_digest TEXT NOT NULL, family TEXT NOT NULL CHECK(family IN ('legacy-v1','prepared-v3')),
 principal_id TEXT, attempt_id TEXT, envelope_sha256 TEXT)"""


class FamilyHeld(ValueError):
    pass


def initialize_empty(db):
    """Initialize only a demonstrably empty reference queue, never migrate rows."""
    if not isinstance(db, sqlite3.Connection) or not db.in_transaction:
        raise FamilyHeld('owned write transaction required')
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_admission_family_candidate'").fetchone():
        raise FamilyHeld('new empty schema required')
    if db.execute('SELECT 1 FROM reference_jobs LIMIT 1').fetchone():
        raise FamilyHeld('existing jobs require offline provenance migration')
    db.execute(SCHEMA)


def _snapshot(db, job_id, authority_id, family):
    if not isinstance(db, sqlite3.Connection) or not db.in_transaction:
        raise FamilyHeld('owned transaction required')
    try:
        p._uuid(job_id, 'job ID'); p._uuid(authority_id, 'authority ID')
        auth = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchall()
        if [tuple(item) for item in auth] != [(authority_id,)]:
            raise ValueError('authority mismatch')
        row = db.execute('SELECT generation,environment,request_digest FROM reference_jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is None: raise ValueError('job missing')
        generation, environment, digest = row
        p._integer(generation, 'generation'); p._environment(environment); p._hash(digest, 'request digest')
        owner = db.execute('SELECT principal_id,environment,request_digest,protocol FROM reference_viewer_submission_owners WHERE job_id=?', (job_id,)).fetchone()
        envelope = db.execute('SELECT principal_id,environment,viewer_attempt_id,envelope_sha256 FROM reference_viewer_envelopes_v2 WHERE job_id=?', (job_id,)).fetchone()
        activation = db.execute('SELECT 1 FROM reference_viewer_activation WHERE job_id=?', (job_id,)).fetchone()
        policy = db.execute('SELECT 1 FROM reference_viewer_v3_policy WHERE job_id=?', (job_id,)).fetchone()
        if family == 'legacy-v1':
            if owner or envelope or activation or policy: raise ValueError('legacy has prepared evidence')
            evidence = (None, None, None)
        elif family == 'prepared-v3':
            if not owner or not envelope or not policy: raise ValueError('prepared evidence missing')
            principal, owner_env, owner_digest, protocol = owner
            ep, ee, attempt, envelope_digest = envelope
            p._slug(principal, 'principal'); p._uuid(attempt, 'Viewer attempt'); p._hash(envelope_digest, 'envelope digest')
            if (protocol != 'ltds-viewer-prepared-envelope-jcs-v3' or owner_env != environment
                    or owner_digest != digest or ep != principal or ee != environment):
                raise ValueError('prepared evidence mismatch')
            evidence = (principal, attempt, envelope_digest)
        else:
            raise ValueError('unknown family')
        return (job_id, authority_id, generation, environment, digest, family, *evidence)
    except (sqlite3.Error, ValueError, TypeError):
        raise FamilyHeld('admission family evidence unavailable') from None


def admit_new_job(db, job_id, authority_id, family, create):
    """Atomically create a new job and marker; never classify a prior row.

    create is trusted internal admission code and must write the complete job,
    owner, envelope and policy using this exact connection, without committing.
    """
    if not isinstance(db, sqlite3.Connection) or db.in_transaction or not callable(create):
        raise FamilyHeld('fresh owned transaction required')
    try:
        db.execute('BEGIN IMMEDIATE')
        if db.execute('SELECT 1 FROM reference_jobs WHERE job_id=?', (job_id,)).fetchone():
            raise FamilyHeld('existing job requires offline provenance migration')
        create(db)
        if not db.in_transaction:
            raise FamilyHeld('admission transaction was not retained')
        expected = _snapshot(db, job_id, authority_id, family)
        db.execute('INSERT INTO reference_admission_family_candidate VALUES(?,?,?,?,?,?,?,?,?)', expected)
        db.commit()
    except Exception:
        if db.in_transaction: db.rollback()
        raise


def validate_family(db, job_id, authority_id):
    """Check exact evidence; missing or changed rows never imply legacy."""
    try:
        row = db.execute('SELECT job_id,authority_id,generation,environment,request_digest,family,principal_id,attempt_id,envelope_sha256 FROM reference_admission_family_candidate WHERE job_id=?', (job_id,)).fetchone()
        if row is None or tuple(row) != _snapshot(db, job_id, authority_id, row[5]):
            raise FamilyHeld('admission family unavailable')
        return row[5]
    except (sqlite3.Error, IndexError, TypeError):
        raise FamilyHeld('admission family unavailable') from None
