"""Disabled offline reference claim fence for uncertain remote mutations.

Started remote intents block claim and phase edits after session loss. There is
no automated unblocking or authenticated stop-proof protocol in this candidate.
"""
DDL = "CREATE TABLE reference_remote_fence_required (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))"
TRIGGERS = {
    'remote_reference_job_update': "CREATE TRIGGER remote_reference_job_update BEFORE UPDATE ON reference_jobs WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND (job_id=OLD.job_id OR job_id=NEW.job_id)) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
    'remote_reference_job_delete': "CREATE TRIGGER remote_reference_job_delete BEFORE DELETE ON reference_jobs WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
    'remote_reference_job_insert': "CREATE TRIGGER remote_reference_job_insert BEFORE INSERT ON reference_jobs WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
    'remote_reference_claim_update': "CREATE TRIGGER remote_reference_claim_update BEFORE UPDATE ON reference_claims WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND (job_id=OLD.job_id OR job_id=NEW.job_id)) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
    'remote_reference_claim_delete': "CREATE TRIGGER remote_reference_claim_delete BEFORE DELETE ON reference_claims WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
    'remote_reference_claim_insert': "CREATE TRIGGER remote_reference_claim_insert BEFORE INSERT ON reference_claims WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'remote reference intent held'); END",
}


class RemoteFenceHeld(ValueError): pass


def attest(db):
    marker=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_remote_fence_required'").fetchone()
    if marker is None or marker[0] != DDL or db.execute('SELECT COUNT(*) FROM reference_remote_fence_required WHERE singleton=1 AND required=1').fetchone()[0] != 1:
        raise RemoteFenceHeld('reference remote fence unavailable')
    for name,sql in TRIGGERS.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if row is None or row[0] != sql:raise RemoteFenceHeld('reference remote fence unavailable')


def install_offline(db, *, enabled=False):
    if enabled is not True or db.in_transaction:raise RemoteFenceHeld('offline empty installation required')
    db.execute('BEGIN IMMEDIATE')
    try:
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held')").fetchone() or db.execute('SELECT 1 FROM broker_intents').fetchone():
            raise RemoteFenceHeld('offline empty installation required')
        db.execute(DDL)
        db.execute('INSERT INTO reference_remote_fence_required VALUES(1,1)')
        for sql in TRIGGERS.values():db.execute(sql)
        attest(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise
