"""Durable source-ingress mode; this does not authorize direct intake.

The caller creates the job and claims its mode in the same write transaction.
Old installations are enrolled as spool only at the one-time migration.
"""
from .maintenance import Conflict

TABLE = 'reference_source_intake_modes'
CREATE = """CREATE TABLE reference_source_intake_modes (
    job_id TEXT NOT NULL PRIMARY KEY REFERENCES reference_source_intakes(job_id),
    mode TEXT NOT NULL CHECK(mode IN ('spool-v1','direct-v2')))"""
TRIGGERS = {
    'source_intake_mode_no_replace': "CREATE TRIGGER source_intake_mode_no_replace BEFORE INSERT ON reference_source_intake_modes WHEN EXISTS (SELECT 1 FROM reference_source_intake_modes WHERE job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'source intake mode immutable'); END",
    'source_intake_mode_no_update': "CREATE TRIGGER source_intake_mode_no_update BEFORE UPDATE ON reference_source_intake_modes BEGIN SELECT RAISE(ABORT,'source intake mode immutable'); END",
    'source_intake_mode_no_delete': "CREATE TRIGGER source_intake_mode_no_delete BEFORE DELETE ON reference_source_intake_modes BEGIN SELECT RAISE(ABORT,'source intake mode immutable'); END",
}

def _normalize(sql):
    return ' '.join(sql.split())

def _check_schema(db):
    row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone()
    if row is None or _normalize(row['sql']) != _normalize(CREATE):
        raise Conflict('source intake mode schema changed')
    for name, expected in TRIGGERS.items():
        row = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)).fetchone()
        if row is None or _normalize(row['sql']) != _normalize(expected):
            raise Conflict('source intake mode guards changed')

def initialize(db):
    """Create and backfill only once; callers hold the startup transaction."""
    if not db.in_transaction:
        db.execute('BEGIN IMMEDIATE')
    columns = {row['name'] for row in db.execute('PRAGMA table_info(coordinator_identity)')}
    if 'source_mode_version' not in columns:
        db.execute('ALTER TABLE coordinator_identity ADD COLUMN source_mode_version INTEGER NOT NULL DEFAULT 0')
    marker = db.execute('SELECT source_mode_version FROM coordinator_identity WHERE singleton=1').fetchone()
    if marker is None or type(marker[0]) is not int or marker[0] not in (0, 1):
        raise Conflict('source intake mode migration marker invalid')
    exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone() is not None
    if (marker[0] == 1) != exists:
        raise Conflict('source intake mode migration marker mismatch')
    if not exists:
        db.execute(CREATE)
        db.execute("INSERT INTO reference_source_intake_modes SELECT job_id,'spool-v1' FROM reference_source_intakes")
        for sql in TRIGGERS.values():
            db.execute(sql)
        db.execute('UPDATE coordinator_identity SET source_mode_version=1 WHERE singleton=1 AND source_mode_version=0')
    _check_schema(db)
    if db.execute('''SELECT 1 FROM reference_source_intakes s LEFT JOIN reference_source_intake_modes m
        ON m.job_id=s.job_id WHERE m.job_id IS NULL LIMIT 1''').fetchone():
        raise Conflict('source intake mode missing')
    if db.execute('''SELECT 1 FROM reference_source_intake_modes m LEFT JOIN reference_source_intakes s
        ON m.job_id=s.job_id WHERE s.job_id IS NULL OR m.mode NOT IN ('spool-v1','direct-v2') LIMIT 1''').fetchone():
        raise Conflict('source intake mode orphaned')

def claim(db, job_id, mode):
    """Claim on the caller's write transaction, never switching an existing mode."""
    if not db.in_transaction or mode not in ('spool-v1', 'direct-v2'):
        raise Conflict('source intake mode transaction required')
    _check_schema(db)
    row = db.execute('SELECT mode FROM reference_source_intake_modes WHERE job_id=?', (job_id,)).fetchone()
    if row is None:
        if db.execute('SELECT 1 FROM reference_source_intakes WHERE job_id=?', (job_id,)).fetchone() is None:
            raise Conflict('source intake mode parent missing')
        db.execute('INSERT INTO reference_source_intake_modes(job_id,mode) VALUES(?,?)', (job_id, mode))
    elif row['mode'] != mode:
        raise Conflict('source intake mode already claimed')

def require(db, job_id, mode):
    _check_schema(db)
    row = db.execute('SELECT mode FROM reference_source_intake_modes WHERE job_id=?', (job_id,)).fetchone()
    if row is None or row['mode'] != mode:
        raise Conflict('source intake mode mismatch')
