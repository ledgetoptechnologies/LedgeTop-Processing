"""Inactive migration experiment: hold coordinator state during uncertain upload.

The transfer journal and these triggers must be installed offline before use.
This is not a physical engine fence and cannot stop an older schema writer from
dropping triggers or modifying the transfer journal directly.
"""
import sqlite3

from .engine_ingress_guard import STREAM_DDL

JOBS_DDL = '''CREATE TABLE jobs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                request_id TEXT UNIQUE NOT NULL, environment TEXT NOT NULL,
                profile TEXT NOT NULL, status TEXT NOT NULL, worker_id TEXT,
                claim_hash TEXT, result TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP, node_id TEXT, held_reason TEXT)'''
_JOBS_SCHEMA = "(SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs') IS '" + JOBS_DDL.replace("'", "''") + "'"

TRIGGERS = {
    'guard_started_job_update': "CREATE TRIGGER guard_started_job_update BEFORE UPDATE ON jobs WHEN (EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE (job_id=OLD.id OR job_id=NEW.id) AND state='started')) AND NOT (" + _JOBS_SCHEMA + " AND OLD.status IS 'running' AND NEW.status IS 'held' AND (NEW.held_reason IS 'coordinator_restart' OR NEW.held_reason IS 'transfer_uncertain') AND NEW.seq IS OLD.seq AND NEW.id IS OLD.id AND NEW.request_id IS OLD.request_id AND NEW.environment IS OLD.environment AND NEW.profile IS OLD.profile AND NEW.worker_id IS OLD.worker_id AND NEW.node_id IS OLD.node_id AND NEW.claim_hash IS OLD.claim_hash AND NEW.result IS OLD.result AND NEW.created_at IS OLD.created_at) BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_job_insert': "CREATE TRIGGER guard_started_job_insert BEFORE INSERT ON jobs WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=NEW.id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_job_delete': "CREATE TRIGGER guard_started_job_delete BEFORE DELETE ON jobs WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_task_update': "CREATE TRIGGER guard_started_task_update BEFORE UPDATE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE (job_id=OLD.job_id OR job_id=NEW.job_id) AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_task_insert': "CREATE TRIGGER guard_started_task_insert BEFORE INSERT ON viewer_tasks WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=NEW.job_id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_task_delete': "CREATE TRIGGER guard_started_task_delete BEFORE DELETE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.job_id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_file_insert': "CREATE TRIGGER guard_started_file_insert BEFORE INSERT ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN engine_ingress_uploads u ON u.job_id=t.job_id WHERE t.uuid=NEW.uuid AND u.state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_file_update': "CREATE TRIGGER guard_started_file_update BEFORE UPDATE ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN engine_ingress_uploads u ON u.job_id=t.job_id WHERE (t.uuid=OLD.uuid OR t.uuid=NEW.uuid) AND u.state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_file_delete': "CREATE TRIGGER guard_started_file_delete BEFORE DELETE ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN engine_ingress_uploads u ON u.job_id=t.job_id WHERE t.uuid=OLD.uuid AND u.state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
}


class TransferFenceError(ValueError): pass


def attest(db):
    job = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
    if job is None or job[0] != JOBS_DDL: raise TransferFenceError('jobs schema unavailable')
    row = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='engine_ingress_uploads'").fetchone()
    if row is None or row[0] != STREAM_DDL: raise TransferFenceError('transfer schema unavailable')
    for name, ddl in TRIGGERS.items():
        row = db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?", (name,)).fetchone()
        if row is None or row[0] != ddl: raise TransferFenceError('transfer fence unavailable')


def install_offline(db, *, enabled=False):
    """Migration primitive, caller must establish an independently idle owner."""
    if enabled is not True or db.in_transaction: raise TransferFenceError('offline migration required')
    db.execute('BEGIN IMMEDIATE')
    try:
        job = db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
        if job is None or job[0] != JOBS_DDL: raise TransferFenceError('jobs schema unavailable')
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held') LIMIT 1").fetchone():
            raise TransferFenceError('idle coordinator required')
        existing = db.execute("SELECT 1 FROM sqlite_master WHERE name='engine_ingress_uploads'").fetchone()
        if existing: raise TransferFenceError('existing upload journal requires inspection')
        db.execute(STREAM_DDL)
        for ddl in TRIGGERS.values(): db.execute(ddl)
        attest(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise
