"""Inactive migration experiment: hold coordinator state during uncertain upload.

The transfer journal and these triggers must be installed offline before use.
This is not a physical engine fence and cannot stop an older schema writer from
dropping triggers or modifying the transfer journal directly.
"""
import sqlite3

from .engine_ingress_guard import STREAM_DDL

TRIGGERS = {
    'guard_started_job_update': "CREATE TRIGGER guard_started_job_update BEFORE UPDATE ON jobs WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_job_delete': "CREATE TRIGGER guard_started_job_delete BEFORE DELETE ON jobs WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_task_update': "CREATE TRIGGER guard_started_task_update BEFORE UPDATE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.job_id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
    'guard_started_task_delete': "CREATE TRIGGER guard_started_task_delete BEFORE DELETE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM engine_ingress_uploads WHERE job_id=OLD.job_id AND state='started') BEGIN SELECT RAISE(ABORT,'started transfer held'); END",
}


class TransferFenceError(ValueError): pass


def attest(db):
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
