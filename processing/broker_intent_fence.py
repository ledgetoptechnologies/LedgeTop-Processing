"""Offline-only experimental coordinator intent fence; no production wiring."""

DDL = "CREATE TABLE broker_intents (operation_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, grant_hash TEXT NOT NULL, state TEXT NOT NULL)"
MARKER_DDL = "CREATE TABLE broker_intent_required (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))"
TRIGGERS = {
    'broker_job_update': "CREATE TRIGGER broker_job_update BEFORE UPDATE ON jobs WHEN (EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND (job_id=OLD.id OR job_id=NEW.id)) AND NOT (OLD.status IS 'running' AND NEW.status IS 'held' AND NEW.held_reason IS 'coordinator_restart' AND NEW.id IS OLD.id AND NEW.seq IS OLD.seq AND NEW.request_id IS OLD.request_id AND NEW.environment IS OLD.environment AND NEW.profile IS OLD.profile AND NEW.worker_id IS OLD.worker_id AND NEW.node_id IS OLD.node_id AND NEW.claim_hash IS OLD.claim_hash AND NEW.result IS OLD.result AND NEW.created_at IS OLD.created_at)) OR (NEW.status IS 'running' AND OLD.status IS NOT 'running' AND EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete')) BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_job_insert': "CREATE TRIGGER broker_job_insert BEFORE INSERT ON jobs WHEN NEW.status IS 'running' AND EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete') BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_job_delete': "CREATE TRIGGER broker_job_delete BEFORE DELETE ON jobs WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=OLD.id) BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_task_update': "CREATE TRIGGER broker_task_update BEFORE UPDATE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND (job_id=OLD.job_id OR job_id=NEW.job_id)) BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_task_delete': "CREATE TRIGGER broker_task_delete BEFORE DELETE ON viewer_tasks WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=OLD.job_id) BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_task_insert': "CREATE TRIGGER broker_task_insert BEFORE INSERT ON viewer_tasks WHEN EXISTS (SELECT 1 FROM broker_intents WHERE state IS NOT 'complete' AND job_id=NEW.job_id) BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_file_update': "CREATE TRIGGER broker_file_update BEFORE UPDATE ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN broker_intents i ON i.job_id=t.job_id WHERE (t.uuid=OLD.uuid OR t.uuid=NEW.uuid) AND i.state IS NOT 'complete') BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_file_delete': "CREATE TRIGGER broker_file_delete BEFORE DELETE ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN broker_intents i ON i.job_id=t.job_id WHERE t.uuid=OLD.uuid AND i.state IS NOT 'complete') BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
    'broker_file_insert': "CREATE TRIGGER broker_file_insert BEFORE INSERT ON viewer_files WHEN EXISTS (SELECT 1 FROM viewer_tasks t JOIN broker_intents i ON i.job_id=t.job_id WHERE t.uuid=NEW.uuid AND i.state IS NOT 'complete') BEGIN SELECT RAISE(ABORT,'broker intent held'); END",
}

class IntentFenceError(ValueError): pass

def attest(db):
    from .transfer_fence import JOBS_DDL
    jobs=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone()
    if jobs is None or jobs[0]!=JOBS_DDL: raise IntentFenceError('jobs schema unavailable')
    for table, required in (('viewer_tasks',{'uuid','job_id'}),('viewer_files',{'uuid'})):
        columns={row[1] for row in db.execute('PRAGMA table_info('+table+')')}
        if not required <= columns: raise IntentFenceError('viewer schema unavailable')
    marker=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_intent_required'").fetchone()
    if marker is None or marker[0]!=MARKER_DDL or db.execute('SELECT COUNT(*) FROM broker_intent_required WHERE singleton=1 AND required=1').fetchone()[0]!=1: raise IntentFenceError('broker intent marker unavailable')
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_intents'").fetchone()
    if row is None or row[0]!=DDL: raise IntentFenceError('broker intent schema unavailable')
    for name, sql in TRIGGERS.items():
        row=db.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",(name,)).fetchone()
        if row is None or row[0]!=sql: raise IntentFenceError('broker intent fence unavailable')

def clear(db, allowed=None):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='broker_intents'").fetchone()
    if row is None:
        return not bool(db.execute("SELECT 1 FROM sqlite_master WHERE name='broker_intent_required'").fetchone())
    try: attest(db)
    except IntentFenceError: return False
    rows=db.execute("SELECT operation_id,job_id,grant_hash,state FROM broker_intents WHERE state IS NOT 'complete'").fetchall()
    return not rows or (allowed is not None and len(rows)==1 and tuple(rows[0])==(*allowed,'started'))

def install_offline(db, *, enabled=False):
    if enabled is not True or db.in_transaction: raise IntentFenceError('offline migration required')
    db.execute('BEGIN IMMEDIATE')
    try:
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','held')").fetchone(): raise IntentFenceError('idle coordinator required')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='broker_intents'").fetchone(): raise IntentFenceError('existing intent journal')
        db.execute(MARKER_DDL)
        db.execute('INSERT INTO broker_intent_required VALUES(1,1)')
        db.execute(DDL)
        for sql in TRIGGERS.values(): db.execute(sql)
        attest(db);db.commit()
    except BaseException:
        db.rollback();raise
