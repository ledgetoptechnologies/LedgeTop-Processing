"""Disabled experimental stopped-copy repair; not qualified for live cutover.

Requires external exclusion of noncooperating writers and a fresh independently
owned copy. Never opens a listener, starts an engine or invokes a scheduler.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3

from processing.prototype import _require_safe_idle_database_file, VIEWER_PROFILE


def repair_copy(source, copy):
    """Reconcile exact legacy splits on a byte-identical stopped SQLite copy."""
    import fcntl
    source, copy = Path(source).absolute(), Path(copy).absolute()
    if source.resolve(strict=True) != source or copy.resolve(strict=True) != copy or source == copy:
        raise ValueError('distinct canonical directories required')
    locks=[]
    try:
        for root in (source,copy):
            fd=os.open(root/'coordinator.lock',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
            locks.append(fd);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        old=source/'jobs.sqlite3';new=copy/'jobs.sqlite3'
        for path in (old,new):_require_safe_idle_database_file(path)
        if os.path.samefile(old,new):raise ValueError('copy must not alias source')
        def digest(path):
            h=hashlib.sha256()
            with path.open('rb') as f:
                for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
            return h.digest()
        if digest(old)!=digest(new):raise ValueError('database copy differs')
        db=sqlite3.connect(new,timeout=10);db.row_factory=sqlite3.Row
        try:
            db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
            if db.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise ValueError('invalid database')
            if db.execute("SELECT 1 FROM jobs WHERE status IS NULL OR status NOT IN ('succeeded','failed','cancelled') LIMIT 1").fetchone():raise ValueError('active job')
            rows=db.execute("SELECT * FROM viewer_tasks WHERE stage IS NULL OR stage NOT IN ('succeeded','failed','cancelled')").fetchall()
            for row in rows:
                if row['removed'] or row['stage']!='running' or row['phase'] not in ('processing','commit_pending','cancel_pending'):
                    raise ValueError('unresolved Viewer task')
                job=db.execute('SELECT * FROM jobs WHERE id=?',(row['job_id'],)).fetchone()
                expected={'engineTaskId':row['engine_uuid'],'nodeId':row['node_id'],'environment':row['environment'],'cancelled':False}
                if (job is None or job['status'] not in ('succeeded','failed') or job['profile']!=VIEWER_PROFILE
                    or job['node_id']!=row['node_id'] or job['environment']!=row['environment']
                    or not row['engine_uuid'] or not row['node_id'] or not row['claim_token']
                    or not job['claim_hash'] or not hmac.compare_digest(hashlib.sha256(row['claim_token'].encode()).hexdigest(),job['claim_hash'])
                    or json.loads(job['result'])!=expected):raise ValueError('split identity differs')
                db.execute("UPDATE viewer_tasks SET stage=?,phase='complete',progress=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                    (job['status'],100 if job['status']=='succeeded' else row['progress'],row['uuid']))
            db.commit()
            return len(rows)
        except BaseException:
            db.rollback();raise
        finally:db.close()
    finally:
        for fd in locks:os.close(fd)
