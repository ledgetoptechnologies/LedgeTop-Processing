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


def repair_copy(source, copy, *, synthetic_only=False, expected_copy_inode=None):
    """Reconcile exact legacy splits on a byte-identical stopped SQLite copy."""
    import fcntl
    if synthetic_only is not True:raise ValueError('synthetic-only operation disabled')
    source, copy = Path(source).absolute(), Path(copy).absolute()
    if source.resolve(strict=True) != source or copy.resolve(strict=True) != copy or source == copy:
        raise ValueError('distinct canonical directories required')
    info=copy.stat()
    if (info.st_uid!=os.geteuid() or info.st_mode & 0o022 or type(expected_copy_inode) is not tuple
            or (info.st_dev,info.st_ino)!=expected_copy_inode):raise ValueError('copy identity unavailable')
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
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' LIMIT 1").fetchone():raise ValueError('triggers require inspection')
            hashes={r['name']:hashlib.sha256(r['sql'].encode()).hexdigest() for r in db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name IN ('jobs','viewer_tasks')")}
            if hashes.get('jobs')!='3171569c8b6a9a63a41d3e6f97521adebc89d060b7550088506dadebe9654705' or hashes.get('viewer_tasks') not in ('e7bbc127917f0c8c21007a79f35b434d00a1446fed25b369e1b3ab7579c628a3','a637e43b58241be981e0c056d8e6fd7d4a83e7c590598f837f6fccfdcf77d424'):
                raise ValueError('schema requires inspection')
            if db.execute("SELECT 1 FROM jobs WHERE status IS NULL OR status NOT IN ('succeeded','failed','cancelled') LIMIT 1").fetchone():raise ValueError('active job')
            if db.execute("SELECT 1 FROM viewer_tasks WHERE stage IN ('succeeded','failed','cancelled') AND phase<>'complete' LIMIT 1").fetchone():raise ValueError('terminal task phase requires inspection')
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='viewer_reservations'").fetchone() and db.execute("SELECT 1 FROM viewer_reservations r LEFT JOIN viewer_tasks t ON t.uuid=r.uuid WHERE r.state IS NULL OR r.state NOT IN ('reserved','cancelled') OR (r.state='reserved' AND (t.uuid IS NULL OR t.stage NOT IN ('succeeded','failed','cancelled'))) LIMIT 1").fetchone():
                raise ValueError('reservations require inspection')
            rows=db.execute("SELECT * FROM viewer_tasks WHERE stage IS NULL OR stage NOT IN ('succeeded','failed','cancelled')").fetchall()
            for row in rows:
                if row['removed'] or row['stage']!='running' or row['phase'] not in ('processing','commit_pending','cancel_pending'):
                    raise ValueError('unresolved Viewer task')
                if db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE job_id=?',(row['job_id'],)).fetchone()[0]!=1:
                    raise ValueError('job belongs to multiple Viewer tasks')
                job=db.execute('SELECT * FROM jobs WHERE id=?',(row['job_id'],)).fetchone()
                expected={'engineTaskId':row['engine_uuid'],'nodeId':row['node_id'],'environment':row['environment'],'cancelled':False}
                if (job is None or job['status'] not in ('succeeded','failed') or job['profile']!=VIEWER_PROFILE
                    or job['node_id']!=row['node_id'] or job['environment']!=row['environment']
                    or not row['engine_uuid'] or not row['node_id'] or not row['claim_token']
                    or not job['claim_hash'] or not hmac.compare_digest(hashlib.sha256(row['claim_token'].encode()).hexdigest(),job['claim_hash'])
                    or job['result']!=json.dumps(expected,sort_keys=True,allow_nan=False,separators=(',',':'))):raise ValueError('split identity differs')
                db.execute("UPDATE viewer_tasks SET stage=?,phase='complete',progress=?,updated_at=CURRENT_TIMESTAMP WHERE uuid=?",
                    (job['status'],100 if job['status']=='succeeded' else row['progress'],row['uuid']))
            db.commit()
            return len(rows)
        except BaseException:
            db.rollback();raise
        finally:db.close()
    finally:
        for fd in locks:os.close(fd)
