"""Disabled-by-default durable enforcement for explicitly enrolled state.

Enrollment is an offline cutover primitive, not a deployed updater. The caller
must fence old launchers and stable mounts, and retain/reconcile its backup.
"""
import os
from pathlib import Path
import sqlite3
import stat
import uuid
from .legacy_state_gate import acquire, check_distinct

DDL = ("CREATE TABLE legacy_fence_requirement (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
       "device TEXT NOT NULL, inode TEXT NOT NULL, authority TEXT NOT NULL)")
LEGACY_LOCK = '/legacy-state/coordinator.lock'


def _connect(state):
    path=Path(state).absolute()/'jobs.sqlite3'
    if path.resolve()!=path or path.is_symlink():
        raise RuntimeError('legacy_fence_database_invalid')
    try:
        item=path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(item.st_mode):raise RuntimeError('legacy_fence_database_invalid')
    return sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)


def requirement(state):
    db=_connect(state)
    if db is None:return None
    try:
        db.execute('BEGIN')
        rows=db.execute("SELECT type,sql,tbl_name FROM sqlite_master WHERE name='legacy_fence_requirement'").fetchall()
        if not rows:return None
        if rows != [('table',DDL,'legacy_fence_requirement')]:raise RuntimeError('legacy_fence_marker_invalid')
        if db.execute("SELECT 1 FROM sqlite_master WHERE tbl_name='legacy_fence_requirement' AND type IN ('index','trigger') LIMIT 1").fetchone():
            raise RuntimeError('legacy_fence_marker_invalid')
        rows=db.execute('SELECT singleton,device,inode,authority FROM legacy_fence_requirement').fetchall()
        if len(rows)!=1 or rows[0][0]!=1 or any(not isinstance(v,str) or not v.isascii() or not v.isdecimal() or len(v)>20 or (len(v)>1 and v[0]=='0') for v in rows[0][1:3]):
            raise RuntimeError('legacy_fence_marker_invalid')
        try:
            if str(uuid.UUID(rows[0][3])) != rows[0][3]: raise ValueError()
        except (ValueError, TypeError, AttributeError):raise RuntimeError('legacy_fence_marker_invalid') from None
        return rows[0][1:]
    except sqlite3.Error:
        raise RuntimeError('legacy_fence_marker_invalid') from None
    finally:db.close()


def _authority(state):
    db=_connect(state)
    if db is None:raise RuntimeError('legacy_fence_authority_unavailable')
    try:
        rows=db.execute('SELECT singleton,authority_id FROM coordinator_identity').fetchall()
        if len(rows)!=1 or rows[0][0]!=1:raise ValueError()
        value=rows[0][1]
        if str(uuid.UUID(value)) != value:raise ValueError()
        return value
    except (sqlite3.Error, ValueError, TypeError, AttributeError):
        raise RuntimeError('legacy_fence_authority_unavailable') from None
    finally:db.close()


def enforce(state, lock_path=LEGACY_LOCK, *, required=False):
    identity=requirement(state)
    if identity is None:
        if required:raise RuntimeError('legacy_fence_marker_required')
        return None
    fd=acquire(lock_path)
    try:
        check_distinct(fd,state)
        info=os.fstat(fd)
        if identity[:2]!=(str(info.st_dev),str(info.st_ino)):
            raise RuntimeError('legacy_fence_identity_mismatch')
        if _authority(state)!=identity[2] or _authority(Path(lock_path).parent)!=identity[2]:
            raise RuntimeError('legacy_fence_authority_mismatch')
        return fd
    except BaseException:
        os.close(fd);raise


def enroll_offline(state, lock_path):
    """Enroll an existing stopped candidate DB; caller guarantees stable private paths."""
    state=Path(state).absolute()
    if state.resolve()!=state or not state.is_dir():raise RuntimeError('legacy_fence_candidate_state_invalid')
    candidate_fd=acquire(state/'coordinator.lock')
    old_fd=None
    try:
        old_fd=acquire(lock_path);check_distinct(old_fd,str(state))
        if requirement(state) is not None:raise RuntimeError('legacy_fence_already_enrolled')
        authority=_authority(state)
        if _authority(Path(lock_path).parent)!=authority:raise RuntimeError('legacy_fence_authority_mismatch')
        path=state/'jobs.sqlite3'
        # Check the file by read-only path before opening it writable.
        check=_connect(state)
        if check is None:raise RuntimeError('legacy_fence_database_invalid')
        check.close()
        db=sqlite3.connect(path)
        try:
            db.execute('BEGIN IMMEDIATE')
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='legacy_fence_requirement'").fetchone():
                raise RuntimeError('legacy_fence_already_enrolled')
            info=os.fstat(old_fd);db.execute(DDL)
            db.execute('INSERT INTO legacy_fence_requirement VALUES(1,?,?,?)',(str(info.st_dev),str(info.st_ino),authority))
            db.commit()
        except BaseException:
            db.rollback();raise
        finally:db.close()
    finally:
        if old_fd is not None:os.close(old_fd)
        os.close(candidate_fd)
