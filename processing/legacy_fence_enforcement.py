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
VECTOR_ROOT = '/legacy-state-vector'
VECTOR_DDL = ("CREATE TABLE legacy_fence_vector (slot INTEGER PRIMARY KEY, device TEXT NOT NULL, "
              "inode TEXT NOT NULL, authority TEXT NOT NULL)")
MAX_VECTOR_LOCKS = 16


def close_fence(value):
    if value is not None:
        for fd in value if isinstance(value, tuple) else (value,):
            os.close(fd)


def vector_requirement(state):
    db = _connect(state)
    if db is None: return None
    try:
        db.execute('BEGIN')
        objects = db.execute("SELECT type,sql,tbl_name FROM sqlite_master WHERE tbl_name='legacy_fence_vector'").fetchall()
        if not objects: return None
        if objects != [('table', VECTOR_DDL, 'legacy_fence_vector')]: raise RuntimeError('legacy_fence_vector_invalid')
        rows = db.execute('SELECT slot,device,inode,authority FROM legacy_fence_vector ORDER BY slot').fetchall()
        if not 2 <= len(rows) <= MAX_VECTOR_LOCKS: raise RuntimeError('legacy_fence_vector_invalid')
        identities = set()
        for slot, device, inode, authority in rows:
            if (type(slot) is not int or slot < 0 or any(not isinstance(v,str) or not v.isascii()
                or not v.isdecimal() or len(v)>20 or (len(v)>1 and v[0]=='0') for v in (device,inode))):
                raise RuntimeError('legacy_fence_vector_invalid')
            try:
                if str(uuid.UUID(authority)) != authority: raise ValueError()
            except (ValueError, TypeError, AttributeError): raise RuntimeError('legacy_fence_vector_invalid') from None
            identities.add((device,inode))
        if [row[0] for row in rows] != list(range(len(rows))) or len(identities) != len(rows):
            raise RuntimeError('legacy_fence_vector_invalid')
        return rows
    except sqlite3.Error:
        raise RuntimeError('legacy_fence_vector_invalid') from None
    finally: db.close()


def _vector_acquire(state, rows, paths):
    """Hold all enrolled legacy locks or release every partial acquisition."""
    if len(rows) != len(paths): raise RuntimeError('legacy_fence_vector_invalid')
    held = []
    try:
        for slot, device, inode, authority in sorted(rows, key=lambda row:(int(row[1]),int(row[2]))):
            path = Path(paths[slot]); fd = acquire(path); held.append(fd)
            check_distinct(fd,state)
            info = os.fstat(fd)
            if (str(info.st_dev),str(info.st_ino)) != (device,inode): raise RuntimeError('legacy_fence_identity_mismatch')
            if _authority(state) != authority or _authority(path.parent) != authority:
                raise RuntimeError('legacy_fence_authority_mismatch')
        return tuple(held)
    except BaseException:
        close_fence(tuple(held)); raise


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


def enforce(state, lock_path=LEGACY_LOCK, *, required=False, vector_root=None):
    identity=requirement(state)
    vector=vector_requirement(state)
    if vector is not None:
        if identity is not None: raise RuntimeError('legacy_fence_vector_conflict')
        root=Path(VECTOR_ROOT if vector_root is None else vector_root)
        if not root.is_absolute() or root.resolve()!=root: raise RuntimeError('legacy_fence_path_invalid')
        return _vector_acquire(state,vector,[root/str(slot)/'coordinator.lock' for slot,*_ in vector])
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


def enroll_vector_offline(state, lock_paths):
    """Enroll locks in slot order; caller must exclude launchers and writers."""
    state=Path(state).absolute()
    if state.resolve()!=state or not state.is_dir(): raise RuntimeError('legacy_fence_candidate_state_invalid')
    if type(lock_paths) not in (tuple,list) or not 2 <= len(lock_paths) <= MAX_VECTOR_LOCKS:
        raise RuntimeError('legacy_fence_vector_invalid')
    candidate_fd=acquire(state/'coordinator.lock'); held=()
    try:
        if requirement(state) is not None or vector_requirement(state) is not None:
            raise RuntimeError('legacy_fence_already_enrolled')
        authority=_authority(state); rows=[]
        for slot,raw in enumerate(lock_paths):
            path=Path(raw)
            if not path.is_absolute() or path.resolve()!=path: raise RuntimeError('legacy_fence_path_invalid')
            info=os.stat(path,follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode): raise RuntimeError('legacy_fence_path_invalid')
            candidate=os.fstat(candidate_fd)
            if (info.st_dev,info.st_ino)==(candidate.st_dev,candidate.st_ino):
                raise RuntimeError('legacy_fence_same_candidate_lock')
            rows.append((slot,str(info.st_dev),str(info.st_ino),authority))
        if len({(row[1],row[2]) for row in rows})!=len(rows): raise RuntimeError('legacy_fence_vector_invalid')
        held=_vector_acquire(state,rows,lock_paths)
        db=sqlite3.connect(state/'jobs.sqlite3')
        try:
            db.execute('BEGIN IMMEDIATE')
            if db.execute("SELECT 1 FROM sqlite_master WHERE name IN ('legacy_fence_requirement','legacy_fence_vector')").fetchone():
                raise RuntimeError('legacy_fence_already_enrolled')
            db.execute(VECTOR_DDL);db.executemany('INSERT INTO legacy_fence_vector VALUES(?,?,?,?)',rows);db.commit()
        except BaseException:
            db.rollback();raise
        finally:db.close()
    finally:
        close_fence(held);os.close(candidate_fd)


def enroll_offline(state, lock_path):
    """Enroll an existing stopped candidate DB; caller guarantees stable private paths."""
    state=Path(state).absolute()
    if state.resolve()!=state or not state.is_dir():raise RuntimeError('legacy_fence_candidate_state_invalid')
    candidate_fd=acquire(state/'coordinator.lock')
    old_fd=None
    try:
        old_fd=acquire(lock_path);check_distinct(old_fd,str(state))
        if requirement(state) is not None or vector_requirement(state) is not None:raise RuntimeError('legacy_fence_already_enrolled')
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
            if db.execute("SELECT 1 FROM sqlite_master WHERE name IN ('legacy_fence_requirement','legacy_fence_vector')").fetchone():
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
