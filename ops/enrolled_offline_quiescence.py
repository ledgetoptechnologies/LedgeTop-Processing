"""Hold both coordinator state locks while verifying enrolled idle state.

No Docker access, remote communication, deployment, or automatic update. The
caller must first prove the exact container stopped with clean exit, stable
volume mounts and exclusive host/daemon control. This only verifies local state
and holds its locks for the duration of the context.
"""
from contextlib import contextmanager
import os
import hashlib
import sqlite3
from pathlib import Path
from processing.legacy_state_gate import acquire, check_distinct
from processing.legacy_fence_enforcement import requirement, _authority
from processing.prototype import _require_idle_state, _require_safe_idle_database_file


@contextmanager
def stopped_enrolled_state(state_dir, legacy_dir):
    state=Path(state_dir).absolute();legacy=Path(legacy_dir).absolute()
    if (state.resolve()!=state or legacy.resolve()!=legacy or not state.is_dir() or not legacy.is_dir()):
        raise RuntimeError('offline_state_path_invalid')
    current_fd=acquire(state/'coordinator.lock');old_fd=None
    try:
        old_fd=acquire(legacy/'coordinator.lock');check_distinct(old_fd,str(state))
        # Reject unsafe SQLite journals before opening either database.
        _require_safe_idle_database_file(state/'jobs.sqlite3')
        _require_safe_idle_database_file(legacy/'jobs.sqlite3')
        marker=requirement(state)
        if marker is None:raise RuntimeError('legacy_fence_marker_required')
        info=os.fstat(old_fd)
        if marker[:2]!=(str(info.st_dev),str(info.st_ino)):
            raise RuntimeError('legacy_fence_identity_mismatch')
        if _authority(state)!=marker[2] or _authority(legacy)!=marker[2]:
            raise RuntimeError('legacy_fence_authority_mismatch')
        _require_idle_state(state/'jobs.sqlite3')
        db=sqlite3.connect((state/'jobs.sqlite3').as_uri()+'?mode=ro',uri=True)
        try:
            db.execute('BEGIN')
            names={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            known={'reference_jobs','reference_outbox'}
            objects=db.execute("SELECT name,type,sql FROM sqlite_master WHERE name IN ('reference_jobs','reference_outbox')").fetchall()
            expected={'reference_jobs':'216067b1999140c8c0ab8c3ec38d7448a83742216507c6a77508623f1a6591c7',
                      'reference_outbox':'c38e45bdf912a28792c283bf7c6be8976ec722d03b38ef48280ef9f56028c031'}
            if (any(kind!='table' or not isinstance(sql,str) or hashlib.sha256(sql.encode()).hexdigest()!=expected[name]
                    for name,kind,sql in objects) or bool(names & known) != known.issubset(names)):
                raise RuntimeError('offline_reference_state_unavailable')
            if not (names & known) and db.execute("SELECT 1 FROM sqlite_master WHERE name GLOB 'reference_*' LIMIT 1").fetchone():
                raise RuntimeError('offline_reference_state_unavailable')
            if 'reference_jobs' in names and db.execute("SELECT 1 FROM reference_jobs WHERE phase IS NULL OR phase NOT IN ('completed','cancelled') LIMIT 1").fetchone():
                raise RuntimeError('offline_reference_work_unsettled')
            if 'reference_outbox' in names and db.execute("SELECT 1 FROM reference_outbox WHERE acknowledged IS NOT 1 OR typeof(acknowledged)!='integer' LIMIT 1").fetchone():
                raise RuntimeError('offline_reference_outbox_unsettled')
        except sqlite3.Error:
            raise RuntimeError('offline_reference_state_unavailable') from None
        finally:db.close()
        # Caller must keep both descriptors held through backup or census; the
        # yielded metadata is point-in-time, not authorization to start later.
        yield {'authorityId':marker[2], 'legacyDevice':marker[0], 'legacyInode':marker[1], 'heldStateChecked':True}
    finally:
        if old_fd is not None:os.close(old_fd)
        os.close(current_fd)
