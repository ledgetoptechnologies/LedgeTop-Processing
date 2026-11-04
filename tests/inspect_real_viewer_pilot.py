"""Inspect a confirmed-stopped, checkpointed pilot; never resume or clean."""
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid


def inspect(root, registry):
    root, registry = Path(root), Path(registry)
    if (not root.is_absolute() or not registry.is_absolute() or root.resolve()!=root
            or registry.resolve()!=registry or not root.is_dir() or not registry.is_dir()):
        raise ValueError('real absolute directories required')
    def read_json(path):
        if path.is_symlink() or not path.is_file() or path.stat().st_size>65536:
            raise ValueError('private pilot record unavailable')
        return json.loads(path.read_text(encoding='utf-8'))
    owner=read_json(root/'owner.json'); active=read_json(registry/'active.json')
    receipt=read_json(root/'pilot-receipt.json')
    if not re.fullmatch(r'[a-f0-9]{32}',str(owner.get('owner',''))) or owner['owner']!=active.get('owner') or owner['owner']!=receipt.get('owner'):
        raise ValueError('pilot owner binding mismatch')
    for key in ('attemptId','taskId','providerTaskId'):
        try:
            if str(uuid.UUID(receipt[key]))!=receipt[key]:raise ValueError()
        except (ValueError,KeyError,TypeError,AttributeError):
            raise ValueError('pilot identity invalid') from None
    dbpath=root/'viewer.sqlite'
    if dbpath.is_symlink() or not dbpath.is_file():raise ValueError('pilot database unavailable')
    sidecars=[Path(str(dbpath)+s) for s in ('-wal','-shm','-journal')]
    if any(os.path.lexists(p) for p in sidecars):raise ValueError('pilot database is not cleanly checkpointed')
    before=dbpath.stat()
    # Immutable mode avoids creating SQLite shared-memory/lock files. It can
    # miss uncheckpointed WAL state; require a stopped, checkpointed pilot.
    # An operator must independently prove no writer can reopen this database.
    db=sqlite3.connect(dbpath.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=5)
    try:
        row=db.execute('SELECT id,task_id,provider_task_id,status,submission_phase,uploaded_file_count,error_code FROM processing_attempts WHERE id=?',
                       (receipt.get('attemptId'),)).fetchone()
        count=db.execute('SELECT COUNT(*) FROM processing_attempts').fetchone()[0]
        if not row or count!=1 or row[1]!=receipt.get('taskId') or row[2]!=receipt.get('providerTaskId'):
            raise ValueError('pilot attempt binding mismatch')
        safe=lambda value: value if value is None or (isinstance(value,str) and re.fullmatch(r'[a-z0-9_]{1,80}',value)) else 'unrecognized'
        jobs=[{'type':safe(r[0]),'status':safe(r[1])} for r in db.execute('SELECT job_type,status FROM processing_jobs WHERE attempt_id=? ORDER BY created_at,id',(row[0],))]
        complete=(root/'pilot-complete.json').is_file() and not (root/'pilot-complete.json').is_symlink()
        # Data, raw error text, endpoint and credentials are never emitted.
        after=dbpath.stat()
        if ((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)
                or any(os.path.lexists(p) for p in sidecars)):raise ValueError('pilot database changed during inspection')
        return {'owner':owner['owner'],'attemptId':row[0],'providerTaskId':row[2],
                'status':safe(row[3]),'submissionPhase':safe(row[4]),'uploadedFileCount':row[5],
                'errorCode':safe(row[6]),'jobs':jobs,'completeMarkerPresent':complete,
                'recovery':'inspection_only_no_resume_or_cleanup','writerStoppedNotVerified':True}
    finally: db.close()


if __name__=='__main__':
    if len(sys.argv)!=3:raise SystemExit('usage: inspect_real_viewer_pilot.py PILOT_ROOT REGISTRY_ROOT')
    print(json.dumps(inspect(sys.argv[1],sys.argv[2]),separators=(',',':')))
