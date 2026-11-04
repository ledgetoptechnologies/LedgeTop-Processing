"""Unmounted early queue preflight; caller holds the existing coordinator lock.

This is a point-in-time read under a cooperative OS lease, not protection from
writers that bypass it and not a fence against older binaries.
"""
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
import sqlite3


def preflight_existing(path):
    from . import reference_protocol as p
    from .maintenance import Conflict
    from .prototype import _require_safe_idle_database_file
    from .admission_family_candidate import SCHEMA, FamilyHeld, validate_family
    from .reference_store import ReferenceStore
    path=Path(path)
    try:
        _require_safe_idle_database_file(path)
        with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row
            db.execute('BEGIN')
            shape=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='reference_admission_family_candidate'").fetchone()
            if shape is None or ' '.join(shape[0].split())!=' '.join(SCHEMA.split()):
                raise ValueError('candidate family schema unavailable')
            ids=db.execute('SELECT singleton,authority_id FROM coordinator_identity').fetchall()
            if len(ids)!=1 or ids[0]['singleton']!=1: raise ValueError('authority unavailable')
            authority=ids[0]['authority_id']
            p._uuid(authority,'coordinator authority')
            if db.execute('SELECT 1 FROM reference_admission_family_candidate f LEFT JOIN reference_jobs r ON r.job_id=f.job_id WHERE r.job_id IS NULL LIMIT 1').fetchone():
                raise ValueError('orphan family')
            store=object.__new__(ReferenceStore)
            store.admission_family_candidate=True
            store.jcs_runtime_enabled=True
            store.queue=SimpleNamespace(authority_id=authority)
            for entry in db.execute('SELECT job_id FROM reference_jobs').fetchall():
                validate_family(db,entry[0],authority)
                row=store._row(db,entry[0])
                store._submission_owner(db,row)
                if db.execute('SELECT 1 FROM reference_viewer_activation WHERE job_id=?',(entry[0],)).fetchone():
                    store._activation(db,row)
    except (sqlite3.Error,OSError,ValueError,TypeError,KeyError,IndexError,AttributeError,ImportError,RuntimeError,Conflict) as exc:
        raise RuntimeError('candidate_family_preflight_unavailable') from exc
