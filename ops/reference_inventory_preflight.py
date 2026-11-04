"""Inspect an offline SQLite backup for a reference-to-Viewer-only rehearsal.

This is advisory evidence, not a migration, authorization, lock, or live fence.
Do not pass a live database or a raw copy without its WAL; use a consistent
SQLite backup created after the writer has stopped and ownership is established.
"""
import argparse
from contextlib import closing
import json
import sqlite3
from pathlib import Path


def inspect_snapshot(path):
    path = Path(path).absolute()
    if path.is_symlink() or not path.is_file() or path.resolve() != path:
        raise ValueError('regular offline database snapshot required')
    if any(Path(str(path) + suffix).exists() for suffix in ('-wal', '-journal')):
        raise ValueError('snapshot has unmerged journal files')
    # Immutable is intentional: this accepts only a self-contained SQLite backup,
    # never a live database whose WAL may still contain committed transactions.
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        if db.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
            raise ValueError('database integrity check failed')
        names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        expected = {'jobs', 'viewer_tasks', 'coordinator_identity', 'coordinator_authority_lease', 'reference_jobs'}
        if not expected <= names:
            raise ValueError('required tables missing')
        reference = {}
        for name in sorted(n for n in names if n.startswith('reference_')):
            # Table names come from SQLite, and double quotes are escaped rather
            # than interpolating untrusted identifiers directly.
            quoted = '"' + name.replace('"', '""') + '"'
            reference[name] = db.execute('SELECT COUNT(*) FROM ' + quoted).fetchone()[0]
        jobs = dict(db.execute('SELECT status, COUNT(*) FROM jobs GROUP BY status'))
        viewer = dict(db.execute('SELECT phase, COUNT(*) FROM viewer_tasks GROUP BY phase'))
        if any(not isinstance(value, str) for value in (*jobs, *viewer)):
            raise ValueError('unrecognized state')
        authority_count = db.execute('SELECT COUNT(*) FROM coordinator_identity').fetchone()[0]
        lease_count = db.execute('SELECT COUNT(*) FROM coordinator_authority_lease').fetchone()[0]
        return {'referenceTableCounts': reference, 'jobStatusCounts': jobs,
                'viewerPhaseCounts': viewer, 'authorityRows': authority_count,
                'authorityLeaseRows': lease_count,
                'snapshotHasNoReferenceRows': all(count == 0 for count in reference.values()),
                'snapshotHasNoActiveJobs': not any(count for status, count in jobs.items()
                                                   if status not in ('succeeded', 'failed')),
                'advisoryOnly': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('offline_sqlite_backup')
    args = parser.parse_args()
    try:
        print(json.dumps(inspect_snapshot(args.offline_sqlite_backup), sort_keys=True))
    except (OSError, sqlite3.Error, ValueError):
        parser.exit(2, 'Snapshot inspection failed; preserve the state for review.\n')


if __name__ == '__main__':
    main()
