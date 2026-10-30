"""Read-only R1 check: does the Viewer have any processing work in flight?

The cutover runbook's step 1 (R1) needs the Viewer to have no active processing
job or attempt, including retries scheduled for later. This opens the Viewer's
SQLite database without writing to it and reports counts only: no names, task
IDs, credentials or client data.

The Viewer uses WAL mode, so a live database is opened ``mode=ro`` (it must be
able to read the ``-wal``/``-shm`` files; run it as the Viewer's owner). Opening
a live WAL database ``immutable=1`` would silently miss uncheckpointed writes,
so ``--snapshot`` (immutable) is only for a consistent copy taken with SQLite's
backup API, never for the live file.

Quiet means all of:
- no ``processing_jobs`` row is ``pending`` or ``leased`` (this includes
  pending retries whose ``available_at`` is in the future);
- no ``processing_attempts`` row is in a status that can still talk to a
  processing provider (pending through ingesting);
- no ``derivative_jobs`` row is ``pending`` or ``leased``, and no attempt is in
  ``derivatives``. These are Viewer-local, but are counted conservatively
  until it is shown they never touch the coordinator.

Exit 0 when quiet, 1 when anything is active, 2 when the schema is not the one
this was written against or the database cannot be read.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

PROVIDER_ACTIVE = ('pending', 'admitted', 'initializing', 'uploading', 'committed', 'queued_upstream',
                   'running', 'ingesting')
VIEWER_LOCAL_ACTIVE = ('derivatives',)
SETTLED = ('ready_for_review', 'published', 'failed', 'cancelled')
ATTEMPT_STATUSES = frozenset(PROVIDER_ACTIVE + VIEWER_LOCAL_ACTIVE + SETTLED)
JOB_STATUSES = frozenset(('pending', 'leased', 'complete', 'failed', 'cancelled'))
REQUIRED = {
    'processing_jobs': {'id', 'attempt_id', 'job_type', 'status', 'available_at', 'lease_expires_at'},
    'processing_attempts': {'id', 'provider_id', 'status'},
    'processing_providers': {'id', 'endpoint', 'enabled'},
    'derivative_jobs': {'id', 'attempt_id', 'status'},
}


class SchemaError(Exception):
    pass


def connect(path, snapshot):
    path = Path(path)
    if not path.is_file():
        raise SchemaError('database not found')
    mode = 'ro&immutable=1' if snapshot else 'ro'
    return sqlite3.connect('file:%s?mode=%s' % (path.as_posix(), mode), uri=True)


def check_schema(db):
    for table, columns in REQUIRED.items():
        present = {row[1] for row in db.execute('PRAGMA table_info("%s")' % table)}
        if not present:
            raise SchemaError('missing table ' + table)
        if not columns <= present:
            raise SchemaError('table %s lacks %s' % (table, sorted(columns - present)))


def count_by(db, sql, parameters=()):
    return {key: count for key, count in db.execute(sql, parameters)}


def report(db, now):
    check_schema(db)
    jobs = count_by(db, 'SELECT status, COUNT(*) FROM processing_jobs GROUP BY status')
    attempts = count_by(db, 'SELECT status, COUNT(*) FROM processing_attempts GROUP BY status')
    derivatives = count_by(db, 'SELECT status, COUNT(*) FROM derivative_jobs GROUP BY status')
    unknown = sorted((set(jobs) - JOB_STATUSES) | (set(attempts) - ATTEMPT_STATUSES) | (set(derivatives) - JOB_STATUSES))
    if unknown:
        raise SchemaError('unexpected status values: %s' % ', '.join(map(str, unknown)))
    active_jobs = db.execute("SELECT job_type, status, COUNT(*) FROM processing_jobs "
                             "WHERE status IN ('pending','leased') GROUP BY job_type, status").fetchall()
    scheduled_later = db.execute("SELECT COUNT(*) FROM processing_jobs WHERE status='pending' AND available_at > ?",
                                 (now,)).fetchone()[0]
    marks = ','.join('?' * len(PROVIDER_ACTIVE))
    by_provider = db.execute(
        'SELECT a.provider_id, p.endpoint, p.enabled, COUNT(*) FROM processing_attempts a '
        'LEFT JOIN processing_providers p ON p.id = a.provider_id WHERE a.status IN (%s) '
        'GROUP BY a.provider_id, p.endpoint, p.enabled' % marks, PROVIDER_ACTIVE).fetchall()
    provider_active = sum(attempts.get(status, 0) for status in PROVIDER_ACTIVE)
    local_active = (sum(attempts.get(status, 0) for status in VIEWER_LOCAL_ACTIVE)
                    + derivatives.get('pending', 0) + derivatives.get('leased', 0))
    job_active = jobs.get('pending', 0) + jobs.get('leased', 0)
    return {
        'quiet': job_active == 0 and provider_active == 0 and local_active == 0,
        'checkedAt': now,
        'processingJobs': {'active': job_active, 'scheduledLater': scheduled_later,
                           'activeByTypeAndStatus': [{'type': t, 'status': s, 'count': c} for t, s, c in active_jobs],
                           'byStatus': jobs},
        'processingAttempts': {'providerActive': provider_active, 'viewerLocalActive':
                               sum(attempts.get(s, 0) for s in VIEWER_LOCAL_ACTIVE), 'byStatus': attempts,
                               'providerActiveByProvider': [{'provider': p, 'endpoint': e, 'enabled': bool(n),
                                                             'count': c} for p, e, n, c in by_provider]},
        'derivativeJobs': {'active': derivatives.get('pending', 0) + derivatives.get('leased', 0),
                           'byStatus': derivatives},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('database', help="the Viewer's SQLite database (live, read-only) or a backup-API snapshot")
    parser.add_argument('--snapshot', action='store_true',
                        help='the file is a consistent backup copy, not the live WAL database')
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    try:
        db = connect(args.database, args.snapshot)
        try:
            db.execute('BEGIN')  # one consistent read transaction for every count
            result = report(db, now)
        finally:
            db.close()
    except (SchemaError, sqlite3.Error) as error:
        print(json.dumps({'quiet': False, 'error': str(error)}))
        return 2
    print(json.dumps(result, sort_keys=True, indent=1))
    return 0 if result['quiet'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
