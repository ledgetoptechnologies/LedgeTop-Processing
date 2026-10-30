"""Read-only census of a stopped coordinator state directory.

Run only against a stopped volume mounted read-only. SQLite is opened with
``immutable=1`` so no journal, WAL or shared-memory file is created. The output
holds hashes, counts and modes, never row contents, tokens or file names under
``viewer-tasks``, which can name client photos.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

ACTIVE_JOB_STATUSES = ('queued', 'running', 'held')
TERMINAL_STAGES = ('succeeded', 'failed', 'cancelled')


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def tree(root):
    entries = []
    for directory, names, files in os.walk(root, followlinks=False):
        names.sort()
        for name in sorted(names) + sorted(files):
            path = Path(directory) / name
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            entry = {'path': hashlib.sha256(relative.encode()).hexdigest(),
                     'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid}
            if stat.S_ISDIR(info.st_mode):
                entry['kind'] = 'directory'
            elif stat.S_ISREG(info.st_mode):
                entry.update(kind='file', bytes=info.st_size, sha256=_sha256(path))
            else:
                entry['kind'] = 'other'
            entries.append(entry)
    entries.sort(key=lambda item: item['path'])
    canonical = json.dumps(entries, sort_keys=True, separators=(',', ':')).encode()
    top = sorted(p.name for p in Path(root).iterdir())
    incoming = sum(1 for p in Path(root).rglob('.incoming-*'))
    spool = Path(root) / 'viewer-spool'
    info = Path(root).lstat()
    return {'treeSha256': hashlib.sha256(canonical).hexdigest(), 'entries': len(entries),
            'root': {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid},
            'regularFiles': sum(1 for e in entries if e['kind'] == 'file'),
            'bytes': sum(e.get('bytes', 0) for e in entries),
            'other': sum(1 for e in entries if e['kind'] == 'other'),
            'topLevel': top, 'incomingPartials': incoming,
            'spoolEntries': sum(1 for _ in spool.iterdir()) if spool.is_dir() else None}


def database(path):
    uri = 'file:' + str(path) + '?mode=ro&immutable=1'
    db = sqlite3.connect(uri, uri=True)
    try:
        check = db.execute('PRAGMA quick_check').fetchone()[0]
        tables = [row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name")]
        schema = hashlib.sha256('\n'.join(row[0] or '' for row in db.execute(
            'SELECT sql FROM sqlite_master ORDER BY type,name')).encode()).hexdigest()
        counts = {name: db.execute('SELECT COUNT(*) FROM "%s"' % name.replace('"', '""')).fetchone()[0]
                  for name in tables}
        active = {}
        if 'jobs' in tables:
            marks = ','.join('?' * len(ACTIVE_JOB_STATUSES))
            active['jobs'] = db.execute('SELECT COUNT(*) FROM jobs WHERE status IN (%s)' % marks,
                                        ACTIVE_JOB_STATUSES).fetchone()[0]
        if 'viewer_tasks' in tables:
            marks = ','.join('?' * len(TERMINAL_STAGES))
            active['viewerTasks'] = db.execute(
                'SELECT COUNT(*) FROM viewer_tasks WHERE removed=0 AND stage NOT IN (%s)' % marks,
                TERMINAL_STAGES).fetchone()[0]
        authority = None
        if 'coordinator_identity' in tables:
            row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            authority = row[0] if row else None
        return {'quickCheck': check, 'schemaSha256': schema, 'rowCounts': counts,
                'active': active, 'authorityId': authority,
                'sidecars': sorted(p.name for p in Path(path).parent.glob(Path(path).name + '-*'))}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('root')
    parser.add_argument('--database', default='jobs.sqlite3')
    args = parser.parse_args(argv)
    root = Path(args.root)
    result = {'schemaVersion': 1, 'tree': tree(root)}
    if (root / args.database).is_file():
        result['database'] = database(root / args.database)
    json.dump(result, sys.stdout, sort_keys=True, indent=1)
    sys.stdout.write('\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
