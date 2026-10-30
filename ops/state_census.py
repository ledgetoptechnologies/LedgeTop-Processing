"""Read-only census and comparison of a stopped coordinator state directory.

``census`` walks a stopped volume mounted read-only and records, for every
entry, its kind, mode, owner, size, whole-second mtime and SHA-256 contents.
SQLite is opened with ``immutable=1``, so no journal, WAL or shared-memory file
is created. Paths are identified only by HMAC-SHA256 under a per-run secret key
read from stdin, so guessable client names such as ``DJI_0001.JPG`` cannot be
recovered from the output by hashing candidates. Row contents, tokens and file
names are never printed. Symlinks, special files and hard-linked files are
rejected (exit 3): their copy and restore semantics are not qualified.

``compare`` diffs two census documents made with the same key. Any difference
that is not explicitly allowed by path, table or schema-object name fails
(exit 1).
"""

import argparse
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import sys

ACTIVE_JOB_STATUSES = ('queued', 'running', 'held')
TERMINAL_STAGES = ('succeeded', 'failed', 'cancelled')
FILE_FIELDS = ('kind', 'mode', 'uid', 'gid', 'mtime', 'bytes', 'sha256')
DIRECTORY_FIELDS = ('kind', 'mode', 'uid', 'gid', 'mtime')
SCHEMA_VERSION = 2


class Rejected(Exception):
    pass


def read_key(stream):
    text = stream.read().strip()
    if not re.fullmatch(r'[0-9a-f]{64}', text):
        raise SystemExit('a 64-hex-digit census key is required on stdin')
    return bytes.fromhex(text)


def path_id(key, relative):
    return hmac.new(key, relative.encode('utf-8', 'surrogateescape'), hashlib.sha256).hexdigest()


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _entry(path, info):
    entry = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
             'mtime': int(info.st_mtime)}
    if stat.S_ISDIR(info.st_mode):
        entry['kind'] = 'directory'
    elif stat.S_ISREG(info.st_mode):
        entry.update(kind='file', bytes=info.st_size, links=info.st_nlink, sha256=_sha256(path))
    elif stat.S_ISLNK(info.st_mode):
        entry['kind'] = 'symlink'
    else:
        entry['kind'] = 'other'
    return entry


def tree(root, key):
    root = Path(root)
    entries = {path_id(key, '.'): dict(_entry(root, root.lstat()), root=True)}
    errors = []

    def failed(error):
        errors.append(type(error).__name__)

    for directory, names, files in os.walk(root, followlinks=False, onerror=failed):
        names.sort()
        for name in sorted(names) + sorted(files):
            path = Path(directory) / name
            try:
                entries[path_id(key, path.relative_to(root).as_posix())] = _entry(path, path.lstat())
            except OSError as error:
                failed(error)
    values = list(entries.values())
    rejected = {'symlinks': sum(e['kind'] == 'symlink' for e in values),
                'other': sum(e['kind'] == 'other' for e in values),
                'hardlinked': sum(e.get('links', 1) > 1 for e in values),
                'unreadable': len(errors)}
    canonical = json.dumps(sorted(entries.items()), sort_keys=True, separators=(',', ':')).encode()
    spool = root / 'viewer-spool'
    summary = {'treeSha256': hashlib.sha256(canonical).hexdigest(), 'entries': len(values),
               'regularFiles': sum(e['kind'] == 'file' for e in values),
               'bytes': sum(e.get('bytes', 0) for e in values),
               'incomingPartials': sum(1 for _ in root.rglob('.incoming-*')),
               'spoolEntries': sum(1 for _ in spool.iterdir()) if spool.is_dir() else None,
               'rejected': rejected}
    return summary, entries


def database(path):
    uri = 'file:' + str(path) + '?mode=ro&immutable=1'
    db = sqlite3.connect(uri, uri=True)
    try:
        check = db.execute('PRAGMA quick_check').fetchone()[0]
        objects = {'%s:%s' % (kind, name): hashlib.sha256((sql or '').encode()).hexdigest()
                   for kind, name, sql in db.execute('SELECT type,name,sql FROM sqlite_master')}
        tables = sorted(name for name, in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'"))
        counts, digests = {}, {}
        for name in tables:
            rows = sorted(repr(tuple(row)) for row in db.execute('SELECT * FROM "%s"' % name.replace('"', '""')))
            counts[name] = len(rows)
            # Content digest only: detects in-place updates that keep the row count.
            digests[name] = hashlib.sha256('\n'.join(rows).encode()).hexdigest()
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
        return {'quickCheck': check, 'objects': objects, 'rowCounts': counts, 'tableSha256': digests,
                'active': active, 'authorityId': authority,
                'sidecars': sorted(p.name for p in Path(path).parent.glob(Path(path).name + '-*'))}
    finally:
        db.close()


def census(root, key, database_name='jobs.sqlite3'):
    summary, entries = tree(root, key)
    document = {'schemaVersion': SCHEMA_VERSION, 'tree': summary, 'entries': entries}
    if (Path(root) / database_name).is_file():
        document['database'] = database(Path(root) / database_name)
        document['databaseId'] = path_id(key, database_name)
    return document


def compare(before, after, key, *, allow_paths=(), allow_tables=(), allow_objects=(), allow_new_tables=()):
    """Return the differences that no allowance covers, as counts and opaque IDs."""
    for document in (before, after):
        if document.get('schemaVersion') != SCHEMA_VERSION:
            raise SystemExit('census schema version %s is not supported' % document.get('schemaVersion'))
    allowed = {path_id(key, path) for path in allow_paths}
    old, new = before['entries'], after['entries']
    differences, allowed_changes = [], 0
    for identifier in sorted(set(old) | set(new)):
        a, b = old.get(identifier), new.get(identifier)
        if a is None or b is None:
            change, fields = ('added' if a is None else 'removed'), []
        else:
            names = FILE_FIELDS if 'file' in (a['kind'], b['kind']) else DIRECTORY_FIELDS
            fields = [field for field in names if a.get(field) != b.get(field)]
            if not fields:
                continue
            change = 'contentChanged' if 'sha256' in fields or 'bytes' in fields else 'metadataChanged'
        if identifier in allowed:
            allowed_changes += 1
        else:
            differences.append({'id': identifier, 'change': change, 'fields': fields})
    database_differences = []
    da, db = before.get('database'), after.get('database')
    if (da is None) != (db is None):
        database_differences.append({'database': 'presence'})
    elif da is not None:
        for name in sorted(set(da['objects']) | set(db['objects'])):
            if da['objects'].get(name) != db['objects'].get(name) and name.split(':', 1)[1] not in allow_objects:
                database_differences.append({'object': name})
        for name in sorted(set(da['tableSha256']) | set(db['tableSha256'])):
            if name not in da['tableSha256']:
                if name not in allow_new_tables:
                    database_differences.append({'table': name, 'change': 'added'})
                elif db['rowCounts'][name] and name not in allow_tables:
                    database_differences.append({'table': name, 'change': 'newTableNotEmpty'})
            elif name not in db['tableSha256']:
                database_differences.append({'table': name, 'change': 'removed'})
            elif da['tableSha256'][name] != db['tableSha256'][name] and name not in allow_tables:
                database_differences.append({'table': name, 'change': 'contentChanged'})
        for field in ('quickCheck', 'authorityId', 'active', 'sidecars'):
            if da[field] != db[field]:
                database_differences.append({'field': field})
    for side, document in (('before', before), ('after', after)):
        for name, count in document['tree']['rejected'].items():
            if count:
                database_differences.append({'rejected': name, 'side': side})
    counts = {}
    for item in differences:
        counts[item['change']] = counts.get(item['change'], 0) + 1
    return {'identical': not differences and not database_differences, 'fileDifferences': counts,
            'allowedFileChanges': allowed_changes, 'files': differences, 'database': database_differences}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest='command', required=True)
    keygen = commands.add_parser('keygen', help='write a new private census key file')
    keygen.add_argument('output', type=Path)
    take = commands.add_parser('census', help='census a stopped state directory; key on stdin')
    take.add_argument('root')
    take.add_argument('--database', default='jobs.sqlite3')
    diff = commands.add_parser('compare', help='compare two census documents; key on stdin')
    diff.add_argument('before', type=Path)
    diff.add_argument('after', type=Path)
    for option in ('allow-path', 'allow-table', 'allow-object', 'allow-new-table'):
        diff.add_argument('--' + option, action='append', default=[])
    args = parser.parse_args(argv)
    if args.command == 'keygen':
        os.umask(0o077)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            stream.write(secrets.token_hex(32) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        return 0
    key = read_key(sys.stdin)
    if args.command == 'census':
        result = census(args.root, key, args.database)
        status = 3 if any(result['tree']['rejected'].values()) else 0
    else:
        result = compare(json.loads(args.before.read_text()), json.loads(args.after.read_text()), key,
                         allow_paths=args.allow_path, allow_tables=args.allow_table,
                         allow_objects=args.allow_object, allow_new_tables=args.allow_new_table)
        status = 0 if result['identical'] else 1
    json.dump(result, sys.stdout, sort_keys=True, indent=1)
    sys.stdout.write('\n')
    return status


if __name__ == '__main__':
    raise SystemExit(main())
