"""Read-only census and comparison of a stopped coordinator state directory.

``census`` walks a stopped volume mounted read-only and records, for every
entry, its kind, mode, owner, size, whole-second mtime and contents. SQLite is
opened with ``immutable=1``, so no journal, WAL or shared-memory file is
created. Every client-derived value is keyed by HMAC-SHA256 under a per-run
secret read from stdin: path identifiers, file contents and table digests. A
known photo or a guessable name such as ``DJI_0001.JPG`` therefore cannot be
matched against the output without the key. Only schema-object hashes are
plain SHA-256, because schema SQL is public source, so an exact expected hash
can be reviewed. Sizes and row counts are printed in clear. Row contents,
tokens and names are never printed. Symlinks, special files, hard-linked files
and unreadable entries are rejected (exit 3): their copy and restore semantics
are not qualified.

``compare`` diffs two census documents made with the same key. Allowances are
exact and narrow:

- ``--allow-path PATH:FIELD[,FIELD]`` lets only the named fields of an entry
  that exists on both sides differ. Adding, removing or changing the kind of an
  entry is never allowed.
- ``--allow-object TYPE:NAME=SHA256`` lets one schema object change only to
  that exact expected SQL hash. It is never allowed to disappear.
- ``--allow-table NAME`` lets the content of a table that exists on **both**
  sides differ.
- ``--allow-new-table NAME`` lets a table appear only if it is empty.

Equal censuses show equality at the two moments they were taken. They do not
prove that nothing was written and reverted in between.
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
FILE_FIELDS = ('kind', 'mode', 'uid', 'gid', 'mtime', 'bytes', 'content')
DIRECTORY_FIELDS = ('kind', 'mode', 'uid', 'gid', 'mtime')
ALLOWABLE_FIELDS = frozenset(FILE_FIELDS) - {'kind'}
SCHEMA_VERSION = 3


def read_key(stream):
    text = stream.read().strip()
    if not re.fullmatch(r'[0-9a-f]{64}', text):
        raise SystemExit('a 64-hex-digit census key is required on stdin')
    return bytes.fromhex(text)


def _keyed(key, data=b''):
    return hmac.new(key, data, hashlib.sha256)


def path_id(key, relative):
    return _keyed(key, b'path\0' + relative.encode('utf-8', 'surrogateescape')).hexdigest()


def _content(key, path):
    digest = _keyed(key, b'content\0')
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _entry(key, path, info):
    entry = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
             'mtime': int(info.st_mtime)}
    if stat.S_ISDIR(info.st_mode):
        entry['kind'] = 'directory'
    elif stat.S_ISREG(info.st_mode):
        entry.update(kind='file', bytes=info.st_size, links=info.st_nlink, content=_content(key, path))
    elif stat.S_ISLNK(info.st_mode):
        entry['kind'] = 'symlink'
    else:
        entry['kind'] = 'other'
    return entry


def tree(root, key):
    root = Path(root)
    entries = {path_id(key, '.'): dict(_entry(key, root, root.lstat()), root=True)}
    errors = []

    def failed(error):
        errors.append(type(error).__name__)

    for directory, names, files in os.walk(root, followlinks=False, onerror=failed):
        names.sort()
        for name in sorted(names) + sorted(files):
            path = Path(directory) / name
            try:
                entries[path_id(key, path.relative_to(root).as_posix())] = _entry(key, path, path.lstat())
            except OSError as error:
                failed(error)
    values = list(entries.values())
    rejected = {'symlinks': sum(e['kind'] == 'symlink' for e in values),
                'other': sum(e['kind'] == 'other' for e in values),
                'hardlinked': sum(e.get('links', 1) > 1 for e in values),
                'unreadable': len(errors)}
    canonical = json.dumps(sorted(entries.items()), sort_keys=True, separators=(',', ':')).encode()
    spool = root / 'viewer-spool'
    summary = {'treeDigest': _keyed(key, canonical).hexdigest(), 'entries': len(values),
               'regularFiles': sum(e['kind'] == 'file' for e in values),
               'bytes': sum(e.get('bytes', 0) for e in values),
               'incomingPartials': sum(1 for _ in root.rglob('.incoming-*')),
               'spoolEntries': sum(1 for _ in spool.iterdir()) if spool.is_dir() else None,
               'rejected': rejected}
    return summary, entries


def database(path, key):
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
            # Keyed so that a guessed row cannot be confirmed; detects same-count updates.
            digests[name] = _keyed(key, b'table\0' + '\n'.join(rows).encode()).hexdigest()
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
        return {'quickCheck': check, 'objects': objects, 'rowCounts': counts, 'tableDigest': digests,
                'active': active, 'authorityId': authority,
                'sidecars': sorted(p.name for p in Path(path).parent.glob(Path(path).name + '-*'))}
    finally:
        db.close()


def census(root, key, database_name='jobs.sqlite3'):
    summary, entries = tree(root, key)
    document = {'schemaVersion': SCHEMA_VERSION, 'tree': summary, 'entries': entries}
    if (Path(root) / database_name).is_file():
        document['database'] = database(Path(root) / database_name, key)
    return document


def parse_path_allowances(values, key):
    allowed = {}
    for value in values:
        path, separator, fields = value.rpartition(':')
        names = set(fields.split(',')) if fields else set()
        if not separator or not path or not names or not names <= ALLOWABLE_FIELDS:
            raise SystemExit('--allow-path needs PATH:FIELD[,FIELD] with fields from %s'
                             % ','.join(sorted(ALLOWABLE_FIELDS)))
        allowed.setdefault(path_id(key, path), set()).update(names)
    return allowed


def parse_object_allowances(values):
    allowed = {}
    for value in values:
        match = re.fullmatch(r'((?:table|index|view|trigger):[^=]+)=([0-9a-f]{64})', value)
        if not match:
            raise SystemExit('--allow-object needs TYPE:NAME=SHA256')
        allowed[match[1]] = match[2]
    return allowed


def compare(before, after, key, *, allow_paths=(), allow_tables=(), allow_objects=(), allow_new_tables=()):
    """Return the differences that no allowance covers, as counts and opaque IDs."""
    for document in (before, after):
        if document.get('schemaVersion') != SCHEMA_VERSION:
            raise SystemExit('census schema version %s is not supported' % document.get('schemaVersion'))
    path_allowances = parse_path_allowances(allow_paths, key)
    object_allowances = parse_object_allowances(allow_objects)
    old, new = before['entries'], after['entries']
    differences, allowed_changes = [], 0
    for identifier in sorted(set(old) | set(new)):
        a, b = old.get(identifier), new.get(identifier)
        if a is None or b is None:
            differences.append({'id': identifier, 'change': 'added' if a is None else 'removed', 'fields': []})
            continue
        names = FILE_FIELDS if 'file' in (a['kind'], b['kind']) else DIRECTORY_FIELDS
        fields = [field for field in names if a.get(field) != b.get(field)]
        if not fields:
            continue
        unallowed = [field for field in fields if field not in path_allowances.get(identifier, ())]
        if not unallowed:
            allowed_changes += 1
            continue
        change = 'contentChanged' if {'content', 'bytes'} & set(unallowed) else 'metadataChanged'
        differences.append({'id': identifier, 'change': change, 'fields': unallowed})
    database_differences = []
    da, db = before.get('database'), after.get('database')
    if (da is None) != (db is None):
        database_differences.append({'database': 'presence'})
    elif da is not None:
        for name in sorted(set(da['objects']) | set(db['objects'])):
            was, now = da['objects'].get(name), db['objects'].get(name)
            if was != now and (now is None or object_allowances.get(name) != now):
                database_differences.append({'object': name})
        for name, expected in sorted(object_allowances.items()):
            if db['objects'].get(name) != expected:
                database_differences.append({'object': name, 'change': 'allowanceNotMatched'})
        for name in sorted(set(da['tableDigest']) | set(db['tableDigest'])):
            if name not in da['tableDigest']:
                if name not in allow_new_tables:
                    database_differences.append({'table': name, 'change': 'added'})
                elif db['rowCounts'][name]:
                    # Never relaxed by --allow-table: a new table must be empty.
                    database_differences.append({'table': name, 'change': 'newTableNotEmpty'})
            elif name not in db['tableDigest']:
                database_differences.append({'table': name, 'change': 'removed'})
            elif da['tableDigest'][name] != db['tableDigest'][name] and name not in allow_tables:
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
            'allowedFileChanges': allowed_changes, 'files': differences, 'database': database_differences,
            'scope': 'observed equality at two census times; transient writes are not excluded'}


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
