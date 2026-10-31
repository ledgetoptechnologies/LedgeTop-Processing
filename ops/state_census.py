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
- ``--schema-allowlist FILE`` adds exact schema-object allowances from a file
  written by ``schema-allowlist``. The loader refuses any file that is not
  exactly ``{schemaAllowlistVersion: 1, objects: {TYPE:NAME: SHA256}}``, so the
  file can never carry path, table or new-table allowances, or other options.

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
MAX_TREE_ENTRIES = 250_000


def read_key(stream):
    text = stream.read().strip()
    if not re.fullmatch(r'[0-9a-f]{64}', text):
        raise SystemExit('a 64-hex-digit census key is required on stdin')
    return bytes.fromhex(text)


def _keyed(key, data=b''):
    return hmac.new(key, data, hashlib.sha256)


def path_id(key, relative):
    return _keyed(key, b'path\0' + relative.encode('utf-8', 'surrogateescape')).hexdigest()


def _content(key, path, expected):
    digest = _keyed(key, b'content\0')
    # Bind the content digest to the object discovered by the walk. A stopped
    # volume is a prerequisite; this check detects changes, it is not a lock.
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_BINARY', 0))
    try:
        def identity(info):
            fields = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
                      info.st_nlink, info.st_size, info.st_mtime_ns)
            # Windows may report different ctime through a handle and path.
            return fields if os.name == 'nt' else fields + (info.st_ctime_ns,)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or identity(before) != identity(expected):
            raise OSError('census file changed')
        total = 0
        while chunk := os.read(fd, 1 << 20):
            total += len(chunk)
            if total > before.st_size:
                raise OSError('census file changed')
            digest.update(chunk)
        if total != before.st_size or identity(os.fstat(fd)) != identity(before) or identity(path.lstat()) != identity(before):
            raise OSError('census file changed')
    finally:
        os.close(fd)
    return digest.hexdigest()


def _entry(key, path, info):
    entry = {'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid, 'gid': info.st_gid,
             'mtime': int(info.st_mtime)}
    if stat.S_ISDIR(info.st_mode):
        entry['kind'] = 'directory'
    elif stat.S_ISREG(info.st_mode):
        entry.update(kind='file', bytes=info.st_size, links=info.st_nlink, content=_content(key, path, info))
    elif stat.S_ISLNK(info.st_mode):
        entry['kind'] = 'symlink'
    else:
        entry['kind'] = 'other'
    return entry


def tree(root, key, *, limits=None):
    root = Path(root)
    root_info = root.lstat()
    entries = {path_id(key, '.'): dict(_entry(key, root, root_info), root=True)}
    errors = []
    directories = []
    pending = [(root, root_info)]
    discovered = 1
    incoming = 0
    spool_entries = None
    total_bytes = 0
    entry_limit = MAX_TREE_ENTRIES if limits is None else min(MAX_TREE_ENTRIES, limits['entries'])

    def failed(error):
        errors.append(type(error).__name__)

    def directory_identity(info):
        if not stat.S_ISDIR(info.st_mode):
            raise OSError('census directory changed')
        return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
                info.st_mtime_ns, info.st_ctime_ns)

    def names_in(directory):
        names = []
        with os.scandir(directory) as scan:
            for item in scan:
                names.append(item.name)
                if len(names) > entry_limit:
                    raise OSError('census entry limit')
        return tuple(sorted(names))

    while pending:
        directory, observed = pending.pop()
        try:
            before = directory_identity(directory.lstat())
            if before != directory_identity(observed):
                raise OSError('census directory changed')
            names = names_in(directory)
            if directory_identity(directory.lstat()) != before:
                raise OSError('census directory changed')
            discovered += len(names)
            if discovered > entry_limit:
                raise OSError('census entry limit')
            directories.append((directory, before, names))
            if directory == root / 'viewer-spool':
                spool_entries = len(names)
            for name in names:
                path = directory / name
                info = path.lstat()
                if limits is not None and stat.S_ISREG(info.st_mode):
                    if info.st_size > limits['file_bytes'] or total_bytes + info.st_size > limits['total_bytes']:
                        raise OSError('census byte limit')
                    total_bytes += info.st_size
                entries[path_id(key, path.relative_to(root).as_posix())] = _entry(key, path, info)
                if name.startswith('.incoming-'):
                    incoming += 1
                if stat.S_ISDIR(info.st_mode):
                    pending.append((path, info))
        except OSError as error:
            failed(error)

    # Detect persistent membership or metadata changes after a directory was
    # first scanned. This remains a point-in-time check, never a writer fence.
    for directory, identity, names in directories:
        try:
            if directory_identity(directory.lstat()) != identity or names_in(directory) != names:
                raise OSError('census directory changed')
        except OSError as error:
            failed(error)
    values = list(entries.values())
    rejected = {'symlinks': sum(e['kind'] == 'symlink' for e in values),
                'other': sum(e['kind'] == 'other' for e in values),
                'hardlinked': sum(e.get('links', 1) > 1 for e in values),
                'unreadable': len(errors)}
    canonical = json.dumps(sorted(entries.items()), sort_keys=True, separators=(',', ':')).encode()
    summary = {'treeDigest': _keyed(key, canonical).hexdigest(), 'entries': len(values),
               'regularFiles': sum(e['kind'] == 'file' for e in values),
               'bytes': sum(e.get('bytes', 0) for e in values),
               'incomingPartials': incoming,
               'spoolEntries': spool_entries,
               'rejected': rejected}
    return summary, entries


VIEWER_LEGACY_COLUMNS = ('uuid','generation','directory','name','options','outputs','environment',
    'stage','phase','job_id','node_id','engine_uuid','claim_token','error','progress','output_lines',
    'archive_path','removed','created_at','updated_at')
VIEWER_NEW_COLUMN = 'option_evidence_required'
# Exact d738â†’aa5b33b SQLite DDL, independently checked against the committed
# legacy schema and the specific ADD COLUMN statement. Other variants hold.
VIEWER_LEGACY_SCHEMA_SHA256 = 'e7bbc127917f0c8c21007a79f35b434d00a1446fed25b369e1b3ab7579c628a3'
VIEWER_MIGRATED_SCHEMA_SHA256 = 'a637e43b58241be981e0c056d8e6fd7d4a83e7c590598f837f6fccfdcf77d424'

def database(path, key, *, limits=None):
    # immutable mode ignores WAL: callers must census stopped, checkpointed
    # state with no live writer or SQLite sidecar; it is not a live snapshot.
    with Path(path).open('rb') as stream:
        header = stream.read(20)
    header_mode = list(header[18:20]) if len(header) == 20 and header[:16] == b'SQLite format 3\x00' else None
    uri = 'file:' + str(path) + '?mode=ro&immutable=1'
    db = sqlite3.connect(uri, uri=True)
    try:
        if limits is not None:
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, limits['row_bytes'])
        check = db.execute('PRAGMA quick_check').fetchone()[0]
        objects = {'%s:%s' % (kind, name): hashlib.sha256((sql or '').encode()).hexdigest()
                   for kind, name, sql in db.execute('SELECT type,name,sql FROM sqlite_master')}
        tables = sorted(name for name, in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*'"))
        counts, digests = {}, {}
        total_rows = 0
        materialized_rows = 0
        total_row_bytes = 0
        for name in tables:
            if limits is not None:
                count = db.execute('SELECT COUNT(*) FROM "%s"' % name.replace('"', '""')).fetchone()[0]
                total_rows += count
                if total_rows > limits['rows']:
                    raise ValueError('census row limit')
            rows = []
            for row in db.execute('SELECT * FROM "%s"' % name.replace('"', '""')):
                rendered = repr(tuple(row))
                if limits is not None:
                    materialized_rows += 1
                    total_row_bytes += len(rendered.encode())
                    if materialized_rows > limits['rows'] or total_row_bytes > limits['row_total_bytes']:
                        raise ValueError('census row materialization limit')
                rows.append(rendered)
            rows.sort()
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
        viewer_projection = None
        if 'viewer_tasks' in tables:
            column_info = db.execute('PRAGMA table_xinfo(viewer_tasks)').fetchall()
            columns = tuple(row[1] for row in column_info)
            if all(row[6] == 0 for row in column_info) and columns in (VIEWER_LEGACY_COLUMNS, VIEWER_LEGACY_COLUMNS + (VIEWER_NEW_COLUMN,)):
                names = ','.join('"'+name+'"' for name in VIEWER_LEGACY_COLUMNS)
                rows = []
                projection_bytes = 0
                for row in db.execute('SELECT '+names+' FROM viewer_tasks'):
                    rendered = repr(tuple(row))
                    projection_bytes += len(rendered.encode())
                    if limits is not None and (len(rows)>=limits['rows'] or projection_bytes>limits['row_total_bytes']):
                        raise ValueError('census projection limit')
                    rows.append(rendered)
                rows.sort()
                invalid = None if columns == VIEWER_LEGACY_COLUMNS else db.execute(
                    "SELECT count(*) FROM viewer_tasks WHERE option_evidence_required IS NOT 0 OR typeof(option_evidence_required) != 'integer'").fetchone()[0]
                viewer_projection = {'columns':list(columns), 'digest':_keyed(key,b'viewer-legacy\0'+'\n'.join(rows).encode()).hexdigest(), 'invalidDefaults':invalid}
        authority = None
        if 'coordinator_identity' in tables:
            row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            authority = row[0] if row else None
        return {'quickCheck': check, 'objects': objects, 'rowCounts': counts, 'tableDigest': digests,
                'viewerTaskProjection': viewer_projection, 'sqliteHeaderMode':header_mode,
                'active': active, 'authorityId': authority,
                'sidecars': sorted(p.name for p in Path(path).parent.glob(Path(path).name + '-*'))}
    finally:
        db.close()


def census(root, key, database_name='jobs.sqlite3', *, limits=None):
    summary, entries = tree(root, key, limits=limits)
    document = {'schemaVersion': SCHEMA_VERSION, 'tree': summary, 'entries': entries}
    if (Path(root) / database_name).is_file():
        document['database'] = database(Path(root) / database_name, key, limits=limits)
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


OBJECT_NAME = re.compile(r'(?:table|index|view|trigger):[A-Za-z0-9_]{1,128}')
DIGEST = re.compile(r'[0-9a-f]{64}')
ALLOWLIST_VERSION = 1


def parse_object_allowances(values):
    allowed = {}
    for value in values:
        name, separator, digest = value.partition('=')
        if not separator or not OBJECT_NAME.fullmatch(name) or not DIGEST.fullmatch(digest):
            raise SystemExit('--allow-object needs TYPE:NAME=SHA256')
        if allowed.setdefault(name, digest) != digest:
            raise SystemExit('conflicting allowances for ' + name)
    return allowed


def load_schema_allowlist(path):
    """Load a schema-only allowlist file; anything broader or malformed refuses.

    The file can only ever name exact schema-object hashes. It cannot carry
    path, table or new-table allowances, or command-line options of any kind.
    """
    try:
        document = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, UnicodeError, ValueError):
        raise SystemExit('schema allowlist is unreadable or not JSON') from None
    if (type(document) is not dict or set(document) != {'schemaAllowlistVersion', 'objects'}
            or document['schemaAllowlistVersion'] != ALLOWLIST_VERSION
            or type(document['objects']) is not dict or not document['objects']):
        raise SystemExit('schema allowlist must be exactly {schemaAllowlistVersion: 1, objects: {...}}')
    for name, digest in document['objects'].items():
        if not OBJECT_NAME.fullmatch(name) or type(digest) is not str or not DIGEST.fullmatch(digest):
            raise SystemExit('schema allowlist entries must be TYPE:NAME to a 64-hex SQL hash')
    return ['%s=%s' % item for item in sorted(document['objects'].items())]


def make_schema_allowlist(document, names):
    """Take exact schema-object hashes for the named objects from a census."""
    if not names or len(set(names)) != len(names):
        raise SystemExit('name each schema object exactly once')
    objects = (document.get('database') or {}).get('objects') or {}
    result = {}
    for name in names:
        if not OBJECT_NAME.fullmatch(name):
            raise SystemExit('object must be TYPE:NAME')
        if name not in objects:
            raise SystemExit('object not present in census: ' + name)
        result[name] = objects[name]
    return {'schemaAllowlistVersion': ALLOWLIST_VERSION, 'objects': result}


# Must equal processing.authority_lease.MARKER_DDL exactly (a test enforces it).
# This file runs alone inside the old image, so it cannot import that module.
LEASE_MARKER_TABLE = 'coordinator_authority_lease'
LEASE_MARKER_DDL = ('CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), '
                    'required INTEGER NOT NULL CHECK(required=1))')
LEASE_MARKER_ROWS = [(1, 1)]


def lease_marker_digest(key):
    rows = sorted(repr(row) for row in LEASE_MARKER_ROWS)
    return _keyed(key, b'table\0' + '\n'.join(rows).encode()).hexdigest()


def compare(before, after, key, *, allow_paths=(), allow_tables=(), allow_objects=(), allow_new_tables=(),
            allow_lease_marker=False, allow_viewer_task_default_column=False):
    """Return the differences that no allowance covers, as counts and opaque IDs.

    ``allow_lease_marker`` permits exactly one thing: the authority-lease marker
    table appearing with its exact DDL and its single ``(1, 1)`` row. It also
    asserts the marker is present afterwards. No other table or row is relaxed.
    """
    for document in (before, after):
        if document.get('schemaVersion') != SCHEMA_VERSION:
            raise SystemExit('census schema version %s is not supported' % document.get('schemaVersion'))
    if allow_viewer_task_default_column and 'viewer_tasks' in allow_tables:
        raise SystemExit('viewer_tasks cannot use both broad and narrow allowances')
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
    if allow_viewer_task_default_column and (da is None or db is None):
        database_differences.append({'table':'viewer_tasks','change':'legacyProjectionRequiresDatabase'})
    if allow_lease_marker and (da is None or db is None):
        # The marker can only be proven inside a database: never pass vacuously.
        database_differences.append({'database': 'leaseMarkerRequiresDatabase'})
    if (da is None) != (db is None):
        database_differences.append({'database': 'presence'})
    elif da is not None:
        marker_object = 'table:' + LEASE_MARKER_TABLE
        marker_hash = hashlib.sha256(LEASE_MARKER_DDL.encode()).hexdigest()
        for name in sorted(set(da['objects']) | set(db['objects'])):
            was, now = da['objects'].get(name), db['objects'].get(name)
            if allow_lease_marker and name == marker_object and was is None and now == marker_hash:
                continue
            if was != now and (now is None or object_allowances.get(name) != now):
                database_differences.append({'object': name})
        if allow_lease_marker:
            # Asserted on the after-state unconditionally: a marker that was
            # already present (even identically malformed or empty in both
            # censuses) must still have the exact DDL and the single row.
            present = db['objects'].get(marker_object)
            if present is None:
                database_differences.append({'object': marker_object, 'change': 'leaseMarkerMissing'})
            else:
                if present != marker_hash:
                    database_differences.append({'object': marker_object, 'change': 'leaseMarkerSchemaInvalid'})
                if (db['rowCounts'].get(LEASE_MARKER_TABLE) != 1
                        or db['tableDigest'].get(LEASE_MARKER_TABLE) != lease_marker_digest(key)):
                    database_differences.append({'table': LEASE_MARKER_TABLE, 'change': 'leaseMarkerInvalid'})
        for name, expected in sorted(object_allowances.items()):
            if db['objects'].get(name) != expected:
                database_differences.append({'object': name, 'change': 'allowanceNotMatched'})
        if allow_viewer_task_default_column and (da.get('sidecars') or db.get('sidecars') or da.get('sqliteHeaderMode') != [1,1] or db.get('sqliteHeaderMode') != [1,1]):
            database_differences.append({'table':'viewer_tasks','change':'requiresCheckpointedDatabase'})
        old_projection=da.get('viewerTaskProjection');new_projection=db.get('viewerTaskProjection')
        viewer_qualifies = (isinstance(old_projection,dict) and isinstance(new_projection,dict)
            and da['objects'].get('table:viewer_tasks')==VIEWER_LEGACY_SCHEMA_SHA256
            and db['objects'].get('table:viewer_tasks')==VIEWER_MIGRATED_SCHEMA_SHA256
            and object_allowances.get('table:viewer_tasks')==VIEWER_MIGRATED_SCHEMA_SHA256
            and old_projection.get('columns')==list(VIEWER_LEGACY_COLUMNS)
            and new_projection.get('columns')==list(VIEWER_LEGACY_COLUMNS+(VIEWER_NEW_COLUMN,))
            and old_projection.get('invalidDefaults') is None
            and type(new_projection.get('invalidDefaults')) is int and new_projection['invalidDefaults']==0
            and isinstance(old_projection.get('digest'),str) and len(old_projection['digest'])==64
            and old_projection['digest']==new_projection.get('digest')
            and 'viewer_tasks' in da['rowCounts'] and 'viewer_tasks' in db['rowCounts']
            and da['rowCounts']['viewer_tasks']==db['rowCounts']['viewer_tasks'])
        if allow_viewer_task_default_column and not viewer_qualifies:
            database_differences.append({'table':'viewer_tasks','change':'legacyProjectionMismatch'})
        for name in sorted(set(da['tableDigest']) | set(db['tableDigest'])):
            if name not in da['tableDigest']:
                if allow_lease_marker and name == LEASE_MARKER_TABLE:
                    pass  # validated unconditionally above
                elif name not in allow_new_tables:
                    database_differences.append({'table': name, 'change': 'added'})
                elif db['rowCounts'][name]:
                    # Never relaxed by --allow-table: a new table must be empty.
                    database_differences.append({'table': name, 'change': 'newTableNotEmpty'})
            elif name not in db['tableDigest']:
                database_differences.append({'table': name, 'change': 'removed'})
            elif da['tableDigest'][name] != db['tableDigest'][name] and name not in allow_tables:
                qualifies = name=='viewer_tasks' and allow_viewer_task_default_column and viewer_qualifies
                if not qualifies: database_differences.append({'table': name, 'change': 'contentChanged'})
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
    diff.add_argument('--allow-viewer-task-default-column', action='store_true',
                      help='permit only preserved legacy viewer_tasks rows with a new zero-valued option_evidence_required column')
    diff.add_argument('--allow-lease-marker', action='store_true',
                      help='allow and require exactly the authority-lease marker table and its single row')
    diff.add_argument('--schema-allowlist', type=Path,
                      help='schema-only allowlist file; refuses anything but exact object hashes')
    make = commands.add_parser('schema-allowlist', help='write exact schema-object hashes from a census')
    make.add_argument('census', type=Path)
    make.add_argument('--object', action='append', default=[], required=True)
    make.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == 'schema-allowlist':
        document = make_schema_allowlist(json.loads(args.census.read_text()), args.object)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(document, stream, sort_keys=True, indent=1)
            stream.write('\n')
        load_schema_allowlist(args.output)  # the written file must pass the same strict loader
        return 0
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
        objects = list(args.allow_object)
        if args.schema_allowlist is not None:
            objects += load_schema_allowlist(args.schema_allowlist)
        result = compare(json.loads(args.before.read_text()), json.loads(args.after.read_text()), key,
                         allow_paths=args.allow_path, allow_tables=args.allow_table,
                         allow_objects=objects, allow_new_tables=args.allow_new_table,
                         allow_lease_marker=args.allow_lease_marker,
                         allow_viewer_task_default_column=args.allow_viewer_task_default_column)
        status = 0 if result['identical'] else 1
    json.dump(result, sys.stdout, sort_keys=True, indent=1)
    sys.stdout.write('\n')
    return status


if __name__ == '__main__':
    raise SystemExit(main())
