"""Read-only custody census for the exact installed 693cb7e closed predecessor.

This does not stop a writer, acquire a lease, reconcile a source spool, delete
anything, or authorize engine-cache cleanup. The cutover controller must prove
writer exclusion separately and compare offline/backup snapshots under one key.
"""
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import uuid

PROFILE = 'installed-693cb7e-closed-v1'
SOURCE = '693cb7eca32295da959a4f42399f317577ef8a8a'
SCHEMA_SHA256 = '4368b50f2ba34bb969e9750b49fdd85478f88183276a1aa164744a5cc2188642'
SCHEMA_PATH = Path(__file__).resolve().parents[1] / 'contracts/coordinator-693cb7e-schema.json'
HEX = re.compile(r'[0-9a-f]{64}\Z')


class CensusRefused(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise CensusRefused(reason)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _json(value):
    def unique(pairs):
        result = {}
        for key, item in pairs:
            _require(key not in result, 'duplicate_metadata_key')
            result[key] = item
        return result
    _require(type(value) is str, 'metadata_text_required')
    return json.loads(value, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(CensusRefused('nonfinite_metadata')))


def _uuid(value):
    _require(type(value) is str and str(uuid.UUID(value)) == value, 'canonical_identity_required')
    return value


def _same(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _owned(path, uid, *, directory=False, root=False):
    _require(path.absolute() == path.resolve() and not any(p.is_symlink() for p in path.parents),
             'original_path_required')
    info = path.lstat()
    _require(info.st_uid == uid and info.st_gid == uid and
             (stat.S_ISDIR(info.st_mode) if directory else
              stat.S_ISREG(info.st_mode) and info.st_nlink == 1), 'owned_regular_state_required')
    mode = stat.S_IMODE(info.st_mode)
    _require(mode in ((0o700, 0o755) if root else (0o700,)) if directory else not mode & 0o022,
             'nonwritable_foreign_state_required')
    return info


def _hash_file(path, uid, limit, key):
    before = _owned(path, uid)
    _require(before.st_size <= limit, 'retained_file_budget_exceeded')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(_same(os.fstat(fd)) == _same(before), 'retained_file_identity_changed')
        plain = hashlib.sha256()
        keyed = hmac.new(key, b'file\0', hashlib.sha256)
        size = 0
        while chunk := os.read(fd, 1024 * 1024):
            size += len(chunk)
            _require(size <= before.st_size, 'retained_file_changed')
            plain.update(chunk); keyed.update(chunk)
        _require(size == before.st_size and _same(before) == _same(os.fstat(fd)) == _same(path.lstat()),
                 'retained_file_changed')
        return dict(bytes=size, sha256=plain.hexdigest(), content=keyed.hexdigest(),
                    mode=stat.S_IMODE(before.st_mode))
    finally:
        os.close(fd)


def _read_small_json(path, uid):
    before = _owned(path, uid)
    _require(before.st_size <= 65536, 'receipt_metadata_budget_exceeded')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(_same(before) == _same(os.fstat(fd)), 'receipt_identity_changed')
        data = os.read(fd, before.st_size + 1)
        _require(len(data) == before.st_size and _same(before) == _same(os.fstat(fd)) == _same(path.lstat()),
                 'receipt_identity_changed')
        return _json(data.decode('utf-8'))
    finally:
        os.close(fd)


def _schema(db):
    profile = _json(SCHEMA_PATH.read_text())
    _require(hashlib.sha256(_canonical(profile)).hexdigest() == SCHEMA_SHA256,
             'reviewed_schema_profile_changed')
    _require(profile['profile'] == PROFILE and profile['sourceCommit'] == SOURCE,
             'reviewed_schema_profile_required')
    objects = [list(row) for row in db.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    normalize = lambda entries: [row[:3] + [re.sub(r'\s+', ' ', row[3]).strip() if row[3] else None]
                                  for row in entries]
    _require(normalize(objects) == normalize(profile['objects']), 'unreviewed_predecessor_schema')
    return objects


def census(root, *, key, authority_id, node_ids, uid, max_artifact_bytes,
           max_metadata_bytes=64 * 1024 * 1024, expected_counts=None,
           logical_root='/prototype-state'):
    """Return private keyed equality evidence; no controller/ownership assertion.

    ``expected_counts`` is an optional reviewed plan checkpoint, never a hardcoded
    terminal-job count. All fields, including callback flags, remain in row hashes.
    Budgets are explicit and independent: artifacts cannot borrow metadata quota.
    """
    _require(type(key) is bytes and len(key) == 32, 'private_census_key_required')
    _uuid(authority_id)
    _require(type(uid) is int and uid >= 0 and type(max_artifact_bytes) is int and
             max_artifact_bytes > 0 and type(max_metadata_bytes) is int and max_metadata_bytes > 0,
             'reviewed_census_budgets_required')
    _require(type(node_ids) in (tuple, list) and node_ids and len(set(node_ids)) == len(node_ids)
             and all(type(n) is str and re.fullmatch('[a-z][a-z0-9_-]{0,63}', n) for n in node_ids),
             'reviewed_node_inventory_required')
    root = Path(root).absolute()
    root_info = _owned(root, uid, directory=True, root=True)
    database = root / 'jobs.sqlite3'
    initial = _owned(database, uid)
    _require(stat.S_IMODE(initial.st_mode) == 0o600, 'private_database_required')
    _require(not any(Path(str(database) + suffix).exists() or Path(str(database) + suffix).is_symlink()
                     for suffix in ('-journal', '-wal', '-shm')), 'sqlite_io_not_quiescent')
    with database.open('rb') as stream:
        header = stream.read(100)
    _require(len(header) == 100 and header[:16] == b'SQLite format 3\0' and header[18:20] == b'\1\1',
             'reviewed_rollback_database_required')
    db = sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        _require(db.execute('PRAGMA quick_check').fetchall()[0][0] == 'ok', 'sqlite_integrity_failed')
        schema = _schema(db)
        _require(not db.execute('PRAGMA foreign_key_check').fetchall(), 'detached_retained_reference')
        tables, rows, logical_bytes, row_count = {}, {}, 0, 0
        for name in sorted(item[1] for item in schema if item[0] == 'table'):
            values = [dict(row) for row in db.execute('SELECT * FROM "' + name + '"')]
            encoded = sorted(_canonical(row) for row in values)
            logical_bytes += sum(len(value) for value in encoded)
            row_count += len(values)
            _require(logical_bytes <= max_metadata_bytes and row_count <= 100000,
                     'predecessor_metadata_budget_exceeded')
            digest = hmac.new(key, b'table\0' + name.encode() + b'\0', hashlib.sha256)
            for value in encoded:
                digest.update(len(value).to_bytes(8, 'big')); digest.update(value)
            tables[name] = dict(count=len(values), content=digest.hexdigest())
            rows[name] = values
        if expected_counts is not None:
            _require(type(expected_counts) is dict and all(type(n) is str and type(c) is int and c >= 0
                     and n in tables and tables[n]['count'] == c for n, c in expected_counts.items()),
                     'reviewed_predecessor_counts_changed')
        _require(rows['coordinator_identity'] == [dict(singleton=1, authority_id=authority_id)] and
                 rows['coordinator_authority_lease'] == [dict(singleton=1, required=1)],
                 'enrolled_predecessor_authority_changed')
        _require({r['id'] for r in rows['nodes']} == set(node_ids), 'predecessor_nodes_changed')
        _require(all(r['state'] == 'active' and not r['plan_id'] and not r['update_hash']
                     for r in rows['nodes']), 'foreign_node_maintenance_pending')
        _require(not rows['owned_maintenance_schedules'] and not rows['viewer_reservations'] and
                 not any(values for name, values in rows.items() if name.startswith('reference_')),
                 'unsettled_predecessor_execution')
        jobs = {r['id']: r for r in rows['jobs']}
        _require(all(r['status'] in ('succeeded', 'failed') and r['environment'] in ('production', 'staging')
                     for r in jobs.values()), 'predecessor_jobs_not_closed')
        for r in jobs.values():
            _uuid(r['id'])
            if r['result'] is not None: _json(r['result'])
        expected_files = {'jobs.sqlite3': None, 'coordinator.lock': None}
        expected_dirs = {'viewer-tasks', 'viewer-spool'}
        tasks, inputs_by_task = [], {}
        for item in rows['viewer_files']:
            inputs_by_task.setdefault(item['uuid'], []).append(item)
        for task in rows['viewer_tasks']:
            tasks.append((task, inputs_by_task.get(task['uuid'], [])))
        current = {r['uuid']: r for r in rows['viewer_tasks']}
        _require(all(r['uuid'] in current for r in rows['viewer_files']), 'orphan_viewer_file')
        for history in rows['viewer_incarnations']:
            task, files = _json(history['metadata']), _json(history['files'])
            _require(type(task) is dict and type(files) is list and task['uuid'] == history['uuid']
                     and task['generation'] == history['generation'] and task['job_id'] == history['job_id']
                     and history['uuid'] in current and history['generation'] < current[history['uuid']]['generation'],
                     'detached_viewer_incarnation')
            tasks.append((task, files))
        task_keys, job_owners, evidence = set(), set(), {}
        for item in rows['viewer_option_evidence']:
            identity = (item['uuid'], item['generation'])
            definitions = _json(item['definitions'])
            _require(type(definitions) is list and HEX.fullmatch(item['sha256']) is not None and
                     hashlib.sha256(item['definitions'].encode()).hexdigest() == item['sha256'],
                     'invalid_option_evidence')
            evidence[identity] = item
        archive_checks = []
        for task, inputs in tasks:
            identifier = _uuid(task['uuid']); generation = task['generation']
            _require(type(generation) is int and generation > 0, 'invalid_viewer_generation')
            identity = (identifier, generation)
            _require(identity not in task_keys and task['job_id'] not in job_owners, 'duplicate_viewer_owner')
            task_keys.add(identity); job_owners.add(task['job_id'])
            job = jobs.get(task['job_id'])
            request = identifier if generation == 1 else str(uuid.uuid5(uuid.UUID(identifier), 'generation:' + str(generation)))
            _require(job is not None and job['status'] == 'succeeded' and job['profile'] == 'odm-viewer-v1'
                     and job['environment'] == task['environment'] and job['request_id'] == request and
                     task['stage'] == 'succeeded' and task['phase'] == 'complete' and
                     task['node_id'] in node_ids and task['node_id'] == job['node_id'] and
                     type(task['claim_token']) is str and task['claim_token'] and
                     hashlib.sha256(task['claim_token'].encode()).hexdigest() == job['claim_hash'] and
                     type(task['engine_uuid']) is str and task['engine_uuid'], 'viewer_execution_binding_invalid')
            _require(type(task['directory']) is str and re.fullmatch(re.escape(identifier) + '-' + str(generation)
                     + '(?:-[0-9a-f]{16})?', task['directory']), 'viewer_directory_binding_invalid')
            directory = 'viewer-tasks/' + task['directory']
            _require(directory not in expected_dirs, 'duplicate_viewer_directory')
            expected_dirs.add(directory)
            _require(task.get('option_evidence_required', 0) in (0, 1) and
                     bool(task.get('option_evidence_required')) == (identity in evidence), 'missing_option_evidence')
            for field in ('options', 'outputs', 'output_lines'): _json(task[field])
            for item in inputs:
                _require(item['uuid'] == identifier and type(item['path']) is str and
                         re.fullmatch('input-[0-9a-f]{32}', item['path']) and type(item['bytes']) is int
                         and item['bytes'] > 0 and type(item['sha256']) is str and HEX.fullmatch(item['sha256'])
                         and item['is_photo'] in (0, 1) and type(item['name']) is str and
                         re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._ -]{0,179}', item['name']) and
                         not item['name'].endswith((' ', '.')) and item['name_key'] == item['name'].casefold() and
                         Path(item['name']).suffix.lower() in ('.jpg','.jpeg','.png','.tif','.tiff','.txt','.json','.csv') and
                         bool(item['is_photo']) == (Path(item['name']).suffix.lower() in ('.jpg','.jpeg','.png','.tif','.tiff')),
                         'viewer_input_binding_invalid')
                relative = directory + '/' + item['path']
                _require(relative not in expected_files, 'duplicate_viewer_input')
                expected_files[relative] = (item['bytes'], item['sha256'])
            archive = directory + '/all.zip'
            if task['archive_path'] is not None:
                _require(task['archive_path'] == logical_root.rstrip('/') + '/' + archive,
                         'viewer_archive_path_changed')
            if (root / archive).exists() or task['archive_path'] is not None:
                receipt_path = archive + '.receipt'
                receipt = _read_small_json(root / receipt_path, uid)
                identity_fields = dict(version=1, task=identifier, generation=generation,
                    job=task['job_id'], node=task['node_id'], engine=task['engine_uuid'])
                _require(type(receipt) is dict and set(receipt) == set(identity_fields) | {'bytes', 'sha256'}
                         and all(type(receipt[k]) is type(v) and receipt[k] == v for k, v in identity_fields.items())
                         and type(receipt['bytes']) is int and receipt['bytes'] >= 4 and
                         type(receipt['sha256']) is str and HEX.fullmatch(receipt['sha256']),
                         'archive_receipt_binding_invalid')
                expected_files[archive] = (receipt['bytes'], receipt['sha256'])
                expected_files[receipt_path] = None
                archive_checks.append(archive)
        _require(set(evidence) <= task_keys, 'orphan_option_evidence')
        page_size, page_count = db.execute('PRAGMA page_size').fetchone()[0], db.execute('PRAGMA page_count').fetchone()[0]
        # A bounded logical-to-physical envelope includes per-row/index page
        # overhead and reviewed free-page headroom. Large retained artifacts have
        # their own budget and do not increase this SQLite allowance.
        sqlite_bound = 4 * max_metadata_bytes + (4 * row_count + 256) * page_size
        _require(page_count * page_size == initial.st_size <= sqlite_bound, 'sqlite_physical_budget_exceeded')
        files, total_artifact, directories = {}, 0, {}
        for path in root.rglob('*'):
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                _require(relative in expected_dirs, 'unknown_predecessor_path')
                directories[relative] = _same(_owned(path, uid, directory=True))
                continue
            _require(relative in expected_files, 'unknown_predecessor_path')
            result = _hash_file(path, uid, sqlite_bound if relative == 'jobs.sqlite3' else max_artifact_bytes, key)
            expected = expected_files[relative]
            _require(expected is None or (result['bytes'], result['sha256']) == expected,
                     'retained_artifact_digest_mismatch')
            if relative not in ('jobs.sqlite3', 'coordinator.lock'):
                total_artifact += result['bytes']
                _require(total_artifact <= max_artifact_bytes, 'retained_artifact_budget_exceeded')
            path_id = hmac.new(key, b'path\0' + relative.encode(), hashlib.sha256).hexdigest()
            files[path_id] = {k: v for k, v in result.items() if k != 'sha256'}
        _require(all((root / path).is_file() for path in expected_files) and
                 all((root / path).is_dir() for path in expected_dirs), 'missing_retained_path')
        _require(all(_same((root / path).lstat()) == identity for path, identity in directories.items()),
                 'retained_directory_changed_during_census')
        _require(_same(initial) == _same(database.lstat()) and _same(root_info) == _same(root.lstat()),
                 'predecessor_changed_during_census')
        return dict(schemaVersion=1, profile=PROFILE, sourceCommit=SOURCE, authorityId=authority_id,
                    schemaSha256=hashlib.sha256(_canonical(schema)).hexdigest(), tables=tables, files=files,
                    metadataBytes=logical_bytes, artifactBytes=total_artifact, sqliteBytes=initial.st_size,
                    sqlitePhysicalBound=sqlite_bound, archiveReceipts=len(archive_checks),
                    rootMode=stat.S_IMODE(root_info.st_mode), writerExclusionProven=False,
                    signedViewerAcceptanceProven=False)
    finally:
        db.close()
