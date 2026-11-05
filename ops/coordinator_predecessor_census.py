"""Read-only custody census for the exact installed 693cb7e closed predecessor.

This does not stop a writer, acquire a lease, reconcile a source spool, delete
anything, or authorize engine-cache cleanup. The cutover controller must prove
writer exclusion separately and compare offline/backup snapshots under one key.
"""
from datetime import datetime
import base64
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time
import uuid

PROFILE = 'installed-693cb7e-closed-v1'
PROBE_PROFILE = 'installed-693cb7e-metadata-diagnostic-v1'
SOURCE = '693cb7eca32295da959a4f42399f317577ef8a8a'
SCHEMA_SHA256 = '4368b50f2ba34bb969e9750b49fdd85478f88183276a1aa164744a5cc2188642'
_MODULE_PATH = Path(__file__) if globals().get('__file__') and not __file__.startswith('<') else Path('/__diagnostic__/probe.py')
SCHEMA_PATH = _MODULE_PATH.resolve().parents[1] / 'contracts/coordinator-693cb7e-schema.json'
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


class _ReadBudget:
    """Cooperative bounds for direct retained-file reads; no timeout thread."""
    def __init__(self, seconds, maximum):
        _require(seconds is None or type(seconds) in (int,float) and math.isfinite(seconds) and seconds > 0,
                 'positive_duration_bound_required')
        _require(maximum is None or type(maximum) is int and maximum > 0, 'positive_read_bound_required')
        self.deadline = time.monotonic() + seconds if seconds is not None else None
        self.maximum, self.used = maximum, 0

    def check(self):
        _require(self.deadline is None or time.monotonic() < self.deadline, 'census_duration_exceeded')

    def preflight(self, amount):
        self.check()
        _require(self.maximum is None or amount <= self.maximum-self.used, 'census_read_budget_exceeded')

    def read(self, descriptor, amount):
        self.preflight(amount)
        value = os.read(descriptor, amount)
        self.used += len(value)
        self.check()
        return value


def _hash_file(path, uid, limit, key, budget):
    before = _owned(path, uid)
    _require(before.st_size <= limit, 'retained_file_budget_exceeded')
    budget.preflight(before.st_size)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(_same(os.fstat(fd)) == _same(before), 'retained_file_identity_changed')
        plain = hashlib.sha256()
        keyed = hmac.new(key, b'file\0', hashlib.sha256)
        size = 0
        while size < before.st_size:
            chunk = budget.read(fd, min(1024 * 1024, before.st_size-size))
            _require(bool(chunk), 'retained_file_changed')
            size += len(chunk)
            _require(size <= before.st_size, 'retained_file_changed')
            plain.update(chunk); keyed.update(chunk)
        _require(size == before.st_size and _same(before) == _same(os.fstat(fd)) == _same(path.lstat()),
                 'retained_file_changed')
        return dict(bytes=size, sha256=plain.hexdigest(), content=keyed.hexdigest(),
                    mode=stat.S_IMODE(before.st_mode))
    finally:
        os.close(fd)


def _read_small_json(path, uid, budget=None):
    budget = budget or _ReadBudget(None,None)
    before = _owned(path, uid)
    _require(before.st_size <= 65536, 'receipt_metadata_budget_exceeded')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(_same(before) == _same(os.fstat(fd)), 'receipt_identity_changed')
        data = budget.read(fd, before.st_size)
        _require(len(data) == before.st_size and _same(before) == _same(os.fstat(fd)) == _same(path.lstat()),
                 'receipt_identity_changed')
        return _json(data.decode('utf-8'))
    finally:
        os.close(fd)


def _schema(db, reviewed_schema_bytes=None):
    if reviewed_schema_bytes is None:
        reviewed_schema_bytes = SCHEMA_PATH.read_bytes()
    _require(type(reviewed_schema_bytes) is bytes and len(reviewed_schema_bytes) <= 65536,
             'reviewed_schema_bytes_required')
    profile = _json(reviewed_schema_bytes.decode('utf-8'))
    _require(hashlib.sha256(_canonical(profile)).hexdigest() == SCHEMA_SHA256,
             'reviewed_schema_profile_changed')
    _require(profile['profile'] == PROFILE and profile['sourceCommit'] == SOURCE,
             'reviewed_schema_profile_required')
    _require(db.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()[0] == len(profile['objects']),
             'unreviewed_predecessor_schema')
    schema_bytes = db.execute("SELECT COALESCE(SUM(length(CAST(type AS BLOB))+length(CAST(name AS BLOB))+"
        "length(CAST(tbl_name AS BLOB))+COALESCE(length(CAST(sql AS BLOB)),0)),0) FROM sqlite_master").fetchone()[0]
    _require(schema_bytes <= max(65536,2*len(_canonical(profile['objects']))), 'schema_metadata_budget_exceeded')
    objects = [list(row) for row in db.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    normalize = lambda entries: [row[:3] + [re.sub(r'\s+', ' ', row[3]).strip() if row[3] else None]
                                  for row in entries]
    _require(normalize(objects) == normalize(profile['objects']), 'unreviewed_predecessor_schema')
    return objects


def _row_bytes(value, remaining, budget):
    """Exact ASCII JSON length before allocating a potentially expanded string."""
    size = 2
    def count(item, limit):
        if type(item) is not str:
            _require(item is None or type(item) in (int,float), 'unreviewed_binary_metadata')
            return len(_canonical(item))
        result = 2
        for start in range(0,len(item),4096):
            budget.check()
            for char in item[start:start+4096]:
                code = ord(char)
                result += (2 if char in '\\"' or char in '\b\f\n\r\t' else
                           6 if code < 32 or 126 < code <= 65535 else 12 if code > 65535 else 1)
            _require(result <= limit, 'predecessor_metadata_budget_exceeded')
        return result
    for index, (key, item) in enumerate(sorted(value.items())):
        budget.check()
        size += (1 if index else 0) + count(key,remaining-size) + 1
        size += count(item,remaining-size)
        _require(size <= remaining, 'predecessor_metadata_budget_exceeded')
    encoded = _canonical(value)
    _require(len(encoded) == size and size <= remaining, 'predecessor_metadata_budget_exceeded')
    return encoded


def _maintenance_history(nodes, schedules, authority_id, jobs):
    """Validate retained 693 owned receipts without refreshing/adopting a plan."""
    node_by_id = {node['id']: node for node in nodes}
    receipts_by_node = {identifier: [] for identifier in node_by_id}
    requests, plans, revisions = set(), set(), set()
    request_fields = {'schemaVersion','requestId','authorityId','expectedRevision',
                      'drainAt','notBefore','afterJobId','targetImage'}
    receipt_fields = request_fields | {'nodeId','requestSha256','revision','planId','state','receiptSha256'}
    def stamp(value):
        _require(type(value) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)',value),
                 'invalid_owned_schedule_timestamp')
        parsed = datetime.fromisoformat(value.replace('Z','+00:00'))
        return parsed, parsed.isoformat().replace('+00:00','Z')
    for row in schedules:
        request, receipt = _json(row['request_json']), _json(row['receipt_json'])
        _require(type(request) is dict and set(request) == request_fields and type(receipt) is dict
                 and set(receipt) == receipt_fields, 'unreviewed_owned_schedule_receipt')
        identifier = row['node_id']
        _require(identifier in node_by_id and row['authority_id'] == authority_id and
                 request['authorityId'] == authority_id and request['requestId'] == row['request_id'] and
                 type(request['schemaVersion']) is int and request['schemaVersion'] == 1 and
                 type(request['expectedRevision']) is int and 0 <= request['expectedRevision'] < 2**53-1,
                 'owned_schedule_authority_mismatch')
        _uuid(request['requestId']); _uuid(receipt['planId'])
        if request['afterJobId'] is not None:
            _uuid(request['afterJobId'])
            after = jobs.get(request['afterJobId'])
            _require(after is not None and after['node_id'] == identifier, 'owned_schedule_job_binding_invalid')
        drain, drain_text = stamp(request['drainAt']); earliest, earliest_text = stamp(request['notBefore'])
        _require(earliest >= drain and type(request['targetImage']) is str and
                 re.fullmatch(r'sha256:[0-9a-f]{64}',request['targetImage']), 'owned_schedule_request_invalid')
        digest = hashlib.sha256(_canonical(dict(nodeId=identifier,request=request))).hexdigest()
        expected = dict(schemaVersion=1, requestId=request['requestId'], authorityId=authority_id,
                        nodeId=identifier, requestSha256=digest, expectedRevision=request['expectedRevision'],
                        revision=request['expectedRevision']+1, planId=receipt['planId'], state=receipt['state'],
                        drainAt=drain_text, notBefore=earliest_text, afterJobId=request['afterJobId'],
                        targetImage=request['targetImage'])
        expected['receiptSha256'] = hashlib.sha256(_canonical(expected)).hexdigest()
        _require(row['request_json'] == _canonical(request).decode() and row['request_sha256'] == digest and
                 row['receipt_json'] == _canonical(expected).decode() and _canonical(receipt) == _canonical(expected) and
                 receipt['state'] in ('scheduled','draining') and
                 (request['afterJobId'] is None or receipt['state'] == 'draining'), 'owned_schedule_receipt_mismatch')
        revision_key = (identifier, receipt['revision'])
        _require(row['request_id'] not in requests and receipt['planId'] not in plans and
                 revision_key not in revisions, 'duplicate_owned_schedule_history')
        requests.add(row['request_id']); plans.add(receipt['planId']); revisions.add(revision_key)
        receipts_by_node[identifier].append(receipt)
    for node in nodes:
        _require(node['state'] == 'active' and type(node['revision']) is int and 0 <= node['revision'] < 2**53,
                 'foreign_node_maintenance_pending')
        history = sorted(receipts_by_node[node['id']], key=lambda receipt: receipt['revision'])
        if node['plan_id'] is None:
            _require(not history and all(node[field] is None for field in
                     ('drain_at','not_before','after_job_id','target_image','result','update_hash','finish_status')),
                     'unbound_node_maintenance_history')
            continue
        _uuid(node['plan_id'])
        _require(history and history[-1]['planId'] == node['plan_id'] and
                 node['revision'] > history[-1]['revision'], 'current_node_plan_not_closed')
        last = history[-1]
        _require(all(node[column] == last[field] for column, field in
                 (('drain_at','drainAt'),('not_before','notBefore'),('after_job_id','afterJobId'),('target_image','targetImage'))),
                 'current_node_plan_binding_mismatch')
        result = _json(node['result']) if node['result'] is not None else None
        if node['finish_status'] == 'succeeded':
            _require(type(node['update_hash']) is str and HEX.fullmatch(node['update_hash']) and
                     type(result) is dict and result.get('healthy') is True, 'node_update_completion_invalid')
        else:
            _require(node['finish_status'] is None and node['update_hash'] is None,
                     'foreign_node_maintenance_pending')
            if result is not None:
                _require(type(result) is dict and set(result) == {'operatorAction','note','workerStopped','healthy'} and
                         result['operatorAction'] == 'resume' and result['workerStopped'] is True and result['healthy'] is True and
                         type(result['note']) is str and 0 < len(result['note']) <= 256 and result['note'].strip(),
                         'node_resume_completion_invalid')


def _census(root, *, key, authority_id, node_ids, uid, max_artifact_bytes,
            metadata_only, reviewed_schema_bytes=None, max_duration_seconds=None,
            max_read_bytes=None, max_database_bytes=None,
            max_metadata_bytes=64 * 1024 * 1024, expected_counts=None,
            logical_root='/prototype-state'):
    """Return private keyed equality evidence; no controller/ownership assertion.

    ``expected_counts`` is an optional reviewed plan checkpoint, never a hardcoded
    terminal-job count. All fields, including callback flags, remain in row hashes.
    Budgets are explicit and independent: artifacts cannot borrow metadata quota.
    """
    budget = _ReadBudget(max_duration_seconds,max_read_bytes)
    budget.check()
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
    coarse_physical_bound = 8*max_metadata_bytes + 100000*1024 + 1024*1024
    _require(initial.st_size <= coarse_physical_bound, 'sqlite_coarse_physical_budget_exceeded')
    _require(max_database_bytes is None or type(max_database_bytes) is int and max_database_bytes > 0
             and initial.st_size <= max_database_bytes, 'diagnostic_database_size_exceeded')
    _require(not any(Path(str(database) + suffix).exists() or Path(str(database) + suffix).is_symlink()
                     for suffix in ('-journal', '-wal', '-shm')), 'sqlite_io_not_quiescent')
    descriptor = os.open(database,os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        _require(_same(initial) == _same(os.fstat(descriptor)), 'database_header_identity_changed')
        header = budget.read(descriptor,100)
    finally:
        os.close(descriptor)
    _require(len(header) == 100 and header[:16] == b'SQLite format 3\0' and header[18:20] == b'\1\1',
             'reviewed_rollback_database_required')
    db = sqlite3.connect(database.as_uri() + '?mode=ro&immutable=1', uri=True)
    db.row_factory = sqlite3.Row
    try:
        def progress():
            return int(budget.deadline is not None and time.monotonic() >= budget.deadline)
        db.set_progress_handler(progress,1000)
        _require(callable(getattr(db,'setlimit',None)), 'bounded_sqlite_reader_required')
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,65536)
        db.execute('BEGIN')
        schema = _schema(db,reviewed_schema_bytes)
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,max(65536,max_metadata_bytes))
        table_names = sorted(item[1] for item in schema if item[0] == 'table')
        sizes, raw_total, total_rows = {}, 0, 0
        for name in table_names:
            budget.check()
            count = db.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0]
            total_rows += count
            _require(total_rows <= 100000, 'predecessor_metadata_budget_exceeded')
            columns = [row['name'] for row in db.execute('PRAGMA table_info("'+name+'")')]
            length_sql = '+'.join('COALESCE(length(CAST("'+column+'" AS BLOB)),0)' for column in columns)
            sizes[name] = []
            for identity, length in db.execute('SELECT rowid,'+length_sql+' FROM "'+name+'"'):
                budget.check()
                raw_total += length
                _require(raw_total <= max_metadata_bytes, 'predecessor_metadata_budget_exceeded')
                sizes[name].append((identity,length))
        _require(db.execute('PRAGMA quick_check').fetchone()[0] == 'ok', 'sqlite_integrity_failed')
        _require(db.execute('PRAGMA foreign_key_check').fetchone() is None, 'detached_retained_reference')
        tables, rows, logical_bytes, row_count = {}, {}, 0, 0
        for name in table_names:
            values, encoded = [], []
            for identity, length in sizes[name]:
                budget.check()
                _require(length <= max_metadata_bytes-logical_bytes, 'predecessor_metadata_budget_exceeded')
                value = dict(db.execute('SELECT * FROM "'+name+'" WHERE rowid=?',(identity,)).fetchone())
                wire = _row_bytes(value,max_metadata_bytes-logical_bytes,budget)
                logical_bytes += len(wire); row_count += 1
                values.append(value); encoded.append(wire)
            encoded.sort()
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
        jobs = {r['id']: r for r in rows['jobs']}
        _maintenance_history(rows['nodes'], rows['owned_maintenance_schedules'], authority_id, jobs)
        _require(not rows['viewer_reservations'] and
                 not any(values for name, values in rows.items() if name.startswith('reference_')),
                 'unsettled_predecessor_execution')
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
                receipt = _read_small_json(root / receipt_path, uid, budget)
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
            budget.check()
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                _require(relative in expected_dirs, 'unknown_predecessor_path')
                directories[relative] = _same(_owned(path, uid, directory=True))
                continue
            _require(relative in expected_files, 'unknown_predecessor_path')
            limit = sqlite_bound if relative == 'jobs.sqlite3' else max_artifact_bytes
            expected = expected_files[relative]
            if metadata_only:
                observed = _owned(path,uid)
                _require(observed.st_size <= limit and (expected is None or observed.st_size == expected[0]),
                         'retained_artifact_size_mismatch')
                result = dict(bytes=observed.st_size,mode=stat.S_IMODE(observed.st_mode))
            else:
                result = _hash_file(path, uid, limit, key, budget)
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
        budget.check()
        return dict(schemaVersion=1, profile=PROBE_PROFILE if metadata_only else PROFILE,
                    validationScope='metadata-paths-receipts-only' if metadata_only else 'full-retained-custody',
                    retainedFilesValidated=not metadata_only, directFileBytesRead=budget.used,
                    sourceCommit=SOURCE, authorityId=authority_id,
                    schemaSha256=hashlib.sha256(_canonical(schema)).hexdigest(), tables=tables, files=files,
                    metadataBytes=logical_bytes, artifactBytes=total_artifact, sqliteBytes=initial.st_size,
                    sqlitePhysicalBound=sqlite_bound, sqliteCoarsePhysicalBound=coarse_physical_bound,
                    archiveReceipts=len(archive_checks),
                    rootMode=stat.S_IMODE(root_info.st_mode), writerExclusionProven=False,
                    signedViewerAcceptanceProven=False)
    except sqlite3.DataError:
        raise CensusRefused('predecessor_metadata_budget_exceeded') from None
    except sqlite3.OperationalError:
        budget.check()
        raise
    finally:
        db.close()


def census(root, **kwargs):
    """Full retained-content census; default behavior retains full hashing."""
    return _census(root,metadata_only=False,**kwargs)


def metadata_probe(root, *, reviewed_schema_bytes, max_duration_seconds=5,
                   max_read_bytes=1024*1024, max_database_bytes=8*1024*1024, **kwargs):
    """Diagnostic only; independently bounded metadata and direct receipt reads."""
    _require(type(reviewed_schema_bytes) is bytes, 'reviewed_schema_bytes_required')
    _require(max_duration_seconds is not None and max_read_bytes is not None and max_database_bytes is not None,
             'diagnostic_bounds_required')
    return _census(root,metadata_only=True,reviewed_schema_bytes=reviewed_schema_bytes,
                   max_duration_seconds=max_duration_seconds,max_read_bytes=max_read_bytes,
                   max_database_bytes=max_database_bytes,**kwargs)


def probe_wire(raw):
    """Exact closed JSON wire bytes; aggregate/keyed diagnostic output only."""
    try:
        _require(type(raw) is bytes, 'diagnostic_request_bytes_required')
        _require(len(raw) <= 131072, 'diagnostic_request_too_large')
        request = _json(raw.decode('utf-8'))
        fields = {'schemaBase64','keyHex','root','authorityId','nodeIds','uid','maxArtifactBytes',
                  'maxMetadataBytes','expectedCounts','logicalRoot','maxDurationSeconds','maxReadBytes','maxDatabaseBytes'}
        _require(type(request) is dict and set(request) == fields, 'diagnostic_request_fields_invalid')
        _require(type(request['keyHex']) is str and HEX.fullmatch(request['keyHex']), 'private_census_key_required')
        schema = base64.b64decode(request['schemaBase64'],validate=True)
        result = metadata_probe(request['root'], reviewed_schema_bytes=schema,key=bytes.fromhex(request['keyHex']),
            authority_id=request['authorityId'],node_ids=request['nodeIds'],uid=request['uid'],
            max_artifact_bytes=request['maxArtifactBytes'],max_metadata_bytes=request['maxMetadataBytes'],
            expected_counts=request['expectedCounts'],logical_root=request['logicalRoot'],
            max_duration_seconds=request['maxDurationSeconds'],max_read_bytes=request['maxReadBytes'],
            max_database_bytes=request['maxDatabaseBytes'])
        inventory = result.pop('files')
        result['fileCount'] = len(inventory)
        result['fileInventorySha256'] = hashlib.sha256(_canonical(inventory)).hexdigest()
        print(json.dumps(dict(ok=True,result=result),sort_keys=True,separators=(',',':')))
        return 0
    except Exception as error:
        reason = str(error) if type(error) is CensusRefused else 'diagnostic_probe_failed'
        if not re.fullmatch('[a-z][a-z0-9_]{0,100}',reason): reason = 'diagnostic_probe_failed'
        print(json.dumps(dict(ok=False,error=reason),sort_keys=True,separators=(',',':')))
        return 2


def probe_stdin():
    return probe_wire(sys.stdin.buffer.read(131073))


def serializable_probe_source(request=None):
    """Reviewed module source; accepts reviewed schema bytes entirely in memory.

    With ``request`` supplied, run the returned source via ``python -`` stdin.
    Otherwise run via ``python -c SOURCE`` and supply the closed JSON on stdin.
    No target file/package is needed. In-memory schemaBase64 is always pinned.
    Keep the generated source private: it embeds the ephemeral census key.
    """
    tail = 'raise SystemExit(probe_stdin())' if request is None else 'raise SystemExit(probe_wire('+repr(_canonical(request))+'))'
    return Path(__file__).read_text() + '\n' + tail + '\n'
