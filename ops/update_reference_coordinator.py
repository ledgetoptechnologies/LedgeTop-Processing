"""Explicit reference-to-reference sole-coordinator update; never run on import.

This is a new checkpoint ABI. Historical native-to-reference install artifacts
are neither loaded nor modified. Uncertain mutation responses permit inspection
only, never automatic mutation replay or database restoration.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import inspect
import json
import os
from pathlib import Path
import re
import select
import socket
import stat
import sys
import time
import uuid
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops.guarded_script import explicit_guard_source

class _HistoricalTransport:
    """Defer the Linux-only historical helper until an operation uses it."""
    def __getattr__(self,name):
        if os.name!='posix':raise Stopped('linux_reference_update_required')
        from ops import install_reference_coordinator
        return getattr(install_reference_coordinator,name)


legacy=_HistoricalTransport()
NAME='ledgetop-processing-prototype-c3fcb3a'
VOLUME=NAME+'-state'
CLUSTER_PROJECT='lt-migration-1120ae14a8988e84513a4a8a46b78741'
CLUSTER_NAME=CLUSTER_PROJECT+'-candidate'
AUTHORITY='7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
# The installed predecessor: the 693cb7e coordinator that the e2ccf489 update
# created, from a read-only inspection of the live container on 2026-11-03.
OLD_ID = '525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977'
OLD_IMAGE = 'sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004'
OLD_SOURCE = '693cb7eca32295da959a4f42399f317577ef8a8a'
# Historical fixture count, not a live authorization boundary. Each operation
# pins the current idle history and requires that same count through its census.
REVIEWED_JOB_COUNT = 20
# Private space for the restart preflight that reads through a hot rollback
# journal. It counts against --memory 256m; jobs.sqlite3 is about 0.25 MiB.
TMPFS = {'/tmp': 'rw,nosuid,nodev,noexec,size=64m,mode=1777'}
# The retained Viewer tasks add about 1.2 GB that the backup copies once and
# hashes twice; 120 seconds covered only the empty historical volume.
BACKUP_TIMEOUT = 900
# Every volume census reads those same bytes, online at --cpus 0.5, and a timeout
# after stop is needs_inspection. It reuses the reviewed backup budget unchanged.
CENSUS_TIMEOUT = BACKUP_TIMEOUT
LABEL = 'org.ledgetop.reference.update'
MODE = 'reference-to-reference-v1'
# The 693cb7e predecessor pins and the /tmp tmpfs are new and not yet reviewed
# against the live host. Explicit preparation, immutable argument binding and
# every live fence remain; activation and rollback stay refused until review.
ACTIVATION_REVIEWED = False
# Manifest digest of the opaque pilot-journals tree, pinned by a human reviewer
# from the live read-only census. None holds preparation and activation, as does
# any other value; an absent tree is the digest of the empty manifest, not a
# special case. A hash cannot show a pilot finished: see the pre-live audit in
# docs/reference-coordinator-update.md.
REVIEWED_PILOT_JOURNALS_SHA256 = None
class Stopped(ValueError):pass


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


def cluster_http_json(route,token,deadline,limit=65536):
    """Fixed private NodeODM reads with one absolute header/body deadline."""
    if (route not in ('/info','/task/list') or type(token) is not str or not 1<=len(token)<=4096
            or any(not 0x21<=ord(char)<=0x7e for char in token)):
        raise Stopped('fixed_private_cluster_route_required')
    connection=None
    try:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise ValueError()
        connection=socket.create_connection(('127.0.0.1',13001),timeout=remaining)
        connection.setblocking(False)
        target=route+'?'+urlencode({'token':token})
        request=('GET '+target+' HTTP/1.1\r\nHost: 127.0.0.1:13001\r\nAccept: application/json\r\nConnection: close\r\n\r\n').encode('ascii')
        def ready(read):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise ValueError()
            r,w,_=select.select([connection] if read else [],[] if read else [connection],[],remaining)
            if not r and not w or time.monotonic()>=deadline:raise ValueError()
        while request:
            ready(False)
            try:sent=connection.send(request)
            except BlockingIOError:continue
            if sent<=0:raise ValueError()
            request=request[sent:]
        data=bytearray()
        while b'\r\n\r\n' not in data:
            ready(True)
            try:chunk=connection.recv(1024)
            except BlockingIOError:continue
            if not chunk:raise ValueError()
            data.extend(chunk)
            if len(data)>8192+limit or b'\r\n\r\n' not in data and len(data)>8192:raise ValueError()
        header,body=bytes(data).split(b'\r\n\r\n',1)
        if len(header)>8192:raise ValueError()
        lines=header.decode('ascii').split('\r\n')
        if not re.fullmatch(r'HTTP/1\.[01] 200(?: [^\r\n]*)?',lines[0]):raise ValueError()
        headers={}
        for line in lines[1:]:
            key,value=line.split(':',1);key=key.lower()
            if key in headers or not re.fullmatch('[a-z0-9-]+',key):raise ValueError()
            headers[key]=value.strip()
        if (headers.get('content-type','').split(';',1)[0].lower()!='application/json'
                or 'transfer-encoding' in headers or headers.get('content-encoding','identity')!='identity'):raise ValueError()
        length=None
        if 'content-length' in headers:
            if not re.fullmatch('[0-9]{1,8}',headers['content-length']):raise ValueError()
            length=int(headers['content-length'])
            if length>limit:raise ValueError()
        while length is None or len(body)<length:
            if len(body)>limit:raise ValueError()
            ready(True)
            try:chunk=connection.recv(min(4096,limit-len(body)+1))
            except BlockingIOError:continue
            if not chunk:break
            body+=chunk
        if len(body)>limit or length is not None and len(body)!=length:raise ValueError()
        def unique(pairs):
            value={}
            for key,item in pairs:
                if key in value:raise ValueError()
                value[key]=item
            return value
        result=json.loads(body,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
        if time.monotonic()>=deadline:raise ValueError()
        return result
    except Exception:raise Stopped('cluster_readonly_api_unavailable') from None
    finally:
        if connection is not None:connection.close()


def volume_census(root, *, uid=10001, offline=False):
    """Pure read-only census; also serialized into the network-none helper.

    Job/receipt contents never leave this function. Only the two system node
    rows are retained privately to prove exact operation-owned CAS transitions.
    """
    import hashlib, json, os, re, sqlite3, stat
    from pathlib import Path
    root = Path(root)
    root_info = root.lstat()
    if not (stat.S_ISDIR(root_info.st_mode) and root_info.st_uid == uid and (root_info.st_gid == uid)):
        raise AssertionError()

    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

    def sha(value):
        return hashlib.sha256(encode(value)).hexdigest()
    allowed = {'jobs', 'sqlite_sequence', 'coordinator_identity', 'coordinator_authority_lease', 'nodes', 'viewer_tasks', 'viewer_files', 'viewer_incarnations', 'reference_jobs', 'reference_acceptances', 'reference_outbox', 'reference_claims', 'reference_worker_receipts', 'reference_transfer_grants', 'reference_worker_failures', 'reference_terminal_acceptances', 'owned_maintenance_schedules', 'reference_viewer_receipts', 'reference_viewer_transfers', 'viewer_reservations', 'viewer_option_evidence', 'viewer_full_option_evidence', 'reference_released_claims', 'viewer_guarded_dispatch_checkpoint', 'viewer_guarded_dispatch_history'}
    # Literal schemas are embedded so the isolated serialized helper needs no candidate imports.
    prepared_ddl = ('CREATE TABLE reference_viewer_activation (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), activation_id TEXT UNIQUE NOT NULL,\n principal_id TEXT NOT NULL, environment TEXT NOT NULL, viewer_attempt_id TEXT NOT NULL,\n binding_json TEXT NOT NULL, binding_sha256 TEXT NOT NULL,\n UNIQUE(principal_id, environment, viewer_attempt_id))', 'CREATE TABLE reference_viewer_submission_owners (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,\n environment TEXT NOT NULL, request_digest TEXT NOT NULL, protocol TEXT NOT NULL)', 'CREATE TABLE reference_viewer_activation_schema (\n singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))', 'CREATE TABLE reference_viewer_envelopes_v2 (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,\n environment TEXT NOT NULL, activation_id TEXT UNIQUE NOT NULL, viewer_task_id TEXT NOT NULL,\n viewer_attempt_id TEXT NOT NULL, envelope_json TEXT NOT NULL, envelope_sha256 TEXT NOT NULL,\n UNIQUE(principal_id, environment, viewer_attempt_id))', 'CREATE TABLE reference_viewer_v3_copy_hold (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), envelope_sha256 TEXT NOT NULL,\n payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL)', 'CREATE TABLE reference_viewer_v3_policy (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), policy_json TEXT NOT NULL, policy_sha256 TEXT NOT NULL)')
    prepared_names = {'reference_viewer_activation','reference_viewer_submission_owners','reference_viewer_activation_schema','reference_viewer_envelopes_v2','reference_viewer_v3_copy_hold','reference_viewer_v3_policy'}
    allowed.update(prepared_names)
    files = []
    journals = []
    journal_files = 0
    journal_bytes = 0
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        info = path.lstat()
        if not (info.st_uid == uid and info.st_gid == uid and (not stat.S_ISLNK(info.st_mode))):
            raise AssertionError()
        # The reviewed baseline contains system metadata, an empty spool, and
        # retained terminal Viewer task directories. Those are client inputs
        # and results: every byte is hashed so update and rollback preserve them.
        retained = relative.startswith('viewer-tasks/')
        # Operator pilot records with no writer in this source: pilot-journals,
        # depth-1 directories and depth-2 files only. Directories are exactly
        # 0700 because the backup must write into them; files have no group,
        # other or special bits, which its umask could not reproduce. Their
        # bytes are opaque: hashed like the rest of the tree, never parsed.
        parts = relative.split('/')
        journal = parts[0] == 'pilot-journals'
        if journal:
            directory = stat.S_ISDIR(info.st_mode)
            mode = stat.S_IMODE(info.st_mode)
            if not (len(parts) <= 3 and directory == (len(parts) < 3) and (mode == 0o700 if directory else not mode & 0o7077)):
                raise AssertionError('pilot journal outside reviewed tree')
        if not (relative in ('jobs.sqlite3', 'coordinator.lock', 'viewer-tasks', 'viewer-spool') or retained or journal):
            raise AssertionError()
        if stat.S_ISDIR(info.st_mode):
            files.append(dict(path=relative, kind='directory', mode=stat.S_IMODE(info.st_mode)))
            if journal:
                journals.append([relative, stat.S_IMODE(info.st_mode), None, None])
        else:
            if not (stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and (info.st_size <= (21474836480 if retained else 65536 if journal else 67108864))):
                raise AssertionError()
            if journal:
                journal_files += 1
                journal_bytes += info.st_size
                if journal_files > 64 or journal_bytes > 1048576:
                    raise AssertionError('pilot journals exceed reviewed bounds')
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                before = os.fstat(fd)
                if not (before.st_dev, before.st_ino) == (info.st_dev, info.st_ino):
                    raise AssertionError()
                h = hashlib.sha256()
                size = 0
                while True:
                    data = os.read(fd, 1048576)
                    if not data:
                        break
                    size += len(data)
                    if not size <= info.st_size:
                        raise AssertionError()
                    h.update(data)
                after = path.lstat()
                if not (size == info.st_size and (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)):
                    raise AssertionError()
            finally:
                os.close(fd)
            files.append(dict(path=relative, kind='file', mode=stat.S_IMODE(info.st_mode), bytes=size, sha256=h.hexdigest()))
            if journal:
                journals.append([relative, stat.S_IMODE(info.st_mode), size, h.hexdigest()])
    if not any((item['path'] == 'jobs.sqlite3' for item in files)):
        raise AssertionError()
    uri = 'file:' + str(root / 'jobs.sqlite3') + '?mode=ro' + ('&immutable=1' if offline else '')
    db = sqlite3.connect(uri, uri=True, timeout=3)
    try:
        db.execute('BEGIN')
        if not db.execute('PRAGMA quick_check').fetchone()[0] == 'ok':
            raise AssertionError()
        objects = [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        names = {row[1] for row in objects if row[0] == 'table'}
        if not (not names - allowed and {'jobs', 'coordinator_identity', 'nodes', 'reference_jobs', 'viewer_tasks'} <= names):
            raise AssertionError()
        prepared = names & prepared_names
        if prepared and prepared != prepared_names:
            raise AssertionError('partial prepared schema')
        if prepared:
            expected_db = sqlite3.connect(':memory:')
            try:
                for statement in prepared_ddl: expected_db.execute(statement)
                expected_objects = [list(row) for row in expected_db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
            finally: expected_db.close()
            normalize = lambda rows: [[*r[:3], re.sub(r'\s+', ' ', r[3]).strip() if r[3] is not None else None] for r in rows]
            if normalize([r for r in objects if r[2] in prepared_names]) != normalize(expected_objects):
                raise AssertionError('unreviewed prepared schema')
            for name in sorted(prepared_names):
                if name == 'reference_viewer_activation_schema':
                    if [tuple(row) for row in db.execute('SELECT * FROM reference_viewer_activation_schema LIMIT 2')] != [(1,1)]:
                        raise AssertionError('invalid prepared marker')
                elif db.execute('SELECT 1 FROM "'+name+'" LIMIT 1').fetchone():
                    raise AssertionError('nonempty prepared schema')
        viewer_extra = {'viewer_reservations', 'viewer_option_evidence'}
        if names & viewer_extra and not viewer_extra <= names:
            raise AssertionError('partial Viewer schema')
        if 'coordinator_authority_lease' in names:
            entries = [row for row in objects if row[2] == 'coordinator_authority_lease']
            expected_lease = 'CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))'
            if (len(entries) != 1 or entries[0][:3] != ['table', 'coordinator_authority_lease', 'coordinator_authority_lease']
                    or re.sub(r'\s+', ' ', entries[0][3]).strip() != expected_lease
                    or list(db.execute('SELECT singleton,required FROM coordinator_authority_lease')) != [(1, 1)]):
                raise AssertionError('authority lease marker invalid')
        if 'owned_maintenance_schedules' in names:
            entries = [row for row in objects if row[2] == 'owned_maintenance_schedules']
            expected_owned = ('CREATE TABLE owned_maintenance_schedules ( request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL, '
                              'request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)')
            table = [row for row in entries if row[0] == 'table']
            indexes = [row for row in entries if row[0] == 'index']
            if (len(table) != 1 or re.sub(r'\s+', ' ', table[0][3]).strip() != expected_owned
                    or indexes != [['index', 'sqlite_autoindex_owned_maintenance_schedules_1', 'owned_maintenance_schedules', None]]
                    or len(entries) != 2):
                raise AssertionError('owned schedule schema invalid')
            authority_row = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
            if not authority_row:
                raise AssertionError('owned schedule authority missing')
            node_ids = {row[0] for row in db.execute('SELECT id FROM nodes')}
            for request_id, node_id, authority_id, request_hash, request_json, receipt_json in db.execute(
                    'SELECT request_id,node_id,authority_id,request_sha256,request_json,receipt_json FROM owned_maintenance_schedules'):
                try:
                    if (type(request_json) is not str or type(receipt_json) is not str
                            or len(request_json.encode()) > 65536 or len(receipt_json.encode()) > 65536):
                        raise ValueError()
                    request, receipt = json.loads(request_json), json.loads(receipt_json)
                    if type(request) is not dict or type(receipt) is not dict:
                        raise ValueError()
                    digest = sha({'nodeId': node_id, 'request': request})
                    receipt_digest = sha({k: v for k, v in receipt.items() if k != 'receiptSha256'})
                    if (node_id not in node_ids or authority_id != authority_row[0]
                            or encode(request).decode() != request_json or encode(receipt).decode() != receipt_json
                            or digest != request_hash or receipt.get('receiptSha256') != receipt_digest
                            or (request.get('requestId'), request.get('authorityId'), receipt.get('requestId'),
                                receipt.get('authorityId'), receipt.get('nodeId'), receipt.get('requestSha256'))
                            != (request_id, authority_id, request_id, authority_id, node_id, request_hash)):
                        raise ValueError()
                except (ValueError, TypeError, KeyError, UnicodeError, OverflowError):
                    raise AssertionError('owned schedule evidence invalid') from None
        expected = {
            'viewer_reservations': "CREATE TABLE viewer_reservations ( uuid TEXT PRIMARY KEY, environment TEXT NOT NULL, capability_hash TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','cancelled')))",
            'viewer_option_evidence': "CREATE TABLE viewer_option_evidence ( uuid TEXT NOT NULL, generation INTEGER NOT NULL, definitions TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(uuid,generation))",
            'viewer_full_option_evidence': "CREATE TABLE viewer_full_option_evidence ( uuid TEXT NOT NULL, generation INTEGER NOT NULL, definitions TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(uuid,generation))",
            'reference_released_claims': "CREATE TABLE reference_released_claims ( request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, node_id TEXT NOT NULL, worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, token_hash TEXT NOT NULL)",
            'viewer_guarded_dispatch_checkpoint': "CREATE TABLE viewer_guarded_dispatch_checkpoint ( uuid TEXT PRIMARY KEY, generation INTEGER NOT NULL, job_id TEXT NOT NULL, node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL, sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')))",
            'viewer_guarded_dispatch_history': "CREATE TABLE viewer_guarded_dispatch_history ( uuid TEXT NOT NULL, generation INTEGER NOT NULL, job_id TEXT NOT NULL, node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL, sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')), PRIMARY KEY(uuid,sequence))",
        }
        for name in sorted(names & set(expected)):
            entries = [row for row in objects if row[2] == name]
            table = [row for row in entries if row[0] == 'table']
            indexes = [row for row in entries if row[0] == 'index']
            if (len(table) != 1 or not isinstance(table[0][3], str)
                    or re.sub(r'\s+', ' ', table[0][3]).strip() != expected[name]
                    or indexes != [['index', 'sqlite_autoindex_' + name + '_1', name, None]]
                    or len(entries) != 2):
                raise AssertionError('unreviewed Viewer schema')
        if not not any((row[0] in ('trigger', 'view') for row in objects)):
            raise AssertionError()
        if not all((row[0] != 'index' or row[1].startswith('sqlite_autoindex_') or row[1] == 'viewer_engine_uuid' for row in objects)):
            raise AssertionError()
        tables = {}
        for name in sorted(names):
            if not name in allowed:
                raise AssertionError()
            columns = [list(row) for row in db.execute('PRAGMA table_info("' + name + '")')]
            rows = []
            for row in db.execute('SELECT * FROM "' + name + '"'):
                rows.append([{'blob': value.hex()} if isinstance(value, bytes) else value for value in row])
            rows.sort(key=encode)
            tables[name] = dict(count=len(rows), sha256=sha(rows), columns=columns)
        # Pinned additive columns and defaults (MIGRATION_COLUMNS; tested equal).
        # Rows never leave this helper: only a hash of each row projected onto
        # the other columns, and a NULL-aware count of non-default values per
        # pinned column. Without pinned columns the projection is the full row.
        pinned = {'viewer_tasks': {'full_option_evidence_required': 0, 'node_guarded': 0}, 'reference_jobs': {'viewer_receipt_required': None}}
        for name in sorted(names & set(pinned)):
            present = [column[1] for column in tables[name]['columns']]
            kept = [column for column in present if column not in pinned[name]]
            if not kept or any('"' in column for column in present):
                raise AssertionError('unreviewed pinned table columns')
            rows = []
            for row in db.execute('SELECT ' + ','.join('"' + column + '"' for column in kept) + ' FROM "' + name + '"'):
                rows.append([{'blob': value.hex()} if isinstance(value, bytes) else value for value in row])
            rows.sort(key=encode)
            tables[name]['projectedSha256'] = sha(rows)
            tables[name]['nonDefault'] = {column: db.execute('SELECT COUNT(*) FROM "' + name + '" WHERE "' + column + '" IS NOT ?', (default,)).fetchone()[0]
                for column, default in sorted(pinned[name].items()) if column in present}
        authority =db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]
        active = db.execute("SELECT COUNT(*) FROM jobs WHERE status NOT IN ('succeeded','failed')").fetchone()[0]
        if not active == 0:
            raise AssertionError()
        # Terminal Viewer tasks are retained, never removed for an update. Any
        # nonterminal task, reservation or orphaned task row holds instead.
        retained_viewer = {'viewer_tasks', 'viewer_files', 'viewer_incarnations', 'viewer_option_evidence'}
        for name in names:
            if (name.startswith(('reference_', 'viewer_')) and tables[name]['count'] != 0
                    and name not in retained_viewer and name != 'reference_viewer_activation_schema'):
                raise AssertionError('nonempty guarded table')
        if 'viewer_tasks' in names:
            if db.execute("SELECT COUNT(*) FROM viewer_tasks WHERE NOT (phase='complete' AND stage IN ('succeeded','failed','cancelled'))").fetchone()[0]:
                raise AssertionError('nonterminal Viewer task')
            for name in sorted((retained_viewer & names) - {'viewer_tasks'}):
                if db.execute('SELECT COUNT(*) FROM "' + name + '" WHERE uuid NOT IN (SELECT uuid FROM viewer_tasks)').fetchone()[0]:
                    raise AssertionError('orphaned Viewer task row')
        node_columns = [row[1] for row in db.execute('PRAGMA table_info(nodes)')]
        nodes = {row[0]: dict(zip(node_columns, row)) for row in db.execute('SELECT * FROM nodes ORDER BY id')}
        journals.sort(key=lambda row: row[0])
        pilot = dict(files=journal_files, bytes=journal_bytes, sha256=sha(journals))
        return dict(authorityId=authority, schema=objects, tables=tables, files=files, pilotJournals=pilot, nodeRows=nodes, rootMode=stat.S_IMODE(root_info.st_mode), sqliteQuickCheck='ok')
    finally:
        db.close()


def owned_schema():
    import sqlite3
    db=sqlite3.connect(':memory:')
    try:
        db.execute('''CREATE TABLE owned_maintenance_schedules (
            request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)''')
        return [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:db.close()

def viewer_extra_schema():
    """Exact empty additive tables accepted for the reviewed Viewer upgrade."""
    import sqlite3
    db=sqlite3.connect(':memory:')
    try:
        db.execute("CREATE TABLE viewer_reservations ( uuid TEXT PRIMARY KEY, environment TEXT NOT NULL, capability_hash TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','cancelled')))")
        db.execute("CREATE TABLE viewer_option_evidence ( uuid TEXT NOT NULL, generation INTEGER NOT NULL, definitions TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(uuid,generation))")
        return [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:db.close()


# Reviewed additive start-up migration of the candidate over the live predecessor
# volume (read-only live schema check: all absent there). Exact DDL text must
# remain verbatim in processing/ (tested); nothing else is normalized away.
MIGRATION_TABLES=(
    '''CREATE TABLE reference_viewer_receipts (
                  receipt_id TEXT PRIMARY KEY, storage_receipt_id TEXT NOT NULL UNIQUE,
                  acceptance_receipt_id TEXT NOT NULL UNIQUE REFERENCES reference_acceptances(receipt_id),
                  job_id TEXT NOT NULL REFERENCES reference_jobs(job_id), digest TEXT NOT NULL,
                  receipt_json TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)''',
    '''CREATE TABLE reference_viewer_transfers (
                  job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id),
                  receipt_id TEXT UNIQUE NOT NULL, acceptance_json TEXT NOT NULL,
                  viewer_manifest_json TEXT,
                  attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                  lease_token TEXT, lease_until REAL, last_error TEXT,
                  completed_at TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)''',
    'ALTER TABLE reference_viewer_transfers ADD COLUMN verified_full_coverage INTEGER',
    '''CREATE TABLE reference_released_claims (
                  request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, node_id TEXT NOT NULL,
                  worker_id TEXT NOT NULL, instance_id TEXT NOT NULL, token_hash TEXT NOT NULL)''',
    '''CREATE TABLE viewer_full_option_evidence (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL,
                definitions TEXT NOT NULL, sha256 TEXT NOT NULL,
                PRIMARY KEY(uuid,generation))''',
    '''CREATE TABLE viewer_guarded_dispatch_checkpoint (
                uuid TEXT PRIMARY KEY, generation INTEGER NOT NULL, job_id TEXT NOT NULL,
                node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL,
                sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')))''',
    '''CREATE TABLE viewer_guarded_dispatch_history (
                uuid TEXT NOT NULL, generation INTEGER NOT NULL, job_id TEXT NOT NULL,
                node_id TEXT NOT NULL, engine_uuid TEXT NOT NULL,
                sequence INTEGER NOT NULL, operation TEXT NOT NULL, evidence TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','acknowledged')),
                PRIMARY KEY(uuid,sequence))''')
# (table, column, exact statement); defaults are 0, 0 and NULL respectively.
MIGRATION_COLUMNS=(
    ('viewer_tasks','full_option_evidence_required','ALTER TABLE viewer_tasks ADD COLUMN full_option_evidence_required INTEGER NOT NULL DEFAULT 0 CHECK(full_option_evidence_required IN (0,1))'),
    ('viewer_tasks','node_guarded','ALTER TABLE viewer_tasks ADD COLUMN node_guarded INTEGER NOT NULL DEFAULT 0 CHECK(node_guarded IN (0,1))'),
    ('reference_jobs','viewer_receipt_required','ALTER TABLE reference_jobs ADD COLUMN viewer_receipt_required INTEGER'))


def migration_schema():
    """Exact empty tables the reviewed candidate creates on an older volume."""
    import sqlite3
    db=sqlite3.connect(':memory:')
    try:
        for statement in MIGRATION_TABLES:db.execute(statement)
        return [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:db.close()


def migrated_table(row,statements):
    """Replay pinned ADD COLUMNs over the baseline table DDL, in memory only."""
    import sqlite3
    if (len(row)!=4 or row[0]!='table' or row[1]!=row[2] or type(row[3]) is not str
            or not row[3].startswith('CREATE TABLE '+row[1]+' (')):raise Stopped('unreviewed_migration_base')
    db=sqlite3.connect(':memory:')
    try:
        db.execute(row[3])
        for statement in statements:db.execute(statement)
        return ([list(item) for item in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type='table' AND name=?",(row[1],))],
                [list(item) for item in db.execute('PRAGMA table_info("'+row[1]+'")')])
    finally:db.close()


def normalized_schema(rows):
    return [[*row[:3],re.sub(r'\s+',' ',row[3]).strip() if row[3] is not None else None] for row in rows]


def remove_migration(a,b):
    """Strip only pinned additions absent from baseline; never reads rows.

    Pinned tables stay empty. Retained terminal viewer_tasks rows are kept, so
    a pinned column is accepted only when the observed rows projected onto the
    baseline columns hash exactly to the baseline rows, the counts match, and
    no row holds a value other than the pinned default (NULL-aware). A table
    with only some of its pinned columns is not a reviewed base.
    """
    pinned=normalized_schema(migration_schema())
    for name in sorted({row[2] for row in pinned}):
        if name in b['tables'] or name not in a['tables']:continue
        if (normalized_schema([row for row in a['schema'] if row[2]==name])!=[row for row in pinned if row[2]==name]
                or a['tables'][name]['count']!=0):raise Stopped('unreviewed_migration_schema_or_rows')
        a['schema']=[row for row in a['schema'] if row[2]!=name];a['tables'].pop(name)
    for table in sorted({item[0] for item in MIGRATION_COLUMNS}):
        if table not in a['tables'] or table not in b['tables']:continue
        present={column[1] for column in b['tables'][table]['columns']}
        added=[column for name,column,_statement in MIGRATION_COLUMNS if name==table and column not in present]
        statements=[statement for name,column,statement in MIGRATION_COLUMNS if name==table and column in added]
        if not statements:continue
        if len(added)!=sum(name==table for name,_column,_statement in MIGRATION_COLUMNS):raise Stopped('unreviewed_migration_base')
        original=[row for row in b['schema'] if row[0]=='table' and row[1]==table]
        observed=[row for row in a['schema'] if row[0]=='table' and row[1]==table]
        if len(original)!=1:raise Stopped('unreviewed_migration_base')
        schema,columns=migrated_table(original[0],statements)
        before,after=b['tables'][table],a['tables'][table]
        if (observed!=schema or after['columns']!=columns or after['count']!=before['count']
                or before.get('projectedSha256')!=before['sha256'] or before.get('nonDefault')!={}
                or after.get('projectedSha256')!=before['sha256']
                or after.get('nonDefault')!={column:0 for column in added}):raise Stopped('unreviewed_migration_columns_or_rows')
        a['schema']=[original[0] if row in observed else row for row in a['schema']]
        a['tables'][table]=json.loads(canonical(before))


def expected_empty_prepared_census():
    """Exact review-only prepared schema; no authorization to run old code."""
    import sqlite3
    db=sqlite3.connect(':memory:')
    try:
        statements=('CREATE TABLE reference_viewer_activation (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), activation_id TEXT UNIQUE NOT NULL,\n principal_id TEXT NOT NULL, environment TEXT NOT NULL, viewer_attempt_id TEXT NOT NULL,\n binding_json TEXT NOT NULL, binding_sha256 TEXT NOT NULL,\n UNIQUE(principal_id, environment, viewer_attempt_id))', 'CREATE TABLE reference_viewer_submission_owners (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,\n environment TEXT NOT NULL, request_digest TEXT NOT NULL, protocol TEXT NOT NULL)', 'CREATE TABLE reference_viewer_activation_schema (\n singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL CHECK(version=1))', 'CREATE TABLE reference_viewer_envelopes_v2 (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), principal_id TEXT NOT NULL,\n environment TEXT NOT NULL, activation_id TEXT UNIQUE NOT NULL, viewer_task_id TEXT NOT NULL,\n viewer_attempt_id TEXT NOT NULL, envelope_json TEXT NOT NULL, envelope_sha256 TEXT NOT NULL,\n UNIQUE(principal_id, environment, viewer_attempt_id))', 'CREATE TABLE reference_viewer_v3_copy_hold (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), envelope_sha256 TEXT NOT NULL,\n payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL)', 'CREATE TABLE reference_viewer_v3_policy (\n job_id TEXT PRIMARY KEY REFERENCES reference_jobs(job_id), policy_json TEXT NOT NULL, policy_sha256 TEXT NOT NULL)')
        for statement in statements:db.execute(statement)
        db.execute('INSERT INTO reference_viewer_activation_schema VALUES(1,1)')
        schema=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        tables={}
        for name in (r[1] for r in schema if r[0]=='table'):
            rows=[list(row) for row in db.execute('SELECT * FROM "'+name+'"')]
            tables[name]=dict(count=len(rows),sha256=digest(rows),columns=[list(row) for row in db.execute('PRAGMA table_info("'+name+'")')])
        return normalized_schema(schema),tables
    finally:db.close()


def compatible(observed, baseline, *, ignore_nodes=False, allow_owned=False, allow_viewer_additions=False, allow_migration=False, allow_prepared_forward=False):
    """Compare complete censuses; allow exact empty additions only explicitly.

    Viewer additions are permitted for candidate verification, not the offline
    rollback gate: an older binary's behavior on that schema is unqualified.
    The pinned migration is also accepted for rollback: the predecessor names
    every viewer_tasks/reference_jobs INSERT column and ignores unknown tables.
    That is a census rule only; the activation and rollback gates stay closed
    until the predecessor image itself is qualified on migrated state.
    """
    a,b=json.loads(canonical(observed)),json.loads(canonical(baseline))
    for value in (a,b):
        if value['authorityId']!=AUTHORITY or value['sqliteQuickCheck']!='ok':raise Stopped('census_authority_or_integrity_changed')
    prepared={'reference_viewer_activation','reference_viewer_submission_owners','reference_viewer_activation_schema','reference_viewer_envelopes_v2','reference_viewer_v3_copy_hold','reference_viewer_v3_policy'}
    if allow_prepared_forward:
        if prepared & set(b['tables']) or not prepared <= set(a['tables']):
            raise Stopped('prepared_forward_only')
        expected_schema,expected_tables=expected_empty_prepared_census()
        if (normalized_schema([r for r in a['schema'] if r[2] in prepared]) != expected_schema
                or {name:a['tables'][name] for name in prepared} != expected_tables):
            raise Stopped('unreviewed_prepared_schema_or_rows')
        a['schema']=[row for row in a['schema'] if row[2] not in prepared]
        for name in prepared:a['tables'].pop(name)
    if allow_migration:remove_migration(a,b)
    extra={'viewer_reservations','viewer_option_evidence'}
    added=extra & set(a['tables']);prior=extra & set(b['tables'])
    if allow_viewer_additions and not prior and added:
        additions=[row for row in a['schema'] if row[2] in extra]
        if (added!=extra or normalized_schema(additions)!=normalized_schema(viewer_extra_schema())
                or any(a['tables'][name]['count']!=0 for name in extra)):
            raise Stopped('unreviewed_viewer_schema_or_rows')
        a['schema']=[row for row in a['schema'] if row[2] not in extra]
        for name in extra:a['tables'].pop(name)
    if allow_owned and 'owned_maintenance_schedules' not in b['tables'] and 'owned_maintenance_schedules' in a['tables']:
        additions=[row for row in a['schema'] if row[2]=='owned_maintenance_schedules']
        if (normalized_schema(additions)!=normalized_schema(owned_schema())
                or a['tables']['owned_maintenance_schedules']['count']!=0):raise Stopped('unreviewed_owned_schema_or_receipts')
        a['schema']=[row for row in a['schema'] if row[2]!='owned_maintenance_schedules']
        a['tables'].pop('owned_maintenance_schedules')
    for value in (a,b):
        # SQLite page layout changes during our own maintenance; all logical
        # bytes remain bound separately, while the raw file hash is retained.
        value['files']=[{key:item for key,item in entry.items() if entry['path']!='jobs.sqlite3' or key not in ('sha256','bytes')}
            for entry in value['files']]
        if ignore_nodes:value['tables'].pop('nodes',None);value.pop('nodeRows',None)
    if a!=b:raise Stopped('full_census_changed')


def reviewed_pilot_journals(census):
    """Only a human pin accepts the tree; compatible() never relaxes it."""
    if REVIEWED_PILOT_JOURNALS_SHA256 is None:raise Stopped('reviewed_pilot_journals_required')
    if census.get('pilotJournals',{}).get('sha256')!=REVIEWED_PILOT_JOURNALS_SHA256:raise Stopped('reviewed_pilot_journals_changed')


TLS_CENSUS = """import hashlib,json,os,stat
from pathlib import Path
root=Path('/run/processing-reference');result=[]
rs=root.lstat();assert stat.S_ISDIR(rs.st_mode) and rs.st_uid==10001 and rs.st_gid==10001 and stat.S_IMODE(rs.st_mode)==0o700
for p in sorted(root.rglob('*')):
 s=p.lstat();assert s.st_uid==10001 and s.st_gid==10001 and not stat.S_ISLNK(s.st_mode)
 assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size<=1048576
 fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
 try:
  a=os.fstat(fd);assert (a.st_dev,a.st_ino)==(s.st_dev,s.st_ino)
  h=hashlib.sha256();n=0
  while True:
   b=os.read(fd,65536)
   if not b:break
   n+=len(b);assert n<=s.st_size;h.update(b)
  z=p.lstat();assert (z.st_dev,z.st_ino,z.st_size,z.st_mtime_ns)==(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
  result.append(dict(path=p.relative_to(root).as_posix(),uid=s.st_uid,gid=s.st_gid,mode=stat.S_IMODE(s.st_mode),dev=s.st_dev,ino=s.st_ino,bytes=n,sha256=h.hexdigest()))
 finally:os.close(fd)
assert result
print(json.dumps(dict(root=dict(dev=rs.st_dev,ino=rs.st_ino,uid=rs.st_uid,gid=rs.st_gid,mode=stat.S_IMODE(rs.st_mode)),files=result)))
"""

# The predecessor's loader accepts runtime configuration schemaVersion 1-3;
# the candidate accepts 1-5. Version 3 requires stagedAckPrincipals, which the
# predecessor's runtime refuses (STAGED_ACK_SCHEMA_QUALIFIED is False there;
# the real 7fae9385 image exits on a v3 config), so only 1 and 2 restart it.
# Rollback restarts it against the same read-only mount: the exact bytes and
# version are captured at preparation and any change refuses rollback.
PREDECESSOR_CONFIG_VERSIONS = (1, 2)
CONFIG_CENSUS = """import hashlib,json,os,stat
p='/run/processing-reference/runtime.json'
s=os.lstat(p);assert stat.S_ISREG(s.st_mode) and s.st_uid==10001 and s.st_gid==10001 and s.st_nlink==1 and s.st_size<=1048576
fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
try:
 a=os.fstat(fd);assert (a.st_dev,a.st_ino)==(s.st_dev,s.st_ino);raw=b''
 while True:
  b=os.read(fd,65536)
  if not b:break
  raw+=b;assert len(raw)<=s.st_size
finally:os.close(fd)
assert len(raw)==s.st_size
def unique(pairs):
 assert len({k for k,_ in pairs})==len(pairs);return dict(pairs)
value=json.loads(raw,object_pairs_hook=unique)
version=value.get('schemaVersion') if type(value) is dict else None
print(json.dumps(dict(path='runtime.json',bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),schemaVersion=version if type(version) is int else None)))
"""


def lease_binding(directory):
    """Record the existing private authority lock without creating it."""
    if type(directory) is not str or not directory.startswith('/') or ',' in directory or '\n' in directory:
        raise Stopped('absolute_private_lease_directory_required')
    path=Path(directory)
    if path.resolve(strict=True)!=path or any(p.is_symlink() for p in (path,*path.parents)):
        raise Stopped('absolute_private_lease_directory_required')
    st=path.stat(); lock=(path/(AUTHORITY+'.lease')).lstat()
    if (not stat.S_ISDIR(st.st_mode) or (st.st_uid,st.st_gid,stat.S_IMODE(st.st_mode))!=(10001,10001,0o711)
            or not stat.S_ISREG(lock.st_mode) or (lock.st_uid,lock.st_gid,stat.S_IMODE(lock.st_mode),lock.st_nlink)!=(10001,10001,0o600,1)
            or lock.st_dev!=st.st_dev):raise Stopped('private_authority_lease_required')
    return dict(path=str(path),device=st.st_dev,inode=st.st_ino,lockDevice=lock.st_dev,lockInode=lock.st_ino)


def application_environment(transport, item, *, lease=False):
    env=transport.environment()
    default_entries=transport.image(item['Image'])['Config'].get('Env',[])
    entries=item['Config']['Env']
    defaults=dict(value.split('=',1) for value in default_entries)
    observed=dict(value.split('=',1) for value in entries)
    if len(defaults)!=len(default_entries) or len(observed)!=len(entries):raise Stopped('duplicate_application_environment_key')
    application=dict(env,PROCESSING_REFERENCE_CONFIG=legacy.CONFIG_PATH)
    expected=dict(defaults,**application)
    if lease:expected['PROCESSING_AUTHORITY_LEASE_DIR']='/run/ledgetop-authority-lease'
    if observed!=expected:raise Stopped('reference_application_environment_changed')
    return digest(application)


def owned_api_probe(operator):
    """Prove the operator lookup path answers on the running coordinator.

    An exact authenticated missing-receipt 404 shows the fixed loopback origin,
    operator token and receipt lookup work. The 693cb7e predecessor has this
    route too, so it does not prove the candidate: identity comes from the
    container, image and sole-owner checks.
    """
    if operator.origin!='http://127.0.0.1:18080' or operator.node:raise Stopped('fixed_operator_probe_required')
    connection=http.client.HTTPConnection(operator.parsed.hostname,operator.parsed.port,timeout=10)
    try:
        connection.request('GET','/v1/prototype/nodes/cluster/schedule-owned/00000000-0000-4000-8000-000000000001',
            headers={'Authorization':'Bearer '+operator.token,'Accept':'application/json'})
        response=connection.getresponse();raw=response.read(4097)
        if (response.status!=404 or len(raw)>4096 or json.loads(raw)!=dict(error='owned_schedule_not_found')):
            raise Stopped('owned_schedule_api_not_installed')
    finally:connection.close()
    return dict(operatorLookupVerified=True)


def backup_script():
    extra=inspect.getsource(volume_census)+'''
before=volume_census(source,offline=True); copied=volume_census(target,offline=True)
assert copied.pop('rootMode')==0o700
comparison=dict(before);comparison.pop('rootMode');assert comparison==copied
counts['fullCensusSha256']=hashlib.sha256(json.dumps(before,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
print(json.dumps(counts))
'''
    script=legacy.BACKUP.replace('print(json.dumps(counts))',extra)
    # The reviewed backup keeps only terminal Viewer tasks, the same rule as the
    # census above; refuse a backup script that no longer states it.
    terminal_viewer=("  if 'viewer_tasks' in tables:\n"
        "   assert db.execute(\"SELECT COUNT(*) FROM viewer_tasks WHERE "+legacy.NOT_TERMINAL_VIEWER_TASK+"\").fetchone()[0]==0\n")
    if script.count(terminal_viewer)!=1:raise Stopped('reviewed_backup_script_changed')
    return explicit_guard_source(script)


def copy_backup(transport,root,state):
    destination=root/'original-state-backup'
    if destination.exists() or destination.is_symlink():raise Stopped('new_backup_destination_required')
    installer='''import os,stat,sys
from pathlib import Path
p=Path(sys.argv[1]);assert p.is_absolute() and p.name=='original-state-backup'
assert p.parent.resolve()==p.parent and not any(q.is_symlink() for q in p.parents)
s=p.parent.stat();assert s.st_uid==1000 and not stat.S_IMODE(s.st_mode)&0o077
os.mkdir(p,0o700);os.chown(p,10001,10001)
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
'''
    transport.command(['sudo','-n','python3','-c',explicit_guard_source(installer),str(destination)])
    info=destination.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid!=10001 or info.st_gid!=10001 or stat.S_IMODE(info.st_mode)!=0o700):
        raise Stopped('private_numeric_uid_backup_required')
    return json.loads(transport.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount','type=volume,src='+VOLUME+',dst=/source,readonly',
        '--mount','type=bind,src='+str(destination)+',dst=/backup','--entrypoint','python',state['image'],'-c',backup_script(),timeout=BACKUP_TIMEOUT))


class ReferenceCoordinatorUpdate:
    def __init__(self,root,*,gate_factory,transport=legacy,cluster_probe=None,lease_dir=None):
        self.root=Path(root).absolute();self.t=transport;self.gate_factory=gate_factory
        self.lease_dir=lease_dir
        self.cluster_probe=cluster_probe or self.cluster_idle
        if self.root.resolve()!=self.root or any(path.is_symlink() for path in (self.root,*self.root.parents)):
            raise Stopped('private_update_root_required')
        info=self.root.stat()
        if info.st_uid!=os.geteuid() or not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:raise Stopped('private_update_root_required')
        if not callable(gate_factory) or not isinstance(getattr(gate_factory,'binding',None),dict):raise Stopped('live_gate_factory_required')

    def save(self,state,phase=None,**fields):
        if phase:state['phase']=phase
        state.update(fields);self.t.save(self.root,state)

    def load(self):
        state=json.loads(self.t.private_file(self.root/'checkpoint.json').read_text())
        self.validate(state);return state

    def validate(self,state):
        if ((self.lease_dir is None and 'leaseBinding' in state) or
                (self.lease_dir is not None and state.get('leaseBinding')!=lease_binding(self.lease_dir))):
            raise Stopped('lease_binding_changed')
        if (state.get('schemaVersion')!=2 or state.get('mode')!=MODE or state.get('authorityId')!=AUTHORITY
                or state.get('old',{}).get('containerId')!=OLD_ID or state['old'].get('image')!=OLD_IMAGE
                or state.get('gateBinding')!=self.gate_factory.binding):raise Stopped('reference_update_checkpoint_binding_changed')
        if (type(state.get('image')) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',state['image'])
                or type(state.get('sourceCommit')) is not str or not re.fullmatch(r'[a-f0-9]{40}',state['sourceCommit'])
                or state.get('oldSourceCommit')!=OLD_SOURCE or type(state.get('operation')) is not str
                or not re.fullmatch(r'[a-f0-9]{32}',state['operation'])
                or state.get('rollbackName')!=NAME+'-pre-reference-update-'+state['operation']
                or state.get('failedName')!=NAME+'-failed-reference-update-'+state['operation']
                or type(state.get('engineImages')) is not dict or set(state['engineImages'])!={'main','cluster'}
                or any(type(value) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',value) for value in state['engineImages'].values())
                or type(state.get('clusterContainerId')) is not str or not re.fullmatch(r'[a-f0-9]{64}',state['clusterContainerId'])
                or ('newContainerId' in state and (type(state['newContainerId']) is not str or not re.fullmatch(r'[a-f0-9]{64}',state['newContainerId'])))):
            raise Stopped('immutable_checkpoint_identity_changed')

    def census(self,state,*,offline=False,container=None):
        code=explicit_guard_source(inspect.getsource(volume_census)+'\nimport json\nprint(json.dumps(volume_census('+repr('/source' if offline else '/prototype-state')+',offline='+repr(offline)+')))')
        if offline:
            if self.t.docker('ps','--filter','volume='+VOLUME,'--format','{{.ID}}').strip():raise Stopped('offline_volume_has_owner')
            raw=self.t.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
                '--cap-drop','ALL','--security-opt','no-new-privileges','--mount','type=volume,src='+VOLUME+',dst=/source,readonly',
                '--entrypoint','python',state['image'],'-c',code,timeout=CENSUS_TIMEOUT)
        else:raw=self.t.docker('exec','--user','10001:10001',container or state['old']['containerId'],'python','-c',code,timeout=CENSUS_TIMEOUT)
        result=json.loads(raw)
        history_count = state.get('history', {}).get('count')
        if (type(history_count) is not int or history_count < 0 or result['authorityId'] != AUTHORITY
                or result['tables']['jobs']['count'] != history_count):
            raise Stopped('reviewed_idle_history_required')
        return result

    def tls(self,state):
        raw=self.t.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--mount',
            'type=bind,src='+str(legacy.TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '--entrypoint','python',state['image'],'-c',explicit_guard_source(TLS_CENSUS))
        return json.loads(raw)

    def predecessor_config(self,state):
        """Exact runtime configuration the predecessor reads when restarted."""
        raw=self.t.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--mount',
            'type=bind,src='+str(legacy.TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '--entrypoint','python',state['image'],'-c',explicit_guard_source(CONFIG_CENSUS))
        value=json.loads(raw)
        if (type(value) is not dict or set(value)!={'path','bytes','sha256','schemaVersion'} or value['path']!='runtime.json'
                or type(value['bytes']) is not int or type(value['sha256']) is not str or not re.fullmatch(r'[a-f0-9]{64}',value['sha256'])
                or type(value['schemaVersion']) is not int or value['schemaVersion'] not in PREDECESSOR_CONFIG_VERSIONS):
            raise Stopped('predecessor_config_schema_unsupported')
        return value

    def rollback_config(self,state):
        """Rollback stays closed unless the predecessor reads the captured config."""
        captured=state.get('predecessorConfig')
        if (type(captured) is not dict or captured.get('schemaVersion') not in PREDECESSOR_CONFIG_VERSIONS
                or self.predecessor_config(state)!=captured):raise Stopped('predecessor_config_changed_rollback_refused')

    def cluster_idle(self,state):
        """Fixed existing private backend: no token/command arguments in proof."""
        from processing.container_update import ComposeNode
        item=self.t.inspected(CLUSTER_NAME)
        labels=item['Config'].get('Labels') or {}
        bindings=item.get('NetworkSettings',{}).get('Ports',{}).get('3000/tcp')
        if (item['Id']!=state['clusterContainerId'] or item['Name']!='/'+CLUSTER_NAME
                or item['Image']!=state['engineImages']['cluster'] or item['State']['Running'] is not True
                or labels.get('com.docker.compose.project')!=CLUSTER_PROJECT
                or labels.get('com.docker.compose.service')!='nodeodm'
                or bindings!=[{'HostIp':'127.0.0.1','HostPort':'13001'}]):raise Stopped('fixed_cluster_backend_changed')
        token=self.t.environment().get('LOCAL_NODE_TOKEN')
        if type(token) is not str or not token:raise Stopped('private_cluster_credential_unavailable')
        deadline=time.monotonic()+10
        info=cluster_http_json('/info',token,deadline);tasks=cluster_http_json('/task/list',token,deadline)
        if (type(info) is not dict or info.get('engine')!='odm' or type(info.get('taskQueueCount')) is not int
                or info['taskQueueCount']!=0 or type(tasks) is not list or tasks!=[]):raise Stopped('cluster_backend_tasks_not_idle')
        # These two existing inspectors perform only the fixed upload-directory
        # and engine-child census. They cannot run Compose replacement actions.
        inspector=object.__new__(ComposeNode)
        def docker(command,**kwargs):
            if not command or command[0]!='docker':raise Stopped('fixed_local_inspection_required')
            return self.t.docker(*command[1:],**kwargs)
        inspector.docker=docker
        if not inspector.uploads_idle(item['Id']) or not inspector.processing_idle(item['Id']):
            raise Stopped('cluster_upload_or_child_process_not_idle')

    def original(self,state=None):
        item=self.t.inspected(NAME)
        if (item['Id']!=OLD_ID or item['Image']!=OLD_IMAGE or not item['State']['Running']
                or item['State'].get('Health',{}).get('Status')!='healthy'
                or item['HostConfig'].get('Tmpfs')):raise Stopped('exact_current_reference_primary_required')
        facts=self.facts(item);self.t.sole_owner(OLD_ID)
        if self.t.image(OLD_IMAGE)['Config'].get('Labels',{}).get('org.opencontainers.image.revision')!=OLD_SOURCE:
            raise Stopped('predecessor_source_changed')
        if state and (facts!=state['old'] or application_environment(self.t,item,lease=self.lease_dir is not None)!=state['applicationEnvironmentSha256']):
            raise Stopped('prepared_reference_primary_changed')
        return item,facts

    def activity(self,state,api):
        if self.t.activity(api,retained_viewer=True)!=state['history']:raise Stopped('public_history_or_native_activity_changed')

    def prepare(self,target,source_commit,main_image):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',target) or not re.fullmatch(r'[0-9a-f]{40}',source_commit):raise Stopped('immutable_candidate_required')
        if target==OLD_IMAGE:raise Stopped('new_reference_image_required')
        if (self.root/'checkpoint.json').exists():raise Stopped('new_private_operation_required')
        old,original=self.original();application=application_environment(self.t,old,lease=self.lease_dir is not None)
        candidate=self.t.image(target)
        if (candidate['Id']!=target or candidate['Config'].get('Labels',{}).get('org.opencontainers.image.revision')!=source_commit
                or any(candidate['Config'].get(key)!=old['Config'].get(key) for key in ('User','WorkingDir','Cmd','Entrypoint','Healthcheck'))):
            raise Stopped('candidate_recipe_or_source_not_verified')
        _env,api,_operator=self.t.local_clients();history=self.t.activity(api,retained_viewer=True)
        if type(history.get('count')) is not int or history['count'] < 0:
            raise Stopped('reviewed_terminal_job_count_required')
        nodes={row['id']:row for row in api.json('GET','/v1/prototype/nodes')['nodes']}
        if set(nodes)!={'main','cluster'} or any(row['state']!='active' for row in nodes.values()):raise Stopped('other_maintenance_present')
        cluster=self.t.inspected(CLUSTER_NAME)
        if (not cluster['State']['Running'] or not re.fullmatch(r'[a-f0-9]{64}',cluster['Id'])
                or not re.fullmatch(r'sha256:[0-9a-f]{64}',main_image)):raise Stopped('verified_processing_images_required')
        operation=uuid.uuid4().hex
        state=dict(schemaVersion=2,mode=MODE,operation=operation,phase='preparing',image=target,sourceCommit=source_commit,
            oldSourceCommit=OLD_SOURCE,old=original,authorityId=AUTHORITY,history=history,plans={},
            applicationEnvironmentSha256=application,gateBinding=self.gate_factory.binding,
            engineImages=dict(main=main_image,cluster=cluster['Image']),clusterContainerId=cluster['Id'],
            rollbackName=NAME+'-pre-reference-update-'+operation,failedName=NAME+'-failed-reference-update-'+operation)
        if self.lease_dir is not None:state['leaseBinding']=lease_binding(self.lease_dir)
        self.validate(state)
        self.cluster_probe(state)
        for name in (state['rollbackName'],state['failedName']):
            if self.t.docker('ps','--all','--filter','name=^/'+name+'$','--format','{{.ID}}').strip():raise Stopped('retention_name_exists')
        self.save(state);self.t.candidate_loader(target,operation)
        baseline=self.census(state)
        try:reviewed_pilot_journals(baseline)
        except Stopped:
            # The observed manifest stays private evidence for the human reviewer.
            self.save(state,'pilot_journals_held',observedPilotJournals=baseline['pilotJournals']);raise
        self.save(state,'prepared',baseline=baseline,tls=self.tls(state),predecessorConfig=self.predecessor_config(state))
        return state

    def check(self,state,gates):
        self.validate(state);gates.check()
        if self.tls(state)!=state['tls']:raise Stopped('tls_or_private_configuration_changed')
        self.cluster_probe(state)
        # TLS/Docker reads can take time. Never act on the earlier gate proof.
        gates.check()

    def mutation(self,state,gates,*args,**kwargs):
        self.check(state,gates);return self.t.docker(*args,**kwargs)

    def candidate(self,state):
        identifier=state.get('newContainerId')
        if not identifier:
            matches=self.t.docker('ps','--all','--filter','label='+LABEL+'='+state['operation'],'--format','{{.ID}}').split()
            if len(matches)>1:raise Stopped('ambiguous_candidate_identity')
            if not matches:return None
            identifier=matches[0]
        item=self.t.inspected(identifier)
        if (not re.fullmatch(r'[a-f0-9]{64}',item['Id']) or item['Image']!=state['image']
                or item['Config'].get('Labels',{}).get(LABEL)!=state['operation']
                or item['Name'] not in ('/'+NAME,'/'+state['failedName'])):raise Stopped('foreign_candidate_refused')
        self.facts(item)
        if item['HostConfig'].get('Tmpfs')!=TMPFS:raise Stopped('candidate_tmpfs_changed')
        if application_environment(self.t,item,lease=self.lease_dir is not None)!=state['applicationEnvironmentSha256']:raise Stopped('candidate_application_environment_changed')
        self.save(state,newContainerId=item['Id']);return item

    def facts(self,item):
        if self.lease_dir is None:return self.t.facts(item,reference=True)
        lease_binding(self.lease_dir)
        mounts=item.get('Mounts') or []
        found=[m for m in mounts if m.get('Destination')=='/run/ledgetop-authority-lease']
        if len(found)!=1 or found[0].get('Type')!='bind' or found[0].get('Source')!=self.lease_dir or found[0].get('RW') is not True:
            raise Stopped('authority_lease_mount_changed')
        return self.t.facts(dict(item,Mounts=[m for m in mounts if m is not found[0]]),reference=True)

    def plans(self,state,api):
        for identifier,plan in state['plans'].items():
            current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if (current['state']!='draining' or current['planId']!=plan['planId']
                    or current!=plan['acknowledgedNode']):raise Stopped('owned_draining_plan_changed')

    def node_census(self,state,observed,*,reopening=False):
        baseline=state['baseline']['nodeRows']
        if set(baseline)!=set(observed['nodeRows']) or set(state['plans'])!={'main','cluster'}:raise Stopped('node_inventory_changed')
        for identifier,original in baseline.items():
            plan=state['plans'][identifier];wire=plan['payload']
            if wire['expectedRevision']!=original['revision']:raise Stopped('node_revision_not_prepared')
            expected=dict(original,state='draining',revision=wire['expectedRevision']+1,plan_id=plan['planId'],
                drain_at=wire['drainAt'],not_before=wire['notBefore'],after_job_id=None,target_image=wire['targetImage'],
                result=None,update_hash=None,finish_status=None)
            # Only a FULL-saved exact cancel intent may account for a reopened
            # row. All original/private node columns remain compared.
            intent=plan.get('cancelIntent')
            if reopening and intent:
                if intent!={'planId':plan['planId'],'expectedRevision':expected['revision']}:raise Stopped('cancel_census_binding_changed')
                if observed['nodeRows'][identifier]['state']=='active':
                    expected.update(state='active',revision=expected['revision']+1)
            if observed['nodeRows'][identifier]!=expected:raise Stopped('unowned_node_row_transition')

    def create_args(self,state):
        args=('create','--name',NAME,'--pull=never','--init','--restart','unless-stopped','--network','bridge',
            '--read-only','--tmpfs','/tmp:'+TMPFS['/tmp'],'--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--pids-limit','64','--cpus','0.5','--memory','256m','--memory-swap','512m','--ipc','private','--shm-size','64m',
            '--log-driver','json-file','--workdir','/app','--env-file',str(legacy.NATIVE_ENV),'-e','PROCESSING_REFERENCE_CONFIG='+legacy.CONFIG_PATH,
            '--mount','type=volume,src='+VOLUME+',dst=/prototype-state',
            '--mount','type=bind,src='+str(legacy.TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '-p','127.0.0.1:18080:8080','-p','192.168.50.89:3005:8080','-p','192.168.50.89:18443:8443',
            '--label',LABEL+'='+state['operation'])
        if self.lease_dir is not None:
            if state.get('leaseBinding')!=lease_binding(self.lease_dir):raise Stopped('lease_binding_changed')
            args+=('-e','PROCESSING_AUTHORITY_LEASE_DIR=/run/ledgetop-authority-lease',
                   '--mount','type=bind,src='+self.lease_dir+',dst=/run/ledgetop-authority-lease')
        return args+(state['image'],)

    def backup(self,state,gates):
        self.check(state,gates)
        # Historical copier hashes every byte and fsyncs files/directories. Its
        # old count check is supplemented by our full logical/content manifest.
        before=self.census(state,offline=True)
        compatible(before,state['drained'])
        result=copy_backup(self.t,self.root,state)
        if result.get('fullCensusSha256')!=digest(before):raise Stopped('backup_full_content_proof_changed')
        self.check(state,gates)
        after=self.census(state,offline=True);compatible(after,before)
        return dict(**result,sourceCensusSha256=digest(before),sourceRawFiles=before['files'])

    def reopen(self,state,gates,api,operator):
        if state.get('pendingPlan'):raise Stopped('unknown_schedule_outcome_never_adopted')
        for identifier,plan in state['plans'].items():
            self.check(state,gates)
            if not state.get('reopeningStarted'):
                self.activity(state,api)
                observed=self.census(state,container=state.get('newContainerId') or OLD_ID)
                self.node_census(state,observed)
                compatible(observed,state['drained'],allow_owned=True,allow_viewer_additions=True,allow_migration=True)
            current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if current['planId']!=plan['planId'] or current['targetImage']!=state['engineImages'][identifier]:raise Stopped('cannot_reopen_peer_plan')
            intent=plan.get('cancelIntent')
            if current['state']=='active' and intent and current['revision']==intent['expectedRevision']+1:
                expected=dict(plan['acknowledgedNode'],state='active',revision=intent['expectedRevision']+1)
                if current!=expected:raise Stopped('reopen_readback_changed')
                self.check(state,gates);plan['cancelled']=True;self.save(state);continue
            if current!=plan['acknowledgedNode'] or intent:raise Stopped('reopen_outcome_requires_inspection')
            payload=dict(planId=plan['planId'],expectedRevision=current['revision'])
            plan['cancelIntent']=payload
            self.save(state,'reopening',reopeningStarted=True)
            self.check(state,gates)
            try:operator.json('POST','/v1/prototype/nodes/'+identifier+'/cancel',payload)
            except Exception:
                after=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
                if (after['state']!='active' or after['planId']!=plan['planId'] or after['revision']!=payload['expectedRevision']+1
                        or after['targetImage']!=state['engineImages'][identifier]):raise Stopped('reopen_outcome_requires_inspection') from None
            after=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if after!=dict(plan['acknowledgedNode'],state='active',revision=payload['expectedRevision']+1):raise Stopped('reopen_readback_changed')
            self.check(state,gates)
            plan['cancelled']=True;self.save(state)

    def activate(self,state):
        self.validate(state)
        if state['phase']!='prepared':raise Stopped('prepared_operation_only_no_replay')
        reviewed_pilot_journals(state['baseline'])
        self.original(state);_env,api,operator=self.t.local_clients();self.activity(state,api)
        compatible(self.census(state),state['baseline'])
        with self.gate_factory() as gates:
            self.check(state,gates)
            try:
                for identifier in ('cluster','main'):
                    current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
                    if current['state']!='active':raise Stopped('node_no_longer_active')
                    stamp=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
                    payload=dict(expectedRevision=current['revision'],targetImage=state['engineImages'][identifier],drainAt=stamp,notBefore=stamp,afterJobId=None)
                    self.save(state,'schedule_pending',pendingPlan=dict(nodeId=identifier,payload=payload))
                    self.check(state,gates)
                    # This legacy route has no durable caller correlation; the
                    # updater does not use schedule-owned, though 693cb7e has it.
                    # Any lost ACK is held, even when readback has the same target.
                    plan=operator.json('POST','/v1/prototype/nodes/'+identifier+'/schedule',payload)['node']
                    if (plan['id']!=identifier or plan['state']!='draining' or plan['targetImage']!=payload['targetImage']
                            or type(plan['planId']) is not str or str(uuid.UUID(plan['planId']))!=plan['planId']):raise Stopped('exact_schedule_ack_required')
                    if plan['revision']!=payload['expectedRevision']+1:raise Stopped('schedule_revision_changed')
                    state['plans'][identifier]=dict(planId=plan['planId'],targetImage=plan['targetImage'],payload=payload,acknowledgedNode=plan)
                    state.pop('pendingPlan');self.save(state)
                self.plans(state,api);self.activity(state,api)
                drained=self.census(state);self.node_census(state,drained)
                compatible(drained,state['baseline'],ignore_nodes=True)
                self.save(state,'stop_old_pending',drained=drained)
                self.mutation(state,gates,'stop','--time','-1',OLD_ID,timeout=60)
                if self.t.inspected(OLD_ID)['State']['Running']:raise Stopped('old_not_confirmed_stopped')
                compatible(self.census(state,offline=True),drained)
                self.save(state,'backup_pending');backed=self.backup(state,gates)
                self.save(state,'rename_old_pending',backup=backed)
                self.mutation(state,gates,'rename',OLD_ID,state['rollbackName'])
                self.save(state,'create_pending')
                identifier=self.mutation(state,gates,*self.create_args(state)).strip()
                if not re.fullmatch(r'[a-f0-9]{64}',identifier):raise Stopped('create_response_unknown')
                self.save(state,'created',newContainerId=identifier)
                self.candidate(state)
                self.save(state,'start_pending')
                self.mutation(state,gates,'start',identifier)
                self.save(state,'started',replacementStarted=True)
                self.t.health(api,container_id=identifier);self.t.sole_owner(identifier)
                self.check(state,gates);self.activity(state,api);self.plans(state,api)
                compatible(self.census(state,container=identifier),drained,allow_owned=True,allow_viewer_additions=True,allow_migration=True)
                verified=self.t.verify_reference();verified.update(owned_api_probe(operator))
                self.save(state,'verified',verification=verified)
                self.reopen(state,gates,api,operator)
                self.check(state,gates)
                self.save(state,'completed',sameAuthority=True,sameStateVolume=True,databaseRestored=False)
            except Exception:
                # No automatic rollback. It is a separately invoked guarded
                # operation; this also avoids replay after unknown stop/start.
                self.save(state,'needs_inspection');raise
        return state

    def retain(self,state,gates,item):
        self.check(state,gates)
        current=self.candidate(state)
        if current is None or current['Id']!=item['Id'] or current['Name']!='/'+NAME:raise Stopped('canonical_candidate_retention_only')
        if not current['State']['Running']:
            if self.t.docker('ps','--filter','volume='+VOLUME,'--format','{{.ID}}').strip():raise Stopped('retention_volume_has_other_owner')
            self.save(state,'retain_start_pending')
            self.mutation(state,gates,'start',item['Id'])
        else:self.t.sole_owner(item['Id'])
        _env,api,_operator=self.t.local_clients()
        self.t.health(api,container_id=item['Id']);self.t.sole_owner(item['Id'])
        self.save(state,'needs_inspection',downgradeRefused=True,replacementRetained=True)

    def finish_rollback(self,state,gates):
        self.check(state,gates);self.original(state)
        _env,api,operator=self.t.local_clients();self.t.health(api,container_id=OLD_ID)
        self.t.sole_owner(OLD_ID);self.activity(state,api)
        compatible(self.census(state,container=OLD_ID),state['drained'],allow_owned=True,allow_migration=True)
        self.t.verify_reference()
        state.pop('newContainerId',None)
        self.reopen(state,gates,api,operator)
        self.check(state,gates)
        self.save(state,'rolled_back',databaseRestored=False,referenceTLSRetained=True)
        return state

    def rollback(self,state):
        self.validate(state)
        if any(name.startswith('reference_viewer_activation') or name in ('reference_viewer_submission_owners','reference_viewer_envelopes_v2','reference_viewer_v3_copy_hold','reference_viewer_v3_policy') for name in state.get('drained',{}).get('tables',{})):
            raise Stopped('prepared_schema_rollback_unqualified')
        if state.get('reopeningStarted') or state.get('pendingPlan'):raise Stopped('reopen_or_schedule_uncertainty_prevents_rollback')
        with self.gate_factory() as gates:
            self.check(state,gates);self.rollback_config(state);item=self.candidate(state)
            old=self.t.inspected(OLD_ID)
            if old['State']['Running']:
                if state['phase']!='rollback_start_old_pending' or old['Name']!='/'+NAME:
                    raise Stopped('unexpected_running_predecessor')
                # Exact old-start intent + observed sole restored owner proves
                # the lost ACK without an offline helper or another start.
                return self.finish_rollback(state,gates)
            if item is not None and item['State']['Running']:
                _env,api,_operator=self.t.local_clients();self.activity(state,api)
                compatible(self.census(state,container=item['Id']),state['drained'],allow_owned=True,allow_viewer_additions=True,allow_migration=True)
                self.save(state,'rollback_stop_pending')
                self.mutation(state,gates,'stop','--time','-1',item['Id'],timeout=60)
                if self.t.inspected(item['Id'])['State']['Running']:raise Stopped('candidate_not_confirmed_stopped')
            try:compatible(self.census(state,offline=True),state['drained'],allow_owned=True,allow_migration=True)
            except Exception:
                if item is not None:self.retain(state,gates,item)
                raise Stopped('offline_rollback_refused') from None
            old=self.t.inspected(OLD_ID)
            if old['Image']!=OLD_IMAGE or old['Name'] not in ('/'+NAME,'/'+state['rollbackName']):raise Stopped('exact_old_reference_rollback_only')
            self.facts(old)
            if application_environment(self.t,old,lease=self.lease_dir is not None)!=state['applicationEnvironmentSha256']:raise Stopped('rollback_application_environment_changed')
            if item is not None and item['Name']=='/'+NAME:
                self.save(state,'rollback_rename_candidate_pending');self.mutation(state,gates,'rename',item['Id'],state['failedName'])
            if old['Name']!='/'+NAME:
                self.save(state,'rollback_rename_old_pending');self.mutation(state,gates,'rename',OLD_ID,NAME)
            if not old['State']['Running']:
                self.rollback_config(state)
                self.save(state,'rollback_start_old_pending');self.mutation(state,gates,'start',OLD_ID)
            self.finish_rollback(state,gates)
        return state


class GateFactory:
    """Cluster executor half of the reviewed RootRelay transport.

    The root relay launches authenticated, host-key-pinned main/cluster SSH
    processes. This executor's stdin/stdout are exclusively bridge frames.
    """
    def __init__(self,config,*,operation_id=None,transport=legacy):
        from ops.coordinator_gate_bridge import BridgeBinding,LocalGate,BridgeGateClient,GateSet,helper_digest
        if (type(config) is not dict or set(config)!={'schemaVersion','cluster','main','sessionNonce','timeoutSeconds'}
                or type(config['schemaVersion']) is not int or config['schemaVersion']!=1
                or type(config['sessionNonce']) is not str or not re.fullmatch(r'[a-f0-9]{64}',config['sessionNonce'])
                or type(config['timeoutSeconds']) is not int or not 1<=config['timeoutSeconds']<=30):raise Stopped('closed_bridge_configuration_required')
        bindings={}
        fields={'engineId','nodeId','authorityId','coordinatorOrigin','generation','helperSha256'}
        for name in ('cluster','main'):
            row=config[name]
            if (type(row) is not dict or set(row)!=fields or row['nodeId']!=name or row['authorityId']!=AUTHORITY
                    or row['coordinatorOrigin']!='http://127.0.0.1:18080' or row['helperSha256']!=helper_digest()):
                raise Stopped('fixed_live_gate_binding_required')
            bindings[name]=BridgeBinding(engine_id=row['engineId'],node_id=name,authority_id=row['authorityId'],
                coordinator_origin=row['coordinatorOrigin'],gate_generation=row['generation'],helper_sha256=row['helperSha256'])
        self.binding={name:item.public() for name,item in bindings.items()}
        self.bindings=bindings;self.operation_id=operation_id;self.config=config;self.t=transport
        self.classes=(LocalGate,BridgeGateClient,GateSet)

    def __call__(self):
        import sys
        if not self.operation_id:raise Stopped('prepared_bridge_operation_required')
        LocalGate,BridgeGateClient,GateSet=self.classes
        def engine_probe():return self.t.docker('info','--format','{{.ID}}').strip()
        local=LocalGate(self.bindings['cluster'],engine_probe=engine_probe)
        remote=BridgeGateClient(sys.stdin.buffer,sys.stdout.buffer,binding=self.bindings['main'],
            operation_id=self.operation_id,session_nonce=self.config['sessionNonce'],timeout_seconds=self.config['timeoutSeconds'])
        return GateSet(local,remote)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('prepare','activate','rollback'))
    parser.add_argument('--image',required=True);parser.add_argument('--source-commit',required=True)
    parser.add_argument('--receipt-dir',required=True);parser.add_argument('--verified-main-image',required=True)
    parser.add_argument('--gate-config',required=True,help='private reviewed live dual-host gate bridge configuration')
    parser.add_argument('--lease-dir',required=True,help='existing host authority lease directory')
    args=parser.parse_args(argv)
    try:
        if os.name!='posix':raise Stopped('linux_reference_update_required')
        import pwd
        if (os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name!='bkoltz'
                or socket.gethostname().split('.')[0].lower()!='cluster'):raise Stopped('fixed_cluster_account_required')
        with legacy.operation_lock():
            root=Path(args.receipt_dir)
            if not root.is_absolute() or root.resolve()!=root or any(path.is_symlink() for path in (root,*root.parents)):
                raise Stopped('absolute_private_operation_required')
            if args.action=='prepare':
                parent=root.parent.stat()
                if parent.st_uid!=os.geteuid() or parent.st_mode & 0o077:raise Stopped('private_operation_parent_required')
                root.mkdir(mode=0o700)
            state=None if args.action=='prepare' else json.loads(legacy.private_file(root/'checkpoint.json').read_text())
            factory=GateFactory(json.loads(legacy.private_file(args.gate_config).read_text()),operation_id=state['operation'] if state else None)
            updater=ReferenceCoordinatorUpdate(root,gate_factory=factory,lease_dir=args.lease_dir)
            if args.action=='prepare':state=updater.prepare(args.image,args.source_commit,args.verified_main_image)
            else:
                if not ACTIVATION_REVIEWED:raise Stopped('source_review_required_before_activation')
                state=updater.load()
                if (state['image'],state['sourceCommit'],state['engineImages']['main'])!=(args.image,args.source_commit,args.verified_main_image):raise Stopped('prepared_argument_binding_changed')
                state=updater.activate(state) if args.action=='activate' else updater.rollback(state)
        # Activation stdout belongs to the root relay until process EOF.
        print(json.dumps(dict(operation=state['operation'],phase=state['phase'],authorityId=AUTHORITY)),
            file=__import__('sys').stdout if args.action=='prepare' else __import__('sys').stderr)
        return 0
    except Exception:
        print('Reference coordinator update held; retain private evidence and inspect locally.',file=__import__('sys').stderr)
        return 2


if __name__=='__main__':raise SystemExit(main())
