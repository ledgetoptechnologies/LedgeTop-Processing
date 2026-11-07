"""Read-only legacy archive observation and comparison with frozen custody.

The observer deliberately uses a read-only queue shim. Its output is NOT proof
of a live HTTP route, coordinator lease, a frozen predecessor, or image identity.
An independent caller must verify the actual container image and source before
accepting the output. The driver must additionally compare it with the real
post-drain full-custody baseline before admission opens.
"""
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3

from . import coordinator_predecessor_census as census
from .enrolled_coordinator_transport import canonical, decode, require

PROTOCOL = 'ltds-legacy-delivery-observation-v1'
TABLES = ('jobs', 'viewer_tasks', 'viewer_files')
SCOPE = 'read-only-candidate-reader-with-test-queue-shim'
CAPTURE_PROTOCOL = 'ltds-legacy-delivery-isolated-capture-v1'
HEX = re.compile(r'[0-9a-f]{64}\Z')


def _table_snapshot(path, key):
    budget = census._ReadBudget(60, 64 * 1024 * 1024)
    db = sqlite3.connect(Path(path).as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
        authority = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()
        require(authority is not None, 'legacy_authority_missing')
        result = {}
        for name in TABLES:
            values, used = [], 0
            for row in db.execute('SELECT * FROM "' + name + '"'):
                require(len(values) < 100000, 'legacy_metadata_budget_exceeded')
                value = census._row_bytes(dict(row), 64 * 1024 * 1024 - used, budget)
                used += len(value); require(used <= 64 * 1024 * 1024, 'legacy_metadata_budget_exceeded')
                values.append(value)
            digest = hmac.new(key, b'table\0' + name.encode() + b'\0', hashlib.sha256)
            for value in sorted(values):
                digest.update(len(value).to_bytes(8, 'big')); digest.update(value)
            result[name] = dict(count=len(values), content=digest.hexdigest())
        rows = db.execute('SELECT uuid,environment,directory FROM viewer_tasks WHERE removed=0').fetchall()
        require(len(rows) <= 1000, 'legacy_task_budget_exceeded')
        return authority[0], result, [tuple(row) for row in rows]
    finally:
        db.close()


class _InspectionFreeze:
    def _check(self): pass
    def register_legacy_archive(self, owner): pass
    def finish_legacy_archive(self, owner): pass
    def uncertain_legacy_archive(self, owner): raise RuntimeError('legacy_archive_close_uncertain')


class _InspectionQueue:
    def __init__(self, path, authority, nodes):
        self.path, self.authority_id, self.node_ids = str(path), authority, tuple(nodes)
        self.startup_id, self._coordinator_freeze = 'read-only-inspection', _InspectionFreeze()

    @contextmanager
    def _db(self):
        db = sqlite3.connect(Path(self.path).as_uri() + '?mode=ro', uri=True)
        try: yield db
        finally: db.close()


def observe(root, *, key, candidate_image, candidate_source, nodes, max_archive_bytes):
    """Use the currently imported candidate reader against retained RO files.

    A caller must independently verify the container's exact image ID and mount
    the original state read-only. The 32-byte census key must not be logged.
    """
    require(type(key) is bytes and len(key) == 32, 'legacy_census_key_required')
    require(type(candidate_image) is str and re.fullmatch('sha256:[0-9a-f]{64}', candidate_image)
            and type(candidate_source) is str and re.fullmatch('[0-9a-f]{40}', candidate_source),
            'legacy_candidate_identity_required')
    require(type(nodes) in (tuple, list) and nodes and len(set(nodes)) == len(nodes)
            and all(type(node) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_-]{0,63}', node) for node in nodes),
            'legacy_candidate_nodes_required')
    from processing import legacy_viewer_readonly as reader
    path = Path(root) / 'jobs.sqlite3'
    authority, tables, tasks = _table_snapshot(path, key)
    service = reader.LegacyViewerReadonly(_InspectionQueue(path, authority, nodes),
                                          max_archive_bytes=max_archive_bytes)
    archives = {}
    for identifier, environment, directory in tasks:
        info = service.info(identifier, environment)
        require(info['status']['code'] == 40, 'legacy_non_success_task_refused')
        stream, size = service.download(identifier, environment)
        try:
            mode = os.fstat(stream.fileno()).st_mode & 0o777
            keyed, total = hmac.new(key, b'file\0', hashlib.sha256), 0
            while True:
                part = stream.read(1024 * 1024)
                if not part: break
                total += len(part); require(total <= size, 'legacy_archive_size_changed'); keyed.update(part)
            require(total == size, 'legacy_archive_size_changed')
        finally: stream.close()
        relative = 'viewer-tasks/' + directory + '/all.zip'
        name = hmac.new(key, b'path\0' + relative.encode(), hashlib.sha256).hexdigest()
        require(name not in archives, 'duplicate_legacy_archive')
        archives[name] = dict(bytes=size, content=keyed.hexdigest(), mode=mode)
    after_authority, after_tables, after_tasks = _table_snapshot(path, key)
    require((authority, tables, tasks) == (after_authority, after_tables, after_tasks), 'legacy_observation_changed')
    return dict(schemaVersion=1, protocol=PROTOCOL, scope=SCOPE, authorityId=authority,
                candidateImageId=candidate_image, candidateSourceCommit=candidate_source,
                readerSha256=hashlib.sha256(Path(reader.__file__).read_bytes()).hexdigest(),
                tables=tables, archives=archives)


def validate_artifact(raw, plan, *, reader_sha256):
    require(type(raw) is bytes and len(raw) <= 1024 * 1024, 'bounded_legacy_qualification_required')
    value = decode(raw, 1024 * 1024)
    require(type(value) is dict and set(value) == {'schemaVersion','protocol','scope','authorityId',
            'candidateImageId','candidateSourceCommit','readerSha256','tables','archives'}
            and type(value['schemaVersion']) is int and value['schemaVersion'] == 1
            and value['protocol'] == PROTOCOL and value['scope'] == SCOPE and raw == canonical(value),
            'legacy_qualification_invalid')
    require(value['authorityId'] == plan['authorityId'] and value['candidateImageId'] == plan['candidate']['imageId']
            and value['candidateSourceCommit'] == plan['candidate']['sourceCommit']
            and type(reader_sha256) is str and HEX.fullmatch(reader_sha256)
            and value['readerSha256'] == reader_sha256, 'legacy_qualification_candidate_changed')
    require(type(value['tables']) is dict and set(value['tables']) == set(TABLES)
            and all(type(row) is dict and set(row) == {'count','content'}
                    and type(row['count']) is int and 0 <= row['count'] <= 100000
                    and type(row['content']) is str and HEX.fullmatch(row['content']) for row in value['tables'].values()),
            'legacy_qualification_tables_invalid')
    require(type(value['archives']) is dict and len(value['archives']) <= 1000
            and all(type(name) is str and HEX.fullmatch(name) and type(row) is dict
                    and set(row) == {'bytes','content','mode'} and type(row['bytes']) is int and row['bytes'] >= 4
                    and type(row['mode']) is int and row['mode'] & 0o077 == 0
                    and type(row['content']) is str and HEX.fullmatch(row['content'])
                    for name, row in value['archives'].items()), 'legacy_qualification_archives_invalid')
    return value


def validate_against_baseline(raw, plan, baseline, *, reader_sha256):
    value = validate_artifact(raw, plan, reader_sha256=reader_sha256)
    require(type(baseline) is dict and baseline.get('profile') == census.PROFILE
            and baseline.get('validationScope') == 'full-retained-custody'
            and baseline.get('retainedFilesValidated') is True and baseline.get('authorityId') == value['authorityId']
            and type(baseline.get('tables')) is dict and type(baseline.get('files')) is dict
            and type(baseline.get('archiveReceipts')) is int
            and baseline['archiveReceipts'] == len(value['archives']), 'frozen_legacy_custody_required')
    require(all(baseline['tables'].get(name) == row for name, row in value['tables'].items())
            and all(baseline['files'].get(name) == row for name, row in value['archives'].items()),
            'legacy_observation_differs_from_frozen_custody')
    return value


def seal_capture(observation, *, key, container_id, state_volume, helper_hashes):
    require(type(key) is bytes and len(key)==32 and type(container_id) is str and HEX.fullmatch(container_id)
            and type(state_volume) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',state_volume)
            and type(helper_hashes) is dict and helper_hashes
            and all(type(k) is str and type(v) is str and HEX.fullmatch(v) for k,v in helper_hashes.items()),
            'legacy_capture_binding_invalid')
    value=dict(schemaVersion=1,protocol=CAPTURE_PROTOCOL,containerId=container_id,stateVolume=state_volume,
               network='none',readOnlyRoot=True,readOnlyState=True,uid=10001,helpers=helper_hashes,
               observation=observation)
    value['mac']=hmac.new(key,b'legacy-capture\0'+canonical(value),hashlib.sha256).hexdigest()
    return canonical(value)


def validate_capture(raw, plan, *, reader_sha256, key, state_volume, helper_hashes):
    value=decode(raw,1024*1024)
    require(type(value) is dict and set(value)=={'schemaVersion','protocol','containerId','stateVolume','network',
            'readOnlyRoot','readOnlyState','uid','helpers','observation','mac'} and raw==canonical(value)
            and type(value['schemaVersion']) is int and value['schemaVersion']==1
            and value['protocol']==CAPTURE_PROTOCOL and type(value['containerId']) is str
            and HEX.fullmatch(value['containerId']) and value['stateVolume']==state_volume
            and value['network']=='none' and value['readOnlyRoot'] is True and value['readOnlyState'] is True
            and type(value['uid']) is int and value['uid']==10001 and value['helpers']==helper_hashes
            and type(key) is bytes and len(key)==32 and type(value['mac']) is str and HEX.fullmatch(value['mac']),
            'legacy_capture_receipt_invalid')
    body={k:v for k,v in value.items() if k!='mac'}
    require(hmac.compare_digest(value['mac'],hmac.new(key,b'legacy-capture\0'+canonical(body),hashlib.sha256).hexdigest()),
            'legacy_capture_mac_changed')
    validate_artifact(canonical(value['observation']),plan,reader_sha256=reader_sha256)
    return value


def validate_capture_against_baseline(raw,plan,baseline,*,reader_sha256,key,state_volume,helper_hashes):
    value=validate_capture(raw,plan,reader_sha256=reader_sha256,key=key,state_volume=state_volume,helper_hashes=helper_hashes)
    validate_against_baseline(canonical(value['observation']),plan,baseline,reader_sha256=reader_sha256)
    return value
