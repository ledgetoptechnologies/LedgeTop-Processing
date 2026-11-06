"""Typed configuration review before cutover; never installation or readiness proof.

Only public enrollment metadata and preparation receipts are read. In particular,
this module must not open worker config, native credentials, archives or TLS keys.
The plan pins the reviewed bytes; no approval or runtime success is self-issued.
"""
import hashlib
import os
import stat
from pathlib import Path

from . import enroll_reference_worker as install
from .enrolled_coordinator_transport import canonical, decode, digest, private_bytes, require

PROTOCOL = 'ltds-worker-prestart-review-v1'
MAX_BYTES = 262144


def _closed(value, fields):
    require(type(value) is dict and set(value) == set(fields), 'closed_worker_prestart_review_required')


def _hash(value):
    install.sha(value)
    require(value != '0'*64, 'completed_worker_preparation_required')


def _read(descriptor):
    _closed(descriptor, ('path', 'sha256'))
    install.absolute(descriptor['path']); _hash(descriptor['sha256'])
    raw = private_bytes(descriptor['path'], maximum=65536)
    require(hashlib.sha256(raw).hexdigest() == descriptor['sha256'], 'worker_review_reference_changed')
    return decode(raw)


def _worker(entry, worker, authority):
    _closed(entry, ('nodeId', 'workerReferenceSha256', 'manifest', 'preparationReceipt', 'enrollment'))
    require(entry['nodeId'] == worker['nodeId'] and entry['workerReferenceSha256'] == digest(worker),
            'worker_review_bundle_binding_changed')
    manifest = install.validate_manifest(_read(entry['manifest']))
    for value in (manifest['config']['sha256'], manifest['source']['sha256'],
                  manifest['source']['footprintSha256'], manifest['source']['requirementsSha256'],
                  manifest['dependencies']['sha256'], manifest['dependencies']['pythonSha256'],
                  manifest['unitSha256'], manifest['gate']['sha256']): _hash(value)
    require(manifest['config']['bytes'] > 1, 'completed_worker_preparation_required')
    expected, settings = worker['expected'], worker['settings']
    root, home = Path(manifest['installationRoot']), Path(manifest['service']['home'])
    require(manifest['gate']['nodeId'] == worker['nodeId'] == expected['nodeId']
        and manifest['gate']['authorityId'] == expected['authorityId'] == authority
        and manifest['host']['dockerEngineId'] == expected['engineId']
        and manifest['gate']['generation'] == expected['gateGeneration']
        and manifest['engine']['version'] == expected['engineVersion']
        and manifest['service']['uid'] == worker['sshUid']
        and manifest['config']['sha256'] == expected['configSha256']
        and manifest['source']['footprintSha256'] == expected['sourceSha256'] == settings['sourceSha256']
        and manifest['unitSha256'] == settings['unitSha256']
        and settings['sourceRoot'] == str(root/'source')
        and settings['pythonExecutable'] == str(root/'venv/bin/python')
        and settings['configPath'] == str(home/'.config/ledgetop-processing/reference-worker.json')
        and settings['unitName'] == 'ledgetop-processing-reference-worker.service'
        and settings['unitFile'] == str(home/'.config/systemd/user/ledgetop-processing-reference-worker.service')
        and expected['activation'] is None, 'worker_review_enrollment_binding_changed')
    prepared = _read(entry['preparationReceipt'])
    wanted = dict(schemaVersion=1, protocol='ltds-first-worker-private-preparation-v1',
        workerConfigSha256=manifest['config']['sha256'], manifestSha256=digest(manifest),
        unitSha256=manifest['unitSha256'], preparationOnly=True)
    require(prepared == wanted and type(prepared.get('schemaVersion')) is int
            and type(prepared.get('preparationOnly')) is bool, 'worker_preparation_receipt_changed')
    enrollment = entry['enrollment']
    require(type(enrollment) is dict and enrollment.get('kind') in ('install-intent', 'enrolled-static'),
            'worker_enrollment_scope_required')
    fields = ('kind', 'operationId', 'manifestSha256')
    _closed(enrollment, fields + (('receipt',) if enrollment['kind'] == 'enrolled-static' else ()))
    require(enrollment['operationId'] == manifest['operationId'] and enrollment['manifestSha256'] == digest(manifest),
            'worker_enrollment_reference_changed')
    if enrollment['kind'] == 'enrolled-static':
        receipt = _read(enrollment['receipt'])
        fields = ('schemaVersion', 'protocol', 'operationId', 'manifestSha256', 'sourceSha256',
            'configSha256', 'unitSha256', 'unitInstalled', 'serviceStartIssued', 'serviceEnableIssued',
            'engineMutationIssued', 'installedTreeSchemaVersion', 'installedTreeSha256')
        _closed(receipt, fields); _hash(receipt['installedTreeSha256'])
        require(receipt == dict(schemaVersion=1, protocol=install.PROTOCOL, operationId=manifest['operationId'],
            manifestSha256=digest(manifest), sourceSha256=manifest['source']['footprintSha256'],
            configSha256=manifest['config']['sha256'], unitSha256=manifest['unitSha256'], unitInstalled=True,
            serviceStartIssued=False, serviceEnableIssued=False, engineMutationIssued=False,
            installedTreeSchemaVersion=1, installedTreeSha256=receipt['installedTreeSha256'])
            and all(type(receipt[k]) is bool for k in ('unitInstalled', 'serviceStartIssued',
                'serviceEnableIssued', 'engineMutationIssued'))
            and type(receipt['schemaVersion']) is int and type(receipt['installedTreeSchemaVersion']) is int,
            'historical_worker_enrollment_receipt_changed')


def compose(bundle, plan, entries):
    """Deterministic preparation only. Does not need a finalized deployment hash."""
    from .enrolled_coordinator_cutover import validate_plan
    from .enrolled_coordinator_deployment import worker as validate_worker
    plan = validate_plan(plan)
    require(bundle['operationId'] == plan['operationId'] and bundle['authorityId'] == plan['authorityId'],
            'worker_review_operation_changed')
    require(bundle['candidateSourceCommit'] == plan['candidate']['sourceCommit'], 'worker_review_candidate_changed')
    for key, value in plan['candidate'].items():
        if key != 'sourceCommit': _hash(value.removeprefix('sha256:'))
    workers = bundle['workers']
    require(type(workers) is list and len(workers) == 2 and type(entries) is list and len(entries) == 2
            and [w['nodeId'] for w in workers] == [n['id'] for n in bundle['nodeEndpoints']]
            and {w['nodeId'] for w in workers} == {'main', 'cluster'}, 'exact_worker_review_inventory_required')
    for entry, worker in zip(entries, workers):
        validate_worker(worker, plan['authorityId'])
        _worker(entry, worker, plan['authorityId'])
    require(len({w['expected']['engineId'] for w in workers}) == len(workers),
            'distinct_worker_physical_daemons_required')
    value = dict(schemaVersion=1, protocol=PROTOCOL, scope='configuration-review', operationId=plan['operationId'],
        authorityId=plan['authorityId'], candidate=plan['candidate'], workers=entries)
    raw = canonical(value); require(len(raw) <= MAX_BYTES, 'worker_review_budget_exceeded')
    return raw


def validate(raw, bundle, plan):
    require(type(raw) is bytes and len(raw) <= MAX_BYTES, 'bounded_worker_prestart_review_required')
    value = decode(raw)
    _closed(value, ('schemaVersion', 'protocol', 'scope', 'operationId', 'authorityId', 'candidate', 'workers'))
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1
            and value['protocol'] == PROTOCOL and value['scope'] == 'configuration-review',
            'worker_prestart_review_profile_required')
    require(raw == compose(bundle, plan, value['workers']), 'worker_prestart_review_binding_changed')
    return value


def verify(bundle, plan):
    """Pure gate used before acquiring the host operation/journal mutation locks."""
    descriptor = bundle['qualifications']['workerQualificationSha256']
    require(descriptor is not None, 'typed_worker_prestart_review_required')
    raw = private_bytes(descriptor['path'], maximum=MAX_BYTES)
    require(hashlib.sha256(raw).hexdigest() == descriptor['sha256'] == plan['gates']['workerQualificationSha256'],
            'worker_prestart_review_artifact_changed')
    return validate(raw, bundle, plan)


def prepare(spec):
    """Write one NEW private artifact; a partial outcome is retained, never reused."""
    require(os.name == 'posix', 'posix_worker_review_preparation_required')
    _closed(spec, ('bundlePath', 'planPath', 'entries', 'outputPath'))
    bundle = decode(private_bytes(spec['bundlePath'], maximum=262144))
    plan = decode(private_bytes(spec['planPath'], maximum=262144))
    raw = compose(bundle, plan, spec['entries'])
    target = install.absolute(spec['outputPath']); parent = install.directory_identity(target.parent)
    require(parent[3] == os.geteuid() and stat.S_IMODE(parent[2]) == 0o700,
            'private_worker_review_parent_required')
    directory = os.open(target.parent, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        opened = os.fstat(directory)
        require([opened.st_dev,opened.st_ino,opened.st_mode,opened.st_uid,opened.st_gid] == parent ==
                install.directory_identity(target.parent), 'worker_review_parent_changed')
        descriptor = os.open(target.name, os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC, 0o600, dir_fd=directory)
        try:
            with os.fdopen(descriptor, 'wb', closefd=False) as stream: stream.write(raw); stream.flush()
            os.fsync(descriptor); os.lseek(descriptor, 0, os.SEEK_SET)
            require(os.read(descriptor, len(raw)+1) == raw, 'worker_review_readback_changed')
            info = os.fstat(descriptor)
            require(info.st_nlink == 1 and stat.S_IMODE(info.st_mode) == 0o600,
                    'private_worker_review_file_required')
        finally: os.close(descriptor)
        os.fsync(directory)
        require(install.directory_identity(target.parent) == parent and
                (target.lstat().st_dev,target.lstat().st_ino) == (info.st_dev,info.st_ino),
                'worker_review_parent_changed')
        return dict(schemaVersion=1, protocol=PROTOCOL, scope='configuration-review',
                    sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    finally: os.close(directory)


def main():
    import argparse
    import json
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    try: print(json.dumps(prepare(decode(private_bytes(args.spec, maximum=262144)))))
    except Exception: raise SystemExit('Worker pre-start review held; inspect retained private preparation') from None


if __name__ == '__main__': main()
