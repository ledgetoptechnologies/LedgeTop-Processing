"""Bounded spool observation for a spool-first coordinator, not a durability guarantee.

Run inside the candidate container, under its runtime UID, against the actual
read-write spool bind and exact final configuration. Only a unique probe file
created by this process is removed. Results are point-in-time observations;
the actual runtime must independently revalidate its spool on each operation.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
import time

from processing.source_spool import SourceSpool
from .enrolled_coordinator_transport import canonical, decode, digest, require

PROTOCOL = 'ltds-local-spool-observation-v1'
PROBE_BYTES = 4096


def _recipe_binding(config, recipe):
    settings = config['sourceSpool']
    require(type(recipe) is dict and type(recipe.get('imageId')) is str and
            re.fullmatch(r'sha256:[a-f0-9]{64}', recipe['imageId']) and
            type(recipe.get('mounts')) is list, 'reviewed_spool_recipe_required')
    target = PurePosixPath(settings['mountPath'])
    root = PurePosixPath(settings['root'])
    require(target.is_absolute() and root.is_absolute() and
            (root == target or target in root.parents), 'reviewed_spool_recipe_required')
    related = [m for m in recipe['mounts'] if type(m) is dict and type(m.get('target')) is str and
               (PurePosixPath(m['target']) == target or target in PurePosixPath(m['target']).parents or
                PurePosixPath(m['target']) in target.parents)]
    require(len(related) == 1 and related[0].get('target') == str(target) and
            related[0].get('type') == 'bind' and related[0].get('readOnly') is False and
            type(related[0].get('source')) is str and
            PurePosixPath(related[0]['source']).is_absolute(), 'exact_writable_spool_bind_required')
    return related[0]['source']


def observe(config_bytes, final_recipe, *, required_free_bytes=0):
    """Verify current identity, capacity and a durable round trip on the bind."""
    require(type(config_bytes) is bytes and 0 < len(config_bytes) <= 1024 * 1024,
            'bounded_spool_config_required')
    config = decode(config_bytes)
    require(type(config) is dict and type(config.get('sourceSpool')) is dict,
            'source_spool_config_required')
    settings = config['sourceSpool']
    required = ('root', 'mountPath', 'mountId', 'markerName', 'safetyFloorBytes',
                'maxObjectBytes', 'chunkBytes')
    require(set(settings) == set(required), 'closed_spool_config_required')
    bind_source = _recipe_binding(config, final_recipe)
    require(type(required_free_bytes) is int and 0 <= required_free_bytes <= 1024**4,
            'bounded_spool_reservation_required')
    spool = SourceSpool(settings['root'], mount_path=settings['mountPath'],
        mount_id=settings['mountId'], marker_name=settings['markerName'],
        safety_floor_bytes=settings['safetyFloorBytes'],
        max_object_bytes=settings['maxObjectBytes'], chunk_bytes=settings['chunkBytes'])
    spool.check_capacity(required_free_bytes + PROBE_BYTES)
    available = spool.available_bytes()
    root_info = os.stat(spool.root, follow_symlinks=False)
    mount_info = os.stat(spool.mount_path, follow_symlinks=False)
    marker_info = os.stat(spool.marker, follow_symlinks=False)
    marker = hashlib.sha256(settings['mountId'].encode('ascii')).hexdigest()
    suffix = secrets.token_hex(16)
    temporary = spool.root / ('.qualification-' + suffix + '.tmp')
    final = spool.root / ('.qualification-' + suffix + '.probe')
    data = secrets.token_bytes(PROBE_BYTES)
    fd = None
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        view = memoryview(data)
        while view:
            count = os.write(fd, view)
            require(count > 0, 'spool_probe_write_failed')
            view = view[count:]
        os.fsync(fd)
        os.close(fd); fd = None
        spool._assert_mount()
        # link is exclusive: an unrelated pre-existing name is never replaced.
        os.link(temporary, final, follow_symlinks=False)
        os.unlink(temporary)
        spool._fsync_directory(spool.root)
        read_fd = os.open(final, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(read_fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and
                    info.st_uid == os.geteuid() and info.st_size == PROBE_BYTES,
                    'spool_probe_identity_changed')
            actual = bytearray()
            while len(actual) <= PROBE_BYTES:
                chunk = os.read(read_fd, PROBE_BYTES + 1 - len(actual))
                if not chunk: break
                actual.extend(chunk)
            require(bytes(actual) == data, 'spool_probe_readback_failed')
        finally:
            os.close(read_fd)
        spool._assert_mount()
        os.unlink(final)
        spool._fsync_directory(spool.root)
        spool._assert_mount()
    finally:
        if fd is not None:
            os.close(fd)
        # Never attempt to delete a file whose ownership/identity is uncertain.
        # A failed probe may leave its randomly named task-owned file for review.
    return dict(schemaVersion=1, protocol=PROTOCOL,
        finalConfigSha256=hashlib.sha256(config_bytes).hexdigest(),
        finalRecipeSha256=digest(final_recipe), candidateImageId=final_recipe['imageId'],
        bindSource=bind_source, mountDevice=mount_info.st_dev, mountInode=mount_info.st_ino,
        root=settings['root'], mountPath=settings['mountPath'], markerName=settings['markerName'],
        markerSha256=marker, rootDevice=root_info.st_dev, rootInode=root_info.st_ino,
        markerDevice=marker_info.st_dev, markerInode=marker_info.st_ino,
        runtimeUid=os.geteuid(), safetyFloorBytes=settings['safetyFloorBytes'],
        requiredFreeBytes=required_free_bytes, availableBytes=available,
        probeBytes=PROBE_BYTES, observedUnixSeconds=int(time.time()))


def validate(raw, config_bytes, final_recipe, *, expected_uid=10001, max_age_seconds=86400, now=None):
    """Validate typed, config-bound evidence. Does not replace live revalidation."""
    record = decode(raw)
    keys = {'schemaVersion', 'protocol', 'finalConfigSha256', 'finalRecipeSha256',
        'candidateImageId', 'bindSource', 'mountDevice', 'mountInode', 'root', 'mountPath',
        'markerName', 'markerSha256', 'rootDevice', 'rootInode', 'markerDevice',
        'markerInode', 'runtimeUid', 'safetyFloorBytes', 'requiredFreeBytes',
        'availableBytes', 'probeBytes', 'observedUnixSeconds'}
    require(type(record) is dict and set(record) == keys and record['schemaVersion'] == 1 and
            type(record['schemaVersion']) is int and record['protocol'] == PROTOCOL,
            'typed_local_spool_observation_required')
    config = decode(config_bytes)
    require(type(config) is dict and type(config.get('sourceSpool')) is dict,
            'source_spool_config_required')
    settings = config['sourceSpool']
    bind_source = _recipe_binding(config, final_recipe)
    require(record['finalConfigSha256'] == hashlib.sha256(config_bytes).hexdigest() and
            record['finalRecipeSha256'] == digest(final_recipe) and
            record['candidateImageId'] == final_recipe['imageId'] and record['bindSource'] == bind_source and
            record['root'] == settings.get('root') and record['mountPath'] == settings.get('mountPath') and
            record['markerName'] == settings.get('markerName') and
            record['markerSha256'] == hashlib.sha256(settings['mountId'].encode('ascii')).hexdigest() and
            record['safetyFloorBytes'] == settings.get('safetyFloorBytes'), 'spool_observation_binding_changed')
    for name in ('rootDevice', 'rootInode', 'mountDevice', 'mountInode', 'markerDevice', 'markerInode', 'runtimeUid',
                 'safetyFloorBytes', 'requiredFreeBytes', 'availableBytes', 'observedUnixSeconds'):
        require(type(record[name]) is int and record[name] >= 0, 'invalid_spool_observation')
    verify_host_binding(record)
    verify_age(record, max_age_seconds=max_age_seconds, now=now)
    require(type(expected_uid) is int and expected_uid >= 0 and record['runtimeUid'] == expected_uid and
            record['rootDevice'] == record['markerDevice'] == record['mountDevice'] and
            record['rootInode'] > 0 and record['mountInode'] > 0 and
            record['markerInode'] > 0 and record['probeBytes'] == PROBE_BYTES and
            type(record['probeBytes']) is int and
            record['availableBytes'] >= record['safetyFloorBytes'] + record['requiredFreeBytes'] + PROBE_BYTES,
            'stale_or_insufficient_spool_observation')
    return record


def verify_age(record, *, max_age_seconds=86400, now=None):
    """A once-valid observation must not authorize a much later resume."""
    current = int(time.time()) if now is None else now
    require(type(record) is dict and type(record.get('observedUnixSeconds')) is int and
            type(max_age_seconds) is int and 0 < max_age_seconds <= 86400 and type(current) is int and
            0 <= current - record['observedUnixSeconds'] <= max_age_seconds,
            'stale_or_insufficient_spool_observation')


def verify_host_binding(record):
    """Recheck that the host path would bind the same filesystem object."""
    try:
        source = Path(record['bindSource'])
        source_info = source.lstat()
        require(not source.is_symlink() and source.resolve(strict=True) == source and
                stat.S_ISDIR(source_info.st_mode) and
                (source_info.st_dev, source_info.st_ino) == (record['mountDevice'], record['mountInode']),
                'reviewed_spool_host_bind_changed')
    except OSError:
        require(False, 'reviewed_spool_host_bind_changed')


def live_probe_source():
    """Read current spool identity inside the actual final container, without writes."""
    # docker exec invokes this with Python isolated mode (-I), which omits the
    # working directory from sys.path. The image installs the application at
    # /app, so explicitly add that fixed, image-owned package root before the
    # import. Without this, every final-container spool readiness check fails
    # with ModuleNotFoundError even though the application itself is healthy.
    return _live_probe_source('/app')


def _live_probe_source(package_root):
    require(type(package_root) is str and PurePosixPath(package_root).is_absolute() and
            PurePosixPath(package_root).as_posix() == package_root, 'invalid_live_probe_package_root')
    return (('import sys\nsys.path.insert(0, ' + json.dumps(package_root) + ')\n') + '''import hashlib,json,os
from pathlib import Path
from processing.source_spool import SourceSpool
raw=Path(os.environ['PROCESSING_REFERENCE_CONFIG']).read_bytes()
config=json.loads(raw);s=config['sourceSpool']
spool=SourceSpool(s['root'],mount_path=s['mountPath'],mount_id=s['mountId'],marker_name=s['markerName'],safety_floor_bytes=s['safetyFloorBytes'],max_object_bytes=s['maxObjectBytes'],chunk_bytes=s['chunkBytes'])
root=os.stat(spool.root,follow_symlinks=False);mount=os.stat(spool.mount_path,follow_symlinks=False);marker=os.stat(spool.marker,follow_symlinks=False)
print(json.dumps(dict(finalConfigSha256=hashlib.sha256(raw).hexdigest(),runtimeUid=os.geteuid(),rootDevice=root.st_dev,rootInode=root.st_ino,mountDevice=mount.st_dev,mountInode=mount.st_ino,markerDevice=marker.st_dev,markerInode=marker.st_ino,availableBytes=spool.available_bytes()),sort_keys=True,separators=(',',':')))
''').encode('utf-8')


def verify_live(record, live):
    """Fail closed if the final container sees different retained spool objects."""
    fields = ('finalConfigSha256', 'runtimeUid', 'rootDevice', 'rootInode',
              'mountDevice', 'mountInode', 'markerDevice', 'markerInode')
    verify_age(record)
    require(type(record) is dict and type(live) is dict and
            all(field in live and field in record and live[field] == record[field] for field in fields) and
            type(live.get('availableBytes')) is int and
            live['availableBytes'] >= record['safetyFloorBytes'] + record['requiredFreeBytes'],
            'live_final_spool_identity_or_capacity_changed')
    # Available space is rechecked on every read but omitted from the durable
    # observation, since ordinary unrelated writes can legitimately change it.
    return {field: live[field] for field in fields}
