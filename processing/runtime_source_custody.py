"""Operator observation of the existing runtime, never custody admission.

This supplements independent process/FD/flock/marker and projection proofs. It
does not create sessions, touch source data, or grant any cutover authority.
"""
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid
from contextlib import contextmanager

from .maintenance import Conflict


@contextmanager
def _read_only_db(queue):
    # The caller holds the existing Queue lifecycle mutex throughout. mode=ro
    # cannot create a missing database, and query_only prohibits accidental SQL
    # writes even while the ordinary gate is open.
    db = sqlite3.connect(Path(queue.path).absolute().as_uri()+'?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        yield db
    finally:db.close()


def _small_proc(path):
    with open(path, 'rb') as stream:
        value = stream.read(8193)
    if len(value) > 8192:
        raise ValueError('bounded process metadata required')
    return value.decode('ascii')


def _process_start():
    value = _small_proc('/proc/self/stat')
    prefix, fields = value.rsplit(')', 1)
    if int(prefix.split(' ', 1)[0]) != os.getpid():
        raise ValueError('process identity changed')
    ticks = fields.split()[19]  # Linux stat field 22; remaining fields begin at 3.
    if not re.fullmatch(r'[1-9][0-9]{0,19}', ticks):
        raise ValueError('bounded process start required')
    return ticks


def _held_flock(descriptor):
    """Read the current descriptor's kernel lock metadata, never take a lock."""
    info = os.fstat(descriptor)
    matches = []
    for line in _small_proc('/proc/self/fdinfo/' + str(descriptor)).splitlines():
        fields = line.split()
        if fields[:1] != ['lock:']:
            continue
        if (len(fields) != 9 or fields[2:5] != ['FLOCK', 'ADVISORY', 'WRITE']
                or fields[5] != str(os.getpid()) or fields[7:] != ['0', 'EOF']):
            continue
        device = fields[6].split(':')
        if (len(device) == 3 and int(device[0], 16) == os.major(info.st_dev)
                and int(device[1], 16) == os.minor(info.st_dev) and int(device[2]) == info.st_ino):
            matches.append(line)
    if len(matches) != 1 or os.get_inheritable(descriptor):
        raise ValueError('existing original flock required')


def observe_runtime_source_custody(queue, runtime):
    result = dict(schemaVersion=1, protocol='ltds-runtime-source-custody-v1', status='unavailable',
        authorityId=None, startupId=None, processId=os.getpid(), processStartTicks=None,
        sourceReadersPresent=runtime is not None and getattr(runtime, '_source_readers', None) is not None,
        epoch=None, bindingSha256=None, sessionVerified=False, leaseVerified=False, journalMatches=False)
    try:
        if not sys.platform.startswith('linux') or queue is None or queue._creator_pid != os.getpid():
            return result
        result['processStartTicks'] = _process_start()
        from .reference_runtime import ReferenceRuntime
        from .reference_source_readers import SourceReaders, _json, _OWNERS, _OWNERS_LOCK
        from .coordinator_freeze import _wire
        gate = queue._coordinator_freeze
        gate._check()
        with queue._lifecycle_lock:
            if any(type(value) is not str or str(uuid.UUID(value)) != value
                   for value in (queue.authority_id, queue.startup_id)):
                return result
            binding = gate._binding()
            _held_flock(queue._lock.fileno())
            enrolled = getattr(queue, 'authority_lease', None)
            if enrolled is not None:
                enrolled.check(Path(queue.path).parent)
                _held_flock(enrolled._descriptor)
            with _read_only_db(queue) as db:
                authority = db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1 AND length(CAST(authority_id AS BLOB))=36').fetchone()
                epoch = db.execute('''SELECT binding_json,binding_sha256,state FROM coordinator_operation_epochs
                    WHERE epoch=? AND length(CAST(binding_json AS BLOB))<=65536
                    AND length(binding_sha256)=64 AND length(state)<=16''',
                    (queue.startup_id,)).fetchone()
                expected = _wire(binding)
                if (not authority or authority[0] != queue.authority_id or not epoch or epoch['state'] != 'active'
                        or epoch['binding_json'] != expected
                        or epoch['binding_sha256'] != hashlib.sha256(expected.encode('ascii')).hexdigest()):
                    return result
            result.update(authorityId=queue.authority_id, startupId=queue.startup_id)
            if runtime is None:
                result['status'] = 'absent'
                return result
            if type(runtime) is not ReferenceRuntime or runtime.queue is not queue or runtime.store.queue is not queue:
                return result
            readers = runtime._source_readers
            if readers is None:
                result['status'] = 'absent'
                return result
            if (type(readers) is not SourceReaders or readers.queue is not queue or readers.store is not runtime.store
                    or getattr(queue, '_source_reader_registry', None) is not readers):
                return result
            readers._process()  # Check creator PID before touching session mutex.
            with readers._mutex, _OWNERS_LOCK:
                if _OWNERS.get(str(readers.lock_path)) is not readers:
                    return result
                readers.check()
                _held_flock(readers._fd)
                serialized = _json(readers.binding)
                if not re.fullmatch(r'[a-f0-9]{64}', readers.epoch):
                    return result
                # Existing authority remains locked; only mode=ro connections
                # read bounded facts. No SourceReaders._db/reserve/constructor.
                with _read_only_db(queue) as db:
                    saved = db.execute('''SELECT binding,epoch FROM source_reader_binding WHERE id=1
                        AND length(CAST(binding AS BLOB))<=65536 AND length(epoch)=64''').fetchone()
                    state = db.execute('SELECT state FROM source_reader_epochs WHERE epoch=? AND length(state)<=16', (readers.epoch,)).fetchone()
                    if (not saved or saved['binding'] != serialized or saved['epoch'] != readers.epoch
                            or not state or state['state'] != 'active'):
                        return result
                readers.check()
                _held_flock(readers._fd)
                result.update(status='verified', epoch=readers.epoch,
                    bindingSha256=hashlib.sha256(serialized.encode('utf-8')).hexdigest(),
                    sessionVerified=True, leaseVerified=True, journalMatches=True)
                return result
    except (OSError, RuntimeError, ValueError, TypeError, AttributeError, KeyError, IndexError, sqlite3.Error, Conflict):
        return result

