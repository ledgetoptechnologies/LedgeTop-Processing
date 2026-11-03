"""Disabled node-local mutation adapter, not an HTTP server or deployment.

Only a trusted server may call this with a transport verdict derived from its
own socket and an already-spooled, owner-private regular file. The native ODM
endpoint must be unreachable to untrusted clients; this module cannot enforce
network isolation. There is deliberately no cancellation or release path.
"""
import hashlib
import hmac
import os
from contextlib import ExitStack
from pathlib import Path
import re
import secrets
import stat
import uuid

from . import reference_protocol as protocol
from .host_mutation_journal import HostMutationJournal, MutationHeld
from .node_gate import GateBusy, GateError, NodeGate
from .viewer_tasks import TaskStore


class UnissuedGuardedUpload(MutationHeld):
    """This call is known not to have issued any engine mutation."""


class DuplicateGuardedUpload(UnissuedGuardedUpload):
    """This call did not issue a mutation; a durable prior intent owns it."""


class GuardedUploadGateBusy(UnissuedGuardedUpload):
    """This call failed while acquiring the gate, before journal access."""


class GuardedNativeProxy:
    def __init__(self, *, engine, engine_id, node_id, worker_id, authority_id,
                 coordinator_origin, generation, bearer, spool_root):
        if (type(bearer) is not str or not 16 <= len(bearer) <= 4096
                or not all(0x21 <= ord(c) <= 0x7e for c in bearer)):
            raise ValueError('private bearer required')
        root = Path(spool_root).absolute()
        if root.resolve() != root or any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError('canonical private spool required')
        mode = root.stat()
        if (not stat.S_ISDIR(mode.st_mode) or (os.name != 'nt'
                and (mode.st_uid != os.geteuid() or mode.st_mode & 0o077))):
            raise ValueError('owner-private spool required')
        self.engine, self.engine_id = engine, engine_id
        self.node_id, self.worker_id, self.authority_id = node_id, worker_id, authority_id
        self.origin, self.generation, self._bearer, self.root = coordinator_origin, generation, bearer, root

    def _auth(self, bearer, protected_transport):
        if (protected_transport is not True or type(bearer) is not str
                or not hmac.compare_digest(bearer.encode(), self._bearer.encode())):
            raise GateError('unauthorized node mutation')

    def _task(self, task):
        try:
            if type(task) is not str or str(uuid.UUID(task)) != task:
                raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise GateError('invalid engine task identity') from None

    def _owner(self, gate, task):
        state = gate.snapshot()
        if (state['nodeId'], state['authorityId'], state['coordinatorOrigin'], state['generation']) != (
                self.node_id, self.authority_id, self.origin, self.generation):
            raise GateError('node gate binding differs')
        reservation = state['reservation']
        expected = dict(id=task, workerId=self.worker_id, nodeId=self.node_id, authorityId=self.authority_id)
        if reservation is None or reservation['job'] != expected:
            raise GateError('engine task does not own node')
        return reservation['receiptId']

    @staticmethod
    def _op(task, kind, discriminator=''):
        return str(uuid.uuid5(uuid.UUID(task), kind + ':' + discriminator))

    @staticmethod
    def _mutate(journal, op, kind, request, action, validate=lambda _: True):
        digest = protocol.digest(request)
        # Even a completed retry is held: journal stores only an ack digest,
        # not the original response needed to prove a compatible replay.
        if journal.begin(op, kind, digest) is not None:
            raise MutationHeld('mutation already issued; inspect before retry')
        result = action()
        if not validate(result):
            raise MutationHeld('engine acknowledgment uncertain')
        journal.acknowledge(op, protocol.digest(dict(task=request['task'], kind=kind,
            requestDigest=digest, validation='native-engine-contract-passed')))
        return result

    def initialize(self, task, *, bearer, protected_transport):
        self._auth(bearer, protected_transport)
        if type(task) is not dict or set(task) != {'name', 'engine_uuid', 'outputs', 'options'}:
            raise GateError('invalid task')
        identifier = task['engine_uuid']
        self._task(identifier)
        name, options, outputs = task['name'], task['options'], task['outputs']
        if (type(name) is not str or len(name) > 256 or any(ord(c) < 32 for c in name)
                or type(options) is not list or len(options) > 1000
                or any(type(option) is not dict or set(option) != {'name', 'value'}
                       or type(option['name']) is not str
                       or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,119}', option['name'])
                       for option in options)
                or len({option['name'] for option in options}) != len(options)
                or (outputs is not None and (type(outputs) is not list or len(outputs) > 1000
                    or any(type(output) is not str or len(output) > 256 for output in outputs)))):
            raise GateError('invalid native task settings')
        # Validate the exact request before acquiring a reservation: JSON may
        # parse 1e999 as infinity even when NaN tokens are rejected.
        try:
            if len(protocol.canonical_json(task)) > 65536:
                raise ValueError()
        except (ValueError, protocol.ProtocolError):
            raise GateError('invalid native task settings') from None
        with NodeGate(self.engine_id) as gate:
            receipt = secrets.token_hex(16)
            gate.reserve(self.node_id, self.origin, self.authority_id, self.generation, receipt)
            gate.bind(receipt, dict(id=identifier, workerId=self.worker_id,
                nodeId=self.node_id, authorityId=self.authority_id))
            journal = HostMutationJournal(gate, receipt).create()
            try:
                request = dict(task=identifier, name=task['name'], outputs=task['outputs'], options=task['options'])
                self._mutate(journal, self._op(identifier, 'init'), 'init', request,
                    lambda: self.engine.initialize(task))
            finally:
                journal.close()

    def upload(self, task, item, *, bearer, protected_transport):
        self._auth(bearer, protected_transport)
        self._task(task)
        if (type(item) is not dict or set(item) != {'path','name','bytes','sha256'}
                or type(item['name']) is not str or not item['name']
                or type(item['bytes']) is not int or item['bytes'] <= 0
                or type(item['sha256']) is not str or len(item['sha256']) != 64
                or any(c not in '0123456789abcdef' for c in item['sha256'])):
            raise GateError('invalid spooled input')
        try:
            name_key = TaskStore._filename(item['name'])
        except ValueError:
            raise GateError('invalid spooled input name') from None
        path = Path(item['path']).absolute()
        if (path == self.root or self.root not in path.parents or path.resolve() != path
                or any(p.is_symlink() for p in (path, *path.parents) if p != self.root.parent)):
            raise GateError('spooled input escaped private root')
        file = path.stat()
        if (not stat.S_ISREG(file.st_mode) or file.st_size != item['bytes']
                or (os.name != 'nt' and (file.st_uid != os.geteuid() or file.st_mode & 0o077))):
            raise GateError('invalid private spooled input')
        request = dict(task=task, name=item['name'], bytes=item['bytes'], sha256=item['sha256'])
        with ExitStack() as stack:
            try:
                gate = stack.enter_context(NodeGate(self.engine_id))
            except GateBusy:
                # Catch ONLY acquisition failure. A GateBusy raised later by a
                # backend must remain ambiguous and preserve the spool.
                raise GuardedUploadGateBusy('node gate busy before upload') from None
            journal = HostMutationJournal(gate, self._owner(gate, task)).open()
            try:
                # Names are the native upload identities. A repeated name is
                # held even when byte metadata differs.
                op = self._op(task, 'upload', hashlib.sha256(name_key.encode('ascii')).hexdigest())
                if journal.contains_operation(op):
                    raise DuplicateGuardedUpload('prior upload intent already exists')
                try:
                    self._mutate(journal, op, 'upload', request, lambda: self.engine.upload(task, item))
                except UnissuedGuardedUpload:
                    # Once begin may have committed, no downstream exception
                    # can establish that this request did not mutate NodeODM.
                    raise MutationHeld('upload outcome requires inspection') from None
                request_digest = protocol.digest(request)
                ack_digest = protocol.digest(dict(task=task, kind='upload',
                    requestDigest=request_digest, validation='native-engine-contract-passed'))
                journal.require_done(op, request_digest, ack_digest)
                return {'task': task, 'name': item['name'], 'bytes': item['bytes'],
                        'sha256': item['sha256'], 'operationId': op,
                        'requestDigest': request_digest, 'ackDigest': ack_digest}
            finally:
                journal.close()

    def commit(self, task, *, bearer, protected_transport):
        self._auth(bearer, protected_transport)
        self._task(task)
        with NodeGate(self.engine_id) as gate:
            journal = HostMutationJournal(gate, self._owner(gate, task)).open()
            try:
                return self._mutate(journal, self._op(task, 'commit'), 'commit', dict(task=task),
                    lambda: self.engine.action('commit', task),
                    lambda value: type(value) is dict and value.get('uuid') == task
                        and 'error' not in value and value.get('success') is not False)
            finally:
                journal.close()

    def recover_acknowledged_spool(self, budget, name):
        """Reconcile one locally owned upload, never an uncertain mutation.

        No network route calls this method. External exclusion of old/direct
        writers and retention of the original source remain prerequisites.
        """
        if os.name == 'nt' or budget.root.absolute() != self.root or not budget._valid_name(name):
            raise GateError('invalid recovery spool')
        with NodeGate(self.engine_id) as gate:
            record = budget.upload_identity(name)
            if record is None:
                raise MutationHeld('spool identity missing; inspection required')
            size, stage, receipt, task, filename, sha256, device, inode = record
            if stage not in ('mutation_started', 'discardable') or self._owner(gate, task) != receipt:
                raise MutationHeld('spool ownership or stage uncertain')
            self._task(task)
            key = hashlib.sha256(TaskStore._filename(filename).encode('ascii')).hexdigest()
            request_digest = protocol.digest(dict(task=task, name=filename, bytes=size, sha256=sha256))
            ack_digest = protocol.digest(dict(task=task, kind='upload', requestDigest=request_digest,
                                              validation='native-engine-contract-passed'))
            journal = HostMutationJournal(gate, receipt).open()
            try:
                journal.require_done(self._op(task, 'upload', key), request_digest, ack_digest)
            finally:
                journal.close()
            root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                root, path = os.fstat(root_fd), os.stat(self.root, follow_symlinks=False)
                if (root.st_dev, root.st_ino) != (path.st_dev, path.st_ino):
                    raise MutationHeld('spool directory changed')
                try:
                    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root_fd)
                except FileNotFoundError:
                    if stage != 'discardable':
                        raise MutationHeld('spool missing before discard') from None
                else:
                    try:
                        info = os.fstat(fd)
                        if ((info.st_dev, info.st_ino) != (device, inode) or not stat.S_ISREG(info.st_mode)
                                or info.st_nlink != 1 or info.st_uid != os.geteuid()
                                or info.st_mode & 0o077 or info.st_size != size):
                            raise MutationHeld('spool identity changed')
                        digest = hashlib.sha256()
                        while True:
                            block = os.read(fd, 1024 * 1024)
                            if not block: break
                            digest.update(block)
                        current = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
                        if (current.st_dev, current.st_ino) != (device, inode) or digest.hexdigest() != sha256:
                            raise MutationHeld('spool contents changed')
                        if stage == 'mutation_started':
                            budget.transition(name, stage, 'discardable')
                        os.unlink(name, dir_fd=root_fd)
                        os.fsync(root_fd)
                    finally:
                        os.close(fd)
                # Make a previous unlink durable before releasing accounting.
                os.fsync(root_fd)
                budget.release_discarded(name, root_fd)
            finally:
                os.close(root_fd)
