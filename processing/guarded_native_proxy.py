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
                 coordinator_origin, generation, bearer, spool_root, job_claim_key=None, require_job_claim=False):
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
        if job_claim_key is not None and (not isinstance(job_claim_key, bytes) or not 32 <= len(job_claim_key) <= 4096
                or hmac.compare_digest(job_claim_key, bearer.encode())):
            raise ValueError('independent job claim key required')
        if type(require_job_claim) is not bool or (require_job_claim and job_claim_key is None):
            raise ValueError('job claim enforcement unavailable')
        self._job_claim_key = job_claim_key
        self._require_job_claim = require_job_claim

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

    def _authorize_operation(self, journal, task, op, kind, request, operation_claim):
        if operation_claim is None and not self._require_job_claim and not journal.has_viewer_job():
            return
        from .node_operation_claim import verify_operation_claim
        job = verify_operation_claim(operation_claim, self._job_claim_key, node=self.node_id,
            authority=self.authority_id, gate_generation=self.generation, task=task,
            origin=self.origin, operation_id=op, kind=kind, request_digest=protocol.digest(request))
        journal.verify_viewer_job(job)

    def preflight_upload(self, task, name, size, sha256, operation_claim, *, bearer, protected_transport):
        """Reject unauthorized transfers before allocating spool space."""
        self._auth(bearer, protected_transport)
        self._task(task)
        key = TaskStore._filename(name)
        op = self._op(task, 'upload', hashlib.sha256(key.encode('ascii')).hexdigest())
        request = dict(task=task, name=name, bytes=size, sha256=sha256)
        with NodeGate(self.engine_id) as gate:
            journal = HostMutationJournal(gate, self._owner(gate, task)).open()
            try: self._authorize_operation(journal, task, op, 'upload', request, operation_claim)
            finally: journal.close()

    @staticmethod
    def _mutate(journal, op, kind, request, action, validate=lambda _: True, before_action=lambda: None):
        digest = protocol.digest(request)
        # Even a completed retry is held: journal stores only an ack digest,
        # not the original response needed to prove a compatible replay.
        if journal.begin(op, kind, digest) is not None:
            raise MutationHeld('mutation already issued; inspect before retry')
        before_action()
        result = action()
        if not validate(result):
            raise MutationHeld('engine acknowledgment uncertain')
        journal.acknowledge(op, protocol.digest(dict(task=request['task'], kind=kind,
            requestDigest=digest, validation='native-engine-contract-passed')))
        return result

    def initialize(self, task, *, bearer, protected_transport, job_claim=None):
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
        verified = None
        if self._require_job_claim and job_claim is None:
            raise GateError('signed job claim required')
        if job_claim is not None:
            from .node_job_claim import verify_job_claim
            verified = verify_job_claim(job_claim, self._job_claim_key, node=self.node_id,
                authority=self.authority_id, gate_generation=self.generation, task=identifier, origin=self.origin)
            if verified['requestSha256'] != protocol.digest(task):
                raise GateError('job claim request differs')
        with NodeGate(self.engine_id) as gate:
            receipt = secrets.token_hex(16)
            if verified is not None:
                from .job_claim_ledger import JobClaimLedger
                ledger = JobClaimLedger(gate)
                # Replay inspection is read-only. A new claim must pass all
                # gate preconditions before it is consumed, otherwise a busy
                # node would permanently burn a valid scheduler claim.
                ledger.check_existing(verified)
                gate.check_reservable(self.node_id, self.origin, self.authority_id, self.generation, receipt)
                current_claim = verify_job_claim(job_claim, self._job_claim_key, node=self.node_id,
                    authority=self.authority_id, gate_generation=self.generation, task=identifier, origin=self.origin)
                if current_claim != verified or protocol.digest(task) != verified['requestSha256']:
                    raise GateError('job claim changed during admission')
                ledger.consume(verified, receipt)
            gate.reserve(self.node_id, self.origin, self.authority_id, self.generation, receipt)
            gate.bind(receipt, dict(id=identifier, workerId=self.worker_id,
                nodeId=self.node_id, authorityId=self.authority_id))
            journal = HostMutationJournal(gate, receipt).create()
            try:
                if verified is not None:
                    journal.bind_viewer_job(verified)
                request = dict(task=identifier, name=task['name'], outputs=task['outputs'], options=task['options'])
                def pre_issue():
                    if verified is None:
                        return
                    current_claim = verify_job_claim(job_claim, self._job_claim_key, node=self.node_id,
                        authority=self.authority_id, gate_generation=self.generation, task=identifier, origin=self.origin)
                    if current_claim != verified or protocol.digest(task) != verified['requestSha256']:
                        raise GateError('job claim changed before mutation')
                self._mutate(journal, self._op(identifier, 'init'), 'init', request,
                    lambda: self.engine.initialize(task), before_action=pre_issue)
            finally:
                journal.close()

    def upload(self, task, item, *, bearer, protected_transport, operation_claim=None):
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
                self._authorize_operation(journal, task, op, 'upload', request, operation_claim)
                if journal.contains_operation(op):
                    raise DuplicateGuardedUpload('prior upload intent already exists')
                try:
                    self._mutate(journal, op, 'upload', request, lambda: self.engine.upload(task, item),
                        before_action=lambda: self._authorize_operation(journal, task, op, 'upload', request, operation_claim))
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

    def commit(self, task, *, bearer, protected_transport, operation_claim=None):
        self._auth(bearer, protected_transport)
        self._task(task)
        with NodeGate(self.engine_id) as gate:
            journal = HostMutationJournal(gate, self._owner(gate, task)).open()
            try:
                op, request = self._op(task, 'commit'), dict(task=task)
                self._authorize_operation(journal, task, op, 'commit', request, operation_claim)
                return self._mutate(journal, op, 'commit', request,
                    lambda: self.engine.action('commit', task),
                    lambda value: type(value) is dict and value.get('uuid') == task
                        and 'error' not in value and value.get('success') is not False,
                    before_action=lambda: self._authorize_operation(journal, task, op, 'commit', request, operation_claim))
            finally:
                journal.close()

    def inspect_mutation(self, claim, *, bearer, protected_transport):
        """Return held-only node evidence, never permission to replay or resume.

        The currently provisioned gate job ID is the engine task UUID.  It is
        not a proven Viewer queue job ID; a different expected job fails closed.
        mTLS and the fixed bearer authenticate a node peer, not a job claim.
        """
        self._auth(bearer, protected_transport)
        required = {'jobId', 'nodeId', 'authorityId', 'generation', 'engineTask',
                    'operationId', 'kind', 'requestDigest', 'nonce'}
        if (type(claim) is not dict or set(claim) != required
                or type(claim['generation']) is not int or claim['generation'] < 1
                or type(claim['nonce']) is not str or not re.fullmatch(r'[0-9a-f]{64}', claim['nonce'])):
            raise GateError('invalid proof request')
        self._task(claim['engineTask'])
        if (claim['nodeId'], claim['authorityId'], claim['generation']) != (self.node_id, self.authority_id, self.generation):
            raise GateError('proof binding differs')
        with NodeGate(self.engine_id) as gate:
            receipt = self._owner(gate, claim['engineTask'])
            state = gate.snapshot()
            if state['reservation']['job']['id'] != claim['jobId']:
                raise GateError('proof job binding differs')
            journal = HostMutationJournal(gate, receipt).open()
            try:
                evidence = journal.inspect_operation(claim['operationId'], claim['kind'], claim['requestDigest'])
            finally:
                journal.close()
            return {'schema': 'ledgetop-node-mutation-proof-v1', 'claim': dict(claim),
                    'state': evidence['state'], 'ackDigest': evidence['ackDigest'],
                    'held': True}

    def inspect_bound_mutation(self, envelope, operation, *, bearer, protected_transport):
        """Verify durable Viewer-to-node mapping; return observation only."""
        self._auth(bearer, protected_transport)
        from .node_job_claim import verify_job_claim
        if (type(operation) is not dict or set(operation) != {'operationId', 'kind', 'requestDigest', 'nonce'}
                or type(operation['nonce']) is not str or not re.fullmatch(r'[0-9a-f]{64}', operation['nonce'])):
            raise GateError('invalid inspection operation')
        claim = envelope.get('claim') if isinstance(envelope, dict) else None
        task = claim.get('engineTaskId') if isinstance(claim, dict) else None
        self._task(task)
        verified = verify_job_claim(envelope, self._job_claim_key, node=self.node_id,
            authority=self.authority_id, gate_generation=self.generation, task=task, origin=self.origin, admission=False)
        with NodeGate(self.engine_id) as gate:
            receipt = self._owner(gate, task)
            journal = HostMutationJournal(gate, receipt).open()
            try:
                journal.verify_viewer_job(verified)
                evidence = journal.inspect_operation(operation['operationId'], operation['kind'], operation['requestDigest'])
            finally:
                journal.close()
        return {'schema': 'ledgetop-bound-node-observation-v1', 'claim': verified,
                'operation': dict(operation), 'state': evidence['state'],
                'ackDigest': evidence['ackDigest'], 'held': True}

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
