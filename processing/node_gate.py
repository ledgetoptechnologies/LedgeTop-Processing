"""Host-local admission fence for cooperative processing workers.

Every Docker writer for a physical Engine must use this same OS-user gate.
This is not a sandbox against another user or a caller with direct Docker access.
The canonical path is independent of worker scratch/coordinator state. The OS
lock protects transactions; the persisted reservation survives crashes and never
expires. Only the exact acknowledged empty claim or completion frees it.

Provision explicitly before use: ``python -m processing.node_gate provision``.
Handover needs an idle reservation, current generation and stopped-old-worker
attestation. A new coordinator using a separate database has a separate authority
UUID and cannot use the old binding merely because its node slug matches.
"""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import urllib.parse
import uuid


SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z')
ENGINE = re.compile(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,255}\Z')
RECEIPT = re.compile(r'[0-9a-f]{32}\Z')


class GateError(Exception):
    """Safe failure category; never includes credentials or server responses."""


class GateBusy(GateError):
    """The OS lock is contended, distinct from invalid state or I/O failure."""


def _uuid(value):
    try:
        return isinstance(value, str) and str(uuid.UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _binding(node_id, origin, authority_id):
    if not isinstance(node_id, str) or not SLUG.fullmatch(node_id) or not _uuid(authority_id):
        raise GateError('invalid node or authority identity')
    if (not isinstance(origin, str) or not origin.isascii()
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127
                   for character in origin)):
        raise GateError('invalid coordinator origin')
    try:
        parsed = urllib.parse.urlsplit(origin)
        port = parsed.port
    except ValueError:
        raise GateError('invalid coordinator origin') from None
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment or port == 0):
        raise GateError('invalid coordinator origin')
    # Exact normalized spelling is intentional: aliases require explicit handover.
    return {'nodeId': node_id, 'coordinatorOrigin': origin.rstrip('/'),
            'authorityId': authority_id}


def canonical_gate_path(engine_id):
    """One gate per physical Engine and OS user, never per scratch directory."""
    if not isinstance(engine_id, str) or not ENGINE.fullmatch(engine_id):
        raise GateError('invalid Docker Engine identity')
    if os.name == 'nt':
        home = os.environ.get('LOCALAPPDATA')
        if not home:
            raise GateError('LOCALAPPDATA is required for the canonical node gate')
        root = Path(home)
    else:
        home = os.environ.get('XDG_STATE_HOME')
        root = Path(home) if home else Path.home() / '.local' / 'state'
    if not root.is_absolute():
        raise GateError('node gate state home must be absolute')
    return root / 'LedgeTop-Processing' / 'node-gates' / hashlib.sha256(engine_id.encode()).hexdigest()


def _digest(payload):
    if (not isinstance(payload, dict) or payload.get('status') not in ('succeeded', 'failed')
            or not isinstance(payload.get('result'), dict)):
        raise GateError('invalid completion payload')
    try:
        raw = json.dumps({'status': payload['status'], 'result': payload['result']},
                         sort_keys=True, allow_nan=False, separators=(',', ':')).encode()
    except (TypeError, ValueError, RecursionError):
        raise GateError('invalid completion payload') from None
    if len(raw) > 8192:
        raise GateError('completion exceeds gate limit')
    return hashlib.sha256(raw).hexdigest()


class NodeGate:
    def __init__(self, engine_id):
        self.engine_id = engine_id
        self.root = canonical_gate_path(engine_id)
        self._stream = None

    def __enter__(self):
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.is_symlink() or self.root.resolve() != self.root.absolute():
            raise GateError('canonical node gate path must be a real directory')
        if os.name != 'nt':
            self.root.chmod(0o700)
        lock = self.root / 'gate.lock'
        if lock.is_symlink():
            raise GateError('invalid node gate lock')
        # New canonical locks must satisfy the live holder's private-file
        # checks even on hosts whose normal umask permits group writes.
        # Existing lock inodes and their modes are never replaced here.
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_APPEND | getattr(os, 'O_BINARY', 0), 0o600)
        self._stream = os.fdopen(descriptor, 'a+b')
        try:
            if os.name == 'nt':
                import msvcrt
                self._stream.seek(0, os.SEEK_END)
                if self._stream.tell() == 0:
                    self._stream.write(b'0')
                    self._stream.flush()
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            self._stream.close()
            self._stream = None
            if error.errno in (errno.EACCES, errno.EAGAIN):
                raise GateBusy('another worker owns this physical node gate') from None
            raise GateError('physical node gate lock unavailable') from None
        return self

    def __exit__(self, *_):
        if self._stream:
            if os.name == 'nt':
                import msvcrt
                self._stream.seek(0)
                msvcrt.locking(self._stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            self._stream.close()
            self._stream = None

    def _check_lock(self):
        if self._stream is None:
            raise GateError('node gate must be used as a context manager')

    def _load(self):
        self._check_lock()
        path = self.root / 'gate.json'
        if not path.exists():
            if path.is_symlink():
                raise GateError('invalid node gate state')
            return None
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
                raise ValueError()
            state = json.loads(path.read_bytes())
            if (not isinstance(state, dict) or type(state.get('schema')) is not int
                    or state['schema'] not in (1, 2)
                    or (state['schema'] == 1 and 'hostMutationJournal' in state)
                    or state.get('engineId') != self.engine_id
                    or type(state.get('generation')) is not int or state['generation'] < 1
                    or not {'nodeId', 'coordinatorOrigin', 'authorityId', 'reservation'} <= state.keys()):
                raise ValueError()
            binding = _binding(state['nodeId'], state['coordinatorOrigin'], state['authorityId'])
            if any(state[key] != value for key, value in binding.items()):
                raise ValueError()
            reservation = state['reservation']
            if reservation is not None:
                if (not isinstance(reservation, dict)
                        or set(reservation) != {'receiptId', 'job', 'completionDigest'}
                        or not isinstance(reservation['receiptId'], str)
                        or not RECEIPT.fullmatch(reservation['receiptId'])):
                    raise ValueError()
                job = reservation['job']
                if job is not None:
                    if not isinstance(job, dict) or set(job) != {'id', 'workerId', 'nodeId', 'authorityId'}:
                        raise ValueError()
                    self._job(state, job)
                digest = reservation['completionDigest']
                if digest is not None and (job is None or not isinstance(digest, str)
                                          or not re.fullmatch('[0-9a-f]{64}', digest)):
                    raise ValueError()
            return state
        except Exception:
            raise GateError('node gate state requires operator inspection') from None

    def _save(self, state):
        self._check_lock()
        temporary = self.root / ('gate-' + uuid.uuid4().hex + '.tmp')
        with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as stream:
            json.dump(state, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.root / 'gate.json')
        if os.name != 'nt':
            descriptor = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def snapshot(self):
        state = self._load()
        if state is None:
            raise GateError('physical node gate has not been provisioned')
        return state

    def provision(self, node_id, origin, authority_id):
        binding = _binding(node_id, origin, authority_id)
        if self._load() is not None:
            raise GateError('node gate already provisioned; explicit idle handover required')
        state = {'schema': 1, 'engineId': self.engine_id, **binding,
                 'generation': 1, 'reservation': None}
        self._save(state)
        return state

    def handover(self, node_id, origin, authority_id, expected_generation, *, old_worker_stopped, note):
        binding = _binding(node_id, origin, authority_id)
        state = self.snapshot()
        self._journal_blocks_lifecycle(state)
        self._generation(state, expected_generation)
        if old_worker_stopped is not True or not isinstance(note, str) or not 1 <= len(note) <= 256:
            raise GateError('handover requires stopped-old-worker attestation and note')
        if state['reservation'] is not None:
            raise GateError('unresolved reservation blocks node ownership handover')
        state.update(binding, generation=state['generation'] + 1, handoverNote=note)
        self._save(state)
        return state

    @staticmethod
    def _generation(state, generation):
        if type(generation) is not int or generation != state['generation']:
            raise GateError('stale node ownership generation')

    def check_reservable(self, node_id, origin, authority_id, generation, receipt_id):
        """Validate reservation preconditions under this gate's held OS lock."""
        binding = _binding(node_id, origin, authority_id)
        state = self.snapshot()
        self._journal_blocks_lifecycle(state)
        self._generation(state, generation)
        if any(state[key] != value for key, value in binding.items()):
            raise GateError('coordinator does not own this physical node')
        if not isinstance(receipt_id, str) or not RECEIPT.fullmatch(receipt_id):
            raise GateError('invalid worker receipt identity')
        if state['reservation'] is not None:
            raise GateError('unresolved reservation blocks new work')
        return state

    def reserve(self, node_id, origin, authority_id, generation, receipt_id):
        state = self.check_reservable(node_id, origin, authority_id, generation, receipt_id)
        state['reservation'] = {'receiptId': receipt_id, 'job': None, 'completionDigest': None}
        self._save(state)
        return state['reservation']

    def _reservation(self, receipt_id):
        state = self.snapshot()
        if state['reservation'] is None or state['reservation']['receiptId'] != receipt_id:
            raise GateError('receipt does not own this node reservation')
        return state, state['reservation']

    @staticmethod
    def _job(state, job):
        if (not isinstance(job, dict) or not _uuid(job.get('id'))
                or not isinstance(job.get('workerId'), str) or not SLUG.fullmatch(job['workerId'])
                or job.get('nodeId') != state['nodeId']
                or job.get('authorityId') != state['authorityId']):
            raise GateError('claimed job does not match node ownership')
        return {key: job[key] for key in ('id', 'workerId', 'nodeId', 'authorityId')}

    def bind(self, receipt_id, job):
        state, reservation = self._reservation(receipt_id)
        identity = self._job(state, job)
        if reservation['job'] is not None and reservation['job'] != identity:
            raise GateError('reservation is already bound to another job')
        reservation['job'] = identity
        self._save(state)
        return reservation

    @staticmethod
    def _journal_blocks_lifecycle(state):
        if "hostMutationJournal" in state:
            raise GateError("host mutation journal requires reconciliation; lifecycle blocked")

    def prepare_completion(self, receipt_id, payload):
        state, reservation = self._reservation(receipt_id)
        self._journal_blocks_lifecycle(state)
        if reservation['job'] is None:
            raise GateError('completion requires a bound job')
        digest = _digest(payload)
        if reservation['completionDigest'] is not None and reservation['completionDigest'] != digest:
            raise GateError('completion differs from durable node result')
        reservation['completionDigest'] = digest
        self._save(state)

    def acknowledge(self, receipt_id, job_ack):
        state = self.snapshot()
        self._journal_blocks_lifecycle(state)
        identity, digest = self._job(state, job_ack), _digest(job_ack)
        tombstone = {'receiptId': receipt_id, 'job': identity, 'completionDigest': digest,
                     'generation': state['generation'], 'kind': 'completion'}
        if state['reservation'] is None and state.get('lastAcknowledgment') == tombstone:
            return
        state, reservation = self._reservation(receipt_id)
        if (reservation['job'] is None or reservation['completionDigest'] is None
                or identity != reservation['job'] or digest != reservation['completionDigest']):
            raise GateError('completion acknowledgment does not match durable result')
        state['reservation'] = None
        state['lastAcknowledgment'] = tombstone
        self._save(state)

    def completion_acknowledged(self, receipt_id, job_ack):
        """Read-only replay check even if another job now holds the node.

        A later acknowledgment may replace the single tombstone; in that case
        replay fails closed and the old worker must reconcile independently.
        """
        state = self.snapshot()
        identity, digest = self._job(state, job_ack), _digest(job_ack)
        expected = {'receiptId': receipt_id, 'job': identity, 'completionDigest': digest,
                    'generation': state['generation'], 'kind': 'completion'}
        return state.get('lastAcknowledgment') == expected

    def release_empty(self, receipt_id, response):
        state = self.snapshot()
        self._journal_blocks_lifecycle(state)
        tombstone = {'receiptId': receipt_id, 'generation': state['generation'], 'kind': 'empty'}
        if (state['reservation'] is None and state.get('lastAcknowledgment') == tombstone
                and response == {'job': None}):
            return
        state, reservation = self._reservation(receipt_id)
        if reservation['job'] is not None or response != {'job': None}:
            raise GateError('only an exact acknowledged empty claim frees an unbound reservation')
        state['reservation'] = None
        state['lastAcknowledgment'] = tombstone
        self._save(state)

    def resolve(self, receipt_id, expected_generation, *, old_worker_stopped,
                coordinator_resolved, note):
        """Operator-only abandonment after independent local AND remote checks.

        This records an abandonment, never a successful completion. Inspect the
        exact old process/container and reconcile its claim in the coordinator
        first, including uncertain claims whose job UUID was never received.
        The new generation fences stale worker invocations after recovery.
        """
        state, reservation = self._reservation(receipt_id)
        self._journal_blocks_lifecycle(state)
        self._generation(state, expected_generation)
        if (old_worker_stopped is not True or coordinator_resolved is not True
                or not isinstance(note, str) or not 1 <= len(note) <= 256):
            raise GateError('resolution requires stopped worker, reconciled coordinator and note')
        state['lastResolution'] = {'outcome': 'operator_abandoned',
                                   'generation': state['generation'],
                                   'reservation': reservation, 'note': note}
        state.update(reservation=None, generation=state['generation'] + 1)
        self._save(state)
        return state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine-id', required=True, help='Independently inspected local Docker Engine ID')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('status')
    resolve = commands.add_parser('resolve')
    resolve.add_argument('--receipt-id', required=True)
    resolve.add_argument('--expected-generation', required=True, type=int)
    resolve.add_argument('--old-worker-stopped', required=True, action='store_true')
    resolve.add_argument('--coordinator-resolved', required=True, action='store_true')
    resolve.add_argument('--note', required=True)
    for command in ('provision', 'handover'):
        sub = commands.add_parser(command)
        sub.add_argument('--node-id', required=True)
        sub.add_argument('--coordinator', required=True)
        sub.add_argument('--authority-id', required=True)
        if command == 'handover':
            sub.add_argument('--expected-generation', required=True, type=int)
            sub.add_argument('--old-worker-stopped', required=True, action='store_true')
            sub.add_argument('--note', required=True)
    args = parser.parse_args(argv)
    try:
        with NodeGate(args.engine_id) as gate:
            if args.command == 'status':
                state = gate.snapshot()
            elif args.command == 'provision':
                state = gate.provision(args.node_id, args.coordinator, args.authority_id)
            elif args.command == 'resolve':
                state = gate.resolve(args.receipt_id, args.expected_generation,
                                     old_worker_stopped=args.old_worker_stopped,
                                     coordinator_resolved=args.coordinator_resolved, note=args.note)
            else:
                state = gate.handover(args.node_id, args.coordinator, args.authority_id,
                                      args.expected_generation, old_worker_stopped=args.old_worker_stopped,
                                      note=args.note)
            print(json.dumps(state, sort_keys=True))
        return 0
    except (GateError, OSError):
        print('Node gate refused operation; inspect ownership and durable reservation.', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
