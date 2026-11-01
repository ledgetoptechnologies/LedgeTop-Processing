"""Disabled direct-Viewer terminal observation bridge; never transfers artifacts.

This is not wired into runtime. The caller must own the physical node gate and
provide an authenticated coordinator transport and receipt verifier. An outbox
receipt covers only the tiny observation object, never engine output custody.
"""
import hashlib
import os
from pathlib import Path
import stat

from . import reference_protocol as protocol
from .direct_outbox_contract import validate_binding
from .worker_outbox import OutboxConflict, WorkerOutbox


class ObservationHeld(ValueError):
    """Unsafe or uncertain state; retain the engine task and gate."""


class DirectTerminalObserver:
    def __init__(self, *, binding, gate, engine, outbox, spool, engine_version):
        self.binding = dict(binding)
        self.binding_digest = validate_binding(self.binding)
        if not isinstance(outbox, WorkerOutbox):
            raise ValueError('durable outbox required')
        if not isinstance(engine_version, str) or not engine_version or len(engine_version) > 128:
            raise ValueError('engine version required')
        self.gate, self.engine, self.outbox = gate, engine, outbox
        self.spool, self.version = Path(spool), engine_version

    def _check_gate(self):
        if validate_binding(self.binding) != self.binding_digest:
            raise ObservationHeld('direct binding changed')
        state = self.gate.snapshot()
        b = self.binding
        for a, k in (('nodeId', 'nodeId'), ('coordinatorOrigin', 'coordinatorOrigin'),
                     ('authorityId', 'authorityId'), ('generation', 'gateGeneration'), ('engineId', 'engineId')):
            if state.get(a) != b[k]:
                raise ObservationHeld('node gate identity changed')
        reservation = state.get('reservation')
        expected_job = {'id': b['jobId'], 'workerId': b['workerId'], 'nodeId': b['nodeId'], 'authorityId': b['authorityId']}
        if not isinstance(reservation, dict) or reservation.get('receiptId') != b['gateReservationId'] or reservation.get('job') != expected_job:
            raise ObservationHeld('node reservation does not own job')

    def observe(self):
        self._check_gate()
        b = self.binding
        try:
            info = self.engine.read('/task/' + b['engineTaskId'] + '/info')
        except Exception:
            raise ObservationHeld('engine status unavailable') from None
        if not isinstance(info, dict) or info.get('uuid') != b['engineTaskId'] or not isinstance(info.get('status'), dict):
            raise ObservationHeld('engine identity unavailable')
        code = info['status'].get('code')
        if type(code) is not int or code not in (30, 40, 50):
            raise ObservationHeld('engine has not reached a known terminal state')
        self._check_gate()
        observation = protocol.canonical_json({'schemaVersion': 1, 'bindingSha256': self.binding_digest,
                                                'engineTaskId': b['engineTaskId'], 'statusCode': code})
        digest = hashlib.sha256(observation).hexdigest()
        prefix = 'jobs/' + b['gateReservationId']
        directory = self.spool / 'jobs' / b['gateReservationId']
        # The execution owner must provision this private directory; no inferred
        # directory creation, cleanup, or authorization occurs here.
        if any(p.is_symlink() for p in (self.spool, self.spool / 'jobs', directory)) or not directory.is_dir():
            raise ObservationHeld('private observation directory unavailable')
        path = directory / 'terminal-observation.json'
        try:
            if path.is_symlink():
                raise ObservationHeld('observation path is a symlink')
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            except FileExistsError:
                fd = os.open(path, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
                with os.fdopen(fd, 'r+b') as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or stream.read(len(observation) + 1) != observation:
                        raise ObservationHeld('existing observation differs')
                    os.fsync(stream.fileno())
            else:
                with os.fdopen(fd, 'wb') as stream:
                    stream.write(observation); stream.flush(); os.fsync(stream.fileno())
            if os.name != 'nt':
                fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try: os.fsync(fd)
                finally: os.close(fd)
        except OSError:
            raise ObservationHeld('observation persistence uncertain') from None
        self._check_gate()
        # Pipeline identity seals every direct binding field, not just the
        # original options digest; it is an observation identity only.
        manifest = dict(schemaVersion=1, jobId=b['jobId'], generation=b['viewerGeneration'],
                        environment=b['environment'], pipelineDigest=self.binding_digest,
                        engineVersions={'odm': self.version}, objects=[dict(id='terminal-observation',
                            role='engine_terminal_observation', byteSize=len(observation), sha256=digest)])
        manifest['resultManifestSha256'] = protocol.digest(manifest)
        return self.outbox.persist_terminal(job_id=b['jobId'], generation=b['viewerGeneration'],
            environment=b['environment'], input_manifest_sha256=b['inputSetSha256'], pipeline_digest=self.binding_digest,
            result_manifest_sha256=manifest['resultManifestSha256'], result_manifest=manifest,
            owned_prefix=prefix, local_objects={'terminal-observation': prefix + '/terminal-observation.json'},
            terminal_state={30: 'failed', 40: 'succeeded', 50: 'cancelled'}[code])

    def deliver_once(self, transport, verify_receipt):
        """Use caller-authenticated transport and verifier; never release the gate.

        Receipt accepts ONLY the observation object. It does not establish
        storage of the engine archive, Viewer acceptance, or cleanup permission.
        """
        self._check_gate()
        delivery = self.outbox.claim_for_binding(job_id=self.binding['jobId'], generation=self.binding['viewerGeneration'],
            environment=self.binding['environment'], pipeline_digest=self.binding_digest)
        if delivery is None:
            return 'idle'
        try:
            payload = delivery['payload']
            if (payload['jobId'] != self.binding['jobId'] or payload['generation'] != self.binding['viewerGeneration']
                    or payload['environment'] != self.binding['environment'] or payload['pipelineDigest'] != self.binding_digest):
                raise ObservationHeld('outbox contains a different job')
            receipt = transport({key: delivery[key] for key in ('outboxId', 'payloadSha256', 'payload')})
            if verify_receipt(receipt, delivery) is not True:
                raise ObservationHeld('coordinator receipt unverified')
            self._check_gate()
            self.outbox.acknowledge(delivery['outboxId'], receipt)
        except Exception:
            try: self.outbox.failed_attempt(delivery['outboxId'], delivery['claimToken'])
            except OutboxConflict: pass
            return 'retry'
        return 'acknowledged'

    def step(self, transport, verify_receipt):
        """Retry retained work first, even if the engine is temporarily offline."""
        outcome = self.deliver_once(transport, verify_receipt)
        if outcome != 'idle':
            return outcome
        try:
            self.observe()
        except ObservationHeld:
            return 'held'
        return self.deliver_once(transport, verify_receipt)

    def run(self, stop, transport, verify_receipt, *, poll_seconds=5):
        """Opt-in bounded polling loop; caller owns supervision and credentials."""
        if type(poll_seconds) not in (int, float) or not 1 <= poll_seconds <= 60:
            raise ValueError('invalid poll interval')
        while not stop.is_set():
            try:
                self.step(transport, verify_receipt)
            except Exception:
                # No mutation or cleanup follows uncertain ownership/state.
                pass
            stop.wait(poll_seconds)
