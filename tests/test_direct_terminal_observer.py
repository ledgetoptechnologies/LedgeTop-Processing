import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.direct_terminal_observer import DirectTerminalObserver, ObservationHeld
from processing import reference_protocol
from processing.node_gate import NodeGate
from processing.worker_outbox import WorkerOutbox, BINDING
from test_direct_outbox_contract import binding


class Engine:
    def __init__(self, task): self.task, self.code, self.calls = task, 40, []
    def read(self, path):
        self.calls.append(path)
        return {'uuid': self.task, 'status': {'code': self.code}}


class ObserverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name); self.binding = binding(); self.now = [100.0]
        self.gate_path = patch('processing.node_gate.canonical_gate_path', return_value=root / 'gate')
        self.gate_path.start(); self.addCleanup(self.gate_path.stop)
        self.gate = NodeGate(self.binding['engineId']); self.gate.__enter__(); self.addCleanup(self.gate.__exit__, None, None, None)
        b = self.binding
        self.gate.provision(b['nodeId'], b['coordinatorOrigin'], b['authorityId'])
        self.gate.reserve(b['nodeId'], b['coordinatorOrigin'], b['authorityId'], b['gateGeneration'], b['gateReservationId'])
        self.gate.bind(b['gateReservationId'], dict(id=b['jobId'], workerId=b['workerId'], nodeId=b['nodeId'], authorityId=b['authorityId']))
        self.spool = root / 'spool'; (self.spool / 'jobs' / b['gateReservationId']).mkdir(parents=True)
        self.outbox_path = root / 'outbox.sqlite3'; self.engine = Engine(b['engineTaskId'])
        self.make()

    def make(self):
        self.outbox = WorkerOutbox(self.outbox_path, clock=lambda:self.now[0], random_source=lambda:0)
        self.observer = DirectTerminalObserver(binding=self.binding, gate=self.gate, engine=self.engine,
            outbox=self.outbox, spool=self.spool, engine_version='3.6.2')

    @staticmethod
    def receipt(envelope):
        p = envelope['payload']
        return dict(type='durable-result-receipt', durable=True, receiptId='observation-receipt',
            storageReceiptId='observation-storage', outboxId=envelope['outboxId'],
            payloadSha256=envelope['payloadSha256'], acceptedObjectIds=['terminal-observation'],
            **{k:p[k] for k in BINDING})

    def test_disconnect_restart_retry_and_exact_observation_ack(self):
        identity = self.observer.observe()
        self.assertEqual(self.engine.calls, ['/task/' + self.binding['engineTaskId'] + '/info'])
        self.assertEqual(self.observer.deliver_once(lambda _: (_ for _ in ()).throw(OSError()), lambda *_: True), 'retry')
        self.make(); self.assertEqual(self.observer.observe(), identity)
        self.assertEqual(self.observer.deliver_once(lambda _: self.fail('not due'), lambda *_: True), 'idle')
        self.now[0] += 3; seen = []
        def send(envelope): seen.append(copy.deepcopy(envelope)); return self.receipt(envelope)
        self.assertEqual(self.observer.deliver_once(send, lambda receipt, delivery: True), 'acknowledged')
        self.assertEqual(self.observer.deliver_once(send, lambda *_: True), 'idle')
        self.assertEqual(seen[0]['payload']['resultManifest']['objects'][0]['role'], 'engine_terminal_observation')
        self.assertIsNotNone(self.gate.snapshot()['reservation'])
        self.assertTrue((self.spool / 'jobs' / self.binding['gateReservationId'] / 'terminal-observation.json').is_file())

    def test_invalid_receipt_and_changed_terminal_result_hold(self):
        identity = self.observer.observe()
        self.assertEqual(self.observer.deliver_once(lambda envelope:self.receipt(envelope), lambda *_:False), 'retry')
        self.assertEqual(self.outbox.get(identity)['state'], 'pending')
        self.engine.code = 30
        with self.assertRaises(ObservationHeld): self.observer.observe()
        self.assertIsNotNone(self.gate.snapshot()['reservation'])

    def test_running_and_unowned_gate_do_not_persist(self):
        self.engine.code = 20
        with self.assertRaises(ObservationHeld): self.observer.observe()
        self.binding['engineTaskId'] = '55555555-5555-4555-8555-555555555555'
        self.make()
        self.observer.binding['workerId'] = 'different'
        with self.assertRaises(ObservationHeld): self.observer.observe()

    def test_lost_ack_replays_same_observation_and_rejects_wrong_binding(self):
        identity = self.observer.observe(); received = []
        def lost(envelope):
            received.append(copy.deepcopy(envelope))
            raise OSError('lost reply')
        self.assertEqual(self.observer.deliver_once(lost, lambda *_: True), 'retry')
        self.make(); self.now[0] += 3
        def bad(envelope):
            received.append(copy.deepcopy(envelope)); value = self.receipt(envelope)
            value['payloadSha256'] = 'b' * 64
            return value
        self.assertEqual(self.observer.deliver_once(bad, lambda *_: True), 'retry')
        self.assertEqual(received[0], received[1]); self.now[0] += 5
        self.assertEqual(self.observer.deliver_once(self.receipt, lambda *_: True), 'acknowledged')
        self.assertEqual(self.outbox.get(identity)['state'], 'acknowledged')
        self.assertIsNotNone(self.gate.snapshot()['reservation'])

    def test_failed_and_cancelled_observations_are_not_success(self):
        self.engine.code = 30; identity = self.observer.observe()
        self.assertEqual(self.outbox.get(identity)['payload']['terminalState'], 'failed')

    def test_worker_loop_retries_during_engine_outage_and_keeps_gate(self):
        self.observer.observe()
        self.observer.deliver_once(lambda _: (_ for _ in ()).throw(OSError()), lambda *_:True)
        self.make(); self.now[0] += 3
        self.engine.read = lambda _: (_ for _ in ()).throw(OSError())
        class Stop:
            n = 0
            def is_set(self): return self.n > 0
            def wait(self, seconds): self.n += 1
        self.observer.run(Stop(), self.receipt, lambda *_:True, poll_seconds=1)
        self.assertEqual(self.outbox.claim_next(), None)
        self.assertIsNotNone(self.gate.snapshot()['reservation'])

    def test_gate_change_during_delivery_never_acknowledges(self):
        identity = self.observer.observe()
        def changed(envelope):
            self.observer.binding['workerId'] = 'different'
            return self.receipt(envelope)
        self.assertEqual(self.observer.deliver_once(changed, lambda *_:True), 'retry')
        self.assertEqual(self.outbox.get(identity)['state'], 'pending')
        self.assertIsNotNone(self.gate.snapshot()['reservation'])

    def test_delivery_never_claims_or_backs_off_another_job(self):
        first = self.observer.observe(); old = self.outbox.get(first)
        manifest = copy.deepcopy(old['payload']['resultManifest'])
        manifest['jobId'] = '66666666-6666-4666-8666-666666666666'
        manifest['resultManifestSha256'] = reference_protocol.digest({k:v for k,v in manifest.items() if k != 'resultManifestSha256'})
        other = self.outbox.persist_terminal(job_id=manifest['jobId'], generation=manifest['generation'],
            environment=manifest['environment'], input_manifest_sha256=old['payload']['inputManifestSha256'],
            pipeline_digest=manifest['pipelineDigest'], result_manifest_sha256=manifest['resultManifestSha256'],
            result_manifest=manifest, owned_prefix=old['local_metadata']['ownedPrefix'],
            local_objects=old['local_metadata']['localObjects'])
        with self.outbox._transaction() as db:
            db.execute('UPDATE completions SET next_at=0 WHERE id=?', (other,))
        self.assertEqual(self.observer.deliver_once(lambda _: (_ for _ in ()).throw(OSError()), lambda *_: True), 'retry')
        row = self.outbox.get(other)
        self.assertEqual((row['state'], row['attempts']), ('pending', 0))


if __name__ == '__main__': unittest.main()
