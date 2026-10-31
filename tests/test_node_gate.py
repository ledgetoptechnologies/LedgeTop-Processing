from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing.node_gate import GateError, NodeGate, canonical_gate_path


ENGINE = 'physical-docker-engine:one'
AUTHORITY = '12345678-1234-1234-1234-123456789abc'
STAGING = '87654321-1234-1234-1234-123456789abc'
ORIGIN = 'http://127.0.0.1:18080'
RECEIPT = 'a' * 32


class NodeGateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'gate'
        self.paths = patch('processing.node_gate.canonical_gate_path', return_value=self.root)
        self.paths.start()
        with NodeGate(ENGINE) as gate:
            gate.provision('cluster', ORIGIN, AUTHORITY)

    def tearDown(self):
        self.paths.stop()
        self.temp.cleanup()

    def reserve(self, gate, receipt=RECEIPT):
        return gate.reserve('cluster', ORIGIN, AUTHORITY, 1, receipt)

    @staticmethod
    def job():
        return {'id': '12345678-1234-1234-1234-123456789abd',
                'workerId': 'worker-one', 'nodeId': 'cluster', 'authorityId': AUTHORITY}

    @unittest.skipIf(os.name == 'nt', 'POSIX creation modes')
    def test_new_lock_stays_private_with_group_writable_host_umask(self):
        other = Path(self.temp.name) / 'permissive-umask'
        previous = os.umask(0o002)
        try:
            with patch('processing.node_gate.canonical_gate_path', return_value=other):
                with NodeGate(ENGINE) as gate:
                    gate.provision('cluster', ORIGIN, AUTHORITY)
                    self.assertEqual((other / 'gate.lock').stat().st_mode & 0o777, 0o600)
        finally:
            os.umask(previous)

    @unittest.skipIf(os.name == 'nt', 'POSIX existing inode modes')
    def test_existing_lock_mode_inode_and_state_are_not_silently_rewritten(self):
        lock = self.root / 'gate.lock'
        lock.chmod(0o664)
        before = lock.stat()
        state = (self.root / 'gate.json').read_bytes()
        with NodeGate(ENGINE) as gate:
            self.assertEqual(gate.snapshot()['generation'], 1)
        after = lock.stat()
        self.assertEqual((after.st_dev, after.st_ino), (before.st_dev, before.st_ino))
        self.assertEqual(after.st_mode & 0o777, 0o664)
        self.assertEqual((self.root / 'gate.json').read_bytes(), state)

    def test_explicit_provisioning_required_and_cannot_overwrite(self):
        with NodeGate(ENGINE) as gate:
            with self.assertRaises(GateError):
                gate.provision('cluster', ORIGIN, STAGING)
        other = Path(self.temp.name) / 'unprovisioned'
        with patch('processing.node_gate.canonical_gate_path', return_value=other):
            with NodeGate(ENGINE) as gate:
                with self.assertRaises(GateError):
                    gate.snapshot()

    def test_two_coordinators_and_node_alias_rejected_before_claim(self):
        with NodeGate(ENGINE) as gate:
            for node, origin, authority in [('cluster', ORIGIN, STAGING),
                    ('cluster', 'http://127.0.0.1:18081', AUTHORITY),
                    ('other', ORIGIN, AUTHORITY)]:
                with self.assertRaises(GateError):
                    gate.reserve(node, origin, authority, 1, RECEIPT)
            self.assertIsNone(gate.snapshot()['reservation'])

    def test_simultaneous_process_lock_blocks_other_worker(self):
        def attempt():
            with NodeGate(ENGINE):
                return True
        with NodeGate(ENGINE):
            with ThreadPoolExecutor(max_workers=1) as executor:
                with self.assertRaises(GateError):
                    executor.submit(attempt).result()

    def test_crash_guard_survives_lock_release_without_expiry(self):
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
        with NodeGate(ENGINE) as gate:
            self.assertEqual(gate.snapshot()['reservation']['receiptId'], RECEIPT)
            with self.assertRaises(GateError):
                self.reserve(gate, 'b' * 32)
            with self.assertRaises(GateError):
                gate.handover('cluster', ORIGIN, STAGING, 1,
                              old_worker_stopped=True, note='Confirmed old process stopped')

    def test_idle_handover_increments_fence_and_old_owner_is_rejected(self):
        with NodeGate(ENGINE) as gate:
            state = gate.handover('cluster', ORIGIN, STAGING, 1,
                                 old_worker_stopped=True, note='Isolated staging ownership')
            self.assertEqual(state['generation'], 2)
            with self.assertRaises(GateError):
                self.reserve(gate)
            with self.assertRaises(GateError):
                gate.reserve('cluster', ORIGIN, STAGING, 1, RECEIPT)
            gate.reserve('cluster', ORIGIN, STAGING, 2, RECEIPT)

    def test_handover_requires_attestation_note_and_current_generation(self):
        with NodeGate(ENGINE) as gate:
            for generation, stopped, note in [(0, True, 'done'), (1, False, 'done'), (1, True, '')]:
                with self.assertRaises(GateError):
                    gate.handover('cluster', ORIGIN, STAGING, generation,
                                  old_worker_stopped=stopped, note=note)
            self.assertEqual(gate.snapshot()['generation'], 1)

    def test_invalid_origins_and_noncanonical_authority_are_refused(self):
        with NodeGate(ENGINE) as gate:
            for origin in ['http://bad host', 'http://[', 'http://host:invalid',
                           'http://localhost/path', 'http://user:secret@host',
                           'http://localhost\n', 'http://localhost?token=secret']:
                with self.assertRaises(GateError):
                    gate.handover('cluster', origin, STAGING, 1,
                                  old_worker_stopped=True, note='test')
            with self.assertRaises(GateError):
                gate.handover('cluster', ORIGIN, STAGING.upper(), 1,
                              old_worker_stopped=True, note='test')

    def test_only_exact_empty_claim_releases_unbound_guard(self):
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
            for response in [{}, {'job': {}}, {'job': None, 'extra': 1}]:
                with self.assertRaises(GateError):
                    gate.release_empty(RECEIPT, response)
            with self.assertRaises(GateError):
                gate.release_empty('b' * 32, {'job': None})
            gate.release_empty(RECEIPT, {'job': None})
            self.assertIsNone(gate.snapshot()['reservation'])

    def test_bound_job_must_match_node_and_authority(self):
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
            for key, value in [('nodeId', 'staging'), ('authorityId', STAGING), ('id', 'invalid')]:
                with self.assertRaises(GateError):
                    gate.bind(RECEIPT, {**self.job(), key: value})
            gate.bind(RECEIPT, self.job())
            gate.bind(RECEIPT, self.job())
            with self.assertRaises(GateError):
                gate.bind(RECEIPT, {**self.job(), 'id': str(uuid.uuid4())})
            with self.assertRaises(GateError):
                gate.release_empty(RECEIPT, {'job': None})

    def test_completion_ack_requires_exact_persisted_result_and_identity(self):
        result = {'status': 'succeeded', 'result': {'outputHash': 'abc'}}
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
            gate.bind(RECEIPT, self.job())
            with self.assertRaises(GateError):
                gate.acknowledge(RECEIPT, {**self.job(), **result})
            gate.prepare_completion(RECEIPT, {**result, 'claimToken': 'not-stored'})
        self.assertNotIn('not-stored', (self.root / 'gate.json').read_text())
        with NodeGate(ENGINE) as gate:
            for delta in [{'status': 'failed'}, {'result': {'outputHash': 'different'}},
                          {'workerId': 'other'}, {'authorityId': STAGING}]:
                with self.assertRaises(GateError):
                    gate.acknowledge(RECEIPT, {**self.job(), **result, **delta})
            with self.assertRaises(GateError):
                gate.prepare_completion(RECEIPT, {'status': 'failed', 'result': {}})
            gate.acknowledge(RECEIPT, {**self.job(), **result})
            self.assertIsNone(gate.snapshot()['reservation'])

    def test_operator_abandonment_requires_exact_guard_and_both_checks(self):
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
            gate.bind(RECEIPT, self.job())
            for receipt, generation, stopped, reconciled, note in [
                    ('b' * 32, 1, True, True, 'done'),
                    (RECEIPT, 0, True, True, 'done'),
                    (RECEIPT, 1, False, True, 'done'),
                    (RECEIPT, 1, True, False, 'done'),
                    (RECEIPT, 1, True, True, '')]:
                with self.assertRaises(GateError):
                    gate.resolve(receipt, generation, old_worker_stopped=stopped,
                                 coordinator_resolved=reconciled, note=note)
            state = gate.resolve(RECEIPT, 1, old_worker_stopped=True,
                                 coordinator_resolved=True, note='Exact old container stopped; job resolved failed')
            self.assertEqual(state['generation'], 2)
            self.assertIsNone(state['reservation'])
            self.assertEqual(state['lastResolution']['outcome'], 'operator_abandoned')
            self.assertEqual(state['lastResolution']['reservation']['job'], self.job())
            with self.assertRaises(GateError):
                self.reserve(gate)
        with NodeGate(ENGINE) as gate:
            self.assertEqual(gate.snapshot()['lastResolution']['reservation']['receiptId'], RECEIPT)
            gate.reserve('cluster', ORIGIN, AUTHORITY, 2, 'b' * 32)

    def test_corrupt_state_or_engine_replacement_retains_guard(self):
        with NodeGate(ENGINE) as gate:
            self.reserve(gate)
        for change in [{'engineId': 'different-engine'}, {'generation': True},
                       {'reservation': {'receiptId': RECEIPT}}]:
            original = json.loads((self.root / 'gate.json').read_text())
            (self.root / 'gate.json').write_text(json.dumps({**original, **change}))
            with NodeGate(ENGINE) as gate:
                with self.assertRaises(GateError):
                    gate.snapshot()
            (self.root / 'gate.json').write_text(json.dumps(original))

    def test_no_methods_without_os_lock(self):
        with self.assertRaises(GateError):
            NodeGate(ENGINE).snapshot()

    def test_canonical_path_is_engine_specific_and_not_scratch_dependent(self):
        self.paths.stop()
        try:
            self.assertEqual(canonical_gate_path(ENGINE), canonical_gate_path(ENGINE))
            self.assertNotEqual(canonical_gate_path(ENGINE), canonical_gate_path('different-engine'))
            self.assertNotIn('scratch', str(canonical_gate_path(ENGINE)))
            with self.assertRaises(GateError):
                canonical_gate_path('../engine')
        finally:
            self.paths.start()

    @unittest.skipIf(os.name == 'nt', 'symlink creation requires Windows privilege')
    def test_symlink_state_refused(self):
        target = self.root / 'gate.json'
        saved = target.read_bytes()
        target.unlink()
        real = Path(self.temp.name) / 'elsewhere.json'
        real.write_bytes(saved)
        target.symlink_to(real)
        with NodeGate(ENGINE) as gate:
            with self.assertRaises(GateError):
                gate.snapshot()


if __name__ == '__main__':
    unittest.main()
