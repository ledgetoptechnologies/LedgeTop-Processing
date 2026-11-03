"""Compatibility checks against the actual 693cb7e gate implementation."""
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

from processing.host_mutation_journal import HostMutationJournal
from processing.node_gate import GateError, NodeGate

SPEC = importlib.util.spec_from_file_location(
    'legacy_gate_693cb7e', Path(__file__).parent / 'fixtures' / 'legacy_node_gate_693cb7e.py')
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


class DowngradeFenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'LOCALAPPDATA' if os.name == 'nt' else 'XDG_STATE_HOME': self.temp.name})
        self.env.start()
        self.authority = str(uuid.uuid4())
        self.receipt = 'a' * 32
        self.job = {'id': str(uuid.uuid4()), 'workerId': 'worker', 'nodeId': 'cluster', 'authorityId': self.authority}
        with NodeGate('engine') as gate:
            gate.provision('cluster', 'http://127.0.0.1:8080', self.authority)
            gate.reserve('cluster', 'http://127.0.0.1:8080', self.authority, 1, self.receipt)
            gate.bind(self.receipt, self.job)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def assert_legacy_refuses(self):
        with legacy.NodeGate('engine') as gate:
            for action in (lambda: gate.snapshot(),
                           lambda: gate.prepare_completion(self.receipt, self.job),
                           lambda: gate.acknowledge(self.receipt, self.job),
                           lambda: gate.reserve('cluster', 'http://127.0.0.1:8080', self.authority, 1, 'b' * 32)):
                with self.assertRaises(legacy.GateError):
                    action()

    def test_legacy_refuses_pending_marker_and_marker_missing_file(self):
        with NodeGate('engine') as gate:
            journal = HostMutationJournal(gate, self.receipt).create()
            journal.begin(str(uuid.uuid4()), 'init', 'b' * 64)
            journal.close()
            self.assertEqual(gate.snapshot()['schema'], 2)
        self.assert_legacy_refuses()
        with NodeGate('engine') as gate:
            (gate.root / gate.snapshot()['hostMutationJournal']['file']).unlink()
        self.assert_legacy_refuses()
        with NodeGate('engine') as gate:
            with self.assertRaises(GateError): HostMutationJournal(gate, self.receipt).open()

    def test_crash_before_marker_never_sends_mutation(self):
        with NodeGate('engine') as gate:
            with patch.object(gate, '_save', side_effect=OSError('simulated crash')):
                with self.assertRaises(OSError): HostMutationJournal(gate, self.receipt).create()
            state = gate.snapshot()
            self.assertEqual(state['schema'], 1)
            self.assertNotIn('hostMutationJournal', state)
            self.assertIsNotNone(state['reservation'])

    def test_crash_after_marker_before_file_holds_old_and_new_code(self):
        original_open = os.open
        def fail_journal(path, flags, *args, **kwargs):
            if Path(path).name.startswith('mutations-'):
                raise OSError('simulated crash after marker')
            return original_open(path, flags, *args, **kwargs)
        with NodeGate('engine') as gate:
            with patch('processing.host_mutation_journal.os.open', side_effect=fail_journal):
                with self.assertRaises(OSError): HostMutationJournal(gate, self.receipt).create()
            state = gate.snapshot()
            self.assertEqual(state['schema'], 2)
            self.assertFalse((gate.root / state['hostMutationJournal']['file']).exists())
            with self.assertRaises(GateError): gate.resolve(self.receipt, 1, old_worker_stopped=True,
                                                            coordinator_resolved=True, note='test')
        self.assert_legacy_refuses()

    def test_completed_journal_keeps_schema_fence(self):
        with NodeGate('engine') as gate:
            journal = HostMutationJournal(gate, self.receipt).create()
            for kind in ('init', 'commit'):
                op = str(uuid.uuid4())
                journal.begin(op, kind, 'b' * 64)
                journal.acknowledge(op, 'c' * 64)
            result = {**self.job, 'status': 'succeeded', 'result': {'outputHash': 'abc'}}
            journal.checkpoint_completion(engine_task=self.job['id'], outbox_id='d' * 64, payload=result)
            journal.acknowledge_completion(outbox_id='d' * 64, job_ack=result)
            journal.close()
            self.assertEqual(gate.snapshot()['schema'], 2)
            self.assertNotIn('hostMutationJournal', gate.snapshot())
        self.assert_legacy_refuses()

    def test_dangling_marker_blocks_reserve_and_handover(self):
        with NodeGate('engine') as gate:
            journal = HostMutationJournal(gate, self.receipt).create()
            journal.close()
            state = gate.snapshot()
            state['reservation'] = None  # emulate legacy/corrupt dangling marker
            gate._save(state)
            with self.assertRaises(GateError):
                gate.reserve('cluster', 'http://127.0.0.1:8080', self.authority, 1, 'b' * 32)
            with self.assertRaises(GateError):
                gate.handover('cluster', 'http://127.0.0.1:8080', self.authority, 1,
                              old_worker_stopped=True, note='test')
        self.assert_legacy_refuses()

    def test_unsafe_old_schema_marker_is_not_adopted(self):
        with NodeGate('engine') as gate:
            journal = HostMutationJournal(gate, self.receipt).create()
            journal.close()
            state = gate.snapshot()
            state['schema'] = 1
            gate._save(state)
            with self.assertRaises(GateError): gate.snapshot()


if __name__ == '__main__':
    unittest.main()
