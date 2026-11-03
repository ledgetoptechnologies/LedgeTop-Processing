import os
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from processing.host_mutation_journal import HostMutationJournal, MutationHeld
from processing.node_gate import GateBusy, GateError, NodeGate


class HostMutationJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        variable = 'LOCALAPPDATA' if os.name == 'nt' else 'XDG_STATE_HOME'
        self.env = patch.dict(os.environ, {variable: self.temp.name})
        self.env.start()
        self.engine, self.receipt = 'test-engine', 'a' * 32
        self.authority, self.job = str(uuid.uuid4()), str(uuid.uuid4())
        with NodeGate(self.engine) as gate:
            gate.provision('cluster', 'http://127.0.0.1:8080', self.authority)
            gate.reserve('cluster', 'http://127.0.0.1:8080', self.authority, 1, self.receipt)
            gate.bind(self.receipt, {'id': self.job, 'workerId': 'worker', 'nodeId': 'cluster', 'authorityId': self.authority})
            journal = HostMutationJournal(gate, self.receipt).create()
            self.init = str(uuid.uuid4())
            journal.begin(self.init, 'init', 'b' * 64)
            journal.acknowledge(self.init, 'c' * 64)
            journal.close()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_exact_replay_order_and_changed_identity(self):
        with NodeGate(self.engine) as gate:
            journal = HostMutationJournal(gate, self.receipt).open()
            self.assertEqual(journal.begin(self.init, 'init', 'b' * 64), 'c' * 64)
            with self.assertRaises(GateError): journal.begin(self.init, 'init', 'd' * 64)
            upload = str(uuid.uuid4())
            self.assertIsNone(journal.begin(upload, 'upload', 'e' * 64))
            with self.assertRaises(MutationHeld): journal.begin(upload, 'upload', 'e' * 64)
            with self.assertRaises(MutationHeld): journal.begin(str(uuid.uuid4()), 'commit', 'f' * 64)
            journal.acknowledge(upload, 'a' * 64)
            commit = str(uuid.uuid4())
            journal.begin(commit, 'commit', 'f' * 64)
            journal.acknowledge(commit, 'd' * 64)
            with self.assertRaises(GateError): journal.begin(str(uuid.uuid4()), 'upload', 'e' * 64)
            journal.close()

    def test_two_processes_cannot_hold_gate_and_crash_keeps_pending(self):
        script = '''import os,sys\nfrom processing.node_gate import NodeGate\nfrom processing.host_mutation_journal import HostMutationJournal\nwith NodeGate(sys.argv[1]) as g:\n j=HostMutationJournal(g,sys.argv[2]).open()\n j.begin(sys.argv[3],"upload","e"*64)\n os._exit(0)\n'''
        op = str(uuid.uuid4())
        with NodeGate(self.engine):
            child = subprocess.run([sys.executable, '-c', script, self.engine, self.receipt, op], capture_output=True, timeout=15)
            self.assertNotEqual(child.returncode, 0)
        child = subprocess.run([sys.executable, '-c', script, self.engine, self.receipt, op], capture_output=True, timeout=15)
        self.assertEqual(child.returncode, 0, child.stderr.decode(errors='replace'))
        with NodeGate(self.engine) as gate:
            journal = HostMutationJournal(gate, self.receipt).open()
            with self.assertRaises(MutationHeld): journal.begin(op, 'upload', 'e' * 64)
            self.assertIsNotNone(gate.snapshot()['reservation'])
            journal.close()

    def test_connections_serialize_checks_and_intent_insert(self):
        first_in_check = threading.Event()
        second_started = threading.Event()
        release = threading.Event()
        results = []
        with NodeGate(self.engine) as gate:
            def submit(first):
                journal = HostMutationJournal(gate, self.receipt).open()
                if first:
                    def trace(sql):
                        if sql.startswith('SELECT seq,op_id'):
                            first_in_check.set()
                            if not release.wait(5):
                                raise RuntimeError('test release timed out')
                    journal.db.set_trace_callback(trace)
                else:
                    journal.db.set_trace_callback(
                        lambda sql: second_started.set() if sql == 'BEGIN IMMEDIATE' else None)
                try:
                    journal.begin(str(uuid.uuid4()), 'upload', 'e' * 64)
                    results.append('started')
                except MutationHeld:
                    results.append('held')
                except BaseException as exc:
                    results.append(repr(exc))
                finally:
                    journal.close()
            first = threading.Thread(target=submit, args=(True,))
            second = threading.Thread(target=submit, args=(False,))
            first.start()
            self.assertTrue(first_in_check.wait(5))
            second.start()
            self.assertTrue(second_started.wait(5))
            release.set()
            first.join(10)
            second.join(10)
            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertCountEqual(results, ['started', 'held'])

    def test_marker_blocks_completion_and_operator_release(self):
        with NodeGate(self.engine) as gate:
            journal = HostMutationJournal(gate, self.receipt).open()
            journal.begin(str(uuid.uuid4()), 'upload', 'e' * 64)
            job = gate.snapshot()['reservation']['job']
            with self.assertRaises(GateError): gate.prepare_completion(self.receipt, job)
            with self.assertRaises(GateError): gate.acknowledge(self.receipt, job)
            with self.assertRaises(GateError): gate.release_empty(self.receipt, {'job': None})
            with self.assertRaises(GateError):
                gate.resolve(self.receipt, 1, old_worker_stopped=True,
                             coordinator_resolved=True, note='test')
            self.assertIsNotNone(gate.snapshot()['reservation'])
            journal.close()

    def test_missing_journal_is_not_recreated(self):
        with NodeGate(self.engine) as gate:
            journal = HostMutationJournal(gate, self.receipt)
            path = gate.root / gate.snapshot()['hostMutationJournal']['file']
            path.unlink()
            with self.assertRaises(GateError): journal.open()
            with self.assertRaises(GateError): journal.create()

    def test_process_death_after_ack_replays_recorded_ack(self):
        script = '''import os,sys\nfrom processing.node_gate import NodeGate\nfrom processing.host_mutation_journal import HostMutationJournal\nwith NodeGate(sys.argv[1]) as g:\n j=HostMutationJournal(g,sys.argv[2]).open()\n j.begin(sys.argv[3],"upload","e"*64)\n j.acknowledge(sys.argv[3],"f"*64)\n os._exit(0)\n'''
        op = str(uuid.uuid4())
        child = subprocess.run([sys.executable, '-c', script, self.engine, self.receipt, op], capture_output=True, timeout=15)
        self.assertEqual(child.returncode, 0, child.stderr.decode(errors='replace'))
        with NodeGate(self.engine) as gate:
            journal = HostMutationJournal(gate, self.receipt).open()
            self.assertEqual(journal.begin(op, 'upload', 'e' * 64), 'f' * 64)
            journal.close()


if __name__ == '__main__':
    unittest.main()
