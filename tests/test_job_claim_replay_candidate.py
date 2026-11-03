from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from processing.host_mutation_journal import MutationHeld
from processing.job_claim_ledger import JobClaimLedger
from processing.node_gate import GateError, NodeGate
from tests.test_bound_node_job_candidate import BoundJobTests


class JobClaimReplayTests(BoundJobTests):
    def test_exact_replay_held_and_changed_claim_rejected_without_reissue(self):
        env = self.envelope()
        self.proxy.initialize(self.task, job_claim=env, **self.auth)
        with self.assertRaises(MutationHeld):
            self.proxy.initialize(self.task, job_claim=env, **self.auth)
        replacement = self.envelope(nonce=env['claim']['nonce'], viewerGeneration=9)
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, job_claim=replacement, **self.auth)
        self.assertEqual(self.backend.counts()['initialize'], 1)

    def test_consumed_claim_cannot_be_reused_after_reservation_retired(self):
        env = self.envelope()
        self.proxy.initialize(self.task, job_claim=env, **self.auth)
        with NodeGate('physical-engine') as gate:
            state = gate.snapshot()
            state['reservation'] = None  # fixture only: simulate prior completed job
            gate._save(state)
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, job_claim=env, **self.auth)
        self.assertEqual(self.backend.counts()['initialize'], 1)

    def test_crash_after_consumption_before_reservation_stays_consumed(self):
        env = self.envelope()
        with patch.object(NodeGate, 'reserve', side_effect=RuntimeError('synthetic interruption')):
            with self.assertRaises(RuntimeError):
                self.proxy.initialize(self.task, job_claim=env, **self.auth)
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
            self.assertEqual(gate.snapshot()['schema'], 2)
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, job_claim=env, **self.auth)
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_missing_ledger_never_silently_recreated(self):
        env = self.envelope()
        with patch.object(NodeGate, 'reserve', side_effect=RuntimeError('synthetic interruption')):
            with self.assertRaises(RuntimeError):
                self.proxy.initialize(self.task, job_claim=env, **self.auth)
        (self.root / 'gate' / 'job-claim-ledger.sqlite').unlink()
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, job_claim=env, **self.auth)
        self.assertFalse((self.root / 'gate' / 'job-claim-ledger.sqlite').exists())
        self.assertEqual(self.backend.counts()['initialize'], 0)

    @unittest.skipIf(os.name == 'nt', 'POSIX independent process test')
    def test_competing_processes_consume_nonce_once(self):
        env = self.envelope()
        script = r'''
import json,sys,time
from pathlib import Path
from unittest.mock import patch
from processing.node_gate import NodeGate, GateError
from processing.job_claim_ledger import JobClaimLedger
claim=json.loads(sys.argv[2])
with patch('processing.node_gate.canonical_gate_path',return_value=Path(sys.argv[1])):
    for i in range(100):
        try:
            with NodeGate('physical-engine') as gate:
                JobClaimLedger(gate).consume(claim,sys.argv[3])
            print('consumed'); break
        except GateError as e:
            if e.__class__.__name__ == 'GateBusy': time.sleep(.01); continue
            print('held'); break
    else: print('timeout')
'''
        import json
        command = [sys.executable, '-c', script, str(self.root / 'gate'), json.dumps(env['claim'])]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(subprocess.run, command + [r * 32], capture_output=True, text=True, timeout=15)
                       for r in ('a', 'b')]
        results = [f.result() for f in futures]
        self.assertTrue(all(r.returncode == 0 for r in results), [r.stderr for r in results])
        self.assertEqual(sorted(r.stdout.strip() for r in results), ['consumed', 'held'])


if __name__ == '__main__': unittest.main()
