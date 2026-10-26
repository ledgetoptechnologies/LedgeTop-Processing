"""Actual HTTP + SQLite restart/outbox tests, with no engine execution."""
import importlib.util
import json
import contextlib
import io
import os
from pathlib import Path
import tempfile
import threading
from http.server import ThreadingHTTPServer
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen
import uuid

from processing.app import make_handler
from processing.config import Config
from processing.node_gate import NodeGate, GateError
from tests.test_service import TOKEN
from tests.test_prototype import submission
from scripts.orchestrator_shadow import main as shadow_main

SPEC = importlib.util.spec_from_file_location('resilience_worker', Path(__file__).resolve().parents[1] /
                                           'engines/treedar/scripts/prototype_worker.py')
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)


class ResilienceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.server = None
        self.handler = None
        self.port = 0
        self.start()

    def start(self):
        self.handler = make_handler(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=str(self.root / 'queue')))
        self.server = ThreadingHTTPServer(('127.0.0.1', self.port), self.handler)
        self.port = self.server.server_port
        self.base = f'http://127.0.0.1:{self.port}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.handler.prototype_queue.close()
        self.server = None

    def tearDown(self):
        if self.server:
            self.stop()
        self.temp.cleanup()

    def request(self, route, payload):
        request = Request(self.base + route, data=json.dumps(payload).encode(),
            headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    def exercise_restart(self, commit_before_reply_loss):
        self.request('/v1/prototype/jobs', submission('resilience'))
        job = self.request('/v1/prototype/claim', {'workerId': 'worker'})['job']
        self.request('/v1/prototype/jobs', submission('next'))
        receipt_path = self.root / (uuid.uuid4().hex + '.json')
        artifact = self.root / 'retained-result.bin'
        artifact.write_bytes(b'synthetic artifact retained until separately transferred')
        receipt = {'state': 'completion_pending', 'job': job, 'coordinator': self.base,
                   'workerId': 'worker', 'nodeId': 'cluster',
                   'payload': {'claimToken': job['claimToken'], 'status': 'succeeded',
                               'result': {'jobId': job['id'], 'nodeId': 'cluster', 'trees': 3}}}
        worker.save(receipt_path, receipt)
        real_post = worker.post
        attempts = []
        now = [1000.0]

        def post(base, token, route, payload):
            attempts.append(json.loads(json.dumps(payload)))
            if len(attempts) == 1:
                if commit_before_reply_loss:
                    real_post(base, token, route, payload)
                self.stop()
                self.start()
                if not commit_before_reply_loss:
                    self.assertEqual(self.handler.prototype_queue.get(job['id'])['heldReason'], 'coordinator_restart')
                    self.assertIsNone(self.handler.prototype_queue.claim({'workerId': 'competitor'}))
                raise worker.RetryableDelivery('reply lost')
            return real_post(base, token, route, payload)

        def sleep(delay):
            now[0] += delay

        with patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate'):
            with NodeGate('synthetic-engine') as gate:
                gate.provision('cluster', self.base, job['authorityId'])
                gate.reserve('cluster', self.base, job['authorityId'], 1, receipt_path.stem)
                gate.bind(receipt_path.stem, job)
                gate.prepare_completion(receipt_path.stem, receipt['payload'])
                with patch.object(worker, 'post', side_effect=post), patch.object(worker, 'execute') as execute:
                    worker.complete(receipt_path, receipt, TOKEN, 10, clock=lambda: now[0],
                                    monotonic=lambda: now[0], sleep=sleep, jitter=lambda *_: 1)
                    execute.assert_not_called()
                gate.acknowledge(receipt_path.stem, receipt['acknowledgment'])
                self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0], attempts[1])
        self.assertTrue(artifact.exists())
        self.assertEqual(json.loads(receipt_path.read_text())['state'], 'completed')
        self.assertEqual(self.handler.prototype_queue.get(job['id'])['status'], 'succeeded')
        self.assertIsNotNone(self.handler.prototype_queue.claim({'workerId': 'next-worker'}))

    def test_outage_restart_original_result_reconciles_without_rerun(self):
        self.exercise_restart(False)

    def test_commit_lost_ack_restart_replay_is_exactly_idempotent(self):
        self.exercise_restart(True)

    def test_independent_staging_authority_cannot_reserve_production_node(self):
        production = self.handler.prototype_queue.authority_id
        staging_handler = make_handler(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=str(self.root / 'staging-queue')))
        try:
            staging = staging_handler.prototype_queue.authority_id
            with patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate'):
                with NodeGate('shared-physical-engine') as gate:
                    gate.provision('cluster', self.base, production)
                    with self.assertRaises(GateError):
                        gate.reserve('cluster', self.base, staging, 1, uuid.uuid4().hex)
                    gate.reserve('cluster', self.base, production, 1, uuid.uuid4().hex)
                with NodeGate('shared-physical-engine') as gate:
                    with self.assertRaises(GateError):
                        gate.handover('cluster', 'http://127.0.0.1:8890', staging, 1,
                                      old_worker_stopped=True, note='cannot steal busy node')
        finally:
            staging_handler.prototype_queue.close()

    def test_shadow_observer_proposes_production_without_assigning_work(self):
        self.stop()
        observer = 'isolated-observer-token-1234567890123456789'
        self.handler = make_handler(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=str(self.root / 'queue'), prototype_observer_token=observer))
        self.server = ThreadingHTTPServer(('127.0.0.1', self.port), self.handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.request('/v1/prototype/jobs', submission('staging-first'))
        production = self.request('/v1/prototype/jobs', submission('production-second', 'production'))['job']
        before = self.handler.prototype_queue.list()
        output = io.StringIO()
        with patch('sys.argv', ['orchestrator_shadow.py', '--coordinator', self.base]), \
                patch.dict(os.environ, {'PROCESSING_OBSERVER_TOKEN': observer}), \
                contextlib.redirect_stdout(output):
            self.assertEqual(shadow_main(), 0)
        result = json.loads(output.getvalue())
        self.assertFalse(result['executesWork'])
        self.assertEqual(result['proposedJobId'], production['id'])
        self.assertEqual(before, self.handler.prototype_queue.list())


if __name__ == '__main__':
    unittest.main()
