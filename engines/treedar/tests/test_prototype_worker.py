"""Stdlib-only safety and recovery checks; no engine or daemon required."""

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    'prototype_worker', Path(__file__).resolve().parents[1] / 'scripts/prototype_worker.py')
worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(worker)
RESOLVE_DOCKER_ENDPOINT = worker.resolve_docker_endpoint
IMAGE = 'sha256:' + 'a' * 64
ENGINE = 'test-local-docker-engine'
AUTHORITY = '12345678-1234-4234-8234-123456789abd'
JOB = {'id': '12345678-1234-4234-8234-123456789abc', 'environment': 'staging',
       'profile': worker.PROFILE, 'claimToken': 'secret_' + 'a' * 36, 'nodeId': 'cluster',
       'workerId': 'test-worker', 'authorityId': AUTHORITY}


def result(stdout='', returncode=0):
    return subprocess.CompletedProcess([], returncode, stdout, '')


def runtime_docker(args, timeout=20):
    return result(ENGINE if args[0] == 'info' else IMAGE)


def acknowledgment(receipt):
    return {'job': {**receipt['job'], 'workerId': receipt['workerId'],
                    'status': receipt['payload']['status'], 'result': receipt['payload']['result']}}


class FakeClock:
    def __init__(self):
        self.now = 1_000_000.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.gate_temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.gate_temp.cleanup)
        patches = [patch('processing.node_gate.canonical_gate_path',
                         return_value=Path(self.gate_temp.name) / 'gate'),
                   patch.object(worker, 'resolve_docker_endpoint', return_value='unix:///var/run/docker.sock'),
                   patch.object(worker, 'authority', return_value=AUTHORITY)]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        with worker.NodeGate(ENGINE) as gate:
            gate.provision('cluster', 'http://127.0.0.1:8899', AUTHORITY)

    def args(self, root, **changes):
        values = dict(coordinator='http://127.0.0.1:8899', verified_origin=None,
                      image=IMAGE, scratch=str(root), worker_id='test-worker',
                      timeout=240, resume=None, docker_engine_id=ENGINE, node_generation=1)
        values.update(changes)
        return argparse.Namespace(**values)

    def test_returned_job_cannot_select_paths_profile_or_environment(self):
        for key, value in [('id', '../../escape'), ('id', 'not-a-uuid'),
                           ('environment', '../production'), ('environment', 'dev'),
                           ('profile', 'shell'), ('claimToken', '\r\nX: evil'),
                           ('nodeId', 'main'), ('nodeId', '../cluster'), ('nodeId', 'clustér')]:
            with self.subTest(key=key, value=value), self.assertRaises(worker.WorkerError):
                worker.validate_job({**JOB, key: value})
        self.assertEqual(worker.validate_job(JOB), JOB)

    def test_wrong_node_claim_is_retained_without_engine_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            wrong = {**JOB, 'nodeId': 'main'}
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', return_value={'job': wrong}) as post, \
                    patch.object(worker, 'execute') as execute, \
                    self.assertRaises(worker.WorkerError):
                worker.run(self.args(root))
            execute.assert_not_called()
            self.assertEqual(post.call_args.args[3], {'workerId': 'test-worker', 'nodeId': 'cluster'})
            receipt = json.loads(next((root / '.prototype-receipts').glob('*.json')).read_text())
            self.assertEqual(receipt['state'], 'claimed')
            self.assertEqual(receipt['job']['nodeId'], 'main')

    def test_explicit_node_identity_reaches_claim_and_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            posts = []
            with worker.NodeGate(ENGINE) as gate:
                gate.handover('main', 'http://127.0.0.1:8899', AUTHORITY, 1,
                              old_worker_stopped=True, note='Test-only isolated node mapping')

            def post(base, token, route, payload):
                posts.append((route, payload))
                return {'job': {**JOB, 'nodeId': 'main', 'workerId': 'test-worker',
                                'status': payload.get('status'), 'result': payload.get('result')}}

            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', side_effect=post), \
                    patch.object(worker, 'execute', return_value=('succeeded', {'n_trees': 3})), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(worker.run(self.args(root, node_id='main', node_generation=2)), 0)
            self.assertEqual(posts[0][1], {'workerId': 'test-worker', 'nodeId': 'main'})
            self.assertEqual(posts[-1][1]['result']['nodeId'], 'main')

    def test_invalid_node_id_refused_before_claim(self):
        for node in ('../cluster', 'clustér', '', 'cluster/other'):
            with self.subTest(node=node), tempfile.TemporaryDirectory() as directory, \
                    patch.object(worker, 'post') as post, self.assertRaises(worker.WorkerError):
                worker.run(self.args(Path(directory), node_id=node))
            post.assert_not_called()

    def test_verified_origin_and_redirect_policy(self):
        self.assertEqual(worker.origin('http://localhost:8899/'), 'http://localhost:8899')
        for value in ['http://localhost:8899/path', 'http://user:pass@localhost',
                      'file:///tmp/a', 'http://cluster:8899', 'http://localhost/?x=1']:
            with self.subTest(value=value), self.assertRaises(worker.WorkerError):
                worker.origin(value)
        self.assertEqual(worker.origin('http://cluster:8899', 'http://cluster:8899'),
                         'http://cluster:8899')
        with self.assertRaises(worker.WorkerError):
            worker.NoRedirect().redirect_request(None, None, 302, '', {}, 'http://evil')

    def test_ambiguous_claim_is_not_repeated_even_next_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', side_effect=worker.WorkerError('uncertain')) as post:
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
            self.assertEqual(post.call_count, 1)
            receipt = json.loads(next((root / '.prototype-receipts').glob('*.json')).read_text())
            self.assertEqual(receipt['state'], 'claim_pending')

    def test_completion_failure_retains_exact_payload_and_resume_never_claims(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            posts = []

            def post(base, token, route, payload):
                posts.append((route, payload))
                if route.endswith('/claim'):
                    return {'job': JOB}
                raise worker.RetryableDelivery('uncertain completion')

            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', side_effect=post), \
                    patch.object(worker, 'execute', return_value=('succeeded', {'n_trees': 3})):
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
                receipt_path = next((root / '.prototype-receipts').glob('*.json'))
                receipt = json.loads(receipt_path.read_text())
                self.assertEqual(receipt['state'], 'completion_pending')
                self.assertEqual(receipt['payload'], posts[-1][1])
                with patch.object(worker, 'post', return_value=acknowledgment(receipt)) as resume, \
                        patch.object(worker.time, 'time', return_value=receipt['delivery']['nextAttemptAt']):
                    self.assertEqual(worker.run(self.args(root, resume=str(receipt_path))), 0)
                self.assertEqual(resume.call_args.args[3], receipt['payload'])
                self.assertTrue(resume.call_args.args[2].endswith('/complete'))
                self.assertEqual(resume.call_count, 1)
                self.assertEqual(json.loads(receipt_path.read_text())['state'], 'completed')

    def test_physical_reservation_blocks_second_scratch_before_any_claim(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first, second = Path(first).resolve(), Path(second).resolve()

            def post(base, token, route, payload):
                if route.endswith('/claim'):
                    return {'job': JOB}
                raise worker.RetryableDelivery('offline')

            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', side_effect=post) as posted, \
                    patch.object(worker, 'execute', return_value=('succeeded', {'n_trees': 3})) as execute:
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(first))
                posted.reset_mock()
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(second))
                posted.assert_not_called()
                self.assertEqual(execute.call_count, 1)
            with worker.NodeGate(ENGINE) as gate:
                self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_staging_authority_cannot_claim_production_owned_engine(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                patch.object(worker, 'docker', side_effect=runtime_docker), \
                patch.object(worker, 'authority', return_value='87654321-1234-1234-1234-123456789abc'), \
                patch.object(worker, 'post') as post, patch.object(worker, 'execute') as execute:
            with self.assertRaises(worker.WorkerError):
                worker.run(self.args(Path(directory).resolve()))
            post.assert_not_called()
            execute.assert_not_called()

    def test_completed_receipt_releases_guard_after_crash_without_network_or_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()

            def post(base, token, route, payload):
                if route.endswith('/claim'):
                    return {'job': JOB}
                return {'job': {**JOB, 'status': payload['status'], 'result': payload['result']}}

            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', side_effect=post), \
                    patch.object(worker, 'execute', return_value=('succeeded', {'n_trees': 3})), \
                    patch.object(worker.NodeGate, 'acknowledge', side_effect=worker.GateError('simulated process crash')):
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
            receipt_path = next((root / '.prototype-receipts').glob('*.json'))
            self.assertEqual(json.loads(receipt_path.read_text())['state'], 'completed')
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post') as post, patch.object(worker, 'execute') as execute:
                self.assertEqual(worker.run(self.args(root, resume=str(receipt_path))), 0)
                self.assertEqual(worker.run(self.args(root, resume=str(receipt_path))), 0)
                post.assert_not_called()
                execute.assert_not_called()
                with worker.NodeGate(ENGINE) as gate:
                    self.assertIsNone(gate.snapshot()['reservation'])
                    gate.reserve('cluster', 'http://127.0.0.1:8899', AUTHORITY, 1, 'b' * 32)
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root, resume=str(receipt_path)))
                with worker.NodeGate(ENGINE) as gate:
                    self.assertEqual(gate.snapshot()['reservation']['receiptId'], 'b' * 32)

    def test_empty_receipt_can_release_guard_after_crash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', return_value={'job': None}), \
                    patch.object(worker.NodeGate, 'release_empty', side_effect=worker.GateError('crash')):
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
            receipt_path = next((root / '.prototype-receipts').glob('*.json'))
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post') as post, patch.object(worker, 'execute') as execute:
                self.assertEqual(worker.run(self.args(root, resume=str(receipt_path))), 0)
                post.assert_not_called()
                execute.assert_not_called()

    def test_docker_context_precedence_and_remote_endpoint_rejection(self):
        contexts = json.dumps([{'Endpoints': {'docker': {'Host': 'tcp://remote:2376'}}}])
        with patch.dict(os.environ, {'DOCKER_CONTEXT': 'selected', 'DOCKER_HOST': 'unix:///other.sock'}), \
                patch.object(worker.subprocess, 'run', return_value=result(contexts)) as run:
            with self.assertRaises(worker.WorkerError):
                RESOLVE_DOCKER_ENDPOINT()
            self.assertEqual(run.call_args.args[0], ['docker', 'context', 'inspect', 'selected'])
        with patch.dict(os.environ, {'DOCKER_CONTEXT': '', 'DOCKER_HOST': 'unix:///var/run/docker.sock'}), \
                patch.object(worker.subprocess, 'run') as run:
            self.assertEqual(RESOLVE_DOCKER_ENDPOINT(), 'unix:///var/run/docker.sock')
            run.assert_not_called()

    def test_pinned_docker_calls_cannot_follow_later_environment_context_change(self):
        with patch.object(worker, 'DOCKER_ENDPOINT', 'unix:///approved.sock'), \
                patch.dict(os.environ, {'DOCKER_CONTEXT': 'different', 'DOCKER_HOST': 'tcp://remote:2376'}), \
                patch.object(worker.subprocess, 'run', return_value=result()) as run:
            worker.docker(['info'])
            self.assertEqual(run.call_args.args[0], ['docker', '--host', 'unix:///approved.sock', 'info'])
            self.assertNotIn('DOCKER_CONTEXT', run.call_args.kwargs['env'])
            self.assertNotIn('DOCKER_HOST', run.call_args.kwargs['env'])

    def test_timeout_requires_actual_container_stop_before_failed_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            calls = []

            def docker(args, timeout=20):
                calls.append(args)
                if args[0] == 'start':
                    raise subprocess.TimeoutExpired('docker', timeout)
                if args[0] == 'inspect':
                    return result('false')
                return result()

            with patch.object(worker, 'docker', side_effect=docker):
                status, payload = worker.execute(IMAGE, Path(directory), 'exact-container', 240)
            self.assertEqual(status, 'failed')
            self.assertEqual(payload, {'error': 'synthetic_execution_failed'})
            self.assertEqual(calls[-2:], [['stop', '--time', '5', 'exact-container'],
                                          ['inspect', '--format', '{{.State.Running}}', 'exact-container']])
            create = calls[0]
            for flag in ('--read-only', '--pids-limit', '--cap-drop', '--security-opt', '--tmpfs'):
                self.assertIn(flag, create)
            self.assertIn(IMAGE, create)
            self.assertNotIn('api-secret', str(create))
            self.assertNotIn('docker.sock', str(create))
            self.assertEqual(create[create.index('--network') + 1], 'none')

    def test_unverified_stop_leaves_claim_uncompleted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker), \
                    patch.object(worker, 'post', return_value={'job': JOB}) as post, \
                    patch.object(worker, 'execute', side_effect=worker.WorkerError('stop unverified')):
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
            self.assertEqual(post.call_count, 1)
            receipt = json.loads(next((root / '.prototype-receipts').glob('*.json')).read_text())
            self.assertEqual(receipt['state'], 'claimed')
            self.assertIn('container', receipt)

    def test_execute_does_not_trust_stdout_without_matching_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            data = {'n_trees': 3, 'treedar_version': '0.1',
                    'manifest_sha256': '0' * 64, 'outputs_sha256': '0' * 64}

            def docker(args, timeout=20):
                if args[0] == 'start':
                    return result('PROTOTYPE_RESULT=' + json.dumps(data))
                if args[0] == 'inspect':
                    return result('false')
                return result()

            with patch.object(worker, 'docker', side_effect=docker):
                self.assertEqual(worker.execute(IMAGE, Path(directory), 'test', 240)[0], 'failed')

    def test_timeout_with_unverified_container_stop_raises(self):
        def docker(args, timeout=20):
            if args[0] == 'start':
                raise subprocess.TimeoutExpired('docker', timeout)
            if args[0] == 'inspect':
                return result('true')
            return result()

        with tempfile.TemporaryDirectory() as directory, \
                patch.object(worker, 'docker', side_effect=docker), \
                self.assertRaisesRegex(worker.WorkerError, 'stop unverified'):
            worker.execute(IMAGE, Path(directory), 'test', 240)

    def test_success_requires_stopped_container_and_matching_result_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'synthetic').mkdir()
            (root / 'synthetic' / 'manifest.json').write_bytes(b'{}')
            for name in ('summary.json', 'report.html'):
                (root / 'synthetic' / name).write_bytes(b'fixture')
            hashes = {name: hashlib.sha256(b'fixture').hexdigest() for name in ('summary.json', 'report.html')}
            encoded = json.dumps(hashes).encode()
            (root / 'output-sha256.json').write_bytes(encoded)
            digest = hashlib.sha256(b'{}').hexdigest()
            data = {'n_trees': 3, 'treedar_version': '0.1',
                    'manifest_sha256': digest, 'outputs_sha256': hashlib.sha256(encoded).hexdigest()}

            def docker(args, timeout=20):
                if args[0] == 'start':
                    return result('PROTOTYPE_RESULT=' + json.dumps(data))
                if args[0] == 'inspect':
                    return result('false')
                return result()

            with patch.object(worker, 'docker', side_effect=docker):
                self.assertEqual(worker.execute(IMAGE, root, 'test', 240), ('succeeded', data))
                (root / 'synthetic' / 'report.html').write_bytes(b'changed after container output')
                self.assertEqual(worker.execute(IMAGE, root, 'test', 240)[0], 'failed')

    def test_empty_queue_does_not_run_container_or_print_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            stdout = io.StringIO()
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', side_effect=runtime_docker) as docker, \
                    patch.object(worker, 'post', return_value={'job': None}), \
                    contextlib.redirect_stdout(stdout):
                self.assertEqual(worker.run(self.args(root)), 0)
            self.assertEqual(docker.call_count, 2)
            self.assertEqual(stdout.getvalue(), 'No work available.\n')
            receipt = json.loads(next((root / '.prototype-receipts').glob('*.json')).read_text())
            self.assertIs(receipt['claimAcknowledgedEmpty'], True)
            self.assertEqual(receipt['nodeId'], 'cluster')
            self.assertTrue({'job', 'container', 'payload'}.isdisjoint(receipt))

    def test_mutable_image_refused_before_claim(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(worker, 'post') as post, self.assertRaises(worker.WorkerError):
            worker.run(self.args(Path(directory), image='treedar:latest'))
        post.assert_not_called()

    def pending(self, root):
        path = root / 'receipt.json'
        receipt = {'coordinator': 'http://127.0.0.1:8899', 'workerId': 'test-worker',
                   'nodeId': 'cluster', 'state': 'completion_pending', 'job': dict(JOB),
                   'payload': {'claimToken': JOB['claimToken'], 'status': 'succeeded',
                               'result': {'n_trees': 3, 'nodeId': 'cluster'}}}
        worker.save(path, receipt)
        return path, receipt

    def deliver(self, path, receipt, clock, wait=600):
        return worker.complete(path, receipt, 'api-secret', wait_seconds=wait,
            clock=clock.clock, monotonic=clock.clock, sleep=clock.sleep, jitter=lambda low, high: 1)

    def test_outage_backoff_retains_same_payload_until_exact_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, receipt = self.pending(root)
            clock = FakeClock()
            sent = []
            artifact = root / 'retained-output.bin'
            artifact.write_bytes(b'output')

            def post(base, token, route, payload):
                sent.append(json.dumps(payload, sort_keys=True))
                persisted = json.loads(path.read_text())
                self.assertEqual(persisted['delivery']['attempts'], len(sent))
                self.assertGreater(persisted['delivery']['nextAttemptAt'], clock.now)
                self.assertEqual(artifact.read_bytes(), b'output')
                if len(sent) <= 4:
                    raise worker.RetryableDelivery('offline')
                return acknowledgment(receipt)

            with patch.object(worker, 'post', side_effect=post):
                self.deliver(path, receipt, clock)
            self.assertEqual(clock.sleeps, [1, 2, 4, 8])
            self.assertEqual(len(set(sent)), 1)
            self.assertEqual(json.loads(path.read_text())['state'], 'completed')
            self.assertEqual(json.loads(path.read_text())['acknowledgment'], acknowledgment(receipt)['job'])
            self.assertTrue(artifact.exists())

    def test_commit_lost_ack_replays_only_identical_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            clock = FakeClock()
            committed = []

            def post(base, token, route, payload):
                self.assertTrue(route.endswith('/complete'))
                committed.append(json.dumps(payload, sort_keys=True))
                if len(committed) == 1:
                    raise worker.RetryableDelivery('commit response lost')
                self.assertEqual(committed[0], committed[1])
                return acknowledgment(receipt)

            with patch.object(worker, 'post', side_effect=post), patch.object(worker, 'execute') as execute:
                self.deliver(path, receipt, clock)
            execute.assert_not_called()
            self.assertEqual(receipt['delivery']['attempts'], 2)

    def test_resume_preserves_future_schedule_and_attempt_count(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            clock = FakeClock()
            with patch.object(worker, 'post', side_effect=worker.RetryableDelivery('offline')):
                with self.assertRaises(worker.WorkerError):
                    self.deliver(path, receipt, clock, wait=0)
            restarted = json.loads(path.read_text())
            with patch.object(worker, 'post') as post:
                with self.assertRaises(worker.WorkerError):
                    self.deliver(path, restarted, clock, wait=0)
                post.assert_not_called()
            with patch.object(worker, 'post', return_value=acknowledgment(restarted)):
                self.deliver(path, restarted, clock, wait=1)
            self.assertEqual(clock.sleeps, [1])
            self.assertEqual(restarted['delivery']['attempts'], 2)

    def test_retry_delay_caps_and_foreground_infinite_mode_reconnects(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            clock = FakeClock()
            replies = [worker.RetryableDelivery('offline')] * 14 + [acknowledgment(receipt)]
            with patch.object(worker, 'post', side_effect=replies):
                self.deliver(path, receipt, clock, wait=-1)
            self.assertEqual(clock.sleeps[:4], [1, 2, 4, 8])
            self.assertEqual(clock.sleeps[-1], 300)
            self.assertLessEqual(max(clock.sleeps), 300)

    def test_restart_clamps_remaining_delay_after_backward_wall_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            clock = FakeClock()
            receipt['delivery'] = {'attempts': 2, 'nextAttemptAt': clock.now + 10 * 365 * 86400}
            worker.save(path, receipt)
            with patch.object(worker, 'post', return_value=acknowledgment(receipt)) as post:
                self.deliver(path, receipt, clock, wait=300)
            self.assertEqual(clock.sleeps, [300])
            self.assertEqual(post.call_count, 1)
            self.assertEqual(receipt['delivery']['attempts'], 3)

    def test_in_process_backward_wall_clock_does_not_change_monotonic_backoff(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            elapsed = FakeClock()
            wall = FakeClock()
            calls = []

            def post(*args):
                calls.append(elapsed.now)
                if len(calls) == 1:
                    wall.now -= 20 * 365 * 86400
                    raise worker.RetryableDelivery('offline during clock correction')
                return acknowledgment(receipt)

            with patch.object(worker, 'post', side_effect=post):
                worker.complete(path, receipt, 'secret', wait_seconds=2, clock=wall.clock,
                    monotonic=elapsed.clock, sleep=elapsed.sleep, jitter=lambda low, high: 1)
            self.assertEqual(elapsed.sleeps, [1])
            self.assertEqual(calls[1] - calls[0], 1)

    def test_attempt_limit_stays_valid_and_prevents_further_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            clock = FakeClock()
            receipt['delivery'] = {'attempts': 999_999, 'nextAttemptAt': 0}
            with patch.object(worker, 'post', side_effect=worker.RetryableDelivery('offline')) as post:
                with self.assertRaises(worker.WorkerError):
                    self.deliver(path, receipt, clock, wait=600)
            self.assertEqual(post.call_count, 1)
            stored = json.loads(path.read_text())
            self.assertEqual(stored['delivery']['attempts'], 1_000_000)
            self.assertEqual(stored['delivery']['blockedReason'], 'delivery_attempt_limit_reached')
            with patch.object(worker, 'post') as post:
                with self.assertRaises(worker.WorkerError):
                    self.deliver(path, stored, clock)
                post.assert_not_called()

    def test_permanent_failure_and_wrong_ack_are_retained_and_not_retried(self):
        for response in (worker.PermanentDelivery('auth failed'),
                         {'job': {'status': 'succeeded'}},
                         {'job': {**JOB, 'workerId': 'different-worker', 'status': 'succeeded',
                                  'result': {'n_trees': 3, 'nodeId': 'cluster'}}}):
            with self.subTest(response=response), tempfile.TemporaryDirectory() as directory:
                path, receipt = self.pending(Path(directory))
                clock = FakeClock()
                replies = [response] if isinstance(response, Exception) else [response]
                with patch.object(worker, 'post', side_effect=replies) as post:
                    with self.assertRaises(worker.WorkerError):
                        self.deliver(path, receipt, clock)
                    self.assertEqual(post.call_count, 1)
                stored = json.loads(path.read_text())
                self.assertEqual(stored['state'], 'completion_pending')
                self.assertEqual(stored['payload'], receipt['payload'])
                self.assertIn('blockedReason', stored['delivery'])
                with patch.object(worker, 'post') as post:
                    with self.assertRaises(worker.WorkerError):
                        self.deliver(path, stored, clock)
                    post.assert_not_called()

    def test_authority_is_preserved_and_acknowledgment_must_match(self):
        authority = '12345678-1234-4234-8234-123456789abd'
        self.assertEqual(worker.validate_job({**JOB, 'authorityId': authority})['authorityId'], authority)
        with self.assertRaises(worker.WorkerError):
            worker.validate_job({**JOB, 'authorityId': 'invalid'})
        with tempfile.TemporaryDirectory() as directory:
            path, receipt = self.pending(Path(directory))
            receipt['job']['authorityId'] = authority
            response = acknowledgment(receipt)
            response['job']['authorityId'] = JOB['id']
            with patch.object(worker, 'post', return_value=response), self.assertRaises(worker.WorkerError):
                self.deliver(path, receipt, FakeClock())

    def test_completion_http_status_classifies_transient_and_permanent_failures(self):
        for status in (401, 403, 409, 429, 500, 503):
            with self.subTest(status=status), patch.object(worker.urllib.request, 'build_opener') as opener:
                opener.return_value.open.side_effect = worker.urllib.error.HTTPError(
                    'http://127.0.0.1', status, 'category', {}, None)
                expected = worker.RetryableDelivery if status == 429 or status >= 500 else worker.PermanentDelivery
                with self.assertRaises(expected):
                    worker.post('http://127.0.0.1', 'secret', '/complete', {})

    def test_receipt_save_refuses_unfinished_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'receipt.json'
            path.with_suffix('.tmp').write_bytes(b'partial earlier write')
            with self.assertRaises(worker.WorkerError):
                worker.save(path, {'state': 'completed'})

    def test_verified_artifact_flushes_files_without_changing_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            output = root / 'staging' / 'job'
            nested = output / 'synthetic'
            nested.mkdir(parents=True)
            files = [output / 'output-sha256.json', nested / 'manifest.json', nested / 'report.html']
            for path in files:
                path.write_bytes(b'verified fixture')
            actual_fsync = worker.os.fsync
            with patch.object(worker.os, 'fsync', wraps=actual_fsync) as sync:
                worker.durable_artifacts(output, files)
            expected_dirs = 0 if os.name == 'nt' else 4
            self.assertEqual(sync.call_count, len(files) + expected_dirs)
            self.assertTrue(all(path.read_bytes() == b'verified fixture' for path in files))

    def test_artifact_flush_refuses_paths_outside_output_and_nonfiles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            output = root / 'output'
            output.mkdir()
            outside = root / 'outside.bin'
            outside.write_bytes(b'outside')
            for path in (outside, output):
                with self.subTest(path=path), patch.object(worker.os, 'fsync') as sync:
                    with self.assertRaises(worker.WorkerError):
                        worker.durable_artifacts(output, [path])
                    sync.assert_not_called()

    def test_success_flush_failure_cannot_report_succeeded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            artifact_root = root / 'synthetic'
            artifact_root.mkdir()
            for name in ('manifest.json', 'summary.json', 'report.html'):
                (artifact_root / name).write_bytes(b'fixture')
            hashes = {name: hashlib.sha256(b'fixture').hexdigest() for name in ('summary.json', 'report.html')}
            encoded = json.dumps(hashes).encode()
            (root / 'output-sha256.json').write_bytes(encoded)
            data = {'n_trees': 3, 'treedar_version': '0.1',
                    'manifest_sha256': hashlib.sha256(b'fixture').hexdigest(),
                    'outputs_sha256': hashlib.sha256(encoded).hexdigest()}

            def docker(args, timeout=20):
                if args[0] == 'start':
                    return result('PROTOTYPE_RESULT=' + json.dumps(data))
                return result('false') if args[0] == 'inspect' else result()

            with patch.object(worker, 'docker', side_effect=docker), \
                    patch.object(worker, 'durable_artifacts', side_effect=OSError('disk flush failed')):
                self.assertEqual(worker.execute(IMAGE, root, 'test', 240)[0], 'failed')


if __name__ == '__main__':
    unittest.main()
