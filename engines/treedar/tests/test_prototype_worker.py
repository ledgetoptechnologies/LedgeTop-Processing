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
IMAGE = 'sha256:' + 'a' * 64
JOB = {'id': '12345678-1234-4234-8234-123456789abc', 'environment': 'staging',
       'profile': worker.PROFILE, 'claimToken': 'secret_' + 'a' * 36}


def result(stdout='', returncode=0):
    return subprocess.CompletedProcess([], returncode, stdout, '')


class WorkerTests(unittest.TestCase):
    def args(self, root, **changes):
        values = dict(coordinator='http://127.0.0.1:8899', verified_origin=None,
                      image=IMAGE, scratch=str(root), worker_id='test-worker',
                      timeout=240, resume=None)
        values.update(changes)
        return argparse.Namespace(**values)

    def test_returned_job_cannot_select_paths_profile_or_environment(self):
        for key, value in [('id', '../../escape'), ('id', 'not-a-uuid'),
                           ('environment', '../production'), ('environment', 'dev'),
                           ('profile', 'shell'), ('claimToken', '\r\nX: evil')]:
            with self.subTest(key=key, value=value), self.assertRaises(worker.WorkerError):
                worker.validate_job({**JOB, key: value})
        self.assertEqual(worker.validate_job(JOB), JOB)

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
                    patch.object(worker, 'docker', return_value=result(IMAGE)), \
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
                raise worker.WorkerError('uncertain completion')

            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', return_value=result(IMAGE)), \
                    patch.object(worker, 'post', side_effect=post), \
                    patch.object(worker, 'execute', return_value=('succeeded', {'n_trees': 3})):
                with self.assertRaises(worker.WorkerError):
                    worker.run(self.args(root))
                receipt_path = next((root / '.prototype-receipts').glob('*.json'))
                receipt = json.loads(receipt_path.read_text())
                self.assertEqual(receipt['state'], 'completion_pending')
                self.assertEqual(receipt['payload'], posts[-1][1])
                with patch.object(worker, 'post', return_value={'job': {'status': 'succeeded'}}) as resume:
                    self.assertEqual(worker.run(self.args(root, resume=str(receipt_path))), 0)
                self.assertEqual(resume.call_args.args[3], receipt['payload'])
                self.assertTrue(resume.call_args.args[2].endswith('/complete'))
                self.assertEqual(resume.call_count, 1)
                self.assertEqual(json.loads(receipt_path.read_text())['state'], 'completed')

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
                    patch.object(worker, 'docker', return_value=result(IMAGE)), \
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
            (root / 'output-sha256.json').write_bytes(b'{}')
            digest = hashlib.sha256(b'{}').hexdigest()
            data = {'n_trees': 3, 'treedar_version': '0.1',
                    'manifest_sha256': digest, 'outputs_sha256': digest}

            def docker(args, timeout=20):
                if args[0] == 'start':
                    return result('PROTOTYPE_RESULT=' + json.dumps(data))
                if args[0] == 'inspect':
                    return result('false')
                return result()

            with patch.object(worker, 'docker', side_effect=docker):
                self.assertEqual(worker.execute(IMAGE, root, 'test', 240), ('succeeded', data))

    def test_empty_queue_does_not_run_container_or_print_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            stdout = io.StringIO()
            with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                    patch.object(worker, 'docker', return_value=result(IMAGE)) as docker, \
                    patch.object(worker, 'post', return_value={'job': None}), \
                    contextlib.redirect_stdout(stdout):
                self.assertEqual(worker.run(self.args(root)), 0)
            self.assertEqual(docker.call_count, 1)
            self.assertEqual(stdout.getvalue(), 'No work available.\n')

    def test_mutable_image_refused_before_claim(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(worker, 'post') as post, self.assertRaises(worker.WorkerError):
            worker.run(self.args(Path(directory), image='treedar:latest'))
        post.assert_not_called()


if __name__ == '__main__':
    unittest.main()
