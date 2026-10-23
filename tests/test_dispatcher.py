"""Actual coordinator HTTP with child execution replaced at its boundary."""

import argparse
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request, urlopen
import uuid

from processing.app import make_handler
from processing.config import Config
from tests.test_service import TOKEN, start_server


SPEC = importlib.util.spec_from_file_location(
    'prototype_dispatcher', Path(__file__).resolve().parents[1] / 'scripts/prototype_dispatcher.py')
dispatcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dispatcher)
IMAGE = 'sha256:' + 'a' * 64


class DispatcherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.scratch = self.root / 'scratch'
        self.state = self.root / 'state'
        self.scratch.mkdir()
        self.state.mkdir()
        self.handler = make_handler(Config(TOKEN, '', '', 800, 20000,
                                          prototype_state_dir=str(self.root / 'coordinator'),
                                          photo_smoke_enabled=True))
        self.server = start_server(self.handler)
        self.base = f'http://127.0.0.1:{self.server.server_port}'
        self.launched = []

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.handler.prototype_queue.close()
        self.temporary.cleanup()

    def args(self, **changes):
        values = dict(coordinator=self.base, verified_origin=None, image=IMAGE,
                      scratch=str(self.scratch), state=str(self.state), worker_id='dispatch-test',
                      timeout=240, max_jobs=2)
        values.update(changes)
        return argparse.Namespace(**values)

    def request(self, route, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = Request(self.base + route, data=data, headers={
            'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        with urlopen(request, timeout=5) as response:
            return json.load(response)

    def submit(self, request, environment='staging', profile=dispatcher.PROFILE):
        return self.request('/v1/prototype/jobs',
                            {'requestId': request, 'environment': environment, 'profile': profile})['job']

    def fake_child(self, args, base, scratch):
        # A child cannot be launched before durable intent exists.
        unresolved = [json.loads(path.read_text()) for path in self.state.glob('dispatch-*.json')
                      if json.loads(path.read_text())['state'] != 'completed']
        self.assertEqual(len(unresolved), 1)
        self.assertEqual(unresolved[0]['state'], 'launch_pending')
        claim = self.request('/v1/prototype/claim', {'workerId': args.worker_id})['job']
        self.assertIsNotNone(claim)
        self.launched.append(claim['id'])
        result = {'jobId': claim['id'], 'environment': claim['environment'], 'image_id': IMAGE,
                  'n_trees': 3, 'treedar_version': '0.test',
                  'manifest_sha256': 'b' * 64, 'outputs_sha256': 'c' * 64}
        payload = {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': result}
        self.request('/v1/prototype/jobs/' + claim['id'] + '/complete', payload)
        receipts = scratch / '.prototype-receipts'
        receipts.mkdir(exist_ok=True)
        (receipts / (uuid.uuid4().hex + '.json')).write_text(json.dumps({
            'state': 'completed', 'coordinator': base, 'workerId': args.worker_id,
            'job': claim, 'payload': payload}))
        return 0

    def invoke(self, child=None, **changes):
        output = io.StringIO()
        with patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN}), \
                patch.object(dispatcher, 'verify_image'), \
                patch.object(dispatcher, 'launch', side_effect=child or self.fake_child) as launch, \
                contextlib.redirect_stdout(output):
            code = dispatcher.run(self.args(**changes))
        return code, json.loads(output.getvalue()), launch

    def test_production_then_staging_serial_drain_with_real_http_queue(self):
        staging = self.submit('stage-first')
        production = self.submit('production-second', 'production')
        code, report, launch = self.invoke(max_jobs=3)
        self.assertEqual(code, 0)
        self.assertEqual(self.launched, [production['id'], staging['id']])
        self.assertEqual(report, {'completed': 2, 'completedJobIds': self.launched, 'reason': 'empty'})
        self.assertEqual(launch.call_count, 2)
        records = [json.loads(path.read_text()) for path in self.state.glob('dispatch-*.json')]
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record['state'] == 'completed' for record in records))

    def test_bounded_jobs_and_repeated_invocation_after_verified_completion(self):
        jobs = [self.submit('bounded-' + str(index)) for index in range(3)]
        _, report, launch = self.invoke(max_jobs=1)
        self.assertEqual(report, {'completed': 1, 'completedJobIds': [jobs[0]['id']], 'reason': 'max_jobs'})
        self.assertEqual(launch.call_count, 1)
        _, report, launch = self.invoke(max_jobs=3)
        self.assertEqual(report, {'completed': 2, 'completedJobIds': [job['id'] for job in jobs[1:]],
                                  'reason': 'empty'})
        self.assertEqual(launch.call_count, 2)

    def test_empty_unsupported_running_and_held_skip_launch(self):
        _, report, launch = self.invoke()
        self.assertEqual(report['reason'], 'empty')
        launch.assert_not_called()
        photo = self.submit('unsupported', profile='odx-photo-smoke-v1')
        self.submit('behind-photo')
        _, report, launch = self.invoke()
        self.assertEqual(report['reason'], 'unsupported_head')
        launch.assert_not_called()
        # The synthetic worker cannot claim the earlier unsupported task.
        self.assertIsNone(self.request('/v1/prototype/claim', {'workerId': 'legacy'})['job'])
        claimed = self.request('/v1/prototype/claim', {'workerId': 'photo-worker',
                                                     'profiles': ['odx-photo-smoke-v1']})['job']
        self.assertEqual(claimed['id'], photo['id'])
        _, report, launch = self.invoke()
        self.assertEqual(report['reason'], 'running')
        launch.assert_not_called()
        self.handler.prototype_queue.operator(photo['id'], 'hold', {'note': 'test hold'})
        _, report, launch = self.invoke()
        self.assertEqual(report['reason'], 'held')
        launch.assert_not_called()

    def test_priority_does_not_bypass_production_for_older_unsupported_staging(self):
        self.submit('photo-stage', profile='odx-photo-smoke-v1')
        production = self.submit('tree-prod', 'production')
        _, report, launch = self.invoke()
        self.assertEqual(report, {'completed': 1, 'completedJobIds': [production['id']],
                                  'reason': 'unsupported_head'})
        self.assertEqual(launch.call_count, 1)

    def test_child_crash_nonzero_and_no_progress_persist_guard_across_restart(self):
        self.submit('crash-case')
        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(child=lambda *args: (_ for _ in ()).throw(OSError('private secret detail')))
        records = list(self.state.glob('dispatch-*.json'))
        self.assertEqual(len(records), 1)
        self.assertEqual(json.loads(records[0].read_text())['state'], 'launch_pending')
        with patch.object(dispatcher, 'launch') as launch, \
                patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN}), \
                self.assertRaisesRegex(dispatcher.DispatchError, 'unresolved dispatch'):
            dispatcher.run(self.args())
        launch.assert_not_called()

    def test_nonzero_child_stops_even_when_job_is_completed(self):
        self.submit('child-error')

        def child(*args):
            self.fake_child(*args)
            return 1

        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(child=child)
        self.assertEqual(json.loads(next(self.state.glob('dispatch-*.json')).read_text())['state'],
                         'launch_pending')

    def test_zero_exit_without_progress_stops_after_one_child(self):
        self.submit('no-progress')
        called = []

        def child(*args):
            called.append(True)
            return 0

        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(child=child)
        self.assertEqual(called, [True])
        self.assertEqual(len(list(self.state.glob('dispatch-*.json'))), 1)

    def test_racing_new_priority_head_is_not_silently_accepted(self):
        old = self.submit('old-staging')

        def child(*args):
            self.submit('racing-production', 'production')
            return self.fake_child(*args)

        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(child=child)
        self.assertNotEqual(self.launched, [old['id']])
        self.assertEqual(json.loads(next(self.state.glob('dispatch-*.json')).read_text())['state'],
                         'launch_pending')

    def test_receipt_without_matching_queue_success_is_not_progress(self):
        self.submit('mismatch')

        def child(*args):
            self.fake_child(*args)
            path = next((self.scratch / '.prototype-receipts').glob('*.json'))
            receipt = json.loads(path.read_text())
            receipt['payload']['result']['n_trees'] = 99
            path.write_text(json.dumps(receipt))
            return 0

        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(child=child)

    def test_concurrent_os_lock_prevents_second_dispatcher(self):
        self.submit('lock-test')
        with dispatcher.process_lock(self.state / 'dispatcher.lock'), \
                patch.object(dispatcher, 'launch') as launch, \
                patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN}), \
                self.assertRaisesRegex(dispatcher.DispatchError, 'another dispatcher'):
            dispatcher.run(self.args())
        launch.assert_not_called()

    def test_unresolved_worker_receipt_prevents_launch_without_new_dispatch(self):
        self.submit('unresolved')
        root = self.scratch / '.prototype-receipts'
        root.mkdir()
        (root / (uuid.uuid4().hex + '.json')).write_text('{"state":"completion_pending"}')
        with self.assertRaisesRegex(dispatcher.DispatchError, 'unresolved worker'):
            self.invoke()
        self.assertFalse(list(self.state.glob('dispatch-*.json')))

    def test_child_environment_strips_operator_token_and_has_no_timeout(self):
        with patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN,
                                    'PROCESSING_PROTOTYPE_OPERATOR_TOKEN': 'operator-secret'}), \
                patch.object(dispatcher.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
            self.assertEqual(dispatcher.launch(self.args(), self.base, self.scratch), 0)
        kwargs = run.call_args.kwargs
        self.assertNotIn('PROCESSING_PROTOTYPE_OPERATOR_TOKEN', kwargs['env'])
        self.assertEqual(kwargs['env']['PROCESSING_API_TOKEN'], TOKEN)
        self.assertNotIn('timeout', kwargs)
        self.assertEqual(kwargs['stdout'], subprocess.DEVNULL)
        self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
        self.assertNotIn(TOKEN, ' '.join(run.call_args.args[0]))

    def test_origin_and_pinned_image_validation(self):
        for value in ('http://localhost/path', 'http://user:pass@localhost', 'http://remote:8080',
                      'http://localhost/?query=1', 'http://localhost/#frag'):
            with self.subTest(origin=value), self.assertRaises(dispatcher.DispatchError):
                dispatcher.origin(value)
        self.assertEqual(dispatcher.origin('http://remote:8080', 'http://remote:8080'),
                         'http://remote:8080')
        with self.assertRaises(dispatcher.DispatchError):
            self.invoke(image='treedar:latest')


if __name__ == '__main__':
    unittest.main()
