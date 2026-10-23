"""Isolated photo smoke contract tests. No photos, Docker or network required."""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing import odx_smoke_worker as worker


JOB = {'id': '12345678-1234-4234-8234-123456789abc', 'environment': 'staging',
       'profile': worker.PROFILE, 'claimToken': 'claim_secret_' + 'x' * 32}
IMAGE = 'sha256:' + 'a' * 64


def fixture(root):
    photos = root / 'photos'
    photos.mkdir()
    entries = []
    for i in range(10):
        path = photos / f'photo{i:02}.JPG'
        data = b'\xff\xd8\xff' + bytes([i]) + b'\xff\xd9'
        path.write_bytes(data)
        entries.append({'name': path.name, 'bytes': len(data),
                        'sha256': hashlib.sha256(data).hexdigest()})
    manifest = root / 'manifest.json'
    manifest.write_text(json.dumps({'files': entries}))
    return photos, manifest, entries


def archive_for(files, shots=10, extra=None):
    stream = io.BytesIO()
    data = [{'shots': {item['name']: {} for item in files[:shots]},
             'cameras': {'camera': {}}, 'points': {'0': {'coordinates': [0, 0, 0]}}}]
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr('opensfm/reconstruction.json', json.dumps(data))
        zipped.writestr('cameras.json', '{}')
        zipped.writestr('task_output.txt', 'test')
        if extra:
            zipped.writestr(extra, 'unsafe')
    return stream.getvalue()


class FakeCoordinator:
    def __init__(self):
        self.calls = []
        self.ambiguous_complete = False

    def json(self, method, route, payload=None, **kwargs):
        self.calls.append((method, route, payload))
        if route.endswith('/claim'):
            return {'job': dict(JOB)}
        if self.ambiguous_complete:
            raise worker.SmokeError('completion uncertain')
        return {'job': {'id': JOB['id'], 'status': payload['status']}}


class FakeNode:
    def __init__(self, files):
        self.calls = []
        self.files = files
        self.identifier = None
        self.uploads = []
        self.code = 40
        self.count = 10
        self.archive = archive_for(files)
        self.fail_route = None
        self.keep_removed = False

    def json(self, method, route, payload=None, **kwargs):
        self.calls.append((method, route, payload, kwargs))
        if route == self.fail_route:
            raise worker.SmokeError('uncertain mutation')
        if route == '/info':
            return {'engine': 'odx', 'version': '2.test', 'engineVersion': '1.test'}
        if route == '/options':
            return worker.OPTIONS
        if route == '/task/list':
            return [{'uuid': self.identifier}] if self.keep_removed and self.identifier else []
        if route == '/task/new/init':
            self.identifier = kwargs['headers']['set-uuid']
            return {'uuid': self.identifier}
        if route.startswith('/task/new/commit/'):
            return {'uuid': self.identifier}
        if route.endswith('/info'):
            return {'status': {'code': self.code}, 'imagesCount': self.count}
        if route == '/task/remove':
            if payload != {'uuid': self.identifier}:
                raise AssertionError('removed wrong engine task')
            return {'success': True}
        raise AssertionError(route)

    def upload(self, identifier, path, digest):
        self.uploads.append((identifier, path.name, digest))

    def request(self, method, route, **kwargs):
        self.calls.append((method, route, None, kwargs))
        return self.archive


class WorkerTests(unittest.TestCase):
    def args(self, root, **changes):
        values = dict(coordinator='http://coordinator:8899', node_origin='http://node:3000',
                      manifest=str(root / 'manifest.json'), photos=str(root / 'photos'),
                      work=str(root / 'work'), worker_id='isolated-test', engine_image=IMAGE,
                      resume_completion=False)
        values.update(changes)
        return argparse.Namespace(**values)

    def invoke(self, root, node, coordinator, **changes):
        def client(origin, *args, **kwargs):
            return coordinator if 'coordinator' in origin else node

        with patch.dict(os.environ, {'PROCESSING_API_TOKEN': 'api-secret'}), \
                patch.object(worker, 'Client', side_effect=client), \
                patch('sys.stdout', io.StringIO()):
            return worker.run(self.args(root, **changes))

    def setup_root(self, directory):
        root = Path(directory).resolve()
        photos, manifest, files = fixture(root)
        (root / 'work').mkdir()
        return root, photos, manifest, files

    def test_complete_contract_and_original_hashes_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root, photos, manifest, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            before = {path.name: path.read_bytes() for path in photos.iterdir()}
            self.assertEqual(self.invoke(root, node, coordinator), 0)
            self.assertEqual(before, {path.name: path.read_bytes() for path in photos.iterdir()})
            claim = coordinator.calls[0]
            self.assertEqual(claim, ('POST', '/v1/prototype/claim',
                                     {'workerId': 'isolated-test', 'profiles': [worker.PROFILE]}))
            init = next(call for call in node.calls if call[1] == '/task/new/init')
            self.assertEqual(json.loads(init[3]['fields']['options']), worker.OPTIONS)
            self.assertEqual(init[3]['fields']['skipPostProcessing'], 'true')
            self.assertEqual(json.loads(init[3]['fields']['outputs']), worker.OUTPUTS)
            self.assertEqual(uuid.UUID(init[3]['headers']['set-uuid']).version, 4)
            self.assertEqual(len(node.uploads), 10)
            payload = coordinator.calls[-1][2]
            self.assertEqual(payload['status'], 'succeeded')
            self.assertEqual(payload['result']['registeredShots'], 10)
            self.assertEqual(payload['result']['cleanupVerified'], True)
            self.assertLessEqual(len(worker.canonical(payload['result'])), 4096)
            self.assertNotIn('api-secret', json.dumps(payload['result']))
            self.assertEqual({path.name for path in (root / 'work').iterdir()}, {'receipt.json'})
            self.assertEqual(node.calls[-1][1], '/task/list')

    def test_hash_mismatch_blocks_claim_and_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            root, photos, _, files = self.setup_root(directory)
            (photos / files[0]['name']).write_bytes(b'\xff\xd8\xffBAD')
            node, coordinator = FakeNode(files), FakeCoordinator()
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertEqual(coordinator.calls, [])
            self.assertEqual(node.calls, [])

    def test_manifest_rejects_traversal_duplicates_excess_and_non_jpeg(self):
        with tempfile.TemporaryDirectory() as directory:
            root, photos, manifest, files = self.setup_root(directory)
            for changed in [[{**files[0], 'name': '../escape.jpg'}, *files[1:]],
                            [files[0]] * 10, files[:9], files * 6,
                            [{**files[0], 'name': 'a.png'}, *files[1:]],
                            [{**files[0], 'bytes': worker.MAX_INPUT + 1}, *files[1:]]]:
                with self.subTest(changed=changed[0]), self.assertRaises(worker.SmokeError):
                    manifest.write_text(json.dumps({'files': changed}))
                    worker.read_manifest(manifest, photos)

    def test_ambiguous_init_is_persisted_and_never_retried_or_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            node.fail_route = '/task/new/init'
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            receipt = json.loads((root / 'work/receipt.json').read_bytes())
            self.assertEqual(receipt['stage'], 'init_pending')
            self.assertEqual(uuid.UUID(receipt['engineUUID']).version, 4)
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertEqual(len(coordinator.calls), 1)
            self.assertEqual(sum(call[1] == '/task/new/init' for call in node.calls), 1)
            self.assertFalse(any(call[1] == '/task/remove' for call in node.calls))

    def test_canceled_task_never_removed_and_claim_not_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            node.code = 50
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertEqual(len(coordinator.calls), 1)
            self.assertFalse(any(call[1] == '/task/remove' for call in node.calls))

    def test_ambiguous_upload_and_commit_never_retried_or_cleaned(self):
        for mutation in ('upload', 'commit'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root, _, _, files = self.setup_root(directory)
                node, coordinator = FakeNode(files), FakeCoordinator()
                original = node.json

                def json_request(method, route, payload=None, **kwargs):
                    if mutation == 'commit' and route.startswith('/task/new/commit/'):
                        raise worker.SmokeError('uncertain commit')
                    return original(method, route, payload, **kwargs)

                node.json = json_request
                if mutation == 'upload':
                    def upload(*args):
                        raise worker.SmokeError('uncertain upload')
                    node.upload = upload
                with self.assertRaises(worker.SmokeError):
                    self.invoke(root, node, coordinator)
                receipt = json.loads((root / 'work/receipt.json').read_bytes())
                self.assertEqual(receipt['stage'], mutation + '_pending')
                self.assertEqual(len(coordinator.calls), 1)
                self.assertFalse(any(call[1] == '/task/remove' for call in node.calls))

    def test_running_timeout_preserves_claim_and_engine_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            node.code = 20
            with patch.object(worker.time, 'monotonic', side_effect=[0, 0, 1, 599, 601]), \
                    patch.object(worker.time, 'sleep'), self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertEqual(len(coordinator.calls), 1)
            self.assertFalse(any(call[1] == '/task/remove' for call in node.calls))

    def test_terminal_failure_removed_before_failed_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            node.code = 30
            self.assertEqual(self.invoke(root, node, coordinator), 1)
            self.assertEqual(coordinator.calls[-1][2]['status'], 'failed')
            self.assertTrue(coordinator.calls[-1][2]['result']['cleanupVerified'])
            self.assertTrue(any(call[1] == '/task/remove' for call in node.calls))

    def test_cleanup_ambiguity_leaves_claim_running(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            node.fail_route = '/task/remove'
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertEqual(len(coordinator.calls), 1)
            receipt = json.loads((root / 'work/receipt.json').read_bytes())
            self.assertEqual(receipt['stage'], 'remove_pending')

    def test_failed_reconstruction_or_image_count_still_removes_terminal_task(self):
        for low_registration in (True, False):
            with self.subTest(low_registration=low_registration), tempfile.TemporaryDirectory() as directory:
                root, _, _, files = self.setup_root(directory)
                node, coordinator = FakeNode(files), FakeCoordinator()
                if low_registration:
                    node.archive = archive_for(files, shots=7)
                else:
                    node.count = 9
                self.assertEqual(self.invoke(root, node, coordinator), 1)
                self.assertEqual(coordinator.calls[-1][2]['status'], 'failed')
                self.assertTrue(any(call[1] == '/task/remove' for call in node.calls))

    def test_completion_replay_is_exact_and_does_not_touch_node(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            coordinator.ambiguous_complete = True
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            receipt = json.loads((root / 'work/receipt.json').read_bytes())
            self.assertEqual(receipt['stage'], 'completion_pending')
            before = list(node.calls)
            coordinator.ambiguous_complete = False
            self.assertEqual(self.invoke(root, node, coordinator, resume_completion=True), 0)
            self.assertEqual(before, node.calls)
            self.assertEqual(coordinator.calls[-1][2], receipt['payload'])
            self.assertEqual(sum(call[1].endswith('/claim') for call in coordinator.calls), 1)

    def test_reconstruction_bounds_paths_and_registration_threshold(self):
        files = [{'name': f'p{i}.jpg'} for i in range(10)]
        self.assertEqual(worker.reconstruction(archive_for(files, shots=8), files)['registeredShots'], 8)
        for path in ('../escape', '/absolute', 'C:/drive'):
            with self.subTest(path=path), self.assertRaises(worker.SmokeError):
                worker.reconstruction(archive_for(files, extra=path), files)
        raw = archive_for(files, extra='a/backslash').replace(b'a/backslash', b'a\\backslash')
        with self.assertRaises(worker.SmokeError):
            worker.reconstruction(raw, files)
        with self.assertRaises(worker.SmokeError):
            worker.reconstruction(archive_for(files, shots=7), files)
        with patch.object(worker, 'MAX_EXPANDED', 10), self.assertRaises(worker.SmokeError):
            worker.reconstruction(archive_for(files), files)
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as zipped:
            for i in range(513):
                zipped.writestr(str(i), '')
        with self.assertRaises(worker.SmokeError):
            worker.reconstruction(stream.getvalue(), files)

    def test_production_claim_rejected_before_engine_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, files = self.setup_root(directory)
            node, coordinator = FakeNode(files), FakeCoordinator()
            original = coordinator.json

            def claimed(*args, **kwargs):
                reply = original(*args, **kwargs)
                reply['job']['environment'] = 'production'
                return reply

            coordinator.json = claimed
            with self.assertRaises(worker.SmokeError):
                self.invoke(root, node, coordinator)
            self.assertFalse(any(call[0] == 'POST' for call in node.calls))


class HttpContractTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.do_POST()

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
                owner.requests.append((self.command, self.path, dict(self.headers), body))
                if self.path.startswith('/redirect'):
                    self.send_response(302)
                    self.send_header('Location', '/never-follow')
                    data = b''
                elif self.path.startswith('/error'):
                    self.send_response(200)
                    data = b'{"error":"contains-sensitive-server-details"}'
                else:
                    self.send_response(200)
                    data = b'{"success":true}'
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_real_http_multipart_streaming_and_query_auth(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'one.JPG'
            data = b'\xff\xd8\xff' + b'x' * (1024 * 1024 + 1)
            path.write_bytes(data)
            node = worker.Client(self.origin, 'node-secret', node=True)
            node.upload('test-id', path, hashlib.sha256(data).hexdigest())
        method, route, headers, body = self.requests[-1]
        self.assertEqual(method, 'POST')
        self.assertEqual(route, '/task/new/upload/test-id?token=node-secret')
        self.assertIn(b'name="images"; filename="one.JPG"', body)
        self.assertIn(data, body)
        self.assertEqual(int(headers['Content-Length']), len(body))
        self.assertNotIn('Authorization', headers)

    def test_http_200_json_error_and_redirect_refused_without_secret_details(self):
        client = worker.Client(self.origin, 'api-secret')
        for route in ('/error', '/redirect'):
            with self.subTest(route=route), self.assertRaises(worker.SmokeError) as caught:
                client.json('POST', route, {'fixed': True})
            self.assertNotIn('sensitive-server-details', str(caught.exception))
            self.assertNotIn('api-secret', str(caught.exception))
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.requests[0][2]['Authorization'], 'Bearer api-secret')


if __name__ == '__main__':
    unittest.main()
