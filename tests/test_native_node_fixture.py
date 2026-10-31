from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr
import hashlib
import http.client
import io
import json
from pathlib import Path
import tempfile
import unittest
import uuid
import zipfile

from ops.native_node_fixture import NativeNodeFixture, MAX_BODY, ARCHIVE
from processing.odx_smoke_worker import SmokeError
from processing.viewer_service import NativeEngine, EngineUnavailable


class NativeNodeFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.fixture = NativeNodeFixture(self.root / 'native.sqlite', synthetic_enabled=True).start()
        self.engine = NativeEngine(self.fixture.origin, 'synthetic-query-token-not-logged')
        self.job = str(uuid.uuid4())

    def tearDown(self):
        self.fixture.close()
        self.temp.cleanup()

    def initialize(self):
        return self.engine.initialize(dict(name='synthetic task', engine_uuid=self.job, outputs=None,
            options=[dict(name='fast-mode',value=True), dict(name='numeric-limit',value=3)]))

    def upload(self, body=b'abc'):
        path = self.root / 'photo.jpg'
        path.write_bytes(body)
        return self.engine.upload(self.job, dict(path=str(path), bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), name='photo.jpg'))

    def raw(self, method, path, body=None, headers=None):
        address = self.fixture._server.server_address
        connection = http.client.HTTPConnection(address[0], address[1], timeout=2)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        result = response.status, response.read(), dict(response.getheaders())
        connection.close()
        return result

    def test_real_native_engine_http_mutations_probes_and_fixed_archive(self):
        info = self.engine.read('/info')
        self.assertEqual(info['version'], '2.4.0')
        self.assertEqual(info['engineVersion'], 'synthetic-transport-only')
        self.assertEqual((info['taskQueueCount'], info['maxImages'], info['maxParallelTasks']), (0,1000,1))
        self.assertEqual({option['type'] for option in self.engine.read('/options')}, {'bool','int'})
        self.assertEqual(self.engine.read('/task/list'), [])
        self.initialize()
        self.upload()
        self.engine.action('commit', self.job)
        self.assertEqual(self.engine.read('/task/' + self.job + '/info')['status']['code'], 40)
        self.assertEqual(self.engine.read('/task/list'), [{'uuid': self.job}])
        with self.engine.download(self.job) as response:
            archive = response.read()
            self.assertEqual(response.headers['Content-Length'], '22')
        self.assertEqual(archive, ARCHIVE)
        self.assertTrue(zipfile.is_zipfile(io.BytesIO(archive)))
        self.assertEqual(self.fixture.counts(), dict(initialize=1,upload=1,commit=1,download=1))

    def test_duplicate_mutations_rejected_without_reexecution(self):
        self.initialize()
        with self.assertRaises(SmokeError):
            self.initialize()
        self.upload()
        with self.assertRaises(SmokeError):
            self.upload()
        self.engine.action('commit', self.job)
        with self.assertRaises(SmokeError):
            self.engine.action('commit', self.job)
        self.assertEqual(self.fixture.counts(), dict(initialize=1,upload=1,commit=1,download=0))

    def test_real_native_remove_only_exact_completed_task(self):
        self.initialize()
        with self.assertRaises(SmokeError):
            self.engine.action('remove', self.job)
        self.assertEqual(self.fixture.counts().get('remove', 0), 0)
        self.upload()
        self.engine.action('commit', self.job)
        self.assertEqual(self.engine.action('remove', self.job), {'success': True})
        self.assertIsNone(self.engine.read('/task/' + self.job + '/info', missing=True))
        self.assertEqual(self.engine.read('/task/list'), [])
        with self.assertRaises(SmokeError):
            self.engine.action('remove', self.job)
        self.assertEqual(self.fixture.counts()['remove'], 1)

    def test_unknown_ids_and_nonfixture_upload_never_create_results(self):
        self.assertIsNone(self.engine.read('/task/' + self.job + '/info', missing=True))
        self.initialize()
        with self.assertRaises(SmokeError):
            self.upload(b'abd')
        with self.assertRaises(SmokeError):
            self.engine.action('commit', self.job)
        with self.assertRaises(Exception):
            self.engine.download(self.job)
        self.assertEqual(self.fixture.counts(), dict(initialize=1,upload=0,commit=0,download=0))

    def test_restart_retains_status_and_mutation_counts(self):
        self.initialize()
        self.upload()
        self.engine.action('commit', self.job)
        self.fixture.close()
        self.fixture = NativeNodeFixture(self.root / 'native.sqlite', synthetic_enabled=True).start()
        self.engine = NativeEngine(self.fixture.origin)
        self.assertEqual(self.engine.read('/task/' + self.job + '/info')['status']['code'], 40)
        with self.assertRaises(SmokeError):
            self.engine.action('commit', self.job)
        self.assertEqual(self.fixture.counts()['commit'], 1)

    def test_body_limit_canonical_uuid_and_no_token_path_logging(self):
        status, body, _headers = self.raw('POST', '/task/new/init', b'x' * (MAX_BODY + 1), {'set-uuid': self.job})
        self.assertEqual(status, 413)
        self.assertNotIn(str(self.root), body.decode())
        status, body, _headers = self.raw('POST', '/task/new/init', b'', {'set-uuid': '../private'})
        self.assertEqual(status, 400)
        errors = io.StringIO()
        with redirect_stderr(errors):
            status, body, _headers = self.raw('GET', '/unknown?token=synthetic-private-value')
        self.assertEqual(status, 404)
        self.assertNotIn(b'synthetic-private-value', body)
        self.assertEqual(errors.getvalue(), '')
        self.assertEqual(sum(self.fixture.counts().values()), 0)

    def test_concurrent_commit_has_one_durable_mutation(self):
        self.initialize()
        self.upload()

        def commit(_value):
            try:
                self.engine.action('commit', self.job)
                return True
            except SmokeError:
                return False

        with ThreadPoolExecutor(max_workers=4) as pool:
            self.assertEqual(sum(pool.map(commit, range(4))), 1)
        self.assertEqual(self.fixture.counts()['commit'], 1)

    def test_fixture_requires_explicit_opt_in_and_loopback(self):
        with self.assertRaises(ValueError):
            NativeNodeFixture(self.root / 'disabled.sqlite')
        with self.assertRaises(ValueError):
            NativeNodeFixture(self.root / 'external.sqlite', host='0.0.0.0', synthetic_enabled=True)


if __name__ == '__main__':
    unittest.main()
