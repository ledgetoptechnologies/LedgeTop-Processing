"""Result proxy failures distinguish transient loss from identity failures."""
from contextlib import nullcontext
import io
import ssl
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

from processing import reference_protocol as p
from processing.reference_api import ReferenceAPI
from processing.reference_result_proxy import ResultProxy, ResultProxyUnavailable
from processing.reference_runtime import ReferenceRuntime


class ResultRetryStatusTests(unittest.TestCase):
    def setUp(self):
        manifest = {'schemaVersion': 1, 'jobId': 'example', 'generation': 1,
            'environment': 'staging', 'pipelineDigest': 'c' * 64,
            'engineVersions': {'odm': 'test'},
            'objects': [{'id': 'archive', 'role': 'odm_archive', 'byteSize': 3, 'sha256': 'a' * 64}]}
        manifest['resultManifestSha256'] = p.digest(manifest)
        self.job = {'jobId': 'example', 'generation': 1, 'environment': 'staging',
            'pipelineDigest': 'c' * 64, 'nodeId': 'node',
            'resultManifestSha256': manifest['resultManifestSha256'], 'resultManifest': manifest}

    def request(self, opener, job=None, admit=None):
        store = Mock()
        store.get.return_value = job or self.job
        proxy = ResultProxy({'node': 'https://worker.invalid'},
            worker_bearers={'node': 'synthetic-worker-token'}, opener=opener,
            admit_read=admit or (lambda _: nullcontext()))
        with patch('processing.reference_terminal.TerminalService'):
            api = ReferenceAPI(store, bearer_tokens={'synthetic-staging-token': 'staging'},
                capability_fingerprint='a' * 64, options_fingerprint='b' * 64, result_proxy=proxy)
        return api.handle('GET', '/ltds/v1/jobs/example/results/archive',
            {'Authorization': 'Bearer synthetic-staging-token'}, protected_transport=True)

    def test_offline_timeout_and_503_are_retryable(self):
        for error in (URLError(ConnectionRefusedError()), TimeoutError(),
                      HTTPError('https://worker.invalid', 503, 'private details', {}, io.BytesIO())):
            with self.subTest(error=type(error).__name__):
                response = self.request(Mock(side_effect=error))
                self.assertEqual((response.status, response.body), (503, {'error': 'result_read_unavailable'}))

    def test_auth_tls_and_result_identity_faults_are_not_classified_transient(self):
        errors = (HTTPError('https://worker.invalid', 401, 'secret', {}, io.BytesIO()),
                  URLError(ssl.SSLCertVerificationError('secret')))
        for error in errors:
            with self.subTest(error=type(error).__name__):
                response = self.request(Mock(side_effect=error))
                self.assertEqual((response.status, response.body), (502, {'error': 'result_read_failed'}))
        bad = dict(self.job, resultManifestSha256='0' * 64)
        response = self.request(Mock(), job=bad)
        self.assertEqual((response.status, response.body), (502, {'error': 'result_read_failed'}))

    def test_runtime_distinguishes_busy_streams_from_fixed_object_limit(self):
        runtime = ReferenceRuntime.__new__(ReferenceRuntime)
        runtime.settings = SimpleNamespace(stream_budget={'maxObjectBytes': 2})
        runtime._streams = threading.BoundedSemaphore(1)
        response = self.request(Mock(), admit=runtime._admit_read)
        self.assertEqual((response.status, response.body), (502, {'error': 'result_read_failed'}))
        runtime.settings.stream_budget['maxObjectBytes'] = 3
        self.assertTrue(runtime._streams.acquire(blocking=False))
        try:
            response = self.request(Mock(), admit=runtime._admit_read)
            self.assertEqual((response.status, response.body), (503, {'error': 'result_read_unavailable'}))
        finally:
            runtime._streams.release()


if __name__ == '__main__':
    unittest.main()
