import json
import http.client
import threading
import unittest
from unittest.mock import patch

from processing.reference_binary import BinaryResponse
from processing.reference_result_api import ReferenceResultAPI, PREFIX
from processing.reference_http import create_reference_http_server
from tests.test_reference_results import ResultsFixture


TOKEN = 'synthetic-result-read-token-0001'
VIEWER = 'synthetic-viewer-token-0002'


class ReferenceResultAPITests(ResultsFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.policy_observations = []
        self.api = self.make_api()

    def make_api(self, *, policy=None, environments=('staging',)):
        def approve(metadata):
            self.policy_observations.append(metadata)
            return metadata['environment'] == 'staging'

        return ReferenceResultAPI(self.results, bearer_readers={TOKEN: dict(node_id='cluster', worker_id='worker',
            environments=environments, share_policy=approve if policy is None else policy)})

    def request(self, *, token=TOKEN, protected=True, extra_headers=None, body=b'', path=None):
        headers = {'Authorization': 'Bearer ' + token, 'X-Result-Generation': '1',
            'X-Result-Sha256': self.object['sha256'], 'X-Result-Manifest-Sha256': self.metadata['resultManifestSha256']}
        headers.update(extra_headers or {})
        return self.api.handle('GET', path or PREFIX + '/jobs/' + self.job['jobId'] + '/results/' + self.object['id'],
                               headers, body, protected_transport=protected)

    def test_actual_share_decision_before_viewer_acceptance_returns_pinned_binary(self):
        response = self.request()
        self.assertIsInstance(response, BinaryResponse)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers['X-Result-Sha256'], self.object['sha256'])
        self.assertEqual(response.headers['X-Result-Manifest-Sha256'], self.metadata['resultManifestSha256'])
        self.assertEqual(self.active_pins(), 1)
        self.assertEqual(b''.join(response.stream.chunks()), self.engine.archive)
        self.assertEqual(self.active_pins(), 0)
        self.assertEqual(self.policy_observations[0]['requestedObjectId'], self.object['id'])
        encoded = json.dumps(self.policy_observations)
        for private in (str(self.root), 'claimToken', 'localObjects', 'ownedPrefix', TOKEN):
            self.assertNotIn(private, encoded)
            self.assertNotIn(private, repr(response))

    def test_separate_bearer_protected_transport_and_explicit_share_policy(self):
        self.assertEqual(self.request(protected=False).status, 503)
        self.assertEqual(self.request(token=VIEWER).status, 401)
        self.api = self.make_api(policy=lambda _: False)
        self.assertEqual(self.request().status, 403)
        self.api = self.make_api(policy=lambda _: 1)
        self.assertEqual(self.request().status, 403)
        self.api = self.make_api(environments=('production',))
        self.assertEqual(self.request().status, 404)
        self.assertEqual(self.active_pins(), 0)

    def test_readiness_requires_protected_authenticated_reader(self):
        observation = {'ready': True, 'taskQueueCount': 0, 'maxImages': 1000,
                       'availableScratchBytes': 4096, 'outputBudgetBytes': 1024}
        self.api = ReferenceResultAPI(self.results, bearer_readers={TOKEN: dict(
            node_id='cluster', worker_id='worker', environments=('staging',),
            share_policy=lambda _: True)}, readiness_provider=lambda: observation)
        path = PREFIX + '/readiness'
        self.assertEqual(self.request(path=path, protected=False).status, 503)
        self.assertEqual(self.request(path=path, token=VIEWER).status, 401)
        response = self.request(path=path)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, {'nodeId': 'cluster', 'workerId': 'worker',
                                        'observation': observation})
        self.api.readiness_provider = lambda: None
        self.assertEqual(self.request(path=path).status, 503)

    def test_required_generation_object_and_manifest_bindings(self):
        for headers, expected in (({'X-Result-Generation': '2'}, 409), ({'X-Result-Sha256': '0' * 64}, 409),
                                  ({'X-Result-Manifest-Sha256': '0' * 64}, 409), ({'X-Result-Generation': '0'}, 400),
                                  ({'X-Result-Sha256': '../secret'}, 400), ({'x-result-generation': '1'}, 400),
                                  ({'authorization': 'Bearer ' + TOKEN}, 401)):
            with self.subTest(headers=headers):
                response = self.request(extra_headers=headers)
                self.assertEqual(response.status, expected)
                self.assertNotIn(TOKEN, json.dumps(response.body))
        self.assertEqual(self.active_pins(), 0)

    def test_no_caller_paths_environment_or_claim_routes(self):
        self.assertEqual(self.request(body=b'{"path":"C:/private/result","environment":"production"}').status, 400)
        self.assertEqual(self.request(path=PREFIX + '/claim').status, 404)
        self.assertEqual(self.request(path=PREFIX + '/jobs/' + self.job['jobId'] + '/results/../secret').status, 404)
        self.assertEqual(self.request(path=PREFIX + '/jobs/' + self.job['jobId'] + '/results/' + self.object['id'] + '?path=secret').status, 404)

    def test_ranges_and_corrupt_file_errors_remain_path_free(self):
        response = self.request(extra_headers={'Range': 'bytes=0-9'})
        self.assertIsInstance(response, BinaryResponse)
        self.assertEqual(response.status, 206)
        self.assertEqual(b''.join(response.stream.chunks()), self.engine.archive[:10])
        response = self.request(extra_headers={'Range': 'bytes=999999-'})
        self.assertEqual(response.status, 416)
        self.assertEqual(response.headers['Content-Range'], 'bytes */' + str(self.object['byteSize']))
        self.archive.write_bytes(b'corrupted')
        response = self.request()
        self.assertEqual((response.status, response.body), (409, {'error': 'result_unavailable'}))
        self.assertNotIn(str(self.root), json.dumps(response.body))
        self.assertEqual(self.active_pins(), 0)

    def test_fixed_worker_identity_and_policy_configuration_required(self):
        for config in (dict(node_id='other', worker_id='worker', environments=('staging',), share_policy=lambda _: True),
                       dict(node_id='cluster', worker_id='worker', environments=('staging',), share_policy=True)):
            with self.assertRaises(ValueError):
                ReferenceResultAPI(self.results, bearer_readers={TOKEN: config})

    def test_real_local_http_binary_factory_streams_and_releases_read_pin(self):
        closed = threading.Event()
        original = self.results.open

        def opened(*args, **kwargs):
            stream = original(*args, **kwargs)
            release = stream._release

            def released():
                release()
                closed.set()

            stream._release = released
            return stream

        server = create_reference_http_server(self.api)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)
        thread.start()
        try:
            with patch.object(self.results, 'open', side_effect=opened):
                connection = http.client.HTTPConnection(*server.server_address, timeout=2)
                connection.request('GET', PREFIX + '/jobs/' + self.job['jobId'] + '/results/' + self.object['id'],
                    headers={'Authorization': 'Bearer ' + TOKEN, 'X-Result-Generation': '1',
                        'X-Result-Sha256': self.object['sha256'],
                        'X-Result-Manifest-Sha256': self.metadata['resultManifestSha256']})
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.getheader('Content-Type'), 'application/zip')
                self.assertEqual(response.read(), self.engine.archive)
                connection.close()
                self.assertTrue(closed.wait(timeout=2))
                self.assertEqual(self.active_pins(), 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
