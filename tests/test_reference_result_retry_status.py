"""Reference result availability is retryable and does not reveal internals."""
import unittest
from unittest.mock import Mock, patch

from processing.reference_api import ReferenceAPI
from processing.reference_result_proxy import ResultProxyError


class ResultRetryStatusTests(unittest.TestCase):
    def test_assigned_worker_result_open_failure_is_retryable(self):
        store = Mock()
        job = {'environment': 'staging', 'resultManifest': {'objects': [{'id': 'archive'}]}}
        store.get.return_value = job
        proxy = Mock()
        proxy.open.side_effect = ResultProxyError('synthetic credential and private path')
        with patch('processing.reference_terminal.TerminalService'):
            api = ReferenceAPI(store, bearer_tokens={'synthetic-staging-token': 'staging'},
                capability_fingerprint='a' * 64, options_fingerprint='b' * 64, result_proxy=proxy)
        response = api.handle('GET', '/ltds/v1/jobs/example/results/archive',
            {'Authorization': 'Bearer synthetic-staging-token'}, protected_transport=True)
        self.assertEqual((response.status, response.body), (503, {'error': 'result_read_unavailable'}))
        self.assertEqual(store.get.call_count, 2)
        self.assertEqual(proxy.open.call_count, 1)


if __name__ == '__main__':
    unittest.main()
