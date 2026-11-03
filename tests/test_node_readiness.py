import unittest

from processing.node_readiness import NodeReadiness


class NodeReadinessTests(unittest.TestCase):
    def test_expiry_invalid_observation_and_clock_regression_fail_closed(self):
        now = [100.0]
        cache = NodeReadiness(('small',), ttl_seconds=5, clock=lambda: now[0])
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': 1000, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        self.assertIsNone(cache.get('small'))
        cache.publish('small', ready, ticket=cache.begin('small'))
        ready['maxImages'] = 5
        result = cache.get('small')
        self.assertEqual(result['maxImages'], 1000)
        result['ready'] = False
        self.assertTrue(cache.get('small')['ready'])
        now[0] = 99
        self.assertIsNone(cache.get('small'))
        now[0] = 105
        self.assertIsNone(cache.get('small'))
        cache.publish('small', {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}, ticket=cache.begin('small'))
        with self.assertRaises(ValueError):
            cache.publish('small', {'ready': True, 'taskQueueCount': False, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}, ticket=cache.begin('small'))
        self.assertIsNone(cache.get('small'))
        with self.assertRaises(ValueError):
            cache.get('other')

    def test_late_or_replayed_probe_cannot_overwrite_newer_result(self):
        cache = NodeReadiness(('small',))
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                 'availableScratchBytes': 1000, 'outputBudgetBytes': 100}
        older = cache.begin('small')
        newer = cache.begin('small')
        cache.publish('small', {**ready, 'ready': False}, ticket=newer)
        with self.assertRaises(ValueError):
            cache.publish('small', ready, ticket=older)
        with self.assertRaises(ValueError):
            cache.publish('small', ready, ticket=newer)
        self.assertFalse(cache.get('small')['ready'])


if __name__ == '__main__':
    unittest.main()
