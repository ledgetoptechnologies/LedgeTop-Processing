import unittest

from processing.node_readiness import NodeReadiness


class NodeReadinessTests(unittest.TestCase):
    def test_expiry_invalid_observation_and_clock_regression_fail_closed(self):
        now = [100.0]
        cache = NodeReadiness(('small',), ttl_seconds=5, clock=lambda: now[0])
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': 1000, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        self.assertIsNone(cache.get('small'))
        cache.publish('small', ready)
        ready['maxImages'] = 5
        result = cache.get('small')
        self.assertEqual(result['maxImages'], 1000)
        result['ready'] = False
        self.assertTrue(cache.get('small')['ready'])
        now[0] = 99
        self.assertIsNone(cache.get('small'))
        now[0] = 105
        self.assertIsNone(cache.get('small'))
        cache.publish('small', {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
        with self.assertRaises(ValueError):
            cache.publish('small', {'ready': True, 'taskQueueCount': False, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
        self.assertIsNone(cache.get('small'))
        with self.assertRaises(ValueError):
            cache.get('other')


if __name__ == '__main__':
    unittest.main()
