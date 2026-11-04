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

    def test_probe_ttl_starts_before_network_request(self):
        now = [100.0]
        cache = NodeReadiness(('small',), ttl_seconds=5, clock=lambda: now[0])
        report = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                  'availableScratchBytes': 1000, 'outputBudgetBytes': 100}
        ticket = cache.begin('small')
        now[0] = 105
        with self.assertRaises(ValueError):
            cache.publish('small', report, ticket=ticket)
        self.assertIsNone(cache.get('small'))

    def test_status_separates_unknown_from_expired_and_keeps_ticket_fencing(self):
        now = [100.0]
        cache = NodeReadiness(('small',), ttl_seconds=5, clock=lambda: now[0])
        report = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                  'availableScratchBytes': 1000, 'outputBudgetBytes': 100}
        expired = {'ready': False, 'reason': 'readiness_expired'}
        self.assertIsNone(cache.status('small'))  # Startup is bounded the same way.
        now[0] = 105
        self.assertEqual(cache.status('small'), expired)
        cache.publish('small', report, ticket=cache.begin('small'))
        self.assertEqual(cache.status('small'), report)
        older = cache.begin('small')
        self.assertIsNone(cache.get('small'))
        self.assertIsNone(cache.status('small'))  # Mid-probe: unknown, never busy.
        newer = cache.begin('small')
        with self.assertRaises(ValueError):
            cache.publish('small', report, ticket=older)
        self.assertIsNone(cache.status('small'))
        now[0] = 109.9  # A probe that never resolves does not refresh the evidence.
        cache.begin('small')
        self.assertIsNone(cache.status('small'))
        now[0] = 110
        self.assertEqual(cache.status('small'), expired)
        cache.begin('small')
        self.assertEqual(cache.status('small'), expired)
        now[0] = 111
        cache.publish('small', report, ticket=cache.begin('small'))
        self.assertEqual(cache.status('small'), report)
        cache.invalidate('small')
        self.assertIsNone(cache.status('small'))
        now[0] = 99
        self.assertIsNone(cache.status('small'))
        with self.assertRaises(ValueError):
            cache.status('other')


if __name__ == '__main__':
    unittest.main()
