"""Independent release regression for lost-ACK cancellation recovery."""

import unittest

from tests import test_viewer_service as fixtures


class ViewerRecoveryReviewTests(unittest.TestCase):
    def test_lost_commit_ack_running_readback_restores_cancellation(self):
        fixture = fixtures.ViewerServiceTests()
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        identifier = fixture.submit()
        next_identifier = fixture.submit()
        fixture.engines['cluster'].fail = 'commit'
        fixture.service.tick()
        self.assertEqual(fixture.task(identifier)['phase'], 'commit_pending')
        self.assertEqual(fixture.task(identifier)['job']['status'], 'running')

        # The engine started exactly once despite the lost mutation response.
        # A matching read-only code20 proves cancellation can target that task.
        fixture.engines['cluster'].fail = None
        fixture.service.tick()
        self.assertEqual(fixture.task(identifier)['phase'], 'processing')
        fixture.service.cancel(identifier)
        fixture.service.cancel(identifier)
        fixture.service.tick()
        self.assertEqual(fixture.task(identifier)['job']['status'], 'running')
        self.assertIsNone(fixture.task(next_identifier)['node_id'])
        actions = fixture.engines['cluster'].calls
        self.assertEqual(sum(call[0] == 'commit' for call in actions), 1)
        self.assertEqual(sum(call[0] == 'cancel' for call in actions), 1)

        fixture.finish(identifier, 50)
        fixture.service.tick()
        # NodeODM's cancelled status alone does not prove its processes have
        # stopped. Keep the global slot held until termination is verified.
        info = fixture.service.info(identifier)
        self.assertEqual(info['status']['code'], 20)
        self.assertTrue(info['ledgetopHeld'])
        self.assertEqual(fixture.task(identifier)['job']['status'], 'held')
        self.assertEqual(fixture.task(identifier)['error'], 'cancel_termination_unverified')
        self.assertIsNone(fixture.task(next_identifier)['node_id'])
        self.assertEqual(fixture.task(next_identifier)['job']['status'], 'queued')
        self.assertEqual(sum(call[0] == 'commit' for call in actions), 1)
        self.assertEqual(sum(call[0] == 'cancel' for call in actions), 1)
        self.assertEqual(fixture.fleet.max_active, 1)


if __name__ == '__main__':
    unittest.main()
