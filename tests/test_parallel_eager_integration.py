"""Cross-feature check for concurrent node dispatch and slow result custody."""
import threading
import unittest

from tests import test_eager_custody_gap as eager_fixture
from tests import test_viewer_parallel_dispatch as parallel_fixture

WAIT = parallel_fixture.WAIT
wait_until = parallel_fixture.wait_until


class ParallelEagerIntegration(parallel_fixture.Harness):
    def open_queue(self):
        self.config.viewer_eager_result_custody = True
        self.config.viewer_eager_custody_reserve_bytes = 1
        self.config.viewer_eager_custody_max_bytes = 1024 ** 3
        for engine in self.engines.values():
            eager_fixture.EagerCustody.reports_identity(engine)
        super().open_queue()

    def test_slow_custody_does_not_block_other_node_and_shutdown_waits(self):
        first = self.submit()
        self.service.tick()
        self.phase(first, 'processing')
        self.assertEqual(self.task(first)['node_id'], 'cluster')
        second = self.submit()
        self.service.tick()
        self.phase(second, 'processing')
        self.assertEqual(self.task(second)['node_id'], 'main')

        first_engine = self.engines['cluster']
        blocked = eager_fixture.Blocked(first_engine)
        self.addCleanup(blocked.undo)
        first_engine.finish(self.task(first)['engine_uuid'])
        self.service.tick()
        self.assertTrue(blocked.reading.wait(WAIT))
        self.assertEqual(self.task(first)['job']['status'], 'succeeded')
        self.assertEqual(self.task(second)['job']['status'], 'running')

        third = self.submit()
        self.service.tick()
        self.phase(third, 'processing')
        self.assertEqual(self.task(third)['node_id'], 'cluster')
        self.assertEqual(self.task(second)['job']['status'], 'running')

        done = threading.Event()
        waiter = threading.Thread(target=lambda: (self.service.wait_for_transfers(), done.set()))
        waiter.start()
        try:
            self.assertFalse(done.wait(0.1), 'shutdown wait must retain authority during custody write')
        finally:
            blocked.release.set()
            waiter.join(WAIT)
        self.assertTrue(done.is_set())
        self.assertTrue(wait_until(lambda: self.service.info(first).get('ledgetopResultCustody') == {'state': 'verified'}))


if __name__ == '__main__':
    unittest.main()
