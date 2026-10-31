"""Deterministic tests of the real worker service scheduling loop."""
import unittest
from types import SimpleNamespace

from ops.reference_worker_daemon import (
    FETCH_YIELD_SECONDS, MAX_IMMEDIATE_FETCH_STEPS, WorkerService,
)


class Stop:
    def __init__(self):
        self.stopped = False
        self.waits = []

    def is_set(self):
        return self.stopped

    def wait(self, seconds):
        self.waits.append(seconds)
        return self.stopped


class ReferenceWorkerProgressTests(unittest.TestCase):
    def service(self, driver):
        return WorkerService(driver, SimpleNamespace(), poll_seconds=1)

    def test_ten_thousand_fetch_steps_use_bounded_yields(self):
        stop = Stop()
        class Driver:
            calls = 0
            def step(self):
                self.calls += 1
                if self.calls == 10_000:
                    stop.stopped = True
                return {'state': 'fetching'}
        driver = Driver()
        self.service(driver)._drive(stop)
        self.assertEqual(driver.calls, 10_000)
        self.assertEqual(len(stop.waits), 10_000 // MAX_IMMEDIATE_FETCH_STEPS)
        self.assertEqual(set(stop.waits), {FETCH_YIELD_SECONDS})

    def test_retry_idle_and_nonfetch_states_keep_original_poll_and_backoff(self):
        stop = Stop()
        states = iter(('retry_wait', 'retry_wait', 'fetching', 'idle', 'held', 'executing'))
        class Driver:
            def step(self):
                value = next(states)
                if value == 'executing':
                    stop.stopped = True
                return {'state': value}
        self.service(Driver())._drive(stop)
        self.assertEqual(stop.waits, [2, 4, 1, 2, 1])

    def test_stop_during_yield_prevents_next_fetch(self):
        class StopOnWait(Stop):
            def wait(self, seconds):
                result = super().wait(seconds)
                self.stopped = True
                return result
        stop = StopOnWait()
        class Driver:
            calls = 0
            def step(self):
                self.calls += 1
                return {'state': 'fetching'}
        driver = Driver()
        self.service(driver)._drive(stop)
        self.assertEqual(driver.calls, MAX_IMMEDIATE_FETCH_STEPS)
        self.assertEqual(stop.waits, [FETCH_YIELD_SECONDS])


if __name__ == '__main__':
    unittest.main()
