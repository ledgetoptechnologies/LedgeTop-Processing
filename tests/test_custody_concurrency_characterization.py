"""Characterize custody shutdown/admission hazards; these are not safety claims."""
import tempfile
import threading
import unittest

from processing.admission_drain import AdmissionDrain
from processing.prototype import Queue
from processing.viewer_service import ViewerService


class _StillRunningFetch:
    def join(self, timeout=None):
        pass  # Model a fetch that outlives the bounded join without sleeping.

    def is_alive(self):
        return True


class CustodyCharacterization(unittest.TestCase):
    def test_current_close_releases_with_live_custody_writer(self):
        service = object.__new__(ViewerService)
        service._stop = threading.Event()
        service._thread = None
        service._lock = threading.Lock()
        service._dispatch_threads = {}
        fetch = _StillRunningFetch()
        service._adopt_threads = {'synthetic': fetch}
        # CoordinatorService calls these before releasing its queue authority.
        service.close()
        service.wait_for_transfers()
        self.assertTrue(fetch.is_alive())

    def test_current_admission_barrier_serializes_independent_work(self):
        with tempfile.TemporaryDirectory() as root:
            queue = Queue(root)
            drain = AdmissionDrain(queue)
            first_entered, release_first = threading.Event(), threading.Event()
            second_attempted, second_entered = threading.Event(), threading.Event()

            def first():
                with drain.admit():
                    first_entered.set()
                    if not release_first.wait(3):
                        raise RuntimeError('test failed to release first admission')

            def second():
                second_attempted.set()
                with drain.admit():
                    second_entered.set()

            a = threading.Thread(target=first)
            b = threading.Thread(target=second)
            try:
                a.start()
                self.assertTrue(first_entered.wait(2))
                b.start()
                self.assertTrue(second_attempted.wait(2))
                self.assertFalse(second_entered.wait(.2))
            finally:
                release_first.set()
                a.join(3)
                if b.ident is not None:
                    b.join(3)
                queue.close()
            self.assertFalse(a.is_alive())
            self.assertFalse(b.is_alive())
            self.assertTrue(second_entered.is_set())


if __name__ == '__main__':
    unittest.main()
