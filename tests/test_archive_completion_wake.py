import threading
import unittest
from unittest.mock import Mock

from processing.viewer_service import ViewerService


class ArchiveCompletionWakeTests(unittest.TestCase):
    def test_close_racing_wake_clear_stops_before_scanning_or_waiting(self):
        service = object.__new__(ViewerService)
        service._stop = threading.Event()
        service._archive_wake = threading.Event()
        service._archive_cursor = 0
        service.store = Mock()
        original_clear = service._archive_wake.clear

        def close_then_clear():
            service._stop.set()
            service._archive_wake.set()
            original_clear()

        service._archive_wake.clear = Mock(side_effect=close_then_clear)
        service._archive_wake.wait = Mock()
        service._archive_run()
        service.store.next_unarchived_success.assert_not_called()
        service._archive_wake.wait.assert_not_called()

    def test_metadata_outage_is_retried_without_claiming_healthy(self):
        service = object.__new__(ViewerService)
        service._stop = threading.Event()
        service._archive_wake = threading.Event()
        service._archive_cursor = 0
        service._archive_retry = {}
        service._archive_status_persistence_failed = False
        service._archive_child_unreaped = threading.Event()
        service.store = Mock()
        service.store.next_unarchived_success.side_effect = [OSError('synthetic outage'), (1, 'task')]
        service.store.task.return_value = {'generation': 1, 'job_id': 'job'}
        service._archive_once = Mock(side_effect=lambda *_args, **_kwargs: service._stop.set())
        service._stop.wait = Mock(return_value=False)

        service._archive_run()

        self.assertTrue(service._archive_status_persistence_failed)
        service._archive_once.assert_called_once_with('task', expected_identity=(1, 'job'))
        service.store.set_archive_status.assert_not_called()

    def test_completion_racing_empty_scan_wakes_capture_without_timeout(self):
        service = object.__new__(ViewerService)
        service._stop = threading.Event()
        service._archive_wake = threading.Event()
        service._archive_cursor = 0
        service._archive_retry = {}
        service._archive_status_persistence_failed = False
        service._archive_child_unreaped = threading.Event()
        service.store = Mock()
        scans = []

        def scan(_cursor):
            scans.append(1)
            if len(scans) == 1:
                # Simulate a success commit after the scan observed no row,
                # but before the archive thread has begun waiting.
                service._archive_wake.set()
                return None
            return (1, 'synthetic-task')

        service.store.next_unarchived_success.side_effect = scan
        service.store.task.return_value = {'generation': 1, 'job_id': 'synthetic-job'}
        service._archive_once = Mock(side_effect=lambda *_args, **_kwargs: service._stop.set())
        original_wait = service._archive_wake.wait
        results = []
        def wait_without_delay(_timeout):
            result = original_wait(0)
            results.append(result)
            return result
        service._archive_wake.wait = Mock(side_effect=wait_without_delay)

        service._archive_run()

        self.assertEqual(results, [True])
        self.assertEqual(len(scans), 2)
        service._archive_once.assert_called_once_with('synthetic-task', expected_identity=(1, 'synthetic-job'))


if __name__ == '__main__':
    unittest.main()
