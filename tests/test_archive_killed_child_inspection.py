import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from processing.maintenance import Conflict
from processing.viewer_service import EngineUnavailable, ViewerService


class KilledCaptureTests(unittest.TestCase):
    def run_killed_capture(self, directory, *, broken_pipe=False, shutdown=False):
        task = dict(uuid='test-task', generation=1, job_id='job', node_id='cluster',
                    engine_uuid='engine', environment='production', directory=str(directory),
                    archive_path=None, job={'status': 'succeeded'})
        service = object.__new__(ViewerService)
        service._downloads_lock = threading.Lock()
        service._downloads = {}
        service._stop = threading.Event()
        service._archive_registry = False
        service.store = Mock()
        service.store.task.return_value = task
        service.queue = SimpleNamespace(_lock=SimpleNamespace(closed=False, fileno=lambda: 8),
                                        authority_lease=SimpleNamespace(_descriptor=9))
        service.config = SimpleNamespace(local_node_url='http://127.0.0.1', local_node_token='',
                                         viewer_max_archive_bytes=1000, viewer_archive_free_floor=1)
        child = Mock()
        child.stdin.closed = False
        child.stdin.fileno.return_value = 10
        child.poll.return_value = -9
        child.wait.return_value = -9
        def write(_fd, data):
            if shutdown:
                service._stop.set()
            if broken_pipe:
                raise BrokenPipeError('synthetic pipe failure')
            return len(data)
        with patch('processing.viewer_service.subprocess.Popen', return_value=child), \
             patch('processing.viewer_service.os.set_blocking'), \
             patch('processing.viewer_service.os.write', side_effect=write):
            service._archive_once_locked('test-task', expected_identity=(1, 'job'))

    def test_signal_with_partial_reports_inspection_and_keeps_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            partial = Path(temp) / 'all.zip.partial'
            partial.write_bytes(b'synthetic partial')
            with self.assertRaisesRegex(Conflict, 'partial_requires_inspection'):
                self.run_killed_capture(temp)
            self.assertEqual(partial.read_bytes(), b'synthetic partial')

    def test_signal_before_slot_creation_stays_retryable(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(EngineUnavailable):
                self.run_killed_capture(temp)
            self.assertEqual(list(Path(temp).iterdir()), [])

    def test_broken_pipe_and_shutdown_preserve_partial_for_inspection(self):
        for mode in ('broken_pipe', 'shutdown'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                partial = Path(temp) / 'all.zip.partial'
                partial.write_bytes(b'partial')
                with self.assertRaisesRegex(Conflict, 'partial_requires_inspection'):
                    self.run_killed_capture(temp, **{mode: True})
                self.assertEqual(partial.read_bytes(), b'partial')

    def test_unreadable_slot_is_conservatively_reported_for_inspection(self):
        with tempfile.TemporaryDirectory() as temp, \
             patch('pathlib.Path.lstat', side_effect=PermissionError('synthetic outage')):
            with self.assertRaisesRegex(Conflict, 'partial_requires_inspection'):
                self.run_killed_capture(temp)


if __name__ == '__main__':
    unittest.main()
