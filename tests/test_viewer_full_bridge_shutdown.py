"""Bounded, synthetic-only fixture shutdown checks."""
import threading
import unittest
from unittest.mock import Mock, patch

from tests.viewer_full_bridge_server import stop_worker


class FullBridgeShutdownTests(unittest.TestCase):
    def test_cooperative_worker_is_joined(self):
        cancel = threading.Event()
        worker = threading.Thread(target=cancel.wait, daemon=True)
        worker.start()
        stop_worker(worker, cancel, timeout=1)
        self.assertTrue(cancel.is_set())
        self.assertFalse(worker.is_alive())

    def test_stuck_worker_forces_owned_fixture_process_exit(self):
        cancel = threading.Event()
        worker = Mock()
        worker.is_alive.return_value = True
        with patch('tests.viewer_full_bridge_server.os.write'), patch(
            'tests.viewer_full_bridge_server.os._exit', side_effect=RuntimeError('forced exit')
        ) as exit_process:
            with self.assertRaisesRegex(RuntimeError, 'forced exit'):
                stop_worker(worker, cancel, timeout=0.01)
        worker.join.assert_called_once_with(0.01)
        exit_process.assert_called_once_with(2)
        self.assertTrue(cancel.is_set())
