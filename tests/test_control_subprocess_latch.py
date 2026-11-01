"""No real processes or signals: latch and concurrent-spawn rehearsal."""
from concurrent.futures import ThreadPoolExecutor
import subprocess
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from processing import reference_control_subprocess as transport_module


class _UnreapedChild:
    pid = 43210
    returncode = None
    def __init__(self): self.calls = 0
    def communicate(self, *args, **kwargs):
        self.calls += 1
        raise subprocess.TimeoutExpired('synthetic', kwargs.get('timeout', 0))
    def poll(self): return None
    def kill(self): pass


class LatchTests(unittest.TestCase):
    def test_uncertain_reap_latches_before_second_concurrent_spawn(self):
        transport = transport_module.ProcessControlTransport(
            'https://example.test', str(Path.cwd() / 'synthetic-ca.pem'), '0' * 64, deadline=0.5)
        child = _UnreapedChild()
        start = threading.Barrier(2)
        def invoke():
            start.wait(timeout=2)
            try:
                transport.request('GET', '/ltds/worker/v1/next', 'synthetic-token')
            except Exception as error:
                return type(error), str(error)
            return None
        # This is the only reset and exists solely in a fake-process test.
        with patch.object(transport_module, '_PROCESS_HELD', False), \
             patch.object(transport_module, '_PROCESS_UNREAPED', None), \
             patch.object(transport_module.subprocess, 'Popen', return_value=child) as spawn:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(invoke) for _ in range(2)]
                results = [future.result(timeout=3) for future in futures]
            self.assertEqual(spawn.call_count, 1)
            self.assertTrue(transport_module._PROCESS_HELD)
            self.assertIs(transport_module._PROCESS_UNREAPED, child)
            self.assertEqual(transport_module.unreaped_child_status(),
                             {'pid': 43210, 'returncode': None, 'latched': True})
            self.assertTrue(all(result == (transport_module.ControlTransportHeld,
                                           'control_child_reaping_uncertain') or
                                result == (transport_module.ControlTransportUnavailable,
                                           'control_transport_busy') for result in results))
            self.assertIn((transport_module.ControlTransportHeld,
                           'control_child_reaping_uncertain'), results)
            with self.assertRaises(transport_module.ControlTransportHeld):
                transport.request('GET', '/ltds/worker/v1/next', 'synthetic-token')
            self.assertEqual(spawn.call_count, 1)

    def test_busy_lock_is_bounded_without_spawning(self):
        transport = transport_module.ProcessControlTransport(
            'https://example.test', str(Path.cwd() / 'synthetic-ca.pem'), '0' * 64, deadline=0.05)
        with patch.object(transport_module, '_PROCESS_HELD', False), \
             patch.object(transport_module.subprocess, 'Popen') as spawn:
            transport_module._PROCESS_LOCK.acquire()
            try:
                with self.assertRaises(transport_module.ControlTransportUnavailable):
                    transport.request('GET', '/ltds/worker/v1/next', 'synthetic-token')
            finally:
                transport_module._PROCESS_LOCK.release()
            spawn.assert_not_called()

if __name__ == '__main__': unittest.main()
