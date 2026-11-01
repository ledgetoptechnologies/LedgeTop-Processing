import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest

class NetfilterIsolationTests(unittest.TestCase):
    def test_streaming_drain_and_restart_keep_direct_denied(self):
        helper = Path(__file__).with_name('netfilter_gateway_fixture.py')
        net_fd = os.open('/proc/self/ns/net', os.O_RDONLY | os.O_CLOEXEC)
        try:
            user_fd = os.open('/proc/self/ns/user', os.O_RDONLY | os.O_CLOEXEC)
            try:
                child = subprocess.Popen(['unshare', '--user', '--map-root-user', '--net', sys.executable, str(helper), str(os.getpid()), str(net_fd), str(user_fd), 'run'],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True, pass_fds=(net_fd, user_fd))
            finally:
                os.close(user_fd)
        finally:
            os.close(net_fd)
        try:
            deadline = time.monotonic() + 25
            while True:
                result = os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if result is not None: break
                if time.monotonic() >= deadline: raise TimeoutError('owned fixture timed out')
                time.sleep(0.02)
            # WNOWAIT leaves leader as a zombie, reserving its pid/pgid until
            # group cleanup has been attempted, including after leader death.
            try: os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError: pass
            out, err = child.communicate(timeout=3)
            self.assertEqual(child.returncode, 0, err)
            self.assertEqual(json.loads(out), {'directDeniedBeforeDuringAfterRestart': True, 'streamAndDrainSucceeded': True})
        finally:
            if child.returncode is None:
                try: os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                child.communicate(timeout=3)








if __name__ == '__main__': unittest.main()
