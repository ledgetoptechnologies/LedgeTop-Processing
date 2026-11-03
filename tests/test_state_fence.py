import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from ops import state_fence

POSIX = os.name == 'posix'


@unittest.skipUnless(POSIX, 'flock and /proc/locks are Linux-only')
class StateFenceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.states = []
        for name in ('original', 'verify'):
            state = Path(self.temporary.name) / name
            state.mkdir()
            (state / 'coordinator.lock').write_bytes(b'')
            self.states.append(str(state))
        self.holders = []

    def tearDown(self):
        for holder in self.holders:
            holder.kill()
            holder.wait()
        self.temporary.cleanup()

    def start(self, *states):
        holder = subprocess.Popen([sys.executable, '-B', 'ops/state_fence.py', *states],
                                  stdout=subprocess.PIPE, text=True)
        self.holders.append(holder)
        self.assertIn('"fenced"', holder.stdout.readline())
        return holder

    def d738_lock_attempt(self, state):
        """The exact lock acquisition d738's Queue makes before touching its database."""
        import fcntl
        with open(Path(state) / 'coordinator.lock', 'a+b') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return 'acquired'
            except OSError:
                return 'prototype state already owned'

    def test_holder_blocks_d738_lock_and_refuses_owned_state(self):
        self.start(*self.states)
        for state in self.states:
            self.assertEqual(self.d738_lock_attempt(state), 'prototype state already owned')
        second = subprocess.run([sys.executable, '-B', 'ops/state_fence.py', self.states[0]],
                                capture_output=True, text=True)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn('owned by a running coordinator', second.stderr)

    def test_holder_never_creates_a_lock_file(self):
        empty = Path(self.temporary.name) / 'empty'
        empty.mkdir()
        result = subprocess.run([sys.executable, '-B', 'ops/state_fence.py', str(empty)], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((empty / 'coordinator.lock').exists())

    def test_verify_proves_current_ownership_of_every_lock(self):
        self.start(*self.states)
        result, ok = state_fence.verify(self.states)
        self.assertTrue(ok, result)
        self.assertEqual(result, {'holders': 1, 'locks': 2, 'held': 2})

    def test_verify_fails_when_holder_is_gone_or_holds_only_some(self):
        holder = self.start(self.states[0])
        result, ok = state_fence.verify(self.states)
        self.assertFalse(ok)
        self.assertEqual(result['held'], 1)
        holder.kill()
        holder.wait()
        time.sleep(0.2)
        result, ok = state_fence.verify(self.states[:1])
        self.assertFalse(ok)
        self.assertEqual(result['holders'], 0)

    def test_verify_does_not_credit_a_lock_held_by_someone_else(self):
        import fcntl
        with open(Path(self.states[0]) / 'coordinator.lock', 'a+b') as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)  # e.g. a running d738
            result, ok = state_fence.verify(self.states[:1])
        self.assertFalse(ok)
        self.assertEqual(result['held'], 0)

    def test_parser_ignores_waiters_and_shared_or_posix_locks(self):
        text = ('1: FLOCK  ADVISORY  WRITE 42 00:2f:100 0 EOF\n'
                '1: -> FLOCK  ADVISORY  WRITE 43 00:2f:100 0 EOF\n'
                '2: FLOCK  ADVISORY  READ  44 00:2f:101 0 EOF\n'
                '3: POSIX  ADVISORY  WRITE 45 00:2f:102 0 EOF\n')
        self.assertEqual(state_fence.held_flocks(text), {(0, 0x2f, 100): {42}})


if __name__ == '__main__':
    unittest.main()
