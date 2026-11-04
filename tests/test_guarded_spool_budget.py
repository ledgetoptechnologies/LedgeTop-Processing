import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from processing.guarded_spool_budget import GuardedSpoolBudget, SpoolBudgetError


@unittest.skipIf(os.name == 'nt', 'node-local spool reservations require POSIX')
class GuardedSpoolBudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.budget = GuardedSpoolBudget(self.root, cap=10, floor=0)

    def name(self, digit): return 'incoming-' + digit * 48

    def test_concurrent_connections_cannot_overreserve(self):
        barrier = threading.Barrier(2); outcomes = []
        def claim(digit):
            ledger = GuardedSpoolBudget(self.root, cap=10, floor=0)
            barrier.wait(timeout=5)
            try:
                ledger.reserve(self.name(digit), 6); outcomes.append('ok')
            except SpoolBudgetError:
                outcomes.append('held')
        threads = [threading.Thread(target=claim, args=(digit,)) for digit in ('a', 'b')]
        for thread in threads: thread.start()
        for thread in threads: thread.join(10); self.assertFalse(thread.is_alive())
        self.assertCountEqual(outcomes, ['ok', 'held'])
        self.assertEqual(sum(x['bytes'] for x in self.budget.inspect()['reservations']), 6)

    def test_headroom_and_unknown_file_refuse_new_reservations(self):
        with patch('processing.guarded_spool_budget.os.statvfs', return_value=SimpleNamespace(f_bavail=1, f_frsize=1)):
            with self.assertRaises(SpoolBudgetError): self.budget.reserve(self.name('a'), 2)
        (self.root / self.name('b')).write_bytes(b'abc')
        self.assertEqual(self.budget.inspect()['untracked'], [self.name('b')])
        with self.assertRaises(SpoolBudgetError): self.budget.reserve(self.name('a'), 1)

    def test_conflicting_process_policy_cannot_expand_quota(self):
        with self.assertRaises(SpoolBudgetError): GuardedSpoolBudget(self.root, cap=100, floor=0)
        with self.assertRaises(SpoolBudgetError): GuardedSpoolBudget(self.root, cap=10, floor=1)

    def test_process_death_at_each_stage_keeps_charge_without_deletion(self):
        script = '''
import os, sys
from pathlib import Path
from processing.guarded_spool_budget import GuardedSpoolBudget
root, name, stage = sys.argv[1:]
ledger = GuardedSpoolBudget(root, cap=10, floor=0)
ledger.reserve(name, 3)
if stage != 'receiving':
    Path(root, name).write_bytes(b'abc')
    ledger.transition(name, 'receiving', 'spooled')
if stage == 'mutation_started': ledger.transition(name, 'spooled', 'mutation_started')
os._exit(37)
'''
        for i, stage in enumerate(('receiving', 'spooled', 'mutation_started')):
            name = self.name('abc'[i])
            result = subprocess.run([sys.executable, '-c', script, str(self.root), name, stage], timeout=10)
            self.assertEqual(result.returncode, 37)
        reopened = GuardedSpoolBudget(self.root, cap=10, floor=0)
        report = reopened.inspect()['reservations']
        self.assertEqual({r['stage'] for r in report}, {'receiving', 'spooled', 'mutation_started'})
        self.assertEqual(sum(r['bytes'] for r in report), 9)
        self.assertEqual((self.root / self.name('c')).read_bytes(), b'abc')
        with self.assertRaises(SpoolBudgetError): reopened.reserve(self.name('d'), 2)

    def test_discard_release_refuses_existing_file_and_reopens(self):
        name = self.name('a'); self.budget.reserve(name, 3)
        file = self.root / name; file.write_bytes(b'abc')
        self.budget.transition(name, 'receiving', 'discardable')
        root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with self.assertRaises(SpoolBudgetError): self.budget.release_discarded(name, root_fd)
            file.unlink()
            GuardedSpoolBudget(self.root, cap=10, floor=0).release_discarded(name, root_fd)
        finally:
            os.close(root_fd)
        self.assertEqual(self.budget.inspect()['reservations'], [])

    def test_discard_release_refuses_foreign_or_replaced_directory(self):
        name = self.name('a'); self.budget.reserve(name, 3)
        (self.root / name).write_bytes(b'abc')
        self.budget.transition(name, 'receiving', 'discardable')
        with tempfile.TemporaryDirectory() as foreign:
            fd = os.open(foreign, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaises(SpoolBudgetError):
                    self.budget.release_discarded(name, fd)
            finally:
                os.close(fd)
        self.assertEqual(self.budget.inspect()['reservations'][0]['bytes'], 3)
        self.assertEqual((self.root / name).read_bytes(), b'abc')

        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        moved = self.root.with_name(self.root.name + '-moved')
        try:
            self.root.rename(moved)
            self.root.mkdir()
            with self.assertRaises(SpoolBudgetError):
                self.budget.release_discarded(name, fd)
            replacement_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaises(SpoolBudgetError):
                    self.budget.release_discarded(name, replacement_fd)
            finally:
                os.close(replacement_fd)
        finally:
            os.close(fd)
            self.root.rmdir()
            moved.rename(self.root)
        self.assertEqual(self.budget.inspect()['reservations'][0]['bytes'], 3)

    def test_crash_after_discard_unlink_keeps_charge_until_explicit_reconciliation(self):
        name = self.name('e')
        script = '''
import os, sys
from pathlib import Path
from processing.guarded_spool_budget import GuardedSpoolBudget
root, name = sys.argv[1:]
ledger = GuardedSpoolBudget(root, cap=10, floor=0)
ledger.reserve(name, 3)
Path(root, name).write_bytes(b'abc')
ledger.transition(name, 'receiving', 'spooled')
ledger.transition(name, 'spooled', 'mutation_started')
ledger.transition(name, 'mutation_started', 'discardable')
fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
os.unlink(name, dir_fd=fd)
os.fsync(fd)
os._exit(37)
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.root), name], timeout=10)
        self.assertEqual(result.returncode, 37)
        reopened = GuardedSpoolBudget(self.root, cap=10, floor=0)
        self.assertEqual(reopened.inspect()['reservations'],
                         [{'name': name, 'bytes': 3, 'stage': 'discardable', 'present': False}])
        root_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            reopened.release_discarded(name, root_fd)
            with self.assertRaises(SpoolBudgetError): reopened.release_discarded(name, root_fd)
        finally:
            os.close(root_fd)
        self.assertEqual(reopened.inspect()['reservations'], [])


if __name__ == '__main__': unittest.main()
