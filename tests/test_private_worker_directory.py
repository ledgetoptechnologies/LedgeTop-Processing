import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from ops.private_worker_directory import private_worker_directory
from ops.reference_worker_daemon import DaemonStateLock, _STATE_OWNERS


@unittest.skipUnless(os.name == 'posix', 'Linux directory publication and ownership')
class PrivateWorkerDirectoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_nested_creation_syncs_entire_chain_bottom_up_and_keeps_private_modes(self):
        target = self.root / 'first' / 'second' / 'state'
        synced = []
        real = os.fsync
        def sync(fd):
            synced.append((os.fstat(fd).st_dev, os.fstat(fd).st_ino))
            real(fd)
        with patch('ops.private_worker_directory.os.fsync', side_effect=sync):
            self.assertEqual(private_worker_directory(target), target)
        chain = [target, *target.parents]
        self.assertEqual(synced, [(p.stat().st_dev, p.stat().st_ino) for p in chain])
        for path in (target, target.parent, target.parent.parent):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)

    def test_existing_root_also_syncs_and_never_repairs_permissions(self):
        target = self.root / 'state'
        target.mkdir(mode=0o700)
        with patch('ops.private_worker_directory.os.chmod', side_effect=AssertionError('repair')):
            private_worker_directory(target)
        target.chmod(0o750)
        with self.assertRaisesRegex(ValueError, 'unavailable'):
            private_worker_directory(target)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o750)

    def test_parent_symlink_refused_before_creating_child(self):
        outside = self.root / 'outside'
        outside.mkdir(mode=0o700)
        link = self.root / 'link'
        link.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'unavailable'):
            private_worker_directory(link / 'state')
        self.assertEqual(list(outside.iterdir()), [])

    def test_file_ancestor_refused_and_retained(self):
        parent = self.root / 'file'
        parent.write_bytes(b'original')
        with self.assertRaises(ValueError):
            private_worker_directory(parent / 'state')
        self.assertEqual(parent.read_bytes(), b'original')

    def test_sync_failure_holds_retains_tree_and_does_not_leak_descriptors(self):
        target = self.root / 'nested' / 'state'
        before = set(os.listdir('/proc/self/fd'))
        with patch('ops.private_worker_directory.os.fsync', side_effect=OSError('synthetic fsync')):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                private_worker_directory(target)
        self.assertTrue(target.is_dir())
        self.assertEqual(set(os.listdir('/proc/self/fd')), before)

    def test_directory_replaced_during_sync_is_held_and_both_trees_retained(self):
        target = self.root / 'state'
        target.mkdir(mode=0o700)
        moved = self.root / 'retained'
        real = os.fsync
        changed = False
        def sync(fd):
            nonlocal changed
            real(fd)
            if not changed:
                changed = True
                target.rename(moved)
                target.mkdir(mode=0o700)
        with patch('ops.private_worker_directory.os.fsync', side_effect=sync):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                private_worker_directory(target)
        self.assertTrue(target.is_dir())
        self.assertTrue(moved.is_dir())

    def test_worker_lease_is_not_registered_if_file_publication_fails(self):
        target = self.root / 'state'
        with patch('ops.reference_worker_daemon.sync_worker_lease', side_effect=OSError('sync failed')):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                with DaemonStateLock(target):
                    self.fail('ownership granted')
        self.assertNotIn(str(target), _STATE_OWNERS)
        self.assertTrue((target / 'worker-daemon.lock').is_file())
        with DaemonStateLock(target) as owner:
            owner.check(target)

    def test_privacy_drift_during_sync_is_held_without_repair(self):
        target = self.root / 'scratch'
        target.mkdir(mode=0o700)
        real = os.fsync
        changed = False
        def sync(fd):
            nonlocal changed
            real(fd)
            if not changed:
                changed = True
                target.chmod(0o755)
        with patch('ops.private_worker_directory.os.fsync', side_effect=sync):
            with self.assertRaisesRegex(ValueError, 'unavailable'):
                private_worker_directory(target)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o755)

    def test_relative_and_parent_traversal_are_refused(self):
        for path in ('state', str(self.root) + '/../state'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                private_worker_directory(path)


if __name__ == '__main__':
    unittest.main()
