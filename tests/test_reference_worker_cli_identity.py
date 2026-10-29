"""CLI dispatch must use the same ownership classes as imported helpers.

Source-only tests execute the real file's __main__ dispatch. They do not start a
worker service or qualify the packaged image's real Docker entrypoint.
"""
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest.mock import patch

from ops import reference_worker_daemon as daemon
from processing import reference_indexed_fetch as indexed


class WorkerCliIdentityTests(unittest.TestCase):
    def test_main_dispatch_preserves_arguments_and_uses_canonical_live_owner(self):
        observed=[]
        argv=['ops.reference_worker_daemon','--config','/synthetic/worker.json']
        def canonical_probe():
            with tempfile.TemporaryDirectory() as temporary:
                root=Path(temporary).absolute()
                with daemon.DaemonStateLock(root) as owner:
                    actual,identity=indexed._root(root,owner)
                    self.assertIs(type(owner),daemon.DaemonStateLock)
                    self.assertEqual(actual,root)
                    self.assertEqual(identity,(root.stat().st_dev,root.stat().st_ino))
                    self.assertIs(daemon._STATE_OWNERS[str(root)],owner)
                    observed.append(list(sys.argv))
                self.assertNotIn(str(root),daemon._STATE_OWNERS)
            return 17
        with patch.object(sys,'argv',argv),patch.object(daemon,'main',side_effect=canonical_probe) as entry:
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(daemon.__file__,run_name='__main__')
        self.assertEqual(result.exception.code,17)
        entry.assert_called_once_with()
        # runpy, like python -m, sets argv[0] to the module file itself.
        self.assertEqual(observed,[[daemon.__file__,*argv[1:]]])

    def test_foreign_module_owner_is_still_refused_before_owner_check(self):
        namespace=runpy.run_path(daemon.__file__,run_name='__worker_identity_test__')
        self.assertIsNot(namespace['DaemonStateLock'],daemon.DaemonStateLock)
        with tempfile.TemporaryDirectory() as temporary:
            owner=namespace['DaemonStateLock'](temporary)
            with patch.object(owner,'check',side_effect=AssertionError('foreign owner must not be used')) as check:
                with self.assertRaisesRegex(indexed.IndexedFetchHeld,'actual_indexed_state_owner_required'):
                    indexed._root(temporary,owner)
            check.assert_not_called()

    def test_non_main_file_execution_does_not_activate_canonical_cli(self):
        with patch.object(daemon,'main',side_effect=AssertionError('import must not activate')) as entry:
            namespace=runpy.run_path(daemon.__file__,run_name='__worker_import_test__')
        entry.assert_not_called()
        self.assertEqual(namespace['main'].__module__,'__worker_import_test__')


if __name__=='__main__':unittest.main()
