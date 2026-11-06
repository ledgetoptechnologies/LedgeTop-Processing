"""Opt-in qualification for an isolated, freshly provisioned local bind mount.

The caller owns the whole test mount. Never run against an installed spool.
"""
import hashlib
import io
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from processing.source_spool import SourceSpool, SourceSpoolError


@unittest.skipUnless(os.environ.get('LTDS_TEST_LOCAL_SPOOL_ROOT'), 'local spool mount not supplied')
class LocalSpoolMountedTests(unittest.TestCase):
    def test_local_bind_mount_publication_reopen_and_capacity(self):
        root = Path(os.environ['LTDS_TEST_LOCAL_SPOOL_ROOT'])
        self.assertTrue(root.is_absolute())
        self.assertTrue(os.path.ismount(root), 'fixture must be a real mount, not an ordinary directory')
        self.assertEqual(list(root.iterdir()), [], 'fixture mount must be empty and task-owned')
        marker = root / '.ltds-source-spool-id'
        marker.write_text('a' * 64, encoding='ascii')
        marker.chmod(0o600)
        spool = SourceSpool(root, mount_path=root, mount_id='a' * 64,
                            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)
        kwargs = dict(job_id='00000000-0000-4000-8000-000000000001', generation=1,
                      object_id='00000000-0000-4000-8000-000000000002',
                      byte_size=3, sha256=hashlib.sha256(b'abc').hexdigest())
        receipt, replay = spool.receive_object(**kwargs, reader=io.BytesIO(b'abc'))
        self.assertFalse(replay)
        again = SourceSpool(root, mount_path=root, mount_id='a' * 64, safety_floor_bytes=0)
        self.assertTrue(again.verify_object(**kwargs))
        self.assertEqual(again.receive_object(**kwargs, reader=io.BytesIO(b'abc'))[0], receipt)
        with patch('processing.source_spool.shutil.disk_usage', return_value=type(
                'Usage', (), {'total': 10, 'used': 9, 'free': 1})()):
            with self.assertRaisesRegex(SourceSpoolError, 'spool_capacity_unavailable'):
                again.check_capacity(2)
        marker.write_text('b' * 64, encoding='ascii')
        with self.assertRaisesRegex(SourceSpoolError, 'spool_mount_changed'):
            again.available_bytes()
