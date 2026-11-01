"""Failed synthetic lease initialization must release its owned flock."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing import authority_lease


@unittest.skipUnless(os.name == 'posix', 'POSIX flock required')
class LeaseInitTests(unittest.TestCase):
    def test_post_flock_failure_closes_descriptor_and_allows_reacquire(self):
        root=Path(tempfile.mkdtemp(prefix='lt-lease-init-')).resolve()
        print('retained synthetic lease root:',root,flush=True)
        for operation in ('fstat','ftruncate','pwrite','fsync'):
            with self.subTest(operation=operation):
                authority=str(uuid.uuid4())
                with patch.object(authority_lease.os,operation,side_effect=OSError('synthetic fault')):
                    with self.assertRaises(OSError):authority_lease.AuthorityLease(root,authority)
                lease=authority_lease.AuthorityLease(root,authority)
                lease.close()


if __name__=='__main__':unittest.main()
