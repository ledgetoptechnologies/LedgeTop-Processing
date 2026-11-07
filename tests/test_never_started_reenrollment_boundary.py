"""Document concrete boundaries blocking replacement of an inactive install.

These tests do not implement or authorize replacement. A future supported
replacement must update the expectations along with its journaled protocol.
"""
import os
from pathlib import Path
import tempfile
import time
import unittest

from ops.enroll_reference_worker import ActualBoundary, identity
from ops.enrolled_coordinator_transport import TransportHeld


class InactiveReenrollmentBoundaryTests(unittest.TestCase):
    def test_first_installer_refuses_masked_unit_even_when_inactive(self):
        class MaskedBoundary(ActualBoundary):
            def as_user(self, manifest, args, **kwargs):
                return (b'LoadState=masked\nActiveState=inactive\n'
                        b'MainPID=0\nUnitFileState=masked\n')

        with self.assertRaisesRegex(TransportHeld, 'existing_worker_unit_requires_handover'):
            MaskedBoundary().no_unit({})

    @unittest.skipUnless(os.name == 'posix', 'Linux file identity includes ctime')
    def test_rename_restore_cannot_recreate_original_file_identity(self):
        with tempfile.TemporaryDirectory(prefix='worker-retirement-identity-') as directory:
            unit = Path(directory) / 'unit'
            unit.write_bytes(b'original unit')
            before = identity(unit.lstat())
            retired = Path(directory) / 'retired-unit'
            time.sleep(0.05)
            unit.rename(retired)
            retired.rename(unit)
            after = identity(unit.lstat())
            self.assertEqual(unit.read_bytes(), b'original unit')
            self.assertEqual(before[:2], after[:2])
            self.assertNotEqual(before, after)


if __name__ == '__main__':
    unittest.main()
