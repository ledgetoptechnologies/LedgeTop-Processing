import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests.advisory_live_pilot_preflight import converter_status, registry_marker_present, sample_size


class AdvisoryPilotPreflightTests(unittest.TestCase):
    def test_registry_uses_lexists(self):
        root=Path('example')
        with patch('tests.advisory_live_pilot_preflight.os.path.lexists',return_value=True) as check:
            self.assertTrue(registry_marker_present(root))
            check.assert_called_once_with(root/'active.json')

    def test_registry_counts_dangling_path_as_occupied(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            self.assertFalse(registry_marker_present(root))
            try:
                (root/'active.json').symlink_to(root/'missing')
            except (OSError,NotImplementedError):
                self.skipTest('symlink creation unavailable')
            self.assertTrue(registry_marker_present(root))

    def test_count_is_bounded_and_does_not_copy_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for i in range(30):(root/f'{i:03}.jpg').write_bytes(bytes([i])*3)
            before={p.name:p.read_bytes() for p in root.iterdir()}
            self.assertEqual(sample_size(root),75)
            self.assertEqual({p.name:p.read_bytes() for p in root.iterdir()},before)

    def test_converter_requires_exact_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'binary').write_bytes(b'correct')
            with patch('tests.advisory_live_pilot_preflight.CONVERTER',{'binary':hashlib.sha256(b'correct').hexdigest()}):
                self.assertTrue(converter_status(root))
                (root/'binary').write_bytes(b'changed')
                self.assertFalse(converter_status(root))


if __name__=='__main__':unittest.main()
