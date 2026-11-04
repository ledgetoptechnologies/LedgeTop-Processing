import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from tests.prepare_pilot_input import prepare


class PreparePilotInputTests(unittest.TestCase):
    def test_copy_is_bounded_and_sources_are_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root / 'source'; source.mkdir()
            for i in range(30):
                (source / ('%03d.jpg' % i)).write_bytes(('invented-%d' % i).encode())
            before = {p.name: p.read_bytes() for p in source.iterdir()}
            receipt = prepare(source, root / 'pilot')
            self.assertEqual(receipt['count'], 25)
            self.assertEqual({p.name: p.read_bytes() for p in source.iterdir()}, before)
            manifest = json.loads((root / 'pilot/input-manifest.json').read_text())
            self.assertEqual(manifest['owner'], receipt['owner'])
            for f in manifest['files']:
                value = (root / 'pilot/input' / f['relativePath']).read_bytes()
                self.assertEqual(hashlib.sha256(value).hexdigest(), f['sha256'])
                self.assertEqual(len(value), f['byteSize'])
            with self.assertRaises(ValueError):
                prepare(source, root / 'pilot')

    def test_refuses_in_source_target_and_too_small_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); source = root / 'source'; source.mkdir()
            with self.assertRaises(ValueError):
                prepare(source, source / 'pilot')
            with self.assertRaises(ValueError):
                prepare(source, root / 'pilot')
            self.assertFalse((root / 'pilot').exists())


if __name__ == '__main__':
    unittest.main()
