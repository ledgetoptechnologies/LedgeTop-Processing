"""Pinned synthetic vectors for the existing Viewer archive inventory hash.

The expected JSON and hashes are checked-in fixtures generated independently
with Python from explicitly ordered objects. Node loads Viewer's actual
canonical/validation helpers; no archive data or running service is used.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parent
VIEWER = os.environ.get('LTDS_VIEWER_SOURCE_DIR')
EXPECTED_HASHES = {
    'empty_inventory': '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945',
    'single_empty_file': 'f77b668c0a61974c906f763d473c516feaeeab9608e861013ea98d57c1a0fd42',
    'utf16_order_and_json_escaping': 'bfba2135f9191d3fd57be4c4cdf99fc819978b4a8a27058057839d9f8c0c4f3c',
}


class ViewerArchiveInventoryVectors(unittest.TestCase):
    def test_fixture_digests_and_utf16_order_are_fixed(self):
        vectors = json.loads((ROOT / 'fixtures/archive_inventory_v1_vectors.json').read_text(encoding='utf-8'))
        self.assertEqual(vectors['schemaVersion'], 1)
        self.assertEqual({v['name'] for v in vectors['vectors']}, set(EXPECTED_HASHES))
        for v in vectors['vectors']:
            self.assertEqual(v['sha256'], EXPECTED_HASHES[v['name']])
            self.assertEqual(hashlib.sha256(v['canonicalJson'].encode('utf-8')).hexdigest(), v['sha256'])
        ordered = json.loads(next(v['canonicalJson'] for v in vectors['vectors'] if v['name'] == 'utf16_order_and_json_escaping'))
        self.assertEqual([item['relativePath'] for item in ordered],
                         ['a"quote.txt', 'sub/\u2028.txt', 'tree/\U0001f600.txt', 'tree/\ue000.txt'])
        self.assertIn('\\"', next(v['canonicalJson'] for v in vectors['vectors'] if v['name'] == 'utf16_order_and_json_escaping'))

    @unittest.skipUnless(VIEWER, 'set LTDS_VIEWER_SOURCE_DIR to a pinned Viewer checkout')
    def test_actual_viewer_inventory_helpers_match_fixtures(self):
        vectors = json.loads((ROOT / 'fixtures/archive_inventory_v1_vectors.json').read_text(encoding='utf-8'))['vectors']
        for v in vectors:
            with self.subTest(vector=v['name']):
                # Deliberately reorder keys and add a non-hashed field. Viewer
                # must project the three wire fields in the specified order.
                files = [{'sha256': f['sha256'], 'extra': 'ignored',
                          'relativePath': f['relativePath'], 'byteSize': f['byteSize']} for f in v['files']]
                result = subprocess.run(['node', str(ROOT / 'support/viewer_archive_inventory_probe.cjs')],
                    input=json.dumps(files, ensure_ascii=True), capture_output=True,
                    text=True, encoding='utf-8', timeout=20, check=True)
                actual = json.loads(result.stdout)
                self.assertEqual(actual['canonicalJson'], v['canonicalJson'])
                self.assertEqual(actual['sha256'], v['sha256'])
                self.assertEqual(actual['accepted'], v['accepted'])


if __name__ == '__main__':
    unittest.main()
