"""Synthetic database-backed Viewer export; no source files or live services."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing.viewer_paged_frame_candidate import verify_dataset_count_v2

ROOT = Path(__file__).resolve().parents[1]


class DatabaseLargeCandidate(unittest.TestCase):
    def test_pinned_viewer_database_export(self):
        root = os.environ.get('PINNED_VIEWER_ROOT')
        if not root:
            if os.environ.get('GCP_VIEWER_PAIR_REQUIRED') == '1':
                self.fail('PINNED_VIEWER_ROOT required')
            self.skipTest('pinned Viewer checkout required')
        root = Path(root)
        exporter = root / 'apps/viewer/server/viewerPagedSourceExport.js'
        self.assertEqual(hashlib.sha256(exporter.read_bytes()).hexdigest(),
                         '0615c7d6a3a6197129d3a64e444d00d8eb824378d924cd9e61a2bcc65acfc360')
        run = subprocess.run(['node', str(ROOT / 'tests/support/viewer_gcp_export_pair.cjs'), 'normal'],
                             env=dict(os.environ, PINNED_VIEWER_ROOT=str(root)),
                             capture_output=True, check=True, timeout=120)
        export = json.loads(run.stdout)
        self.assertEqual(export['sourceCount'], 16834)
        frames = base64.b64decode(export['sourceFrames'], validate=True)
        result = verify_dataset_count_v2(io.BytesIO(frames),
            expected_page_hashes=export['sourcePageHashes'],
            expected_dataset_items=export['sourceCount'])
        self.assertEqual(result['objects'], 16835)
        self.assertEqual(result['generatedGcp'], 1)
        self.assertEqual(result['state'], 'held_no_viewer_authority_or_mapping')


if __name__ == '__main__':
    unittest.main()
