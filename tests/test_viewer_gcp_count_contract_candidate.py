"""Held-only compatibility of real Viewer export and Processing frame decoder."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing.viewer_paged_frame_candidate import verify, verify_dataset_count_v2, FrameHeld

ROOT = Path(__file__).resolve().parents[1]


class ViewerGcpCountContract(unittest.TestCase):
    def test_generated_gcp_count_is_not_dataset_file_count(self):
        root = os.environ.get('PINNED_VIEWER_ROOT')
        if not root:
            if os.environ.get('GCP_VIEWER_PAIR_REQUIRED') == '1':
                self.fail('pinned Viewer checkout required')
            self.skipTest('pinned Viewer checkout required')
        root = Path(root)
        expected = {'viewerPagedSourceExport.js': '87b20bc1e188228054add2aa41871410ab412b48d299f87c7a52ffbc23ba3a17',
                    'processingRepository.js': 'ee27e7dd3fee9e2df16efb7574d09c4114cb88d40fc4e19ebf2c172a99141a71'}
        for name, digest in expected.items():
            self.assertEqual(hashlib.sha256((root / 'apps/viewer/server' / name).read_bytes()).hexdigest(), digest)
        run = subprocess.run(['node', str(ROOT / 'tests/support/viewer_gcp_export_pair.cjs'), 'mixed'],
            env=dict(os.environ, PINNED_VIEWER_ROOT=str(root)), capture_output=True, check=True, timeout=45)
        value = json.loads(run.stdout)
        self.assertEqual(value['datasetFileCount'], 4)
        self.assertEqual(value['sourceCount'], 5)
        wire = base64.b64decode(value['sourceFrames'], validate=True)
        pins = value['sourcePageHashes']
        self.assertEqual(verify(io.BytesIO(wire), expected_page_hashes=pins,
                                expected_items=value['sourceCount'])['generatedGcp'], 1)
        self.assertEqual(verify_dataset_count_v2(io.BytesIO(wire), expected_page_hashes=pins,
            expected_dataset_items=value['datasetFileCount'])['generatedGcp'], 1)
        with self.assertRaises(FrameHeld):
            verify_dataset_count_v2(io.BytesIO(wire), expected_page_hashes=pins,
                                    expected_dataset_items=value['sourceCount'])


if __name__ == '__main__':
    unittest.main()
