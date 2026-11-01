"""Compare Viewer/Processing canonical acceptance digests on selected source."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing import reference_protocol as p


class AcceptanceCrossLanguageTests(unittest.TestCase):
    def test_viewer_canonical_digest_matches_processing(self):
        viewer_dir = os.environ.get('LT_VIEWER_CANDIDATE_DIR')
        if not viewer_dir: self.skipTest('LT_VIEWER_CANDIDATE_DIR is not configured')
        module = Path(viewer_dir) / 'nativeReferenceProtocolCandidate.js'
        before = hashlib.sha256(module.read_bytes()).hexdigest()
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        receipts = [fixture['acceptance'],dict(fixture['acceptance'],acceptedAt='2026-10-27🔥12:00:00+00:00')]
        for receipt in receipts:
            script = "const p=require(process.argv[1]),c=require('node:crypto'),v=JSON.parse(require('node:fs').readFileSync(0,'utf8'));process.stdout.write(c.createHash('sha256').update(p.canonical(v),'utf8').digest('hex'));"
            child = subprocess.run(['node','-e',script,str(module)],input=json.dumps(receipt),
                capture_output=True,text=True,timeout=10)
            self.assertEqual(child.returncode,0,child.stderr[-2000:])
            self.assertEqual(child.stdout,p.digest(receipt))
        self.assertEqual(before,hashlib.sha256(module.read_bytes()).hexdigest())
        print('\nViewer nativeReferenceProtocolCandidate.js SHA256:',before)


if __name__ == '__main__': unittest.main()
