"""Held-only large frame compatibility against a pinned Viewer source tree."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest

from processing.viewer_paged_frame_candidate import MAX_FRAME_BYTES, verify


class ActualViewerLargeFramesTests(unittest.TestCase):
    def test_actual_exporter_splits_long_records_and_processing_verifies(self):
        root = os.environ.get('VIEWER_EXPORT_ROOT')
        node = shutil.which('node')
        if not root or not node:
            if os.environ.get('PAGED_MANIFEST_PAIRED_REQUIRED') == '1':
                self.fail('paired mode requires Node and VIEWER_EXPORT_ROOT')
            self.skipTest('pinned Viewer source tree and Node required')
        root = Path(root)
        if not root.is_absolute():
            self.fail('Viewer source root must be absolute')
        server = root / 'apps' / 'viewer' / 'server'
        expected = {
            'viewerPagedSourceExport.js': '0615c7d6a3a6197129d3a64e444d00d8eb824378d924cd9e61a2bcc65acfc360',
            'referenceProtocol.js': 'bb3dcd40739eb7b7021624ccdb7d9e3375854949b89cda1bcfd647c5ac69a49c',
            'processingProvenance.js': 'aa9e63c3a4a2989a658669ec6b327ab8ced140904ac450ee249523f58b4092a8',
        }
        for name, digest in expected.items():
            self.assertEqual(hashlib.sha256((server / name).read_bytes()).hexdigest(), digest)
        script = r"""
const p=require(process.argv[1]);
const dir='a'.repeat(200)+'/'+'b'.repeat(200)+'/';
const records=Array.from({length:16832},(_,i)=>({
 type:'dataset_file',datasetFileId:`file-${String(i).padStart(5,'0')}`,
 relativePath:dir+`IMG_${String(i).padStart(5,'0')}.jpg`,
 byteSize:9089354,sha256:'a'.repeat(64),processingRole:'image',mediaType:'image/jpeg',kind:'image'
}));
const result=p.frameStream(records,p.SOURCE_VERSION,p.SOURCE_DOMAIN);
process.stdout.write(JSON.stringify({bytes:result.bytes.toString('base64'),
 pins:result.pageHashes,counts:result.frames.map(f=>JSON.parse(f.bytes.toString()).records.length),
 sizes:result.frames.map(f=>f.bytes.length)}));
"""
        run = subprocess.run([node, '-e', script, str(server / 'viewerPagedSourceExport.js')],
                             capture_output=True, check=True, timeout=45)
        result = json.loads(run.stdout)
        self.assertEqual(sum(result['counts']), 16832)
        self.assertTrue(any(count < 512 for count in result['counts'][:-1]))
        self.assertTrue(all(size <= MAX_FRAME_BYTES + 1 for size in result['sizes']))
        checked = verify(io.BytesIO(base64.b64decode(result['bytes'], validate=True)),
                         expected_page_hashes=result['pins'], expected_items=16832)
        self.assertEqual(checked['objects'], 16832)
        self.assertEqual(checked['state'], 'held_no_viewer_authority_or_mapping')


if __name__ == '__main__':
    unittest.main()
