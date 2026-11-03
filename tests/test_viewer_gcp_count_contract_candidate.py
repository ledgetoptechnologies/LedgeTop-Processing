"""Held-only compatibility of real Viewer export and Processing frame decoder."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import unittest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from processing.viewer_paged_frame_candidate import verify, verify_dataset_count_v2, FrameHeld
from processing.gcp_evidence_stream_candidate import verify as verify_evidence
from processing.gcp_file_stream_candidate import verify as verify_file
from processing.held_viewer_snapshot_verifier_candidate import verify_snapshot
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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

    def test_actual_viewer_signed_snapshot_and_gcp_streams(self):
        root = os.environ.get('PINNED_VIEWER_ROOT')
        if not root:
            if os.environ.get('GCP_VIEWER_PAIR_REQUIRED') == '1': self.fail('pinned Viewer checkout required')
            self.skipTest('pinned Viewer checkout required')
        expected = {'heldSourceSnapshot.js': '511a3b4f92e0667486f8dc7d61d038fd4ab3fcad1cdf303cfe4006d9b8da8055',
                    'viewerPagedSourceExport.js': '87b20bc1e188228054add2aa41871410ab412b48d299f87c7a52ffbc23ba3a17',
                    'referenceProtocol.js': 'bb3dcd40739eb7b7021624ccdb7d9e3375854949b89cda1bcfd647c5ac69a49c'}
        for name, digest in expected.items():
            self.assertEqual(hashlib.sha256((Path(root) / 'apps/viewer/server' / name).read_bytes()).hexdigest(), digest)
        run = subprocess.run(['node', str(ROOT / 'tests/support/viewer_gcp_export_pair.cjs'), 'snapshot'],
            env=os.environ, capture_output=True, check=True, timeout=45)
        result = json.loads(run.stdout)
        raw = base64.b64decode(result['intentBytes'], validate=True)
        signature = base64.b64decode(result['signature'], validate=True)
        key = load_pem_public_key(result['publicKey'].encode())
        key.verify(signature, b'ltds-viewer-held-source-snapshot-v1\0' + raw)
        with self.assertRaises(InvalidSignature):
            key.verify(signature, b'ltds-viewer-held-source-snapshot-v1\0' + raw + b' ')
        intent = result['intent']; self.assertEqual(json.loads(raw), intent)
        self.assertEqual(intent['sourceCount'], intent['datasetFileCount'] + 1)
        source = base64.b64decode(result['sourceFrames'], validate=True)
        self.assertEqual(verify_dataset_count_v2(io.BytesIO(source),
            expected_page_hashes=intent['sourcePageHashes'], expected_dataset_items=intent['datasetFileCount'])['generatedGcp'], 1)
        gcp = intent['gcp']
        self.assertEqual(verify_evidence(io.BytesIO(base64.b64decode(result['evidenceFrames'])),
            attempt_id=intent['attemptId'], evidence_sha256=gcp['evidenceSha256'],
            page_hashes=gcp['evidencePageHashes'], page_bytes=gcp['evidencePageBytes'],
            decoded_bytes=gcp['evidenceByteCount'])['state'], 'held_evidence_not_authorized')
        self.assertEqual(verify_file(io.BytesIO(base64.b64decode(result['fileFrames'])),
            attempt_id=intent['attemptId'], evidence_sha256=gcp['evidenceSha256'], file_sha256=gcp['fileSha256'],
            page_hashes=gcp['filePageHashes'], page_bytes=gcp['filePageBytes'],
            decoded_bytes=gcp['fileByteCount'])['state'], 'held_file_not_authorized')
        arguments = dict(intent_bytes=raw, signature=signature, public_key=key,
            issuer_id='synthetic-issuer', key_id='synthetic-key', dataset_id=intent['datasetId'],
            task_id=intent['taskId'], attempt_id=intent['attemptId'], source_frames=source,
            selected_frames=base64.b64decode(result['selectedFrames']),
            evidence_frames=base64.b64decode(result['evidenceFrames']),
            file_frames=base64.b64decode(result['fileFrames']))
        self.assertEqual(verify_snapshot(**arguments)['state'], 'held_snapshot_not_authorized')
        for mutation in ({'public_key': Ed25519PrivateKey.generate().public_key()},
                         {'task_id': '00000000-0000-4000-8000-000000000000'},
                         {'file_frames': arguments['file_frames'][:-1]}):
            with self.subTest(mutation=list(mutation)), self.assertRaises(ValueError):
                verify_snapshot(**{**arguments, **mutation})


if __name__ == '__main__':
    unittest.main()
