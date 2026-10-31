"""The same frozen wire fixture is checked by Python and the Viewer JS tests."""
import hashlib
import json
from pathlib import Path
import unittest

from processing.reference_protocol import (canonical_json, validate_submission,
    validate_result_manifest, validate_acceptance)


class SharedReferenceContractTests(unittest.TestCase):
    def test_exact_shared_fixture_and_cross_language_bytes(self):
        path = Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json'
        raw = path.read_bytes()
        self.assertEqual(hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest(),
                         '2c49ff86b8e20384c400ff56c8365418e643108aa9e6c29b46e938554e989b37')
        fixture = json.loads(raw)
        submission = validate_submission(fixture['submission'], environment='staging',
            capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'])
        self.assertEqual(submission['manifestSha256'], fixture['manifestSha256'])
        manifest = {k: v for k, v in fixture['submission']['manifest'].items() if k != 'sha256'}
        self.assertEqual(canonical_json(manifest).decode(), fixture['canonicalManifestUtf8'])
        expected = fixture['resultManifest']
        result = validate_result_manifest(expected, job_id=expected['jobId'],
            generation=expected['generation'], environment='staging',
            pipeline_digest=expected['pipelineDigest'])
        self.assertEqual(result['resultManifestSha256'], fixture['resultManifestSha256'])
        self.assertEqual(canonical_json({k: v for k, v in result.items()
                         if k != 'resultManifestSha256'}).decode(), fixture['canonicalResultManifestUtf8'])
        accepted = validate_acceptance(fixture['acceptance'], result_manifest=result,
                                      input_manifest_sha256=fixture['manifestSha256'])
        self.assertEqual(accepted['acceptedObjectIds'], ['result-1'])
        self.assertNotIn('result-2', accepted['acceptedObjectIds'])


if __name__ == '__main__':
    unittest.main()
