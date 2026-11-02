"""The same frozen wire fixture is checked by Python and the Viewer JS tests."""
import hashlib
import json
from pathlib import Path
import unittest

from processing.reference_protocol import (canonical_json, digest, validate_submission,
    validate_result_manifest, validate_acceptance)


class SharedReferenceContractTests(unittest.TestCase):
    def test_exact_shared_fixture_and_cross_language_bytes(self):
        path = Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json'
        raw = path.read_bytes()
        self.assertEqual(hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest(),
                         'fad338cc40b56fce9f571fa0b34a9041983a3868e830a0a90d6242cde5729210')
        fixture = json.loads(raw)
        self.assertEqual(fixture['transferGrantAcknowledgement']['grantSha256'],
                         digest(fixture['transferGrant']))
        self.assertEqual({key: fixture['transferGrantAcknowledgement'][key] for key in
                          ('reservationId', 'generation', 'manifestSha256', 'nonce', 'accepted')},
                         {key: fixture['transferGrant'][key] for key in
                          ('reservationId', 'generation', 'manifestSha256', 'nonce')} | {'accepted': True})
        for vector in fixture['transferGrantCanonicalizationVectors']:
            grant = {**fixture['transferGrant'], 'token': vector['token']}
            self.assertEqual(digest(grant), vector['grantSha256'])
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
