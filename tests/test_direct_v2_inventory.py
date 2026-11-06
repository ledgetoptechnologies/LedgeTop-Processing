import copy
import hashlib
import unittest

from processing import direct_v2_inventory as inventory
from processing import reference_protocol as protocol
from processing.reference_source_intake import INTAKE_FIELDS
from tests import test_reference_source_intake as fixtures


class DirectV2InventoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReferenceSourceIntakeTests()
        self.fixture.setUp()
        signed = self.fixture.validate()
        self.payload = {key: signed[key] for key in INTAKE_FIELDS}
        self.options = dict(authenticated_principal='viewer-staging', environment='staging',
            capability_fingerprint='a'*64,
            options_fingerprint=self.payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options=set(), trusted_source_keys=self.fixture.trusted_keys,
            authority_id=fixtures.AUTHORITY)

    def test_raw_gcp_and_derived_manifest_are_separate_and_non_authorizing(self):
        result = inventory.prepare(self.payload, **self.options)
        self.assertEqual(result['sourceManifestSha256'], self.payload['sourceManifest']['sha256'])
        self.assertEqual(result['derivedManifestSha256'], self.payload['derivedManifest']['sha256'])
        self.assertEqual(result['pipelineManifestSha256'], self.payload['manifest']['sha256'])
        self.assertEqual(result['sourceAttestation'], self.payload['sourceAttestation'])
        self.assertEqual(result['pipelineManifest'], self.payload['manifest'])
        self.assertEqual(result['pipeline'], self.payload['pipeline'])
        self.assertEqual(len(result['sourceManifest']['objects']), 2)
        self.assertEqual(result['sourceManifest']['objects'][1]['retentionClass'], 'permanent_viewer_hold')
        self.assertEqual(len(result['derivedManifest']['objects']), 1)
        self.assertFalse(result['sourceCustodyAccepted'])
        self.assertFalse(result['transferAuthorized'])
        self.assertFalse(result['processingAuthorized'])
        self.assertEqual(result['inventorySha256'], protocol.digest(
            {key:value for key,value in result.items() if key != 'inventorySha256'}))
        result['sourceManifest']['objects'].clear()
        self.assertEqual(len(self.payload['sourceManifest']['objects']), 2)

    def test_wrong_principal_and_tampered_retention_rejected(self):
        with self.assertRaises(ValueError):
            inventory.prepare(self.payload, **dict(self.options, authenticated_principal='other-viewer'))
        modified = copy.deepcopy(self.payload)
        modified['sourceManifest']['objects'][1]['retentionClass'] = 'release_after_acceptance'
        with self.assertRaises(ValueError):
            inventory.prepare(modified, **self.options)

    def test_worker_revalidates_original_proof_and_independent_binding(self):
        prepared = inventory.prepare(self.payload, **self.options)
        fields = ('authorityId', 'principalId', 'environment', 'submissionId', 'viewerTaskId',
            'providerTaskId', 'sourceManifestSha256', 'derivedManifestSha256',
            'pipelineManifestSha256', 'pipelineDigest', 'attestationSha256',
            'sourceSnapshotSha256', 'preparedEnvelopeSha256', 'activationId')
        binding = {key:prepared[key] for key in fields}
        self.assertEqual(inventory.verify(prepared, expected_binding=binding, **self.options), prepared)
        changed = copy.deepcopy(prepared)
        changed['sourceAttestation']['signature'] = 'A' * 86
        with self.assertRaises(ValueError):
            inventory.verify(changed, expected_binding=binding, **self.options)
        binding['sourceSnapshotSha256'] = 'f' * 64
        with self.assertRaises(ValueError):
            inventory.verify(prepared, expected_binding=binding, **self.options)

    def test_zero_byte_raw_gcp_has_exact_v2_transfer_spec(self):
        self.fixture.payload['sourceManifest']['objects'][1]['byteSize'] = 0
        self.fixture.payload['sourceManifest']['objects'][1]['sha256'] = hashlib.sha256(b'').hexdigest()
        self.fixture.payload['sourceManifest']['totalBytes'] = self.fixture.payload['sourceManifest']['objects'][0]['byteSize']
        self.fixture.rehash()
        signed = self.fixture.validate()
        payload = {key:signed[key] for key in INTAKE_FIELDS}
        prepared = inventory.prepare(payload, **self.options)
        fields = ('authorityId', 'principalId', 'environment', 'submissionId', 'viewerTaskId',
            'providerTaskId', 'sourceManifestSha256', 'derivedManifestSha256',
            'pipelineManifestSha256', 'pipelineDigest', 'attestationSha256',
            'sourceSnapshotSha256', 'preparedEnvelopeSha256', 'activationId')
        binding = {key:prepared[key] for key in fields}
        obj = inventory.worker_object(prepared, fixtures.GCP_ID, expected_binding=binding, **self.options)
        self.assertEqual((obj['protocol'],obj['byteSize'],obj['sha256']),
            ('ltds-direct-worker-object-v2',0,hashlib.sha256(b'').hexdigest()))
        self.assertEqual((obj['objectKind'],obj['retentionClass'],obj['retentionHoldId']),
            ('source','permanent_viewer_hold',fixtures.GCP_ID))
        self.assertFalse(obj['sourceCustodyAccepted'])
        with self.assertRaises(ValueError):
            inventory.worker_object(prepared, 'unknown-id', expected_binding=binding, **self.options)


if __name__ == '__main__':
    unittest.main()
