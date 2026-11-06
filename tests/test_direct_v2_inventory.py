import copy
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


if __name__ == '__main__':
    unittest.main()
