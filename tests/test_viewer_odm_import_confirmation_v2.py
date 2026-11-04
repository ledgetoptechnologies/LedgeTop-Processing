import base64
import copy
import hashlib
import json
import shutil
import subprocess
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as protocol
from processing.viewer_import_confirmation import ImportConfirmationRejected
from processing.viewer_import_confirmation import SIGNATURE_DOMAIN as OLD_DOMAIN
from processing.viewer_odm_import_confirmation_v2 import (
    ARCHIVE_INVENTORY_FORMAT, OUTPUT_INVENTORY_FORMAT, PROTOCOL, SIGNATURE_DOMAIN,
    verify_odm_import_confirmation,
)
from tests.viewer_receipt_signer import SyntheticViewerSigner


class OdmImportConfirmationV2Tests(unittest.TestCase):
    def setUp(self):
        self.copy_signer = SyntheticViewerSigner()
        self.import_key = Ed25519PrivateKey.generate()
        public = self.import_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
        self.import_keys = {'import-key': {'issuerId': self.copy_signer.issuer_id, 'publicKeyPem': public}}
        self.manifest = dict(schemaVersion=1, jobId='job-1', generation=1, environment='staging',
            pipelineDigest='a' * 64, engineVersions={'odm': 'synthetic'},
            objects=[dict(id='odm-archive', role='odm_archive', byteSize=1, sha256='b' * 64)])
        self.manifest['resultManifestSha256'] = protocol.digest(self.manifest)
        self.expected = dict(jobId='job-1', generation=1, environment='staging',
            inputManifestSha256='d' * 64, pipelineDigest='a' * 64,
            resultManifestSha256=self.manifest['resultManifestSha256'], viewerTaskId='task-1',
            viewerAttemptId='attempt-1', viewerIssuerId=self.copy_signer.issuer_id)
        acceptance = dict(schemaVersion=1, receiptId='copy-acceptance', storageReceiptId='copy-storage',
            jobId='job-1', generation=1, environment='staging', inputManifestSha256='d' * 64,
            pipelineDigest='a' * 64, resultManifestSha256=self.manifest['resultManifestSha256'],
            acceptedObjectIds=['odm-archive'], acceptedAt='2026-11-03T01:00:00.000Z',
            validationVersion=protocol.VALIDATION_VERSION)
        self.envelope = self.copy_signer.envelope(acceptance, self.manifest)
        envelope_sha = protocol.digest(self.envelope)
        self.expected['acceptedEnvelopeSha256'] = [envelope_sha]
        receipt = self.envelope['viewerReceipt']
        copy_binding = dict(acceptanceReceiptId=acceptance['receiptId'],
            acceptanceStorageReceiptId=acceptance['storageReceiptId'], viewerReceiptId=receipt['receiptId'],
            viewerStorageReceiptId=receipt['storageReceiptId'], envelopeSha256=envelope_sha)
        self.confirmation = dict(protocol=PROTOCOL, schemaVersion=2, issuerId=self.copy_signer.issuer_id,
            keyId='import-key', confirmationId='confirm-1', jobId='job-1', generation=1, environment='staging',
            inputManifestSha256='d' * 64, pipelineDigest='a' * 64,
            resultManifestSha256=self.manifest['resultManifestSha256'], viewerTaskId='task-1',
            viewerAttemptId='attempt-1', modelId='model-1', modelVersionId='version-1', outputId='output-1',
            outputInventoryFormat=OUTPUT_INVENTORY_FORMAT, outputInventorySha256='e' * 64,
            acceptedCopies=[copy_binding], objects=copy.deepcopy(self.manifest['objects']),
            archive=dict(objectId='odm-archive', byteSize=1, sha256='b' * 64,
                         inventoryFormat=ARCHIVE_INVENTORY_FORMAT, inventorySha256='f' * 64),
            milestone='ready_for_review', readyAt='2026-11-03T01:02:03.000Z')
        self.sign()

    def sign(self, *, domain=SIGNATURE_DOMAIN):
        body = {key: value for key, value in self.confirmation.items() if key != 'signature'}
        self.confirmation['signature'] = base64.urlsafe_b64encode(
            self.import_key.sign(domain + protocol.canonical_json(body))).decode('ascii').rstrip('=')

    def verify(self, **overrides):
        kwargs = dict(trusted_import_keys=self.import_keys, trusted_copy_keys=self.copy_signer.trusted_keys,
            expected=self.expected, result_manifest=self.manifest, accepted_envelopes=[self.envelope])
        kwargs.update(overrides)
        return verify_odm_import_confirmation(self.confirmation, **kwargs)

    def test_signed_viewer_assertions_are_not_prebound_processing_expectations(self):
        first = self.verify()
        self.assertEqual(first.confirmation, self.confirmation)
        self.assertEqual(self.verify(prior_confirmation_sha256=first.sha256).sha256, first.sha256)
        detached = first.confirmation
        detached['modelId'] = 'changed-local-copy'
        self.assertEqual(first.confirmation['modelId'], 'model-1')
        # A legitimately signed post-ingest model/inventory assertion is not a
        # pre-existing Processing expectation, but cannot replace first proof.
        self.confirmation['modelId'] = 'viewer-chosen-model'
        self.confirmation['archive']['inventorySha256'] = '9' * 64
        self.confirmation['outputInventorySha256'] = '8' * 64
        self.sign()
        self.assertEqual(self.verify().confirmation['modelId'], 'viewer-chosen-model')
        with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_replay_changed'):
            self.verify(prior_confirmation_sha256=first.sha256)

    def test_validly_signed_changed_independent_binding_is_rejected(self):
        for field, value in [('jobId', 'other-job'), ('generation', 2), ('environment', 'production'),
                             ('inputManifestSha256', '0' * 64), ('pipelineDigest', '0' * 64),
                             ('resultManifestSha256', '0' * 64), ('viewerTaskId', 'other-task'),
                             ('viewerAttemptId', 'other-attempt')]:
            with self.subTest(field=field):
                old = self.confirmation[field]
                self.confirmation[field] = value
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_binding_mismatch'):
                    self.verify()
                self.confirmation[field] = old
        self.sign()

    def test_signed_archive_identity_must_match_local_manifest(self):
        for field, value in [('objectId', 'other-object'), ('byteSize', 101), ('sha256', '0' * 64),
                             ('inventoryFormat', OUTPUT_INVENTORY_FORMAT)]:
            with self.subTest(field=field):
                old = self.confirmation['archive'][field]
                self.confirmation['archive'][field] = value
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_closure_mismatch'):
                    self.verify()
                self.confirmation['archive'][field] = old
        self.confirmation['archive']['byteSize'] = True
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_closure_mismatch'):
            self.verify()

    def test_extra_object_treedar_only_and_mixed_manifests_are_refused(self):
        extra = copy.deepcopy(self.manifest)
        extra['objects'].append(dict(id='report', role='report', byteSize=1, sha256='c' * 64))
        extra['resultManifestSha256'] = protocol.digest({k: v for k, v in extra.items() if k != 'resultManifestSha256'})
        roles = ('ground_model', 'canopy_height', 'canopy_height_unfiltered', 'crown_labels',
                 'crowns', 'tree_table', 'tree_features', 'run_summary', 'report', 'preview',
                 'area_map_preview', 'engine_manifest')
        tree = [dict(id='tree-' + role, role=role, byteSize=5, sha256='c' * 64) for role in roles]
        for engines, objects in [({'odm': 'synthetic'}, extra['objects']),
                                 ({'treedar': 'synthetic'}, tree),
                                 ({'odm': 'synthetic', 'treedar': 'synthetic'}, self.manifest['objects'] + tree)]:
            with self.subTest(engines=engines):
                if set(engines) == {'odm'}:
                    manifest = extra
                else:
                    manifest = dict(protocol='ltds-reference-result-v2', schemaVersion=2,
                        jobId='job-1', generation=1, environment='staging', pipelineDigest='a' * 64,
                        engineVersions=engines, objects=objects)
                    manifest['resultManifestSha256'] = protocol.digest(manifest)
                expected = dict(self.expected, resultManifestSha256=manifest['resultManifestSha256'])
                with self.assertRaisesRegex(ImportConfirmationRejected, 'unsupported_result_closure'):
                    self.verify(expected=expected, result_manifest=manifest)

    def test_untrusted_copy_or_forged_confirmation_never_verifies(self):
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_accepted_copy'):
            self.verify(trusted_copy_keys={})
        with self.assertRaisesRegex(ImportConfirmationRejected, 'untrusted_import_key'):
            self.verify(trusted_import_keys={})
        self.sign(domain=OLD_DOMAIN)
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()
        self.sign()
        self.confirmation['signature'] = 'A' * 86
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()

    def test_post_ingest_assertions_are_covered_by_signature(self):
        self.confirmation['modelId'] = 'tampered-model'
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()
        self.confirmation['modelId'] = 'model-1'
        self.confirmation['outputInventorySha256'] = '0' * 64
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()
        self.confirmation['outputInventorySha256'] = 'e' * 64
        self.confirmation['archive']['inventorySha256'] = '0' * 64
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()

    def test_expected_copy_digest_and_full_acceptance_are_required(self):
        self.expected['acceptedEnvelopeSha256'] = ['0' * 64]
        with self.assertRaisesRegex(ImportConfirmationRejected, 'copy_digest_binding_mismatch'):
            self.verify()
        self.expected['acceptedEnvelopeSha256'] = [protocol.digest(self.envelope)]
        acceptance = copy.deepcopy(self.envelope['acceptance'])
        acceptance['acceptedObjectIds'] = []
        self.envelope = self.copy_signer.envelope(acceptance, self.manifest)
        self.expected['acceptedEnvelopeSha256'] = [protocol.digest(self.envelope)]
        with self.assertRaisesRegex(ImportConfirmationRejected, 'incomplete_copy_coverage'):
            self.verify()

    def test_missing_prebinding_and_wrong_issuer_are_refused(self):
        expected = copy.deepcopy(self.expected)
        del expected['viewerAttemptId']
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_expected_binding'):
            self.verify(expected=expected)
        expected = dict(self.expected, viewerIssuerId='other-issuer')
        with self.assertRaisesRegex(ImportConfirmationRejected, 'copy_issuer_mismatch'):
            self.verify(expected=expected)

    def test_caller_mutation_after_snapshot_cannot_change_verified_bytes(self):
        original = copy.deepcopy(self.confirmation)
        real_canonical = protocol.canonical_json
        def mutate(value):
            self.confirmation['objects'][0]['byteSize'] = 999
            self.expected['viewerAttemptId'] = 'changed-after-snapshot'
            return real_canonical(value)
        with patch('processing.viewer_odm_import_confirmation_v2.protocol.canonical_json', side_effect=mutate):
            result = self.verify()
        self.assertEqual(result.confirmation, original)
        self.assertEqual(hashlib.sha256(result.canonical_bytes).hexdigest(), result.sha256)

    def test_wrong_format_milestone_and_extra_fields_are_refused(self):
        for field, value, reason in [('outputInventoryFormat', ARCHIVE_INVENTORY_FORMAT, 'invalid_output_inventory'),
                                     ('milestone', 'accepted_copy', 'invalid_import_milestone'),
                                     ('readyAt', '2026-02-30T01:02:03.000Z', 'invalid_ready_at')]:
            with self.subTest(field=field):
                old = self.confirmation[field]
                self.confirmation[field] = value
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, reason):
                    self.verify()
                self.confirmation[field] = old
        self.confirmation['extra'] = 'not-allowed'
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_confirmation_fields'):
            self.verify()

    def test_bounded_snapshot_rejects_adversarial_input_before_hashing(self):
        self.confirmation['archive']['inventorySha256'] = 'x' * 8193
        with patch('processing.viewer_odm_import_confirmation_v2.protocol.canonical_json',
                   side_effect=AssertionError('canonicalization must not run')):
            with self.assertRaisesRegex(ImportConfirmationRejected, 'input_string_too_large'):
                self.verify()

    def test_cyclic_input_and_caller_created_result_object_are_not_proof(self):
        verified = self.verify()
        original = self.confirmation
        cycle = []
        cycle.append(cycle)
        self.confirmation = copy.deepcopy(original)
        self.confirmation['objects'] = cycle
        with patch('processing.viewer_odm_import_confirmation_v2.protocol.canonical_json',
                   side_effect=AssertionError('canonicalization must not run')):
            with self.assertRaisesRegex(ImportConfirmationRejected, 'input_structure_too_large'):
                self.verify()
        self.confirmation = verified
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_input_type'):
            self.verify()

    @unittest.skipUnless(shutil.which('node'), 'Node is required for the cross-language inventory vector')
    def test_js_inventory_format_unicode_path_order_and_signed_assertions(self):
        rows = [dict(relativePath='\ue000.tif', byteSize=4, sha256='b' * 64),
                dict(relativePath='\U0001f600.tif', byteSize=3, sha256='a' * 64),
                dict(relativePath='a.tif', byteSize=2, sha256='c' * 64)]
        js = """const crypto=require('node:crypto');
          const rows=JSON.parse(process.argv[1]);
          rows.sort((a,b)=>a.relativePath < b.relativePath ? -1 : a.relativePath > b.relativePath ? 1 : 0);
          const inventory=rows.map(({relativePath,byteSize,sha256})=>({relativePath,byteSize,sha256}));
          const text=JSON.stringify(inventory);
          process.stdout.write(JSON.stringify({paths:inventory.map(x=>x.relativePath),text,
            sha256:crypto.createHash('sha256').update(text,'utf8').digest('hex')}));"""
        result = subprocess.run([shutil.which('node'), '-e', js, json.dumps(rows, ensure_ascii=True)],
                                capture_output=True, encoding='utf-8', timeout=10, check=True)
        node = json.loads(result.stdout)
        self.assertEqual(node['paths'], ['a.tif', '\U0001f600.tif', '\ue000.tif'])
        self.assertNotEqual(node['paths'], sorted(node['paths']))  # Python Unicode order differs.
        expected = ('[{"relativePath":"a.tif","byteSize":2,"sha256":"' + 'c' * 64
                    + '"},{"relativePath":"\U0001f600.tif","byteSize":3,"sha256":"' + 'a' * 64
                    + '"},{"relativePath":"\ue000.tif","byteSize":4,"sha256":"' + 'b' * 64 + '"}]')
        self.assertEqual(node['text'], expected)
        digest = hashlib.sha256(expected.encode('utf-8')).hexdigest()
        self.assertEqual(node['sha256'], digest)
        self.confirmation['archive']['inventorySha256'] = digest
        self.confirmation['outputInventorySha256'] = digest
        self.sign()
        self.assertEqual(self.verify().confirmation['archive']['inventorySha256'], digest)

    @unittest.skipUnless(shutil.which('node'), 'Node is required for the cross-language signature vector')
    def test_node_ed25519_signature_verifies_against_python_canonical_body(self):
        body = {key: value for key, value in self.confirmation.items() if key != 'signature'}
        js = """const crypto=require('node:crypto');
          const canonical=x=>Array.isArray(x) ? x.map(canonical) :
            x && typeof x==='object' ? Object.fromEntries(Object.keys(x).sort().map(k=>[k,canonical(x[k])])) : x;
          const body=JSON.parse(process.argv[1]); const text=JSON.stringify(canonical(body));
          const pair=crypto.generateKeyPairSync('ed25519');
          const domain=Buffer.concat([Buffer.from('LTDS-VIEWER-ODM-IMPORT-CONFIRMATION-V2','ascii'),Buffer.from([0])]);
          const signature=crypto.sign(null,Buffer.concat([domain,Buffer.from(text,'utf8')]),pair.privateKey).toString('base64url');
          process.stdout.write(JSON.stringify({text,signature,
            publicKeyPem:pair.publicKey.export({type:'spki',format:'pem'}).toString()}));"""
        result = subprocess.run([shutil.which('node'), '-e', js, json.dumps(body, ensure_ascii=True)],
                                capture_output=True, encoding='utf-8', timeout=10, check=True)
        signed = json.loads(result.stdout)
        self.assertEqual(signed['text'].encode('utf-8'), protocol.canonical_json(body))
        self.confirmation['signature'] = signed['signature']
        self.import_keys['import-key']['publicKeyPem'] = signed['publicKeyPem']
        self.assertEqual(self.verify().confirmation, self.confirmation)


if __name__ == '__main__':
    unittest.main()
