import base64
import copy
from dataclasses import FrozenInstanceError
import hashlib
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as protocol
from processing.viewer_acceptance_receipt import SIGNATURE_DOMAIN as COPY_DOMAIN
from processing.viewer_import_confirmation import (
    ImportConfirmationRejected, PROTOCOL, SIGNATURE_DOMAIN, VerifiedImportConfirmation,
    verify_import_confirmation,
)
from tests.viewer_receipt_signer import SyntheticViewerSigner


class ViewerImportConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.copy_signer = SyntheticViewerSigner()
        self.import_key = Ed25519PrivateKey.generate()
        public = self.import_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
        self.import_keys = {'import-key': {'issuerId': self.copy_signer.issuer_id, 'publicKeyPem': public}}
        self.manifest = dict(schemaVersion=1, jobId='job-1', generation=1, environment='staging',
            pipelineDigest='a' * 64, engineVersions={'odm': 'synthetic'}, objects=[
                dict(id='archive-1', role='odm_archive', byteSize=100, sha256='b' * 64),
                dict(id='report-1', role='report', byteSize=1, sha256='c' * 64)])
        self.manifest['resultManifestSha256'] = protocol.digest(self.manifest)
        self.expected = dict(jobId='job-1', generation=1, environment='staging',
            inputManifestSha256='d' * 64, pipelineDigest='a' * 64,
            resultManifestSha256=self.manifest['resultManifestSha256'], viewerTaskId='task-1',
            viewerAttemptId='attempt-1', modelId='model-1', modelVersionId='version-1',
            outputId='output-1', outputManifestSha256='e' * 64, archiveInventorySha256='f' * 64,
            viewerIssuerId=self.copy_signer.issuer_id)
        self.envelopes = [self.envelope('a1', 's1', ['archive-1']),
                          self.envelope('a2', 's2', ['report-1'])]
        self.expected['acceptedEnvelopeSha256'] = sorted(protocol.digest(item) for item in self.envelopes)
        copies = []
        for item in self.envelopes:
            a, receipt = item['acceptance'], item['viewerReceipt']
            copies.append(dict(acceptanceReceiptId=a['receiptId'], acceptanceStorageReceiptId=a['storageReceiptId'],
                viewerReceiptId=receipt['receiptId'], viewerStorageReceiptId=receipt['storageReceiptId'],
                envelopeSha256=protocol.digest(item)))
        copies.sort(key=lambda item: item['envelopeSha256'])
        identity = {key: value for key, value in self.expected.items()
                    if key not in ('acceptedEnvelopeSha256', 'archiveInventorySha256', 'viewerIssuerId')}
        self.confirmation = dict(protocol=PROTOCOL, schemaVersion=1, issuerId=self.copy_signer.issuer_id,
            keyId='import-key', confirmationId='confirm-1', **identity, acceptedCopies=copies,
            objects=copy.deepcopy(self.manifest['objects']),
            archive=dict(objectId='archive-1', byteSize=100, sha256='b' * 64, inventorySha256='f' * 64),
            milestone='ready_for_review', readyAt='2026-11-03T01:02:03.000Z')
        self.sign()

    def envelope(self, receipt_id, storage_id, ids):
        acceptance = dict(schemaVersion=1, receiptId=receipt_id, storageReceiptId=storage_id,
            jobId='job-1', generation=1, environment='staging', inputManifestSha256='d' * 64,
            pipelineDigest='a' * 64, resultManifestSha256=self.manifest['resultManifestSha256'],
            acceptedObjectIds=ids, acceptedAt='2026-11-03T01:00:00.000Z',
            validationVersion=protocol.VALIDATION_VERSION)
        return self.copy_signer.envelope(acceptance, self.manifest)

    def sign(self, *, domain=SIGNATURE_DOMAIN):
        body = {key: value for key, value in self.confirmation.items() if key != 'signature'}
        self.confirmation['signature'] = base64.urlsafe_b64encode(
            self.import_key.sign(domain + protocol.canonical_json(body))).decode('ascii').rstrip('=')

    def verify(self, **overrides):
        options = dict(trusted_import_keys=self.import_keys, trusted_copy_keys=self.copy_signer.trusted_keys,
            expected=self.expected, result_manifest=self.manifest, accepted_envelopes=self.envelopes)
        options.update(overrides)
        return verify_import_confirmation(self.confirmation, **options)

    def test_valid_signature_full_ordered_closure_and_exact_replay(self):
        verified = self.verify()
        self.assertEqual(verified.confirmation, self.confirmation)
        self.assertEqual(self.verify(prior_confirmation_sha256=verified.sha256).sha256, verified.sha256)
        detached = verified.confirmation
        detached['modelId'] = 'changed-local-copy'
        self.assertEqual(verified.confirmation['modelId'], 'model-1')
        self.assertEqual(self.confirmation['modelId'], 'model-1')
        self.assertEqual(hashlib.sha256(verified.canonical_bytes).hexdigest(), verified.sha256)
        with self.assertRaises(FrozenInstanceError):
            verified.canonical_bytes = b'changed'
        with self.assertRaisesRegex(ValueError, 'invalid immutable confirmation snapshot'):
            VerifiedImportConfirmation(bytearray(verified.canonical_bytes), verified.sha256)
        with self.assertRaisesRegex(ValueError, 'invalid immutable confirmation snapshot'):
            VerifiedImportConfirmation(verified.canonical_bytes, '0' * 64)

    def test_rejects_adversarial_inputs_before_any_canonicalization(self):
        deep = None
        for _ in range(15):
            deep = {'nested': deep}
        cycle = []
        cycle.append(cycle)
        aggregate = [['x' * 8192] * 400]
        cases = [('deep', deep, 'input_structure_too_large'),
                 ('cycle', cycle, 'input_structure_too_large'),
                 ('long_string', 'x' * 8193, 'input_string_too_large'),
                 ('many_items', [None] * 10001, 'input_container_too_large'),
                 ('aggregate_bytes', aggregate, 'input_bytes_too_large'),
                 ('wide_utf8', '\U0001f600' * 2049, 'input_string_too_large'),
                 ('surrogate', '\ud800', 'invalid_input_utf8'),
                 ('large_integer', 1 << 60, 'invalid_input_integer'),
                 ('non_json', b'bytes', 'invalid_input_type')]
        original = self.confirmation['archive']
        for label, value, reason in cases:
            with self.subTest(label=label):
                self.confirmation['archive'] = value
                with patch('processing.viewer_import_confirmation.protocol.canonical_json',
                           side_effect=AssertionError('must not serialize unbounded input')):
                    with self.assertRaisesRegex(ImportConfirmationRejected, reason):
                        self.verify()
        self.confirmation['archive'] = original

    def test_prevalidation_covers_all_external_argument_trees(self):
        bad_expected = copy.deepcopy(self.expected)
        bad_expected['viewerTaskId'] = 'x' * 8193
        bad_manifest = copy.deepcopy(self.manifest)
        bad_manifest['objects'][0]['id'] = '\ud800'
        bad_envelopes = copy.deepcopy(self.envelopes)
        bad_envelopes[0]['viewerReceipt']['objects'] = [None] * 10001
        bad_import_keys = copy.deepcopy(self.import_keys)
        bad_import_keys['import-key']['publicKeyPem'] = 'x' * 8193
        bad_copy_keys = copy.deepcopy(self.copy_signer.trusted_keys)
        bad_copy_keys[self.copy_signer.key_id]['issuerId'] = 'x' * 8193
        for kwargs, reason in [({'expected': bad_expected}, 'input_string_too_large'),
                               ({'result_manifest': bad_manifest}, 'invalid_input_utf8'),
                               ({'accepted_envelopes': bad_envelopes}, 'input_container_too_large'),
                               ({'trusted_import_keys': bad_import_keys}, 'input_string_too_large'),
                               ({'trusted_copy_keys': bad_copy_keys}, 'input_string_too_large'),
                               ({'prior_confirmation_sha256': 'x' * 8193}, 'input_string_too_large')]:
            with self.subTest(argument=next(iter(kwargs))):
                with patch('processing.viewer_import_confirmation.protocol.canonical_json',
                           side_effect=AssertionError('must not serialize unbounded input')):
                    with self.assertRaisesRegex(ImportConfirmationRejected, reason):
                        self.verify(**kwargs)

    def test_input_mutation_during_hashing_cannot_change_verified_snapshot(self):
        original = copy.deepcopy(self.confirmation)
        real_canonical = protocol.canonical_json
        def mutate_inputs(value):
            self.confirmation['objects'][0]['byteSize'] = 999
            self.expected['viewerAttemptId'] = 'changed-after-snapshot'
            return real_canonical(value)
        with patch('processing.viewer_import_confirmation.protocol.canonical_json', side_effect=mutate_inputs):
            verified = self.verify()
        self.assertEqual(verified.confirmation, original)
        self.assertEqual(hashlib.sha256(verified.canonical_bytes).hexdigest(), verified.sha256)

    def test_validly_signed_changed_binding_is_rejected(self):
        for field, value in [('viewerAttemptId', 'attempt-2'), ('modelId', 'other-model'),
                             ('generation', 2), ('environment', 'production'),
                             ('outputManifestSha256', '0' * 64)]:
            with self.subTest(field=field):
                old = self.confirmation[field]
                self.confirmation[field] = value
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_binding_mismatch'):
                    self.verify()
                self.confirmation[field] = old
        self.sign()

    def test_copy_signature_domain_cannot_authorize_import(self):
        self.sign(domain=COPY_DOMAIN)
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()

    def test_tampered_import_signature_is_rejected(self):
        signature = self.confirmation['signature']
        self.confirmation['signature'] = ('A' if signature[0] != 'A' else 'B') + signature[1:]
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_signature'):
            self.verify()

    def test_changed_signed_replay_is_rejected(self):
        original = self.verify().sha256
        self.confirmation['confirmationId'] = 'confirm-2'
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_replay_changed'):
            self.verify(prior_confirmation_sha256=original)

    def test_confirmation_and_copy_issuer_must_match_trusted_binding(self):
        self.expected['viewerIssuerId'] = 'different-viewer'
        with self.assertRaisesRegex(ImportConfirmationRejected, 'copy_issuer_mismatch'):
            self.verify()
        self.expected['viewerIssuerId'] = self.copy_signer.issuer_id
        self.confirmation['issuerId'] = 'different-viewer'
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_issuer_mismatch'):
            self.verify()

    def test_rejects_missing_or_overlapping_copy_coverage(self):
        self.envelopes.pop()
        self.expected['acceptedEnvelopeSha256'] = [protocol.digest(self.envelopes[0])]
        with self.assertRaisesRegex(ImportConfirmationRejected, 'incomplete_copy_coverage'):
            self.verify()
        second = self.envelope('a3', 's3', ['archive-1'])
        self.envelopes.append(second)
        self.expected['acceptedEnvelopeSha256'] = sorted(protocol.digest(item) for item in self.envelopes)
        with self.assertRaisesRegex(ImportConfirmationRejected, 'duplicate_or_empty_copy_coverage'):
            self.verify()

    def test_rejects_reordered_objects_archive_tampering_and_boolean_size(self):
        for mutation in ('reorder', 'archive_hash', 'boolean_size'):
            with self.subTest(mutation=mutation):
                old = copy.deepcopy(self.confirmation)
                if mutation == 'reorder':
                    self.confirmation['objects'].reverse()
                elif mutation == 'archive_hash':
                    self.confirmation['archive']['sha256'] = '0' * 64
                else:
                    self.confirmation['objects'][1]['byteSize'] = True
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_closure_mismatch'):
                    self.verify()
                self.confirmation = old

    def test_rejects_changed_copy_and_untrusted_key(self):
        original = copy.deepcopy(self.envelopes[0])
        self.envelopes[0]['viewerReceipt']['signature'] = 'A' * 86
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_accepted_copy'):
            self.verify()
        self.envelopes[0] = original
        with self.assertRaisesRegex(ImportConfirmationRejected, 'untrusted_import_key'):
            self.verify(trusted_import_keys={})

    def test_requires_ready_milestone_and_strict_timestamp(self):
        self.confirmation['milestone'] = 'accepted_copy'
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_import_milestone'):
            self.verify()
        self.confirmation['milestone'] = 'ready_for_review'
        self.confirmation['readyAt'] = '2026-02-30T01:02:03.000Z'
        self.sign()
        with self.assertRaisesRegex(ImportConfirmationRejected, 'invalid_ready_at'):
            self.verify()

    def test_treedar_and_combined_v2_require_full_ordered_closure(self):
        roles = ('ground_model', 'canopy_height', 'canopy_height_unfiltered', 'crown_labels',
                 'crowns', 'tree_table', 'tree_features', 'run_summary', 'report', 'preview',
                 'area_map_preview', 'engine_manifest')
        for with_odm in (False, True):
            with self.subTest(with_odm=with_odm):
                tree = [dict(id='tree-' + role, role=role, byteSize=5, sha256='c' * 64) for role in roles]
                odm = [dict(id='archive-1', role='odm_archive', byteSize=100, sha256='b' * 64)] if with_odm else []
                self.manifest = dict(protocol='ltds-reference-result-v2', schemaVersion=2,
                    jobId='job-1', generation=1, environment='staging', pipelineDigest='a' * 64,
                    engineVersions=({'odm': 'synthetic', 'treedar': 'synthetic'} if with_odm else {'treedar': 'synthetic'}),
                    objects=odm + tree)
                self.manifest['resultManifestSha256'] = protocol.digest(self.manifest)
                self.expected['resultManifestSha256'] = self.manifest['resultManifestSha256']
                self.expected['archiveInventorySha256'] = 'f' * 64 if with_odm else None
                self.envelopes = [self.envelope('a1', 's1', [item['id'] for item in self.manifest['objects']])]
                envelope = self.envelopes[0]
                receipt = envelope['viewerReceipt']
                digest = protocol.digest(envelope)
                self.expected['acceptedEnvelopeSha256'] = [digest]
                self.confirmation['resultManifestSha256'] = self.manifest['resultManifestSha256']
                self.confirmation['acceptedCopies'] = [dict(acceptanceReceiptId='a1', acceptanceStorageReceiptId='s1',
                    viewerReceiptId=receipt['receiptId'], viewerStorageReceiptId=receipt['storageReceiptId'],
                    envelopeSha256=digest)]
                self.confirmation['objects'] = copy.deepcopy(self.manifest['objects'])
                self.confirmation['archive'] = (dict(objectId='archive-1', byteSize=100, sha256='b' * 64,
                    inventorySha256='f' * 64) if with_odm else None)
                self.sign()
                self.assertEqual(self.verify().confirmation['objects'], self.manifest['objects'])
                self.confirmation['objects'].pop()
                self.sign()
                with self.assertRaisesRegex(ImportConfirmationRejected, 'confirmation_closure_mismatch'):
                    self.verify()


if __name__ == '__main__':
    unittest.main()
