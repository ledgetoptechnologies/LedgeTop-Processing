"""Private Viewer v2 manifests preserve exact ODM→TreeDar output closure."""
import copy
import hashlib
import unittest

from processing import reference_protocol as protocol
from processing.viewer_result_manifest import (
    TREEDAR_MAX_RESULT_OBJECTS,
    ViewerManifestRejected,
    derive_viewer_result_manifest,
    validate_viewer_acceptance,
    validate_viewer_result_manifest,
)
from processing.viewer_acceptance_receipt import verify_viewer_acceptance_receipt
from tests.viewer_receipt_signer import SyntheticViewerSigner


class ViewerResultManifestTests(unittest.TestCase):
    def setUp(self):
        self.job = dict(jobId='job-test', generation=3, environment='staging',
            pipelineDigest='a' * 64, manifestSha256='b' * 64)
        self.bytes = {
            **{f'treedar-{role}': f'synthetic {role}'.encode() for role in (
                'ground_model', 'canopy_height', 'canopy_height_unfiltered', 'crown_labels',
                'crowns', 'tree_table', 'tree_features', 'run_summary', 'report', 'preview',
                'area_map_preview', 'engine_manifest')},
            'odm-archive': b'synthetic odm archive',
        }
        self.tree_objects = [self._obj(f'treedar-{role}', role) for role in (
            'ground_model', 'canopy_height', 'canopy_height_unfiltered', 'crown_labels',
            'crowns', 'tree_table', 'tree_features', 'run_summary', 'report', 'preview',
            'area_map_preview')]
        self.internal = dict(schemaVersion=1, **{key: self.job[key] for key in (
            'jobId', 'generation', 'environment', 'pipelineDigest')},
            engineVersions={'odm': 'odm-test', 'treedar': 'treedar-test'},
            objects=self.tree_objects + [self._obj('odm-archive', 'odm_archive'),
                self._obj('treedar-engine_manifest', 'engine_manifest')])
        self.internal['resultManifestSha256'] = protocol.digest(self.internal)
        self.viewer = derive_viewer_result_manifest(self.internal, **self._identity())

    def _obj(self, object_id, role):
        value = self.bytes[object_id]
        return dict(id=object_id, role=role, byteSize=len(value), sha256=hashlib.sha256(value).hexdigest())

    def _identity(self):
        return dict(job_id=self.job['jobId'], generation=self.job['generation'],
            environment=self.job['environment'], pipeline_digest=self.job['pipelineDigest'])

    def _acceptance(self, manifest=None, ids=None):
        manifest = manifest or self.viewer
        return dict(schemaVersion=1, receiptId='receipt-test', storageReceiptId='storage-test',
            jobId=self.job['jobId'], generation=self.job['generation'], environment=self.job['environment'],
            inputManifestSha256=self.job['manifestSha256'], pipelineDigest=self.job['pipelineDigest'],
            resultManifestSha256=manifest['resultManifestSha256'],
            acceptedObjectIds=list(ids if ids is not None else [obj['id'] for obj in manifest['objects']]),
            acceptedAt='2026-11-02T12:00:00.000Z', validationVersion=protocol.VALIDATION_VERSION)

    def test_combined_job_derives_closed_v2_manifest_without_reordering(self):
        self.assertEqual(TREEDAR_MAX_RESULT_OBJECTS, 102)
        self.assertEqual(self.viewer['protocol'], 'ltds-reference-result-v2')
        self.assertEqual(self.viewer['schemaVersion'], 2)
        self.assertEqual(self.viewer['engineVersions'], {'odm': 'odm-test', 'treedar': 'treedar-test'})
        self.assertEqual([obj['id'] for obj in self.viewer['objects']],
            [obj['id'] for obj in self.internal['objects']])
        self.assertEqual(self.viewer['resultManifestSha256'], protocol.digest(
            {key: value for key, value in self.viewer.items() if key != 'resultManifestSha256'}))

    def test_tree_only_manifest_is_allowed_but_odm_v1_is_unchanged(self):
        tree = copy.deepcopy(self.internal)
        tree['engineVersions'] = {'treedar': 'treedar-test'}
        tree['objects'] = self.tree_objects + [self._obj('treedar-engine_manifest', 'engine_manifest')]
        tree['resultManifestSha256'] = protocol.digest({key: value for key, value in tree.items()
            if key != 'resultManifestSha256'})
        wire = derive_viewer_result_manifest(tree, **self._identity())
        self.assertEqual(wire['engineVersions'], {'treedar': 'treedar-test'})
        odm = copy.deepcopy(self.internal)
        odm['engineVersions'] = {'odm': 'odm-test'}
        odm['objects'] = [self._obj('odm-archive', 'odm_archive')]
        odm['resultManifestSha256'] = protocol.digest({key: value for key, value in odm.items()
            if key != 'resultManifestSha256'})
        self.assertEqual(derive_viewer_result_manifest(odm, **self._identity()), odm)

    def test_rejects_missing_engine_manifest_unknown_role_and_bad_odm_closure(self):
        for mutate in (
            lambda value: value['objects'].pop(),
            lambda value: value['objects'].__setitem__(0, dict(value['objects'][0], role='unknown_role')),
            lambda value: value['objects'].__setitem__(1, dict(value['objects'][1], role='tree_table')),
        ):
            invalid = copy.deepcopy(self.internal)
            mutate(invalid)
            invalid['resultManifestSha256'] = protocol.digest({key: value for key, value in invalid.items()
                if key != 'resultManifestSha256'})
            with self.assertRaises(ViewerManifestRejected):
                derive_viewer_result_manifest(invalid, **self._identity())

    def test_v2_acceptance_requires_full_manifest_order_and_signed_receipt_binds_it(self):
        acceptance = self._acceptance()
        checked = validate_viewer_acceptance(acceptance, result_manifest=self.viewer,
            input_manifest_sha256=self.job['manifestSha256'])
        signer = SyntheticViewerSigner()
        signed = signer.receipt(checked, self.viewer)
        verified = verify_viewer_acceptance_receipt(signed, trusted_keys=signer.trusted_keys,
            expected={key: checked[key] for key in ('jobId', 'generation', 'environment',
                'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')},
            result_manifest=self.viewer, accepted_object_ids=checked['acceptedObjectIds'])
        self.assertEqual(verified.receipt, signed)
        with self.assertRaises(ViewerManifestRejected):
            validate_viewer_acceptance(self._acceptance(ids=list(reversed(acceptance['acceptedObjectIds']))),
                result_manifest=self.viewer, input_manifest_sha256=self.job['manifestSha256'])

    def test_v2_validator_preserves_producer_order_and_rejects_digest_changes(self):
        value = copy.deepcopy(self.viewer)
        value['objects'].reverse()
        value['resultManifestSha256'] = protocol.digest({key: field for key, field in value.items()
            if key != 'resultManifestSha256'})
        # Order itself is not normalized; IDs are valid in any producer order.
        validated = validate_viewer_result_manifest(value, **self._identity())
        self.assertEqual([obj['id'] for obj in validated['objects']],
            [obj['id'] for obj in reversed(self.viewer['objects'])])
        value['resultManifestSha256'] = '0' * 64
        with self.assertRaises(ViewerManifestRejected):
            validate_viewer_result_manifest(value, **self._identity())

    def test_optional_producer_roles_match_viewer_allowlist(self):
        value = copy.deepcopy(self.internal)
        value['objects'].insert(-2, {
            'id': 'final-test-predictions', 'role': 'final_test_predictions',
            'byteSize': 3, 'sha256': hashlib.sha256(b'csv').hexdigest(),
        })
        value['resultManifestSha256'] = protocol.digest({key: item for key, item in value.items()
            if key != 'resultManifestSha256'})
        self.assertIn('final_test_predictions', [item['role'] for item in
            derive_viewer_result_manifest(value, **self._identity())['objects']])

    def test_v2_validator_requires_the_same_minimum_tree_outputs_as_viewer(self):
        value = copy.deepcopy(self.viewer)
        value['objects'] = [item for item in value['objects'] if item['role'] != 'ground_model']
        value['resultManifestSha256'] = protocol.digest({key: item for key, item in value.items()
            if key != 'resultManifestSha256'})
        with self.assertRaises(ViewerManifestRejected):
            validate_viewer_result_manifest(value, **self._identity())


if __name__ == '__main__':
    unittest.main()
