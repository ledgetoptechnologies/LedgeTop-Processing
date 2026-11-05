"""Accepted, signed V2 intake claims remain the authority for derived GCP input."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from processing import reference_protocol as p
from processing.reference_odm_inputs import validate_odm_input_closure, ODMInputError
from processing.reference_driver import ReferenceDriver, DriverHeld
from processing.source_spool import SourceSpool
from tests import test_reference_source_intake as intake_fixture
from tests import test_reference_engine_worker as engine_fixture


@unittest.skipUnless(__import__('sys').platform.startswith('linux'), 'real source-reader lease requires Linux')
class ODMInputClosureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.helper = intake_fixture.ReferenceSourceIntakeTests(methodName='runTest')
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.root = Path(self.temp.name)
        spool_root = self.root / 'spool'
        spool_root.mkdir(mode=0o700)
        marker = spool_root / '.ltds-source-spool-id'
        marker.write_text('a' * 64); marker.chmod(0o600)
        mounted = patch('processing.source_spool.os.path.ismount', return_value=True)
        mounted.start(); self.addCleanup(mounted.stop)
        spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0, max_object_bytes=1024)
        self.queue, self.store, self.control, job, _ = self.helper._make_reserved_source_job(
            self.root / 'queue', spool, 'odm-gcp')
        self.addCleanup(self.queue.close)
        self.job = self.store.get(job['jobId'], private=True)
        self.authority = self.queue.authority_id

    def validate(self, job=None):
        return validate_odm_input_closure(job or self.job, authority_id=self.authority)

    def test_signed_intake_claim_admits_only_its_snapshot_bound_derived_gcp(self):
        objects = self.validate()
        self.assertEqual([item['kind'] for item in objects], ['image', 'auxiliary'])
        self.assertEqual(objects[1]['name'], 'gcp_list.txt')
        objects[1]['name'] = 'changed.txt'
        self.assertEqual(self.job['submission']['manifest']['objects'][1]['name'], 'gcp_list.txt')
        self.assertEqual(self.job['submission']['manifest']['inputCounts']['images'], 1)
        self.assertEqual(self.job['submission']['manifest']['totalBytes'], 9)

    def test_legacy_images_remain_accepted_but_legacy_auxiliary_is_not(self):
        self.assertEqual(self.validate({'submission': {'manifest': {'objects': [{'kind': 'image'}]}}}), [{'kind': 'image'}])
        for key in ('sourceIntakeVersion', 'sourceAcceptanceBinding', 'derivedReceipt'):
            bad = copy.deepcopy(self.job); bad.pop(key, None)
            with self.subTest(key=key), self.assertRaises(ODMInputError): self.validate(bad)

    def test_changed_derived_receipt_snapshot_identity_and_hash_fail_closed(self):
        mutations = [
            lambda job: job['derivedReceipt'].update(environment='production'),
            lambda job: job['derivedReceipt'].update(attemptId='00000000-0000-4000-8000-000000000099'),
            lambda job: job['derivedReceipt']['objects'][0].update(snapshotEvidenceSha256='0' * 64),
            lambda job: job.update(derivedReceiptSha256='0' * 64),
            lambda job: job['sourceAcceptanceBinding'].update(derivedReceiptSha256='0' * 64),
            lambda job: job['sourceAcceptanceBinding'].update(coordinatorAuthorityId='00000000-0000-4000-8000-000000000099'),
            lambda job: job.update(sourceState='held'),
        ]
        for index, mutate in enumerate(mutations):
            bad = copy.deepcopy(self.job); mutate(bad)
            with self.subTest(index=index), self.assertRaises(ODMInputError): self.validate(bad)

    def test_renamed_raw_input_wrong_mime_and_duplicate_or_unbound_auxiliary_fail(self):
        mutations = [
            lambda job: job['submission']['manifest']['objects'][1].update(id=intake_fixture.GCP_ID),
            lambda job: job['submission']['manifest']['objects'][1].update(name='control.csv'),
            lambda job: job['submission']['manifest']['objects'][1].update(mediaType='application/octet-stream'),
            lambda job: job['submission']['manifest']['objects'][1].update(sha256='0' * 64),
            lambda job: job['submission']['manifest']['objects'][1].update(byteSize=999),
            lambda job: job['submission']['manifest']['objects'].append(copy.deepcopy(job['submission']['manifest']['objects'][1])),
            lambda job: job['submission']['manifest']['objects'][0].update(name='gcp_list.txt'),
        ]
        for index, mutate in enumerate(mutations):
            bad = copy.deepcopy(self.job); mutate(bad)
            with self.subTest(index=index), self.assertRaises(ODMInputError): self.validate(bad)

    def test_self_consistent_unapproved_derived_manifest_does_not_gain_trust(self):
        bad = copy.deepcopy(self.job)
        receipt = bad['derivedReceipt']
        receipt['objects'][0]['name'] = 'geo.txt'
        manifest = dict(schemaVersion=1, attemptId=bad['attemptId'], objects=receipt['objects'])
        manifest['sha256'] = p.digest(manifest)
        receipt['derivedManifestSha256'] = manifest['sha256']
        bad['derivedManifestSha256'] = manifest['sha256']
        bad['sourceAcceptanceBinding']['derivedManifestSha256'] = manifest['sha256']
        bad['derivedReceiptSha256'] = p.digest(receipt)
        bad['sourceAcceptanceBinding']['derivedReceiptSha256'] = p.digest(receipt)
        with self.assertRaises(ODMInputError): self.validate(bad)

    def test_driver_calls_shared_closure_before_execution(self):
        driver = object.__new__(ReferenceDriver)
        driver.worker = SimpleNamespace(authority_id=self.authority, allowed_options={'numeric-probe'})
        driver.node_id = 'cluster'; driver.allowed_environments = {'staging'}
        driver.execution_router = None; driver._combined_policy = None
        driver._option_definitions = {'numeric-probe': dict(name='numeric-probe', type='float', domain='positive float', value=1e-7)}
        driver._validate_job(self.job)
        bad = copy.deepcopy(self.job); bad['derivedReceiptSha256'] = '0' * 64
        with self.assertRaises(DriverHeld): driver._validate_job(bad)

    def test_combined_gcp_is_rejected_before_a_stage_plan_can_drop_it(self):
        from processing.reference_pipeline import _submission, PipelineHeld
        from processing.reference_source_intake_canonical import bind_validated_submission
        original = copy.deepcopy(self.job['submission'])
        original['manifest']['objects'].append(dict(id='lidar-1', name='original.laz', byteSize=3,
            sha256=__import__('hashlib').sha256(b'abc').hexdigest(), kind='lidar', mediaType='application/vnd.las'))
        manifest = original['manifest']
        manifest['objectCount'] = len(manifest['objects']); manifest['totalBytes'] += 3
        manifest['inputCounts'].update(lidarObjects=1, points=1)
        manifest['sha256'] = p.manifest_sha256(manifest)
        original['pipeline']['treedar'].update(requested=True, inputObjectIds=['lidar-1'], provenanceReceiptId='lidar-proof')
        raw = {key: original[key] for key in ('schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        checked = p.validate_submission(raw, environment=original['environment'],
            capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar'),
            allowed_odm_options={'numeric-probe'})
        normalized = bind_validated_submission(checked, original)
        with self.assertRaisesRegex(PipelineHeld, 'images_then_one_original_lidar_required'):
            _submission(normalized, original['environment'])

    def test_missing_or_mutated_derived_bytes_never_start_the_backend(self):
        from processing.node_gate import NodeGate
        from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
        from processing.scratch_policy import ScratchLedger
        from processing.worker_outbox import WorkerOutbox
        gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'node-gate')
        gate_patch.start(); self.addCleanup(gate_patch.stop)
        origin = 'http://127.0.0.1:8000'
        with NodeGate('physical-gcp') as gate:
            gate.provision('cluster', origin, self.authority)
        scratch = self.root / 'scratch'; scratch.mkdir(mode=0o700)
        ledger = ScratchLedger(self.root / 'scratch.sqlite', scratch, capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
        engine = engine_fixture.FakeNativeEngine()
        worker = ReferenceEngineWorker(self.root / 'worker.sqlite', ledger=ledger,
            outbox=WorkerOutbox(self.root / 'outbox.sqlite'), engine=engine, engine_id='physical-gcp',
            node_id='cluster', authority_id=self.authority, coordinator_origin=origin, gate_generation=1,
            worker_id='worker', engine_version='synthetic-v1', allowed_odm_options={'numeric-probe'},
            engine_probe=lambda _deadline: 'physical-gcp')
        ledger.reserve(job_id=self.job['jobId'], generation=1, reservation_id='gcp-reservation',
            manifest_sha256=self.job['manifestSha256'], input_bytes=self.job['submission']['manifest']['totalBytes'],
            output_budget=1024, environment='staging', ttl_seconds=3600, worker_ready=True)
        directory = scratch / 'jobs' / 'gcp-reservation'; directory.mkdir(parents=True, mode=0o700)
        paths = {item['id']: f'jobs/gcp-reservation/{item["id"]}.input' for item in self.job['submission']['manifest']['objects']}
        (scratch / paths[intake_fixture.IMAGE_ID]).write_bytes(b'abc')
        gcp = scratch / paths['gcp_' + intake_fixture.ATTEMPT]
        for case in ('missing', 'mutated'):
            if case == 'mutated': gcp.write_bytes(b'xxxxxx')
            with self.subTest(case=case), self.assertRaises((WorkerHeld, OSError)):
                worker.start(self.job, reservation_id='gcp-reservation', input_paths=paths)
            self.assertEqual(engine.calls, [], 'all input hashes must verify before initialize/upload/commit')

    def test_changed_signed_owner_on_existing_submission_is_a_replay_conflict(self):
        before = self.store.list(private=True)
        signed = self.helper.validate(allowed_odm_options={'numeric-probe'})
        payload = copy.deepcopy(self.helper.payload); payload['sourceAttestation'] = signed['sourceAttestation']
        proof = payload['sourceAttestation']['proof']
        proof['principalId'] = 'different-owner'
        from processing import viewer_source_attestation as attestation
        import base64, hashlib
        message = attestation.DOMAIN + p.canonical_json(proof)
        payload['sourceAttestation'].update(signature=base64.urlsafe_b64encode(self.helper.signing_key.sign(message)).decode().rstrip('='),
            attestationSha256=hashlib.sha256(message).hexdigest())
        response = self.control.handle('POST', '/ltds/v2/jobs', {'Authorization': 'Bearer synthetic-staging-token'},
            p.canonical_json(payload), protected_transport=True)
        self.assertEqual(response.status, 409)
        self.assertEqual(response.body, {'error': 'reference_conflict'})
        self.assertEqual(self.store.list(private=True), before)

    def test_fresh_signed_wrong_owner_is_rejected_and_matched_owner_admits(self):
        # Rebind the complete attempt closure, including derived object IDs and
        # snapshot attempt identity. A fresh submission must reach signature /
        # principal validation rather than duplicate-ID replay reconciliation.
        import json, base64, hashlib
        from processing import viewer_source_attestation as attestation
        fresh = '00000000-0000-4000-8000-000000000099'
        payload = json.loads(json.dumps(self.helper.payload).replace(intake_fixture.ATTEMPT, fresh))
        payload['providerTaskId'] = '00000000-0000-4000-8000-000000000098'
        for name in ('sourceManifest', 'derivedManifest'):
            payload[name]['sha256'] = p.digest({k:v for k,v in payload[name].items() if k != 'sha256'})
        payload['manifest']['sha256'] = p.manifest_sha256(payload['manifest'])
        valid = self.helper.validate(payload=payload, allowed_odm_options={'numeric-probe'})
        payload['sourceAttestation'] = valid['sourceAttestation']
        wrong = copy.deepcopy(payload)
        proof = wrong['sourceAttestation']['proof']
        proof['principalId'] = 'different-owner'
        message = attestation.DOMAIN + p.canonical_json(proof)
        wrong['sourceAttestation'].update(
            signature=base64.urlsafe_b64encode(self.helper.signing_key.sign(message)).decode().rstrip('='),
            attestationSha256=hashlib.sha256(message).hexdigest())
        before = self.store.list(private=True)
        headers = {'Authorization': 'Bearer synthetic-staging-token'}
        refused = self.control.handle('POST', '/ltds/v2/jobs', headers,
            p.canonical_json(wrong), protected_transport=True)
        self.assertEqual(refused.status, 400)
        self.assertEqual(refused.body, {'error': 'invalid_request'})
        self.assertEqual(self.store.list(private=True), before)
        # Exactly the same newly rebound request with the original valid owner
        # must admit, proving the negative was not another stale-binding error.
        accepted = self.control.handle('POST', '/ltds/v2/jobs', headers,
            p.canonical_json(payload), protected_transport=True)
        self.assertEqual(accepted.status, 201, accepted.body)
        self.assertEqual(accepted.body['attemptId'], fresh)
        self.assertEqual(accepted.body['sourceState'], 'receiving')
        after = self.store.list(private=True)
        self.assertEqual(len(after), len(before) + 1)
        self.assertTrue(all(job in after for job in before))
