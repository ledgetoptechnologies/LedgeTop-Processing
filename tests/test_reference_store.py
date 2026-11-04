import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from datetime import datetime, timezone
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue, PROFILE as LEGACY_PROFILE, VIEWER_PROFILE
from processing.node_readiness import NodeReadiness
from processing.maintenance import Conflict
from processing.reference_failure import FailureReceiver
from processing.reference_recovery import SCHEMA as RECOVERY_SCHEMA
from processing.reference_store import ClaimReleased, MAX_CLAIM_RELEASES_PER_JOB, ReferenceStore, PROFILE
from processing.reference_terminal import TerminalService
from tests.reference_activation_fixture import activate_reference_job, prepare_reference_job
from processing.source_range_transfer import CHUNK_BYTES, source_version_for_manifest


class ReferenceStoreTests(unittest.TestCase):
    def setUp(self):
        # Only exercises post-activation mechanics with a synthetic future grant.
        self.ready_patch = patch.object(ReferenceStore, '_activation_bound_grant_ready', return_value=True)
        self.ready_patch.start()
        self.addCleanup(self.ready_patch.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  "contracts/reference-v1.contract.json").read_text())
        self.submission = p.validate_submission(self.fixture["submission"], environment="staging",
            capability_fingerprint=self.fixture["capabilities"]["capabilityFingerprint"])

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def release(self, job, binding, token):
        return self.store.release_unstarted_claim(job['jobId'], **binding, claim_token=token,
            worker_attestation=p.release_attestation(job_id=job['jobId'], **binding))

    def new_submission(self):
        raw = copy.deepcopy(self.fixture["submission"])
        raw["submissionId"] = str(uuid.uuid4())
        return p.validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"])

    def reserve(self, job):
        self.store.claim(job["jobId"], node_id="cluster", worker_id="worker",
                         worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])
        transfer = copy.deepcopy(self.fixture["transferRequest"])
        transfer.update(manifestSha256=job["manifestSha256"], generation=job["generation"],
                        byteBudget=self.submission["manifest"]["totalBytes"])
        return self.store.reserve(job["jobId"], transfer, node_id="cluster")

    def result_ready(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.reserve(job)
        self.store.update_phase(job["jobId"], "fetching")
        self.store.update_phase(job["jobId"], "processing")
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"])
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        self.store.set_result_manifest(job["jobId"], result)
        self.store.update_phase(job["jobId"], "awaiting_acceptance")
        return job, result

    def receipt(self, job, result):
        receipt = copy.deepcopy(self.fixture["acceptance"])
        receipt.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"],
                       inputManifestSha256=job["manifestSha256"],
                       resultManifestSha256=result["resultManifestSha256"])
        return receipt

    def test_tightened_environment_rejects_unsigned_legacy_acceptance(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        self.assertFalse(self.store.acceptance_receipts(job['jobId'])['viewerCopyRequired'])
        self.store.viewer_required_environments = frozenset(('staging',))
        self.assertTrue(self.store.acceptance_receipts(job['jobId'])['viewerCopyRequired'])
        with self.assertRaises(Conflict):
            self.store.accept(job['jobId'], receipt)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances WHERE job_id=?',
                                       (job['jobId'],)).fetchone()[0], 0)
        self.assertEqual(self.store.get(job['jobId'])['phase'], 'awaiting_acceptance')

    def test_observed_copy_requirement_survives_restart_and_policy_relaxation(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        self.store.viewer_required_environments = frozenset(('staging',))
        self.assertTrue(self.store.acceptance_receipts(job['jobId'])['viewerCopyRequired'])
        reopened = ReferenceStore(self.queue, viewer_required_environments=())
        self.assertTrue(reopened.acceptance_receipts(job['jobId'])['viewerCopyRequired'])
        with self.assertRaises(Conflict):
            reopened.accept(job['jobId'], receipt)
        self.assertIn(job['jobId'], reopened.pending_viewer_transfer_jobs())

    def test_rejected_unsigned_receipt_latches_requirement_before_rollback(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        self.store.viewer_required_environments = frozenset(('staging',))
        with self.assertRaises(Conflict):
            self.store.accept(job['jobId'], receipt)
        reopened = ReferenceStore(self.queue, viewer_required_environments=())
        self.assertTrue(reopened.acceptance_receipts(job['jobId'])['viewerCopyRequired'])
        with self.assertRaises(Conflict):
            reopened.accept(job['jobId'], receipt)

    def test_relay_does_not_report_unsigned_legacy_completion_as_verified(self):
        from tests.viewer_receipt_signer import SyntheticViewerSigner
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        receipt['acceptedObjectIds'] = sorted(obj['id'] for obj in result['objects'])
        self.assertTrue(self.store.accept(job['jobId'], receipt)['fullyAccepted'])
        self.store.viewer_required_environments = frozenset(('staging',))
        relay = self.store.acceptance_receipts(job['jobId'])
        self.assertTrue(relay['viewerCopyRequired'])
        self.assertFalse(relay['fullyAccepted'])
        signer = SyntheticViewerSigner()
        self.store.accept(job['jobId'], receipt, viewer_receipt=signer.receipt(receipt, result),
                          trusted_viewer_receipt_keys=signer.trusted_keys)
        self.assertTrue(self.store.acceptance_receipts(job['jobId'])['fullyAccepted'])

    def test_retry_transfer_budget_has_explicit_three_copy_ceiling(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                         worker_instance_id='instance', claim_request_id='claim-budget')
        transfer = copy.deepcopy(self.fixture['transferRequest'])
        for budget in (2, 10):
            transfer['byteBudget'] = budget
            with self.assertRaises(Conflict):
                self.store.reserve(job['jobId'], transfer, node_id='cluster')
        transfer['byteBudget'] = 9
        self.assertEqual(self.store.reserve(job['jobId'], transfer, node_id='cluster')['phase'], 'awaiting_transfer')

    def test_wire_expiry_requires_viewer_compatible_utc_milliseconds(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='claim-expiry')
        for value in ('2030-01-01T00:00:00.123456Z', '2030-01-01T00:00:00Z', '2030-01-01T00:00:00.000+00:00'):
            transfer = {**self.fixture['transferRequest'], 'expiresAt': value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.reserve(job['jobId'], transfer, node_id='cluster')
        self.assertEqual(self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')['phase'], 'awaiting_transfer')

    def test_source_range_v2_reservation_is_bound_and_durable_in_the_shared_queue(self):
        submission = copy.deepcopy(self.fixture['submission'])
        submission['submissionId'] = str(uuid.uuid4())
        submission['manifest']['id'] = 'dataset-fixture-store'
        submission['manifest']['sha256'] = p.manifest_sha256(submission['manifest'])
        submission = p.validate_submission(submission, environment='staging',
            capability_fingerprint=submission['capabilityFingerprint'])
        job, _ = prepare_reference_job(self.store, submission)
        activate_reference_job(self.store, job)
        claim = self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance-v2', claim_request_id='claim-v2-' + job['jobId'])
        manifest = claim['submission']['manifest']
        chunks = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
        transfer = {
            'protocol': 'source-range-v2', 'reservationId': 'reservation-v2',
            'generation': claim['generation'], 'expiresAt': '2030-01-01T00:00:00.000Z',
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version_for_manifest(manifest),
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': chunks, 'byteBudget': manifest['totalBytes'],
            'nonce': 'nonce-v2', 'attemptBudget': chunks * 3,
        }
        reserved = self.store.reserve(job['jobId'], transfer, node_id='cluster')
        self.assertEqual(reserved['phase'], 'awaiting_transfer')
        self.assertEqual(reserved['transferRequest'], transfer)
        self.assertEqual(self.store.reserve(job['jobId'], transfer, node_id='cluster'), reserved)

        grant = {**transfer, 'expiresAt': '2029-01-01T00:00:00.000Z',
            'jobId': job['jobId'], 'environment': 'staging', 'token': 'A' * 43}
        receipt = self.store.receive_transfer_grant(job['jobId'], grant)
        self.assertTrue(receipt['accepted'])
        public = self.store.get(job['jobId'])
        self.assertNotIn('token', json.dumps(public))
        self.assertEqual(self.store.private_transfer_grant(job['jobId']), grant)

    def test_expired_source_range_exact_replay_preserves_reservation_and_restart_hold(self):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        raw['manifest']['id'] = 'dataset-replay-fixture'
        raw['manifest']['sha256'] = p.manifest_sha256(raw['manifest'])
        submission = p.validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'])
        job, _ = prepare_reference_job(self.store, submission)
        activate_reference_job(self.store, job)
        claim = self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance-v2', claim_request_id='replay-' + job['jobId'])
        manifest = claim['submission']['manifest']
        chunks = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
        request = {'protocol': 'source-range-v2', 'reservationId': 'replay-reservation',
            'generation': claim['generation'], 'expiresAt': '2030-01-01T00:00:00.000Z',
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version_for_manifest(manifest),
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': chunks, 'byteBudget': manifest['totalBytes'],
            'nonce': 'replay-nonce', 'attemptBudget': chunks * 3}
        with patch('processing.reference_store.time.time', return_value=1893455999):
            accepted = self.store.reserve(job['jobId'], request, node_id='cluster')
        # Reconstruct the store after losing its response; exact persisted bytes only.
        reopened = ReferenceStore(self.queue)
        with patch('processing.reference_store.time.time', return_value=1893456001):
            self.assertEqual(reopened.reserve(job['jobId'], request, node_id='cluster'), accepted)
            for key, value in (('nonce', 'different-nonce'), ('byteBudget', request['byteBudget'] + 1),
                               ('expiresAt', '2031-01-01T00:00:00.000Z')):
                changed = dict(request, **{key: value})
                with self.subTest(key=key), self.assertRaises(Conflict):
                    reopened.reserve(job['jobId'], changed, node_id='cluster')
            with self.assertRaises(Conflict):
                reopened.reserve(job['jobId'], request, node_id='node-2')
            with self.queue._db() as db:
                persisted = json.loads(db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?',
                    (job['jobId'],)).fetchone()[0])
            self.assertEqual(persisted, request)
            self.assertIsNone(reopened.private_transfer_grant(job['jobId']))
        # A full coordinator restart intentionally holds in-flight work; do not
        # acknowledge it as running or reset ownership merely to permit replay.
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=('cluster', 'node-2'))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        with patch('processing.reference_store.time.time', return_value=1893456001):
            with self.assertRaises(Conflict):
                self.store.reserve(job['jobId'], request, node_id='cluster')
        self.assertEqual(self.store.get(job['jobId'])['status'], 'held')

    def test_source_range_v2_reservation_rejects_source_identity_and_unbounded_budgets(self):
        submission = copy.deepcopy(self.fixture['submission'])
        submission['submissionId'] = str(uuid.uuid4())
        submission['manifest']['id'] = 'dataset-fixture-store'
        submission['manifest']['sha256'] = p.manifest_sha256(submission['manifest'])
        submission = p.validate_submission(submission, environment='staging',
            capability_fingerprint=submission['capabilityFingerprint'])
        job, _ = prepare_reference_job(self.store, submission)
        activate_reference_job(self.store, job)
        claim = self.store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance-v2', claim_request_id='claim-v2-' + job['jobId'])
        manifest = claim['submission']['manifest']
        chunks = sum((item['byteSize'] + CHUNK_BYTES - 1) // CHUNK_BYTES for item in manifest['objects'])
        base = {'protocol': 'source-range-v2', 'reservationId': 'reservation-v2',
            'generation': claim['generation'], 'expiresAt': '2030-01-01T00:00:00.000Z',
            'manifestSha256': manifest['sha256'], 'sourceVersion': source_version_for_manifest(manifest),
            'objectIds': [item['id'] for item in manifest['objects']], 'chunkBytes': CHUNK_BYTES,
            'requestBudget': chunks, 'byteBudget': manifest['totalBytes'],
            'nonce': 'nonce-v2', 'attemptBudget': chunks * 3}
        for mutation in (
            {'sourceVersion': 'dataset:wrong:' + manifest['sha256']},
            {'objectIds': ['unbound-object']},
            {'requestBudget': chunks + 1},
            {'attemptBudget': chunks * 4},
            {'expiresAt': '2020-01-01T00:00:00.000Z'},
        ):
            with self.subTest(mutation=mutation), self.assertRaises(Conflict):
                self.store.reserve(job['jobId'], {**base, **mutation}, node_id='cluster')

    def test_disabled_profile_fails_closed(self):
        self.queue.allowed_profiles.remove(PROFILE)
        with self.assertRaises(ValueError):
            ReferenceStore(self.queue)

    def test_atomic_concurrent_idempotency_and_shared_queue(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            answers = list(pool.map(lambda _: self.store.submit(self.submission), range(8)))
        self.assertEqual(sum(created for _, created in answers), 1)
        self.assertEqual(len({job["jobId"] for job, _ in answers}), 1)
        queue_jobs = self.queue.list()
        self.assertEqual(len(queue_jobs), 1)
        self.assertEqual(queue_jobs[0]["profile"], PROFILE)

    def test_submission_conflicts_and_unvalidated_digests(self):
        self.store.submit(self.submission)
        spoofed = copy.deepcopy(self.submission)
        spoofed["environment"] = "production"
        with self.assertRaises(Conflict):
            self.store.submit(spoofed)
        for mutation in ({"requestDigest": "0" * 64}, {"credential": "secret"},
                         {"manifestSha256": "0" * 64}):
            bad = copy.deepcopy(self.submission)
            bad.update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.store.submit(bad)

    def test_pending_quota_is_not_historical_count(self):
        store = ReferenceStore(self.queue, max_queued_jobs=1)
        job, _ = store.submit(self.submission)
        other = self.new_submission()
        with self.assertRaises(Conflict):
            store.submit(other)
        store.update_phase(job["jobId"], "cancelled")
        store.submit(other)
        self.assertEqual(len(self.queue.list()), 2)

    def test_fifty_gigabyte_ten_thousand_photo_manifest_routes_to_main(self):
        raw = copy.deepcopy(self.fixture["submission"])
        raw["submissionId"] = str(uuid.uuid4())
        manifest = raw["manifest"]
        manifest["objects"] = [
            {"id": f"image-{index:05d}", "name": f"image-{index:05d}.jpg",
             "byteSize": 5_000_000, "sha256": "a" * 64,
             "kind": "image", "mediaType": "image/jpeg"}
            for index in range(10_000)
        ]
        manifest.update(totalBytes=50_000_000_000, objectCount=10_000,
                        inputCounts={"images": 10_000, "lidarObjects": 0, "points": 0})
        manifest["sha256"] = p.manifest_sha256(manifest)
        checked = p.validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"])
        self.assertEqual(checked["manifest"]["totalBytes"], 50_000_000_000)
        job, created = prepare_reference_job(self.store, checked)
        activate_reference_job(self.store, job)
        self.assertTrue(created)
        self.assertEqual(self.store._required_node(checked), "node-2")
        self.assertIsNone(self.store.next_for_node("cluster"))
        self.assertEqual(self.store.next_for_node("node-2")["jobId"], job["jobId"])

    def test_candidate_inventory_atomic_discovery_and_claims(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {node['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                 for node in inventory}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda n: ready[n])
        first, _ = store.submit(self.new_submission())
        second, _ = store.submit(self.new_submission())
        self.assertEqual(store.next_for_node('cluster')['jobId'], first['jobId'])
        self.assertIsNone(store.next_for_node('node-2'))
        claim = lambda job, node: store.claim(job, node_id=node, worker_id=node,
            worker_instance_id='instance', claim_request_id='claim-' + job)
        self.assertIsNone(claim(first['jobId'], 'node-2'))
        self.assertIsNotNone(claim(first['jobId'], 'cluster'))
        self.assertEqual(store.next_for_node('node-2')['jobId'], second['jobId'])
        self.assertIsNotNone(claim(second['jobId'], 'node-2'))
        self.assertEqual([self.queue.get(job['jobId'])['nodeId'] for job in (first, second)],
                         ['cluster', 'node-2'])

    def test_candidate_inventory_routes_the_three_photo_batch_example(self):
        # Held-only: exercise actual principal/authority authentication, v3
        # prepared persistence and activation. Synthetic queue completion does
        # not prove an engine stopped or that Viewer accepted a result.
        from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
        from processing.prepared_envelope_jcs_candidate import PROTOCOL
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1000, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        report = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                  'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory,
                               node_readiness=lambda _: report, jcs_runtime_enabled=True)
        capabilities = self.fixture['capabilities']
        legacy = dict(capabilityFingerprint=capabilities['capabilityFingerprint'],
            optionsFingerprint=capabilities['engines']['odm']['optionsFingerprint'],
            allowedOdmOptions=['resolution'], availableEngines=['odm'])
        policy = dict(capabilityFingerprint=legacy['capabilityFingerprint'],
            optionsFingerprint=legacy['optionsFingerprint'], availableEngines=['odm'],
            optionDefinitions={'resolution': {'type': 'float', 'domain': {'min': -100, 'max': 100}}})
        tokens = {'production':'synthetic-v3-production-token-001', 'staging':'synthetic-v3-staging-token-001'}
        api = PreparedSubmissionAPI(store, credentials=[dict(token=token, principalId='viewer-a', environment=env)
            for env, token in tokens.items()], legacy_bearer_tokens=[],
            policies={env:legacy for env in tokens}, jcs_policies={env:policy for env in tokens},
            expected_authority_id=self.queue.authority_id)

        def post(path, value, environment, token=None):
            return api.handle('POST', path, {'Authorization':'Bearer '+(token or tokens[environment]),
                'X-LTDS-Expected-Coordinator-Authority':self.queue.authority_id},
                json.dumps(value).encode(), protected_transport=True)

        def submit(count, environment='production'):
            raw = copy.deepcopy(self.fixture['submission'])
            raw['submissionId'] = str(uuid.uuid4())
            manifest = raw['manifest']
            manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}',
                                    'name': f'image-{i}.jpg'} for i in range(count)]
            manifest.update(objectCount=count, totalBytes=count * 3,
                            inputCounts={'images':count, 'lidarObjects':0, 'points':0})
            manifest['sha256'] = p.manifest_sha256(manifest)
            envelope = dict(schemaVersion=3, protocol=PROTOCOL, submission=raw,
                viewerTaskId=str(uuid.uuid4()), viewerAttemptId=str(uuid.uuid4()), activationId=str(uuid.uuid4()))
            response = post('/ltds/prepared/v3/submissions', envelope, environment)
            self.assertEqual(response.status, 201, response.body)
            job=response.body
            found=store.find_viewer_envelope_v2(raw['submissionId'], authenticated_principal='viewer-a',
                authenticated_environment=environment, expected_protocol=PROTOCOL)
            activation=dict(activationId=envelope['activationId'], viewerTaskId=envelope['viewerTaskId'],
                viewerAttemptId=envelope['viewerAttemptId'], envelopeSha256=found['envelopeSha256'])
            path='/ltds/prepared/v3/submissions/'+raw['submissionId']+'/activation'
            # Without both test-only source/worker readiness overrides, v3
            # activation is rejected and jobs cannot be claimed.
            with patch.object(ReferenceStore, '_v3_source_contract_ready', return_value=False):
                self.assertEqual(post(path,activation,environment).status,503)
            self.assertIsNone(store.claim(job['jobId'], node_id='cluster', worker_id='cluster',
                worker_instance_id='instance', claim_request_id='unactivated-'+job['jobId']))
            self.assertEqual(post(path,activation,environment,token='invalid-auth-token-000000').status,401)
            with patch.object(ReferenceStore, '_v3_source_contract_ready', return_value=True):
                self.assertEqual(post(path,activation,environment).status,200)
            return job,envelope

        def claim(job, node):
            return store.claim(job['jobId'], node_id=node, worker_id=node,
                worker_instance_id='instance', claim_request_id='claim-'+job['jobId'])

        (small,senv),(medium,menv),(large,_) = (submit(count) for count in (100,500,5000))
        self.assertEqual(store.next_for_node('cluster')['jobId'],small['jobId'])
        first_claim=claim(small,'cluster')
        self.assertEqual(first_claim['sourceViewerAttemptId'],senv['viewerAttemptId'])
        self.assertEqual(store.next_for_node('node-2')['jobId'],medium['jobId'])
        second_claim=claim(medium,'node-2')
        self.assertEqual(second_claim['sourceViewerAttemptId'],menv['viewerAttemptId'])
        self.assertEqual(self.queue.get(large['jobId'])['status'],'queued')
        self.queue.complete(small['jobId'],{'claimToken':first_claim['claimToken'],'status':'succeeded','result':{}})
        self.assertIsNone(store.next_for_node('cluster'))
        staging,_=submit(20,'staging')
        later_small,_=submit(50)
        self.assertEqual(store.next_for_node('cluster')['jobId'],later_small['jobId'])
        self.assertIsNone(claim(staging,'cluster'))
        self.assertIsNotNone(claim(later_small,'cluster'))
        self.assertEqual(self.queue.get(large['jobId'])['status'],'queued')
        self.assertEqual(self.queue.get(staging['jobId'])['status'],'queued')
        self.queue.complete(medium['jobId'],{'claimToken':second_claim['claimToken'],'status':'succeeded','result':{}})
        self.assertEqual(store.next_for_node('node-2')['jobId'],large['jobId'])
        self.assertIsNotNone(claim(large,'node-2'))
        self.assertEqual(self.queue.get(staging['jobId'])['status'],'queued')

    def test_unowned_legacy_reference_job_remains_held(self):
        job,_=self.store.submit(self.submission)
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertIsNone(self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',
            worker_instance_id='instance',claim_request_id='unowned-legacy-claim'))
        self.assertEqual(self.queue.get(job['jobId'])['status'],'queued')

    def test_candidate_inventory_racing_claims_do_not_share_a_slot(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        store = ReferenceStore(self.queue, node_inventory=inventory,
            node_readiness=lambda _: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
        job, _ = store.submit(self.new_submission())
        def claim(i):
            return store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                worker_instance_id='instance', claim_request_id='race-' + str(i))
        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(claim, range(2)))
        self.assertEqual(sum(value is not None for value in claims), 1)

    def test_reference_can_pass_viewer_job_only_on_photo_ineligible_node(self):
        self.queue.per_node_slots = True
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda _: ready)
        viewer, _ = self.queue.submit({'requestId': 'viewer-first', 'environment': 'staging',
                                       'profile': VIEWER_PROFILE})
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
            db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')
            db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('viewer-task', viewer['id']))
            db.executemany('INSERT INTO viewer_files VALUES(?,1)', [('viewer-task',), ('viewer-task',)])
        reference, _ = store.submit(self.new_submission())
        self.assertEqual(store.next_for_node('cluster')['jobId'], reference['jobId'])
        self.assertIsNone(store.next_for_node('node-2'))
        self.assertIsNone(store.claim(reference['jobId'], node_id='node-2', worker_id='main',
                                      worker_instance_id='instance', claim_request_id='wrong-node'))
        self.assertIsNotNone(store.claim(reference['jobId'], node_id='cluster', worker_id='local',
                                         worker_instance_id='instance', claim_request_id='small-node'))
        self.assertEqual(self.queue.get(viewer['id'])['status'], 'queued')

    def test_reference_does_not_bypass_unknown_viewer_photo_count(self):
        self.queue.per_node_slots = True
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda _: ready)
        self.queue.submit({'requestId': 'viewer-missing', 'environment': 'staging', 'profile': VIEWER_PROFILE})
        reference, _ = store.submit(self.new_submission())
        self.assertIsNone(store.next_for_node('cluster'))
        self.assertIsNone(store.claim(reference['jobId'], node_id='cluster', worker_id='local',
                                      worker_instance_id='instance', claim_request_id='missing-evidence'))

    def test_mixed_profile_concurrent_claims_take_distinct_eligible_nodes(self):
        self.queue.per_node_slots = True
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda _: ready)
        viewer, _ = self.queue.submit({'requestId': 'mixed-viewer', 'environment': 'staging',
                                       'profile': VIEWER_PROFILE})
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
            db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')
            db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('viewer-task', viewer['id']))
            db.executemany('INSERT INTO viewer_files VALUES(?,1)', [('viewer-task',), ('viewer-task',)])
        reference, _ = store.submit(self.new_submission())
        barrier = threading.Barrier(2)
        def take_viewer():
            barrier.wait(timeout=5)
            return self.queue.claim({'workerId': 'viewer', 'nodeId': 'node-2',
                'profiles': [VIEWER_PROFILE], 'expectedJobId': viewer['id']},
                allow_viewer_same_priority_skip=True)
        def take_reference():
            barrier.wait(timeout=5)
            return store.claim(reference['jobId'], node_id='cluster', worker_id='reference',
                               worker_instance_id='instance', claim_request_id='mixed-claim')
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (pool.submit(take_viewer), pool.submit(take_reference))
            results = [future.result(timeout=10) for future in futures]
        self.assertTrue(all(results))
        self.assertEqual(self.queue.get(viewer['id'])['nodeId'], 'node-2')
        self.assertEqual(self.queue.get(reference['jobId'])['nodeId'], 'cluster')

    def test_oversized_production_viewer_yields_idle_small_node_to_staging_reference(self):
        self.queue.per_node_slots = True
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        self.queue.node_photo_limits = {'cluster': 1, 'node-2': None}
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda _: ready)
        viewer, _ = self.queue.submit({'requestId': 'production-viewer', 'environment': 'production',
                                       'profile': VIEWER_PROFILE})
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
            db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')
            db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('viewer-task', viewer['id']))
            db.executemany('INSERT INTO viewer_files VALUES(?,1)', [('viewer-task',), ('viewer-task',)])
        reference, _ = store.submit(self.new_submission())
        self.assertEqual(store.next_for_node('cluster')['jobId'], reference['jobId'])
        self.assertIsNotNone(store.claim(reference['jobId'], node_id='cluster', worker_id='reference',
                                         worker_instance_id='instance', claim_request_id='priority-claim'))
        self.assertEqual(self.queue.get(reference['jobId'])['status'], 'running')
        self.assertEqual(self.queue.get(viewer['id'])['status'], 'queued')

    def test_viewer_claim_bypasses_reference_only_if_node_cannot_fit_it(self):
        self.queue.per_node_slots = True
        self.queue.node_photo_limits = {'cluster': 1, 'node-2': None}
        self.queue.allowed_profiles.add(VIEWER_PROFILE)
        raw = copy.deepcopy(self.fixture['submission'])
        manifest = raw['manifest']
        manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}',
                                'name': f'image-{i}.jpg'} for i in range(2)]
        manifest.update(objectCount=2, totalBytes=6,
                        inputCounts={'images': 2, 'lidarObjects': 0, 'points': 0})
        manifest['sha256'] = p.manifest_sha256(manifest)
        reference, _ = self.store.submit(p.validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint']))
        viewer, _ = self.queue.submit({'requestId': 'viewer-after-reference', 'environment': 'staging',
                                       'profile': VIEWER_PROFILE})
        request = {'workerId': 'viewer-dispatch', 'nodeId': 'node-2',
                   'profiles': [VIEWER_PROFILE], 'expectedJobId': viewer['id']}
        self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'node-2'))
        self.assertIsNone(self.queue.claim(request, allow_viewer_same_priority_skip=True))
        self.assertTrue(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        # A changed manifest after advisory discovery must block atomic claim.
        with self.queue._db() as db:
            db.execute('UPDATE reference_jobs SET manifest_sha=? WHERE job_id=?', ('0' * 64, reference['jobId']))
        request['nodeId'] = 'cluster'
        self.assertIsNone(self.queue.claim(request, allow_viewer_same_priority_skip=True))
        with self.queue._db() as db:
            db.execute('UPDATE reference_jobs SET manifest_sha=? WHERE job_id=?',
                       (manifest['sha256'], reference['jobId']))
        self.assertIsNotNone(self.queue.claim(request, allow_viewer_same_priority_skip=True))
        self.assertEqual(self.queue.get(reference['jobId'])['status'], 'queued')

    def test_released_unstarted_claim_is_fenced_from_replay(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='release-request')
        claim = self.store.claim(job['jobId'], **binding)
        token = claim['claimToken']
        self.assertTrue(self.release(job, binding, token))
        self.assertFalse(self.release(job, binding, token))
        self.assertEqual(self.store.get(job['jobId'])['phase'], 'waiting_capacity')
        with self.assertRaises(Conflict):
            self.store.claim(job['jobId'], **binding)
        new = self.store.claim(job['jobId'], **{**binding, 'claim_request_id': 'new-request'})
        self.assertNotEqual(new['claimToken'], token)
        identity = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')
        with self.assertRaises(Conflict):
            self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster',
                               worker_identity=identity, claim_token=token)
        with self.assertRaises(Conflict):
            self.store.update_phase(job['jobId'], 'fetching', worker_identity=identity, claim_token=token)
        with self.assertRaises(Conflict):
            self.store.private_transfer_grant(job['jobId'], worker_identity=identity, claim_token=token)
        with self.assertRaises(Conflict):
            self.store.claim_snapshot(job['jobId'], node_id='cluster', worker_id='worker',
                worker_instance_id='instance', claim_token=token)

    def test_release_requires_exact_worker_attestation_and_keeps_generation(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='attested')
        token = self.store.claim(job['jobId'], **binding)['claimToken']
        valid = p.release_attestation(job_id=job['jobId'], **binding)
        changed = p.release_attestation(job_id=job['jobId'], **{**binding, 'claim_request_id': 'other'})
        for attestation in (None, {}, changed, {**valid, 'attestationSha256': '0' * 64}):
            with self.subTest(attestation=attestation), self.assertRaises(Conflict):
                self.store.release_unstarted_claim(job['jobId'], **binding, claim_token=token,
                                                   worker_attestation=attestation)
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')
        self.assertTrue(self.release(job, binding, token))
        new = self.store.claim(job['jobId'], **{**binding, 'claim_request_id': 'attested-new'})
        self.assertEqual(new['generation'], 1)

    def test_release_refuses_any_worker_or_viewer_evidence(self):
        FailureReceiver(self.store)
        TerminalService(self.store)
        with self.queue._db() as db:
            db.execute(RECOVERY_SCHEMA)
        inserts = {
            'reference_worker_failures': "INSERT INTO reference_worker_failures VALUES(?,1,'f','p','{}','{}','pending_queue_completion')",
            'reference_terminal_acceptances': "INSERT INTO reference_terminal_acceptances VALUES('t','s',?,1,'d','{}')",
            'reference_recovery_receipts': "INSERT INTO reference_recovery_receipts VALUES('r',?,'e','s','{}','{}')",
            'reference_worker_receipts': "INSERT INTO reference_worker_receipts VALUES('o','s',?,'{}')",
            'reference_acceptances': "INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES('a',?,'d','{}')",
            'reference_viewer_receipts': "INSERT INTO reference_viewer_receipts(receipt_id,storage_receipt_id,acceptance_receipt_id,job_id,digest,receipt_json) VALUES('v','s','a',?,'d','{}')",
            'reference_viewer_transfers': "INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json) VALUES(?,'r','{}')",
        }
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='reported')
        token = self.store.claim(job['jobId'], **binding)['claimToken']
        for table, statement in inserts.items():
            with self.subTest(table=table):
                with self.queue._db() as db:
                    db.execute(statement, (job['jobId'],))
                with self.assertRaises(Conflict):
                    self.release(job, binding, token)
                self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')
                with self.queue._db() as db:
                    db.execute(f'DELETE FROM {table}')
        with self.subTest(table='reference_outbox'):
            with self.queue._db() as db:
                db.execute("INSERT INTO reference_outbox(identity,kind,payload_json,digest) VALUES('i','terminal',?,'d')",
                           (json.dumps({'jobId': job['jobId']}),))
            with self.assertRaises(Conflict):
                self.release(job, binding, token)
            with self.queue._db() as db:
                db.execute('DELETE FROM reference_outbox')
        self.assertTrue(self.release(job, binding, token))

    def test_release_budget_bounds_tombstones_and_leaves_claim_owned(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')
        for cycle in range(50):
            request = {**binding, 'claim_request_id': 'cycle-' + str(cycle)}
            claim = self.store.claim(job['jobId'], **request)
            if cycle < MAX_CLAIM_RELEASES_PER_JOB:
                self.assertTrue(self.release(job, request, claim['claimToken']))
                continue
            with self.assertRaisesRegex(Conflict, 'budget'):
                self.release(job, request, claim['claimToken'])
            break
        self.assertEqual(cycle, MAX_CLAIM_RELEASES_PER_JOB)
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_released_claims').fetchone()[0],
                             MAX_CLAIM_RELEASES_PER_JOB)

    def test_release_refuses_changed_coordinator_authority(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='authority')
        token = self.store.claim(job['jobId'], **binding)['claimToken']
        with self.queue._db() as db:
            db.execute("UPDATE coordinator_identity SET authority_id='different-authority'")
        with self.assertRaises(Conflict):
            self.release(job, binding, token)
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')

    def test_closed_admission_replay_answers_released_like_open_claim(self):
        from processing.admission_drain import AdmissionDrain
        self.queue.admission_drain = AdmissionDrain(self.queue)
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='drain-release')
        self.release(job, binding, self.store.claim(job['jobId'], **binding)['claimToken'])
        with self.assertRaises(ClaimReleased):
            self.store.claim(job['jobId'], **binding)
        gate = self.queue.admission_drain
        gate.set_closed(True, request_id=str(uuid.uuid4()), expected_generation=0, expected_closed=False,
                        expected_authority=self.queue.authority_id)
        with self.assertRaises(ClaimReleased):
            self.store.claim(job['jobId'], **binding)
        with self.assertRaises(Conflict) as other:
            self.store.claim(job['jobId'], **{**binding, 'worker_instance_id': 'another'})
        self.assertNotIsInstance(other.exception, ClaimReleased)

    def test_release_refuses_after_transfer_request(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='release-after-reserve')
        claim = self.store.claim(job['jobId'], **binding)
        self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster')
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])

    def test_release_and_reservation_race_has_one_winner(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='race-release')
        claim = self.store.claim(job['jobId'], **binding)
        identity = {key: binding[key] for key in ('node_id', 'worker_id', 'worker_instance_id')}
        barrier = threading.Barrier(2)
        def attempt(kind):
            barrier.wait(timeout=5)
            try:
                if kind == 'release':
                    self.release(job, binding, claim['claimToken'])
                else:
                    self.store.reserve(job['jobId'], self.fixture['transferRequest'], node_id='cluster',
                        worker_identity=identity, claim_token=claim['claimToken'])
                return kind
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(attempt, ('release', 'reserve')))
        self.assertEqual(sum(item is not None for item in outcomes), 1)
        current = self.store.get(job['jobId'])
        if 'release' in outcomes:
            self.assertEqual((current['status'], current['phase']), ('queued', 'waiting_capacity'))
            self.assertIsNone(current['transferRequest'])
        else:
            self.assertEqual((current['status'], current['phase']), ('running', 'awaiting_transfer'))

    def test_release_refuses_inconsistent_queue_claim_hash(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='corrupt-claim')
        claim = self.store.claim(job['jobId'], **binding)
        with self.queue._db() as db:
            db.execute('UPDATE jobs SET claim_hash=? WHERE id=?', ('0' * 64, job['jobId']))
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')

    def test_release_refuses_sealed_failure_or_worker_receipt(self):
        from processing.reference_failure import FailureReceiver
        FailureReceiver(self.store)
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='sealed-failure')
        claim = self.store.claim(job['jobId'], **binding)
        with self.queue._db() as db:
            db.execute("INSERT INTO reference_worker_failures VALUES(?,?,?,?,?,?,'pending_queue_completion')",
                       (job['jobId'], 1, 'a' * 64, 'b' * 64, '{}', '{}'))
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_worker_failures WHERE job_id=?', (job['jobId'],))
            db.execute('INSERT INTO reference_worker_receipts VALUES(?,?,?,?)',
                       ('outbox-id', 'c' * 64, job['jobId'], '{}'))
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_worker_receipts WHERE job_id=?', (job['jobId'],))
            db.execute('INSERT INTO reference_outbox(identity,kind,payload_json,digest) VALUES(?,?,?,?)',
                       ('synthetic-outbox', 'synthetic', json.dumps({'jobId': job['jobId']}), 'd' * 64))
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'running')

    def test_release_refuses_changed_coordinator_authority(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='authority-release')
        claim = self.store.claim(job['jobId'], **binding)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=? WHERE singleton=1', (str(uuid.uuid4()),))
        with self.assertRaises(Conflict):
            self.release(job, binding, claim['claimToken'])

    def test_released_claim_tombstone_survives_coordinator_restart(self):
        job, _ = self.store.submit(self.new_submission())
        binding = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
                       claim_request_id='restart-release')
        old = self.store.claim(job['jobId'], **binding)
        self.release(job, binding, old['claimToken'])
        before = self.store.get(job['jobId'])
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=('cluster', 'node-2'))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        after = self.store.get(job['jobId'])
        self.assertEqual((after['phase'], after['manifestSha256'], after['pipelineDigest']),
                         ('waiting_capacity', before['manifestSha256'], before['pipelineDigest']))
        with self.assertRaises(Conflict):
            self.store.claim(job['jobId'], **binding)
        self.assertFalse(self.release(job, binding, old['claimToken']))
        new = self.store.claim(job['jobId'], **{**binding, 'claim_request_id': 'restart-new'})
        self.assertNotEqual(new['claimToken'], old['claimToken'])

    def test_candidate_inventory_skips_ineligible_head_without_bypassing_production(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {node['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                 for node in inventory}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda n: ready[n])
        def submission(images, environment):
            raw = copy.deepcopy(self.fixture['submission'])
            raw['submissionId'] = str(uuid.uuid4())
            m = raw['manifest']
            m['objects'] = [{**m['objects'][0], 'id': 'image-' + str(i),
                             'name': 'image-' + str(i) + '.jpg'} for i in range(images)]
            m['objectCount'] = images
            m['totalBytes'] = images * 3
            m['inputCounts']['images'] = images
            m['sha256'] = p.manifest_sha256(m)
            return p.validate_submission(raw, environment=environment,
                capability_fingerprint=raw['capabilityFingerprint'])
        def claim(job, node):
            return store.claim(job['jobId'], node_id=node, worker_id=node,
                worker_instance_id='instance', claim_request_id='claim-' + job['jobId'])
        running, _ = store.submit(submission(2, 'production'))
        self.assertIsNotNone(claim(running, 'node-2'))
        large, _ = store.submit(submission(2, 'production'))
        staging, _ = store.submit(submission(1, 'staging'))
        small, _ = store.submit(submission(1, 'production'))
        self.assertEqual(store.next_for_node('cluster')['jobId'], small['jobId'])
        self.assertIsNone(claim(staging, 'cluster'))
        self.assertIsNotNone(claim(small, 'cluster'))
        self.assertEqual(self.queue.get(large['jobId'])['status'], 'queued')
        self.assertEqual(self.queue.get(staging['jobId'])['status'], 'queued')

    def test_candidate_inventory_rechecks_node_readiness_on_claim(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        ready = {n['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                 for n in inventory}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda n: ready[n])
        job, _ = store.submit(self.new_submission())
        self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])
        ready['cluster'] = {'ready': False, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        def claim(node):
            return store.claim(job['jobId'], node_id=node, worker_id=node,
                worker_instance_id='instance', claim_request_id='claim-' + node)
        self.assertIsNone(claim('cluster'))
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
        self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
        self.assertIsNotNone(claim('node-2'))

    def test_candidate_inventory_rechecks_maintenance_after_discovery(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        report = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                  'availableScratchBytes': 1000000, 'outputBudgetBytes': 1024}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda _: report)
        job, _ = store.submit(self.new_submission())
        self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])
        now = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': now,
            'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        self.assertIsNone(store.claim(job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='maintenance-race'))
        self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')

    def test_candidate_inventory_bad_readiness_fails_closed(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        observations = {'cluster': None, 'node-2': None}
        store = ReferenceStore(self.queue, node_inventory=inventory,
                               node_readiness=lambda name: observations[name])
        job, _ = store.submit(self.new_submission())
        malformed = (None, {}, {'ready': True, 'taskQueueCount': False, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024},
                     {'ready': True, 'taskQueueCount': 0, 'maxImages': True},
                     {'ready': True, 'taskQueueCount': 1, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024},
                     {'ready': True, 'taskQueueCount': 0},
                     {'ready': False, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
        for report in malformed:
            with self.subTest(report=report):
                observations['cluster'] = report
                self.assertIsNone(store.next_for_node('cluster'))
                self.assertIsNone(store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                    worker_instance_id='instance', claim_request_id='bad-report'))
                self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
        observations['cluster'] = {'ready': True, 'taskQueueCount': 0, 'maxImages': 1, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
        self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])

    def test_candidate_inventory_requires_capacity_for_inputs_and_outputs(self):
        self.queue.per_node_slots = True
        inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                     dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
        reports = {'cluster': {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                    'availableScratchBytes': 1026, 'outputBudgetBytes': 1024},
                   'node-2': {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                    'availableScratchBytes': 1027, 'outputBudgetBytes': 1024}}
        store = ReferenceStore(self.queue, node_inventory=inventory, node_readiness=lambda n: reports[n])
        job, _ = store.submit(self.new_submission())
        self.assertIsNone(store.next_for_node('cluster'))
        self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
        del reports['node-2']['availableScratchBytes']
        self.assertIsNone(store.next_for_node('node-2'))
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')

    def test_metadata_quota_rolls_back_both_tables(self):
        store = ReferenceStore(self.queue, max_metadata_bytes=1)
        with self.assertRaises(Conflict):
            store.submit(self.submission)
        self.assertEqual(self.queue.list(), [])
        self.assertEqual(store.list(), [])

    def test_reservation_identity_transition_and_public_redaction(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "processing")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "awaiting_transfer")
        reserved = self.reserve(job)
        self.assertEqual(reserved["phase"], "awaiting_transfer")
        self.assertNotIn("submission", self.store.get(job["jobId"]))
        self.assertEqual(self.store.get(job["jobId"], private=True)["submission"], self.submission)
        self.assertEqual(self.reserve(job), reserved)
        with self.assertRaises(ValueError):
            self.store.update_phase(job["jobId"], "held", error="https://secret/path")
        with self.assertRaises(Conflict):
            self.store.get("unknown")

    def test_global_engine_lock_includes_legacy_treedar(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.reserve(job)
        legacy, _ = self.queue.submit({"requestId": "legacy", "environment": "staging", "profile": LEGACY_PROFILE})
        claim = self.store.get(job["jobId"], private=True)
        self.assertEqual(claim["jobId"], job["jobId"])
        self.assertIsNone(self.queue.claim({"workerId": "legacy", "profiles": [LEGACY_PROFILE]}))
        self.store.update_phase(job["jobId"], "held", error="worker_uncertain")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "cancelled")
        self.assertIsNone(self.queue.claim({"workerId": "legacy", "profiles": [LEGACY_PROFILE]}))
        self.assertEqual(self.queue.get(legacy["id"])["status"], "queued")

    def test_restart_preserves_identity_and_holds_uncertain_engine(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.reserve(job)
        self.queue.claim({"workerId": "worker", "profiles": [PROFILE]})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.assertEqual(self.store.get(job["jobId"])["phase"], "held")
        self.assertFalse(prepare_reference_job(self.store, self.submission)[1])
        self.assertIsNone(self.queue.claim({"workerId": "worker", "profiles": [PROFILE]}))

    def test_acceptance_subsets_and_changed_receipt(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        accepted = self.store.accept(job["jobId"], receipt)
        self.assertFalse(accepted["fullyAccepted"])
        self.assertFalse(self.store.accept(job["jobId"], receipt)["created"])
        changed = copy.deepcopy(receipt)
        changed["acceptedAt"] = "2026-10-28T12:00:00Z"
        with self.assertRaises(Conflict):
            self.store.accept(job["jobId"], changed)
        changed["receiptId"] = "receipt-final"
        changed['storageReceiptId'] = 'storage-final'
        changed["acceptedObjectIds"] = [obj["id"] for obj in result["objects"]]
        accepted = self.store.accept(job["jobId"], changed)
        self.assertTrue(accepted["fullyAccepted"])
        self.assertEqual(accepted["job"]["phase"], "completed")
        # Acceptance is metadata; it never releases the shared worker lock.
        self.assertEqual(accepted["job"]["status"], "running")

    def test_claim_replay_atomicity_binding_and_restart(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        kwargs = dict(node_id="cluster", worker_id="worker", worker_instance_id="instance-1", claim_request_id="claim-1")
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.store.claim(job["jobId"], **kwargs), range(8)))
        token = claims[0]["claimToken"]
        self.assertEqual({claim["claimToken"] for claim in claims}, {token})
        self.assertNotIn("claimToken", self.store.get(job["jobId"]))
        for changed in ({"worker_instance_id": "instance-2"}, {"worker_id": "other"}, {"node_id": "node-2"}):
            with self.subTest(changed=changed), self.assertRaises(Conflict):
                self.store.claim(job["jobId"], **{**kwargs, **changed})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        replay = self.store.claim(job["jobId"], **kwargs)
        self.assertEqual(replay["claimToken"], token)
        self.assertEqual((replay["status"], replay["phase"]), ("held", "held"))
        self.queue.complete(job["jobId"], {"claimToken": token, "status": "succeeded", "result": {}})
        self.assertEqual(self.store.claim(job["jobId"], **kwargs)["status"], "succeeded")

    def test_claim_transaction_rolls_back_queue_when_envelope_insert_fails(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        with self.queue._db() as db:
            db.execute("CREATE TRIGGER reject_claim BEFORE INSERT ON reference_claims BEGIN SELECT RAISE(ABORT,'test crash'); END")
        with self.assertRaises(Exception):
            self.store.claim(job["jobId"], node_id="cluster", worker_id="worker",
                             worker_instance_id="instance", claim_request_id="claim-1")
        self.assertEqual(self.queue.get(job["jobId"])["status"], "queued")
        self.assertIsNone(self.store.get(job["jobId"], private=True)["claimToken"])

    def test_unclaimed_execution_and_reservation_rejected(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        with self.assertRaises(Conflict):
            self.store.reserve(job["jobId"], self.fixture["transferRequest"], node_id="cluster")
        with self.assertRaises(Conflict):
            self.store.update_phase(job["jobId"], "fetching")

    def test_claim_respects_shared_priority_and_node_maintenance(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.queue.submit({"requestId": "priority", "environment": "production", "profile": LEGACY_PROFILE})
        kwargs = dict(node_id="cluster", worker_id="worker", worker_instance_id="instance", claim_request_id="claim-1")
        self.assertIsNone(self.store.claim(job["jobId"], **kwargs))
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='succeeded' WHERE profile=?", (LEGACY_PROFILE,))
            db.execute("UPDATE nodes SET state='held' WHERE id='cluster'")
        self.assertIsNone(self.store.claim(job["jobId"], **kwargs))
        with self.queue._db() as db:
            db.execute("UPDATE nodes SET state='active' WHERE id='cluster'")
        self.assertEqual(self.store.claim(job["jobId"], **kwargs)["jobId"], job["jobId"])

    def test_surviving_worker_terminal_result_recovers_without_engine_replay(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        self.reserve(job)
        self.store.update_phase(job["jobId"], "fetching")
        self.store.update_phase(job["jobId"], "processing")
        token = self.store.get(job["jobId"], private=True)["claimToken"]
        result = copy.deepcopy(self.fixture["resultManifest"])
        result.update(jobId=job["jobId"], pipelineDigest=job["pipelineDigest"])
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        self.queue.close()
        self.queue = Queue(self.directory.name, node_ids=("cluster", "node-2"))
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        recovered = self.store.get(job["jobId"], private=True)
        self.assertEqual((recovered["phase"], recovered["executionPhase"]), ("held", "processing"))
        with self.assertRaises(Conflict):
            self.store.set_result_manifest(job["jobId"], result)
        kwargs = dict(claim_token=token, node_id="cluster", worker_id="worker",
                      worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])
        with self.assertRaises(Conflict):
            self.store.record_worker_result(job["jobId"], result, **{**kwargs, "claim_token": "wrong"})
        self.store.record_worker_result(job["jobId"], result, **kwargs)
        self.queue.complete(job["jobId"], {"claimToken": token, "status": "succeeded", "result": {}})
        execution_done = self.store.get(job["jobId"])
        self.assertEqual((execution_done["status"], execution_done["phase"]), ("succeeded", "waiting_for_delivery"))
        self.store.update_phase(job["jobId"], "awaiting_acceptance")
        receipt = self.receipt(job, result)
        receipt["acceptedObjectIds"] = [obj["id"] for obj in result["objects"]]
        self.assertTrue(self.store.accept(job["jobId"], receipt)["fullyAccepted"])

    def test_operator_hold_blocks_worker_result_recovery(self):
        job, _ = self.result_ready()
        private = self.store.get(job["jobId"], private=True)
        self.store.update_phase(job["jobId"], "held", error="operator_uncertain")
        with self.assertRaises(Conflict):
            self.store.record_worker_result(job["jobId"], private["resultManifest"],
                claim_token=private["claimToken"], node_id="cluster", worker_id="worker",
                worker_instance_id="instance-1", claim_request_id="claim-" + job["jobId"])

    def test_stale_result_receipt_and_sealed_result(self):
        job, result = self.result_ready()
        receipt = self.receipt(job, result)
        receipt["environment"] = "production"
        with self.assertRaises(ValueError):
            self.store.accept(job["jobId"], receipt)
        result["objects"][0]["byteSize"] += 1
        result["resultManifestSha256"] = p.digest({k: v for k, v in result.items() if k != "resultManifestSha256"})
        with self.assertRaises(Conflict):
            self.store.set_result_manifest(job["jobId"], result)

    def test_outbox_durable_idempotency_backoff_and_no_secrets(self):
        job, _ = prepare_reference_job(self.store, self.submission)
        activate_reference_job(self.store, job)
        payload = {"jobId": job["jobId"], "generation": 1, "environment": "staging", "receiptId": "receipt-1"}
        self.assertTrue(self.store.enqueue_outbox("delivery_ack", "ack-1", payload))
        self.assertFalse(self.store.enqueue_outbox("delivery_ack", "ack-1", payload))
        self.assertEqual(len(self.store.pending_outbox(now=100)), 1)
        self.assertEqual(self.store.retry_outbox("ack-1", now=100), 1)
        self.assertEqual(self.store.pending_outbox(now=100), [])
        self.assertEqual(len(self.store.pending_outbox(now=101)), 1)
        self.store = ReferenceStore(self.queue)
        self.assertEqual(len(self.store.pending_outbox(now=101)), 1)
        self.store.acknowledge_outbox("ack-1")
        self.assertEqual(self.store.pending_outbox(), [])
        payload["token"] = "secret"
        with self.assertRaises(ValueError):
            self.store.enqueue_outbox("delivery_ack", "ack-2", payload)


class InventorySchedulingTests(unittest.TestCase):
    """Synthetic queues for "earliest job eligible for each available node"."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name, node_ids=('cluster', 'node-2'), treedar_node_ids=('node-2',),
                           viewer_enabled=True, reference_enabled=True, per_node_slots=True,
                           node_capabilities={'cluster': ['odm'], 'node-2': ['odm', 'treedar']},
                           node_photo_limits={'cluster': 2, 'node-2': None})
        self.inventory = [dict(id='cluster', maxPhotos=2, capabilities=['odm']),
                          dict(id='node-2', maxPhotos=None, capabilities=['odm', 'treedar'])]
        self.reports = {'cluster': self.report(), 'node-2': self.report()}
        self.store = ReferenceStore(self.queue, node_inventory=self.inventory,
                                    node_readiness=lambda node: self.reports[node])
        self.fixture = json.loads((Path(__file__).resolve().parents[1] /
                                  'contracts/reference-v1.contract.json').read_text())
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
            db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    @staticmethod
    def report(**changes):
        return {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                'availableScratchBytes': 10 ** 9, 'outputBudgetBytes': 1024, **changes}

    def reference(self, images, *, size=3, environment='staging', store=None):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        manifest = raw['manifest']
        manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}', 'name': f'image-{i}.jpg',
                                'byteSize': size} for i in range(images)]
        manifest.update(objectCount=images, totalBytes=images * size)
        manifest['inputCounts']['images'] = images
        manifest['sha256'] = p.manifest_sha256(manifest)
        return (store or self.store).submit(p.validate_submission(raw, environment=environment,
            capability_fingerprint=raw['capabilityFingerprint']))[0]

    def treedar(self):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        manifest = raw['manifest']
        manifest['objects'][0].update(name='genuine.las', kind='lidar', mediaType='application/octet-stream')
        manifest['inputCounts'].update(images=0, lidarObjects=1, points=1)
        manifest['sha256'] = p.manifest_sha256(manifest)
        raw['pipeline']['odm'].update(requested=False)
        raw['pipeline']['treedar'].update(requested=True, inputObjectIds=['image-1'],
                                          provenanceReceiptId='trusted-ingestion-receipt')
        return self.store.submit(p.validate_submission(raw, environment='staging',
            capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar')))[0]

    def viewer(self, photos, environment='staging'):
        job, _ = self.queue.submit({'requestId': 'viewer-' + uuid.uuid4().hex[:12], 'environment': environment,
                                    'profile': VIEWER_PROFILE})
        with self.queue._db() as db:
            db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', (job['id'], job['id']))
            db.executemany('INSERT INTO viewer_files VALUES(?,1)', [(job['id'],)] * photos)
        return job

    def claim(self, job, node, store=None):
        return (store or self.store).claim(job['jobId'], node_id=node, worker_id=node, worker_instance_id='instance',
                                           claim_request_id='claim-' + uuid.uuid4().hex[:12])

    def claim_viewer(self, job, node):
        return self.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': node, 'profiles': [VIEWER_PROFILE],
                                 'expectedJobId': job['id']}, allow_viewer_same_priority_skip=True)

    def status(self, job):
        return self.queue.get(job.get('jobId') or job['id'])['status']

    def occupy_main(self):
        self.assertIsNotNone(self.claim(self.reference(5), 'node-2'))

    def test_idle_node_runs_viewer_past_job_proven_too_large_for_its_scratch(self):
        self.reports['cluster'] = self.report(availableScratchBytes=5000)
        self.occupy_main()
        blocked, viewer, later = self.reference(1, size=100000), self.viewer(1), self.reference(1)
        self.assertTrue(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        self.assertEqual(self.claim_viewer(viewer, 'cluster')['nodeId'], 'cluster')
        self.assertEqual((self.status(blocked), self.status(later)), ('queued', 'queued'))

    def test_later_reference_passes_jobs_proven_ineligible_on_idle_node(self):
        self.reports['cluster'] = self.report(availableScratchBytes=5000)
        self.occupy_main()
        blocked, viewer, later = self.reference(1, size=100000), self.viewer(5), self.reference(1)
        self.assertEqual(self.store.next_for_node('cluster')['jobId'], later['jobId'])
        self.assertEqual(self.claim(later, 'cluster')['nodeId'], 'cluster')
        self.assertEqual((self.status(blocked), self.status(viewer)), ('queued', 'queued'))

    def test_uncertain_scratch_evidence_keeps_the_viewer_barrier(self):
        self.occupy_main()
        self.reference(1, size=100000)
        viewer = self.viewer(1)
        for report in (None, {}, {'ready': False, 'reason': 'readiness_expired'}, self.report(ready=False),
                       self.report(taskQueueCount=1), self.report(availableScratchBytes=None),
                       self.report(outputBudgetBytes=-1), self.report(availableScratchBytes=10 ** 9)):
            with self.subTest(report=report):
                self.reports['cluster'] = report
                self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
                self.assertIsNone(self.claim_viewer(viewer, 'cluster'))
        def broken(_):
            raise OSError('synthetic readiness failure')
        self.store.node_readiness = broken
        self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        self.assertEqual(self.status(viewer), 'queued')

    def test_tampered_reference_metadata_proves_nothing(self):
        self.reports['cluster'] = self.report(availableScratchBytes=5000)
        self.occupy_main()
        blocked, viewer = self.reference(1, size=100000), self.viewer(1)
        self.assertTrue(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        with self.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (blocked['jobId'],)).fetchone()
            submission = json.loads(row['input_json'])
            submission['manifest']['totalBytes'] += 1
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(submission), blocked['jobId']))
        self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))

    def test_treedar_job_needing_main_does_not_idle_the_cluster(self):
        self.occupy_main()
        lidar, viewer = self.treedar(), self.viewer(1)
        self.assertTrue(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        self.assertEqual(self.claim_viewer(viewer, 'cluster')['nodeId'], 'cluster')
        self.assertEqual(self.status(lidar), 'queued')

    def test_unconfigured_capability_is_not_proof(self):
        self.occupy_main()
        self.treedar()
        viewer = self.viewer(1)
        self.queue.node_capabilities = None
        self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))
        self.assertIsNone(self.claim_viewer(viewer, 'cluster'))

    def test_reference_passes_viewer_on_node_without_odm(self):
        self.occupy_main()
        self.queue.node_capabilities['cluster'] = frozenset({'treedar'})
        self.inventory[0]['capabilities'] = ['treedar']
        self.queue.treedar_node_ids = ('cluster', 'node-2')
        self.viewer(1)
        lidar = self.treedar()
        self.assertEqual(self.store.next_for_node('cluster')['jobId'], lidar['jobId'])

    def test_reported_cap_alone_is_not_proof_in_either_direction(self):
        self.reports.update({'cluster': self.report(ready=False), 'node-2': self.report(maxImages=1)})
        viewer, later = self.viewer(5), self.reference(1)
        self.assertIsNone(self.claim(later, 'node-2'))
        self.assertEqual((self.status(viewer), self.status(later)), ('queued', 'queued'))
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='failed' WHERE status='queued'")
        self.reports.update({'cluster': self.report(maxImages=1), 'node-2': self.report()})
        self.occupy_main()
        self.reference(2)
        viewer = self.viewer(1)
        self.assertFalse(self.queue.viewer_skip_is_safe(viewer['id'], 'cluster'))

    def polled(self, clock):
        cache = NodeReadiness(('cluster', 'node-2'), ttl_seconds=30, clock=clock)
        for node in cache.node_ids:
            cache.publish(node, self.report(), ticket=cache.begin(node))
        return cache, ReferenceStore(self.queue, node_inventory=self.inventory, node_readiness=cache.status)

    def test_routine_probe_of_idle_cluster_does_not_overflow_to_main(self):
        cache, store = self.polled(lambda: 100.0)
        job = self.reference(1, store=store)
        self.assertIsNone(store.next_for_node('node-2'))
        ticket = cache.begin('cluster')
        self.assertIsNone(store.next_for_node('node-2'))
        self.assertIsNone(self.claim(job, 'node-2', store))
        self.assertIsNone(self.claim(job, 'cluster', store))  # Claims still need a current observation.
        self.assertEqual(self.status(job), 'queued')
        cache.publish('cluster', self.report(), ticket=ticket)
        self.assertEqual(self.claim(job, 'cluster', store)['nodeId'], 'cluster')

    def test_confirmed_busy_cluster_may_overflow_to_main(self):
        cache, store = self.polled(lambda: 100.0)
        running = self.reference(1, store=store)
        self.assertIsNotNone(self.claim(running, 'cluster', store))
        occupied = self.reference(1, store=store)
        self.assertEqual(store.next_for_node('node-2')['jobId'], occupied['jobId'])
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='failed' WHERE id=?", (running['jobId'],))
        for observed in (self.report(taskQueueCount=1), self.report(ready=False)):
            with self.subTest(observed=observed):
                cache.publish('cluster', observed, ticket=cache.begin('cluster'))
                self.assertEqual(store.next_for_node('node-2')['jobId'], occupied['jobId'])

    def test_unknown_node_stops_holding_later_nodes_after_its_ttl(self):
        now = [100.0]
        cache, store = self.polled(lambda: now[0])
        job = self.reference(1, store=store)
        now[0] = 105
        cache.begin('cluster')  # This probe, and every later one, never answers.
        now[0] = 128
        cache.publish('node-2', self.report(), ticket=cache.begin('node-2'))
        now[0] = 129.9
        self.assertIsNone(store.next_for_node('node-2'))
        now[0] = 130
        cache.begin('cluster')
        self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
        cache.publish('cluster', self.report(), ticket=cache.begin('cluster'))
        self.assertIsNone(store.next_for_node('node-2'))
        self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])

    def test_startup_without_observation_is_bounded(self):
        now = [100.0]
        cache = NodeReadiness(('cluster', 'node-2'), ttl_seconds=30, clock=lambda: now[0])
        store = ReferenceStore(self.queue, node_inventory=self.inventory, node_readiness=cache.status)
        job = self.reference(1, store=store)
        now[0] = 129
        cache.publish('node-2', self.report(), ticket=cache.begin('node-2'))
        self.assertIsNone(store.next_for_node('node-2'))
        now[0] = 130
        self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])

    def test_unknown_node_holds_only_jobs_it_might_run(self):
        self.reports['cluster'] = None
        large = self.reference(5)
        self.assertEqual(self.store.next_for_node('node-2')['jobId'], large['jobId'])
        small = self.reference(1)
        self.assertEqual(self.claim(large, 'node-2')['nodeId'], 'node-2')
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='failed' WHERE id=?", (large['jobId'],))
        # The held small job also holds later order: a long job taking main now
        # would strand it behind that job if the cluster proves to be down.
        later = self.reference(5)
        self.assertIsNone(self.store.next_for_node('node-2'))
        self.assertEqual((self.status(small), self.status(later)), ('queued', 'queued'))
        # A job main cannot run gives main nothing to hold for.
        self.reports['node-2'] = self.report(availableScratchBytes=2000)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='failed' WHERE id=?", (small['jobId'],))
        self.reference(1, size=5000)
        self.assertEqual(self.store.next_for_node('node-2')['jobId'], later['jobId'])

    def test_unknown_node_does_not_let_staging_pass_production(self):
        self.reports['cluster'] = None
        production = self.reference(1, environment='production')
        staging = self.reference(5)
        self.assertIsNone(self.store.next_for_node('node-2'))
        self.assertIsNone(self.claim(staging, 'node-2'))
        self.assertEqual((self.status(production), self.status(staging)), ('queued', 'queued'))

    def job_of(self, kind, photos, environment, size=3):
        return (self.reference(photos, environment=environment, size=size) if kind == 'reference'
                else self.viewer(photos, environment))

    def claim_any(self, job, node):
        return self.claim(job, node) if 'jobId' in job else self.claim_viewer(job, node)

    def clear_jobs(self):
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='failed' WHERE status IN ('queued','running','held')")

    def each_pair(self, check, photos, *, production_kinds=('reference', 'viewer'), before=None):
        """Run check(production, staging) for each job kind in both orderings."""
        for production_kind in production_kinds:
            for staging_kind in ('reference', 'viewer'):
                for production_first in (True, False):
                    with self.subTest(production=production_kind, staging=staging_kind, first=production_first):
                        if before:
                            before()
                        if production_first:
                            production = self.job_of(production_kind, photos, 'production')
                            staging = self.job_of(staging_kind, 1, 'staging')
                        else:
                            staging = self.job_of(staging_kind, 1, 'staging')
                            production = self.job_of(production_kind, photos, 'production')
                        check(production, staging)
                    self.clear_jobs()

    def staging_stays_queued(self, staging, node='cluster'):
        if 'jobId' in staging:
            self.assertIsNone(self.store.next_for_node(node))
        self.assertIsNone(self.claim_any(staging, node))
        self.assertEqual(self.status(staging), 'queued')

    def test_staging_takes_idle_cluster_while_oversized_production_waits_for_main(self):
        def check(production, staging):
            self.assertTrue(self.queue.staging_may_claim('cluster'))
            if 'jobId' in staging:
                self.assertEqual(self.store.next_for_node('cluster')['jobId'], staging['jobId'])
            self.assertEqual(self.claim_any(staging, 'cluster')['nodeId'], 'cluster')
            self.assertEqual(self.status(production), 'queued')
            with self.queue._db() as db:
                db.execute("UPDATE jobs SET status='failed' WHERE node_id='node-2'")
            self.assertEqual(self.claim_any(production, 'node-2')['nodeId'], 'node-2')
            self.assertEqual(self.status(staging), 'running')
        self.each_pair(check, 5, before=self.occupy_main)

    def test_unknown_viewer_evidence_keeps_staging_off_the_idle_node(self):
        def no_files(db, job):
            db.execute('DELETE FROM viewer_files WHERE uuid=?', (job['id'],))
        def two_tasks(db, job):
            db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('second-' + job['id'], job['id']))
        def ambiguous_file(db, job):
            db.execute('INSERT INTO viewer_files VALUES(?,NULL)', (job['id'],))
        def no_task(db, job):
            db.execute('UPDATE viewer_tasks SET removed=1 WHERE job_id=?', (job['id'],))
        for damage in (no_files, two_tasks, ambiguous_file, no_task):
            for kind in ('reference', 'viewer'):
                with self.subTest(damage=damage.__name__, staging=kind):
                    self.occupy_main()
                    production, staging = self.viewer(5, 'production'), self.job_of(kind, 1, 'staging')
                    self.assertTrue(self.queue.staging_may_claim('cluster'))
                    with self.queue._db() as db:
                        damage(db, production)
                    self.assertFalse(self.queue.staging_may_claim('cluster'))
                    self.staging_stays_queued(staging)
                self.clear_jobs()

    def test_unknown_reference_evidence_keeps_staging_off_the_idle_node(self):
        for kind in ('reference', 'viewer'):
            with self.subTest(staging=kind):
                self.occupy_main()
                production, staging = self.reference(5, environment='production'), self.job_of(kind, 1, 'staging')
                with self.queue._db() as db:
                    row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',
                                     (production['jobId'],)).fetchone()
                    submission = json.loads(row['input_json'])
                    submission['manifest']['totalBytes'] += 1
                    db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',
                               (json.dumps(submission), production['jobId']))
                self.staging_stays_queued(staging)
            self.clear_jobs()

    def test_scratch_proof_needs_a_current_observation_to_pass_staging(self):
        for kind in ('reference', 'viewer'):
            with self.subTest(staging=kind):
                self.occupy_main()
                self.reports['cluster'] = self.report(availableScratchBytes=5000)
                self.reference(1, environment='production', size=100000)
                staging = self.job_of(kind, 1, 'staging')
                self.assertTrue(self.queue.staging_may_claim('cluster'))
                self.reports['cluster'] = None
                self.assertFalse(self.queue.staging_may_claim('cluster'))
                self.staging_stays_queued(staging)
                self.reports['cluster'] = self.report(availableScratchBytes=5000)
                self.assertEqual(self.claim_any(staging, 'cluster')['nodeId'], 'cluster')
            self.reports['cluster'] = self.report()
            self.clear_jobs()

    def test_reported_cap_alone_does_not_pass_staging(self):
        self.occupy_main()
        self.reports['cluster'] = self.report(maxImages=1)
        self.reference(2, environment='production')
        staging = self.reference(1)
        self.staging_stays_queued(staging)

    def test_other_queued_profile_proves_nothing_for_staging(self):
        self.occupy_main()
        self.queue.submit({'requestId': 'synthetic-production', 'environment': 'production',
                           'profile': LEGACY_PROFILE})
        self.staging_stays_queued(self.reference(1))

    def test_production_that_fits_the_node_takes_it_over_staging(self):
        def check(production, staging):
            if 'jobId' in production:
                self.assertEqual(self.store.next_for_node('cluster')['jobId'], production['jobId'])
            self.assertIsNone(self.claim_any(staging, 'cluster'))
            self.assertEqual(self.claim_any(production, 'cluster')['nodeId'], 'cluster')
            self.assertEqual(self.status(staging), 'queued')
        self.each_pair(check, 1)

    def test_one_fitting_production_job_blocks_staging_past_an_oversized_one(self):
        self.occupy_main()
        self.reference(5, environment='production')
        fits = self.viewer(1, 'production')
        staging = self.reference(1)
        self.staging_stays_queued(staging)
        self.assertEqual(self.claim_viewer(fits, 'cluster')['nodeId'], 'cluster')

    def test_cluster_waits_while_production_lands_on_idle_main(self):
        def check(production, staging):
            self.assertFalse(self.queue.staging_may_claim('cluster'))
            self.staging_stays_queued(staging)
            self.assertEqual(self.claim(production, 'node-2')['nodeId'], 'node-2')
            self.assertTrue(self.queue.staging_may_claim('cluster'))  # landed: it keeps only its own node
        self.each_pair(check, 5, production_kinds=('reference',))

    def test_running_production_leaves_other_idle_nodes_to_staging(self):
        for kind in ('reference', 'viewer'):
            with self.subTest(staging=kind):
                running = self.reference(5, environment='production')
                self.assertIsNotNone(self.claim(running, 'node-2'))
                self.reference(5, environment='production')  # oversized for cluster, waits for main
                self.job_of(kind, 1, 'staging')
                self.assertTrue(self.queue.staging_may_claim('cluster'))
                self.assertFalse(self.queue.staging_may_claim('node-2'))
            self.clear_jobs()

    def test_running_production_still_yields_to_queued_production_that_fits(self):
        running = self.reference(5, environment='production')
        self.assertIsNotNone(self.claim(running, 'node-2'))
        fitting = self.reference(1, environment='production')  # fits cluster, so it keeps priority
        staging = self.job_of('reference', 1, 'staging')
        self.assertFalse(self.queue.staging_may_claim('cluster'))
        self.assertEqual(self.claim(fitting, 'cluster')['jobId'], fitting['jobId'])
        self.assertEqual(self.status(staging), 'queued')

    def test_held_production_still_blocks_staging_on_idle_nodes(self):
        for kind in ('reference', 'viewer'):
            with self.subTest(staging=kind):
                held = self.reference(5, environment='production')
                self.assertIsNotNone(self.claim(held, 'node-2'))
                self.reference(5, environment='production')
                staging = self.job_of(kind, 1, 'staging')
                self.queue.operator(held['jobId'], 'hold', {'note': 'inspect'})
                self.assertFalse(self.queue.staging_may_claim('cluster'))
                self.staging_stays_queued(staging)
            self.clear_jobs()

    def test_racing_staging_past_production_keeps_one_job_per_node(self):
        for round_number in range(12):
            if round_number % 2:
                self.occupy_main()
            jobs = [self.reference(5, environment='production'), self.viewer(5, 'production'),
                    self.reference(1), self.viewer(1), self.reference(1)]
            late = self.reference(1, environment='production') if round_number % 3 == 0 else None
            attempts = [(job, 'cluster') for job in jobs[2:] + ([late] if late else [])]
            attempts += [(jobs[0], 'node-2')] * 2 + [(jobs[1], 'node-2')] * 2
            barrier = threading.Barrier(len(attempts))
            def attempt(item):
                barrier.wait(timeout=10)
                return self.claim_any(*item)
            with ThreadPoolExecutor(max_workers=len(attempts)) as pool:
                list(pool.map(attempt, attempts))
            running = [job for job in self.queue.list() if job['status'] == 'running']
            nodes = [job['nodeId'] for job in running]
            self.assertEqual(len(nodes), len(set(nodes)))
            if late:
                self.assertNotIn('staging', [job['environment'] for job in running if job['nodeId'] == 'cluster'])
            self.clear_jobs()

    def test_racing_mixed_claims_keep_one_job_per_node(self):
        for _ in range(12):
            jobs = [self.viewer(1), self.reference(1), self.viewer(5), self.reference(5)]
            attempts = [(job, node) for job in jobs for node in ('cluster', 'node-2')]
            barrier = threading.Barrier(len(attempts))
            def attempt(item):
                job, node = item
                barrier.wait(timeout=10)
                return self.claim_viewer(job, node) if 'id' in job and 'jobId' not in job else self.claim(job, node)
            with ThreadPoolExecutor(max_workers=len(attempts)) as pool:
                list(pool.map(attempt, attempts))
            running = [job['nodeId'] for job in self.queue.list() if job['status'] == 'running']
            self.assertTrue(running)
            self.assertEqual(len(running), len(set(running)))
            with self.queue._db() as db:
                db.execute("UPDATE jobs SET status='failed' WHERE status IN ('queued','running')")


if __name__ == "__main__":
    unittest.main()
