"""Durable retry/lease integration for the Viewer acceptance outbox."""
import copy
import hashlib
import json
import unittest
import uuid
from unittest.mock import patch

from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore, ViewerTransferJobConflict
from processing import reference_protocol as protocol
from processing.reference_viewer_transfer_worker import ReferenceViewerTransferWorker
from processing.viewer_acceptance_transfer import ViewerTransferUnavailable
from tests.test_reference_api import ReferenceAPITests
from tests.viewer_receipt_signer import SyntheticViewerSigner


class FakeSender:
    def __init__(self, signer): self.signer, self.calls, self.fail = signer, [], False
    def transfer(self, job, acceptance, *, viewer_result_manifest=None):
        self.calls.append((job['jobId'], acceptance['receiptId'], acceptance['acceptedAt']))
        if self.fail:
            self.fail = False
            raise ViewerTransferUnavailable('viewer_transport_unavailable')
        return self.signer.envelope(acceptance, viewer_result_manifest or job['resultManifest'])


class ReferenceViewerTransferWorkerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = ReferenceAPITests()
        self.fixture.setUp()
        self.signer = SyntheticViewerSigner()
        self.fixture.viewer_signer = self.signer
        self.fixture.store = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        self.fixture.api = self.fixture.make_api()
        self.job, self.result, _ = self.fixture.ready()
        self.clock_value = 100.0
        self.sender = FakeSender(self.signer)
        self.sender.fail = True
        self.store = self.fixture.store
        self.worker = ReferenceViewerTransferWorker(self.store, self.sender,
            trusted_receipt_keys=self.signer.trusted_keys, clock=lambda: self.clock_value)

    def tearDown(self): self.fixture.tearDown()

    def test_unrepresentable_result_does_not_strand_next_transfer(self):
        real_ensure = self.store.ensure_viewer_transfer
        invalid = 'unrepresentable-job'
        self.sender.fail = False
        def ensure(job_id):
            if job_id == invalid:
                raise ViewerTransferJobConflict('result cannot be represented')
            return real_ensure(job_id)
        with patch.object(self.store, 'pending_viewer_transfer_page',
                          return_value=[('a', invalid), ('b', self.job['jobId'])]), \
             patch.object(self.store, 'ensure_viewer_transfer', side_effect=ensure):
            result = self.worker.run_once()
        self.assertEqual(result, {'state': 'accepted', 'jobId': self.job['jobId']})
        self.assertTrue(self.store.viewer_transfer_status(self.job['jobId'])['completed'])

    def test_storage_failure_still_holds_transfer_worker(self):
        with patch.object(self.store, 'ensure_viewer_transfer', side_effect=OSError('disk unavailable')):
            self.assertEqual(self.worker.run_once(),
                             {'state': 'held', 'reason': 'viewer_transfer_state_unavailable'})
        self.assertEqual(self.sender.calls, [])

    def test_global_metadata_quota_is_not_treated_as_a_job_conflict(self):
        with patch.object(self.store, '_quota', side_effect=Conflict('reference metadata quota reached')):
            self.assertEqual(self.worker.run_once(),
                             {'state': 'held', 'reason': 'viewer_transfer_state_unavailable'})
        self.assertIsNone(self.worker._discovery_after)
        self.assertEqual(self.sender.calls, [])

    def test_exact_full_conflicted_page_then_empty_page_stays_held(self):
        first = [('2000', 'bad-%02d' % i) for i in range(20)]
        with patch.object(self.store, 'pending_viewer_transfer_page', side_effect=[first, [], []]), \
             patch.object(self.store, 'ensure_viewer_transfer',
                          side_effect=ViewerTransferJobConflict('result invalid')):
            held = {'state': 'held', 'reason': 'viewer_transfer_jobs_need_reconciliation'}
            self.assertEqual(self.worker.run_once(), held)
            self.assertEqual(self.worker.run_once(), held)
            self.assertEqual(self.worker.run_once(), {'state': 'idle'})

    def _full_acceptance(self):
        receipt = copy.deepcopy(self.fixture.fixture['acceptance'])
        receipt.update(jobId=self.job['jobId'], pipelineDigest=self.job['pipelineDigest'],
                       resultManifestSha256=self.result['resultManifestSha256'],
                       acceptedObjectIds=sorted(o['id'] for o in self.result['objects']))
        return receipt

    def test_viewer_targeted_api_rejects_unsigned_acceptance_and_allows_signed(self):
        targeted = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        # Reopen without the Viewer environment: durable job policy must win.
        reopened = ReferenceStore(self.fixture.queue)
        api = self.fixture.make_api(store=reopened)
        route = '/jobs/' + self.job['jobId'] + '/acceptance'
        receipt = self._full_acceptance()
        self.assertEqual(self.fixture.request('POST', route, receipt, api=api).status, 409)
        self.assertEqual(targeted.get(self.job['jobId'])['phase'], 'awaiting_acceptance')
        self.assertEqual(targeted.acceptance_receipts(self.job['jobId'])['receipts'], [])
        signed = self.fixture.viewer_signer.receipt(receipt, self.result)
        response = self.fixture.request('POST', route, {'acceptance': receipt, 'viewerReceipt': signed}, api=api)
        self.assertEqual(response.status, 200)
        self.assertTrue(response.body['fullyAccepted'])

    def test_ambiguous_legacy_unsigned_completion_stays_held(self):
        receipt = self._full_acceptance()
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_jobs SET viewer_receipt_required=0 WHERE job_id=?', (self.job['jobId'],))
        self.assertEqual(self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance', receipt).status, 200)
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'completed')
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_jobs SET viewer_receipt_required=NULL WHERE job_id=?', (self.job['jobId'],))
        targeted = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        self.sender.fail = False
        worker = ReferenceViewerTransferWorker(targeted, self.sender,
            trusted_receipt_keys=self.signer.trusted_keys, clock=lambda: self.clock_value)
        self.assertEqual(worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})
        self.assertIsNone(targeted.viewer_transfer_status(self.job['jobId']))
        with self.assertRaises(Conflict):
            targeted.accept(self.job['jobId'], receipt)
        self.assertEqual(targeted.get(self.job['jobId'])['phase'], 'completed')

    def test_prior_unsigned_partial_does_not_complete_signed_partial(self):
        first = self._full_acceptance()
        first['acceptedObjectIds'] = ['result-1']
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_jobs SET viewer_receipt_required=0 WHERE job_id=?', (self.job['jobId'],))
        self.assertEqual(self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance', first).status, 200)
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_jobs SET viewer_receipt_required=1 WHERE job_id=?', (self.job['jobId'],))
        targeted = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        api = self.fixture.make_api(store=targeted)
        second = self._full_acceptance()
        second.update(receiptId=str(uuid.uuid4()), storageReceiptId=str(uuid.uuid4()), acceptedObjectIds=['result-2'])
        signed = self.fixture.viewer_signer.receipt(second, self.result)
        response = self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance',
                                        {'acceptance': second, 'viewerReceipt': signed}, api=api)
        self.assertEqual(response.status, 200)
        self.assertFalse(response.body['fullyAccepted'])
        self.assertEqual(response.body['acceptedObjectIds'], ['result-2'])
        self.assertEqual(targeted.get(self.job['jobId'])['phase'], 'awaiting_acceptance')

    def test_signed_subset_cannot_complete_full_durable_transfer(self):
        targeted = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        full = targeted.ensure_viewer_transfer(self.job['jobId'])
        subset = dict(full, acceptedObjectIds=['result-1'])
        signed = self.fixture.viewer_signer.receipt(subset, self.result)
        api = self.fixture.make_api(store=targeted)
        response = self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance',
                                        {'acceptance': subset, 'viewerReceipt': signed}, api=api)
        self.assertEqual(response.status, 409)
        self.assertFalse(targeted.viewer_transfer_status(self.job['jobId'])['completed'])
        self.assertEqual(targeted.acceptance_receipts(self.job['jobId'])['receipts'], [])

    def test_completed_transfer_without_signed_receipt_requires_reconciliation(self):
        self.store.ensure_viewer_transfer(self.job['jobId'])
        with self.fixture.queue._db() as db:
            db.execute("UPDATE reference_viewer_transfers SET completed_at='2026-01-01' WHERE job_id=?", (self.job['jobId'],))
        self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})
        self.assertEqual(self.store.acceptance_receipts(self.job['jobId'])['receipts'], [])
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')

    def test_legacy_completed_transfer_with_unrelated_signed_partial_is_held(self):
        partial = self._full_acceptance()
        partial['acceptedObjectIds'] = ['result-1']
        signed = self.fixture.viewer_signer.receipt(partial, self.result)
        route = '/jobs/' + self.job['jobId'] + '/acceptance'
        response = self.fixture.request('POST', route, {'acceptance': partial, 'viewerReceipt': signed})
        self.assertEqual(response.status, 200)
        self.assertFalse(response.body['fullyAccepted'])
        self.store.ensure_viewer_transfer(self.job['jobId'])
        with self.fixture.queue._db() as db:
            db.execute("UPDATE reference_jobs SET phase='completed' WHERE job_id=?", (self.job['jobId'],))
            db.execute("UPDATE reference_viewer_transfers SET completed_at='2026-01-01' WHERE job_id=?", (self.job['jobId'],))
        self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})
        self.assertEqual(len(self.store.acceptance_receipts(self.job['jobId'])['receipts']), 1)
        self.assertIsNone(self.store.viewer_transfer_status(self.job['jobId'])['lastError'])

    def test_completed_job_with_signed_partial_but_no_transfer_gets_full_transfer(self):
        partial = self._full_acceptance()
        partial['acceptedObjectIds'] = ['result-1']
        signed = self.fixture.viewer_signer.receipt(partial, self.result)
        response = self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance',
                                        {'acceptance': partial, 'viewerReceipt': signed})
        self.assertFalse(response.body['fullyAccepted'])
        with self.fixture.queue._db() as db:
            db.execute("UPDATE reference_jobs SET phase='completed' WHERE job_id=?", (self.job['jobId'],))
        self.sender.fail = False
        self.assertEqual(self.worker.run_once(), {'state': 'accepted', 'jobId': self.job['jobId']})
        self.assertTrue(self.store.viewer_transfer_status(self.job['jobId'])['completed'])
        self.assertFalse(self.store.has_viewer_acceptance_reconciliation(trusted_keys=self.signer.trusted_keys))

    def test_missing_matching_signed_row_invalidates_full_coverage_marker(self):
        self.sender.fail = False
        self.assertEqual(self.worker.run_once()['state'], 'accepted')
        with self.fixture.queue._db() as db:
            db.execute('DELETE FROM reference_viewer_receipts WHERE job_id=?', (self.job['jobId'],))
        self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})

    def test_completed_evidence_is_reverified_before_idle(self):
        self.sender.fail = False
        self.assertEqual(self.worker.run_once()['state'], 'accepted')
        self.assertEqual(self.worker.run_once(), {'state': 'idle'})
        with self.fixture.queue._db() as db:
            original = db.execute('SELECT receipt_json,digest FROM reference_viewer_receipts WHERE job_id=?',
                                  (self.job['jobId'],)).fetchone()
            original_json, original_digest = original['receipt_json'], original['digest']
        altered_signature = json.loads(original_json)
        signature = altered_signature['signature']
        altered_signature['signature'] = ('A' if signature[0] != 'A' else 'B') + signature[1:]
        altered_binding = json.loads(original_json)
        altered_binding['jobId'] = str(uuid.uuid4())
        variants = [('invalid_json', '{}', original_digest),
                    ('digest', original_json, '0' * 64),
                    ('signature', json.dumps(altered_signature), protocol.digest(altered_signature)),
                    ('binding', json.dumps(altered_binding), protocol.digest(altered_binding))]
        for name, body, digest in variants:
            with self.subTest(name=name):
                with self.fixture.queue._db() as db:
                    db.execute('UPDATE reference_viewer_receipts SET receipt_json=?,digest=? WHERE job_id=?',
                               (body, digest, self.job['jobId']))
                self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_viewer_receipts SET receipt_json=?,digest=? WHERE job_id=?',
                       (original_json, original_digest, self.job['jobId']))
            db.execute('UPDATE reference_acceptances SET digest=? WHERE job_id=?', ('0' * 64, self.job['jobId']))
        self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_policy_requires_reconciliation'})

    def test_corrupt_prior_signed_partial_cannot_complete_on_later_receipt(self):
        first = self._full_acceptance()
        first['acceptedObjectIds'] = ['result-1']
        route = '/jobs/' + self.job['jobId'] + '/acceptance'
        first_signed = self.fixture.viewer_signer.receipt(first, self.result)
        response = self.fixture.request('POST', route, {'acceptance': first, 'viewerReceipt': first_signed})
        self.assertEqual(response.status, 200)
        self.assertFalse(response.body['fullyAccepted'])
        with self.fixture.queue._db() as db:
            db.execute("UPDATE reference_viewer_receipts SET receipt_json='{}' WHERE job_id=?", (self.job['jobId'],))
        second = self._full_acceptance()
        second.update(receiptId=str(uuid.uuid4()), storageReceiptId=str(uuid.uuid4()), acceptedObjectIds=['result-2'])
        second_signed = self.fixture.viewer_signer.receipt(second, self.result)
        response = self.fixture.request('POST', route, {'acceptance': second, 'viewerReceipt': second_signed})
        self.assertEqual(response.status, 409)
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances WHERE job_id=?', (self.job['jobId'],)).fetchone()[0], 1)

    def test_multi_page_audit_never_reports_idle_before_last_page(self):
        self.sender.fail = False
        self.assertEqual(self.worker.run_once()['state'], 'accepted')
        with patch.object(self.store, 'audit_viewer_acceptance_page',
                          side_effect=[{'state': 'more', 'after': 'cursor-1'}, {'state': 'clean'}]) as audit:
            self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_acceptance_audit_in_progress'})
            self.assertEqual(self.worker.run_once(), {'state': 'idle'})
            self.assertIsNone(audit.call_args_list[0].kwargs['after_job'])
            self.assertEqual(audit.call_args_list[1].kwargs['after_job'], 'cursor-1')

    def test_non_viewer_policy_is_not_reinterpreted_after_config_enable(self):
        with self.fixture.queue._db() as db:
            db.execute('UPDATE reference_jobs SET viewer_receipt_required=0 WHERE job_id=?', (self.job['jobId'],))
        configured_later = ReferenceStore(self.fixture.queue, viewer_required_environments=('staging',))
        api = self.fixture.make_api(store=configured_later)
        response = self.fixture.request('POST', '/jobs/' + self.job['jobId'] + '/acceptance',
                                        self._full_acceptance(), api=api)
        self.assertEqual(response.status, 200)
        self.assertTrue(response.body['fullyAccepted'])
        self.assertIsNone(configured_later.viewer_transfer_status(self.job['jobId']))
        self.assertFalse(configured_later.has_viewer_acceptance_reconciliation())

    def test_full_page_of_conflicts_does_not_starve_later_job(self):
        self.sender.fail = False
        with self.fixture.queue._db() as db:
            for i in range(25):
                bad = 'bad-%02d' % i
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,?)",
                           (bad, bad, 'staging', 'reference-pipeline-v1', 'succeeded'))
                db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,
                    environment,input_json,manifest_sha,pipeline_digest,generation,phase,result_json,created_at,viewer_receipt_required)
                    SELECT ?,?,request_digest,environment,input_json,manifest_sha,pipeline_digest,
                    generation,'awaiting_acceptance',?,'2000-01-01 00:00:00',1
                    FROM reference_jobs WHERE job_id=?""", (bad, bad, 'not-json' if i == 0 else '{}', self.job['jobId']))
        self.assertEqual(self.worker.run_once(), {'state': 'held', 'reason': 'viewer_transfer_jobs_need_reconciliation'})
        self.assertEqual(self.worker.run_once(), {'state': 'accepted', 'jobId': self.job['jobId']})
        self.assertTrue(self.store.viewer_transfer_status(self.job['jobId'])['completed'])
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_jobs WHERE job_id LIKE 'bad-%' AND phase='awaiting_acceptance'").fetchone()[0], 25)

    def test_stable_outbox_retries_and_signed_acceptance_completes_job(self):
        first = self.worker.run_once()
        self.assertEqual(first['state'], 'retry_wait')
        original = self.sender.calls[0]
        status = self.store.viewer_transfer_status(self.job['jobId'])
        self.assertEqual((status['attempts'], status['completed'], status['lastError']),
                         (1, False, 'viewer_transport_unavailable'))
        self.clock_value += 1
        self.assertEqual(self.worker.run_once()['state'], 'idle')
        self.clock_value += 1
        restarted_store = ReferenceStore(self.fixture.queue)
        restarted = ReferenceViewerTransferWorker(restarted_store, self.sender,
            trusted_receipt_keys=self.signer.trusted_keys, clock=lambda: self.clock_value)
        result = restarted.run_once()
        self.assertEqual(result, {'state': 'accepted', 'jobId': self.job['jobId']})
        self.assertEqual(self.sender.calls[1], original)
        self.assertEqual(restarted_store.get(self.job['jobId'])['phase'], 'completed')
        self.assertTrue(restarted_store.viewer_transfer_status(self.job['jobId'])['completed'])
        receipt = restarted_store.acceptance_receipts(self.job['jobId'])['receipts']
        self.assertEqual(len(receipt), 1)
        self.assertIn('viewerReceipt', receipt[0])

    def test_expired_outbox_lease_is_recovered_with_same_acceptance(self):
        acceptance = self.store.ensure_viewer_transfer(self.job['jobId'],
            accepted_at='2026-11-02T12:00:00.000Z')
        lease = self.store.claim_viewer_transfer(now=100, lease_seconds=10)
        self.assertEqual(lease['acceptance'], acceptance)
        self.assertEqual(lease['viewerResultManifest'], self.result)
        self.assertIsNone(self.store.claim_viewer_transfer(now=109, lease_seconds=10))
        recovered = self.store.claim_viewer_transfer(now=110, lease_seconds=10)
        self.assertEqual(recovered['acceptance'], acceptance)
        self.assertEqual(recovered['viewerResultManifest'], lease['viewerResultManifest'])
        self.assertNotEqual(recovered['leaseToken'], lease['leaseToken'])

    def test_combined_job_persists_v2_manifest_and_completes_exact_viewer_closure(self):
        from tempfile import TemporaryDirectory
        from processing.prototype import Queue
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        queue = Queue(temp.name, node_ids=('cluster', 'main'), treedar_node_ids=('main',), reference_enabled=True)
        self.addCleanup(queue.close)
        store = ReferenceStore(queue, main_node_id='main', viewer_required_environments=('staging',))
        request = copy.deepcopy(self.fixture.fixture['submission'])
        request['submissionId'] = str(uuid.uuid4())
        manifest = request['manifest']
        lidar = {"id": "lidar-1", "name": "Cloud.las", "byteSize": 4,
            "sha256": hashlib.sha256(b"LAS!").hexdigest(), "kind": "lidar",
            "mediaType": "application/octet-stream"}
        manifest['objects'].append(lidar)
        manifest['totalBytes'] += lidar['byteSize']
        manifest['objectCount'] += 1
        manifest['inputCounts']['lidarObjects'] = 1
        manifest['sha256'] = __import__('processing.reference_protocol', fromlist=['manifest_sha256']).manifest_sha256(manifest)
        request['pipeline']['treedar'].update(requested=True, inputObjectIds=['lidar-1'],
            provenanceReceiptId='provenance-test')
        api = self.fixture.make_api(store=store, treedar_available=True)
        created = self.fixture.request('POST', '/jobs', request, api=api)
        self.assertEqual(created.status, 201)
        job = created.body
        binding = dict(node_id='main', worker_id='worker', worker_instance_id='instance',
            claim_request_id='claim-combined')
        claim = store.claim(job['jobId'], **binding)
        transfer_request = copy.deepcopy(self.fixture.fixture['transferRequest'])
        transfer_request.update(manifestSha256=manifest['sha256'], byteBudget=manifest['totalBytes'])
        store.reserve(job['jobId'], transfer_request, node_id='main')
        store.update_phase(job['jobId'], 'fetching')
        store.update_phase(job['jobId'], 'processing')
        roles = ['ground_model', 'canopy_height', 'canopy_height_unfiltered', 'crown_labels',
            'crowns', 'tree_table', 'tree_features', 'run_summary', 'report', 'preview',
            'area_map_preview', 'engine_manifest']
        payloads = {f'treedar-{role}': f'synthetic {role}'.encode() for role in roles}
        payloads['odm-archive'] = b'synthetic odm archive'
        result = dict(schemaVersion=1, jobId=job['jobId'], generation=job['generation'],
            environment=job['environment'], pipelineDigest=job['pipelineDigest'],
            engineVersions={'odm': 'odm-test', 'treedar': 'treedar-test'},
            objects=[*[
                dict(id=f'treedar-{role}', role=role, byteSize=len(payloads[f'treedar-{role}']),
                    sha256=hashlib.sha256(payloads[f'treedar-{role}']).hexdigest()) for role in roles],
                dict(id='odm-archive', role='odm_archive', byteSize=len(payloads['odm-archive']),
                    sha256=hashlib.sha256(payloads['odm-archive']).hexdigest())])
        result['resultManifestSha256'] = __import__('processing.reference_protocol', fromlist=['digest']).digest(result)
        store.record_worker_result(job['jobId'], result, claim_token=claim['claimToken'], **binding)
        store.queue.complete(job['jobId'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {}})
        store.update_phase(job['jobId'], 'awaiting_acceptance')
        sender = FakeSender(self.signer)
        worker = ReferenceViewerTransferWorker(store, sender,
            trusted_receipt_keys=self.signer.trusted_keys, clock=lambda: self.clock_value)
        outcome = worker.run_once()
        self.assertEqual(outcome, {'state': 'accepted', 'jobId': job['jobId']})
        self.assertEqual(store.get(job['jobId'])['phase'], 'completed')
        self.assertTrue(store.viewer_transfer_status(job['jobId'])['completed'])
        receipts = store.acceptance_receipts(job['jobId'])['receipts']
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]['acceptance']['acceptedObjectIds'],
            [obj['id'] for obj in result['objects']])


if __name__ == '__main__': unittest.main()
