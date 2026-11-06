"""Durable retry/lease integration for the Viewer acceptance outbox."""
import copy
import hashlib
import json
import unittest
import uuid

from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore
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
        self.job, self.result, _ = self.fixture.ready()
        self.signer = SyntheticViewerSigner()
        self.clock_value = 100.0
        self.sender = FakeSender(self.signer)
        self.sender.fail = True
        self.store = self.fixture.store
        self.worker = ReferenceViewerTransferWorker(self.store, self.sender,
            trusted_receipt_keys=self.signer.trusted_keys, clock=lambda: self.clock_value)

    def tearDown(self): self.fixture.tearDown()

    def test_source_bound_jobs_are_not_discovered_or_leased_by_legacy_push(self):
        # Even an outbox left by an earlier release must remain untouched.
        self.store.ensure_viewer_transfer(self.job['jobId'])
        with self.fixture.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (self.job['jobId'],)).fetchone()
            body = json.loads(row['input_json'])
            body['sourceIntakeVersion'] = 2
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(body), self.job['jobId']))
        self.assertIsNone(self.store.claim_viewer_transfer(now=self.clock_value))
        with self.assertRaises(Conflict):
            self.store.ensure_viewer_transfer(self.job['jobId'])
        self.assertEqual(self.worker.run_once(), {'state': 'idle'})
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.store.viewer_transfer_status(self.job['jobId'])['attempts'], 0)

    def test_source_bound_pending_job_stays_available_for_viewer_pull(self):
        with self.fixture.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (self.job['jobId'],)).fetchone()
            body = json.loads(row['input_json'])
            body['sourceIntakeVersion'] = 2
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(body), self.job['jobId']))
        self.assertEqual(self.store.pending_viewer_transfer_jobs(), [])
        self.assertEqual(self.worker.run_once(), {'state': 'idle'})
        self.assertEqual(self.store.get(self.job['jobId'])['phase'], 'awaiting_acceptance')
        self.assertIsNone(self.store.viewer_transfer_status(self.job['jobId']))

    def test_source_bound_job_does_not_block_legacy_push_queue(self):
        other = str(uuid.uuid4())
        with self.fixture.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (self.job['jobId'],)).fetchone()
            body = json.loads(row['input_json'])
            body['sourceIntakeVersion'] = 2
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,?)",
                       (other, other, 'staging', 'reference-pipeline-v1', 'succeeded'))
            db.execute('''INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
                manifest_sha,pipeline_digest,generation,phase,result_json,created_at)
                SELECT ?,?,request_digest,environment,?,manifest_sha,pipeline_digest,generation,phase,result_json,'2000-01-01'
                FROM reference_jobs WHERE job_id=?''', (other, other, json.dumps(body), self.job['jobId']))
        self.sender.fail = False
        self.assertEqual(self.worker.run_once(), {'state': 'accepted', 'jobId': self.job['jobId']})
        self.assertIsNone(self.store.viewer_transfer_status(other))

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
        store = ReferenceStore(queue, main_node_id='main')
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
