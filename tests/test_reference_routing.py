import json
import uuid
from pathlib import Path
import tempfile
import unittest

from processing.prototype import Queue, REFERENCE_PROFILE
from processing.reference_store import ReferenceStore
from processing.reference_protocol import validate_submission, manifest_sha256


class ReferenceRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.queue = Queue(self.temp.name, node_ids=('cluster', 'main'),
                           treedar_node_ids=('main',), reference_enabled=True)
        self.store = ReferenceStore(self.queue, local_max_input_bytes=3)
        self.payload = json.loads((Path(__file__).resolve().parents[1] /
            'contracts/reference-v1.contract.json').read_text())['submission']

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def submit(self):
        normalized = validate_submission(self.payload, environment='staging',
            capability_fingerprint=self.payload['capabilityFingerprint'],
            available_engines=('odm', 'treedar'))
        return self.store.submit(normalized)[0]

    def claim(self, job, node):
        return self.store.claim(job['jobId'], node_id=node, worker_id='worker',
            worker_instance_id='instance', claim_request_id='request-' + node)

    def test_small_job_cannot_take_main_or_bypass_private_claim(self):
        with self.assertRaises(ValueError):
            self.queue.submit({'requestId': 'unbound', 'environment': 'production',
                               'profile': REFERENCE_PROFILE})
        job = self.submit()
        self.assertIsNone(self.claim(job, 'main'))
        self.assertIsNone(self.queue.claim({'workerId': 'old-worker',
            'profiles': [REFERENCE_PROFILE], 'nodeId': 'cluster'}))
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
        self.assertEqual(self.claim(job, 'cluster')['nodeId'], 'cluster')

    def test_input_byte_limit_selects_main_without_local_overflow(self):
        self.store = ReferenceStore(self.queue, local_max_input_bytes=2)
        job = self.submit()
        self.assertIsNone(self.claim(job, 'cluster'))
        self.assertEqual(self.claim(job, 'main')['nodeId'], 'main')

    def test_small_job_waits_while_cluster_held(self):
        job = self.submit()
        self.queue.schedule_node('cluster', {'expectedRevision': 0,
            'drainAt': '2020-01-01T00:00:00Z', 'notBefore': '2020-01-01T00:00:00Z',
            'afterJobId': None, 'targetImage': 'sha256:' + 'a'*64})
        self.assertIsNone(self.claim(job, 'cluster'))
        self.assertIsNone(self.claim(job, 'main'))
        self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')

    def test_treedar_metadata_is_main_only_even_when_tiny(self):
        manifest = self.payload['manifest']
        manifest['objects'][0].update(name='genuine.las', kind='lidar', mediaType='application/octet-stream')
        manifest['inputCounts'].update(images=0, lidarObjects=1, points=1)
        manifest['sha256'] = manifest_sha256(manifest)
        self.payload['pipeline']['odm'].update(requested=False)
        self.payload['pipeline']['treedar'].update(requested=True,
            inputObjectIds=['image-1'], provenanceReceiptId='trusted-ingestion-receipt')
        job = self.submit()
        self.assertIsNone(self.claim(job, 'cluster'))
        self.assertEqual(self.claim(job, 'main')['nodeId'], 'main')
        # Metadata admission does not prove actual LiDAR provenance or engine readiness.

    def test_later_production_precedes_waiting_staging(self):
        staging = self.submit()
        self.payload['submissionId'] = str(uuid.uuid4())
        production = self.store.submit(validate_submission(self.payload, environment='production',
            capability_fingerprint=self.payload['capabilityFingerprint']))[0]
        self.assertIsNone(self.claim(staging, 'cluster'))
        claimed = self.claim(production, 'cluster')
        self.assertEqual(claimed['environment'], 'production')
        self.assertEqual(self.queue.get(staging['jobId'])['status'], 'queued')


if __name__ == '__main__':
    unittest.main()
