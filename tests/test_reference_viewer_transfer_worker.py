"""Durable retry/lease integration for the Viewer acceptance outbox."""
import unittest

from processing.reference_store import ReferenceStore
from processing.reference_viewer_transfer_worker import ReferenceViewerTransferWorker
from processing.viewer_acceptance_transfer import ViewerTransferUnavailable
from tests.test_reference_api import ReferenceAPITests
from tests.viewer_receipt_signer import SyntheticViewerSigner


class FakeSender:
    def __init__(self, signer): self.signer, self.calls, self.fail = signer, [], False
    def transfer(self, job, acceptance):
        self.calls.append((job['jobId'], acceptance['receiptId'], acceptance['acceptedAt']))
        if self.fail:
            self.fail = False
            raise ViewerTransferUnavailable('viewer_transport_unavailable')
        return self.signer.envelope(acceptance, job['resultManifest'])


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
        self.assertIsNone(self.store.claim_viewer_transfer(now=109, lease_seconds=10))
        recovered = self.store.claim_viewer_transfer(now=110, lease_seconds=10)
        self.assertEqual(recovered['acceptance'], acceptance)
        self.assertNotEqual(recovered['leaseToken'], lease['leaseToken'])


if __name__ == '__main__': unittest.main()
