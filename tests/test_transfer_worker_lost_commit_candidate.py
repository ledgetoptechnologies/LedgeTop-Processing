"""Actual worker + transfer + durable store; Viewer boundary is synthetic."""
import unittest
from processing.reference_store import ReferenceStore
from processing.reference_viewer_transfer_worker import ReferenceViewerTransferWorker
from processing.viewer_acceptance_transfer import ViewerAcceptanceTransfer
from tests.test_reference_api import ReferenceAPITests
from tests.test_viewer_acceptance_transfer import FakeViewer,FakeResultProxy,SECRET
from tests.viewer_receipt_signer import SyntheticViewerSigner

class TransferWorkerLostCommitTests(unittest.TestCase):
 def test_lost_viewer_commit_reply_reopens_store_without_reupload(self):
  fixture=ReferenceAPITests('runTest');fixture.setUp()
  try:
   job,result,_=fixture.ready();signer=SyntheticViewerSigner();viewer=FakeViewer(signer)
   viewer.fail_after_commit=True
   proxy=FakeResultProxy({'result-1':b'abc','result-2':b''})
   sender=ViewerAcceptanceTransfer('https://viewer.synthetic',key_id='synthetic-service',secret=SECRET,
       trusted_receipt_keys=signer.trusted_keys,result_proxy=proxy,opener=viewer,chunk_bytes=2)
   now=[100.0]
   worker=ReferenceViewerTransferWorker(fixture.store,sender,trusted_receipt_keys=signer.trusted_keys,clock=lambda:now[0])
   first=worker.run_once();self.assertEqual(first['state'],'retry_wait')
   self.assertIsNotNone(viewer.accepted)
   self.assertEqual(fixture.store.get(job['jobId'])['phase'],'awaiting_acceptance')
   upload_count=len([call for call in viewer.calls if '/objects/' in call[1]])
   self.assertGreater(upload_count,0)
   original=fixture.store.viewer_transfer_status(job['jobId'])
   self.assertFalse(original['completed'])
   # Reconstruct the actual store and worker on the existing durable queue DB.
   reopened=ReferenceStore(fixture.queue);now[0]+=2
   worker=ReferenceViewerTransferWorker(reopened,sender,trusted_receipt_keys=signer.trusted_keys,clock=lambda:now[0])
   self.assertEqual(worker.run_once()['state'],'accepted')
   self.assertEqual(len([call for call in viewer.calls if '/objects/' in call[1]]),upload_count)
   self.assertEqual(bytes(viewer.buffers['result-1']),b'abc')
   self.assertEqual(proxy.objects['result-1'],b'abc')
   self.assertEqual(viewer.calls[-1][0],'GET')
   self.assertEqual(reopened.get(job['jobId'])['phase'],'completed')
   self.assertTrue(reopened.viewer_transfer_status(job['jobId'])['completed'])
   receipts=reopened.acceptance_receipts(job['jobId'])['receipts']
   self.assertEqual(len(receipts),1)
   self.assertEqual(receipts[0]['acceptance'],viewer.accepted['acceptance'])
   self.assertEqual(receipts[0]['viewerReceipt'],viewer.accepted['viewerReceipt'])
  finally:fixture.tearDown()

if __name__=='__main__':unittest.main()