"""Direct results cannot borrow the verified coordinator-spool receipt path."""
import sys
import tempfile
import unittest
from pathlib import Path
from processing.maintenance import Conflict
from processing.source_intake_mode import TRIGGERS
from tests.test_reference_v2_store_acceptance import ReferenceV2StoreAcceptanceTests
from tests.viewer_v2_receipt_signer import SyntheticViewerCopyV2Signer

@unittest.skipUnless(sys.platform.startswith('linux'),'real Linux source and receipt fixture')
class DirectResultModeGuard(unittest.TestCase):
    def test_direct_mode_refuses_spool_receipt_even_when_result_and_source_look_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            helper=ReferenceV2StoreAcceptanceTests('runTest')
            state=helper._make_source_result_ready(directory,Path('.'))
            try:
                queue,store=state['queue'],state['store']
                signer=SyntheticViewerCopyV2Signer()
                envelope=signer.envelope(state['expected'],state['acceptance'],state['viewer_result_manifest'])
                with queue._db() as db:
                    db.execute('DROP TRIGGER source_intake_mode_no_update')
                    db.execute("UPDATE reference_source_intake_modes SET mode='direct-v2' WHERE job_id=?",(state['job']['jobId'],))
                    db.execute(TRIGGERS['source_intake_mode_no_update'])
                with self.assertRaisesRegex(Conflict,'source intake mode mismatch'):
                    store.accept(state['job']['jobId'],envelope['acceptance'],viewer_receipt=envelope['viewerReceipt'],
                        trusted_viewer_receipt_keys=signer.trusted_keys)
                with queue._db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_viewer_receipts').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT phase FROM reference_jobs WHERE job_id=?',(state['job']['jobId'],)).fetchone()[0],'awaiting_acceptance')
                # Model damaged state that hides both normal source indicators;
                # the durable direct-mode marker must still stop legacy acceptance.
                import json
                with queue._db() as db:
                    db.execute('PRAGMA foreign_keys=OFF')
                    raw=db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(state['job']['jobId'],)).fetchone()[0]
                    submission=json.loads(raw);submission.pop('sourceIntakeVersion')
                    db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',(json.dumps(submission),state['job']['jobId']))
                    db.execute('DELETE FROM reference_source_intakes WHERE job_id=?',(state['job']['jobId'],))
                with self.assertRaisesRegex(Conflict,'source intake mode mismatch'):
                    store.accept(state['job']['jobId'],envelope['acceptance'],viewer_receipt=envelope['viewerReceipt'],
                        trusted_viewer_receipt_keys=signer.trusted_keys)
                with queue._db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT phase FROM reference_jobs WHERE job_id=?',(state['job']['jobId'],)).fetchone()[0],'awaiting_acceptance')
            finally:helper._close_state(state)
