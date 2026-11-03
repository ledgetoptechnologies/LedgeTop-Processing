"""Injected synthetic completion; prepared activation remains disabled in runtime."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_odm_import_journal_candidate import ReferenceOdmImportJournalCandidate
from processing.viewer_import_confirmation_store import ImportConfirmationStoreHeld
from tests.test_viewer_odm_import_confirmation_v2 import OdmImportConfirmationV2Tests


@unittest.skipUnless(os.name == 'posix' and hasattr(os, 'O_DIRECTORY') and hasattr(os, 'O_NOFOLLOW'), 'directory sync required')
class ReferenceOdmJournalCandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='odm-reference-proof-'); self.addCleanup(self.tmp.cleanup)
        self.queue=Queue(self.tmp.name,node_ids=('cluster',),reference_enabled=True)
        self.addCleanup(self.close_queue)
        self.ref=ReferenceStore(self.queue)
        self.fixture=OdmImportConfirmationV2Tests('test_signed_viewer_assertions_are_not_prebound_processing_expectations')
        self.fixture.setUp()
        sub=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        env=dict(schemaVersion=2,protocol='ltds-viewer-prepared-envelope-v2',submission=sub,
            viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        policy=dict(capabilityFingerprint='a'*64,optionsFingerprint='b'*64,allowedOdmOptions=[],availableEngines=['odm'])
        job,_=self.ref.prepare_viewer_envelope_v2(env,authenticated_principal='viewer-prod',authenticated_environment='staging',trusted_policy=policy)
        self.scope=dict(jobId=job['jobId'],generation=1,environment='staging')
        self.env=env
        self.journal=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys)
        self.assertFalse(self.ref._activation_bound_grant_ready())
        self.assertIsNone(self.ref.next_for_node('cluster'))
        self.inject_synthetic_completion(job)

    def close_queue(self):
        if not self.queue._lock.closed: self.queue.close()

    def inject_synthetic_completion(self,job):
        # Test-only direct rows model a completed upstream workflow that cannot
        # yet run through the intentionally blocked v2 activation path.
        f=self.fixture; f.manifest.update(jobId=job['jobId'],pipelineDigest=job['pipelineDigest'])
        f.manifest['resultManifestSha256']=p.digest({k:v for k,v in f.manifest.items() if k!='resultManifestSha256'})
        a=copy.deepcopy(f.envelope['acceptance']); a.update(jobId=job['jobId'],pipelineDigest=job['pipelineDigest'],
            inputManifestSha256=job['manifestSha256'],resultManifestSha256=f.manifest['resultManifestSha256'])
        f.envelope=f.copy_signer.envelope(a,f.manifest); receipt=f.envelope['viewerReceipt']
        f.confirmation.update(jobId=job['jobId'],pipelineDigest=job['pipelineDigest'],inputManifestSha256=job['manifestSha256'],
            resultManifestSha256=f.manifest['resultManifestSha256'],viewerTaskId=self.env['viewerTaskId'],viewerAttemptId=self.env['viewerAttemptId'])
        f.confirmation['acceptedCopies']=[dict(acceptanceReceiptId=a['receiptId'],acceptanceStorageReceiptId=a['storageReceiptId'],
            viewerReceiptId=receipt['receiptId'],viewerStorageReceiptId=receipt['storageReceiptId'],envelopeSha256=p.digest(f.envelope))]
        f.sign()
        binding=dict(schemaVersion=1,protocol='ltds-viewer-activation-v1',activationId=self.env['activationId'],
            principalId='viewer-prod',submissionId=self.env['submission']['submissionId'],jobId=job['jobId'],generation=1,
            environment='staging',viewerTaskId=self.env['viewerTaskId'],viewerAttemptId=self.env['viewerAttemptId'],
            requestDigest=p.digest(self.env['submission']),manifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'])
        with self.queue._db() as db:
            db.execute('INSERT INTO reference_viewer_activation VALUES(?,?,?,?,?,?,?)',
                (job['jobId'],binding['activationId'],'viewer-prod','staging',binding['viewerAttemptId'],p.canonical_json(binding).decode(),p.digest(binding)))
            db.execute("UPDATE reference_jobs SET result_json=?,phase='completed' WHERE job_id=?",(p.canonical_json(f.manifest).decode(),job['jobId']))
            db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',
                (a['receiptId'],job['jobId'],p.digest(a),p.canonical_json(a).decode()))
            db.execute('INSERT INTO reference_viewer_receipts(receipt_id,storage_receipt_id,acceptance_receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?,?,?)',
                (receipt['receiptId'],receipt['storageReceiptId'],a['receiptId'],job['jobId'],p.digest(receipt),p.canonical_json(receipt).decode()))
            db.execute('INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json,viewer_manifest_json,completed_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)',
                (job['jobId'],a['receiptId'],p.canonical_json(a).decode(),p.canonical_json(f.manifest).decode()))

    def test_same_db_first_proof_restart_replay_and_no_owner_after_close(self):
        result=self.journal.record(self.scope,self.fixture.confirmation)
        self.assertEqual(result.state,'recorded')
        self.assertEqual(self.journal.load_verified(self.scope).sha256,result.sha256)
        self.queue.close()
        with self.assertRaises(ImportConfirmationStoreHeld): self.journal.load_verified(self.scope)
        self.queue=Queue(self.tmp.name,node_ids=('cluster',),reference_enabled=True)
        self.ref=ReferenceStore(self.queue)
        self.journal=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys)
        self.assertEqual(self.journal.record(self.scope,self.fixture.confirmation).state,'replayed')

    def test_mutated_binding_or_missing_copy_holds_existing_proof(self):
        self.journal.record(self.scope,self.fixture.confirmation)
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_viewer_receipts WHERE job_id=?',(self.scope['jobId'],))
        with self.assertRaises(ImportConfirmationStoreHeld): self.journal.load_verified(self.scope)

    def test_missing_prebinding_or_wrong_configured_principal_holds(self):
        other=ReferenceOdmImportJournalCandidate(self.ref,principal='other',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys)
        with self.assertRaises(ImportConfirmationStoreHeld): other.record(self.scope,self.fixture.confirmation)
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_viewer_activation WHERE job_id=?',(self.scope['jobId'],))
        with self.assertRaises(ImportConfirmationStoreHeld): self.journal.record(self.scope,self.fixture.confirmation)

    def test_binding_read_and_first_insert_share_write_transaction(self):
        loaded=threading.Event(); proceed=threading.Event(); original=self.journal.journal.loader
        def paused(db,scope):
            value=original(db,scope); loaded.set()
            if not proceed.wait(5): raise RuntimeError('test timed out')
            return value
        self.journal.journal.loader=paused
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(self.journal.record,self.scope,self.fixture.confirmation)
            try:
                self.assertTrue(loaded.wait(5))
                with sqlite3.connect(self.queue.path,timeout=0.1) as other:
                    with self.assertRaises(sqlite3.OperationalError):
                        other.execute('UPDATE reference_viewer_transfers SET completed_at=NULL WHERE job_id=?',(self.scope['jobId'],))
            finally: proceed.set()
            self.assertEqual(future.result(timeout=5).state,'recorded')


if __name__ == '__main__': unittest.main()
