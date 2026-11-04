"""Injected synthetic completion; prepared activation remains disabled in runtime."""
import copy
import builtins
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_odm_import_journal_candidate import ReferenceOdmImportJournalCandidate
from processing.viewer_import_confirmation_store import ImportConfirmationStoreHeld
from tests.test_viewer_odm_import_confirmation_v2 import OdmImportConfirmationV2Tests


@unittest.skipUnless(os.name == 'posix' and hasattr(os, 'O_DIRECTORY') and hasattr(os, 'O_NOFOLLOW'), 'directory sync required')
class ReferenceOdmJournalCandidateTests(unittest.TestCase):
    PREPARED='ltds-viewer-prepared-envelope-v2'
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='odm-reference-proof-'); self.addCleanup(self.tmp.cleanup)
        self.queue=Queue(self.tmp.name,node_ids=('cluster',),reference_enabled=True)
        self.addCleanup(self.close_queue)
        self.ref=ReferenceStore(self.queue,jcs_runtime_enabled=True)
        self.fixture=OdmImportConfirmationV2Tests('test_signed_viewer_assertions_are_not_prebound_processing_expectations')
        self.fixture.setUp()
        sub=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        env=dict(schemaVersion=3 if self.PREPARED.endswith('jcs-v3') else 2,protocol=self.PREPARED,submission=sub,
            viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        if self.PREPARED.endswith('jcs-v3'):
            sub['pipeline']['odm']['options']={'resolution':1.25,'quality':'high'}
            policy=dict(capabilityFingerprint='a'*64,optionsFingerprint='b'*64,availableEngines=['odm'],
                        optionDefinitions={'resolution':dict(type='float',domain=dict(min=0,max=5)),
                                           'quality':dict(type='enum',domain=['high','low'])})
            job,_=self.ref.prepare_viewer_envelope_jcs_v3(json.dumps(env).encode(),authenticated_principal='viewer-prod',authenticated_environment='staging',trusted_policy=policy)
        else:
            policy=dict(capabilityFingerprint='a'*64,optionsFingerprint='b'*64,allowedOdmOptions=[],availableEngines=['odm'])
            job,_=self.ref.prepare_viewer_envelope_v2(env,authenticated_principal='viewer-prod',authenticated_environment='staging',trusted_policy=policy)
        self.scope=dict(jobId=job['jobId'],generation=1,environment='staging')
        self.env=env
        self.journal=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=self.PREPARED)
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
        self.ref=ReferenceStore(self.queue,jcs_runtime_enabled=True)
        self.journal=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=self.PREPARED)
        self.assertEqual(self.journal.record(self.scope,self.fixture.confirmation).state,'replayed')

    def test_mutated_binding_or_missing_copy_holds_existing_proof(self):
        self.journal.record(self.scope,self.fixture.confirmation)
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_viewer_receipts WHERE job_id=?',(self.scope['jobId'],))
        with self.assertRaises(ImportConfirmationStoreHeld): self.journal.load_verified(self.scope)

    def test_missing_prebinding_or_wrong_configured_principal_holds(self):
        other=ReferenceOdmImportJournalCandidate(self.ref,principal='other',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=self.PREPARED)
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

    def test_cross_protocol_substitution_and_unpinned_keys_hold(self):
        other_version=('ltds-viewer-prepared-envelope-jcs-v3' if self.PREPARED.endswith('v2') else 'ltds-viewer-prepared-envelope-v2')
        other=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,
            copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=other_version)
        with self.assertRaises(ImportConfirmationStoreHeld): other.record(self.scope,self.fixture.confirmation)
        self.journal.record(self.scope,self.fixture.confirmation)
        other_keys=copy.deepcopy(self.fixture.import_keys); other_keys['import-key']['issuerId']='wrong-issuer'
        wrong=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=other_keys,
            copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=self.PREPARED)
        with self.assertRaises(ImportConfirmationStoreHeld): wrong.load_verified(self.scope)

    def test_durable_binding_rejects_rewritten_other_version_with_same_identities(self):
        import rfc8785
        from processing.prepared_envelope_jcs_candidate import DOMAIN as jcs_domain
        from processing.reference_store import _ENVELOPE_V2_DOMAIN
        original=self.journal.record(self.scope,self.fixture.confirmation)
        new_version=('ltds-viewer-prepared-envelope-jcs-v3' if self.PREPARED.endswith('v2') else 'ltds-viewer-prepared-envelope-v2')
        rewritten=copy.deepcopy(self.env); rewritten.update(protocol=new_version,schemaVersion=3 if new_version.endswith('jcs-v3') else 2)
        canonical=rfc8785.dumps(rewritten) if new_version.endswith('jcs-v3') else p.canonical_json(rewritten)
        digest=hashlib.sha256((jcs_domain if new_version.endswith('jcs-v3') else _ENVELOPE_V2_DOMAIN)+canonical).hexdigest()
        with self.queue._db() as db:
            db.execute('UPDATE reference_viewer_submission_owners SET protocol=? WHERE job_id=?',(new_version,self.scope['jobId']))
            db.execute('UPDATE reference_viewer_envelopes_v2 SET envelope_json=?,envelope_sha256=? WHERE job_id=?',
                       (canonical.decode(),digest,self.scope['jobId']))
        with self.assertRaises(ImportConfirmationStoreHeld): self.journal.load_verified(self.scope)
        new=ReferenceOdmImportJournalCandidate(self.ref,principal='viewer-prod',environment='staging',
            issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,
            copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=new_version)
        with self.assertRaisesRegex(ImportConfirmationStoreHeld,'stored_confirmation_conflict'):
            new.load_verified(self.scope)
        self.assertEqual(original.state,'recorded')

    def test_legacy_v2_reopens_and_replays_without_jcs_dependency(self):
        if self.PREPARED.endswith('jcs-v3'):
            self.skipTest('v2-only compatibility regression')
        self.queue.close()
        self.queue=Queue(self.tmp.name,node_ids=('cluster',),reference_enabled=True)
        actual_import=builtins.__import__
        def without_jcs(name,*args,**kwargs):
            if name == 'rfc8785' or name.startswith('rfc8785.'):
                raise ImportError('synthetic missing optional JCS dependency')
            return actual_import(name,*args,**kwargs)
        with patch('builtins.__import__',side_effect=without_jcs):
            ref=ReferenceStore(self.queue)  # Default JCS-disabled configuration.
            journal=ReferenceOdmImportJournalCandidate(ref,principal='viewer-prod',environment='staging',
                issuer=self.fixture.copy_signer.issuer_id,import_keys=self.fixture.import_keys,
                copy_keys=self.fixture.copy_signer.trusted_keys,prepared_protocol=self.PREPARED)
            first=journal.record(self.scope,self.fixture.confirmation)
            self.assertEqual(first.state,'recorded')
            self.assertEqual(journal.load_verified(self.scope).sha256,first.sha256)
            self.assertEqual(journal.record(self.scope,self.fixture.confirmation).state,'replayed')


class ReferenceOdmJcsJournalCandidateTests(ReferenceOdmJournalCandidateTests):
    PREPARED='ltds-viewer-prepared-envelope-jcs-v3'


if __name__ == '__main__': unittest.main()
