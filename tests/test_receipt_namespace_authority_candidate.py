import concurrent.futures
import hashlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from processing.prototype import Queue
from processing.reference_store import ReferenceStore,JCSRuntimeUnavailable
from processing.viewer_import_confirmation_store import ImportConfirmationStore
from processing.viewer_import_confirmation_store import ImportConfirmationStoreHeld
from processing.viewer_odm_import_confirmation_store_v2 import OdmImportConfirmationStoreV2
from processing.receipt_namespace_authority_candidate import ReceiptNamespaceGate,ReceiptNamespaceHeld
from tests.test_viewer_import_confirmation_store import fixture_loader
from tests import test_viewer_import_confirmation as import_fixture
from processing import reference_protocol


@unittest.skipUnless(os.name=='posix','journal durability requires POSIX')
class ReceiptNamespaceAuthorityTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='receipt-authority-');self.addCleanup(self.temp.cleanup)
  self.queue=Queue(self.temp.name,reference_enabled=True);self.addCleanup(self.queue.close)
  self.store=ReferenceStore(self.queue)
  self.v1=ImportConfirmationStore(self.queue.path,trusted_loader=lambda *_:None)
  self.v2=OdmImportConfirmationStoreV2(self.queue.path,trusted_loader=lambda *_:None)
  self.gate=ReceiptNamespaceGate(self.queue,self.v1,self.v2)
 def digest(self):return hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
 def test_clean_snapshot_is_read_only_and_gate_is_explicit(self):
  before=self.digest();r=self.gate.census()
  self.assertEqual(r,{'state':'clear','identities':0,'censusBytes':0});self.assertEqual(self.digest(),before)
  self.assertEqual(self.store.configure_receipt_namespace_gate(self.gate),r)
  self.assertEqual(self.digest(),before)
 def test_different_existing_database_is_rejected_without_mutation(self):
  other=Path(self.temp.name)/'separate.sqlite';foreign=OdmImportConfirmationStoreV2(other,trusted_loader=lambda *_:None)
  before=self.digest();other_before=hashlib.sha256(other.read_bytes()).hexdigest()
  with self.assertRaisesRegex(ReceiptNamespaceHeld,'shared_receipt_database_required'):
   ReceiptNamespaceGate(self.queue,self.v1,foreign).census()
  with self.assertRaises(JCSRuntimeUnavailable):self.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(self.queue,self.v1,foreign))
  self.assertEqual(self.digest(),before);self.assertEqual(hashlib.sha256(other.read_bytes()).hexdigest(),other_before)
 def test_existing_cross_version_collision_holds_without_rewriting_history(self):
  with self.queue._db() as db:
   db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('job-a',1,'production','collision','a','b',X'01')")
   db.execute("INSERT INTO viewer_odm_import_confirmations_candidate_v2 VALUES('job-b',1,'staging','collision','c','d',X'02')")
  before=self.digest()
  with self.assertRaisesRegex(ReceiptNamespaceHeld,'receipt_namespace_collision'):self.gate.census()
  with self.assertRaises(JCSRuntimeUnavailable):self.store.configure_receipt_namespace_gate(self.gate)
  self.assertEqual(self.digest(),before)
 def test_competing_cooperative_writers_are_serialized_with_census(self):
  def write(table,job):
   db=sqlite3.connect(self.queue.path,timeout=5,isolation_level=None);db.row_factory=sqlite3.Row
   try:
    db.execute('BEGIN IMMEDIATE');self.gate.check_in_transaction(db,proposed=('same-id',))
    db.execute('INSERT INTO '+table+' VALUES(?,1,\'production\',\'same-id\',\'a\',\'b\',X\'01\')',(job,));db.commit();return 'written'
   except ReceiptNamespaceHeld:
    db.rollback();return 'held'
   finally:db.close()
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   results=list(pool.map(lambda args:write(*args),[('viewer_import_confirmations_candidate_v1','job-a'),('viewer_odm_import_confirmations_candidate_v2','job-b')]))
  self.assertCountEqual(results,['written','held']);self.assertEqual(self.gate.census()['identities'],1)
 def test_real_signed_import_writer_rechecks_other_version_in_transaction(self):
  fixture=import_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay');fixture.setUp()
  scope={'jobId':'job-1','generation':1,'environment':'staging'}
  context=dict(expected=fixture.expected,result_manifest=fixture.manifest,accepted_envelopes=fixture.envelopes,
               trusted_import_keys=fixture.import_keys,trusted_copy_keys=fixture.copy_signer.trusted_keys)
  self.v1.loader=fixture_loader
  with self.queue._db() as db:
   db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
   db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',(*scope.values(),reference_protocol.canonical_json(context).decode()))
  self.store.configure_receipt_namespace_gate(self.gate)
  with self.queue._db() as db:
   db.execute('INSERT INTO viewer_odm_import_confirmations_candidate_v2 VALUES (?,1,\'staging\',?,\'a\',\'b\',X\'01\')',('other-job',fixture.confirmation['confirmationId']))
  before=self.digest()
  with self.assertRaisesRegex(ImportConfirmationStoreHeld,'shared_receipt_namespace_unavailable'):
   self.v1.record(scope,fixture.confirmation)
  self.assertEqual(self.digest(),before)
  with self.queue._db() as db:
   self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_import_confirmations_candidate_v1').fetchone()[0],0)
 def test_real_signed_import_exact_replay_survives_configured_gate(self):
  fixture=import_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay');fixture.setUp()
  scope={'jobId':'job-1','generation':1,'environment':'staging'}
  context=dict(expected=fixture.expected,result_manifest=fixture.manifest,accepted_envelopes=fixture.envelopes,
               trusted_import_keys=fixture.import_keys,trusted_copy_keys=fixture.copy_signer.trusted_keys)
  self.v1.loader=fixture_loader
  with self.queue._db() as db:
   db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
   db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',(*scope.values(),reference_protocol.canonical_json(context).decode()))
  self.store.configure_receipt_namespace_gate(self.gate)
  first=self.v1.record(scope,fixture.confirmation);again=self.v1.record(scope,fixture.confirmation)
  self.assertEqual((first.state,again.state),('recorded','replayed'));self.assertEqual(first.sha256,again.sha256)

if __name__=='__main__':unittest.main()
