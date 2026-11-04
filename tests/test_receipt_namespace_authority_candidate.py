import concurrent.futures
import copy
import hashlib
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from processing.prototype import Queue
from processing.reference_store import ReferenceStore,JCSRuntimeUnavailable
from processing.reference_terminal import TerminalService
from processing.viewer_import_confirmation_store import ImportConfirmationStore
from processing.viewer_import_confirmation_store import ImportConfirmationStoreHeld
from processing.viewer_odm_import_confirmation_store_v2 import OdmImportConfirmationStoreV2
from processing.receipt_namespace_authority_candidate import ReceiptNamespaceGate,ReceiptNamespaceHeld
from tests.test_viewer_import_confirmation_store import fixture_loader
from tests import test_viewer_import_confirmation as import_fixture
from tests import test_reference_delivery as delivery_fixture
from tests import test_reference_terminal as terminal_fixture
from processing.maintenance import Conflict
from processing import reference_protocol


@unittest.skipUnless(os.name=='posix','journal durability requires POSIX')
class ReceiptNamespaceAuthorityTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='receipt-authority-');self.addCleanup(self.temp.cleanup)
  self.queue=Queue(self.temp.name,reference_enabled=True);self.addCleanup(self.queue.close)
  self.store=ReferenceStore(self.queue)
  TerminalService(self.store)
  self.v1=ImportConfirmationStore(self.queue.path,trusted_loader=lambda *_:None)
  self.v2=OdmImportConfirmationStoreV2(self.queue.path,trusted_loader=lambda *_:None)
  self.gate=ReceiptNamespaceGate(self.queue,self.v1,self.v2)
 def digest(self):return hashlib.sha256(Path(self.queue.path).read_bytes()).hexdigest()
 def test_v2_fixture_gate_exact_same_job_alias_and_conflicts(self):
  from processing.prepared_v3_copy_v2_candidate import SCHEMA
  from tests.test_viewer_copy_v2_candidate import ViewerCopyV2CandidateTests
  f=ViewerCopyV2CandidateTests('test_valid_full_closure_and_public_api_disabled');f.setUp()
  wire=reference_protocol.canonical_json({'acceptance':f.a,'viewerReceipt':f.r})
  acceptance=reference_protocol.canonical_json(f.a).decode()
  with self.queue._db() as db:
   db.execute(SCHEMA)
   db.execute('INSERT INTO fixture_v3_copy_v2 VALUES(?,?,?,?,?,?,?)',
    (f.a['jobId'],wire,hashlib.sha256(wire).hexdigest(),f.r['receiptId'],f.r['storageReceiptId'],f.a['receiptId'],f.a['storageReceiptId']))
   db.execute('INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json) VALUES(?,?,?)',
    (f.a['jobId'],f.a['receiptId'],acceptance))
  self.assertEqual(self.gate.census()['identities'],4)
  with self.queue._db() as db:
   db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',
    (f.a['receiptId'],f.a['jobId'],'a'*64,acceptance))
  self.assertEqual(self.gate.census()['identities'],4)
  with self.queue._db() as db:
   altered=copy.deepcopy(f.a);altered['acceptedAt']='2026-11-04T00:00:02.000Z'
   db.execute('UPDATE reference_viewer_transfers SET acceptance_json=?',(reference_protocol.canonical_json(altered).decode(),))
  with self.assertRaises(ReceiptNamespaceHeld):self.gate.census()
  with self.queue._db() as db:
   db.execute('UPDATE reference_viewer_transfers SET acceptance_json=?',(acceptance,))
   db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('job-a',1,'production',?,'a','b',X'01')",(f.a['receiptId'],))
  with self.assertRaises(ReceiptNamespaceHeld):self.gate.census()
 def test_clean_snapshot_is_read_only_and_gate_is_explicit(self):
  before=self.digest();r=self.gate.census()
  self.assertEqual(r,{'state':'local_scan_clear_history_unverified','identities':0,'censusBytes':0});self.assertEqual(self.digest(),before)
  self.assertEqual(self.store.configure_receipt_namespace_gate(self.gate),r)
  self.assertEqual(self.digest(),before)
 def test_different_existing_database_is_rejected_without_mutation(self):
  other=Path(self.temp.name)/'separate.sqlite';foreign=OdmImportConfirmationStoreV2(other,trusted_loader=lambda *_:None)
  before=self.digest();other_before=hashlib.sha256(other.read_bytes()).hexdigest()
  with self.assertRaisesRegex(ReceiptNamespaceHeld,'shared_receipt_database_required'):
   ReceiptNamespaceGate(self.queue,self.v1,foreign).census()
  with self.assertRaises(JCSRuntimeUnavailable):self.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(self.queue,self.v1,foreign))
  self.assertEqual(self.digest(),before);self.assertEqual(hashlib.sha256(other.read_bytes()).hexdigest(),other_before)
 def test_missing_terminal_namespace_holds_without_creating_it(self):
  with tempfile.TemporaryDirectory(prefix='missing-terminal-') as root:
   queue=Queue(root,reference_enabled=True)
   try:
    ReferenceStore(queue)
    v1=ImportConfirmationStore(queue.path,trusted_loader=lambda *_:None)
    v2=OdmImportConfirmationStoreV2(queue.path,trusted_loader=lambda *_:None)
    before=hashlib.sha256(Path(queue.path).read_bytes()).hexdigest()
    with self.assertRaisesRegex(ReceiptNamespaceHeld,'receipt_namespace_missing'):
     ReceiptNamespaceGate(queue,v1,v2).census()
    self.assertEqual(hashlib.sha256(Path(queue.path).read_bytes()).hexdigest(),before)
   finally:queue.close()
 def test_existing_cross_version_collision_holds_without_rewriting_history(self):
  with self.queue._db() as db:
   db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('job-a',1,'production','collision','a','b',X'01')")
   db.execute("INSERT INTO viewer_odm_import_confirmations_candidate_v2 VALUES('job-b',1,'staging','collision','c','d',X'02')")
  before=self.digest()
  with self.assertRaisesRegex(ReceiptNamespaceHeld,'receipt_namespace_collision'):self.gate.census()
  with self.assertRaises(JCSRuntimeUnavailable):self.store.configure_receipt_namespace_gate(self.gate)
  self.assertEqual(self.digest(),before)
 def test_oversized_scalar_and_deep_legacy_json_hold_without_mutation(self):
  with self.queue._db() as db:
   db.execute('INSERT INTO viewer_import_confirmations_candidate_v1 VALUES(?,1,\'staging\',?,\'a\',\'b\',X\'01\')',('job-a','x'*1000000))
  before=self.digest()
  with self.assertRaises(ReceiptNamespaceHeld):self.gate.census()
  self.assertEqual(self.digest(),before)
  # A separate synthetic database exercises bounded JSON depth without
  # deleting or rewriting the first hostile row.
  with tempfile.TemporaryDirectory(prefix='dense-receipt-') as root:
   q=Queue(root,reference_enabled=True)
   try:
    store=ReferenceStore(q);TerminalService(store)
    v1=ImportConfirmationStore(q.path,trusted_loader=lambda *_:None);v2=OdmImportConfirmationStoreV2(q.path,trusted_loader=lambda *_:None)
    with q._db() as db:db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',('synthetic','job','f'*64,'['*14+'0'+']'*14))
    digest=hashlib.sha256(Path(q.path).read_bytes()).hexdigest()
    with self.assertRaises(ReceiptNamespaceHeld):ReceiptNamespaceGate(q,v1,v2).census()
    self.assertEqual(hashlib.sha256(Path(q.path).read_bytes()).hexdigest(),digest)
   finally:q.close()
 def test_nul_and_ambiguous_legacy_rows_are_held_without_mutation(self):
  cases=(('id','ok\x00'+'x'*1000000),('json','{}\x00'+'x'*1000000),
         ('json','{"receiptId":"hidden","receiptId":"visible","storageReceiptId":"storage"}'))
  for kind,value in cases:
   with self.subTest(kind=kind, prefix=value[:30]), tempfile.TemporaryDirectory(prefix='hostile-census-') as root:
    q=Queue(root,reference_enabled=True)
    try:
     store=ReferenceStore(q);TerminalService(store)
     v1=ImportConfirmationStore(q.path,trusted_loader=lambda *_:None);v2=OdmImportConfirmationStoreV2(q.path,trusted_loader=lambda *_:None)
     with q._db() as db:
      if kind=='id':db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('job',1,'staging',?,'a','b',X'01')",(value,))
      else:db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',('visible','job','f'*64,value))
     before=hashlib.sha256(Path(q.path).read_bytes()).hexdigest()
     with self.assertRaises(ReceiptNamespaceHeld):ReceiptNamespaceGate(q,v1,v2).census()
     self.assertEqual(hashlib.sha256(Path(q.path).read_bytes()).hexdigest(),before)
    finally:q.close()
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
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   repeats=list(pool.map(lambda _:self.v1.record(scope,fixture.confirmation),range(2)))
  self.assertEqual([r.state for r in repeats],['replayed','replayed']);self.assertTrue(all(r.sha256==first.sha256 for r in repeats))
 def test_real_terminal_writer_rejects_import_id_collision(self):
  fixture=terminal_fixture.ReferenceTerminalTests('test_unavailable_bound_grant_still_blocks_runtime_claims');fixture.setUp()
  try:
   os.chmod(fixture.queue.path.parent,0o700)
   fixture.confirm();v1=ImportConfirmationStore(fixture.queue.path,trusted_loader=lambda *_:None);v2=OdmImportConfirmationStoreV2(fixture.queue.path,trusted_loader=lambda *_:None)
   fixture.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(fixture.queue,v1,v2))
   receipt=fixture.receipt()
   with fixture.queue._db() as db:db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('other',1,'staging',?,'a','b',X'01')",(receipt['receiptId'],))
   with self.assertRaisesRegex(Conflict,'shared receipt namespace unavailable'):fixture.terminal.accept(fixture.job['jobId'],receipt,'staging')
   with fixture.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_terminal_acceptances').fetchone()[0],0)
  finally:fixture.doCleanups()
 def test_real_terminal_and_signed_import_race_one_writer_holds(self):
  fixture=terminal_fixture.ReferenceTerminalTests('test_unavailable_bound_grant_still_blocks_runtime_claims');fixture.setUp()
  try:
   os.chmod(fixture.queue.path.parent,0o700);fixture.confirm()
   v1=ImportConfirmationStore(fixture.queue.path,trusted_loader=fixture_loader);v2=OdmImportConfirmationStoreV2(fixture.queue.path,trusted_loader=lambda *_:None)
   proof=import_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay');proof.setUp()
   scope={'jobId':'job-1','generation':1,'environment':'staging'}
   context=dict(expected=proof.expected,result_manifest=proof.manifest,accepted_envelopes=proof.envelopes,trusted_import_keys=proof.import_keys,trusted_copy_keys=proof.copy_signer.trusted_keys)
   receipt=fixture.receipt();proof.confirmation['confirmationId']=receipt['receiptId'];proof.sign()
   with fixture.queue._db() as db:
    db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
    db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',(*scope.values(),reference_protocol.canonical_json(context).decode()))
   fixture.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(fixture.queue,v1,v2));barrier=threading.Barrier(2)
   def terminal():
    barrier.wait(timeout=5)
    try:fixture.terminal.accept(fixture.job['jobId'],receipt,'staging');return 'written'
    except Conflict:return 'held'
   def imported():
    barrier.wait(timeout=5)
    try:v1.record(scope,proof.confirmation);return 'written'
    except ImportConfirmationStoreHeld:return 'held'
   with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    futures=[pool.submit(terminal),pool.submit(imported)];results=[x.result(timeout=10) for x in futures]
   self.assertCountEqual(results,['written','held'])
  finally:fixture.doCleanups()
 def test_real_acceptance_writer_rejects_import_id_and_allows_exact_replay(self):
  with patch.object(ReferenceStore,'_activation',return_value={'viewerAttemptId':'synthetic'}),patch.object(ReferenceStore,'_activation_bound_grant_ready',return_value=True):
   fixture=delivery_fixture.ReferenceDeliveryTests('test_lost_viewer_acceptance_response_replays_after_restart_without_cleanup');fixture.setUp()
   try:
    os.chmod(fixture.queue.path.parent,0o700)
    fixture.receiver.receive(fixture.envelope,**fixture.identity)
    TerminalService(fixture.store);v1=ImportConfirmationStore(fixture.queue.path,trusted_loader=lambda *_:None);v2=OdmImportConfirmationStoreV2(fixture.queue.path,trusted_loader=lambda *_:None)
    fixture.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(fixture.queue,v1,v2))
    receipt=copy.deepcopy(fixture.fixture['acceptance']);receipt.update(jobId=fixture.job['jobId'],pipelineDigest=fixture.job['pipelineDigest'],inputManifestSha256=fixture.job['manifestSha256'],resultManifestSha256=fixture.result['resultManifestSha256'])
    collision=copy.deepcopy(receipt);collision['receiptId']='import-reserved'
    with fixture.queue._db() as db:db.execute("INSERT INTO viewer_import_confirmations_candidate_v1 VALUES('other',1,'staging','import-reserved','a','b',X'01')")
    with self.assertRaisesRegex(Conflict,'shared receipt namespace unavailable'):fixture.store.accept(fixture.job['jobId'],collision)
    first=fixture.store.accept(fixture.job['jobId'],receipt);again=fixture.store.accept(fixture.job['jobId'],receipt)
    self.assertTrue(first['created']);self.assertFalse(again['created'])
   finally:fixture.tearDown()
 def test_real_acceptance_holds_before_decoding_nul_prefixed_legacy_row(self):
  with patch.object(ReferenceStore,'_activation',return_value={'viewerAttemptId':'synthetic'}),patch.object(ReferenceStore,'_activation_bound_grant_ready',return_value=True):
   fixture=delivery_fixture.ReferenceDeliveryTests('test_lost_viewer_acceptance_response_replays_after_restart_without_cleanup');fixture.setUp()
   try:
    fixture.receiver.receive(fixture.envelope,**fixture.identity)
    receipt=copy.deepcopy(fixture.fixture['acceptance']);receipt.update(jobId=fixture.job['jobId'],pipelineDigest=fixture.job['pipelineDigest'],inputManifestSha256=fixture.job['manifestSha256'],resultManifestSha256=fixture.result['resultManifestSha256'])
    with fixture.queue._db() as db:db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',('hostile','other','f'*64,'{}\x00'+'x'*1000000))
    before=hashlib.sha256(Path(fixture.queue.path).read_bytes()).hexdigest()
    with patch('processing.reference_copy_hold_integrity.bounded_json',side_effect=AssertionError('decoder reached')):
     with self.assertRaisesRegex(Conflict,'acceptance identity unavailable'):fixture.store.accept(fixture.job['jobId'],receipt)
    self.assertEqual(hashlib.sha256(Path(fixture.queue.path).read_bytes()).hexdigest(),before)
   finally:fixture.tearDown()
 def test_real_acceptance_and_signed_import_race_one_writer_holds(self):
  with patch.object(ReferenceStore,'_activation',return_value={'viewerAttemptId':'synthetic'}),patch.object(ReferenceStore,'_activation_bound_grant_ready',return_value=True):
   fixture=delivery_fixture.ReferenceDeliveryTests('test_lost_viewer_acceptance_response_replays_after_restart_without_cleanup');fixture.setUp()
   try:
    os.chmod(fixture.queue.path.parent,0o700);fixture.receiver.receive(fixture.envelope,**fixture.identity)
    TerminalService(fixture.store);v1=ImportConfirmationStore(fixture.queue.path,trusted_loader=fixture_loader);v2=OdmImportConfirmationStoreV2(fixture.queue.path,trusted_loader=lambda *_:None)
    proof=import_fixture.ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay');proof.setUp()
    scope={'jobId':'job-1','generation':1,'environment':'staging'}
    context=dict(expected=proof.expected,result_manifest=proof.manifest,accepted_envelopes=proof.envelopes,trusted_import_keys=proof.import_keys,trusted_copy_keys=proof.copy_signer.trusted_keys)
    receipt=copy.deepcopy(fixture.fixture['acceptance']);receipt.update(jobId=fixture.job['jobId'],pipelineDigest=fixture.job['pipelineDigest'],inputManifestSha256=fixture.job['manifestSha256'],resultManifestSha256=fixture.result['resultManifestSha256'])
    proof.confirmation['confirmationId']=receipt['receiptId'];proof.sign()
    with fixture.queue._db() as db:
     db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
     db.execute('INSERT INTO trusted_import_fixture VALUES (?,?,?,?)',(*scope.values(),reference_protocol.canonical_json(context).decode()))
    fixture.store.configure_receipt_namespace_gate(ReceiptNamespaceGate(fixture.queue,v1,v2));barrier=threading.Barrier(2)
    def accept():
     barrier.wait(timeout=5)
     try:fixture.store.accept(fixture.job['jobId'],receipt);return 'written'
     except Conflict:return 'held'
    def imported():
     barrier.wait(timeout=5)
     try:v1.record(scope,proof.confirmation);return 'written'
     except ImportConfirmationStoreHeld:return 'held'
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
     futures=[pool.submit(accept),pool.submit(imported)];results=[x.result(timeout=10) for x in futures]
    self.assertCountEqual(results,['written','held'])
   finally:fixture.tearDown()

if __name__=='__main__':unittest.main()
