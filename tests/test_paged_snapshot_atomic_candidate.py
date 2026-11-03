import base64,copy,hashlib,io,json,sqlite3,subprocess,sys,unittest
from processing import reference_protocol as p
from processing import paged_frame_receiver_candidate as frames
from processing.paged_snapshot_journal_candidate import initialize,inspect,ServerSnapshotTrust,SnapshotHeld,DOMAIN
from processing.viewer_paged_frame_candidate import VERSION as SV,DOMAIN as SD
from processing.viewer_selected_frames_candidate import VERSION as TV,DOMAIN as TD,binding_digest
from processing.large_paged_manifest_candidate import build,_digest
from tests.test_viewer_selected_frames_candidate import frames as encode_frames
from tests.test_paged_snapshot_signature_candidate import PagedSnapshotSignatureTests


class AtomicSnapshotTests(unittest.TestCase):
 statement_hash=PagedSnapshotSignatureTests.statement_hash
 def setUp(self):
  PagedSnapshotSignatureTests.setUp(self)
  with self.queue._db() as db:initialize(db);frames.initialize(db)
  source=[dict(type='dataset_file',datasetFileId='photo',relativePath='flight/a.jpg',byteSize=3,sha256='a'*64,processingRole='image',mediaType='image/jpeg',kind='image')]
  selected=[dict(type='dataset_file',ordinal=0,pageObjectId='photo',datasetFileId='photo')]
  self.source,self.sourcepins=encode_frames(source,SV,SD);self.selected,self.selectedpins=encode_frames(selected,TV,TD)
  closure,pages=build([dict(id='photo',name='a.jpg',byteSize=3,sha256='a'*64,kind='image',mediaType='image/jpeg')]);self.closure,self.pages=closure,pages
  inp=[dict(relativePath='flight/a.jpg',byteSize=3,sha256='a'*64)]
  inputhash=hashlib.sha256(json.dumps(inp,separators=(',',':')).encode()).hexdigest()
  self.statement.update(sourcePageHashes=self.sourcepins,sourcePageBytes=[len(self.source)-1],sourceCount=1,selectedPageHashes=self.selectedpins,selectedPageBytes=[len(self.selected)-1],selectedCount=1,inputManifestSha256=inputhash,processingClosureSha256=_digest(closure))
  self.binding=dict(datasetId=self.statement['datasetId'],datasetManifestSha256=self.statement['datasetManifestSha256'],principalId='viewer-a',environment='staging',authorityId=self.queue.authority_id,viewerTaskId=self.env['viewerTaskId'],viewerAttemptId=self.env['viewerAttemptId'],pipelineDigest=self.statement['pipelineDigest'],revocationEpoch=1,sourcePageHashes=self.sourcepins,sourceCount=1,selectedPageHashes=self.selectedpins,selectedCount=1,inputManifestSha256=inputhash,processingClosureSha256=_digest(closure))
  self.trust=ServerSnapshotTrust(authority_id=self.queue.authority_id,key_entries=self.config,clock_provider=lambda:1893456300000)
 def raw(self):
  value=copy.deepcopy(self.statement);value['signature']=base64.urlsafe_b64encode(self.private.sign(DOMAIN+p.canonical_json(value))).decode().rstrip('=');return p.canonical_json(value)
 def run_stage(self,source=None,fault=None):
  bundle=dict(binding=self.binding,bindingSha256=binding_digest(self.binding),sourceStream=io.BytesIO(self.source if source is None else source),selectedStream=io.BytesIO(self.selected),processingClosure=self.closure,processingPages=self.pages,evidenceStream=None)
  return inspect(self.store,self.env['submission']['submissionId'],self.raw(),principal='viewer-a',environment='staging',trust=self.trust,stage_bundle=bundle,_fault=fault)
 def counts(self):
  with self.queue._db() as db:return tuple(db.execute('SELECT (SELECT count(*) FROM paged_snapshot_journal_v1),(SELECT count(*) FROM candidate_frame_receipts),(SELECT count(*) FROM candidate_frame_pages)').fetchone())
 def test_commit_and_orderly_replay(self):
  self.assertEqual(self.run_stage()['reason'],'signed_pages_staged_only');self.assertEqual(self.counts(),(1,1,3))
  self.assertEqual(self.run_stage(source=b'')['reason'],'snapshot_replay_status_only');self.assertEqual(self.counts(),(1,1,3))
  with self.assertRaises(SnapshotHeld):self.run_stage(source=self.source[:-1])
  self.assertEqual(self.counts(),(1,1,3))
 def test_failure_each_phase_rolls_back_all(self):
  for phase in ('journal','frames','evidence'):
   def fault(at):
    if at==phase:raise sqlite3.OperationalError('synthetic failure')
   with self.subTest(phase=phase),self.assertRaises(SnapshotHeld):self.run_stage(fault=fault)
   self.assertEqual(self.counts(),(0,0,0))
 def test_truncated_stream_and_changed_pin_roll_back(self):
  with self.assertRaises(SnapshotHeld):self.run_stage(source=self.source[:-1])
  self.assertEqual(self.counts(),(0,0,0))
  self.statement['sourcePageBytes']=[2]
  with self.assertRaises(SnapshotHeld):self.run_stage()
  self.assertEqual(self.counts(),(0,0,0))
 def test_missing_journal_with_marker_cannot_be_recreated(self):
  self.run_stage()
  with self.queue._db() as db:db.execute('DROP TABLE paged_snapshot_journal_v1')
  with self.queue._db() as db:
   with self.assertRaises(SnapshotHeld):initialize(db)
 def test_oversize_stored_statement_holds(self):
  self.run_stage()
  with self.queue._db() as db:db.execute('UPDATE paged_snapshot_journal_v1 SET signed_bytes=?',(b'x'*65537,))
  with self.assertRaises(SnapshotHeld):self.run_stage()
 def test_committed_snapshot_survives_process_reopen_after_lost_reply(self):
  self.run_stage()
  arg=dict(root=self.temp.name,authority=self.queue.authority_id,entries=self.config,submission=self.env['submission']['submissionId'],raw=self.raw().decode(),binding=self.binding,closure=self.closure,pages=self.pages)
  self.queue.close()
  script='''import io,json,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.paged_snapshot_journal_candidate import inspect,ServerSnapshotTrust
from processing.viewer_selected_frames_candidate import binding_digest
a=json.load(sys.stdin);q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
try:
 s=ReferenceStore(q,jcs_runtime_enabled=True);t=ServerSnapshotTrust(authority_id=a['authority'],key_entries=a['entries'],clock_provider=lambda:1893460000000)
 b=dict(binding=a['binding'],bindingSha256=binding_digest(a['binding']),sourceStream=io.BytesIO(b''),selectedStream=io.BytesIO(b''),processingClosure=a['closure'],processingPages=a['pages'],evidenceStream=None)
 print(json.dumps(inspect(s,a['submission'],a['raw'].encode(),principal='viewer-a',environment='staging',trust=t,stage_bundle=b)))
finally:q.close()
'''
  result=subprocess.run([sys.executable,'-c',script],input=json.dumps(arg),capture_output=True,text=True,check=True,timeout=20)
  self.assertEqual(json.loads(result.stdout)['reason'],'snapshot_replay_status_only')
