import base64,copy,hashlib,json,os,sqlite3,subprocess,sys,threading,queue,unittest,uuid
from pathlib import Path
from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.prepared_envelope_jcs_candidate import PROTOCOL
from processing.viewer_selected_frames_candidate import binding_digest
from processing.large_paged_manifest_candidate import build,_digest
from processing.viewer_dual_digest_binding_candidate import initialize as init_binding,PROVENANCE_FIELDS,VERSION
from processing.paged_snapshot_journal_candidate import initialize_frames as init_frames, FRAME_PAGES_DDL, FRAME_MARKER_DDL, SnapshotHeld
from processing.viewer_no_gcp_atomic_candidate import stage,NoGcpHeld,initialize as init_combined
from tests.test_paged_snapshot_signature_candidate import PagedSnapshotSignatureTests

SCRIPT=r'''import base64,json,os,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.viewer_no_gcp_atomic_candidate import stage
a=json.loads(sys.stdin.readline());q=Queue(a['root']);q.allowed_profiles.add(PROFILE)
a['bundle']['source']=base64.b64decode(a['bundle']['source']);a['bundle']['selection']=base64.b64decode(a['bundle']['selection'])
def fault(phase):
 if phase==a['phase']:print('ready',flush=True);sys.stdin.read(1)
stage(ReferenceStore(q,jcs_runtime_enabled=True),a['submission'],a['statement'],principal='viewer-a',environment='staging',configured_authority=a['authority'],synthetic_viewer_provenance=a['provenance'],synthetic_bundle=a['bundle'],_fault=fault)
os._exit(73)
'''
class NoGcpAtomic(unittest.TestCase):
 statement_hash=PagedSnapshotSignatureTests.statement_hash
 @classmethod
 def setUpClass(cls):
  viewer=Path(os.environ['PINNED_VIEWER_ROOT']);head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=viewer,text=True).strip()
  if head!='4ed7dcd566380c3c0cd7f03a765ee1badf2004d5' or subprocess.check_output(['git','status','--porcelain'],cwd=viewer,text=True).strip():raise RuntimeError('clean pinned Viewer required')
  out=subprocess.check_output(['node','tests/support/viewer_no_gcp_export_pair.cjs'],text=True,timeout=20);cls.x=json.loads(out)
 def setUp(self):
  PagedSnapshotSignatureTests.setUp(self);x=self.x
  src=base64.b64decode(x['sourceFrames']);sel=base64.b64decode(x['selectedFrames']);records=[r for l in src.splitlines() for r in json.loads(l)['records']];selected=[r for l in sel.splitlines() for r in json.loads(l)['records']];byid={r['datasetFileId']:r for r in records};objects=[]
  for r in selected:
   item=byid[r['datasetFileId']];objects.append(dict(id=r['pageObjectId'],name=item['relativePath'].rsplit('/',1)[-1],byteSize=item['byteSize'],sha256=item['sha256'],kind=item['kind'],mediaType=item['mediaType']))
  closure,pages=build(objects);env=copy.deepcopy(self.env);env['submission']['submissionId']=str(uuid.uuid4());env['viewerTaskId']=x['taskId'];env['viewerAttemptId']=x['attemptId'];env['activationId']=str(uuid.uuid4());manifest=env['submission']['manifest'];manifest.update(objects=objects,objectCount=len(objects),totalBytes=sum(o['byteSize'] for o in objects),inputCounts=dict(images=sum(o['kind']=='image' for o in objects),lidarObjects=sum(o['kind']=='lidar' for o in objects),points=0));manifest['sha256']=p.manifest_sha256(manifest)
  job,_=self.store.prepare_viewer_envelope_jcs_v3(json.dumps(env).encode(),authenticated_principal='viewer-a',authenticated_environment='staging',trusted_policy=self.policy,expected_authority_id=self.queue.authority_id);found=self.store.find_viewer_envelope_v2(env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=PROTOCOL,expected_authority_id=self.queue.authority_id);self.submission=env['submission']['submissionId']
  self.provenance={k:copy.deepcopy(x[k]) for k in PROVENANCE_FIELDS if k.startswith(('gcpEvidence','gcpFile'))};self.provenance.update(viewerDatasetId=x['datasetId'],viewerDatasetManifestSha256=x['datasetManifestSha256'],viewerTaskId=x['taskId'],viewerAttemptId=x['attemptId'],viewerInputManifestSha256=x['inputManifestSha256'],sourcePageHashes=x['sourcePageHashes'],sourcePageBytes=[len(z)-1 for z in src.splitlines(keepends=True)],sourceCount=x['sourceCount'],selectedPageHashes=x['selectedPageHashes'],selectedPageBytes=[len(z)-1 for z in sel.splitlines(keepends=True)],selectedCount=x['selectedCount'])
  self.statement=dict(version=VERSION,schemaVersion=1,principalId='viewer-a',environment='staging',coordinatorAuthorityId=self.queue.authority_id,submissionId=self.submission,jobId=job['jobId'],generation=job['generation'],activationId=env['activationId'],preparedEnvelopeSha256=found['envelopeSha256'],transportManifestSha256=manifest['sha256'],pipelineDigest=p.digest(env['submission']['pipeline']),**self.provenance)
  b=dict(datasetId=x['datasetId'],datasetManifestSha256=x['datasetManifestSha256'],principalId='viewer-a',environment='staging',authorityId=self.queue.authority_id,viewerTaskId=x['taskId'],viewerAttemptId=x['attemptId'],pipelineDigest=self.statement['pipelineDigest'],revocationEpoch=0,sourcePageHashes=x['sourcePageHashes'],sourceCount=x['sourceCount'],selectedPageHashes=x['selectedPageHashes'],selectedCount=x['selectedCount'],inputManifestSha256=x['inputManifestSha256'],processingClosureSha256=_digest(closure))
  self.bundle=dict(binding=b,bindingSha256=binding_digest(b),source=src,selection=sel,processingClosure=closure,processingPages=pages)
  with self.queue._db() as db:init_combined(db)
 def call(self,bundle=None,fault=None):return stage(self.store,self.submission,self.statement,principal='viewer-a',environment='staging',configured_authority=self.queue.authority_id,synthetic_viewer_provenance=self.provenance,synthetic_bundle=bundle or self.bundle,_fault=fault)
 def counts(self):
  with self.queue._db() as db:return tuple(db.execute('SELECT (SELECT count(*) FROM synthetic_viewer_dataset_transport_v1),(SELECT count(*) FROM candidate_frame_receipts),(SELECT count(*) FROM candidate_frame_pages)').fetchone())
 def test_commit_exact_replay_and_tamper(self):
  self.assertTrue(all(self.statement[k] is None for k in PROVENANCE_FIELDS if k.startswith(('gcpEvidence','gcpFile'))));self.assertNotEqual(self.statement['viewerDatasetManifestSha256'],self.statement['transportManifestSha256']);self.assertTrue(self.call()['created']);self.assertFalse(self.call()['created']);self.assertEqual(self.counts(),(1,1,3))
  bad=copy.deepcopy(self.bundle);bad['selection']=bad['selection'][:-1]
  with self.assertRaises(NoGcpHeld):self.call(bad)
  self.assertEqual(self.counts(),(1,1,3))
 def test_real_child_termination_and_replay(self):
  bundle=copy.deepcopy(self.bundle);bundle['source']=base64.b64encode(bundle['source']).decode();bundle['selection']=base64.b64encode(bundle['selection']).decode();root=str(self.queue.path.parent);data=dict(root=root,submission=self.submission,statement=self.statement,authority=self.queue.authority_id,provenance=self.provenance,bundle=bundle);self.queue.close()
  for phase in ('binding','source','selected','verified'):
   child=subprocess.Popen([sys.executable,'-c',SCRIPT],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
   try:
    child.stdin.write(json.dumps(dict(data,phase=phase))+'\n');child.stdin.flush();out=queue.Queue();reader=threading.Thread(target=lambda:out.put(child.stdout.readline()),daemon=True);reader.start();self.assertEqual(out.get(timeout=20).strip(),'ready');child.kill();child.wait(timeout=10);reader.join(timeout=5);self.assertFalse(reader.is_alive())
   finally:
    if child.poll() is None:child.kill();child.wait(timeout=10)
    child.communicate(timeout=5)
   self.queue=Queue(root);self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True);self.assertEqual(self.counts(),(0,0,0));self.queue.close()
  result=subprocess.run([sys.executable,'-c',SCRIPT],input=json.dumps(dict(data,phase='none'))+'\n',text=True,capture_output=True,timeout=20);self.assertEqual(result.returncode,73,msg=result.stderr);self.queue=Queue(root);self.queue.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.queue,jcs_runtime_enabled=True);self.addCleanup(self.queue.close);self.assertEqual(self.counts(),(1,1,3));self.assertFalse(self.call()['created'])

 def test_bad_mapping_and_transport_rollback(self):
  for mode in ('id','omit','transport','binding'):
   bad=copy.deepcopy(self.bundle)
   if mode in ('id','omit'):
    frame=json.loads(bad['selection']);r=frame['records']
    if mode=='id':r[0]['pageObjectId']='forged-id'
    else:r.pop()
    wire=p.canonical_json(frame);bad['selection']=wire+b'\n'
   elif mode=='transport':bad['processingPages'][0]['body']['objects'].pop()
   else:bad['binding']['principalId']='forged'
   with self.subTest(mode=mode),self.assertRaises(NoGcpHeld):self.call(bad)
   self.assertEqual(self.counts(),(0,0,0))

 def test_corrupt_stored_pages_rejected_before_replay_loading(self):
  for mode in ('oversized','wrongtype','extra_pages'):
   with self.subTest(mode=mode):
    self.assertTrue(self.call()['created'])
    with self.queue._db() as db:
     if mode=='oversized':db.execute("UPDATE candidate_frame_pages SET wire=? WHERE stage_id=? AND kind='source'",(b'x'*(262144+1),self.statement['jobId']))
     elif mode=='wrongtype':db.execute("UPDATE candidate_frame_pages SET wire='bad' WHERE stage_id=? AND kind='source'",(self.statement['jobId'],))
     else:
      for i in range(1,65):db.execute("INSERT INTO candidate_frame_pages VALUES(?,?,?,?)",(self.statement['jobId'],'source',i,b'{}'))
    with self.assertRaises(NoGcpHeld):self.call()
    # Restore only this task's synthetic database for the next subcase.
    with self.queue._db() as db:
     db.execute('DELETE FROM candidate_frame_pages WHERE stage_id=?',(self.statement['jobId'],));db.execute('DELETE FROM candidate_frame_receipts WHERE stage_id=?',(self.statement['jobId'],));db.execute('DELETE FROM synthetic_viewer_dataset_transport_v1 WHERE job_id=?',(self.statement['jobId'],))

 def test_selected_prescan_is_bounded_and_strict(self):
  samples=(b'{}\n'*100000,b'{"version":"x","version":"y","index":0,"records":[]}\n',b'{broken}\n',b'{"index":0,"records":[],"version":"ltds-viewer-selected-frames-v1"}\n')
  valid=tuple(p.canonical_json(dict(version='ltds-viewer-selected-frames-v1',index=i,records=[dict(type='dataset_file',datasetFileId='x',pageObjectId='x',ordinal=i)]))+b'\n' for i in range(65))
  samples=samples+(b''.join(valid),)
  for wire in samples:
   bad=copy.deepcopy(self.bundle);bad['selection']=wire
   with self.subTest(size=len(wire)),self.assertRaises(NoGcpHeld):self.call(bad)
   self.assertEqual(self.counts(),(0,0,0))

 def test_schema_corruption_holds_before_binding_and_rolls_back(self):
  for mode in ('trigger','index','marker_index','missing_marker','altered_table'):
   with self.queue._db() as db:
    if mode=='trigger':db.execute('CREATE TRIGGER bad_frame_trigger AFTER INSERT ON candidate_frame_pages BEGIN SELECT 1; END')
    elif mode=='index':db.execute('CREATE INDEX bad_frame_index ON candidate_frame_pages(kind)')
    elif mode=='marker_index':db.execute('CREATE INDEX bad_marker_index ON paged_snapshot_frame_marker_v1(version)')
    elif mode=='missing_marker':db.execute('DROP TABLE paged_snapshot_frame_marker_v1')
    else:db.execute('DROP TABLE candidate_frame_pages');db.execute('CREATE TABLE candidate_frame_pages(stage_id TEXT,kind TEXT,ordinal INTEGER,wire BLOB)')
   with self.subTest(mode=mode),self.assertRaises(NoGcpHeld):self.call()
   self.assertEqual(self.counts()[0],0)
   with self.queue._db() as db:
    if mode=='trigger':db.execute('DROP TRIGGER bad_frame_trigger')
    elif mode=='index':db.execute('DROP INDEX bad_frame_index')
    elif mode=='marker_index':db.execute('DROP INDEX bad_marker_index')
    elif mode=='missing_marker':db.execute(FRAME_MARKER_DDL);db.execute('INSERT INTO paged_snapshot_frame_marker_v1 VALUES(1,1)')
    else:db.execute('DROP TABLE candidate_frame_pages');db.execute(FRAME_PAGES_DDL)
 def test_partial_schema_initialization_does_not_repair(self):
  db=sqlite3.connect(':memory:');db.execute('CREATE TABLE candidate_frame_receipts(stage_id TEXT)');db.commit()
  try:
   with self.assertRaises(SnapshotHeld):init_frames(db)
   names={x[0] for x in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
   self.assertEqual(names,{'candidate_frame_receipts'})
  finally:db.close()

 def test_cross_history_partial_loss_never_recreates(self):
  for lost in ('frames','binding'):
   db=sqlite3.connect(':memory:');init_combined(db)
   try:
    db.execute('PRAGMA foreign_keys=OFF')
    if lost=='frames':
     db.execute("INSERT INTO synthetic_viewer_dataset_transport_v1 VALUES('job','hash',X'01')")
     for name in ('candidate_frame_pages','candidate_frame_receipts','paged_snapshot_frame_marker_v1'):db.execute('DROP TABLE '+name)
    else:
     db.execute("INSERT INTO candidate_frame_receipts VALUES('job','hash','{}','{}')")
     for name in ('synthetic_viewer_dataset_transport_v1','synthetic_viewer_dataset_transport_marker_v1'):db.execute('DROP TABLE '+name)
    db.commit();before=list(db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"))
    with self.subTest(lost=lost),self.assertRaises(NoGcpHeld):init_combined(db)
    self.assertEqual(before,list(db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")))
   finally:db.close()
 def test_orphan_history_holds_without_new_binding(self):
  for mode in ('page','receipt','binding'):
   with self.queue._db() as db:
    if mode=='page':db.execute("INSERT INTO candidate_frame_pages VALUES('orphan','source',0,X'01')")
    elif mode=='receipt':db.execute("INSERT INTO candidate_frame_receipts VALUES('orphan','hash','{}','{}')")
    else:db.execute("INSERT INTO synthetic_viewer_dataset_transport_v1 VALUES('orphan','hash',X'01')")
   with self.subTest(mode=mode),self.assertRaises(NoGcpHeld):self.call()
   with self.queue._db() as db:
    self.assertEqual(db.execute('SELECT count(*) FROM synthetic_viewer_dataset_transport_v1 WHERE job_id=?',(self.statement['jobId'],)).fetchone()[0],0)
    db.execute("DELETE FROM candidate_frame_pages WHERE stage_id='orphan'");db.execute("DELETE FROM candidate_frame_receipts WHERE stage_id='orphan'");db.execute("DELETE FROM synthetic_viewer_dataset_transport_v1 WHERE job_id='orphan'")

 def test_missing_frame_kind_is_not_accepted_as_history(self):
  self.assertTrue(self.call()['created'])
  with self.queue._db() as db:db.execute("DELETE FROM candidate_frame_pages WHERE stage_id=? AND kind='source'",(self.statement['jobId'],))
  with self.assertRaises(NoGcpHeld):self.call()
  self.assertEqual(self.counts()[:2],(1,1))
