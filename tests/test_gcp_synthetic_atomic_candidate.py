import copy,hashlib,io,json,sqlite3,tempfile,unittest
from processing import reference_protocol as protocol
from processing.viewer_selected_frames_candidate import DOMAIN as SELECTED_DOMAIN
from tests.test_actual_viewer_gcp_stream_pair import ViewerGcpPair,decoded
from processing import gcp_synthetic_atomic_candidate as a
from processing.large_paged_manifest_candidate import build,_digest
from processing.viewer_selected_frames_candidate import binding_digest

class AtomicGcp(unittest.TestCase):
 @classmethod
 def setUpClass(cls):ViewerGcpPair.setUpClass();cls.x=ViewerGcpPair.x
 def bundle(self):
  x=self.x; prefix=('gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256','sourceCount','sourcePageHashes','selectedPageHashes')
  st={k:copy.deepcopy(x[k]) for k in prefix};st.update(version='synthetic-gcp-combined-v1',attemptId=x['attemptId'],sourcePageBytes=[len(z)-1 for z in decoded(x['sourceFrames']).splitlines(keepends=True)],selectedPageBytes=[len(z)-1 for z in decoded(x['selectedFrames']).splitlines(keepends=True)])
  streams={k:decoded(x[v]) for k,v in (('source','sourceFrames'),('selection','selectedFrames'),('evidence','gcpEvidenceFrames'),('file','gcpFileFrames'))}
  rows=json.loads(decoded(x['storedEvidence']))['correspondences'];images={r['imageDatasetFileId']:dict(relativePath=r['imageRelativePath'],width=r['imageWidth'],height=r['imageHeight']) for r in rows}
  source=[r for line in streams['source'].splitlines() for r in json.loads(line)['records']];selected=[r for line in streams['selection'].splitlines() for r in json.loads(line)['records']]
  by_id={r['datasetFileId']:r for r in source if r['type']=='dataset_file'};gcp=next(r for r in source if r['type']=='attempt_generated_gcp')
  objects=[]
  for r in selected:
   item=by_id[r['datasetFileId']] if r['type']=='dataset_file' else gcp
   objects.append(dict(id=r['pageObjectId'],name=item['relativePath'].rsplit('/',1)[-1] if r['type']=='dataset_file' else 'gcp_list.txt',byteSize=item['byteSize'],sha256=item['sha256'],kind=item['kind'] if r['type']=='dataset_file' else 'auxiliary',mediaType=item['mediaType'] if r['type']=='dataset_file' else r['mediaType']))
  closure,pages=build(objects)
  binding=dict(datasetId=x['datasetId'],datasetManifestSha256=x['datasetManifestSha256'],principalId='synthetic',environment='staging',authorityId='synthetic',viewerTaskId=x['taskId'],viewerAttemptId=x['attemptId'],pipelineDigest='b'*64,revocationEpoch=0,sourcePageHashes=st['sourcePageHashes'],sourceCount=st['sourceCount'],selectedPageHashes=st['selectedPageHashes'],selectedCount=x['selectedCount'],inputManifestSha256=x['inputManifestSha256'],processingClosureSha256=_digest(closure))
  return dict(bundle_id='synthetic',statement=st,streams=streams,trusted_images=images,trusted_correspondences=copy.deepcopy(rows),trusted_binding=binding,expected_binding_sha256=binding_digest(binding),processing_closure=closure,processing_pages=pages)
 def test_success_and_failures_rollback(self):
  with tempfile.TemporaryDirectory() as d:
   bundle=self.bundle();db=sqlite3.connect(d+'/candidate.sqlite');self.addCleanup(db.close);a.initialize(db);db.commit()
   for phase in ('source','selection','evidence','file','semantic'):
    db.execute('BEGIN IMMEDIATE')
    def fail(where):
     if where==phase:raise ValueError('fault')
    with self.subTest(phase=phase),self.assertRaises(a.SyntheticHeld):a.stage_owned(db,**bundle,_fault=fail)
    db.rollback();self.assertEqual(db.execute('SELECT count(*) FROM synthetic_gcp_bundle').fetchone()[0],0)
   db.execute('BEGIN IMMEDIATE');self.assertEqual(a.stage_owned(db,**bundle)['state'],'held_synthetic_only');db.commit()
   self.assertEqual(db.execute('SELECT count(*) FROM synthetic_gcp_bundle').fetchone()[0],1)
   db.execute('BEGIN IMMEDIATE')
   with self.assertRaises(a.SyntheticHeld):a.stage_owned(db,**bundle)
   db.rollback();self.assertEqual(db.execute('SELECT count(*) FROM synthetic_gcp_bundle').fetchone()[0],1);db.close()

 def test_changed_file_rolls_back_all(self):
  with tempfile.TemporaryDirectory() as d:
   bundle=self.bundle();bad=copy.deepcopy(bundle);bad['streams']['file']=bad['streams']['file'][:-1]
   db=sqlite3.connect(d+'/candidate.sqlite');a.initialize(db);db.commit()
   try:
    db.execute('BEGIN IMMEDIATE')
    with self.assertRaises(a.SyntheticHeld):a.stage_owned(db,**bad)
    db.rollback();self.assertEqual(db.execute('SELECT count(*) FROM synthetic_gcp_bundle').fetchone()[0],0)
   finally:db.close()

 def test_selection_mutations_cannot_match_independent_closure(self):
  for case in ('omit_photo','bad_id','bad_version','reorder'):
   bundle=self.bundle();frame=json.loads(bundle['streams']['selection']);records=frame['records']
   if case=='omit_photo':records.pop(1)
   elif case=='bad_id':records[0]['pageObjectId']='incorrect-id'
   elif case=='bad_version':frame['version']='forged-version'
   else:records[0],records[1]=records[1],records[0]
   for i,r in enumerate(records):r['ordinal']=i
   wire=protocol.canonical_json(frame);pin=hashlib.sha256(SELECTED_DOMAIN+wire).hexdigest()
   bundle['streams']['selection']=wire+b'\n';bundle['statement']['selectedPageHashes']=[pin];bundle['statement']['selectedPageBytes']=[len(wire)]
   # Simulate a forged yet self-consistent pin and binding; the independently
   # fixed processing closure and input manifest still enforce exact selection.
   bundle['trusted_binding']['selectedPageHashes']=[pin];bundle['trusted_binding']['selectedCount']=len(records);bundle['expected_binding_sha256']=binding_digest(bundle['trusted_binding'])
   db=sqlite3.connect(':memory:');a.initialize(db);db.commit();db.execute('BEGIN IMMEDIATE')
   try:
    with self.subTest(case=case),self.assertRaises(a.SyntheticHeld):a.stage_owned(db,**bundle)
    db.rollback();self.assertEqual(db.execute('SELECT count(*) FROM synthetic_gcp_bundle').fetchone()[0],0)
   finally:db.close()
