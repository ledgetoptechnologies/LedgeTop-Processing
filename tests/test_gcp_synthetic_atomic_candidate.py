import copy,io,json,sqlite3,tempfile,unittest
from tests.test_actual_viewer_gcp_stream_pair import ViewerGcpPair,decoded
from processing import gcp_synthetic_atomic_candidate as a

class AtomicGcp(unittest.TestCase):
 @classmethod
 def setUpClass(cls):ViewerGcpPair.setUpClass();cls.x=ViewerGcpPair.x
 def bundle(self):
  x=self.x; prefix=('gcpEvidenceVersion','gcpEvidencePageHashes','gcpEvidencePageBytes','gcpEvidencePageCount','gcpEvidenceByteCount','gcpEvidenceSha256','gcpFileVersion','gcpFilePageHashes','gcpFilePageBytes','gcpFilePageCount','gcpFileByteCount','gcpFileSha256','sourceCount','sourcePageHashes','selectedPageHashes')
  st={k:copy.deepcopy(x[k]) for k in prefix};st.update(version='synthetic-gcp-combined-v1',attemptId=x['attemptId'],sourcePageBytes=[len(z)-1 for z in decoded(x['sourceFrames']).splitlines(keepends=True)],selectedPageBytes=[len(z)-1 for z in decoded(x['selectedFrames']).splitlines(keepends=True)])
  streams={k:decoded(x[v]) for k,v in (('source','sourceFrames'),('selection','selectedFrames'),('evidence','gcpEvidenceFrames'),('file','gcpFileFrames'))}
  rows=json.loads(decoded(x['storedEvidence']))['correspondences'];images={r['imageDatasetFileId']:dict(relativePath=r['imageRelativePath'],width=r['imageWidth'],height=r['imageHeight']) for r in rows}
  return dict(bundle_id='synthetic',statement=st,streams=streams,trusted_images=images,trusted_correspondences=copy.deepcopy(rows))
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
