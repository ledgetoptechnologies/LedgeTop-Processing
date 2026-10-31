"""Synthetic geometry binding tests; no v2 bytes or runtime activation."""
import concurrent.futures,unittest
from processing.viewer_chunk_v2_geometry import SyntheticV2Geometry,geometry
from processing.viewer_chunk_spool import SyntheticChunkSpool
from processing.viewer_archive_spool import SpoolHeld
from tests.test_viewer_archive_spool import ViewerArchiveSpoolTests

class V2GeometryTests(unittest.TestCase):
 def setUp(self):
  self.f=ViewerArchiveSpoolTests('test_authenticated_exact_spool_and_replay');self.addCleanup(self.f.doCleanups);self.f.setUp()
  self.job=self.f.fixture.job;self.ident=self.f.identity()
  with self.f.fixture.queue._db() as db:self.token=db.execute('SELECT token FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()[0]
  self.v2=SyntheticV2Geometry(self.f.spool,synthetic_fixture=True)
 def register(self,**kw):return self.v2.negotiate(self.job,self.ident,self.token,version=kw.get('version',2),chunk_bytes=kw.get('chunk_bytes',16))
 def test_auth_exact_replay_and_v1_fence(self):
  with self.assertRaises(Exception):self.v2.negotiate(self.job,dict(self.ident,worker_id='wrong'),self.token,version=2,chunk_bytes=16)
  first=self.register();self.assertEqual(first['state'],'held_no_transfer');self.assertEqual(first,self.register())
  with self.assertRaises(SpoolHeld):self.register(chunk_bytes=17)
  with self.assertRaises(SpoolHeld):self.register(version=1)
  with self.assertRaisesRegex(SpoolHeld,'v2_session'):SyntheticChunkSpool(self.f.spool,synthetic_fixture=True).begin(self.job,self.ident,self.token,chunk_bytes=16)
  with self.f.fixture.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
 def test_concurrent_same_binding_and_v1_prior_hold(self):
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   values=list(pool.map(lambda _:self.register(),range(2)))
  self.assertEqual(values[0],values[1])
 def test_pure_geometry_more_than_one_gib_not_capacity_proof(self):
  value=geometry(1536*1024*1024,512*1024,2);self.assertEqual(value['chunkCount'],3072)
  self.assertEqual(geometry(4096,1,2)['chunkCount'],4096)
  for version,size,chunk in ((1,1,1),(None,1,1),(2,4097,1),(2,1,1024*1024+1),(2,4*1024**3+1,1024*1024)):
   with self.subTest(version=version,size=size,chunk=chunk):
    with self.assertRaises(SpoolHeld):geometry(size,chunk,version)
