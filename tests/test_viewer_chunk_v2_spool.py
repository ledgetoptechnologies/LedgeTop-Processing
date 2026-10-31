import os,unittest
from tests.test_viewer_chunk_v2_geometry import V2GeometryTests
from processing.viewer_chunk_v2_spool import SyntheticV2Spool
from processing.viewer_archive_spool import SpoolHeld

class V2SpoolTests(V2GeometryTests):
 def test_actual_small_archive_journal_and_replay(self):
  import hashlib
  data=self.f.base.base.archive;self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True)
  self.assertEqual(spool.prepare(self.job,self.ident,self.token),{'state':'ready'})
  chunks=[data[i:i+16] for i in range(0,len(data),16)]
  for index,chunk in enumerate(chunks):
   first=spool.write(self.job,self.ident,self.token,index,index*16,chunk)
   self.assertEqual(first,spool.write(self.job,self.ident,self.token,index,index*16,chunk))
  seal=spool.finish(self.job,self.ident,self.token,len(chunks));self.assertEqual(seal['state'],'sealed_no_receipt');self.assertEqual(seal['seal']['sha256'],hashlib.sha256(data).hexdigest())
  self.assertEqual(spool.prepare(self.job,self.ident,self.token),{'state':'sealed'})
  with self.f.fixture.queue._db() as db:self.assertIsNone(db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone())
 def test_bad_geometry_and_header_substitution_hold(self):
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True);spool.prepare(self.job,self.ident,self.token)
  with self.assertRaises(Exception):spool.write(self.job,self.ident,self.token,0,1,b'x'*16)
  with self.f.fixture.queue._db() as db:key=db.execute('SELECT slot_key FROM viewer_chunk_v2_slots WHERE job_id=?',(self.job,)).fetchone()[0]
  header=self.f.root/('slot-'+key+'.alloc.chunks-v2.header');header.unlink()
  with self.assertRaises(SpoolHeld):spool.prepare(self.job,self.ident,self.token)
  self.assertFalse(header.exists())
 def test_killed_after_write_leaves_ambiguous_intent(self):
  import json,os,signal,subprocess,sys
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True);spool.prepare(self.job,self.ident,self.token)
  with self.f.fixture.queue._db() as db:row=db.execute('SELECT binding,slot_key FROM viewer_chunk_v2_slots WHERE job_id=?',(self.job,)).fetchone();binding=row[0];key=row[1]
  data=self.f.base.base.archive[:16]
  code='''import json,os,signal,sys
from unittest.mock import patch
from processing.direct_output_slot import _open_slot,OutputSlot
from processing.viewer_chunk_v2_journal import SyntheticV2ChunkJournal
root,identity,key,binding,data=sys.argv[1:];b=json.loads(binding)
slot=_open_slot(root,json.loads(identity),key,b["archiveBytes"],max_budget=65536,allow_chunk_journal=True)
j=SyntheticV2ChunkJournal(slot,b,legacy_state="new",synthetic_fixture=True);original=OutputSlot.write_at
def kill(self,*args):
 original(self,*args);os.kill(os.getpid(),signal.SIGKILL)
with patch.object(OutputSlot,"write_at",kill):j.write(0,0,bytes.fromhex(data))
'''
  child=subprocess.run([sys.executable,'-c',code,str(self.f.root),json.dumps(self.f.root_identity),key,binding,data.hex()],capture_output=True,timeout=10)
  self.assertEqual(child.returncode,-signal.SIGKILL,child.stderr)
  with self.assertRaises(Exception):spool.write(self.job,self.ident,self.token,0,0,data)
 def test_missing_geometry_with_existing_v2_slot_never_recreated_or_v1_adopted(self):
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True);spool.prepare(self.job,self.ident,self.token)
  with self.f.fixture.queue._db() as db:db.execute('DELETE FROM viewer_chunk_v2_geometry WHERE job_id=?',(self.job,))
  with self.assertRaisesRegex(SpoolHeld,'v2_geometry_missing_for_slot'):self.register(chunk_bytes=16)
  from processing.viewer_chunk_spool import SyntheticChunkSpool
  with self.assertRaisesRegex(SpoolHeld,'v2_session'):SyntheticChunkSpool(self.f.spool,synthetic_fixture=True).begin(self.job,self.ident,self.token,chunk_bytes=16)
  with self.assertRaisesRegex(SpoolHeld,'v2_session'):self.f.spool.receive(self.job,self.ident,self.token,iter([self.f.base.base.archive]))

 def test_changed_geometry_schema_blocks_v2_session(self):
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True);spool.prepare(self.job,self.ident,self.token)
  with self.f.fixture.queue._db() as db:db.execute('CREATE INDEX geometry_unexpected ON viewer_chunk_v2_geometry(binding)')
  with self.assertRaisesRegex(SpoolHeld,'v2_schema_requires_inspection'):spool.prepare(self.job,self.ident,self.token)

 def test_creation_intent_survives_failure_and_blocks_recreate(self):
  from unittest.mock import patch
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True)
  with patch('processing.viewer_chunk_v2_spool.SyntheticV2ChunkJournal',side_effect=RuntimeError('synthetic crash')):
   with self.assertRaises(RuntimeError):spool.prepare(self.job,self.ident,self.token)
  with self.f.fixture.queue._db() as db:
   phase=db.execute('SELECT phase FROM viewer_chunk_v2_geometry WHERE job_id=?',(self.job,)).fetchone()[0]
  self.assertEqual(phase,'creating')
  with self.assertRaises(SpoolHeld):spool.prepare(self.job,self.ident,self.token)
 def test_missing_slot_table_after_creation_never_recreates(self):
  self.register(chunk_bytes=16);spool=SyntheticV2Spool(self.f.spool,synthetic_fixture=True);spool.prepare(self.job,self.ident,self.token)
  with self.f.fixture.queue._db() as db:
   key=db.execute('SELECT slot_key FROM viewer_chunk_v2_slots WHERE job_id=?',(self.job,)).fetchone()[0]
   db.execute('DROP TABLE viewer_chunk_v2_slots')
  for path in self.f.root.glob('slot-'+key+'*'):path.unlink()
  with self.assertRaisesRegex(SpoolHeld,'v2_slot_schema_missing'):spool.prepare(self.job,self.ident,self.token)
  self.assertEqual(list(self.f.root.glob('slot-'+key+'*')),[])

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class V2MarkerRaceTests(unittest.TestCase):
 def test_v2_marker_added_before_ordinary_flock_is_rejected(self):
  import tempfile,threading
  from pathlib import Path
  from unittest.mock import patch
  from concurrent.futures import ThreadPoolExecutor
  from processing.direct_output_slot import open_slot,_identity
  import processing.direct_output_slot as module
  with tempfile.TemporaryDirectory() as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:identity=_identity(fd)
   finally:os.close(fd)
   key='f'*64;slot=open_slot(root,identity,key,16,max_budget=16,create=True);slot.close()
   entered=threading.Event();release=threading.Event();original=module.fcntl.flock
   def barrier(fd,op):
    if threading.current_thread().name.startswith('v2ordinary'):
     entered.set()
     if not release.wait(5):raise RuntimeError('barrier timeout')
    return original(fd,op)
   with patch.object(module.fcntl,'flock',side_effect=barrier):
    with ThreadPoolExecutor(max_workers=1,thread_name_prefix='v2ordinary') as pool:
     task=pool.submit(open_slot,root,identity,key,16,max_budget=16)
     try:
      self.assertTrue(entered.wait(5));(Path(root)/('slot-'+key+'.alloc.chunks-v2.partial')).write_bytes(b'partial')
     finally:release.set()
     with self.assertRaisesRegex(Exception,'slot_chunk_journal_requires_inspection'):task.result(timeout=5)

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class V2JournalScaleTests(unittest.TestCase):
 def test_4096_real_small_chunks_with_bounded_directory_work(self):
  import hashlib,resource,tempfile
  from unittest.mock import patch
  from processing.direct_output_slot import open_slot,_identity
  from processing.viewer_chunk_v2_journal import SyntheticV2ChunkJournal
  data=b'x'*4096
  with tempfile.TemporaryDirectory() as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:identity=_identity(fd)
   finally:os.close(fd)
   slot=open_slot(root,identity,'b'*64,len(data),max_budget=len(data),create=True)
   binding=dict(jobId='11111111-1111-4111-8111-111111111111',taskId='22222222-2222-4222-8222-222222222222',generation=1,environment='staging',archiveBytes=len(data),archiveSha256=hashlib.sha256(data).hexdigest(),version=2,chunkBytes=1,chunkCount=4096)
   try:
    SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True,allow_create=True)
    with patch('processing.viewer_chunk_v2_journal.os.listdir',side_effect=AssertionError('unexpected directory scan')):
     for i in range(4096):
      j=SyntheticV2ChunkJournal(slot,binding,legacy_state='new',synthetic_fixture=True);j.write(i,i,b'x')
     self.assertEqual(j.finish(4096)['sha256'],binding['archiveSha256'])
    print('v2 synthetic chunks=4096 journal_entries=8194 rss_kib=%d'%resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,flush=True)
   finally:slot.close()
