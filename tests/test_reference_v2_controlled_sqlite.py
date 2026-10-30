import os,sqlite3,subprocess,sys,tempfile,unittest
from pathlib import Path
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_controlled_sqlite import ControlledSQLite,ControlledOpenHeld,EPOCH_DDL,INTENT_DDL

@unittest.skipUnless(os.name=='posix' and os.path.isdir('/proc/self/fd'),'Linux procfs required')
class ControlledSQLiteTest(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.lock=self.root/'lock';self.lock.mkdir(mode=0o700);fd=os.open(self.lock/'worker.lock',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
  self.dir=self.root/'data';self.dir.mkdir(mode=0o700);self.path=self.dir/'db'
  c=sqlite3.connect(self.path);c.execute(INTENT_DDL);c.execute(EPOCH_DDL);c.execute("INSERT INTO trusted_epoch VALUES(1,'epoch-1')");c.commit();c.close();self.path.chmod(0o600)
  s=self.path.stat();self.inode=(s.st_dev,s.st_ino);self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close)
 def test_replaced_file_detected_and_wrong_inode_rejected(self):
  c=ControlledSQLite(fence=self.fence,path=self.path,expected_inode=self.inode,expected_epoch='epoch-1')
  os.rename(self.path,self.dir/'old');replacement=sqlite3.connect(self.path);replacement.execute(INTENT_DDL);replacement.commit();replacement.close();self.path.chmod(0o600)
  with self.assertRaises(ControlledOpenHeld):c.check()
  c.close()
  with self.assertRaises(ControlledOpenHeld):ControlledSQLite(fence=self.fence,path=self.path,expected_inode=self.inode,expected_epoch='epoch-1')
 def test_process_exit_after_commit_reopens_intent(self):
  self.fence.close()
  code="""import os,sys
from processing.reference_worker_fence_v2 import WorkerFence
from processing.reference_v2_controlled_sqlite import ControlledSQLite
f=WorkerFence(sys.argv[1]);c=ControlledSQLite(fence=f,path=sys.argv[2],expected_inode=(int(sys.argv[3]),int(sys.argv[4])),expected_epoch='epoch-1')
c.db.execute('PRAGMA synchronous=FULL');c.db.execute('BEGIN IMMEDIATE');c.db.execute(\"INSERT INTO intent VALUES('pending')\");c.db.execute('COMMIT');os._exit(73)
"""
  proc=subprocess.run([sys.executable,'-c',code,str(self.lock),str(self.path),str(self.inode[0]),str(self.inode[1])],timeout=10)
  self.assertEqual(proc.returncode,73);self.fence=WorkerFence(self.lock);self.addCleanup(self.fence.close)
  c=ControlledSQLite(fence=self.fence,path=self.path,expected_inode=self.inode,expected_epoch='epoch-1')
  try:self.assertEqual(c.lookup('11111111-2222-4333-8444-555555555555'),'pending');c.check()
  finally:c.close()
