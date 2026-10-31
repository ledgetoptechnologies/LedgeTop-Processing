"""Large-accounting regressions use invented rows, not large file transfers."""
import io,tempfile,unittest,uuid
from pathlib import Path
from processing.policy import plan
from processing.prototype import Queue
from processing.viewer_tasks import InputLimitExceeded,TaskStore,VIEWER_PROFILE

class LargeAdmissionTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);root=Path(self.tmp.name)
  self.queue=Queue(root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',));self.addCleanup(self.queue.close)
  self.queue.allowed_profiles.add(VIEWER_PROFILE);self.identifier=str(uuid.uuid4());self.inputs=root/'inputs'
 def store(self,limit):
  store=TaskStore(self.queue,self.inputs,max_input_bytes=limit);store.initialize(self.identifier,'synthetic',[],None,'production');return store
 def seed(self,count,size):
  with self.queue._db() as db:
   db.executemany('INSERT INTO viewer_files(uuid,name_key,name,path,bytes,sha256,is_photo) VALUES(?,?,?,?,?,?,1)',((self.identifier,'p%05d.jpg'%i,'p%05d.jpg'%i,'synthetic-%05d'%i,size,'a'*64) for i in range(count)))
 def test_10000_file_accounting_rejects_over_limit_and_prefers_main(self):
  size=5*1024**2;limit=9999*size+1;store=self.store(limit);self.seed(9999,size)
  store.upload(self.identifier,'last.jpg',io.BytesIO(b'x'),1)
  with self.queue._db() as db:
   row=db.execute('SELECT COUNT(*),SUM(bytes) FROM viewer_files WHERE uuid=?',(self.identifier,)).fetchone()
  self.assertEqual(tuple(row),(10000,limit))
  with self.assertRaises(InputLimitExceeded):store.upload(self.identifier,'over.jpg',io.BytesIO(b'x'),1)
  self.assertEqual(plan(10000,limit,{'ready':True},{'ready':True},1000,20*1024**3)['decision'],'main')
 def test_default_20_gib_cap_rejects_large_recorded_total(self):
  store=self.store(20*1024**3);self.seed(5000,5*1024**2)
  with self.assertRaises(InputLimitExceeded):store.upload(self.identifier,'over.jpg',io.BytesIO(b'x'),1)
if __name__=='__main__':unittest.main()
