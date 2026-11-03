import hashlib, io, sqlite3, tempfile, unittest
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_batch_identity_candidate import Identity
from processing.viewer_batch_intent_candidate import initialize
from processing.maintenance import Conflict

class IdentityTest(unittest.TestCase):
 def test_scoped_fresh_owner_and_generation(self):
  with tempfile.TemporaryDirectory() as root:
   q=Queue(Path(root)/'q')
   try:
    s=TaskStore(q,Path(root)/'in');i=Identity(s,[{'token':'a'*32,'principalId':'a','environment':'production'},{'token':'b'*32,'principalId':'b','environment':'production'},{'token':'c'*32,'principalId':'a','environment':'staging'}],['z'*32],q.authority_id);i.initialize();initialize(s)
    task='11111111-2222-4333-8444-555555555555';cap='A'*43
    with self.assertRaises(Conflict):i.reserve_fresh(task,cap,'a'*32,False)
    i.reserve_fresh(task,cap,'a'*32,True);i.reserve_fresh(task,cap,'a'*32,True)
    with q._db() as db:
     db.execute('PRAGMA recursive_triggers=OFF')
     with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO held_viewer_owner_v1 VALUES(?,?,?,?)',(task,'b','production',q.authority_id))
    s.initialize(task,'x',[],None,'production',cap)
    files=[{'name':'a.jpg','bytes':3,'sha256':hashlib.sha256(b'abc').hexdigest()}]
    i.prepare(task,cap,'a'*32,True,1,'batch',files)
    for token in ('b'*32,'c'*32,'z'*32):
     with self.assertRaises(Conflict):i.prepare(task,cap,token,True,1,'batch',files)
    with self.assertRaises(Conflict):i.upload(task,cap,'a'*32,True,2,'batch','a.jpg',io.BytesIO(b'abc'),3)
    i.upload(task,cap,'a'*32,True,1,'batch','a.jpg',io.BytesIO(b'abc'),3)
   finally:q.close()
