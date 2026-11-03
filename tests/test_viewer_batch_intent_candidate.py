import hashlib, io, tempfile, unittest
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_batch_intent_candidate import initialize, prepare, upload_held
from processing.maintenance import Conflict

class BatchIntentTest(unittest.TestCase):
 def test_durable_intent_partial_and_changed_bytes(self):
  with tempfile.TemporaryDirectory() as tmp:
   q=Queue(Path(tmp)/'q'); task='11111111-2222-4333-8444-555555555555'; cap='A'*43
   try:
    s=TaskStore(q,Path(tmp)/'in');s.reserve(task,'production',cap);s.initialize(task,'x',[],None,'production',cap);initialize(s)
    files=[{'name':n,'bytes':3,'sha256':hashlib.sha256(v).hexdigest()} for n,v in [('a.jpg',b'abc'),('b.jpg',b'def')]]
    prepare(s,task,'production',cap,1,'batch1',files)
    upload_held(s,task,'production',cap,1,'batch1','a.jpg',io.BytesIO(b'abc'),3)
    with self.assertRaises(Conflict):upload_held(s,task,'production',cap,1,'batch1','b.jpg',io.BytesIO(b'xyz'),3)
    q.close();q=Queue(Path(tmp)/'q');s=TaskStore(q,Path(tmp)/'in');initialize(s)
    prepare(s,task,'production',cap,1,'batch1',files)
    for name,value in [('a.jpg',b'abc'),('b.jpg',b'def')]:upload_held(s,task,'production',cap,1,'batch1',name,io.BytesIO(value),3)
    changed=[dict(x) for x in files];changed[0]['sha256']=hashlib.sha256(b'xyz').hexdigest()
    with self.assertRaises(Conflict):prepare(s,task,'production',cap,1,'batch1',changed)
    with self.assertRaises(Conflict):upload_held(s,task,'production',cap,2,'batch1','a.jpg',io.BytesIO(b'abc'),3)
   finally:q.close()
