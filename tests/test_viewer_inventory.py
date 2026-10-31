import base64, hashlib, io, json, tempfile, unittest, uuid
from pathlib import Path
from urllib.parse import urlencode
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore, VIEWER_PROFILE
from processing.viewer_http import ViewerFacade
from tests.test_viewer_http import MemoryHandler, STAGING, TOKEN

class InventoryTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup); root=Path(self.tmp.name)
  self.q=Queue(root/'db',node_ids=('cluster','main'),treedar_node_ids=('main',));self.addCleanup(self.q.close);self.q.allowed_profiles.add(VIEWER_PROFILE)
  self.s=TaskStore(self.q,root/'inputs',inventory_enabled=True);self.id=str(uuid.uuid4());self.cap=base64.urlsafe_b64encode(bytes(range(32))).decode().rstrip('=')
  self.s.reserve(self.id,'staging',self.cap);self.s.initialize(self.id,'test',[],None,'staging',self.cap)
 def inv(self,cursor=None):return self.s.input_inventory(self.id,'staging',self.cap,cursor)
 def add(self,name,body=b'x'):self.s.upload(self.id,name,io.BytesIO(body),len(body),'staging',self.cap)
 def test_auth_and_metadata(self):
  self.add('a.jpg'); result=self.inv();self.assertEqual(result['files'][0]['state'],'recorded');self.assertEqual(result['files'][0]['sha256'],hashlib.sha256(b'x').hexdigest());self.assertEqual(result['semantics'],'recorded-metadata-only')
  for env,cap in [('production',self.cap),('staging',None),('staging','wrong')]:
   with self.assertRaises(KeyError):self.s.input_inventory(self.id,env,cap)
 def test_pages_and_changed_snapshot(self):
  with self.q._db() as db:
   for i in range(101):
    n=f'{i:03d}.jpg';db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)',(self.id,n,n,'input-x',1,'a'*64,1))
  first=self.inv();self.assertEqual(len(first['files']),100);self.assertEqual(len(self.inv(first['nextCursor'])['files']),1)
  with self.q._db() as db:db.execute('DELETE FROM viewer_files WHERE uuid=? AND name_key=?',(self.id,'100.jpg'))
  with self.assertRaises(Conflict):self.inv(first['nextCursor'])
 def test_pending_and_invalid_identity(self):
  with self.q._db() as db:
   row=db.execute('SELECT * FROM viewer_tasks WHERE uuid=?',(self.id,)).fetchone()
   db.execute('INSERT INTO viewer_upload_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(self.id,row['generation'],row['directory'],'staging','p.jpg','p.jpg','.incoming-x','input-x',1,'a'*64,1,'1','2'))
  self.assertEqual(self.inv()['files'][0]['state'],'pending_intent')
  with self.q._db() as db:db.execute('UPDATE viewer_upload_intents SET generation=generation+1')
  with self.assertRaises(Conflict):self.inv()
 def test_generation_change_invalidates_cursor(self):
  with self.q._db() as db:
   for i in range(101):
    n=f'{i:03d}.jpg';db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)',(self.id,n,n,'input-x',1,'a'*64,1))
  cursor=self.inv()['nextCursor']
  with self.q._db() as db:db.execute('UPDATE viewer_tasks SET generation=generation+1 WHERE uuid=?',(self.id,))
  with self.assertRaises(Conflict):self.inv(cursor)
 def test_revision_detects_aba(self):
  with self.q._db() as db:
   for i in range(101):
    n=f'{i:03d}.jpg';db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)',(self.id,n,n,'input-x',1,'a'*64,1))
  first=self.inv()
  with self.q._db() as db:db.execute('UPDATE viewer_files SET bytes=2 WHERE uuid=? AND name_key=?',(self.id,'100.jpg'))
  with self.q._db() as db:db.execute('UPDATE viewer_files SET bytes=1 WHERE uuid=? AND name_key=?',(self.id,'100.jpg'))
  with self.assertRaises(Conflict):self.inv(first['nextCursor'])
 def test_restart_invalidates_cursor_and_tampered_schema_is_rejected(self):
  with self.q._db() as db:
   for i in range(101):
    n=f'{i:03d}.jpg';db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)',(self.id,n,n,'input-x',1,'a'*64,1))
  cursor=self.inv()['nextCursor'];reopened=TaskStore(self.q,self.s.root,inventory_enabled=True)
  with self.assertRaises(Conflict):reopened.input_inventory(self.id,'staging',self.cap,cursor)
  with self.q._db() as db:db.execute('DROP TRIGGER viewer_inventory_viewer_files_insert')
  with self.assertRaises(Conflict):self.inv()
  with self.assertRaises(Conflict):TaskStore(self.q,self.s.root,inventory_enabled=True)
 def test_facade_environment_capability_and_read_only(self):
  self.add('a.jpg')
  class Service:
   def __init__(inner,s):inner.s=s
   def authorize(inner,identifier,environment,capability=None):
    inner.s.authorize_reservation(identifier,environment,capability)
    if inner.s.task_environment(identifier)!=environment:raise KeyError('unknown task')
   def input_inventory(inner,*args):return inner.s.input_inventory(*args)
  f=ViewerFacade(Service(self.s),TOKEN,STAGING,inventory_enabled=True)
  def get(token,cap=None):
   h=MemoryHandler('/task/'+self.id+'/ledgetop/input-inventory?'+urlencode({'token':token}),method='GET',**({'X_LedgeTop_Capability':cap} if cap else {}));f.handle(h);return h
  self.assertEqual(get(STAGING,self.cap).status,200);self.assertEqual(get(STAGING).status,404);self.assertEqual(get(TOKEN,self.cap).status,404)
  self.assertEqual(self.inv()['files'][0]['name'],'a.jpg')
  f.inventory_enabled=False;self.assertEqual(get(STAGING,self.cap).status,404)
 def test_legacy_inventory_is_not_exposed(self):
  identifier=str(uuid.uuid4());self.s.initialize(identifier,'legacy',[],None,'staging')
  with self.assertRaises(KeyError):self.s.input_inventory(identifier,'staging')
 def test_inconsistent_photo_flag_or_empty_record_is_rejected(self):
  self.add('a.jpg')
  with self.q._db() as db:db.execute('UPDATE viewer_files SET is_photo=0 WHERE uuid=?',(self.id,))
  with self.assertRaises(Conflict):self.inv()
  with self.q._db() as db:db.execute('UPDATE viewer_files SET is_photo=1,bytes=0 WHERE uuid=?',(self.id,))
  with self.assertRaises(Conflict):self.inv()
 def test_disabled_store_does_not_install_inventory_schema(self):
  root=Path(self.tmp.name)/'other';root.mkdir();q=Queue(root/'db',node_ids=('cluster','main'),treedar_node_ids=('main',));self.addCleanup(q.close)
  store=TaskStore(q,root/'inputs')
  with q._db() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'viewer_inventory_%'").fetchone()[0],0)
  with self.assertRaises(Conflict):store.input_inventory(self.id,'staging',self.cap)
if __name__=='__main__':unittest.main()
