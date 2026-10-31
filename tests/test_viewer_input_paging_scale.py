"""Bounded 10k metadata paging, not a 10k processing qualification."""
import hashlib,hmac,json,os,resource,sqlite3,subprocess,sys,tempfile,unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from processing import reference_protocol as p
from processing.viewer_node_inputs import ViewerNodeInputs,InputHeld

class InputPagingScaleTests(unittest.TestCase):
 def test_10000_mixed_objects_paging_restart_and_mutation(self):
  with tempfile.TemporaryDirectory() as root:
   path=Path(root)/'inventory.sqlite';db=sqlite3.connect(path);key=b'k'*32
   db.execute('CREATE TABLE policy(singleton INTEGER PRIMARY KEY,mac_key TEXT)');db.execute('INSERT INTO policy VALUES(1,?)',(key.hex(),))
   db.execute('CREATE TABLE objects(ordinal INTEGER PRIMARY KEY,object_id TEXT UNIQUE,entry TEXT,mac TEXT,phase TEXT,slot_key TEXT,slot_identity TEXT)')
   digest=hashlib.sha256();digest.update(b'[');photos=0
   for i in range(10000):
    photo=i%10!=0;photos+=photo;entry=dict(name=f'{i:05d}.'+('jpg' if photo else 'txt'),bytes=1,sha256=hashlib.sha256(b'x').hexdigest(),isPhoto=photo)
    object_id=p.digest(dict(ordinal=i,entry=entry));full=dict(ordinal=i,objectId=object_id,**entry);encoded=p.canonical_json(full).decode()
    mac=hmac.new(key,encoded.encode(),hashlib.sha256).hexdigest();db.execute('INSERT INTO objects VALUES(?,?,?,?,?,?,?)',(i,object_id,encoded,mac,'verified','slot','identity'))
    if i:digest.update(b',')
    digest.update(p.canonical_json(entry))
   digest.update(b']');db.commit();size=path.stat().st_size;db.close()
   self.assertLess(size,64*1024*1024)
   code='''import sqlite3,sys,resource
from contextlib import contextmanager
from unittest.mock import patch
from processing.viewer_node_inputs import ViewerNodeInputs
path,digest,photos=sys.argv[1:];db=sqlite3.connect(path)
obj=object.__new__(ViewerNodeInputs);obj.claim=dict(objectCount=10000,imageCount=int(photos),inputBytes=10000,inputInventorySha256=digest)
@contextmanager
def connect(self):yield db
with patch.object(ViewerNodeInputs,"_db",connect):
 obj._verify_inventory_rows(db,("verified",digest,10000,10000))
 count=sum(1 for entry,phase,key,identity in obj._verified_input_rows(0,10000))
 assert count==10000
 print("objects=%d maxrss_kib=%d"%(count,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))
db.close()
'''
   child=subprocess.run([sys.executable,'-c',code,str(path),digest.hexdigest(),str(photos)],capture_output=True,text=True,timeout=20)
   self.assertEqual(child.returncode,0,child.stderr);self.assertIn('objects=10000',child.stdout)
   print('metadata_db_bytes=%d '%size+child.stdout.strip(),flush=True)
   db=sqlite3.connect(path)
   @contextmanager
   def connect(_self):yield db
   obj=object.__new__(ViewerNodeInputs)
   try:
    with patch.object(ViewerNodeInputs,'_db',connect):
     iterator=obj._verified_input_rows(0,10000,page_size=100)
     for _ in range(100):next(iterator)
     db.execute("UPDATE objects SET entry='{}' WHERE ordinal=100");db.commit()
     with self.assertRaisesRegex(InputHeld,'input_metadata_changed'):next(iterator)
   finally:db.close()
