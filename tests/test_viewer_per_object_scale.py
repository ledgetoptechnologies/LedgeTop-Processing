"""Bounded local filesystem scale test; no daemon, network, or client data."""
import os,resource,tempfile,unittest
from unittest.mock import patch
from processing.direct_output_slot import _identity
from processing.viewer_node_inputs import ViewerNodeInputs,InputHeld

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class PerObjectLayoutTests(unittest.TestCase):
 def test_10000_actual_slots_have_bounded_marker_scans(self):
  with tempfile.TemporaryDirectory(dir='/tmp') as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:identity=_identity(fd)
   finally:os.close(fd)
   inputs=object.__new__(ViewerNodeInputs);inputs.policy={'inputRoot':root,'inputRootIdentity':identity,'maxObject':32}
   real=os.scandir;scans=[0,0]
   class Counting:
    def __init__(self,fd):self.inner=real(fd);scans[0]+=1
    def __enter__(self):self.inner.__enter__();return self
    def __exit__(self,*args):return self.inner.__exit__(*args)
    def __iter__(self):
     for e in self.inner:scans[1]+=1;yield e
   with patch('processing.direct_output_slot.os.scandir',Counting):
    for i in range(10000):
     ident=f'{i:064x}';slot=inputs._input_slot(ident,ident,1,create=True)
     slot.write_at(0,b'x');slot.close()
   self.assertEqual(scans[0],20000);self.assertLessEqual(scans[1],20000)
   self.assertLess(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,128*1024)
   print('10k allocated synthetic slots; marker scans=%d scanned entries=%d maxrss_kib=%d'%(*scans,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),flush=True)
 def test_missing_replaced_and_ambiguous_directories_hold(self):
  import json
  with tempfile.TemporaryDirectory(dir='/tmp') as root:
   os.chmod(root,0o700);fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:root_id=_identity(fd)
   finally:os.close(fd)
   inputs=object.__new__(ViewerNodeInputs);inputs.policy={'inputRoot':root,'inputRootIdentity':root_id,'maxObject':32}
   ident='a'*64;slot=inputs._input_slot(ident,ident,1,create=True)
   recorded=json.dumps(dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta)))
   slot.close();path=os.path.join(root,'object-'+ident);os.rename(path,path+'-retained');os.mkdir(path,0o700)
   with self.assertRaises(Exception):inputs._input_slot(ident,ident,1,create=False,recorded=recorded)
   with self.assertRaises(InputHeld):inputs._input_slot(ident,ident,1,create=True)

 def test_paging_10000_objects_and_gap_hold(self):
  import sqlite3
  from contextlib import contextmanager
  from processing.viewer_synthetic_supervisor import _object_ids,SupervisorHeld
  db=sqlite3.connect(':memory:');self.addCleanup(db.close)
  db.execute('CREATE TABLE objects(ordinal INTEGER PRIMARY KEY,object_id TEXT)')
  db.executemany('INSERT INTO objects VALUES(?,?)',((i,f'{i:064x}') for i in range(10000)))
  class Inputs:
   @contextmanager
   def _db(self):yield db
  count=0
  for item in _object_ids(Inputs(),10000):self.assertEqual(item,f'{count:064x}');count+=1
  self.assertEqual(count,10000)
  db.execute('DELETE FROM objects WHERE ordinal=5000');db.execute('INSERT INTO objects VALUES(10000,?)',('a'*64,))
  with self.assertRaises(SupervisorHeld):list(_object_ids(Inputs(),10000))
