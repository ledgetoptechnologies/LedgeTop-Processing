import os, sqlite3, subprocess, tempfile, unittest
from pathlib import Path
from processing.prototype import Queue, _require_idle_state
from processing.viewer_tasks import TaskStore
from tests.test_state_census import _run

FLAGS = Path(__file__).resolve().parents[1] / 'ops' / 'cutover_census_flags.sh'

class InventoryRehearsalTests(unittest.TestCase):
 def flags(self, intents='0', inventory='0'):
  env=os.environ.copy();env['CUTOVER_REHEARSAL_UPLOAD_INTENTS']=intents;env['CUTOVER_REHEARSAL_INVENTORY']=inventory
  p=subprocess.run(['bash','-c','set -e; source "$1"; printf "%s\\n" "${CENSUS_FEATURE_FLAGS[@]}"','--',str(FLAGS)],env=env,capture_output=True,text=True)
  return p.returncode,[line for line in p.stdout.splitlines() if line]
 def test_opt_in_selection_and_invalid_values(self):
  self.assertEqual(self.flags(),(0,[]))
  self.assertEqual(self.flags('1','1'),(0,['--allow-empty-upload-intents','--allow-inventory-revision']))
  self.assertNotEqual(self.flags('2','0')[0],0);self.assertNotEqual(self.flags('0','true')[0],0)
 def test_real_synthetic_migration_census_and_pending_hold(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);q=Queue(root);TaskStore(q,root/'viewer-tasks');q.close()
   status,before=_run(['census',str(root)]);self.assertEqual(status,0)
   q=Queue(root);TaskStore(q,root/'viewer-tasks',inventory_enabled=True);q.close()
   _require_idle_state(root/'jobs.sqlite3')
   status,after=_run(['census',str(root)]);self.assertEqual(status,0)
   import json
   before_path=root.parent/(root.name+'-before.json');after_path=root.parent/(root.name+'-after.json')
   try:
    before_path.write_text(json.dumps(before));after_path.write_text(json.dumps(after))
    base=['compare',str(before_path),str(after_path),'--allow-path','jobs.sqlite3:bytes,content,mtime','--allow-path','.:mtime']
    self.assertEqual(_run(base)[0],1)
    code,flags=self.flags('1','1');self.assertEqual(code,0)
    self.assertEqual(_run(base+flags)[0],0)
    with sqlite3.connect(root/'jobs.sqlite3') as db:
     db.execute("INSERT INTO viewer_upload_intents VALUES ('synthetic',1,'dir','production','a.jpg','a.jpg','tmp','file',1,?,1,'1','1')",('a'*64,))
    with self.assertRaises(RuntimeError):_require_idle_state(root/'jobs.sqlite3')
    _,pending=_run(['census',str(root)]);after_path.write_text(json.dumps(pending))
    self.assertEqual(_run(base+flags)[0],1)
   finally:
    before_path.unlink(missing_ok=True);after_path.unlink(missing_ok=True)
 def test_pr66_shaped_upgrade_creates_only_exact_allowed_schema(self):
  import json
  with tempfile.TemporaryDirectory() as temp:
   parent=Path(temp);root=parent/'state';root.mkdir()
   q=Queue(root);TaskStore(q,root/'viewer-tasks');q.close()
   with sqlite3.connect(root/'jobs.sqlite3') as db:db.execute('DROP TABLE viewer_upload_intents')
   _,before=_run(['census',str(root)])
   q=Queue(root);TaskStore(q,root/'viewer-tasks',inventory_enabled=True);q.close()
   _require_idle_state(root/'jobs.sqlite3')
   _,after=_run(['census',str(root)])
   a=parent/'before.json';b=parent/'after.json';a.write_text(json.dumps(before));b.write_text(json.dumps(after))
   base=['compare',str(a),str(b),'--allow-path','jobs.sqlite3:bytes,content,mtime','--allow-path','.:mtime']
   self.assertEqual(_run(base)[0],1)
   self.assertEqual(_run(base+self.flags('1','1')[1])[0],0)
if __name__=='__main__':unittest.main()
