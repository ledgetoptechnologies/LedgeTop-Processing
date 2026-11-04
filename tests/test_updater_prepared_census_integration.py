import inspect, json, os, sqlite3, subprocess, sys, tempfile, unittest
from pathlib import Path
from ops import update_reference_coordinator as u
from processing.prototype import Queue
from processing import reference_store as r

DDL=(r._ACTIVATION_SCHEMA,r._OWNER_SCHEMA,r._SCHEMA_MARKER,r._ENVELOPE_V2_SCHEMA,r._V3_COPY_HOLD_SCHEMA,r._V3_POLICY_SCHEMA)

@unittest.skipIf(os.name=='nt','Linux serialized census')
class PreparedUpdaterCensusTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.queue=Queue(self.root);self.addCleanup(self.queue.close)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=?',(u.AUTHORITY,))
            db.execute('CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY, viewer_receipt_required INTEGER)')
            db.execute('CREATE TABLE viewer_tasks(uuid TEXT PRIMARY KEY, phase TEXT, stage TEXT, full_option_evidence_required INTEGER DEFAULT 0, node_guarded INTEGER DEFAULT 0)')
        (self.root/'viewer-tasks').mkdir();(self.root/'viewer-spool').mkdir()
    def census(self):return u.volume_census(self.root,uid=os.geteuid())
    def add(self):
        with self.queue._db() as db:
            for statement in DDL:db.execute(statement)
            db.execute('INSERT INTO reference_viewer_activation_schema VALUES(1,1)')
    def test_exact_empty_forward_only_and_serialized_helper(self):
        before=self.census();self.add();after=self.census()
        with self.assertRaises(u.Stopped):u.compatible(after,before)
        u.compatible(after,before,allow_prepared_forward=True)
        with self.assertRaises(u.Stopped):u.compatible(before,after,allow_prepared_forward=True)
        changed=json.loads(json.dumps(after));changed['schema'].append(['table','extra','extra','CREATE TABLE extra(x)'])
        with self.assertRaises(u.Stopped):u.compatible(changed,before,allow_prepared_forward=True)
        changed=json.loads(json.dumps(after));changed['tables']['reference_viewer_v3_policy']['sha256']='0'*64
        with self.assertRaises(u.Stopped):u.compatible(changed,before,allow_prepared_forward=True)
        code=inspect.getsource(u.volume_census)+'\nimport json,os\nprint(json.dumps(volume_census('+repr(str(self.root))+',uid=os.geteuid())))'
        result=subprocess.run([sys.executable,'-c',u.explicit_guard_source(code)],capture_output=True,text=True,timeout=10,check=True)
        self.assertEqual(json.loads(result.stdout),after)
    def test_partial_nonempty_wrong_marker_and_unknown_table_hold(self):
        with self.queue._db() as db:db.execute(DDL[0])
        with self.assertRaises(AssertionError):self.census()
        with self.queue._db() as db:db.execute('DROP TABLE reference_viewer_activation')
        self.add()
        with self.queue._db() as db:db.execute("INSERT INTO reference_viewer_v3_policy VALUES('test','{}','bad')")
        with self.assertRaises(AssertionError):self.census()
        with self.queue._db() as db:db.execute('DELETE FROM reference_viewer_v3_policy');db.execute('DELETE FROM reference_viewer_activation_schema')
        with self.assertRaises(AssertionError):self.census()
        with self.queue._db() as db:db.execute('INSERT INTO reference_viewer_activation_schema VALUES(1,1)');db.execute('CREATE TABLE unknown_foreign(id)')
        with self.assertRaises(AssertionError):self.census()
