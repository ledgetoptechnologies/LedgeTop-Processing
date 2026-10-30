"""Narrow, synthetic viewer_tasks migration census coverage."""
import hashlib,os,sqlite3,tempfile,unittest
from pathlib import Path
from ops import state_census as c
KEY=b'k'*32
DDL="CREATE TABLE viewer_tasks (\n                uuid TEXT PRIMARY KEY, generation INTEGER NOT NULL DEFAULT 1,\n                directory TEXT NOT NULL, name TEXT NOT NULL, options TEXT NOT NULL,\n                outputs TEXT NOT NULL, environment TEXT NOT NULL,\n                stage TEXT NOT NULL DEFAULT 'uploading', phase TEXT NOT NULL DEFAULT 'uploading',\n                job_id TEXT, node_id TEXT, engine_uuid TEXT, claim_token TEXT,\n                error TEXT, progress REAL NOT NULL DEFAULT 0,\n                output_lines TEXT NOT NULL DEFAULT '[]', archive_path TEXT,\n                removed INTEGER NOT NULL DEFAULT 0,\n                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,\n                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
class ViewerTaskProjectionTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.path=self.root/'jobs.sqlite3'
  self.db=sqlite3.connect(self.path);self.db.execute(DDL)
  self.db.execute("insert into viewer_tasks(uuid,directory,name,options,outputs,environment,stage) values ('one','one-1','synthetic','[]','null','production','succeeded')")
  self.db.commit();self.db.close();self.before=c.census(self.root,KEY)
  self.db=sqlite3.connect(self.path);self.db.execute('ALTER TABLE viewer_tasks ADD COLUMN option_evidence_required INTEGER NOT NULL DEFAULT 0 CHECK(option_evidence_required IN (0,1))');self.db.commit()
  self.expected='table:viewer_tasks=a637e43b58241be981e0c056d8e6fd7d4a83e7c590598f837f6fccfdcf77d424'
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def compare(self):
  self.db.commit();after=c.census(self.root,KEY)
  return c.compare(self.before,after,KEY,allow_paths=['.:mtime','jobs.sqlite3:bytes,content,mtime'],allow_objects=[self.expected],allow_viewer_task_default_column=True)
 def test_original_row_and_zero_default_pass(self):self.assertTrue(self.compare()['identical'])
 def test_changed_original_value_fails(self):
  self.db.execute("update viewer_tasks set name='altered'");self.assertFalse(self.compare()['identical'])
 def test_deleted_row_fails(self):
  self.db.execute('delete from viewer_tasks');self.assertFalse(self.compare()['identical'])
 def test_added_row_fails(self):
  self.db.execute("insert into viewer_tasks(uuid,directory,name,options,outputs,environment) values ('two','two-1','synthetic','[]','null','production')");self.assertFalse(self.compare()['identical'])
 def test_nonzero_default_fails(self):
  self.db.execute('update viewer_tasks set option_evidence_required=1');self.assertFalse(self.compare()['identical'])
 def test_extra_column_fails(self):
  self.db.execute('alter table viewer_tasks add column extra text');self.assertFalse(self.compare()['identical'])
 def test_broad_and_narrow_are_incompatible(self):
  self.db.commit();after=c.census(self.root,KEY)
  with self.assertRaises(SystemExit):c.compare(self.before,after,KEY,allow_tables=['viewer_tasks'],allow_viewer_task_default_column=True)
 def test_older_census_without_projection_fails_closed(self):
  self.db.commit();after=c.census(self.root,KEY);del self.before['database']['viewerTaskProjection']
  result=c.compare(self.before,after,KEY,allow_paths=['.:mtime','jobs.sqlite3:bytes,content,mtime'],allow_objects=[self.expected],allow_viewer_task_default_column=True)
  self.assertFalse(result['identical'])
 def rewrite_schema(self,old,new):
  self.db.execute('PRAGMA writable_schema=ON')
  schema=self.db.execute("select sql from sqlite_master where name='viewer_tasks'").fetchone()[0]
  self.assertIn(old,schema)
  self.db.execute("update sqlite_master set sql=? where name='viewer_tasks'",(schema.replace(old,new),))
  self.db.commit();self.db.execute('PRAGMA writable_schema=OFF')
 def test_wrong_default_schema_fails(self):
  self.rewrite_schema('DEFAULT 0 CHECK(option_evidence_required IN (0,1))','DEFAULT 1 CHECK(option_evidence_required IN (0,1))');self.assertFalse(self.compare()['identical'])
 def test_wrong_type_schema_fails(self):
  self.rewrite_schema('option_evidence_required INTEGER','option_evidence_required TEXT');self.assertFalse(self.compare()['identical'])
 def test_missing_constraint_schema_fails(self):
  self.rewrite_schema(' CHECK(option_evidence_required IN (0,1))','');self.assertFalse(self.compare()['identical'])
 def test_wal_mode_is_rejected_even_if_census_otherwise_matches(self):
  self.db.commit();self.assertEqual(self.db.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
  self.db.execute("update viewer_tasks set name='new'");self.db.commit()
  result=self.compare()
  self.assertFalse(result['identical'])
  self.assertTrue(any(x.get('change')=='requiresCheckpointedDatabase' for x in result['database']))
 def test_hidden_generated_column_fails(self):
  self.db.execute('alter table viewer_tasks add column computed TEXT GENERATED ALWAYS AS (name) VIRTUAL');self.assertFalse(self.compare()['identical'])
 def test_rehearsal_discovery_with_preserved_nonempty_history(self):
  self.db.execute('CREATE TABLE viewer_reservations (uuid TEXT PRIMARY KEY)')
  self.db.execute('CREATE TABLE viewer_option_evidence (uuid TEXT PRIMARY KEY)')
  self.db.execute(c.LEASE_MARKER_DDL);self.db.execute('INSERT INTO coordinator_authority_lease VALUES (1,1)')
  self.db.commit();after=c.census(self.root,KEY)
  fixed=dict(allow_paths=['.:mtime','jobs.sqlite3:bytes,content,mtime'],
      allow_new_tables=['viewer_reservations','viewer_option_evidence'],allow_lease_marker=True,
      allow_viewer_task_default_column=True)
  discovery=c.compare(self.before,after,KEY,allow_objects=[self.expected],**fixed)
  self.assertTrue(all('object' in x for x in discovery['database']),discovery)
  names=['table:viewer_tasks']+[x['object'] for x in discovery['database']]
  allowed=c.make_schema_allowlist(after,names)['objects']
  final=c.compare(self.before,after,KEY,allow_objects=[k+'='+v for k,v in allowed.items()],**fixed)
  self.assertTrue(final['identical'],final)
  self.db.execute("update viewer_tasks set name='changed'");self.db.commit()
  changed=c.compare(self.before,c.census(self.root,KEY),KEY,allow_objects=[k+'='+v for k,v in allowed.items()],**fixed)
  self.assertFalse(changed['identical'])
 def test_feature_absent_does_not_pass(self):
  self.db.commit();after=c.census(self.root,KEY)
  result=c.compare(self.before,after,KEY,allow_paths=['.:mtime','jobs.sqlite3:bytes,content,mtime'],allow_objects=[self.expected])
  self.assertFalse(result['identical'])
if __name__=='__main__':unittest.main()
