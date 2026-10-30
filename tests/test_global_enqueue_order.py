import copy, hashlib, io, json, tempfile, unittest, uuid, sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from processing import reference_protocol as p
from processing.prototype import Queue, PROFILE
from processing.reference_store import ReferenceStore, PROFILE as REFERENCE_PROFILE
from processing.viewer_tasks import TaskStore, VIEWER_PROFILE
from processing.reference_manifest_v2 import DOMAIN
from processing.reference_v2_metadata_ledger import V2MetadataLedger
from processing.global_enqueue_order import GlobalOrder
from processing.maintenance import Conflict

class GlobalOrderTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name);self.q=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True);self.q.allowed_profiles.add(REFERENCE_PROFILE)
  self.f=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
 def tearDown(self):self.q.close();self.tmp.cleanup()
 def legacy(self,request,environment='staging'):return self.q.submit(dict(requestId=request,environment=environment,profile=PROFILE))[0]['id']
 def v2(self,ledger,environment='staging'):
  raw=copy.deepcopy(self.f['submission']);raw['schemaVersion']=2;raw['submissionId']=str(uuid.uuid4());m=raw['manifest'];m['sha256']=hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in m.items() if k!='sha256'})).hexdigest()
  return ledger.submit(raw,principal='producer',environment=environment,capability_fingerprint=raw['capabilityFingerprint'],allowed_odm_options=set(raw['pipeline']['odm']['options']))['jobId']
 def test_migration_and_all_enqueue_paths_with_vacuum_restart(self):
  old1=self.legacy('old1');old2=self.legacy('old2','production');order=GlobalOrder(self.q,enabled=True)
  ledger=V2MetadataLedger(self.q,enabled=True);v2=self.v2(ledger);legacy=self.legacy('new')
  raw=copy.deepcopy(self.f['submission']);raw['submissionId']=str(uuid.uuid4());store=ReferenceStore(self.q);r,_=store.submit(p.validate_submission(raw,environment='production',capability_fingerprint=raw['capabilityFingerprint']))
  viewer=TaskStore(self.q,self.root/'inputs');identifier=str(uuid.uuid4());viewer.initialize(identifier,'Test',[],['all.zip'],'staging');viewer.upload(identifier,'test.jpg',io.BytesIO(b'abc'),3);viewer.commit(identifier);viewerid=viewer.task(identifier)['job_id']
  entries=order.snapshot();byseq=sorted(entries,key=lambda e:e['seq']);self.assertEqual([x['job_id'] for x in byseq],[old1,old2,v2,legacy,r['jobId'],viewerid])
  self.assertEqual([x['environment'] for x in entries],['production','production','staging','staging','staging','staging'])
  with self.q._db() as db:db.execute('VACUUM')
  self.assertEqual(order.snapshot(),entries)
  self.q.close();self.q=Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True);self.q.allowed_profiles.add(REFERENCE_PROFILE);order=GlobalOrder(self.q,enabled=True);self.assertEqual(order.snapshot(),entries)
 def test_existing_v2_refuses_ambiguous_migration(self):
  ledger=V2MetadataLedger(self.q,enabled=True);self.v2(ledger)
  with self.assertRaises(Conflict):GlobalOrder(self.q,enabled=True)
  with self.q._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='global_enqueue_order'").fetchone())
 def test_concurrent_atomic_enqueues(self):
  order=GlobalOrder(self.q,enabled=True);ledger=V2MetadataLedger(self.q,enabled=True)
  with ThreadPoolExecutor(max_workers=2) as pool:
   a=pool.submit(self.legacy,'concurrent');b=pool.submit(self.v2,ledger);ids={a.result(),b.result()}
  self.assertEqual({e['job_id'] for e in order.snapshot()},ids)
 def test_order_is_atomic_and_replace_refused(self):
  order=GlobalOrder(self.q,enabled=True);job=self.legacy('one');entries=order.snapshot()
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("INSERT OR REPLACE INTO global_enqueue_order(seq,kind,job_id,environment) VALUES(100,'v1',?,'staging')",(job,))
  with self.assertRaises(RuntimeError):
   with self.q._db() as db:
    db.execute('BEGIN IMMEDIATE');db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('rollback','rollback','staging',?,'queued')",(PROFILE,));raise RuntimeError('rollback')
  self.assertEqual(order.snapshot(),entries)
 def test_v2_promotion_reuses_original_order(self):
  order=GlobalOrder(self.q,enabled=True);ledger=V2MetadataLedger(self.q,enabled=True);v2=self.v2(ledger)
  before=order.snapshot()
  with self.q._db() as db:db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'running')",(v2,'v2:'+v2,'staging','reference-pipeline-v2'))
  self.assertEqual(order.snapshot(),before)
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('wrong','wrong','staging','reference-pipeline-v2','running')")
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("UPDATE jobs SET profile=? WHERE id=?",(PROFILE,v2))
 def test_cross_kind_collision_and_deleted_v2_cannot_promote(self):
  GlobalOrder(self.q,enabled=True);ledger=V2MetadataLedger(self.q,enabled=True);v1=self.legacy('first');v2=self.v2(ledger)
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("INSERT INTO reference_v2_staged_jobs(job_id,submission_id,principal,environment,request_json,request_sha) VALUES(?,?,?,?,?,?)",(v1,str(uuid.uuid4()),'producer','staging','{}','a'*64))
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'running')",(v2,'v1-collision','staging',PROFILE))
  with self.q._db() as db:db.execute('DELETE FROM reference_v2_staged_jobs WHERE job_id=?',(v2,))
  with self.assertRaises(sqlite3.IntegrityError):
   with self.q._db() as db:db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'running')",(v2,'v2:'+v2,'staging','reference-pipeline-v2'))
 def test_indexed_pending_pages_skip_ten_thousand_tombstones(self):
  order=GlobalOrder(self.q,enabled=True);ledger=V2MetadataLedger(self.q,enabled=True)
  with self.q._db() as db:
   db.execute('BEGIN IMMEDIATE')
   db.executemany("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging',?,'queued')",[(f'tomb-{i}',f'tomb-{i}',PROFILE) for i in range(10001)])
   db.execute("DELETE FROM jobs WHERE id LIKE 'tomb-%'")
  staging=self.legacy('staging-new');v2=self.v2(ledger,'production');production=self.legacy('production-new','production')
  one=order.pending_page(limit=1);self.assertEqual(one['entries'][0]['job_id'],v2)
  two=order.pending_page(limit=1,after=one['after']);self.assertEqual(two['entries'][0]['job_id'],production)
  three=order.pending_page(limit=1,after=two['after']);self.assertEqual(three['entries'][0]['job_id'],staging)
  self.assertEqual(order.pending_page(limit=1,after=three['after'])['entries'],[])
  with self.q._db() as db:
   plan=db.execute("EXPLAIN QUERY PLAN SELECT seq FROM global_enqueue_order WHERE environment=? AND seq>? ORDER BY seq LIMIT ?",('staging',0,10)).fetchall()
   self.assertTrue(any('global_order_by_environment' in str(tuple(row)) for row in plan))
 def test_promoted_queued_v2_remains_visible_once(self):
  order=GlobalOrder(self.q,enabled=True);ledger=V2MetadataLedger(self.q,enabled=True);v2=self.v2(ledger,'production')
  with self.q._db() as db:db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,?,?,'queued')",(v2,'v2:'+v2,'production','reference-pipeline-v2'))
  entries=order.pending_page()['entries'];self.assertEqual([e['job_id'] for e in entries],[v2])
 def test_missing_trigger_detected(self):
  order=GlobalOrder(self.q,enabled=True)
  with self.q._db() as db:db.execute('DROP TRIGGER global_order_v1_insert')
  with self.assertRaises(Conflict):order.snapshot()

if __name__=='__main__':unittest.main()
