import copy, hashlib, json, tempfile, unittest
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from processing import reference_protocol as p
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE
from processing.reference_manifest_v2 import DOMAIN
from processing.reference_input_budget_v2 import plan_input_budget
from processing.reference_v2_metadata_ledger import V2MetadataLedger

class V2LedgerTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.queue=Queue(self.tmp.name,node_ids=('cluster','node-2'));self.queue.allowed_profiles.add(PROFILE)
  self.ledger=V2MetadataLedger(self.queue,enabled=True);self.f=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
  self.raw=copy.deepcopy(self.f['submission']);self.raw['schemaVersion']=2;m=self.raw['manifest'];o=m['objects'][0]
  m['objects']=[dict(o,id=f'image-{i}',name=f'image-{i}.jpg') for i in range(10001)]
  m['objectCount']=10001;m['totalBytes']=30003;m['inputCounts']['images']=10001
  m['sha256']=hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in m.items() if k!='sha256'})).hexdigest()
  self.kw=dict(principal='producer',environment='staging',capability_fingerprint=self.raw['capabilityFingerprint'],allowed_odm_options=set(self.raw['pipeline']['odm']['options']))
 def tearDown(self): self.queue.close();self.tmp.cleanup()
 def test_disabled(self):
  with self.assertRaises(Conflict):V2MetadataLedger(self.queue)
 def test_large_submission_identity_claim_reservation_restart(self):
  first=self.ledger.submit(self.raw,**self.kw);self.assertEqual(self.ledger.submit(self.raw,**self.kw)['jobId'],first['jobId'])
  altered=copy.deepcopy(self.raw);altered['pipeline']['odm']['optionsFingerprint']='c'*64
  with self.assertRaises(Conflict):self.ledger.submit(altered,**self.kw)
  with self.assertRaises(Conflict):self.ledger.claim_metadata(first['jobId'],principal='other',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
  claim=self.ledger.claim_metadata(first['jobId'],principal='producer',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
  m=self.raw['manifest'];plan=dict(protocolVersion=2,jobId=first['jobId'],generation=1,reservationId='reserve',manifestSha256=m['sha256'],environment='staging',capabilityFingerprint=self.raw['capabilityFingerprint'],expiresAt='2030-01-01T00:00:00.000Z',objects=[{k:o[k] for k in ('id','byteSize','sha256')} for o in m['objects']],rangeBytes=16,retryRequests=0,retryBytes=0)
  calc=plan_input_budget(plan);env={k:calc[k] for k in ('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes','requestBudget','byteBudget')};env['nonce']='nonce'
  kw=dict(principal='producer',claim_token=claim['claimToken'],clock=lambda:1000)
  bound=self.ledger.bind_reservation(first['jobId'],env,plan,**kw);self.assertEqual(bound,self.ledger.bind_reservation(first['jobId'],env,plan,**kw))
  snap=self.ledger.snapshot_bytes(first['jobId'],**kw);self.assertEqual(snap['manifestBytes'],p.canonical_json(m));self.assertEqual(snap['planBytes'],p.canonical_json(plan));self.assertEqual(snap['planSha256'],p.digest(plan));self.assertEqual(snap['reservationBytes'],p.canonical_json(env))
  with self.assertRaises(Conflict):self.ledger.snapshot_bytes(first['jobId'],**dict(kw,clock=lambda:1893456000))
  with self.assertRaises(Conflict):self.ledger.snapshot_bytes(first['jobId'],**kw)
  with self.assertRaises(Conflict):self.ledger.bind_reservation(first['jobId'],env,plan,**kw)
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT high FROM reference_v2_clock WHERE singleton=1').fetchone()[0],1893456000)
  with self.assertRaises(Conflict):self.ledger.snapshot_bytes(first['jobId'],principal='producer',claim_token='wrong',clock=lambda:1000)
  for column in ('plan_sha','reservation_sha'):
   with self.queue._db() as db:
    old=db.execute(f'SELECT {column} FROM reference_v2_staged_jobs WHERE job_id=?',(first['jobId'],)).fetchone()[0]
    db.execute(f'UPDATE reference_v2_staged_jobs SET {column}=? WHERE job_id=?',('0'*64,first['jobId']))
   with self.assertRaises(Conflict):self.ledger.snapshot_bytes(first['jobId'],**kw)
   with self.queue._db() as db:db.execute(f'UPDATE reference_v2_staged_jobs SET {column}=? WHERE job_id=?',(old,first['jobId']))
  bad=dict(kw,claim_token='wrong')
  with self.assertRaises(Conflict):self.ledger.bind_reservation(first['jobId'],env,plan,**bad)
  self.queue.close();self.queue=Queue(self.tmp.name,node_ids=('cluster','node-2'));self.queue.allowed_profiles.add(PROFILE);self.ledger=V2MetadataLedger(self.queue,enabled=True)
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT high FROM reference_v2_clock WHERE singleton=1').fetchone()[0],1893456000)
  with self.assertRaises(Conflict):self.ledger.bind_reservation(first['jobId'],env,plan,**kw)
  with self.assertRaises(Conflict):self.ledger.snapshot_bytes(first['jobId'],**kw)
  with self.queue._db() as db:self.assertEqual(db.execute('SELECT reservation_sha FROM reference_v2_staged_jobs').fetchone()[0],bound['reservationSha256'])
 def test_concurrent_submission_and_principal_conflict(self):
  with ThreadPoolExecutor(max_workers=2) as pool:
   results=list(pool.map(lambda _:self.ledger.submit(self.raw,**self.kw),range(2)))
  self.assertEqual(results[0]['jobId'],results[1]['jobId'])
  self.assertEqual(sum(x['created'] for x in results),1)
  with self.assertRaises(Conflict):self.ledger.submit(self.raw,**dict(self.kw,principal='other'))
 def test_persisted_corruption_fails_closed(self):
  job=self.ledger.submit(self.raw,**self.kw)
  with self.queue._db() as db:db.execute("UPDATE reference_v2_staged_jobs SET request_sha=? WHERE job_id=?",('0'*64,job['jobId']))
  with self.assertRaises(Conflict):self.ledger.claim_metadata(job['jobId'],principal='producer',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
 def test_empty_nonnull_json_fields_fail_closed(self):
  job=self.ledger.submit(self.raw,**self.kw)
  for field in ('claim_json','plan_json','reservation_json'):
   with self.queue._db() as db:db.execute(f'UPDATE reference_v2_staged_jobs SET {field}=? WHERE job_id=?',('',job['jobId']))
   with self.subTest(field=field),self.assertRaises(Conflict):self.ledger.claim_metadata(job['jobId'],principal='producer',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
   with self.queue._db() as db:db.execute(f'UPDATE reference_v2_staged_jobs SET {field}=NULL WHERE job_id=?',(job['jobId'],))
 def test_oversized_corrupt_plan_is_rejected_before_read(self):
  job=self.ledger.submit(self.raw,**self.kw)
  with self.queue._db() as db:db.execute('UPDATE reference_v2_staged_jobs SET plan_json=zeroblob(?) WHERE job_id=?',(16*1024*1024+1,job['jobId']))
  with self.assertRaises(Conflict):self.ledger.claim_metadata(job['jobId'],principal='producer',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
 def test_unexpected_schema_trigger_fails_closed(self):
  job=self.ledger.submit(self.raw,**self.kw)
  with self.queue._db() as db:db.execute('CREATE TRIGGER unexpected AFTER UPDATE ON reference_v2_staged_jobs BEGIN SELECT 1; END')
  with self.assertRaises(Conflict):self.ledger.claim_metadata(job['jobId'],principal='producer',node_id='node-2',worker_id='worker',instance_id='instance',request_id='claim')
 def test_v2_staging_does_not_block_v1_production_claim(self):
  self.ledger.submit(self.raw,**self.kw)
  store=ReferenceStore(self.queue);raw=self.f['submission'];sub=p.validate_submission(raw,environment='production',capability_fingerprint=raw['capabilityFingerprint'])
  job,_=store.submit(sub);claim=store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='v1-claim')
  self.assertIsNotNone(claim)

if __name__=='__main__':unittest.main()

