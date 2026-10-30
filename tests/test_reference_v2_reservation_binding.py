import copy, hashlib, json, tempfile, unittest, threading, uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from processing import reference_protocol as p
from processing.prototype import Queue
from processing.maintenance import Conflict
from processing.admission_drain import AdmissionDrain
from processing.reference_store import ReferenceStore, PROFILE
from processing.reference_manifest_v2 import DOMAIN
from processing.reference_input_budget_v2 import plan_input_budget

class ReservationBindingTests(unittest.TestCase):
    def setUp(self):
        self.dir=tempfile.TemporaryDirectory(); self.q=Queue(self.dir.name,node_ids=('cluster','node-2')); self.q.allowed_profiles.add(PROFILE)
        self.store=ReferenceStore(self.q,enable_v2_binding=True)
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        raw=self.fixture['submission'];sub=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'])
        self.job,_=self.store.submit(sub)
        self.claim=self.store.claim(self.job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-1')
        self.reservation=copy.deepcopy(self.fixture['transferRequest']);self.store.reserve(self.job['jobId'],self.reservation,node_id='cluster')
        self.manifest=copy.deepcopy(raw['manifest']); self.manifest['sha256']=hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in self.manifest.items() if k!='sha256'})).hexdigest()
        self.plan=dict(protocolVersion=2,jobId=self.job['jobId'],generation=1,reservationId=self.reservation['reservationId'],manifestSha256=self.manifest['sha256'],environment='staging',capabilityFingerprint=raw['capabilityFingerprint'],expiresAt=self.reservation['expiresAt'],objects=[{k:o[k] for k in ('id','byteSize','sha256')} for o in self.manifest['objects']],rangeBytes=16,retryRequests=0,retryBytes=0)
        calculated=plan_input_budget(self.plan)
        self.envelope={k:calculated[k] for k in ('protocolVersion','jobId','generation','reservationId','manifestSha256','environment','capabilityFingerprint','expiresAt','rangeBytes','requestBudget','byteBudget')};self.envelope['nonce']=self.reservation['nonce']
        self.owner=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_token=self.claim['claimToken'],clock=lambda:1000)
    def tearDown(self):
        self.q.close();self.dir.cleanup()
    def bind(self): return self.store.bind_v2_reservation(self.job['jobId'],self.envelope,self.manifest,self.plan,**self.owner)
    def test_default_disabled(self):
        disabled=ReferenceStore(self.q)
        with self.assertRaises(Conflict): disabled.bind_v2_reservation(self.job['jobId'],self.envelope,self.manifest,self.plan,**self.owner)
    def test_durable_idempotent_and_restart_holds(self):
        with ThreadPoolExecutor(max_workers=2) as pool: outputs=list(pool.map(lambda _:self.bind(),range(2)))
        self.assertEqual(outputs[0],outputs[1])
        with self.q._db() as db:
            record=db.execute('SELECT binding_json,binding_sha FROM reference_v2_bindings').fetchone(); self.assertEqual(record['binding_sha'],outputs[0]['bindingSha256'])
            stored=json.loads(record['binding_json']);self.assertEqual(stored['v1ManifestSha256'],self.job['manifestSha256']);self.assertEqual(stored['envelope']['manifestSha256'],self.manifest['sha256'])
        self.q.close();self.q=Queue(self.dir.name,node_ids=('cluster','node-2'));self.q.allowed_profiles.add(PROFILE);self.store=ReferenceStore(self.q,enable_v2_binding=True)
        with self.q._db() as db: self.assertEqual(db.execute('SELECT binding_sha FROM reference_v2_bindings').fetchone()[0],outputs[0]['bindingSha256'])
        with self.assertRaises(Conflict): self.bind()
    def test_wrong_owner_expiry_and_binding_fail(self):
        for key,value in [('claim_token','wrong'),('worker_id','other'),('node_id','node-2'),('worker_instance_id','other'),('clock',lambda:2e9)]:
            original=self.owner[key];self.owner[key]=value
            with self.subTest(key=key),self.assertRaises(Conflict): self.bind()
            self.owner[key]=original
        for key,value in [('nonce','other'),('environment','production'),('requestBudget',2),('byteBudget',2)]:
            original=self.envelope[key];self.envelope[key]=value
            with self.subTest(key=key),self.assertRaises(Conflict): self.bind()
            self.envelope[key]=original
        with self.q._db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_v2_bindings').fetchone()[0],0)
    def test_expiry_during_locked_writer_does_not_commit(self):
        called = threading.Event()
        current = [1000]
        def clock():
            called.set()
            return current[0]
        self.owner['clock'] = clock
        with ThreadPoolExecutor(max_workers=1) as pool:
            with self.q._db() as db:
                db.execute('BEGIN IMMEDIATE')
                pending = pool.submit(self.bind)
                self.assertTrue(called.wait(5))
                current[0] = 2e9
                db.commit()
            with self.assertRaises(Conflict): pending.result(timeout=10)
        with self.q._db() as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_v2_bindings').fetchone()[0],0)
    def test_pending_and_closed_gate_block_new_and_replay(self):
        gate=AdmissionDrain(self.q);self.q.admission_drain=gate
        gate._closing.set()
        try:
            with self.assertRaises(Conflict): self.bind()
        finally: gate._closing.clear()
        first=self.bind()
        gate._closing.set()
        try:
            with self.assertRaises(Conflict): self.bind()
        finally: gate._closing.clear()
        state=gate.status()
        gate.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=state['generation'],expected_closed=False,expected_authority=self.q.authority_id)
        with self.assertRaises(Conflict): self.bind()
        with self.q._db() as db: self.assertEqual(db.execute('SELECT binding_sha FROM reference_v2_bindings').fetchone()[0],first['bindingSha256'])
    def test_changed_persisted_full_manifest_rejected(self):
        self.manifest['objects'][0]['name']='different.JPG'
        self.manifest['sha256']=hashlib.sha256(DOMAIN+p.canonical_json({k:v for k,v in self.manifest.items() if k!='sha256'})).hexdigest()
        self.plan['manifestSha256']=self.manifest['sha256'];self.envelope['manifestSha256']=self.manifest['sha256']
        with self.assertRaises(Conflict): self.bind()

if __name__=='__main__': unittest.main()
