"""Fresh three-retained-job profile; no metadata/file retirement or new authority."""
from contextlib import contextmanager,closing
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from ops import bootstrap_worker_input_accounting as b
from ops import reference_worker_daemon as daemon
from processing import reference_indexed_fetch as j
from processing import reference_indexed_recovery as recovery
from processing import reference_protocol as p
from tests import test_reference_driver_input_copies as inputs
from tests import test_reference_driver_indexed_fetch as indexed_fixture
from tests import test_reference_indexed_fetch as journal_fixture
from tests import test_reference_worker_input_bootstrap as bootstrap_fixture
from tests import test_reference_indexed_recovery as recovery_fixture

PROFILE={'schemaVersion':3,'fetchJournal':{'schemaVersion':1,'policyId':j.THREE_POLICY_ID}}
V2={'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':j.POLICY_ID}}


class ThreeRetainedContractTests(unittest.TestCase):
    def test_v2_descriptor_is_exact_qualified_bytes_and_v3_is_detached(self):
        self.assertEqual(hashlib.sha256(p.canonical_json(j.compiled_profile_descriptor())).hexdigest(),
            '18dc06d78582538fc268ee93ed843fef0dd968c8353eafc5ac9b9c8ea454e0a5')
        old=j.compiled_profile_descriptor();new=j.compiled_profile_descriptor(j.THREE_POLICY_ID)
        self.assertEqual(old['schemaSha256'],new['schemaSha256']);self.assertNotEqual(old['policySha256'],new['policySha256'])
        self.assertEqual(new['limits']['databasePageCap']*4096,128*1024**2)
        self.assertEqual(new['limits']['physicalAllowanceBytes'],384*1024**2)
        self.assertEqual(new['limits']['retainedJobs'],3)
        new['limits']['retainedJobs']=100
        self.assertEqual(j.compiled_profile_descriptor(j.THREE_POLICY_ID)['limits']['retainedJobs'],3)
        self.assertEqual(j.compiled_profile_descriptor(),old)

    def test_closed_version_policy_pair_has_no_caller_capacity_override(self):
        self.assertTrue(b.indexed_profile({'inputCopyAccounting':PROFILE}))
        for flag in (dict(PROFILE,schemaVersion=True),dict(PROFILE,capacity=30),
                     dict(V2,fetchJournal=PROFILE['fetchJournal']),dict(PROFILE,fetchJournal=V2['fetchJournal'])):
            with self.subTest(flag=flag),self.assertRaises(ValueError):b.input_profile({'inputCopyAccounting':flag})
        binding={'inputCopyAccounting':PROFILE,'indexedFetchJournal':j.compiled_profile_descriptor(j.THREE_POLICY_ID)}
        self.assertEqual(j.policy_from_binding(binding).capacity,3)
        with self.assertRaises(ValueError):j.policy_from_binding(dict(binding,inputCopyAccounting=V2))
        with self.assertRaises(ValueError):j.policy_from_binding({'indexedFetchJournal':binding['indexedFetchJournal']})


@unittest.skipUnless(os.name=='posix','actual POSIX ownership and selected journal')
class ThreeRetainedDriverTests(unittest.TestCase):
    make_driver=indexed_fixture.IndexedDriverTests.make_driver
    def setUp(self):
        self.fixture=inputs.InputCopyDriverTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.f=self.fixture.f;self.downloads=self.fixture.downloads
        self.owner=daemon.DaemonStateLock(self.f.root).__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.path=self.f.root/'three-driver.sqlite'
        self.binding=dict(stateRoot=str(self.f.root),scratchRoot=str(self.f.scratch),inputCopyAccounting=copy.deepcopy(PROFILE),
            indexedFetchJournal=j.compiled_profile_descriptor(j.THREE_POLICY_ID),
            physical=dict(nodeId='cluster',workerId='worker',workerInstanceId='instance',authorityId=self.f.queue.authority_id,engineId='engine',gateGeneration=1),
            policy=dict(safetyFloor=1024))
        self.context=j.initialize_fresh_profile(self.path,bootstrap_id=str(uuid.uuid4()),binding=self.binding,owner=self.owner)
        self.f.ledger.probe=j.indexed_capacity_probe(self.context,self.owner,self.f.scratch)
        self.f.driver=self.make_driver()

    def accept(self,job,index):
        result=self.f.store.get(job['jobId'],private=True)['resultManifest']
        receipt=dict(schemaVersion=1,receiptId='viewer-'+str(index),storageReceiptId='storage-'+str(index),jobId=job['jobId'],generation=1,
            environment=job['environment'],inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],
            resultManifestSha256=result['resultManifestSha256'],acceptedObjectIds=[o['id'] for o in result['objects']],
            acceptedAt='2026-10-29T12:00:00.000Z',validationVersion='ltds-reference-v1')
        self.assertTrue(self.f.store.accept(job['jobId'],receipt)['fullyAccepted'])

    def test_three_queued_jobs_finish_in_production_priority_and_fourth_waits_without_deletion(self):
        staged=self.f.submit();production=self.f.submit(environment='production');third=self.f.submit()
        order=[production,staged,third]
        for job in order:self.f.advance(job,'awaiting_acceptance')
        self.assertEqual([self.f.queue.get(v['jobId'])['status'] for v in order],['succeeded']*3)
        self.assertEqual(len(self.downloads),3)
        self.assertEqual(sum(c[0]=='initialize' for c in self.f.transport.calls),3)
        with self.f.driver._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],3)
            self.assertEqual(db.execute('SELECT SUM(charge) FROM indexed_quota').fetchone()[0],3*j.JOB_CHARGE)
            self.assertEqual([r[0] for r in db.execute('SELECT job FROM driver_jobs ORDER BY created')],[v['jobId'] for v in order])
        retained={str(v.relative_to(self.f.scratch)):v.read_bytes() for v in self.f.scratch.rglob('*') if v.is_file()}
        for index,job in enumerate(order):self.accept(job,index)
        for _ in order:
            self.f.driver._indexed_background_due=0;self.f.driver.step()
        with self.f.driver._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM driver_jobs WHERE stage='completed'").fetchone()[0],3)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],3)
        fourth=self.f.submit();calls=[];request=self.f.driver._request
        def observed(method,suffix,*args,**kwargs):calls.append(suffix);return request(method,suffix,*args,**kwargs)
        with patch.object(self.f.driver,'_request',observed):outcome=self.f.driver.step()
        self.assertEqual(outcome,{'state':'held','reason':'indexed_metadata_quota_full'})
        self.assertNotIn('/claim',calls);self.assertEqual(self.f.queue.get(fourth['jobId'])['status'],'queued')
        self.assertIsNone(self.f.store.get(fourth['jobId'],private=True)['claimToken'])
        self.assertEqual(retained,{str(v.relative_to(self.f.scratch)):v.read_bytes() for v in self.f.scratch.rglob('*') if v.is_file()})
        self.assertIsNone(self.f.ledger.release_authority);self.assertEqual(self.f.ledger.input_cleanup_candidates(),[])

    def test_v3_shared_probe_and_admission_charge_384m_floor_outstanding_and_control_once(self):
        ledger=self.f.ledger;probe=ledger.probe
        allocated=getattr(self.path.stat(),'st_blocks',0)*512
        self.assertEqual(probe.remaining(),384*1024**2-allocated)
        ledger.reserve(job_id='retained',generation=1,reservation_id='held',manifest_sha256='a'*64,
            input_bytes=10,output_budget=1000,environment='staging',ttl_seconds=240,worker_ready=True)
        ledger.record_allocated('held',10)
        ledger.prepare_control_charge('held',charge_id='charge',scope_sha256='b'*64,parent_binding_sha256='c'*64,
            expected_allocated=10,allowance_bytes=100,maximum_scope_charges=1)
        with ledger._transaction() as db:
            outstanding=db.execute('SELECT SUM(remaining) FROM reservations').fetchone()[0]+db.execute('SELECT SUM(bytes) FROM control_charges').fetchone()[0]
        remaining=probe.remaining();threshold=remaining+ledger.floor+outstanding
        with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=threshold-1)):
            with self.assertRaisesRegex(ValueError,'disk'):
                with self.f.driver._db() as db:self.f.driver._indexed.reserve_slot(db,'next',ledger=ledger)
        with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=threshold)):
            with self.f.driver._db() as db:self.f.driver._indexed.reserve_slot(db,'next',ledger=ledger)
        with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=remaining+ledger.floor+99)):
            from processing.scratch_policy import PolicyConflict
            with self.assertRaises(PolicyConflict):ledger.reserve(job_id='later',generation=1,reservation_id='later',manifest_sha256='d'*64,
                input_bytes=50,output_budget=50,environment='staging',ttl_seconds=240,worker_ready=True)

    def test_restart_retains_three_sealed_results_without_changing_profile(self):
        jobs=[self.f.submit() for _ in range(3)]
        for job in jobs:self.f.advance(job,'awaiting_acceptance')
        before=self.path.read_bytes();context=j.read_only_profile(self.path,bootstrap_id=self.context.bootstrap_id,binding=self.binding,owner=self.owner)
        self.assertEqual(context,self.context);self.assertEqual(before,self.path.read_bytes())
        self.f.driver=self.make_driver()
        for job in jobs:self.assertIsNotNone(self.f.store.get(job['jobId'],private=True)['resultManifest'])
        self.assertEqual(self.f.driver.indexed_fetch_context.policy.capacity,3)
        with self.f.driver._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_claims').fetchone()[0],3)


@unittest.skipUnless(os.name=='posix','actual bootstrap/inode/SQLite ownership')
class ThreeRetainedBootstrapTests(unittest.TestCase):
    def setUp(self):
        bootstrap_fixture.InputBootstrapTests.setUp(self);self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)
    def snapshot(self):
        return {str(v.relative_to(self.root)):(v.stat().st_ino,hashlib.sha256(v.read_bytes()).hexdigest()) for v in self.root.rglob('*') if v.is_file()}
    def test_fresh_v3_bootstrap_is_idempotent_and_all_profile_flips_hold_before_clock(self):
        first=b.bootstrap_input_copy_accounting(self.config);before=self.snapshot()
        self.assertEqual(b.bootstrap_input_copy_accounting(self.config)['state'],'already_enabled');self.assertEqual(self.snapshot(),before)
        intent=b._file(self.state/b.DIRECTORY/'intent.json')[0]
        self.assertEqual(intent['binding']['indexedFetchJournal'],j.compiled_profile_descriptor(j.THREE_POLICY_ID))
        for flag in (V2,{'schemaVersion':1},None):
            config=dict(self.config)
            if flag is None:config.pop('inputCopyAccounting')
            else:config['inputCopyAccounting']=flag
            with self.subTest(flag=flag),patch.object(daemon,'ScratchLedger',side_effect=AssertionError('no constructor')):
                with self.assertRaises((ValueError,FileNotFoundError)):daemon.build_worker(config)
            self.assertEqual(self.snapshot(),before)
        self.assertEqual(first['state'],'enabled')

    def test_existing_v2_empty_and_retained_state_cannot_be_adopted(self):
        for retained in (False,True):
            config=dict(self.config,stateRoot=str(self.root/('v2-'+str(retained))),scratchRoot=str(self.root/('scratch-'+str(retained))),inputCopyAccounting=copy.deepcopy(V2))
            b.bootstrap_input_copy_accounting(config)
            path=Path(config['stateRoot'])/'driver.sqlite'
            if retained:
                with closing(sqlite3.connect(path)) as db,db:db.execute("INSERT INTO readiness_tasks VALUES('retained-control-history')")
            before=self.snapshot();candidate=dict(config,inputCopyAccounting=copy.deepcopy(PROFILE))
            with self.assertRaises(ValueError):b.bootstrap_input_copy_accounting(candidate)
            with daemon.DaemonStateLock(config['stateRoot']) as owner:
                with self.assertRaises(ValueError):b.require_worker_input_profile(candidate,owner)
            self.assertEqual(self.snapshot(),before)

    def test_fully_sealed_retained_v2_claim_refuses_v3_adoption_without_db_clock_change(self):
        self.config['inputCopyAccounting']=copy.deepcopy(V2);b.bootstrap_input_copy_accounting(self.config)
        intent=b._file(self.state/b.DIRECTORY/'intent.json')[0];bound=intent['binding'];path=self.state/'driver.sqlite'
        settings={**bound['physical'],'origin':bound['origins']['coordinatorOrigin'],'brokerOrigin':bound['origins']['brokerOrigin'],
            'engineOrigin':bound['origins']['engineOrigin'],'gateOrigin':bound['origins']['gateCoordinatorOrigin'],
            **{k:bound['policy'][k] for k in ('outputBudget','transferTtl','safetyFloor','environments')},
            'inputCopyAccounting':{'schemaVersion':1},'indexedFetchJournal':bound['indexedFetchJournal']}
        with daemon.DaemonStateLock(self.state) as owner:
            context=b.require_worker_indexed_context(self.config,owner)
            with closing(sqlite3.connect(path)) as db,db:db.execute('INSERT INTO driver_binding VALUES(1,?)',(p.canonical_json(settings).decode(),))
            journal=j.IndexedFetchJournal(path,context,owner,clock=lambda:100.)
            raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
            normalized=p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'])
            claim=dict(jobId='retained-sealed',status='running',phase='queued',nodeId='cluster',generation=1,environment='staging',
                manifestSha256=normalized['manifestSha256'],pipelineDigest=normalized['pipelineDigest'],claimToken='synthetic-owned-claim',submission=normalized)
            with closing(sqlite3.connect(path)) as db,db:
                journal.reserve_slot(db,claim['jobId']);db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,'retained-request','claimed','{}')",(claim['jobId'],))
                journal.seal_claim(db,claim['jobId'],'retained-request',claim)
                from processing.reference_driver import _INDEXED_FIXED_CLAIM,_INDEXED_MUTABLE_CLAIM
                projection={'claim':{k:v for k,v in claim.items() if k in _INDEXED_FIXED_CLAIM+_INDEXED_MUTABLE_CLAIM}}
                db.execute('UPDATE driver_jobs SET data=?',(p.canonical_json(projection).decode(),))
                recovery.validate_business_connection(db,bound)
            self.assertEqual(j.read_only_profile(path,bootstrap_id=context.bootstrap_id,binding=bound,owner=owner),context)
        before=self.snapshot();candidate=dict(self.config,inputCopyAccounting=copy.deepcopy(PROFILE))
        with self.assertRaises(ValueError):b.bootstrap_input_copy_accounting(candidate)
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('no constructor')):
            with self.assertRaises(ValueError):daemon.build_worker(candidate)
        self.assertEqual(self.snapshot(),before)

    def test_selected_health_can_read_valid_larger_v3_database_only_through_bound_context(self):
        b.bootstrap_input_copy_accounting(self.config)
        with daemon.DaemonStateLock(self.state) as owner:
            ctx=b.require_worker_indexed_context(self.config,owner);path=self.state/'driver.sqlite'
            # Legal freelist pages retain the exact schema and empty business state.
            with closing(sqlite3.connect(path)) as db,db:
                db.execute("INSERT INTO readiness_tasks VALUES(CAST(zeroblob(34*1024*1024) AS TEXT))")
                db.execute('DELETE FROM readiness_tasks')
            self.assertGreater(path.stat().st_size,64*1024**2)
            self.assertEqual(j.read_only_profile(path,bootstrap_id=ctx.bootstrap_id,binding=json.loads(ctx.binding_json),owner=owner),ctx)
            before=path.read_bytes()
            with self.assertRaises(ValueError):daemon._health_sql(path,'SELECT COUNT(*) FROM driver_jobs')
            self.assertEqual(daemon._health_sql(path,'SELECT COUNT(*) FROM driver_jobs',indexed_context=ctx),[(0,)])
            self.assertEqual(before,path.read_bytes())

    def test_actual_v3_dirty_page_recovery_preserves_inode_clock_and_selected_capsule(self):
        b.bootstrap_input_copy_accounting(self.config);self.path=self.state/'driver.sqlite';self.journal=Path(str(self.path)+'-journal')
        intent=b._file(self.state/b.DIRECTORY/'intent.json')[0];bound=intent['binding']
        settings={**bound['physical'],'origin':bound['origins']['coordinatorOrigin'],'brokerOrigin':bound['origins']['brokerOrigin'],
            'engineOrigin':bound['origins']['engineOrigin'],'gateOrigin':bound['origins']['gateCoordinatorOrigin'],
            **{k:bound['policy'][k] for k in ('outputBudget','transferTtl','safetyFloor','environments')},
            'inputCopyAccounting':{'schemaVersion':1},'indexedFetchJournal':bound['indexedFetchJournal']}
        with closing(sqlite3.connect(self.path)) as db,db:db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(settings),))
        self.initial=self.path.read_bytes();self.inode=self.path.stat().st_ino;ledger=(self.state/'scratch.sqlite').read_bytes()
        recovery_fixture.IndexedRecoveryTests.crash_writer(self)
        with daemon.DaemonStateLock(self.state) as owner:
            self.assertTrue(recovery.recover_indexed_driver(self.config,owner));self.assertTrue(b.require_worker_input_profile(self.config,owner))
        self.assertEqual(self.path.read_bytes(),self.initial);self.assertEqual(self.path.stat().st_ino,self.inode)
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),ledger);self.assertFalse(self.journal.exists())
        summary=recovery.inspect_completed_recovery_capsule(self.state,bootstrap_id=intent['bootstrapId'],binding_sha256=intent['bindingSha256'],
            root_identity=bound['stateRootIdentity'],driver_identity=b._identity(self.path.stat()),policy=j.policy_from_binding(bound))
        self.assertEqual(summary['phase'],'complete')


@unittest.skipUnless(os.name=='posix','actual simultaneous SQLite policy contexts')
class ThreeRetainedJournalTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);self.root=Path(temp.name);self.root.chmod(0o700)
    def make(self,name,profile):
        root=self.root/name;root.mkdir(mode=0o700);owner=daemon.DaemonStateLock(root).__enter__();self.addCleanup(owner.__exit__,None,None,None)
        binding=dict(stateRoot=str(root),scratchRoot=str(root/'scratch'),physical={},policy={'safetyFloor':0})
        if profile is not None:binding.update(inputCopyAccounting=profile,indexedFetchJournal=j.compiled_profile_descriptor(profile['fetchJournal']['policyId']))
        path=root/'driver.sqlite';ctx=j.initialize_fresh_profile(path,bootstrap_id=str(uuid.uuid4()),binding=binding,owner=owner)
        with closing(sqlite3.connect(path)) as db,db:db.execute('INSERT INTO driver_binding VALUES(1,?)',(p.canonical_json(dict(inputCopyAccounting={'schemaVersion':1},indexedFetchJournal=j.compiled_profile_descriptor(ctx.policy.policy_id))).decode(),))
        return path,ctx,owner,j.IndexedFetchJournal(path,ctx,owner,clock=lambda:100.)
    def test_simultaneous_v2_and_v3_preserve_separate_caps_and_policy_mismatch_is_readonly(self):
        a,ac,ao,aj=self.make('v2',None);z,zc,zo,zj=self.make('v3',copy.deepcopy(PROFILE))
        with closing(sqlite3.connect(a)) as db,db:aj.reserve_slot(db,'one')
        with closing(sqlite3.connect(a)) as db,db:
            with self.assertRaisesRegex(ValueError,'quota'):aj.reserve_slot(db,'two')
        with closing(sqlite3.connect(z)) as db,db:
            for job in ('one','two','three'):zj.reserve_slot(db,job)
            with self.assertRaisesRegex(ValueError,'quota'):zj.reserve_slot(db,'four')
        self.assertEqual(ac.policy.capacity,1);self.assertEqual(zc.policy.capacity,3)
        before=a.read_bytes()
        changed=dict(json.loads(ac.binding_json),inputCopyAccounting=PROFILE,indexedFetchJournal=j.compiled_profile_descriptor(j.THREE_POLICY_ID))
        with self.assertRaises(ValueError):j.read_only_profile(a,bootstrap_id=ac.bootstrap_id,binding=changed,owner=ao)
        self.assertEqual(a.read_bytes(),before)


    @unittest.skipUnless(Path('/dev/shm').is_dir(),'separate Linux tmpfs required')
    def test_v3_separate_device_scratch_does_not_subtract_state_physical_allowance(self):
        path,ctx,owner,journal=self.make('separate-state',copy.deepcopy(PROFILE))
        with tempfile.TemporaryDirectory(dir='/dev/shm') as tmp:
            scratch=Path(tmp);self.assertNotEqual(scratch.stat().st_dev,path.parent.stat().st_dev)
            binding=dict(json.loads(ctx.binding_json),scratchRoot=str(scratch));other=path.parent/'separate-driver.sqlite'
            otherctx=j.initialize_fresh_profile(other,bootstrap_id=str(uuid.uuid4()),binding=binding,owner=owner)
            probe=j.indexed_capacity_probe(otherctx,owner,scratch)
            self.assertGreater(probe.remaining(),300*1024**2)
            with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=123456)):
                self.assertEqual(probe(scratch),123456)

    def test_three_maximum_metadata_slots_fit_prepaid_bound_without_global_policy_changes(self):
        # This is the same physical SQLite worst-case oracle as v2, repeated
        # three times. It grants no claim/custody/cleanup authority.
        path,ctx,owner,journal=self.make('worst-three',copy.deepcopy(PROFILE))
        low,high=0,200
        while low<high:
            mid=(low+high+1)//2
            try:claim=journal_fixture.IndexedJournalTests.claim(self,10000,mid,job_id='j'*128);fits=len(p.canonical_json(claim))<=p.MAX_BODY_BYTES-16384
            except p.ProtocolError:fits=False
            if fits:low=mid
            else:high=mid-1
        claims=[]
        for number in range(3):
            job='j'*127+str(number);claim=journal_fixture.IndexedJournalTests.claim(self,10000,low,job_id=job)
            with closing(sqlite3.connect(path)) as db,db:
                db.row_factory=sqlite3.Row;journal.reserve_slot(db,job)
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'fetching','{}')",(job,'request-'+str(number)))
                journal.seal_claim(db,job,'request-'+str(number),claim)
                request=dict(reservationId='r'*127+str(number),generation=1,expiresAt='2030-01-01T00:00:00.000Z',manifestSha256=claim['manifestSha256'],byteBudget=30000,nonce='n'*128)
                journal.seal_reservation(db,job,request)
                grant=dict(request,jobId=job,environment='staging',objectIds=[o['id'] for o in claim['submission']['manifest']['objects']],token='x'*4096)
                journal.seal_grant(db,job,grant)
                attempts=[(job,seq,attempt,uuid.uuid4().hex,'confirmed' if attempt==(3 if seq<5 else 1) else 'started')
                    for seq in range(10000) for attempt in range(1,4 if seq<5 else 2)]
                db.executemany('INSERT INTO indexed_attempts VALUES(?,?,?,?,?)',attempts)
                db.execute('UPDATE indexed_fetch SET next=10000,bytes=10000,attempts=10010,revision=20010 WHERE job=?',(job,))
                db.execute('UPDATE driver_jobs SET data=? WHERE job=?',(json.dumps({'bounded':'x'*8000}),job))
            claims.append(dict(claim,transferRequest=request))
        with closing(sqlite3.connect(path)) as db:
            db.row_factory=sqlite3.Row
            for claim in claims:journal.validate_cold(db,claim['jobId'],claim)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_objects').fetchone()[0],30000)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_attempts').fetchone()[0],30030)
            pages=db.execute('PRAGMA page_count').fetchone()[0]
        self.assertLessEqual(pages*4096,3*j.JOB_CHARGE+j.NON_JOB_ALLOWANCE)
        self.assertLessEqual(path.stat().st_size,ctx.policy.database_cap)
        print(json.dumps(dict(oracle='three-retained-physical-metadata',jobs=3,objects=30000,attempts=30030,
            databaseBytes=pages*4096,prepaidBytes=3*j.JOB_CHARGE+j.NON_JOB_ALLOWANCE,pageCapBytes=ctx.policy.database_cap)),flush=True)


@unittest.skipUnless(os.name=='posix','actual canonical factory/TLS/gate/census')
class ThreeRetainedMaintenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_worker_maintenance import WorkerMaintenanceTests
        WorkerMaintenanceTests.setUpClass();cls.addClassCleanup(WorkerMaintenanceTests.doClassCleanups)
    def setUp(self):
        from tests.test_reference_worker_maintenance import WorkerMaintenanceTests
        from ops.node_ingress import save
        self.f=WorkerMaintenanceTests();original=self.f.start_worker
        def start():
            self.f.config['inputCopyAccounting']=copy.deepcopy(getattr(self,'profile',PROFILE))
            if not self.f.worker_state.exists():save(self.f.config_path,self.f.config);b.bootstrap_input_copy_accounting(self.f.config)
            original()
        self.f.start_worker=start;self.addCleanup(self.f.doCleanups);self.f.setUp()
    def test_actual_v3_factory_owned_mount_census_and_history_are_selected_and_readonly(self):
        from ops import reference_worker_maintenance as m
        from ops import reference_worker_indexed_maintenance as indexed
        self.assertEqual(self.f.driver.indexed_fetch_context.policy.capacity,3)
        c=m.WorkerMaintenance(self.f.managed,self.f.policy,self.f.api,runner=self.f.runner);self.addCleanup(c.close)
        c.mounts();self.f.stop_worker();c._owner=daemon.DaemonStateLock(self.f.worker_state).__enter__()
        context=indexed.stopped_context(c)
        before={str(v):hashlib.sha256(v.read_bytes()).hexdigest() for v in self.f.worker_state.rglob('*') if v.is_file()}
        self.assertEqual(indexed.indexed_database_cap(context,self.f.worker_state),128*1024**2)
        census=m.offline_census(self.f.worker_state,indexed_context=context)
        self.assertEqual(m.immutable_history(self.f.worker_state,indexed_context=context)['driver.sqlite'],census['driver.sqlite']['__indexed_history__'])
        self.assertEqual(before,{str(v):hashlib.sha256(v.read_bytes()).hexdigest() for v in self.f.worker_state.rglob('*') if v.is_file()})
    def test_actual_v3_factory_update_restart_preserves_distinct_policy(self):
        from dataclasses import replace,asdict
        from ops import reference_worker_maintenance as m
        from ops.node_ingress import save
        self.f.policy=replace(self.f.policy,timeout_seconds=30)
        profile=m.read_json(self.f.managed/'maintenance-worker-v2.json');profile['worker']=asdict(self.f.policy);save(self.f.managed/'maintenance-worker-v2.json',profile)
        result,folder=self.f.run_update();self.assertEqual(result['status'],'succeeded')
        self.assertEqual(self.f.events,['stop','backend','start','open'])
        self.assertEqual(self.f.driver.indexed_fetch_context.policy.capacity,3)

    def test_populated_three_job_actual_coordinator_worker_proxy_acceptance_and_maintenance_history(self):
        import io,ssl,time
        from urllib.request import Request,build_opener,ProxyHandler,HTTPSHandler
        from ops import reference_worker_maintenance as m
        from ops import reference_worker_indexed_maintenance as indexed
        tls=ssl.create_default_context(cafile=str(self.f.cert));opener=build_opener(ProxyHandler({}),HTTPSHandler(context=tls))
        def call(method,path,value=None,binary=False):
            request=Request(self.f.config['coordinatorOrigin']+path,data=None if value is None else p.canonical_json(value),method=method,
                headers={'Authorization':'Bearer '+'synthetic-viewer-'+'c'*32,'Content-Type':'application/json','Accept-Encoding':'identity'})
            with opener.open(request,timeout=5) as reply:
                body=reply.read(4*1024**2+1);self.assertLessEqual(len(body),4*1024**2)
                return (body,reply.headers) if binary else json.loads(body)
        count=getattr(self,'job_count',3)
        caps=call('GET','/ltds/v1/capabilities');contents={};granted=set();accepted=set();jobs=[]
        copies=self.f.root/'synthetic-viewer-copies';copies.mkdir(mode=0o700)
        def broker_open(request,timeout):
            data=contents[request.full_url.rsplit('/',1)[1]];response=io.BytesIO(data);response.status=200
            response.headers={'Content-Length':str(len(data))};response.geturl=lambda:request.full_url;return response
        self.f.driver.broker._open=broker_open
        for number in range(count):
            raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
            raw['submissionId']=str(uuid.uuid4());obj=raw['manifest']['objects'][0];obj['id']='synthetic-input-'+str(number)
            contents[obj['id']]=b'abc';raw['capabilityFingerprint']=caps['capabilityFingerprint']
            raw['pipeline']['odm']['optionsFingerprint']=caps['engines']['odm']['optionsFingerprint'];raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
            jobs.append(call('POST','/ltds/v1/jobs',raw))
        deadline=time.monotonic()+35
        while time.monotonic()<deadline:
            for number,job in enumerate(jobs):
                path='/ltds/v1/jobs/'+job['jobId'];public=call('GET',path)
                if public['phase']=='awaiting_transfer' and job['jobId'] not in granted:
                    grant=dict(public['transferRequest'],jobId=job['jobId'],environment='staging',objectIds=['synthetic-input-'+str(number)],token='synthetic-broker-grant')
                    call('POST',path+'/transfer-grants',grant);granted.add(job['jobId'])
                if public['phase']=='awaiting_acceptance' and job['jobId'] not in accepted:
                    manifest=call('GET',path+'/results')
                    for obj in manifest['objects']:
                        body,headers=call('GET',path+'/results/'+obj['id'],binary=True)
                        self.assertEqual(len(body),obj['byteSize']);self.assertEqual(hashlib.sha256(body).hexdigest(),obj['sha256'])
                        with (copies/(job['jobId']+'-'+obj['id'])).open('xb') as durable:
                            durable.write(body);durable.flush();os.fsync(durable.fileno())
                        descriptor=os.open(copies,os.O_RDONLY|os.O_DIRECTORY)
                        try:os.fsync(descriptor)
                        finally:os.close(descriptor)
                    receipt=dict(schemaVersion=1,receiptId='three-viewer-'+str(number),storageReceiptId='three-storage-'+str(number),jobId=job['jobId'],generation=job['generation'],
                        environment='staging',inputManifestSha256=job['manifestSha256'],pipelineDigest=job['pipelineDigest'],resultManifestSha256=manifest['resultManifestSha256'],
                        acceptedObjectIds=[obj['id'] for obj in manifest['objects']],acceptedAt='2026-10-29T12:00:00.000Z',validationVersion='ltds-reference-v1')
                    self.assertTrue(call('POST',path+'/acceptance',receipt)['fullyAccepted']);accepted.add(job['jobId'])
            with self.f.driver._db() as db:completed=db.execute("SELECT COUNT(*) FROM driver_jobs WHERE stage='completed'").fetchone()[0]
            if completed==count:break
            self.f.stop.wait(.03)
        self.assertEqual(completed,count);self.assertEqual(len(accepted),count)
        self.assertEqual(len(list(self.f.scratch.glob('jobs/*/*.input'))),count)
        self.assertEqual(len(list(self.f.scratch.glob('jobs/*/all.zip'))),count)
        c=m.WorkerMaintenance(self.f.managed,self.f.policy,self.f.api,runner=self.f.runner);self.addCleanup(c.close)
        c.mounts();self.f.stop_worker();c._owner=daemon.DaemonStateLock(self.f.worker_state).__enter__();context=indexed.stopped_context(c)
        before={str(v):v.read_bytes() for v in self.f.worker_state.rglob('*') if v.is_file()}
        census=m.offline_census(self.f.worker_state,indexed_context=context)
        self.assertEqual(m.immutable_history(self.f.worker_state,indexed_context=context)['driver.sqlite'],census['driver.sqlite']['__indexed_history__'])
        self.assertEqual(before,{str(v):v.read_bytes() for v in self.f.worker_state.rglob('*') if v.is_file()})

        # No test fabricates an ownership record: a cached UUID is accepted
        # only above after the real worker delivered and Viewer acknowledged it.
        foreign=str(uuid.uuid4());owned=jobs[0]['jobId']
        for values in ([foreign],[owned,foreign],[None]):
            with closing(sqlite3.connect(self.f.worker_state/'driver.sqlite')) as db,db:
                db.execute('DELETE FROM readiness_tasks');db.executemany('INSERT INTO readiness_tasks VALUES(?)',[(v,) for v in values])
            before={str(v):v.read_bytes() for v in self.f.worker_state.rglob('*') if v.is_file()}
            with self.assertRaisesRegex(ValueError,'indexed_readiness_task_unresolved'):m.offline_census(self.f.worker_state,indexed_context=context)
            self.assertEqual(before,{str(v):v.read_bytes() for v in self.f.worker_state.rglob('*') if v.is_file()})


@unittest.skipUnless(os.name=='posix','actual completed v2 readiness cache compatibility')
class V2ReadinessCacheCompatibilityTests(unittest.TestCase):
    profile=V2
    job_count=1
    @classmethod
    def setUpClass(cls):ThreeRetainedMaintenanceTests.setUpClass.__func__(cls)
    setUp=ThreeRetainedMaintenanceTests.setUp
    test_completed_v2_cache_is_audited_and_foreign_or_malformed_cache_held=ThreeRetainedMaintenanceTests.test_populated_three_job_actual_coordinator_worker_proxy_acceptance_and_maintenance_history

if __name__=='__main__':unittest.main()
