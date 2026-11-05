import concurrent.futures
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import sqlite3
import hashlib
import json
import copy
import os
import importlib.util
from contextlib import closing

from processing.scratch_policy import FENCED_LEASE, LeaseLapsed, MAX_INTEGER, ScratchLedger, PolicyConflict


class ScratchPolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.free = 1000
        self.ledger = self.open()

    def open(self):
        return ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
            capacity_probe=lambda _: self.free, clock=lambda: self.now,
            safety_floor_bytes=100, cleanup_grace_seconds=10)

    def reserve(self, **kwargs):
        values = dict(job_id='j1', generation=1, reservation_id='r1',
            manifest_sha256='a'*64, input_bytes=100, output_budget=300,
            environment='production', ttl_seconds=20, worker_ready=True)
        values.update(kwargs)
        return self.ledger.reserve(**values)

    @unittest.skipIf(os.name == 'nt', 'POSIX directory permissions')
    def test_job_directories_are_private_under_permissive_umask(self):
        self.reserve()
        previous = os.umask(0o002)
        try:
            directory = self.ledger.prepare_job_directory('r1')
        finally:
            os.umask(previous)
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(directory.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.ledger.prepare_job_directory('r1'), directory)

    @unittest.skipIf(os.name == 'nt', 'POSIX directory permissions')
    def test_job_directory_refuses_existing_other_writers_without_chmod(self):
        self.reserve()
        parent = self.root / 'jobs'
        parent.mkdir(mode=0o700)
        parent.chmod(0o775)
        with self.assertRaises(PolicyConflict):
            self.ledger.prepare_job_directory('r1')
        self.assertEqual(parent.stat().st_mode & 0o777, 0o775)
        self.assertFalse((parent / 'r1').exists())

    @unittest.skipIf(os.name == 'nt', 'POSIX nofollow directory traversal')
    def test_job_directory_refuses_symlink_and_unreserved_name(self):
        self.reserve()
        with self.assertRaises(PolicyConflict):
            self.ledger.prepare_job_directory('unreserved')
        self.assertFalse((self.root / 'jobs').exists())
        target = self.root / 'other'
        target.mkdir(mode=0o700)
        (self.root / 'jobs').symlink_to(target, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.ledger.prepare_job_directory('r1')
        self.assertEqual(list(target.iterdir()), [])
        with self.assertRaises(ValueError):
            self.ledger.prepare_job_directory('../other')

    def register(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger.register_result('r1', job_id='j1', generation=1,
            manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, objects=[
                dict(object_id='o1', bytes=100, sha256='d'*64, relative_path='jobs/r1/o1.bin'),
                dict(object_id='o2', bytes=150, sha256='e'*64, relative_path='jobs/r1/o2.bin')])

    def accept(self, **kwargs):
        values = dict(receipt_id='ack1', storage_receipt_id='nas1', job_id='j1',
            generation=1, manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, accepted_object_ids=['o1'],
            viewer_receipt={'synthetic': 'signature-verified-by-fixture'}, verified_copy=True)
        values.update(kwargs)
        self.ledger.accept_receipt('r1', **values)

    def test_unsigned_processing_acceptance_does_not_make_outputs_cleanup_eligible(self):
        self.register()
        self.ledger.accept_receipt('r1', receipt_id='legacy-ack', storage_receipt_id='legacy-storage',
            job_id='j1', generation=1, manifest_sha256='a'*64, pipeline_sha256='b'*64,
            result_manifest_sha256='c'*64, accepted_object_ids=['o1', 'o2'])
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 10000
        self.assertEqual(self.ledger.cleanup_candidates(), [])

    def test_readiness_identity_and_environment(self):
        with self.assertRaises(PolicyConflict):
            self.reserve(worker_ready=False)
        first = self.reserve()
        self.assertEqual(first, self.reserve())
        with self.assertRaises(PolicyConflict):
            self.reserve(environment='staging')
        with self.assertRaises(ValueError):
            self.reserve(input_bytes=True)
        with self.assertRaises(ValueError):
            self.reserve(generation=0)
        with self.assertRaises(ValueError):
            self.reserve(job_id='_invalid')

    def test_fifty_gigabyte_job_respects_worker_free_space_and_output_budget(self):
        input_bytes = 50_000_000_000
        output_budget = 100 * 1024 ** 3
        safety_floor = 1024 ** 3
        capacities = {
            'cluster': 118_186_557_440,
            'main': 1_490_274_271_232,
        }
        for node, free_bytes in capacities.items():
            root = self.root / node
            ledger = ScratchLedger(root / 'private' / 'ledger.sqlite', root / 'scratch',
                capacity_probe=lambda _, value=free_bytes: value, clock=lambda: self.now,
                safety_floor_bytes=safety_floor, cleanup_grace_seconds=10)
            request = dict(job_id='large-photo-job', generation=1,
                reservation_id='large-' + node, manifest_sha256='a' * 64,
                input_bytes=input_bytes, output_budget=output_budget,
                environment='production', ttl_seconds=20, worker_ready=True)
            if node == 'cluster':
                with self.assertRaises(PolicyConflict):
                    ledger.reserve(**request)
            else:
                reservation = ledger.reserve(**request)
                self.assertEqual(reservation['remaining'], input_bytes + output_budget)

    def control(self,**changes):
        values=dict(charge_id='control-one',scope_sha256='b'*64,parent_binding_sha256='c'*64,expected_allocated=100,allowance_bytes=50,maximum_scope_charges=8)
        values.update(changes);return self.ledger.prepare_control_charge('r1',**values)

    def test_control_charge_and_hold_are_atomic_exact_idempotent_and_readonly(self):
        self.reserve();self.ledger.record_allocated('r1',100);first=self.control()
        self.assertEqual(first,self.control());self.assertEqual(first['allocatedAfter'],150)
        before=self.ledger.path.read_bytes();self.assertEqual(first,self.ledger.read_control_charge('r1'));self.assertEqual(before,self.ledger.path.read_bytes())
        with self.ledger._transaction() as db:
            row=self.ledger._reservation(db,'r1');self.assertEqual((row['allocated'],row['remaining']),(150,250))
        for key,value in [('expected_allocated',101),('allowance_bytes',49),('scope_sha256','d'*64),('parent_binding_sha256','d'*64),('charge_id','other'),('maximum_scope_charges',7)]:
            with self.subTest(key=key),self.assertRaises(PolicyConflict):self.control(**{key:value})

    def test_precharge_does_not_free_unmaterialized_capacity_and_terminal_does_not_release_hold(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.free=400
        self.control()
        # Remaining250 + retained hold50 + floor100 leave no capacity.
        with self.assertRaises(PolicyConflict):self.reserve(job_id='j2',reservation_id='r2',input_bytes=1,output_budget=0)
        self.ledger.set_engine_state('r1','terminal',terminal_attested=True);self.free=150
        with self.assertRaises(PolicyConflict):self.reserve(job_id='j2',reservation_id='r2',input_bytes=1,output_budget=0)

    def test_control_charge_needs_fresh_full_input_baseline_and_no_engine_start(self):
        self.reserve()
        with self.assertRaises(PolicyConflict):self.control()
        self.ledger.record_allocated('r1',100);self.ledger.set_engine_state('r1','engine_started')
        with self.assertRaises(PolicyConflict):self.control()

    def test_control_charge_commit_failure_never_changes_counter_without_receipt(self):
        self.reserve();self.ledger.record_allocated('r1',100)
        original=self.ledger._transaction
        from contextlib import contextmanager
        @contextmanager
        def fail():
            with original() as db:
                yield db
                raise OSError('synthetic before commit')
        with patch.object(self.ledger,'_transaction',fail),self.assertRaises(OSError):self.control()
        self.assertIsNone(self.ledger.read_control_charge('r1'))
        with self.ledger._transaction() as db:self.assertEqual(self.ledger._reservation(db,'r1')['allocated'],100)

    def publishable(self):
        path=str(self.root/'recovery.sqlite');lock=path+'.lock'
        descriptor=dict(scope=dict(protocol='ltds-original-claim-recovery-worker-v2',parentPolicy=dict(policySha256='a'*64,nodeId='main',workerId='worker',workerInstanceId='instance',authorityId='authority',engineId='engine',coordinatorOrigin='http://127.0.0.1:1',gateGeneration=1),
            policy=dict(per_job_attempts=8,global_attempts=64,max_row_bytes=8192,max_serialized_bytes=131072,max_db_pages=64,preparation_allowance=0),pipelineStoragePageCap=0,transportPolicy='d'*64,root=str(self.root),rootIdentity=[1,2],path=path,lockPath=lock),files={path:[1,3],lock:[1,4]},anchorReservation='r1',anchorChargeId='control-one')
        digest=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        return descriptor,digest(descriptor['scope'])

    def test_control_publication_cas_keeps_typed_historical_receipt(self):
        self.reserve();self.ledger.record_allocated('r1',100)
        descriptor,scope=self.publishable();self.control(scope_sha256=scope);path=str(self.root/'recovery.sqlite')
        digest=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
        values=dict(charge_id='control-one',scope_sha256=scope,parent_binding_sha256='c'*64,publication_descriptor=descriptor)
        receipt=self.ledger.publish_control_charge('r1',**values);self.assertEqual(receipt,self.ledger.publish_control_charge('r1',**values))
        self.assertEqual(self.ledger.control_scope_publications(scope,8),[digest(descriptor)])
        changed=copy.deepcopy(descriptor);changed['files'][path][1]+=1
        with self.assertRaises(PolicyConflict):self.ledger.publish_control_charge('r1',**dict(values,publication_descriptor=changed))
        self.ledger.set_engine_state('r1','engine_started');self.assertEqual(receipt,self.control(scope_sha256=scope))

    def test_concurrent_identical_control_preparation_charges_once(self):
        self.reserve();self.ledger.record_allocated('r1',100)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:receipts=list(executor.map(lambda _:self.control(),range(2)))
        self.assertEqual(receipts[0],receipts[1])
        with self.ledger._transaction() as db:self.assertEqual(self.ledger._reservation(db,'r1')['allocated'],150)

    def legacy_module(self):
        folder=Path(__file__).parent/'fixtures'/'scratch_policy_c60834c'
        source=folder/'legacy_scratch_policy.py';raw=source.read_bytes();attributes=json.loads((folder/'fixture.attrs').read_text())
        expected='770381c4115148cad3e31d58d282abe3c665ad8d185f716a5b4057202ac36e88'
        self.assertEqual(hashlib.sha256(raw).hexdigest(),expected);self.assertEqual(len(raw),25324)
        self.assertEqual(attributes['sha256'],expected);self.assertEqual(attributes['byteSize'],len(raw))
        self.assertEqual(attributes['sourceCommit'],'c60834cd156b7ec073ab381c0d9cde4ecee12bd3')
        self.assertEqual(attributes['gitBlob'],'85092a8db5395e7529effc63b0ef20a97509f03a')
        self.assertTrue(attributes['preserveExactBytes'])
        spec=importlib.util.spec_from_file_location('archived_scratch_policy_c60834c',source)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

    def old_open(self,module):
        return module.ScratchLedger(self.ledger.path,self.root,capacity_probe=lambda _:self.free,clock=lambda:self.now,safety_floor_bytes=100,cleanup_grace_seconds=10)

    def config(self):
        with closing(sqlite3.connect(self.ledger.path)) as db,db:return json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])

    def dump(self):
        with closing(sqlite3.connect(self.ledger.path)) as db,db:return '\n'.join(db.iterdump())

    def row(self,reservation='r1'):
        with closing(sqlite3.connect(self.ledger.path)) as db,db:
            return db.execute('SELECT * FROM reservations WHERE id=?',(reservation,)).fetchone()

    def fences(self):
        with closing(sqlite3.connect(self.ledger.path)) as db,db:
            return db.execute('SELECT * FROM transfer_fences ORDER BY reservation').fetchall()

    def test_fenced_partial_transfer_keeps_budget_past_lease_and_restart(self):
        self.reserve();self.ledger.begin_transfer('r1')
        part=self.ledger.prepare_job_directory('r1')/'.reference-synthetic.part';part.write_bytes(b'x'*50)
        self.free=950;before=self.row();self.now=500;self.ledger=self.open()
        # 950-400-100 leaves 450, so 500 fits only if the fenced 400 had been released.
        with self.assertRaisesRegex(PolicyConflict,'insufficient unreserved scratch'):
            self.reserve(job_id='j2',reservation_id='r2',input_bytes=200)
        self.assertEqual(self.row(),before);self.assertEqual(self.row()[6:9],('reserved',400,0))
        self.assertIsNone(self.row('r2'));self.assertEqual(part.read_bytes(),b'x'*50)
        self.ledger.record_allocated('r1',100);self.assertEqual(self.row()[6:9],('reserved',300,100))

    def test_begin_transfer_is_idempotent_and_keeps_first_fence(self):
        self.reserve();self.ledger.begin_transfer('r1');first=self.fences();self.now=110
        self.ledger.begin_transfer('r1');self.assertEqual(self.fences(),first);self.assertEqual(first,[('r1',100.0)])
        self.now=500;self.ledger=self.open();self.ledger.begin_transfer('r1')
        self.assertEqual(self.fences(),first);self.assertEqual(self.row()[6:9],('reserved',400,0))

    def test_lease_past_deadline_before_first_write_is_refused_without_a_fence(self):
        self.reserve();self.now=120
        with self.assertRaisesRegex(PolicyConflict,'allocation outside reservation'):self.ledger.begin_transfer('r1')
        self.assertEqual(self.fences(),[]);self.assertEqual(self.row()[6:9],('expired',0,0))
        with self.assertRaises(PolicyConflict):self.ledger.begin_transfer('unknown')

    def test_a_fence_never_revives_an_allocated_lease_already_past_its_deadline(self):
        self.reserve();self.ledger.record_allocated('r1',10);self.now=500
        for call in (self.ledger.begin_transfer,self.ledger.claim_engine_lease):
            with self.assertRaises(LeaseLapsed):call('r1')
        self.assertEqual(self.fences(),[]);self.assertEqual(self.row()[6:9],('reserved',390,10))

    def test_fenced_lease_past_its_deadline_starts_its_engine(self):
        self.reserve();self.ledger.begin_transfer('r1');self.now=500
        self.ledger.record_allocated('r1',100);self.ledger.set_engine_state('r1','engine_started')
        self.assertEqual(self.row()[6:9],('engine_started',300,100))

    def test_fenced_lease_past_its_deadline_takes_and_publishes_its_control_charge(self):
        self.reserve();self.ledger.begin_transfer('r1');self.ledger.record_allocated('r1',100);self.now=500
        descriptor,scope=self.publishable();receipt=self.control(scope_sha256=scope);self.assertEqual(self.row()[6:9],('reserved',250,150))
        self.assertEqual(self.ledger.publish_control_charge('r1',charge_id='control-one',scope_sha256=scope,parent_binding_sha256='c'*64,publication_descriptor=descriptor)['chargeId'],receipt['chargeId'])
        self.assertEqual(self.config()['transferFenceVersion'],1);self.assertEqual(self.config()['accountingVersion'],1)
        self.ledger=self.open();self.ledger.set_engine_state('r1','engine_started')

    def test_unfenced_lease_past_its_deadline_refuses_control_charge_and_publication(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.now=500
        with self.assertRaisesRegex(PolicyConflict,'fresh input-only control baseline required'):self.control()
        self.assertEqual(self.row()[6:9],('reserved',300,100))
        self.assertNotIn('accountingVersion',self.config())

    def test_unfenced_charge_cannot_publish_after_its_deadline_and_no_fence_rescues_it(self):
        self.reserve();self.ledger.record_allocated('r1',100);descriptor,scope=self.publishable();self.control(scope_sha256=scope);self.now=500
        with self.assertRaisesRegex(PolicyConflict,'pre-execution control publication required'):
            self.ledger.publish_control_charge('r1',charge_id='control-one',scope_sha256=scope,parent_binding_sha256='c'*64,publication_descriptor=descriptor)
        with self.assertRaises(LeaseLapsed):self.ledger.begin_transfer('r1')

    def test_claim_engine_lease_fences_a_live_lease_so_it_cannot_lapse_before_start(self):
        self.reserve();self.ledger.claim_engine_lease('r1');self.now=500;self.ledger.claim_engine_lease('r1')
        self.ledger.set_engine_state('r1','engine_started');self.ledger.claim_engine_lease('r1')
        self.assertEqual(self.fences(),[('r1',100.0)])
        self.reserve(job_id='j2',reservation_id='r2',ttl_seconds=1);self.now=600
        with self.assertRaisesRegex(LeaseLapsed,'lease expired before engine mutation'):self.ledger.claim_engine_lease('r2')
        self.assertEqual(self.row('r2')[6:9],('expired',0,0));self.assertEqual(self.fences(),[('r1',100.0)])

    def test_no_accepted_clock_can_end_a_fenced_zero_allocated_reservation(self):
        self.reserve();self.ledger.begin_transfer('r1');kept=self.row();self.assertEqual(kept[5:9],(FENCED_LEASE,'reserved',400,0))
        for clock in (float(MAX_INTEGER),2**53,1e300):
            with self.subTest(clock=clock):
                self.now=clock;self.ledger=self.open()
                self.ledger.begin_transfer('r1');self.ledger.claim_engine_lease('r1');self.ledger.record_allocated('r1',0)
                self.assertEqual(self.row(),kept)
        self.ledger.set_engine_state('r1','engine_started');self.assertEqual(self.row()[6:9],('engine_started',400,0))

    def test_no_accepted_clock_stops_a_fenced_lease_taking_its_control_charge(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.ledger.begin_transfer('r1');self.now=1e300
        descriptor,scope=self.publishable();receipt=self.control(scope_sha256=scope)
        self.assertEqual(self.ledger.publish_control_charge('r1',charge_id='control-one',scope_sha256=scope,parent_binding_sha256='c'*64,publication_descriptor=descriptor)['chargeId'],receipt['chargeId'])

    def test_an_engine_terminal_is_never_read_as_a_never_started_settlement(self):
        self.reserve();self.ledger.begin_transfer('r1');self.ledger.set_engine_state('r1','engine_started')
        self.assertFalse(self.ledger.never_started('r1'));self.ledger.set_engine_state('r1','terminal',terminal_attested=True)
        self.assertEqual(self.ledger.settle_unstarted('r1',transfer_ended=True),'terminal');self.assertFalse(self.ledger.never_started('r1'))
        self.reserve(job_id='j2',reservation_id='r2');self.ledger.set_engine_state('r2','terminal',terminal_attested=True)
        self.assertEqual(self.ledger.settle_unstarted('r2',transfer_ended=True),'terminal');self.assertFalse(self.ledger.never_started('r2'))

    def test_extreme_clock_still_expires_an_unfenced_lease_and_refuses_a_new_one(self):
        self.reserve();self.reserve(job_id='j2',reservation_id='r2');self.ledger.record_allocated('r2',10);self.now=float(MAX_INTEGER)-5
        with self.assertRaisesRegex(PolicyConflict,'lease deadline outside supported range'):self.reserve(job_id='j3',reservation_id='r3',ttl_seconds=5)
        self.assertEqual(self.row()[6:9],('expired',0,0));self.assertIsNone(self.row('r3'))
        with self.assertRaises(LeaseLapsed):self.ledger.claim_engine_lease('r2')

    def test_terminal_release_frees_a_fenced_reservation_past_its_lease(self):
        self.reserve();self.ledger.begin_transfer('r1');self.now=500
        self.ledger.set_engine_state('r1','terminal',terminal_attested=True);self.assertEqual(self.row()[6:9],('terminal',0,0))
        with self.assertRaises(PolicyConflict):self.ledger.begin_transfer('r1')
        self.ledger=self.open();self.reserve(job_id='j2',reservation_id='r2',input_bytes=500)

    def test_settlement_ends_a_never_started_fence_only_on_an_explicit_promise(self):
        self.reserve();self.ledger.begin_transfer('r1');self.now=500
        for promise in (False,None,1):
            with self.assertRaisesRegex(PolicyConflict,'settlement must guarantee'):self.ledger.settle_unstarted('r1',transfer_ended=promise)
        self.assertEqual(self.row()[6:9],('reserved',400,0))
        self.assertEqual(self.ledger.settle_unstarted('r1',transfer_ended=True),'unstarted');self.assertEqual(self.row()[6:9],('terminal',0,0))
        self.assertEqual(self.row()[9],500.0);self.assertEqual(self.ledger.settle_unstarted('r1',transfer_ended=True),'unstarted')
        self.assertTrue(self.ledger.never_started('r1'))
        self.assertIsNone(self.ledger.settle_unstarted('never-reserved',transfer_ended=True))
        self.ledger=self.open();self.reserve(job_id='j2',reservation_id='r2',input_bytes=500)

    def test_settlement_leaves_every_started_outcome_to_its_engine(self):
        self.reserve();self.ledger.begin_transfer('r1');self.ledger.set_engine_state('r1','engine_started')
        self.assertEqual(self.ledger.settle_unstarted('r1',transfer_ended=True),'engine_started')
        self.ledger.set_engine_state('r1','uncertain');self.assertEqual(self.ledger.settle_unstarted('r1',transfer_ended=True),'uncertain')
        self.assertEqual(self.row()[6:9],('uncertain',400,0))

    def test_unfenced_allocated_lease_still_refuses_terminal_past_deadline(self):
        self.reserve();self.ledger.record_allocated('r1',10);self.now=500
        with self.assertRaisesRegex(PolicyConflict,'lease expired before engine mutation'):
            self.ledger.set_engine_state('r1','terminal',terminal_attested=True)

    def test_fenced_result_reaches_ordinary_cleanup(self):
        self.reserve();self.ledger.begin_transfer('r1');self.ledger.record_allocated('r1',100);self.ledger.set_engine_state('r1','engine_started')
        identity=dict(job_id='j1',generation=1,manifest_sha256='a'*64,pipeline_sha256='b'*64,result_manifest_sha256='c'*64)
        self.ledger.register_result('r1',**identity,objects=[dict(object_id='o1',bytes=1,sha256='d'*64,relative_path='jobs/r1/o1.bin')])
        self.ledger.accept_receipt('r1',receipt_id='receipt',storage_receipt_id='storage',**identity,accepted_object_ids=['o1'],viewer_receipt=dict(id='v'),verified_copy=True)
        self.ledger.set_engine_state('r1','terminal',terminal_attested=True);self.now=500
        self.assertEqual([row['id'] for row in self.ledger.cleanup_candidates()],['o1'])

    def old_reserve(self,old,**values):
        return old.reserve(**dict(dict(job_id='old-job',generation=1,reservation_id='old-res',manifest_sha256='d'*64,input_bytes=500,output_budget=0,environment='staging',ttl_seconds=20,worker_ready=True),**values))

    def test_actual_old_allocator_after_expiry_refuses_a_fenced_ledger_and_changes_nothing(self):
        module=self.legacy_module();self.reserve();self.ledger.begin_transfer('r1');self.free=950;self.now=500
        before=self.ledger.path.read_bytes();dump=self.dump()
        with self.assertRaises(module.PolicyConflict):self.old_open(module)
        self.assertEqual(before,self.ledger.path.read_bytes());self.assertEqual(dump,self.dump())
        self.assertEqual(self.row()[6:9],('reserved',400,0))
        self.ledger=self.open();self.ledger.record_allocated('r1',100);self.ledger.set_engine_state('r1','engine_started')

    def test_old_allocator_already_running_past_expiry_cannot_release_or_reuse_a_fence(self):
        module=self.legacy_module();old=self.old_open(module);self.reserve();self.ledger.begin_transfer('r1');self.free=950;self.now=500
        # 950-400-100 leaves 450, so 500 fits only if the old expiry released the fenced 400.
        with self.assertRaisesRegex(module.PolicyConflict,'insufficient unreserved scratch'):self.old_reserve(old)
        self.assertEqual(self.row()[6:9],('reserved',400,0));self.assertIsNone(self.row('old-res'))

    def test_latch_keeps_an_older_allocator_refused_after_every_fence_ends(self):
        module=self.legacy_module();self.reserve();self.ledger.begin_transfer('r1')
        self.reserve(job_id='j2',reservation_id='r2');self.ledger.begin_transfer('r2')
        self.ledger.settle_unstarted('r1',transfer_ended=True);self.ledger.set_engine_state('r2','terminal',terminal_attested=True)
        self.assertEqual((self.row()[6],self.row('r2')[6]),('terminal','terminal'));before=self.ledger.path.read_bytes()
        with self.assertRaises(module.PolicyConflict):self.old_open(module)
        self.assertEqual(self.ledger.path.read_bytes(),before);self.assertEqual(self.config()['transferFenceVersion'],1)
        self.ledger=self.open();self.reserve(job_id='j3',reservation_id='r3')

    def test_every_order_of_fence_and_control_charge_keeps_old_code_refused(self):
        module=self.legacy_module()
        self.reserve();self.ledger.record_allocated('r1',100);self.control();self.ledger.begin_transfer('r1')
        self.assertEqual((self.config()['accountingVersion'],self.config()['transferFenceVersion']),(1,1))
        with self.assertRaises(module.PolicyConflict):self.old_open(module)
        self.ledger=self.open();self.assertIsNotNone(self.ledger.read_control_charge('r1'))

    def test_a_ledger_that_never_fenced_stays_old_compatible_past_expiry(self):
        module=self.legacy_module();self.reserve();self.now=500
        self.assertNotIn('transferFenceVersion',self.config());old=self.old_open(module)
        self.old_reserve(old);self.assertEqual(self.row('old-res')[6:9],('reserved',500,0));self.assertEqual(self.row()[6:9],('expired',0,0))

    def test_failed_fence_transaction_leaves_no_fence_deadline_or_latch(self):
        self.reserve();before=self.row();config=self.config()
        with patch.object(self.ledger,'_latch',side_effect=OSError('synthetic')),self.assertRaises(OSError):self.ledger.begin_transfer('r1')
        self.assertEqual(self.fences(),[]);self.assertEqual(self.row(),before);self.assertEqual(self.config(),config)

    def test_fence_marker_and_deadline_cannot_drift_apart(self):
        self.reserve();self.ledger.begin_transfer('r1');coherent=self.config()
        def tamper(*statements):
            with closing(sqlite3.connect(self.ledger.path)) as db,db:
                for statement,values in statements:db.execute(statement,values)
        lost={k:v for k,v in coherent.items() if k!='transferFenceVersion'}
        repairs=('UPDATE reservations SET expires=? WHERE id=?',(9007199254740991.0,'r1')),('INSERT OR REPLACE INTO transfer_fences VALUES(?,?)',('r1',100.0)),("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(coherent,sort_keys=True,separators=(',',':')),))
        for name,change in (('marker lost',("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(lost,sort_keys=True,separators=(',',':')),))),
                ('unknown marker',("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(dict(coherent,transferFenceVersion=2),sort_keys=True,separators=(',',':')),))),
                ('deadline lowered',('UPDATE reservations SET expires=? WHERE id=?',(120.0,'r1'))),
                ('fence removed',('DELETE FROM transfer_fences WHERE reservation=?',('r1',))),
                ('orphan fence',('INSERT INTO transfer_fences VALUES(?,?)',('ghost',1.0)))):
            with self.subTest(name):
                tamper(change);before=self.ledger.path.read_bytes();self.now+=1
                with self.assertRaises(PolicyConflict):self.reserve(job_id='blocked',reservation_id='blocked')
                with self.assertRaises(PolicyConflict):self.open()
                self.assertEqual(before,self.ledger.path.read_bytes())
                tamper(*repairs,('DELETE FROM transfer_fences WHERE reservation=?',('ghost',)))

    def test_old_allocator_database_gains_the_fence_table_on_new_open(self):
        module=self.legacy_module();path=self.root/'private'/'old.sqlite'
        old=module.ScratchLedger(path,self.root,capacity_probe=lambda _:self.free,clock=lambda:self.now,safety_floor_bytes=100,cleanup_grace_seconds=10)
        old.reserve(job_id='j1',generation=1,reservation_id='r1',manifest_sha256='a'*64,input_bytes=100,output_budget=300,environment='staging',ttl_seconds=20,worker_ready=True)
        ScratchLedger(path,self.root,capacity_probe=lambda _:self.free,clock=lambda:self.now,safety_floor_bytes=100,cleanup_grace_seconds=10).begin_transfer('r1')

    def test_actual_pinned_old_allocator_still_opens_nocharge_legacy_config(self):
        module=self.legacy_module();self.reserve();self.ledger.record_allocated('r1',100)
        self.assertNotIn('accountingVersion',self.config());old=self.old_open(module)
        old.reserve(job_id='old-job',generation=1,reservation_id='old-res',manifest_sha256='d'*64,input_bytes=1,output_budget=0,environment='staging',ttl_seconds=20,worker_ready=True)
        self.assertNotIn('accountingVersion',self.config())

    def test_actual_pinned_old_cold_allocator_refuses_charged_state_without_any_db_changes(self):
        module=self.legacy_module();self.reserve();self.ledger.record_allocated('r1',100);self.control()
        self.assertEqual(self.config()['accountingVersion'],1);before=self.ledger.path.read_bytes();dump=self.dump()
        with self.assertRaises(module.PolicyConflict):self.old_open(module)
        self.assertEqual(self.ledger.path.read_bytes(),before);self.assertEqual(self.dump(),dump)
        with closing(sqlite3.connect(self.ledger.path)) as db,db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reservations').fetchone()[0],1)

    def test_marker_receipt_counter_and_hold_rollback_together_before_commit(self):
        self.reserve();self.ledger.record_allocated('r1',100);original=self.ledger._transaction
        from contextlib import contextmanager
        @contextmanager
        def fail():
            with original() as db:
                yield db
                raise OSError('synthetic first-charge rollback')
        with patch.object(self.ledger,'_transaction',fail),self.assertRaises(OSError):self.control()
        self.assertNotIn('accountingVersion',self.config());self.assertIsNone(self.ledger.read_control_charge('r1'))
        with closing(sqlite3.connect(self.ledger.path)) as db,db:
            self.assertEqual(db.execute('SELECT allocated,remaining FROM reservations').fetchone(),(100,300))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM control_charges').fetchone()[0],0)
        self.old_open(self.legacy_module())

    def test_new_reopen_and_preexisting_new_instance_survive_first_charge_lost_ack(self):
        self.reserve();self.ledger.record_allocated('r1',100);other=self.open();original=self.ledger.prepare_control_charge;receipts=[]
        def lose(*args,**kwargs):receipts.append(original(*args,**kwargs));raise OSError('lost first-charge ACK')
        with patch.object(self.ledger,'prepare_control_charge',side_effect=lose),self.assertRaises(OSError):self.control()
        self.assertEqual(other.read_control_charge('r1'),receipts[0]);self.ledger=self.open()
        self.assertEqual(self.control(),receipts[0]);self.assertEqual(self.config()['accountingVersion'],1)
        with closing(sqlite3.connect(self.ledger.path)) as db,db:self.assertEqual(db.execute('SELECT allocated,remaining FROM reservations').fetchone(),(150,250))

    def test_missing_or_unknown_accounting_marker_blocks_all_admission_before_clock_writes(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.control();coherent=self.config()
        variants=[{k:v for k,v in coherent.items() if k!='accountingVersion'},dict(coherent,accountingVersion=2),dict(coherent,accountingVersion=True),dict(coherent,accountingVersion='1'),None]
        for value in variants:
            with self.subTest(value=value):
                with closing(sqlite3.connect(self.ledger.path)) as db,db:
                    if value is None:db.execute("DELETE FROM metadata WHERE key='config'")
                    else:db.execute("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(value,sort_keys=True,separators=(',',':')),))
                before=self.ledger.path.read_bytes();dump=self.dump();self.now+=1
                with self.assertRaises(PolicyConflict):self.reserve(job_id='blocked',reservation_id='blocked')
                with self.assertRaises(PolicyConflict):self.ledger.read_control_charge('r1')
                with self.assertRaises(PolicyConflict):self.ledger.control_scope_publications('b'*64,8)
                with self.assertRaises(PolicyConflict):self.open()
                self.assertEqual(before,self.ledger.path.read_bytes());self.assertEqual(dump,self.dump())
                with closing(sqlite3.connect(self.ledger.path)) as db,db:db.execute("INSERT OR REPLACE INTO metadata VALUES('config',?)",(json.dumps(coherent,sort_keys=True,separators=(',',':')),))

    def test_attempted_in_transaction_marker_removal_is_rolled_back(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.control();coherent=self.config()
        with self.assertRaises(PolicyConflict):
            with self.ledger._transaction() as db:
                value=dict(coherent);del value['accountingVersion']
                db.execute("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(value,sort_keys=True,separators=(',',':')),))
        self.assertEqual(self.config(),coherent);self.assertIsNotNone(self.ledger.read_control_charge('r1'))

    def test_valid_marker_does_not_hide_lowered_hold_or_missing_control_records(self):
        self.reserve();self.ledger.record_allocated('r1',100);self.control()
        with closing(sqlite3.connect(self.ledger.path)) as db,db:db.execute('UPDATE control_charges SET bytes=1')
        before=self.ledger.path.read_bytes();self.now+=1
        with self.assertRaises(PolicyConflict):self.reserve(job_id='blocked',reservation_id='blocked')
        self.assertEqual(before,self.ledger.path.read_bytes())
        with closing(sqlite3.connect(self.ledger.path)) as db,db:db.execute('DELETE FROM control_charges')
        with self.assertRaises(PolicyConflict):self.open()

    def test_expired_retry_preserves_job_environment_after_restart(self):
        self.reserve()
        self.now = 120
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r2', environment='staging')
        self.reserve(reservation_id='r2')
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r3')
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.reserve(reservation_id='r4', generation=2, environment='staging')

    def test_startup_schema_and_config_are_one_full_sync_transaction(self):
        real_connect = sqlite3.connect
        observations = []

        class ObservedConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql.strip().startswith(('CREATE', 'DROP', 'INSERT OR IGNORE INTO metadata')):
                    observations.append((self.in_transaction, super().execute('PRAGMA synchronous').fetchone()[0]))
                return super().execute(sql, parameters)

        def connect(*args, **kwargs):
            return real_connect(*args, **kwargs, factory=ObservedConnection)
        with patch('processing.scratch_policy.sqlite3.connect', connect):
            self.open()
        self.assertTrue(observations)
        self.assertTrue(all(in_transaction and sync == 2 for in_transaction, sync in observations))

    def test_concurrent_startup_rejects_conflicting_config(self):
        path = self.root / 'private' / 'concurrent.sqlite'
        def start(grace):
            try:
                ScratchLedger(path, self.root, safety_floor_bytes=100, cleanup_grace_seconds=grace)
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(start, [10, 11])), 1)

    def test_concurrent_capacity(self):
        def request(index):
            try:
                self.reserve(job_id=f'j{index}', reservation_id=f'r{index}')
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(request, range(8)))
        self.assertEqual(sum(outcomes), 2)

    def test_allocation_is_not_double_counted(self):
        self.reserve()
        self.free = 900  # 100 allocated bytes no longer appear in free snapshot.
        self.ledger.record_allocated('r1', 100)
        self.reserve(job_id='j2', reservation_id='r2')
        with self.assertRaises(PolicyConflict):
            self.ledger.record_allocated('r1', 99)

    def test_expiry_before_vs_after_engine(self):
        self.reserve()
        self.now = 119
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        self.now = 120
        self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')

    def test_engine_and_uncertain_reservations_do_not_expire(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.now = 1000
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)
        self.ledger.set_engine_state('r1', 'uncertain')
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)

    def test_clock_rollback_does_not_reopen_expired_lease(self):
        self.reserve()
        self.now = 130
        self.ledger.cleanup_candidates()  # Commit the observed clock high water mark.
        self.now = 110
        with self.assertRaises(PolicyConflict):
            self.open().set_engine_state('r1', 'engine_started')

    def test_rejected_expired_engine_start_persists_clock_and_expiry(self):
        self.reserve()  # t=100, expires=120
        self.now = 130
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.now = 110
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger = self.open()
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        self.reserve(reservation_id='r2')  # Durable safe expiry allows new identity.

    def test_ledger_cannot_be_rebound_to_different_policy_or_root(self):
        with self.assertRaises(PolicyConflict):
            ScratchLedger(self.root / 'private' / 'ledger.sqlite', self.root,
                capacity_probe=lambda _: self.free, cleanup_grace_seconds=0)

    def test_partial_acceptance_lost_ack_restart_and_tombstone(self):
        self.register()
        self.ledger.record_allocated('r1', 350)
        self.free = 650  # Allocated bytes remain occupied after unused budget release.
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        self.assertEqual(self.open().cleanup_candidates(), [])
        self.accept()
        self.accept()  # Lost response/retry preserves original accepted timestamp.
        self.now = 209
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        self.now = 210
        plans = self.open().cleanup_candidates()
        self.assertEqual([p['id'] for p in plans], ['o1'])
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)
        with self.assertRaises(PolicyConflict):
            self.accept(result_manifest_sha256='f'*64)
        with self.assertRaises(PolicyConflict):
            self.accept(accepted_object_ids=['o2'])
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='f'*64,
                intent_token=intent['intent_token'], executor_attested=True)
        self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
            intent_token=intent['intent_token'], executor_attested=True)
        self.assertEqual(self.open().cleanup_candidates(), [])
        self.accept()  # Receipt and object tombstone survive restart.
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=600)

    def test_terminal_attestation_releases_only_unused_budget(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        self.ledger.record_allocated('r1', 100)
        self.free = 900
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'terminal')
        with self.assertRaises(PolicyConflict):
            self.reserve(job_id='j2', reservation_id='r2', input_bytes=400)
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger = self.open()
        self.reserve(job_id='j2', reservation_id='r2', input_bytes=400)
        with self.assertRaises(PolicyConflict):
            self.ledger.record_allocated('r1', 101)
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'engine_started')
        with self.assertRaises(PolicyConflict):
            self.ledger.set_engine_state('r1', 'uncertain')

    def test_case_aliases_and_unsafe_windows_names_are_rejected(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        for paths in [('jobs/r1/o.bin', 'jobs/r1/O.BIN'),
                      ('jobs/r1/a/o.bin', 'jobs/r1/A/O.BIN')]:
            with self.assertRaises(PolicyConflict):
                self.ledger.register_result('r1', job_id='j1', generation=1,
                    manifest_sha256='a'*64, pipeline_sha256='b'*64,
                    result_manifest_sha256='c'*64, objects=[
                        dict(object_id=f'o{i}', bytes=10, sha256='d'*64, relative_path=p)
                        for i, p in enumerate(paths)])
        for name in ('o.bin.', 'o.bin ', 'CON', 'nul.txt', 'LPT1.dat', 'bad?file', 'x\x00'):
            with self.assertRaises(ValueError):
                self.ledger._owned_path('r1', f'jobs/r1/{name}')

    def test_existing_hardlinks_cannot_be_registered_as_distinct_objects(self):
        self.reserve()
        self.ledger.set_engine_state('r1', 'engine_started')
        folder = self.root / 'jobs' / 'r1'
        folder.mkdir(parents=True)
        original = folder / 'original.bin'
        original.write_bytes(b'synthetic')
        alias = folder / 'alias.bin'
        try:
            alias.hardlink_to(original)
        except OSError:
            self.skipTest('hardlink creation is unavailable')
        with self.assertRaises(PolicyConflict):
            self.ledger.register_result('r1', job_id='j1', generation=1,
                manifest_sha256='a'*64, pipeline_sha256='b'*64,
                result_manifest_sha256='c'*64, objects=[
                    dict(object_id=f'o{i}', bytes=9, sha256='d'*64,
                         relative_path=f'jobs/r1/{name}')
                    for i, name in enumerate(('original.bin', 'alias.bin'))])

    def test_uncertain_pins_survive_restart_and_time(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.ledger.set_pin('r1', 'o1', 'reader1', kind='read', state='uncertain')
        self.now = 10000
        ledger = self.open()
        self.assertEqual(ledger.cleanup_candidates(), [])
        ledger.set_pin('r1', 'o1', 'reader1', kind='read', state='released')
        self.assertEqual(len(ledger.cleanup_candidates()), 1)
        ledger.set_pin('r1', 'o1', 'worker1', kind='worker', state='active')
        with self.assertRaises(PolicyConflict):
            ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)

    def test_bounded_plans_and_confirmation(self):
        self.register()
        self.accept(accepted_object_ids=['o1', 'o2'])
        self.now = 200
        self.assertEqual(self.ledger.cleanup_candidates(), [])  # Engine still running.
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 210
        self.assertEqual(len(self.ledger.cleanup_candidates(max_count=1)), 1)
        self.assertEqual(self.ledger.cleanup_candidates(max_bytes=99), [])
        self.assertEqual(len(self.ledger.cleanup_candidates(max_bytes=150)), 1)
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
                intent_token='unfenced', executor_attested=False)

    def test_cleanup_intent_fences_racing_pins_and_survives_uncertainty(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        intent = self.ledger.begin_cleanup('r1', 'o1', generation=1, sha256='d'*64, lease_seconds=10)
        self.assertEqual(self.open().cleanup_candidates(), [])
        with self.assertRaises(PolicyConflict):
            self.open().set_pin('r1', 'o1', 'lateReader', kind='read', state='active')
        self.now = 210
        self.assertEqual(self.open().cleanup_intent('r1', 'o1')['state'], 'uncertain')
        with self.assertRaises(PolicyConflict):
            self.open().set_pin('r1', 'o1', 'lateWorker', kind='worker', state='active')
        with self.assertRaises(PolicyConflict):
            self.ledger.mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
                intent_token='wrong', executor_attested=True)
        self.open().mark_cleaned('r1', 'o1', generation=1, sha256='d'*64,
            intent_token=intent['intent_token'], executor_attested=True)
        self.assertEqual(self.open().cleanup_intent('r1', 'o1')['state'], 'cleaned')

    def test_pin_winning_race_blocks_cleanup_intent(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        plan = self.ledger.cleanup_candidates()[0]
        self.open().set_pin('r1', 'o1', 'reader', kind='read', state='active')
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_cleanup('r1', 'o1', generation=plan['generation'], sha256=plan['sha'])

    def test_concurrent_pin_and_cleanup_cannot_both_win(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        def attempt(action):
            try:
                if action == 'pin':
                    self.open().set_pin('r1', 'o1', 'racer', kind='read', state='active')
                else:
                    self.open().begin_cleanup('r1', 'o1', generation=1, sha256='d'*64)
                return True
            except PolicyConflict:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(attempt, ['pin', 'cleanup'])), 1)

    def test_paths_and_changed_results(self):
        self.register()
        for invalid in ('/nas/source', 'jobs/r1/../x', 'jobs/r1//x', 'C:/history/x', 'jobs/r2/x'):
            with self.assertRaises(ValueError):
                self.ledger._owned_path('r1', invalid)
        with self.assertRaises(PolicyConflict):
            self.ledger.register_result('r1', job_id='j1', generation=1,
                manifest_sha256='a'*64, pipeline_sha256='b'*64,
                result_manifest_sha256='f'*64, objects=[dict(object_id='o1', bytes=100,
                    sha256='d'*64, relative_path='jobs/r1/o1.bin')])

    def test_cleanup_rejects_symlink_created_after_registration(self):
        self.register()
        self.accept()
        self.ledger.set_engine_state('r1', 'terminal', terminal_attested=True)
        self.now = 200
        jobs = self.root / 'jobs'
        try:
            jobs.symlink_to(self.root / 'elsewhere', target_is_directory=True)
        except OSError:
            self.skipTest('symlink creation is unavailable to this Windows account')
        with self.assertRaises(ValueError):
            self.ledger.cleanup_candidates()


if __name__ == '__main__':
    unittest.main()
