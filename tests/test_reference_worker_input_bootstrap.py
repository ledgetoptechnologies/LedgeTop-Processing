"""Real local SQLite/bootstrap lifecycle; no installed workers or client data."""
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import uuid

from ops import bootstrap_worker_input_accounting as b
from ops import reference_worker_daemon as daemon
from processing.scratch_policy import ScratchLedger


@unittest.skipUnless(os.name=='posix','Linux directory durability and daemon ownership')
class InputBootstrapTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name);self.root.chmod(0o700)
        self.config=dict(stateRoot=str(self.root/'state'),scratchRoot=str(self.root/'scratch'),
            nodeId='cluster',workerId='worker-test',workerInstanceId='instance-test',authorityId=str(uuid.uuid4()),
            engineId='engine-test',gateGeneration=1,coordinatorOrigin='https://localhost:18443',
            brokerOrigin='https://localhost:18444',gateCoordinatorOrigin='http://127.0.0.1:18080',
            engineOrigin='http://127.0.0.1:13001',outputBudget=1024,transferTtl=300,safetyFloor=0,
            cleanupGrace=10,allowedEnvironments=['staging'],inputCopyAccounting=dict(schemaVersion=1))
        self.state=Path(self.config['stateRoot']);self.scratch=Path(self.config['scratchRoot'])

    def check(self,config=None):
        config=config or self.config
        with daemon.DaemonStateLock(config['stateRoot']) as owner:
            return b.require_worker_input_profile(config,owner)

    def bootstrap(self):return b.bootstrap_input_copy_accounting(self.config)

    def test_explicit_bootstrap_is_bound_pre_latched_and_idempotent_without_release_authority(self):
        first=self.bootstrap();self.assertEqual(first['state'],'enabled');self.assertTrue(self.check())
        with patch.object(ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('must not relatch')):
            second=self.bootstrap();self.assertEqual(second['state'],'already_enabled')
        self.assertEqual(first['bootstrapId'],second['bootstrapId'])
        ledger=ScratchLedger(self.state/'scratch.sqlite',self.scratch,safety_floor_bytes=0,cleanup_grace_seconds=10,input_copy_accounting=True)
        self.assertIsNone(ledger.release_authority)
        self.assertEqual(set(p.name for p in self.state.iterdir()),{'worker-daemon.lock',b.DIRECTORY,'scratch.sqlite'})

    def test_no_profile_keeps_legacy_startup_and_creates_no_bootstrap(self):
        config=dict(self.config);config.pop('inputCopyAccounting')
        self.assertFalse(self.check(config));self.assertFalse((self.state/b.DIRECTORY).exists())
        self.assertFalse(self.scratch.exists());self.assertFalse((self.state/'scratch.sqlite').exists())

    def test_bad_closed_profiles_refuse_before_any_state_root(self):
        for value in (True,1,None,{},dict(schemaVersion=True),dict(schemaVersion=2),dict(schemaVersion=1,enabled=True)):
            config=dict(self.config,inputCopyAccounting=value)
            with self.assertRaises(ValueError):b.input_profile(config)
        self.assertFalse(self.state.exists())

    def test_normal_start_does_not_initialize_missing_profile(self):
        with patch.object(b,'ScratchLedger',side_effect=AssertionError('must not construct')):
            with self.assertRaises((ValueError,FileNotFoundError)):self.check()
        self.assertFalse((self.state/b.DIRECTORY).exists())

    def test_existing_legacy_journals_and_nonempty_scratch_refuse_before_ledger(self):
        for filename in ('driver.sqlite','scratch.sqlite','outbox.sqlite','worker.sqlite','unexpected'):
            with self.subTest(filename=filename):
                state=self.root/filename.replace('.','-');state.mkdir(mode=0o700)
                path=state/filename;path.write_bytes(b'unchanged');path.chmod(0o600)
                config=dict(self.config,stateRoot=str(state))
                with patch.object(b,'ScratchLedger',side_effect=AssertionError('must not construct')):
                    with self.assertRaises(ValueError):b.bootstrap_input_copy_accounting(config)
                self.assertEqual(path.read_bytes(),b'unchanged');self.assertFalse((state/b.DIRECTORY).exists())
        self.scratch.mkdir(mode=0o700);(self.scratch/'retained-input').write_bytes(b'keep')
        with self.assertRaises(ValueError):self.bootstrap()
        self.assertFalse((self.state/'scratch.sqlite').exists());self.assertEqual((self.scratch/'retained-input').read_bytes(),b'keep')

    def test_crash_after_enable_resumes_exact_known_ledger_without_reenable(self):
        write=b._write_record
        def crash(path,value,owner):
            if path.name=='completed.json':raise OSError('simulated power loss after latch')
            return write(path,value,owner)
        with patch.object(b,'_write_record',side_effect=crash):
            with self.assertRaises(OSError):self.bootstrap()
        self.assertTrue((self.state/b.DIRECTORY/'ledger-created.json').exists())
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            config=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertEqual(config['inputCopyAccountingVersion'],1)
        with patch.object(ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('no replay')):
            self.assertEqual(self.bootstrap()['state'],'enabled')
        self.assertTrue(self.check())

    def test_known_pre_enable_checkpoint_resumes_but_bootstrap_never_contacts_engine(self):
        with patch.object(ScratchLedger,'enable_input_copy_accounting',side_effect=OSError('simulated pre-enable stop')):
            with self.assertRaises(OSError):self.bootstrap()
        with patch.object(daemon,'NativeEngine',side_effect=AssertionError('bootstrap must be offline')):
            self.assertEqual(self.bootstrap()['state'],'enabled')
        self.assertTrue(self.check())

    def test_known_checkpoint_missing_claim_holds_without_recreating_it(self):
        with patch.object(ScratchLedger,'enable_input_copy_accounting',side_effect=OSError('stop at known checkpoint')):
            with self.assertRaises(OSError):self.bootstrap()
        claim=self.scratch/b.CLAIM;claim.unlink()
        with patch.object(b,'_write_record',side_effect=AssertionError('must not repair checkpoint')):
            with self.assertRaises(FileNotFoundError):self.bootstrap()
        self.assertFalse(claim.exists())

    def test_crash_after_ledger_creation_without_receipt_holds_no_adoption(self):
        write=b._write_record
        def crash(path,value,owner):
            if path.name=='ledger-created.json':raise OSError('simulated uncheckpointed ledger')
            return write(path,value,owner)
        with patch.object(b,'_write_record',side_effect=crash):
            with self.assertRaises(OSError):self.bootstrap()
        before=(self.state/'scratch.sqlite').read_bytes()
        with patch.object(b,'ScratchLedger',side_effect=AssertionError('must not adopt')):
            with self.assertRaises(ValueError):self.bootstrap()
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),before)
        self.assertFalse((self.state/b.DIRECTORY/'completed.json').exists())

    def test_late_foreign_file_holds_before_enable(self):
        records=b._records
        def foreign(*args,**kwargs):
            result=records(*args,**kwargs)
            (self.state/'foreign.sqlite').write_bytes(b'retain')
            return result
        with patch.object(b,'_records',side_effect=foreign),patch.object(ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('must not enable')):
            with self.assertRaises(ValueError):self.bootstrap()
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            config=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertNotIn('inputCopyAccountingVersion',config)

    def test_other_state_root_cannot_claim_opted_in_scratch_even_with_default_profile(self):
        self.bootstrap();other=dict(self.config,stateRoot=str(self.root/'other-state'),workerInstanceId='other-instance')
        with patch.object(b,'ScratchLedger',side_effect=AssertionError('must not borrow')):
            with self.assertRaises(ValueError):b.bootstrap_input_copy_accounting(other)
            other.pop('inputCopyAccounting')
            with self.assertRaises(ValueError):self.check(other)
        self.assertFalse((Path(other['stateRoot'])/'scratch.sqlite').exists())

    def test_profile_flip_and_identity_policy_drift_refuse_before_ledger(self):
        self.bootstrap()
        changed=dict(self.config);changed.pop('inputCopyAccounting')
        with self.assertRaises(ValueError):self.check(changed)
        for field,value in (('workerId','different-worker'),('workerInstanceId','different-instance'),('nodeId','main'),
                            ('engineId','other-engine'),('authorityId',str(uuid.uuid4())),('gateGeneration',2),
                            ('safetyFloor',1),('cleanupGrace',11),('outputBudget',2048),('transferTtl',301),
                            ('brokerOrigin','https://localhost:19444')):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):self.check(dict(self.config,**{field:value}))

    def test_credentials_cannot_be_reflected_into_the_bootstrap_identity(self):
        self.config['workerBearer']='identity-secret-value-that-must-not-be-written'
        self.config['workerId']=self.config['workerBearer']
        with self.assertRaises(ValueError):self.bootstrap()
        self.assertFalse((self.state/b.DIRECTORY).exists())

    def test_scratch_root_replacement_claim_replacement_and_symlink_refuse(self):
        self.bootstrap();claim=self.scratch/b.CLAIM;raw=claim.read_bytes()
        held=self.scratch/'held-claim';claim.rename(held);claim.write_bytes(raw);claim.chmod(0o600)
        with self.assertRaises(ValueError):self.check()
        claim.unlink();held.rename(claim)
        old=self.root/'old-scratch';self.scratch.rename(old);self.scratch.mkdir(mode=0o700)
        with self.assertRaises(ValueError):self.check()
        self.scratch.rmdir();old.rename(self.scratch)
        saved=self.root/'original-claim';claim.rename(saved);claim.symlink_to(saved)
        with self.assertRaises(ValueError):self.check()

    def test_copied_state_root_cannot_reuse_the_original_completed_receipt(self):
        self.bootstrap();old=self.root/'old-state';self.state.rename(old)
        shutil.copytree(old,self.state);self.state.chmod(0o700)
        with self.assertRaises(ValueError):self.check()
        self.assertTrue(old.exists());self.assertTrue(self.state.exists())

    def test_completed_receipt_alone_is_not_authority_and_marker_tamper_holds(self):
        self.bootstrap();path=self.state/'scratch.sqlite'
        with closing(sqlite3.connect(path)) as db:
            row=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0]);row.pop('inputCopyAccountingVersion')
            db.execute("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(row),));db.commit()
        before=path.read_bytes()
        with self.assertRaises(ValueError):self.check()
        self.assertEqual(path.read_bytes(),before)

    def test_existing_empty_legacy_driver_is_not_adopted_after_bootstrap(self):
        self.bootstrap();path=self.state/'driver.sqlite'
        with closing(sqlite3.connect(path)) as db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)')
            db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps({'nodeId':'cluster'}),));db.commit()
        path.chmod(0o600);before=path.read_bytes()
        with self.assertRaises(ValueError):self.check()
        self.assertEqual(path.read_bytes(),before)

    def test_boolean_driver_profile_refuses_before_any_durable_constructor(self):
        self.bootstrap();path=self.state/'driver.sqlite'
        with closing(sqlite3.connect(path)) as db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT NOT NULL)')
            db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps({'inputCopyAccounting':{'schemaVersion':True}}),));db.commit()
        path.chmod(0o600)
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not construct')):
            with self.assertRaises(ValueError):daemon.build_worker(self.config)

    def test_control_charge_marker_is_preserved_and_default_off_writer_remains_fenced(self):
        self.bootstrap();ledger=ScratchLedger(self.state/'scratch.sqlite',self.scratch,safety_floor_bytes=0,cleanup_grace_seconds=10,input_copy_accounting=True)
        ledger.reserve(job_id='j1',generation=1,reservation_id='r1',manifest_sha256='a'*64,input_bytes=6,output_budget=300,environment='staging',ttl_seconds=300,worker_ready=True)
        ledger.record_allocated('r1',6)
        ledger.prepare_control_charge('r1',charge_id='c1',scope_sha256='e'*64,parent_binding_sha256='f'*64,expected_allocated=6,allowance_bytes=10,maximum_scope_charges=1)
        self.assertTrue(self.check())
        with closing(sqlite3.connect(ledger.path)) as db:
            config=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        self.assertEqual((config['accountingVersion'],config['inputCopyAccountingVersion']),(1,1))
        with self.assertRaises(ValueError):ScratchLedger(ledger.path,self.scratch,safety_floor_bytes=0,cleanup_grace_seconds=10)


@unittest.skipUnless(os.name=='posix','actual Linux factory composition')
class InputBootstrapFactoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_parent_guard import ParentGuardTests
        ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        from tests.test_reference_worker_service import WorkerServiceTests
        WorkerServiceTests.setUp(self)

    def write_config(self):
        self.path.write_text(json.dumps(self.config));self.path.chmod(0o600)

    def test_factory_requires_bootstrap_before_any_durable_constructor(self):
        self.config['inputCopyAccounting']=dict(schemaVersion=1);self.write_config();config=daemon.load_config(str(self.path))
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not construct')),patch.object(daemon,'WorkerOutbox',side_effect=AssertionError('must not construct')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
            with self.assertRaises((ValueError,FileNotFoundError)):daemon.build_worker(config)
        self.assertFalse((Path(config['stateRoot'])/'scratch.sqlite').exists())

    def test_bootstrapped_factory_wires_real_ledger_and_driver_and_reopens(self):
        self.config['inputCopyAccounting']=dict(schemaVersion=1);self.config['cleanupEnabled']=False;self.config['engineCleanupEnabled']=False
        self.write_config();config=daemon.load_config(str(self.path));b.bootstrap_input_copy_accounting(config)
        driver,server=daemon.build_worker(config)
        try:
            self.assertTrue(driver.input_copy_accounting);self.assertTrue(driver.ledger.input_copy_accounting)
            self.assertIsNone(driver.ledger.release_authority)
            self.assertIsNone(getattr(driver.ledger.probe,'context',None))
            with closing(sqlite3.connect(driver.path)) as db:
                value=json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
            self.assertEqual(value['inputCopyAccounting'],{'schemaVersion':1})
        finally:server.server_close()
        driver,server=daemon.build_worker(config)
        try:self.assertTrue(driver.input_copy_accounting)
        finally:server.server_close()

    def test_legacy_factory_cannot_borrow_claimed_scratch_with_another_state_root(self):
        self.config['inputCopyAccounting']=dict(schemaVersion=1);self.write_config()
        config=daemon.load_config(str(self.path));b.bootstrap_input_copy_accounting(config)
        config=dict(config,stateRoot=str(self.root/'other-worker-state'));config.pop('inputCopyAccounting')
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not construct')),patch.object(daemon,'WorkerOutbox',side_effect=AssertionError('must not construct')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
            with self.assertRaises(ValueError):daemon.build_worker(config)
        self.assertFalse((Path(config['stateRoot'])/'scratch.sqlite').exists())

    def test_foreign_driver_physical_binding_refuses_before_any_durable_constructor(self):
        self.config['inputCopyAccounting']=dict(schemaVersion=1);self.write_config()
        config=daemon.load_config(str(self.path));b.bootstrap_input_copy_accounting(config)
        driver,server=daemon.build_worker(config);path=driver.path;server.server_close()
        with closing(sqlite3.connect(path)) as db:
            binding=json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
            binding['workerInstanceId']='different-worker-instance'
            db.execute('UPDATE driver_binding SET binding=? WHERE singleton=1',(json.dumps(binding),));db.commit()
        before=path.read_bytes()
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not construct')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
            with self.assertRaises(ValueError):daemon.build_worker(config)
        self.assertEqual(path.read_bytes(),before)


if __name__=='__main__':unittest.main()
