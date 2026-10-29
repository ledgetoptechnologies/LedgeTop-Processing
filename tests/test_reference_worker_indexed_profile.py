"""Private indexed profile parsing and canonical daemon composition guards."""
import copy
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import threading
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import uuid

from ops import bootstrap_worker_input_accounting as bootstrap
from ops import reference_worker_daemon as daemon


PROFILE={'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-v1'}}


class IndexedProfileParsingTests(unittest.TestCase):
    def test_explicit_closed_version_two_and_unchanged_version_one_flags(self):
        self.assertFalse(bootstrap.input_profile({}))
        self.assertFalse(bootstrap.indexed_profile({}))
        self.assertTrue(bootstrap.input_profile({'inputCopyAccounting':{'schemaVersion':1}}))
        self.assertFalse(bootstrap.indexed_profile({'inputCopyAccounting':{'schemaVersion':1}}))
        self.assertTrue(bootstrap.input_profile({'inputCopyAccounting':copy.deepcopy(PROFILE)}))
        self.assertTrue(bootstrap.indexed_profile({'inputCopyAccounting':copy.deepcopy(PROFILE)}))

    def test_v2_nested_fields_versions_boolean_substitutes_and_policy_are_closed(self):
        cases=[True,2,None,{'schemaVersion':True},{'schemaVersion':2},
               {'schemaVersion':2,'fetchJournal':None},
               {'schemaVersion':2,'fetchJournal':{'schemaVersion':True,'policyId':'indexed-worker-fetch-v1'}},
               {'schemaVersion':2,'fetchJournal':{'schemaVersion':2,'policyId':'indexed-worker-fetch-v1'}},
               {'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'other'}},
               {'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-v1','maxBytes':123}},
               {'schemaVersion':1,'fetchJournal':PROFILE['fetchJournal']},
               dict(PROFILE,enable=True)]
        for value in cases:
            with self.subTest(value=value),self.assertRaises(ValueError):bootstrap.input_profile({'inputCopyAccounting':value})

    def test_v2_refuses_treedar_composition_before_opening_state(self):
        config={'inputCopyAccounting':copy.deepcopy(PROFILE),'executionRouting':{'treedar':{'imageId':'not-an-installed-runtime'}}}
        with self.assertRaises(ValueError):bootstrap.input_profile(config)


@unittest.skipUnless(os.name=='posix','Linux owned bootstrap and SQLite journal')
class IndexedBootstrapTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapTests
        InputBootstrapTests.setUp(self)
        self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)

    def bootstrap(self):return bootstrap.bootstrap_input_copy_accounting(self.config)

    def check(self,config=None):
        config=self.config if config is None else config
        with daemon.DaemonStateLock(config['stateRoot']) as owner:
            return bootstrap.require_worker_input_profile(config,owner)

    def bytes(self):
        return {str(p.relative_to(self.state)):p.read_bytes() for p in self.state.rglob('*') if p.is_file() and p.name!='worker-daemon.lock'}

    def test_fresh_v2_pins_real_indexed_context_and_distinct_claim_before_completion(self):
        from processing.reference_indexed_fetch import compiled_profile_descriptor,IndexedFetchContext
        self.assertEqual(self.bootstrap()['state'],'enabled')
        directory=self.state/bootstrap.DIRECTORY
        intent=bootstrap._file(directory/'intent.json')[0]
        self.assertEqual(intent['binding']['inputCopyAccounting'],PROFILE)
        self.assertEqual(intent['binding']['indexedFetchJournal'],compiled_profile_descriptor())
        claim=bootstrap._file(self.scratch/bootstrap.CLAIM)[0]
        self.assertEqual(claim['schemaVersion'],2)
        self.assertEqual(claim['indexedFetchJournal'],compiled_profile_descriptor())
        receipt=bootstrap._file(directory/'completed.json')[0]
        self.assertEqual(receipt['schemaVersion'],2);self.assertIn('indexedCreatedSha256',receipt)
        self.assertTrue(self.check())
        with daemon.DaemonStateLock(self.state) as owner:
            context=bootstrap.require_worker_indexed_context(self.config,owner)
            self.assertIs(type(context),IndexedFetchContext)
            self.assertEqual(list(context.driver_identity),bootstrap._identity((self.state/'driver.sqlite').stat()))
        with closing(sqlite3.connect(self.state/'driver.sqlite')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM driver_binding').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_profile').fetchone()[0],1)
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            self.assertEqual(json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])['inputCopyAccountingVersion'],1)

    def test_v2_idempotent_and_v1_or_omitted_profile_downgrades_change_no_db_clock(self):
        self.bootstrap();before=self.bytes()
        from processing import reference_indexed_fetch as indexed
        with patch.object(indexed,'initialize_fresh_profile',side_effect=AssertionError('must not recreate')),patch.object(bootstrap.ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('must not relatch')):
            self.assertEqual(self.bootstrap()['state'],'already_enabled')
        self.assertEqual(self.bytes(),before)
        for value in ({'schemaVersion':1},{'schemaVersion':2},None):
            config=dict(self.config)
            if value is None:config.pop('inputCopyAccounting')
            else:config['inputCopyAccounting']=value
            with self.subTest(value=value),patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not open ledger')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not contact engine')):
                with self.assertRaises(ValueError):daemon.build_worker(config)
            self.assertEqual(self.bytes(),before)

    def test_v1_bootstrap_cannot_be_adopted_as_version_two(self):
        self.config['inputCopyAccounting']={'schemaVersion':1};self.bootstrap();before=self.bytes()
        self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)
        with self.assertRaises(ValueError):self.bootstrap()
        self.assertEqual(self.bytes(),before);self.assertFalse((self.state/'driver.sqlite').exists())

    def test_v2_post_enable_crash_resumes_known_indexed_receipt_without_recreation(self):
        write=bootstrap._write_record
        def crash(path,value,owner):
            if path.name=='completed.json':raise OSError('synthetic stop after both latches')
            return write(path,value,owner)
        with patch.object(bootstrap,'_write_record',side_effect=crash):
            with self.assertRaises(OSError):self.bootstrap()
        driver_before=(self.state/'driver.sqlite').read_bytes()
        from processing import reference_indexed_fetch as indexed
        with patch.object(indexed,'initialize_fresh_profile',side_effect=AssertionError('must not recreate')),patch.object(bootstrap.ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('must not relatch')):
            self.assertEqual(self.bootstrap()['state'],'enabled')
        self.assertEqual((self.state/'driver.sqlite').read_bytes(),driver_before)

    def test_uncheckpointed_indexed_driver_holds_without_enable_or_adoption(self):
        write=bootstrap._write_record
        def crash(path,value,owner):
            if path.name=='indexed-created.json':raise OSError('synthetic uncheckpointed driver')
            return write(path,value,owner)
        with patch.object(bootstrap,'_write_record',side_effect=crash):
            with self.assertRaises(OSError):self.bootstrap()
        before=self.bytes()
        with patch.object(bootstrap.ScratchLedger,'enable_input_copy_accounting',side_effect=AssertionError('must not enable')):
            with self.assertRaises(ValueError):self.bootstrap()
        self.assertEqual(self.bytes(),before)

    def test_out_of_order_indexed_checkpoint_refuses_before_new_ledger_or_claim(self):
        write=bootstrap._write_record
        def stop(path,value,owner):
            result=write(path,value,owner)
            if path.name=='intent.json':raise OSError('stop at initial intent')
            return result
        with patch.object(bootstrap,'_write_record',side_effect=stop):
            with self.assertRaises(OSError):self.bootstrap()
        path=self.state/bootstrap.DIRECTORY/'indexed-created.json';path.write_text('{}');path.chmod(0o600)
        with patch.object(bootstrap,'ScratchLedger',side_effect=AssertionError('must not construct')):
            with self.assertRaises(ValueError):self.bootstrap()
        self.assertFalse((self.state/'scratch.sqlite').exists());self.assertFalse((self.scratch/bootstrap.CLAIM).exists())

    def test_scratch_replacement_with_copied_claim_is_refused_before_ledger_creation(self):
        write=bootstrap._write_record
        def replace(path,value,owner):
            result=write(path,value,owner)
            if path.name==bootstrap.CLAIM:
                raw=path.read_bytes();self.scratch.rename(self.root/'original-scratch')
                self.scratch.mkdir(mode=0o700);path.write_bytes(raw);path.chmod(0o600)
            return result
        with patch.object(bootstrap,'_write_record',side_effect=replace),patch.object(bootstrap,'ScratchLedger',side_effect=AssertionError('must not construct')):
            with self.assertRaises(ValueError):self.bootstrap()
        self.assertFalse((self.state/'scratch.sqlite').exists())
        self.assertTrue((self.root/'original-scratch'/bootstrap.CLAIM).exists())

    def test_policy_or_indexed_schema_drift_holds_before_ledger_constructor(self):
        self.bootstrap();before=self.bytes();descriptor=bootstrap._indexed_descriptor();descriptor['policySha256']='0'*64
        with patch.object(bootstrap,'_indexed_descriptor',return_value=descriptor),patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not open ledger')):
            with self.assertRaises(ValueError):daemon.build_worker(self.config)
        self.assertEqual(self.bytes(),before)
        with closing(sqlite3.connect(self.state/'driver.sqlite')) as db:
            db.execute('DROP TRIGGER indexed_objects_sealed_insert');db.commit()
        changed=self.bytes()
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not open ledger')):
            with self.assertRaises(ValueError):daemon.build_worker(self.config)
        self.assertEqual(self.bytes(),changed)


class IndexedServiceYieldTests(unittest.TestCase):
    class Owner:
        def __init__(self):self.checks=0
        def check(self,*args):self.checks+=1

    def test_json_flags_and_non_boolean_proofs_cannot_skip_the_idle_delay(self):
        class Driver:pass
        service=daemon.WorkerService(Driver(),SimpleNamespace(),poll_seconds=.1)
        with patch.object(daemon.time,'sleep',side_effect=AssertionError('must not yield')):
            self.assertFalse(service._progress_yield({'state':'fetching','indexedFetchProgress':True},threading.Event()))
            service.driver.immediate_fetch_progress=lambda result:1
            self.assertFalse(service._progress_yield({'state':'fetching'},threading.Event()))

    def test_proved_progress_checks_owner_and_zero_yields_but_checks_stop(self):
        class Driver:pass
        driver=Driver();driver._state_owner=self.Owner();driver.path=Path('/synthetic/driver.sqlite')
        result={'state':'fetching'};driver.immediate_fetch_progress=lambda value:value is result
        service=daemon.WorkerService(driver,SimpleNamespace(),poll_seconds=.1);stop=threading.Event()
        with patch.object(daemon.time,'sleep') as sleep:
            self.assertTrue(service._progress_yield(result,stop));sleep.assert_called_once_with(0)
            stop.set();self.assertTrue(service._progress_yield(result,stop));self.assertEqual(sleep.call_count,1)
        self.assertEqual(driver._state_owner.checks,2)

    def test_actual_service_loop_does_not_poll_sleep_between_proved_objects(self):
        stop=threading.Event();waits=[];ordinary_wait=stop.wait
        def wait(timeout=None):
            if threading.current_thread().name=='reference-driver':waits.append(timeout)
            return ordinary_wait(timeout)
        stop.wait=wait
        class Server:
            def __init__(self):self.done=threading.Event()
            def serve_forever(self,**kwargs):self.done.wait()
            def shutdown(self):self.done.set()
            def server_close(self):pass
        class Driver:
            def __init__(self):self.count=0;self._state_owner=IndexedServiceYieldTests.Owner();self.path=Path('/synthetic/driver.sqlite');self.result=None
            def step(self):
                self.count+=1;self.result={'state':'fetching' if self.count<3 else 'held'}
                if self.count==3:stop.set()
                return self.result
            def immediate_fetch_progress(self,result):return result is self.result and self.count<3
        driver=Driver();service=daemon.WorkerService(driver,Server(),poll_seconds=.1)
        self.assertEqual(service.run(stop),0)
        self.assertEqual(driver.count,3);self.assertEqual(driver._state_owner.checks,2)
        self.assertEqual(waits,[.2])


@unittest.skipUnless(os.name=='posix','actual canonical Linux factory/readiness lease')
class IndexedCanonicalFactoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUpClass.__func__(cls)

    def setUp(self):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUp(self)
        self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)
        self.config['cleanupEnabled']=False;self.config['engineCleanupEnabled']=False
        self.write_config();config=daemon.load_config(str(self.path))
        bootstrap.bootstrap_input_copy_accounting(config)
        self.driver,self.server=daemon.build_worker(config);self.addCleanup(self.server.server_close)

    def write_config(self):
        self.path.write_text(json.dumps(self.config));self.path.chmod(0o600)

    def pending(self,job=None,request=None,data='{}'):
        job=job or str(uuid.uuid4());request=request or str(uuid.uuid4())
        with self.driver._db() as db:
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claim_pending',?)",(job,request,data))
        return job,request

    def lease(self,job,request,*,phase='initial_claim'):
        from processing.reference_driver import DriverStepLease
        entered,release=threading.Event(),threading.Event();errors=[]
        def hold():
            driver=self.driver
            try:
                with driver._step_guard,driver._lock:
                    driver._active_step=DriverStepLease(driver)
                    try:
                        with driver._readiness_window(phase,job if phase=='initial_claim' else None,request if phase=='initial_claim' else None):
                            entered.set();release.wait(5)
                    finally:driver._active_step=None
            except BaseException as error:errors.append(error);entered.set()
        thread=threading.Thread(target=hold);thread.start();self.assertTrue(entered.wait(3));self.assertEqual(errors,[])
        def close():
            release.set();thread.join(5);self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.addCleanup(close)
        return close

    def test_actual_factory_binds_v2_context_and_cold_reopens(self):
        from processing.reference_indexed_fetch import compiled_profile_descriptor,IndexedCapacityProbe
        self.assertIsNotNone(self.driver.indexed_fetch_context)
        self.assertIs(type(self.driver.ledger.probe),IndexedCapacityProbe)
        self.assertIs(self.driver.ledger.probe.context,self.driver.indexed_fetch_context)
        self.assertIs(self.driver.ledger.probe.owner,self.driver._state_owner)
        with closing(sqlite3.connect(self.driver.path)) as db:
            binding=json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
        self.assertEqual(binding['indexedFetchJournal'],compiled_profile_descriptor())
        self.assertEqual(binding['inputCopyAccounting'],{'schemaVersion':1})
        self.server.server_close()
        self.driver,self.server=daemon.build_worker(daemon.load_config(str(self.path)))
        self.addCleanup(self.server.server_close)
        self.assertIsNotNone(self.driver.indexed_fetch_context)

    def test_pending_exception_requires_actual_cross_thread_lease_and_is_read_only(self):
        job,request=self.pending();before=self.driver.path.read_bytes()
        self.assertEqual(daemon._work_health(self.driver)['pendingJobs'],1)
        close=self.lease(job,request)
        self.assertEqual(daemon._work_health(self.driver)['pendingJobs'],0)
        self.assertEqual(self.driver.path.read_bytes(),before)
        close()
        self.assertEqual(daemon._work_health(self.driver)['pendingJobs'],1)
        self.assertEqual(self.driver.path.read_bytes(),before)

    def test_foreign_pending_row_and_discovery_lease_cannot_use_indexed_exception(self):
        job,request=self.pending();other,other_request=self.pending();before=self.driver.path.read_bytes()
        close=self.lease(job,request)
        lease=self.driver._readiness_rpc
        self.assertFalse(self.driver.indexed_empty_claim_pending(other,other_request,lease))
        self.assertFalse(self.driver.indexed_empty_claim_pending(job,request,{'phase':'initial_claim'}))
        self.assertEqual(daemon._work_health(self.driver)['pendingJobs'],2);close()
        close=self.lease(job,request,phase='discovery')
        self.assertFalse(self.driver.indexed_empty_claim_pending(job,request,self.driver._readiness_rpc));close()
        self.assertEqual(self.driver.path.read_bytes(),before)

    def test_mutated_profile_cannot_fall_back_to_legacy_empty_json_exception(self):
        job,request=self.pending();before=self.driver.path.read_bytes();close=self.lease(job,request)
        original=self.driver.indexed_fetch_context;self.driver.indexed_fetch_context=None
        try:self.assertEqual(daemon._work_health(self.driver)['pendingJobs'],1)
        finally:self.driver.indexed_fetch_context=original;close()
        self.assertEqual(self.driver.path.read_bytes(),before)


@unittest.skipUnless(os.name=='posix','actual ordinary factory and readonly footprint guard')
class NonIndexedEarlyFootprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUpClass.__func__(cls)

    def setUp(self):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUp(self)

    def write_config(self):
        self.path.write_text(json.dumps(self.config));self.path.chmod(0o600)

    def coherent_ordinary_driver(self,mode,suffix):
        config=copy.deepcopy(self.config)
        config['stateRoot']=str(self.root/(mode+'-state-'+suffix))
        config['scratchRoot']=str(self.root/(mode+'-scratch-'+suffix))
        config['cleanupEnabled']=False;config['engineCleanupEnabled']=False
        if mode=='v1':config['inputCopyAccounting']={'schemaVersion':1}
        else:config.pop('inputCopyAccounting',None)
        self.path.write_text(json.dumps(config));self.path.chmod(0o600)
        loaded=daemon.load_config(str(self.path))
        if mode=='v1':bootstrap.bootstrap_input_copy_accounting(loaded)
        driver,server=daemon.build_worker(loaded)
        path=driver.path;server.server_close()
        return loaded,path

    def assert_early_readonly_refusal(self,config):
        root=Path(config['stateRoot'])
        snapshot=lambda:{str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
        before=snapshot()
        with patch.object(daemon,'ScratchLedger',side_effect=AssertionError('must not observe ledger clock')),patch.object(daemon,'NativeEngine',side_effect=AssertionError('must not construct native client')):
            with self.assertRaises(ValueError):daemon.build_worker(config)
        self.assertEqual(snapshot(),before)

    def test_default_and_v1_reject_case_insensitive_indexed_namespace_before_components(self):
        for mode in ('default','v1'):
            with self.subTest(mode=mode):
                config,path=self.coherent_ordinary_driver(mode,'table')
                with closing(sqlite3.connect(path)) as db:
                    db.execute('CREATE TABLE InDeXeD_profile(value TEXT)');db.commit()
                self.assert_early_readonly_refusal(config)

    def test_default_and_v1_reject_lone_indexed_binding_before_components(self):
        for mode in ('default','v1'):
            with self.subTest(mode=mode):
                config,path=self.coherent_ordinary_driver(mode,'binding')
                with closing(sqlite3.connect(path)) as db:
                    settings=json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
                    settings['indexedFetchJournal']={'schemaVersion':1,'policyId':'indexed-worker-fetch-v1'}
                    db.execute('UPDATE driver_binding SET binding=? WHERE singleton=1',(json.dumps(settings),));db.commit()
                self.assert_early_readonly_refusal(config)

    def test_nonindexed_core_binding_checker_refuses_new_marker_without_other_drift(self):
        config,path=self.coherent_ordinary_driver('v1','core')
        intent=bootstrap._file(Path(config['stateRoot'])/bootstrap.DIRECTORY/'intent.json')[0]
        with closing(sqlite3.connect(path)) as db:
            settings=json.loads(db.execute('SELECT binding FROM driver_binding WHERE singleton=1').fetchone()[0])
        bootstrap._driver_settings(settings,intent['binding'],indexed=False)
        settings['indexedFetchJournal']=None
        with self.assertRaises(ValueError):bootstrap._driver_settings(settings,intent['binding'],indexed=False)


if __name__=='__main__':unittest.main()
