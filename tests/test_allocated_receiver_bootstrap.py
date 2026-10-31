"""Fresh opt-in receiver bootstrapping, without engine or network access."""
import copy
import json
import os
from contextlib import closing
import sqlite3
import unittest
from unittest.mock import patch

from ops import bootstrap_worker_input_accounting as b
from ops import reference_worker_daemon as daemon
from processing.scratch_policy import ScratchLedger
from tests import test_reference_worker_input_bootstrap as fixtures

PROFILE={'schemaVersion':5,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-three-retained-v1'},'allocatedReceiver':{'schemaVersion':1}}


@unittest.skipUnless(os.name=='posix','Linux fresh ledger and inode identity')
class AllocatedBootstrapTests(unittest.TestCase):
    def setUp(self):
        fixtures.InputBootstrapTests.setUp(self)
        self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)

    def bootstrap(self):return b.bootstrap_input_copy_accounting(self.config)

    def check(self,config=None):
        config=self.config if config is None else config
        with daemon.DaemonStateLock(config['stateRoot']) as owner:
            return b.require_worker_input_profile(config,owner)

    def test_fresh_profile_latches_and_bootstraps_idempotently(self):
        self.assertEqual(self.bootstrap()['state'],'enabled')
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            marker=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
            self.assertEqual(marker['allocatedReceiverVersion'],1)
            self.assertEqual(db.execute('SELECT count(*) FROM allocated_input_receipts').fetchone()[0],0)
        self.assertTrue(self.check())
        with patch.object(ScratchLedger,'enable_allocated_receiver',side_effect=AssertionError('must not relatch')):
            self.assertEqual(self.bootstrap()['state'],'already_enabled')

    def test_legacy_profile_and_receiver_schema_tamper_fail_before_ledger_mutation(self):
        self.bootstrap()
        legacy=dict(self.config,inputCopyAccounting={'schemaVersion':3,'fetchJournal':PROFILE['fetchJournal']})
        before=(self.state/'scratch.sqlite').read_bytes()
        with self.assertRaises(ValueError):self.check(legacy)
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),before)
        with closing(sqlite3.connect(self.state/'scratch.sqlite')) as db:
            db.execute('CREATE TRIGGER foreign_allocated_trigger AFTER INSERT ON allocated_input_receipts BEGIN SELECT 1; END');db.commit()
        changed=(self.state/'scratch.sqlite').read_bytes()
        with self.assertRaises(ValueError):self.check()
        self.assertEqual((self.state/'scratch.sqlite').read_bytes(),changed)

    def test_crash_after_latch_resumes_known_fresh_state(self):
        write=b._write_record
        def interrupted(path,value,owner):
            if path.name=='completed.json':raise OSError('stop after latch')
            return write(path,value,owner)
        with patch.object(b,'_write_record',side_effect=interrupted):
            with self.assertRaises(OSError):self.bootstrap()
        with patch.object(ScratchLedger,'enable_allocated_receiver',side_effect=AssertionError('must not relatch')):
            self.assertEqual(self.bootstrap()['state'],'enabled')
        self.assertTrue(self.check())

    def test_rejects_extra_or_non_boolean_opt_in_and_existing_foreign_state(self):
        for bad in ({'schemaVersion':True},{'schemaVersion':1,'enabled':True},True):
            value=copy.deepcopy(PROFILE);value['allocatedReceiver']=bad
            with self.subTest(bad=bad),self.assertRaises(ValueError):b.input_profile({'inputCopyAccounting':value})
        self.state.mkdir(mode=0o700);foreign=self.state/'scratch.sqlite';foreign.write_bytes(b'keep');foreign.chmod(0o600)
        with self.assertRaises(ValueError):self.bootstrap()
        self.assertEqual(foreign.read_bytes(),b'keep')


@unittest.skipUnless(os.name=='posix','actual daemon factory and node gate')
class AllocatedFactoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUpClass.__func__(cls)

    def setUp(self):
        from tests.test_reference_worker_input_bootstrap import InputBootstrapFactoryTests
        InputBootstrapFactoryTests.setUp(self)

    def write_config(self):
        self.path.write_text(json.dumps(self.config));self.path.chmod(0o600)

    def test_actual_factory_opens_and_reopens_latched_receiver(self):
        self.config['inputCopyAccounting']=copy.deepcopy(PROFILE)
        self.config['cleanupEnabled']=False;self.config['engineCleanupEnabled']=False
        self.path.write_text(json.dumps(self.config));self.path.chmod(0o600)
        config=daemon.load_config(str(self.path))
        b.bootstrap_input_copy_accounting(config)
        for _ in range(2):
            driver,server=daemon.build_worker(config)
            try:
                self.assertTrue(driver.ledger.allocated_receiver)
                with driver.ledger._transaction() as db:
                    self.assertEqual(driver.ledger.validate_accounting(db)['allocatedReceiverVersion'],1)
            finally:server.server_close()
