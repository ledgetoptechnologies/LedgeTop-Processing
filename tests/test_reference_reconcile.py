"""Original envelope recovery is independent of all current catalog state."""
import copy
import os
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as p
from tests import test_reference_catalog_cache as catalog

@unittest.skipUnless(os.name=='posix','Linux catalog/authoritative Queue fixture')
class OriginalSubmissionReconciliationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):catalog.CatalogCacheTests.setUpClass()
    @classmethod
    def tearDownClass(cls):catalog.CatalogCacheTests.tearDownClass()
    def setUp(self):
        self.fixture=catalog.CatalogCacheTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
    def test_epoch_change_expiry_outage_and_restart_reconcile_exact_original_without_writes(self):
        f=self.fixture;original=f.raw();accepted=f.submit(original)
        for node in f.natives.values():node.definitions[0]['help']='Different current definition'
        self.assertEqual(f.call('GET','/capabilities').status,200)
        f.offline();f.now[0]+=61;self.assertEqual(f.call('GET','/capabilities').status,503)
        f.cold_reopen();before=f.queue.path.read_bytes()
        with patch.object(f.runtime.store.catalog_admission,'observe',side_effect=AssertionError('reconciliation must not discover')):
            response=f.call('POST','/jobs/reconcile',p.canonical_json(original))
        self.assertEqual(response.status,200);self.assertEqual(response.body,accepted.body)
        self.assertEqual(f.queue.path.read_bytes(),before)
        with f.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_option_evidence').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_transfer_grants').fetchone()[0],0)
    def test_changed_original_absence_environment_and_auth_never_create(self):
        f=self.fixture;original=f.raw();f.submit(original);f.offline();before=f.queue.path.read_bytes()
        changed=copy.deepcopy(original);changed['pipeline']['odm']['options']['future-quality']=2
        self.assertEqual(f.call('POST','/jobs/reconcile',p.canonical_json(changed)).status,409)
        absent=copy.deepcopy(original);absent['submissionId']=str(uuid.uuid4())
        self.assertEqual(f.call('POST','/jobs/reconcile',p.canonical_json(absent)).status,404)
        self.assertEqual(f.call('POST','/jobs/reconcile',p.canonical_json(original),True).status,404)
        self.assertEqual(f.call('POST','/jobs/reconcile',p.canonical_json(original),headers={'Authorization':'Bearer bad'}).status,401)
        malformed=copy.deepcopy(original);malformed['pipeline']={}
        self.assertEqual(f.call('POST','/jobs/reconcile',p.canonical_json(malformed)).status,400)
        self.assertEqual(f.queue.path.read_bytes(),before)
