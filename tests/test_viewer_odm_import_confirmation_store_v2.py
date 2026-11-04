import copy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.viewer_import_confirmation_store import ImportConfirmationStore, ImportConfirmationStoreHeld
from processing.viewer_odm_import_confirmation_store_v2 import OdmImportConfirmationStoreV2
from tests.test_viewer_import_confirmation_store import fixture_loader
from tests.test_viewer_odm_import_confirmation_v2 import OdmImportConfirmationV2Tests
from tests.test_viewer_import_confirmation import ViewerImportConfirmationTests


@unittest.skipUnless(os.name == 'posix' and hasattr(os, 'O_DIRECTORY') and hasattr(os, 'O_NOFOLLOW'),
                     'directory fsync is required')
class OdmImportDurableV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='odm-proof-v2-')
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'proof.sqlite'
        fixture = OdmImportConfirmationV2Tests('test_signed_viewer_assertions_are_not_prebound_processing_expectations')
        fixture.setUp(); self.fixture = fixture
        self.scope = dict(jobId='job-1', generation=1, environment='staging')
        self.context = dict(expected=fixture.expected, result_manifest=fixture.manifest,
            accepted_envelopes=[fixture.envelope], trusted_import_keys=fixture.import_keys,
            trusted_copy_keys=fixture.copy_signer.trusted_keys)
        self.store = OdmImportConfirmationStoreV2(self.path, trusted_loader=fixture_loader)
        with self.store._connect() as db:
            db.execute('CREATE TABLE trusted_import_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
            db.execute('INSERT INTO trusted_import_fixture VALUES(?,?,?,?)',
                (*self.scope.values(),json.dumps(self.context)))

    def set_context(self, value):
        with self.store._connect() as db:
            db.execute('UPDATE trusted_import_fixture SET payload=?', (json.dumps(value),))

    def test_first_proof_restart_exact_replay_and_signed_inventory_conflicts(self):
        result = self.store.record(self.scope, self.fixture.confirmation)
        self.assertEqual(result.state,'recorded')
        self.store = OdmImportConfirmationStoreV2(self.path, trusted_loader=fixture_loader)
        self.assertEqual(self.store.load_verified(self.scope).sha256,result.sha256)
        self.assertEqual(self.store.record(self.scope,self.fixture.confirmation).state,'replayed')
        for path in (('modelId',),('outputInventorySha256',),('archive','inventorySha256')):
            old = copy.deepcopy(self.fixture.confirmation)
            target = self.fixture.confirmation
            for key in path[:-1]: target = target[key]
            target[path[-1]] = '9'*64 if 'Sha256' in path[-1] else 'other-model'
            self.fixture.sign()
            with self.subTest(path=path),self.assertRaises(ImportConfirmationStoreHeld):
                self.store.record(self.scope,self.fixture.confirmation)
            self.fixture.confirmation = old
        self.assertEqual(self.store.load_verified(self.scope).sha256,result.sha256)

    def test_bound_viewer_identity_archive_and_pins_are_required_on_reverify(self):
        self.store.record(self.scope,self.fixture.confirmation)
        for section,key,value in [('expected','viewerAttemptId','other-attempt'),
                                  ('expected','viewerIssuerId','other-issuer'),
                                  ('result_manifest','objects',[])]:
            context=copy.deepcopy(self.context); context[section][key]=value; self.set_context(context)
            with self.subTest(key=key),self.assertRaises(ImportConfirmationStoreHeld): self.store.load_verified(self.scope)
        context=copy.deepcopy(self.context); context['trusted_import_keys']={}; self.set_context(context)
        with self.assertRaises(ImportConfirmationStoreHeld): self.store.load_verified(self.scope)
        self.set_context(self.context)
        self.assertIsNotNone(self.store.load_verified(self.scope))

    def test_archive_size_hash_and_viewer_attempt_cannot_be_rebound(self):
        for path,value in [(('archive','sha256'),'0'*64),(('archive','byteSize'),2),
                           (('viewerAttemptId',),'other-attempt')]:
            old=copy.deepcopy(self.fixture.confirmation); target=self.fixture.confirmation
            for key in path[:-1]: target=target[key]
            target[path[-1]]=value; self.fixture.sign()
            with self.subTest(path=path),self.assertRaises(ImportConfirmationStoreHeld):
                self.store.record(self.scope,self.fixture.confirmation)
            self.fixture.confirmation=old
        self.assertIsNone(self.store.load_verified(self.scope))

    def test_concurrent_exact_first_record_and_crash_rollback(self):
        with patch('processing.viewer_import_confirmation_store._after_insert',side_effect=RuntimeError('synthetic failure')):
            with self.assertRaises(RuntimeError): self.store.record(self.scope,self.fixture.confirmation)
        self.assertIsNone(self.store.load_verified(self.scope))
        def record(_):
            store=OdmImportConfirmationStoreV2(self.path,trusted_loader=fixture_loader)
            return store.record(self.scope,self.fixture.confirmation).state
        with ThreadPoolExecutor(max_workers=2) as pool: states=list(pool.map(record,range(2)))
        self.assertCountEqual(states,['recorded','replayed'])

    def test_lost_commit_response_replays_and_conflicting_race_holds(self):
        first=copy.deepcopy(self.fixture.confirmation)
        self.fixture.confirmation['outputInventorySha256']='8'*64; self.fixture.sign()
        second=copy.deepcopy(self.fixture.confirmation)
        def record(confirmation):
            store=OdmImportConfirmationStoreV2(self.path,trusted_loader=fixture_loader)
            try: return store.record(self.scope,confirmation).state
            except ImportConfirmationStoreHeld: return 'held'
        with ThreadPoolExecutor(max_workers=2) as pool: states=list(pool.map(record,[first,second]))
        self.assertCountEqual(states,['recorded','held'])
        retained=self.store.load_verified(self.scope).confirmation
        with patch('processing.viewer_import_confirmation_store._after_commit',side_effect=RuntimeError('lost reply')):
            with self.assertRaises(RuntimeError): self.store.record(self.scope,retained)
        self.assertEqual(OdmImportConfirmationStoreV2(self.path,trusted_loader=fixture_loader).record(self.scope,retained).state,'replayed')

    def test_cross_version_same_scope_no_supersession_or_cross_replay(self):
        v1=ViewerImportConfirmationTests('test_valid_signature_full_ordered_closure_and_exact_replay')
        v1.setUp()
        context=dict(expected=v1.expected,result_manifest=v1.manifest,accepted_envelopes=v1.envelopes,
                     trusted_import_keys=v1.import_keys,trusted_copy_keys=v1.copy_signer.trusted_keys)
        with self.store._connect() as db:
            db.execute('CREATE TABLE trusted_import_v1_fixture(job_id TEXT,generation INTEGER,environment TEXT,payload TEXT)')
            db.execute('INSERT INTO trusted_import_v1_fixture VALUES(?,?,?,?)',(*self.scope.values(),json.dumps(context)))
        def v1_loader(db, scope):
            row=db.execute('SELECT payload FROM trusted_import_v1_fixture WHERE job_id=? AND generation=? AND environment=?',
                           (scope['jobId'],scope['generation'],scope['environment'])).fetchone()
            return json.loads(row[0])
        old=ImportConfirmationStore(self.path,trusted_loader=v1_loader)
        old_first=old.record(self.scope,v1.confirmation)
        self.assertIsNone(self.store.load_verified(self.scope))
        new_first=self.store.record(self.scope,self.fixture.confirmation)
        self.assertNotEqual(old_first.sha256,new_first.sha256)
        with self.assertRaises(ImportConfirmationStoreHeld): old.record(self.scope,self.fixture.confirmation)
        with self.assertRaises(ImportConfirmationStoreHeld): self.store.record(self.scope,v1.confirmation)
        old=ImportConfirmationStore(self.path,trusted_loader=v1_loader)
        new=OdmImportConfirmationStoreV2(self.path,trusted_loader=fixture_loader)
        self.assertEqual(old.record(self.scope,v1.confirmation).state,'replayed')
        self.assertEqual(new.record(self.scope,self.fixture.confirmation).state,'replayed')
        self.assertEqual(old.load_verified(self.scope).sha256,old_first.sha256)
        self.assertEqual(new.load_verified(self.scope).sha256,new_first.sha256)


if __name__ == '__main__': unittest.main()
