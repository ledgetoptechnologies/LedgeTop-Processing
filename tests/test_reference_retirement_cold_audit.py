"""Focused cold-boundary dispatch tests; no worker, engine or source release.

These host tests check original-before-projection sequencing and fail-closed
dispatch. They do not qualify the physical v4 profile or archived job semantics;
the separate POSIX retirement fixture must exercise those real stores.
"""
from contextlib import contextmanager
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

from processing import reference_indexed_recovery as recovery
from processing import reference_metadata_retirement as retirement
from ops import reference_worker_indexed_maintenance as maintenance


class RetirementColdDispatchTests(unittest.TestCase):
    def setUp(self):
        self.policy=SimpleNamespace(policy_id=retirement.POLICY_ID,capacity=3)
        self.db=sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE witness(value TEXT)')
        self.db.execute("INSERT INTO witness VALUES('unchanged')")
        self.db.execute('CREATE TABLE readiness_tasks(uuid TEXT PRIMARY KEY)')
        self.db.execute('CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,stage TEXT)')

    def test_invalid_original_partition_is_held_before_any_projection(self):
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',side_effect=retirement.RetirementHeld('bad original')), \
                patch.object(retirement,'logical_projection') as projection:
            with self.assertRaisesRegex(ValueError,'bad original'):
                recovery.validate_business_connection(self.db,{})
        projection.assert_not_called()
        self.assertEqual(self.db.execute('SELECT value FROM witness').fetchall(),[('unchanged',)])

    def test_every_archived_job_and_active_partition_are_checked(self):
        observations=[]
        @contextmanager
        def project(db,*,job=None,partition=None):
            self.assertIs(db,self.db)
            value=object();observations.append(('projection',job,value))
            yield value
        def validate(db,binding):observations.append(('audit',db,binding))
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',return_value=('archived-a','archived-b')), \
                patch.object(retirement,'validate_archived_closures',create=True), \
                patch.object(retirement,'logical_projection',side_effect=project), \
                patch.object(recovery,'_validate_hot_business_connection',side_effect=validate):
            recovery.validate_business_connection(self.db,{'original':'binding'},state_root='/unused')
        projections=[v for v in observations if v[0]=='projection']
        audits=[v for v in observations if v[0]=='audit']
        self.assertEqual([v[1] for v in projections],[None,'archived-a','archived-b'])
        self.assertEqual([v[1] for v in audits],[v[2] for v in projections])
        self.assertTrue(all(v[2]=={'original':'binding'} for v in audits))

    def test_archived_business_failure_is_not_suppressed(self):
        @contextmanager
        def project(db,*,job=None,partition=None):yield job
        def audit(db,binding):
            if db=='bad':raise recovery.indexed.IndexedFetchHeld('archived business invalid')
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',return_value=('bad',)), \
                patch.object(retirement,'validate_archived_closures',create=True), \
                patch.object(retirement,'logical_projection',side_effect=project), \
                patch.object(recovery,'_validate_hot_business_connection',side_effect=audit):
            with self.assertRaisesRegex(ValueError,'archived business invalid'):
                recovery.validate_business_connection(self.db,{},state_root='/unused')

    def test_legacy_profile_uses_original_validator_without_archive_reads(self):
        legacy=SimpleNamespace(policy_id='indexed-worker-fetch-three-retained-v1')
        with patch.object(recovery.indexed,'policy_from_binding',return_value=legacy), \
                patch.object(recovery,'_validate_hot_business_connection') as original, \
                patch.object(retirement,'retired_jobs') as archives:
            recovery.validate_business_connection(self.db,{'legacy':True})
        original.assert_called_once_with(self.db,{'legacy':True})
        archives.assert_not_called()

    def test_original_foreign_cache_is_held_before_any_projection(self):
        self.db.execute('INSERT INTO readiness_tasks VALUES(?)',(str(uuid.uuid4()),))
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',return_value=()), \
                patch.object(retirement,'logical_projection') as projection:
            with self.assertRaises(ValueError):recovery.validate_business_connection(self.db,{})
        projection.assert_not_called()

    def test_archived_proof_requires_independent_closure_resolver_and_root(self):
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',return_value=('archived',)), \
                patch.object(retirement,'validate_archived_closures',None,create=True), \
                patch.object(retirement,'logical_projection') as projection:
            with self.assertRaises(ValueError):
                recovery.validate_business_connection(self.db,{},state_root='/unused')
        projection.assert_not_called()
        with patch.object(recovery.indexed,'policy_from_binding',return_value=self.policy), \
                patch.object(retirement,'retired_jobs',return_value=('archived',)), \
                patch.object(retirement,'validate_archived_closures',create=True) as resolve, \
                patch.object(retirement,'logical_projection') as projection:
            with self.assertRaises(ValueError):recovery.validate_business_connection(self.db,{})
        resolve.assert_not_called();projection.assert_not_called()

    def test_original_hot_plus_archived_completed_cache_is_accounted(self):
        for _ in range(5):
            job=str(uuid.uuid4())
            self.db.execute('INSERT INTO driver_jobs VALUES(?,?)',(job,'completed'))
            self.db.execute('INSERT INTO readiness_tasks VALUES(?)',(job,))
        recovery.validate_retirement_readiness(self.db,self.policy,2)
        with self.assertRaises(ValueError):recovery.validate_retirement_readiness(self.db,self.policy,1)

    def test_noncompleted_or_malformed_cache_is_not_terminal_evidence(self):
        for job in (str(uuid.uuid4()),'not-a-uuid'):
            with self.subTest(job=job):
                self.db.execute('DELETE FROM readiness_tasks');self.db.execute('DELETE FROM driver_jobs')
                self.db.execute('INSERT INTO driver_jobs VALUES(?,?)',(job,'executing'))
                self.db.execute('INSERT INTO readiness_tasks VALUES(?)',(job,))
                with self.assertRaises(ValueError):recovery.validate_retirement_readiness(self.db,self.policy,0)


class RetirementMaintenanceDispatchTests(unittest.TestCase):
    def test_original_readiness_is_audited_before_maintenance_projection(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.execute('CREATE TABLE readiness_tasks(uuid TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,stage TEXT)')
        db.execute('INSERT INTO readiness_tasks VALUES(?)',(str(uuid.uuid4()),))
        ro=SimpleNamespace(check=lambda actual:self.assertIs(actual,db))
        policy=SimpleNamespace(policy_id=retirement.POLICY_ID,capacity=3)
        with patch.object(maintenance,'_authority',return_value=({'binding':{}},{})), \
                patch.object(maintenance.j,'policy_from_binding',return_value=policy), \
                patch.object(maintenance,'_ReadOnlyJournal',return_value=ro), \
                patch.object(maintenance,'_check',return_value=(SimpleNamespace(policy=SimpleNamespace(scratch_root='/unused-scratch')),{})), \
                patch.object(retirement,'retired_jobs',return_value=()), \
                patch.object(retirement,'logical_projection') as project:
            with self.assertRaises(ValueError):maintenance.inspect_driver(db,'/unused',object())
        project.assert_not_called()

    def test_actual_profile_check_precedes_archive_or_projection(self):
        intent={'binding':{}}
        ro=SimpleNamespace(check=lambda db: (_ for _ in ()).throw(ValueError('original schema held')))
        with patch.object(maintenance,'_authority',return_value=(intent,{})), \
                patch.object(maintenance.j,'policy_from_binding',return_value=SimpleNamespace(policy_id=retirement.POLICY_ID)), \
                patch.object(maintenance,'_ReadOnlyJournal',return_value=ro), \
                patch.object(retirement,'retired_jobs') as archives, \
                patch.object(retirement,'logical_projection') as project:
            with self.assertRaisesRegex(ValueError,'original schema held'):
                maintenance.inspect_driver(object(),'/unused',object())
        archives.assert_not_called();project.assert_not_called()

    def test_legacy_maintenance_preserves_original_dispatch(self):
        db=object();root='/unused';receipts={'retained':'unchanged'}
        ro=SimpleNamespace(check=lambda actual:self.assertIs(actual,db))
        policy=SimpleNamespace(policy_id='indexed-worker-fetch-v1')
        with patch.object(maintenance,'_authority',return_value=({'binding':{}},receipts)), \
                patch.object(maintenance.j,'policy_from_binding',return_value=policy), \
                patch.object(maintenance,'_ReadOnlyJournal',return_value=ro), \
                patch.object(maintenance,'_inspect_hot_driver',return_value=('hashes','history')) as original, \
                patch.object(retirement,'retired_jobs') as archives:
            self.assertEqual(maintenance.inspect_driver(db,root,object()),('hashes','history'))
        original.assert_called_once_with(db,root,ro,policy,receipts,binding={})
        archives.assert_not_called()


class HotMaintenanceBindingTests(unittest.TestCase):
    """Exercise the extracted helper through its real driver-binding branch."""
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        with closing(sqlite3.connect(self.root/'scratch.sqlite')) as scratch:
            scratch.execute('CREATE TABLE witness(id INTEGER PRIMARY KEY)');scratch.commit()
        self.db=sqlite3.connect(':memory:');self.addCleanup(self.db.close)
        self.db.row_factory=sqlite3.Row
        self.db.executescript('''CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,claim_request TEXT,stage TEXT,data TEXT);
          CREATE TABLE indexed_claims(job TEXT PRIMARY KEY);
          CREATE TABLE indexed_quota(job TEXT PRIMARY KEY,charge INTEGER);
          CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT);
          CREATE TABLE readiness_tasks(uuid TEXT PRIMARY KEY);''')
        for table in ('indexed_objects','indexed_seals','indexed_reservations','indexed_grants','indexed_fetch','indexed_attempts'):
            self.db.execute('CREATE TABLE '+table+'(job TEXT PRIMARY KEY)')
        self.settings={'stored':'actual-row'}
        self.db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(self.settings),));self.db.commit()
        self.policy=SimpleNamespace(retained_jobs=3,capacity=3,database_cap=128*1024**2)

    def test_hot_helper_passes_explicit_authority_not_a_missing_ambient_intent(self):
        expected={'authority':'original-checked-binding'}
        # No ro.intent attribute: projected readers intentionally don't expose it.
        with patch.object(maintenance.bootstrap,'_driver_settings') as validate:
            hashes,history=maintenance._inspect_hot_driver(self.db,self.root,object(),self.policy,{},binding=expected)
        validate.assert_called_once_with(self.settings,expected,indexed=True)
        self.assertIn('driver_binding',hashes);self.assertIn('tables',history)

    def test_hot_helper_preserves_binding_validation_refusal(self):
        with patch.object(maintenance.bootstrap,'_driver_settings',side_effect=ValueError('original binding held')):
            with self.assertRaisesRegex(ValueError,'original binding held'):
                maintenance._inspect_hot_driver(self.db,self.root,object(),self.policy,{},binding={'wrong':'binding'})


class ProjectedMaintenanceReaderTests(unittest.TestCase):
    """Projection registration checks; actual physical audit is separately POSIX."""
    def setUp(self):
        from tests.test_reference_metadata_retirement import ArchiveStorageFixture
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.f=ArchiveStorageFixture(Path(self.temp.name)/'driver.sqlite');self.addCleanup(self.f.close)
        self.db=self.f.db;self.db.execute('BEGIN')
        self.original=maintenance._ReadOnlyJournal({},Path(self.temp.name))

    def test_registered_reader_physically_checks_only_original_not_memory(self):
        with retirement.validated_partition(self.db) as token, \
                retirement.logical_projection(self.db,partition=token) as logical, \
                patch.object(maintenance._ReadOnlyJournal,'check',autospec=True) as physical:
            self.assertEqual(logical.execute('PRAGMA journal_mode').fetchone()[0],'memory')
            reader=maintenance._ProjectedBusinessJournal(self.original,self.db,logical,token)
            reader.check(logical)
            self.assertTrue(physical.call_count>=2)
            self.assertTrue(all(call.args[1] is self.db for call in physical.call_args_list))

    def test_foreign_or_escaped_projection_never_becomes_a_journal(self):
        with closing(sqlite3.connect(':memory:')) as foreign, \
                retirement.validated_partition(self.db) as token, \
                retirement.logical_projection(self.db,partition=token) as logical, \
                patch.object(maintenance._ReadOnlyJournal,'check',autospec=True):
            reader=maintenance._ProjectedBusinessJournal(self.original,self.db,logical,token)
            with self.assertRaises(ValueError):reader.check(foreign)
            with self.assertRaises(ValueError):
                maintenance._ProjectedBusinessJournal(self.original,self.db,foreign,token)
        with patch.object(maintenance._ReadOnlyJournal,'check',autospec=True):
            with self.assertRaises(ValueError):reader.check(logical)

    def test_changed_then_read_only_projection_is_still_rejected(self):
        with retirement.validated_partition(self.db) as token, \
                retirement.logical_projection(self.db,partition=token) as logical, \
                patch.object(maintenance._ReadOnlyJournal,'check',autospec=True):
            reader=maintenance._ProjectedBusinessJournal(self.original,self.db,logical,token)
            logical.execute('PRAGMA query_only=OFF')
            logical.execute("UPDATE driver_jobs SET data='{}'")
            logical.execute('PRAGMA query_only=ON')
            with self.assertRaises(ValueError):reader.check(logical)

    def test_temp_view_cannot_shadow_a_registered_business_table(self):
        with retirement.validated_partition(self.db) as token, \
                retirement.logical_projection(self.db,partition=token) as logical, \
                patch.object(maintenance._ReadOnlyJournal,'check',autospec=True):
            reader=maintenance._ProjectedBusinessJournal(self.original,self.db,logical,token)
            before=(logical.total_changes,logical.execute('PRAGMA main.schema_version').fetchone()[0])
            logical.execute('PRAGMA query_only=OFF')
            logical.execute("CREATE TEMP VIEW driver_jobs AS SELECT * FROM main.driver_jobs WHERE 0")
            logical.execute('PRAGMA query_only=ON')
            self.assertEqual(before,(logical.total_changes,logical.execute('PRAGMA main.schema_version').fetchone()[0]))
            self.assertEqual(logical.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0],0)
            with self.assertRaises(ValueError):reader.check(logical)


@unittest.skipUnless(os.name=='posix','actual private POSIX retained stores required')
class RetirementActualStoreColdTests(unittest.TestCase):
    """Logical cold audits with real stores; no daemon bootstrap/budget claim."""
    def setUp(self):
        from tests.test_reference_metadata_retirement import CompletedV4Fixture
        self.f=CompletedV4Fixture()
        self.f.complete_driver_binding=self._testMethodName=='test_full_retiring_maintenance_wrapper_audits_actual_active_and_archive_bindings'
        self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.job=self.f.complete();self.f.archive_for_storage_test(self.job)

    def files(self):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.f.root.glob('*.sqlite')}

    @contextmanager
    def original(self):
        with closing(sqlite3.connect(self.f.path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            db.execute('BEGIN')
            # Physical/schema authority is checked on the ORIGINAL file, before
            # passing only logical inputs to the pure business projections.
            recovery.indexed.validate_profile_connection(db,bootstrap_id=self.f.context.bootstrap_id,
                binding=self.f.binding,root_identity=self.f.context.root_identity,
                driver_identity=self.f.context.driver_identity)
            yield db

    def cold_audit(self):
        with self.original() as db:
            recovery.validate_business_connection(db,self.f.binding,state_root=self.f.root)

    def test_archived_actual_store_cold_and_terminal_audits_are_read_only(self):
        before=self.files();self.cold_audit()
        ro=maintenance._ReadOnlyJournal(dict(bootstrapId=self.f.context.bootstrap_id,
            binding=self.f.binding),self.f.root)
        with self.original() as db,closing(sqlite3.connect((self.f.root/'scratch.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as scratch:
            scratch.row_factory=sqlite3.Row;scratch.execute('BEGIN')
            with retirement.validated_partition(db) as token:
                retirement.validate_archived_closures(db,state_root=self.f.root,binding=self.f.binding,partition=token)
                with retirement.logical_projection(db,job=self.job['jobId'],partition=token) as logical:
                    logical.row_factory=sqlite3.Row
                    row=logical.execute('SELECT * FROM driver_jobs').fetchone()
                    projected=maintenance._ProjectedBusinessJournal(ro,db,logical,token,job=self.job['jobId'])
                    maintenance.audit_terminal_job(logical,projected,row,scratch)
        self.assertEqual(before,self.files())
        self.assertIsNone(self.f.f.ledger.release_authority)

    def test_full_retiring_maintenance_wrapper_audits_actual_active_and_archive_bindings(self):
        # Low-level actual completed-store fixture, not normal daemon authority.
        before=self.files();intent=dict(bootstrapId=self.f.context.bootstrap_id,binding=self.f.binding)
        ro=maintenance._ReadOnlyJournal(intent,self.f.root)
        with self.original() as db:
            db.row_factory=sqlite3.Row;ro.check(db)
            with retirement.validated_partition(db) as token:
                hashes,history=maintenance._inspect_retiring_driver(db,self.f.root,ro,
                    self.f.context.policy,{},intent,retirement,token,actual_scratch_root=self.f.scratch)
        self.assertIn('driver_binding',hashes)
        self.assertEqual(history['profile'],retirement.POLICY_ID)
        self.assertEqual(history['archives']['jobs'],1)
        self.assertEqual(before,self.files())

    def test_changed_original_worker_delivery_is_not_hidden_by_archive_hash(self):
        with closing(sqlite3.connect(self.f.root/'worker.sqlite')) as db:
            db.execute("UPDATE executions SET phase='prepared' WHERE job=?",(self.job['jobId'],));db.commit()
        before=self.files()
        with self.assertRaises(ValueError):self.cold_audit()
        self.assertEqual(before,self.files())

    def test_missing_original_retained_input_holds_without_file_cleanup(self):
        with closing(sqlite3.connect(self.f.root/'scratch.sqlite')) as db:
            relative=db.execute('SELECT path FROM input_copies LIMIT 1').fetchone()[0]
        path=self.f.scratch/relative;held=path.with_name(path.name+'.held-test-copy')
        original=path.read_bytes();path.rename(held)
        before=self.files()
        with self.assertRaises((ValueError,OSError)):self.cold_audit()
        self.assertEqual(before,self.files());self.assertEqual(held.read_bytes(),original)

    def test_changed_actual_coordinator_receipt_holds_despite_sealed_archive(self):
        with closing(sqlite3.connect(self.f.root/'outbox.sqlite')) as db:
            identifier,raw=db.execute('SELECT id,receipt FROM completions LIMIT 1').fetchone()
            receipt=json.loads(raw);receipt['storageReceiptId']='different-storage-receipt'
            db.execute('UPDATE completions SET receipt=? WHERE id=?',
                (json.dumps(receipt,sort_keys=True,separators=(',',':')),identifier));db.commit()
        before=self.files()
        with self.assertRaises(ValueError):self.cold_audit()
        self.assertEqual(before,self.files())

    def test_changed_actual_viewer_coverage_is_not_a_release_or_retirement_proof(self):
        with closing(sqlite3.connect(self.f.root/'scratch.sqlite')) as db:
            identifier,raw=db.execute('SELECT id,payload FROM receipts LIMIT 1').fetchone()
            receipt=json.loads(raw);receipt['accepted_object_ids']=[]
            db.execute('UPDATE receipts SET payload=? WHERE id=?',
                (json.dumps(receipt,sort_keys=True,separators=(',',':')),identifier));db.commit()
        before=self.files()
        with self.assertRaises(ValueError):self.cold_audit()
        self.assertEqual(before,self.files())

    def test_orphan_original_hot_row_holds_before_projection(self):
        with closing(sqlite3.connect(self.f.path)) as db:
            db.execute('INSERT INTO indexed_objects VALUES(?,?,?,?)',('orphan',0,'orphan-object','{}'));db.commit()
        before=self.files()
        with patch.object(retirement,'logical_projection') as projected:
            with self.assertRaises(ValueError):self.cold_audit()
        projected.assert_not_called();self.assertEqual(before,self.files())


if __name__=='__main__':unittest.main()
