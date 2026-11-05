"""Synthetic exact-declaration validator faults; no deployment qualification."""
import copy
import hashlib
import json
import os
import sqlite3
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from ops import coordinator_candidate_projection as v
from ops import coordinator_predecessor_census as old
from tests import test_coordinator_predecessor_census as closed_fixture


class FreezeDeclarationTests(unittest.TestCase):
    def arguments(self):
        authority,plan,bootstrap,current=[str(uuid.uuid4()) for _ in range(4)]
        return dict(authority_id=authority,plan_id=plan,owner_sha256='a'*64,
            bootstrap_startup_id=bootstrap,current_startup_id=current,
            request_ids={action:str(uuid.uuid5(uuid.UUID(plan),action)) for action in ('intent','drain','seal')},
            operation_binding={'authorityId':authority,'independentlyObserved':'fixture'},
            reserved_control_bytes=3000,current_closed=False)

    def test_receipts_reconstructed_from_independent_request_identities(self):
        args=self.arguments();rows=v.freeze_expectations(**args)
        self.assertEqual(rows['coordinator_freeze_state'][0]['revision'],3)
        self.assertEqual(rows['coordinator_operation_pins'],[])
        for index,row in enumerate(rows['coordinator_freeze_receipts']):
            request=json.loads(row['request_json']);receipt=json.loads(row['receipt_json'])
            self.assertEqual(request['expectedRevision'],index)
            self.assertEqual(receipt['revision'],index+1)
            self.assertEqual(row['request_sha256'],hashlib.sha256(row['request_json'].encode()).hexdigest())
        self.assertEqual([r['state'] for r in rows['coordinator_operation_epochs']],['closed','active'])

    def test_foreign_authority_or_unbounded_reservation_is_rejected(self):
        for mutation in ({'operation_binding':{'authorityId':str(uuid.uuid4())}},
                         {'reserved_control_bytes':8192}, {'reserved_control_bytes':True}):
            with self.subTest(mutation=mutation):
                args=self.arguments();args.update(mutation)
                with self.assertRaises(old.CensusRefused):v.freeze_expectations(**args)


@unittest.skipUnless(os.name=='posix','Linux custody ownership')
class CandidateProjectionTests(unittest.TestCase):
    def setUp(self):
        self.fixture=closed_fixture.ClosedPredecessorCensusTests();self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.baseline=self.fixture.census()
        with self.fixture.db() as db:
            old_columns={name:[r[1] for r in db.execute('PRAGMA table_info("'+name+'")')]
                         for name in self.baseline['tables']}
            db.execute('ALTER TABLE nodes ADD COLUMN target_engine_version TEXT')
            db.execute('CREATE TABLE coordinator_fixture_startup(singleton INTEGER PRIMARY KEY, epoch TEXT NOT NULL)')
            db.execute('INSERT INTO coordinator_fixture_startup VALUES(1,?)',('synthetic-known-epoch',))
            objects=[list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        phase=dict(objects=objects,rows={'coordinator_fixture_startup':[{'singleton':1,'epoch':{'binding':'epoch'}}]},files={},directories={})
        self.policy=dict(schemaVersion=1,profile=v.PROFILE,candidateImageId='sha256:'+'a'*64,
             candidateSourceCommit='b'*40,oldColumns=old_columns,
             additionalColumns={'nodes':{'target_engine_version':None}},bindingNames=['epoch'],
             phases={name:copy.deepcopy(phase) for name in v.PHASES})

    def verify(self,**changes):
        wire=old._canonical(self.policy)
        args=dict(baseline=self.baseline,policy_bytes=wire,expected_policy_sha256=hashlib.sha256(wire).hexdigest(),
             expected_baseline_sha256=hashlib.sha256(old._canonical(self.baseline)).hexdigest(),
             key=self.fixture.key,phase='bootstrap-frozen',gate_binding={'epoch':'synthetic-known-epoch'},
             uid=os.geteuid(),max_metadata_bytes=1048576,max_artifact_bytes=16*1048576)
        args.update(changes)
        return v.verify_projection(self.fixture.root,**args)

    def test_exact_additive_schema_and_full_legacy_projection(self):
        result=self.verify();self.assertTrue(result['retainedFilesValidated'])
        self.assertFalse(result['writerExclusionProven'])
        self.assertEqual(result['baselineSha256'],hashlib.sha256(old._canonical(self.baseline)).hexdigest())

    def test_in_memory_schema_avoids_target_contract_file(self):
        schema=old.SCHEMA_PATH.read_bytes()
        with patch.object(Path,'read_bytes',side_effect=AssertionError('no target contract file')):
            self.assertTrue(self.verify(reviewed_schema_bytes=schema)['retainedFilesValidated'])
        with self.assertRaisesRegex(old.CensusRefused,'schema_artifact_changed'):
            self.verify(reviewed_schema_bytes=b'{}')

    def test_diagnostic_cannot_be_promoted(self):
        probe=copy.deepcopy(self.baseline);probe['retainedFilesValidated']=False
        with self.assertRaisesRegex(old.CensusRefused,'full_predecessor'):self.verify(baseline=probe)

    def test_candidate_schema_cannot_supply_its_own_allowlist(self):
        self.fixture.mutate('CREATE TABLE unexpected_secret(value TEXT)')
        with self.assertRaisesRegex(old.CensusRefused,'undeclared_candidate_schema'):self.verify()

    def test_legacy_history_change_refused(self):
        self.fixture.mutate("UPDATE jobs SET result='changed' WHERE rowid=(SELECT MIN(rowid) FROM jobs)")
        with self.assertRaisesRegex(old.CensusRefused,'legacy_rows_changed'):self.verify()

    def test_added_column_must_be_exact_null(self):
        self.fixture.mutate("UPDATE nodes SET target_engine_version='unexpected'")
        with self.assertRaisesRegex(old.CensusRefused,'added_column'):self.verify()

    def test_startup_binding_mismatch_refused(self):
        with self.assertRaisesRegex(old.CensusRefused,'startup_rows'):
            self.verify(gate_binding={'epoch':'foreign-epoch'})

    def test_retained_file_mutation_refused(self):
        target=next(p for p in self.fixture.root.rglob('*') if p.is_file() and p.name=='all.zip')
        target.write_bytes(b'changed')
        with self.assertRaisesRegex(old.CensusRefused,'file_changed'):self.verify()

    def test_unknown_file_and_empty_directory_refused(self):
        target=self.fixture.root/'unknown';target.mkdir(mode=0o700)
        with self.assertRaisesRegex(old.CensusRefused,'directory'):self.verify()

    def test_declared_startup_table_cannot_hide_payload(self):
        with self.fixture.db() as db:
            db.execute('CREATE TABLE source_releases(id TEXT)');db.execute("INSERT INTO source_releases VALUES('bad')")
            objects=[list(r) for r in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
        for phase in self.policy['phases'].values():phase['objects']=objects
        for phase in self.policy['phases'].values():phase['rows']['source_releases']=[{'id':'bad'}]
        with self.assertRaisesRegex(old.CensusRefused,'new_payload_before_cutover'):self.verify()

    def test_policy_changed_before_database_open(self):
        with patch.object(sqlite3,'connect',side_effect=AssertionError('must not connect')):
            with self.assertRaisesRegex(old.CensusRefused,'digest_changed'):self.verify(expected_policy_sha256='0'*64)

    def test_physical_budget_before_database_connect(self):
        with open(self.fixture.path,'ab') as output:output.truncate(110*1048576)
        real=sqlite3.connect
        def bounded(path,*args,**kwargs):
            if path!=':memory:':raise AssertionError('oversized live database opened')
            return real(path,*args,**kwargs)
        with patch.object(sqlite3,'connect',side_effect=bounded):
            with self.assertRaisesRegex(old.CensusRefused,'physical_budget'):self.verify(max_metadata_bytes=1)
