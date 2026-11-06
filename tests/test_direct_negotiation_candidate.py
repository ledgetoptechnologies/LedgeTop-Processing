"""Isolated profile selection only; no authenticated network or reservation."""
import copy
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
import uuid
from unittest.mock import patch
from processing import direct_negotiation_candidate as n, direct_limits as limits, reference_protocol as p

ID='00000000-0000-4000-8000-000000000001'
class NegotiationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'candidate.sqlite'
        self.worker=dict(authorityId=ID,nodeId='node',workerId='worker',workerInstanceId='instance')
        self.req=dict(operationId=ID,principalId='viewer',jobId=ID,attemptId=ID,reservationId=ID,generation=1,
            environment='staging',manifestSha256='a'*64,derivedManifestSha256='b'*64,
            profile=limits.PROFILE,**self.worker)
        self.cap=dict(profile=limits.PROFILE,maxSource=10000,maxDerived=10000,maxManifestBytes=16*1024*1024,**self.worker)
        self.guard=lambda db,req,cap: True # synthetic stub; not deployed freeze/freshness
    def run_select(self,req=None,cap=None,principal='viewer',worker=None,guard=None):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('PRAGMA synchronous=FULL')
            return n.negotiate(db,p.canonical_json(self.req if req is None else req),
                p.canonical_json(self.cap if cap is None else cap),authenticated_principal=principal,
                authenticated_worker=self.worker if worker is None else worker,admission_guard=self.guard if guard is None else guard)
    def test_selection_persists_exact_replay_and_never_grants_custody(self):
        first=self.run_select();row=json.loads(first)
        self.assertEqual(row['state'],'selected_not_reserved')
        self.assertFalse(row['sourceCustodyAccepted']);self.assertFalse(row['transferAuthorized'])
        self.assertEqual(first,self.run_select())
    def test_changed_request_or_capability_cannot_downgrade_or_rebind(self):
        first=self.run_select()
        for changed_req,changed_cap in ((dict(self.req,profile='legacy'),self.cap),
            (dict(self.req,manifestSha256='c'*64),self.cap),(self.req,dict(self.cap,maxSource=9999)),
            (self.req,dict(self.cap,maxSource=10001))):
            with self.subTest(request=changed_req,capability=changed_cap):
                with self.assertRaises(n.NegotiationHeld):self.run_select(changed_req,changed_cap)
        self.assertEqual(first,self.run_select())
    def test_principal_and_worker_are_trusted_inputs_not_payload_claims(self):
        with self.assertRaises(n.NegotiationHeld):self.run_select(principal='other')
        with self.assertRaises(n.NegotiationHeld):self.run_select(worker=dict(self.worker,workerInstanceId='other'))
        with self.assertRaises(n.NegotiationHeld):self.run_select(cap=dict(self.cap,workerId='other'))
    def test_raw_bounds_depth_duplicates_and_noncanonical_numbers(self):
        for raw in (b' '*4097,b'{"a":1,"a":2}',b'{"a":[[[[0]]]]}',b'{"a":1.0}',
                    b'{"a":NaN}',b'{"a":1}\n',b'{"a":"\\u0061"}',b'\xff'):
            with self.subTest(raw=raw[:40]):
                with self.assertRaises(n.NegotiationHeld):n._decode(raw,n.MAX_REQUEST_BYTES)
    def test_no_partial_row_after_injected_insert_failure(self):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('''CREATE TABLE direct_negotiation_candidate (principal TEXT NOT NULL,operation TEXT NOT NULL,
                request BLOB NOT NULL,capability BLOB NOT NULL,response BLOB NOT NULL,PRIMARY KEY(principal,operation))''')
            db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON direct_negotiation_candidate BEGIN SELECT RAISE(ABORT, 'injected'); END")
            db.commit()
            with self.assertRaises(sqlite3.IntegrityError):
                n.negotiate(db,p.canonical_json(self.req),p.canonical_json(self.cap),
                    authenticated_principal='viewer',authenticated_worker=self.worker,admission_guard=self.guard)
            self.assertEqual(db.execute('SELECT count(*) FROM direct_negotiation_candidate').fetchone()[0],0)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('DROP TRIGGER fail_insert');db.commit()
        self.assertEqual(json.loads(self.run_select())['state'],'selected_not_reserved')

    def test_guard_rechecked_on_replay_and_refuses_freeze_or_stale_worker(self):
        first=self.run_select();calls=[]
        def fail(db,req,cap):
            calls.append(db.in_transaction);return False
        with self.assertRaisesRegex(n.NegotiationHeld,'freshness'):
            self.run_select(guard=fail)
        self.assertEqual(calls,[True]);self.assertEqual(first,self.run_select())
        with self.assertRaises(n.NegotiationHeld):self.run_select(guard=False)

    def test_bounded_rows_and_bytes_hold_without_discarding_replay(self):
        first=self.run_select()
        next_req=dict(self.req,operationId='00000000-0000-4000-8000-000000000002')
        with patch.object(n,'MAX_SELECTIONS',1):
            with self.assertRaisesRegex(n.NegotiationHeld,'capacity'):self.run_select(next_req)
            self.assertEqual(first,self.run_select())
        with patch.object(n,'MAX_STORAGE_BYTES',1):
            with self.assertRaisesRegex(n.NegotiationHeld,'capacity'):self.run_select(next_req)
            self.assertEqual(first,self.run_select())

class IntegratedFreezeCensusTests(NegotiationTests):
    def setUp(self):
        super().setUp()
        from processing.prototype import Queue
        from processing.reference_store import ReferenceStore
        self.queue=Queue(str(Path(self.temp.name)/'queue'),node_ids=('cluster',),reference_enabled=True)
        self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
    def selected(self,request=None,guard=None):
        return n.negotiate_in_coordinator(self.store,p.canonical_json(self.req if request is None else request),
            p.canonical_json(self.cap),authenticated_principal='viewer',authenticated_worker=self.worker,
            admission_guard=self.guard if guard is None else guard)
    def test_real_freeze_and_shared_census(self):
        from processing.coordinator_freeze import CoordinatorFrozen
        with self.queue._db() as db:before=self.store._metadata_used_bytes(db)
        first=self.selected()
        with self.queue._db() as db:after=self.store._metadata_used_bytes(db)
        self.assertGreater(after,before)
        self.store.max_metadata_bytes=after
        with self.assertRaisesRegex(Exception,'quota'):
            self.selected(dict(self.req,operationId='00000000-0000-4000-8000-000000000002'))
        self.assertEqual(first,self.selected())
        self.store.max_metadata_bytes=64*1024*1024
        gate=self.queue._coordinator_freeze
        gate.transition(dict(requestId=str(uuid.uuid4()),planId=str(uuid.uuid4()),action='intent',
            expectedRevision=gate.status()['revision'],startupId=self.queue.startup_id),owner_secret='synthetic-owner')
        with self.assertRaises(CoordinatorFrozen):self.selected()
    def test_changed_schema_fails_closed(self):
        from processing.maintenance import Conflict
        with self.queue._db() as db:
            db.execute('CREATE TABLE direct_negotiation_candidate (principal TEXT, operation TEXT, request BLOB, capability BLOB, response BLOB)')
        with self.queue._db() as db:
            with self.assertRaisesRegex(Conflict,'schema requires inspection'):self.store._metadata_used_bytes(db)
        with self.assertRaises(Conflict):self.selected()
    def test_selection_replays_after_coordinator_restart_with_fresh_guard(self):
        from processing.prototype import Queue
        from processing.reference_store import ReferenceStore
        first=self.selected();self.queue.close()
        self.queue=Queue(str(Path(self.temp.name)/'queue'),node_ids=('cluster',),reference_enabled=True)
        self.addCleanup(self.queue.close);self.store=ReferenceStore(self.queue)
        calls=[]
        self.assertEqual(first,self.selected(guard=lambda db,req,cap: calls.append(db.in_transaction) or True))
        self.assertEqual(calls,[True])
        with self.assertRaises(n.NegotiationHeld):self.selected(guard=lambda db,req,cap:False)

if __name__=='__main__':unittest.main()
