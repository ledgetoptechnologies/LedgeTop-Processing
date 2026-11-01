"""Synthetic tests for the inactive incremental invariant."""
import hashlib
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch
from tests.test_upload_intent_census import UploadCensusTests as _Base
from processing import node_mutation_broker as broker
from processing import incremental_upload_census as incremental
from processing.prototype import Queue

class IncrementalCensusTests(_Base):
    def enable_incremental(self):
        with self.queue._db() as db:incremental.install_offline(db,enabled=True)
    def test_legacy_audit_quarantines_ambiguous_and_preserves_classified_init(self):
        self.install();good=self.row('init');bad=self.row('upload_remote')
        self.enable_incremental()
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(good,)).fetchone()[0],'complete')
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(bad,)).fetchone()[0],'receipt_uncertain')
        self.queued();self.assertIsNone(self.queue.claim({'workerId':'synthetic'}))
    def test_writer_fence_and_invariant_refuse_unqualified_transitions(self):
        self.install();self.enable_incremental();operation=str(uuid.uuid4());job=str(uuid.uuid4());digest='a'*64
        with self.queue._db() as db:
            with self.assertRaises(sqlite3.IntegrityError):db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",(operation,job,digest))
            db.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(operation,job,digest))
            db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(operation,job,digest,'upload_remote'))
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=?",(operation,))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO upload_intent_kinds VALUES(?,?,?,?)',(operation,job,digest,'upload_local'))
            db.execute('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',(operation,job,str(uuid.uuid4()),digest))
            db.execute('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(operation,job,str(uuid.uuid4()),digest,'{}','b'*64))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO broker_sender_evidence VALUES(?,?,?,?)',(operation,job,str(uuid.uuid4()),digest))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO broker_node_receipts VALUES(?,?,?,?,?,?)',(operation,job,str(uuid.uuid4()),digest,'{}','b'*64))
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=?",(operation,))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM broker_intents WHERE operation_id=?',(operation,))
        raw=sqlite3.connect(self.root/'queue'/'jobs.sqlite3')
        try:
            with self.assertRaises(sqlite3.OperationalError):raw.execute("INSERT INTO broker_intents VALUES(?,?,?,'started')",(str(uuid.uuid4()),job,digest))
            with self.assertRaises(sqlite3.OperationalError):raw.execute("UPDATE broker_intents SET state='complete' WHERE operation_id=?",(operation,))
        finally:raw.close()
    def test_missing_trigger_and_missing_marker_fail_required_mode(self):
        self.install();self.enable_incremental()
        self.queued()
        with self.queue._db() as db:db.execute('DROP TRIGGER upload_guard_intent_update')
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}))
        self.queue.close()
        for _ in range(2):
            with self.assertRaises(incremental.IncrementalError):Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_incremental_upload_census=True)
    def test_missing_unresolved_index_fails_closed(self):
        self.install();self.enable_incremental();self.queued()
        with self.queue._db() as db:db.execute('DROP INDEX upload_census_unresolved')
        self.assertIsNone(self.queue.claim({'workerId':'synthetic'}));self.queue.close()
        with self.assertRaises(incremental.IncrementalError):
            Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_incremental_upload_census=True)

    def test_missing_incremental_marker_refuses_required_startup(self):
        self.install();self.queue.close()
        with self.assertRaises(Exception):Queue(self.root/'queue',node_ids=('cluster','main'),viewer_enabled=True,require_incremental_upload_census=True)
    def test_migration_refuses_empty_and_null_intent_keys(self):
        self.install()
        with self.queue._db() as db:db.execute("INSERT INTO broker_intents VALUES(?,?,?,'complete')",('',str(uuid.uuid4()),'a'*64))
        with self.assertRaises(incremental.IncrementalError):self.enable_incremental()
        with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='upload_incremental_required'").fetchone())

    def test_migration_refuses_orphan_classification(self):
        self.install()
        with self.queue._db() as db:db.execute('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',(str(uuid.uuid4()),str(uuid.uuid4()),'a'*64,'init'))
        with self.assertRaises(incremental.IncrementalError):self.enable_incremental()
        with self.queue._db() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='upload_incremental_required'").fetchone())
    def test_upgrade_can_audit_history_above_old_capacity(self):
        self.install();self.queued();intents=[];kinds=[]
        for _ in range(12001):
            operation=str(uuid.uuid4());job=str(uuid.uuid4());digest='a'*64
            intents.append((operation,job,digest,'complete'));kinds.append((operation,job,digest,'init'))
        with self.queue._db() as db:
            db.executemany('INSERT INTO broker_intents VALUES(?,?,?,?)',intents)
            db.executemany('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',kinds)
        with self.queue._db() as db:self.assertFalse(self.queue._transfer_admission_clear(db))
        self.enable_incremental()
        with self.queue._db() as db:self.assertTrue(self.queue._transfer_admission_clear(db))
        self.assertIsNotNone(self.queue.claim({'workerId':'synthetic'}))

    def test_10000_remote_history_has_indexed_constant_admission(self):
        self.install();self.queued();intents=[];kinds=[];proofs=[];receipts=[]
        for _ in range(10000):
            grant=self.upload_grant();digest=hashlib.sha256(broker._json(grant)).hexdigest();receipt,receipt_hash=broker._upload_receipt(grant,{'success':True},digest)
            intents.append((grant['operationId'],grant['jobId'],digest,'complete'));kinds.append((grant['operationId'],grant['jobId'],digest,'upload_remote'))
            proofs.append((grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest));receipts.append((grant['operationId'],grant['jobId'],grant['coordinatorEpoch'],digest,receipt,receipt_hash))
        with self.queue._db() as db:
            db.executemany('INSERT INTO broker_intents VALUES(?,?,?,?)',intents);db.executemany('INSERT INTO upload_intent_kinds VALUES(?,?,?,?)',kinds)
            db.executemany('INSERT INTO broker_sender_evidence VALUES(?,?,?,?)',proofs);db.executemany('INSERT INTO broker_node_receipts VALUES(?,?,?,?,?,?)',receipts)
        start=time.monotonic();self.enable_incremental();print('synthetic offline 10000 receipt audit seconds:',round(time.monotonic()-start,3),flush=True)
        start=time.monotonic()
        with self.queue._db() as db:
            self.assertTrue(self.queue._transfer_admission_clear(db))
            plan=' '.join(str(tuple(r)) for r in db.execute("EXPLAIN QUERY PLAN SELECT operation_id,job_id,grant_hash,state FROM broker_intents WHERE state IS NOT 'complete' LIMIT 2"))
            self.assertIn('upload_census_unresolved',plan)
        print('synthetic incremental 10000 receipt admission seconds:',round(time.monotonic()-start,4),flush=True)

class IncrementalEndToEndTests(unittest.TestCase):
    def test_real_synthetic_mtls_upload_and_receipt_replay(self):
        from tests.test_node_authority_session import NodeAuthoritySessionTests
        case=NodeAuthoritySessionTests('test_mtls_upload_uses_coordinator_descriptor_and_node_broker')
        case._enable_incremental_test=True
        result=unittest.TestResult();case.run(result)
        self.assertTrue(result.wasSuccessful(),str(result.errors)+str(result.failures))

if __name__=='__main__':unittest.main()
