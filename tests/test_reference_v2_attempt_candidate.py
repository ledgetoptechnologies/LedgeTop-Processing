"""Owned synthetic coordinator database; no network or authority."""
import unittest
import sqlite3
from unittest.mock import patch
from processing import reference_v2_attempt_candidate as a
from tests import test_reference_store as fixture

class AttemptTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.ReferenceStoreTests();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.flag=patch.object(a,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
        a.install_offline(self.f.store,enabled=True)
        job,_=self.f.store.submit(self.f.submission)
        record=self.f.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance-1',claim_request_id='claim-1')
        self.claim=dict(jobId=job['jobId'],nodeId='cluster',workerId='worker',workerInstanceId='instance-1',claimToken=record['claimToken'])
    def test_exact_attempt_replay_and_changed_claim_hold(self):
        first=a.create(self.f.store,request_id='attempt-request-1',claim=self.claim)
        self.assertEqual(a.create(self.f.store,request_id='attempt-request-1',claim=self.claim),first)
        self.assertEqual(first['authorization'],'unqualified');self.assertNotIn(self.claim['claimToken'],str(first))
        with self.assertRaises(a.AttemptHeld):a.create(self.f.store,request_id='different-request',claim=self.claim)
        with self.assertRaises(Exception):a.create(self.f.store,request_id='attempt-request-1',claim=dict(self.claim,claimToken='wrong'))
        with self.f.queue._db() as db:
            db.execute('PRAGMA recursive_triggers=OFF')
            row=db.execute('SELECT * FROM v2_attempts').fetchone()
            for sql,args in (('DELETE FROM v2_attempts',()),("UPDATE v2_attempts SET identity='bad'",()),('INSERT OR REPLACE INTO v2_attempts VALUES(?,?,?,?,?)',tuple(row))):
                with self.assertRaisesRegex(sqlite3.IntegrityError,'v2 attempt immutable'):db.execute(sql,args)
    def test_attempt_does_not_block_reservation_and_never_reissues(self):
        first=a.create(self.f.store,request_id='attempt-request-1',claim=self.claim)
        import copy
        transfer=copy.deepcopy(self.f.fixture['transferRequest']);transfer.update(manifestSha256=self.f.submission['manifestSha256'],generation=1,byteBudget=self.f.submission['manifest']['totalBytes'])
        self.f.store.reserve(self.claim['jobId'],transfer,node_id='cluster')
        self.assertEqual(a.create(self.f.store,request_id='attempt-request-1',claim=self.claim),first)
        with self.assertRaises(a.AttemptHeld):a.create(self.f.store,request_id='different',claim=self.claim)
        with self.f.queue._db() as db:self.assertEqual(db.execute('SELECT attempt_id FROM v2_attempts').fetchone()[0],first['attemptId'])
    def test_added_unique_index_holds_cooperative_entry(self):
        a.create(self.f.store,request_id='attempt-request-1',claim=self.claim)
        with self.f.queue._db() as db:
            db.execute('PRAGMA recursive_triggers=OFF');db.execute('CREATE UNIQUE INDEX extra_attempt_identity ON v2_attempts(identity)')
            row=db.execute('SELECT * FROM v2_attempts').fetchone()
            with self.assertRaisesRegex(sqlite3.IntegrityError,'v2 attempt immutable'):
                db.execute('INSERT OR REPLACE INTO v2_attempts VALUES(?,?,?,?,?)',('other-request','other-job','other-attempt',row['claim_sha'],row['identity']))
        with self.assertRaisesRegex(a.AttemptHeld,'attempt index changed'):
            a.create(self.f.store,request_id='attempt-request-1',claim=self.claim)
