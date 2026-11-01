"""Actual synthetic coordinator DB; proposed event remains unauthenticated."""
import copy
import sqlite3
import hashlib
import json
import unittest
from unittest.mock import patch
from processing import reference_protocol as p,reference_v2_event_pin_candidate as pin,reference_v2_attempt_candidate as attempts
from tests import test_reference_store as reference_fixture

class EventPinTests(unittest.TestCase):
    def setUp(self):
        self.fixture=reference_fixture.ReferenceStoreTests();self.fixture.setUp();self.addCleanup(self.fixture.tearDown)
        self.flag=patch.object(pin,'QUALIFIED',True);self.flag.start();self.addCleanup(self.flag.stop)
        self.attempt_flag=patch.object(attempts,'QUALIFIED',True);self.attempt_flag.start();self.addCleanup(self.attempt_flag.stop)
        pin.install_offline(self.fixture.store,enabled=True)
        attempts.install_offline(self.fixture.store,enabled=True)
        job,_=self.fixture.store.submit(self.fixture.submission)
        claim=self.fixture.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance-1',claim_request_id='claim-1')
        self.claim=dict(jobId=job['jobId'],nodeId='cluster',workerId='worker',workerInstanceId='instance-1',claimToken=claim['claimToken'])
        self.attempt=attempts.create(self.fixture.store,request_id='attempt-1',claim=self.claim)
        transfer=copy.deepcopy(self.fixture.fixture['transferRequest']);transfer.update(manifestSha256=job['manifestSha256'],generation=job['generation'],byteBudget=self.fixture.submission['manifest']['totalBytes'])
        self.fixture.store.reserve(job['jobId'],transfer,node_id='cluster');self.fixture.store.update_phase(job['jobId'],'fetching')
        self.event=dict(domain='test',ledger='viewer',epoch='epoch-1',operation='op-1',previousRevision=0,previousDigest='a'*64,nextRevision=1,nextDigest='b'*64)
    def test_exact_claim_event_pin_and_replay_blocks_mutation(self):
        raw=p.canonical_json(self.event);first=pin.pin(self.fixture.store,claim=self.claim,event_raw=raw)
        self.assertEqual(first['authorization'],'unqualified');self.assertEqual(pin.pin(self.fixture.store,claim=self.claim,event_raw=raw),first)
        with self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(dict(self.event,nextDigest='c'*64)))
        with self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(dict(self.event,operation='op-2')))
        with self.assertRaises(sqlite3.IntegrityError):self.fixture.store.update_phase(self.claim['jobId'],'processing')
        with self.fixture.queue._db() as db:
            db.execute('PRAGMA recursive_triggers=OFF')
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM reference_claims WHERE job_id=?',(self.claim['jobId'],))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('DELETE FROM v2_event_pins WHERE operation=?',(self.event['operation'],))
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE v2_event_pins SET event_sha=? WHERE operation=?",('0'*64,self.event['operation']))
            row=db.execute('SELECT * FROM v2_event_pins WHERE operation=?',(self.event['operation'],)).fetchone()
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO v2_event_pins VALUES(?,?,?,?,?,?,?,?,?,?)',tuple(row))
            self.assertNotIn(self.claim['claimToken'],row['local_identity'])
            self.assertNotIn(row['claim_sha'],row['local_identity'])
            self.assertEqual(first['localIdentitySha256'],hashlib.sha256(pin.LOCAL_IDENTITY_DOMAIN+row['local_identity'].encode()).hexdigest())
            self.assertEqual(json.loads(row['local_identity'])['reservationId'],self.fixture.fixture['transferRequest']['reservationId'])
            self.assertEqual(json.loads(row['local_identity'])['attemptId'],self.attempt['attemptId'])
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO jobs SELECT * FROM jobs WHERE id=?',(self.claim['jobId'],))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO reference_jobs SELECT * FROM reference_jobs WHERE job_id=?',(self.claim['jobId'],))
            # REPLACE can delete a pinned row through a different unique key,
            # even when recursive triggers are disabled.
            for table,where,changes in (('jobs','id',{'id':"'other-id'",'request_id':"'other-request'"}),
                                        ('jobs','id',{'id':"'other-id'",'seq':'NULL'}),
                                        ('reference_jobs','job_id',{'job_id':"'other-id'"}),
                                        ('reference_claims','job_id',{'job_id':"'other-id'"})):
                columns=[row[1] for row in db.execute('PRAGMA table_info('+table+')')]
                expressions=[changes.get(column,'"'+column+'"') for column in columns]
                with self.subTest(table=table,changes=changes),self.assertRaisesRegex(sqlite3.IntegrityError,'v2 event pin held'):
                    db.execute('INSERT OR REPLACE INTO '+table+' SELECT '+','.join(expressions)+' FROM '+table+' WHERE '+where+'=?',(self.claim['jobId'],))
    def test_wrong_claim_invalid_event_and_missing_trigger_hold(self):
        for changed in (dict(self.claim,claimToken='wrong'),dict(self.claim,workerInstanceId='other')):
            with self.assertRaises(Exception):pin.pin(self.fixture.store,claim=changed,event_raw=p.canonical_json(self.event))
        for raw in (p.canonical_json(self.event)+b' ',p.canonical_json(dict(self.event,nextRevision=2)),p.canonical_json(dict(self.event,nextDigest='bad'))):
            with self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=raw)
        with self.fixture.queue._db() as db:db.execute('DROP TRIGGER v2_pin_claim_delete')
        with self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))
    def test_new_unique_index_rejected(self):
        with self.fixture.queue._db() as db:db.execute('CREATE UNIQUE INDEX extra_unique ON jobs(environment)')
        with self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))
    def test_missing_attempt_guard_holds(self):
        with self.fixture.queue._db() as db:db.execute('DROP TRIGGER v2_attempt_delete')
        with self.assertRaisesRegex(pin.PinHeld,'attempt guard missing'):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))
    def test_invalid_or_missing_transfer_holds_before_pin(self):
        with self.fixture.queue._db() as db:
            original=db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?',(self.claim['jobId'],)).fetchone()[0]
        value=json.loads(original)
        for malformed in (None,'{}',json.dumps(dict(value,generation=2)),json.dumps(dict(value,manifestSha256='0'*64)),json.dumps(dict(value,byteBudget=1)),json.dumps(dict(value,expiresAt='invalid'))):
            with self.fixture.queue._db() as db:db.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',(malformed,self.claim['jobId']))
            with self.subTest(malformed=malformed),self.assertRaises(pin.PinHeld):pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))
        with self.fixture.queue._db() as db:db.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',(original,self.claim['jobId']))
        self.assertEqual(pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))['state'],'held')
    def test_changed_reservation_is_reflected_in_local_identity(self):
        with self.fixture.queue._db() as db:
            original=db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?',(self.claim['jobId'],)).fetchone()[0]
            transfer=json.loads(original);transfer['reservationId']='different-reservation'
            db.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',(json.dumps(transfer),self.claim['jobId']))
        pin.pin(self.fixture.store,claim=self.claim,event_raw=p.canonical_json(self.event))
        with self.fixture.queue._db() as db:
            observed=json.loads(db.execute('SELECT local_identity FROM v2_event_pins').fetchone()[0])
            self.assertEqual(observed['reservationId'],'different-reservation')
