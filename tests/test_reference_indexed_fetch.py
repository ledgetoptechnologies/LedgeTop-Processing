"""Fresh indexed journal ABI, bounded metadata and ordinary corruption guards."""
from contextlib import contextmanager, closing
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import os
from types import SimpleNamespace
from unittest.mock import patch
import unittest
import uuid

from ops.reference_worker_daemon import DaemonStateLock
from processing import reference_indexed_fetch as j
from processing import reference_protocol as p


class IndexedJournalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.path=self.root/'driver.sqlite'
        self.owner=DaemonStateLock(self.root);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.binding=dict(stateRoot=str(self.root),scratchRoot=str(self.root/'scratch'),physical={},policy={'safetyFloor':0})
        self.bootstrap=str(uuid.uuid4())
        self.context=j.initialize_fresh_profile(self.path,bootstrap_id=self.bootstrap,binding=self.binding,owner=self.owner)

    @contextmanager
    def db(self):
        connection=sqlite3.connect(self.path);connection.row_factory=sqlite3.Row
        try:
            with connection:
                connection.execute('BEGIN IMMEDIATE');yield connection
        finally:connection.close()

    def journal(self):
        with self.db() as db:
            db.execute('INSERT OR IGNORE INTO driver_binding VALUES(1,?)',(p.canonical_json(dict(inputCopyAccounting={'schemaVersion':1},indexedFetchJournal=j.compiled_profile_descriptor())).decode(),))
        return j.IndexedFetchJournal(self.path,self.context,self.owner,clock=lambda:100.)

    def claim(self,count=2,padding=0,job_id='job1'):
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1-fixtures.json').read_text())['submission']
        objects=[dict(id='i'+str(i).zfill(5)+'x'*122,name='P'+str(i).zfill(5)+'x'*padding+'.jpg',byteSize=1,
            sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(count)]
        fixture['manifest'].update(objects=objects,totalBytes=count,objectCount=count,inputCounts=dict(images=count,lidarObjects=0,points=0))
        fixture['manifest']['sha256']=p.manifest_sha256(fixture['manifest'])
        normalized=p.validate_submission(fixture,environment='staging',capability_fingerprint=fixture['capabilityFingerprint'])
        return dict(jobId=job_id,status='running',phase='queued',nodeId='main',generation=1,environment='staging',
            manifestSha256=normalized['manifestSha256'],pipelineDigest=normalized['pipelineDigest'],claimToken='synthetic-claim',submission=normalized)

    def seed(self,journal,claim):
        job=claim['jobId']
        with self.db() as db:
            journal.reserve_slot(db,job)
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,'request1','fetching','{}')",(job,))
            journal.seal_claim(db,job,'request1',claim)

    def test_detached_compiled_descriptor_and_fresh_readonly_context(self):
        descriptor=j.compiled_profile_descriptor();descriptor['limits']['maxObjects']=1
        self.assertEqual(j.compiled_profile_descriptor()['limits']['maxObjects'],10000)
        before=self.path.read_bytes()
        self.assertEqual(j.read_only_profile(self.path,bootstrap_id=self.bootstrap,binding=self.binding,owner=self.owner),self.context)
        self.assertEqual(self.path.read_bytes(),before)
        with self.assertRaises(j.IndexedFetchHeld):j.initialize_fresh_profile(self.path,bootstrap_id=self.bootstrap,binding=self.binding,owner=self.owner)

    def test_unbound_business_rows_are_never_adopted(self):
        with self.db() as db:db.execute("INSERT INTO indexed_quota VALUES('foreign',1)")
        with self.assertRaises(j.IndexedFetchHeld):j.read_only_profile(self.path,bootstrap_id=self.bootstrap,binding=self.binding,owner=self.owner)

    def test_disallowed_and_lone_profile_footprints(self):
        with self.assertRaises(j.IndexedFetchHeld):j.reject_indexed_footprint(self.path)
        other=self.root/'lone.sqlite'
        with closing(sqlite3.connect(other)) as db,db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT)')
            db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps({'indexedFetchJournal':{}}),))
        other.chmod(0o600)
        with self.assertRaises(j.IndexedFetchHeld):j.reject_indexed_footprint(other)
        upper=self.root/'upper.sqlite'
        with closing(sqlite3.connect(upper)) as db,db:db.execute('CREATE TABLE INDEXED_PROFILE(x)')
        upper.chmod(0o600)
        with self.assertRaises(j.IndexedFetchHeld):j.reject_indexed_footprint(upper)

    def test_immutable_objects_and_schema_drift(self):
        journal=self.journal();self.seed(journal,self.claim())
        with self.assertRaises(sqlite3.IntegrityError):
            with self.db() as db:db.execute("UPDATE indexed_objects SET id='changed'")
        with self.db() as db:db.execute('DROP TRIGGER indexed_objects_update')
        with self.assertRaises(j.IndexedFetchHeld):
            with self.db() as db:journal.check(db)

    def test_quota_holds_before_second_owned_claim(self):
        journal=self.journal()
        with self.db() as db:journal.reserve_slot(db,'job1')
        with self.assertRaisesRegex(j.IndexedFetchHeld,'quota'):
            with self.db() as db:journal.reserve_slot(db,'job2')
        with self.db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],1)

    def test_cas_attempt_budget_and_clock_rollback(self):
        journal=self.journal();claim=self.claim();self.seed(journal,claim)
        with self.db() as db:
            journal.seal_grant(db,'job1',{'fixture':'private-grant'})
            first=journal.progress(db,'job1');second,intent=journal.begin_attempt(db,'job1',0,first)
        with self.assertRaises(j.IndexedFetchHeld):
            with self.db() as db:journal.begin_attempt(db,'job1',0,first)
        journal.clock=lambda:99.
        with self.assertRaisesRegex(j.IndexedFetchHeld,'rollback'):
            with self.db() as db:journal.advance(db,'job1',0,1,second,intent)

    def test_shared_capacity_counts_actual_database_and_rollback_blocks_once(self):
        scratch=self.root/'scratch';scratch.mkdir(mode=0o700)
        probe=j.indexed_capacity_probe(self.context,self.owner,scratch)
        allocated=getattr(self.path.stat(),'st_blocks',0)*512
        self.assertEqual(probe.remaining(),j.PHYSICAL_ALLOWANCE-allocated)
        rollback=Path(str(self.path)+'-journal')
        with rollback.open('xb') as stream:stream.write(b'x'*8192);stream.flush();os.fsync(stream.fileno())
        rollback.chmod(0o600)
        actual=getattr(rollback.stat(),'st_blocks',0)*512
        with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=j.PHYSICAL_ALLOWANCE+1000)):
            self.assertEqual(probe(scratch),1000+allocated+actual)
        rollback.unlink()

    @unittest.skipUnless(Path('/dev/shm').is_dir(),'actual separate Linux tmpfs required')
    def test_separate_device_scratch_probe_does_not_charge_state_allowance(self):
        with tempfile.TemporaryDirectory(dir='/dev/shm') as temporary:
            scratch=Path(temporary);self.assertNotEqual(scratch.stat().st_dev,self.root.stat().st_dev)
            binding=dict(self.binding,scratchRoot=str(scratch));other=self.root/'separate-driver.sqlite'
            context=j.initialize_fresh_profile(other,bootstrap_id=str(uuid.uuid4()),binding=binding,owner=self.owner)
            probe=j.indexed_capacity_probe(context,self.owner,scratch)
            with patch.object(j.shutil,'disk_usage',return_value=SimpleNamespace(free=123456)):
                self.assertEqual(probe(scratch),123456)

    def test_worst_legal_near_wire_limit_with_10000_objects_and_10010_attempts_fits_charge(self):
        journal=self.journal()
        job='j'*128
        # IDs are 128 ASCII characters; tune legal names toward the unchanged 4MiB wire cap.
        low,high=0,200
        while low<high:
            middle=(low+high+1)//2
            try:claim=self.claim(10000,middle,job_id=job);fits=len(p.canonical_json(claim))<=p.MAX_BODY_BYTES-16384
            except p.ProtocolError:fits=False
            if fits:low=middle
            else:high=middle-1
        claim=self.claim(10000,low,job_id=job);envelope_bytes=len(p.canonical_json(claim))
        self.assertGreater(envelope_bytes,int(.97*p.MAX_BODY_BYTES))
        self.seed(journal,claim)
        ids=[obj['id'] for obj in claim['submission']['manifest']['objects']]
        with self.db() as db:
            request=dict(reservationId='r'*128,generation=1,expiresAt='2030-01-01T00:00:00.000Z',
                manifestSha256=claim['manifestSha256'],byteBudget=30000,nonce='n'*128)
            journal.seal_reservation(db,job,request)
            grant=dict(request,jobId=job,environment='staging',objectIds=ids,token='x'*4096)
            claim=dict(claim,transferRequest=request)
            from processing.reference_transfer import validate_grant
            validate_grant(grant,job=claim,clock=lambda:100.)
            journal.seal_grant(db,job,grant)
            attempts=[]
            for seq in range(10000):
                for attempt in range(1,4 if seq<5 else 2):
                    attempts.append((job,seq,attempt,uuid.uuid4().hex,'confirmed' if attempt==(3 if seq<5 else 1) else 'started'))
            db.executemany('INSERT INTO indexed_attempts VALUES(?,?,?,?,?)',attempts)
            db.execute('UPDATE indexed_fetch SET next=10000,bytes=10000,attempts=10010,revision=20010 WHERE job=?',(job,))
            db.execute("UPDATE driver_jobs SET data=?",(json.dumps({'bounded':'x'*8000}),))
        with self.db() as db:
            journal.validate_cold(db,job,claim)
            pages=db.execute('PRAGMA page_count').fetchone()[0]
        self.assertLessEqual(pages*j.PAGE_SIZE,j.JOB_CHARGE)
        self.assertLess(self.path.stat().st_size,j.DB_CAP)
        print(json.dumps({'oracle':'indexed-worst-legal-metadata','claimBytes':envelope_bytes,'objectCount':10000,
            'attempts':10010,'databasePages':pages,'databaseBytes':pages*j.PAGE_SIZE,'perJobCharge':j.JOB_CHARGE}),flush=True)


if __name__=='__main__':unittest.main()
