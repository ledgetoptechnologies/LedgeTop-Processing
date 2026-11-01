"""Synthetic claim-observation API cache tests; no production state."""
import copy
import json
import os
import time
import unittest
from unittest.mock import patch
from processing import reference_claim_observation_cache_candidate as candidate
from processing import reference_protocol as protocol
from processing.reference_store import ReferenceStore
from tests.test_reference_driver import ReferenceDriverTests, TOKEN

class ClaimObservationCacheCandidateTests(unittest.TestCase):
    tearDown=ReferenceDriverTests.tearDown
    make_driver=ReferenceDriverTests.make_driver
    request=ReferenceDriverTests.request
    sender_open=ReferenceDriverTests.sender_open
    download=ReferenceDriverTests.download
    submit=ReferenceDriverTests.submit
    supply_grants=ReferenceDriverTests.supply_grants
    advance=ReferenceDriverTests.advance

    def setUp(self):
        original=ReferenceStore.__init__
        self.qualifier=patch.object(candidate,'QUALIFIED',True);self.qualifier.start();self.addCleanup(self.qualifier.stop)
        def initialize(store,*args,**kwargs):
            kwargs.update(claim_observation_cache=True,local_max_photos=10000)
            original(store,*args,**kwargs)
        with patch.object(ReferenceStore,'__init__',initialize):ReferenceDriverTests.setUp(self)

    def test_ten_thousand_photo_actual_api_observation_and_invalidation(self):
        job=self.submit(contents=(b'x',)*10000)
        claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        started=time.monotonic();first=self.api.handle('GET',path,headers,protected_transport=True)
        first_elapsed=time.monotonic()-started
        self.assertEqual(first.status,200)
        started=time.monotonic()
        with patch.object(protocol,'manifest_sha256',side_effect=AssertionError('full manifest was rescanned')):
            second=self.api.handle('GET',path,headers,protected_transport=True)
        cached_elapsed=time.monotonic()-started
        self.assertEqual(second.status,200);self.assertEqual(second.body,first.body)
        print('synthetic 10000-photo API observation seconds: first=%.6f cached=%.6f'%(first_elapsed,cached_elapsed),flush=True)
        with self.queue._db() as db:
            db.execute('UPDATE reference_jobs SET manifest_sha=? WHERE job_id=?',('0'*64,job['jobId']))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claim_observation_cache').fetchone()[0],0)
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,409)

    def test_live_claim_revocation_and_schema_tamper_are_rejected(self):
        job=self.submit();claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
        with self.queue._db() as db:db.execute('DELETE FROM reference_claims WHERE job_id=?',(job['jobId'],))
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,409)
        with self.queue._db() as db:db.execute('DROP TRIGGER reference_claim_cache_update')
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,409)

    def test_all_source_updates_invalidate_and_revalidate(self):
        job=self.submit();claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
        for field in ('input_json','manifest_sha','pipeline_digest','environment','generation'):
            with self.subTest(field=field):
                with self.queue._db() as db:
                    db.execute('UPDATE reference_jobs SET '+field+'='+field+' WHERE job_id=?',(job['jobId'],))
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claim_observation_cache').fetchone()[0],0)
                with patch.object(protocol,'manifest_sha256',wraps=protocol.manifest_sha256) as hashed:
                    self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
                    self.assertGreater(hashed.call_count,0)

    def test_delete_reinsert_drops_cache_and_live_status_is_observed(self):
        job=self.submit();claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
        with self.queue._db() as db:
            db.execute('PRAGMA foreign_keys=OFF')
            original=db.execute('SELECT * FROM reference_jobs WHERE job_id=?',(job['jobId'],)).fetchone()
            db.execute('DELETE FROM reference_jobs WHERE job_id=?',(job['jobId'],))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claim_observation_cache').fetchone()[0],0)
            db.execute('INSERT INTO reference_jobs VALUES('+','.join('?' for _ in original)+')',tuple(original))
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='held',held_reason='operator_hold' WHERE id=?",(job['jobId'],))
        observed=self.api.handle('GET',path,headers,protected_transport=True)
        self.assertEqual(observed.status,200);self.assertEqual(observed.body['status'],'held');self.assertEqual(observed.body['heldReason'],'operator_hold')

    def test_epoch_change_requires_fresh_full_validation(self):
        import uuid
        job=self.submit();claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
        original=self.queue.startup_id
        try:
            self.queue.startup_id=str(uuid.uuid4())
            with patch.object(protocol,'manifest_sha256',wraps=protocol.manifest_sha256) as hashed:
                self.assertEqual(self.api.handle('GET',path,headers,protected_transport=True).status,200)
                self.assertGreater(hashed.call_count,0)
        finally:self.queue.startup_id=original

    def test_first_cache_write_during_concurrent_source_update_fails_closed(self):
        import sqlite3
        import threading
        job=self.submit();claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim-cache-test')
        headers={'Authorization':'Bearer '+TOKEN,'X-Claim-Token':claim['claimToken']}
        path='/ltds/worker/v1/jobs/'+job['jobId']+'/claim-state'
        read=threading.Event();release=threading.Event();responses=[];failures=[]
        original=self.store._claim_snapshot_uncached
        def pause_after_read(*args,**kwargs):
            result=original(*args,**kwargs);read.set()
            if not release.wait(3):raise RuntimeError('synthetic barrier timed out')
            return result
        def observe():
            try:responses.append(self.api.handle('GET',path,headers,protected_transport=True))
            except BaseException as error:failures.append(type(error).__name__)
        writer=sqlite3.connect(self.queue.path,timeout=2)
        thread=threading.Thread(target=observe)
        try:
            with patch.object(self.store,'_claim_snapshot_uncached',side_effect=pause_after_read):
                thread.start();self.assertTrue(read.wait(3))
                writer.execute('BEGIN IMMEDIATE')
                writer.execute('UPDATE reference_jobs SET manifest_sha=? WHERE job_id=?',('0'*64,job['jobId']))
                release.set();thread.join(4)
                self.assertFalse(thread.is_alive())
                self.assertEqual(failures,[]);self.assertEqual(responses[0].status,500)
                writer.rollback()
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claim_observation_cache').fetchone()[0],0)
        finally:
            release.set();writer.rollback();writer.close()
            if thread.ident is not None:thread.join(4)
            self.assertFalse(thread.is_alive())

    def test_normalized_ten_thousand_metadata_one_actual_fetch_has_bounded_checkpoint(self):
        from ops.reference_worker_daemon import DaemonStateLock
        from processing.reference_driver import ReferenceDriver
        from processing import reference_transfer_journal_candidate as journal
        with patch.object(journal,'QUALIFIED',True),patch.object(journal,'NORMALIZED_QUALIFIED',True),DaemonStateLock(self.root) as owner:
            self.driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,
                'https://coordinator.invalid:9443',TOKEN,state_path=self.root/'normalized-driver.sqlite',ssl_context=self.context,
                output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,failure_outbox=self.failures,
                worker_instance_id='instance',state_owner=owner,transfer_journal=True,normalized_transfer=True)
            self.driver._request=self.request;self.driver.sender._open=self.sender_open
            job=self.submit(contents=(b'x',)*10000);self.advance(job,'fetching')
            started=time.monotonic();result=self.driver.step();elapsed=time.monotonic()-started
            self.assertEqual(result['state'],'fetching')
            with self.driver._db() as db:
                row=db.execute('SELECT length(data),data FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()
                self.assertLess(row[0],10000);self.assertEqual(json.loads(row[1])['inputCursor'],1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM transfer_journal_object WHERE state="charged"').fetchone()[0],1)
                self.assertGreater(db.execute('SELECT length(payload) FROM transfer_journal_private').fetchone()[0],1000000)
            print('synthetic 10000-photo metadata: first actual driver fetch seconds=%.6f checkpoint_bytes=%d'%(elapsed,row[0]),flush=True)

    @unittest.skipUnless(os.environ.get('LT_ENABLE_SYNTHETIC_10K_BENCHMARK') == '1', 'explicit heavy synthetic benchmark opt-in required')
    def test_normalized_all_ten_thousand_small_synthetic_inputs(self):
        import resource
        from pathlib import Path
        from ops.reference_worker_daemon import DaemonStateLock
        from processing.reference_driver import ReferenceDriver
        from processing.reference_transfer import TransferResponseUnavailable
        from processing import reference_transfer_journal_candidate as journal
        with patch.object(journal,'QUALIFIED',True),patch.object(journal,'NORMALIZED_QUALIFIED',True),DaemonStateLock(self.root) as owner:
            self.driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,
                'https://coordinator.invalid:9443',TOKEN,state_path=self.root/'normalized-driver.sqlite',ssl_context=self.context,
                output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,failure_outbox=self.failures,
                worker_instance_id='instance',state_owner=owner,transfer_journal=True,normalized_transfer=True)
            self.driver._request=self.request;self.driver.sender._open=self.sender_open
            job=self.submit(contents=(b'x',)*10000);self.advance(job,'fetching')
            attempts={};injected=[False]
            def small_fetch(grant,object_id,destination):
                attempts[object_id]=attempts.get(object_id,0)+1
                if len(attempts)==5000 and not injected[0]:
                    injected[0]=True;raise TransferResponseUnavailable('synthetic retry')
                Path(destination).write_bytes(b'x')
                return Path(destination)
            started=time.monotonic();maximum_checkpoint=0;retries=0
            with patch.object(self.broker,'download_object',side_effect=small_fetch):
                for index in range(10002):
                    if time.monotonic()-started>110: self.fail('synthetic 10k transfer exceeded 110-second work budget after %d unique attempts and %d retries'%(len(attempts),retries))
                    result=self.driver.step()
                    if result['state']=='retry_wait':retries+=1;continue
                    if result['state']=='processing_pending':break
                    self.assertEqual(result['state'],'fetching',result)
                    if index%1000==0:
                        with self.driver._db() as db:maximum_checkpoint=max(maximum_checkpoint,db.execute('SELECT length(data) FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0])
                else:self.fail('synthetic 10k transfer did not complete')
            elapsed=time.monotonic()-started
            self.assertEqual(len(attempts),10000);self.assertEqual(retries,1)
            self.assertEqual(sum(attempts.values()),10001)
            with self.driver._db() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM transfer_journal_object WHERE state='charged'").fetchone()[0],10000)
                private_bytes=db.execute('SELECT length(payload) FROM transfer_journal_private').fetchone()[0]
            print('synthetic complete 10000 one-byte inputs: seconds=%.3f peak_rss_kib=%d max_checkpoint_bytes=%d private_bytes=%d retries=%d'%(elapsed,resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,maximum_checkpoint,private_bytes,retries),flush=True)

    @unittest.skipUnless(os.environ.get('LT_ENABLE_SYNTHETIC_PROFILE') == '1', 'explicit synthetic profile opt-in required')
    def test_profile_500_fetches_under_ten_thousand_metadata(self):
        import cProfile
        import io
        import pstats
        from ops.reference_worker_daemon import DaemonStateLock
        from processing.reference_driver import ReferenceDriver
        from processing import reference_transfer_journal_candidate as journal
        with patch.object(journal,'QUALIFIED',True),patch.object(journal,'NORMALIZED_QUALIFIED',True),DaemonStateLock(self.root) as owner:
            self.driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,
                'https://coordinator.invalid:9443',TOKEN,state_path=self.root/'normalized-driver.sqlite',ssl_context=self.context,
                output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,failure_outbox=self.failures,
                worker_instance_id='instance',state_owner=owner,transfer_journal=True,normalized_transfer=True)
            self.driver._request=self.request;self.driver.sender._open=self.sender_open
            job=self.submit(contents=(b'x',)*10000);self.advance(job,'fetching')
            profiler=cProfile.Profile();started=time.monotonic();profiler.enable()
            try:
                for _ in range(500):self.assertEqual(self.driver.step()['state'],'fetching')
            finally:profiler.disable()
            elapsed=time.monotonic()-started
            with self.driver._db() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM transfer_journal_object WHERE state='charged'").fetchone()[0],500)
            report=io.StringIO();pstats.Stats(profiler,stream=report).strip_dirs().sort_stats('cumulative').print_stats(12)
            print('synthetic 500/10000 one-byte fetches: seconds=%.3f'%elapsed,report.getvalue(),flush=True)

    @unittest.skipUnless(os.environ.get('LT_ENABLE_SYNTHETIC_PROFILE') == '1', 'explicit synthetic profile opt-in required')
    def test_profile_500_batched_fetches_under_ten_thousand_metadata(self):
        import cProfile
        import io
        import pstats
        from ops.reference_worker_daemon import DaemonStateLock
        from processing.reference_driver import ReferenceDriver
        from processing import reference_transfer_journal_candidate as journal
        from processing import reference_fetch_pin_candidate as pin
        from processing import reference_fetch_batch_candidate as batch
        with patch.object(journal,'QUALIFIED',True),patch.object(journal,'NORMALIZED_QUALIFIED',True),patch.object(pin,'QUALIFIED',True),patch.object(batch,'QUALIFIED',True),DaemonStateLock(self.root) as owner:
            pin.install(self.ledger,owner,enabled=True)
            self.driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,
                'https://coordinator.invalid:9443',TOKEN,state_path=self.root/'batch-driver.sqlite',ssl_context=self.context,
                output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,failure_outbox=self.failures,
                worker_instance_id='instance',state_owner=owner,transfer_journal=True,normalized_transfer=True,fetch_pin=True,fetch_batch_size=4)
            self.driver._request=self.request;self.driver.sender._open=self.sender_open
            job=self.submit(contents=(b'x',)*10000);self.advance(job,'fetching')
            profiler=cProfile.Profile();started=time.monotonic();profiler.enable()
            try:
                for _ in range(125):self.assertEqual(self.driver.step()['state'],'fetching')
            finally:profiler.disable()
            elapsed=time.monotonic()-started
            with self.driver._db() as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM transfer_fetch_batch_items WHERE state='charged'").fetchone()[0],500)
            report=io.StringIO();pstats.Stats(profiler,stream=report).strip_dirs().sort_stats('cumulative').print_stats(12)
            print('synthetic 500/10000 one-byte batched fetches: seconds=%.3f'%elapsed,report.getvalue(),flush=True)
