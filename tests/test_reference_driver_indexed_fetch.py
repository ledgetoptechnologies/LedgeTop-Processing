"""Actual one-object driver steps and SQLite writes for the explicit fresh ABI."""
import json
import os
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch
import uuid
from types import SimpleNamespace

from ops.reference_worker_daemon import DaemonStateLock
from processing.reference_driver import ReferenceDriver
from processing import reference_indexed_fetch as journal
from tests import test_reference_driver_input_copies as inputs_fixture
from tests import test_reference_driver as base_fixture


class IndexedDriverTests(unittest.TestCase):
    def setUp(self):
        self.fixture=inputs_fixture.InputCopyDriverTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups);self.f=self.fixture.f;self.downloads=self.fixture.downloads
        self.owner=DaemonStateLock(self.f.root);self.owner.__enter__();self.addCleanup(self.owner.__exit__,None,None,None)
        self.path=self.f.root/'indexed-driver.sqlite'
        self.binding=dict(stateRoot=str(self.f.root),scratchRoot=str(self.f.scratch),
            physical=dict(nodeId='cluster',workerId='worker',workerInstanceId='instance',
                authorityId=self.f.queue.authority_id,engineId='engine',gateGeneration=1),
            policy=dict(safetyFloor=1024))
        self.context=journal.initialize_fresh_profile(self.path,bootstrap_id=str(uuid.uuid4()),binding=self.binding,owner=self.owner)
        self.f.ledger.probe=journal.indexed_capacity_probe(self.context,self.owner,self.f.scratch)
        self.f.driver=self.make_driver()

    def make_driver(self):
        driver=ReferenceDriver(self.f.worker,self.f.ledger,self.f.outbox,self.f.broker,
            'https://coordinator.invalid:9443',base_fixture.TOKEN,state_path=self.path,ssl_context=self.f.context,
            output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,failure_outbox=self.f.failures,
            worker_instance_id='instance',input_copy_accounting=True,indexed_fetch_context=self.context,state_owner=self.owner)
        driver._request=self.f.request;driver.sender._open=self.f.sender_open
        return driver

    def fetching(self,contents=(b'first',b'second')):
        job=self.f.submit(contents=contents);self.f.advance(job,'fetching');return job

    def progress(self,job):
        with self.f.driver._db() as db:return self.f.driver._indexed.progress(db,job['jobId'])

    def test_one_object_progress_uses_bounded_projection_and_real_broker(self):
        job=self.fetching()
        for count in (1,2):
            result=self.f.driver.step();self.assertEqual(result['state'],'fetching')
            self.assertTrue(self.f.driver.immediate_fetch_progress(result))
            self.assertFalse(self.f.driver.immediate_fetch_progress(dict(result)))
            self.assertEqual(len(self.downloads),count);self.assertEqual(self.progress(job)['next'],count)
        self.assertEqual(self.f.driver.step()['state'],'processing_pending')
        with self.f.driver._db() as db:
            raw=db.execute('SELECT data FROM driver_jobs').fetchone()[0]
        self.assertLess(len(raw.encode()),8192)
        data=json.loads(raw);self.assertNotIn('submission',data['claim']);self.assertNotIn('inputPaths',data)
        self.assertNotIn('grant',data);self.assertNotIn('fetchAttempts',data)

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocated receiver required')
    def test_opt_in_driver_uses_real_indexed_broker_into_allocated_inode(self):
        self.f.ledger.allocated_receiver=True
        self.f.ledger.enable_allocated_receiver()
        job=self.fetching(contents=(b'first',))
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(len(self.downloads),1)
        self.assertEqual(self.progress(job)['next'],1)
        self.assertEqual(self.f.driver.step()['state'],'processing_pending')

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocated receiver required')
    def test_opt_in_driver_interruption_holds_without_second_broker_get(self):
        self.f.ledger.allocated_receiver=True
        self.f.ledger.enable_allocated_receiver()
        job=self.fetching(contents=(b'first',))
        def lost(grant,object_id,fd):
            os.pwrite(fd,b'f',0)
            raise OSError('synthetic interruption')
        with patch.object(self.f.broker,'download_indexed_object_fd',side_effect=lost) as send:
            first=self.f.driver.step()
            self.assertIn(first['state'],('retry_wait','held'))
            self.assertEqual(send.call_count,1)
            self.f.driver=self.make_driver()
            self.assertEqual(self.f.driver.step()['state'],'held')
            self.assertEqual(send.call_count,1)
        self.assertEqual(self.progress(job)['next'],0)

    def test_success_materializes_at_execution_boundary_and_retains_inputs(self):
        job=self.f.submit();self.f.advance(job,'awaiting_acceptance')
        self.assertEqual(self.f.queue.get(job['jobId'])['status'],'succeeded')
        self.assertEqual(self.f.ledger.input_cleanup_candidates(),[])
        self.assertEqual(len(list(self.f.scratch.glob('jobs/*/*.input'))),1)

    def test_cold_resume_revalidates_published_object_without_new_get(self):
        job=self.fetching(contents=(b'once',));original=self.f.broker.download_indexed_object
        def lost(*args):original(*args);raise OSError('synthetic lost publication result')
        with patch.object(self.f.broker,'download_indexed_object',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(len(self.downloads),1);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'fetching');self.assertEqual(len(self.downloads),1)
        self.assertEqual(self.progress(job)['next'],1)

    def test_confirmation_commit_loss_reconciles_without_duplicate_download(self):
        job=self.fetching(contents=(b'confirmed',));original=self.f.ledger.confirm_input_copy
        def lost(*args,**kwargs):original(*args,**kwargs);raise OSError('synthetic lost confirmation')
        with patch.object(self.f.ledger,'confirm_input_copy',side_effect=lost):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(self.progress(job)['next'],0);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'fetching');self.assertEqual(len(self.downloads),1)

    def test_actual_progress_commit_then_lost_return_is_reconciled_on_restart(self):
        job=self.fetching(contents=(b'committed',));original_db=self.f.driver._db
        advance=self.f.driver._indexed.advance;pending={'lost':False}
        def mark(*args,**kwargs):
            value=advance(*args,**kwargs);pending['lost']=True;return value
        @contextmanager
        def lost_db():
            with original_db() as db:yield db
            if pending['lost']:pending['lost']=False;raise OSError('synthetic loss after real SQLite commit')
        with patch.object(self.f.driver,'_db',side_effect=lost_db),patch.object(self.f.driver._indexed,'advance',side_effect=mark):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(self.progress(job)['next'],1);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'processing_pending');self.assertEqual(len(self.downloads),1)

    def test_outside_closure_attempt_at_completed_prefix_is_refused_cold(self):
        job=self.fetching(contents=(b'one',));self.f.driver.step()
        with self.f.driver._db() as db:
            db.execute("INSERT INTO indexed_attempts VALUES(?,1,1,?,'started')",(job['jobId'],uuid.uuid4().hex))
            db.execute('UPDATE indexed_fetch SET attempts=attempts+1')
        self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'indexed_attempt_object_changed'})
        self.assertEqual(len(self.downloads),1)

    def test_unowned_next_attempt_inserted_into_warm_session_holds_before_get(self):
        job=self.fetching();self.f.driver.step()
        with self.f.driver._db() as db:
            db.execute("INSERT INTO indexed_attempts VALUES(?,1,1,?,'started')",(job['jobId'],uuid.uuid4().hex))
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'indexed_attempt_predecessor_changed'})
        self.assertEqual(len(self.downloads),1)

    def test_late_extra_attempt_after_final_object_cannot_enter_processing(self):
        job=self.fetching(contents=(b'one',));self.f.driver.step()
        with self.f.driver._db() as db:
            db.execute("INSERT INTO indexed_attempts VALUES(?,1,1,?,'started')",(job['jobId'],uuid.uuid4().hex))
        self.assertEqual(self.f.driver.step()['state'],'held')
        self.assertEqual(len(self.downloads),1)
        self.assertFalse(any(call[0]=='initialize' for call in self.f.transport.calls))

    def test_past_attempt_identity_is_immutable_and_past_state_drift_holds_at_boundary(self):
        job=self.fetching();self.f.driver.step()
        with self.assertRaises(sqlite3.IntegrityError):
            with self.f.driver._db() as db:db.execute('UPDATE indexed_attempts SET intent=?',(uuid.uuid4().hex,))
        with self.f.driver._db() as db:db.execute("UPDATE indexed_attempts SET state='started' WHERE seq=0")
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(self.f.driver.step()['state'],'held')
        self.assertFalse(any(call[0]=='initialize' for call in self.f.transport.calls))

    def test_legitimate_spent_failed_get_attempt_retries_same_object(self):
        job=self.fetching(contents=(b'retry',))
        with patch.object(self.f.broker,'download_indexed_object',side_effect=OSError('synthetic unavailable GET')):
            self.assertEqual(self.f.driver.step()['state'],'retry_wait')
        self.assertEqual(self.progress(job)['attempts'],1);self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step()['state'],'fetching')
        self.assertEqual(self.f.driver.step()['state'],'processing_pending')
        self.assertEqual(self.progress(job)['attempts'],2);self.assertEqual(len(self.downloads),1)

    def test_expiry_retains_prefix_without_next_get_or_claim_reset(self):
        job=self.fetching();self.f.driver.step();original_clock=self.f.driver.clock
        self.f.driver.clock=lambda:original_clock()+300
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'indexed_transfer_unavailable'})
        self.assertEqual(self.progress(job)['next'],1);self.assertEqual(len(self.downloads),1)

    def test_cursor_drift_holds_in_hot_session(self):
        job=self.fetching();self.f.driver.step()
        with self.f.driver._db() as db:db.execute('UPDATE indexed_fetch SET next=2,bytes=11')
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(len(self.downloads),1)

    def test_original_claim_request_drift_holds_cold_and_hot(self):
        job=self.fetching();self.f.driver.step()
        with self.f.driver._db() as db:db.execute("UPDATE driver_jobs SET claim_request='changed'")
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'indexed_claim_request_changed'})
        self.f.driver=self.make_driver()
        self.assertEqual(self.f.driver.step(),{'state':'held','reason':'indexed_claim_request_changed'})
        self.assertEqual(len(self.downloads),1)

    def test_projection_reservation_drift_holds_without_adoption(self):
        job=self.fetching();self.f.driver.step()
        with self.f.driver._db() as db:
            row=db.execute('SELECT data FROM driver_jobs').fetchone();data=json.loads(row[0])
            data['reservation']['reservationId']='foreign'
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(data),))
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(len(self.downloads),1)

    def test_future_row_schema_bypass_holds_before_next_get(self):
        self.fetching();self.f.driver.step()
        with closing(sqlite3.connect(self.path)) as db,db:
            db.execute('DROP TRIGGER indexed_objects_update')
            db.execute("UPDATE indexed_objects SET descriptor='{}' WHERE seq=1")
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(len(self.downloads),1)

    def test_quota_refuses_second_job_before_claim_rpc(self):
        job=self.f.submit();self.f.advance(job,'awaiting_acceptance')
        second=self.f.submit();calls=[];original=self.f.driver._request
        def observe(method,suffix,*args,**kwargs):
            calls.append(suffix);return original(method,suffix,*args,**kwargs)
        self.f.driver._request=observe
        self.assertEqual(self.f.driver.step()['state'],'held')
        self.assertNotIn('/claim',calls)
        self.assertEqual(self.f.queue.get(second['jobId'])['status'],'queued')

    def test_background_pending_acceptance_never_hydrates_claim_or_grant(self):
        job=self.f.submit();self.f.advance(job,'awaiting_acceptance')
        self.f.driver._indexed_background_due=0
        with patch.object(self.f.driver._indexed,'claim',side_effect=AssertionError('full claim load')), \
                patch.object(self.f.driver._indexed,'grant',side_effect=AssertionError('full grant load')):
            self.assertEqual(self.f.driver._background()['state'],'background_idle')

    def test_injected_submission_projection_holds_before_hydration_or_get(self):
        job=self.fetching()
        with self.f.driver._db() as db:
            data=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
            data['claim']['submission']={'submissionId':'mutable-caller'}
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(data),))
        self.assertEqual(self.f.driver.step()['state'],'held');self.assertEqual(self.downloads,[])

    def test_oversized_small_cells_refuse_at_sql_boundary(self):
        job=self.fetching()
        with self.assertRaises(sqlite3.IntegrityError):
            with self.f.driver._db() as db:db.execute('UPDATE driver_jobs SET data=?',('x'*8193,))
        with self.f.driver._db() as db:progress=self.f.driver._indexed.progress(db,job['jobId'])
        with self.assertRaises(sqlite3.IntegrityError):
            with self.f.driver._db() as db:
                db.execute("INSERT INTO indexed_attempts VALUES(?,0,1,?,'started')",(job['jobId'],'f'*513))
        self.assertEqual(self.downloads,[])

    def test_preclaim_shared_capacity_subtracts_outstanding_and_control_holds(self):
        ledger=self.f.ledger
        ledger.reserve(job_id='retained',generation=1,reservation_id='held',manifest_sha256='a'*64,
            input_bytes=10,output_budget=1000,environment='staging',ttl_seconds=240,worker_ready=True)
        ledger.record_allocated('held',10)
        ledger.prepare_control_charge('held',charge_id='charge',scope_sha256='b'*64,parent_binding_sha256='c'*64,
            expected_allocated=10,allowance_bytes=100,maximum_scope_charges=1)
        with ledger._transaction() as db:
            outstanding=db.execute('SELECT SUM(remaining) FROM reservations').fetchone()[0]+db.execute('SELECT SUM(bytes) FROM control_charges').fetchone()[0]
        remaining=ledger.probe.remaining()
        with patch.object(journal.shutil,'disk_usage',return_value=SimpleNamespace(free=remaining+ledger.floor+outstanding-1)):
            with self.assertRaisesRegex(journal.IndexedFetchHeld,'disk'):
                with self.f.driver._db() as db:self.f.driver._indexed.reserve_slot(db,'nextjob',ledger=ledger)
        with self.f.driver._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_quota').fetchone()[0],0)

    def test_later_scratch_reservation_preserves_same_global_metadata_hold(self):
        ledger=self.f.ledger;remaining=ledger.probe.remaining()
        with patch.object(journal.shutil,'disk_usage',return_value=SimpleNamespace(free=remaining+ledger.floor+99)):
            from processing.scratch_policy import PolicyConflict
            with self.assertRaises(PolicyConflict):
                ledger.reserve(job_id='new',generation=1,reservation_id='new',manifest_sha256='a'*64,
                    input_bytes=50,output_budget=50,environment='staging',ttl_seconds=240,worker_ready=True)

    def test_real_10000_driver_steps_use_no_full_manifest_or_json_rewrite_per_object(self):
        self.f.store.local_max_photos=10000  # Synthetic metadata-only control fixture; no physical routing change.
        job=self.fetching(contents=(b'z',)*10000)
        with patch.object(self.f.driver._indexed,'claim',wraps=self.f.driver._indexed.claim) as claims, \
                patch.object(self.f.driver._indexed,'grant',wraps=self.f.driver._indexed.grant) as grants, \
                patch.object(self.f.driver,'_save',wraps=self.f.driver._save) as saves, \
                patch.object(self.f.driver._indexed,'validate_attempt_closure',wraps=self.f.driver._indexed.validate_attempt_closure) as audits, \
                patch.object(self.f.ledger,'_closure_matches',wraps=self.f.ledger._closure_matches) as scans:
            for index in range(10000):
                observed=self.f.driver.step()
                self.assertEqual(observed['state'],'fetching',f'object {index}: {observed}')
            self.assertEqual(self.f.driver.step()['state'],'processing_pending')
            self.assertLessEqual(claims.call_count,2);self.assertEqual(grants.call_count,1)
            self.assertEqual(saves.call_count,1);self.assertEqual(scans.call_count,2)
            self.assertEqual(audits.call_count,2)
        self.assertEqual(len(self.downloads),10000);self.assertEqual(self.progress(job)['next'],10000)
        with self.f.driver._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_objects').fetchone()[0],10000)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM indexed_attempts').fetchone()[0],10000)
            raw=db.execute('SELECT data FROM driver_jobs').fetchone()[0]
        self.assertLess(len(raw.encode()),8192)
        print(json.dumps({'oracle':'indexed-real-driver-10000','objects':10000,'realDownloads':len(self.downloads),
            'fullClaimLoads':claims.call_count,'fullGrantLoads':grants.call_count,'realSaveCallsDuringFetch':saves.call_count,
            'fullScratchClosureChecks':scans.call_count,'driverProjectionBytes':len(raw.encode()),
            'fullAttemptClosureChecks':audits.call_count,
            'saveWasWrappedNotReplaced':True}),flush=True)


if __name__=='__main__':unittest.main()
