from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid
from urllib.request import Request,build_opener,ProxyHandler

from processing.prototype import Queue
from processing.reference_delivery import DeliveryReceiver
from processing.reference_http import create_reference_http_server
from processing import reference_protocol as p
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI,PREFIX

WORKER='synthetic-recovery-worker-0001'
OTHER='synthetic-recovery-other-0002'


@unittest.skipUnless(os.name=='posix','Linux coordinator ownership required')
class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.identities={WORKER:dict(node_id='cluster',worker_id='worker',worker_instance_id='original-instance'),
            OTHER:dict(node_id='cluster',worker_id='other-worker',worker_instance_id='other-instance')}
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.open()
        raw=self.fixture['submission']
        self.job=self.store.submit(p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
        self.claim=self.store.claim(self.job['jobId'],**self.identities[WORKER],claim_request_id='original-claim')
        self.store.reserve(self.job['jobId'],self.fixture['transferRequest'],node_id='cluster')
        self.store.update_phase(self.job['jobId'],'fetching');self.store.update_phase(self.job['jobId'],'processing')
        self.restart()

    def open(self):
        self.queue=Queue(self.root,reference_enabled=True,node_ids=('cluster','main'));self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue)
        binding=RecoveryNodeBinding('cluster','physical-cluster',self.queue.authority_id,'http://127.0.0.1:18080',1,1000)
        self.api=ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers=self.identities,recovery_bindings={'cluster':binding})

    def restart(self):self.queue.close();self.open()

    def call(self,method,operation,body=None,*,worker=WORKER,token=None,protected=True):
        return self.api.handle(method,PREFIX+'/jobs/'+self.job['jobId']+'/'+operation,
            {'Authorization':'Bearer '+worker,'X-Claim-Token':self.claim['claimToken'] if token is None else token},
            b'' if body is None else p.canonical_json(body),protected_transport=protected)

    def state(self):
        response=self.call('GET','recovery-state');self.assertEqual(response.status,200);return response.body

    def request(self):
        state=self.state();claim=state['claim'];binding=state['physicalBinding'];reservation=state['reservation']
        parent={key:claim[key] for key in ('jobId','generation','environment','nodeId','workerId','workerInstanceId','authorityId',
            'claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')}
        parent.update(engineId=binding['engine_id'],coordinatorOrigin=binding['coordinator_origin'],gateGeneration=binding['gate_generation'],
            outputBudget=binding['output_budget'],inputBytes=reservation['inputBytes'],reservationId=reservation['reservationId'],gateReceiptId='a'*32)
        return dict(schemaVersion=1,requestId=str(uuid.uuid4()),coordinatorEpoch=state['coordinatorEpoch'],
            expectedNodeRevision=state['node']['revision'],parentBinding=parent,
            transferRequestSha256=reservation['transferRequestSha256'],workerEvidenceSha256='e'*64)

    def facts(self):
        with self.queue._db() as db:
            ref=dict(db.execute('SELECT * FROM reference_jobs').fetchone())
            claim=dict(db.execute('SELECT * FROM reference_claims').fetchone())
            grants=[tuple(row) for row in db.execute('SELECT * FROM reference_transfer_grants')]
            jobs=[dict(row) for row in db.execute('SELECT * FROM jobs')]
        for job in jobs:
            for key in ('status','held_reason','updated_at'):job.pop(key)
        return ref,claim,grants,jobs

    def test_exact_original_claim_resumes_without_requeue_or_input_changes(self):
        before=self.facts();request=self.request();old_token=self.claim['claimToken']
        response=self.call('POST','recover',request);self.assertEqual(response.status,200,response.body)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'running')
        self.assertEqual(self.facts(),before)
        self.assertEqual(response.body['parentBindingSha256'],p.digest(request['parentBinding']))
        self.assertNotIn(old_token,json.dumps(response.body))
        self.assertEqual(self.store.claim(self.job['jobId'],**self.identities[WORKER],claim_request_id='original-claim')['claimToken'],old_token)
        self.assertIsNone(self.store.next_for_node('cluster'))

    def test_challenge_and_historical_lookup_are_read_only(self):
        before=Path(self.queue.path).read_bytes()
        with patch.object(self.queue,'_refresh_nodes',side_effect=AssertionError('no refresh')):
            first=self.state();second=self.state()
        self.assertEqual(first,second);self.assertEqual(before,Path(self.queue.path).read_bytes())
        self.assertTrue(first['eligible']);self.assertEqual(first['claim']['heldReason'],'coordinator_restart')

    def test_old_receipt_replay_cannot_clear_a_later_restart_or_operator_hold(self):
        request=self.request();receipt=self.call('POST','recover',request).body
        self.restart();self.assertNotEqual(self.state()['coordinatorEpoch'],request['coordinatorEpoch'])
        before=Path(self.queue.path).read_bytes()
        self.assertEqual(self.call('POST','recover',request).body,receipt)
        self.assertEqual(self.call('GET','recovery-receipts/'+request['requestId']).body,receipt)
        self.assertEqual(before,Path(self.queue.path).read_bytes())
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'held')
        self.queue.operator(self.job['jobId'],'hold',dict(note='synthetic operator hold'))
        before=Path(self.queue.path).read_bytes()
        self.assertEqual(self.call('POST','recover',request).body,receipt)
        self.assertEqual(before,Path(self.queue.path).read_bytes())
        self.assertEqual(self.queue.get(self.job['jobId'])['heldReason'],'operator')

    def test_stale_epoch_new_request_and_same_id_changed_body_are_refused(self):
        request=self.request();self.restart()
        before=Path(self.queue.path).read_bytes()
        self.assertEqual(self.call('POST','recover',request).status,409)
        self.assertEqual(before,Path(self.queue.path).read_bytes())
        request=self.request();self.assertEqual(self.call('POST','recover',request).status,200)
        request['workerEvidenceSha256']='f'*64
        self.assertEqual(self.call('POST','recover',request).status,409)

    def test_authentication_transport_and_optional_activation(self):
        request=self.request()
        self.assertEqual(self.call('POST','recover',request,worker='viewer-not-a-worker-token').status,401)
        self.assertEqual(self.call('POST','recover',request,worker=OTHER).status,409)
        self.assertEqual(self.call('POST','recover',request,token='wrong').status,409)
        self.assertEqual(self.call('POST','recover',request,protected=False).status,503)
        disabled=ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers=self.identities)
        active=self.api;self.api=disabled
        try:self.assertEqual(self.call('POST','recover',request).status,404)
        finally:self.api=active

    def test_strict_identity_node_revision_and_reservation_checks(self):
        changes=[('generation',True),('generation',2),('environment','production'),('workerInstanceId','other'),
            ('manifestSha256','0'*64),('pipelineDigest','0'*64),('engineId','other-engine'),('gateGeneration',2),
            ('reservationId','other'),('inputBytes',999),('outputBudget',999),('claimRequestId','other')]
        for key,value in changes:
            request=self.request();request['parentBinding'][key]=value
            with self.subTest(key=key):self.assertIn(self.call('POST','recover',request).status,(400,409))
        request=self.request();request['expectedNodeRevision']+=1
        self.assertEqual(self.call('POST','recover',request).status,409)
        request=self.request();request['transferRequestSha256']='0'*64
        self.assertEqual(self.call('POST','recover',request).status,409)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'held')

    def test_current_parent_may_finish_draining_but_never_updating_or_held_node(self):
        for state in ('active','scheduled','draining'):
            with self.queue._db() as db:db.execute('UPDATE nodes SET state=?,revision=revision+1 WHERE id=?',(state,'cluster'))
            request=self.request()
            with self.subTest(state=state):self.assertEqual(self.call('POST','recover',request).status,200)
            self.restart()
        for state in ('updating','held','failed'):
            with self.queue._db() as db:db.execute('UPDATE nodes SET state=?,revision=revision+1 WHERE id=?',(state,'cluster'))
            request=self.request()
            with self.subTest(state=state):self.assertEqual(self.call('POST','recover',request).status,409)

    def test_reference_phase_hold_and_error_never_resume(self):
        for phase,error in (('held','operator'),('fetching',None),('processing','failed_stage')):
            with self.queue._db() as db:db.execute('UPDATE reference_jobs SET phase=?,error=?',(phase,error))
            self.assertEqual(self.call('POST','recover',self.request()).status,409)

    def test_another_node_occupied_slot_does_not_block_resume(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id) VALUES(?,?,'production','treedar-synthetic-v1','held','main')",(str(uuid.uuid4()),str(uuid.uuid4())))
        self.assertEqual(self.call('POST','recover',self.request()).status,200)
        with self.queue._db() as db:
            other = db.execute("SELECT status FROM jobs WHERE node_id='main'").fetchone()
        self.assertEqual(other['status'], 'held')

    def test_same_node_occupied_slot_still_blocks_resume(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id) VALUES(?,?,'production','treedar-synthetic-v1','held','cluster')",(str(uuid.uuid4()),str(uuid.uuid4())))
        self.assertEqual(self.call('POST','recover',self.request()).status,409)

    def test_maintenance_revision_race_refuses_without_touching_job(self):
        request=self.request()
        with self.queue._db() as db:db.execute("UPDATE nodes SET state='draining',revision=revision+1 WHERE id='cluster'")
        self.assertEqual(self.call('POST','recover',request).status,409)
        self.assertEqual(self.call('POST','recover',self.request()).status,200)

    def test_commit_failure_rolls_back_both_receipt_and_resume(self):
        request=self.request();original=self.queue._db
        @contextmanager
        def fail_commit():
            with original() as db:
                yield db
                raise OSError('synthetic failure before commit')
        before=Path(self.queue.path).read_bytes()
        with patch.object(self.queue,'_db',fail_commit):self.assertEqual(self.call('POST','recover',request).status,500)
        self.assertEqual(before,Path(self.queue.path).read_bytes())
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'held')

    def test_committed_lost_reply_is_resolved_by_exact_receipt_without_reexecution(self):
        request=self.request();original=self.api.recovery.resume
        def lose(*args,**kwargs):original(*args,**kwargs);raise OSError('synthetic lost reply')
        with patch.object(self.api.recovery,'resume',side_effect=lose):self.assertEqual(self.call('POST','recover',request).status,500)
        receipt=self.call('GET','recovery-receipts/'+request['requestId'])
        self.assertEqual(receipt.status,200);self.assertEqual(receipt.body['requestSha256'],p.digest(request))
        before=Path(self.queue.path).read_bytes()
        self.assertEqual(self.call('POST','recover',request).body,receipt.body)
        self.assertEqual(before,Path(self.queue.path).read_bytes())

    def test_concurrent_same_request_has_one_receipt_and_different_request_cannot_recover_again(self):
        request=self.request();barrier=threading.Barrier(3);responses=[]
        def resume():barrier.wait();responses.append(self.call('POST','recover',request))
        threads=[threading.Thread(target=resume) for _ in range(2)]
        for thread in threads:thread.start()
        barrier.wait()
        for thread in threads:thread.join(5);self.assertFalse(thread.is_alive())
        self.assertEqual([r.status for r in responses],[200,200]);self.assertEqual(responses[0].body,responses[1].body)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_recovery_receipts').fetchone()[0],1)
        self.assertEqual(self.call('POST','recover',self.request()).status,409)

    def test_close_waits_for_recovery_commit_and_closed_queue_refuses_new_writes(self):
        request=self.request();entered=threading.Event();release=threading.Event();closed=threading.Event();results=[]
        original=self.api.recovery._check
        def paused(db=None):
            original(db)
            if db is not None and db.execute('SELECT COUNT(*) FROM reference_recovery_receipts').fetchone()[0]:
                entered.set();self.assertTrue(release.wait(5))
        def resume():results.append(self.call('POST','recover',request))
        def close():self.queue.close();closed.set()
        with patch.object(self.api.recovery,'_check',side_effect=paused):
            worker=threading.Thread(target=resume);worker.start();self.assertTrue(entered.wait(5))
            closer=threading.Thread(target=close);closer.start();self.assertFalse(closed.wait(0.05));release.set()
            worker.join(5);closer.join(5);self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive())
        self.assertTrue(closed.is_set());self.assertEqual(results[0].status,200)
        with self.assertRaisesRegex(RuntimeError,'closed'):
            with self.queue._db():self.fail('closed queue used')

    def test_pid_and_authority_drift_are_refused(self):
        request=self.request()
        with patch('processing.reference_recovery.os.getpid',return_value=os.getpid()+1):
            self.assertEqual(self.call('POST','recover',request).status,409)
        with self.queue._db() as db:db.execute('UPDATE coordinator_identity SET authority_id=?',(str(uuid.uuid4()),))
        self.assertEqual(self.call('POST','recover',request).status,409)

    def test_same_thread_close_during_recovery_rolls_back_without_releasing_authority(self):
        request=self.request();original=self.api.recovery._check
        def close_in_transaction(db=None):
            original(db)
            if db is not None and db.execute('SELECT COUNT(*) FROM reference_recovery_receipts').fetchone()[0]:
                self.queue.close()
        with patch.object(self.api.recovery,'_check',side_effect=close_in_transaction):
            self.assertEqual(self.call('POST','recover',request).status,500)
        self.assertFalse(self.queue._lock.closed)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'held')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_recovery_receipts').fetchone()[0],0)
        self.assertEqual(self.call('POST','recover',request).status,200)

    def test_metadata_quota_refusal_preserves_held_parent(self):
        request=self.request();self.store.max_metadata_bytes=1
        before=Path(self.queue.path).read_bytes()
        self.assertEqual(self.call('POST','recover',request).status,409)
        self.assertEqual(before,Path(self.queue.path).read_bytes())

    def test_entire_stored_receipt_is_checked_on_read_and_replay(self):
        request=self.request();receipt=self.call('POST','recover',request).body
        for key,value in (('resumedStatus','failed'),('generation',2),('protocol','other'),('extra',True)):
            damaged=dict(receipt);damaged[key]=value
            with self.queue._db() as db:db.execute('UPDATE reference_recovery_receipts SET receipt_json=?',(p.canonical_json(damaged).decode(),))
            before=Path(self.queue.path).read_bytes()
            with self.subTest(key=key):
                self.assertEqual(self.call('GET','recovery-receipts/'+request['requestId']).status,409)
                self.assertEqual(self.call('POST','recover',request).status,409)
                self.assertEqual(before,Path(self.queue.path).read_bytes())

    def test_feature_reconstruction_in_same_queue_does_not_hold_running_parent(self):
        request=self.request();self.assertEqual(self.call('POST','recover',request).status,200)
        epoch=self.queue.startup_id
        binding=self.api.recovery.bindings['cluster']
        self.api=ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers=self.identities,recovery_bindings={'cluster':binding})
        self.assertEqual(self.queue.startup_id,epoch)
        self.assertEqual(self.queue.get(self.job['jobId'])['status'],'running')

    def test_real_loopback_transport_keeps_claim_token_out_of_receipt(self):
        server=create_reference_http_server(self.api);thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            request=self.request();url='http://127.0.0.1:'+str(server.server_port)+PREFIX+'/jobs/'+self.job['jobId']+'/recover'
            wire=Request(url,data=p.canonical_json(request),method='POST',headers={'Authorization':'Bearer '+WORKER,
                'X-Claim-Token':self.claim['claimToken'],'Content-Type':'application/json'})
            with build_opener(ProxyHandler({})).open(wire,timeout=5) as response:
                body=response.read();self.assertEqual(response.headers['Cache-Control'],'no-store');self.assertEqual(response.status,200)
            self.assertNotIn(self.claim['claimToken'].encode(),body)
        finally:server.shutdown();thread.join(5);server.server_close()


if __name__=='__main__':unittest.main()
