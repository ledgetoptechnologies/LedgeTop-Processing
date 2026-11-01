"""Disabled managed reference driver/worker synthetic end-to-end candidate."""
import json
import time
from processing.node_gate import NodeGate
from pathlib import Path
import unittest
from unittest.mock import patch
from tests.test_reference_driver import ReferenceDriverTests, TOKEN
from processing.local_capacity_authority_candidate import LocalCapacityAuthority
from processing.managed_capacity_gate_candidate import ManagedCapacityGate
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_driver import ReferenceDriver, DriverHeld


class ManagedReferenceDriverCandidateTests(ReferenceDriverTests):
    def tearDown(self):
        for item in reversed(self.patches):item.stop()
        self.queue.close();self.gate_patch.stop()
        print('Retained synthetic managed driver root:',self.root,flush=True)
        # The fixture root remains for inspection.
        self.temp._finalizer.detach()

    def test_managed_driver_restart_handoff_and_terminal_outbox(self):
        root=self.root/'capacity';root.mkdir(mode=0o700)
        journal=root/'cross.sqlite';journal.touch(mode=0o600)
        j=journal.stat();r=self.ledger.root.stat();d=self.ledger.path.stat()
        binding=root/'authority.json'
        binding.write_text(json.dumps({'schemaVersion':1,'authorityId':'a'*32,'journal':str(journal),
            'journalIdentity':[j.st_dev,j.st_ino],'ledgers':{'local':{'root':str(self.ledger.root),
            'journal':str(self.ledger.path.absolute()),'floor':self.ledger.floor,
            'identity':[r.st_dev,r.st_ino,d.st_dev,d.st_ino]}}}));binding.chmod(0o600)
        with patch('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True), \
             patch('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True), \
             patch('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',journal), \
             patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True), \
             LocalCapacityAuthority(root,binding,journal,{'local':self.ledger}) as authority:
            managed=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
            worker_path=self.root/'managed-worker.sqlite';driver_path=self.root/'managed-driver.sqlite'
            def build():
                self.worker=ReferenceEngineWorker(worker_path,ledger=self.ledger,outbox=self.outbox,engine=self.native,
                    engine_id='engine',node_id='cluster',authority_id=self.queue.authority_id,
                    coordinator_origin=self.gate_origin,gate_generation=1,worker_id='worker',
                    engine_version='pinned-engine-1',managed_gate=managed)
                driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,'https://coordinator.invalid:9443',TOKEN,
                    state_path=driver_path,ssl_context=self.context,output_budget=1024*1024,transfer_ttl=240,
                    safety_floor=1024,failure_outbox=self.failures,worker_instance_id='instance')
                driver._request=self.request;driver.sender._open=self.sender_open
                return driver
            self.driver=build();job=self.submit(contents=(b'abc',))
            original_request=self.driver._request
            accepted=[]
            def lost_reservation(method,suffix,payload=None,*,claim_token=None):
                result=original_request(method,suffix,payload,claim_token=claim_token)
                if suffix.endswith('/reservation'):
                    self.assertEqual(result[0],200);accepted.append(True)
                    raise OSError('synthetic lost reservation response')
                return result
            with patch.object(self.driver,'_request',side_effect=lost_reservation):
                for _ in range(30):
                    self.driver.step()
                    with self.driver._db() as db:
                        row=db.execute('select stage from driver_jobs where job=?',(job['jobId'],)).fetchone()
                    if row and row[0]=='reservation_pending':break
                else:self.fail('reservation boundary not reached')
            self.assertEqual(accepted,[True])
            with self.driver._db() as db:
                row=db.execute('select data from driver_jobs where job=?',(job['jobId'],)).fetchone()
                data=json.loads(row[0]);reservation=data['reservation']['reservationId']
                self.assertTrue(data['capacityPermit']['signature'])
            self.assertEqual(self.store.get(job['jobId'],private=True)['transferRequest'],data['reservation'])
            # Recreate actual driver/worker journal owners, then proceed using
            # the exact persisted permit and existing reservation.
            self.driver=build()
            self.advance(job,'awaiting_acceptance')
            self.assertEqual(self.queue.get(job['jobId'])['status'],'succeeded')
            self.assertEqual(self.store.get(job['jobId'],private=True)['transferRequest'],data['reservation'])
            self.assertEqual(sum(x[0]=='initialize' for x in self.transport.calls),1)
            self.assertEqual(sum(x[0]=='commit' for x in self.transport.calls),1)
            with self.ledger._transaction() as db:
                rows=db.execute('select id,state from reservations').fetchall()
                self.assertEqual(len(rows),1);self.assertEqual(rows[0]['id'],reservation)
                self.assertEqual(rows[0]['state'],'terminal')
            result=self.store.get(job['jobId'],private=True)['resultManifest']
            viewer_receipt=dict(schemaVersion=1,receiptId='viewer-accepted',storageReceiptId='viewer-stored',
                jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
                pipelineDigest=job['pipelineDigest'],resultManifestSha256=result['resultManifestSha256'],
                acceptedObjectIds=[obj['id'] for obj in result['objects']],acceptedAt='2026-10-27T22:00:00Z',
                validationVersion='ltds-reference-v1')
            self.store.accept(job['jobId'],viewer_receipt)
            self.driver.step()
            with self.driver._db() as db:
                self.assertEqual(db.execute('select stage from driver_jobs where job=?',(job['jobId'],)).fetchone()[0],'completed')
            self.assertTrue((self.ledger.root/'jobs'/reservation/'all.zip').exists())

    def test_managed_failure_delivery_lost_response_and_exact_receipt(self):
        root=self.root/'capacity';root.mkdir(mode=0o700)
        journal=root/'cross.sqlite';journal.touch(mode=0o600)
        j=journal.stat();r=self.ledger.root.stat();d=self.ledger.path.stat()
        binding=root/'authority.json'
        binding.write_text(json.dumps({'schemaVersion':1,'authorityId':'a'*32,'journal':str(journal),
            'journalIdentity':[j.st_dev,j.st_ino],'ledgers':{'local':{'root':str(self.ledger.root),
            'journal':str(self.ledger.path.absolute()),'floor':self.ledger.floor,
            'identity':[r.st_dev,r.st_ino,d.st_dev,d.st_ino]}}}));binding.chmod(0o600)
        with patch('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True), \
             patch('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True), \
             patch('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',journal), \
             patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True), \
             LocalCapacityAuthority(root,binding,journal,{'local':self.ledger}) as authority:
            managed=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=self.ledger)
            worker_path=self.root/'managed-worker.sqlite';driver_path=self.root/'managed-driver.sqlite'
            def build():
                self.worker=ReferenceEngineWorker(worker_path,ledger=self.ledger,outbox=self.outbox,engine=self.native,
                    engine_id='engine',node_id='cluster',authority_id=self.queue.authority_id,
                    coordinator_origin=self.gate_origin,gate_generation=1,worker_id='worker',
                    engine_version='pinned-engine-1',managed_gate=managed)
                driver=ReferenceDriver(self.worker,self.ledger,self.outbox,self.broker,'https://coordinator.invalid:9443',TOKEN,
                    state_path=driver_path,ssl_context=self.context,output_budget=1024*1024,transfer_ttl=240,
                    safety_floor=1024,failure_outbox=self.failures,worker_instance_id='instance')
                driver._request=self.request;driver.sender._open=self.sender_open
                return driver
            self.transport.code=30
            self.driver=build();job=self.submit(contents=(b'abc',))
            original_request=self.driver._request
            accepted=[]
            def lost_reservation(method,suffix,payload=None,*,claim_token=None):
                result=original_request(method,suffix,payload,claim_token=claim_token)
                if suffix.endswith('/reservation'):
                    self.assertEqual(result[0],200);accepted.append(True)
                    raise OSError('synthetic lost reservation response')
                return result
            with patch.object(self.driver,'_request',side_effect=lost_reservation):
                for _ in range(30):
                    self.driver.step()
                    with self.driver._db() as db:
                        row=db.execute('select stage from driver_jobs where job=?',(job['jobId'],)).fetchone()
                    if row and row[0]=='reservation_pending':break
                else:self.fail('reservation boundary not reached')
            self.assertEqual(accepted,[True])
            with self.driver._db() as db:
                row=db.execute('select data from driver_jobs where job=?',(job['jobId'],)).fetchone()
                data=json.loads(row[0]);reservation=data['reservation']['reservationId']
                self.assertTrue(data['capacityPermit']['signature'])
            self.assertEqual(self.store.get(job['jobId'],private=True)['transferRequest'],data['reservation'])
            # Recreate actual driver/worker journal owners, then proceed using
            # the exact persisted permit and existing reservation.
            self.driver=build()
            original_failure_request=self.driver._request;failure_calls=[]
            def lose_first_failure(method,suffix,payload=None,*,claim_token=None):
                result=original_failure_request(method,suffix,payload,claim_token=claim_token)
                if suffix.endswith('/terminal-outcomes'):
                    failure_calls.append((payload['delivery']['outboxId'],payload['delivery']['payloadSha256']))
                    if len(failure_calls)==1:
                        self.assertEqual(result[0],200)
                        raise OSError('synthetic lost failure acknowledgment')
                return result
            with patch.object(self.driver,'_request',side_effect=lose_first_failure):
                for _ in range(80):
                    self.supply_grants();self.driver.step()
                    if failure_calls:break
                else:self.fail('failure acknowledgment boundary not reached')
                self.failures.clock=lambda:time.time()+120
                self.advance(job,'awaiting_failure_acceptance')
            self.assertEqual(len(failure_calls),2)
            self.assertEqual(failure_calls[0],failure_calls[1])
            with self.driver._db() as db:
                failure_data=json.loads(db.execute('select data from driver_jobs where job=?',(job['jobId'],)).fetchone()[0])
            failure_outbox=self.failures.get(failure_data['failureOutboxId'])
            self.assertEqual(failure_outbox['state'],'acknowledged')
            self.assertEqual(failure_outbox['receipt']['outboxId'],failure_data['failureOutboxId'])
            self.assertEqual(failure_outbox['receipt']['failureSha256'],failure_data['failure']['failureSha256'])
            with NodeGate('engine') as node:
                state=node.snapshot();self.assertIsNone(state['reservation'])
                self.assertEqual(state['lastAcknowledgment']['job']['id'],job['jobId'])
            release=self.driver._managed_failure_release_binding(job['jobId'],failure_data)
            self.assertTrue(managed.release_verified(failure_data['claim'],failure_data['capacityPermit'],release))
            failure=self.api.viewer.terminal.outcome(job['jobId'],'staging')
            viewer_receipt=dict(schemaVersion=1,receiptId='failure-viewer',storageReceiptId='failure-storage',
                jobId=job['jobId'],generation=1,environment='staging',inputManifestSha256=job['manifestSha256'],
                pipelineDigest=job['pipelineDigest'],failureSha256=failure['failureSha256'],
                acceptedAt='2026-10-27T22:00:00Z',validationVersion='ltds-reference-v1')
            self.api.viewer.terminal.accept(job['jobId'],viewer_receipt,'staging')
            self.driver.step()
            with self.driver._db() as db:
                self.assertEqual(db.execute('select stage from driver_jobs where job=?',(job['jobId'],)).fetchone()[0],'completed')
            self.assertEqual(self.queue.get(job['jobId'])['status'],'failed')
            self.assertEqual(self.store.get(job['jobId'],private=True)['transferRequest'],data['reservation'])
            self.assertEqual(sum(x[0]=='initialize' for x in self.transport.calls),1)
            self.assertEqual(sum(x[0]=='commit' for x in self.transport.calls),1)
            with self.ledger._transaction() as db:
                rows=db.execute('select id,state from reservations').fetchall()
                self.assertEqual(len(rows),1);self.assertEqual(rows[0]['id'],reservation)
                self.assertEqual(rows[0]['state'],'terminal')
