"""Owned synthetic admission state; roots intentionally retained."""
import tempfile
import sqlite3
import threading
import json
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch
from processing.admission_drain import AdmissionDrain, AdmissionClosed
from processing.maintenance import Conflict
from processing.prototype import Queue, PROFILE


class RecoveryAdmissionHoldTests(unittest.TestCase):
    def test_hold_blocks_actual_queue_claim_and_reopen_after_restart(self):
        root=tempfile.mkdtemp(prefix='lt-recovery-hold-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root,node_ids=('cluster','main'))
        try:
            gate=AdmissionDrain(q);q.admission_drain=gate
            q.submit({'requestId':'synthetic-one','environment':'production','profile':PROFILE})
            gate.arm_recovery_hold();gate.arm_recovery_hold()
            state=gate.status();self.assertTrue(state['closed'])
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            with self.assertRaises(Conflict):
                gate.set_closed(False,request_id=str(uuid.uuid4()),expected_generation=state['generation'],
                                expected_closed=True,expected_authority=state['authorityId'])
        finally:q.close()
        q=Queue(root,node_ids=('cluster','main'))
        try:
            self.assertFalse(hasattr(q,'admission_drain'))
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            gate=AdmissionDrain(q);q.admission_drain=gate
            self.assertTrue(gate.status()['closed'])
            with self.assertRaises(AdmissionClosed):
                with gate.admit():self.fail('recovery hold opened')
        finally:q.close()


    def test_failed_install_rolls_back_closed_state_and_both_markers(self):
        root=tempfile.mkdtemp(prefix='lt-recovery-failed-install-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root)
        try:
            gate=AdmissionDrain(q)
            with patch.object(AdmissionDrain,'_GUARD_DDL','not a SQL statement'):
                with self.assertRaises(sqlite3.OperationalError):gate.arm_recovery_hold()
            self.assertFalse(gate.status()['closed'])
            with q._db() as db:
                rows=db.execute("SELECT name FROM sqlite_master WHERE name IN ('coordinator_recovery_hold','coordinator_recovery_guard')").fetchall()
                self.assertEqual(rows,[])
        finally:q.close()

    def test_arm_waits_for_in_progress_claim_transaction(self):
        root=tempfile.mkdtemp(prefix='lt-recovery-claim-race-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root);release=threading.Event();entered=threading.Event();arm_started=threading.Event();arm_done=threading.Event();out=[]
        try:
            gate=AdmissionDrain(q)
            q.submit({'requestId':'synthetic-one','environment':'production','profile':PROFILE})
            check=q._transfer_admission_clear
            def paused(db):
                entered.set()
                if not release.wait(3):raise RuntimeError('synthetic test timed out')
                return check(db)
            def claim():out.append(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            def arm():arm_started.set();gate.arm_recovery_hold();arm_done.set()
            with patch.object(q,'_transfer_admission_clear',side_effect=paused):
                claimant=threading.Thread(target=claim);armer=threading.Thread(target=arm)
                claimant.start()
                try:
                    self.assertTrue(entered.wait(2));armer.start();self.assertTrue(arm_started.wait(2))
                    self.assertFalse(arm_done.wait(.1))
                finally:
                    release.set();claimant.join(4)
                    if armer.ident is not None:armer.join(4)
                self.assertFalse(claimant.is_alive());self.assertFalse(armer.is_alive())
            self.assertTrue(arm_done.is_set());self.assertEqual(len(out),1)
            self.assertIsNotNone(out[0]);self.assertTrue(gate.status()['closed'])
        finally:q.close()

    def test_lost_one_marker_still_denies_claim_and_reopen(self):
        root=tempfile.mkdtemp(prefix='lt-recovery-missing-marker-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root)
        try:
            gate=AdmissionDrain(q)
            q.submit({'requestId':'synthetic-one','environment':'production','profile':PROFILE})
            gate.arm_recovery_hold()
            with q._db() as db:db.execute('DROP TABLE coordinator_recovery_hold')
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            with self.assertRaises(AdmissionClosed):gate.status()
            with self.assertRaises(AdmissionClosed):
                gate.set_closed(False,request_id=str(uuid.uuid4()),expected_generation=1,
                                expected_closed=True,expected_authority=q.authority_id)
        finally:q.close()

    def test_required_hold_refuses_missing_both_on_runtime_and_restart(self):
        from processing.app import make_handler
        from processing.config import Config
        root=tempfile.mkdtemp(prefix='lt-required-hold-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root)
        try:
            job,_=q.submit({'requestId':'history','environment':'production','profile':PROFILE})
            AdmissionDrain(q).arm_recovery_hold()
        finally:q.close()
        config=Config('synthetic-api-'+'a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,
                      prototype_state_dir=root,prototype_operator_token='synthetic-op-'+'b'*32,
                      admission_drain_enabled=True,recovery_hold_required=True)
        with self.assertRaisesRegex(RuntimeError,'recovery_hold_not_qualified'):make_handler(config)
        with patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True), patch('processing.config.RECOVERY_HOLD_QUALIFIED',True):
            handler=make_handler(config);q=handler.prototype_queue
            try:
                with self.assertRaises(AttributeError):q.recovery_hold_required=False
                with self.assertRaises(AttributeError):q._recovery_hold_required=False
                with self.assertRaises(AttributeError):del q._recovery_hold_required
                with self.assertRaises(AttributeError):del q.recovery_hold_required
                with q._db() as db:
                    db.execute('DROP TABLE coordinator_recovery_hold')
                    db.execute('DROP TABLE coordinator_recovery_guard')
                del q.admission_drain
                self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
                self.assertEqual(q.get(job['id'])['status'],'queued')
            finally:q.close()
            with self.assertRaisesRegex(RuntimeError,'required_recovery_hold_missing'):make_handler(config)
        with self.assertRaisesRegex(RuntimeError,'required_recovery_hold_missing'):
            Queue(root,recovery_hold_required=True)

    def test_missing_required_hold_refuses_before_running_job_recovery(self):
        from processing.app import make_handler
        from processing.config import Config
        root=tempfile.mkdtemp(prefix='lt-required-preflight-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root)
        try:
            job,_=q.submit({'requestId':'running-history','environment':'production','profile':PROFILE})
            q.claim({'workerId':'synthetic','nodeId':'cluster'})
            gate=AdmissionDrain(q);gate.arm_recovery_hold()
            with q._db() as db:
                db.execute('DROP TABLE coordinator_recovery_hold')
                db.execute('DROP TABLE coordinator_recovery_guard')
        finally:q.close()
        config=Config('synthetic-api-'+'a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,
                      prototype_state_dir=root,prototype_operator_token='synthetic-op-'+'b'*32,
                      admission_drain_enabled=True,recovery_hold_required=True)
        with patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True),patch('processing.config.RECOVERY_HOLD_QUALIFIED',True):
            with self.assertRaisesRegex(RuntimeError,'required_recovery_hold_missing'):make_handler(config)
        db=sqlite3.connect((Path(root)/'jobs.sqlite3').as_uri()+'?mode=ro',uri=True)
        try:
            row=db.execute('SELECT status,held_reason FROM jobs WHERE id=?',(job['id'],)).fetchone()
            self.assertEqual(row,('running',None))
        finally:db.close()

    def test_hold_blocks_actual_reference_claim(self):
        from processing.reference_api import ReferenceAPI
        from processing.reference_store import ReferenceStore
        root=tempfile.mkdtemp(prefix='lt-recovery-reference-hold-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root,reference_enabled=True)
        try:
            store=ReferenceStore(q)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
            token='synthetic-reference-token-0001'
            api=ReferenceAPI(store,bearer_tokens={token:'staging'},
                capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
                options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],
                allowed_odm_options={'pc-quality','projection'})
            response=api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+token},
                                json.dumps(fixture['submission']).encode(),protected_transport=True)
            self.assertEqual(response.status,201)
            gate=AdmissionDrain(q);q.admission_drain=gate;other=AdmissionDrain(q);gate.arm_recovery_hold()
            state=other.status()
            with self.assertRaises(Conflict):
                other.set_closed(False,request_id=str(uuid.uuid4()),expected_generation=state['generation'],
                                 expected_closed=True,expected_authority=state['authorityId'])
            del q.admission_drain
            self.assertIsNone(store.claim(response.body['jobId'],node_id='cluster',worker_id='worker',
                                          worker_instance_id='instance',claim_request_id='claim'))
        finally:q.close()


    def test_required_hold_applies_to_reference_store_after_restart(self):
        from processing.reference_api import ReferenceAPI
        from processing.reference_store import ReferenceStore
        root=tempfile.mkdtemp(prefix='lt-required-reference-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root,reference_enabled=True)
        try:
            store=ReferenceStore(q)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
            token='synthetic-reference-token-0001'
            api=ReferenceAPI(store,bearer_tokens={token:'staging'},
                capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
                options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],
                allowed_odm_options={'pc-quality','projection'})
            response=api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+token},
                                json.dumps(fixture['submission']).encode(),protected_transport=True)
            AdmissionDrain(q).arm_recovery_hold()
        finally:q.close()
        q=Queue(root,reference_enabled=True,recovery_hold_required=True)
        try:
            store=ReferenceStore(q)
            with q._db() as db:
                db.execute('DROP TABLE coordinator_recovery_hold')
                db.execute('DROP TABLE coordinator_recovery_guard')
            self.assertIsNone(store.claim(response.body['jobId'],node_id='cluster',worker_id='worker',
                                          worker_instance_id='instance',claim_request_id='claim'))
        finally:q.close()

    def test_hold_denies_existing_reference_worker_execution_snapshot(self):
        from processing.reference_api import ReferenceAPI
        from processing.reference_store import ReferenceStore
        root=tempfile.mkdtemp(prefix='lt-recovery-snapshot-hold-')
        print('retained synthetic recovery hold root:',root,flush=True)
        q=Queue(root,reference_enabled=True)
        try:
            store=ReferenceStore(q)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
            token='synthetic-reference-token-0001'
            api=ReferenceAPI(store,bearer_tokens={token:'staging'},
                capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
                options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],
                allowed_odm_options={'pc-quality','projection'})
            response=api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+token},
                                json.dumps(fixture['submission']).encode(),protected_transport=True)
            binding=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_request_id='claim')
            claim=store.claim(response.body['jobId'],**binding)
            identity={k:v for k,v in binding.items() if k!='claim_request_id'}
            store.claim_snapshot(response.body['jobId'],**identity,claim_token=claim['claimToken'])
            AdmissionDrain(q).arm_recovery_hold()
            with self.assertRaises(Conflict):
                store.claim_snapshot(response.body['jobId'],**identity,claim_token=claim['claimToken'])
        finally:q.close()


if __name__=='__main__':unittest.main()
