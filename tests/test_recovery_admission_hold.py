"""Owned synthetic admission state; roots intentionally retained."""
import tempfile
import json
from pathlib import Path
import unittest
import uuid
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
            gate=AdmissionDrain(q);q.admission_drain=gate
            self.assertTrue(gate.status()['closed'])
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            with self.assertRaises(AdmissionClosed):
                with gate.admit():self.fail('recovery hold opened')
        finally:q.close()


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
            gate=AdmissionDrain(q);q.admission_drain=gate;gate.arm_recovery_hold()
            self.assertIsNone(store.claim(response.body['jobId'],node_id='cluster',worker_id='worker',
                                          worker_instance_id='instance',claim_request_id='claim'))
        finally:q.close()


if __name__=='__main__':unittest.main()
