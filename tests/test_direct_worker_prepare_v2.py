import copy
import os
import unittest
import uuid
from unittest.mock import patch

from processing.direct_worker_prepare_v2 import prepare_signed_job
from processing.scratch_policy import PolicyConflict
from processing.scratch_policy import ScratchLedger
from processing.node_gate import NodeGate
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests


@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class WorkerPrepareTests(unittest.TestCase):
    def fixture(self):
        f=WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        f.ledger._retire_direct_writer(f.observation['scratchReservationId'],f.readiness['body']['pinId'])
        kwargs=dict(payload=f.payload,item=f.item,principal='coord-a',worker=f.worker,engine_id='fixture-v2',
            coordinator_origin='https://127.0.0.1:8443',gate_generation=1,source_verification=f.options,
            mount_path='/tmp',mount_id='a'*32,marker_name=f.spool.marker.name,challenge_id=str(uuid.uuid4()),
            issuer_id='worker-issuer',key_id='worker-key',private_key=f.proof_key,validity_seconds=10)
        return f,kwargs

    def test_signed_inventory_prepares_and_replays_exact_readiness(self):
        f,kwargs=self.fixture()
        index,spool,proof=prepare_signed_job(f.ledger,**kwargs)
        self.assertFalse(proof['body']['sourceCustodyAccepted'])
        _,_,again=prepare_signed_job(f.ledger,**kwargs)
        self.assertEqual(proof,again)
        self.assertEqual(len(index.object_ids(reservation_binding=f.reservation)),3)

    def test_changed_source_or_worker_never_acquires_new_pin(self):
        f,kwargs=self.fixture();payload=copy.deepcopy(f.payload)
        payload['sourceManifest']['objects'][0]['sha256']='0'*64
        with self.assertRaises(ValueError):prepare_signed_job(f.ledger,**dict(kwargs,payload=payload))
        wrong=dict(f.worker,workerInstanceId='other')
        with self.assertRaises(PolicyConflict):prepare_signed_job(f.ledger,**dict(kwargs,worker=wrong))
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],0)

    def test_signed_source_creates_fresh_node_and_scratch_fences(self):
        f,kwargs=self.fixture()
        root=f.root/'fresh-worker';root.mkdir(mode=0o700)
        ledger=ScratchLedger(root/'ledger.sqlite',root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        with patch('processing.node_gate.canonical_gate_path',return_value=root/'gate'):
            with NodeGate('fixture-v2') as gate:
                gate.provision('cluster','https://127.0.0.1:8443',f.worker['authorityId'])
            with self.assertRaises(ValueError):prepare_signed_job(ledger,**dict(kwargs,principal='!invalid'))
            with NodeGate('fixture-v2') as gate:self.assertIsNone(gate.snapshot()['reservation'])
            with self.assertRaises(ValueError):prepare_signed_job(ledger,**dict(kwargs,mount_id='0'*32))
            with NodeGate('fixture-v2') as gate:self.assertIsNone(gate.snapshot()['reservation'])
            _,_,proof=prepare_signed_job(ledger,**kwargs)
            with NodeGate('fixture-v2') as gate:
                self.assertEqual(gate.snapshot()['schema'],2)
                self.assertEqual(gate.snapshot()['reservation']['job']['id'],f.item['jobId'])
            with ledger._transaction() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],1)
                self.assertEqual(db.execute('SELECT state FROM direct_readiness_issues').fetchone()[0],'issued')
            self.assertFalse(proof['body']['processingAuthorized'])

    def test_interrupted_atomic_gate_binding_replays_exact_reservation(self):
        f,kwargs=self.fixture()
        root=f.root/'restart-worker';root.mkdir(mode=0o700)
        ledger=ScratchLedger(root/'ledger.sqlite',root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        with patch('processing.node_gate.canonical_gate_path',return_value=root/'gate'):
            with NodeGate('fixture-v2') as gate:gate.provision('cluster','https://127.0.0.1:8443',f.worker['authorityId'])
            original=NodeGate._save;seen=[]
            def save_then_fail(gate,state):
                original(gate,state)
                if state['reservation'] is not None and not seen:
                    seen.append(True);raise OSError('synthetic lost gate reply')
            with patch.object(NodeGate,'_save',save_then_fail):
                with self.assertRaises(OSError):prepare_signed_job(ledger,**kwargs)
            with NodeGate('fixture-v2') as gate:
                self.assertEqual(gate.snapshot()['reservation']['job']['id'],f.item['jobId'])
            _,_,proof=prepare_signed_job(ledger,**kwargs)
            self.assertFalse(proof['body']['sourceCustodyAccepted'])
