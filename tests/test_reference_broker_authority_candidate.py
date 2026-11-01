"""Synthetic same-owner reference init witness; no production qualification."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch

from processing import node_mutation_broker as broker
from processing.authority_lease import AuthorityLease, record_required
from processing.broker_intent_fence import install_offline
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore, PROFILE
from processing.viewer_tasks import TaskStore
from processing.engine_ingress_guard import BackendPin
from processing.node_gate import NodeGate


@unittest.skipUnless(os.name == 'posix', 'POSIX authority lease required')
class ReferenceBrokerAuthorityCandidateTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='lt-reference-broker-authority-')).resolve()
        print('retained synthetic authority root:', self.root, flush=True)
        self.queue = Queue(self.root / 'queue')
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add(PROFILE)
        lease_root = self.root / 'leases'
        lease_root.mkdir(mode=0o700)
        self.lease = AuthorityLease(lease_root, self.queue.authority_id)
        self.addCleanup(self.lease.close)
        self.queue.authority_lease = self.lease
        record_required(self.queue, self.lease)
        self.store = ReferenceStore(self.queue)
        TaskStore(self.queue, self.root / 'queue' / 'viewer-tasks')
        with self.queue._db() as db:
            install_offline(db, enabled=True)
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        token = 'synthetic-reference-viewer-token'
        api = ReferenceAPI(self.store, bearer_tokens={token:'staging'},
            capability_fingerprint=fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality','projection'})
        result = api.handle('POST', '/ltds/v1/jobs', {'Authorization':'Bearer '+token},
                            json.dumps(fixture['submission']).encode(), protected_transport=True)
        self.assertEqual(result.status, 201)
        self.job = result.body
        self.claim = self.store.claim(self.job['jobId'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='claim-1')
        self.assertIsNotNone(self.claim)
        self.store.reserve(self.job['jobId'], fixture['transferRequest'], node_id='cluster')
        self.store.update_phase(self.job['jobId'], 'fetching')
        self.store.update_phase(self.job['jobId'], 'processing')
        self.patch = patch.object(broker, 'REFERENCE_AUTHORITY_QUALIFIED', True)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        with self.queue._db() as db:
            broker.install_reference_completion_candidate(db, enabled=True)
        self.authority = broker.ReferenceCoordinatorAuthority(self.queue)

    def grant(self):
        return dict(profile='reference-init-v1', authorityId=self.queue.authority_id,
            coordinatorEpoch=self.queue.startup_id, nodeId='cluster', engineId='synthetic',
            jobId=self.job['jobId'], engineTaskId=self.job['jobId'], environment='staging',
            gateGeneration=1, taskGeneration=self.job['generation'], operationId=str(uuid.uuid4()),
            operation='init', bodySha256=hashlib.sha256(b'init').hexdigest(), contentType='application/json',
            workerId='worker', workerInstanceId='instance', claimRequestId='claim-1',
            claimTokenSha256=hashlib.sha256(self.claim['claimToken'].encode()).hexdigest(),
            inputManifestSha256=self.job['manifestSha256'], pipelineDigest=self.job['pipelineDigest'])

    def test_claim_and_manifest_mismatch_hold(self):
        grant = self.grant()
        for key in ('claimTokenSha256','inputManifestSha256','pipelineDigest'):
            with self.subTest(key=key), self.assertRaises(broker.BrokerHeld):
                with self.authority.hold(dict(grant, **{key:'0'*64}), 2, create=True):
                    self.fail('mismatch crossed witness')
        with self.authority.hold(grant, 2, create=True):
            self.authority.completed(grant, {'uuid': grant['jobId'], 'success': True})
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',
                                        (grant['operationId'],)).fetchone()[0], 'complete')

    def test_writer_is_serialized_and_revoked_claim_holds(self):
        grant = self.grant()
        with self.authority.hold(grant, 2, create=True):
            other = sqlite3.connect(self.queue.path, timeout=0.05)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute("UPDATE jobs SET status='held' WHERE id=?", (grant['jobId'],))
            finally:
                other.close()
            self.authority.completed(grant, {'uuid': grant['jobId'], 'success': True})
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='held',held_reason='test_hold' WHERE id=?", (grant['jobId'],))
        with self.assertRaises(broker.BrokerHeld):
            with self.authority.hold(grant, 2, create=True):
                self.fail('held claim crossed witness')

    def test_failed_forward_retains_coordinator_intent(self):
        grant = self.grant()
        with self.assertRaisesRegex(RuntimeError, 'synthetic failure'):
            with self.authority.hold(grant, 2, create=True):
                raise RuntimeError('synthetic failure')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',
                                        (grant['operationId'],)).fetchone()[0], 'started')
            self.assertFalse(self.queue._transfer_admission_clear(db))

    def test_stale_coordinator_epoch_holds(self):
        grant = dict(self.grant(), coordinatorEpoch=str(uuid.uuid4()))
        with self.assertRaises(broker.BrokerHeld):
            with self.authority.hold(grant, 2, create=True):
                self.fail('stale coordinator crossed witness')

    def test_reference_grant_reaches_real_broker_and_exact_init_replays(self):
        gate_root = self.root / 'gate'
        with patch.object(broker, 'QUALIFIED', True), \
             patch('processing.node_gate.canonical_gate_path', return_value=gate_root), \
             patch.object(broker, 'canonical_gate_path', return_value=gate_root):
            with NodeGate('synthetic') as gate:
                gate.provision('cluster', 'http://127.0.0.1:18080', self.queue.authority_id)
            state = gate_root / 'mutation-broker'
            state.mkdir(mode=0o700)
            adapter = broker.LoopbackNodeODM('synthetic', BackendPin('a'*64, 'sha256:'+'b'*64, 12345), '')
            secret = b'synthetic-signing-key-' * 2
            service = broker.NodeMutationBroker(state, node_id='cluster', engine_id='synthetic',
                authority_id=self.queue.authority_id, coordinator_epoch=self.queue.startup_id,
                coordinator_origin='http://127.0.0.1:18080', gate_generation=1, secret=secret,
                adapter=adapter, authority_witness=self.authority)
            self.addCleanup(service.close)
            grant = self.grant()
            with patch.object(adapter, 'exchange', return_value={'uuid':grant['jobId'],'success':True}) as exchange:
                result = service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                self.assertEqual(result['uuid'], grant['jobId'])
                self.assertEqual(service.mutate(grant, broker.sign_grant(grant, secret), b'init'), result)
                self.assertEqual(exchange.call_count, 1)
                changed = dict(grant, workerInstanceId='different')
                with self.assertRaises(broker.BrokerHeld):
                    service.mutate(changed, broker.sign_grant(changed, secret), b'init')
                self.assertEqual(exchange.call_count, 1)
                reservation = service.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()[0]
                service.db.execute('DELETE FROM reservation WHERE singleton=1')
                service.db.commit()
                for corrupt_proof in (False, True):
                    with self.queue._db() as db:
                        old_proof = db.execute('SELECT response_sha256 FROM reference_broker_completion_candidate WHERE operation_id=?', (grant['operationId'],)).fetchone()[0]
                        if corrupt_proof:
                            db.execute('UPDATE reference_broker_completion_candidate SET response_sha256=? WHERE operation_id=?', ('0'*64, grant['operationId']))
                    with self.assertRaises(broker.BrokerHeld):
                        service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                    self.assertIsNone(service.db.execute('SELECT 1 FROM reservation').fetchone())
                    with self.queue._db() as db:
                        db.execute('UPDATE reference_broker_completion_candidate SET response_sha256=? WHERE operation_id=?', (old_proof, grant['operationId']))
                self.assertEqual(exchange.call_count, 1)
                service.db.execute('INSERT INTO reservation VALUES(1,?)', (reservation,))
                service.db.commit()
                service.db.execute('UPDATE operations SET reply=? WHERE id=?',
                                   (json.dumps({'uuid': grant['jobId'], 'success': False}), grant['operationId']))
                service.db.commit()
                with self.assertRaises(broker.BrokerHeld):
                    service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                self.assertEqual(exchange.call_count, 1)
                service.db.execute('DELETE FROM operations WHERE id=?', (grant['operationId'],))
                service.db.commit()
                with self.assertRaises(broker.BrokerHeld):
                    service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                self.assertEqual(exchange.call_count, 1)
                empty_gate = MagicMock()
                empty_gate.snapshot.return_value = {'nodeId': 'cluster', 'coordinatorOrigin': 'http://127.0.0.1:18080',
                    'authorityId': self.queue.authority_id, 'generation': 1, 'reservation': None}
                empty_gate.__enter__.return_value = empty_gate
                with patch.object(broker, 'NodeGate', return_value=empty_gate):
                    with self.assertRaises(broker.BrokerHeld):
                        service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                empty_gate.reserve.assert_not_called()
                empty_gate.bind.assert_not_called()
                self.assertEqual(exchange.call_count, 1)

    def test_coordinator_lock_timeout_never_reaches_forward(self):
        grant = self.grant()
        other = sqlite3.connect(self.queue.path, timeout=0.05)
        try:
            other.execute('BEGIN IMMEDIATE')
            with self.assertRaises((broker.BrokerHeld, sqlite3.OperationalError)):
                with self.authority.hold(grant, 0.1, create=True):
                    self.fail('lock contention crossed authority deadline')
        finally:
            other.rollback()
            other.close()
        with self.queue._db() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM broker_intents WHERE operation_id=?',
                                         (grant['operationId'],)).fetchone())

    def test_lifecycle_lock_timeout_never_reaches_forward(self):
        held = threading.Event()
        release = threading.Event()
        def holder():
            with self.queue._lifecycle_lock:
                held.set()
                release.wait(3)
        thread = threading.Thread(target=holder)
        thread.start()
        self.assertTrue(held.wait(2))
        try:
            with self.assertRaises(broker.BrokerHeld):
                with self.authority.hold(self.grant(), 0.1, create=True):
                    self.fail('lifecycle lock crossed authority deadline')
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())

    def test_execution_mutex_timeout_never_reaches_forward(self):
        service = object.__new__(broker.NodeMutationBroker)
        service._mutex = threading.Lock()
        service._mutex.acquire()
        try:
            with self.assertRaises(broker.BrokerHeld):
                with service._execution_lock(time.monotonic() + 0.1):
                    self.fail('broker mutex crossed execution deadline')
        finally:
            service._mutex.release()

    def test_broker_uncertain_forward_retains_both_intents_and_gate(self):
        gate_root = self.root / 'gate'
        with patch.object(broker, 'QUALIFIED', True), \
             patch('processing.node_gate.canonical_gate_path', return_value=gate_root), \
             patch.object(broker, 'canonical_gate_path', return_value=gate_root):
            with NodeGate('synthetic') as gate:
                gate.provision('cluster', 'http://127.0.0.1:18080', self.queue.authority_id)
            state = gate_root / 'mutation-broker'
            state.mkdir(mode=0o700)
            adapter = broker.LoopbackNodeODM('synthetic', BackendPin('a'*64, 'sha256:'+'b'*64, 12345), '')
            secret = b'synthetic-signing-key-' * 2
            service = broker.NodeMutationBroker(state, node_id='cluster', engine_id='synthetic',
                authority_id=self.queue.authority_id, coordinator_epoch=self.queue.startup_id,
                coordinator_origin='http://127.0.0.1:18080', gate_generation=1, secret=secret,
                adapter=adapter, authority_witness=self.authority)
            self.addCleanup(service.close)
            grant = self.grant()
            with patch.object(adapter, 'exchange', side_effect=broker.BrokerHeld('synthetic response lost')) as exchange:
                with self.assertRaises(broker.BrokerHeld):
                    service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                with self.assertRaises(broker.BrokerHeld):
                    service.mutate(grant, broker.sign_grant(grant, secret), b'init')
                self.assertEqual(exchange.call_count, 1)
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',
                                            (grant['operationId'],)).fetchone()[0], 'started')
                self.assertFalse(self.queue._transfer_admission_clear(db))
            with NodeGate('synthetic') as gate:
                self.assertIsNotNone(gate.snapshot()['reservation'])
            self.assertEqual(service.db.execute('SELECT state FROM operations WHERE id=?',
                                                (grant['operationId'],)).fetchone()[0], 'started')


if __name__ == '__main__':
    unittest.main()
