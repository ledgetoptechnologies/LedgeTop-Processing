"""Synthetic same-owner reference init witness; no production qualification."""
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
import zipfile
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
from processing.reference_engine_worker import ReferenceEngineWorker


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
            if getattr(self,'REMOTE_FENCE_REQUIRED',False):
                from processing.node_reference_remote_fence import install_offline as install_remote_fence
                install_remote_fence(db,enabled=True)
                from processing.node_reference_plan import install_plan_binding_offline
                install_plan_binding_offline(db,node_id='cluster',authority_id=self.queue.authority_id,
                                             engine_id='synthetic',gate_generation=1,enabled=True)
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

    def test_manifest_inventory_requires_every_exact_unique_confirmed_receipt(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
        manifest = fixture['submission']['manifest']
        obj = manifest['objects'][0]
        grant = self.grant()
        secret = b'synthetic-signing-key-' * 2
        receipt = dict(objectId=obj['id'], fileName=obj['name'], fileBytes=obj['byteSize'],
                       sha256=obj['sha256'], operationId=str(uuid.uuid4()), state='complete',
                       scopeSha256=hashlib.sha256(broker._json(grant)).hexdigest())
        receipt['signature'] = broker._reference_receipt_signature(receipt, secret)
        issues = {obj['id']: receipt['operationId']}
        def check(items, active_grant=grant, active_manifest=manifest):
            broker._reference_verified_image_only_uploads(active_manifest, active_manifest['sha256'], items,
                                                         init_grant=active_grant, issues=issues, secret=secret)
        check([receipt])
        for receipts in ([], [receipt, receipt], [dict(receipt, sha256='0'*64)],
                         [dict(receipt, state='started')], [dict(receipt, objectId='other')]):
            with self.subTest(receipts=receipts), self.assertRaises(broker.BrokerHeld):
                check(receipts)
        for change in ({'jobId': str(uuid.uuid4())}, {'coordinatorEpoch': str(uuid.uuid4())},
                       {'taskGeneration': grant['taskGeneration'] + 1}, {'claimTokenSha256': '0'*64}):
            with self.subTest(change=change), self.assertRaises(broker.BrokerHeld):
                check([receipt], dict(grant, **change))
        single = json.loads(json.dumps(manifest))
        single['objects'][0]['byteSize'] = 1
        single['totalBytes'] = 1
        from processing.reference_protocol import manifest_sha256
        single['sha256'] = manifest_sha256(single)
        single_grant = dict(grant, inputManifestSha256=single['sha256'])
        bad_boolean = dict(receipt, fileBytes=True, scopeSha256=hashlib.sha256(broker._json(single_grant)).hexdigest())
        bad_boolean['signature'] = broker._reference_receipt_signature(bad_boolean, secret)
        with self.assertRaises(broker.BrokerHeld):
            check([bad_boolean], single_grant, single)

    def test_reference_upload_issue_binds_completed_init_and_manifest(self):
        grant = self.grant()
        secret = b'synthetic-signing-key-' * 2
        signature = broker.sign_grant(grant, secret)
        operation = str(uuid.uuid4())
        with self.assertRaises(broker.BrokerHeld):
            self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=operation, secret=secret)
        with self.authority.hold(grant, 2, create=True):
            self.authority.completed(grant, {'uuid': grant['jobId'], 'success': True})
        with self.assertRaises(broker.BrokerHeld):
            self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=grant['operationId'], secret=secret)
        issue = self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=operation, secret=secret)
        self.assertEqual(issue['fileBytes'], 3)
        self.assertEqual(issue, self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=operation, secret=secret))
        with self.assertRaises(broker.BrokerHeld):
            self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=str(uuid.uuid4()), secret=secret)
        with self.assertRaises(broker.BrokerHeld):
            self.authority._issue_upload_candidate(grant, signature, object_id='missing', operation_id=str(uuid.uuid4()), secret=secret)
        with self.queue._db() as db:
            db.execute("UPDATE jobs SET status='held',held_reason='test_hold' WHERE id=?", (grant['jobId'],))
        with self.assertRaises(broker.BrokerHeld):
            self.authority._issue_upload_candidate(grant, signature, object_id='image-1', operation_id=operation, secret=secret)

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

    def test_commit_success_only_does_not_weaken_init_identity(self):
        task=str(uuid.uuid4())
        self.assertEqual(broker._commit_reply({'success':True},task),{'success':True})
        with self.assertRaises(broker.BrokerHeld):broker._reply({'success':True},task)
        for reply in ({'success':False},{'success':True,'uuid':str(uuid.uuid4())},{'error':'failed'}):
            with self.subTest(reply=reply),self.assertRaises(broker.BrokerHeld):broker._commit_reply(reply,task)

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
                issue_id = str(uuid.uuid4())
                issue_signature = broker.sign_reference_issue(grant, 'image-1', issue_id, secret)
                issue = service.issue_reference_upload_candidate(grant, broker.sign_grant(grant, secret), object_id='image-1', operation_id=issue_id, issue_signature=issue_signature)
                self.assertEqual(issue['fileSha256'], hashlib.sha256(b'abc').hexdigest())
                with self.assertRaises(broker.BrokerHeld):
                    service.issue_reference_upload_candidate(grant, '0'*64, object_id='image-1', operation_id=issue_id, issue_signature=issue_signature)
                with self.assertRaises(broker.BrokerHeld):
                    service.issue_reference_upload_candidate(grant, broker.sign_grant(grant, secret), object_id='image-1', operation_id=str(uuid.uuid4()), issue_signature=issue_signature)
                with self.assertRaises(broker.BrokerHeld):
                    service.issue_reference_upload_candidate(grant, broker.sign_grant(grant, secret), object_id='other', operation_id=issue_id, issue_signature=issue_signature)
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
                allowance = service.start_reference_upload_intent_candidate(grant, broker.sign_grant(grant, secret),
                    object_id='image-1', operation_id=issue_id, issue_signature=issue_signature)
                with self.authority.hold(grant, 2, _allowed_upload=allowance):
                    self.authority.check_deadline()
                with self.assertRaises(broker.BrokerHeld):
                    with self.authority.hold(grant, 2, _allowed_upload=(allowance[0], allowance[1], '0'*64)):
                        self.fail('changed upload intent crossed authority')
                with self.assertRaises(broker.BrokerHeld):
                    service.start_reference_upload_intent_candidate(grant, broker.sign_grant(grant, secret),
                        object_id='image-1', operation_id=issue_id, issue_signature=issue_signature)
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

    def _sender_rejects_corrupted_local_init(self, corruption):
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
            signature = broker.sign_grant(grant, secret)
            with patch.object(adapter, 'exchange', return_value={'uuid':grant['jobId'],'success':True}):
                service.mutate(grant, signature, b'init')
            op = str(uuid.uuid4())
            issue_sig = broker.sign_reference_issue(grant, 'image-1', op, secret)
            service.issue_reference_upload_candidate(grant, signature, object_id='image-1', operation_id=op, issue_signature=issue_sig)
            if corruption == 'grant':
                service.db.execute('UPDATE operations SET grant_hash=? WHERE id=?', ('0'*64,grant['operationId']))
            elif corruption == 'reply':
                service.db.execute('UPDATE operations SET reply=? WHERE id=?', (json.dumps({'uuid':grant['jobId'],'success':True,'changed':True}),grant['operationId']))
            service.db.commit()
            worker = object.__new__(ReferenceEngineWorker)
            worker.engine_id, worker.node_id, worker.authority_id = 'synthetic', 'cluster', self.queue.authority_id
            worker.origin, worker.gate_generation = 'http://127.0.0.1:18080', 1
            worker.ledger = MagicMock()
            ledger_db = worker.ledger._transaction.return_value.__enter__.return_value
            worker.ledger._reservation.return_value = dict(job=grant['jobId'], generation=grant['taskGeneration'],
                environment=grant['environment'], settings=json.dumps({'manifest_sha256':grant['inputManifestSha256']}))
            path = self.root / 'input.jpg'
            path.write_bytes(b'abc')
            worker._file = MagicMock(return_value=(path,3,hashlib.sha256(b'abc').hexdigest()))
            def consume(upload, source, **kwargs):
                chunks = []
                source.settimeout(2)
                while data := source.recv(1024):
                    chunks.append(data)
                self.assertEqual(b''.join(chunks), b'abc')
                if corruption == 'loss':
                    raise broker.BrokerHeld('synthetic response lost')
                return {'success':True}
            with patch.object(adapter, 'exchange_upload', side_effect=consume) as send:
                if corruption in ('grant', 'reply', 'loss'):
                    message = {'grant':'reference local init reservation changed', 'reply':'reference local init proof changed', 'loss':'synthetic response lost'}[corruption]
                    with self.assertRaisesRegex(broker.BrokerHeld, message):
                        service.send_reference_upload_candidate(grant, signature, object_id='image-1', operation_id=op,
                            issue_signature=issue_sig, worker=worker, reservation_id='synthetic', relative_path='input.jpg')
                else:
                    result = service.send_reference_upload_candidate(grant, signature, object_id='image-1', operation_id=op,
                        issue_signature=issue_sig, worker=worker, reservation_id='synthetic', relative_path='input.jpg')
                    self.assertEqual(result['state'], 'complete')
                self.assertEqual(send.call_count, 0 if corruption in ('grant','reply') else 1)
                if corruption == 'loss':
                    with self.assertRaises(broker.BrokerHeld):
                        service.send_reference_upload_candidate(grant, signature, object_id='image-1', operation_id=op,
                            issue_signature=issue_sig, worker=worker, reservation_id='synthetic', relative_path='input.jpg')
                    self.assertEqual(send.call_count, 1)
            worker._file.assert_called_once()
            local = service.db.execute('SELECT state FROM operations WHERE id=?',(op,)).fetchone()
            if corruption in ('grant','reply'):
                self.assertIsNone(local)
            else:
                self.assertEqual(local['state'], 'started' if corruption == 'loss' else 'complete')
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(op,)).fetchone()[0], 'started' if corruption in ('grant','reply','loss') else 'complete')
                receipt = db.execute('SELECT receipt_json FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(op,)).fetchone()
                self.assertEqual(receipt is not None, corruption in ('success','commit_success','commit_loss','commit_crash'))
            if corruption == 'success':
                self.assertEqual(service.verify_reference_upload_inventory_candidate(grant, signature), 1)
                saved_reservation = service.db.execute('SELECT identity FROM reservation WHERE singleton=1').fetchone()[0]
                service.db.execute('DELETE FROM reservation WHERE singleton=1')
                service.db.commit()
                with self.assertRaisesRegex(broker.BrokerHeld, 'reference inventory init changed'):
                    service.verify_reference_upload_inventory_candidate(grant, signature)
                service.db.execute('INSERT INTO reservation VALUES(1,?)', (saved_reservation,))
                old_reply = service.db.execute('SELECT reply FROM operations WHERE id=?', (grant['operationId'],)).fetchone()[0]
                service.db.execute('UPDATE operations SET reply=? WHERE id=?', (json.dumps({'uuid':grant['jobId'],'success':True,'changed':True}),grant['operationId']))
                service.db.commit()
                with self.assertRaisesRegex(broker.BrokerHeld, 'reference inventory init changed'):
                    service.verify_reference_upload_inventory_candidate(grant, signature)
                service.db.execute('UPDATE operations SET reply=? WHERE id=?', (old_reply,grant['operationId']))
                service.db.commit()
                with self.queue._db() as db:
                    saved_intent = tuple(db.execute('SELECT * FROM broker_intents WHERE operation_id=?', (grant['operationId'],)).fetchone())
                    db.execute('DELETE FROM broker_intents WHERE operation_id=?', (grant['operationId'],))
                with self.assertRaisesRegex(broker.BrokerHeld, 'reference inventory init changed'):
                    service.verify_reference_upload_inventory_candidate(grant, signature)
                with self.queue._db() as db:
                    db.execute('INSERT INTO broker_intents VALUES(?,?,?,?)', saved_intent)
                    db.execute('UPDATE broker_intents SET grant_hash=? WHERE operation_id=?', ('0'*64, grant['operationId']))
                with self.assertRaisesRegex(broker.BrokerHeld, 'reference inventory init changed'):
                    service.verify_reference_upload_inventory_candidate(grant, signature)
                with self.queue._db() as db:
                    db.execute('UPDATE broker_intents SET grant_hash=? WHERE operation_id=?', (saved_intent[2],grant['operationId']))
                    saved_receipt = db.execute('SELECT receipt_json FROM reference_broker_upload_receipt_candidate WHERE operation_id=?',(op,)).fetchone()[0]
                    changed_receipt = json.loads(saved_receipt)
                    changed_receipt['sha256'] = '0'*64
                    db.execute('UPDATE reference_broker_upload_receipt_candidate SET receipt_json=? WHERE operation_id=?', (json.dumps(changed_receipt),op))
                bad_commit_id = str(uuid.uuid4())
                bad_commit_sig = broker.sign_reference_commit(grant,bad_commit_id,b'','application/json',secret)
                with patch.object(adapter,'exchange') as forward:
                    with self.assertRaises(broker.BrokerHeld):
                        service.commit_reference_candidate(grant,signature,operation_id=bad_commit_id,body=b'',
                            content_type='application/json',commit_signature=bad_commit_sig)
                    stale = dict(grant,taskGeneration=grant['taskGeneration']+1)
                    with self.assertRaises(broker.BrokerHeld):
                        service.commit_reference_candidate(stale,broker.sign_grant(stale,secret),operation_id=bad_commit_id,body=b'',
                            content_type='application/json',commit_signature=broker.sign_reference_commit(stale,bad_commit_id,b'','application/json',secret))
                    forward.assert_not_called()
                with self.queue._db() as db:
                    db.execute('DELETE FROM reference_broker_upload_receipt_candidate WHERE operation_id=?', (op,))
                with self.assertRaises(broker.BrokerHeld):
                    service.verify_reference_upload_inventory_candidate(grant, signature)
                commit_id = str(uuid.uuid4())
                commit_sig = broker.sign_reference_commit(grant,commit_id,b'','application/json',secret)
                with patch.object(adapter,'exchange') as forward:
                    with self.assertRaises(broker.BrokerHeld):
                        service.commit_reference_candidate(grant,signature,operation_id=commit_id,body=b'',
                            content_type='application/json',commit_signature=commit_sig)
                    forward.assert_not_called()
            if corruption in ('commit_success','commit_loss','commit_crash'):
                commit_id = str(uuid.uuid4())
                commit_sig = broker.sign_reference_commit(grant,commit_id,b'','application/json',secret)
                response = {'uuid':grant['jobId'],'success':True}
                effect = response if corruption in ('commit_success','commit_crash') else broker.BrokerHeld('synthetic commit response lost')
                def crash_after_accept(*args, **kwargs):
                    marker = Path(os.environ['LT_REFERENCE_CRASH_MARKER'])
                    payload = json.dumps({'queue':str(self.queue.path),'broker':str(service.path),'operation':commit_id,
                        'root':str(self.root),'epoch':grant['coordinatorEpoch']}).encode()
                    fd = os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                    try:
                        os.write(fd,payload)
                        os.fsync(fd)
                    finally:os.close(fd)
                    directory = os.open(marker.parent,os.O_RDONLY|os.O_DIRECTORY)
                    try:os.fsync(directory)
                    finally:os.close(directory)
                    os._exit(77)
                callback = crash_after_accept if corruption == 'commit_crash' else effect if isinstance(effect,Exception) else None
                with patch.object(adapter,'exchange',side_effect=callback,
                                  return_value=response) as forward:
                    with self.assertRaisesRegex(broker.BrokerHeld,'reference commit authentication failed'):
                        service.commit_reference_candidate(grant,signature,operation_id=commit_id,body=b'changed',
                            content_type='application/json',commit_signature=commit_sig)
                    with self.assertRaisesRegex(broker.BrokerHeld,'reference commit authentication failed'):
                        service.commit_reference_candidate(grant,signature,operation_id=str(uuid.uuid4()),body=b'',
                            content_type='application/json',commit_signature=commit_sig)
                    self.assertIsNone(service.db.execute('SELECT 1 FROM operations WHERE id=?',(commit_id,)).fetchone())
                    forward.assert_not_called()
                    if corruption == 'commit_loss':
                        with self.assertRaisesRegex(broker.BrokerHeld,'synthetic commit response lost'):
                            service.commit_reference_candidate(grant,signature,operation_id=commit_id,body=b'',
                                content_type='application/json',commit_signature=commit_sig)
                    else:
                        self.assertEqual(service.commit_reference_candidate(grant,signature,operation_id=commit_id,body=b'',
                            content_type='application/json',commit_signature=commit_sig),response)
                    with self.assertRaises(broker.BrokerHeld):
                        service.commit_reference_candidate(grant,signature,operation_id=commit_id,body=b'',
                            content_type='application/json',commit_signature=commit_sig)
                    self.assertEqual(forward.call_count,1)
                with self.queue._db() as db:
                    state = db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(commit_id,)).fetchone()[0]
                    self.assertEqual(state,'complete' if corruption == 'commit_success' else 'started')
                if corruption == 'commit_success':
                    self.assertEqual(service.verify_reference_commit_candidate(grant,signature,operation_id=commit_id),response)
                    from processing import reference_engine_worker as worker_module
                    worker.worker_id = grant['workerId']
                    worker.path = self.root / 'candidate-worker.sqlite3'
                    worker.engine = MagicMock()
                    worker.engine.read.return_value = {'uuid':grant['jobId'],'status':{'code':10}}
                    with sqlite3.connect(worker.path) as worker_db:
                        worker_db.execute('CREATE TABLE executions (job TEXT PRIMARY KEY, identity TEXT, receipt TEXT, phase TEXT, outbox_id TEXT, result TEXT)')
                        worker_db.execute('INSERT INTO executions(job,identity,receipt,phase) VALUES(?,?,?,?)',(grant['jobId'],json.dumps({
                            'job':{'jobId':grant['jobId']},'brokerProof':{'initGrant':grant,'initSignature':signature,
                            'commitOperationId':commit_id}}),uuid.UUID(grant['jobId']).hex,'broker_processing_held'))
                    with patch.object(worker_module,'BROKER_WORKER_CANDIDATE_QUALIFIED',True):
                        self.assertEqual(worker.resume_broker_candidate(grant['jobId'],broker=service),'broker_processing')
                        self.assertIsNone(worker.poll(grant['jobId'],broker=service))
                        self.assertEqual(worker.engine.read.call_count,1)
                        with self.queue._db() as db:
                            db.execute("UPDATE jobs SET status='held',held_reason='synthetic_revocation' WHERE id=?",(grant['jobId'],))
                        with self.assertRaises(broker.BrokerHeld):
                            worker.poll(grant['jobId'],broker=service)
                        self.assertEqual(worker.engine.read.call_count,1)
                        with self.queue._db() as db:
                            db.execute("UPDATE jobs SET status='running',held_reason=NULL WHERE id=?",(grant['jobId'],))
                    with self.queue._db() as db:
                        db.execute('UPDATE reference_broker_completion_candidate SET response_sha256=? WHERE operation_id=?',('0'*64,commit_id))
                    with self.assertRaisesRegex(broker.BrokerHeld,'reference commit reply changed'):
                        service.verify_reference_commit_candidate(grant,signature,operation_id=commit_id)
                    with self.queue._db() as db:
                        db.execute('UPDATE reference_broker_completion_candidate SET response_sha256=? WHERE operation_id=?',
                                   (hashlib.sha256(broker._json(response)).hexdigest(),commit_id))
                    from processing import reference_protocol as protocol
                    from processing.reference_delivery import DeliveryReceiver
                    result = dict(schemaVersion=1,jobId=grant['jobId'],generation=grant['taskGeneration'],
                        environment=grant['environment'],pipelineDigest=grant['pipelineDigest'],engineVersions={'odm':'synthetic'},
                        objects=[dict(id='odm-archive',role='odm_archive',byteSize=1,sha256=hashlib.sha256(b'x').hexdigest())])
                    result['resultManifestSha256'] = protocol.digest(result)
                    binding = dict(jobId=grant['jobId'],generation=grant['taskGeneration'],environment=grant['environment'],
                        inputManifestSha256=grant['inputManifestSha256'],pipelineDigest=grant['pipelineDigest'],
                        resultManifestSha256=result['resultManifestSha256'])
                    payload = dict(**binding,resultManifest=result,terminalState='succeeded')
                    outbox_id = protocol.digest(binding)
                    envelope = dict(outboxId=outbox_id,payloadSha256=protocol.digest(payload),payload=payload)
                    with worker._db() as worker_db:
                        worker_db.execute("UPDATE executions SET outbox_id=?,result=?,phase='awaiting_delivery' WHERE job=?",
                                          (outbox_id,json.dumps(result),grant['jobId']))
                    worker.outbox = MagicMock()
                    worker.outbox.get.return_value = {'state':'acknowledged'}
                    with NodeGate('synthetic') as gate:
                        gate.prepare_completion(uuid.UUID(grant['jobId']).hex,dict(status='succeeded',
                            result={'resultManifestSha256':result['resultManifestSha256']}))
                    with patch.object(worker_module,'BROKER_WORKER_CANDIDATE_QUALIFIED',True):
                        with self.assertRaises(broker.BrokerHeld):
                            worker.acknowledge_delivery(grant['jobId'],broker=service)
                        receiver = DeliveryReceiver(self.store)
                        durable = receiver.receive(envelope,node_id='cluster',worker_id='worker',worker_instance_id='instance',
                            claim_request_id='claim-1',claim_token=self.claim['claimToken'])
                        self.assertTrue(durable['durable'])
                        worker.acknowledge_delivery(grant['jobId'],broker=service)
                    with NodeGate('synthetic') as gate:
                        self.assertIsNone(gate.snapshot()['reservation'])
                else:
                    with self.assertRaises(broker.BrokerHeld):
                        service.verify_reference_commit_candidate(grant,signature,operation_id=commit_id)
                local_commit = service.db.execute('SELECT state FROM operations WHERE id=?',(commit_id,)).fetchone()
                self.assertEqual(local_commit['state'],'complete' if corruption == 'commit_success' else 'started')
            self.assertEqual(service._unreaped, [])

    def test_sender_rejects_corrupted_init_grant(self):
        self._sender_rejects_corrupted_local_init('grant')

    def test_sender_rejects_corrupted_init_reply(self):
        self._sender_rejects_corrupted_local_init('reply')

    def test_sender_success_records_receipt(self):
        self._sender_rejects_corrupted_local_init('success')

    def test_sender_lost_response_keeps_started_intents(self):
        self._sender_rejects_corrupted_local_init('loss')

    def test_commit_success_cannot_replay(self):
        self._sender_rejects_corrupted_local_init('commit_success')

    def test_commit_lost_response_cannot_replay(self):
        self._sender_rejects_corrupted_local_init('commit_loss')

    def test_commit_crash_after_acceptance(self):
        marker_name = os.environ.get('LT_REFERENCE_CRASH_MARKER')
        if marker_name and sys.argv == ['-c','--lt-reference-crash-child']:
            self._sender_rejects_corrupted_local_init('commit_crash')
            self.fail('child did not crash at acceptance')
        crash_root = Path(tempfile.mkdtemp(prefix='lt-reference-crash-parent-')).resolve()
        print('retained synthetic crash root:',crash_root,flush=True)
        marker = crash_root / 'accepted.json'
        env = dict(os.environ, LT_REFERENCE_CRASH_MARKER=str(marker))
        code = "import unittest; suite=unittest.defaultTestLoader.loadTestsFromName('tests.test_reference_broker_authority_candidate.ReferenceBrokerAuthorityCandidateTests.test_commit_crash_after_acceptance'); result=unittest.TextTestRunner().run(suite); raise SystemExit(not result.wasSuccessful())"
        child = subprocess.Popen([sys.executable,'-c',code,'--lt-reference-crash-child'],
            stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
        try:
            try:
                output,_ = child.communicate(timeout=12)
            except subprocess.TimeoutExpired:
                self.fail('owned synthetic crash child timed out')
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=3)
        self.assertEqual(child.returncode,77,output.decode(errors='replace')[-2000:])
        details = json.loads(marker.read_text())
        queue_path, broker_path = Path(details['queue']),Path(details['broker'])
        root = Path(details['root']).resolve()
        self.assertTrue(queue_path.resolve().is_relative_to(root))
        self.assertTrue(broker_path.resolve().is_relative_to(root))
        from processing.broker_intent_fence import clear
        coordinator = sqlite3.connect('file:'+str(queue_path)+'?mode=ro',uri=True)
        local = sqlite3.connect('file:'+str(broker_path)+'?mode=ro',uri=True)
        try:
            self.assertFalse(clear(coordinator))
            self.assertEqual(coordinator.execute('SELECT state FROM broker_intents WHERE operation_id=?',(details['operation'],)).fetchone()[0],'started')
            self.assertEqual(local.execute('SELECT state FROM operations WHERE id=?',(details['operation'],)).fetchone()[0],'started')
        finally:
            coordinator.close();local.close()
        restarted = Queue(root / 'queue')
        try:
            self.assertNotEqual(restarted.startup_id,details['epoch'])
            with restarted._db() as db:
                self.assertFalse(restarted._transfer_admission_clear(db))
                self.assertEqual(db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(details['operation'],)).fetchone()[0],'started')
        finally: restarted.close()

    def test_broker_pending_phases_never_poll_engine(self):
        worker = object.__new__(ReferenceEngineWorker)
        worker.engine_id = 'synthetic'
        worker.engine = MagicMock()
        fake_gate = MagicMock()
        fake_gate.__enter__.return_value = fake_gate
        with patch('processing.reference_engine_worker.NodeGate', return_value=fake_gate):
            for phase in ('broker_commit_pending','broker_processing_held'):
                worker._row = MagicMock(return_value={'outbox_id':None,'phase':phase})
                with self.assertRaisesRegex(ValueError,'uncertain precommit'):
                    worker.poll(str(uuid.uuid4()))
        worker.engine.read.assert_not_called()

    def test_actual_worker_signed_plan_starts_through_broker_only(self):
        from processing import reference_engine_worker as worker_module
        gate_root = self.root / 'gate'
        with patch.object(broker, 'QUALIFIED', True), \
             patch.object(worker_module,'BROKER_WORKER_CANDIDATE_QUALIFIED',True), \
             patch('processing.node_gate.canonical_gate_path', return_value=gate_root), \
             patch.object(broker, 'canonical_gate_path', return_value=gate_root):
            with NodeGate('synthetic') as gate:
                gate.provision('cluster','http://127.0.0.1:18080',self.queue.authority_id)
            state = gate_root / 'mutation-broker'
            state.mkdir(mode=0o700)
            adapter = broker.LoopbackNodeODM('synthetic',BackendPin('a'*64,'sha256:'+'b'*64,12345),'')
            secret = b'synthetic-signing-key-' * 2
            service = broker.NodeMutationBroker(state,node_id='cluster',engine_id='synthetic',
                authority_id=self.queue.authority_id,coordinator_epoch=self.queue.startup_id,
                coordinator_origin='http://127.0.0.1:18080',gate_generation=1,secret=secret,
                adapter=adapter,authority_witness=self.authority)
            self.addCleanup(service.close)
            worker = object.__new__(ReferenceEngineWorker)
            worker.engine_id,worker.node_id,worker.authority_id = 'synthetic','cluster',self.queue.authority_id
            worker.worker_id,worker.origin,worker.gate_generation = 'worker','http://127.0.0.1:18080',1
            worker.allowed_options = frozenset({'pc-quality','projection'})
            worker.path = self.root / 'actual-worker.sqlite3'
            worker.engine = MagicMock()
            worker.engine.read.return_value = {'uuid':self.job['jobId'],'status':{'code':10}}
            from processing.scratch_policy import ScratchLedger
            from processing.worker_outbox import WorkerOutbox
            storage = self.root / 'worker-storage'
            storage.mkdir(mode=0o700)
            worker.ledger = ScratchLedger(self.root / 'scratch.sqlite3',storage,capacity_probe=lambda _:1000000,safety_floor_bytes=0)
            worker.ledger.reserve(job_id=self.job['jobId'],generation=self.job['generation'],reservation_id='synthetic',
                manifest_sha256=self.job['manifestSha256'],input_bytes=3,output_budget=4096,
                environment='staging',ttl_seconds=3600,worker_ready=True)
            directory = worker.ledger.ensure_job_directory('synthetic')
            source = directory / 'input.jpg'
            source.write_bytes(b'abc')
            worker.ledger.record_allocated('synthetic',3)
            worker.outbox = WorkerOutbox(self.root / 'outbox.sqlite3')
            worker.version = 'synthetic'
            with sqlite3.connect(worker.path) as db:
                db.execute('CREATE TABLE executions (job TEXT PRIMARY KEY, identity TEXT, receipt TEXT, phase TEXT, result TEXT, outbox_id TEXT, archive_size INTEGER, archive_sha TEXT, part_dev TEXT, part_ino TEXT, archive_expected_size TEXT)')
            job = self.store.get(self.job['jobId'],private=True)
            body, content_type = worker_module._reference_broker_init_body(job)
            grant = dict(self.grant(),bodySha256=hashlib.sha256(body).hexdigest(),contentType=content_type)
            signature = broker.sign_grant(grant,secret)
            upload_id,commit_id = str(uuid.uuid4()),str(uuid.uuid4())
            plan = dict(initGrant=grant,initSignature=signature,initBody=body,
                uploads=[dict(objectId='image-1',operationId=upload_id,signature=broker.sign_reference_issue(grant,'image-1',upload_id,secret))],
                commit=dict(operationId=commit_id,body=b'',contentType='application/json',
                            signature=broker.sign_reference_commit(grant,commit_id,b'','application/json',secret)))
            def consume(upload,sock,**kwargs):
                sock.settimeout(2)
                chunks=[]
                while data := sock.recv(1024):chunks.append(data)
                self.assertEqual(b''.join(chunks),b'abc')
                return {'success':True}
            with patch.object(adapter,'exchange',return_value={'uuid':grant['jobId'],'success':True}) as mutate, \
                 patch.object(adapter,'exchange_upload',side_effect=consume) as upload:
                with self.assertRaisesRegex(ValueError,'initialization differs'):
                    worker.start_broker_candidate(job,reservation_id='synthetic',input_paths={'image-1':'jobs/synthetic/input.jpg'},
                        broker=service,plan=dict(plan,initBody=b'wrong'))
                mutate.assert_not_called()
                self.assertEqual(worker.start_broker_candidate(job,reservation_id='synthetic',input_paths={'image-1':'jobs/synthetic/input.jpg'},
                    broker=service,plan=plan),'broker_processing_held')
                self.assertEqual(mutate.call_count,2)
                self.assertEqual(upload.call_count,1)
                worker.engine.initialize.assert_not_called()
                worker.engine.upload.assert_not_called()
                worker.engine.action.assert_not_called()
                self.assertEqual(worker.resume_broker_candidate(job['jobId'],broker=service),'broker_processing')
                self.assertIsNone(worker.poll(job['jobId'],broker=service))
                self.assertEqual(service._unreaped,[])
                output = io.BytesIO()
                with zipfile.ZipFile(output,'w') as archive:
                    archive.writestr('synthetic.txt','synthetic output')
                archive_bytes = output.getvalue()
                def synthetic_download(row,reservation,part,budget):
                    part.write_bytes(archive_bytes)
                    worker.ledger.record_allocated(reservation,3+len(archive_bytes))
                    return len(archive_bytes),hashlib.sha256(archive_bytes).hexdigest()
                worker._download_archive = synthetic_download
                worker.engine.read.return_value = {'uuid':grant['jobId'],'status':{'code':40}}
                outbox_id = worker.poll(job['jobId'],broker=service)
                claimed = worker.outbox.claim_next()
                self.assertEqual(claimed['outboxId'],outbox_id)
                from processing.reference_delivery import DeliveryReceiver
                receiver = DeliveryReceiver(self.store)
                envelope = {k:claimed[k] for k in ('outboxId','payloadSha256','payload')}
                receipt = receiver.receive(envelope,node_id='cluster',worker_id='worker',worker_instance_id='instance',
                    claim_request_id='claim-1',claim_token=self.claim['claimToken'])
                from processing import reference_protocol as protocol
                viewer_receipt = dict(schemaVersion=1,receiptId='synthetic-viewer-receipt',storageReceiptId='synthetic-viewer-storage',
                    **{k:claimed['payload'][k] for k in ('jobId','generation','environment','inputManifestSha256','pipelineDigest','resultManifestSha256')},
                    acceptedObjectIds=['odm-archive'],acceptedAt='2026-11-01T00:00:00Z',validationVersion=protocol.VALIDATION_VERSION)
                self.assertTrue(self.store.accept(job['jobId'],viewer_receipt)['fullyAccepted'])
                # Simulate a lost ACK by replaying exactly before local acknowledgement.
                self.assertEqual(receiver.receive(envelope,node_id='cluster',worker_id='worker',worker_instance_id='instance',
                    claim_request_id='claim-1',claim_token=self.claim['claimToken']),receipt)
                worker.outbox.acknowledge(outbox_id,receipt)
                worker.acknowledge_delivery(job['jobId'],broker=service)
                with NodeGate('synthetic') as gate:
                    self.assertIsNone(gate.snapshot()['reservation'])
                self.assertTrue((directory / 'all.zip').is_file())
                self.assertTrue(source.is_file())


if __name__ == '__main__':
    unittest.main()
