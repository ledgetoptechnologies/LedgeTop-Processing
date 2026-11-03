import os
import unittest
import uuid
from unittest.mock import patch

from processing.host_mutation_journal import HostMutationJournal, MutationHeld
from processing.node_gate import GateError, NodeGate
from processing.reference_engine_worker import WorkerHeld
from processing import reference_protocol as protocol
from processing.odx_smoke_worker import SmokeError
from processing.viewer_service import NativeEngine
from ops.native_node_fixture import NativeNodeFixture
from tests.test_reference_engine_worker import ReferenceEngineWorkerTests


class GuardedWorkerTests(ReferenceEngineWorkerTests):
    # Do not rerun the parent class's full suite under guarded mode.
    def make_guarded(self):
        return self.make_worker(host_mutation_journal=True)

    def test_completion_checkpoint_restarts_and_exact_receipt_releases(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'uuid': identifier}
        self.start()
        original = NodeGate._save
        def crash_after_checkpoint(gate, state):
            original(gate, state)
            if state.get('hostMutationJournal', {}).get('completion'):
                raise OSError('synthetic crash after durable gate checkpoint')
        with patch.object(NodeGate, '_save', crash_after_checkpoint):
            with self.assertRaises(OSError): self.worker.poll(self.job['jobId'])
        self.worker = self.make_worker()
        outbox_id = self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls.count('download'), 1)
        completion = self.outbox.get(outbox_id)
        with self.assertRaises(WorkerHeld): self.worker.acknowledge_delivery(self.job['jobId'])
        payload = completion['payload']
        receipt = dict(type='durable-result-receipt', durable=True, receiptId='receipt-1',
            storageReceiptId='storage-1', outboxId=outbox_id, payloadSha256=completion['digest'],
            acceptedObjectIds=[o['id'] for o in payload['resultManifest']['objects']],
            **{key: payload[key] for key in ('jobId', 'generation', 'environment',
                'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')})
        self.outbox.claim_next()
        self.outbox.acknowledge(outbox_id, receipt)
        self.worker.acknowledge_delivery(self.job['jobId'])
        self.worker.acknowledge_delivery(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            state = gate.snapshot()
            self.assertIsNone(state['reservation'])
            self.assertNotIn('hostMutationJournal', state)
        self.assertTrue((self.scratch / 'jobs/reservation-1/all.zip').exists())

    def test_crash_after_release_replays_durable_tombstone(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'uuid': identifier}
        self.start()
        outbox_id = self.worker.poll(self.job['jobId'])
        completion = self.outbox.get(outbox_id)
        payload = completion['payload']
        receipt = dict(type='durable-result-receipt', durable=True, receiptId='receipt-2',
            storageReceiptId='storage-2', outboxId=outbox_id, payloadSha256=completion['digest'],
            acceptedObjectIds=[o['id'] for o in payload['resultManifest']['objects']],
            **{key: payload[key] for key in ('jobId', 'generation', 'environment',
                'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')})
        self.outbox.claim_next()
        self.outbox.acknowledge(outbox_id, receipt)
        original = NodeGate._save
        def crash_after_release(gate, state):
            original(gate, state)
            if state['reservation'] is None:
                raise OSError('synthetic crash after durable release')
        with patch.object(NodeGate, '_save', crash_after_release):
            with self.assertRaises(OSError): self.worker.acknowledge_delivery(self.job['jobId'])
        # A different job may acquire the node before the old worker's local
        # journal records its already-durable release.
        with NodeGate('physical-engine') as gate:
            gate.reserve('cluster', 'http://127.0.0.1:8000', self.authority, 1, 'b' * 32)
            gate.bind('b' * 32, dict(id=str(uuid.uuid4()), workerId='worker',
                nodeId='cluster', authorityId=self.authority))
            other = HostMutationJournal(gate, 'b' * 32).create()
            other.close()
            before = gate.snapshot()
        self.worker = self.make_worker()
        self.worker.acknowledge_delivery(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            self.assertEqual(gate.snapshot(), before)
        self.assertTrue((self.scratch / 'jobs/reservation-1/all.zip').exists())

    def test_nonterminal_or_mismatched_task_never_checkpoints(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'uuid': identifier}
        self.start()
        self.engine.code = 20
        self.assertIsNone(self.worker.poll(self.job['jobId']))
        self.engine.read = lambda route: {'uuid': str(uuid.uuid4()), 'status': {'code': 40}}
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            state = gate.snapshot()
            self.assertIsNone(state['reservation']['completionDigest'])
            self.assertNotIn('completion', state['hostMutationJournal'])

    def test_guarded_real_worker_records_exact_identity_and_checkpoints_completion(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'success': True, 'uuid': identifier}
        self.assertEqual(self.start(), 'processing')
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            rows = journal.db.execute('SELECT kind,request_digest,state FROM operations ORDER BY seq').fetchall()
            self.assertEqual([r[0] for r in rows], ['init', 'upload', 'commit'])
            self.assertTrue(all(r[2] == 'done' for r in rows))
            task = dict(name=self.job['jobId'], engine_uuid=self.job['jobId'], outputs=None, options=[])
            self.assertEqual(rows[0][1], protocol.digest(task))
            self.assertEqual(rows[1][1], protocol.digest(dict(task=self.job['jobId'], name=self.job['submission']['manifest']['objects'][0]['name'], bytes=3,
                sha256=self.job['submission']['manifest']['objects'][0]['sha256'])))
            self.assertEqual(rows[2][1], protocol.digest(dict(task=self.job['jobId'])))
            journal.check_reconciliation_ready()
            self.assertEqual(journal.job['id'], self.job['jobId'])
            with self.assertRaises(GateError):
                gate.prepare_completion(row['receipt'], journal.job)
            journal.close()
        # Restart with the default disabled does not unguard prior executions.
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'processing')
        outbox = self.worker.poll(self.job['jobId'])
        self.assertIsNotNone(outbox)
        with NodeGate('physical-engine') as gate:
            state = gate.snapshot()
            self.assertEqual(state['hostMutationJournal']['completion']['outboxId'], outbox)
            self.assertIsNotNone(state['reservation']['completionDigest'])
        self.assertEqual(self.engine.calls.count('initialize'), 1)
        self.assertEqual(self.engine.calls.count('upload'), 1)

    @unittest.skipUnless(hasattr(os, 'fork'), 'requires actual process death')
    def test_crash_during_actual_upload_leaves_pending_no_replay(self):
        self.worker = self.make_guarded()
        self.engine.upload = lambda *args: os._exit(17)
        child = os.fork()
        if child == 0:
            try:
                self.start()
            finally:
                os._exit(99)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 17)
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            rows = journal.db.execute('SELECT op_id,kind,request_digest,state FROM operations ORDER BY seq').fetchall()
            self.assertEqual([(r[1], r[3]) for r in rows], [('init','done'), ('upload','pending')])
            with self.assertRaises(MutationHeld): journal.begin(rows[1][0], rows[1][1], rows[1][2])
            journal.close()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'upload_pending')
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls, [])

    def test_invalid_commit_response_remains_pending(self):
        self.worker = self.make_guarded()
        self.engine.action = lambda action, identifier: {'uuid': 'wrong'}
        with self.assertRaises((WorkerHeld, SmokeError)): self.start()
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            self.assertEqual(journal.db.execute('SELECT kind,state FROM operations ORDER BY seq DESC LIMIT 1').fetchone(), ('commit','pending'))
            journal.close()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'commit_pending')
        with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])



class GuardedActualHTTPTests(ReferenceEngineWorkerTests):
    def exercise_response(self, response, valid):
        fixture = NativeNodeFixture(self.root / 'fixture.sqlite', synthetic_enabled=True).start()
        self.addCleanup(fixture.close)
        original = fixture._dispatch
        def respond(method, path, headers, body):
            status, value, binary = original(method, path, headers, body)
            if method == 'POST' and path.startswith('/task/new/commit/') and status == 200:
                value = response(self.job['jobId'])
            return status, value, binary
        fixture._dispatch = respond
        self.engine = NativeEngine(fixture.origin)
        self.worker = self.make_worker(host_mutation_journal=True)
        if valid:
            self.assertEqual(self.start(), 'processing')
        else:
            with self.assertRaises((WorkerHeld, SmokeError)): self.start()
        self.assertEqual(fixture.counts(), dict(initialize=1, upload=1, commit=1, download=0))
        row = self.worker._row(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, row['receipt']).open()
            self.assertEqual(journal.db.execute('SELECT kind,state FROM operations ORDER BY seq DESC LIMIT 1').fetchone(),
                             ('commit', 'done' if valid else 'pending'))
            journal.close()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'processing' if valid else 'commit_pending')
        if valid:
            self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        else:
            with self.assertRaises(WorkerHeld): self.worker.poll(self.job['jobId'])
        self.assertEqual(fixture.counts()['commit'], 1)

    def test_http_matching_uuid_acknowledged(self):
        self.exercise_response(lambda job: {'uuid': job}, True)

    def test_http_empty_object_is_uncertain(self):
        self.exercise_response(lambda job: {}, False)

    def test_http_success_without_uuid_is_uncertain(self):
        self.exercise_response(lambda job: {'success': True}, False)

    def test_http_wrong_uuid_is_uncertain(self):
        self.exercise_response(lambda job: {'uuid': str(uuid.uuid4())}, False)

