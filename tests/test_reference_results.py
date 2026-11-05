from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing.node_gate import NodeGate
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_protocol import validate_submission
from processing.reference_results import ReferenceResults, ResultAuthorization, ResultDenied, ResultRangeError, ResultUnavailable
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing.worker_outbox import WorkerOutbox
from tests.test_reference_engine_worker import FakeNativeEngine
from tests.viewer_receipt_signer import SyntheticViewerSigner


class ResultsFixture:
    cleanup_grace_seconds = 0

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.gate_patch.start()
        self.authority = str(uuid.uuid4())
        with NodeGate('physical-engine') as gate:
            gate.provision('cluster', 'http://127.0.0.1:8000', self.authority)
        scratch = self.root / 'scratch'
        scratch.mkdir(mode=0o700)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', scratch,
            capacity_probe=lambda _: 1_000_000, safety_floor_bytes=0,
            cleanup_grace_seconds=self.cleanup_grace_seconds)
        self.outbox = WorkerOutbox(self.root / 'outbox' / 'outbox.sqlite')
        self.engine = FakeNativeEngine()
        raw = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        submission = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'])
        self.job = dict(jobId=str(uuid.uuid4()), generation=1, environment='staging', status='running', nodeId='cluster',
            manifestSha256=submission['manifestSha256'], pipelineDigest=submission['pipelineDigest'], submission=submission)
        self.ledger.reserve(job_id=self.job['jobId'], generation=1, reservation_id='reservation-1',
            manifest_sha256=self.job['manifestSha256'], input_bytes=3, output_budget=1024,
            environment='staging', ttl_seconds=3600, worker_ready=True)
        self.directory = scratch / 'jobs/reservation-1'
        self.directory.mkdir(parents=True, mode=0o700)
        (self.directory / 'image-1.jpg').write_bytes(b'abc')
        self.ledger.record_allocated('reservation-1', 3)
        self.worker = self.make_worker()
        self.viewer_signer = SyntheticViewerSigner()
        self.worker.viewer_receipt_keys = self.viewer_signer.trusted_keys
        self.worker.start(self.job, reservation_id='reservation-1', input_paths={'image-1': 'jobs/reservation-1/image-1.jpg'})
        self.worker.poll(self.job['jobId'])
        self.results = ReferenceResults(self.worker)
        self.metadata = self.results.metadata(self.job['jobId'])
        self.object = self.metadata['objects'][0]
        self.archive = self.directory / 'all.zip'
        self.authorization = ResultAuthorization(node_id='cluster', worker_id='worker', job_id=self.job['jobId'],
            generation=1, environment='staging', result_manifest_sha256=self.metadata['resultManifestSha256'],
            object_ids=(self.object['id'],), share_approved=True)

    def make_worker(self):
        return ReferenceEngineWorker(self.root / 'journal/worker.sqlite', ledger=self.ledger, outbox=self.outbox,
            engine=self.engine, engine_id='physical-engine', node_id='cluster', authority_id=self.authority,
            coordinator_origin='http://127.0.0.1:8000', gate_generation=1, worker_id='worker', engine_version='synthetic-v1',
            engine_probe=lambda deadline: 'physical-engine')

    def tearDown(self):
        self.gate_patch.stop()
        self.temp.cleanup()

    def open_result(self, **kwargs):
        values = dict(generation=1, object_id=self.object['id'], result_manifest_sha256=self.metadata['resultManifestSha256'],
                      authorization=self.authorization, object_sha256=self.object['sha256'])
        values.update(kwargs)
        return self.results.open(self.job['jobId'], **values)

    def active_pins(self):
        with self.ledger._transaction() as db:
            return db.execute("SELECT COUNT(*) FROM pins WHERE state!='released'").fetchone()[0]

    def accept(self):
        receipt = dict(schemaVersion=1, receiptId='viewer-acceptance', storageReceiptId='viewer-storage',
            jobId=self.job['jobId'], generation=1, environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=self.metadata['resultManifestSha256'],
            acceptedObjectIds=[self.object['id']], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        result = json.loads(self.worker._row(self.job['jobId'])['result'])
        self.worker.apply_viewer_acceptance(self.job['jobId'], [
            self.viewer_signer.envelope(receipt, result)])


class ReferenceResultsTests(ResultsFixture, unittest.TestCase):
    def test_full_get_before_viewer_acceptance_is_pinned_path_free_and_idempotent(self):
        calls = list(self.engine.calls)
        for _ in range(2):
            stream = self.open_result()
            self.assertEqual((stream.status, stream.headers['Content-Type']), (200, 'application/zip'))
            self.assertEqual(self.active_pins(), 1)
            self.assertEqual(b''.join(stream.chunks()), self.engine.archive)
            self.assertTrue(stream.closed)
            self.assertEqual(self.active_pins(), 0)
            for private in (str(self.root), 'localObjects', 'ownedPrefix', 'claimToken'):
                self.assertNotIn(private, json.dumps(stream.headers))
        self.assertEqual(self.engine.calls, calls)

    def test_actual_share_decision_and_exact_scopes_required(self):
        for mutation in ({'share_approved': False}, {'worker_id': 'other'}, {'environment': 'production'},
                         {'generation': 2}, {'object_ids': ()}):
            with self.subTest(mutation=mutation), self.assertRaises(ResultDenied):
                self.open_result(authorization=replace(self.authorization, **mutation))
        for mutation in ({'generation': 2}, {'object_sha256': '0' * 64}, {'result_manifest_sha256': '0' * 64}):
            with self.subTest(mutation=mutation), self.assertRaises(ResultUnavailable):
                self.open_result(**mutation)
        self.assertEqual(self.active_pins(), 0)

    def test_single_ranges_and_invalid_ranges(self):
        for header, expected in (('bytes=0-9', self.engine.archive[:10]), ('bytes=-8', self.engine.archive[-8:]),
                                 ('bytes=10-', self.engine.archive[10:])):
            with self.subTest(header=header), self.open_result(range_header=header) as stream:
                self.assertEqual(stream.status, 206)
                self.assertEqual(int(stream.headers['Content-Length']), len(expected))
                self.assertEqual(b''.join(stream.chunks()), expected)
        for header in ('bytes=1-0', 'bytes=9999-', 'bytes=-0', 'bytes=0-1,4-5', 'invalid'):
            with self.subTest(header=header), self.assertRaises(ResultRangeError):
                self.open_result(range_header=header)

    def test_pin_lasts_until_response_close_and_cleanup_race_is_blocked(self):
        self.accept()
        original = self.ledger.set_pin
        checked = []

        def pin(*args, **kwargs):
            original(*args, **kwargs)
            if kwargs['state'] == 'active':
                with self.assertRaises(PolicyConflict):
                    self.ledger.begin_cleanup('reservation-1', self.object['id'], generation=1, sha256=self.object['sha256'])
                checked.append(True)

        with patch.object(self.ledger, 'set_pin', side_effect=pin):
            stream = self.open_result()
            self.assertEqual(stream.read(), self.engine.archive)
            self.assertEqual(self.active_pins(), 1)
            self.assertEqual(self.ledger.cleanup_candidates(), [])
            stream.close()
        self.assertEqual(checked, [True])
        self.assertEqual(self.active_pins(), 0)
        self.assertEqual(len(self.ledger.cleanup_candidates()), 1)
        self.assertTrue(self.archive.exists())

    def test_cleanup_fence_prevents_new_stream_before_any_file_open(self):
        self.accept()
        self.ledger.begin_cleanup('reservation-1', self.object['id'], generation=1, sha256=self.object['sha256'])
        with patch('processing.reference_results.os.open', wraps=os.open) as observed:
            with self.assertRaises(ResultUnavailable):
                self.open_result()
            self.assertEqual(observed.call_count, 0)

    def test_corrupt_or_hardlinked_result_is_rejected_and_pin_released(self):
        data = self.archive.read_bytes()
        self.archive.write_bytes(b'x' + data[1:])
        with self.assertRaises(ResultUnavailable):
            self.open_result()
        self.assertEqual(self.active_pins(), 0)
        self.archive.write_bytes(data)
        os.link(self.archive, self.archive.with_name('hardlink.zip'))
        with self.assertRaises(ResultUnavailable):
            self.open_result()
        self.assertEqual(self.active_pins(), 0)

    def test_symlink_result_rejected(self):
        original = self.archive.with_name('original.zip')
        self.archive.rename(original)
        try:
            self.archive.symlink_to(original)
        except OSError:
            self.skipTest('symlink creation unavailable')
        with self.assertRaises(ResultUnavailable):
            self.open_result()
        self.assertEqual(self.active_pins(), 0)

    def test_restart_uses_durable_journal_and_outbox_without_engine_mutations(self):
        calls = list(self.engine.calls)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.root / 'scratch',
            capacity_probe=lambda _: 1_000_000, safety_floor_bytes=0, cleanup_grace_seconds=0)
        self.outbox = WorkerOutbox(self.root / 'outbox/outbox.sqlite')
        self.worker = self.make_worker()
        self.results = ReferenceResults(self.worker)
        with self.open_result() as stream:
            self.assertEqual(hashlib.sha256(b''.join(stream.chunks())).hexdigest(), self.object['sha256'])
        self.assertEqual(self.engine.calls, calls)

    def test_bounded_reads_and_abort_release_pin_after_descriptor_closes(self):
        original = os.read
        requests = []

        def observed(descriptor, size):
            requests.append(size)
            return original(descriptor, size)

        with patch('processing.reference_results.os.read', side_effect=observed):
            stream = self.open_result()
            self.assertEqual(stream.read(1), self.engine.archive[:1])
            self.assertEqual(self.active_pins(), 1)
            stream.close()
        self.assertLessEqual(max(requests), 64 * 1024)
        self.assertTrue(stream.closed)
        self.assertEqual(self.active_pins(), 0)

    def test_result_changed_after_open_aborts_and_releases_pin(self):
        stream = self.open_result()
        self.archive.write_bytes(b'changed')
        with self.assertRaises(ResultUnavailable):
            stream.read()
        self.assertTrue(stream.closed)
        self.assertEqual(self.active_pins(), 0)


if __name__ == '__main__':
    unittest.main()
