import copy
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.node_gate import NodeGate, GateError
from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
from processing.reference_protocol import validate_submission, validate_result_manifest
from processing.scratch_policy import ScratchLedger
from processing.worker_outbox import WorkerOutbox


class FakeNativeEngine:
    origin = 'http://127.0.0.1:3001'

    def __init__(self):
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, 'w') as archive:
            archive.writestr('report.txt', b'synthetic result')
        self.archive = stream.getvalue()
        self.calls = []
        self.fail = None
        self.code = 40
        self.declared_length = False

    def initialize(self, task):
        self.calls.append('initialize')
        if self.fail == 'initialize':
            raise OSError('synthetic uncertain initialization')

    def upload(self, identifier, item):
        self.calls.append('upload')
        if self.fail == 'upload':
            raise OSError('synthetic uncertain upload')

    def action(self, action, identifier):
        self.calls.append(action)
        if self.fail == 'commit':
            raise OSError('synthetic uncertain commit')

    def read(self, route):
        self.calls.append('read')
        return {'uuid': route.split('/')[2], 'status': {'code': self.code}}

    def download(self, identifier):
        self.calls.append('download')
        if self.fail == 'open_download':
            raise OSError('synthetic source offline')
        if self.fail == 'download':
            class Partial(io.BytesIO):
                def read(self, size=-1):
                    if self.tell():
                        raise OSError('synthetic interrupted response')
                    return super().read(min(size, 16))
            response = Partial(self.archive)
        else:
            response = io.BytesIO(self.archive)
        if self.declared_length:
            response.headers = {'Content-Length': str(len(self.archive))}
        return response


class ReferenceEngineWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.gate_patch.start()
        self.authority = str(uuid.uuid4())
        with NodeGate('physical-engine') as gate:
            gate.provision('cluster', 'http://127.0.0.1:8000', self.authority)
        self.scratch = self.root / 'scratch'
        self.scratch.mkdir(mode=0o700)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.scratch,
            capacity_probe=lambda _: 1_000_000, safety_floor_bytes=0, cleanup_grace_seconds=0)
        self.outbox = WorkerOutbox(self.root / 'outbox' / 'outbox.sqlite')
        raw = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        submission = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'])
        self.job = dict(jobId=str(uuid.uuid4()), generation=1, environment='staging', status='running',
            nodeId='cluster', manifestSha256=submission['manifestSha256'], pipelineDigest=submission['pipelineDigest'],
            submission=submission, claimToken='synthetic-private-claim')
        self.engine = FakeNativeEngine()
        self.reserve(output_budget=1024)
        self.worker = self.make_worker()

    def tearDown(self):
        self.gate_patch.stop()
        self.temp.cleanup()

    def reserve(self, output_budget):
        self.ledger.reserve(job_id=self.job['jobId'], generation=1, reservation_id='reservation-1',
            manifest_sha256=self.job['manifestSha256'], input_bytes=3, output_budget=output_budget,
            environment='staging', ttl_seconds=3600, worker_ready=True)
        directory = self.scratch / 'jobs' / 'reservation-1'
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        (directory / 'image-1.jpg').write_bytes(b'abc')
        self.ledger.record_allocated('reservation-1', 3)
        self.input_paths = {'image-1': 'jobs/reservation-1/image-1.jpg'}

    def make_worker(self, **kwargs):
        kwargs.setdefault('engine_probe', lambda deadline: 'physical-engine')
        return ReferenceEngineWorker(self.root / 'journal' / 'worker.sqlite', ledger=self.ledger,
            outbox=self.outbox, engine=self.engine, engine_id='physical-engine', node_id='cluster',
            authority_id=self.authority, coordinator_origin='http://127.0.0.1:8000', gate_generation=1,
            worker_id='worker', engine_version='synthetic-v1', **kwargs)

    def start(self):
        return self.worker.start(self.job, reservation_id='reservation-1', input_paths=self.input_paths)

    def test_real_gate_ledger_archive_checksum_outbox_and_metadata_ack(self):
        self.assertEqual(self.start(), 'processing')
        self.assertEqual(self.start(), 'processing')
        outbox_id = self.worker.poll(self.job['jobId'])
        completion = self.outbox.get(outbox_id)
        result = completion['payload']['resultManifest']
        validate_result_manifest(result, job_id=self.job['jobId'], generation=1,
            environment='staging', pipeline_digest=self.job['pipelineDigest'])
        archive = self.scratch / 'jobs/reservation-1/all.zip'
        self.assertEqual(result['objects'][0]['sha256'], hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(result['objects'][0]['byteSize'], len(self.engine.archive))
        self.assertEqual(self.engine.calls.count('initialize'), 1)
        self.assertEqual(self.engine.calls.count('commit'), 1)
        self.assertEqual(self.engine.calls.count('download'), 1)
        with self.assertRaises(WorkerHeld):
            self.worker.acknowledge_delivery(self.job['jobId'])
        payload = completion['payload']
        receipt = dict(type='durable-result-receipt', durable=True, receiptId='delivery-1', storageReceiptId='coordinator-1',
            outboxId=outbox_id, payloadSha256=completion['digest'],
            acceptedObjectIds=[o['id'] for o in result['objects']], **{k: payload[k] for k in (
                'jobId', 'generation', 'environment', 'inputManifestSha256', 'pipelineDigest', 'resultManifestSha256')})
        self.outbox.claim_next()
        self.outbox.acknowledge(outbox_id, receipt)
        self.worker.acknowledge_delivery(self.job['jobId'])
        self.worker.acknowledge_delivery(self.job['jobId'])
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.assertTrue(archive.exists())
        with self.ledger._transaction() as db:
            self.assertIsNone(db.execute('SELECT accepted FROM objects').fetchone()[0])

    def test_uncertain_initialize_or_upload_is_never_replayed(self):
        self.engine.fail = 'initialize'
        with self.assertRaises(OSError):
            self.start()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'initialize_pending')
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls, ['initialize'])

    def test_uncertain_commit_is_read_only_reconciled_without_rerun(self):
        self.engine.fail = 'commit'
        with self.assertRaises(OSError):
            self.start()
        self.worker = self.make_worker()
        self.assertEqual(self.start(), 'commit_pending')
        self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls.count('commit'), 1)
        self.assertEqual(self.engine.calls.count('initialize'), 1)

    def test_partial_download_retries_one_owned_part_then_recovers_without_engine_replay(self):
        self.start()
        self.engine.fail = 'download'
        with self.assertRaises(OSError):
            self.worker.poll(self.job['jobId'])
        self.worker = self.make_worker()
        for _ in range(3):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        files = list((self.scratch / 'jobs/reservation-1').glob('*.part'))
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].stat().st_size, 16)
        self.assertEqual(self.engine.calls.count('download'), 4)
        self.engine.fail = None
        self.worker = self.make_worker()
        self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        self.assertEqual(self.engine.calls.count('initialize'), 1)
        self.assertEqual(self.engine.calls.count('commit'), 1)
        self.assertFalse(files[0].exists())
        self.assertEqual((self.scratch / 'jobs/reservation-1/all.zip').stat().st_size, len(self.engine.archive))

    def test_source_offline_preserves_incomplete_owned_cache(self):
        self.start()
        self.engine.fail = 'download'
        with self.assertRaises(OSError):
            self.worker.poll(self.job['jobId'])
        part = self.scratch / 'jobs/reservation-1/.archive.part'
        original = part.read_bytes()
        self.engine.fail = 'open_download'
        self.worker = self.make_worker()
        with self.assertRaises(OSError):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(part.read_bytes(), original)

    def test_complete_declared_part_recovers_without_second_get(self):
        self.start()
        self.engine.declared_length = True
        original = self.worker._file

        def crash(reservation, relative, *args):
            if relative.endswith('.archive.part'):
                raise OSError('synthetic crash after complete download')
            return original(reservation, relative, *args)

        with patch.object(self.worker, '_file', side_effect=crash):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        self.engine.fail = 'open_download'
        self.worker = self.make_worker()
        self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_retry_requires_engine_still_confirmed_completed(self):
        self.start()
        self.engine.fail = 'download'
        with self.assertRaises(OSError):
            self.worker.poll(self.job['jobId'])
        part = self.scratch / 'jobs/reservation-1/.archive.part'
        original = part.read_bytes()
        self.engine.code = 20
        self.engine.fail = None
        self.assertIsNone(self.worker.poll(self.job['jobId']))
        self.assertEqual(part.read_bytes(), original)
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_foreign_replaced_or_hardlinked_part_is_never_discarded(self):
        self.start()
        self.engine.fail = 'download'
        with self.assertRaises(OSError):
            self.worker.poll(self.job['jobId'])
        part = self.scratch / 'jobs/reservation-1/.archive.part'
        original = part.with_name('original-cache')
        part.rename(original)
        part.write_bytes(b'foreign')
        self.engine.fail = None
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(part.read_bytes(), b'foreign')
        part.unlink()
        original.rename(part)
        os.link(part, part.with_name('foreign-hardlink'))
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_oversized_archive_retries_reuse_same_bounded_part(self):
        self.engine.archive = b'x' * 1025
        self.start()
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertLessEqual((self.scratch / 'jobs/reservation-1/.archive.part').stat().st_size, 1024)
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls.count('download'), 2)

    def test_complete_archive_recovers_after_outbox_failure_without_redownload(self):
        self.start()
        with patch.object(self.outbox, 'persist_terminal', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        self.worker = self.make_worker()
        self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        self.assertEqual(self.engine.calls.count('download'), 1)
        self.assertEqual(self.engine.calls.count('read'), 1)

    def test_archive_ready_checksum_rejects_tampered_recovery_artifact(self):
        self.start()
        with patch.object(self.outbox, 'persist_terminal', side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        (self.scratch / 'jobs/reservation-1/all.zip').write_bytes(b'tampered')
        with self.assertRaises(WorkerHeld):
            self.worker.poll(self.job['jobId'])
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_complete_part_recovers_publication_crash_without_redownload(self):
        self.start()
        with patch('processing.reference_engine_worker.os.link', side_effect=OSError('synthetic publication crash')):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        self.assertEqual(self.worker._row(self.job['jobId'])['phase'], 'archive_ready')
        self.assertTrue((self.scratch / 'jobs/reservation-1/.archive.part').exists())
        self.worker = self.make_worker()
        self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_two_link_publication_crash_recovers_only_exact_same_inode(self):
        self.start()
        original = Path.unlink

        def crash(path, *args, **kwargs):
            if path.name == '.archive.part':
                raise OSError('synthetic unlink crash')
            return original(path, *args, **kwargs)

        with patch.object(Path, 'unlink', crash):
            with self.assertRaises(OSError):
                self.worker.poll(self.job['jobId'])
        self.worker = self.make_worker()
        self.assertIsNotNone(self.worker.poll(self.job['jobId']))
        self.assertFalse((self.scratch / 'jobs/reservation-1/.archive.part').exists())
        self.assertEqual(self.engine.calls.count('download'), 1)

    def test_gate_hold_survives_journal_commit_failure(self):
        with self.worker._db() as db:
            db.execute("CREATE TRIGGER reject_start BEFORE INSERT ON executions BEGIN SELECT RAISE(ABORT,'synthetic crash'); END")
        with self.assertRaises(Exception):
            self.start()
        with NodeGate('physical-engine') as gate:
            self.assertIsNotNone(gate.snapshot()['reservation'])
        with self.assertRaises(GateError):
            self.start()
        self.assertEqual(self.engine.calls, [])

    def test_bad_input_or_external_engine_fails_before_mutation(self):
        (self.scratch / 'jobs/reservation-1/image-1.jpg').write_bytes(b'bad')
        with self.assertRaises(WorkerHeld):
            self.start()
        self.engine.origin = 'http://192.0.2.1:3001'
        with self.assertRaises(ValueError):
            self.make_worker()
        self.assertEqual(self.engine.calls, [])

    def test_local_engine_identity_mismatch_fails_closed_before_mutation(self):
        self.worker = self.make_worker(engine_probe=lambda deadline: 'different-engine')
        with self.assertRaisesRegex(WorkerHeld, 'physical_engine_identity_changed'):
            self.start()
        self.assertEqual(self.engine.calls, [])
        with NodeGate('physical-engine') as gate:
            self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_local_engine_identity_is_rechecked_before_each_mutation(self):
        observed = iter(('physical-engine', 'different-engine'))
        self.worker = self.make_worker(engine_probe=lambda deadline: next(observed))
        with self.assertRaisesRegex(WorkerHeld, 'physical_engine_identity_changed'):
            self.start()
        self.assertEqual(self.engine.calls, ['initialize'])
        self.assertNotIn('upload', self.engine.calls)
        self.assertNotIn('commit', self.engine.calls)

    def test_viewer_acceptance_is_validated_locally_and_never_deletes_or_bypasses_pins(self):
        self.start()
        self.worker.poll(self.job['jobId'])
        result = json.loads(self.worker._row(self.job['jobId'])['result'])
        receipt = dict(schemaVersion=1, receiptId='viewer-acceptance', storageReceiptId='viewer-storage',
            jobId=self.job['jobId'], generation=1, environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=[], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        self.assertFalse(self.worker.apply_viewer_acceptance(self.job['jobId'], [receipt])['fullyAccepted'])
        self.ledger.set_pin('reservation-1', 'odm-archive', 'reader', kind='read', state='active')
        receipt.update(receiptId='viewer-acceptance-full', storageReceiptId='viewer-storage-full', acceptedObjectIds=['odm-archive'])
        observed = self.worker.apply_viewer_acceptance(self.job['jobId'], [receipt])
        self.assertEqual(observed, {'acceptedObjectIds': ['odm-archive'], 'fullyAccepted': True})
        self.assertEqual(self.worker.apply_viewer_acceptance(self.job['jobId'], [receipt]), observed)
        self.assertTrue((self.scratch / 'jobs/reservation-1/all.zip').exists())
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        with self.assertRaises(WorkerHeld):
            self.worker.apply_viewer_acceptance(self.job['jobId'], {'fullyAccepted': True, 'receipts': [receipt]})

    def test_stale_viewer_receipts_do_not_mark_local_objects_accepted(self):
        self.start()
        self.worker.poll(self.job['jobId'])
        result = json.loads(self.worker._row(self.job['jobId'])['result'])
        receipt = dict(schemaVersion=1, receiptId='viewer-acceptance', storageReceiptId='viewer-storage',
            jobId=self.job['jobId'], generation=2, environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=result['resultManifestSha256'],
            acceptedObjectIds=['odm-archive'], acceptedAt='2026-10-27T22:00:00Z', validationVersion='ltds-reference-v1')
        with self.assertRaises(ValueError):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [receipt])
        with self.ledger._transaction() as db:
            self.assertIsNone(db.execute('SELECT accepted FROM objects').fetchone()[0])

    @unittest.skipIf(os.name == 'nt', 'POSIX permissions')
    def test_private_journal_mode_and_parent_are_enforced(self):
        self.worker.path.chmod(0o644)
        self.worker = self.make_worker()
        self.assertEqual(self.worker.path.stat().st_mode & 0o777, 0o600)
        self.worker.path.parent.chmod(0o755)
        with self.assertRaises(ValueError):
            self.make_worker()


if __name__ == '__main__':
    unittest.main()
