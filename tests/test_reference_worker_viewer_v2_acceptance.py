"""Real worker + ScratchLedger: relayed Viewer v2 (TreeDar) accepted-copy proof.

Synthetic data only. The worker derives the Viewer manifest from its own durable
result; relayed manifests, flags, or v1-bound TreeDar receipts never release data.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.reference_engine_worker import ReferenceEngineWorker, WorkerHeld
from processing.reference_protocol import validate_submission
from processing.scratch_cleanup import ScratchCleanupExecutor
from processing.scratch_policy import ScratchLedger, PolicyConflict
from processing.viewer_result_manifest import TREEDAR_REQUIRED_ROLES, derive_viewer_result_manifest
from processing.worker_outbox import WorkerOutbox
from tests.test_reference_engine_worker import FakeNativeEngine
from tests.viewer_receipt_signer import SyntheticViewerSigner

RESERVATION = 'reservation-1'


class ReferenceWorkerViewerV2AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        gate_patch.start(); self.addCleanup(gate_patch.stop)
        self.authority = str(uuid.uuid4())
        with NodeGate('physical-engine') as gate:
            gate.provision('cluster', 'http://127.0.0.1:8000', self.authority)
        self.scratch = self.root / 'scratch'; self.scratch.mkdir(mode=0o700)
        raw = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        submission = validate_submission(raw, environment='staging', capability_fingerprint=raw['capabilityFingerprint'])
        self.job = dict(jobId=str(uuid.uuid4()), generation=1, environment='staging', status='running',
            nodeId='cluster', manifestSha256=submission['manifestSha256'], pipelineDigest=submission['pipelineDigest'],
            submission=submission, claimToken='synthetic-private-claim')
        self.signer = SyntheticViewerSigner()
        self.open()

    def open(self):
        """Reconstruct ledger, outbox and actual worker from durable files (restart)."""
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.scratch,
            capacity_probe=lambda _: 1_000_000, safety_floor_bytes=0, cleanup_grace_seconds=0)
        self.outbox = WorkerOutbox(self.root / 'outbox' / 'outbox.sqlite')
        self.worker = ReferenceEngineWorker(self.root / 'journal' / 'worker.sqlite', ledger=self.ledger,
            outbox=self.outbox, engine=FakeNativeEngine(), engine_id='physical-engine', node_id='cluster',
            authority_id=self.authority, coordinator_origin='http://127.0.0.1:8000', gate_generation=1,
            worker_id='worker', engine_version='synthetic-v1', viewer_receipt_keys=self.signer.trusted_keys)

    def terminal(self, engines, roles):
        """Durable local terminal state as TreeDarDelivery.poll leaves it (synthetic files)."""
        self.ledger.reserve(job_id=self.job['jobId'], generation=1, reservation_id=RESERVATION,
            manifest_sha256=self.job['manifestSha256'], input_bytes=3, output_budget=65536,
            environment='staging', ttl_seconds=3600, worker_ready=True)
        folder = self.scratch / 'jobs' / RESERVATION; folder.mkdir(parents=True, mode=0o700)
        (folder / 'input.las').write_bytes(b'abc'); self.ledger.record_allocated(RESERVATION, 3)
        objects, local = [], {}
        # Producer order is deliberately reverse-sorted so manifest != sorted order.
        for index, role in enumerate(roles):
            object_id = 'z%02d-%s' % (len(roles) - index, role.replace('_', '-'))
            data = ('synthetic ' + role).encode()
            relative = 'jobs/' + RESERVATION + '/' + object_id + '.bin'
            (self.scratch / relative).write_bytes(data)
            objects.append(dict(id=object_id, role=role, byteSize=len(data), sha256=hashlib.sha256(data).hexdigest()))
            local[object_id] = relative
        result = dict(schemaVersion=1, jobId=self.job['jobId'], generation=1, environment='staging',
            pipelineDigest=self.job['pipelineDigest'], engineVersions=engines, objects=objects)
        result['resultManifestSha256'] = p.digest(result)
        self.ledger.set_engine_state(RESERVATION, 'engine_started')
        self.ledger.register_result(RESERVATION, job_id=self.job['jobId'], generation=1,
            manifest_sha256=self.job['manifestSha256'], pipeline_sha256=self.job['pipelineDigest'],
            result_manifest_sha256=result['resultManifestSha256'], objects=[dict(object_id=o['id'],
                bytes=o['byteSize'], sha256=o['sha256'], relative_path=local[o['id']]) for o in objects])
        self.ledger.set_engine_state(RESERVATION, 'terminal', terminal_attested=True)
        outbox_id = self.outbox.persist_terminal(job_id=self.job['jobId'], generation=1, environment='staging',
            input_manifest_sha256=self.job['manifestSha256'], pipeline_digest=self.job['pipelineDigest'],
            result_manifest_sha256=result['resultManifestSha256'], result_manifest=result,
            owned_prefix='jobs/' + RESERVATION, local_objects=local)
        with self.worker._db() as db:
            db.execute("INSERT INTO executions(job,identity,receipt,phase,result,outbox_id) VALUES(?,?,?,'delivered',?,?)",
                (self.job['jobId'], p.canonical_json(dict(job=self.job, reservation=RESERVATION, paths={})).decode(),
                 '{}', p.canonical_json(result).decode(), outbox_id))
        self.result, self.local = result, local
        self.viewer = derive_viewer_result_manifest(result, job_id=self.job['jobId'], generation=1,
            environment='staging', pipeline_digest=self.job['pipelineDigest'])
        return result

    def treedar(self):
        return self.terminal({'treedar': '0.0.1'}, sorted(TREEDAR_REQUIRED_ROLES))

    def acceptance(self, manifest, ids, suffix='1'):
        return dict(schemaVersion=1, receiptId='accept-' + suffix, storageReceiptId='storage-' + suffix,
            jobId=self.job['jobId'], generation=1, environment='staging', inputManifestSha256=self.job['manifestSha256'],
            pipelineDigest=self.job['pipelineDigest'], resultManifestSha256=manifest['resultManifestSha256'],
            acceptedObjectIds=list(ids), acceptedAt='2026-11-03T12:00:00Z', validationVersion=p.VALIDATION_VERSION)

    def relayed(self):
        """Exactly what ReferenceStore.acceptance_receipts relays after a v2 transfer."""
        acceptance = self.acceptance(self.viewer, [o['id'] for o in self.viewer['objects']])
        return [self.signer.envelope(acceptance, self.viewer)]

    def verified(self):
        with self.ledger._transaction() as db:
            return {row[0]: (row[1] is not None, row[2]) for row in db.execute(
                'SELECT id,accepted,accepted_copy_verified FROM objects WHERE reservation=?', (RESERVATION,))}

    def assertHeld(self):
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        self.assertFalse(any(verified for _, verified in self.verified().values()))
        for relative in self.local.values():
            self.assertTrue((self.scratch / relative).exists())

    def test_v2_treedar_relay_verifies_every_object_then_is_cleanup_eligible(self):
        self.treedar()
        self.assertEqual(self.viewer['schemaVersion'], 2)
        self.assertNotEqual(self.viewer['resultManifestSha256'], self.result['resultManifestSha256'])
        ids = [o['id'] for o in self.viewer['objects']]
        self.assertNotEqual(ids, sorted(ids))  # signed receipt ids are sorted; acceptance is manifest order
        relayed = self.relayed()
        self.assertEqual(relayed[0]['viewerReceipt']['acceptedObjectIds'], sorted(ids))
        observed = self.worker.apply_viewer_acceptance(self.job['jobId'], relayed)
        self.assertEqual(observed, {'acceptedObjectIds': sorted(ids), 'fullyAccepted': True})
        self.assertEqual(self.verified(), {object_id: (True, 1) for object_id in ids})
        self.assertEqual(sorted(c['id'] for c in self.ledger.cleanup_candidates(max_count=1000)), sorted(ids))
        with self.ledger._transaction() as db:
            payload, signed, flag = db.execute('SELECT payload,viewer_receipt,verified_copy FROM receipts WHERE id=?',
                ('accept-1',)).fetchone()
        # Ledger keeps the registered source identity; the stored signed proof keeps the v2 digest.
        self.assertEqual(json.loads(payload)['result_manifest_sha256'], self.result['resultManifestSha256'])
        self.assertEqual(json.loads(signed)['resultManifestSha256'], self.viewer['resultManifestSha256'])
        self.assertEqual(flag, 1)

    def test_v2_receipt_order_is_canonical_but_acceptance_keeps_viewer_closure_order(self):
        self.treedar()
        ids = [o['id'] for o in self.viewer['objects']]
        # Viewer signs sorted ids for any manifest order; the worker compares canonical sets.
        relayed = self.relayed()
        self.assertTrue(self.worker.apply_viewer_acceptance(self.job['jobId'], relayed)['fullyAccepted'])
        # A permuted processing acceptance is not the coordinator/Viewer v2 closure.
        self.open(); permuted = self.acceptance(self.viewer, list(reversed(ids)), suffix='2')
        with self.assertRaises(WorkerHeld):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [self.signer.envelope(permuted, self.viewer)])

    def test_v1_bound_signed_receipt_for_treedar_and_metadata_coverage_never_release(self):
        self.treedar()
        ids = [o['id'] for o in self.result['objects']]
        v1 = self.acceptance(self.result, ids)
        with self.assertRaises(WorkerHeld):
            self.worker.apply_viewer_acceptance(self.job['jobId'], [self.signer.envelope(v1, self.result)])
        self.assertHeld()
        # Unsigned source-bound coverage is metadata only.
        self.assertTrue(self.worker.apply_viewer_acceptance(self.job['jobId'], [v1])['fullyAccepted'])
        self.assertEqual(self.ledger.cleanup_candidates(), [])
        self.assertEqual(self.verified(), {object_id: (True, 0) for object_id in ids})
        with self.assertRaises(WorkerHeld):
            self.worker.apply_viewer_acceptance(self.job['jobId'], {'fullyAccepted': True, 'receipts': self.relayed()})
        self.assertEqual(self.ledger.cleanup_candidates(), [])

    def test_partial_extra_wrong_digest_or_identity_is_refused_and_held(self):
        self.treedar()
        ids = [o['id'] for o in self.viewer['objects']]
        def envelope(**changes):
            acceptance = self.acceptance(self.viewer, ids); acceptance.update(changes)
            return self.signer.envelope(acceptance, self.viewer)
        def signed_with(field, value, part='viewerReceipt'):
            item = self.relayed()[0]; item[part][field] = value; return item
        cases = [envelope(acceptedObjectIds=ids[:-1]), signed_with('acceptedObjectIds', ids + ['unknown-object'], 'acceptance'),
                 envelope(resultManifestSha256='0' * 64), envelope(generation=2), envelope(jobId='other-job'),
                 signed_with('resultManifestSha256', self.result['resultManifestSha256']),
                 signed_with('acceptedObjectIds', sorted(ids)[:-1])]
        # Partial signed coverage relayed beside a full unsigned claim is still refused.
        cases.append([self.acceptance(self.result, ids, suffix='9'), envelope(acceptedObjectIds=ids[:1])])
        for case in cases:
            with self.subTest(case=json.dumps(case)[:80]):
                with self.assertRaises(ValueError):
                    self.worker.apply_viewer_acceptance(self.job['jobId'], case if type(case) is list else [case])
                self.assertHeld()

    def test_crash_before_durable_verification_then_restart_replays_idempotently(self):
        self.treedar(); relayed = self.relayed()
        with patch.object(self.ledger, 'accept_receipt', side_effect=OSError('synthetic crash before ledger commit')):
            with self.assertRaises(OSError):
                self.worker.apply_viewer_acceptance(self.job['jobId'], relayed)
        self.open(); self.assertHeld()
        first = self.worker.apply_viewer_acceptance(self.job['jobId'], copy.deepcopy(relayed))
        self.open()  # crash after durable verification, before any cleanup
        candidates = self.ledger.cleanup_candidates(max_count=1000)
        self.assertEqual(len(candidates), len(self.local))
        self.assertEqual(self.worker.apply_viewer_acceptance(self.job['jobId'], copy.deepcopy(relayed)), first)
        self.assertEqual(self.ledger.cleanup_candidates(max_count=1000), candidates)

    def test_changed_replay_is_refused_without_changing_verified_state(self):
        self.treedar(); relayed = self.relayed()
        self.worker.apply_viewer_acceptance(self.job['jobId'], relayed)
        before = (self.verified(), self.ledger.cleanup_candidates(max_count=1000))
        resigned = copy.deepcopy(relayed); resigned[0] = self.signer.envelope(relayed[0]['acceptance'], self.viewer)
        changed = copy.deepcopy(relayed); changed[0]['acceptance']['storageReceiptId'] = 'storage-changed'
        changed[0] = self.signer.envelope(changed[0]['acceptance'], self.viewer)
        for case in (resigned, changed):
            with self.assertRaises(PolicyConflict):
                self.worker.apply_viewer_acceptance(self.job['jobId'], case)
            self.assertEqual((self.verified(), self.ledger.cleanup_candidates(max_count=1000)), before)

    @unittest.skipIf(os.name == 'nt', 'cleanup executor requires POSIX rename/fsync')
    def test_cleanup_after_full_verification_never_deletes_twice_across_restart(self):
        self.treedar(); relayed = self.relayed()
        self.worker.apply_viewer_acceptance(self.job['jobId'], relayed)
        first = self.ledger.cleanup_candidates(max_count=1)[0]
        ScratchCleanupExecutor(self.ledger).execute(first['reservation'], first['id'], generation=first['generation'],
            sha256=first['sha'], byte_size=first['bytes'], relative_path=first['path'])
        self.assertFalse((self.scratch / first['path']).exists())
        self.open()  # restart after a partial cleanup pass; the coordinator relays again
        self.assertTrue(self.worker.apply_viewer_acceptance(self.job['jobId'], copy.deepcopy(relayed))['fullyAccepted'])
        remaining = self.ledger.cleanup_candidates(max_count=1000)
        self.assertNotIn(first['id'], [c['id'] for c in remaining])
        with self.assertRaises(PolicyConflict):
            self.ledger.begin_cleanup(first['reservation'], first['id'], generation=1, sha256=first['sha'])
        for item in remaining:
            ScratchCleanupExecutor(self.ledger).execute(item['reservation'], item['id'], generation=item['generation'],
                sha256=item['sha'], byte_size=item['bytes'], relative_path=item['path'])
        self.assertEqual(self.ledger.cleanup_candidates(max_count=1000), [])
        self.assertTrue((self.scratch / 'jobs' / RESERVATION / 'input.las').exists())

    def test_odm_only_v1_signed_path_is_unchanged(self):
        self.terminal({'odm': 'synthetic-v1'}, ['odm_archive'])
        self.assertIs(self.viewer['schemaVersion'], 1)
        self.assertEqual(self.viewer, self.result)
        acceptance = self.acceptance(self.result, [self.result['objects'][0]['id']])
        observed = self.worker.apply_viewer_acceptance(self.job['jobId'], [self.signer.envelope(acceptance, self.result)])
        self.assertTrue(observed['fullyAccepted'])
        self.assertEqual(len(self.ledger.cleanup_candidates()), 1)

    def test_combined_odm_treedar_result_binds_v2(self):
        self.terminal({'odm': 'synthetic-v1', 'treedar': '0.0.1'}, sorted(TREEDAR_REQUIRED_ROLES) + ['odm_archive'])
        self.assertEqual(self.viewer['schemaVersion'], 2)
        self.assertTrue(self.worker.apply_viewer_acceptance(self.job['jobId'], self.relayed())['fullyAccepted'])
        self.assertEqual(len(self.ledger.cleanup_candidates(max_count=1000)), len(self.local))


if __name__ == '__main__':
    unittest.main()
