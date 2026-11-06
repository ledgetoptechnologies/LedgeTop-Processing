"""Synthetic queue-only prototype; never exercise an installed service."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from processing import reference_protocol as p
from processing.direct_intake_reservation import DirectIntakeReservations
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.coordinator_freeze import CoordinatorFrozen
from processing.direct_worker_receiver import derived_digest
from processing import direct_worker_scratch
from processing.scratch_policy import ScratchLedger, PolicyConflict


class DirectIntakeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.queue = Queue(self.temp.name, node_ids=('cluster', 'main'), reference_enabled=True)
        self.store = ReferenceStore(self.queue, local_max_photos=2)
        self.bindings = {node: {'workerId': 'worker-'+node, 'workerInstanceId': 'instance-'+node}
                         for node in self.queue.node_ids}
        self.direct = DirectIntakeReservations(self.store, worker_bindings=self.bindings)
        self.fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def submit(self, environment='production', count=1, direct=True):
        raw = copy.deepcopy(self.fixture['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        original = raw['manifest']['objects'][0]
        objects = [dict(original, id=f'image-{i}', name=f'image-{i}.jpg') for i in range(count)]
        raw['manifest'].update(objects=objects, objectCount=count, totalBytes=count*original['byteSize'],
                               inputCounts={'images': count, 'lidarObjects': 0, 'points': 0})
        raw['manifest']['sha256'] = p.manifest_sha256(raw['manifest'])
        checked = p.validate_submission(raw, environment=environment,
            capability_fingerprint=raw['capabilityFingerprint'])
        job = self.store.submit(checked)[0]
        derived_sha=derived_digest(raw['manifest'],[])
        if direct:
            with self.queue._db() as db:
                row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (job['jobId'],)).fetchone()
                data = json.loads(row['input_json']); data['directIntakeVersion'] = 1
                data['directDerivedManifestSha256'] = derived_sha
                db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',
                           (json.dumps(data), job['jobId']))
        return dict(job_id=job['jobId'], attempt_id=raw['submissionId'], generation=job['generation'],
                    reservation_id=str(uuid.uuid4()), manifest_sha=raw['manifest']['sha256'],derived_manifest_sha=derived_sha)

    def test_small_first_large_uses_eligible_next_node(self):
        small = self.submit(count=1); first = self.direct.reserve(**small)
        large = self.submit(count=3); second = self.direct.reserve(**large)
        self.assertEqual((first['nodeId'], second['nodeId']), ('cluster', 'main'))
        self.assertEqual(first['state'], 'receiving')
        self.assertEqual(self.queue.get(small['job_id'])['status'], 'held')
        self.assertIsNone(self.direct.reserve(**self.submit(count=1)))
        self.assertEqual(self.direct.reserve(**small), dict(first, replayed=True))

    def test_same_node_competing_direct_and_regular_claim_are_atomic(self):
        request = self.submit()
        def direct(): return self.direct.reserve(**request)
        def regular():
            try:
                return self.store.claim(request['job_id'], node_id='cluster', worker_id='worker',
                    worker_instance_id='instance', claim_request_id='test-claim')
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            values = [future.result() for future in (pool.submit(direct), pool.submit(regular))]
        self.assertEqual(sum(value is not None for value in values), 1)

    def test_two_simultaneous_jobs_never_reserve_same_node(self):
        requests = [self.submit(), self.submit()]
        with ThreadPoolExecutor(max_workers=2) as pool:
            values = list(pool.map(lambda request: self.direct.reserve(**request), requests))
        # If the later request races first, FIFO refuses it; retry after the
        # first reservation and verify it goes to the next eligible node.
        if values[1] is None:
            values[1] = self.direct.reserve(**requests[1])
        self.assertEqual([value['nodeId'] for value in values], ['cluster', 'main'])

    def test_prior_production_blocks_staging_and_fifo_is_enforced(self):
        prod = self.submit(environment='production'); stage = self.submit(environment='staging')
        self.assertIsNone(self.direct.reserve(**stage))
        self.assertEqual(self.direct.reserve(**prod)['nodeId'], 'cluster')

    def test_replay_rejects_changed_attempt_and_v2_marker(self):
        request = self.submit(); self.direct.reserve(**request)
        with self.assertRaises(Conflict): self.direct.reserve(**dict(request, attempt_id=str(uuid.uuid4())))
        with self.assertRaises(Conflict): self.direct.reserve(**dict(request, derived_manifest_sha='b'*64))
        ordinary = self.submit(direct=False)
        with self.assertRaises(Conflict): self.direct.reserve(**ordinary)
        marked = self.submit()
        with self.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (marked['job_id'],)).fetchone()
            data = json.loads(row['input_json']); data['sourceIntakeVersion'] = 2
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(data), marked['job_id']))
        with self.assertRaises(Conflict): self.direct.reserve(**marked)

    def test_restart_keeps_slot_held_and_replays_binding(self):
        request = self.submit(); before = self.direct.reserve(**request)
        self.queue.close()
        self.queue = Queue(self.temp.name, node_ids=('cluster', 'main'), reference_enabled=True)
        self.store = ReferenceStore(self.queue, local_max_photos=2)
        self.direct = DirectIntakeReservations(self.store, worker_bindings=self.bindings)
        self.assertEqual(self.direct.reserve(**request), dict(before, replayed=True))
        self.assertEqual(self.queue.get(request['job_id'])['status'], 'held')

    def test_replay_holds_if_persisted_manifest_or_marker_changes(self):
        request = self.submit(); self.direct.reserve(**request)
        with self.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (request['job_id'],)).fetchone()
            data = json.loads(row['input_json']); data['directIntakeVersion'] = 2
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?',
                       (json.dumps(data), request['job_id']))
        with self.assertRaises(Conflict): self.direct.reserve(**request)
        self.assertEqual(self.queue.get(request['job_id'])['status'], 'held')

    def test_ordinary_worker_cannot_discover_direct_job_or_bypass_it(self):
        direct = self.submit(); later = self.submit(direct=False)
        self.assertIsNone(self.store.next_for_node('cluster'))
        self.assertIsNone(self.store.claim(direct['job_id'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='ordinary-claim'))
        self.assertIsNone(self.store.claim(later['job_id'], node_id='cluster', worker_id='worker',
            worker_instance_id='instance', claim_request_id='later-claim'))
        self.assertEqual(self.direct.reserve(**direct)['nodeId'], 'cluster')

    def test_generic_operator_resolve_cannot_release_direct_slot(self):
        request = self.submit(); self.direct.reserve(**request)
        with self.assertRaisesRegex(Conflict, 'custody reconciliation'):
            self.queue.operator(request['job_id'], 'resolve', {'note': 'test', 'workerStopped': True})
        self.assertEqual(self.queue.get(request['job_id'])['status'], 'held')

    def test_coordinator_freeze_refuses_new_direct_reservation(self):
        request = self.submit()
        gate = self.queue._coordinator_freeze
        gate.transition(dict(requestId=str(uuid.uuid4()), planId=str(uuid.uuid4()),
            action='intent', expectedRevision=gate.status()['revision'], startupId=self.queue.startup_id),
            owner_secret='synthetic-owner')
        with self.assertRaises(CoordinatorFrozen): self.direct.reserve(**request)
        self.assertEqual(self.queue.get(request['job_id'])['status'], 'queued')

    def test_reservation_metadata_is_in_existing_shared_quota(self):
        request = self.submit()
        with self.queue._db() as db: before = self.store._metadata_used_bytes(db)
        self.direct.reserve(**request)
        with self.queue._db() as db:
            after = self.store._metadata_used_bytes(db)
            self.assertGreater(after, before)
            self.store.max_metadata_bytes = after - 1
            with self.assertRaisesRegex(Conflict, 'metadata quota'): self.store._quota(db)

    def test_coordinator_derived_binding_cannot_change_at_worker_or_replay(self):
        request=self.submit();held=self.direct.reserve(**request)
        scratch_root=Path(self.temp.name)/'worker';scratch_root.mkdir(mode=0o700)
        ledger=ScratchLedger(scratch_root/'ledger.sqlite',scratch_root,capacity_probe=lambda _:1000000,
                             clock=lambda:100,safety_floor_bytes=100)
        with self.queue._db() as db:
            raw=db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?',(request['job_id'],)).fetchone()
            total=json.loads(raw['input_json'])['manifest']['totalBytes']
        fields=('reservationId','jobId','attemptId','generation','environment','authorityId','nodeId',
                'workerId','workerInstanceId','manifestSha256','derivedManifestSha256')
        binding={name:held[name] for name in fields};binding.update(inputBytes=total,outputBudget=100,ttlSeconds=10)
        worker={name:held[name] for name in ('authorityId','nodeId','workerId','workerInstanceId')}
        observed=direct_worker_scratch.prepare(ledger,binding,expected_worker=worker,worker_ready=True)
        self.assertFalse(observed['sourceCustodyAccepted'])
        with self.assertRaises(PolicyConflict):
            direct_worker_scratch.prepare(ledger,dict(binding,derivedManifestSha256='b'*64),
                                          expected_worker=worker,worker_ready=True)
        with self.assertRaises(Conflict):self.direct.reserve(**dict(request,derived_manifest_sha='b'*64))


if __name__ == '__main__': unittest.main()
