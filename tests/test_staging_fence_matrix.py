"""Mixed-profile scheduling through the real Queue, ReferenceStore and ViewerService.

Beau's decisions: production goes first on nodes it can use; running production
keeps only its own node; held production stops all staging; queued production
that is proven unable to use a node leaves it to staging. Proof is configured
or hash-verified, and doubt blocks. Viewer jobs move through ViewerService.tick
and reference jobs through ReferenceStore.claim, the two real assignment paths.
"""

from concurrent.futures import ThreadPoolExecutor
import copy
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import uuid

from processing import reference_protocol as protocol
from processing.config import ProcessingNode
from processing.prototype import Queue, VIEWER_PROFILE
from processing.reference_store import ReferenceStore
from processing.viewer_service import ViewerService
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS, REQUEST_OPTIONS

FIXTURE = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
TWO = {'cluster': 1, 'main': None}
THREE = {'cluster': 2, 'middle': 4, 'main': None}
# State of production -> where staging may land with cluster and main both offered.
MATRIX = {'running_elsewhere': 'cluster', 'held': None, 'queued_fits': None,
          'queued_cannot_use': 'cluster', 'same_node': None}


class Cell:
    """One real fleet; caps maps ordered node IDs to configured photo ceilings."""

    def __init__(self, root, caps=TWO):
        self.nodes = tuple(caps)
        self.queue = Queue(root, node_ids=self.nodes, treedar_node_ids=('main',),
                           viewer_enabled=True, reference_enabled=True)
        config = SimpleNamespace(prototype_state_dir=str(root), viewer_max_input_bytes=10 ** 9,
            viewer_max_archive_bytes=10 ** 6, local_max_photos=caps['cluster'], local_max_input_bytes=10 ** 9,
            processing_nodes=tuple(ProcessingNode(node, 'http://synthetic-%s.invalid:3000' % node) for node in self.nodes))
        self.reports = {node: {'ready': True, 'taskQueueCount': 0, 'maxImages': caps[node]} for node in self.nodes}
        self.fleet = FakeFleet()
        self.engines = {node: FakeEngine(node, self.fleet) for node in self.nodes}
        self.service = ViewerService(self.queue, config, engines=self.engines,
                                     probe=lambda role, origin, token: self.reports[role])
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS],
            'nodes': [{'role': node, 'available': True} for node in self.nodes]}
        self.reference = ReferenceStore(self.queue, local_max_photos=caps['cluster'], local_max_input_bytes=10 ** 9,
            node_limits={node: {'maxImages': cap} for node, cap in caps.items()})

    def close(self):
        self.service.close()
        self.queue.close()

    def refresh(self):
        self.reference.update_fleet_snapshot({node: {**report, 'maxInputBytes': None}
                                              for node, report in self.reports.items()})

    def viewer_job(self, photos, environment):
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'Synthetic survey', REQUEST_OPTIONS, None, environment)
        for index in range(photos):
            self.service.upload(identifier, f'photo-{index:05}.jpg', io.BytesIO(b'owned'), 5)
        self.service.commit(identifier)
        return self.service.store.task(identifier)['job_id']

    def reference_job(self, photos, environment, treedar=False):
        raw = copy.deepcopy(FIXTURE['submission'])
        raw['submissionId'] = str(uuid.uuid4())
        manifest = raw['manifest']
        original = manifest['objects'][0]
        manifest['objects'] = [dict(original, id=f'image-{i + 1}', name=f'image-{i + 1:05}.jpg') for i in range(photos)]
        manifest.update(objectCount=photos, totalBytes=photos * original['byteSize'],
                        inputCounts={'images': photos, 'lidarObjects': 0, 'points': 0})
        if treedar:
            manifest['objects'][0].update(name='genuine.las', kind='lidar', mediaType='application/octet-stream')
            manifest['inputCounts'].update(images=photos - 1, lidarObjects=1, points=1)
            raw['pipeline']['odm'].update(requested=False)
            raw['pipeline']['treedar'].update(requested=True, inputObjectIds=['image-1'],
                                              provenanceReceiptId='trusted-ingestion-receipt')
        manifest['sha256'] = protocol.manifest_sha256(manifest)
        return self.reference.submit(protocol.validate_submission(raw, environment=environment,
            capability_fingerprint=raw['capabilityFingerprint'], available_engines=('odm', 'treedar')))[0]['jobId']

    def job(self, kind, photos, environment):
        return (self.viewer_job if kind == 'viewer' else self.reference_job)(photos, environment)

    def claim(self, job_id, node):
        self.refresh()
        return self.reference.claim(job_id, node_id=node, worker_id=node + '-worker', worker_instance_id='instance',
                                    claim_request_id='claim-' + uuid.uuid4().hex[:12])

    def offer(self, kind, job_id):
        """One scheduling pass for this job; return the node it lands on."""
        if kind == 'viewer':
            self.service.tick()
        else:
            self.refresh()
            for node in self.nodes:
                offered = self.reference.next_for_node(node)
                if offered is not None and offered['jobId'] == job_id:
                    self.claim(job_id, node)
                    break
                if self.claim(job_id, node) is not None:
                    raise AssertionError('claim passed a job discovery refused')
        job = self.queue.get(job_id)
        return job['nodeId'] if job['status'] == 'running' else None

    def run(self, kind, photos, node, environment='production'):
        job_id = self.job(kind, photos, environment)
        landed = self.offer(kind, job_id)
        assert landed == node, (landed, node)
        return job_id

    def finish(self, job_id):
        task = next((t for t in self.service.store.list_tasks() if t['job_id'] == job_id), None)
        if task is None:
            with self.queue._db() as db:
                token = db.execute('SELECT token FROM reference_claims WHERE job_id=?', (job_id,)).fetchone()[0]
            self.queue.complete(job_id, {'claimToken': token, 'status': 'succeeded', 'result': {}})
        else:
            self.engines[task['node_id']].finish(task['engine_uuid'])
            self.service.tick()
        assert self.queue.get(job_id)['status'] == 'succeeded'

    def busy(self):
        return [job['nodeId'] for job in self.queue.list() if job['status'] in ('running', 'held')]


class FenceCase(unittest.TestCase):
    def cell(self, caps=TWO):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        cell = Cell(Path(directory.name) / 'state', caps)
        self.addCleanup(cell.close)
        return cell


class StagingFenceMatrix(FenceCase):
    def arrange(self, cell, state, kind):
        if state in ('running_elsewhere', 'held'):
            job_id = cell.run(kind, 2, 'main')
            if state == 'held':
                cell.queue.operator(job_id, 'hold', {'note': 'inspect'})
            return job_id
        if state == 'same_node':
            job_id = cell.run(kind, 1, 'cluster')
            cell.reports['main']['ready'] = False  # Leaves only the occupied node to offer.
            return job_id
        if state == 'queued_fits':
            return cell.job(kind, 1, 'production')
        cell.reports['main']['taskQueueCount'] = 1  # Too large for cluster, and main is busy.
        return cell.job(kind, 2, 'production')

    def test_matrix(self):
        for state, expected in MATRIX.items():
            for staging_kind in ('viewer', 'reference'):
                for production_kind in ('viewer', 'reference'):
                    with self.subTest(staging=staging_kind, production=production_kind, state=state):
                        cell = self.cell()
                        production = self.arrange(cell, state, production_kind)
                        before = cell.queue.get(production)
                        staging = cell.job(staging_kind, 1, 'staging')
                        self.assertEqual(cell.offer(staging_kind, staging), expected)
                        self.assertEqual(cell.queue.get(staging)['status'], 'running' if expected else 'queued')
                        if before['status'] != 'queued':
                            after = cell.queue.get(production)
                            self.assertEqual((after['status'], after['nodeId']), (before['status'], before['nodeId']))
                        self.assertEqual(len(cell.busy()), len(set(cell.busy())))

    def test_production_on_small_node_leaves_large_node_to_staging(self):
        for staging_kind in ('viewer', 'reference'):
            for production_kind in ('viewer', 'reference'):
                with self.subTest(staging=staging_kind, production=production_kind):
                    cell = self.cell()
                    cell.run(production_kind, 1, 'cluster')
                    self.assertEqual(cell.offer(staging_kind, cell.job(staging_kind, 1, 'staging')), 'main')

    def test_later_small_staging_passes_blocked_staging_only_on_open_node(self):
        cell = self.cell()
        cell.reports['main']['taskQueueCount'] = 1
        production = cell.reference_job(2, 'production')
        large = cell.viewer_job(2, 'staging')
        small = cell.viewer_job(1, 'staging')
        self.assertIsNone(cell.offer('viewer', large))
        self.assertEqual(cell.queue.get(small)['nodeId'], 'cluster')
        self.assertEqual(cell.queue.get(production)['status'], 'queued')


class MultiNodeScheduling(FenceCase):
    def test_beaus_two_node_scenario(self):
        cell = self.cell({'cluster': 100, 'main': None})
        small = cell.run('viewer', 100, 'cluster')
        large = cell.run('reference', 500, 'main')
        waiting = cell.reference_job(5000, 'production')
        self.assertIsNone(cell.offer('reference', waiting))
        cell.finish(small)
        later = cell.run('viewer', 3, 'cluster')
        self.assertEqual(cell.queue.get(waiting)['status'], 'queued')
        cell.finish(later)
        # Production waits for main and runs nothing on cluster; staging may use it.
        staging = cell.run('reference', 5, 'cluster', 'staging')
        cell.finish(large)
        self.assertEqual(cell.offer('reference', waiting), 'main')
        self.assertEqual(cell.queue.get(staging)['nodeId'], 'cluster')
        self.assertEqual(sorted(cell.busy()), ['cluster', 'main'])

    def test_large_waiter_never_loses_large_node_to_staging(self):
        for production_kind in ('viewer', 'reference'):
            with self.subTest(production=production_kind):
                cell = self.cell()
                holder = cell.run('reference', 2, 'main')
                waiting = cell.job(production_kind, 2, 'production')
                filler = cell.run('viewer', 1, 'cluster', 'staging')
                queued = cell.viewer_job(1, 'staging')
                cell.finish(holder)
                self.assertEqual(cell.offer(production_kind, waiting), 'main')
                self.assertEqual(cell.queue.get(queued)['status'], 'queued')
                self.assertEqual(cell.queue.get(filler)['nodeId'], 'cluster')

    def test_held_production_stops_every_node_until_resolved(self):
        cell = self.cell(THREE)
        production = cell.run('viewer', 1, 'cluster')
        cell.queue.operator(production, 'hold', {'note': 'inspect'})
        viewer, reference = cell.viewer_job(1, 'staging'), cell.reference_job(1, 'staging')
        self.assertIsNone(cell.offer('viewer', viewer))
        self.assertIsNone(cell.offer('reference', reference))
        self.assertEqual({node: cell.queue.staging_open(node) for node in cell.nodes},
                         {node: False for node in cell.nodes})
        cell.queue.operator(production, 'resolve', {'note': 'stopped', 'workerStopped': True})
        self.assertEqual(cell.offer('viewer', viewer), 'cluster')
        self.assertEqual(cell.offer('reference', reference), 'middle')

    def test_three_nodes_mixed_profiles_stay_small_first(self):
        cell = self.cell(THREE)
        first = cell.run('reference', 2, 'cluster')
        second = cell.run('viewer', 4, 'middle')
        third = cell.run('reference', 9, 'main')
        waiting = cell.viewer_job(5, 'production')  # Fits only main.
        self.assertIsNone(cell.offer('viewer', waiting))
        cell.finish(first)
        staging = cell.run('viewer', 1, 'cluster', 'staging')
        cell.finish(second)
        moved = cell.run('reference', 3, 'middle', 'staging')
        self.assertEqual(cell.queue.get(waiting)['status'], 'queued')
        cell.finish(third)
        self.assertEqual(cell.offer('viewer', waiting), 'main')
        self.assertEqual(sorted(cell.busy()), ['cluster', 'main', 'middle'])
        cell.finish(staging)
        cell.finish(moved)

    def test_closed_earlier_node_does_not_strand_staging_on_open_node(self):
        for kind in ('reference', 'viewer'):
            with self.subTest(staging=kind):
                cell = self.cell({'cluster': 5, 'middle': 1, 'main': 5})
                production = cell.reference_job(3, 'production')  # Fits cluster and main, never middle.
                self.assertEqual(cell.offer(kind, cell.job(kind, 1, 'staging')), 'middle')
                self.assertEqual(cell.offer('reference', production), 'cluster')

    def test_concurrent_claims_keep_one_job_per_host(self):
        cell = self.cell(THREE)
        cell.refresh()
        production = [cell.reference_job(1, 'production') for _ in range(2)]
        staging = [cell.viewer_job(1, 'staging') for _ in range(2)] + [cell.reference_job(1, 'staging') for _ in range(2)]

        def attempt(item):
            job_id, node = item
            if job_id in staging[:2]:
                return cell.queue.claim({'workerId': 'viewer-dispatch', 'nodeId': node,
                                         'profiles': [VIEWER_PROFILE], 'expectedJobId': job_id})
            return cell.reference.claim(job_id, node_id=node, worker_id='worker', worker_instance_id='instance',
                                        claim_request_id='claim-' + uuid.uuid4().hex[:12])

        pairs = [(job_id, node) for job_id in production + staging for node in cell.nodes] * 3
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(attempt, pairs))
        self.assertEqual(len(cell.busy()), len(set(cell.busy())))
        self.assertEqual(sorted(cell.busy()), sorted(cell.nodes))
        for job_id in production:
            self.assertEqual(cell.queue.get(job_id)['status'], 'running')


class EvidenceFailsClosed(FenceCase):
    def waiting(self, kind='reference'):
        """A production job over cluster's cap with main busy: the proven case."""
        cell = self.cell()
        cell.reports['main']['taskQueueCount'] = 1
        return cell, cell.job(kind, 2, 'production')

    def assert_blocked(self, cell):
        self.assertFalse(cell.queue.staging_open('cluster'))
        staging = cell.reference_job(1, 'staging')
        self.assertIsNone(cell.offer('reference', staging))
        self.assertEqual(cell.queue.get(staging)['status'], 'queued')

    def test_proof_is_real_before_each_tamper(self):
        for kind in ('reference', 'viewer'):
            cell, _ = self.waiting(kind)
            self.assertTrue(cell.queue.staging_open('cluster'))
            self.assertFalse(cell.queue.staging_open('main'))

    def tamper_reference(self, change):
        cell, job_id = self.waiting()
        with cell.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (job_id,)).fetchone()
            value = json.loads(row['input_json'])
            change(value)
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(value), job_id))
        return cell

    def test_tampered_reference_manifest_proves_nothing(self):
        def inflate(value):
            value['manifest']['inputCounts']['images'] = 500
        def hide(value):
            value['manifest']['objects'].pop()
        for change in (inflate, hide):
            with self.subTest(change=change.__name__):
                self.assert_blocked(self.tamper_reference(change))

    def test_tampered_pipeline_proves_nothing(self):
        cell = self.cell()
        cell.reports['main']['taskQueueCount'] = 1
        job_id = cell.reference_job(1, 'production')
        with cell.queue._db() as db:
            row = db.execute('SELECT input_json FROM reference_jobs WHERE job_id=?', (job_id,)).fetchone()
            value = json.loads(row['input_json'])
            value['pipeline']['treedar']['requested'] = True
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', (json.dumps(value), job_id))
        self.assertFalse(cell.queue.staging_open('cluster'))

    def test_missing_reference_metadata_proves_nothing(self):
        cell, job_id = self.waiting()
        with cell.queue._db() as db:
            db.execute('UPDATE reference_jobs SET input_json=? WHERE job_id=?', ('not json', job_id))
        self.assertFalse(cell.queue.staging_open('cluster'))

    def test_environment_mismatch_proves_nothing(self):
        cell, job_id = self.waiting()
        with cell.queue._db() as db:
            db.execute("UPDATE reference_jobs SET environment='staging' WHERE job_id=?", (job_id,))
        self.assert_blocked(cell)

    def test_doubtful_viewer_metadata_proves_nothing(self):
        def no_files(db, uuid_):
            db.execute('DELETE FROM viewer_files WHERE uuid=?', (uuid_,))
        def odd_flag(db, uuid_):
            db.execute("UPDATE viewer_files SET is_photo=2 WHERE name_key=(SELECT MIN(name_key) FROM viewer_files WHERE uuid=?)", (uuid_,))
            db.execute("UPDATE viewer_files SET is_photo=0 WHERE name_key=(SELECT MAX(name_key) FROM viewer_files WHERE uuid=?)", (uuid_,))
        def bad_options(db, uuid_):
            db.execute("UPDATE viewer_tasks SET options='{' WHERE uuid=?", (uuid_,))
        def no_task(db, uuid_):
            db.execute('UPDATE viewer_tasks SET job_id=NULL WHERE uuid=?', (uuid_,))
        def wrong_environment(db, uuid_):
            db.execute("UPDATE viewer_tasks SET environment='staging' WHERE uuid=?", (uuid_,))
        for change in (no_files, odd_flag, bad_options, no_task, wrong_environment):
            with self.subTest(change=change.__name__):
                cell, job_id = self.waiting('viewer')
                with cell.queue._db() as db:
                    uuid_ = db.execute('SELECT uuid FROM viewer_tasks WHERE job_id=?', (job_id,)).fetchone()[0]
                    change(db, uuid_)
                self.assertFalse(cell.queue.staging_open('cluster'))

    def test_reported_cap_alone_is_not_proof(self):
        for kind in ('viewer', 'reference'):
            with self.subTest(production=kind):
                cell = self.cell()
                cell.reports['main']['maxImages'] = 1  # Reported by the engine, not configured.
                production = cell.job(kind, 2, 'production')
                self.assertIsNone(cell.offer(kind, production))
                self.assertTrue(cell.queue.staging_open('cluster'))
                self.assertFalse(cell.queue.staging_open('main'))
                first = cell.run('reference', 1, 'cluster', 'staging')
                second = cell.reference_job(1, 'staging')
                self.assertIsNone(cell.offer('reference', second))
                self.assertEqual(cell.queue.get(first)['status'], 'running')

    def test_node_under_probe_is_unknown_not_busy(self):
        cell = self.cell(THREE)
        cell.reports['main']['taskQueueCount'] = 1
        cell.reports['middle']['ready'] = False
        production = cell.reference_job(3, 'production')  # Over cluster's cap; fits middle by configuration.
        self.assertIsNone(cell.offer('reference', production))
        self.assertTrue(cell.queue.staging_open('cluster'))
        self.assertFalse(cell.queue.staging_open('middle'))
        self.assertFalse(cell.queue.staging_open('main'))

    def test_configured_capability_is_proof(self):
        cell = self.cell({'cluster': 10, 'main': None})
        cell.reports['main']['taskQueueCount'] = 1
        production = cell.reference_job(2, 'production', treedar=True)
        self.assertEqual(cell.offer('reference', cell.reference_job(1, 'staging')), 'cluster')
        self.assertEqual(cell.queue.get(production)['status'], 'queued')
        self.assertFalse(cell.queue.staging_open('main'))

    def test_only_verified_bytes_over_current_scratch_are_proof(self):
        cell = self.cell({'cluster': 10, 'main': None})
        job_id = cell.reference_job(2, 'production')
        total = cell.reference.get(job_id, private=True)['submission']['manifest']['totalBytes']
        for evidence, proven in (((total, 1), True), ((total + 1, 0), False), (None, False), (('x', 1), False), ('bad', False)):
            with self.subTest(evidence=evidence):
                cell.reference.node_scratch_evidence = lambda node: evidence
                self.assertEqual(cell.queue.staging_open('cluster'), proven)

    def test_viewer_bytes_are_not_scratch_proof(self):
        cell = self.cell({'cluster': 10, 'main': None})
        cell.viewer_job(2, 'production')
        cell.reference.node_scratch_evidence = lambda node: (0, 0)
        self.assertFalse(cell.queue.staging_open('cluster'))

    def test_queue_without_scheduler_proves_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = Queue(directory, node_ids=('cluster', 'main'), treedar_node_ids=('main',))
            try:
                queue.submit({'requestId': 'production', 'environment': 'production', 'profile': 'treedar-synthetic-v1'})
                self.assertFalse(queue.staging_open('cluster'))
            finally:
                queue.close()


if __name__ == '__main__':
    unittest.main()
