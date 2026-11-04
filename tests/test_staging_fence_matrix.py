"""Staging beside production across Viewer and reference, in both slot modes.

Beau's decision: running production on another node allows staging on a node no
queued production can use; held production blocks all staging; one node never
overlaps; a single shared slot keeps the old wait. Viewer staging is observed
through ViewerService.tick and reference staging through ReferenceStore.claim,
the two assignment paths that share Queue._staging_may_pass.
"""

import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as protocol
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.viewer_service import ViewerService
from tests.reference_activation_fixture import activated_prepared_v3_job
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS, REQUEST_OPTIONS

NODES = ('cluster', 'main')
FIXTURE = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())
# Production state: (per-node slots on, per-node slots off); True lands staging on cluster.
MATRIX = {'running_elsewhere': (True, False), 'held': (False, False), 'queued_fits': (False, False),
          'queued_cannot_use': (True, False), 'same_node': (False, False)}


class Cell:
    """One real queue, store and service; cluster takes one photo, main any number."""

    def __init__(self, root, per_node):
        inventory = (dict(id='cluster', physicalHostId='cluster', origin='http://192.0.2.1:3000', token='',
                          maxPhotos=1, capabilities=('odm',)),
                     dict(id='main', physicalHostId='main', origin='http://192.0.2.2:3000', token='',
                          maxPhotos=None, capabilities=('odm',))) if per_node else ()
        config = SimpleNamespace(prototype_state_dir=str(root), viewer_max_input_bytes=10 ** 9,
            viewer_max_archive_bytes=10 ** 6, local_node_url='http://synthetic-cluster.invalid:3000',
            local_node_token='a', main_node_url='http://synthetic-main.invalid:3000', main_node_token='b',
            local_max_photos=1, local_max_input_bytes=10 ** 9, node_inventory=inventory)
        limits = dict(node_capabilities={'cluster': ('odm',), 'main': ('odm',)},
                      node_photo_limits={'cluster': 1, 'main': None}) if per_node else {}
        self.queue = Queue(root, node_ids=NODES, treedar_node_ids=() if per_node else ('main',), viewer_enabled=True,
                           reference_enabled=True, per_node_slots=per_node, **limits)
        self.reports = {node: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'outputBudgetBytes': 1024,
                               'availableScratchBytes': 10 ** 9} for node in NODES}
        fleet = FakeFleet()
        self.service = ViewerService(self.queue, config, engines={node: FakeEngine(node, fleet) for node in NODES},
            probe=lambda role, origin, token: self.reports['cluster' if role == 'local' else role])
        self.service._catalog = lambda: {'ready': True, 'commonOptions': [dict(item) for item in OPTIONS]}
        self.service._selected_options = lambda _: [dict(item) for item in OPTIONS]
        wiring = dict(node_inventory=inventory, node_readiness=lambda node: self.reports[node]) if per_node else {}
        self.reference = ReferenceStore(self.queue, jcs_runtime_enabled=True, local_max_photos=1, **wiring)

    def close(self):
        self.service.close()
        self.queue.close()

    def viewer(self, photos, environment):
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'Synthetic survey', REQUEST_OPTIONS, None, environment)
        for index in range(photos):
            self.service.upload(identifier, f'photo-{index:05}.jpg', io.BytesIO(b'owned'), 5)
        self.service.commit(identifier)
        return self.service.store.task(identifier)['job_id'], identifier

    def reference_job(self, photos, environment):
        raw = json.loads(json.dumps(FIXTURE['submission']))
        raw['submissionId'] = str(uuid.uuid4())
        manifest = raw['manifest']
        manifest['objects'] = [{**manifest['objects'][0], 'id': f'image-{i}', 'name': f'image-{i}.jpg',
                                'byteSize': 3} for i in range(photos)]
        manifest.update(objectCount=photos, totalBytes=photos * 3)
        manifest['inputCounts']['images'] = photos
        manifest['sha256'] = protocol.manifest_sha256(manifest)
        return activated_prepared_v3_job(self.reference, FIXTURE, protocol.validate_submission(
            raw, environment=environment, capability_fingerprint=raw['capabilityFingerprint']))['jobId'], None

    def job(self, kind, photos, environment):
        return (self.viewer if kind == 'viewer' else self.reference_job)(photos, environment)

    def claim_reference(self, job_id, node):
        return self.reference.claim(job_id, node_id=node, worker_id=node, worker_instance_id='instance',
                                    claim_request_id='claim-' + uuid.uuid4().hex[:12])

    def run_production(self, kind, photos, node):
        job_id, _ = self.job(kind, photos, 'production')
        if kind == 'viewer':
            self.service.tick()
        else:
            assert self.claim_reference(job_id, node) is not None
        job = self.queue.get(job_id)
        assert (job['status'], job['nodeId']) == ('running', node), job
        return job_id

    def staging_node(self, kind, job_id, identifier):
        """Offer staging one pass on every node; return where it landed."""
        if kind == 'viewer':
            self.service.tick()
            return self.service.store.task(identifier)['node_id']
        for node in NODES:
            offered = self.reference.next_for_node(node)
            if offered is not None and offered['jobId'] == job_id:
                return self.claim_reference(job_id, node)['nodeId']
            if self.claim_reference(job_id, node) is not None:
                raise AssertionError('claim passed a job discovery refused')
        return None


class StagingFenceMatrix(unittest.TestCase):
    def setUp(self):
        # Only exercises post-activation mechanics with a synthetic future grant.
        ready = patch.object(ReferenceStore, '_activation_bound_grant_ready', return_value=True)
        ready.start()
        self.addCleanup(ready.stop)

    def arrange(self, cell, state, kind):
        """Put production in this state; return its job."""
        if state in ('running_elsewhere', 'held'):
            job_id = cell.run_production(kind, 2, 'main')
            if state == 'held':
                cell.queue.operator(job_id, 'hold', {'note': 'inspect'})
            return job_id
        if state == 'same_node':
            job_id = cell.run_production(kind, 1, 'cluster')
            cell.reports['main']['ready'] = False  # Leaves only the occupied node to offer.
            return job_id
        if state == 'queued_fits':
            return cell.job(kind, 1, 'production')[0]
        cell.reports['main']['taskQueueCount'] = 1  # Too large for cluster, and main is busy.
        return cell.job(kind, 2, 'production')[0]

    def check(self, staging_kind, production_kind, state, per_node, allowed):
        with tempfile.TemporaryDirectory() as directory:
            cell = Cell(Path(directory) / 'state', per_node)
            try:
                production = self.arrange(cell, state, production_kind)
                before = cell.queue.get(production)
                staging, identifier = cell.job(staging_kind, 1, 'staging')
                self.assertEqual(cell.staging_node(staging_kind, staging, identifier), 'cluster' if allowed else None)
                self.assertEqual(cell.queue.get(staging)['status'], 'running' if allowed else 'queued')
                if before['status'] != 'queued':
                    after = cell.queue.get(production)
                    self.assertEqual((after['status'], after['nodeId']), (before['status'], before['nodeId']))
                busy = [job['nodeId'] for job in cell.queue.list() if job['status'] in ('running', 'held')]
                self.assertEqual(len(busy), len(set(busy)))
            finally:
                cell.close()

    def test_matrix(self):
        for state, answers in MATRIX.items():
            for per_node, allowed in zip((True, False), answers):
                for staging_kind in ('viewer', 'reference'):
                    for production_kind in ('viewer', 'reference'):
                        with self.subTest(staging=staging_kind, production=production_kind,
                                          state=state, per_node_slots=per_node):
                            self.check(staging_kind, production_kind, state, per_node, allowed)

    def test_production_on_one_node_leaves_the_other_idle_node_to_staging(self):
        for staging_kind in ('viewer', 'reference'):
            for production_kind in ('viewer', 'reference'):
                with self.subTest(staging=staging_kind, production=production_kind), \
                        tempfile.TemporaryDirectory() as directory:
                    cell = Cell(Path(directory) / 'state', True)
                    try:
                        cell.run_production(production_kind, 1, 'cluster')
                        staging, identifier = cell.job(staging_kind, 1, 'staging')
                        self.assertEqual(cell.staging_node(staging_kind, staging, identifier), 'main')
                    finally:
                        cell.close()


if __name__ == '__main__':
    unittest.main()
