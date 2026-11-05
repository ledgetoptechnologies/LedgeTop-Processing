import json
import tempfile
import unittest
import sys
from types import SimpleNamespace

from processing.config import ProcessingNode
from processing.maintenance import Conflict
from processing.node_inventory import pin_reference_inventory
from processing.prototype import Queue
from processing.reference_store import ReferenceStore


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = None

    def tearDown(self):
        if self.queue: self.queue.close()
        self.directory.cleanup()

    def open(self, ids):
        if self.queue: self.queue.close()
        self.queue = None
        self.queue = Queue(self.directory.name, node_ids=ids, reference_enabled=True)
        return ReferenceStore(self.queue)

    def pin(self, store, ids, *, changed=None):
        nodes = tuple(ProcessingNode(n, 'http://' + n + '.invalid') for n in ids)
        workers = {n: dict(workerId='worker-' + n, instanceId='instance-' + n,
                          resultOrigin='https://' + n + '.invalid') for n in ids}
        if changed: workers[changed]['instanceId'] = 'replacement'
        config = SimpleNamespace(prototype_node_ids=ids, node_endpoints=nodes)
        pin_reference_inventory(store, config, SimpleNamespace(workers=workers, recovery_bindings=None))

    def test_add_reorder_remove_idle_nodes_preserves_history_and_authority(self):
        store = self.open(('small', 'large'))
        authority = self.queue.authority_id
        self.pin(store, ('small', 'large'))
        store = self.open(('small', 'middle', 'large'))
        self.pin(store, ('small', 'middle', 'large'))
        store = self.open(('large', 'small', 'middle'))
        self.pin(store, ('large', 'small', 'middle'))
        store = self.open(('small', 'large'))
        self.pin(store, ('small', 'large'))
        self.assertEqual(self.queue.authority_id, authority)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM processing_node_inventory').fetchone()[0], 4)
            self.assertIsNotNone(db.execute("SELECT id FROM nodes WHERE id='middle'").fetchone())
            saved = json.loads(db.execute('SELECT inventory_json FROM processing_node_inventory ORDER BY revision DESC LIMIT 1').fetchone()[0])
            self.assertEqual(saved['nodeOrder'], ['small', 'large'])

    def test_refused_removal_retains_lease_reopen_and_original_job(self):
        store = self.open(('small', 'large'))
        self.pin(store, ('small', 'large'))
        job, _ = self.queue.submit(dict(requestId='request', environment='production', profile='treedar-synthetic-v1'))
        self.queue.claim(dict(workerId='worker', nodeId='small'))
        self.queue.close(); self.queue = None
        with self.assertRaisesRegex(Conflict, 'occupied or held'):
            Queue(self.directory.name, node_ids=('large',), reference_enabled=True)
        self.queue = Queue(self.directory.name, node_ids=('small', 'large'), reference_enabled=True)
        saved = self.queue.get(job['id'])
        self.assertEqual(saved['nodeId'], 'small')
        self.assertEqual(saved['status'], 'held')

    def test_rebinding_occupied_node_refused_and_pin_unchanged(self):
        store = self.open(('small', 'large'))
        self.pin(store, ('small', 'large'))
        self.queue.submit(dict(requestId='request', environment='production', profile='treedar-synthetic-v1'))
        self.queue.claim(dict(workerId='worker', nodeId='small'))
        with self.assertRaisesRegex(Conflict, 'occupied or held'):
            self.pin(store, ('small', 'large'), changed='small')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM processing_node_inventory').fetchone()[0], 1)

    def test_maintenance_ownership_blocks_retirement(self):
        store = self.open(('small', 'large'))
        self.pin(store, ('small', 'large'))
        with self.queue._db() as db:
            db.execute("UPDATE nodes SET state='updating' WHERE id='small'")
        self.queue.close(); self.queue = None
        with self.assertRaisesRegex(Conflict, 'maintenance'):
            Queue(self.directory.name, node_ids=('large',), reference_enabled=True)

    def test_initial_legacy_pin_checks_exact_persisted_recovery_digest(self):
        store = self.open(('small', 'large'))
        with self.queue._db() as db:
            db.execute('CREATE TABLE reference_recovery_startup(singleton INTEGER PRIMARY KEY, binding_sha256 TEXT)')
            db.execute("INSERT INTO reference_recovery_startup VALUES(1, ?)", ('a' * 64,))
        with self.assertRaisesRegex(Conflict, 'unchanged recovery'):
            self.pin(store, ('small', 'large'))
        with self.queue._db() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_node_inventory'").fetchone())

    def test_inventory_history_counts_exactly_and_replay_survives_full_budget(self):
        from processing.node_inventory import metadata_bytes
        store = self.open(('small', 'large'))
        with self.queue._db() as db:
            before = store._metadata_used_bytes(db)
        self.pin(store, ('small', 'large'))
        with self.queue._db() as db:
            one = metadata_bytes(db)
            self.assertEqual(store._metadata_used_bytes(db), before + one)
        self.pin(store, ('large', 'small'))
        with self.queue._db() as db:
            two = metadata_bytes(db)
            used = store._metadata_used_bytes(db)
            self.assertEqual(two, 2 * one)
            saved = [tuple(row) for row in db.execute('SELECT * FROM processing_node_inventory ORDER BY revision')]
        store.max_metadata_bytes = used - 1
        # Existing exact replay is a read of the pin, not another allocation.
        self.pin(store, ('large', 'small'))
        with self.assertRaisesRegex(Conflict, 'metadata quota'):
            self.pin(store, ('small', 'large'))
        with self.queue._db() as db:
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM processing_node_inventory ORDER BY revision')], saved)
            self.assertEqual(metadata_bytes(db), two)

    def test_first_inventory_over_budget_is_atomic(self):
        store = self.open(('small',))
        store.max_metadata_bytes = 1
        with self.assertRaisesRegex(Conflict, 'metadata quota'):
            self.pin(store, ('small',))
        with self.queue._db() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_node_inventory'").fetchone())

    @unittest.skipUnless(sys.platform.startswith('linux'), 'real signed V2 receipt fixture')
    def test_rebinding_requires_actual_validated_worker_observation_not_row_presence(self):
        from tests.test_reference_acceptance_observed import AcceptanceObservationStoreTests
        fixture = AcceptanceObservationStoreTests('runTest')
        fixture.setUp()
        try:
            self.pin(fixture.store, ('cluster',))
            with self.assertRaisesRegex(Conflict, 'unobserved'):
                self.pin(fixture.store, ('cluster',), changed='cluster')
            self.assertEqual(fixture.post().status, 200)
            with fixture.queue._db() as db:
                saved = db.execute('SELECT ack_json FROM reference_worker_acceptance_observations').fetchone()[0]
                corrupt = json.loads(saved); corrupt['workerInstanceId'] = 'wrong-instance'
                db.execute('UPDATE reference_worker_acceptance_observations SET ack_json=?', (json.dumps(corrupt),))
            with self.assertRaises(Conflict):
                self.pin(fixture.store, ('cluster',), changed='cluster')
            with fixture.queue._db() as db:
                db.execute('UPDATE reference_worker_acceptance_observations SET ack_json=?', (saved,))
            self.pin(fixture.store, ('cluster',), changed='cluster')
            self.assertEqual(fixture.store.get(fixture.job['jobId'])['phase'], 'completed')
        finally:
            fixture.tearDown()


if __name__ == '__main__': unittest.main()

