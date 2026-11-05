from contextlib import contextmanager
import tempfile
import threading
import unittest
import uuid

from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.prototype import Queue


class FreezeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.queue = Queue(self.temp.name)
        self.gate = self.queue._coordinator_freeze
        self.plan = str(uuid.uuid4())
        self.owner = 'synthetic-freeze-owner'

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def request(self, action, **overrides):
        value = dict(requestId=str(uuid.uuid4()), planId=self.plan, action=action,
                     expectedRevision=self.gate.status()['revision'], startupId=self.queue.startup_id)
        value.update(overrides)
        return value

    def change(self, action, **kwargs):
        return self.gate.transition(self.request(action), owner_secret=self.owner, **kwargs)

    def test_intent_linearizes_admission_but_existing_nested_operation_finishes(self):
        with self.queue.coordinator_operation('http'):
            self.change('intent')
            outcome = []
            def denied():
                try:
                    with self.queue.coordinator_operation('http'): outcome.append('admitted')
                except CoordinatorFrozen: outcome.append('retry')
            thread = threading.Thread(target=denied)
            thread.start(); thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome, ['retry'])
            with self.queue.coordinator_operation('receipt-closure'):
                self.queue.submit(dict(requestId='existing', profile='treedar-synthetic-v1', environment='staging'))
            self.change('drain')
            with self.assertRaisesRegex(Conflict, 'still draining'): self.change('seal')
        self.assertEqual(self.change('seal')['phase'], 'frozen')
        self.assertEqual(len(self.queue.list()), 1)

    def test_frozen_blocks_direct_background_db_writer_but_keeps_reads(self):
        for action in ('intent', 'drain', 'seal'): self.change(action)
        self.assertEqual(self.gate.status()['phase'], 'frozen')
        self.assertEqual(self.queue.list(), [])
        with self.assertRaises(CoordinatorFrozen):
            self.queue.submit(dict(requestId='new', profile='treedar-synthetic-v1', environment='staging'))
        with self.assertRaises(CoordinatorFrozen), self.queue._db() as db:
            db.execute("UPDATE nodes SET revision=revision+1")
        self.assertEqual(self.queue.get_node('cluster')['revision'], 0)

    def test_exact_transition_replay_stale_cas_owner_and_health(self):
        request = self.request('intent')
        first = self.gate.transition(request, owner_secret=self.owner)
        self.assertEqual(self.gate.transition(request, owner_secret=self.owner), first)
        with self.assertRaises(Conflict):
            self.gate.transition(request, owner_secret='different-owner')
        self.change('drain'); self.change('seal')
        request = self.request('resume')
        with self.assertRaisesRegex(Conflict, 'health'): self.gate.transition(request, owner_secret=self.owner)
        resumed = self.gate.transition(request, owner_secret=self.owner, healthy=True)
        self.assertEqual(resumed['phase'], 'open')
        with self.queue.coordinator_operation('http'): pass

    def test_in_progress_node_update_prevents_freeze(self):
        with self.queue._db() as db: db.execute("UPDATE nodes SET state='updating'")
        with self.assertRaisesRegex(Conflict, 'maintenance'): self.change('intent')
        self.assertEqual(self.gate.status()['phase'], 'open')

    def test_frozen_state_survives_queue_ownership_restart(self):
        for action in ('intent', 'drain', 'seal'): self.change(action)
        authority = self.queue.authority_id
        old_epoch = self.queue.startup_id
        self.queue.close()
        self.queue = Queue(self.temp.name)
        self.gate = self.queue._coordinator_freeze
        self.assertEqual(self.queue.authority_id, authority)
        self.assertNotEqual(self.queue.startup_id, old_epoch)
        self.assertEqual(self.gate.status()['phase'], 'frozen')
        with self.assertRaises(CoordinatorFrozen), self.queue.coordinator_operation('http'): pass
        self.assertEqual(self.change('resume', healthy=True)['phase'], 'open')


if __name__ == '__main__': unittest.main()
