from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import tempfile
import unittest

from processing.prototype import Conflict, PROFILE, Queue


IMAGE = 'sha256:' + 'a' * 64


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
        self.queue = Queue(self.root.name, node_ids=('cluster', 'main'), clock=lambda: self.now)

    def tearDown(self):
        self.queue.close()
        self.root.cleanup()

    def schedule(self, node='cluster', after=None, seconds=0, wait=0):
        current = self.queue.get_node(node)
        return self.queue.schedule_node(node, {'expectedRevision': current['revision'],
            'drainAt': (self.now + timedelta(seconds=seconds)).isoformat(),
            'notBefore': (self.now + timedelta(seconds=seconds + wait)).isoformat(),
            'afterJobId': after, 'targetImage': IMAGE})

    def submit(self, name='one'):
        return self.queue.submit({'requestId': name, 'profile': PROFILE, 'environment': 'staging'})[0]

    def begin(self, node='cluster'):
        row = self.queue.get_node(node)
        return self.queue.begin_node_update(node, {'expectedRevision': row['revision'], 'planId': row['planId']})

    def finish(self, claim, node='cluster', status='succeeded', result=None):
        return self.queue.finish_node_update(node, {'planId': claim['node']['planId'],
            'updateToken': claim['updateToken'], 'status': status,
            'result': {'healthy': True} if result is None else result})

    def test_cutoff_admission_is_serialized_with_claim(self):
        scheduled = self.schedule(seconds=10)
        self.assertEqual(scheduled['state'], 'scheduled')
        self.submit()
        self.now += timedelta(seconds=10)
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda i: self.queue.claim({'workerId': 'w' + str(i)}), range(8)))
        self.assertEqual(claims, [None] * 8)
        self.assertEqual(self.queue.get_node('cluster')['state'], 'draining')
        self.assertEqual(self.queue.get_node('cluster')['revision'], scheduled['revision'] + 1)

    def test_existing_job_finishes_but_node_takes_no_new_work(self):
        self.submit()
        job = self.queue.claim({'workerId': 'w'})
        self.assertEqual(job['nodeId'], 'cluster')
        self.schedule(after=job['id'])
        with self.assertRaises(Conflict):
            self.begin()
        self.queue.complete(job['id'], {'claimToken': job['claimToken'], 'status': 'succeeded', 'result': {}})
        self.submit('two')
        self.assertIsNone(self.queue.claim({'workerId': 'w'}))
        begun = self.begin()
        self.assertEqual(begun['node']['state'], 'updating')
        self.assertEqual(self.finish(begun)['state'], 'active')
        self.assertIsNotNone(self.queue.claim({'workerId': 'w'}))

    def test_after_job_requires_exact_running_node_and_drains_immediately(self):
        self.submit('cluster-first')
        cluster_job = self.queue.claim({'workerId': 'cluster-worker', 'nodeId': 'cluster'})
        self.assertIsNotNone(cluster_job)
        self.submit('main-target')
        job = self.queue.claim({'workerId': 'w', 'nodeId': 'main'})
        self.assertIsNotNone(job)
        with self.assertRaises(Conflict):
            self.schedule(after=job['id'])
        self.assertEqual(self.schedule('main', after=job['id'], seconds=60)['state'], 'draining')
        with self.assertRaises(Conflict):
            self.schedule(after='does-not-exist')

    def test_node_maintenance_independent_and_slots_are_per_node(self):
        self.schedule()
        self.assertEqual(self.queue.get_node('main')['state'], 'active')
        queued = self.submit()
        self.assertIsNone(queued['nodeId'])
        self.assertIsNone(self.queue.claim({'workerId': 'w'}))
        self.assertEqual(self.queue.claim({'workerId': 'main', 'nodeId': 'main'})['nodeId'], 'main')
        self.submit('two')
        self.assertIsNone(self.queue.claim({'workerId': 'second', 'nodeId': 'main'}))

    def test_not_before_and_revision_compare_and_swap(self):
        row = self.schedule(wait=20)
        with self.assertRaises(Conflict):
            self.begin()
        with self.assertRaises(Conflict):
            self.queue.cancel_node('cluster', {'expectedRevision': row['revision'] - 1, 'planId': row['planId']})
        self.now += timedelta(seconds=20)
        begun = self.begin()
        with self.assertRaises(Conflict):
            self.queue.cancel_node('cluster', {'expectedRevision': begun['node']['revision'], 'planId': row['planId']})
        self.assertEqual(self.finish(begun)['state'], 'active')
        self.assertEqual(self.finish(begun)['state'], 'active')
        with self.assertRaises(Conflict):
            self.finish(begun, result={'healthy': True, 'different': True})

    def test_restart_holds_update_and_invalidates_token(self):
        self.schedule()
        begun = self.begin()
        self.queue.close()
        self.queue = Queue(self.root.name, node_ids=('cluster', 'main'), clock=lambda: self.now)
        row = self.queue.get_node('cluster')
        self.assertEqual(row['state'], 'held')
        with self.assertRaises(Conflict):
            self.finish(begun)
        public = json.dumps(self.queue.list_nodes())
        self.assertNotIn(begun['updateToken'], public)
        self.assertNotIn('update_hash', public)
        self.assertEqual(self.queue.resume_node('cluster', {'expectedRevision': row['revision'],
            'planId': row['planId'], 'note': 'verified update worker and container stopped', 'workerStopped': True, 'healthy': True})['state'], 'active')

    def test_failed_update_blocks_until_operator_resume(self):
        self.schedule()
        begun = self.begin()
        with self.assertRaises(ValueError):
            self.finish(begun, result={'healthy': False})
        row = self.finish(begun, status='failed', result={'healthy': False})
        self.assertEqual(row['state'], 'failed')
        self.submit()
        self.assertIsNone(self.queue.claim({'workerId': 'w'}))
        payload = {'expectedRevision': row['revision'], 'planId': row['planId'], 'note': 'inspected', 'workerStopped': False, 'healthy': True}
        with self.assertRaises(ValueError):
            self.queue.resume_node('cluster', payload)
        payload['workerStopped'] = True
        for bad in ({key: value for key, value in payload.items() if key != 'healthy'}, {**payload, 'healthy': False}):
            with self.assertRaises(ValueError):
                self.queue.resume_node('cluster', bad)
        self.queue.resume_node('cluster', payload)
        with self.assertRaises(Conflict):
            self.finish(begun, status='failed', result={'healthy': False})

    def test_cancel_and_unknown_nodes(self):
        row = self.schedule(seconds=20)
        self.assertEqual(self.queue.cancel_node('cluster', {'expectedRevision': row['revision'], 'planId': row['planId']})['state'], 'active')
        with self.assertRaises(Conflict):
            self.queue.cancel_node('cluster', {'expectedRevision': row['revision'] + 1, 'planId': row['planId']})
        with self.assertRaises(ValueError):
            self.queue.claim({'workerId': 'w', 'nodeId': 'unknown'})

    def test_timestamps_and_image_are_strict(self):
        base = {'expectedRevision': 0, 'drainAt': '2026-10-24T12:00:00Z',
                'notBefore': '2026-10-24T12:00:00Z', 'afterJobId': None, 'targetImage': IMAGE}
        for field, value in [('drainAt', '2026-10-24T12:00:00'),
                             ('drainAt', '2026-10-24T12:00:00-05:00'),
                             ('notBefore', '2026-10-24T11:59:00Z'),
                             ('targetImage', 'engine:latest'), ('expectedRevision', True)]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.queue.schedule_node('cluster', {**base, field: value})

    def test_held_job_blocks_update_and_cannot_be_cleared_by_resume(self):
        self.submit()
        job = self.queue.claim({'workerId': 'w'})
        self.schedule(after=job['id'])
        self.queue.operator(job['id'], 'hold', {'note': 'inspect engine'})
        with self.assertRaises(Conflict):
            self.begin()

    def test_legacy_running_job_migrates_to_held_cluster(self):
        with tempfile.TemporaryDirectory() as root:
            with sqlite3.connect(root + '/jobs.sqlite3') as db:
                db.execute('''CREATE TABLE jobs (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                    request_id TEXT UNIQUE NOT NULL, environment TEXT NOT NULL,
                    profile TEXT NOT NULL, status TEXT NOT NULL, worker_id TEXT,
                    claim_hash TEXT, result TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('legacy','legacy','staging',?,'running')", (PROFILE,))
            db.close()
            queue = Queue(root)
            try:
                self.assertEqual(queue.get('legacy')['nodeId'], 'cluster')
                self.assertEqual(queue.get('legacy')['status'], 'held')
            finally:
                queue.close()

    def test_operator_resume_cannot_clear_held_job(self):
        self.schedule()
        begun = self.begin()
        row = self.finish(begun, status='failed', result={})
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id) VALUES('held','held','staging',?,'held','cluster')", (PROFILE,))
        with self.assertRaises(Conflict):
            self.queue.resume_node('cluster', {'expectedRevision': row['revision'],
                'planId': row['planId'], 'note': 'inspected', 'workerStopped': True, 'healthy': True})

    def test_operator_hold_fences_updater_without_touching_main_work(self):
        self.schedule()
        begun = self.begin()
        self.submit()
        job = self.queue.claim({'workerId': 'main-worker', 'nodeId': 'main'})
        held = self.queue.hold_node('cluster', {
            'expectedRevision': begun['node']['revision'],
            'planId': begun['node']['planId'], 'note': 'updater response uncertain'})
        self.assertEqual(held['state'], 'held')
        self.assertEqual(held['revision'], begun['node']['revision'] + 1)
        self.assertEqual(held['result'], {'operatorAction': 'hold', 'note': 'updater response uncertain'})
        self.assertEqual(self.queue.get(job['id'])['status'], 'running')
        self.assertEqual(self.queue.get(job['id'])['nodeId'], 'main')
        self.assertEqual(self.queue.get_node('main')['state'], 'active')
        with self.assertRaises(Conflict):
            self.finish(begun)
        self.assertNotIn('updateToken', json.dumps(held))

    def test_operator_hold_requires_updating_state_current_revision_and_plan(self):
        row = self.queue.get_node('cluster')
        with self.assertRaises(Conflict):
            self.queue.hold_node('cluster', {'expectedRevision': row['revision'],
                'planId': row['planId'], 'note': 'not updating'})
        self.schedule()
        begun = self.begin()
        payload = {'expectedRevision': begun['node']['revision'],
                   'planId': begun['node']['planId'], 'note': 'inspect updater'}
        for bad in ({**payload, 'expectedRevision': payload['expectedRevision'] - 1},
                    {**payload, 'planId': 'wrong-plan'}):
            with self.assertRaises(Conflict):
                self.queue.hold_node('cluster', bad)
        for note in ('', ' ', 'x' * 257, None):
            with self.assertRaises(ValueError):
                self.queue.hold_node('cluster', {**payload, 'note': note})
        self.assertEqual(self.queue.get_node('cluster')['state'], 'updating')
        self.queue.hold_node('cluster', payload)
        with self.assertRaises(Conflict):
            self.queue.hold_node('cluster', payload)

