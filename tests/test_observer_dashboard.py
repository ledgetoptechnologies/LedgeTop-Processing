import http.client
import unittest
from dataclasses import replace
from urllib.error import HTTPError
from processing.config import Config
from tests.test_prototype_http import PrototypeHTTPTests
from tests.test_service import TOKEN, start_server
from tests.test_prototype import submission
from processing.app import make_handler

class DashboardHTTPTests(PrototypeHTTPTests):
    def test_bounded_observer_auth_and_safe_projection(self):
        self.server.shutdown(); self.server.server_close(); self.handler.prototype_queue.close()
        observer = 'observer-dashboard-only-token-1234567890123456'
        self.handler = make_handler(replace(Config(TOKEN, '', '', 800, 20000,
            prototype_state_dir=self.directory.name), dashboard_token=observer, prototype_observer_token="legacy-observer-token-1234567890123456"))
        self.server = start_server(self.handler)
        self.origin = f'http://127.0.0.1:{self.server.server_port}'
        first = self.request('/v1/prototype/jobs', submission('safe-first'))[1]['job']
        second = self.request('/v1/prototype/jobs', submission('safe-second'))[1]['job']
        self.handler.prototype_queue._db
        for token in (TOKEN, 'wrong', 'legacy-observer-token-1234567890123456'):
            with self.assertRaises(HTTPError) as e: self.request('/v1/observer/dashboard', token=token)
            self.assertEqual(e.exception.code, 401)
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        try:
            conn.putrequest('GET', '/v1/observer/dashboard')
            conn.putheader('Authorization', 'Bearer ' + observer)
            conn.putheader('Authorization', 'Bearer ' + observer)
            conn.endheaders()
            response = conn.getresponse()
            self.assertEqual(response.status, 401)
            response.read()
        finally:
            conn.close()
        for query in ('?limit=101', '?limit=0', '?after=-1', '?after=1&after=2', '?status=bad', '?secret=1'):
            with self.assertRaises(HTTPError) as e: self.request('/v1/observer/dashboard'+query, token=observer)
            self.assertEqual(e.exception.code, 400)
        with self.assertRaises(HTTPError) as e: self.request('/v1/prototype/jobs', token=observer)
        self.assertEqual(e.exception.code, 401)
        page=self.request('/v1/observer/dashboard?limit=1', token=observer)[1]
        self.assertEqual(page['jobs'][0]['jobId'], first['id'])
        self.assertIsNone(page['jobs'][0]['providerTaskId'])
        self.assertNotIn('result', page['jobs'][0]); self.assertNotIn('workerId', page['jobs'][0])
        self.assertNotIn('requestId', page['jobs'][0]); self.assertIsNotNone(page['nextAfter'])
        page2=self.request('/v1/observer/dashboard?limit=1&after='+str(page['nextAfter']), token=observer)[1]
        self.assertEqual(page2['jobs'][0]['jobId'], second['id']); self.assertIsNone(page2['nextAfter'])
        self.assertTrue(page['observedAt']); self.assertEqual(page['consistency'],'per_page_database_snapshot'); self.assertEqual(page['refresh'],'restart_at_zero_and_read_all_pages')

if __name__ == '__main__': unittest.main()

class DashboardJoinTests(unittest.TestCase):
    def test_viewer_identity_join_and_secrets_omitted(self):
        import tempfile
        from processing.prototype import Queue
        from processing.observer_dashboard import snapshot
        with tempfile.TemporaryDirectory() as root:
            q = Queue(root)
            try:
                first, _ = q.submit(submission('dashboard-join'))
                with q._db() as db:
                    db.execute('CREATE TABLE viewer_tasks (uuid TEXT, engine_uuid TEXT, job_id TEXT, removed INTEGER, phase TEXT, upload_cursor INTEGER, progress REAL, error TEXT, claim_token TEXT, directory TEXT, updated_at TEXT)')
                    db.execute('INSERT INTO viewer_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)', ('provider-uuid', 'engine-uuid', first['id'], 0, 'upload_pending', 22, 0, None, 'secret-claim', '/private/path', '2026-11-10T23:20:00Z'))
                out=snapshot(q, True, 0, 50, None, None)
                row=out['jobs'][0]
                self.assertEqual((row['jobId'],row['providerTaskId'],row['engineTaskId']), (first['id'],'provider-uuid','engine-uuid'))
                self.assertEqual(row['uploadedFiles'],22); self.assertEqual(row['viewerUpdatedAt'],'2026-11-10T23:20:00Z'); self.assertNotEqual(row['queueUpdatedAt'],'2026-11-10T23:20:00Z')
                self.assertNotIn('secret-claim',str(out)); self.assertNotIn('/private/path',str(out))
            finally: q.close()

    def test_creation_cursor_requires_rescan_and_configured_order(self):
        import tempfile
        from processing.prototype import Queue
        from processing.observer_dashboard import snapshot
        with tempfile.TemporaryDirectory() as root:
            q=Queue(root, node_ids=('main','cluster'))
            try:
                first,_=q.submit(submission('refresh-first'))
                q.submit(submission('refresh-second'))
                page=snapshot(q,False,0,1,None,None)
                self.assertEqual([n['nodeId'] for n in page['nodes']], ['main','cluster'])
                with q._db() as db:
                    db.execute("UPDATE jobs SET status='running',updated_at='2026-11-10 23:25:00' WHERE id=?",(first['id'],))
                tail=snapshot(q,False,page['nextAfter'],1,None,None)
                self.assertNotIn(first['id'],[r['jobId'] for r in tail['jobs']])
                fresh=snapshot(q,False,0,1,None,None)
                self.assertEqual(fresh['jobs'][0]['status'],'running')
                self.assertEqual(fresh['jobs'][0]['queueUpdatedAt'],'2026-11-10T23:25:00Z')
                self.assertIsNone(fresh['jobs'][0]['viewerUpdatedAt'])
                self.assertEqual(fresh['refresh'],'restart_at_zero_and_read_all_pages')
            finally: q.close()

class DashboardAvailabilityTests(unittest.TestCase):
    def test_unreported_nodes_do_not_look_healthy_or_fresh_and_strings_are_redacted(self):
        import tempfile
        from processing.prototype import Queue
        from processing.observer_dashboard import snapshot
        with tempfile.TemporaryDirectory() as root:
            q = Queue(root, node_ids=('cluster', 'main', 'third'))
            try:
                production, _ = q.submit({**submission('projection-prod'), 'environment': 'production'})
                staging, _ = q.submit({**submission('projection-stage'), 'environment': 'staging'})
                with q._db() as db:
                    db.execute('CREATE TABLE viewer_tasks (uuid TEXT, engine_uuid TEXT, job_id TEXT, removed INTEGER, phase TEXT, upload_cursor INTEGER, progress REAL, error TEXT, updated_at TEXT)')
                    db.execute('INSERT INTO viewer_tasks VALUES (?,?,?,?,?,?,?,?,?)',
                               ('provider-uuid', 'engine-uuid', production['id'], 0,
                                'SECRET_CLIENT_PHASE', 2, 0, 'SECRET_CLIENT_ERROR', '2026-11-10T23:20:00Z'))
                    db.execute('UPDATE jobs SET held_reason=? WHERE id=?', ('SECRET_CLIENT_REASON', production['id']))
                out = snapshot(q, True, 0, 50, None, None)
                self.assertEqual(out['schemaVersion'], 1)
                self.assertEqual([x['nodeId'] for x in out['nodes']], ['cluster', 'main', 'third'])
                self.assertEqual({x['environment'] for x in out['jobs']}, {'production', 'staging'})
                for node in out['nodes']:
                    for key in ('engine', 'container', 'resources'):
                        self.assertEqual(node[key], {'state': 'unavailable', 'reason': 'not_reported', 'observedAt': None})
                self.assertEqual(out['logs']['state'], 'unavailable')
                self.assertNotIn('SECRET_CLIENT_', str(out))
                first = next(x for x in out['jobs'] if x['jobId'] == production['id'])
                self.assertEqual((first['phase'], first['errorCode'], first['heldReason']), ('other', 'other', 'other'))
            finally:
                q.close()

    def test_configured_order_limits_and_live_limit_unknown(self):
        import tempfile
        from processing.config import ProcessingNode
        from processing.prototype import Queue
        from processing.observer_dashboard import snapshot
        with tempfile.TemporaryDirectory() as root:
            q = Queue(root, node_ids=('cluster', 'main', 'third'))
            try:
                nodes = (ProcessingNode('cluster', 'http://cluster.invalid', max_photos=1000),
                         ProcessingNode('main', 'http://main.invalid', max_photos=None),
                         ProcessingNode('third', 'http://third.invalid', max_photos=700))
                out = snapshot(q, False, 0, 50, None, None, configured_nodes=nodes)
                self.assertEqual([n['preferenceIndex'] for n in out['nodes']], [0, 1, 2])
                self.assertEqual([n['configuredPhotoLimit'] for n in out['nodes']], [
                    {'state': 'configured', 'source': 'configuration', 'maxPhotos': 1000},
                    {'state': 'no_configured_cap', 'source': 'configuration', 'maxPhotos': None},
                    {'state': 'configured', 'source': 'configuration', 'maxPhotos': 700}])
                self.assertTrue(all(n['effectivePhotoLimit']['state'] == 'unavailable' for n in out['nodes']))
                fallback = snapshot(q, False, 0, 50, None, None)
                self.assertTrue(all(n['configuredPhotoLimit']['state'] == 'unavailable' for n in fallback['nodes']))
                for bad in ((nodes[1], nodes[0], nodes[2]), (nodes[0], nodes[1]),
                            (nodes[0], nodes[1], nodes[1]),
                            (nodes[0], nodes[1], ProcessingNode('unknown', 'http://x.invalid')),
                            (nodes[0], nodes[1], ProcessingNode('third', 'http://x.invalid', max_photos=-1))):
                    with self.subTest(bad=bad), self.assertRaises(ValueError):
                        snapshot(q, False, 0, 50, None, None, configured_nodes=bad)
            finally:
                q.close()

    def test_missing_configured_node_fails_closed(self):
        import tempfile
        from processing.prototype import Queue
        from processing.observer_dashboard import snapshot
        with tempfile.TemporaryDirectory() as root:
            q = Queue(root, node_ids=('cluster', 'main'))
            try:
                with q._db() as db:
                    db.execute("DELETE FROM nodes WHERE id='main'")
                with self.assertRaises(ValueError): snapshot(q, False, 0, 50, None, None)
            finally:
                q.close()

class DashboardTimestampTests(unittest.TestCase):
    def test_timestamp_contract(self):
        from processing.observer_dashboard import utc_wire
        self.assertIsNone(utc_wire(None))
        self.assertEqual(utc_wire('2026-11-10 23:25:00'), '2026-11-10T23:25:00Z')
        self.assertEqual(utc_wire('2026-11-10T17:25:00-06:00'), '2026-11-10T23:25:00Z')
        self.assertEqual(utc_wire('2026-11-10T23:25:00.125Z'), '2026-11-10T23:25:00.125000Z')
        for bad in ('2026-11-10T23:25:00', 'yesterday', '2026-13-10 00:00:00', '', 1):
            with self.subTest(value=bad), self.assertRaises(ValueError): utc_wire(bad)
