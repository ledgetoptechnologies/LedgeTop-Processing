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
                    db.execute('INSERT INTO viewer_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)', ('provider-uuid', 'engine-uuid', first['id'], 0, 'upload_pending', 22, 0, None, 'secret-claim', '/private/path', 'viewer-fresh'))
                out=snapshot(q, True, 0, 50, None, None)
                row=out['jobs'][0]
                self.assertEqual((row['jobId'],row['providerTaskId'],row['engineTaskId']), (first['id'],'provider-uuid','engine-uuid'))
                self.assertEqual(row['uploadedFiles'],22); self.assertEqual(row['viewerUpdatedAt'],'viewer-fresh'); self.assertNotEqual(row['queueUpdatedAt'],'viewer-fresh')
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
                    db.execute("UPDATE jobs SET status='running',updated_at='new-time' WHERE id=?",(first['id'],))
                tail=snapshot(q,False,page['nextAfter'],1,None,None)
                self.assertNotIn(first['id'],[r['jobId'] for r in tail['jobs']])
                fresh=snapshot(q,False,0,1,None,None)
                self.assertEqual(fresh['jobs'][0]['status'],'running')
                self.assertEqual(fresh['jobs'][0]['queueUpdatedAt'],'new-time')
                self.assertIsNone(fresh['jobs'][0]['viewerUpdatedAt'])
                self.assertEqual(fresh['refresh'],'restart_at_zero_and_read_all_pages')
            finally: q.close()
