"""Real Viewer HTTP visibility with synthetic local engine and queue."""
import json
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from processing.app import make_handler
from processing.config import Config
from processing.maintenance import Conflict
from tests.test_viewer_http import BOUNDARY, TOKEN, STAGING, multipart
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS


class EvidenceVisibilityHTTPTests(unittest.TestCase):
    def test_corrupt_queued_job_cancels_without_dispatch(self):
        self.run_case('queued')

    def test_corrupt_running_job_remains_owned(self):
        self.run_case('running')

    def test_corrupt_ambiguous_job_is_not_cancelled(self):
        self.run_case('ambiguous')

    def test_mismatched_request_identity_is_not_cancelled(self):
        self.run_case('wrong_request')

    def test_claim_race_has_one_winner(self):
        self.run_case('race')

    def run_case(self, state):
        with tempfile.TemporaryDirectory() as root:
            config=Config('api-token','http://127.0.0.1:19001','http://127.0.0.1:19002',1000,20000,
                prototype_state_dir=root,prototype_node_ids=('cluster','main'),viewer_enabled=True,
                viewer_token=TOKEN,viewer_staging_token=STAGING)
            handler=make_handler(config);service=handler.viewer_service;fleet=FakeFleet()
            engines={name:FakeEngine(name,fleet) for name in ('cluster','main')};service.engines=engines
            service._catalog=lambda:{'ready':True,'commonOptions':[dict(x) for x in OPTIONS]}
            service._selected_options=lambda _node:[dict(x) for x in OPTIONS]
            service.probe=lambda role,origin,token:{'ready':True,'taskQueueCount':0,'maxImages':1000}
            server=ThreadingHTTPServer(('127.0.0.1',0),handler)
            thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));thread.start()
            def request(path,body=None,headers=None,token=TOKEN):
                return urlopen(Request('http://127.0.0.1:'+str(server.server_port)+path+'?token='+token,
                                       data=body,headers=headers or {}),timeout=3)
            def submit():
                identifier=str(uuid.uuid4())
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request('/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:
                    self.assertEqual(json.load(response)['uuid'],identifier)
                body=multipart([('images','photo.jpg',b'synthetic')])
                with request('/task/new/upload/'+identifier,body,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:json.load(response)
                with request('/task/new/commit/'+identifier,b'') as response:json.load(response)
                return identifier
            def cancel(identifier, token=TOKEN):
                return request('/task/cancel',('uuid='+identifier).encode(),
                    {'Content-Type':'application/x-www-form-urlencoded'},token)
            try:
                selected=submit()
                if state == 'running':
                    service.tick()
                    self.assertEqual(handler.prototype_queue.list()[0]['status'],'running')
                    submit()
                with handler.prototype_queue._db() as db:
                    db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?',(selected,))
                    if state == 'ambiguous':
                        db.execute("UPDATE jobs SET worker_id='unknown' WHERE status='queued'")
                    if state == 'wrong_request':
                        db.execute("UPDATE jobs SET request_id='unrelated' WHERE status='queued'")
                with self.assertRaises(HTTPError) as error:
                    cancel(selected, STAGING)
                self.assertEqual(error.exception.code,404);error.exception.close()
                if state == 'race':
                    job_id=handler.prototype_queue.list()[0]['id']
                    barrier=threading.Barrier(2)
                    def do_cancel():
                        barrier.wait(timeout=3)
                        try:
                            with cancel(selected) as response:return response.status
                        except HTTPError as error:
                            code=error.code;error.close();return code
                    def do_claim():
                        barrier.wait(timeout=3)
                        return handler.prototype_queue.claim({'workerId':'race','nodeId':'cluster',
                            'profiles':['odm-viewer-v1'],'expectedJobId':job_id})
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        cancellation=pool.submit(do_cancel);claim=pool.submit(do_claim)
                        status=cancellation.result(timeout=5);claimed=claim.result(timeout=5)
                    self.assertIn((status,claimed is not None),[(200,False),(409,True)])
                    self.assertEqual(handler.prototype_queue.list()[0]['status'],
                                     'running' if claimed else 'failed')
                elif state == 'queued':
                    with cancel(selected) as response:
                        self.assertTrue(json.load(response)['success'])
                    # Simulate a lost first response: repeat the same action,
                    # then reconcile through the real task-info route.
                    with cancel(selected) as response:
                        self.assertTrue(json.load(response)['success'])
                    with request('/task/'+selected+'/info') as response:
                        self.assertEqual(json.load(response)['status']['code'],50)
                    service.tick()
                    self.assertEqual(handler.prototype_queue.list()[0]['status'],'failed')
                    self.assertEqual(fleet.commits,[])
                else:
                    with self.assertRaises(HTTPError) as error:
                        cancel(selected)
                    self.assertEqual(error.exception.code,409);error.exception.close()
                    self.assertEqual(handler.prototype_queue.list()[0]['status'],state if state == 'running' else 'queued')
                    self.assertEqual(len(fleet.commits),1 if state == 'running' else 0)
                    if state == 'running':
                        self.assertEqual(handler.prototype_queue.list()[1]['status'],'queued')
            finally:
                server.shutdown();thread.join(3);self.assertFalse(thread.is_alive())
                server.server_close();service.close();handler.prototype_queue.close()
