"""Real Viewer HTTP visibility with synthetic local engine and queue."""
import json
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
    def test_queued_evidence_corruption_is_visible(self):
        self.run_case(False)

    def test_running_evidence_corruption_preserves_slot(self):
        self.run_case(True)

    def test_terminal_evidence_corruption_does_not_become_nonterminal(self):
        self.run_case(True, terminal=True)

    def run_case(self, running, terminal=False):
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
            def inspect(identifier, expected_code):
                with request('/task/'+identifier+'/info') as response:
                    self.assertEqual(response.status,200);value=json.load(response)
                self.assertEqual(value['status']['code'],expected_code)
                self.assertTrue(value['ledgetopHeld'])
                self.assertEqual(value['ledgetopHoldReason'],'option_evidence_invalid')
                self.assertNotIn('definitions',json.dumps(value))
                with self.assertRaises(HTTPError) as error:request('/task/'+identifier+'/info',token=STAGING)
                self.assertEqual(error.exception.code,404);error.exception.close()
            try:
                selected=submit()
                if running:
                    service.tick()
                    self.assertEqual(handler.prototype_queue.list()[0]['status'],'running')
                    submit()
                with handler.prototype_queue._db() as db:
                    db.execute('DELETE FROM viewer_option_evidence WHERE uuid=?',(selected,))
                if terminal:
                    with handler.prototype_queue._db() as db:
                        db.execute("UPDATE jobs SET status='succeeded' WHERE status='running'")
                    with self.assertRaises(HTTPError) as error:
                        request('/task/'+selected+'/info')
                    self.assertEqual(error.exception.code,409);error.exception.close()
                    return
                with self.assertRaises(Conflict):service.tick()
                inspect(selected,20 if running else 10)
                self.assertEqual([job['status'] for job in handler.prototype_queue.list()],
                                 ['running','queued'] if running else ['queued'])
                self.assertEqual(len(fleet.commits),1 if running else 0)
            finally:
                server.shutdown();thread.join(3);self.assertFalse(thread.is_alive())
                server.server_close();service.close();handler.prototype_queue.close()
