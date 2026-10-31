from http.server import BaseHTTPRequestHandler,HTTPServer
import json
import threading
import unittest

from processing.direct_outbox_contract import direct_pipeline_digest
from processing.odx_smoke_worker import SmokeError
from processing.viewer_service import NativeEngine
from processing.viewer_strict_mutations import initialize,commit

ID='11111111-1111-4111-8111-111111111111'


class StrictMutationTests(unittest.TestCase):
    def setUp(self):
        self.calls=[];self.answer=None;outer=self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body=self.rfile.read(int(self.headers.get('Content-Length','0')))
                outer.calls.append((self.path,dict(self.headers),body))
                if outer.answer is False:self.close_connection=True;return
                payload=outer.answer if outer.answer is not None else (
                    {'uuid':ID} if self.path=='/task/new/init' else {'success':True})
                raw=json.dumps(payload).encode();self.send_response(200);self.send_header('Content-Length',str(len(raw)))
                self.end_headers();self.wfile.write(raw)
            def log_message(self,*args):pass
        self.server=HTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.addCleanup(self.close);self.engine=NativeEngine('http://127.0.0.1:'+str(self.server.server_port))
        self.plan={'options':[{'name':'alpha','value':2}],'outputs':['orthophoto']}
        self.digest=direct_pipeline_digest(self.plan['options'],self.plan['outputs'])

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5);self.assertFalse(self.thread.is_alive())

    def test_wire_uses_exact_uuid_and_ordered_plan(self):
        initialize(self.engine,ID,self.plan,self.digest);commit(self.engine,ID)
        self.assertEqual([call[0] for call in self.calls],['/task/new/init','/task/new/commit/'+ID])
        self.assertEqual(self.calls[0][1]['set-uuid'],ID)
        self.assertIn(b'[{"name": "alpha", "value": 2}]',self.calls[0][2])
        self.assertIn(b'["orthophoto"]',self.calls[0][2])

    def test_wrong_plan_or_wrong_identity_fails_closed(self):
        with self.assertRaises(SmokeError):initialize(self.engine,ID,self.plan,'0'*64)
        self.assertEqual(self.calls,[])
        self.answer={'uuid':'22222222-2222-4222-8222-222222222222'}
        with self.assertRaises(SmokeError):initialize(self.engine,ID,self.plan,self.digest)

    def test_missing_commit_success_and_lost_ack_hold(self):
        self.answer={}
        with self.assertRaises(SmokeError):commit(self.engine,ID)
        self.answer=False
        with self.assertRaisesRegex(SmokeError,'uncertain'):commit(self.engine,ID)
        self.assertEqual(len(self.calls),2)
