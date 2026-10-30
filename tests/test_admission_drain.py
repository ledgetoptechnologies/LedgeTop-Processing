import json
import os
import tempfile
import threading
import time
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from dataclasses import replace
from unittest.mock import patch
from processing.app import make_handler
from processing.config import Config, from_environment

API='synthetic-api-'+'a'*32
OP='synthetic-operator-'+'b'*32
VIEW='synthetic-viewer-'+'c'*32


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.config=Config(API,'http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,
            prototype_state_dir=self.temp.name, prototype_operator_token=OP,
            prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),
            viewer_enabled=True,viewer_token=VIEW,admission_drain_enabled=True)
        self.start()

    def start(self):
        self.handler=make_handler(self.config)
        self.server=ThreadingHTTPServer(('127.0.0.1',0),self.handler)
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.02))
        self.thread.start();self.origin='http://127.0.0.1:'+str(self.server.server_port)

    def stop(self):
        self.server.shutdown();self.server.server_close();self.thread.join(2)
        self.handler.viewer_service.close();self.handler.prototype_queue.close()

    def tearDown(self):self.stop()

    def request(self,path,body=None,token=API):
        h={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        try:
            with urlopen(Request(self.origin+path,data=body,headers=h),timeout=4) as r:return r.status,json.loads(r.read())
        except HTTPError as e:return e.code,json.loads(e.read())

    def test_real_http_auth_drain_and_restart(self):
        payload=json.dumps(dict(requestId='synthetic',environment='staging',profile='treedar-synthetic-v1')).encode()
        status,_=self.request('/v1/prototype/jobs',payload);self.assertEqual(status,201)
        path='/v1/prototype/admission-drain';close=b'{"closed":true}'
        self.assertEqual(self.request(path,close,API)[0],401)
        self.assertEqual(self.request(path,close,OP), (200,{'closed':True}))
        self.assertEqual(self.request(path,token=OP), (200,{'closed':True}))
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],503)
        status,data=self.request('/v1/prototype/jobs');self.assertEqual(status,200);self.assertEqual(len(data['jobs']),1)
        for suffix in ('/task/new/init','/task/new/upload/53164287-7755-4c33-ac22-1c02e61959c9','/task/new/commit/53164287-7755-4c33-ac22-1c02e61959c9','/task/ledgetop/reserve'):
            self.assertEqual(self.request(suffix+'?token='+VIEW,b'')[0],503)
        self.stop();self.start()
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],503)
        self.assertEqual(self.request(path,b'{"closed":false}',OP),(200,{'closed':False}))
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],200)

    def test_disabled_restart_does_not_silently_bypass_gate(self):
        self.handler.prototype_queue.admission_drain.set_closed(True)
        self.stop()
        with self.assertRaisesRegex(RuntimeError,'admission_drain_configuration_required'):
            make_handler(replace(self.config,admission_drain_enabled=False))
        self.start()
        self.assertEqual(self.request('/v1/prototype/admission-drain',token=OP),(200,{'closed':True}))

    def test_runtime_environment_cannot_enable_unqualified_drain(self):
        values={'PROCESSING_API_TOKEN':API,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OP,
                'PROCESSING_PROTOTYPE_STATE_DIR':self.temp.name,'PROCESSING_ADMISSION_DRAIN_ENABLED':'1'}
        with patch.dict(os.environ,values,clear=True),self.assertRaisesRegex(ValueError,'not qualified'):
            from_environment()

    def test_close_waits_for_inflight_admission(self):
        entered=threading.Event();release=threading.Event();done=threading.Event()
        original=self.handler.prototype_queue.submit
        def slow_submit(payload):
            entered.set();release.wait(3);return original(payload)
        self.handler.prototype_queue.submit=slow_submit
        payload=json.dumps(dict(requestId='slow-synthetic',environment='staging',profile='treedar-synthetic-v1')).encode()
        admitted=[]
        worker=threading.Thread(target=lambda:admitted.append(self.request('/v1/prototype/jobs',payload)))
        worker.start();self.assertTrue(entered.wait(1))
        result=[]
        closer=threading.Thread(target=lambda:(result.append(self.request('/v1/prototype/admission-drain',b'{"closed":true}',OP)),done.set()))
        closer.start();self.assertFalse(done.wait(.2));release.set();worker.join(2);closer.join(2)
        self.assertEqual(admitted[0][0],201)
        self.assertEqual(result,[(200,{'closed':True})])

    def test_drain_finishes_running_job_but_prevents_next_claim(self):
        def payload(name):return json.dumps(dict(requestId=name,environment='staging',profile='treedar-synthetic-v1')).encode()
        first=self.request('/v1/prototype/jobs',payload('first'))[1]['job']
        self.request('/v1/prototype/jobs',payload('second'))
        claim_body=json.dumps({'workerId':'worker','profiles':['treedar-synthetic-v1'],'nodeId':'main'}).encode()
        running=self.request('/v1/prototype/claim',claim_body)[1]['job']
        self.assertEqual(running['id'],first['id'])
        self.assertEqual(self.request('/v1/prototype/admission-drain',b'{"closed":true}',OP)[0],200)
        completion=json.dumps({'claimToken':running['claimToken'],'status':'succeeded','result':{}}).encode()
        self.assertEqual(self.request('/v1/prototype/jobs/'+first['id']+'/complete',completion)[0],200)
        self.assertIsNone(self.request('/v1/prototype/claim',claim_body)[1]['job'])
        self.assertEqual(self.request('/v1/prototype/admission-drain',b'{"closed":false}',OP)[0],200)
        self.assertIsNotNone(self.request('/v1/prototype/claim',claim_body)[1]['job'])
