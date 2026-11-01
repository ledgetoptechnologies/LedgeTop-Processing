import json
import socket
import subprocess
import sys
import signal
import http.client
import io
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from dataclasses import replace
from unittest.mock import patch
from processing.app import make_handler
from processing.config import Config, from_environment

API='synthetic-api-'+'a'*32
OP='synthetic-operator-'+'b'*32
VIEW='synthetic-viewer-'+'c'*32
STAGE='synthetic-staging-'+'d'*32


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

    def operation(self, closed):
        state=self.handler.prototype_queue.admission_drain.status()
        return json.dumps(dict(closed=closed,requestId=str(uuid.uuid4()),expectedGeneration=state['generation'],expectedClosed=state['closed'],expectedAuthorityId=state['authorityId'])).encode()

    def test_real_http_auth_drain_and_restart(self):
        payload=json.dumps(dict(requestId='synthetic',environment='staging',profile='treedar-synthetic-v1')).encode()
        status,_=self.request('/v1/prototype/jobs',payload);self.assertEqual(status,201)
        path='/v1/prototype/admission-drain';close=self.operation(True)
        self.assertEqual(self.request(path,close,API)[0],401)
        self.assertEqual(self.request(path,close,OP)[0],200)
        self.assertTrue(self.request(path,token=OP)[1]['closed'])
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],503)
        status,data=self.request('/v1/prototype/jobs');self.assertEqual(status,200);self.assertEqual(len(data['jobs']),1)
        for suffix in ('/task/new/init','/task/new/upload/53164287-7755-4c33-ac22-1c02e61959c9','/task/new/commit/53164287-7755-4c33-ac22-1c02e61959c9','/task/ledgetop/reserve','/task/ledgetop/cancel','/task/cancel','/task/remove'):
            self.assertEqual(self.request(suffix+'?token='+VIEW,b'')[0],503)
        self.stop();self.start()
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],503)
        self.assertEqual(self.request(path,self.operation(False),OP)[0],200)
        self.assertEqual(self.request('/v1/prototype/jobs',payload)[0],200)

    def test_fresh_closed_start_blocks_production_and_staging_submissions(self):
        self.stop()
        self.config=replace(self.config,prototype_state_dir=os.path.join(self.temp.name,'closed-start'),
                            admission_start_closed=True,viewer_staging_token=STAGE)
        self.start()
        self.assertTrue(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'])
        body=json.dumps(dict(requestId='synthetic-closed',environment='staging',profile='treedar-synthetic-v1')).encode()
        self.assertEqual(self.request('/v1/prototype/jobs',body)[0],503)
        for token in (VIEW,STAGE):
            self.assertEqual(self.request('/task/53164287-7755-4c33-ac22-1c02e61959c9/download/all.zip?token='+token)[0],503)
        for token in (VIEW,STAGE):
            for suffix in ('/task/new/init','/task/new/upload/53164287-7755-4c33-ac22-1c02e61959c9',
                           '/task/new/commit/53164287-7755-4c33-ac22-1c02e61959c9',
                           '/task/ledgetop/reserve','/task/ledgetop/cancel','/task/cancel','/task/remove'):
                with self.subTest(token=token==VIEW,suffix=suffix):
                    self.assertEqual(self.request(suffix+'?token='+token,b'')[0],503)
        self.stop();self.start()
        self.assertTrue(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'])
        self.assertEqual(self.request('/v1/prototype/jobs',body)[0],503)

    def test_closed_start_only_operator_reopens_and_open_state_cannot_restart_closed(self):
        self.stop()
        self.config=replace(self.config,prototype_state_dir=os.path.join(self.temp.name,'explicit-reopen'),
                            admission_start_closed=True,viewer_staging_token=STAGE)
        self.start()
        path='/v1/prototype/admission-drain';opening=self.operation(False)
        self.assertEqual(self.request(path,opening,API)[0],401)
        self.assertTrue(self.request(path,token=OP)[1]['closed'])
        self.assertEqual(self.request(path,opening,OP)[0],200)
        self.assertFalse(self.request(path,token=OP)[1]['closed'])
        self.stop()
        try:
            from processing.admission_drain import AdmissionClosed
            with self.assertRaises(AdmissionClosed):make_handler(self.config)
        finally:
            self.config=replace(self.config,admission_start_closed=False);self.start()
        self.assertFalse(self.request(path,token=OP)[1]['closed'])

    def test_closed_start_refuses_existing_open_without_reopening_or_changing_gate(self):
        self.assertFalse(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'])
        self.stop()
        try:
            from processing.admission_drain import AdmissionClosed
            with self.assertRaises(AdmissionClosed):make_handler(replace(self.config,admission_start_closed=True))
        finally:self.start()
        self.assertFalse(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'])

    def test_invalid_closed_start_requires_enabled_drain_before_opening_state(self):
        state=os.path.join(self.temp.name,'must-not-create')
        with self.assertRaisesRegex(RuntimeError,'closed_start_requires_admission_drain'):
            make_handler(replace(self.config,prototype_state_dir=state,admission_drain_enabled=False,admission_start_closed=True))
        self.assertFalse(os.path.exists(state))
        for bad in (replace(self.config,prototype_state_dir='',admission_start_closed=True),
                    replace(self.config,prototype_operator_token='',admission_start_closed=True),
                    replace(self.config,reference_config_path='unqualified',admission_start_closed=True)):
            with self.assertRaisesRegex(RuntimeError,'closed_start_requires_admission_drain'):make_handler(bad)
        values={'PROCESSING_API_TOKEN':API,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OP,
                'PROCESSING_PROTOTYPE_STATE_DIR':self.temp.name,'PROCESSING_ADMISSION_START_CLOSED':'1'}
        with patch.dict(os.environ,values,clear=True),self.assertRaisesRegex(ValueError,'closed startup requires'):
            from_environment()

    def test_closed_restart_keeps_preexisting_viewer_and_generic_jobs_unclaimed(self):
        service=self.handler.viewer_service;identifier=str(uuid.uuid4())
        with patch.object(service,'_check_options',return_value=[]):
            service.initialize(identifier,'synthetic',[],None,'production')
            service.upload(identifier,'photo.jpg',io.BytesIO(b'synthetic'),9)
            service.commit(identifier)
        body=json.dumps(dict(requestId='queued-generic',environment='staging',profile='treedar-synthetic-v1')).encode()
        self.assertEqual(self.request('/v1/prototype/jobs',body)[0],201)
        self.assertEqual(self.request('/v1/prototype/admission-drain',self.operation(True),OP)[0],200)
        self.stop();self.config=replace(self.config,admission_start_closed=True);self.start()
        claim=json.dumps({'workerId':'worker','nodeId':'main','profiles':['treedar-synthetic-v1']}).encode()
        self.assertIsNone(self.request('/v1/prototype/claim',claim,API)[1]['job'])
        service=self.handler.viewer_service
        with patch.object(service,'probe',return_value={'ready':True,'maxImages':1000,'taskQueueCount':0}), \
             patch.object(service,'_check_options',return_value=[]):
            service.tick()
        task=service.store.task(identifier)
        self.assertEqual(task['job']['status'],'queued');self.assertIsNone(task['node_id'])
        self.assertIsNone(self.request('/v1/prototype/claim',claim,API)[1]['job'])

    @unittest.skipUnless(sys.platform.startswith('linux'), 'separate POSIX coordinator process')
    def test_closed_nonempty_state_survives_actual_coordinator_restarts(self):
        service=self.handler.viewer_service;identifier=str(uuid.uuid4());photo=b'synthetic-photo-data'
        with patch.object(service,'_check_options',return_value=[]):
            service.initialize(identifier,'synthetic',[],None,'production')
            service.upload(identifier,'photo.jpg',io.BytesIO(photo),len(photo))
            service.commit(identifier)
        task=service.store.task(identifier)
        image_path=next(p for p in Path(task['directory']).rglob('*') if p.is_file() and p.read_bytes()==photo)
        generic=json.dumps(dict(requestId='restart-generic',environment='staging',profile='treedar-synthetic-v1')).encode()
        status,created=self.request('/v1/prototype/jobs',generic);self.assertEqual(status,201)
        generic_job=created['job']['id']
        self.assertEqual(self.request('/v1/prototype/admission-drain',self.operation(True),OP)[0],200)
        state=self.handler.prototype_queue.admission_drain.status()
        expected_job=task['job']['id']
        self.stop()
        env={key:os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        env.update(PROCESSING_API_TOKEN=API,PROCESSING_PROTOTYPE_OPERATOR_TOKEN=OP,
            PROCESSING_PROTOTYPE_STATE_DIR=self.temp.name,PROCESSING_ADMISSION_DRAIN_ENABLED='1',
            PROCESSING_ADMISSION_START_CLOSED='1',PROCESSING_VIEWER_ENABLED='1',
            PROCESSING_VIEWER_TOKEN=VIEW,PROCESSING_VIEWER_STAGING_TOKEN=STAGE,
            PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_TREEDAR_NODE_IDS='main',
            LOCAL_NODE_URL='http://127.0.0.1:1',MAIN_NODE_URL='http://127.0.0.1:2',
            PROCESSING_BIND_HOST='127.0.0.1')
        try:
            for _ in range(2):
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1',0));env['PROCESSING_PORT']=str(probe.getsockname()[1])
                child=subprocess.Popen([sys.executable,'-c',
                    'import processing.config as c;c.ADMISSION_DRAIN_QUALIFIED=True;from processing.app import main;raise SystemExit(main())'],
                    env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                try:
                    self.origin='http://127.0.0.1:'+env['PROCESSING_PORT']
                    deadline=time.monotonic()+5
                    while True:
                        try:
                            status,gate=self.request('/v1/prototype/admission-drain',token=OP)
                            break
                        except (OSError, URLError):
                            if child.poll() is not None or time.monotonic()>=deadline:raise
                            time.sleep(.03)
                    self.assertEqual(status,200);self.assertEqual(gate,state)
                    self.assertEqual(self.request('/v1/prototype/jobs',generic)[0],503)
                    for token in (VIEW,STAGE):
                        self.assertEqual(self.request('/task/new/init?token='+token,b'')[0],503)
                    claim=json.dumps({'workerId':'worker','nodeId':'main','profiles':['treedar-synthetic-v1']}).encode()
                    self.assertIsNone(self.request('/v1/prototype/claim',claim,API)[1]['job'])
                    jobs=self.request('/v1/prototype/jobs')[1]['jobs']
                    self.assertEqual(len(jobs),2)
                    self.assertEqual({j['id'] for j in jobs},{expected_job,generic_job})
                    self.assertTrue(all(j['status']=='queued' for j in jobs))
                    self.assertEqual(next(j for j in jobs if j['id']==expected_job)['status'],'queued')
                    self.assertEqual(image_path.read_bytes(),photo)
                finally:
                    if child.poll() is None:child.send_signal(signal.SIGTERM)
                    try:child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill();child.wait(timeout=5)
                self.assertEqual(child.returncode,0)
        finally:
            self.config=replace(self.config,admission_start_closed=True);self.start()
        task=self.handler.viewer_service.store.task(identifier)
        self.assertEqual(task['job']['id'],expected_job);self.assertEqual(task['job']['status'],'queued')
        self.assertEqual(image_path.read_bytes(),photo)

    def test_refused_start_releases_acquired_authority_lease(self):
        self.stop()
        class Lease:
            closed=False
            def close(self):self.closed=True
        lease=Lease()
        try:
            from processing.admission_drain import AdmissionClosed
            with patch('processing.authority_lease.acquire',return_value=lease), \
                 patch('processing.authority_lease.record_required'):
                with self.assertRaises(AdmissionClosed):
                    make_handler(replace(self.config,admission_start_closed=True,authority_lease_dir='/synthetic'))
            self.assertTrue(lease.closed)
        finally:self.start()

    def test_closed_start_environment_parses_only_under_existing_qualification(self):
        values={'PROCESSING_API_TOKEN':API,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OP,
                'PROCESSING_PROTOTYPE_STATE_DIR':self.temp.name,
                'PROCESSING_ADMISSION_DRAIN_ENABLED':'1','PROCESSING_ADMISSION_START_CLOSED':'1'}
        with patch.dict(os.environ,values,clear=True):
            with self.assertRaisesRegex(ValueError,'not qualified'):from_environment()
            with patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True):
                configured=from_environment()
                self.assertTrue(configured.admission_drain_enabled)
                self.assertTrue(configured.admission_start_closed)

    def test_concurrent_closed_initialization_stays_closed(self):
        from processing.admission_drain import AdmissionDrain
        from processing.prototype import Queue
        root=os.path.join(self.temp.name,'parallel-gate');q=Queue(root)
        try:
            barrier=threading.Barrier(3);values=[];errors=[]
            def initialize():
                try:barrier.wait();values.append(AdmissionDrain(q,initial_closed=True).status())
                except Exception as exc:errors.append(type(exc).__name__)
            threads=[threading.Thread(target=initialize) for _ in range(2)]
            for thread in threads:thread.start()
            barrier.wait()
            for thread in threads:thread.join(5)
            self.assertFalse(any(thread.is_alive() for thread in threads));self.assertEqual(errors,[])
            self.assertEqual(len(values),2);self.assertTrue(all(item['closed'] for item in values))
        finally:q.close()

    def test_disabled_restart_does_not_silently_bypass_gate(self):
        self.request('/v1/prototype/admission-drain',self.operation(True),OP)
        self.stop()
        with self.assertRaisesRegex(RuntimeError,'admission_drain_configuration_required'):
            make_handler(replace(self.config,admission_drain_enabled=False))
        self.start()
        self.assertTrue(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'])

    def test_runtime_environment_cannot_enable_unqualified_drain(self):
        values={'PROCESSING_API_TOKEN':API,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OP,
                'PROCESSING_PROTOTYPE_STATE_DIR':self.temp.name,'PROCESSING_ADMISSION_DRAIN_ENABLED':'1'}
        with patch.dict(os.environ,values,clear=True),self.assertRaisesRegex(ValueError,'not qualified'):
            from_environment()

    def test_delayed_retry_and_stale_reopen_cannot_change_newer_state(self):
        path='/v1/prototype/admission-drain'
        close=self.operation(True)
        receipt=self.request(path,close,OP);self.assertEqual(receipt[0],200)
        self.stop();self.start()
        self.assertEqual(self.request(path,close,OP),receipt)
        reopen=self.operation(False);self.assertEqual(self.request(path,reopen,OP)[0],200)
        self.assertEqual(self.request(path,close,OP),receipt)
        self.assertFalse(self.request(path,token=OP)[1]['closed'])
        self.assertEqual(self.request(path,self.operation(True),OP)[0],200)
        self.assertEqual(self.request(path,reopen,OP)[0],200) # historical receipt only
        self.assertTrue(self.request(path,token=OP)[1]['closed'])
        stale=json.loads(reopen);stale['requestId']=str(uuid.uuid4())
        self.assertEqual(self.request(path,json.dumps(stale).encode(),OP)[0],409)
        changed=json.loads(reopen);changed['closed']=True
        self.assertEqual(self.request(path,json.dumps(changed).encode(),OP)[0],409)
        self.assertTrue(self.request(path,token=OP)[1]['closed'])

    def test_competing_requests_share_one_generation_winner(self):
        path='/v1/prototype/admission-drain';requests=[self.operation(True),self.operation(True)]
        barrier=threading.Barrier(3);results=[]
        def run(body):
            barrier.wait();results.append(self.request(path,body,OP)[0])
        threads=[threading.Thread(target=run,args=(body,)) for body in requests]
        for thread in threads:thread.start()
        barrier.wait()
        for thread in threads:thread.join(2)
        self.assertEqual(sorted(results),[200,409])
        self.assertTrue(self.request(path,token=OP)[1]['closed'])

    def test_wrong_authority_and_reserved_reopen_capacity(self):
        path='/v1/prototype/admission-drain'
        self.assertEqual(self.request(path,b'{"closed":true,"closed":false}',OP)[0],400)
        wrong=json.loads(self.operation(True));wrong['expectedAuthorityId']=str(uuid.uuid4())
        self.assertEqual(self.request(path,json.dumps(wrong).encode(),OP)[0],409)
        self.assertFalse(self.request(path,token=OP)[1]['closed'])
        queue=self.handler.prototype_queue
        with queue._db() as db:
            db.executemany('INSERT INTO coordinator_admission_operations VALUES(?,?,?,?,?,?)',
                ((str(uuid.uuid4()),queue.authority_id,0,0,1,1) for _ in range(9998)))
        self.assertEqual(self.request(path,self.operation(True),OP)[0],200)
        self.assertEqual(self.request(path,self.operation(False),OP)[0],200)
        self.assertEqual(self.request(path,self.operation(True),OP)[0],409)
        self.assertFalse(self.request(path,token=OP)[1]['closed'])

    def test_reserved_init_503_before_body_and_lost_reply_restart_replay(self):
        from tests.test_viewer_http import multipart, BOUNDARY
        identifier=str(uuid.uuid4());cap='YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'
        headers={'Content-Type':'application/x-www-form-urlencoded','X-LedgeTop-Capability':cap}
        def reserve():
            with urlopen(Request(self.origin+'/task/ledgetop/reserve?token='+VIEW,data=('uuid='+identifier).encode(),headers=headers),timeout=3) as r:return r.status
        self.assertEqual(reserve(),200)
        body=multipart([('name',None,b'synthetic'),('options',None,b'[]')]);path='/task/new/init?token='+VIEW
        init_headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier,'X-LedgeTop-Capability':cap}
        gate=self.handler.prototype_queue.admission_drain;gate._closing.set()
        try:
            conn=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=2)
            try:
                conn.putrequest('POST',path);conn.putheader('Content-Length',str(len(body)))
                for key,value in init_headers.items():conn.putheader(key,value)
                conn.endheaders();self.assertEqual(conn.getresponse().status,503)
            finally:conn.close()
        finally:gate._closing.clear()
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks').fetchone()[0],0)
        with patch.object(self.handler.viewer_service,'_check_options',return_value=[]):
            # Send a complete request and disconnect without reading the response.
            raw=('POST '+path+' HTTP/1.1\r\nHost: localhost\r\nContent-Length: '+str(len(body))+'\r\n'+''.join(k+': '+v+'\r\n' for k,v in init_headers.items())+'\r\n').encode()+body
            sock=socket.create_connection(('127.0.0.1',self.server.server_port),timeout=2);sock.sendall(raw);sock.close()
            deadline=time.monotonic()+3
            while time.monotonic()<deadline:
                with self.handler.prototype_queue._db() as db: count=db.execute('SELECT count(*) FROM viewer_tasks').fetchone()[0]
                if count==1:break
                time.sleep(.02)
            self.assertEqual(count,1)
            with urlopen(Request(self.origin+path,data=body,headers=init_headers),timeout=3) as r:self.assertEqual(r.status,200)
        from processing.maintenance import Conflict
        with patch.object(self.handler.viewer_service,'_check_options',side_effect=Conflict('catalog unavailable')) as catalog:
            with urlopen(Request(self.origin+path,data=body,headers=init_headers),timeout=3) as r:self.assertEqual(r.status,200)
            catalog.assert_not_called()
            different=multipart([('name',None,b'different'),('options',None,b'[]')])
            with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=different,headers=init_headers),timeout=3)
            self.assertEqual(e.exception.code,409);catalog.assert_not_called()
            for changed in (multipart([('name',None,b'synthetic'),('options',None,b'[{"name":"pc-ept","value":true}]')]),
                            multipart([('name',None,b'synthetic'),('options',None,b'[]'),('outputs',None,b'["other.zip"]')])):
                with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=changed,headers=init_headers),timeout=3)
                self.assertEqual(e.exception.code,409)
            catalog.assert_not_called()
            wrong=dict(init_headers);wrong['X-LedgeTop-Capability']='YmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI'
            with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=body,headers=wrong),timeout=3)
            self.assertEqual(e.exception.code,404);catalog.assert_not_called()
        with patch.object(self.handler.viewer_service,'_check_options',side_effect=ValueError('option no longer offered')) as catalog:
            with urlopen(Request(self.origin+path,data=body,headers=init_headers),timeout=3) as r:self.assertEqual(r.status,200)
            catalog.assert_not_called()
        self.stop();self.start()
        with patch.object(self.handler.viewer_service,'_check_options',return_value=[]):
            with urlopen(Request(self.origin+path,data=body,headers=init_headers),timeout=3) as r:self.assertEqual(r.status,200)
            wrong=dict(init_headers);wrong['X-LedgeTop-Capability']='YmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmJiYmI'
            with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=body,headers=wrong),timeout=3)
            self.assertEqual(e.exception.code,404)
            different=multipart([('name',None,b'different'),('options',None,b'[]')])
            with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=different,headers=init_headers),timeout=3)
            self.assertEqual(e.exception.code,409)
            cancel=Request(self.origin+'/task/ledgetop/cancel?token='+VIEW,data=('uuid='+identifier).encode(),headers=headers)
            with urlopen(cancel,timeout=3) as r:self.assertEqual(r.status,200)
            with self.assertRaises(HTTPError) as e:urlopen(Request(self.origin+path,data=body,headers=init_headers),timeout=3)
            self.assertEqual(e.exception.code,409)
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks').fetchone()[0],1)

    def test_overlapping_reserved_init_replays_and_conflicts_atomically(self):
        from tests.test_viewer_http import multipart, BOUNDARY
        self.stop();self.config=replace(self.config,admission_drain_enabled=False,prototype_state_dir=os.path.join(self.temp.name,'init-race'));self.start()
        service=self.handler.viewer_service;cap='YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'
        def run_pair(names):
            identifier=str(uuid.uuid4());reserve=Request(self.origin+'/task/ledgetop/reserve?token='+VIEW,data=('uuid='+identifier).encode(),headers={'Content-Type':'application/x-www-form-urlencoded','X-LedgeTop-Capability':cap})
            with urlopen(reserve,timeout=3) as r:self.assertEqual(r.status,200)
            headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier,'X-LedgeTop-Capability':cap};barrier=threading.Barrier(2);results=[];errors=[];original=service.store.initialize
            def simultaneous(*args,**kwargs):barrier.wait(3);return original(*args,**kwargs)
            def call(name):
                try:
                    body=multipart([('name',None,name.encode()),('options',None,b'[]')])
                    with urlopen(Request(self.origin+'/task/new/init?token='+VIEW,data=body,headers=headers),timeout=5) as r:results.append(r.status)
                except HTTPError as e:results.append(e.code)
                except Exception as e:errors.append(e)
            workers=[threading.Thread(target=call,args=(name,)) for name in names]
            with patch.object(service,'_check_options',return_value=[]),patch.object(service.store,'initialize',side_effect=simultaneous):
                try:
                    for t in workers:t.start()
                    for t in workers:t.join(5)
                finally:
                    barrier.abort()
                    for t in workers:t.join(3)
            self.assertFalse(any(t.is_alive() for t in workers));self.assertEqual(errors,[])
            with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()[0],1)
            return sorted(results)
        self.assertEqual(run_pair(('same','same')),[200,200])
        self.assertEqual(run_pair(('first','second')),[200,409])

    def test_reserved_init_transaction_rollback_preserves_orphan_on_retry(self):
        from tests.test_viewer_http import multipart, BOUNDARY
        identifier=str(uuid.uuid4());cap='YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'
        reserve=Request(self.origin+'/task/ledgetop/reserve?token='+VIEW,data=('uuid='+identifier).encode(),headers={'Content-Type':'application/x-www-form-urlencoded','X-LedgeTop-Capability':cap})
        with urlopen(reserve,timeout=3) as r:self.assertEqual(r.status,200)
        body=multipart([('name',None,b'synthetic'),('options',None,b'[]')]);url=self.origin+'/task/new/init?token='+VIEW
        headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier,'X-LedgeTop-Capability':cap}
        with patch.object(self.handler.viewer_service,'_check_options',return_value=[]),patch('processing.viewer_tasks.sync_directory',side_effect=OSError('synthetic failure')):
            with self.assertRaises(HTTPError) as e:urlopen(Request(url,data=body,headers=headers),timeout=3)
            self.assertEqual(e.exception.code,503)
        store=self.handler.viewer_service.store;orphan=store.root/(identifier+'-1')
        self.assertTrue(orphan.is_dir())
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_tasks').fetchone()[0],0)
        with patch.object(self.handler.viewer_service,'_check_options',return_value=[]):
            with urlopen(Request(url,data=body,headers=headers),timeout=3) as r:self.assertEqual(r.status,200)
        with self.handler.prototype_queue._db() as db:
            row=db.execute('SELECT directory FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone()
        self.assertTrue(orphan.is_dir());self.assertNotEqual(row['directory'],orphan.name);self.assertTrue((store.root/row['directory']).is_dir())

    def test_close_waits_for_inflight_admission(self):
        entered=threading.Event();release=threading.Event();done=threading.Event()
        operation=self.operation(True)
        original=self.handler.prototype_queue.submit
        def slow_submit(payload):
            entered.set();release.wait(3);return original(payload)
        self.handler.prototype_queue.submit=slow_submit
        payload=json.dumps(dict(requestId='slow-synthetic',environment='staging',profile='treedar-synthetic-v1')).encode()
        admitted=[]
        worker=threading.Thread(target=lambda:admitted.append(self.request('/v1/prototype/jobs',payload)))
        worker.start();self.assertTrue(entered.wait(1))
        result=[]
        closer=threading.Thread(target=lambda:(result.append(self.request('/v1/prototype/admission-drain',operation,OP)),done.set()))
        closer.start();self.assertFalse(done.wait(.2));release.set();worker.join(2);closer.join(2)
        self.assertEqual(admitted[0][0],201)
        self.assertEqual(result[0][0],200);self.assertTrue(result[0][1]['closed'])

    def test_close_waits_for_inflight_reserved_cancel_and_blocks_followups(self):
        entered=threading.Event();release=threading.Event();closed=threading.Event()
        service=self.handler.viewer_service
        def slow_cancel(identifier,environment,capability):
            entered.set()
            if not release.wait(3):raise RuntimeError('test timeout')
            return {'uuid':identifier,'state':'cancelled'}
        identifier=str(uuid.uuid4());body=('uuid='+identifier).encode()
        headers={'Content-Type':'application/x-www-form-urlencoded','X-LedgeTop-Capability':'YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE'}
        url=self.origin+'/task/ledgetop/cancel?token='+VIEW
        results=[];failures=[]
        def cancel():
            try:
                with urlopen(Request(url,data=body,headers=headers),timeout=4) as response:results.append(response.status)
            except Exception as error:failures.append(error)
        def close():
            try:results.append(self.request('/v1/prototype/admission-drain',operation,OP)[0])
            except Exception as error:failures.append(error)
            finally:closed.set()
        operation=self.operation(True)
        with patch.object(service,'cancel_reserved',side_effect=slow_cancel) as action:
            worker=threading.Thread(target=cancel);closer=threading.Thread(target=close)
            try:
                worker.start();self.assertTrue(entered.wait(2));closer.start();self.assertFalse(closed.wait(.2))
                release.set();worker.join(3);closer.join(3)
                self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive())
                self.assertEqual(failures,[]);self.assertEqual(results,[200,200])
                self.assertEqual(self.request('/task/ledgetop/cancel?token='+VIEW,b'')[0],503)
                self.assertEqual(action.call_count,1)
            finally:
                release.set();worker.join(3)
                if closer.ident is not None:closer.join(3)

    def test_pending_close_rejects_new_upload_and_claim_while_existing_upload_finishes(self):
        from tests.test_viewer_http import multipart, BOUNDARY
        entered=threading.Event();release=threading.Event();closed=threading.Event()
        service=self.handler.viewer_service;identifier=str(uuid.uuid4());results=[];failures=[]
        job=json.dumps(dict(requestId='queued-pending-close',environment='staging',profile='treedar-synthetic-v1')).encode()
        self.assertEqual(self.request('/v1/prototype/jobs',job)[0],201)
        operation=self.operation(True)
        def slow_upload(*args):
            entered.set()
            if not release.wait(3):raise RuntimeError('test timeout')
            return {'success':True}
        body=multipart([('images','a.jpg',b'synthetic')]);url=self.origin+'/task/new/upload/'+identifier+'?token='+VIEW
        def upload():
            try:
                with urlopen(Request(url,data=body,headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY}),timeout=4) as response:results.append(response.status)
            except Exception as error:failures.append(error)
        def close():
            try:results.append(self.request('/v1/prototype/admission-drain',operation,OP)[0])
            except Exception as error:failures.append(error)
            finally:closed.set()
        with patch.object(service,'authorize'),patch.object(service,'upload',side_effect=slow_upload) as action:
            worker=threading.Thread(target=upload);closer=threading.Thread(target=close)
            try:
                worker.start();self.assertTrue(entered.wait(2));closer.start()
                self.assertTrue(self.handler.prototype_queue.admission_drain._closing.wait(2));self.assertFalse(closed.is_set())
                with self.assertRaises(HTTPError) as denied:
                    urlopen(Request(url,data=body,headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY}),timeout=2)
                self.assertEqual(denied.exception.code,503)
                claim=json.dumps({'workerId':'worker','nodeId':'cluster','profiles':['treedar-synthetic-v1']}).encode()
                self.assertIsNone(self.request('/v1/prototype/claim',claim,API)[1]['job'])
                self.assertEqual(action.call_count,1)
                release.set();worker.join(3);closer.join(3)
                self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertFalse(failures)
                self.assertEqual(results,[200,200])
                self.assertEqual(self.request('/v1/prototype/admission-drain',token=OP)[1]['closed'],True)
            finally:
                release.set();worker.join(3)
                if closer.ident is not None:closer.join(3)

    def test_close_waits_for_inflight_download_cache_and_blocks_new_download(self):
        entered=threading.Event();release=threading.Event();closed=threading.Event()
        service=self.handler.viewer_service;identifier=str(uuid.uuid4())
        def recover(identifier):
            entered.set()
            if not release.wait(3):raise RuntimeError('test timeout')
            return io.BytesIO(b'synthetic'),9
        url=self.origin+'/task/'+identifier+'/download/all.zip?token='+VIEW
        results=[];failures=[];operation=self.operation(True)
        def download():
            try:
                with urlopen(url,timeout=4) as response:results.append((response.status,response.read()))
            except Exception as error:failures.append(error)
        def close():
            try:results.append((self.request('/v1/prototype/admission-drain',operation,OP)[0],None))
            except Exception as error:failures.append(error)
            finally:closed.set()
        with patch.object(service,'authorize'),patch.object(service,'download',side_effect=recover) as action:
            worker=threading.Thread(target=download);closer=threading.Thread(target=close)
            try:
                worker.start();self.assertTrue(entered.wait(2));closer.start();self.assertFalse(closed.wait(.2))
                release.set();worker.join(3);closer.join(3)
                self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive());self.assertEqual(failures,[])
                self.assertCountEqual(results,[(200,b'synthetic'),(200,None)])
                self.assertEqual(self.request('/task/'+identifier+'/download/all.zip?token='+VIEW)[0],503)
                self.assertEqual(action.call_count,1)
            finally:
                release.set();worker.join(3)
                if closer.ident is not None:closer.join(3)

    def test_drain_finishes_running_job_but_prevents_next_claim(self):
        def payload(name):return json.dumps(dict(requestId=name,environment='staging',profile='treedar-synthetic-v1')).encode()
        first=self.request('/v1/prototype/jobs',payload('first'))[1]['job']
        self.request('/v1/prototype/jobs',payload('second'))
        claim_body=json.dumps({'workerId':'worker','profiles':['treedar-synthetic-v1'],'nodeId':'main'}).encode()
        running=self.request('/v1/prototype/claim',claim_body)[1]['job']
        self.assertEqual(running['id'],first['id'])
        self.assertEqual(self.request('/v1/prototype/admission-drain',self.operation(True),OP)[0],200)
        completion=json.dumps({'claimToken':running['claimToken'],'status':'succeeded','result':{}}).encode()
        self.assertEqual(self.request('/v1/prototype/jobs/'+first['id']+'/complete',completion)[0],200)
        self.assertIsNone(self.request('/v1/prototype/claim',claim_body)[1]['job'])
        self.assertEqual(self.request('/v1/prototype/admission-drain',self.operation(False),OP)[0],200)
        self.assertIsNotNone(self.request('/v1/prototype/claim',claim_body)[1]['job'])
