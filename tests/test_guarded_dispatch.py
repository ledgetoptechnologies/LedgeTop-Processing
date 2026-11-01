"""Synthetic in-process dispatcher→guard tests; no production topology."""
import io
import json
import hashlib
from pathlib import Path
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from types import SimpleNamespace
from dataclasses import replace
import unittest
from unittest.mock import patch
import uuid

from processing import engine_upload_contract,guarded_native_engine
from processing.engine_ingress_guard import BackendPin,Binding,EngineIngressGuard,OwnerPin
from processing.maintenance import Conflict
from processing.odx_smoke_worker import SmokeError
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore


class GuardedDispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name).resolve()
        self.paths=patch('processing.node_gate.canonical_gate_path',side_effect=lambda engine:self.root/('gate-'+engine));self.paths.start()
        self.flags=[patch.object(engine_upload_contract,'QUALIFIED',True),patch.object(guarded_native_engine,'QUALIFIED',True)]
        for flag in self.flags:flag.start()
        self.calls=[];self.bodies=[];self.large_wire=None;self.drop=False;self.drop_init=False;self.drop_commit=False;self.bad_init=False;self.bad_commit=False;self.early_upload=False;fixture=self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length=int(self.headers['Content-Length']);route=self.path.split('?')[0]
                if fixture.early_upload and '/upload/' in route:
                    self.rfile.read(1);fixture.calls.append(route);data=b'{"success":true}'
                    try:self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data);self.wfile.flush()
                    except OSError:pass
                    self.close_connection=True;return
                if length>1024*1024:
                    digest=hashlib.sha256();remaining=length
                    while remaining:
                        chunk=self.rfile.read(min(65536,remaining))
                        if not chunk:break
                        digest.update(chunk);remaining-=len(chunk)
                    fixture.large_wire=(length,remaining,digest.hexdigest());body=b'<bounded-large>'
                else:body=self.rfile.read(length)
                fixture.calls.append(route);fixture.bodies.append(body)
                if fixture.drop and '/upload/' in route:
                    self.close_connection=True;return
                if (fixture.drop_init and route=='/task/new/init') or (fixture.drop_commit and '/commit/' in route):
                    self.close_connection=True;return
                if route=='/task/new/init':value={'uuid':self.headers['Set-UUID'],**({'success':False} if fixture.bad_init else {})}
                elif '/commit/' in route:value={'uuid':str(uuid.uuid4()) if fixture.bad_commit else route.rsplit('/',1)[1],**({'success':True} if fixture.bad_commit else {})}
                else:value={'success':True}
                data=json.dumps(value).encode()
                try:self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
                except (BrokenPipeError,ConnectionResetError):pass
            def do_GET(self):
                route=self.path.split('?')[0];fixture.calls.append(route)
                identifier=route.split('/')[2]
                data=json.dumps({'uuid':identifier,'status':{'code':10},'progress':0}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
            def log_message(self,*_):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=False
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.01));self.thread.start()
        self.config=SimpleNamespace(prototype_state_dir=str(self.root/'state'),viewer_max_input_bytes=20*1024**3,
            viewer_max_archive_bytes=1024*1024,local_node_url='http://synthetic.invalid',local_node_token='',
            main_node_url='http://synthetic.invalid',main_node_token='',local_max_photos=1000,local_max_input_bytes=20*1024**3)
        self.create_service()

    def create_service(self):
        self.queue=Queue(self.root/'state',node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
        store=TaskStore(self.queue,self.root/'state'/'viewer-tasks');pin=OwnerPin.capture(self.queue);guards={}
        for node in ('cluster','main'):
            binding=Binding('engine-'+node,node,self.queue.authority_id,'http://127.0.0.1:18080',1,self.server.server_port,
                'coordinator-'+'a'*40,'management-'+'b'*40,'backend-'+'c'*40,deadline_seconds=10)
            root=self.root/('gate-'+binding.engine_id)
            if not (root/'gate.json').exists():
                with NodeGate(binding.engine_id) as gate:gate.provision(node,binding.coordinator_origin,self.queue.authority_id)
            guards[node]=EngineIngressGuard(self.queue,binding,owner_pin=pin,backend_pin=BackendPin("a"*64,"sha256:"+"b"*64,binding.backend_port), backend_probe=lambda *_:None,engine_probe=lambda _,eid=binding.engine_id:eid)
        self.service=ViewerService(self.queue,self.config,store=store,guarded_engines=guards,
                                   probe=lambda *args:{'ready':True,'maxImages':None,'taskQueueCount':0})
        self.service._catalog=lambda:{'ready':True,'commonOptions':[]}

    def tearDown(self):
        self.service.close();self.queue.close();self.server.shutdown();self.server.server_close();self.thread.join(5)
        self.assertFalse(self.thread.is_alive());self.paths.stop()
        for flag in self.flags:flag.stop()
        self.tmp.cleanup()

    def submit(self):
        identifier=str(uuid.uuid4());self.service.initialize(identifier,'Synthetic',[],None,'production')
        self.service.upload(identifier,'photo.jpg',io.BytesIO(b'synthetic-photo'),15);self.service.commit(identifier)
        return identifier

    def test_actual_dispatcher_uses_guarded_file_and_commit(self):
        identifier=self.submit();self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual((task['node_id'],task['phase'],task['job']['status']),('cluster','processing','running'))
        self.assertEqual(len(self.calls),3);self.assertEqual(self.calls[0],'/task/new/init')
        self.assertIn('/upload/',self.calls[1]);self.assertIn('/commit/',self.calls[2])
        self.assertIn(b'filename="photo.jpg"',self.bodies[1]);self.assertIn(b'synthetic-photo',self.bodies[1])
        with self.queue._db() as db:
            row=db.execute('SELECT generation,name_key,state FROM viewer_engine_upload_intents').fetchone()
            self.assertEqual(tuple(row),(1,'photo.jpg','complete'))
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'complete')

    def test_lost_upload_reply_holds_no_retry_after_restart(self):
        identifier=self.submit();self.drop=True;self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual(task['job']['status'],'held');self.assertEqual(task['phase'],'held')
        self.assertEqual(len(self.calls),2)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        self.service.tick();self.assertEqual(len(self.calls),2)
        self.service.close();self.queue.close();self.create_service();self.service.tick()
        self.assertEqual(len(self.calls),2);self.assertEqual(self.service.store.task(identifier)['job']['status'],'held')

    def test_direct_unguarded_construction_refuses_before_store_creation(self):
        with patch.object(guarded_native_engine,'QUALIFIED',False):
            with self.assertRaises(Conflict):ViewerService(self.queue,self.config,guarded_engines={})

    def test_lost_init_reply_holds_without_upload_or_retry(self):
        identifier=self.submit();self.drop_init=True;self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual(task['job']['status'],'held');self.assertEqual(task['phase'],'held')
        self.assertEqual(self.calls,['/task/new/init']);self.service.tick();self.assertEqual(self.calls,['/task/new/init'])
        with NodeGate('engine-cluster') as gate:self.assertIsNotNone(gate.snapshot()['reservation'])

    def test_contradictory_init_reply_holds(self):
        identifier=self.submit();self.bad_init=True;self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual((task['job']['status'],task['phase']),('held','held'));self.assertEqual(self.calls,['/task/new/init'])

    def test_lost_commit_reply_keeps_slot_and_never_replays_commit(self):
        identifier=self.submit();self.drop_commit=True;self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual((task['phase'],task['job']['status']),('commit_pending','running'))
        self.assertEqual(sum('/commit/' in route for route in self.calls),1)
        self.service.tick();self.service.tick()
        self.assertEqual(sum('/commit/' in route for route in self.calls),1)
        self.assertEqual(self.service.store.task(identifier)['job']['status'],'running')

    def test_contradictory_commit_reply_remains_uncertain(self):
        identifier=self.submit();self.bad_commit=True;self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual((task['phase'],task['job']['status']),('commit_pending','running'))
        self.service.tick();self.assertEqual(sum('/commit/' in route for route in self.calls),1)

    def test_changed_source_bytes_hold_after_possible_engine_transfer(self):
        identifier=self.submit();path=Path(self.service.store.task(identifier)['files'][0]['path'])
        path.write_bytes(b'changed--photo!');self.service.tick();task=self.service.store.task(identifier)
        self.assertEqual((task['job']['status'],task['phase']),('held','held'))
        self.assertEqual(path.read_bytes(),b'changed--photo!')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')

    def test_source_rejects_named_inode_swap_during_read(self):
        identifier=self.submit();item=self.service.store.task(identifier)['files'][0];path=Path(item['path'])
        retained=path.with_name(path.name+'.retained');original=guarded_native_engine._read_source;swapped=[]
        def replace_after_read(fd,size):
            data=original(fd,size)
            if data and not swapped:
                path.rename(retained);path.write_bytes(data);swapped.append(True)
            return data
        with patch('processing.guarded_native_engine._read_source',side_effect=replace_after_read):self.service.tick()
        self.assertEqual(self.service.store.task(identifier)['job']['status'],'held')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        self.assertTrue(swapped);self.assertEqual(retained.read_bytes(),b'synthetic-photo');self.assertEqual(path.read_bytes(),b'synthetic-photo')

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'mkfifo'),'POSIX FIFO required')
    def test_source_refuses_fifo_without_waiting_for_writer(self):
        path=self.root/'synthetic-fifo';os.mkfifo(path)
        script=('import json,sys\nfrom processing.guarded_native_engine import GuardedNativeEngine\n'
                'from processing.odx_smoke_worker import SmokeError\n'
                'try: GuardedNativeEngine._open_source(json.loads(sys.stdin.read()))\n'
                'except SmokeError: print("held")\n'
                'else: raise SystemExit(2)\n')
        result=subprocess.run([sys.executable,'-c',script],input=json.dumps({'path':str(path),'bytes':1,'sha256':'a'*64}),
                              capture_output=True,text=True,timeout=3)
        self.assertEqual((result.returncode,result.stdout.strip()),(0,'held'),result.stderr)

    def test_file_intent_requires_dispatcher_generation(self):
        identifier=self.submit()
        job=self.queue.claim({'workerId':'viewer-dispatch','nodeId':'cluster','profiles':['odm-viewer-v1']})
        self.service.store.update(identifier,stage='running',phase='upload_pending',node_id='cluster',
                                  engine_uuid=str(uuid.uuid4()),claim_token=job['claimToken'])
        with self.assertRaises(Conflict):
            self.service.store.prepare_guarded_engine_upload(job['id'],job['claimToken'],'photo.jpg',str(uuid.uuid4()),expected_generation=2)
        with self.queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='viewer_engine_upload_intents'").fetchone())

    def test_large_file_streams_in_bounded_chunks(self):
        size=17*1024*1024+19
        class Synthetic:
            def __init__(self):self.remaining=size
            def read(self,n):
                count=min(n,self.remaining);self.remaining-=count;return b'x'*count
        identifier=str(uuid.uuid4());self.service.initialize(identifier,'Synthetic',[],None,'production')
        self.service.upload(identifier,'large.jpg',Synthetic(),size);self.service.commit(identifier)
        requests=[];original=guarded_native_engine._read_source
        def read(fd,count):requests.append(count);return original(fd,count)
        with patch('processing.guarded_native_engine._read_source',side_effect=read):self.service.tick()
        task=self.service.store.task(identifier)
        self.assertEqual((task['job']['status'],task['phase']),('running','processing'))
        self.assertTrue(requests);self.assertLessEqual(max(requests),65536)
        with self.queue._db() as db:
            row=db.execute('SELECT upload_id,bytes,sha256,state FROM viewer_engine_upload_intents').fetchone()
            self.assertEqual((row['bytes'],row['state']),(size,'complete'));upload=row['upload_id']
        boundary='processing-'+uuid.UUID(upload).hex
        prefix=(f'--{boundary}\r\nContent-Disposition: form-data; name="images"; filename="large.jpg"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()
        suffix=f'\r\n--{boundary}--\r\n'.encode();digest=hashlib.sha256(prefix);remaining=size
        while remaining:
            count=min(65536,remaining);digest.update(b'x'*count);remaining-=count
        digest.update(suffix)
        self.assertEqual(self.large_wire,(len(prefix)+size+len(suffix),0,digest.hexdigest()))

    def test_slow_source_deadline_holds_and_stops_sender(self):
        identifier=self.submit();engine=self.service.engines['cluster']
        engine.guard.binding=replace(engine.guard.binding,deadline_seconds=.15)
        original=guarded_native_engine._read_source;first=[]
        def slow(fd,count):
            if not first:first.append(True);time.sleep(.35)
            return original(fd,count)
        with patch('processing.guarded_native_engine._read_source',side_effect=slow):self.service.tick()
        self.assertTrue(first);task=self.service.store.task(identifier)
        self.assertEqual((task['job']['status'],task['phase']),('held','held'))
        self.assertEqual(engine._unreaped,[])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')

    def test_source_short_read_holds_without_retry(self):
        identifier=self.submit();engine=self.service.engines['cluster']
        with patch('processing.guarded_native_engine._read_source',return_value=b''):self.service.tick()
        self.assertEqual(self.service.store.task(identifier)['job']['status'],'held')
        self.assertEqual(engine._unreaped,[])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')
        calls=len(self.calls);self.service.tick();self.assertEqual(len(self.calls),calls)

    def test_unexpected_final_read_failure_holds(self):
        identifier=self.submit();original=guarded_native_engine._read_source
        def read(fd,count):
            if count==1:raise RuntimeError('injected final read failure')
            return original(fd,count)
        with patch('processing.guarded_native_engine._read_source',side_effect=read):self.service.tick()
        self.assertEqual(self.service.store.task(identifier)['job']['status'],'held')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')

    def test_early_backend_response_holds_and_stops_sender(self):
        size=17*1024*1024
        class Synthetic:
            def __init__(self):self.remaining=size
            def read(self,n):
                count=min(n,self.remaining);self.remaining-=count;return b'x'*count
        identifier=str(uuid.uuid4());self.service.initialize(identifier,'Synthetic',[],None,'production')
        self.service.upload(identifier,'large.jpg',Synthetic(),size);self.service.commit(identifier)
        self.early_upload=True;engine=self.service.engines['cluster'];self.service.tick()
        self.assertEqual(self.service.store.task(identifier)['job']['status'],'held')
        self.assertEqual(engine._unreaped,[])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'started')
            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads').fetchone()[0],'started')

    def test_thread_start_failure_closes_owned_fds(self):
        identifier=self.submit();job=self.queue.claim({'workerId':'viewer-dispatch','nodeId':'cluster','profiles':['odm-viewer-v1']})
        self.service.store.update(identifier,stage='running',phase='init_pending',node_id='cluster',engine_uuid=str(uuid.uuid4()),claim_token=job['claimToken'])
        engine=self.service.engines['cluster'];task=self.service.store.task(identifier);engine.initialize(task)
        self.service.store.update(identifier,phase='upload_pending');item=self.service.store.task(identifier)['files'][0]
        opened=[];sockets=[];open_source=engine._open_source;socketpair=socket.socketpair
        def capture_open(value):
            result=open_source(value);opened.append(result[0]);return result
        def capture_pair():
            result=socketpair();sockets.extend(result);return result
        with patch.object(engine,'_open_source',side_effect=capture_open),patch('processing.guarded_native_engine.socket.socketpair',side_effect=capture_pair),patch('processing.guarded_native_engine.threading.Thread.start',side_effect=RuntimeError('injected')):
            with self.assertRaises(RuntimeError):engine.upload(task['engine_uuid'],item)
        self.assertEqual(len(opened),1);self.assertEqual([sock.fileno() for sock in sockets],[-1,-1])
        with self.assertRaises(OSError):os.fstat(opened[0])
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM viewer_engine_upload_intents').fetchone()[0],'prepared')
