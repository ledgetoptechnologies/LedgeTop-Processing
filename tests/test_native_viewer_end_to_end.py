"""Synthetic real-HTTP Viewer/coordinator/two-engine workflow; no ODM work."""
import http.client
import io
import json
import os
from contextlib import contextmanager
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
import zipfile
from http.server import ThreadingHTTPServer

from processing.app import CoordinatorService, make_handler
from processing.config import Config
from processing.prototype import Queue
from processing.synthetic_engine import SyntheticEngine, handler_for
from tests.test_viewer_http import BOUNDARY, multipart


@contextmanager
def retained_synthetic_root():
    # Do not recursively delete a synthetic fixture when ownership is uncertain.
    yield tempfile.mkdtemp(prefix='lt-native-queue-http-')


class NativeViewerEndToEnd(unittest.TestCase):
    def test_real_http_priority_routing_restart_and_exact_result(self):
        if os.environ.get('LT_SYNTHETIC_QUEUE_CHILD') != '1':
            env=dict(os.environ,LT_SYNTHETIC_QUEUE_CHILD='1')
            # This fixture does not enable autocapture or spawn archive workers.
            # The explicitly owned child contains all of its HTTP threads.
            result=subprocess.run([sys.executable,'-m','unittest',
                'tests.test_native_viewer_end_to_end.NativeViewerEndToEnd.test_real_http_priority_routing_restart_and_exact_result'],
                env=env,text=True,capture_output=True,timeout=45)
            self.assertEqual(result.returncode,0,result.stderr[-3000:])
            return
        viewer='synthetic-production-'+'p'*32
        staging='synthetic-staging-'+'s'*32
        node_token='synthetic-node-'+'n'*32
        api='synthetic-api-'+'a'*32
        out=io.BytesIO()
        with zipfile.ZipFile(out,'w') as archive:
            archive.writestr(zipfile.ZipInfo('SYNTHETIC.txt',(2020,1,1,0,0,0)),b'not a real ODM product')
        result_bytes=out.getvalue();fleet_lock=threading.RLock();fleet=[];overlaps=[]
        upload_entered=threading.Event();upload_release=threading.Event()

        class Engine(SyntheticEngine):
            def commit(self,identifier):
                with fleet_lock:
                    if any(t['code']==20 for engine in fleet for t in engine.tasks.values()):
                        overlaps.append(identifier)
                        raise RuntimeError('synthetic global overlap')
                    return super().commit(identifier)
            def download(self,identifier):
                if self.info(identifier)['status']['code']!=40:raise RuntimeError('unfinished')
                return io.BytesIO(result_bytes),len(result_bytes)
            def upload(self,identifier,filename,stream,length):
                if self is main and not upload_entered.is_set():
                    upload_entered.set()
                    if not upload_release.wait(15):raise RuntimeError('fixture upload timeout')
                return super().upload(identifier,filename,stream,length)

        local=Engine(2,3600);main=Engine(None,3600);fleet.extend((local,main))
        servers=[];engine_threads=[];stages=[];cleanup_errors=[]
        def attempt(label,action):
            try:action()
            except BaseException as error:cleanup_errors.append((label,type(error).__name__))
        with retained_synthetic_root() as temporary:
            try:
                lease_dir=Path(temporary)/'leases';lease_dir.mkdir()
                for engine in fleet:
                    BaseHandler=handler_for(engine,node_token)
                    class BoundedHandler(BaseHandler):
                        def setup(self):
                            super().setup();self.connection.settimeout(10)
                    server=ThreadingHTTPServer(('127.0.0.1',0),BoundedHandler)
                    server.daemon_threads=False;server.block_on_close=True
                    servers.append(server)
                    thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02})
                    engine_threads.append(thread);thread.start()
                config=Config(api_token=api,local_node_url='http://127.0.0.1:'+str(servers[0].server_port),
                    main_node_url='http://127.0.0.1:'+str(servers[1].server_port),local_max_photos=2,
                    local_max_input_bytes=20*1024**3,prototype_state_dir=str(Path(temporary)/'state'),
                    prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True,
                    viewer_token=viewer,viewer_staging_token=staging,local_node_token=node_token,main_node_token=node_token,
                    authority_lease_dir=str(lease_dir),viewer_archive_free_floor=1)
                seeded=Queue(config.prototype_state_dir,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
                try: pass
                finally: seeded.close()

                def start():
                    handler=make_handler(config);server=None
                    try:
                        server=ThreadingHTTPServer(('127.0.0.1',0),handler)
                        lifecycle=CoordinatorService(server,handler);stop=threading.Event();exits=[]
                        thread=threading.Thread(target=lambda:exits.append(lifecycle.run(stop)));thread.start()
                    except BaseException:
                        if server is not None:attempt('start-server-close',server.server_close)
                        attempt('start-viewer-close',handler.viewer_service.close)
                        attempt('start-queue-close',handler.prototype_queue.close)
                        if handler.prototype_queue._lock.closed:
                            attempt('start-authority-close',handler.prototype_queue.authority_lease.close)
                        raise
                    stages.append((stop,thread,exits,handler,server))
                    if os.environ.get('LT_SYNTHETIC_QUEUE_FAIL_AFTER_START') == '1':
                        raise RuntimeError('synthetic setup failure')
                    deadline=time.monotonic()+8
                    while not lifecycle.healthy():
                        if not thread.is_alive() or time.monotonic()>deadline:raise AssertionError('coordinator unavailable')
                        time.sleep(.02)
                    return handler,server.server_port

                def stop():
                    event,thread,exits,owned_handler,_=stages[-1];event.set();thread.join(10)
                    self.assertFalse(thread.is_alive());self.assertEqual(exits,[0])
                    # This in-process fixture explicitly closes the lease that
                    # a real coordinator process releases on process exit.
                    self.assertTrue(owned_handler.prototype_queue._lock.closed)
                    owned_handler.prototype_queue.authority_lease.close()

                def request(port,method,path,token,body=None,headers=None):
                    connection=http.client.HTTPConnection('127.0.0.1',port,timeout=10)
                    try:
                        connection.request(method,path+'?token='+token,body=body,headers=headers or {})
                        response=connection.getresponse();data=response.read()
                        if response.status!=200:raise AssertionError('synthetic HTTP status '+str(response.status))
                        return data
                    finally:connection.close()

                def submit(port,token,count):
                    identifier=str(uuid.uuid4())
                    init=multipart([('name',None,b'synthetic'),('options',None,b'[]')])
                    headers={'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}
                    request(port,'POST','/task/new/init',token,init,headers)
                    for start_index in range(0,count,50):
                        parts=[('images','image-%04d.jpg'%index,b'x') for index in range(start_index,min(count,start_index+50))]
                        request(port,'POST','/task/new/upload/'+identifier,token,multipart(parts),
                                {'Content-Type':'multipart/form-data; boundary='+BOUNDARY})
                    request(port,'POST','/task/new/commit/'+identifier,token,b'')
                    return identifier

                def wait_task(handler,identifier,predicate,seconds=15):
                    end=time.monotonic()+seconds
                    while time.monotonic()<end:
                        task=handler.viewer_service.store.task(identifier)
                        if predicate(task):return task
                        time.sleep(.05)
                    raise AssertionError('synthetic task did not reach expected state')

                def finish(engine,engine_uuid):
                    with fleet_lock,engine.lock:engine.tasks[engine_uuid]['code']=40

                handler,port=start()
                running=submit(port,staging,1)
                first=wait_task(handler,running,lambda task:task['job']['status']=='running' and task['phase']=='processing')
                self.assertEqual(first['node_id'],'cluster')
                waiting=submit(port,staging,1)
                large=submit(port,viewer,3)
                self.assertEqual(handler.viewer_service.store.task(waiting)['job']['status'],'queued')
                self.assertEqual(handler.viewer_service.store.task(large)['job']['status'],'queued')
                self.assertEqual((local.commits,main.commits),(1,0));self.assertEqual(overlaps,[])
                # The same engine task survives coordinator restart; no repeat commit.
                stop();handler,port=start()
                finish(local,first['engine_uuid'])
                wait_task(handler,running,lambda task:task['job']['status']=='succeeded')
                self.assertTrue(upload_entered.wait(15),'large job did not begin upload')
                # Fetch an already-finished synthetic ZIP while the later
                # upload is blocked, without starting an archive child process.
                self.assertEqual(request(port,'GET','/task/'+running+'/download/all.zip',staging),result_bytes)
                upload_release.set()
                second=wait_task(handler,large,lambda task:task['job']['status']=='running' and task['phase']=='processing',seconds=15)
                self.assertEqual(second['node_id'],'main')
                self.assertEqual(handler.viewer_service.store.task(waiting)['job']['status'],'queued')
                self.assertEqual((local.commits,main.commits),(1,1));self.assertEqual(overlaps,[])
                finish(main,second['engine_uuid'])
                wait_task(handler,large,lambda task:task['job']['status']=='succeeded')
                self.assertEqual(request(port,'GET','/task/'+large+'/download/all.zip',viewer),result_bytes)
                third=wait_task(handler,waiting,lambda task:task['job']['status']=='running' and task['phase']=='processing')
                self.assertEqual(third['node_id'],'cluster');self.assertEqual((local.commits,main.commits),(2,1))
                finish(local,third['engine_uuid'])
                wait_task(handler,waiting,lambda task:task['job']['status']=='succeeded')
                self.assertEqual(request(port,'GET','/task/'+waiting+'/download/all.zip',staging),result_bytes)
                self.assertEqual(overlaps,[])
                stop()
            finally:
                upload_release.set()
                for event,_,_,_,_ in stages:attempt('stop-event',event.set)
                for _,thread,_,owned_handler,owned_server in stages:
                    attempt('coordinator-join',lambda thread=thread:thread.join(10))
                    if not thread.is_alive():
                        # Release only this fixture's lease after all its actual
                        # coordinator/service threads and HTTP handlers exited.
                        service_thread=getattr(owned_handler.viewer_service,'_thread',None)
                        handlers=getattr(owned_server,'_threads',())
                        handlers=list(handlers) if isinstance(handlers,list) else []
                        if (service_thread is None or not service_thread.is_alive()) and not any(t.is_alive() for t in handlers):
                            attempt('queue-close',owned_handler.prototype_queue.close)
                            if owned_handler.prototype_queue._lock.closed:
                                attempt('authority-close',owned_handler.prototype_queue.authority_lease.close)
                            if owned_handler.prototype_queue.authority_lease._descriptor is not None:
                                cleanup_errors.append(('authority-still-open','inspection'))
                        else:
                            cleanup_errors.append(('owned-service-still-running','inspection'))
                for index,server in enumerate(servers):
                    if index<len(engine_threads) and engine_threads[index].is_alive():
                        attempt('engine-shutdown',server.shutdown)
                    attempt('engine-close',server.server_close)
                for thread in engine_threads:attempt('engine-join',lambda thread=thread:thread.join(5))
                if (cleanup_errors or any(thread.is_alive() for _,thread,_,_,_ in stages)
                        or any(thread.is_alive() for thread in engine_threads)):
                    raise AssertionError('synthetic workflow thread did not exit')
                print('SYNTHETIC_FIXTURE_CLEANUP_COMPLETE')

    def test_setup_failure_releases_owned_fixture_resources(self):
        env=dict(os.environ,LT_SYNTHETIC_QUEUE_CHILD='1',LT_SYNTHETIC_QUEUE_FAIL_AFTER_START='1')
        result=subprocess.run([sys.executable,'-m','unittest',
            'tests.test_native_viewer_end_to_end.NativeViewerEndToEnd.test_real_http_priority_routing_restart_and_exact_result'],
            env=env,text=True,capture_output=True,timeout=25)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('synthetic setup failure',result.stderr)
        self.assertIn('SYNTHETIC_FIXTURE_CLEANUP_COMPLETE',result.stdout)
