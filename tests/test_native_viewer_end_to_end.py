"""Synthetic real-HTTP Viewer/coordinator/two-engine workflow; no ODM work."""
import http.client
import io
import json
from pathlib import Path
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


class NativeViewerEndToEnd(unittest.TestCase):
    def test_real_http_priority_routing_restart_and_exact_result(self):
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
                    if not upload_release.wait(90):raise RuntimeError('fixture upload timeout')
                return super().upload(identifier,filename,stream,length)

        local=Engine(1000,3600);main=Engine(None,3600);fleet.extend((local,main))
        servers=[];engine_threads=[];stages=[];capture_thread=None
        with tempfile.TemporaryDirectory() as temporary:
            try:
                lease_dir=Path(temporary)/'leases';lease_dir.mkdir()
                for engine in fleet:
                    server=ThreadingHTTPServer(('127.0.0.1',0),handler_for(engine,node_token))
                    thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02})
                    servers.append(server);engine_threads.append(thread);thread.start()
                config=Config(api_token=api,local_node_url='http://127.0.0.1:'+str(servers[0].server_port),
                    main_node_url='http://127.0.0.1:'+str(servers[1].server_port),local_max_photos=1000,
                    local_max_input_bytes=20*1024**3,prototype_state_dir=str(Path(temporary)/'state'),
                    prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True,
                    viewer_token=viewer,viewer_staging_token=staging,local_node_token=node_token,main_node_token=node_token,
                    authority_lease_dir=str(lease_dir),viewer_archive_free_floor=1)
                seeded=Queue(config.prototype_state_dir,node_ids=('cluster','main'),treedar_node_ids=('main',),viewer_enabled=True)
                seeded.close()

                def start():
                    handler=make_handler(config);server=None
                    try:
                        server=ThreadingHTTPServer(('127.0.0.1',0),handler)
                        lifecycle=CoordinatorService(server,handler);stop=threading.Event();exits=[]
                        thread=threading.Thread(target=lambda:exits.append(lifecycle.run(stop)));thread.start()
                    except BaseException:
                        if server is not None:server.server_close()
                        handler.viewer_service.close();handler.prototype_queue.close();raise
                    stages.append((stop,thread,exits))
                    deadline=time.monotonic()+8
                    while not lifecycle.healthy():
                        if not thread.is_alive() or time.monotonic()>deadline:raise AssertionError('coordinator unavailable')
                        time.sleep(.02)
                    return handler,server.server_port

                def stop():
                    event,thread,exits=stages[-1];event.set();thread.join(10)
                    self.assertFalse(thread.is_alive());self.assertEqual(exits,[0])
                    # This in-process fixture explicitly closes the lease that
                    # a real coordinator process releases on process exit.
                    self.assertTrue(handler.prototype_queue._lock.closed)
                    handler.prototype_queue.authority_lease.close()

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

                def wait_task(handler,identifier,predicate,seconds=60):
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
                large=submit(port,viewer,1001)
                self.assertEqual(handler.viewer_service.store.task(waiting)['job']['status'],'queued')
                self.assertEqual(handler.viewer_service.store.task(large)['job']['status'],'queued')
                self.assertEqual((local.commits,main.commits),(1,0));self.assertEqual(overlaps,[])
                # The same engine task survives coordinator restart; no repeat commit.
                stop();handler,port=start()
                finish(local,first['engine_uuid'])
                wait_task(handler,running,lambda task:task['job']['status']=='succeeded')
                self.assertTrue(upload_entered.wait(30),'large job did not begin upload')
                # Run the actual archive child while the later HTTP upload is blocked.
                capture_errors=[]
                def capture():
                    try:handler.viewer_service._archive_once(running)
                    except Exception as error:capture_errors.append(error)
                capture_thread=threading.Thread(target=capture);capture_thread.start();capture_thread.join(15)
                self.assertFalse(capture_thread.is_alive(),'archive child blocked behind upload')
                self.assertEqual(capture_errors,[])
                self.assertEqual(request(port,'GET','/task/'+running+'/download/all.zip',staging),result_bytes)
                upload_release.set()
                second=wait_task(handler,large,lambda task:task['job']['status']=='running' and task['phase']=='processing',seconds=100)
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
                if capture_thread is not None:capture_thread.join(20)
                for event,_,_ in stages:event.set()
                for _,thread,_ in stages:thread.join(100)
                for server in servers:server.shutdown();server.server_close()
                for thread in engine_threads:thread.join(5)
                if ((capture_thread is not None and capture_thread.is_alive()) or
                        any(thread.is_alive() for _,thread,_ in stages) or any(thread.is_alive() for thread in engine_threads)):
                    raise AssertionError('synthetic workflow thread did not exit')
