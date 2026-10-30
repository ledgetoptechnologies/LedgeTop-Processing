"""Actual Viewer-compatible HTTP + coordinator recovery, synthetic engine only."""
import hashlib
import http.client
import io
import json
import multiprocessing
import os
from http.server import ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid
import zipfile

from processing.app import make_handler
from processing.config import Config
from processing.viewer_service import EngineUnavailable
from tests.test_viewer_http import BOUNDARY, TOKEN, multipart
from tests.test_viewer_service import FakeEngine, FakeFleet, OPTIONS


def synthetic_coordinator(root, control, offline):
    """Separate task-owned coordinator process; no external processing."""
    config=Config('test-api-token','http://127.0.0.1:19001','http://127.0.0.1:19002',
                  1000,20*1024**3,prototype_state_dir=root,
                  prototype_node_ids=('cluster','main'),viewer_enabled=True,viewer_token=TOKEN)
    handler=make_handler(config);service=handler.viewer_service
    fleet=FakeFleet();engines={name:FakeEngine(name,fleet) for name in ('cluster','main')}
    service.engines=engines
    service._catalog=lambda:{'ready':True,'commonOptions':[dict(x) for x in OPTIONS]}
    service._selected_options=lambda _node:[dict(x) for x in OPTIONS]
    service.probe=lambda role,origin,token:{'ready':True,'taskQueueCount':0,'maxImages':1000 if role=='local' else None}
    original=service.store.update
    def update(task_id, **fields):
        if not offline and 'archive_path' in fields:raise OSError('synthetic state write failure')
        return original(task_id,**fields)
    service.store.update=update
    if offline is True:
        def unavailable(_identifier):raise EngineUnavailable('synthetic offline')
        engines['cluster'].download=unavailable
    if offline == 'crash-link':
        original_link=os.link
        def legacy_crash_after_restore_link(source,destination):
            result=original_link(source,destination)
            if Path(destination).name=='all.zip' and Path(source).name.startswith('.all.zip.restore-'):os._exit(73)
            return result
        os.link=legacy_crash_after_restore_link
    server=ThreadingHTTPServer(('127.0.0.1',0),handler)
    thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));thread.start()
    try:
        control.send((os.getpid(),server.server_port,engines['cluster'].archive))
        while True:
            command,identifier=control.recv()
            if command=='stop':break
            if command=='tick':service.tick()
            elif command=='path':control.send(service.store.task(identifier)['archive_path']);continue
            elif command=='allow_archive_updates':service.store.update=original
            elif command=='delete_cached_archive':
                task=service.store.task(identifier);Path(task['archive_path']).unlink()
            elif command=='crash_after_restore_link':
                original_link=os.link
                def crash_after_restore(source,destination):
                    result=original_link(source,destination)
                    if Path(destination).name=='all.zip' and Path(source).name.startswith('.all.zip.restore-'):os._exit(75)
                    return result
                patch('processing.viewer_service.os.link',side_effect=crash_after_restore).start()
            elif command=='crash_after_archive_link':
                original_link=os.link
                def crash_after_link(source,destination):
                    result=original_link(source,destination)
                    if Path(destination).name=='all.zip':os._exit(74)
                    return result
                patch('processing.viewer_service.os.link',side_effect=crash_after_link).start()
            elif command=='finish':
                task=service.store.task(identifier);engines['cluster'].finish(task['engine_uuid']);service.tick()
            else:raise ValueError('unknown test command')
            control.send('ok')
    finally:
        server.shutdown();thread.join(3);server.server_close();service.close();handler.prototype_queue.close();control.close()


class ViewerArchiveHTTPRecoveryTests(unittest.TestCase):
    @unittest.skipUnless(os.name=='posix','POSIX crash and hard-link semantics')
    def test_restored_archive_survives_process_exit_immediately_after_link(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            active=[];identifier=str(uuid.uuid4())
            def launch(mode):
                parent,child=context.Pipe();process=context.Process(target=synthetic_coordinator,args=(root,child,mode))
                process.start();child.close();active.append((process,parent))
                self.assertTrue(parent.poll(5));pid,port,archive=parent.recv();return process,parent,port,archive
            def stop(process,parent):
                parent.send(('stop',None));process.join(5);self.assertFalse(process.is_alive());self.assertEqual(process.exitcode,0)
                parent.close();active.remove((process,parent))
            def request(port,path,body=None,headers=None):
                return urlopen(Request('http://127.0.0.1:'+str(port)+path+'?token='+TOKEN,data=body,headers=headers or {}),timeout=5)
            try:
                first,control,port,expected=launch('normal')
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request(port,'/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:json.load(response)
                upload=multipart([('images','photo.jpg',b'synthetic')])
                with request(port,'/task/new/upload/'+identifier,upload,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:json.load(response)
                with request(port,'/task/new/commit/'+identifier,b'') as response:json.load(response)
                for action in ('tick','finish'):
                    control.send((action,identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                with request(port,'/task/'+identifier+'/download/all.zip') as response:self.assertEqual(response.read(),expected)
                control.send(('path',identifier));self.assertTrue(control.poll(5));path=Path(control.recv())
                stop(first,control);path.unlink()
                crash,control,port,_=launch('crash-link')
                with self.assertRaises((OSError,HTTPError)):
                    with request(port,'/task/'+identifier+'/download/all.zip') as response:response.read()
                crash.join(5);self.assertFalse(crash.is_alive());self.assertEqual(crash.exitcode,73)
                control.close();active.remove((crash,control))
                self.assertEqual(path.read_bytes(),expected)
                orphans=list(path.parent.glob('.all.zip.restore-*'));self.assertEqual(len(orphans),1)
                second,control,port,_=launch(True)
                with request(port,'/task/'+identifier+'/download/all.zip') as response:self.assertEqual(response.read(),expected)
                self.assertTrue(orphans[0].exists());stop(second,control)
            finally:
                for process,parent in active:
                    if process.is_alive():process.terminate()
                    process.join(5);parent.close();self.assertFalse(process.is_alive())

    def test_recovery_across_distinct_coordinator_processes(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            active=[];identifier=str(uuid.uuid4())
            def launch(offline):
                parent,child=context.Pipe()
                process=context.Process(target=synthetic_coordinator,args=(root,child,offline));process.start();child.close()
                active.append((process,parent))
                self.assertTrue(parent.poll(5),'coordinator did not start')
                pid,port,archive=parent.recv();return process,parent,pid,port,archive
            def stop(process,parent):
                parent.send(('stop',None));process.join(5)
                self.assertFalse(process.is_alive());self.assertEqual(process.exitcode,0)
                parent.close();active.remove((process,parent))
            def request(port,path,body=None,headers=None):
                return urlopen(Request('http://127.0.0.1:'+str(port)+path+'?token='+TOKEN,
                                       data=body,headers=headers or {}),timeout=5)
            try:
                first,control,pid1,port,expected=launch(False)
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request(port,'/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:
                    self.assertEqual(json.load(response)['uuid'],identifier)
                upload=multipart([('images','photo.jpg',b'owned synthetic image bytes')])
                with request(port,'/task/new/upload/'+identifier,upload,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:
                    self.assertTrue(json.load(response)['success'])
                with request(port,'/task/new/commit/'+identifier,b'') as response:self.assertTrue(json.load(response)['success'])
                for command in ('tick','finish'):
                    control.send((command,identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                with self.assertRaises(HTTPError) as raised:request(port,'/task/'+identifier+'/download/all.zip')
                self.assertEqual(raised.exception.code,503);raised.exception.close()
                # Abruptly terminate only this fixture process after the
                # archive publication/state-write failure window.
                first.terminate();first.join(5);self.assertFalse(first.is_alive())
                control.close();active.remove((first,control))
                second,control,pid2,port,_=launch(True);self.assertNotEqual(pid1,pid2)
                with request(port,'/task/'+identifier+'/download/all.zip') as response:
                    self.assertEqual(response.status,200);self.assertEqual(response.headers['Content-Type'],'application/zip')
                    declared=int(response.headers['Content-Length']);actual=response.read()
                self.assertEqual(declared,len(expected));self.assertEqual(actual,expected)
                with zipfile.ZipFile(io.BytesIO(actual)) as archive:
                    self.assertEqual(archive.read('synthetic-result.txt'),b'owned synthetic transfer fixture\n')
                stop(second,control)
            finally:
                for process,parent in active:
                    if process.is_alive():process.terminate()
                    process.join(5);parent.close()
                    self.assertFalse(process.is_alive())
    def test_abrupt_process_exit_after_archive_link_recovers_with_node_offline(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            active=[];identifier=str(uuid.uuid4())
            def launch(offline):
                parent,child=context.Pipe();process=context.Process(target=synthetic_coordinator,args=(root,child,offline));process.start();child.close();active.append((process,parent))
                self.assertTrue(parent.poll(5));pid,port,archive=parent.recv();return process,parent,pid,port,archive
            def request(port,path,body=None,headers=None):
                return urlopen(Request('http://127.0.0.1:'+str(port)+path+'?token='+TOKEN,data=body,headers=headers or {}),timeout=5)
            try:
                first,control,pid1,port,expected=launch(False)
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request(port,'/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:self.assertEqual(response.status,200)
                upload=multipart([('images','photo.jpg',b'owned synthetic image bytes')])
                with request(port,'/task/new/upload/'+identifier,upload,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:self.assertEqual(response.status,200)
                with request(port,'/task/new/commit/'+identifier,b'') as response:self.assertEqual(response.status,200)
                for command in ('tick','finish','crash_after_archive_link'):
                    control.send((command,identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                with self.assertRaises((OSError,http.client.HTTPException)):
                    request(port,'/task/'+identifier+'/download/all.zip')
                first.join(5);self.assertFalse(first.is_alive());self.assertEqual(first.exitcode,74)
                control.close();active.remove((first,control))
                second,control,pid2,port,_=launch(True);self.assertNotEqual(pid1,pid2)
                with request(port,'/task/'+identifier+'/download/all.zip') as response:
                    self.assertEqual(response.status,200);actual=response.read()
                self.assertEqual(actual,expected);self.assertEqual(hashlib.sha256(actual).digest(),hashlib.sha256(expected).digest())
                control.send(('stop',None));second.join(5);self.assertFalse(second.is_alive());self.assertEqual(second.exitcode,0)
                control.close();active.remove((second,control))
            finally:
                for process,parent in active:
                    if process.is_alive():process.terminate()
                    process.join(5);parent.close();self.assertFalse(process.is_alive())

    def test_exit_after_restored_archive_link_survives_offline_restart(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as root:
            active=[];identifier=str(uuid.uuid4())
            def launch(offline):
                parent,child=context.Pipe();process=context.Process(target=synthetic_coordinator,args=(root,child,offline));process.start();child.close();active.append((process,parent))
                self.assertTrue(parent.poll(5));pid,port,archive=parent.recv();return process,parent,pid,port,archive
            def request(port,path,body=None,headers=None):
                return urlopen(Request('http://127.0.0.1:'+str(port)+path+'?token='+TOKEN,data=body,headers=headers or {}),timeout=5)
            try:
                first,control,pid1,port,expected=launch(False)
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request(port,'/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:self.assertEqual(response.status,200)
                upload=multipart([('images','photo.jpg',b'owned synthetic image bytes')])
                with request(port,'/task/new/upload/'+identifier,upload,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:self.assertEqual(response.status,200)
                with request(port,'/task/new/commit/'+identifier,b'') as response:self.assertEqual(response.status,200)
                for command in ('tick','finish'):
                    control.send((command,identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                # First GET establishes receipt and stored archive_path.
                # The fixture intentionally fails archive_path updates by default;
                # this scenario enables them explicitly first.
                control.send(('allow_archive_updates',identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                with request(port,'/task/'+identifier+'/download/all.zip') as response:self.assertEqual(response.read(),expected)
                for command in ('delete_cached_archive','crash_after_restore_link'):
                    control.send((command,identifier));self.assertTrue(control.poll(5));self.assertEqual(control.recv(),'ok')
                with self.assertRaises((OSError,http.client.HTTPException)):
                    request(port,'/task/'+identifier+'/download/all.zip')
                first.join(5);self.assertFalse(first.is_alive());self.assertEqual(first.exitcode,75)
                control.close();active.remove((first,control))
                self.assertTrue(list(Path(root).glob('viewer-tasks/*/.all.zip.restore-*')))
                second,control,pid2,port,_=launch(True);self.assertNotEqual(pid1,pid2)
                with request(port,'/task/'+identifier+'/download/all.zip') as response:
                    self.assertEqual(response.status,200);actual=response.read()
                self.assertEqual(actual,expected);self.assertEqual(hashlib.sha256(actual).digest(),hashlib.sha256(expected).digest())
                self.assertTrue(list(Path(root).glob('viewer-tasks/*/.all.zip.restore-*')))
                control.send(('stop',None));second.join(5);self.assertFalse(second.is_alive());self.assertEqual(second.exitcode,0)
                control.close();active.remove((second,control))
            finally:
                for process,parent in active:
                    if process.is_alive():process.terminate()
                    process.join(5);parent.close();self.assertFalse(process.is_alive())

    def test_published_archive_survives_restart_and_offline_node(self):
        with tempfile.TemporaryDirectory() as root:
            config=Config('test-api-token','http://127.0.0.1:19001','http://127.0.0.1:19002',
                          1000,20*1024**3,prototype_state_dir=root,
                          prototype_node_ids=('cluster','main'),viewer_enabled=True,viewer_token=TOKEN)
            fleet=FakeFleet();engines={name:FakeEngine(name,fleet) for name in ('cluster','main')}
            identifier=str(uuid.uuid4()); current=None

            def launch():
                handler=make_handler(config);service=handler.viewer_service
                service.engines=engines
                service._catalog=lambda:{'ready':True,'commonOptions':[dict(x) for x in OPTIONS]}
                service._selected_options=lambda _node:[dict(x) for x in OPTIONS]
                service.probe=lambda role,origin,token:{'ready':True,'taskQueueCount':0,'maxImages':1000 if role=='local' else None}
                server=ThreadingHTTPServer(('127.0.0.1',0),handler)
                thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));thread.start()
                return handler,service,server,thread

            def close(bundle):
                handler,service,server,thread=bundle
                server.shutdown();thread.join(3);self.assertFalse(thread.is_alive())
                server.server_close();service.close();handler.prototype_queue.close()

            def request(bundle,path,body=None,headers=None):
                url='http://127.0.0.1:'+str(bundle[2].server_port)+path+'?token='+TOKEN
                return urlopen(Request(url,data=body,headers=headers or {}),timeout=5)

            try:
                current=launch();service=current[1]
                init=multipart([('name',None,b'Synthetic survey'),('options',None,b'[]')])
                with request(current,'/task/new/init',init,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY,'set-uuid':identifier}) as response:
                    self.assertEqual(json.load(response)['uuid'],identifier)
                upload=multipart([('images','photo.jpg',b'owned synthetic image bytes')])
                with request(current,'/task/new/upload/'+identifier,upload,{'Content-Type':'multipart/form-data; boundary='+BOUNDARY}) as response:
                    self.assertTrue(json.load(response)['success'])
                with request(current,'/task/new/commit/'+identifier,b'') as response:
                    self.assertTrue(json.load(response)['success'])
                service.tick();task=service.store.task(identifier)
                self.assertEqual(task['node_id'],'cluster')
                engines['cluster'].finish(task['engine_uuid']);service.tick()
                expected=engines['cluster'].archive
                original=service.store.update
                def lose_receipt_commit(task_id, **fields):
                    if 'archive_path' in fields:raise OSError('synthetic database write failure')
                    return original(task_id,**fields)
                with patch.object(service.store,'update',side_effect=lose_receipt_commit):
                    with self.assertRaises(HTTPError) as raised:
                        request(current,'/task/'+identifier+'/download/all.zip')
                    self.assertEqual(raised.exception.code,503);raised.exception.close()
                task=service.store.task(identifier);self.assertIsNone(task['archive_path'])
                self.assertTrue((Path(task['directory'])/'all.zip').is_file())
                self.assertEqual(engines['cluster'].download_calls,1)
                close(current);current=None
                current=launch()
                with patch.object(engines['cluster'],'download',side_effect=EngineUnavailable('synthetic offline')):
                    with request(current,'/task/'+identifier+'/download/all.zip') as response:
                        self.assertEqual(response.status,200)
                        self.assertEqual(response.headers['Content-Type'],'application/zip')
                        self.assertEqual(int(response.headers['Content-Length']),len(expected))
                        actual=response.read()
                self.assertEqual(hashlib.sha256(actual).digest(),hashlib.sha256(expected).digest())
                self.assertEqual(actual,expected)
                with zipfile.ZipFile(io.BytesIO(actual)) as archive:
                    self.assertEqual(archive.read('synthetic-result.txt'),b'owned synthetic transfer fixture\n')
                self.assertEqual(engines['cluster'].download_calls,1)
                self.assertTrue(current[1].store.task(identifier)['archive_path'])
            finally:
                if current is not None:close(current)
