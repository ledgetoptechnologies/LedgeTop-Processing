"""Small synthetic tests for disabled per-input physical reservation."""
import errno
import io
import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from processing import input_space
from processing import config as runtime_config
from processing.app import make_handler
from processing.config import Config
from processing.input_space import InputSpaceUnavailable
from processing.policy import plan
from processing.prototype import Queue
from processing.viewer_http import ViewerFacade
from processing.viewer_tasks import TaskStore, InputLimitExceeded
from tests.test_viewer_http import MemoryHandler, multipart, TOKEN, BOUNDARY

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'POSIX fallocate required')
class ViewerInputSpaceTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-input-reservation-')).resolve()
        self.addCleanup(lambda:print('retained synthetic input root:',self.root,flush=True))
        self.q=Queue(self.root/'queue');self.addCleanup(self.q.close)
        self.store=TaskStore(self.q,self.root/'queue'/'viewer-tasks',input_free_floor=0)
        self.identifier=str(uuid.uuid4());self.store.initialize(self.identifier,'synthetic',[],None,'production')
        self.spool=self.root/'spool';self.spool.mkdir(mode=0o700)
        store=self.store
        class Service:
            def __init__(self):self.store=store
            def upload(self,identifier,filename,stream,size):return self.store.upload(identifier,filename,stream,size)
        self.facade=ViewerFacade(Service(),TOKEN,temp_dir=self.spool,input_free_floor=0)
    def request(self,body):
        return MemoryHandler('/task/new/upload/'+self.identifier+'?token='+TOKEN,body,
            Content_Type='multipart/form-data; boundary='+BOUNDARY,Content_Length=str(len(body)))
    def test_real_http_boundary_reserves_spool_and_exact_destination(self):
        payload=b'small-synthetic-image';body=multipart([('images','image.jpg',payload)])
        req=self.request(body)
        with patch('processing.viewer_http.reserve_input',wraps=input_space.reserve_input) as spool, \
             patch('processing.input_space.reserve_input',wraps=input_space.reserve_input) as dest:
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,200);self.assertEqual(spool.call_count,1);self.assertEqual(dest.call_count,1)
        self.assertEqual(dest.call_args.args[1:],(len(payload),0))
        self.assertGreaterEqual(spool.call_args.args[1],len(payload))
        files=self.store.task(self.identifier)['files'];self.assertEqual(len(files),1)
        self.assertEqual(Path(files[0]['path']).read_bytes(),payload)
    def test_real_local_http_upload_uses_reserved_paths(self):
        facade=self.facade
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                if not facade.handle(self):self.send_error(404)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
        try:
            body=multipart([('images','real-http.jpg',b'synthetic-http')])
            with patch('processing.viewer_http.reserve_input',wraps=input_space.reserve_input) as spool, \
                 patch('processing.input_space.reserve_input',wraps=input_space.reserve_input) as dest:
                connection.request('POST','/task/new/upload/'+self.identifier+'?token='+TOKEN,body,
                                   {'Content-Type':'multipart/form-data; boundary='+BOUNDARY})
                response=connection.getresponse();self.assertEqual(response.status,200);response.read()
            self.assertEqual(spool.call_count,1);self.assertEqual(dest.call_count,1)
            files=self.store.task(self.identifier)['files'];self.assertEqual(Path(files[0]['path']).read_bytes(),b'synthetic-http')
        finally:
            connection.close();server.shutdown();server.server_close();thread.join(5)
            self.assertFalse(thread.is_alive())

    def test_spool_low_space_returns_503_without_destination_write(self):
        body=multipart([('images','image.jpg',b'x')]);req=self.request(body)
        with patch('processing.input_space.os.fstatvfs',return_value=SimpleNamespace(f_bavail=0,f_frsize=4096)):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(json.loads(req.wfile.getvalue())['error'],'insufficient_input_space')
        self.assertEqual(self.store.task(self.identifier)['files'],[])
    def test_destination_reservation_failure_retains_task_and_removes_only_owned_temp(self):
        body=multipart([('images','image.jpg',b'payload')]);req=self.request(body)
        with patch('processing.input_space.reserve_input',side_effect=InputSpaceUnavailable('synthetic')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(self.store.task(self.identifier)['files'],[])
        self.assertEqual(list(self.root.rglob('.incoming-*')),[])
    def test_fallocate_failure_is_not_treated_as_success(self):
        with tempfile.TemporaryFile(dir=self.spool) as output:
            with patch('processing.input_space.os.posix_fallocate',side_effect=OSError(errno.ENOSPC,'synthetic')):
                with self.assertRaises(InputSpaceUnavailable):input_space.reserve_input(output.fileno(),1,0)
    def test_default_task_limit_rejects_50_gib_before_reading(self):
        with self.assertRaises(InputLimitExceeded):self.store.upload(self.identifier,'image.jpg',io.BytesIO(b''),50*1024**3)
    def test_mismatched_facade_store_guard_is_rejected(self):
        with self.assertRaises(ValueError):ViewerFacade(SimpleNamespace(store=SimpleNamespace(input_free_floor=None)),TOKEN,temp_dir=self.spool,input_free_floor=0)

class InputGuardConfigurationTests(unittest.TestCase):
    def test_runtime_guard_cannot_be_enabled_without_qualification(self):
        config=Config('synthetic','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
            prototype_state_dir='/synthetic/not-used',viewer_enabled=True,authority_lease_dir='/synthetic/not-used',
            viewer_input_space_guard=True)
        with self.assertRaisesRegex(RuntimeError,'viewer_input_space_guard_not_qualified'):make_handler(config)
    def test_environment_mode_is_explicit_and_disabled(self):
        env={'PROCESSING_API_TOKEN':'synthetic-api-'+'a'*32,'PROCESSING_VIEWER_ENABLED':'1',
             'PROCESSING_VIEWER_INPUT_SPACE_GUARD':'1','PROCESSING_PROTOTYPE_STATE_DIR':'/synthetic/state',
             'PROCESSING_AUTHORITY_LEASE_DIR':'/synthetic/lease'}
        with patch.dict(os.environ,env,clear=True):
            with self.assertRaisesRegex(ValueError,'not qualified'):runtime_config.from_environment()
        with patch.dict(os.environ,{**env,'PROCESSING_AUTHORITY_LEASE_DIR':''},clear=True):
            with self.assertRaisesRegex(ValueError,'authority lease'):runtime_config.from_environment()

class LargeRoutingTests(unittest.TestCase):
    def test_ten_thousand_photos_route_main_but_config_limit_remains_20_gib(self):
        ready={'ready':True,'taskQueueCount':0,'state':'active','maxImages':None}
        config=Config('synthetic','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3)
        self.assertEqual(config.viewer_max_input_bytes,20*1024**3)
        self.assertEqual(plan(10000,50*1024**3,ready,ready,1000,20*1024**3)['decision'],'main')

if __name__=='__main__':unittest.main()
