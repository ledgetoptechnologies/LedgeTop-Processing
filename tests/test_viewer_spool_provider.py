import http.client
import json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import threading
import unittest
from urllib.parse import quote

from ops.reference_worker_daemon import DaemonStateLock
from processing.config import Config
from processing.viewer_http import ViewerFacade
from processing.viewer_local_result import send_archive
from processing.viewer_service import ViewerService
from tests.test_viewer_archive_spool import ViewerArchiveSpoolTests


class ViewerSpoolProviderTests(unittest.TestCase):
    def setUp(self):
        self.base=ViewerArchiveSpoolTests('test_actual_node_sender_spools_and_replays_receipt')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        with DaemonStateLock(self.base.base.base.base.base.worker) as owner:
            worker=self.base.base.worker(owner);send_archive(worker,synthetic_fixture=True)
        self.fixture=self.base.fixture;queue=self.fixture.queue
        config=Config('legacy-token','http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,
                      prototype_state_dir=str(self.fixture.root/'state'),viewer_enabled=True)
        self.service=ViewerService(queue,config,store=self.fixture.store,engines={},viewer_spool=self.base.spool)
        facade=ViewerFacade(self.service,'viewer-production-token','viewer-staging-token')
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not facade.handle(self):self.send_error(404)
            def log_message(self,*args):pass
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.addCleanup(self.close)

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5)
        self.assertFalse(self.thread.is_alive());self.service.close()

    def get(self,path,token='viewer-production-token'):
        connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        try:
            connection.request('GET',path+'?token='+quote(token));response=connection.getresponse()
            return response.status,dict(response.getheaders()),response.read()
        finally:connection.close()

    def test_authenticated_provider_download_keeps_running_and_retains_data(self):
        self.fixture.stop_server() # coordinator worker TLS offline; local Viewer endpoint stays up
        path='/task/22222222-2222-4222-8222-222222222222'
        status,_,body=self.get(path+'/info');self.assertEqual(status,200)
        self.assertNotEqual(json.loads(body)['status']['code'],40)
        status,headers,body=self.get(path+'/download/all.zip');self.assertEqual(status,200)
        self.assertEqual(body,self.base.base.base.archive)
        self.assertEqual(headers['X-LedgeTop-Archive-SHA256'],self.base.base.record['archiveSha256'])
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)
        self.assertEqual(len(list(self.base.base.base.base.inputs.glob('*.alloc'))),1)

    def test_wrong_credentials_environment_and_generation_are_rejected(self):
        path='/task/22222222-2222-4222-8222-222222222222/download/all.zip'
        self.assertEqual(self.get(path,'wrong')[0],401)
        self.assertEqual(self.get(path,'viewer-staging-token')[0],404)
        self.assertEqual(self.get('/task/33333333-3333-4333-8333-333333333333/download/all.zip')[0],404)
        with self.fixture.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET generation=generation+1 WHERE uuid='22222222-2222-4222-8222-222222222222'")
        self.assertEqual(self.get(path)[0],409)

    def test_reader_close_before_eof_does_not_mark_acceptance(self):
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        reader,length=self.base.spool.open_for_viewer(task)
        try:self.assertTrue(reader.read(min(8,length)))
        finally:reader.close()
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_partial_http_consumer_does_not_remove_retained_archive(self):
        path='/task/22222222-2222-4222-8222-222222222222/download/all.zip?token=viewer-production-token'
        connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=5)
        try:
            connection.request('GET',path);response=connection.getresponse()
            self.assertEqual(response.status,200);self.assertTrue(response.read(8))
        finally:connection.close()
        self.assertEqual(self.get(path.split('?')[0])[2],self.base.base.base.archive)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_task_change_during_read_aborts_stream(self):
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        reader,_=self.base.spool.open_for_viewer(task)
        try:
            self.assertTrue(reader.read(8))
            with self.fixture.queue._db() as db:
                db.execute("UPDATE viewer_tasks SET generation=generation+1 WHERE uuid=?",(task['uuid'],))
            with self.assertRaises(OSError):reader.read(8)
        finally:reader.close()
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)
