import base64
import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import threading
import unittest
from unittest.mock import patch
from urllib.parse import quote

from ops.reference_worker_daemon import DaemonStateLock
from processing.config import Config
from processing.viewer_http import ViewerFacade
from processing.viewer_local_result import send_archive
from processing.viewer_service import ViewerService
from processing.direct_output_slot import OutputSlot
from processing.viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
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

    def test_staged_ack_requires_completion_and_binds_actual_spool(self):
        from processing import reference_protocol as p
        from processing.viewer_staged_ack import ViewerStagedAck,ROUTE,NAMESPACE
        from processing.viewer_staged_ack_schema import initialize_staged_ack_schema
        token=base64.urlsafe_b64encode(b'a'*32).decode().rstrip('=')
        cap=base64.urlsafe_b64encode(b'b'*32).decode().rstrip('=')
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        capability_hash=self.fixture.store._capability_hash(task['uuid'],task['environment'],cap)
        with self.fixture.queue._db() as db:
            db.execute("INSERT INTO viewer_reservations(uuid,environment,capability_hash,state) VALUES(?,?,?,'reserved')",(task['uuid'],task['environment'],capability_hash))
        initialize_staged_ack_schema(self.fixture.queue)
        ack=ViewerStagedAck(self.service,{token:{'environment':task['environment'],'principalId':'viewer-a'}})
        headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':cap};path=ROUTE+task['uuid']
        self.assertEqual(ack.handle('GET',path+'/commitment',headers,protected_transport=True).status,409)
        from processing.viewer_local_result import send_completion
        with DaemonStateLock(self.base.base.base.base.base.worker) as owner:
            send_completion(self.base.base.worker(owner),synthetic_fixture=True)
        commitment=ack.handle('GET',path+'/commitment',headers,protected_transport=True)
        self.assertEqual(commitment.status,200,commitment.body)
        value=dict(commitment.body,attemptId=str(uuid.uuid4()),modelId=str(uuid.uuid4()),modelVersionId=str(uuid.uuid4()),stagingCommitId=str(uuid.uuid4()))
        with self.fixture.queue._db() as db:
            original=db.execute('SELECT observation FROM viewer_worker_completions WHERE job_id=?',(task['job_id'],)).fetchone()[0]
            db.execute('UPDATE viewer_worker_completions SET observation=? WHERE job_id=?',('{}',task['job_id']))
        self.assertNotEqual(ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True).status,200)
        with self.fixture.queue._db() as db:
            db.execute('UPDATE viewer_worker_completions SET observation=? WHERE job_id=?',(original,task['job_id']))
            db.execute("INSERT INTO viewer_worker_permits VALUES(?,?,'upload','active')",(task['job_id'],'late'))
        self.assertEqual(ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True).status,409)
        with self.fixture.queue._db() as db:
            db.execute('DELETE FROM viewer_worker_permits WHERE job_id=? AND operation_id=?',(task['job_id'],'late'))
            db.execute("UPDATE viewer_worker_permits SET state='active' WHERE job_id=? AND operation_id='commit'",(task['job_id'],))
        self.assertEqual(ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True).status,409)
        with self.fixture.queue._db() as db:db.execute("UPDATE viewer_worker_permits SET state='finished' WHERE job_id=? AND operation_id='commit'",(task['job_id'],))
        first=ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True)
        self.assertEqual(first.status,200,first.body);self.assertTrue(first.body['created'])
        replay=ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True)
        self.assertEqual(replay.status,200,replay.body);self.assertFalse(replay.body['created'])
        lookup=ack.handle('GET',path+'/generations/'+str(task['generation']),headers,protected_transport=True)
        self.assertEqual(lookup.body['assertion'],value)
        self.assertEqual(ack.handle('POST',path,headers,p.canonical_json(dict(value,generation=task['generation']+1)),protected_transport=True).status,409)
        self.assertEqual(ack.handle('GET',path+'/commitment',dict(headers,Authorization='Bearer '+cap),protected_transport=True).status,401)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        with self.fixture.queue._db() as db:
            db.execute("UPDATE jobs SET status='held' WHERE id=?",(task['job_id'],))
        self.assertEqual(ack.handle('GET',path+'/commitment',headers,protected_transport=True).status,409)
        self.assertEqual(ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True).status,409)
        self.assertEqual(ack.handle('GET',path+'/generations/'+str(task['generation']),headers,protected_transport=True).body['assertion'],value)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_staged_ack_and_authenticated_hold_serialize_both_orders(self):
        from processing import reference_protocol as p
        from processing.viewer_staged_ack import ViewerStagedAck,ROUTE
        from processing.viewer_staged_ack_schema import initialize_staged_ack_schema
        from processing.viewer_local_result import send_completion
        token=base64.urlsafe_b64encode(b'a'*32).decode().rstrip('=')
        cap=base64.urlsafe_b64encode(b'b'*32).decode().rstrip('=')
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        capability_hash=self.fixture.store._capability_hash(task['uuid'],task['environment'],cap)
        with self.fixture.queue._db() as db:
            db.execute("INSERT INTO viewer_reservations(uuid,environment,capability_hash,state) VALUES(?,?,?,'reserved')",(task['uuid'],task['environment'],capability_hash))
            claim=db.execute('SELECT * FROM viewer_worker_claims WHERE job_id=?',(task['job_id'],)).fetchone()
        identity=dict(node_id=claim['node_id'],worker_id=claim['worker_id'],worker_instance_id=claim['instance_id'])
        with DaemonStateLock(self.base.base.base.base.base.worker) as owner:
            send_completion(self.base.base.worker(owner),synthetic_fixture=True)
        initialize_staged_ack_schema(self.fixture.queue)
        ack=ViewerStagedAck(self.service,{token:{'environment':task['environment'],'principalId':'viewer-a'}})
        headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':cap};path=ROUTE+task['uuid']
        commitment=ack.handle('GET',path+'/commitment',headers,protected_transport=True)
        self.assertEqual(commitment.status,200)
        value=dict(commitment.body,attemptId=str(uuid.uuid4()),modelId=str(uuid.uuid4()),modelVersionId=str(uuid.uuid4()),stagingCommitId=str(uuid.uuid4()))
        def post():return ack.handle('POST',path,headers,p.canonical_json(value),protected_transport=True)
        claims=self.base.spool.claims
        def hold():return claims.mutation_hold(task['job_id'],identity,claim['token'])
        entered=threading.Event();release=threading.Event();contender_started=threading.Event();original=claims._authorize_inventory
        def started_post():contender_started.set();return post()
        def started_hold():contender_started.set();return hold()
        def hold_pause(*args,**kwargs):
            result=original(*args,**kwargs)
            if threading.current_thread().name.startswith('hold-first'):
                entered.set()
                if not release.wait(5):raise RuntimeError('hold barrier timeout')
            return result
        with patch.object(claims,'_authorize_inventory',side_effect=hold_pause):
            with ThreadPoolExecutor(max_workers=2,thread_name_prefix='hold-first') as pool:
                future=pool.submit(hold)
                try:
                    self.assertTrue(entered.wait(5))
                    ack_future=pool.submit(started_post)
                    self.assertTrue(contender_started.wait(5))
                finally:release.set()
                self.assertEqual(future.result(timeout=10)['state'],'held')
                self.assertEqual(ack_future.result(timeout=10).status,409)
        with self.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_staged_acks').fetchone()[0],0)
            db.execute('DELETE FROM viewer_worker_fences WHERE job_id=?',(task['job_id'],))
            db.execute("UPDATE jobs SET status='running',held_reason=NULL WHERE id=?",(task['job_id'],))
        entered.clear();release.clear();contender_started.clear();original_check=self.base.spool.revalidate_staged_ack_locked
        def ack_pause(*args,**kwargs):
            result=original_check(*args,**kwargs);entered.set()
            if not release.wait(5):raise RuntimeError('ack barrier timeout')
            return result
        with patch.object(self.base.spool,'revalidate_staged_ack_locked',side_effect=ack_pause):
            with ThreadPoolExecutor(max_workers=2,thread_name_prefix='ack-first') as pool:
                future=pool.submit(post)
                try:
                    self.assertTrue(entered.wait(5))
                    hold_future=pool.submit(started_hold)
                    self.assertTrue(contender_started.wait(5))
                finally:release.set()
                self.assertEqual(future.result(timeout=10).status,200)
                self.assertEqual(hold_future.result(timeout=10)['state'],'held')
        lookup=ack.handle('GET',path+'/generations/'+str(task['generation']),headers,protected_transport=True)
        self.assertEqual(lookup.body['assertion'],value)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_staged_ack_reader_revalidates_inside_write_barrier(self):
        from processing.viewer_local_result import send_completion
        with DaemonStateLock(self.base.base.base.base.base.worker) as owner:
            send_completion(self.base.base.worker(owner),synthetic_fixture=True)
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        reader,_=self.base.spool.open_for_viewer(task)
        try:
            with self.fixture.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                receipt=self.base.spool.revalidate_staged_ack_locked(db,task,reader)
                self.assertEqual(receipt,reader.receipt)
            with self.fixture.queue._db() as db:
                db.execute("UPDATE viewer_tasks SET generation=generation+1 WHERE uuid=?",(task['uuid'],))
            with self.fixture.queue._db() as db:
                db.execute('BEGIN IMMEDIATE')
                with self.assertRaises(SpoolHeld):self.base.spool.revalidate_staged_ack_locked(db,task,reader)
        finally:reader.close()
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

    def test_open_failure_releases_capacity_even_if_close_fails(self):
        task=self.fixture.store.task('22222222-2222-4222-8222-222222222222')
        original=OutputSlot.close
        def failed_close(slot):
            original(slot)
            raise OSError('injected close failure after descriptor close')
        with patch.object(ViewerArchiveSpool,'_digest',side_effect=SpoolHeld('primary failure')):
            with patch.object(OutputSlot,'close',failed_close):
                with self.assertRaisesRegex(SpoolHeld,'primary failure'):
                    self.base.spool.open_for_viewer(task)
        self.assertTrue(self.base.spool._work.acquire(blocking=False))
        self.assertTrue(self.base.spool._work.acquire(blocking=False))
        try:self.assertFalse(self.base.spool._work.acquire(blocking=False))
        finally:self.base.spool._work.release();self.base.spool._work.release()
