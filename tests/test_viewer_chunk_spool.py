"""Synthetic coordinator database integration; no transport route."""
import unittest
import hashlib
import http.client
import json
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from processing.viewer_archive_spool import SpoolHeld
from processing.viewer_chunk_spool import SyntheticChunkSpool
from tests import test_viewer_archive_spool as spool_tests

class ChunkSpoolTests(unittest.TestCase):
    def setUp(self):
        self.base=spool_tests.ViewerArchiveSpoolTests('test_authenticated_exact_spool_and_replay')
        self.addCleanup(self.base.doCleanups);self.base.setUp()
        self.chunk=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        self.job=self.base.fixture.job;self.identity=self.base.identity()
        with self.base.fixture.queue._db() as db:self.token=db.execute('SELECT token FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()['token']
        self.archive=self.base.base.base.archive
    def test_sender_process_restart_replays_finished_chunks_over_tls(self):
        self.base.fixture.api.viewer_chunks=self.chunk
        code='''import json,os,signal,sys
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_synthetic_chunk_sender as sender
root,config,mode=sys.argv[1:]
original=sender.strict_response
def observe(response):
 value=original(response)
 if mode=='kill' and isinstance(value,dict) and value.get('state')=='finished' and 'receipt' in value:os.kill(os.getpid(),signal.SIGKILL)
 if mode=='kill-seal' and isinstance(value,dict) and value.get('kind')=='synthetic-chunk-seal':os.kill(os.getpid(),signal.SIGKILL)
 if mode=='kill-promotion' and isinstance(value,dict) and value.get('kind')=='viewer-coordinator-archive-receipt':os.kill(os.getpid(),signal.SIGKILL)
 return value
with DaemonStateLock(root) as owner:
 with patch.object(sender,'strict_response',observe):
  print(json.dumps(sender.send(ViewerNodeEnrollment(owner,**json.loads(config)),synthetic_fixture=True,chunk_bytes=17 if mode=='wrong' else 16)))
'''
        root=self.base.base.base.base.base.worker;config=json.dumps(self.base.base.base.base.base.config);args=[sys.executable,'-c',code,str(root),config]
        first=subprocess.run(args+['kill'],capture_output=True,timeout=20);self.assertEqual(first.returncode,-signal.SIGKILL,first.stderr)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM viewer_worker_chunk_permits WHERE state='finished'").fetchone()[0],1)
        wrong=subprocess.run(args+['wrong'],capture_output=True,text=True,timeout=20);self.assertNotEqual(wrong.returncode,0)
        with self.base.fixture.queue._db() as db:self.assertEqual(db.execute("SELECT COUNT(*) FROM viewer_worker_chunk_permits").fetchone()[0],1)
        sealed=subprocess.run(args+['kill-seal'],capture_output=True,text=True,timeout=20)
        self.assertEqual(sealed.returncode,-signal.SIGKILL,sealed.stderr)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_chunk_sessions WHERE job_id=?',(self.job,)).fetchone()['state'],'sealed')
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone()['state'],'receiving')
        second=subprocess.run(args+['kill-promotion'],capture_output=True,text=True,timeout=20)
        self.assertEqual(second.returncode,-signal.SIGKILL,second.stderr)
        third=subprocess.run(args+['resume'],capture_output=True,text=True,timeout=20)
        self.assertEqual(third.returncode,0,third.stderr)
        result=json.loads(third.stdout);self.assertEqual(result['kind'],'viewer-coordinator-archive-receipt')
        wrong=subprocess.run(args+['wrong'],capture_output=True,text=True,timeout=20);self.assertNotEqual(wrong.returncode,0)
        third=subprocess.run(args+['resume'],capture_output=True,text=True,timeout=20)
        self.assertEqual(third.returncode,0,third.stderr);self.assertEqual(json.loads(third.stdout),result)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token)['receipt'],result)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)
        self.assertEqual(len(list(self.base.base.base.base.base.output.glob('*.alloc'))),1)

    def test_assembled_runtime_tls_chunk_is_disabled_by_default(self):
        from processing.config import Config
        from processing.viewer_service import ViewerService
        from processing.reference_runtime import ReferenceRuntime,ReferenceSettings,RuntimeConfigurationError
        fixture=self.base.fixture;queue=fixture.queue
        config=Config('legacy-'+'l'*32,'http://127.0.0.1:19001','http://127.0.0.1:19002',1000,100000,
            prototype_state_dir=str(fixture.root/'state'),prototype_node_ids=('cluster','main'),viewer_enabled=True,
            viewer_worker_claim_enabled=True,viewer_worker_claim_env_policy=(('cluster',('production','staging')),('main',('production','staging'))))
        workers={'cluster':dict(bearer='a'*32,workerId='worker-a',instanceId='instance-a',resultOrigin='https://127.0.0.1:1234',resultBearer='r'*32),
                 'main':dict(bearer='m'*32,workerId='worker-main',instanceId='instance-main',resultOrigin='https://127.0.0.1:1235',resultBearer='s'*32)}
        settings=ReferenceSettings(dict(host='127.0.0.1',port=0,certificate=str(fixture.cert),privateKey=str(fixture.key),clientCA=str(fixture.cert)),
            {'v'*32:'production'},workers,dict(maxConcurrent=1,maxObjectBytes=65536),viewer_archive_spool=dict(root=str(self.base.root),rootIdentity=self.base.root_identity,maxArchiveBytes=65536))
        service=ViewerService(queue,config,store=fixture.store,engines={});self.addCleanup(service.close)
        with self.assertRaises(RuntimeConfigurationError):ReferenceRuntime(queue,config,settings,viewer_service=service,synthetic_chunk_tls=True)
        with patch('processing.reference_runtime.VIEWER_ARCHIVE_SPOOL_SCHEMA_QUALIFIED',True),patch('processing.reference_runtime.SYNTHETIC_CHUNK_TLS_QUALIFIED',True):
            runtime=ReferenceRuntime(queue,config,settings,viewer_service=service,synthetic_chunk_tls=True);runtime.start()
        self.addCleanup(runtime.close)
        runtime.control.worker.viewer_chunks.begin(self.job,self.identity,self.token,chunk_bytes=8)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        route='/ltds/worker/v1/viewer-claims/'+self.job+'/chunks/part-0'
        headers={'Authorization':'Bearer '+'a'*32,'X-Claim-Token':self.token,'X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':digest,'Content-Type':'application/octet-stream'}
        conn=http.client.HTTPSConnection('127.0.0.1',runtime._server.server_port,context=fixture.client,timeout=3)
        try:
            conn.request('POST',route,data,headers);response=conn.getresponse();self.assertEqual(response.status,200);self.assertEqual(json.loads(response.read())['state'],'finished')
        finally:conn.close()
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token),{'state':'held'})

    def test_tls_auth_before_body_and_exact_replay(self):
        self.base.fixture.api.viewer_chunks=self.chunk
        self.chunk.begin(self.job,self.identity,self.token,chunk_bytes=8)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        route='/ltds/worker/v1/viewer-claims/'+self.job+'/chunks/part-0'
        def connect():return http.client.HTTPSConnection('127.0.0.1',self.base.fixture.server.server_port,context=self.base.fixture.client,timeout=3)
        def headers(conn,bearer):
            conn.putrequest('POST',route);conn.putheader('Authorization','Bearer '+bearer)
            conn.putheader('X-Claim-Token',self.token);conn.putheader('X-Chunk-Index','0');conn.putheader('X-Chunk-Offset','0')
            conn.putheader('X-Chunk-SHA256',digest);conn.putheader('Content-Type','application/octet-stream');conn.putheader('Content-Length',str(len(data)));conn.endheaders()
        conn=connect()
        try:
            headers(conn,'wrong-token');response=conn.getresponse();self.assertEqual(response.status,401);response.read()
        finally:conn.close()
        with self.base.fixture.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_worker_chunk_permits').fetchone()[0],0)
        conn=connect()
        try:
            headers(conn,'a'*32);conn.send(data);response=conn.getresponse();self.assertEqual(response.status,200);first=json.loads(response.read());self.assertEqual(first['state'],'finished')
        finally:conn.close()
        conn=connect()
        try:
            # Replay returns only after disk verification and before body read.
            headers(conn,'a'*32);response=conn.getresponse();self.assertEqual(response.status,200);self.assertEqual(json.loads(response.read()),{'state':'finished'})
        finally:conn.close()

    def test_tls_partial_body_stays_active_and_replay_holds(self):
        self.base.fixture.api.viewer_chunks=self.chunk;self.chunk.begin(self.job,self.identity,self.token,chunk_bytes=8)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest();route='/ltds/worker/v1/viewer-claims/'+self.job+'/chunks/part-0'
        def start():
            conn=http.client.HTTPSConnection('127.0.0.1',self.base.fixture.server.server_port,context=self.base.fixture.client,timeout=3)
            conn.putrequest('POST',route)
            for key,value in {'Authorization':'Bearer '+'a'*32,'X-Claim-Token':self.token,'X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':digest,'Content-Type':'application/octet-stream','Content-Length':str(len(data))}.items():conn.putheader(key,value)
            conn.endheaders();return conn
        conn=start();conn.send(data[:1]);conn.close()
        deadline=time.monotonic()+5;row=None
        while time.monotonic()<deadline:
            with self.base.fixture.queue._db() as db:row=db.execute('SELECT state FROM viewer_worker_chunk_permits WHERE job_id=?',(self.job,)).fetchone()
            if row:break
            time.sleep(.05)
        self.assertEqual(row['state'],'active')
        restarted=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True);self.base.fixture.api.viewer_chunks=restarted
        conn=start()
        try:response=conn.getresponse();self.assertEqual(response.status,409);response.read()
        finally:conn.close()
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_tls_admission_then_hold_allows_only_admitted_chunk(self):
        self.base.fixture.api.viewer_chunks=self.chunk;self.chunk.begin(self.job,self.identity,self.token,chunk_bytes=8)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest();route='/ltds/worker/v1/viewer-claims/'+self.job+'/chunks/part-0'
        conn=http.client.HTTPSConnection('127.0.0.1',self.base.fixture.server.server_port,context=self.base.fixture.client,timeout=5)
        try:
            conn.putrequest('POST',route)
            for key,value in {'Authorization':'Bearer '+'a'*32,'X-Claim-Token':self.token,'X-Chunk-Index':'0','X-Chunk-Offset':'0','X-Chunk-SHA256':digest,'Content-Type':'application/octet-stream','Content-Length':str(len(data))}.items():conn.putheader(key,value)
            conn.endheaders()
            deadline=time.monotonic()+5;row=None
            while time.monotonic()<deadline:
                with self.base.fixture.queue._db() as db:row=db.execute('SELECT state FROM viewer_worker_chunk_permits WHERE job_id=?',(self.job,)).fetchone()
                if row:break
                time.sleep(.05)
            self.assertIsNotNone(row)
            self.assertEqual(self.base.fixture.claims.mutation_hold(self.job,self.identity,self.token)['state'],'held')
            conn.send(data);response=conn.getresponse();self.assertEqual(response.status,200);response.read()
        finally:conn.close()
        with self.assertRaises(SpoolHeld):self.chunk.admit_chunk(self.job,self.identity,self.token,'later',1,8,1,hashlib.sha256(b'x').hexdigest())

    def test_exact_coordinator_chunk_replay_and_final_receipt(self):
        self.assertEqual(self.chunk.begin(self.job,self.identity,self.token),{'state':'ready'})
        self.assertEqual(self.chunk.begin(self.job,self.identity,self.token),{'state':'ready'})
        midpoint=len(self.archive)//2;a,b=self.archive[:midpoint],self.archive[midpoint:]
        first=self.chunk.write(self.job,self.identity,self.token,0,0,a)
        self.assertEqual(first,self.chunk.write(self.job,self.identity,self.token,0,0,a))
        with self.assertRaises(Exception):self.chunk.write(self.job,self.identity,self.token,1,midpoint-1,b)
        self.chunk.write(self.job,self.identity,self.token,1,midpoint,b)
        receipt=self.chunk.finish(self.job,self.identity,self.token,2)
        self.assertEqual(receipt['archiveSha256'],self.base.base.record['archiveSha256'])
        self.assertEqual(self.chunk.finish(self.job,self.identity,self.token,2),receipt)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token),{'state':'held'})
        self.assertEqual(self.base.fixture.queue.get(self.job)['status'],'running')
    def test_creation_failure_is_held_without_recreate(self):
        with patch('processing.viewer_chunk_spool.open_slot',side_effect=OSError('synthetic crash')):
            with self.assertRaises(OSError):self.chunk.begin(self.job,self.identity,self.token)
        with self.assertRaisesRegex(SpoolHeld,'chunk_session_requires_inspection'):
            self.chunk.begin(self.job,self.identity,self.token)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_chunk_sessions WHERE job_id=?',(self.job,)).fetchone()['state'],'creating')
        self.assertEqual(list(self.base.root.glob('*.alloc')),[])

    def test_hold_blocks_later_chunk_but_admitted_chunk_finishes(self):
        from processing.viewer_chunk_journal import SyntheticChunkJournal
        self.chunk.begin(self.job,self.identity,self.token)
        entered=threading.Event();release=threading.Event();original=SyntheticChunkJournal.write
        def paused(journal,*args):
            entered.set()
            if not release.wait(5):raise RuntimeError('chunk barrier timeout')
            return original(journal,*args)
        with patch.object(SyntheticChunkJournal,'write',paused):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future=pool.submit(self.chunk.write,self.job,self.identity,self.token,0,0,self.archive[:4])
                try:
                    self.assertTrue(entered.wait(5))
                    self.assertEqual(self.base.fixture.claims.mutation_hold(self.job,self.identity,self.token)['state'],'held')
                finally:release.set()
                self.assertEqual(future.result(timeout=5)['schemaVersion'],1)
        with self.assertRaises(SpoolHeld):self.chunk.write(self.job,self.identity,self.token,1,4,self.archive[4:])
        with self.assertRaises(SpoolHeld):self.chunk.finish(self.job,self.identity,self.token,1)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_durable_admission_survives_hold_only_in_own_process(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.assertEqual(permit['state'],'active')
        other=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            other.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(self.base.fixture.claims.mutation_hold(self.job,self.identity,self.token)['state'],'held')
        receipt=self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(receipt['schemaVersion'],1)
        with self.assertRaises(SpoolHeld):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-1',1,8,1,hashlib.sha256(b'x').hexdigest())
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_finished_permit_replay_rechecks_receipt_and_slot_bytes(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        with self.assertRaises(SpoolHeld):
            self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0','wrong',0,0,data)
        self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        self.assertEqual(self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest),{'state':'finished'})
        allocation=next(self.base.root.glob('*.alloc'))
        with allocation.open('r+b') as stream:stream.write(b'X');stream.flush()
        with self.assertRaises(Exception):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)

    def test_finished_permit_replay_rejects_missing_receipt(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        permit=self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.chunk.write_admitted(self.job,self.identity,self.token,'chunk-0',permit['permit'],0,0,data)
        receipt=next(self.base.root.glob('*.chunks-v1.*.receipt'));receipt.unlink()
        with self.assertRaises(Exception):self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        self.assertFalse(receipt.exists())

    def test_interrupted_admission_does_not_auto_resume(self):
        self.chunk.begin(self.job,self.identity,self.token)
        data=self.archive[:8];digest=hashlib.sha256(data).hexdigest()
        self.chunk.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        restarted=SyntheticChunkSpool(self.base.spool,synthetic_fixture=True)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            restarted.write(self.job,self.identity,self.token,0,0,data)
        with self.assertRaisesRegex(SpoolHeld,'chunk_permit_requires_inspection'):
            restarted.admit_chunk(self.job,self.identity,self.token,'chunk-0',0,0,len(data),digest)
        with self.base.fixture.queue._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_worker_chunk_permits WHERE job_id=?',(self.job,)).fetchone()['state'],'active')

    def test_sealed_replay_hash_does_not_block_hold(self):
        from processing.viewer_chunk_journal import SyntheticChunkJournal
        self.chunk.begin(self.job,self.identity,self.token)
        self.chunk.write(self.job,self.identity,self.token,0,0,self.archive)
        self.chunk.finish(self.job,self.identity,self.token,1)
        entered=threading.Event();release=threading.Event();original=SyntheticChunkJournal.finish
        def paused(journal,count):
            entered.set()
            if not release.wait(5):raise RuntimeError('hash barrier timeout')
            return original(journal,count)
        with patch.object(SyntheticChunkJournal,'finish',paused):
            with ThreadPoolExecutor(max_workers=2) as pool:
                replay=pool.submit(self.chunk.begin,self.job,self.identity,self.token)
                try:
                    self.assertTrue(entered.wait(5))
                    hold=pool.submit(self.base.fixture.claims.mutation_hold,self.job,self.identity,self.token)
                    self.assertEqual(hold.result(timeout=2)['state'],'held')
                finally:release.set()
                with self.assertRaises(SpoolHeld):replay.result(timeout=5)
        self.assertEqual(len(list(self.base.root.glob('*.alloc'))),1)

    def test_promotion_exposes_verified_standard_spool_and_replays(self):
        self.chunk.begin(self.job,self.identity,self.token);self.chunk.write(self.job,self.identity,self.token,0,0,self.archive)
        self.chunk.finish(self.job,self.identity,self.token,1)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token),{'state':'held'})
        receipt=self.chunk.promote(self.job,self.identity,self.token,1)
        self.assertEqual(receipt['kind'],'viewer-coordinator-archive-receipt')
        self.assertEqual(self.chunk.promote(self.job,self.identity,self.token,1),receipt)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token)['receipt'],receipt)
        def forbidden_body():raise AssertionError('mixed-client body consumed');yield b''
        with self.assertRaises(Exception):self.base.spool.receive(self.job,self.identity,self.token,forbidden_body())
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token)['receipt'],receipt)
        task=self.base.fixture.store.task('22222222-2222-4222-8222-222222222222')
        reader,length=self.base.spool.open_for_viewer(task)
        try:self.assertEqual(reader.read(length),self.archive)
        finally:reader.close()
        self.assertEqual(self.base.fixture.queue.get(self.job)['status'],'running')
        from processing.config import Config
        from processing.viewer_service import ViewerService
        from processing.viewer_http import ViewerFacade
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        service=ViewerService(self.base.fixture.queue,Config('legacy','http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,viewer_enabled=True),store=self.base.fixture.store,engines={},viewer_spool=self.base.spool)
        self.addCleanup(service.close);facade=ViewerFacade(service,'viewer-token','staging-token')
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if not facade.handle(self):self.send_error(404)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05},daemon=True);thread.start()
        try:
            conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
            try:
                conn.request('GET','/task/22222222-2222-4222-8222-222222222222/download/all.zip?token=viewer-token')
                response=conn.getresponse();self.assertEqual(response.status,200);self.assertEqual(response.read(),self.archive)
            finally:conn.close()
        finally:server.shutdown();server.server_close();thread.join(5);self.assertFalse(thread.is_alive())
        allocation=next(self.base.root.glob('*.alloc'))
        with allocation.open('r+b') as stream:stream.write(b'X');stream.flush()
        with self.assertRaises(Exception):self.base.spool.lookup(self.job,self.identity,self.token)

    def test_promotion_failure_before_commit_replays_without_receipt(self):
        self.chunk.begin(self.job,self.identity,self.token);self.chunk.write(self.job,self.identity,self.token,0,0,self.archive);self.chunk.finish(self.job,self.identity,self.token,1)
        original=self.chunk._admit_operation;calls=[]
        def fail_second(db,job):
            calls.append(True)
            if len(calls)==2:raise OSError('synthetic precommit failure')
            return original(db,job)
        with patch.object(self.chunk,'_admit_operation',side_effect=fail_second):
            with self.assertRaises(OSError):self.chunk.promote(self.job,self.identity,self.token,1)
        self.assertEqual(self.base.spool.lookup(self.job,self.identity,self.token),{'state':'held'})
        self.assertEqual(self.chunk.promote(self.job,self.identity,self.token,1)['kind'],'viewer-coordinator-archive-receipt')

    def test_hold_during_promotion_hash_prevents_promotion(self):
        from processing.viewer_chunk_journal import SyntheticChunkJournal
        self.chunk.begin(self.job,self.identity,self.token);self.chunk.write(self.job,self.identity,self.token,0,0,self.archive);self.chunk.finish(self.job,self.identity,self.token,1)
        entered=threading.Event();release=threading.Event();original=SyntheticChunkJournal.finish
        def paused(journal,count):
            entered.set()
            if not release.wait(5):raise RuntimeError('hash barrier timeout')
            return original(journal,count)
        with patch.object(SyntheticChunkJournal,'finish',paused):
            with ThreadPoolExecutor(max_workers=2) as pool:
                future=pool.submit(self.chunk.promote,self.job,self.identity,self.token,1)
                try:
                    self.assertTrue(entered.wait(5));hold=pool.submit(self.base.fixture.claims.mutation_hold,self.job,self.identity,self.token)
                    self.assertEqual(hold.result(timeout=2)['state'],'held')
                finally:release.set()
                with self.assertRaises(SpoolHeld):future.result(timeout=5)
        with self.base.fixture.queue._db() as db:self.assertEqual(db.execute('SELECT state FROM viewer_worker_spool WHERE job_id=?',(self.job,)).fetchone()['state'],'receiving')

    def test_legacy_receiving_is_never_adopted(self):
        def fail():yield b'x';raise OSError('interrupted')
        with self.assertRaises(Exception):self.base.spool.receive(self.job,self.identity,self.token,fail())
        with self.assertRaisesRegex(SpoolHeld,'legacy_spool_requires_inspection'):self.chunk.begin(self.job,self.identity,self.token)
    def test_wrong_principal_and_missing_header_hold(self):
        with self.assertRaises(Exception):self.chunk.begin(self.job,dict(self.identity,node_id='main'),self.token)
        self.chunk.begin(self.job,self.identity,self.token)
        header=next(self.base.root.glob('*.chunks-v1.header'));header.unlink()
        with self.assertRaises(Exception):self.chunk.write(self.job,self.identity,self.token,0,0,b'bytes')
        self.assertFalse(header.exists())
