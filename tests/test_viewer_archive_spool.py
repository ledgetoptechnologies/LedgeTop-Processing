import json
import http.client
import os
import subprocess
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.direct_output_slot import _identity
from processing.direct_slot_capture import open_sealed_reader
from processing.viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
from processing.viewer_local_result import _local,send_metadata,send_archive,LocalResultHeld
from processing.reference_http import create_reference_http_server
from processing.viewer_worker_claim import ViewerWorkerClaims
from tests.test_viewer_result_metadata import ViewerResultMetadataTests


class ViewerArchiveSpoolTests(unittest.TestCase):
    def setUp(self):
        self.base=ViewerResultMetadataTests('test_metadata_receipt_exact_replay_does_not_complete_viewer')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.fixture=self.base.fixture;self.root=self.fixture.root/'coordinator-spool';self.root.mkdir(mode=0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.root_identity=_identity(fd)
        finally:os.close(fd)
        self.spool=ViewerArchiveSpool(self.fixture.claims,root=self.root,root_identity=self.root_identity,
                                      max_archive_bytes=65536,enabled=True)
        self.fixture.api.viewer_spool=self.spool
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);send_metadata(worker,synthetic_fixture=True)

    def identity(self):return dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')

    def source(self,worker):
        claim,binding,_,_,_=_local(worker);c=worker.config
        return claim,open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget'])

    def test_authenticated_exact_spool_and_replay(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            with reader:
                receipt=self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],iter(lambda:reader.read(64),b''))
            self.assertEqual(receipt['archiveSha256'],self.base.record['archiveSha256'])
            def forbidden():raise AssertionError('replay must not read body');yield b''
            self.assertEqual(self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],forbidden()),receipt)
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def test_wrong_principal_fails_before_consuming_body(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            with reader:
                def forbidden():raise AssertionError('unauthorized body was read');yield b''
                with self.assertRaises(Exception):
                    self.spool.receive(claim['jobId'],dict(self.identity(),worker_id='other'),claim['claimToken'],forbidden())
        self.assertEqual(list(self.root.glob('*.alloc')),[])

    def test_interrupted_stream_holds_and_retains_spool(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            def broken():
                yield reader.read(16)
                raise OSError('synthetic disconnect')
            with reader:
                with self.assertRaises(OSError):self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],broken())
            with self.assertRaisesRegex(SpoolHeld,'requires_inspection'):
                self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],iter(()))
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def test_wrong_archive_bytes_hold_without_receipt(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            try:
                with self.assertRaisesRegex(SpoolHeld,'stream_mismatch'):
                    self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],iter([b'x'*reader.length]))
            finally:reader.close()
            with self.assertRaisesRegex(SpoolHeld,'requires_inspection'):
                self.spool.receive(claim['jobId'],self.identity(),claim['claimToken'],iter(()))
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def test_actual_tls_streams_sealed_bytes_to_spool(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            connection=http.client.HTTPSConnection('127.0.0.1',self.fixture.server.server_port,context=self.fixture.client,timeout=5)
            try:
                connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/archive')
                connection.putheader('Authorization','Bearer '+'a'*32);connection.putheader('X-Claim-Token',claim['claimToken'])
                connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length',str(reader.length))
                connection.endheaders()
                with reader:
                    while True:
                        data=reader.read(64)
                        if not data:break
                        connection.send(data)
                response=connection.getresponse();self.assertEqual(response.status,200);receipt=json.loads(response.read())
                self.assertEqual(receipt['archiveSha256'],self.base.record['archiveSha256'])
            finally:connection.close()
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')

    def test_actual_node_sender_spools_and_replays_receipt(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner)
            first=send_archive(worker,synthetic_fixture=True)
            self.assertEqual(send_archive(worker,synthetic_fixture=True),first)
            self.assertEqual(first['kind'],'viewer-coordinator-archive-receipt')
            self.assertEqual(first['archiveSha256'],self.base.record['archiveSha256'])
        self.assertEqual(self.fixture.queue.get(self.fixture.job)['status'],'running')
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def test_tls_rejects_unauthorized_before_reading_body(self):
        connection=http.client.HTTPSConnection('127.0.0.1',self.fixture.server.server_port,context=self.fixture.client,timeout=3)
        try:
            connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.fixture.job+'/archive')
            connection.putheader('Authorization','Bearer '+'z'*32);connection.putheader('X-Claim-Token','invalid')
            connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length','1234')
            connection.endheaders();response=connection.getresponse()
            self.assertEqual(response.status,401);response.read()
        finally:connection.close()
        self.assertEqual(list(self.root.glob('*.alloc')),[])

    def test_tls_rejects_wrong_claim_and_length_before_body(self):
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            length=reader.length;reader.close()
            for token,size in (('wrong',length),(claim['claimToken'],length+1)):
                with self.subTest(token=token,size=size):
                    connection=http.client.HTTPSConnection('127.0.0.1',self.fixture.server.server_port,context=self.fixture.client,timeout=3)
                    try:
                        connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/archive')
                        connection.putheader('Authorization','Bearer '+'a'*32);connection.putheader('X-Claim-Token',token)
                        connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length',str(size))
                        connection.endheaders();response=connection.getresponse()
                        self.assertEqual(response.status,409);response.read()
                    finally:connection.close()
        self.assertEqual(list(self.root.glob('*.alloc')),[])

    def test_unconfigured_tls_ingress_is_disabled(self):
        self.fixture.api.viewer_spool=None
        connection=http.client.HTTPSConnection('127.0.0.1',self.fixture.server.server_port,context=self.fixture.client,timeout=3)
        try:
            connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+self.fixture.job+'/archive')
            connection.putheader('Authorization','Bearer '+'a'*32);connection.putheader('X-Claim-Token','invalid')
            connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length','1234')
            connection.endheaders();response=connection.getresponse();self.assertEqual(response.status,404);response.read()
        finally:connection.close()
        self.assertEqual(list(self.root.glob('*.alloc')),[])

    def test_node_kill_after_coordinator_receipt_replays_without_resend(self):
        code='''import json,os,signal,sys
from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_node_enrollment import ViewerNodeEnrollment
import processing.viewer_local_result as result
root,config=sys.argv[1:]
original=result._persist
def persist(root_fd,root_id,name,record):
 if name.endswith('.viewer-archive-receipt'):os.kill(os.getpid(),signal.SIGKILL)
 return original(root_fd,root_id,name,record)
result._persist=persist
with DaemonStateLock(root) as owner:
 worker=ViewerNodeEnrollment(owner,**json.loads(config))
 result.send_archive(worker,synthetic_fixture=True)
'''
        result=subprocess.run([sys.executable,'-c',code,str(self.base.base.base.base.worker),json.dumps(self.base.base.base.base.config)],
                              stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.assertEqual(result.returncode,-9)
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            receipt=send_archive(self.base.worker(owner),synthetic_fixture=True)
        self.assertEqual(receipt['archiveSha256'],self.base.record['archiveSha256'])
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def kill_boundary(self,stage):
        port=self.fixture.server.server_port;self.fixture.stop_server();self.fixture.queue.close();self.fixture.queue=None
        ready=self.fixture.root/'spool-child-ready';barrier=self.fixture.root/'spool-child-barrier'
        code='''import json,ssl,sys,time
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.viewer_archive_spool import ViewerArchiveSpool
from processing.direct_output_slot import OutputSlot
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server
root,cert,key,port,spool,identity,stage,ready,barrier=sys.argv[1:];root=Path(root)
q=Queue(root/'state',node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
t=TaskStore(q,root/'state'/'viewer-tasks')
c=ViewerWorkerClaims(q,t,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
sp=ViewerArchiveSpool(c,root=spool,root_identity=json.loads(identity),max_archive_bytes=65536,enabled=True)
def pause():Path(barrier).write_text('ready');time.sleep(30)
if stage=='write':
 original=OutputSlot.write_at
 def write(self,*args):original(self,*args);pause()
 OutputSlot.write_at=write
elif stage=='sync':
 original=OutputSlot.sync
 def sync(self):original(self);pause()
 OutputSlot.sync=sync
else:
 original=ViewerArchiveSpool.receive
 def receive(self,*args,**kwargs):
  value=original(self,*args,**kwargs);pause();return value
 ViewerArchiveSpool.receive=receive
s=ReferenceStore(q);api=ReferenceWorkerAPI(s,DeliveryReceiver(s),bearer_workers={'a'*32:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')},viewer_claims=c,viewer_spool=sp)
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
server=create_reference_http_server(api,port=int(port),ssl_context=tls)
Path(ready).write_text('ready');server.serve_forever(poll_interval=.05)
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.fixture.root),str(self.fixture.cert),str(self.fixture.key),str(port),
            str(self.root),json.dumps(self.root_identity),stage,str(ready),str(barrier)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def wait_file(path):
            deadline=time.monotonic()+8
            while not path.exists() and time.monotonic()<deadline:
                if child.poll() is not None:self.fail('spool child exited before barrier')
                time.sleep(.02)
            self.assertTrue(path.exists())
        try:
            wait_file(ready)
            with DaemonStateLock(self.base.base.base.base.worker) as owner:
                worker=self.base.worker(owner);claim,reader=self.source(worker)
                def send():
                    connection=http.client.HTTPSConnection('127.0.0.1',port,context=self.fixture.client,timeout=5)
                    try:
                        connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/archive')
                        connection.putheader('Authorization','Bearer '+'a'*32);connection.putheader('X-Claim-Token',claim['claimToken'])
                        connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length',str(reader.length))
                        connection.endheaders()
                        with reader:
                            while True:
                                data=reader.read(64)
                                if not data:break
                                connection.send(data)
                        return connection.getresponse().read()
                    finally:connection.close()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future=pool.submit(send);wait_file(barrier);child.kill();child.wait(timeout=5)
                    with self.assertRaises(Exception):future.result(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        self.fixture.start_queue()
        self.fixture.claims=ViewerWorkerClaims(self.fixture.queue,self.fixture.store,local_max_photos=1000,local_max_input_bytes=100000,
                                               node_environments={'cluster':('production','staging'),'main':('production','staging')})
        self.spool=ViewerArchiveSpool(self.fixture.claims,root=self.root,root_identity=self.root_identity,max_archive_bytes=65536,enabled=True)
        def same_port(api,*,ssl_context):return create_reference_http_server(api,port=port,ssl_context=ssl_context)
        with patch('tests.test_viewer_worker_claim.create_reference_http_server',same_port):self.fixture.start_server()
        self.fixture.api.viewer_spool=self.spool
        with DaemonStateLock(self.base.base.base.base.worker) as owner:
            worker=self.base.worker(owner);claim,reader=self.source(worker)
            try:
                connection=http.client.HTTPSConnection('127.0.0.1',port,context=self.fixture.client,timeout=5)
                connection.putrequest('POST','/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/archive')
                connection.putheader('Authorization','Bearer '+'a'*32);connection.putheader('X-Claim-Token',claim['claimToken'])
                connection.putheader('Content-Type','application/octet-stream');connection.putheader('Content-Length',str(reader.length))
                connection.endheaders();response=connection.getresponse()
                self.assertEqual(response.status,200 if stage=='receipt' else 409);response.read();connection.close()
            finally:reader.close()
            if stage=='receipt':
                self.assertEqual(send_archive(worker,synthetic_fixture=True)['archiveSha256'],self.base.record['archiveSha256'])
            else:
                with self.assertRaisesRegex(LocalResultHeld,'requires_inspection'):
                    send_archive(worker,synthetic_fixture=True)
        self.assertEqual(len(list(self.root.glob('*.alloc'))),1)

    def test_kill_after_spool_write_holds(self):self.kill_boundary('write')
    def test_kill_after_spool_sync_holds(self):self.kill_boundary('sync')
    def test_kill_after_receipt_commit_replays(self):self.kill_boundary('receipt')
