"""Owned reader lifecycle through actual result descriptors and child crashes."""
import http.client
import json
import os
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from processing.reference_api import Response
from processing.reference_binary import BinaryResponse
from processing.reference_http import create_reference_http_server
from processing.reference_readers import ReaderSession
from processing.reference_results import ReferenceResults, ResultUnavailable
from processing.scratch_cleanup import ScratchCleanupExecutor
from tests.test_reference_results import ResultsFixture


CHILD = r'''
import json,os,sqlite3,sys
from pathlib import Path
from unittest.mock import patch
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_readers import ReaderSession,ReadPinHandle
from processing.reference_results import ReferenceResults,ResultAuthorization,ResultUnavailable
from processing.scratch_policy import ScratchLedger
from processing.worker_outbox import WorkerOutbox
from tests.test_reference_engine_worker import FakeNativeEngine
data=json.loads(sys.stdin.read());root=Path(data['root'])
ledger=ScratchLedger(root/'ledger.sqlite',root/'scratch',capacity_probe=lambda _:1000000,
 safety_floor_bytes=0,cleanup_grace_seconds=0)
worker=ReferenceEngineWorker(root/'journal/worker.sqlite',ledger=ledger,
 outbox=WorkerOutbox(root/'outbox/outbox.sqlite'),engine=FakeNativeEngine(),engine_id='physical-engine',
 node_id='cluster',authority_id=data['authority'],coordinator_origin='http://127.0.0.1:8000',
 gate_generation=1,worker_id='worker',engine_version='synthetic-v1')
session=ReaderSession(ledger);session.__enter__()
results=ReferenceResults(worker,reader_session=session)
metadata=results.metadata(data['job']);obj=metadata['objects'][0]
authorization=ResultAuthorization(node_id='cluster',worker_id='worker',job_id=data['job'],generation=1,
 environment='staging',result_manifest_sha256=metadata['resultManifestSha256'],object_ids=(obj['id'],),share_approved=True)
mode=sys.argv[1]
def open_stream():
 return results.open(data['job'],generation=1,object_id=obj['id'],
  result_manifest_sha256=metadata['resultManifestSha256'],authorization=authorization)
if mode=='attach-failure':
 original=ReadPinHandle.attach_fd
 def uncertain(handle,descriptor):
  original(handle,descriptor)
  raise OSError('synthetic attachment response uncertainty')
 with patch.object(ReadPinHandle,'attach_fd',uncertain):
  try:open_stream()
  except ResultUnavailable:pass
  else:raise AssertionError('attachment must fail')
else:stream=open_stream()
assert session.active_count==1
if mode=='fd-failure':
 with patch('processing.reference_results.os.close',side_effect=OSError('synthetic close uncertainty')):
  try:stream.close()
  except ResultUnavailable:pass
  else:raise AssertionError('close must fail')
elif mode=='release-failure':
 with patch.object(session,'_commit',side_effect=sqlite3.OperationalError('synthetic DB uncertainty')):
  try:stream.close()
  except ResultUnavailable:pass
  else:raise AssertionError('release must fail')
if mode!='crash':
 try:session.close()
 except Exception:pass
 else:raise AssertionError('uncertain reader lease must remain held')
 assert session.active_count==1
print(json.dumps({'opened':True,'ownedReaders':session.active_count}),flush=True)
os._exit(0)
'''


@unittest.skipUnless(sys.platform.startswith('linux'), 'owned reader lease requires Linux')
class ReferenceOwnedResultsTests(ResultsFixture,unittest.TestCase):
    def owned(self):
        session=ReaderSession(self.ledger)
        self.results=ReferenceResults(self.worker,reader_session=session)
        return session

    def child(self,mode):
        result=subprocess.run([sys.executable,'-B','-c',CHILD,mode],input=json.dumps(
            dict(root=str(self.root),authority=self.authority,job=self.job['jobId'])),text=True,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout),dict(opened=True,ownedReaders=1))

    def test_owned_full_range_and_abort_release_only_after_descriptor_close(self):
        with self.owned() as session:
            for values in ({},{'range_header':'bytes=0-9'}):
                stream=self.open_result(**values)
                self.assertEqual(session.active_count,1)
                stream.read(1)
                with self.assertRaises(Exception):session.close()
                stream.close()
                self.assertEqual(session.active_count,0)
                self.assertEqual(self.active_pins(),0)
            self.assertEqual(b''.join(self.open_result().chunks()),self.engine.archive)
            self.assertEqual(session.active_count,0)

    def test_failed_open_and_invalid_hash_release_owned_pin_without_leaking(self):
        with self.owned() as session:
            with patch('processing.reference_results.os.open',side_effect=OSError('synthetic file unavailable')):
                with self.assertRaises(ResultUnavailable):self.open_result()
            self.assertEqual(session.active_count,0)
            self.archive.write_bytes(b'x'+self.engine.archive[1:])
            with self.assertRaises(ResultUnavailable):self.open_result()
            self.assertEqual(session.active_count,0)
            self.assertEqual(self.active_pins(),0)

    def test_known_owned_close_allows_fd_number_reuse_without_false_live_pin(self):
        reused=[]
        with self.owned() as session:
            original=session.pin
            descriptor=[None]
            def pin(*args):
                handle=original(*args)
                close=handle.close_fd
                def close_and_reuse():
                    close()
                    replacement=os.open('/dev/null',os.O_RDONLY)
                    reused.append(replacement)
                    if replacement!=descriptor[0]:
                        os.dup2(replacement,descriptor[0]);reused.append(descriptor[0])
                handle.close_fd=close_and_reuse
                return handle
            try:
                with patch.object(session,'pin',side_effect=pin):stream=self.open_result()
                descriptor[0]=stream._descriptor
                stream.close()
                self.assertEqual(session.active_count,0)
                self.assertEqual(self.active_pins(),0)
            finally:
                for fd in set(reused):os.close(fd)

    def test_actual_child_result_open_crash_retires_only_owned_epoch_then_normal_cleanup(self):
        self.accept()
        self.child('crash')
        self.assertEqual(self.active_pins(),1)
        self.assertEqual(self.ledger.cleanup_candidates(),[])
        with self.owned() as session:
            self.assertEqual(session.active_count,0)
            self.assertEqual(self.active_pins(),0)
            item=self.ledger.cleanup_candidates()[0]
            # Exercise descriptor lifecycle beyond the production import hold.
            with patch.object(ScratchCleanupExecutor, '_require_import_confirmation', return_value=None):
                ScratchCleanupExecutor(self.ledger).execute(item['reservation'],item['id'],generation=item['generation'],
                    sha256=item['sha'],byte_size=item['bytes'],relative_path=item['path'])
            self.assertFalse(self.archive.exists())
            self.assertEqual((self.directory/'image-1.jpg').read_bytes(),b'abc')

    def test_child_crash_never_retires_mixed_anonymous_legacy_pin(self):
        self.accept()
        self.ledger.set_pin('reservation-1',self.object['id'],'legacy-read',kind='read',state='active')
        self.child('crash')
        self.assertEqual(self.active_pins(),2)
        with self.owned():
            self.assertEqual(self.active_pins(),1)
            self.assertEqual(self.ledger.cleanup_candidates(),[])
            self.assertTrue(self.archive.exists())

    def test_uncertain_fd_close_and_db_release_hold_lease_until_process_death(self):
        for mode in ('fd-failure','release-failure','attach-failure'):
            with self.subTest(mode=mode):
                self.child(mode)
                self.assertEqual(self.active_pins(),1)
                with self.owned():self.assertEqual(self.active_pins(),0)

    def test_real_http_head_abort_and_handler_drain_close_owned_readers(self):
        session=self.owned()
        session.__enter__()
        entered,release=threading.Event(),threading.Event()
        owner=self
        class API:
            def handle(self,method,path,headers,body,*,protected_transport=False):
                if method not in ('GET','HEAD'):return Response(404,{'error':'not_found'})
                stream=owner.open_result()
                if path=='/blocked':
                    original=stream.chunks
                    def chunks():
                        entered.set();release.wait(3)
                        yield from original()
                    stream.chunks=chunks
                return BinaryResponse(stream,stream.status,stream.headers)
        server=create_reference_http_server(API())
        server.daemon_threads=False;server.block_on_close=True
        serving=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02})
        serving.start()
        try:
            client=http.client.HTTPConnection(*server.server_address,timeout=3)
            client.request('HEAD','/');response=client.getresponse();self.assertEqual(response.status,200)
            response.read();client.close()
            self.assertEqual(session.active_count,0)
            client=http.client.HTTPConnection(*server.server_address,timeout=3)
            client.request('GET','/blocked');response=client.getresponse()
            self.assertTrue(entered.wait(2));self.assertEqual(session.active_count,1)
            response.close();client.close()
            server.shutdown();serving.join(2)
            drained=threading.Event()
            closer=threading.Thread(target=lambda:(server.server_close(),drained.set()))
            closer.start()
            self.assertFalse(drained.wait(0.05))
            with self.assertRaises(Exception):session.close()
            release.set();closer.join(3)
            self.assertTrue(drained.is_set())
            self.assertEqual(session.active_count,0)
        finally:
            release.set();server.shutdown();server.server_close();serving.join(3)
            session.close()
