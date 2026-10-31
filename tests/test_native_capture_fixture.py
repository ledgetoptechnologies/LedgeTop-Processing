import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import parse_qs, urlsplit

from processing.direct_outbox_contract import validate_binding
from processing.direct_output_slot import SlotHeld, _identity, open_slot
from processing.direct_slot_capture import open_sealed_reader, verify_sealed
from processing.native_capture_fixture import FixtureCaptureHeld, capture_native_fixture
from processing.viewer_service import NativeEngine
from tests.test_direct_outbox_contract import binding
from tests.test_direct_slot_capture import zip_bytes


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args):pass
    def do_GET(self):
        parsed=urlsplit(self.path);self.server.paths.append(parsed.path)
        if parsed.path!='/task/'+self.server.task_id+'/download/all.zip' or parse_qs(parsed.query)!={'token':['fixture-only']}:
            self.send_response(404);self.send_header('Content-Length','0');self.end_headers();return
        data=self.server.fixture
        self.send_response(200);self.send_header('Content-Type','application/zip')
        self.send_header('Content-Length',str(len(data)+(10 if self.server.mode=='short' else 0)))
        if self.server.mode=='double':self.send_header('Content-Length',str(len(data)))
        self.send_header('Connection','close');self.end_headers()
        try:
            self.wfile.write(data[:len(data)//2] if self.server.mode=='drop' else data)
            self.wfile.flush()
            if self.server.mode=='drop':self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:pass
        self.close_connection=True


@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'Linux allocation required')
class NativeCaptureFixtureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        try:self.root_id=_identity(fd)
        finally:os.close(fd)
        self.binding=binding();self.binding['outputBudgetBytes']=65536;self.data=zip_bytes()
        self.server=ThreadingHTTPServer(('127.0.0.1',0),FixtureHandler);self.server.daemon_threads=True
        self.server.fixture=self.data;self.server.mode='normal';self.server.paths=[];self.server.task_id=self.binding['engineTaskId']
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.stop_server)
        self.origin='http://127.0.0.1:'+str(self.server.server_port)
        self.engine=NativeEngine(self.origin,'fixture-only')
        slot=open_slot(self.root,self.root_id,validate_binding(self.binding),65536,max_budget=65536,create=True);slot.close()

    def stop_server(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5)
        if self.thread.is_alive():raise AssertionError('fixture server did not stop')

    def seal(self):return capture_native_fixture(self.engine,self.root,self.root_id,self.binding,max_budget=65536)
    def path(self,suffix):return self.root/('slot-'+validate_binding(self.binding)+suffix)

    def test_actual_engine_http_capture_and_locked_replay_without_coordinator(self):
        seal=self.seal()
        self.assertEqual(seal['bytes'],len(self.data))
        with open_sealed_reader(self.root,self.root_id,self.binding,max_budget=65536) as reader:
            chunks=[]
            while chunk:=reader.read(11):chunks.append(chunk)
            self.assertEqual(b''.join(chunks),self.data)
        self.assertEqual(verify_sealed(self.root,self.root_id,self.binding,max_budget=65536),seal)
        self.assertEqual(self.server.paths,['/task/'+self.binding['engineTaskId']+'/download/all.zip'])
        for suffix in ('.alloc','.json','.intent','.seal'):self.assertTrue(self.path(suffix).exists())

    def test_truncated_or_disconnected_response_retains_incomplete_intent(self):
        for mode in ('short','drop'):
            with self.subTest(mode=mode):
                value=dict(self.binding,engineId='fixture-'+mode)
                slot=open_slot(self.root,self.root_id,validate_binding(value),65536,max_budget=65536,create=True);slot.close()
                self.server.mode=mode
                with self.assertRaises(FixtureCaptureHeld):
                    capture_native_fixture(self.engine,self.root,self.root_id,value,max_budget=65536)
                prefix=self.root/('slot-'+validate_binding(value))
                self.assertTrue(Path(str(prefix)+'.alloc').exists());self.assertTrue(Path(str(prefix)+'.intent').exists())
                self.assertFalse(Path(str(prefix)+'.seal').exists())
                with self.assertRaises(SlotHeld):verify_sealed(self.root,self.root_id,value,max_budget=65536)

    def test_wrong_engine_task_is_not_sealed(self):
        value=dict(self.binding,engineTaskId='55555555-5555-4555-8555-555555555555')
        slot=open_slot(self.root,self.root_id,validate_binding(value),65536,max_budget=65536,create=True);slot.close()
        with self.assertRaises(FixtureCaptureHeld):
            capture_native_fixture(self.engine,self.root,self.root_id,value,max_budget=65536)
        prefix=self.root/('slot-'+validate_binding(value))
        self.assertTrue(Path(str(prefix)+'.alloc').exists());self.assertFalse(Path(str(prefix)+'.seal').exists())

    def test_duplicate_length_rejected_before_capture_intent(self):
        self.server.mode='double'
        with self.assertRaises(FixtureCaptureHeld):self.seal()
        self.assertTrue(self.path('.alloc').exists());self.assertFalse(self.path('.intent').exists())

    def test_valid_but_wrong_archive_cannot_be_identified_from_http(self):
        stream=io.BytesIO()
        with __import__('zipfile').ZipFile(stream,'w'):pass
        wrong=stream.getvalue();self.assertNotEqual(wrong,self.data)
        self.server.fixture=wrong
        seal=self.seal()
        self.assertEqual(seal['bytes'],len(wrong))
        with open_sealed_reader(self.root,self.root_id,self.binding,max_budget=65536) as reader:
            self.assertEqual(reader.read(1024),wrong)

    def test_capture_process_killed_after_seal_and_new_process_replays(self):
        ready=self.root/'fixture-ready'
        capture='''import json,sys,time
from pathlib import Path
from processing.native_capture_fixture import capture_native_fixture
from processing.viewer_service import NativeEngine
root,identity,binding,origin,ready=sys.argv[1:]
capture_native_fixture(NativeEngine(origin,'fixture-only'),root,json.loads(identity),json.loads(binding),max_budget=65536)
Path(ready).write_text('ready');time.sleep(30)
'''
        child=subprocess.Popen([sys.executable,'-c',capture,str(self.root),json.dumps(self.root_id),
                                json.dumps(self.binding),self.origin,str(ready)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            end=time.monotonic()+5
            while not ready.exists() and time.monotonic()<end:
                if child.poll() is not None:self.fail('capture process exited before barrier')
                time.sleep(.02)
            self.assertTrue(ready.exists());child.kill();child.wait(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        replay='''import base64,json,sys
from processing.direct_slot_capture import open_sealed_reader
root,identity,binding=sys.argv[1:]
with open_sealed_reader(root,json.loads(identity),json.loads(binding),max_budget=65536) as reader:
 data=b''
 while True:
  part=reader.read(13)
  if not part:break
  data+=part
 print(base64.b64encode(data).decode())
'''
        result=subprocess.run([sys.executable,'-c',replay,str(self.root),json.dumps(self.root_id),json.dumps(self.binding)],
                              capture_output=True,text=True,check=True,timeout=5)
        self.assertEqual(base64.b64decode(result.stdout.strip()),self.data)
        self.assertEqual(len(self.server.paths),1)
        for suffix in ('.alloc','.json','.intent','.seal'):self.assertTrue(self.path(suffix).exists())

    def test_nonloopback_endpoint_is_rejected_before_network_use(self):
        with self.assertRaisesRegex(FixtureCaptureHeld,'loopback_required'):
            capture_native_fixture(NativeEngine('http://192.0.2.1:3000'),self.root,self.root_id,self.binding,max_budget=65536)
        self.assertEqual(self.server.paths,[])
