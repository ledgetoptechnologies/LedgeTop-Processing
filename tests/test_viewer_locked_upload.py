from http.server import BaseHTTPRequestHandler, HTTPServer
import http.client
import json
import os
import threading
import unittest
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.direct_output_slot import open_slot
from processing.odx_smoke_worker import SmokeError
from processing.viewer_locked_upload import upload_locked
from processing.viewer_service import NativeEngine
from tests.test_viewer_node_inputs import NodeInputsTests


class LockedUploadTests(unittest.TestCase):
    def setUp(self):
        self.base=NodeInputsTests('test_tls_inventory_then_actual_verified_transfer_and_restart')
        self.base.setUp();self.addCleanup(self.base.doCleanups)
        self.requests=[];self.drop=False;self.raw_response=None;outer=self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length=int(self.headers['Content-Length']);data=self.rfile.read(length)
                outer.requests.append((self.path,self.headers['Content-Type'],data))
                if outer.drop or len(data)!=length:self.close_connection=True;return
                if outer.raw_response is not None:
                    self.connection.sendall(outer.raw_response);self.close_connection=True;return
                raw=b'{"success":true}';self.send_response(200);self.send_header('Content-Length',str(len(raw)))
                self.end_headers();self.wfile.write(raw)
            def log_message(self,*args):pass
        self.server=HTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5)
        self.assertFalse(self.thread.is_alive())

    def execute(self,*,wrong_hash=False,wrong_identity=False,mutate=False,contend=False):
        with DaemonStateLock(self.base.base.worker) as owner:
            inputs=self.base.make(owner);inputs.fetch_plan();inputs.freeze_inventory()
            object_id=self.base.object_id(inputs);inputs.transfer(object_id)
            with inputs._db() as db:
                row=db.execute('SELECT entry,slot_key,slot_identity FROM objects WHERE object_id=?',(object_id,)).fetchone()
            entry=json.loads(row[0]);item={key:entry[key] for key in ('name','bytes','sha256')}
            if wrong_hash:item['sha256']='0'*64
            with inputs._gate():
                slot=open_slot(inputs.policy['inputRoot'],inputs.policy['inputRootIdentity'],row[1],entry['bytes'],
                               max_budget=inputs.policy['maxObject'],create=False)
                try:
                    engine=NativeEngine('http://127.0.0.1:'+str(self.server.server_port))
                    identity=json.loads(row[2])
                    if wrong_identity:identity['allocation'][1]+=1
                    original=http.client.HTTPConnection.send;changed=[False];threads=[];done=threading.Event()
                    def send(connection,data):
                        result=original(connection,data)
                        if (mutate or contend) and data.startswith(b'--processing-') and not changed[0]:
                            changed[0]=True
                            if mutate:os.pwrite(slot._fd,b'different',0)
                            else:
                                def writer():slot.write_at(0,b'synthetic');done.set()
                                thread=threading.Thread(target=writer);threads.append(thread);thread.start()
                                self.assertFalse(done.wait(.1))
                        return result
                    try:
                        with patch.object(http.client.HTTPConnection,'send',send):
                            upload_locked(engine,inputs.claim['engineTaskId'],item,slot,identity)
                    finally:
                        for thread in threads:thread.join(5)
                        self.assertTrue(all(not thread.is_alive() for thread in threads))
                    if contend:self.assertTrue(done.is_set())
                finally:slot.close()
            return inputs.claim['engineTaskId']

    def test_actual_native_multipart_route_and_verified_bytes(self):
        identifier=self.execute();self.assertEqual(len(self.requests),1)
        path,content_type,body=self.requests[0]
        self.assertEqual(path,'/task/new/upload/'+identifier)
        self.assertTrue(content_type.startswith('multipart/form-data; boundary='))
        self.assertIn(b'filename="photo.jpg"',body);self.assertIn(b'\r\n\r\nsynthetic\r\n',body)

    def test_hash_mismatch_sends_nothing(self):
        with self.assertRaisesRegex(SmokeError,'hash changed'):self.execute(wrong_hash=True)
        self.assertEqual(self.requests,[])

    def test_identity_mismatch_sends_nothing(self):
        with self.assertRaisesRegex(SmokeError,'identity changed'):self.execute(wrong_identity=True)
        self.assertEqual(self.requests,[])

    def test_noncooperative_mutation_aborts_before_multipart_completion(self):
        with self.assertRaisesRegex(SmokeError,'hash changed'):self.execute(mutate=True)

    def test_same_object_writer_waits_until_upload_finishes(self):
        self.execute(contend=True)

    def test_lost_ack_is_uncertain_without_automatic_retry(self):
        self.drop=True
        with self.assertRaisesRegex(SmokeError,'uncertain'):self.execute()
        self.assertEqual(len(self.requests),1)

    def test_truncated_valid_json_prefix_is_uncertain(self):
        self.raw_response=b'HTTP/1.1 200 OK\r\nContent-Length: 30\r\n\r\n{"success":true}'
        with self.assertRaisesRegex(SmokeError,'uncertain'):self.execute()
        self.assertEqual(len(self.requests),1)

    def test_duplicate_keys_and_nonfinite_are_uncertain(self):
        for body in (b'{"success":true,"success":true}',b'{"success":true,"x":NaN}'):
            with self.subTest(body=body):
                self.raw_response=b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body
                with self.assertRaisesRegex(SmokeError,'uncertain'):self.execute()

    def test_conflicting_framing_is_uncertain(self):
        for headers in (b'Content-Length: 16\r\nTransfer-Encoding: chunked',
                        b'Content-Length: 16\r\nContent-Length: 16'):
            with self.subTest(headers=headers):
                self.raw_response=b'HTTP/1.1 200 OK\r\n'+headers+b'\r\n\r\n{"success":true}'
                with self.assertRaisesRegex(SmokeError,'uncertain'):self.execute()
