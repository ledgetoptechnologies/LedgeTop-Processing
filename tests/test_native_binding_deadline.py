import http.client
import io
import socket
import threading
import time
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlencode

from processing import reference_protocol as ref, viewer_http
from processing.admission_drain import AdmissionDrain
from tests.test_native_manifest_binding import BODY, CAP, TOKEN, ManifestBindingHTTP
from tests.test_native_plan_binding import template
from tests.test_native_plan_http import PlanHTTP


def trickle(test, path, body, *, on_started=None):
    sock=socket.create_connection(('127.0.0.1',test.server.server_port),timeout=3)
    sock.settimeout(3);stop=threading.Event();entered=threading.Event();sent=[];errors=[]
    original=viewer_http._binding_body
    def marked(handler,length):
        entered.set();return original(handler,length)
    def sender():
        try:
            for byte in body:
                if stop.is_set():break
                sock.sendall(bytes([byte]));sent.append(byte)
                if stop.wait(.04):break
        except OSError:pass
        except BaseException as error:errors.append(error)
    worker=threading.Thread(target=sender);started=False
    try:
        headers=('POST '+path+'?'+urlencode({'token':TOKEN})+' HTTP/1.1\r\nHost: localhost\r\n'
                 +'Content-Type: application/json\r\nContent-Length: '+str(len(body))+'\r\n'
                 +'X-LedgeTop-Capability: '+CAP+'\r\nConnection: close\r\n\r\n').encode('ascii')
        with patch.object(viewer_http,'BINDING_BODY_DEADLINE_SECONDS',.35), patch.object(viewer_http,'_binding_body',side_effect=marked):
            before=time.monotonic();sock.sendall(headers);worker.start();started=True
            test.assertTrue(entered.wait(2))
            if on_started:on_started()
            response=http.client.HTTPResponse(sock);response.begin();payload=response.read();elapsed=time.monotonic()-before
        test.assertEqual(response.status,408);test.assertIn(b'body_deadline_exceeded',payload)
        test.assertGreaterEqual(elapsed,.20);test.assertLess(elapsed,2.0)
        test.assertGreaterEqual(len(sent),3);test.assertLess(len(sent),len(body));test.assertFalse(errors)
    finally:
        stop.set()
        if started:worker.join(3);test.assertFalse(worker.is_alive())
        sock.close()


class ManifestDeadline(ManifestBindingHTTP):
    def test_manifest_slow_trickle_times_out_and_retry_succeeds(self):
        identifier=str(uuid.uuid4());self.reserve(identifier);path='/task/ledgetop/input-manifest/'+identifier
        trickle(self,path,BODY,on_started=lambda:self.assertEqual(self.request('GET',path)[0],404))
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_manifest_bindings').fetchone()[0],0)
        self.assertEqual(self.binding(identifier)[0],200)


class PlanDeadline(PlanHTTP):
    def test_plan_slow_trickle_releases_real_admission_barrier(self):
        identifier,plan=self.prepare();path='/task/ledgetop/task-plan/'+identifier;body=ref.canonical_json(plan)
        gate=AdmissionDrain(self.handler.prototype_queue);self.handler.prototype_queue.admission_drain=gate
        close_started=threading.Event();close_finished=threading.Event();closed=[];closer=None
        def start_close():
            nonlocal closer
            self.assertEqual(self.request('GET',path)[0],404)
            def close():
                close_started.set()
                try:closed.append(gate.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=0,
                                                   expected_closed=False,expected_authority=gate.queue.authority_id))
                finally:close_finished.set()
            closer=threading.Thread(target=close);closer.start();self.assertTrue(close_started.wait(2))
            self.assertFalse(close_finished.wait(.05))
        try:
            trickle(self,path,body,on_started=start_close)
        finally:
            if closer is not None:closer.join(3);self.assertFalse(closer.is_alive())
        self.assertEqual(len(closed),1);self.assertTrue(closed[0]['closed'])
        with self.handler.prototype_queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM native_plan_bindings').fetchone()[0],0)


class BodyDeadlineUnit(unittest.TestCase):
    def test_success_restores_prior_socket_timeout(self):
        left,right=socket.socketpair();left.settimeout(.8);stream=left.makefile('rb')
        try:
            right.sendall(b'abc')
            self.assertEqual(viewer_http._binding_body(SimpleNamespace(connection=left,rfile=stream),3),b'abc')
            self.assertEqual(left.gettimeout(),.8)
        finally:stream.close();left.close();right.close()

    def test_complete_body_after_deadline_is_rejected_and_timeout_restored(self):
        class Connection:
            def __init__(self):self.timeout=.8
            def gettimeout(self):return self.timeout
            def settimeout(self,value):self.timeout=value
        connection=Connection();handler=SimpleNamespace(connection=connection,rfile=io.BytesIO(b'abc'))
        with patch.object(viewer_http.time,'monotonic',side_effect=[0,0,31]):
            with self.assertRaises(viewer_http.RequestError) as error:viewer_http._binding_body(handler,3)
        self.assertEqual(error.exception.code,'body_deadline_exceeded');self.assertEqual(connection.timeout,.8)
