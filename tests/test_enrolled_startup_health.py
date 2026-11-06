"""Fixed read-only startup observer; no Docker mutation or real engine job."""
import contextlib
import errno
import http.client
import io
import json
import os
from pathlib import Path
import signal
import threading
import time
import unittest
from http.server import HTTPServer, BaseHTTPRequestHandler
from unittest.mock import patch, Mock

from ops.enrolled_coordinator_transport import startup_health_script


class StartupHealthTests(unittest.TestCase):
    def run_script(self):
        output=io.StringIO()
        with patch.dict(os.environ,{'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':'x'*40,'PROCESSING_PORT':'8080'}),contextlib.redirect_stdout(output):
            exec(compile(startup_health_script(),'<fixed-startup-health>','exec'),{})
        return json.loads(output.getvalue())

    @unittest.skipUnless(hasattr(signal,'setitimer') and Path('/proc/1/stat').exists(),'actual Linux observer')
    def test_actual_delayed_socket_one_get_and_timer_retired(self):
        requests=[];ready=threading.Event();stop=threading.Event();server=[]
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append((self.path,self.headers.get('Authorization')))
                raw=b'{"status":"ok","mode":"routing_probe"}'
                self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
            def log_message(self,*args):pass
        def listen():
            stop.wait(.2)
            if stop.is_set():return
            item=HTTPServer(('127.0.0.1',8080),Handler);item.timeout=2;server.append(item);ready.set();item.handle_request();item.server_close()
        worker=threading.Thread(target=listen);worker.start()
        try:
            value=self.run_script();self.assertTrue(ready.is_set());self.assertEqual(value['status'],200)
            self.assertEqual(requests,[('/healthz','Bearer '+'x'*40)])
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.0,0.0))
        finally:
            stop.set()
            if server and worker.is_alive():server[0].server_close()
            worker.join(2);self.assertFalse(worker.is_alive())

    def mocked(self,connection,*,ticks=None,clock=None):
        stack=contextlib.ExitStack();self.addCleanup(stack.close)
        stack.enter_context(patch('http.client.HTTPConnection',return_value=connection))
        stack.enter_context(patch('pathlib.Path.read_text',side_effect=ticks or ['1 (init) '+' '.join(['0']*19+['123'])]*100))
        stack.enter_context(patch('signal.getitimer',create=True,return_value=(0.0,0.0)))
        stack.enter_context(patch('signal.setitimer',create=True))
        stack.enter_context(patch('signal.signal'))
        stack.enter_context(patch.object(signal,'ITIMER_REAL',0,create=True))
        stack.enter_context(patch.object(signal,'SIGALRM',14,create=True))
        if clock:stack.enter_context(patch('time.monotonic',side_effect=clock))
        stack.enter_context(patch('time.sleep'))

    def connection(self,status=200,raw=b'{"status":"ok","mode":"routing_probe"}'):
        connection=Mock();response=connection.getresponse.return_value
        response.status=status;response.read.return_value=raw
        response.getheader.side_effect=lambda name,default=None:'application/json' if name=='Content-Type' else default
        return connection

    def test_refusal_deadline_no_request_dispatched(self):
        connection=self.connection();connection.connect.side_effect=ConnectionRefusedError(errno.ECONNREFUSED,'refused')
        self.mocked(connection,clock=[0,0,20])
        with self.assertRaisesRegex(RuntimeError,'startup_health_deadline'):self.run_script()
        connection.request.assert_not_called()

    def test_http_auth_or_unavailable_status_never_retried(self):
        for status in (401,403,503):
            with self.subTest(status=status):
                connection=self.connection(status=status);self.mocked(connection)
                with self.assertRaisesRegex(RuntimeError,'actual_application_health_required'):self.run_script()
                self.assertEqual(connection.connect.call_count,1)
                connection.getresponse.return_value.read.assert_not_called()

    def test_invalid_json_or_health_never_retried(self):
        for raw in (b'bad',b'{}',b'{"status":"unavailable","mode":"routing_probe"}'):
            with self.subTest(raw=raw):
                connection=self.connection(raw=raw);self.mocked(connection)
                with self.assertRaises((ValueError,RuntimeError)):self.run_script()
                self.assertEqual(connection.connect.call_count,1)

    def test_reset_after_request_is_not_retried(self):
        connection=self.connection();connection.getresponse.side_effect=ConnectionResetError('lost reply')
        self.mocked(connection)
        with self.assertRaises(ConnectionResetError):self.run_script()
        self.assertEqual(connection.connect.call_count,1);self.assertEqual(connection.request.call_count,1)

    def test_nonrefused_connect_errors_never_retried(self):
        for error in (TimeoutError('timeout'),OSError(errno.EACCES,'denied'),ConnectionResetError('reset')):
            with self.subTest(error=type(error).__name__):
                connection=self.connection();connection.connect.side_effect=error;self.mocked(connection)
                with self.assertRaises(type(error)):self.run_script()
                self.assertEqual(connection.connect.call_count,1);connection.request.assert_not_called()

    def test_process_incarnation_change_holds_before_request(self):
        connection=self.connection();ticks=['1 (init) '+' '.join(['0']*19+[value]) for value in ('123','124')]
        self.mocked(connection,ticks=ticks)
        with self.assertRaisesRegex(RuntimeError,'startup_incarnation_changed'):self.run_script()
        connection.connect.assert_not_called()

    def test_deadline_also_checked_after_body(self):
        connection=self.connection();self.mocked(connection,clock=[0,0,21])
        with self.assertRaisesRegex(RuntimeError,'startup_health_deadline'):self.run_script()
        self.assertEqual(connection.request.call_count,1)


if __name__=='__main__':unittest.main()
