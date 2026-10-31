import io
import json
import os
import subprocess
import sys
import unittest
import threading
import time
from http.server import HTTPServer,BaseHTTPRequestHandler
from ops.gateway_health_probe import _probe,Refused

class Response:
    status=200
    def __init__(self,body):self.body=body
    def __enter__(self):return self
    def __exit__(self,*_):pass
    def read(self,n):return self.body[:n]
class Opener:
    def __init__(self,info=b'{"engine":"odm","taskQueueCount":0}',tasks=b'[]'):
        self.values=[info,tasks];self.calls=[]
    def open(self,request,timeout):
        self.calls.append((request.full_url,request.get_method(),timeout))
        return Response(self.values.pop(0))

class Tests(unittest.TestCase):
    def test_only_fixed_gets_and_no_credential_in_output(self):
        opener=Opener();result=_probe(b'{"token":"secret-123","engine":"odm"}',opener=opener)
        self.assertEqual([r[0].split('?')[0] for r in opener.calls],['http://nodeodm:3000/info','http://nodeodm:3000/task/list'])
        self.assertTrue(all(r[1]=='GET' for r in opener.calls))
        self.assertNotIn('secret-123',json.dumps(result));self.assertFalse(result['maintenanceAuthorized'])
    @unittest.skipUnless(os.name=='posix','POSIX alarm required')
    def test_stalled_stdin_exits_without_credentials(self):
        process=subprocess.Popen([sys.executable,'-m','ops.gateway_health_probe'],stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            process.wait(timeout=8)
            output=process.stdout.read()
            self.assertEqual(process.returncode,1)
            self.assertNotIn(b'token',output)
            self.assertEqual(process.stderr.read(),b'')
        finally:
            if process.poll() is None:process.kill();process.wait(timeout=2)
            for stream in (process.stdin,process.stdout,process.stderr):stream.close()

    @unittest.skipUnless(os.name=='posix','POSIX alarm required')
    def test_real_http_redirect_malformed_and_trickle_hold(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                if self.server.mode=='redirect':
                    self.send_response(302);self.send_header('Location','/other');self.end_headers();return
                self.send_response(200);self.end_headers()
                if self.server.mode=='malformed':self.wfile.write(b'{bad');return
                try:
                    for _ in range(60):
                        self.wfile.write(b' ');self.wfile.flush();time.sleep(.1)
                except (BrokenPipeError,ConnectionResetError):pass
        for mode in ('redirect','malformed','trickle'):
            with self.subTest(mode=mode):
                server=HTTPServer(('127.0.0.1',0),Handler);server.mode=mode
                thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
                source=("from ops import gateway_health_probe as m; "
                        "m._ENDPOINTS=('http://127.0.0.1:%d/info','http://127.0.0.1:%d/task/list'); "
                        "raise SystemExit(m.main())") % (server.server_port,server.server_port)
                try:
                    result=subprocess.run([sys.executable,'-c',source],input=b'{"token":"test-secret","engine":"odm"}',
                                          capture_output=True,timeout=8)
                    self.assertEqual(result.returncode,1)
                    self.assertNotIn(b'test-secret',result.stdout+result.stderr)
                finally:
                    server.shutdown();server.server_close();thread.join(timeout=3)
                    self.assertFalse(thread.is_alive())

    def test_bad_input_ambiguous_json_and_busy_hold(self):
        for raw in (b'{"token":"a","token":"b","engine":"odm"}',b'{"token":"x","engine":"odm","url":"x"}',b'x'*8193):
            with self.subTest(raw=raw[:40]),self.assertRaises(Refused):_probe(raw,opener=Opener())
        for info,tasks in ((b'{"engine":"odm","taskQueueCount":true}',b'[]'),
                           (b'{"engine":"odm","taskQueueCount":0}',b'[{}]'),
                           (b'{"engine":"odm","taskQueueCount":0,"engine":"odm"}',b'[]'),
                           (b'{"engine":"odm","taskQueueCount":0}',b'x'*65537)):
            with self.subTest(info=info[:50],tasks=tasks[:5]),self.assertRaises(Refused):
                _probe(b'{"token":"","engine":"odm"}',opener=Opener(info,tasks))
