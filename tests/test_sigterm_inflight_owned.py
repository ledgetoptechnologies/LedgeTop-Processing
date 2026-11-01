"""Real process/listener shutdown rehearsal; synthetic state is retained."""
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from processing.prototype import Queue

CHILD = r'''
import json,signal,sys,threading
from http.server import ThreadingHTTPServer
from processing.app import CoordinatorService, make_handler
from processing.config import from_environment
config=from_environment(); handler=make_handler(config)
original=handler.do_POST
def entered(request):
    print('HANDLER_ENTERED',flush=True)
    return original(request)
handler.do_POST=entered
server=ThreadingHTTPServer(('127.0.0.1',0),handler)
print('PORT='+str(server.server_address[1]),flush=True)
stop=threading.Event()
signal.signal(signal.SIGTERM,lambda *_:stop.set())
sys.exit(CoordinatorService(server,handler).run(stop))
'''

@unittest.skipUnless(sys.platform.startswith('linux'), 'POSIX process and select required')
class SigtermInflightOwned(unittest.TestCase):
    def test_owned_real_http_handler_finishes_before_queue_unlock(self):
        root=Path(tempfile.mkdtemp(prefix='lt-sigterm-inflight-')).resolve()
        print('retained synthetic state:',root,flush=True)
        token='synthetic-token-'+'x'*40
        env={key:os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        env.update(PROCESSING_API_TOKEN=token,PROCESSING_PROTOTYPE_STATE_DIR=str(root))
        proc=subprocess.Popen([sys.executable,'-u','-c',CHILD],cwd=Path(__file__).resolve().parents[1],
                              env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        connection=None
        def line(timeout):
            ready,_,_=select.select([proc.stdout],[],[],timeout)
            if not ready: self.fail('owned child did not report progress')
            return proc.stdout.readline().decode().strip()
        def assert_locked():
            other=None
            try:
                try: other=Queue(root)
                except RuntimeError: return
                self.fail('queue lock unexpectedly available')
            finally:
                if other is not None: other.close()
        try:
            first=line(8)
            self.assertRegex(first,r'^PORT=[0-9]+$')
            port=int(first[5:])  # The child owns this listener continuously.
            connection=socket.create_connection(('127.0.0.1',port),timeout=3)
            payload=json.dumps({'requestId':'sigterm-owned-test','environment':'staging',
                                'profile':'treedar-synthetic-v1'}).encode()
            headers=(f'POST /v1/prototype/jobs HTTP/1.1\r\nHost: localhost\r\n'
                     f'Authorization: Bearer {token}\r\nContent-Type: application/json\r\n'
                     f'Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode()
            connection.sendall(headers+payload[:1])
            self.assertEqual(line(5),'HANDLER_ENTERED')
            assert_locked()
            proc.send_signal(signal.SIGTERM)
            time.sleep(.35)
            self.assertIsNone(proc.poll(),'coordinator exited with an in-flight handler')
            assert_locked()
            connection.sendall(payload[1:])
            reply=b''
            while True:
                chunk=connection.recv(4096)
                if not chunk: break
                reply+=chunk
                self.assertLess(len(reply),65536)
            self.assertIn(b'201 Created',reply)
            out,err=proc.communicate(timeout=8)
            self.assertEqual(proc.returncode,0,err.decode(errors='replace'))
            self.assertNotIn(token.encode(),out+err)
            reopened=Queue(root)
            try: self.assertEqual(len(reopened.list()),1)
            finally: reopened.close()
        finally:
            if connection is not None: connection.close()
            if proc.poll() is None:
                try: proc.kill()
                except OSError: pass
            try: proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                print('owned child exit unverified; PID:',proc.pid,flush=True)
                raise
