"""Synthetic app.main SIGTERM test with an owned ephemeral native listener."""
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
import sys
import processing.app as app
Original=app.ThreadingHTTPServer
class OwnedEphemeralServer(Original):
    def __init__(self,address,handler):
        super().__init__((address[0],0),handler)
        print('PORT='+str(self.server_address[1]),flush=True)
    def finish_request(self,request,address):
        print('HANDLER_ENTERED',file=sys.stderr,flush=True)
        return super().finish_request(request,address)
app.ThreadingHTTPServer=OwnedEphemeralServer
sys.exit(app.main())
'''

@unittest.skipUnless(sys.platform.startswith('linux'), 'POSIX process and select required')
class SigtermMainOwned(unittest.TestCase):
    def test_app_main_joins_real_handler_before_unlock(self):
        root=Path(tempfile.mkdtemp(prefix='lt-main-sigterm-')).resolve()
        print('retained synthetic app.main state:',root,flush=True)
        token='synthetic-api-'+'a'*40
        env={key:os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        env.update(PROCESSING_API_TOKEN=token,PROCESSING_PROTOTYPE_STATE_DIR=str(root),
                   PROCESSING_BIND_HOST='127.0.0.1')
        proc=subprocess.Popen([sys.executable,'-u','-c',CHILD],cwd=Path(__file__).resolve().parents[1],
                              env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        connection=None
        def line(pipe,timeout):
            ready,_,_=select.select([pipe],[],[],timeout)
            if not ready:self.fail('owned app.main child did not report progress')
            return pipe.readline().decode().strip()
        def assert_locked():
            other=None
            try:
                try:other=Queue(root)
                except RuntimeError:return
                self.fail('Queue lock unexpectedly available')
            finally:
                if other is not None:other.close()
        try:
            first=line(proc.stdout,8)
            self.assertRegex(first,r'^PORT=[0-9]+$')
            connection=socket.create_connection(('127.0.0.1',int(first[5:])),timeout=3)
            payload=json.dumps({'requestId':'main-sigterm-test','environment':'staging',
                                'profile':'treedar-synthetic-v1'}).encode()
            headers=(f'POST /v1/prototype/jobs HTTP/1.1\r\nHost: localhost\r\n'
                     f'Authorization: Bearer {token}\r\nContent-Type: application/json\r\n'
                     f'Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode()
            connection.sendall(headers+payload[:1])
            self.assertEqual(line(proc.stderr,5),'HANDLER_ENTERED')
            assert_locked()
            proc.send_signal(signal.SIGTERM)
            time.sleep(.35)
            self.assertIsNone(proc.poll(),'app.main exited before its handler completed')
            assert_locked()
            connection.sendall(payload[1:])
            reply=b''
            while True:
                chunk=connection.recv(4096)
                if not chunk:break
                reply+=chunk
                self.assertLess(len(reply),65536)
            self.assertIn(b'201 Created',reply)
            out,err=proc.communicate(timeout=8)
            self.assertEqual(proc.returncode,0,err.decode(errors='replace'))
            self.assertNotIn(token.encode(),out+err)
            reopened=Queue(root)
            try:self.assertEqual(len(reopened.list()),1)
            finally:reopened.close()
        finally:
            if connection is not None:connection.close()
            if proc.poll() is None:
                try:proc.kill()
                except OSError:pass
            try:proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                print('owned app.main child exit unverified; PID:',proc.pid,flush=True)
                raise
