"""Synthetic Viewer reservation shutdown rehearsal; retained state, POSIX only."""
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
import uuid

from processing.prototype import Queue
from tests.test_sigterm_inflight_owned import CHILD


@unittest.skipUnless(sys.platform.startswith('linux'), 'POSIX process and select required')
class SigtermViewerOwned(unittest.TestCase):
    def test_inflight_reservation_commits_before_queue_unlock(self):
        root=Path(tempfile.mkdtemp(prefix='lt-viewer-sigterm-')).resolve()
        print('retained synthetic Viewer state:',root,flush=True)
        api='synthetic-api-'+'a'*40
        viewer='synthetic-viewer-'+'b'*40
        env={key:os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        env.update(PROCESSING_API_TOKEN=api,PROCESSING_PROTOTYPE_STATE_DIR=str(root),
                   PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_VIEWER_ENABLED='1',
                   PROCESSING_VIEWER_TOKEN=viewer,LOCAL_NODE_URL='http://127.0.0.1:1',
                   MAIN_NODE_URL='http://127.0.0.1:2')
        proc=subprocess.Popen([sys.executable,'-u','-c',CHILD],cwd=Path(__file__).resolve().parents[1],
                              env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        connection=None
        def line(timeout):
            ready,_,_=select.select([proc.stdout],[],[],timeout)
            if not ready: self.fail('owned Viewer child did not report progress')
            return proc.stdout.readline().decode().strip()
        def assert_locked():
            other=None
            try:
                try: other=Queue(root,node_ids=('cluster','main'))
                except RuntimeError:return
                self.fail('queue lock unexpectedly available')
            finally:
                if other is not None:other.close()
        try:
            first=line(8)
            self.assertRegex(first,r'^PORT=[0-9]+$')
            connection=socket.create_connection(('127.0.0.1',int(first[5:])),timeout=3)
            identifier=str(uuid.uuid4())
            payload=('uuid='+identifier).encode()
            headers=(f'POST /task/ledgetop/reserve?token={viewer} HTTP/1.1\r\nHost: localhost\r\n'
                     f'Content-Type: application/x-www-form-urlencoded\r\nContent-Length: {len(payload)}\r\n'
                     'X-LedgeTop-Capability: YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE\r\n'
                     'Connection: close\r\n\r\n').encode()
            connection.sendall(headers+payload[:1])
            self.assertEqual(line(5),'HANDLER_ENTERED')
            assert_locked()
            proc.send_signal(signal.SIGTERM)
            time.sleep(.35)
            self.assertIsNone(proc.poll(),'coordinator exited with an in-flight Viewer request')
            assert_locked()
            connection.sendall(payload[1:])
            reply=b''
            while True:
                chunk=connection.recv(4096)
                if not chunk:break
                reply+=chunk
                self.assertLess(len(reply),65536)
            self.assertIn(b'200 OK',reply)
            out,err=proc.communicate(timeout=8)
            self.assertEqual(proc.returncode,0,err.decode(errors='replace'))
            self.assertNotIn(viewer.encode(),out+err)
            reopened=Queue(root,node_ids=('cluster','main'))
            try:
                with reopened._db() as db:
                    self.assertEqual(db.execute('SELECT state FROM viewer_reservations WHERE uuid=?',
                                                (identifier,)).fetchone()[0],'reserved')
            finally:reopened.close()
        finally:
            if connection is not None:connection.close()
            if proc.poll() is None:
                try:proc.kill()
                except OSError:pass
            try:proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                print('owned Viewer child exit unverified; PID:',proc.pid,flush=True)
                raise
