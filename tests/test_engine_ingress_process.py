"""Synthetic bounded process-loss checks; no real engine or client data."""
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing.engine_ingress_guard import AdmissionError, BackendPin, Binding, EngineIngressGuard, OwnerPin
from processing.node_gate import NodeGate
from processing.prototype import Queue


@unittest.skipUnless(os.name=='posix','owned POSIX process kill required')
class GuardProcessLoss(unittest.TestCase):
    def test_killed_before_mid_and_after_backend_response(self):
        temporary=tempfile.TemporaryDirectory();safe_cleanup=False
        try:
            root=Path(temporary.name).resolve();received=threading.Event();calls=[]
            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    self.connection.settimeout(3)
                    length=int(self.headers['Content-Length']);is_upload='/upload/' in self.path
                    calls.append('upload' if is_upload else 'init')
                    try:
                        if is_upload:
                            first=self.rfile.read(1)
                            if first:received.set()
                            self.rfile.read(length-len(first))
                        else:self.rfile.read(length)
                        body=b'{"success":true}'
                        self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
                    except (OSError,TimeoutError):pass
                def log_message(self,*_):pass
            server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=False
            server_thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));server_thread.start()
            try:
                for mode in ('pre','mid','post'):
                    with self.subTest(mode=mode):
                        run=root/mode;run.mkdir();received.clear();buffer=bytearray();call_start=len(calls)
                        child=subprocess.Popen([sys.executable,'-m','tests.engine_ingress_crash_child',str(run),mode,str(server.server_port)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                        def line():
                            deadline=time.monotonic()+8
                            while b'\n' not in buffer:
                                remaining=deadline-time.monotonic()
                                if remaining<=0 or not select.select([child.stdout],[],[],remaining)[0]:raise AssertionError('child readiness timeout')
                                data=os.read(child.stdout.fileno(),4096)
                                if not data:raise AssertionError('child exited before readiness')
                                buffer.extend(data)
                            raw,_,tail=buffer.partition(b'\n');buffer[:]=tail
                            return json.loads(raw)
                        try:
                            info=line();self.assertEqual(info['kind'],'setup')
                            if mode=='mid':self.assertTrue(received.wait(5))
                            else:self.assertEqual(line()['kind'],mode)
                            child.kill();_,err=child.communicate(timeout=5)
                            self.assertEqual(child.returncode,-signal.SIGKILL,err.decode(errors='replace'))
                            self.assertEqual(calls[call_start:],['init'] if mode=='pre' else ['init','upload'])
                        finally:
                            if child.poll() is None:
                                child.kill();child.communicate(timeout=5)
                        with sqlite3.connect('file:'+str(run/'queue'/'jobs.sqlite3')+'?mode=ro',uri=True) as db:
                            self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads WHERE upload_id=?',(info['upload'],)).fetchone(),('started',))
                            self.assertEqual(db.execute('SELECT status FROM jobs WHERE id=?',(info['job'],)).fetchone(),('running',))
                        with patch('processing.node_gate.canonical_gate_path',return_value=run/'gate'):
                            with NodeGate('synthetic-engine') as gate:
                                self.assertEqual(gate.snapshot()['reservation']['job']['id'],info['job'])
                            queue=Queue(run/'queue',viewer_enabled=True)
                            try:
                                with queue._db() as db:
                                    state=db.execute('SELECT status,held_reason FROM jobs WHERE id=?',(info['job'],)).fetchone()
                                    token=db.execute('SELECT claim_token FROM viewer_tasks WHERE uuid=?',(info['task'],)).fetchone()[0]
                                    self.assertEqual(tuple(state),('held','coordinator_restart'))
                                binding=Binding('synthetic-engine','cluster',queue.authority_id,'http://127.0.0.1:18080',1,server.server_port,
                                    'coordinator-'+'a'*40,'management-'+'b'*40,'backend-'+'c'*40)
                                guard=EngineIngressGuard(queue,binding,owner_pin=OwnerPin.capture(queue),backend_pin=BackendPin("a"*64,"sha256:"+"b"*64,binding.backend_port),backend_probe=lambda *_:None,engine_probe=lambda _:binding.engine_id)
                                source,writer=socket.socketpair();writer.sendall(b'abc');before=len(calls)
                                try:
                                    headers=[('Authorization','Bearer '+binding.coordinator_secret),('X-LedgeTop-Job',info['job']),
                                        ('X-LedgeTop-Claim',token),('X-LedgeTop-Upload-ID',str(uuid.uuid4()))]
                                    with self.assertRaises(AdmissionError):
                                        guard.stream_upload('/task/new/upload/'+info['engine'],headers,source,3,hashlib.sha256(b'abc').hexdigest(),'multipart/form-data; boundary=synthetic')
                                    self.assertEqual(source.recv(3,socket.MSG_PEEK),b'abc');self.assertEqual(len(calls),before)
                                finally:source.close();writer.close()
                                with queue._db() as db:self.assertEqual(db.execute('SELECT state FROM engine_ingress_uploads WHERE upload_id=?',(info['upload'],)).fetchone()[0],'started')
                            finally:queue.close()
            finally:
                server.shutdown();server.server_close();server_thread.join(5);self.assertFalse(server_thread.is_alive())
            safe_cleanup=True
        finally:
            if safe_cleanup:temporary.cleanup()
            else:
                temporary._finalizer.detach()
                print('Synthetic process fixture retained at '+temporary.name,file=sys.stderr)
