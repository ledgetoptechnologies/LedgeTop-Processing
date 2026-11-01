"""Isolated synthetic loopback HTTP checks; not production qualification."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from processing.engine_ingress_guard import AdmissionError, BackendPin
from processing.guarded_native_engine import start_guarded_sender
from processing.node_mutation_broker import LoopbackNodeODM


@unittest.skipUnless(os.name == 'posix', 'owned descriptor sender requires POSIX')
class LoopbackCandidateTests(unittest.TestCase):
    def _exercise(self, mode):
        root = Path(tempfile.mkdtemp(prefix='lt-reference-loopback-')).resolve()
        print('retained synthetic loopback root:', root, flush=True)
        source_file = root / 'input.jpg'
        source_file.write_bytes(b'abc')
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                self.connection.settimeout(2)
                try:
                    length = int(self.headers['Content-Length'])
                    data = self.rfile.read(length)
                    seen.append((data,length))
                    if mode == 'lost':
                        self.connection.shutdown(2)
                        return
                    body = json.dumps({'success':True}).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (OSError, ValueError):
                    return
        class Server(ThreadingHTTPServer):
            daemon_threads = True
            def handle_error(self, request, client_address): pass
        server = Server(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval':0.05}, daemon=True)
        owner = type('Owner', (), {})()
        owner._unreaped = []
        operation_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        item = {'path':str(source_file), 'bytes':3, 'sha256':hashlib.sha256(b'abc').hexdigest(), 'name':'input.jpg'}
        grant = {'operationId':operation_id, 'engineTaskId':task_id, 'fileName':'input.jpg', 'fileBytes':3,
                 'bodySha256': '0'*64 if mode == 'bad_hash' else item['sha256']}
        deadline = time.monotonic() + 3
        completion = None
        started = False
        try:
            thread.start()
            started = True
            adapter = LoopbackNodeODM('synthetic', BackendPin('a'*64, 'sha256:'+'b'*64, server.server_port), '', deadline_seconds=3)
            completion = start_guarded_sender(item, operation_id, deadline, owner)
            with patch('processing.node_mutation_broker.local_engine_identity', return_value='synthetic'), \
                 patch('processing.node_mutation_broker.local_backend_identity'):
                if mode == 'success':
                    self.assertEqual(adapter.exchange_upload(grant, completion.source, absolute_deadline=deadline), {'success':True})
                    completion.verify(deadline)
                    self.assertEqual(len(seen), 1)
                    self.assertIn(b'abc', seen[0][0])
                    self.assertEqual(len(seen[0][0]),seen[0][1])
                else:
                    with self.assertRaisesRegex(AdmissionError, 'upstream exchange unconfirmed'):
                        adapter.exchange_upload(grant, completion.source, absolute_deadline=deadline)
                    if mode == 'lost':
                        self.assertEqual(len(seen), 1)
                        self.assertEqual(len(seen[0][0]),seen[0][1])
                        self.assertIn(b'abc',seen[0][0])
        finally:
            try:
                if completion is not None: completion.stop()
            finally:
                try:
                    if started: server.shutdown()
                finally:
                    server.server_close()
                    if started: thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(owner._unreaped, [])
        if mode == 'bad_hash':
            self.assertTrue(not seen or len(seen[0][0]) < seen[0][1])

    def test_valid_upload(self): self._exercise('success')
    def test_hash_mismatch(self): self._exercise('bad_hash')
    def test_response_lost(self): self._exercise('lost')


if __name__ == '__main__': unittest.main()
