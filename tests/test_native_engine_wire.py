import hashlib
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.synthetic_engine import SyntheticEngine, handler_for
from processing.viewer_service import NativeEngine
from processing.odx_smoke_worker import SmokeError


class NativeEngineWireTests(unittest.TestCase):
    def test_contradictory_init_and_mutation_acknowledgements_hold(self):
        engine=NativeEngine('http://127.0.0.1:3001')
        identifier=str(uuid.uuid4())
        task=dict(name='synthetic',options=[],outputs=None,engine_uuid=identifier)
        for value in (False,0,None,'true'):
            with self.subTest(value=value),patch.object(engine.client,'json',return_value={'uuid':identifier,'success':value}):
                with self.assertRaises(SmokeError):engine.initialize(task)
                with self.assertRaises(SmokeError):engine.action('commit',identifier)
        with patch.object(engine.client,'json',return_value={'uuid':identifier,'error':'rejected'}):
            with self.assertRaises(SmokeError):engine.initialize(task)
        for ack in ({'uuid':identifier},{'uuid':identifier,'success':True}):
            with patch.object(engine.client,'json',return_value=ack):
                engine.initialize(task);engine.action('commit',identifier)

    def test_unbound_commit_ack_is_uncertain(self):
        engine=NativeEngine('http://127.0.0.1:3001');identifier=str(uuid.uuid4())
        for ack in ({},{'message':'ok'},{'uuid':str(uuid.uuid4())},{'success':False}):
            with self.subTest(ack=ack),patch.object(engine.client,'json',return_value=ack):
                with self.assertRaises(SmokeError):engine.action('commit',identifier)
        for ack in ({'success':True},{'uuid':identifier}):
            with self.subTest(ack=ack),patch.object(engine.client,'json',return_value=ack):
                self.assertEqual(engine.action('commit',identifier),ack)

    def test_real_http_truncated_or_ambiguous_commit_ack_is_uncertain(self):
        body=b'{"success":true}'
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*_):pass
            def do_POST(self):
                self.send_response(200)
                if self.server.mode=='truncated':self.send_header('Content-Length',str(len(body)+10))
                elif self.server.mode=='duplicate':
                    self.send_header('Content-Length',str(len(body)));self.send_header('Content-Length',str(len(body)))
                elif self.server.mode=='conflicting':
                    self.send_header('Content-Length',str(len(body)));self.send_header('Transfer-Encoding','chunked')
                elif self.server.mode in ('chunked','chunked-truncated'):
                    self.send_header('Transfer-Encoding','chunked')
                elif self.server.mode!='close-delimited':self.send_header('Content-Length',str(len(body)))
                self.end_headers()
                if self.server.mode in ('chunked','chunked-truncated'):
                    self.wfile.write(('%x\r\n'%len(body)).encode()+body+b'\r\n')
                    if self.server.mode=='chunked':self.wfile.write(b'0\r\n\r\n')
                else:self.wfile.write(body)
                self.wfile.flush();self.close_connection=True
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            engine=NativeEngine('http://127.0.0.1:'+str(server.server_port));identifier=str(uuid.uuid4())
            for mode in ('truncated','duplicate','conflicting','close-delimited','chunked-truncated'):
                with self.subTest(mode=mode):
                    server.mode=mode
                    with self.assertRaises(SmokeError):engine.action('commit',identifier)
            for mode in ('complete','chunked'):
                server.mode=mode;self.assertEqual(engine.action('commit',identifier),{'success':True})
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_real_http_ambiguous_status_and_ack_are_rejected(self):
        from processing.viewer_service import EngineUnavailable
        identifier=str(uuid.uuid4())
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def respond(self):
                self.send_response(200);self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(self.server.payload)));self.end_headers()
                self.wfile.write(self.server.payload)
            def do_GET(self):self.respond()
            def do_POST(self):self.respond()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            engine=NativeEngine('http://127.0.0.1:'+str(server.server_port))
            for raw in (b'{"status":{"code":20,"code":40}}',b'{"progress":NaN}',b'{"progress":1e999}'):
                with self.subTest(raw=raw):
                    server.payload=raw
                    with self.assertRaises(EngineUnavailable):engine.read('/task/'+identifier+'/info')
            server.payload=b'{"success":false,"success":true}'
            with self.assertRaises(SmokeError):engine.action('commit',identifier)
            server.payload=b'{"uuid":"'+identifier.encode()+b'","status":{"code":20},"progress":1.5}'
            self.assertEqual(engine.read('/task/'+identifier+'/info')['progress'],1.5)
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_real_http_incomplete_status_never_becomes_completion(self):
        from processing.viewer_service import EngineUnavailable
        identifier=str(uuid.uuid4())
        body=b'{"uuid":"'+identifier.encode()+b'","status":{"code":40}}'
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*_):pass
            def do_GET(self):
                self.send_response(200);self.send_header('Content-Type','application/json')
                if self.server.mode=='truncated':self.send_header('Content-Length',str(len(body)+8))
                elif self.server.mode=='duplicate':
                    self.send_header('Content-Length',str(len(body)));self.send_header('Content-Length',str(len(body)))
                elif self.server.mode=='conflicting':
                    self.send_header('Content-Length',str(len(body)));self.send_header('Transfer-Encoding','chunked')
                elif self.server.mode!='close':self.send_header('Content-Length',str(len(body)))
                self.end_headers();self.wfile.write(body);self.wfile.flush();self.close_connection=True
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            engine=NativeEngine('http://127.0.0.1:'+str(server.server_port))
            for mode in ('truncated','duplicate','conflicting','close'):
                with self.subTest(mode=mode):
                    server.mode=mode
                    with self.assertRaises(EngineUnavailable):engine.read('/task/'+identifier+'/info')
            server.mode='complete';self.assertEqual(engine.read('/task/'+identifier+'/info')['status']['code'],40)
        finally:
            server.shutdown();server.server_close();thread.join(3);self.assertFalse(thread.is_alive())

    def test_real_http_init_upload_commit_cancel_and_archive(self):
        token = 'synthetic-engine-credential-' + 'x' * 32
        backend = SyntheticEngine(1000, 3600)
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler_for(backend, token))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = NativeEngine('http://127.0.0.1:' + str(server.server_port), token)
        identifier = str(uuid.uuid4())
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'synthetic.jpg'
                content = bytes(range(256)) * 700
                path.write_bytes(content)
                client.initialize({'name': 'synthetic wire fixture', 'options': [],
                                   'outputs': None, 'engine_uuid': identifier})
                client.upload(identifier, {'path': str(path), 'name': path.name,
                    'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()})
                client.action('commit', identifier)
                self.assertEqual(client.read('/task/' + identifier + '/info')['status']['code'], 20)
                # The fake endpoint uses the same URL-encoded-only action
                # boundary as NodeODM. Multipart cancel would fail this request.
                client.action('cancel', identifier)
                self.assertEqual(client.read('/task/' + identifier + '/info')['status']['code'], 50)
                self.assertEqual(backend.tasks[identifier]['files'][path.name]['sha256'],
                                 hashlib.sha256(content).hexdigest())
                backend.tasks[identifier]['code'] = 40
                with client.download(identifier) as response:
                    raw = response.read()
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    manifest = json.loads(archive.read('input-manifest.json'))
                self.assertEqual(manifest[path.name]['bytes'], len(content))
                self.assertEqual(client.read('/task/' + identifier + '/output', line=1)[0], '')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
