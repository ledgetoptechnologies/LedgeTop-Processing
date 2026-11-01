import hashlib
from http.server import ThreadingHTTPServer
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
