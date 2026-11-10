"""Actual native coordinator + Viewer outbox with a synthetic lost TLS reply.

This isolated loopback test uses temporary databases and fake engine bytes. It
does not control an installed coordinator, Viewer, or real dataset.
"""
import datetime
import hashlib
import ipaddress
import json
import os
import queue
import socket
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from types import SimpleNamespace

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from processing.app import create_viewer_tls_server
from processing.viewer_http import ViewerFacade
from tests.test_viewer_service import ViewerServiceTests, REQUEST_OPTIONS
from tests.support.native_viewer_source_pin import verify_viewer_source


@unittest.skipUnless(os.name == 'posix' and os.environ.get('PINNED_VIEWER_ROOT') and os.environ.get('LINUX_NODE'),
                     'requires explicit pinned Viewer source and Linux Node')
class NativeViewerAckPairCandidate(unittest.TestCase):
    def test_source_pin_rejects_modified_missing_and_untracked_server_files(self):
        original = Path(os.environ['PINNED_VIEWER_ROOT'])
        verify_viewer_source(original)
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp)
            (target/'apps/viewer').mkdir(parents=True)
            shutil.copytree(original/'apps/viewer/server', target/'apps/viewer/server')
            for name in ('lod-converter-policy.cjs', 'package.json'):
                shutil.copyfile(original/'apps/viewer'/name, target/'apps/viewer'/name)
            verify_viewer_source(target)
            candidate = target/'apps/viewer/server/viewerAcceptanceAckDelivery.js'
            content = candidate.read_bytes()
            candidate.write_bytes(content + b'\n// synthetic mutation\n')
            with self.assertRaises(ValueError):
                verify_viewer_source(target)
            candidate.write_bytes(content)
            candidate.unlink()
            with self.assertRaises(ValueError):
                verify_viewer_source(target)
            candidate.write_bytes(content)
            (target/'apps/viewer/server/untracked.js').write_text('// synthetic extra\n')
            with self.assertRaises(ValueError):
                verify_viewer_source(target)

    def test_committed_receipt_lost_response_retries_same_outbox_over_tls(self):
        verify_viewer_source(os.environ['PINNED_VIEWER_ROOT'])
        fixture = ViewerServiceTests('runTest')
        fixture.setUp()
        server = None
        thread = None
        child = None
        reader = None
        with tempfile.TemporaryDirectory() as temp:
            try:
                root = Path(temp)
                key = ec.generate_private_key(ec.SECP256R1())
                subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'synthetic-loopback')])
                now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject).public_key(key.public_key())
                    .serial_number(x509.random_serial_number()).not_valid_before(now-datetime.timedelta(minutes=1))
                    .not_valid_after(now+datetime.timedelta(hours=1))
                    .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
                    .add_extension(x509.BasicConstraints(ca=True,path_length=None), critical=True).sign(key, hashes.SHA256()))
                (root/'cert.pem').write_bytes(cert.public_bytes(serialization.Encoding.PEM))
                (root/'key.pem').write_bytes(key.private_bytes(serialization.Encoding.PEM,
                    serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.load_cert_chain(root/'cert.pem', root/'key.pem')
                replies = []

                class DropFirstCommittedReply(ViewerFacade):
                    def _reply(self, handler, status, value, headers=None):
                        if handler.path.startswith('/task/') and '/viewer-acceptance?' in handler.path and value.get('accepted') is True:
                            replies.append((status, dict(value)))
                            if len(replies) == 1:
                                # The actual TaskStore transaction has returned
                                # before dropping the first successful response.
                                handler.close_connection = True
                                try:
                                    handler.connection.shutdown(socket.SHUT_RDWR)
                                except OSError:
                                    pass
                                return
                        return super()._reply(handler, status, value, headers)

                facade = DropFirstCommittedReply(fixture.service, 'synthetic-production-token', 'synthetic-staging-token')

                class Handler(BaseHTTPRequestHandler):
                    def do_POST(self):
                        if not facade.handle(self):
                            self.send_error(404)
                    def log_message(self, *_args):
                        pass

                Handler.reference_listener = SimpleNamespace(_server_context=context)
                server = create_viewer_tls_server(Handler, '127.0.0.1', 0)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                archive = fixture.engines['cluster'].archive
                digest = hashlib.sha256(archive).hexdigest()
                env = dict(os.environ, ACK_TEST_ORIGIN=f'https://127.0.0.1:{server.server_port}',
                           ACK_TEST_ARCHIVE_SHA=digest, NODE_EXTRA_CA_CERTS=str(root/'cert.pem'))
                script = Path(__file__).parent/'support/native_viewer_ack_pair.cjs'
                child = subprocess.Popen([os.environ['LINUX_NODE'], str(script)], env=env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                lines = queue.Queue()
                def read_lines():
                    for line in child.stdout:
                        lines.put(line)
                reader = threading.Thread(target=read_lines, daemon=True)
                reader.start()
                first = json.loads(lines.get(timeout=20))
                self.assertEqual(first['state'], 'ready')
                identifier = first['providerTaskId']
                fixture.service.initialize(identifier, 'Synthetic survey', REQUEST_OPTIONS, None, 'staging')
                fixture.upload(identifier)
                fixture.service.commit(identifier)
                fixture.service.tick()
                fixture.finish(identifier)
                fixture.service.tick()
                stream, _ = fixture.service.download(identifier)
                with stream as source:
                    self.assertEqual(hashlib.sha256(source.read()).hexdigest(), digest)
                retained = Path(fixture.task(identifier)['archive_path'])
                self.assertEqual(hashlib.sha256(retained.read_bytes()).hexdigest(), digest)
                child.stdin.write('go\n'); child.stdin.flush()
                pending = json.loads(lines.get(timeout=20))
                self.assertEqual((pending['state'], pending['eventId'], pending['attemptCount']), ('pending', first['eventId'], 1))
                with fixture.queue._db() as db:
                    rows = db.execute('SELECT event_id,receipt_json FROM viewer_delivery_acceptances WHERE uuid=?', (identifier,)).fetchall()
                self.assertEqual(len(rows), 1)
                original = rows[0]['receipt_json']
                self.assertEqual(rows[0]['event_id'], first['eventId'])
                self.assertEqual(replies[0][0], 202)
                self.assertFalse(replies[0][1]['replayed'])
                child.stdin.write('go\n'); child.stdin.flush()
                delivered = json.loads(lines.get(timeout=20))
                self.assertEqual((delivered['state'], delivered['eventId'], delivered['attemptCount']), ('delivered', first['eventId'], 2))
                self.assertEqual(child.wait(timeout=15), 0, child.stderr.read())
                reader.join(timeout=3)
                with fixture.queue._db() as db:
                    rows = db.execute('SELECT event_id,receipt_json FROM viewer_delivery_acceptances WHERE uuid=?', (identifier,)).fetchall()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['receipt_json'], original)
                self.assertEqual(len(replies), 2)
                self.assertEqual(replies[1][0], 200)
                self.assertTrue(replies[1][1]['replayed'])
                self.assertEqual(hashlib.sha256(retained.read_bytes()).hexdigest(), digest)
                self.assertEqual(len(fixture.fleet.commits), 1)
            finally:
                if child is not None and child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill(); child.wait(timeout=5)
                if reader is not None:
                    reader.join(timeout=5)
                if child is not None:
                    for stream in (child.stdin, child.stdout, child.stderr):
                        if stream is not None:
                            stream.close()
                if server is not None:
                    server.shutdown()
                    if thread is not None:
                        thread.join(timeout=5)
                    server.server_close()
                fixture.tearDown()
