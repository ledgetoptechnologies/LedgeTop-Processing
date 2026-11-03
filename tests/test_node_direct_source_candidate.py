"""Inactive synthetic NAS -> node transfer; not an authenticated job protocol.

The range and guarded-node endpoints are separate local HTTPS servers.  The
range client runs in the simulated node, never in the coordinator directory.
Actual authority/snapshot custody and cross-host rollout are not implemented.
"""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import ssl
import threading

from processing import reference_protocol as p
from processing.guarded_native_client import GuardedNativeClient
from processing.source_range_transfer import (CHUNK_BYTES, SourceRangeClient,
    source_version_for_manifest, validate_source_grant)
from tests.test_guarded_native_https import GuardedHTTPS, TOKEN


class NodeDirectSourceCandidate(GuardedHTTPS):
    def test_nas_ranges_land_on_node_before_guarded_upload_not_coordinator(self):
        # The synthetic NodeODM fixture only accepts this fixed byte string.
        body = b'abc'
        digest = hashlib.sha256(body).hexdigest()
        requests = []
        class Source(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self):
                requests.append((self.path, self.headers.get('Authorization')))
                if self.headers.get('Authorization') != 'Bearer synthetic-source-grant':
                    self.send_error(403); return
                start, end = map(int, self.headers['Range'][6:].split('-'))
                part = body[start:end + 1]
                self.send_response(206)
                self.send_header('Content-Range', f'bytes {start}-{end}/{len(body)}')
                self.send_header('Content-Length', str(len(part)))
                self.send_header('ETag', '"sha256-' + digest + '"')
                self.send_header('X-Processing-Request-ID', self.headers['X-Processing-Request-ID'])
                self.end_headers(); self.wfile.write(part)
        source = ThreadingHTTPServer(('127.0.0.1', 0), Source)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.root / 'server.pem', self.root / 'server.key')
        source.socket = tls.wrap_socket(source.socket, server_side=True)
        thread = threading.Thread(target=lambda: source.serve_forever(poll_interval=.01))
        thread.start()
        def stop_source():
            source.shutdown(); thread.join(3); source.server_close()
            self.assertFalse(thread.is_alive())
        self.addCleanup(stop_source)
        objects = [dict(id='photo-1', name='photo.jpg', byteSize=len(body), sha256=digest,
                        kind='image', mediaType='image/jpeg')]
        manifest = dict(id='dataset-synthetic', revision=1, totalBytes=len(body), objectCount=1,
                        inputCounts=dict(images=1, lidarObjects=0, points=0), objects=objects)
        manifest['sha256'] = p.manifest_sha256(manifest)
        transfer = dict(protocol='source-range-v2', reservationId='reservation-1', generation=1,
                        expiresAt='2036-01-01T00:00:00Z', manifestSha256=manifest['sha256'],
                        sourceVersion=source_version_for_manifest(manifest), objectIds=['photo-1'],
                        chunkBytes=CHUNK_BYTES, requestBudget=1, byteBudget=len(body),
                        attemptBudget=2, nonce='synthetic-nonce')
        job = dict(jobId='synthetic-job', generation=1, environment='staging',
                   manifestSha256=manifest['sha256'], submission=dict(manifest=manifest),
                   transferRequest=transfer)
        raw_grant = {**transfer, 'expiresAt':'2035-01-01T00:00:00Z',
                     'jobId':'synthetic-job', 'environment':'staging', 'token':'synthetic-source-grant'}
        grant = validate_source_grant(raw_grant, job=job, clock=lambda: 100)
        node = self.root / 'node-source'; node.mkdir(mode=0o700)
        coordinator = self.root / 'coordinator'; coordinator.mkdir(mode=0o700)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        source_client = SourceRangeClient('https://127.0.0.1:' + str(source.server_port),
                node / 'range.sqlite', ssl_context=context, clock=lambda: 100)
        photo = source_client.download_object(grant=grant, object_id='photo-1', destination=node / 'photo.jpg')
        self.assertEqual(hashlib.sha256(photo.read_bytes()).hexdigest(), digest)
        self.assertEqual(list(coordinator.iterdir()), [])
        self.assertEqual(len(requests), 1)
        server = self.start_listener()
        self.commit_reply(True)
        self.assertEqual(self.request(server, '/v1/initialize', {'task':self.task})[0], 200)
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        node_client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10)
        node_client.upload(self.job, dict(path=str(photo), name='photo.jpg', bytes=len(body), sha256=digest))
        self.assertEqual(self.request(server, '/v1/commit', {'task':self.job}), (200, {'uuid':self.job}))
        self.assertEqual(self.backend.counts()['upload'], 1)
        self.assertEqual(self.journal(), [('init','done'), ('upload','done'), ('commit','done')])
        self.assertEqual(list(coordinator.iterdir()), [])
