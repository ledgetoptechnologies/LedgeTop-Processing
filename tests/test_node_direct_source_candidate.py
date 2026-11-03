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
import math
import socket
from unittest.mock import patch

from processing import reference_protocol as p
from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable
from processing.source_range_transfer import (CHUNK_BYTES, SourceRangeClient, TransferError,
    source_version_for_manifest, validate_source_grant)
from tests.test_guarded_native_https import GuardedHTTPS, TOKEN


class NodeDirectSourceCandidate(GuardedHTTPS):
    def test_nas_ranges_land_on_node_before_guarded_upload_not_coordinator(self):
        # The synthetic NodeODM fixture only accepts this fixed byte string.
        self._direct_source(b'abc')

    def test_uncertain_upload_keeps_source_and_spool_for_reconciliation(self):
        self._direct_source(b'abc', ack_failure=True)

    def test_recover_durable_ack_releases_spool_without_resending_source(self):
        self._direct_source(b'abc', ack_failure=True, recover_after=True)

    def test_32mib_source_ranges_arrive_intact_on_node_without_coordinator_copy(self):
        body = b'synthetic-32mib-' * (32 * 1024 * 1024 // len(b'synthetic-32mib-'))
        # Permit a large synthetic multipart request and verify the received
        # payload before committing the fixture's task. This fixture buffers
        # the request and does not establish production memory limits.
        import ops.native_node_fixture as native_fixture
        limit = patch.object(native_fixture, 'MAX_BODY', len(body) + 4096)
        limit.start(); self.addCleanup(limit.stop)
        original = self.backend._dispatch
        def dispatch(method, path, headers, payload):
            if method == 'POST' and path == '/task/new/upload/' + self.job:
                prefix_end = payload.find(b'\r\n\r\n')
                self.assertGreater(prefix_end, 0)
                start = prefix_end + 4
                self.assertEqual(hashlib.sha256(payload[start:start + len(body)]).digest(),
                                 hashlib.sha256(body).digest())
                self.assertTrue(payload[start + len(body):].startswith(b'\r\n--'))
                with self.backend._db() as db:
                    db.execute('UPDATE tasks SET uploaded=1 WHERE uuid=?', (self.job,))
                    db.execute("INSERT INTO calls VALUES('upload',?)", (self.job,))
                return 200, {'success':True}, False
            return original(method, path, headers, payload)
        self.backend._dispatch = dispatch
        self._direct_source(body, disconnect_once=True)

    def _direct_source(self, body, disconnect_once=False, ack_failure=False, recover_after=False):
        digest = hashlib.sha256(body).hexdigest()
        requests = []
        failed = []
        class Source(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_GET(self):
                requests.append((self.path, self.headers.get('Authorization'), self.headers.get('Range'),
                                 self.headers.get('X-Processing-Request-ID')))
                if self.headers.get('Authorization') != 'Bearer synthetic-source-grant':
                    self.send_error(403); return
                start, end = map(int, self.headers['Range'][6:].split('-'))
                if disconnect_once and start == CHUNK_BYTES and not failed:
                    failed.append(True)
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
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
                        chunkBytes=CHUNK_BYTES, requestBudget=math.ceil(len(body)/CHUNK_BYTES),
                        byteBudget=len(body), attemptBudget=math.ceil(len(body)/CHUNK_BYTES) + 1,
                        nonce='synthetic-nonce')
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
        if disconnect_once:
            with self.assertRaises(TransferError):
                source_client.download_object(grant=grant, object_id='photo-1', destination=node / 'photo.jpg')
            self.assertEqual(list(coordinator.iterdir()), [])
        photo = source_client.download_object(grant=grant, object_id='photo-1', destination=node / 'photo.jpg')
        self.assertEqual(hashlib.sha256(photo.read_bytes()).hexdigest(), digest)
        self.assertEqual(list(coordinator.iterdir()), [])
        self.assertEqual(len(requests), math.ceil(len(body)/CHUNK_BYTES) + int(disconnect_once))
        if disconnect_once:
            retries = [entry for entry in requests if entry[2].startswith(f'bytes={CHUNK_BYTES}-')]
            self.assertEqual(len(retries), 2)
            self.assertEqual(retries[0][3], retries[1][3])
        server = self.start_listener()
        self.commit_reply(True)
        self.assertEqual(self.request(server, '/v1/initialize', {'task':self.task})[0], 200)
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        node_client = GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10)
        item = dict(path=str(photo), name='photo.jpg', bytes=len(body), sha256=digest)
        if ack_failure:
            from processing.host_mutation_journal import MutationHeld
            with patch('processing.host_mutation_journal.HostMutationJournal.require_done',
                       side_effect=MutationHeld('synthetic acknowledgement unavailable')):
                with self.assertRaises(EngineUnavailable):
                    node_client.upload(self.job, item)
            self.assertTrue(server.wait_drained(5))
            self.assertEqual(photo.read_bytes(), body)
            spools = list(self.spool.glob('incoming-*'))
            self.assertEqual(len(spools), 1)
            self.assertEqual(spools[0].read_bytes(), body)
            self.assertEqual(server.spool_budget.inspect()['reservations'][0]['stage'], 'mutation_started')
            self.assertEqual(self.backend.counts()['upload'], 1)
            self.assertEqual(self.journal(), [('init','done'), ('upload','done')])
            self.assertEqual(list(coordinator.iterdir()), [])
            if recover_after:
                # Reopen the durable ledger after the failed HTTP response, as
                # the node would after a process restart. Recovery checks the
                # original journal ACK and does not resend the engine upload.
                from processing.guarded_spool_budget import GuardedSpoolBudget
                reopened = GuardedSpoolBudget(self.spool, cap=server.spool_budget.cap,
                                               floor=server.spool_budget.floor)
                self.proxy.recover_acknowledged_spool(reopened, spools[0].name)
                self.assertEqual(reopened.inspect()['reservations'], [])
                self.assertFalse(spools[0].exists())
                self.assertEqual(photo.read_bytes(), body)
                self.assertEqual(self.backend.counts()['upload'], 1)
                self.assertEqual(list(coordinator.iterdir()), [])
            return
        node_client.upload(self.job, item)
        self.assertTrue(server.wait_drained(5))
        # Successful acknowledgement permits discarding only the request-owned
        # temporary upload copy, not the node source or the NAS original.
        self.assertEqual(hashlib.sha256(photo.read_bytes()).hexdigest(), digest)
        self.assertEqual(list(self.spool.glob('incoming-*')), [])
        self.assertEqual(server.spool_budget.inspect()['reservations'], [])
        self.assertEqual(self.request(server, '/v1/commit', {'task':self.job}), (200, {'uuid':self.job}))
        self.assertEqual(self.backend.counts()['upload'], 1)
        self.assertEqual(self.journal(), [('init','done'), ('upload','done'), ('commit','done')])
        self.assertEqual(list(coordinator.iterdir()), [])
