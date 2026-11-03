"""Synthetic existing Viewer queue over mTLS to the guarded node adapter.

Only the test seam copies inputs to the node spool using a shared filesystem;
production remote upload, authenticated queue-job identity, direct-engine
isolation and completion/release are deliberately not implemented here.
"""
import hashlib
import http.client
import json
import os
import shutil
import ssl
import subprocess
import threading
import unittest
import uuid

from processing.guarded_native_https import create_guarded_https_server
from processing.viewer_service import EngineUnavailable
from tests.test_viewer_guarded_bridge import ViewerGuardedBridgeTests, TOKEN


class SyntheticMTLSBridge:
    """Fixture only: source transfer uses a local copy, not a network upload."""
    node_guarded = True
    def __init__(self, server, context, native, spool, token=TOKEN):
        self.server, self.context, self.native, self.spool, self.token = server, context, native, spool, token

    def call(self, route, body):
        connection = http.client.HTTPSConnection('127.0.0.1', self.server.server_port,
                                                  context=self.context, timeout=5)
        try:
            connection.request('POST', '/v1/' + route, json.dumps(body),
                               {'Authorization': 'Bearer ' + self.token})
            response = connection.getresponse()
            result = json.loads(response.read())
            if response.status != 200:
                raise EngineUnavailable('guard operation held')
            return result
        except (OSError, http.client.HTTPException) as exc:
            raise EngineUnavailable('guard operation unavailable') from exc
        finally:
            connection.close()

    def initialize(self, task):
        selected = {key: task[key] for key in ('name', 'engine_uuid', 'outputs', 'options')}
        self.call('initialize', {'task': selected})

    def upload(self, identifier, item):
        dst = self.spool / (str(uuid.uuid4()) + '.jpg')
        shutil.copyfile(item['path'], dst)
        if os.name != 'nt': dst.chmod(0o600)
        selected = {key: str(dst) if key == 'path' else item[key] for key in ('path', 'name', 'bytes', 'sha256')}
        self.call('upload', {'task': identifier, 'item': selected})

    def action(self, action, identifier):
        if action != 'commit': raise EngineUnavailable('unsupported action')
        return self.call('commit', {'task': identifier})

    def read(self, *args, **kwargs):
        return self.native.read(*args, **kwargs)

    def download(self, *args, **kwargs):
        return self.native.download(*args, **kwargs)


class ViewerGuardedMTLSTests(ViewerGuardedBridgeTests):
    def setUp(self):
        super().setUp()
        for name, extension in (('server', 'subjectAltName=IP:127.0.0.1'),
                                ('client', 'extendedKeyUsage=clientAuth')):
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                            '-subj', '/CN=' + name, '-addext', extension,
                            '-keyout', str(self.root / (name + '.key')),
                            '-out', str(self.root / (name + '.pem'))],
                           check=True, capture_output=True, timeout=15)
        pem = (self.root / 'client.pem').read_text()
        fingerprint = hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()
        server = create_guarded_https_server(self.proxy, bind_host='127.0.0.1', port=0,
                    certificate=self.root / 'server.pem', private_key=self.root / 'server.key',
                    client_ca=self.root / 'client.pem', client_fingerprint=fingerprint, enabled=True)
        thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01))
        thread.start()
        def close():
            server.shutdown(); thread.join(3); server.server_close()
            self.assertFalse(thread.is_alive())
            self.assertTrue(server.wait_drained(3))
        self.addCleanup(close)
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        original = self.service.engines['cluster']
        bridge = SyntheticMTLSBridge(server, context, original.native, original.spool)
        self.service.engines = {'cluster': bridge, 'main': bridge}

    def test_wrong_guard_bearer_holds_without_native_mutation(self):
        self.service.engines['cluster'].token = 'bad'
        job = self.submit()
        self.service.tick()
        self.assertTrue(self.service.info(job).get('ledgetopHeld'))
        self.assertEqual(self.native_fixture.counts()['initialize'], 0)

    def test_authenticated_wrong_engine_uuid_cannot_mutate_bound_task(self):
        job = self.submit()
        self.service.tick()
        task = self.service.store.task(job)
        before, _ = self.journal_state()
        before_counts = self.native_fixture.counts()
        with self.assertRaises(EngineUnavailable):
            self.service.engines['cluster'].action('commit', str(uuid.uuid4()))
        self.assertEqual(self.native_fixture.counts(), before_counts)
        self.assertEqual(self.journal_state()[0]['reservation'], before['reservation'])
        self.assertEqual(before['reservation']['job']['id'], task['engine_uuid'])

    def test_engine_running_progress_and_log_cursor_forwarding(self):
        job = self.submit()
        self.service.tick()
        task = self.service.store.task(job)
        with self.native_fixture._db() as db:
            db.execute('UPDATE tasks SET status=20 WHERE uuid=?', (task['engine_uuid'],))
        self.service.tick()
        info = self.service.info(job)
        self.assertEqual(info['status']['code'], 20)
        self.assertEqual(info['progress'], 37)
        self.assertEqual(info['ledgetopNodeId'], 'cluster')
        self.assertEqual(self.service.output(job, 1), ['', 'synthetic phase two'])

    def test_ambiguous_success_commit_is_held_without_replay(self):
        original = self.native_fixture._dispatch
        def ambiguous(method, path, headers, body):
            status, response, binary = original(method, path, headers, body)
            if method == 'POST' and '/task/new/commit/' in path and status == 200:
                response = {'success': True}
            return status, response, binary
        self.native_fixture._dispatch = ambiguous
        job = self.submit()
        self.service.tick()
        self.assertTrue(self.service.info(job).get('ledgetopHeld'))
        snapshot, ops = self.journal_state()
        self.assertEqual(ops[-1], ('commit', 'pending'))
        self.service.tick()
        self.assertEqual(self.native_fixture.counts()['commit'], 1)
        self.assertEqual(self.journal_state()[0]['reservation'], snapshot['reservation'])

    def test_mismatched_uuid_commit_is_held_without_replay(self):
        original = self.native_fixture._dispatch
        def mismatched(method, path, headers, body):
            status, response, binary = original(method, path, headers, body)
            if method == 'POST' and '/task/new/commit/' in path and status == 200:
                response = {'uuid': str(uuid.uuid4())}
            return status, response, binary
        self.native_fixture._dispatch = mismatched
        job = self.submit()
        self.service.tick()
        self.assertTrue(self.service.info(job).get('ledgetopHeld'))
        self.assertEqual(self.journal_state()[1][-1], ('commit', 'pending'))
        self.service.tick()
        self.assertEqual(self.native_fixture.counts()['commit'], 1)


if __name__ == '__main__': unittest.main()
