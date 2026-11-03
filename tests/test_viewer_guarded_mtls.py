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
import sys
import threading
import unittest
import uuid

from processing.guarded_native_https import create_guarded_https_server
from processing.viewer_service import EngineUnavailable
from processing.prototype import Conflict
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

    def assert_restart_holds_pending(self, phase, status):
        job = self.submit()
        self.service.tick()
        task = self.service.store.task(job)
        self.assertEqual(task['node_guarded'], 1)
        self.service.store.update(job, phase=phase)
        with self.native_fixture._db() as db:
            db.execute('UPDATE tasks SET status=? WHERE uuid=?', (status, task['engine_uuid']))
        # Reopen service with a replacement object that does not carry the
        # guard marker; the authoritative marker must survive in SQLite.
        self.service.close()
        engine = self.service.engines['cluster']
        engine.node_guarded = False
        from processing.viewer_service import ViewerService
        self.service = ViewerService(self.queue, self.service.config,
                                     engines={'cluster': engine, 'main': engine}, probe=self.service.probe)
        self.addCleanup(self.service.close)
        self.service._catalog = lambda: {'ready': True, 'commonOptions': []}
        self.service._selected_options = lambda _: []
        calls = self.native_fixture.counts()
        self.service.tick()
        self.assertTrue(self.service.info(job).get('ledgetopHeld'))
        self.assertEqual(self.service.store.task(job)['node_guarded'], 1)
        self.assertEqual(self.native_fixture.counts(), calls)
        with self.assertRaises(Conflict):
            self.service.store.update(job, node_guarded=False)

    def test_restart_commit_pending_status_20_holds(self):
        self.assert_restart_holds_pending('commit_pending', 20)

    def test_restart_commit_pending_status_40_holds(self):
        self.assert_restart_holds_pending('commit_pending', 40)

    def test_restart_init_pending_holds(self):
        self.assert_restart_holds_pending('init_pending', 20)

    def test_restart_upload_pending_holds(self):
        self.assert_restart_holds_pending('upload_pending', 40)

    @unittest.skipIf(os.name == 'nt', 'coordinator restart fixture requires Linux gate locking')
    def test_new_process_holds_guarded_commit_before_observing_engine(self):
        job = self.submit()
        self.service.tick()
        self.service.store.update(job, phase='commit_pending')
        self.service.close()
        self.queue.close()
        crash = '''
import os, sys
from processing.prototype import Queue
queue = Queue(sys.argv[1], node_ids=('cluster','main'), treedar_node_ids=('main',), viewer_enabled=True)
os._exit(37)
'''
        crashed = subprocess.run([sys.executable, '-c', crash, str(self.root / 'state')],
                                 text=True, capture_output=True, timeout=15)
        self.assertEqual(crashed.returncode, 37, crashed.stderr)
        program = '''
import sys
from types import SimpleNamespace
from processing.prototype import Queue
from processing.viewer_service import ViewerService
root, identifier = sys.argv[1:]
queue = Queue(root, node_ids=('cluster','main'), treedar_node_ids=('main',), viewer_enabled=True)
config = SimpleNamespace(prototype_state_dir=root, viewer_max_input_bytes=1000,
    viewer_max_archive_bytes=1048576, local_node_url='http://unused-local', local_node_token='unused',
    main_node_url='http://unused-main', main_node_token='unused', local_max_photos=1000,
    local_max_input_bytes=1000)
class Engine:
    node_guarded = False
    def read(self, *args, **kwargs): raise AssertionError('uncertain journal must not be polled')
service = ViewerService(queue, config, engines={'cluster':Engine(),'main':Engine()},
    probe=lambda *args: (_ for _ in ()).throw(AssertionError('must hold before dispatch')))
service.tick()
assert service.info(identifier).get('ledgetopHeld') is True
assert service.store.task(identifier)['node_guarded'] == 1
service.close(); queue.close()
'''
        result = subprocess.run([sys.executable, '-c', program, str(self.root / 'state'), job],
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__': unittest.main()
