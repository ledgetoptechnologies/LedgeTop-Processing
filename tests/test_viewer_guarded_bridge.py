"""Synthetic Viewer queue → in-process guard → real loopback NodeODM wire.

The local wrapper is explicitly a test seam, not an authenticated network
transport. It copies only synthetic data into a private local spool. The tests
show both submission and the unresolved completion/reservation disconnect.
"""
import io
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from ops.native_node_fixture import NativeNodeFixture
from processing.guarded_native_proxy import GuardedNativeProxy
from processing.host_mutation_journal import HostMutationJournal
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.viewer_service import NativeEngine, ViewerService

TOKEN = 'synthetic-guard-token-123456'
AUTH = dict(bearer=TOKEN, protected_transport=True)


class InProcessTestBridge:
    """Not production transport: filesystem copy and trusted boolean are fake."""
    def __init__(self, proxy, native, spool):
        self.proxy, self.native, self.spool = proxy, native, spool

    def initialize(self, task):
        selected = {key: task[key] for key in ('name', 'engine_uuid', 'outputs', 'options')}
        self.proxy.initialize(selected, **AUTH)

    def upload(self, identifier, item):
        dst = self.spool / (str(uuid.uuid4()) + '.jpg')
        shutil.copyfile(item['path'], dst)
        if os.name != 'nt': dst.chmod(0o600)
        self.proxy.upload(identifier, {key: str(dst) if key == 'path' else item[key] for key in ('path', 'name', 'bytes', 'sha256')}, **AUTH)

    def action(self, action, identifier):
        if action != 'commit':
            raise RuntimeError('test bridge does not support cancellation')
        return self.proxy.commit(identifier, **AUTH)

    def read(self, *args, **kwargs):
        return self.native.read(*args, **kwargs)

    def download(self, *args, **kwargs):
        return self.native.download(*args, **kwargs)


class ViewerGuardedBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.gate_patch.start(); self.addCleanup(self.gate_patch.stop)
        self.native_fixture = NativeNodeFixture(self.root / 'native.sqlite', synthetic_enabled=True).start()
        self.addCleanup(self.native_fixture.close)
        original = self.native_fixture._dispatch
        def node(method, path, headers, body):
            status, response, binary = original(method, path, headers, body)
            if method == 'POST' and path.startswith('/task/new/commit/') and status == 200:
                response = {'uuid': path.rsplit('/', 1)[-1]}
            if method == 'GET' and path.endswith('/info') and status == 200:
                response['progress'] = 100 if response['status']['code'] == 40 else 0
            return status, response, binary
        self.native_fixture._dispatch = node
        spool = self.root / 'node-spool'; spool.mkdir(mode=0o700)
        authority = str(uuid.uuid4())
        with NodeGate('synthetic-physical-engine') as gate:
            gate.provision('cluster', 'https://coordinator.invalid', authority)
        native = NativeEngine(self.native_fixture.origin)
        self.proxy = GuardedNativeProxy(engine=native, engine_id='synthetic-physical-engine',
            node_id='cluster', worker_id='node-proxy', authority_id=authority,
            coordinator_origin='https://coordinator.invalid', generation=1, bearer=TOKEN, spool_root=spool)
        engine = InProcessTestBridge(self.proxy, native, spool)
        config = SimpleNamespace(prototype_state_dir=str(self.root / 'state'),
            viewer_max_input_bytes=1000, viewer_max_archive_bytes=1024 ** 2,
            local_node_url='http://unused-local', local_node_token='private-cluster',
            main_node_url='http://unused-main', main_node_token='private-main',
            local_max_photos=1000, local_max_input_bytes=1000)
        self.queue = Queue(self.root / 'state', node_ids=('cluster', 'main'),
                           treedar_node_ids=('main',), viewer_enabled=True)
        self.addCleanup(self.queue.close)
        def probe(role, origin, token):
            # This is the existing ViewerService probe contract: it is unaware
            # of the host guard and cannot report its retained reservation.
            return {'ready': role == 'local', 'maxImages': 1000, 'taskQueueCount': 0}
        self.service = ViewerService(self.queue, config, engines={'cluster': engine, 'main': engine}, probe=probe)
        self.addCleanup(self.service.close)
        self.service._catalog = lambda: {'ready': True, 'commonOptions': []}
        self.service._selected_options = lambda _: []

    def submit(self):
        identifier = str(uuid.uuid4())
        self.service.initialize(identifier, 'synthetic', [], None, 'production')
        self.service.upload(identifier, 'photo.jpg', io.BytesIO(b'abc'), 3)
        self.service.commit(identifier)
        return identifier

    def journal_state(self):
        with NodeGate('synthetic-physical-engine') as gate:
            snapshot = gate.snapshot()
            journal = HostMutationJournal(gate, snapshot['reservation']['receiptId']).open()
            try: return snapshot, journal.db.execute('SELECT kind,state FROM operations ORDER BY seq').fetchall()
            finally: journal.close()

    def test_submission_journal_and_completed_job_still_holds_node(self):
        first = self.submit()
        self.assertEqual(self.service.info(first)['status']['code'], 10)
        self.service.tick()
        task = self.service.store.task(first)
        self.assertEqual(task['node_id'], 'cluster')
        self.assertEqual(self.native_fixture.counts(), dict(initialize=1, upload=1, commit=1, download=0))
        snapshot, operations = self.journal_state()
        self.assertEqual(snapshot['reservation']['job']['id'], task['engine_uuid'])
        self.assertEqual(operations, [('init','done'), ('upload','done'), ('commit','done')])
        self.service.tick()
        self.assertEqual(self.service.info(first)['status']['code'], 40)
        # No Viewer-acceptance acknowledgment has occurred. Guard reservation
        # remains, but the existing scheduler probe does not consult it.
        second = self.submit()
        self.service.tick()
        self.assertTrue(self.service.info(second).get('ledgetopHeld'))
        self.assertEqual(self.native_fixture.counts()['initialize'], 1)
        self.assertEqual(self.journal_state()[0]['reservation']['job']['id'], task['engine_uuid'])


    def test_lost_upload_response_holds_queue_and_never_replays(self):
        original = self.native_fixture._dispatch
        def disconnect_after_commit(method, path, headers, body):
            result = original(method, path, headers, body)
            if method == 'POST' and '/task/new/upload/' in path and result[0] == 200:
                raise OSError('synthetic upload reply lost')
            return result
        self.native_fixture._dispatch = disconnect_after_commit
        first = self.submit()
        self.service.tick()
        self.assertTrue(self.service.info(first).get('ledgetopHeld'))
        snapshot, operations = self.journal_state()
        self.assertEqual(operations, [('init', 'done'), ('upload', 'pending')])
        self.assertEqual(self.native_fixture.counts()['upload'], 1)
        self.service.tick()
        self.assertEqual(self.native_fixture.counts()['upload'], 1)
        self.assertEqual(self.journal_state()[0]['reservation'], snapshot['reservation'])

if __name__ == '__main__': unittest.main()


