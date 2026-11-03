import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from ops.native_node_fixture import NativeNodeFixture
from processing.guarded_native_proxy import GuardedNativeProxy
from processing.host_mutation_journal import HostMutationJournal, MutationHeld
from processing.node_gate import GateError, NodeGate
from processing.odx_smoke_worker import SmokeError
from processing.viewer_service import NativeEngine

TOKEN = 'synthetic-guard-token-123456'


class GuardedNativeProxyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.gate_patch = patch('processing.node_gate.canonical_gate_path', return_value=self.root / 'gate')
        self.gate_patch.start()
        self.addCleanup(self.gate_patch.stop)
        self.backend = NativeNodeFixture(self.root / 'native.sqlite', synthetic_enabled=True).start()
        self.addCleanup(self.backend.close)
        self.spool = self.root / 'spool'
        self.spool.mkdir(mode=0o700)
        file = self.spool / 'photo.jpg'
        file.write_bytes(b'abc')
        if os.name != 'nt': file.chmod(0o600)
        self.item = dict(path=str(file), name='photo.jpg', bytes=3, sha256=hashlib.sha256(b'abc').hexdigest())
        self.authority = str(uuid.uuid4())
        self.job = str(uuid.uuid4())
        self.auth = dict(bearer=TOKEN, protected_transport=True)
        self.task = dict(engine_uuid=self.job, name='synthetic', outputs=None, options=[])
        with NodeGate('physical-engine') as gate:
            gate.provision('main', 'https://coordinator.invalid', self.authority)
        self.proxy = GuardedNativeProxy(engine=NativeEngine(self.backend.origin), engine_id='physical-engine',
            node_id='main', worker_id='node-proxy', authority_id=self.authority,
            coordinator_origin='https://coordinator.invalid', generation=1, bearer=TOKEN, spool_root=self.spool)

    def commit_reply(self, matching):
        original = self.backend._dispatch
        def dispatch(method, path, headers, body):
            status, value, binary = original(method, path, headers, body)
            if method == 'POST' and path.startswith('/task/new/commit/') and status == 200:
                value = {'uuid': self.job if matching else str(uuid.uuid4())}
            return status, value, binary
        self.backend._dispatch = dispatch

    def journal(self):
        with NodeGate('physical-engine') as gate:
            state = gate.snapshot()
            journal = HostMutationJournal(gate, state['reservation']['receiptId']).open()
            try: return journal.db.execute('SELECT kind,state FROM operations ORDER BY seq').fetchall()
            finally: journal.close()

    def test_auth_before_any_engine_or_gate_mutation(self):
        for args in (dict(bearer='incorrect', protected_transport=True),
                     dict(bearer=TOKEN, protected_transport=False)):
            with self.assertRaises(GateError): self.proxy.initialize(self.task, **args)
        with NodeGate('physical-engine') as gate: self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_real_http_journal_and_fences_other_task_and_duplicate(self):
        self.commit_reply(True)
        self.proxy.initialize(self.task, **self.auth)
        with self.assertRaises(GateError):
            self.proxy.initialize({**self.task, 'engine_uuid': str(uuid.uuid4())}, **self.auth)
        self.proxy.upload(self.job, self.item, **self.auth)
        with self.assertRaises(MutationHeld): self.proxy.upload(self.job, self.item, **self.auth)
        self.assertEqual(self.proxy.commit(self.job, **self.auth), {'uuid': self.job})
        with self.assertRaises(MutationHeld): self.proxy.commit(self.job, **self.auth)
        self.assertEqual(self.journal(), [('init','done'),('upload','done'),('commit','done')])
        self.assertEqual(self.backend.counts(), dict(initialize=1, upload=1, commit=1, download=0))
        with NodeGate('physical-engine') as gate:
            self.assertEqual(gate.snapshot()['reservation']['job']['id'], self.job)

    def test_wrong_commit_ack_persists_pending_and_never_replays(self):
        self.commit_reply(False)
        self.proxy.initialize(self.task, **self.auth)
        self.proxy.upload(self.job, self.item, **self.auth)
        with self.assertRaises((SmokeError, MutationHeld)): self.proxy.commit(self.job, **self.auth)
        with self.assertRaises(MutationHeld): self.proxy.commit(self.job, **self.auth)
        self.assertEqual(self.journal()[-1], ('commit','pending'))
        self.assertEqual(self.backend.counts()['commit'], 1)

    def test_file_outside_spool_does_not_reach_engine(self):
        self.proxy.initialize(self.task, **self.auth)
        outside = self.root / 'outside.jpg'
        outside.write_bytes(b'abc')
        with self.assertRaises(GateError): self.proxy.upload(self.job, {**self.item, 'path': str(outside)}, **self.auth)
        self.assertEqual(self.backend.counts()['upload'], 0)

    def test_lost_upload_response_stays_pending_across_new_adapter_instance(self):
        original = self.backend._dispatch
        def dispatch(method, path, headers, body):
            status, value, binary = original(method, path, headers, body)
            if method == 'POST' and '/task/new/upload/' in path and status == 200:
                raise OSError('synthetic response lost after upload')
            return status, value, binary
        self.backend._dispatch = dispatch
        self.proxy.initialize(self.task, **self.auth)
        with self.assertRaises(SmokeError): self.proxy.upload(self.job, self.item, **self.auth)
        replacement = GuardedNativeProxy(engine=NativeEngine(self.backend.origin), engine_id='physical-engine',
            node_id='main', worker_id='node-proxy', authority_id=self.authority,
            coordinator_origin='https://coordinator.invalid', generation=1, bearer=TOKEN, spool_root=self.spool)
        with self.assertRaises(MutationHeld): replacement.upload(self.job, self.item, **self.auth)
        with self.assertRaises(MutationHeld): replacement.commit(self.job, **self.auth)
        self.assertEqual(self.journal(), [('init','done'),('upload','pending')])
        self.assertEqual(self.backend.counts()['upload'], 1)
