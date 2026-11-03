import hashlib
import hmac
import time
import unittest
import uuid

from processing import reference_protocol as protocol
from processing.host_mutation_journal import HostMutationJournal
from processing.node_gate import GateError, NodeGate
from tests.test_guarded_native_https import GuardedHTTPS, TOKEN

KEY = b'synthetic-independent-job-key-123456789'


class BoundJobTests(GuardedHTTPS):
    def envelope(self, **changes):
        claim = dict(schema='ledgetop-node-job-claim-v1', viewerJobId=str(uuid.uuid4()),
                     viewerGeneration=4, engineTaskId=self.job, nodeId='main',
                     authorityId=self.authority, gateGeneration=1, coordinatorOrigin='https://coordinator.invalid',
                     environment='production',
                     requestSha256=protocol.digest(self.task), nonce='a' * 64,
                     expiresAt=int(time.time()) + 300)
        claim.update(changes)
        mac = hmac.new(KEY, b'ledgetop-node-job-claim-v1\x00' + protocol.canonical_json(claim),
                       hashlib.sha256).hexdigest()
        return {'claim': claim, 'mac': mac}

    def setUp(self):
        super().setUp()
        self.proxy._job_claim_key = KEY

    def test_bound_claim_survives_reopen_and_prevents_job_substitution(self):
        server = self.start_listener()
        envelope = self.envelope()
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': envelope}),
                         (200, {'accepted': True, 'held': True}))
        request = dict(task=self.job, name=self.task['name'], outputs=None, options=[])
        operation = dict(operationId=self.proxy._op(self.job, 'init'), kind='init',
                         requestDigest=protocol.digest(request), nonce='b' * 64)
        # The handler reopens the actual gate and durable journal for every
        # inspection; it does not rely on an in-memory claim cache.
        code, result = self.request(server, '/v1/inspect-bound-mutation',
                                    {'jobClaim': envelope, 'operation': operation})
        self.assertEqual((code, result['state'], result['held']), (200, 'done', True))
        for changed in (self.envelope(viewerJobId=str(uuid.uuid4())),
                        self.envelope(viewerGeneration=5), self.envelope(environment='staging'),
                        self.envelope(coordinatorOrigin='https://other.invalid'),
                        self.envelope(engineTaskId=str(uuid.uuid4())),
                        {'claim': envelope['claim'], 'mac': '0' * 64}):
            self.assertEqual(self.request(server, '/v1/inspect-bound-mutation',
                                         {'jobClaim': changed, 'operation': operation})[0], 409)
        self.assertEqual(self.backend.counts()['initialize'], 1)

    def test_rejects_expired_wrong_request_and_forgery_before_gate(self):
        server = self.start_listener()
        expired = self.envelope(expiresAt=int(time.time()) - 1)
        altered = self.envelope(requestSha256='f' * 64)
        forged = self.envelope(); forged['mac'] = '0' * 64
        for env in (expired, altered, forged):
            self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': env})[0], 409)
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_new_proxy_instance_reads_binding_and_legacy_journal_fails_closed(self):
        from processing.guarded_native_proxy import GuardedNativeProxy
        env = self.envelope()
        self.proxy.initialize(self.task, job_claim=env, **self.auth)
        replacement = GuardedNativeProxy(engine=self.proxy.engine, engine_id='physical-engine',
            node_id='main', worker_id='node-proxy', authority_id=self.authority,
            coordinator_origin='https://coordinator.invalid', generation=1, bearer=TOKEN,
            spool_root=self.spool, job_claim_key=KEY)
        request = dict(task=self.job, name=self.task['name'], outputs=None, options=[])
        op = dict(operationId=self.proxy._op(self.job, 'init'), kind='init',
                  requestDigest=protocol.digest(request), nonce='b' * 64)
        result = replacement.inspect_bound_mutation(env, op, **self.auth)
        self.assertEqual((result['held'], result['state']), (True, 'done'))
        with self.assertRaises(GateError):
            replacement.inspect_bound_mutation(self.envelope(viewerJobId=str(uuid.uuid4())), op, **self.auth)

    def test_enforced_mode_disables_unbound_init(self):
        self.proxy._require_job_claim = True
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, **self.auth)
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(self.backend.counts()['initialize'], 0)
        self.proxy.initialize(self.task, job_claim=self.envelope(), **self.auth)
        self.assertEqual(self.backend.counts()['initialize'], 1)

    def test_signed_claim_cannot_be_changed_after_authentication(self):
        env = self.envelope()
        env['claim']['viewerGeneration'] += 1
        with self.assertRaises(GateError):
            self.proxy.initialize(self.task, job_claim=env, **self.auth)
        with NodeGate('physical-engine') as gate:
            self.assertIsNone(gate.snapshot()['reservation'])
        self.assertEqual(self.backend.counts()['initialize'], 0)

    def test_no_retroactive_binding_and_claim_persisted_before_engine(self):
        env = self.envelope()
        original = self.proxy.engine.initialize
        observed = []
        def initialize(task):
            # Read directly from a second journal SQLite connection while
            # the node gate is held by the outer init operation.
            from pathlib import Path
            import sqlite3
            journal_path = next((self.root / 'gate').glob('mutations-*.sqlite'))
            with sqlite3.connect(journal_path) as db:
                observed.extend(db.execute('SELECT claim_sha256 FROM viewer_job_binding').fetchall())
            return original(task)
        self.proxy.engine.initialize = initialize
        self.proxy.initialize(self.task, job_claim=env, **self.auth)
        self.assertEqual(observed, [(protocol.digest(env['claim']),)])
        with NodeGate('physical-engine') as gate:
            journal = HostMutationJournal(gate, gate.snapshot()['reservation']['receiptId']).open()
            try:
                with self.assertRaises(GateError): journal.bind_viewer_job(env['claim'])
            finally: journal.close()


if __name__ == '__main__': unittest.main()
