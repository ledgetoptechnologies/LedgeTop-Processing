import hashlib
import hmac
import ssl
import time
import unittest
import uuid
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable
from tests.test_bound_node_job_candidate import BoundJobTests, KEY, TOKEN


class OperationClaimTests(BoundJobTests):
    def signed_operation(self, job, kind, request, **changes):
        if kind == 'upload':
            from processing.viewer_tasks import TaskStore
            key = hashlib.sha256(TaskStore._filename(request['name']).encode('ascii')).hexdigest()
            operation_id = self.proxy._op(self.job, kind, key)
        else:
            operation_id = self.proxy._op(self.job, kind)
        op = dict(schema='ledgetop-node-operation-claim-v1', jobClaimSha256=protocol.digest(job['claim']),
                  operationId=operation_id, kind=kind, requestSha256=protocol.digest(request),
                  expiresAt=int(time.time()) + 300)
        op.update(changes)
        mac = hmac.new(KEY, b'ledgetop-node-operation-claim-v1\x00' + protocol.canonical_json(op),
                       hashlib.sha256).hexdigest()
        return {'jobClaim': job, 'operation': op, 'mac': mac}

    def client(self, server, job, signer):
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        return GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
                                   job_claim=job, operation_signer=signer)

    def test_real_tls_bound_upload_commit_and_replay_are_fenced(self):
        self.proxy._require_job_claim = True
        self.commit_reply(True)
        server, job = self.start_listener(), self.envelope()
        client = self.client(server, job, lambda kind, request: self.signed_operation(job, kind, request))
        client.initialize(self.task)
        client.upload(self.job, self.item)
        self.assertEqual(client.action('commit', self.job), {'uuid': self.job})
        with self.assertRaises(EngineUnavailable): client.action('commit', self.job)
        self.assertEqual(self.backend.counts()['upload'], 1)
        self.assertEqual(self.backend.counts()['commit'], 1)

    def test_unsigned_substituted_and_expired_claims_never_spool_or_mutate(self):
        self.proxy._require_job_claim = True
        server, job = self.start_listener(), self.envelope()
        self.proxy.initialize(self.task, job_claim=job, **self.auth)
        candidates = [lambda kind, req: None,
                      lambda kind, req: self.signed_operation(job, kind, req, expiresAt=int(time.time()) - 1),
                      lambda kind, req: self.signed_operation(job, kind, req, requestSha256='f' * 64),
                      lambda kind, req: self.signed_operation(self.envelope(viewerJobId=str(uuid.uuid4())), kind, req)]
        for signer in candidates:
            with self.subTest(signer=signer), self.assertRaises(EngineUnavailable):
                self.client(server, job, signer).upload(self.job, self.item)
        self.assertTrue(server.wait_drained(5))
        self.assertEqual(self.backend.counts()['upload'], 0)
        self.assertEqual(list(self.spool.glob('incoming-*')), [])
        self.assertEqual(server.spool_budget.inspect()['reservations'], [])
        self.assertEqual(self.request(server, '/v1/commit', {'task': self.job})[0], 409)
        self.assertEqual(self.backend.counts()['commit'], 0)

    def test_expiry_after_durable_intent_holds_without_native_call(self):
        self.proxy._require_job_claim = True
        job = self.envelope()
        self.proxy.initialize(self.task, job_claim=job, **self.auth)
        request = dict(task=self.job)
        claim = self.signed_operation(job, 'commit', request)
        with patch('processing.node_operation_claim._now', side_effect=[int(time.time()), int(time.time()) + 400]):
            with self.assertRaises(Exception):
                self.proxy.commit(self.job, operation_claim=claim, **self.auth)
        self.assertEqual(self.backend.counts()['commit'], 0)
        with self.assertRaises(Exception):
            self.proxy.commit(self.job, operation_claim=self.signed_operation(job, 'commit', request), **self.auth)
        self.assertEqual(self.backend.counts()['commit'], 0)

    def test_bound_job_cannot_fall_back_to_unbound_route_when_optional(self):
        self.proxy.initialize(self.task, job_claim=self.envelope(), **self.auth)
        with self.assertRaises(Exception):
            self.proxy.commit(self.job, **self.auth)
        self.assertEqual(self.backend.counts()['commit'], 0)

    def test_expiry_during_tls_body_discards_only_unissued_spool_and_retry_works(self):
        self.proxy._require_job_claim = True
        server, job = self.start_listener(), self.envelope()
        client = self.client(server, job, lambda kind, request: self.signed_operation(job, kind, request))
        client.initialize(self.task)
        now = int(time.time())
        # Preflight is still valid; final authorization occurs only after the
        # TLS body is received, synced and accounted for, before journal.begin.
        with patch('processing.node_operation_claim._now', side_effect=[now, now + 400]):
            with self.assertRaises(EngineUnavailable):
                client.upload(self.job, self.item)
        self.assertTrue(server.wait_drained(5))
        self.assertEqual(self.backend.counts()['upload'], 0)
        self.assertEqual(list(self.spool.glob('incoming-*')), [])
        self.assertEqual(server.spool_budget.inspect()['reservations'], [])
        client.upload(self.job, self.item)
        self.assertEqual(self.backend.counts()['upload'], 1)

    def test_expiry_after_upload_intent_retains_spool_and_blocks_retry(self):
        self.proxy._require_job_claim = True
        server, job = self.start_listener(), self.envelope()
        client = self.client(server, job, lambda kind, request: self.signed_operation(job, kind, request))
        client.initialize(self.task)
        now = int(time.time())
        with patch('processing.node_operation_claim._now', side_effect=[now, now, now + 400]):
            with self.assertRaises(EngineUnavailable):
                client.upload(self.job, self.item)
        self.assertTrue(server.wait_drained(5))
        self.assertEqual(self.backend.counts()['upload'], 0)
        self.assertEqual(len(list(self.spool.glob('incoming-*'))), 1)
        self.assertEqual(len(server.spool_budget.inspect()['reservations']), 1)
        with self.assertRaises(EngineUnavailable): client.upload(self.job, self.item)
        self.assertTrue(server.wait_drained(5))
        self.assertEqual(len(list(self.spool.glob('incoming-*'))), 1)
        self.assertEqual(self.backend.counts()['upload'], 0)


if __name__ == '__main__': unittest.main()
