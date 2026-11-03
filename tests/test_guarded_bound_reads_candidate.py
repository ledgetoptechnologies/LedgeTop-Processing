import ssl
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable
from tests.test_bound_node_job_candidate import BoundJobTests, KEY
from tests.test_guarded_native_https import TOKEN
from tests.guarded_test_signer import operation_signer
from ops.native_node_fixture import ARCHIVE


class BoundReadTests(BoundJobTests):
    def client(self, server, claim):
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        return GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10,
                                   job_claim=claim, operation_signer=operation_signer(self.proxy, KEY, claim))

    def test_bound_reads_gate_results_until_commit_and_reject_wrong_job(self):
        server = self.start_listener()
        claim = self.envelope()
        client = self.client(server, claim)
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': claim})[0], 200)
        self.assertEqual(client.read_bound(claim, 'info')['held'], True)
        with self.assertRaises(EngineUnavailable):
            with client.download_bound(claim):
                self.fail('uncommitted download')
        self.assertEqual(self.backend.counts()['download'], 0)
        client.upload(self.job, self.item)
        client.action('commit', self.job)
        info = client.read_bound(claim, 'info')
        self.assertEqual((info['held'], info['value']['status']['code']), (False, 40))
        self.assertEqual(client.read_bound(claim, 'output', line=1),
                         {'held': False, 'value': ['', 'synthetic phase two']})
        with client.download_bound(claim) as result:
            self.assertEqual(result.read(), ARCHIVE)
        self.assertEqual(self.backend.counts()['download'], 1)
        wrong = self.envelope(viewerGeneration=5)
        with self.assertRaises(EngineUnavailable): client.read_bound(wrong, 'info')
        with self.assertRaises(EngineUnavailable):
            with client.download_bound(wrong): self.fail('wrong job')
        self.assertEqual(self.backend.counts()['download'], 1)

    def test_pending_commit_does_not_expose_output_or_archive(self):
        server = self.start_listener()
        claim = self.envelope()
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': claim})[0], 200)
        self.commit_reply(False)
        client = self.client(server, claim)
        client.upload(self.job, self.item)
        with self.assertRaises(EngineUnavailable): client.action('commit', self.job)
        self.assertEqual(client.read_bound(claim, 'output', line=0), {'held': True, 'value': []})
        with self.assertRaises(EngineUnavailable):
            with client.download_bound(claim): self.fail('pending download')
        self.assertEqual(self.backend.counts()['download'], 0)

    def test_reads_and_download_reject_engine_or_parsed_target_replacement(self):
        server = self.start_listener()
        claim = self.envelope()
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': claim})[0], 200)
        client = self.client(server, claim)
        client.upload(self.job, self.item)
        client.action('commit', self.job)
        engine = self.proxy.engine
        with patch.object(engine.client, 'parsed', urlsplit('http://192.0.2.1:3000')):
            with self.assertRaises(EngineUnavailable): client.read_bound(claim, 'info')
            with self.assertRaises(EngineUnavailable):
                with client.download_bound(claim): self.fail('diverged target')
        with patch.object(self.proxy, 'engine', object()):
            with self.assertRaises(EngineUnavailable): client.read_bound(claim, 'info')
            with self.assertRaises(EngineUnavailable):
                with client.download_bound(claim): self.fail('replaced engine')
        self.assertEqual(self.backend.counts()['download'], 0)

    def test_read_and_download_use_captured_endpoint_after_validation(self):
        server = self.start_listener()
        claim = self.envelope()
        self.assertEqual(self.request(server, '/v1/initialize-bound', {'task': self.task, 'jobClaim': claim})[0], 200)
        client = self.client(server, claim)
        client.upload(self.job, self.item)
        client.action('commit', self.job)
        engine = self.proxy.engine
        pinned = engine.client
        original = pinned.fixed_origin
        def switch_after_check():
            fixed = original()
            engine.origin = 'http://192.0.2.1:3000'
            return fixed
        try:
            with patch.object(pinned, 'fixed_origin', side_effect=switch_after_check):
                self.assertEqual(client.read_bound(claim, 'info')['value']['status']['code'], 40)
            engine.origin = self.backend.origin
            # Check a swap immediately before the archive network request,
            # after its preceding NodeODM status check has already succeeded.
            calls = []
            def switch_second():
                fixed = original()
                calls.append(True)
                if len(calls) == 2: engine.origin = 'http://192.0.2.1:3000'
                return fixed
            with patch.object(pinned, 'fixed_origin', side_effect=switch_second):
                with client.download_bound(claim) as result:
                    self.assertEqual(result.read(), ARCHIVE)
            self.assertEqual(self.backend.counts()['download'], 1)
        finally:
            engine.origin = self.backend.origin


if __name__ == '__main__':
    unittest.main()
