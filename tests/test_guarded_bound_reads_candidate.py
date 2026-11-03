import ssl
import unittest

from processing.guarded_native_client import GuardedNativeClient
from processing.viewer_service import EngineUnavailable
from tests.test_bound_node_job_candidate import BoundJobTests
from tests.test_guarded_native_https import TOKEN
from ops.native_node_fixture import ARCHIVE


class BoundReadTests(BoundJobTests):
    def client(self, server):
        context = ssl.create_default_context(cafile=str(self.root / 'server.pem'))
        context.load_cert_chain(self.root / 'client.pem', self.root / 'client.key')
        return GuardedNativeClient('127.0.0.1', server.server_port, context, TOKEN, timeout=10)

    def test_bound_reads_gate_results_until_commit_and_reject_wrong_job(self):
        server = self.start_listener()
        claim = self.envelope()
        client = self.client(server)
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
        client = self.client(server)
        client.upload(self.job, self.item)
        with self.assertRaises(EngineUnavailable): client.action('commit', self.job)
        self.assertEqual(client.read_bound(claim, 'output', line=0), {'held': True, 'value': []})
        with self.assertRaises(EngineUnavailable):
            with client.download_bound(claim): self.fail('pending download')
        self.assertEqual(self.backend.counts()['download'], 0)


if __name__ == '__main__':
    unittest.main()
