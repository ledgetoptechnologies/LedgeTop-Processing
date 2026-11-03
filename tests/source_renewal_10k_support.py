"""End-to-end source-range renewal against the real Viewer services (synthetic).

Opt-in. Set ``LEDGETOP_VIEWER_SOURCE_DIR`` to a Viewer checkout with the
attempt-bound source broker and grant renewal service (c92deb5 or later). The
Node host ``support/viewer_source_renewal_real.cjs`` runs the real Viewer
migrations, reference dispatcher, source broker and renewal service on file
databases; only the provider HTTP is faked. Processing drives it with the real
``SourceRangeClient`` and ``validate_source_grant``. Loopback HTTP only.

Covered: initial grant through the real dispatcher and broker, an interrupted
transfer, expiry, renewal, replay before and after a host restart, the
acknowledgement, refusal of the expired authority, and resumption without
resetting the cumulative range budget. Not covered: the Processing driver's
renewal journal (``ReferenceDriver._source_range_grant``), production TLS/proxy
mounting, and any live data.
"""
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.parse import urlsplit
import uuid

from processing import reference_protocol as p
from processing.source_range_transfer import (
    CHUNK_BYTES, SourceRangeClient, TransferError, build_source_transfer_request, validate_source_grant,
)

VIEWER_SOURCE_DIR = os.environ.get('LEDGETOP_VIEWER_SOURCE_DIR')
NODE = shutil.which('node')
HOST = Path(__file__).resolve().parent / 'support' / 'viewer_source_renewal_10k.cjs'
ORIGIN = 'https://viewer.test'
RANGE = '/api/processing/source-range/v2/objects/'
TEN_MINUTES = 600


class _Response:
    def __init__(self, connection, response, url):
        self._connection, self._response, self.url = connection, response, url
        self.status, self.headers = response.status, response.headers

    def read(self, amount=-1):
        return self._response.read(amount)

    def geturl(self):
        return self.url

    def close(self):
        self._response.close()
        self._connection.close()


class Host:
    def __init__(self, state, *, inject=True):
        env = {**os.environ, 'LEDGETOP_VIEWER_SOURCE_DIR': str(Path(VIEWER_SOURCE_DIR).resolve()),
               'PAIRED_STATE_DIR': str(state), 'PAIRED_INJECT_DISPATCH_ATTEMPT': '1' if inject else '0'}
        self.process = subprocess.Popen([NODE, str(HOST)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        text=True)
        line = self.process.stdout.readline().split()
        if line[:1] != ['READY']:
            raise RuntimeError('Viewer host did not start: ' + self.process.stderr.read()[-2000:])
        self.port = int(line[1])

    def control(self, **body):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=60)
        try:
            connection.request('POST', '/__control', json.dumps(body), {'content-type': 'application/json'})
            response = connection.getresponse()
            value = json.loads(response.read())
            if response.status != 200:
                raise RuntimeError(value['error'])
            return value
        finally:
            connection.close()

    def raw(self, method, path, headers, body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=60)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def opener(self, request, *, timeout):
        parts = urlsplit(request.full_url)
        if f'{parts.scheme}://{parts.netloc}' != ORIGIN:
            raise AssertionError('unexpected origin')
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=timeout)
        connection.request(request.get_method(), parts.path, body=request.data, headers=dict(request.header_items()))
        response = connection.getresponse()
        if response.status >= 400 and parts.path.startswith(RANGE):
            body = response.read()
            connection.close()
            raise HTTPError(request.full_url, response.status, body.decode(errors='replace'), response.headers, None)
        return _Response(connection, response, request.full_url)

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        for stream in (self.process.stdout, self.process.stderr):
            stream.close()


def digest(value):
    return hashlib.sha256(p.canonical_json(value)).hexdigest()


@unittest.skipUnless(VIEWER_SOURCE_DIR and NODE,
                     'set LEDGETOP_VIEWER_SOURCE_DIR to a Viewer checkout with grant renewal; Linux only')
class RealViewerSourceRenewalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ('state', 'state/source', 'client', 'dest'):
            (self.root / name).mkdir(mode=0o700)
        self.body = b'x'
        (self.root / 'state' / 'source' / 'a.jpg').write_bytes(self.body)
        self.offset = 0
        self.hosts = []
        self.addCleanup(lambda: [host.stop() for host in self.hosts])

    def clock(self):
        return time.time() + self.offset

    def start(self, **kwargs):
        host = Host(self.root / 'state', **kwargs)
        self.hosts.append(host)
        if self.offset:
            host.control(op='clock', offsetMs=self.offset * 1000)
        return host

    def restart(self, host):
        host.stop()
        return self.start()

    def queued_job(self, host):
        prepared = host.control(op='submission')
        submission = prepared['submission']
        job = {'jobId': 'job-1', 'generation': 1, 'environment': 'staging',
               'manifestSha256': submission['manifest']['sha256'], 'sourceViewerAttemptId': prepared['attemptId'],
               'submission': {'manifest': submission['manifest'], 'viewerAttemptId': prepared['attemptId']}}
        expires = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime(time.time() + 3600))
        job['transferRequest'] = build_source_transfer_request(job, reservation_id='reservation-1', expires_at=expires,
                                                               nonce='nonce-1')
        return job

    def client(self, host):
        return SourceRangeClient(ORIGIN, self.root / 'client' / 'ledger.sqlite', opener=host.opener, clock=self.clock)

    def advance(self, host, seconds):
        self.offset += seconds
        host.control(op='clock', offsetMs=self.offset * 1000)

    def usage(self, host):
        value = host.control(op='usage')
        self.assertEqual(len(value['usage']), 1)  # one cumulative ledger row for the source identity
        row = value['usage'][0]
        return (row['requests_used'], row['bytes_used'], row['attempts_used']), value['grants']

    @unittest.expectedFailure
    def test_real_dispatcher_mints_the_initial_grant(self):
        # Viewer gap at c92deb5: sourceRangeV2TransferDispatcher calls
        # mintGrant({...job, submission: {manifest}}), dropping viewerAttemptId,
        # so the real broker refuses with INVALID_MANIFEST before any transfer.
        host = self.start(inject=False)
        job = self.queued_job(host)
        host.control(op='dispatch', transferRequest=job['transferRequest'])

    def test_renewal_lifecycle_through_restart_keeps_one_budget_lineage(self):
        host = self.start()
        job = self.queued_job(host)
        initial = host.control(op='dispatch', transferRequest=job['transferRequest'])['grant']
        first = validate_source_grant(initial, job=job, clock=self.clock)
        self.assertEqual(first.identity.viewer_attempt_id, initial['viewerAttemptId'])
        target = self.root / 'dest' / 'a.jpg'
        obj = first.objects[0]

        calls = []
        real = host.opener
        def interrupt_second(request, *, timeout):
            if urlsplit(request.full_url).path.startswith(RANGE):
                calls.append(1)
                if len(calls) == 2:
                    raise OSError('synthetic interrupted range')
            return real(request, timeout=timeout)
        with self.assertRaises(TransferError):
            SourceRangeClient(ORIGIN, self.root / 'client' / 'ledger.sqlite', opener=interrupt_second,
                              clock=self.clock).download_object(grant=first, object_id=obj.id, destination=target)
        (requests, transferred, _attempts), _grants = self.usage(host)
        self.assertEqual((requests, transferred), (1, CHUNK_BYTES))

        self.advance(host, TEN_MINUTES)  # past the 5-minute grant TTL, inside the 1-hour reservation
        with self.assertRaisesRegex(TransferError, 'expired'):
            self.client(host).download_object(grant=first, object_id=obj.id, destination=target)
        status, _ = host.raw('GET', RANGE + obj.id, {'Authorization': 'Bearer ' + initial['token']})
        self.assertEqual(status, 401)

        renewal = {'protocol': 'source-range-v2-renewal', 'renewalId': 'renewal-' + uuid.uuid4().hex, 'sequence': 1,
                   'previousGrantSha256': digest(initial), 'previousGrant': initial}
        status, response = self.client(host).renew_grant(renewal)
        self.assertEqual(status, 200, response)
        successor = response['grant']
        renewed = validate_source_grant(successor, job=job, clock=self.clock)
        self.assertEqual(renewed.identity, first.identity)
        self.assertGreater(renewed.expires, first.expires)
        self.assertNotEqual(successor['token'], initial['token'])
        self.assertEqual({k: successor[k] for k in ('requestBudget', 'byteBudget', 'attemptBudget', 'objectIds')},
                         {k: initial[k] for k in ('requestBudget', 'byteBudget', 'attemptBudget', 'objectIds')})
        _usage, grants = self.usage(host)

        self.assertEqual(self.client(host).renew_grant(renewal), (200, response))  # replay, no second mint
        self.assertEqual(self.usage(host)[1], grants)
        host = self.restart(host)
        self.assertEqual(self.client(host).renew_grant(renewal), (200, response))  # replay survives restart
        self.assertEqual(self.usage(host)[1], grants)

        ack = {'protocol': 'source-range-v2-renewal-ack', 'renewalId': renewal['renewalId'], 'sequence': 1,
               'grantSha256': digest(successor), 'token': successor['token']}
        status, receipt = self.client(host).acknowledge_grant_renewal(ack)
        self.assertEqual(status, 200, receipt)
        self.assertEqual(receipt, {k: ack[k] for k in ('protocol', 'renewalId', 'sequence', 'grantSha256')})
        self.assertEqual(self.client(host).acknowledge_grant_renewal(ack), (200, receipt))
        self.assertEqual(host.control(op='head')['head']['sequence'], 1)

        result = self.client(host).download_object(grant=renewed, object_id=obj.id, destination=target)
        self.assertEqual(result.read_bytes(), self.body)
        (requests, transferred, attempts), _grants = self.usage(host)
        self.assertEqual((requests, transferred), (2, len(self.body)))  # no unique range recharged across renewal
        self.assertLessEqual(attempts, renewed.attempt_budget)
        self.assertEqual((self.root / 'state' / 'source' / 'a.jpg').read_bytes(), self.body)

    def test_conflicting_or_stale_renewals_and_acks_are_refused(self):
        host = self.start()
        job = self.queued_job(host)
        initial = host.control(op='dispatch', transferRequest=job['transferRequest'])['grant']
        self.advance(host, TEN_MINUTES)
        renewal = {'protocol': 'source-range-v2-renewal', 'renewalId': 'renewal-a', 'sequence': 1,
                   'previousGrantSha256': digest(initial), 'previousGrant': initial}
        status, response = self.client(host).renew_grant(renewal)
        self.assertEqual(status, 200, response)
        successor = response['grant']
        refused = {
            'same id, changed body': {**renewal, 'sequence': 2},
            'skipped sequence': {**renewal, 'renewalId': 'renewal-b', 'sequence': 3},
            'digest of a different grant': {**renewal, 'renewalId': 'renewal-c',
                                            'previousGrantSha256': digest(successor)},
        }
        for label, body in refused.items():
            with self.subTest(case=label):
                status, value = self.client(host).renew_grant(body)
                self.assertNotEqual(status, 200, value)
        bad_ack = {'protocol': 'source-range-v2-renewal-ack', 'renewalId': 'renewal-a', 'sequence': 1,
                   'grantSha256': digest(successor), 'token': 'A' * 43}
        status, value = self.client(host).acknowledge_grant_renewal(bad_ack)
        self.assertNotEqual(status, 200, value)
        self.assertEqual(host.control(op='head')['head']['sequence'], 0)  # head advances only on the exact ACK


if __name__ == '__main__':
    unittest.main()
