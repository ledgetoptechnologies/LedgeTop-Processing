"""Processing driver renewal journal against the real Viewer services (synthetic).

Opt-in. Set ``LEDGETOP_VIEWER_SOURCE_DIR`` to a Viewer checkout with the
attempt-bound source broker, grant renewal service and dispatcher mint fix
(4bb8d33 or later). ``support/viewer_source_renewal_real.cjs`` runs the real
Viewer migrations, reference dispatcher, source broker and grant renewal service
on file databases; only the Viewer's provider HTTP is faked. On the Processing
side the actual ``ReferenceDriver`` claims an activated job made from the
Viewer's own submission, receives the grant the real broker minted through the
coordinator store, fetches ranges with its ``SourceRangeClient`` and renews
through ``_source_range_grant``'s durable request/ACK journal.

Simulated, not proven: Viewer provider HTTP and protected transport, the
coordinator HTTP API (the driver test fixture calls ReferenceControlAPI in
process), the disabled runtime gates opened only inside this test
(``_activation_bound_grant_ready`` by the driver fixture and
``_v3_source_contract_ready`` here), the JCS v3 envelope built in Python rather
than by the Viewer prepared outbox,
dropped Viewer admission triggers and a Viewer restart without migrations.
Nothing here enables a runtime flag, contacts a live service or uses client data.
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
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit
import uuid

from processing.source_range_transfer import CHUNK_BYTES

VIEWER_SOURCE_DIR = os.environ.get('LEDGETOP_VIEWER_SOURCE_DIR')
NODE = shutil.which('node')
HOST = Path(__file__).resolve().parent / 'support' / 'viewer_source_renewal_real.cjs'
ORIGIN = 'https://viewer.invalid'
RANGE = '/api/processing/source-range/v2/objects/'
RENEW = '/api/processing/source-range/v2/grants/renew'
# The driver fixture reserves for 240 s; a 60 s Viewer grant must be renewed inside that lease.
GRANT_TTL = 60
PAST_GRANT = 120


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


class ViewerHost:
    def __init__(self, state, attempt):
        env = {**os.environ, 'LEDGETOP_VIEWER_SOURCE_DIR': str(Path(VIEWER_SOURCE_DIR).resolve()),
               'PAIRED_STATE_DIR': str(state), 'PAIRED_ATTEMPT_ID': attempt,
               'PAIRED_MAX_GRANT_TTL_MS': str(GRANT_TTL * 1000)}
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

    def stop(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        for stream in (self.process.stdout, self.process.stderr):
            stream.close()


@unittest.skipUnless(VIEWER_SOURCE_DIR and NODE and os.name == 'posix',
                     'set LEDGETOP_VIEWER_SOURCE_DIR to a Viewer checkout with grant renewal; Linux only')
class RealViewerDriverRenewalTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_driver import ReferenceDriverTests
        self.fixture = ReferenceDriverTests(
            'test_expired_source_grant_is_durably_renewed_and_ack_replays_without_new_grant')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / 'viewer'
        (self.state / 'source').mkdir(parents=True, mode=0o700)
        self.body = hashlib.shake_256(b'paired driver renewal source').digest(CHUNK_BYTES + 5)
        (self.state / 'source' / 'a.jpg').write_bytes(self.body)
        self.attempt = str(uuid.uuid4())
        self.offset = 0
        self.hosts = []
        self.addCleanup(lambda: [host.stop() for host in self.hosts])
        self.lose = set()      # wire kinds whose next response is lost after the Viewer handled it
        self.range_failures = 0
        self.calls = {'range': 0, 'renew': 0, 'ack': 0}
        self.host = self.start_host()

    def clock(self):
        return time.time() + self.offset

    def start_host(self):
        host = ViewerHost(self.state, self.attempt)
        self.hosts.append(host)
        if self.offset:
            host.control(op='clock', offsetMs=self.offset * 1000)
        return host

    def opener(self, request, *, timeout):
        parts = urlsplit(request.full_url)
        if f'{parts.scheme}://{parts.netloc}' != ORIGIN:
            raise AssertionError('Processing requested an unexpected Viewer origin')
        kind = ('range' if parts.path.startswith(RANGE) else 'ack' if parts.path == RENEW + '/ack'
                else 'renew' if parts.path == RENEW else None)
        if kind is None:
            raise AssertionError('Processing requested an unexpected Viewer route')
        self.calls[kind] += 1
        if kind == 'range' and self.range_failures:
            self.range_failures -= 1
            raise OSError('synthetic interrupted range')
        connection = http.client.HTTPConnection('127.0.0.1', self.host.port, timeout=timeout)
        connection.request(request.get_method(), parts.path, body=request.data, headers=dict(request.header_items()))
        response = connection.getresponse()
        if kind in self.lose:  # the Viewer has committed; only the reply is lost
            self.lose.discard(kind)
            response.read()
            connection.close()
            raise OSError('synthetic lost ' + kind + ' response')
        if response.status >= 400 and kind == 'range':
            body = response.read()
            connection.close()
            raise HTTPError(request.full_url, response.status, body.decode(errors='replace'), response.headers, None)
        return _Response(connection, response, request.full_url)

    def driver(self):
        driver = self.fixture.make_driver(source_range_v2_enabled=True, state_path=self.fixture.root / 'driver-real-viewer.sqlite',
                                          clock=self.clock)
        driver.source_range_client.origin = ORIGIN
        driver.source_range_client._open = self.opener
        self.fixture.driver = driver
        return driver

    def submit_viewer_job(self):
        """Prepare and activate the Viewer's own submission as a JCS v3 envelope bound to the host's attempt."""
        import rfc8785
        from processing import prepared_envelope_jcs_candidate as jcs
        from processing.reference_control import ReferenceControlAPI
        from processing.reference_store import ReferenceStore
        from tests.test_reference_driver import TOKEN, VIEWER
        submission = self.host.control(op='submission')['submission']
        odm = submission['pipeline']['odm']
        # Only prepared v3 provenance may carry a Viewer attempt into the source
        # reservation, so the coordinator store runs with its JCS candidate enabled.
        store = ReferenceStore(self.fixture.queue, jcs_runtime_enabled=True)
        # Explicit synthetic gate, like the fixture's _activation_bound_grant_ready
        # patch: the v3 source provenance contract stays disabled in runtime.
        gate = patch.object(ReferenceStore, '_v3_source_contract_ready', return_value=True)
        gate.start()
        self.addCleanup(gate.stop)
        self.fixture.store = store
        # The driver fixture's coordinator API pins the contract-file fingerprints;
        # rebind it to the Viewer submission's real capability and options fingerprints.
        self.fixture.api = ReferenceControlAPI(store, viewer_bearers={VIEWER: 'staging'},
            worker_bearers={TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint=submission['capabilityFingerprint'], options_fingerprint=odm['optionsFingerprint'])
        # The synthetic engine advertises the Viewer submission's ODM options; the
        # driver refreshes its allowed set from the engine's /options catalog.
        self.fixture.transport.options.append(dict(name='dsm', type='bool', value=False, help='Synthetic DSM flag.'))
        policy = dict(capabilityFingerprint=submission['capabilityFingerprint'], optionsFingerprint=odm['optionsFingerprint'],
                      availableEngines=['odm'], optionDefinitions={name: dict(type='bool', domain=None) for name in odm['options']})
        envelope = dict(schemaVersion=3, protocol=jcs.PROTOCOL, submission=submission, viewerTaskId=str(uuid.uuid4()),
                        viewerAttemptId=self.attempt, activationId=str(uuid.uuid4()))
        job, _ = store.prepare_viewer_envelope_jcs_v3(json.dumps(envelope).encode(), authenticated_principal='synthetic-viewer',
                                                      authenticated_environment='staging', trusted_policy=policy)
        digest = hashlib.sha256(jcs.DOMAIN + rfc8785.dumps(envelope)).hexdigest()
        store.activate_viewer_envelope_jcs_v3(submission['submissionId'], dict(activationId=envelope['activationId'],
            viewerTaskId=envelope['viewerTaskId'], viewerAttemptId=self.attempt, envelopeSha256=digest),
            authenticated_principal='synthetic-viewer', authenticated_environment='staging', trusted_policy=policy)
        return job

    def stage(self, driver, job):
        with driver._db() as db:
            row = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
        return (row[0], json.loads(row[1])) if row else (None, None)

    def step_until(self, driver, job, stages, *, limit=40):
        seen = []
        for _ in range(limit):
            result = driver.step()
            if result['state'] == 'held':
                self.fail('unexpected driver hold: ' + str(result.get('reason', '')))
            stage, data = self.stage(driver, job)
            seen.append((result['state'], stage))
            if stage in stages:
                return result, stage, data
        self.fail('driver never reached ' + repr(stages) + ': ' + repr(seen[-6:]))

    def usage(self):
        value = self.host.control(op='usage')
        self.assertEqual(len(value['usage']), 1)  # one cumulative broker ledger row for the source identity
        row = value['usage'][0]
        return row['requests_used'], row['bytes_used'], row['attempts_used']

    def test_driver_renewal_journal_against_real_viewer_through_restarts(self):
        job = self.submit_viewer_job()
        driver = self.driver()
        _result, _stage, data = self.step_until(driver, job, ('awaiting_grant',))
        transfer = data['claim']['transferRequest']
        self.assertEqual(transfer['viewerAttemptId'], self.attempt)
        grant = self.host.control(op='dispatch', transferRequest=transfer, jobId=job['jobId'],
                                  generation=job['generation'])['grant']
        self.fixture.store.receive_transfer_grant(job['jobId'], grant)

        self.range_failures = 0
        ranges_before = self.calls['range']
        def fail_second_range(request, *, timeout, real=self.opener):
            if urlsplit(request.full_url).path.startswith(RANGE) and self.calls['range'] - ranges_before == 1:
                self.calls['range'] += 1
                raise OSError('synthetic interrupted second range')
            return real(request, timeout=timeout)
        driver.source_range_client._open = fail_second_range
        self.step_until(driver, job, ('fetching',))
        for _ in range(5):
            driver.step()
            if self.usage()[0] >= 1:
                break
        self.assertEqual(self.usage()[:2], (1, CHUNK_BYTES))
        stage, data = self.stage(driver, job)
        self.assertEqual(stage, 'fetching')
        self.assertEqual(data['grant'], grant)

        # Past the grant TTL, inside the reservation lease: renewal through the
        # driver journal, with the Viewer reply lost after it committed.
        self.offset += PAST_GRANT
        self.host.control(op='clock', offsetMs=self.offset * 1000)
        driver = self.driver()
        self.lose.add('renew')
        result = driver.step()
        stage, data = self.stage(driver, job)
        self.assertIn('sourceRenewal', data, (result, stage, sorted(data), self.calls))
        self.assertEqual(data['sourceRenewal']['phase'], 'request_pending')
        intent = data['sourceRenewal']['request']
        self.assertEqual(self.calls['renew'], 1)

        # Kill and restart the Viewer, then a fresh driver replays the exact request
        # and the ACK reply is lost.
        self.host.stop()
        self.host = self.start_host()
        driver = self.driver()
        self.lose.add('ack')
        driver.step()
        _stage, data = self.stage(driver, job)
        self.assertEqual(self.calls['renew'], 2)
        self.assertEqual(data['sourceRenewal']['request'], intent)
        self.assertEqual(data['sourceRenewal']['phase'], 'ack_pending')
        successor = data['sourceRenewal']['successorGrant']
        self.assertNotEqual(successor['token'], grant['token'])
        # The Viewer committed the exact ACK before its reply was lost; the driver has not adopted it yet.
        self.assertEqual(self.host.control(op='head')['head']['sequence'], 1)
        self.assertEqual(data['grant'], grant)

        # A fresh driver replays the exact ACK, adopts the successor and resumes.
        driver = self.driver()
        ack_calls = self.calls['ack']
        for _ in range(10):
            driver.step()
            stage, data = self.stage(driver, job)
            if stage != 'fetching':
                break
        self.assertEqual(self.calls['ack'], ack_calls + 1)
        self.assertNotEqual(stage, 'fetching')
        self.assertEqual(data['grant'], successor)
        self.assertEqual(data['sourceRenewalSequence'], 1)
        self.assertNotIn('sourceRenewal', data)
        self.assertEqual(self.host.control(op='head')['head']['sequence'], 1)
        requests, transferred, attempts = self.usage()
        self.assertEqual((requests, transferred), (2, len(self.body)))  # no unique range recharged across renewal
        self.assertLessEqual(attempts, grant['attemptBudget'])
        inputs = list((self.fixture.scratch / 'jobs').rglob('*.input'))
        self.assertEqual([path.read_bytes() for path in inputs], [self.body])
        self.assertEqual((self.state / 'source' / 'a.jpg').read_bytes(), self.body)


if __name__ == '__main__':
    unittest.main()
