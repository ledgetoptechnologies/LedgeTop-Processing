"""Opt-in cross-language test of Processing renewal against the Viewer route.

Set LEDGETOP_VIEWER_SOURCE_DIR to a clean/local LedgeTop Operations checkout
containing the candidate source-range handler. Traffic is confined to an
ephemeral 127.0.0.1 HTTP listener; the configured Processing origin remains an
HTTPS origin and the adapter is test-only. No live Viewer/Hub/NAS service is
contacted and all photo bytes are synthetic.
"""
import base64
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

VIEWER_SOURCE_DIR = os.environ.get('LEDGETOP_VIEWER_SOURCE_DIR')
NODE_BRIDGE = Path(__file__).resolve().parent / 'support' / 'viewer_source_renewal_loopback.cjs'


class _Reply(io.BytesIO):
    def __init__(self, body, *, status, headers, url):
        super().__init__(body)
        self.status = status
        self.headers = headers
        self._url = url

    def geturl(self):
        return self._url


@unittest.skipUnless(VIEWER_SOURCE_DIR, 'set LEDGETOP_VIEWER_SOURCE_DIR to run the paired Viewer/Processing test')
class PairedViewerSourceRenewalTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_driver import ReferenceDriverTests
        self.fixture = ReferenceDriverTests(
            'test_expired_source_grant_is_durably_renewed_and_ack_replays_without_new_grant')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.node = None
        self.node_stderr = None
        self.addCleanup(self._stop_node)

    def _stop_node(self):
        if self.node is not None:
            if self.node.poll() is None:
                self.node.terminate()
                try:
                    self.node.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.node.kill()
                    self.node.wait(timeout=5)
            self.node = None
        if self.node_stderr is not None:
            self.node_stderr.close()
            self.node_stderr = None

    def _start_viewer_handler(self, ready_path):
        env = os.environ.copy()
        env['LEDGETOP_VIEWER_SOURCE_DIR'] = str(Path(VIEWER_SOURCE_DIR).resolve())
        env['LEDGETOP_PAIRED_READY_FILE'] = str(ready_path)
        self.node_stderr = tempfile.TemporaryFile(mode='w+t', encoding='utf-8')
        self.node = subprocess.Popen(
            ['node', str(NODE_BRIDGE)],
            cwd=str(Path(VIEWER_SOURCE_DIR).resolve()),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=self.node_stderr,
        )
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and not ready_path.exists():
            if self.node.poll() is not None:
                self.node_stderr.seek(0)
                raise AssertionError(f'Viewer bridge exited during startup: {self.node_stderr.read()[-2000:]}')
            time.sleep(0.025)
        if not ready_path.exists():
            self.node_stderr.seek(0)
            raise AssertionError(f'Viewer bridge did not become ready: {self.node_stderr.read()[-2000:]}')
        return json.loads(ready_path.read_text(encoding='utf-8'))['port']

    def test_reference_driver_uses_viewer_v2_renewal_routes_and_replays_after_lost_replies(self):
        viewer_root = Path(VIEWER_SOURCE_DIR).resolve()
        for relative in (
            'apps/viewer/server/sourceRangeV2Candidate.js',
            'apps/viewer/server/referenceTransferGrantRenewals.js',
            'apps/viewer/server/referenceProtocol.js',
        ):
            self.assertTrue((viewer_root / relative).is_file(), f'missing Viewer source: {relative}')
        self.assertTrue(NODE_BRIDGE.is_file())

        with tempfile.TemporaryDirectory(prefix='ltds-paired-renewal-') as temp:
            ready_path = Path(temp) / 'viewer-ready.json'
            port = self._start_viewer_handler(ready_path)

            state_path = self.fixture.root / 'driver-viewer-paired-renewal.sqlite'
            driver = self.fixture.make_driver(source_range_v2_enabled=True, state_path=state_path)
            self.fixture.driver = driver
            job = self.fixture.submit(contents=(b'paired-synthetic-photo',))
            self.fixture.advance(job, 'fetching')
            with driver._db() as db:
                row = db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()
            data = json.loads(row[0])
            previous = dict(data['grant'])
            previous['expiresAt'] = '1970-01-01T00:00:00.000Z'
            data['grant'] = previous
            driver._save(job['jobId'], 'fetching', data)

            client = driver.source_range_client
            client.origin = 'https://viewer.invalid'
            local_opener = build_opener(ProxyHandler({})).open
            calls = {'renew': 0, 'ack': 0}
            requests = {'renew': [], 'ack': []}
            wire_responses = []

            def paired_open(request, *, timeout):
                parsed = urlsplit(request.full_url)
                if parsed.scheme != 'https' or parsed.netloc != 'viewer.invalid' or not parsed.path.startswith('/api/processing/source-range/v2/grants/renew'):
                    raise AssertionError('Processing requested an unexpected Viewer endpoint')
                kind = 'ack' if parsed.path.endswith('/ack') else 'renew'
                calls[kind] += 1
                requests[kind].append(json.loads(request.data.decode('utf-8')))
                local_request = Request(
                    f'http://127.0.0.1:{port}{parsed.path}',
                    data=request.data,
                    method=request.get_method(),
                    headers={
                        'Accept': 'application/json',
                        'Accept-Encoding': 'identity',
                        'Content-Type': 'application/json',
                        'X-LedgeTop-Paired-Test': 'loopback-only',
                    },
                )
                try:
                    response = local_opener(local_request, timeout=timeout)
                except HTTPError as error:
                    response = error
                body = response.read(1_048_577)
                status = response.status
                # Keep HTTPMessage's case-insensitive lookup semantics because
                # SourceRangeClient validates response headers by name.
                headers = response.headers
                response.close()
                parsed_body = json.loads(body) if 200 <= status < 300 else {}
                wire_responses.append({
                    'status': status,
                    'contentType': next((value for key, value in headers.items() if key.lower() == 'content-type'), None),
                    'contentEncoding': next((value for key, value in headers.items() if key.lower() == 'content-encoding'), None),
                    'length': len(body),
                    'keys': sorted(parsed_body.keys()) if isinstance(parsed_body, dict) else None,
                    'grantKeys': sorted(parsed_body.get('grant', {}).keys()) if isinstance(parsed_body, dict) and isinstance(parsed_body.get('grant'), dict) else None,
                })
                # The server has accepted and journaled the request before this
                # synthetic network failure, reproducing a lost HTTP response.
                if calls[kind] == 1 and kind in ('renew', 'ack'):
                    raise OSError(f'synthetic {kind} response loss')
                return _Reply(body, status=status, headers=headers, url=request.full_url)

            client._open = paired_open
            driver_state = driver.step()
            self.assertEqual(driver_state['state'], 'retry_wait')
            with driver._db() as db:
                first = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
            self.assertEqual(first['sourceRenewal']['phase'], 'request_pending')
            exact_request = first['sourceRenewal']['request']

            driver = self.fixture.make_driver(source_range_v2_enabled=True, state_path=state_path)
            self.fixture.driver = driver
            driver.source_range_client.origin = 'https://viewer.invalid'
            driver.source_range_client._open = paired_open

            # Continue through renewal, then lose the first ACK response.
            # The download stub is reached only on the final successful replay.
            downloaded = []
            expected_download = {}
            def download(*, grant, object_id, destination, grant_provider=None):
                self.assertEqual(grant.token, expected_download['token'])
                downloaded.append((grant.token, object_id))
                Path(destination).write_bytes(self.fixture.contents[object_id])
                return Path(destination)

            from unittest.mock import patch
            with patch.object(driver.source_range_client, 'download_object', side_effect=download):
                second = driver.step()
            self.assertEqual(second['state'], 'retry_wait', {'state': second, 'wire': wire_responses})
            self.assertEqual(calls, {'renew': 2, 'ack': 1})
            self.assertEqual(requests['renew'][0], requests['renew'][1])
            self.assertEqual(requests['renew'][0], exact_request)
            with driver._db() as db:
                pending = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
            self.assertEqual(pending['sourceRenewal']['phase'], 'ack_pending')
            exact_ack = pending['sourceRenewal']['ackRequest']
            self.assertNotEqual(pending['grant'], pending['sourceRenewal']['successorGrant'])
            expected_download['token'] = pending['sourceRenewal']['successorGrant']['token']
            self.assertEqual(downloaded, [])

            driver = self.fixture.make_driver(source_range_v2_enabled=True, state_path=state_path)
            self.fixture.driver = driver
            driver.source_range_client.origin = 'https://viewer.invalid'
            driver.source_range_client._open = paired_open
            with patch.object(driver.source_range_client, 'download_object', side_effect=download):
                third = driver.step()
            self.assertEqual(third['state'], 'fetching')
            self.assertEqual(calls, {'renew': 2, 'ack': 2})
            self.assertEqual(requests['ack'][0], requests['ack'][1])
            self.assertEqual(requests['ack'][0], exact_ack)
            self.assertEqual(len(downloaded), 1)
            with driver._db() as db:
                final = json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?', (job['jobId'],)).fetchone()[0])
            self.assertEqual(final['grant'], pending['sourceRenewal']['successorGrant'])
            self.assertEqual(final['sourceRenewalSequence'], 1)
            self.assertNotIn('sourceRenewal', final)

            tampered = dict(exact_ack, token='x' * len(exact_ack['token']))
            status, response = driver.source_range_client.acknowledge_grant_renewal(tampered)
            self.assertEqual(status, 400)
            self.assertEqual(response, {'error': 'invalid_renewal'})


if __name__ == '__main__':
    unittest.main()
