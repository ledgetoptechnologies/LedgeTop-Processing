"""Paired Python client / Node broker harness for the source-range v2 candidates.

Synthetic only. It drives the Processing ``SourceRangeClient`` (PR #342) against
the Viewer ``SourceRangeV2BrokerCandidate`` (Ops PR #429) over loopback HTTP; the
broker host asserts protected transport instead of proving TLS. Set
``SOURCE_RANGE_VIEWER_SERVER`` to an exported ``apps/viewer/server`` directory at
the reviewed broker commit. Node 22.5+ (``node:sqlite``) is required.

Not covered here, by design: grant renewal (neither side has a renewal
protocol; re-minting is in-process only), the TrueNAS spool reservation and
writer fence, production TLS/proxy mounting, and real NAS storage durability.
"""
from contextlib import closing
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlsplit

from processing import reference_protocol as p
from processing.scratch_policy import PolicyConflict, ScratchLedger
from processing.source_range_transfer import (
    CHUNK_BYTES, SourceRangeClient, TransferError, build_source_transfer_request,
    source_version_for_manifest, validate_source_grant,
)

ROOT = Path(__file__).resolve().parents[1]
HOST = Path(__file__).with_name('source_range_pair') / 'broker_host.cjs'
VECTORS = json.loads((Path(__file__).with_name('fixtures') / 'source_range_budget_vectors.json').read_text())
SERVER = os.environ.get('SOURCE_RANGE_VIEWER_SERVER')
NODE = shutil.which('node')
ORIGIN = 'https://broker.test'
SECRET = 'synthetic-source-range-grant-secret-0123456789'


def proposed_budgets(base, total):
    """The 1.5x proposal, computed independently of the fixture literals."""
    half = lambda value: (3 * value + 1) // 2
    return dict(requestBudget=half(base), byteBudget=min(p.MAX_SAFE_INTEGER, half(total)), attemptBudget=half(base))


def manifest_for(objects):
    manifest = {'id': 'dataset-pair-1', 'revision': 1, 'totalBytes': sum(o['byteSize'] for o in objects),
                'objectCount': len(objects), 'inputCounts': {'images': len(objects), 'lidarObjects': 0, 'points': 0},
                'objects': objects}
    manifest['sha256'] = p.manifest_sha256(manifest)
    return manifest


def queued_job(manifest, *, budgets=None, job_id='job-pair-1', reservation='reservation-pair-1'):
    job = {'jobId': job_id, 'generation': 1, 'environment': 'staging', 'manifestSha256': manifest['sha256'],
           'submission': {'manifest': manifest}}
    expires = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime(time.time() + 3600))
    job['transferRequest'] = build_source_transfer_request(job, reservation_id=reservation, expires_at=expires, nonce='nonce-pair-1')
    if budgets:
        job['transferRequest'].update(budgets)
    return job


class _Response:
    """http.client response presented with the client's HTTPS URL, as urllib would."""
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


class Broker:
    def __init__(self, root, *, allow_create=True):
        self.root = root
        self.process = subprocess.Popen(
            [NODE, str(HOST), json.dumps({'serverDir': SERVER, 'databasePath': str(root / 'broker' / 'broker.sqlite'),
                                          'managedRoot': str(root / 'source'), 'grantSecret': SECRET,
                                          'allowCreate': allow_create, 'nowMs': None})],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = self.process.stdout.readline().split()
        if line[:1] != ['READY']:
            raise RuntimeError('broker host did not start: ' + self.process.stderr.read()[-800:])
        self.port = int(line[1])

    def control(self, **body):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        try:
            connection.request('POST', '/__control', json.dumps(body), {'content-type': 'application/json'})
            response = connection.getresponse()
            value = json.loads(response.read())
            if response.status != 200:
                raise RuntimeError(value['error'])
            return value
        finally:
            connection.close()

    def raw(self, path, headers):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        try:
            connection.request('GET', path, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def opener(self, request, *, timeout):
        parts = urlsplit(request.full_url)
        if f'{parts.scheme}://{parts.netloc}' != ORIGIN:
            raise AssertionError('unexpected origin')
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=timeout)
        connection.request(request.get_method(), parts.path, headers=dict(request.header_items()))
        response = connection.getresponse()
        if response.status >= 400:
            body = response.read()
            connection.close()
            raise HTTPError(request.full_url, response.status, body.decode(errors='replace'), response.headers, None)
        return _Response(connection, response, request.full_url)

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait()
        for stream in (self.process.stdout, self.process.stderr):
            stream.close()


@unittest.skipUnless(SERVER and NODE and os.name == 'posix', 'set SOURCE_RANGE_VIEWER_SERVER; needs Node 22.5+ on Linux')
class SourceRangePairHarnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ('broker', 'source', 'client', 'dest'):
            (self.root / name).mkdir(mode=0o700)
        self.brokers = []
        self.addCleanup(lambda: [broker.kill() for broker in self.brokers])

    def start(self, **kwargs):
        broker = Broker(self.root, **kwargs)
        self.brokers.append(broker)
        return broker

    def dataset(self, sizes):
        objects, self.paths, self.bytes = [], {}, {}
        for index, size in enumerate(sizes):
            data = hashlib.shake_256(b'synthetic-%d' % index).digest(size)
            path = self.root / 'source' / f'IMG_{index:04d}.JPG'
            path.write_bytes(data)
            object_id = f'photo-{index}'
            self.paths[object_id], self.bytes[object_id] = str(path), data
            objects.append({'id': object_id, 'name': path.name, 'byteSize': size,
                            'sha256': hashlib.sha256(data).hexdigest(), 'kind': 'image', 'mediaType': 'image/jpeg'})
        return manifest_for(objects)

    def mint(self, broker, job):
        raw = broker.control(op='mint', job=job, paths=self.paths)['grant']
        return raw, validate_source_grant(raw, job=job)

    def client(self, broker, ledger='client/ledger.sqlite'):
        return SourceRangeClient(ORIGIN, self.root / ledger, opener=broker.opener)

    def client_usage(self, ledger='client/ledger.sqlite'):
        with closing(sqlite3.connect(self.root / ledger)) as db:
            return db.execute('SELECT requests_used,bytes_used,attempts_used FROM source_budgets').fetchone()

    def broker_usage(self, broker):
        row = broker.control(op='usage')['usage'][0]
        return row['requests_used'], row['bytes_used'], row['attempts_used']

    def assert_sources_retained(self):
        for object_id, data in self.bytes.items():
            self.assertEqual(Path(self.paths[object_id]).read_bytes(), data)

    def fetch_all(self, client, grant):
        for obj in grant.objects:
            target = client.download_object(grant=grant, object_id=obj.id, destination=self.root / 'dest' / (obj.id + '.jpg'))
            self.assertEqual(target.read_bytes(), self.bytes[obj.id])

    # Shared integer vectors ------------------------------------------------

    def test_shared_vectors_match_both_implementations_and_proposal_math(self):
        node_math = textwrap.dedent('''
            const vectors = JSON.parse(require('fs').readFileSync(0, 'utf8')).vectors;
            const half = v => (3n * v + 1n) / 2n, max = BigInt(Number.MAX_SAFE_INTEGER);
            console.log(JSON.stringify(vectors.map(v => {
              const base = v.objects.reduce((s, [b, n]) => s + ((BigInt(b) + 8388607n) / 8388608n) * BigInt(n), 0n);
              const total = v.objects.reduce((s, [b, n]) => s + BigInt(b) * BigInt(n), 0n);
              const min = (a, b) => a < b ? a : b;
              return [String(base), String(total), String(min(max, 3n * total)), String(min(max, 3n * base)),
                      String(half(base)), String(min(max, half(total)))];
            })));''')
        result = subprocess.run([NODE, '-e', node_math], input=json.dumps(VECTORS), capture_output=True, text=True, check=True)
        for vector, node in zip(VECTORS['vectors'], json.loads(result.stdout)):
            with self.subTest(vector=vector['name']):
                current, proposal = vector['profiles']['current-v2-3x'], vector['profiles']['proposed-range-policy-1.5x']
                self.assertEqual([int(x) for x in node], [vector['baseRequests'], vector['totalBytes'], current['byteBudget'],
                                 current['attemptBudget'], proposal['requestBudget'], proposal['byteBudget']])
                self.assertEqual({k: proposal[k] for k in ('requestBudget', 'byteBudget', 'attemptBudget')},
                                 proposed_budgets(vector['baseRequests'], vector['totalBytes']))
                objects = [{'id': f'o-{i}', 'name': f'o-{i}.jpg', 'byteSize': size, 'sha256': 'a' * 64,
                            'kind': 'image', 'mediaType': 'image/jpeg'}
                           for i, size in enumerate(b for b, n in vector['objects'] for _ in range(n))]
                transfer = queued_job(manifest_for(objects))['transferRequest']
                self.assertEqual({k: transfer[k] for k in current}, current)

    def test_broker_and_client_enforce_the_same_minimums_and_accept_both_profiles(self):
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES, CHUNK_BYTES + 1, 3])
        base, total = 4, manifest['totalBytes']
        for label, budgets, accepted in (
                ('current', None, True),
                ('proposal', proposed_budgets(base, total), True),
                ('request below minimum', {'requestBudget': base - 1}, False),
                ('bytes below total', {'byteBudget': total - 1}, False),
                ('attempts below minimum', {'attemptBudget': base - 1}, False)):
            with self.subTest(case=label):
                job = queued_job(manifest, budgets=budgets, job_id='job-' + label.replace(' ', '-'),
                                 reservation='reservation-' + label.replace(' ', '-'))
                if accepted:
                    self.mint(broker, job)
                else:
                    with self.assertRaisesRegex(RuntimeError, 'INVALID_TRANSFER_BUDGET'):
                        broker.control(op='mint', job=job, paths=self.paths)
                    raw = {**job['transferRequest'], 'jobId': job['jobId'], 'environment': 'staging', 'token': 'x' * 43}
                    raw['expiresAt'] = job['transferRequest']['expiresAt']
                    with self.assertRaises(TransferError):
                        validate_source_grant(raw, job=job)

    # Paired transfer and accounting -----------------------------------------

    def test_paired_transfer_charges_each_unique_range_once_on_both_sides(self):
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES, CHUNK_BYTES + 1, 3])
        _raw, grant = self.mint(broker, queued_job(manifest))
        self.fetch_all(self.client(broker), grant)
        self.assertEqual(self.client_usage(), (4, manifest['totalBytes'], 4))
        self.assertEqual(self.broker_usage(broker), (4, manifest['totalBytes'], 4))
        self.fetch_all(self.client(broker), grant)  # published objects are verified, never refetched
        self.assertEqual(self.broker_usage(broker), (4, manifest['totalBytes'], 4))
        self.assert_sources_retained()

    def test_client_crash_points_resume_without_recharging_unique_ranges(self):
        points = {
            'after_intent_before_attempt': "patch.object(SourceRangeClient, '_record_attempt', side_effect=lambda *a, **k: os._exit(9))",
            'after_attempt_before_request': "patch.object(broker, 'opener', side_effect=lambda *a, **k: os._exit(9))",
            'after_body_before_commit': "patch.object(SourceRangeClient, '_commit_range', side_effect=lambda *a, **k: os._exit(9))",
            'after_ranges_before_publish': "patch('os.link', side_effect=lambda *a, **k: os._exit(9))",
            'after_publish_before_mark': "patch.object(SourceRangeClient, '_mark_published', side_effect=lambda *a, **k: os._exit(9))",
        }
        for point, injection in points.items():
            with self.subTest(point=point):
                self.tearDown_case()
                broker = self.start()
                manifest = self.dataset([CHUNK_BYTES + 5])
                job = queued_job(manifest)
                raw, grant = self.mint(broker, job)
                child = textwrap.dedent(f'''
                    import json, os, sys
                    from unittest.mock import patch
                    sys.path.insert(0, {str(ROOT)!r}); sys.path.insert(0, {str(Path(__file__).parent)!r})
                    import test_source_range_pair_harness as h
                    from processing.source_range_transfer import SourceRangeClient, validate_source_grant
                    spec = json.loads(sys.argv[1]); broker = h.Broker.__new__(h.Broker); broker.port = spec['port']
                    grant = validate_source_grant(spec['raw'], job=spec['job'])
                    with {injection}:
                        client = SourceRangeClient(h.ORIGIN, spec['ledger'], opener=broker.opener)
                        client.download_object(grant=grant, object_id='photo-0', destination=spec['target'])
                    sys.exit(3)
                ''')
                target = self.root / 'dest' / 'photo-0.jpg'
                spec = {'port': broker.port, 'raw': raw, 'job': job, 'ledger': str(self.root / 'client/ledger.sqlite'),
                        'target': str(target)}
                result = subprocess.run([sys.executable, '-c', child, json.dumps(spec)], cwd=ROOT, capture_output=True, text=True)
                self.assertEqual(result.returncode, 9, result.stderr[-800:])
                before = self.broker_usage(broker)
                self.fetch_all(self.client(broker), grant)
                requests, transferred, attempts = self.broker_usage(broker)
                self.assertEqual((requests, transferred), (2, manifest['totalBytes']))  # no unique range recharged
                self.assertGreaterEqual(attempts, before[2])
                client_requests, client_bytes, client_attempts = self.client_usage()
                self.assertEqual((client_requests, client_bytes), (2, manifest['totalBytes']))
                self.assertGreaterEqual(client_attempts, attempts)  # the client charges before the broker sees a request
                self.assert_sources_retained()

    def tearDown_case(self):
        for broker in self.brokers:
            broker.kill()
        self.brokers.clear()
        for name in ('broker', 'source', 'client', 'dest'):
            shutil.rmtree(self.root / name)
            (self.root / name).mkdir(mode=0o700)

    def test_broker_restart_keeps_cumulative_budget_and_replays_the_active_grant(self):
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES + 5, 7])
        job = queued_job(manifest)
        raw, grant = self.mint(broker, job)
        client = self.client(broker)
        client.download_object(grant=grant, object_id='photo-1', destination=self.root / 'dest' / 'photo-1.jpg')
        usage = self.broker_usage(broker)
        broker.kill()
        broker = self.start(allow_create=False)
        replay, grant = self.mint(broker, job)
        self.assertEqual(replay['token'], raw['token'])  # same authority, not a fresh one
        self.assertEqual(self.broker_usage(broker), usage)
        self.fetch_all(self.client(broker), grant)
        self.assertEqual(self.broker_usage(broker)[:2], (3, manifest['totalBytes']))

    def test_reminting_after_expiry_is_not_a_renewal_protocol_and_keeps_budgets(self):
        # Current behavior only: an expired grant is revoked and a fresh token is
        # minted in-process under the same cumulative usage row. A renewal
        # protocol (sequence, previous-grant digest, replay rules) does not exist
        # on either side and is not exercised here.
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES + 5])
        job = queued_job(manifest)
        raw, grant = self.mint(broker, job)
        with patch.object(SourceRangeClient, '_commit_range', side_effect=TransferError('synthetic stop')):
            with self.assertRaises(TransferError):
                self.client(broker).download_object(grant=grant, object_id='photo-0', destination=self.root / 'dest' / 'x.jpg')
        usage = self.broker_usage(broker)
        # Past the 5-minute grant TTL but inside the 1-hour reservation lease.
        broker.control(op='now', nowMs=int(time.time() * 1000) + 600_000)
        status, _ = broker.raw('/api/processing/source-range/v2/objects/photo-0', {'Authorization': 'Bearer ' + raw['token']})
        self.assertEqual(status, 401)
        fresh = broker.control(op='mint', job=job, paths=self.paths)['grant']
        self.assertNotEqual(fresh['token'], raw['token'])
        self.assertEqual(self.broker_usage(broker), usage)
        status, _ = broker.raw('/api/processing/source-range/v2/objects/photo-0', {'Authorization': 'Bearer ' + raw['token']})
        self.assertEqual(status, 401)  # the expired authority stays revoked

    def test_attempt_budget_exhaustion_holds_across_client_and_broker_restart(self):
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES + 5])
        job = queued_job(manifest, budgets={'attemptBudget': 2})  # exactly the minimum: no retries
        raw, grant = self.mint(broker, job)
        real = broker.opener
        def lose_first_response(request, *, timeout):
            real(request, timeout=timeout).close()  # the broker charged and served; the client never sees it
            raise TransferError('synthetic lost response')
        target = self.root / 'dest' / 'x.jpg'
        with self.assertRaises(TransferError):
            SourceRangeClient(ORIGIN, self.root / 'client/ledger.sqlite', opener=lose_first_response).download_object(
                grant=grant, object_id='photo-0', destination=target)
        self.assertEqual(self.broker_usage(broker)[2], 1)
        # The retry of range 0 uses the last attempt, so range 1 is refused.
        with self.assertRaisesRegex(TransferError, 'attempt budget exhausted'):
            self.client(broker).download_object(grant=grant, object_id='photo-0', destination=target)
        broker.kill()
        broker = self.start(allow_create=False)
        _raw, grant = self.mint(broker, job)
        with self.assertRaisesRegex(TransferError, 'attempt budget exhausted'):
            self.client(broker).download_object(grant=grant, object_id='photo-0', destination=self.root / 'dest' / 'x.jpg')
        self.assertLessEqual(self.broker_usage(broker)[2], 2)
        self.assertFalse((self.root / 'dest' / 'x.jpg').exists())
        self.assert_sources_retained()

    # Failure classes ------------------------------------------------------

    # Known defect in #342 @ 1c79a8af, found by this test: the unbuffered
    # stream.write() return value is not checked, so on ENOSPC a short write of a
    # range is fsynced and committed with the full chunk hash. The next run then
    # refuses permanently ("committed source bytes are missing or corrupt").
    # Source bytes are unaffected. Remove expectedFailure when the client checks
    # the written length before committing.
    @unittest.expectedFailure
    def test_destination_enospc_holds_then_resumes_without_recharge(self):
        small = os.environ.get('SOURCE_RANGE_SMALL_TMPFS')
        if not small:
            self.skipTest('set SOURCE_RANGE_SMALL_TMPFS to a private tmpfs of about 12 MiB')
        work = Path(tempfile.mkdtemp(dir=small))
        self.addCleanup(shutil.rmtree, work, True)
        dest = work / 'dest'
        dest.mkdir(mode=0o700)
        filler = work / 'filler'
        broker = self.start()
        manifest = self.dataset([2 * CHUNK_BYTES + 5])
        _raw, grant = self.mint(broker, queued_job(manifest))
        free = shutil.disk_usage(small).free
        filler.write_bytes(b'\0' * max(0, free - CHUNK_BYTES - 1024 * 1024))
        with self.assertRaises(TransferError):
            self.client(broker).download_object(grant=grant, object_id='photo-0', destination=dest / 'x.jpg')
        self.assertFalse((dest / 'x.jpg').exists())
        filler.unlink()
        # Room for the partial plus the published hard link of the same inode.
        target = self.client(broker).download_object(grant=grant, object_id='photo-0', destination=dest / 'x.jpg')
        self.assertEqual(target.read_bytes(), self.bytes['photo-0'])
        self.assertEqual(self.broker_usage(broker)[:2], (3, manifest['totalBytes']))
        self.assert_sources_retained()

    def test_source_mutation_is_refused_and_never_repaired_by_the_broker(self):
        broker = self.start()
        manifest = self.dataset([CHUNK_BYTES + 5])
        _raw, grant = self.mint(broker, queued_job(manifest))
        path = Path(self.paths['photo-0'])
        mutated = self.bytes['photo-0'][:-1] + b'!'
        path.write_bytes(mutated)
        with self.assertRaisesRegex(TransferError, 'rejected'):
            self.client(broker).download_object(grant=grant, object_id='photo-0', destination=self.root / 'dest' / 'x.jpg')
        self.assertEqual(path.read_bytes(), mutated)
        self.assertFalse((self.root / 'dest' / 'x.jpg').exists())

    def test_corrupted_local_partial_is_detected_before_any_further_fetch(self):
        broker = self.start()
        manifest = self.dataset([2 * CHUNK_BYTES + 5])
        _raw, grant = self.mint(broker, queued_job(manifest))
        calls = []
        real = broker.opener
        def stop_after_first(request, *, timeout):
            calls.append(request)
            if len(calls) == 2:
                raise TransferError('synthetic stop')
            return real(request, timeout=timeout)
        with self.assertRaises(TransferError):
            SourceRangeClient(ORIGIN, self.root / 'client/ledger.sqlite', opener=stop_after_first).download_object(
                grant=grant, object_id='photo-0', destination=self.root / 'dest' / 'x.jpg')
        part = next((self.root / 'dest').glob('.source-range-*.part'))
        data = bytearray(part.read_bytes()); data[10] ^= 1; part.write_bytes(bytes(data))
        usage = self.broker_usage(broker)
        with self.assertRaisesRegex(TransferError, 'corrupt'):
            self.client(broker).download_object(grant=grant, object_id='photo-0', destination=self.root / 'dest' / 'x.jpg')
        self.assertEqual(self.broker_usage(broker), usage)
        self.assert_sources_retained()

    def test_noncanonical_overlapping_or_unauthenticated_ranges_charge_nothing(self):
        broker = self.start()
        manifest = self.dataset([2 * CHUNK_BYTES + 5])
        raw, _grant = self.mint(broker, queued_job(manifest))
        url = '/api/processing/source-range/v2/objects/photo-0'
        etag = '"sha256-%s"' % manifest['objects'][0]['sha256']
        for label, headers, expected in (
                ('no grant', {'Range': 'bytes=0-8388607', 'If-Match': etag}, 400),  # INVALID_REQUEST before any lookup
                ('misaligned', {'Authorization': 'Bearer ' + raw['token'], 'Range': 'bytes=1-8388608', 'If-Match': etag}, 416),
                ('overlap past chunk', {'Authorization': 'Bearer ' + raw['token'], 'Range': 'bytes=0-8388608', 'If-Match': etag}, 416),
                ('wrong request id', {'Authorization': 'Bearer ' + raw['token'], 'Range': 'bytes=0-8388607', 'If-Match': etag,
                                      'X-Processing-Request-ID': 'f' * 64}, 400)):
            with self.subTest(case=label):
                status, _body = broker.raw(url, headers)
                self.assertEqual(status, expected)
        self.assertEqual(self.broker_usage(broker), (0, 0, 0))
        self.assertEqual(broker.control(op='usage')['ranges'], [])
        self.assert_sources_retained()

    def test_no_fetch_is_possible_before_a_grant_for_the_claimed_job_exists(self):
        broker = self.start()
        manifest = self.dataset([5])
        job = queued_job(manifest)
        status, _ = broker.raw('/api/processing/source-range/v2/objects/photo-0',
                               {'Authorization': 'Bearer ' + 'A' * 43, 'Range': 'bytes=0-4'})
        self.assertEqual(status, 401)
        self.assertEqual(broker.control(op='usage')['usage'], [])
        raw, _grant = self.mint(broker, job)
        broker.control(op='revoke', identityKey=broker.control(op='usage')['usage'][0]['identity_key'])
        status, _ = broker.raw('/api/processing/source-range/v2/objects/photo-0', {'Authorization': 'Bearer ' + raw['token']})
        self.assertEqual(status, 401)
        self.assert_sources_retained()


class ScratchReservationTests(unittest.TestCase):
    """Worker-side admission (already in the candidate): races and restart, synthetic capacity."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'scratch').mkdir(mode=0o700)

    def test_concurrent_reservations_admit_exactly_what_fits_and_survive_restart(self):
        gib = 1024 ** 3
        code = textwrap.dedent('''
            import json, sys
            sys.path.insert(0, sys.argv[1])
            from processing.scratch_policy import PolicyConflict, ScratchLedger
            ledger = ScratchLedger(sys.argv[2], sys.argv[3], capacity_probe=lambda _: 160 * 1024 ** 3,
                                   safety_floor_bytes=10 * 1024 ** 3)
            index = int(sys.argv[4])
            try:
                ledger.reserve(job_id=f"job-{index}", generation=1, reservation_id=f"r-{index}",
                               manifest_sha256="a" * 64, input_bytes=50 * 1024 ** 3, output_budget=0,
                               environment="production", ttl_seconds=3600, worker_ready=True)
                print("admitted")
            except PolicyConflict:
                print("refused")
        ''')
        children = [subprocess.Popen([sys.executable, '-c', code, str(ROOT), str(self.root / 'ledger.sqlite'),
                                      str(self.root / 'scratch'), str(index)], stdout=subprocess.PIPE, text=True)
                    for index in range(5)]
        results = [child.communicate(timeout=60)[0].strip() for child in children]
        self.assertEqual(results.count('admitted'), 3, results)  # 160 GiB - 10 GiB floor fits three 50 GiB inputs
        ledger = ScratchLedger(self.root / 'ledger.sqlite', self.root / 'scratch',
                               capacity_probe=lambda _: 160 * gib, safety_floor_bytes=10 * gib)
        with self.assertRaises(PolicyConflict):
            ledger.reserve(job_id='job-late', generation=1, reservation_id='r-late', manifest_sha256='a' * 64,
                           input_bytes=1, output_budget=0, environment='production', ttl_seconds=3600, worker_ready=True)


if __name__ == '__main__':
    unittest.main()
