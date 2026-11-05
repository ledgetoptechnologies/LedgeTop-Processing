"""Source grant renewal past the reservation deadline, on the real broker path.

The coordinator, HTTP route and SourceRangeClient are real and the driver's
journal logic is the real method. Only time is virtual, so an outage longer than
the lease takes no time, and the source data is the synthetic intake fixture.
"""
import copy
from datetime import datetime
import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
import uuid

from processing import reference_protocol as protocol
from processing import reference_source_intake_store as intake_store
from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import DriverHeld, DriverWait, ReferenceDriver
from processing.reference_recovery import RecoveryNodeBinding
from processing.reference_source_readers import SourceReaderError
from processing.reference_source_intake_store import SOURCE_RANGE_GRANT_TTL_SECONDS
from processing.reference_store import PROFILE, ReferenceStore
from processing.reference_transfer import TransferError
from processing.source_range_transfer import (MAX_RENEWAL_SEQUENCE, RENEWAL_ACK_PATH, RENEWAL_PATH,
    SourceRangeClient, _request_id, validate_source_grant)
from processing.source_spool import SourceSpool
from tests.test_reference_source_range_broker import _ControlOpener, _Response

WORKER_TOKEN = 'synthetic-worker-token-012345'


def stamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


class _Outage(_ControlOpener):
    """Refuse a route before the coordinator sees it, as a closed port does."""

    def __init__(self, control, route):
        super().__init__(control)
        self.route = route

    def open(self, request, timeout=None):
        if urlsplit(request.full_url).path == self.route:
            raise OSError('synthetic coordinator outage')
        return super().open(request, timeout=timeout)


class _BlockedClose:
    """A real descriptor whose close stalls the way a hung NAS mount can."""

    def __init__(self, reader, entered, release, finished):
        self.reader, self.entered, self.release, self.finished = reader, entered, release, finished

    def fileno(self):
        return self.reader.fileno()

    def close(self):
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError('blocked close was not released')
        self.reader.close()
        self.finished.set()


class _UnknownClose:
    """The descriptor is gone but the close reports no usable outcome."""

    def __init__(self, reader):
        self.reader = reader

    def fileno(self):
        return self.reader.fileno()

    def close(self):
        self.reader.close()
        raise OSError('synthetic close outcome unknown')


class RenewalHarness(unittest.TestCase):
    chunk_bytes = None
    extra_objects = 0
    harness_reserves = True

    def setUp(self):
        if self.chunk_bytes is not None:
            chunk = patch('processing.source_range_transfer.CHUNK_BYTES', self.chunk_bytes)
            chunk.start()
            self.addCleanup(chunk.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests
        case = ReferenceSourceIntakeTests('runTest')
        case.setUp()
        case.payload['manifest']['id'] = 'dataset-grant-renewal'
        self.add_objects(case)
        case.rehash()
        self.case = case
        spool_root = self.root / 'spool'
        spool_root.mkdir(mode=0o700)
        marker = spool_root / '.ltds-source-spool-id'
        marker.write_text('a' * 64, encoding='ascii')
        if os.name != 'nt':
            marker.chmod(0o600)
        mount = patch('processing.source_spool.os.path.ismount', return_value=True)
        mount.start()
        self.addCleanup(mount.stop)
        self.spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0,
            max_object_bytes=1024, chunk_bytes=3)
        self.queue, self.store, self.control, job, self.claim_token = case._make_reserved_source_job(
            self.directory.name, self.spool, 'renewal', source_range_v2=True, reserve=self.harness_reserves)
        self.addCleanup(lambda: self.queue.close())
        self.job_id = job['jobId']
        self.start_transfer()
        self.bind_clock()
        self.plan = str(uuid.uuid4())
        self.worker = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
            claim_token=self.claim_token)
        self.client = SourceRangeClient('https://broker.invalid', self.root / 'worker-ledger.sqlite',
            opener=_ControlOpener(self.control), clock=lambda: self.now[0])
        self.client.renewal_headers = self.credentials
        self.data = self.first_journal()
        self.saved, self.saved_stages = [], []
        self.down, self.lose_reply, self.requests = set(), set(), []
        self.driver = self.real_driver()

    def start_transfer(self):
        self.first_grant = self.store.mint_source_transfer_grant(self.job_id, source_spool=self.spool)
        self.store.update_phase(self.job_id, 'fetching')
        self.claim = self.store.get(self.job_id, private=True)
        self.lease = stamp(self.claim['transferRequest']['expiresAt'])
        self.now = [stamp(self.first_grant['expiresAt']) - 60]

    def first_journal(self):
        return {'grant': copy.deepcopy(self.first_grant), 'claim': self.claim,
            'reservation': self.claim['transferRequest']}

    def add_objects(self, case):
        """Synthetic extra photos, so the transfer wire scales with the manifest."""
        case.extra_bodies = {}
        for index in range(self.extra_objects):
            object_id, body = str(uuid.uuid5(uuid.NAMESPACE_URL, f'extra-{index}')), b'photo-%d' % index
            case.extra_bodies[object_id] = body
            digest = hashlib.sha256(body).hexdigest()
            name = f'Extra_{index:05d}.JPG'
            source, manifest = case.payload['sourceManifest'], case.payload['manifest']
            source['objects'].append({'id': object_id, 'name': name, 'byteSize': len(body), 'sha256': digest,
                'sourceType': 'raw_input', 'retentionClass': 'release_after_acceptance', 'retentionHoldId': None})
            manifest['objects'].append({'id': object_id, 'name': name, 'byteSize': len(body), 'sha256': digest,
                'kind': 'image', 'mediaType': 'image/jpeg'})
            for target in (source, manifest):
                target['totalBytes'] += len(body)
                target['objectCount'] += 1
            manifest['inputCounts']['images'] += 1
        for target in (case.payload['sourceManifest'], case.payload['manifest']):
            target['objects'].sort(key=lambda item: item['id'])

    def real_driver(self):
        """Real journal, claim-state and recovery code over a real local journal.

        Everything beyond those methods (engine, ledger, adapters) is absent, so
        a call into any of it fails loudly rather than being faked.
        """
        driver = object.__new__(ReferenceDriver)
        driver.path = self.root / 'driver-journal.sqlite'
        driver._combined_policy = driver._combined_policy_initial = None
        driver.clock, driver.source_range_client = (lambda: self.now[0]), self.client
        driver.worker = SimpleNamespace(authority_id=self.queue.authority_id)
        driver.ledger = SimpleNamespace(transfer_fenced=lambda _reservation: True)
        driver.node_id, driver.worker_id, driver.worker_instance_id = 'cluster', 'worker', 'instance'
        driver._request = self.worker_request
        driver._save = self.save
        with driver._db() as db:
            db.execute('''CREATE TABLE driver_jobs(created INTEGER PRIMARY KEY AUTOINCREMENT,job TEXT UNIQUE NOT NULL,
                claim_request TEXT UNIQUE NOT NULL,stage TEXT NOT NULL,data TEXT NOT NULL)''')
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'claimed','{}')",
                (self.job_id, 'source-claim-renewal'))
        return driver

    def save(self, _job_id, stage, data):
        self.saved.append(copy.deepcopy(data))
        self.saved_stages.append(stage)

    def worker_request(self, method, suffix, payload=None, *, claim_token=None):
        """The driver's real status mapping over the real coordinator API."""
        operation = suffix.rsplit('/', 1)[-1]
        self.requests.append((method, operation))
        if operation in self.down:
            raise DriverWait('coordinator_unavailable')
        headers = {'Authorization': 'Bearer ' + WORKER_TOKEN}
        if claim_token is not None:
            headers['X-Claim-Token'] = claim_token
        response = self.control.handle(method, '/ltds/worker/v1' + suffix, headers,
            b'' if payload is None else protocol.canonical_json(payload), protected_transport=True)
        if operation in self.lose_reply:
            self.lose_reply.discard(operation)
            raise DriverWait('coordinator_unavailable')
        if response.status in (400, 401, 403, 404):
            raise DriverHeld('control_request_rejected')
        if response.status == 409:
            return 409, None
        if response.status != 200:
            raise DriverWait('coordinator_unavailable')
        return 200, response.body

    def admit(self):
        return self.driver._fetch_refusal(self.job_id, 'fetching', self.data, True)

    def crash_worker(self):
        """Only what reached the journal survives a worker death."""
        self.data = copy.deepcopy(self.saved[-1])

    def bind_clock(self):
        for name in ('renew_source_transfer_grant', 'acknowledge_source_transfer_grant', 'source_range_for_grant'):
            real = getattr(self.store, name)
            setattr(self.store, name, lambda *args, real=real, **kwargs: real(
                *args, **{'clock': lambda: self.now[0], **kwargs}))

    def restart_coordinator(self):
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.queue.allowed_profiles.add(PROFILE)
        self.store = ReferenceStore(self.queue)
        self.control = ReferenceControlAPI(self.store, viewer_bearers={'synthetic-staging-token': 'staging'},
            worker_bearers={WORKER_TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint='a' * 64, options_fingerprint=self.case.payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'numeric-probe'}, source_spool=self.spool,
            source_attestation_keys=self.case.trusted_keys, authority_id=self.queue.authority_id,
            recovery_bindings={'cluster': RecoveryNodeBinding('cluster', 'physical-cluster', self.queue.authority_id,
                'http://127.0.0.1:18080', 1, 1000)})
        self.bind_clock()
        self.opener(_ControlOpener(self.control))

    def freeze(self, action, **kwargs):
        gate = self.queue._coordinator_freeze
        return gate.transition(dict(requestId=str(uuid.uuid4()), planId=self.plan, action=action,
            expectedRevision=gate.status()['revision'], startupId=self.queue.startup_id),
            owner_secret='synthetic-freeze-owner', **kwargs)

    def credentials(self, route, _payload):
        headers = {'Authorization': 'Bearer ' + WORKER_TOKEN}
        if route == RENEWAL_PATH:
            headers['X-Claim-Token'] = self.claim_token
        return headers

    def opener(self, opener):
        self.client._open = opener

    def renew(self):
        return self.driver._source_range_grant(self.job_id, self.claim, self.data)

    def after_lease(self, seconds=3600):
        self.now[0] = self.lease + seconds

    def request(self, grant=None, *, renewal_id='explicit-renewal', sequence=1):
        grant = self.first_grant if grant is None else grant
        return {'protocol': 'source-range-v2-renewal', 'renewalId': renewal_id, 'sequence': sequence,
            'previousGrantSha256': hashlib.sha256(protocol.canonical_json(grant)).hexdigest(),
            'previousGrant': grant}

    def renew_directly(self, request=None, **worker):
        return self.store.renew_source_transfer_grant(request or self.request(), source_spool=self.spool,
            worker={**self.worker, **worker})

    def ack(self, reply):
        digest = hashlib.sha256(protocol.canonical_json(reply['grant'])).hexdigest()
        return {'protocol': 'source-range-v2-renewal-ack', 'renewalId': reply['renewalId'],
            'sequence': reply['sequence'], 'grantSha256': digest, 'token': reply['grant']['token']}

    def post(self, route, payload, headers):
        return self.control.handle('POST', route, headers, protocol.canonical_json(payload),
            protected_transport=True)

    def journal(self):
        with self.queue._db() as db:
            return [dict(row) for row in db.execute('''SELECT sequence,renewal_id,successor_sha256,acked
                FROM reference_source_grant_renewals WHERE job_id=? ORDER BY sequence''', (self.job_id,))]

    def current_grant(self):
        with self.queue._db() as db:
            return json.loads(db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?',
                (self.job_id,)).fetchone()[0])

    def execute(self, statement, *values):
        with self.queue._db() as db:
            db.execute(statement, values)

    def set_job_status(self, status):
        self.execute('UPDATE jobs SET status=? WHERE id=?', status, self.job_id)

    def fetch(self, grant, index):
        target = self.root / f'object-{index}-{uuid.uuid4().hex}.bin'
        self.client.download_object(grant=grant, object_id=grant.objects[index].id, destination=target,
            grant_provider=lambda: self.renew())
        return target.read_bytes()

    def read_range(self, token, grant, index=0):
        item = grant.objects[index]
        identity = json.dumps(grant.identity.canonical(), sort_keys=True, separators=(',', ':'))
        reader, _ = self.store.source_range_for_grant(token, item.id, range_start=0, range_length=item.byte_size,
            request_id=_request_id(identity, item, 0, item.byte_size), if_match='"sha256-' + item.sha256 + '"',
            source_spool=self.spool)
        reader.close()

    def during_verification(self, action):
        """Run ``action`` once, after the snapshot and before the commit."""
        real, fired = intake_store._open_pinned_source, []

        def verify(*args, **kwargs):
            if not fired:
                fired.append(True)
                action()
            return real(*args, **kwargs)
        return patch.object(intake_store, '_open_pinned_source', side_effect=verify)

    def usage(self):
        with self.queue._db() as db:
            return tuple(db.execute('''SELECT requests_used,bytes_used,attempts_used
                FROM reference_source_range_usage WHERE job_id=?''', (self.job_id,)).fetchone())


class SourceGrantRenewalTests(RenewalHarness):
    # Coordinator disconnect longer than the TTL.

    def test_disconnect_longer_than_ttl_issues_one_successor_to_the_claim_owner(self):
        self.after_lease()
        renewed = self.renew()
        self.assertNotEqual(renewed.token, self.first_grant['token'])
        self.assertGreater(renewed.expires, self.lease)
        self.assertLessEqual(renewed.expires, self.now[0] + SOURCE_RANGE_GRANT_TTL_SECONDS)
        self.assertEqual([(row['sequence'], row['acked']) for row in self.journal()], [(1, 1)])
        self.assertEqual(self.current_grant()['token'], renewed.token)
        self.assertEqual(self.fetch(renewed, 0), b'abc')

    def test_successor_changes_only_its_expiry_and_token(self):
        self.after_lease()
        self.renew()
        changed = {key for key, value in self.current_grant().items() if value != self.first_grant[key]}
        self.assertEqual(changed, {'expiresAt', 'token'})

    def test_expired_grant_opens_nothing_past_the_reservation(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.after_lease()
        with self.assertRaises((Conflict, TransferError)):
            self.read_range(self.first_grant['token'], first)
        self.renew()
        with self.assertRaises(Conflict):
            self.read_range(self.first_grant['token'], first)

    def test_unrenewed_grant_stays_bound_to_the_reservation(self):
        self.after_lease()
        with self.assertRaisesRegex(TransferError, 'reservation lease'):
            validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0], allow_expired=True)

    # Partial transfer past the TTL.

    def test_partial_transfer_past_ttl_resumes_on_the_same_budget(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.assertEqual(self.fetch(first, 0), b'abc')
        spent = self.usage()
        self.after_lease()
        renewed = self.renew()
        self.assertEqual(self.usage(), spent)
        self.now[0] = renewed.expires + 1
        again = self.renew()
        self.assertEqual(self.data['sourceRenewalSequence'], 2)
        self.assertEqual((again.byte_budget, again.request_budget, again.attempt_budget),
            (first.byte_budget, first.request_budget, first.attempt_budget))
        self.assertEqual(self.fetch(again, 1), b'1,2,3\n')
        self.assertEqual(self.usage(), (spent[0] + 1, spent[1] + 6, spent[2] + 1))

    def test_transfer_renews_through_the_grant_provider_between_objects(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.fetch(first, 0)
        self.after_lease()
        self.assertEqual(self.fetch(first, 1), b'1,2,3\n')
        self.assertEqual(self.data['sourceRenewalSequence'], 1)

    # Lost renewal reply.

    def test_lost_renewal_reply_replays_the_exact_successor(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaisesRegex(DriverWait, 'source_renewal_endpoint_unavailable'):
            self.renew()
        minted = self.journal()
        self.assertEqual([row['acked'] for row in minted], [0])
        intent = copy.deepcopy(self.data['sourceRenewal'])
        self.opener(_ControlOpener(self.control))
        renewed = self.renew()
        self.assertEqual(hashlib.sha256(protocol.canonical_json(self.data['grant'])).hexdigest(),
            minted[0]['successor_sha256'])
        self.assertEqual(self.saved[-2]['sourceRenewal']['request'], intent['request'])
        self.assertEqual(len(self.journal()), 1)
        self.assertEqual(self.fetch(renewed, 0), b'abc')

    def test_lost_ack_reply_installs_the_successor_once(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_ACK_PATH))
        with self.assertRaisesRegex(DriverWait, 'source_renewal_ack_unavailable'):
            self.renew()
        self.opener(_ControlOpener(self.control))
        renewed = self.renew()
        self.assertEqual([row['acked'] for row in self.journal()], [1])
        self.assertEqual(self.current_grant()['token'], renewed.token)

    def test_successor_expired_by_an_outage_after_a_lost_reply_chains_forward(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.opener(_ControlOpener(self.control))
        self.now[0] += 2 * SOURCE_RANGE_GRANT_TTL_SECONDS
        renewed = self.renew()
        self.assertEqual([(row['sequence'], row['acked']) for row in self.journal()], [(1, 1), (2, 1)])
        self.assertGreater(renewed.expires, self.now[0])
        self.assertEqual(self.fetch(renewed, 0), b'abc')

    def test_successor_expired_while_its_ack_was_outstanding_chains_forward(self):
        self.after_lease()
        self.opener(_Outage(self.control, RENEWAL_ACK_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.assertEqual(self.data['sourceRenewal']['phase'], 'ack_pending')
        self.opener(_ControlOpener(self.control))
        self.now[0] += 2 * SOURCE_RANGE_GRANT_TTL_SECONDS
        renewed = self.renew()
        self.assertEqual(self.data['sourceRenewalSequence'], 2)
        self.assertEqual(self.fetch(renewed, 0), b'abc')

    def test_retry_overtaking_the_original_request_gets_the_same_successor(self):
        self.after_lease()
        overtaking = []
        with self.during_verification(lambda: overtaking.append(self.renew_directly())):
            original = self.renew_directly()
        self.assertEqual(original, overtaking[0])
        self.assertEqual(len(self.journal()), 1)

    def test_second_request_for_one_sequence_gets_no_second_successor(self):
        self.after_lease()
        self.renew_directly()
        with self.assertRaisesRegex(Conflict, 'replay conflict'):
            self.renew_directly(self.request(renewal_id='different-renewal'))
        self.assertEqual(len(self.journal()), 1)

    # Wrong instance, generation and environment.

    def test_expired_grant_alone_is_refused_before_any_journal_entry(self):
        self.after_lease()
        worker = {'Authorization': 'Bearer ' + WORKER_TOKEN}
        claim = {'X-Claim-Token': self.claim_token}
        for headers in ({}, claim, worker, {'Authorization': 'Bearer ' + self.first_grant['token'], **claim}):
            reply = self.post(RENEWAL_PATH, self.request(), headers)
            self.assertEqual((reply.status, reply.body), (401, {'error': 'unauthorized'}), headers)
        self.assertEqual(self.journal(), [])

    def test_replay_never_discloses_a_successor_without_the_claim_owner(self):
        self.after_lease()
        self.renew_directly()
        self.assertEqual(self.post(RENEWAL_PATH, self.request(), {}).status, 401)
        for change in ({'worker_instance_id': 'replacement'}, {'claim_token': 'not-the-claim'}):
            with self.assertRaises(Conflict):
                self.renew_directly(**change)

    def test_another_worker_cannot_inherit_the_claim(self):
        self.after_lease()
        for change in ({'worker_instance_id': 'replacement'}, {'worker_id': 'other-worker'},
                       {'node_id': 'main'}, {'claim_token': 'not-the-claim'}):
            with self.assertRaises((Conflict, ValueError), msg=change):
                self.renew_directly(**change)
        self.assertEqual(self.journal(), [])

    def test_another_worker_cannot_install_a_successor(self):
        self.after_lease()
        reply = self.renew_directly()
        other = {key: self.worker[key] for key in ('node_id', 'worker_id')}
        with self.assertRaises(Conflict):
            self.store.acknowledge_source_transfer_grant(self.ack(reply),
                worker=dict(other, worker_instance_id='replacement'))
        self.assertEqual(self.current_grant(), self.first_grant)

    def test_grant_from_another_generation_or_environment_is_refused(self):
        self.after_lease()
        for change in ({'generation': self.first_grant['generation'] + 1},
                       {'environment': 'production' if self.first_grant['environment'] == 'staging' else 'staging'},
                       {'manifestSha256': 'f' * 64}):
            with self.assertRaises(Conflict, msg=change):
                self.renew_directly(self.request(dict(self.first_grant, **change)))
        self.assertEqual(self.journal(), [])

    def test_job_moved_to_a_new_generation_refuses_the_old_grant(self):
        self.after_lease()
        self.execute('UPDATE reference_jobs SET generation=generation+1 WHERE job_id=?', self.job_id)
        with self.assertRaises(Conflict):
            self.renew_directly()
        self.assertEqual(self.journal(), [])

    def test_superseded_grant_cannot_renew(self):
        self.after_lease()
        reply = self.renew_directly()
        self.store.acknowledge_source_transfer_grant(self.ack(reply),
            worker={key: self.worker[key] for key in ('node_id', 'worker_id', 'worker_instance_id')})
        self.now[0] = stamp(reply['grant']['expiresAt']) + 1
        with self.assertRaisesRegex(Conflict, 'previous grant changed'):
            self.renew_directly(self.request(renewal_id='stale-renewal', sequence=2))

    # Cancellation and maintenance during renewal.

    def test_terminal_or_held_job_issues_nothing(self):
        self.after_lease()
        for status in ('cancelled', 'failed', 'succeeded', 'held'):
            self.set_job_status(status)
            with self.assertRaises(DriverHeld, msg=status):
                self.renew()
            self.assertEqual(self.journal(), [])
            self.set_job_status('running')
            self.data.pop('sourceRenewal', None)

    def test_cancellation_during_verification_commits_nothing(self):
        self.after_lease()
        with self.during_verification(lambda: self.set_job_status('cancelled')):
            with self.assertRaises(Conflict):
                self.renew_directly()
        self.assertEqual(self.journal(), [])

    def test_cancellation_after_a_lost_reply_withholds_the_successor(self):
        self.after_lease()
        reply = self.renew_directly()
        self.set_job_status('cancelled')
        with self.assertRaises(Conflict):
            self.renew_directly()
        with self.assertRaises(Conflict):
            self.store.acknowledge_source_transfer_grant(self.ack(reply),
                worker={key: self.worker[key] for key in ('node_id', 'worker_id', 'worker_instance_id')})
        self.assertEqual(self.current_grant(), self.first_grant)

    def test_node_update_or_hold_defers_then_the_same_request_succeeds(self):
        self.after_lease()
        for state in ('updating', 'held', 'failed'):
            self.execute('UPDATE nodes SET state=? WHERE id=?', state, 'cluster')
            with self.assertRaisesRegex(DriverWait, 'source_renewal_not_ready'):
                self.renew()
            self.assertEqual(self.journal(), [])
        renewal_id = self.data['sourceRenewal']['renewalId']
        self.execute("UPDATE nodes SET state='active' WHERE id=?", 'cluster')
        self.renew()
        self.assertEqual([row['renewal_id'] for row in self.journal()], [renewal_id])

    def test_node_hold_during_verification_commits_nothing(self):
        self.after_lease()
        with self.during_verification(lambda: self.execute("UPDATE nodes SET state='held' WHERE id=?", 'cluster')):
            with self.assertRaises(Conflict):
                self.renew_directly()
        self.assertEqual(self.journal(), [])

    def test_node_hold_defers_the_ack_without_losing_the_successor(self):
        self.after_lease()
        self.opener(_Outage(self.control, RENEWAL_ACK_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.opener(_ControlOpener(self.control))
        self.execute("UPDATE nodes SET state='held' WHERE id=?", 'cluster')
        with self.assertRaisesRegex(DriverWait, 'source_renewal_ack_unavailable'):
            self.renew()
        self.execute("UPDATE nodes SET state='active' WHERE id=?", 'cluster')
        self.renew()
        self.assertEqual([(row['sequence'], row['acked']) for row in self.journal()], [(1, 1)])

    def test_drain_starting_during_verification_does_not_refuse_the_claim(self):
        self.after_lease()
        with self.during_verification(lambda: self.execute("UPDATE nodes SET state='draining' WHERE id=?", 'cluster')):
            self.renew_directly()
        self.assertEqual(len(self.journal()), 1)

    def test_draining_node_lets_an_existing_claim_finish(self):
        self.after_lease()
        self.execute("UPDATE nodes SET state='draining' WHERE id=?", 'cluster')
        self.assertEqual(self.fetch(self.renew(), 0), b'abc')

    def test_source_reader_fence_during_verification_commits_nothing(self):
        self.after_lease()
        fence = lambda: self.store.source_readers().fence(self.job_id, self.first_grant['generation'], 'renewal-fence')
        with self.during_verification(fence):
            with self.assertRaisesRegex(Conflict, 'source_release_fenced'):
                self.renew_directly()
        self.assertEqual(self.journal(), [])

    def test_coordinator_freeze_during_renewal_defers_the_exact_request(self):
        self.after_lease()
        with self.during_verification(lambda: self.freeze('intent')):
            with self.assertRaises(CoordinatorFrozen):
                self.renew_directly()
        self.assertEqual(self.journal(), [])
        self.freeze('abort', healthy=True)
        self.renew_directly()
        self.assertEqual(len(self.journal()), 1)

    # Budget and persisted object acknowledgements.

    def test_spent_attempt_budget_gets_no_successor(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.fetch(first, 0)
        self.after_lease()
        self.execute('UPDATE reference_source_range_usage SET attempts_used=? WHERE job_id=?',
            first.attempt_budget, self.job_id)
        self.execute('UPDATE reference_source_range_requests SET attempts_used=? WHERE job_id=?',
            first.attempt_budget, self.job_id)
        with self.assertRaisesRegex(Conflict, 'budget exhausted'):
            self.renew_directly()
        self.assertEqual(self.journal(), [])

    def test_range_ledger_that_disagrees_gets_no_successor(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.fetch(first, 0)
        self.after_lease()
        reservation = self.first_grant['reservationId']
        item = first.objects[0].id
        for tamper, restore in (
                ("UPDATE reference_source_range_usage SET bytes_used=bytes_used+1 WHERE job_id=?",
                 "UPDATE reference_source_range_usage SET bytes_used=bytes_used-1 WHERE job_id=?"),
                ("UPDATE reference_source_range_usage SET reservation_id='other' WHERE job_id=?",
                 f"UPDATE reference_source_range_usage SET reservation_id='{reservation}' WHERE job_id=?"),
                ("UPDATE reference_source_range_requests SET object_id='not-granted' WHERE job_id=?",
                 f"UPDATE reference_source_range_requests SET object_id='{item}' WHERE job_id=?")):
            self.execute(tamper, self.job_id)
            with self.assertRaisesRegex(Conflict, 'range ledger changed', msg=tamper):
                self.renew_directly()
            self.execute(restore, self.job_id)
        self.assertEqual(self.journal(), [])
        self.renew_directly()

    def test_missing_object_receipt_or_changed_pin_gets_no_successor(self):
        self.after_lease()
        object_id = self.first_grant['objectIds'][0]
        self.execute("UPDATE reference_source_object_pins SET inode='0' WHERE job_id=? AND object_id=?",
            self.job_id, object_id)
        with self.assertRaises(Conflict):
            self.renew_directly()
        self.execute('DELETE FROM reference_source_object_receipts WHERE job_id=? AND object_id=?',
            self.job_id, object_id)
        with self.assertRaises(Conflict):
            self.renew_directly()
        self.assertEqual(self.journal(), [])


class ExhaustedBoundsTests(RenewalHarness):
    """There is no wall-clock limit; finite budgets end in a hold, never a delete."""

    def setUp(self):
        super().setUp()
        self.first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.kept = self.root / 'fetched-input.bin'
        self.client.download_object(grant=self.first, object_id=self.first.objects[0].id, destination=self.kept)
        self.after_lease()
        self.before = self.retained()

    def retained(self):
        """Everything either side would lose if an exhausted bound cleaned up."""
        tables = ('reference_source_object_receipts', 'reference_source_object_pins', 'reference_transfer_grants')
        with self.queue._db() as db:
            rows = {table: [tuple(row) for row in db.execute(f'SELECT * FROM {table} WHERE job_id=?', (self.job_id,))]
                    for table in tables}
            status = tuple(db.execute('SELECT status,held_reason FROM jobs WHERE id=?', (self.job_id,)).fetchone())
        spool = {path.name: path.read_bytes() for path in self.spool.root.rglob('*') if path.is_file()}
        worker = sorted(item.name for item in self.root.glob('*.bin'))
        return rows, status, spool, self.kept.read_bytes(), worker

    def assert_held_with_nothing_deleted(self, reason):
        for _attempt in range(2):
            with self.assertRaisesRegex(DriverHeld, reason):
                self.renew()
            self.assertEqual(self.retained(), self.before)
            self.assertEqual(self.journal(), [])
            self.assertEqual(self.data['grant'], self.first_grant)

    def test_spent_attempt_budget_holds(self):
        budget = self.first.attempt_budget
        self.execute('UPDATE reference_source_range_usage SET attempts_used=? WHERE job_id=?', budget, self.job_id)
        self.execute('UPDATE reference_source_range_requests SET attempts_used=? WHERE job_id=?', budget, self.job_id)
        self.assert_held_with_nothing_deleted('source_renewal_rejected')

    def test_spent_byte_budget_holds(self):
        over = self.first.byte_budget + 1
        self.execute('UPDATE reference_source_range_usage SET bytes_used=? WHERE job_id=?', over, self.job_id)
        self.execute('UPDATE reference_source_range_requests SET range_length=? WHERE job_id=?', over, self.job_id)
        self.assert_held_with_nothing_deleted('source_renewal_rejected')

    def test_spent_request_budget_holds(self):
        extra = self.first.request_budget
        for index in range(extra):
            self.execute('INSERT INTO reference_source_range_requests VALUES(?,?,?,?,?,?,?)', self.job_id,
                self.first_grant['generation'], f'extra-{index}', self.first.objects[0].id, 0, 0, 1)
        self.execute('''UPDATE reference_source_range_usage SET requests_used=requests_used+?,
            attempts_used=attempts_used+? WHERE job_id=?''', extra, extra, self.job_id)
        self.assert_held_with_nothing_deleted('source_renewal_rejected')

    def test_spent_renewal_sequence_holds_without_asking_the_coordinator(self):
        self.data['sourceRenewalSequence'] = MAX_RENEWAL_SEQUENCE
        self.opener(lambda *_args, **_kwargs: self.fail('an exhausted sequence must not reach the coordinator'))
        self.assert_held_with_nothing_deleted('source_renewal_sequence_exhausted')

    def test_coordinator_refuses_a_sequence_past_the_bound(self):
        with self.assertRaisesRegex(Conflict, 'sequence invalid'):
            self.renew_directly(self.request(sequence=MAX_RENEWAL_SEQUENCE + 1))
        self.assertEqual(self.retained(), self.before)

    def test_time_alone_is_not_a_bound(self):
        self.now[0] += 10 * 365 * 24 * 3600
        self.assertEqual(self.fetch(self.renew(), 1), b'1,2,3\n')


class RenewalRestartTests(RenewalHarness):
    """A real restart holds every running job, so the journal must outlast the hold."""

    def held_reason(self):
        with self.queue._db() as db:
            return tuple(db.execute('SELECT status,held_reason FROM jobs WHERE id=?', (self.job_id,)).fetchone())

    def test_restart_holds_the_claim_and_renewal_issues_nothing_new(self):
        self.after_lease()
        self.restart_coordinator()
        self.assertEqual(self.held_reason(), ('held', 'coordinator_restart'))
        with self.assertRaisesRegex(DriverHeld, 'source_renewal_rejected'):
            self.renew()
        self.assertEqual(self.journal(), [])
        self.data.pop('sourceRenewal')
        self.assertIsNone(self.admit())
        self.assertEqual(self.fetch(self.renew(), 0), b'abc')

    def test_lost_renewal_reply_replays_the_same_successor_after_a_restart(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        minted = self.journal()
        intent = copy.deepcopy(self.data['sourceRenewal'])
        self.restart_coordinator()
        with self.assertRaisesRegex(DriverHeld, 'source_renewal_rejected'):
            self.renew()
        self.assertEqual(self.data['sourceRenewal'], intent)
        self.assertIsNone(self.admit())
        renewed = self.renew()
        self.assertEqual(hashlib.sha256(protocol.canonical_json(self.data['grant'])).hexdigest(),
            minted[0]['successor_sha256'])
        self.assertEqual([(row['sequence'], row['acked']) for row in self.journal()], [(1, 1)])
        self.assertEqual(self.fetch(renewed, 0), b'abc')

    def test_lost_ack_reply_replays_after_a_restart_but_reads_wait_for_the_hold(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_ACK_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.restart_coordinator()
        renewed = self.renew()
        self.assertEqual([(row['sequence'], row['acked']) for row in self.journal()], [(1, 1)])
        with self.assertRaises(Conflict):
            self.read_range(renewed.token, renewed)
        self.assertIsNone(self.admit())
        self.assertEqual(self.fetch(renewed, 1), b'1,2,3\n')

    def test_replay_after_a_restart_still_needs_the_claim_owner(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.opener(_ControlOpener(self.control))
        request = self.data['sourceRenewal']['request']
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.assertEqual(self.post(RENEWAL_PATH, request, {}).status, 401)
        with self.assertRaises(Conflict):
            self.renew_directly(request, worker_instance_id='replacement')
        self.assertEqual(self.renew_directly(request)['sequence'], 1)

    def test_claim_recovery_resumes_a_source_range_transfer(self):
        self.restart_coordinator()
        identity = {key: self.worker[key] for key in ('node_id', 'worker_id', 'worker_instance_id')}
        state = self.control.worker.recovery.state(self.job_id, identity=identity, claim_token=self.claim_token)
        self.assertTrue(state['eligible'])
        self.assertEqual(state['claim']['executionPhase'], 'fetching')
        self.assertIsNone(self.admit())
        self.assertEqual(self.held_reason(), ('running', None))


class RenewalAcrossRangesTests(RenewalHarness):
    """One object over several ranges, with the canonical chunk scaled down."""
    chunk_bytes = 2

    def test_one_object_resumes_at_its_next_range_after_renewal_past_the_lease(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        commit = self.client._commit_range

        def commit_then_outlive(key, object_id, request_id, start, length, digest):
            commit(key, object_id, request_id, start, length, digest)
            if start == 0:
                self.after_lease()

        with patch.object(self.client, '_commit_range', side_effect=commit_then_outlive):
            self.assertEqual(self.fetch(first, 0), b'abc')
        self.assertEqual(self.data['sourceRenewalSequence'], 1)
        self.assertEqual(self.usage(), (2, 3, 2))
        with self.queue._db() as db:
            served = [tuple(row) for row in db.execute('''SELECT range_start,range_length,attempts_used
                FROM reference_source_range_requests WHERE job_id=? ORDER BY range_start''', (self.job_id,))]
        self.assertEqual(served, [(0, 2, 1), (2, 1, 1)])


class RenewalSourceIoTests(RenewalHarness):
    """Verification's NAS descriptors under the coordinator's own lifecycle."""

    def setUp(self):
        super().setUp()
        self.generation = self.first_grant['generation']
        self.after_lease()
        self.readers = self.store.source_readers()

    def renew_in_background(self):
        outcome = []

        def run():
            try:
                outcome.append(self.renew_directly())
            except BaseException as error:
                outcome.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        self.addCleanup(thread.join, 10)
        return thread, outcome

    @contextmanager
    def blocked_open(self):
        real = intake_store._open_pinned_source
        entered, release = threading.Event(), threading.Event()

        def stalled(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise AssertionError('blocked open was not released')
            return real(*args, **kwargs)
        with patch.object(intake_store, '_open_pinned_source', side_effect=stalled):
            try:
                yield entered, release
            finally:
                release.set()

    @contextmanager
    def blocked_close(self):
        real = intake_store._open_pinned_source
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        wrapped = []

        def opened(*args, **kwargs):
            reader, identity = real(*args, **kwargs)
            if not wrapped:
                wrapped.append(True)
                reader = _BlockedClose(reader, entered, release, finished)
            return reader, identity
        with patch.object(intake_store, '_open_pinned_source', side_effect=opened):
            try:
                yield entered, release, finished
            finally:
                release.set()

    def finish(self, thread, outcome):
        thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcome), 1)
        return outcome[0]

    def assert_one_commit_or_none(self, result):
        self.assertEqual(len(self.journal()), 0 if isinstance(result, BaseException) else 1, repr(result))

    def seal_when_drained(self):
        deadline = time.monotonic() + 5
        while True:
            try:
                return self.freeze('seal')
            except Conflict:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.02)

    def test_blocked_open_keeps_freeze_from_sealing(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            self.freeze('intent')
            self.freeze('drain')
            with self.assertRaises(Conflict):
                self.freeze('seal')
            release.set()
            self.assertIsInstance(self.finish(thread, outcome), CoordinatorFrozen)
        self.assertEqual(self.journal(), [])
        self.assertEqual(self.seal_when_drained()['phase'], 'frozen')
        self.freeze('resume', healthy=True)

    def test_blocked_open_keeps_release_from_starting(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            self.readers.fence(self.job_id, self.generation, 'renewal-release')
            with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
                self.readers.reserve_release(self.job_id, self.generation, 'renewal-release', authorize=lambda _db: None)
            release.set()
            result = self.finish(thread, outcome)
        self.assertIsInstance(result, SourceReaderError)
        self.assertEqual(self.journal(), [])
        self.assertEqual(self.readers.active_count(self.job_id, self.generation), 0)
        self.readers.reserve_release(self.job_id, self.generation, 'renewal-release', authorize=lambda _db: None).close()

    def test_blocked_open_keeps_coordinator_authority(self):
        with self.blocked_open() as (entered, release):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            with self.assertRaises(SourceReaderError):
                self.queue.close()
            release.set()
            self.assertFalse(isinstance(self.finish(thread, outcome), BaseException))
        self.assertEqual(len(self.journal()), 1)

    def test_blocked_close_is_drained_before_freeze_seals(self):
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            sealed = []

            def freeze():
                self.freeze('intent')
                self.freeze('drain')
                self.seal_when_drained()
                sealed.append(finished.is_set())
            freezer = threading.Thread(target=freeze)
            freezer.start()
            self.addCleanup(freezer.join, 10)
            freezer.join(0.5)
            self.assertTrue(freezer.is_alive(), 'freeze sealed with a source close outstanding')
            release.set()
            result = self.finish(thread, outcome)
            freezer.join(10)
        self.assertEqual(sealed, [True])
        self.assert_one_commit_or_none(result)
        self.freeze('resume', healthy=True)

    def test_blocked_close_is_drained_before_release_starts(self):
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            started = []

            def run_release():
                self.readers.fence(self.job_id, self.generation, 'renewal-release')
                deadline = time.monotonic() + 5
                while True:
                    try:
                        handle = self.readers.reserve_release(self.job_id, self.generation, 'renewal-release',
                            authorize=lambda _db: None)
                    except SourceReaderError:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.02)
                        continue
                    started.append(finished.is_set())
                    handle.close()
                    return
            releaser = threading.Thread(target=run_release)
            releaser.start()
            self.addCleanup(releaser.join, 10)
            releaser.join(0.5)
            self.assertTrue(releaser.is_alive(), 'release started with a source close outstanding')
            release.set()
            result = self.finish(thread, outcome)
            releaser.join(10)
        self.assertEqual(started, [True])
        self.assert_one_commit_or_none(result)

    def test_blocked_close_is_drained_before_queue_close(self):
        with self.blocked_close() as (entered, release, finished):
            thread, outcome = self.renew_in_background()
            self.assertTrue(entered.wait(2))
            closing = []

            def close_queue():
                try:
                    self.queue.close()
                    closing.append(('closed', finished.is_set()))
                except (SourceReaderError, RuntimeError, CoordinatorFrozen):
                    closing.append(('refused', finished.is_set()))
            closer = threading.Thread(target=close_queue)
            closer.start()
            self.addCleanup(closer.join, 10)
            closer.join(0.5)
            self.assertTrue(closer.is_alive(), 'Queue.close returned with a source close outstanding')
            release.set()
            self.finish(thread, outcome)
            closer.join(10)
        self.assertTrue(closing[0][1], closing)

    def test_unknown_close_retains_the_pin_and_coordinator_authority(self):
        real, wrapped = intake_store._open_pinned_source, []

        def opened(*args, **kwargs):
            reader, identity = real(*args, **kwargs)
            if not wrapped:
                wrapped.append(True)
                reader = _UnknownClose(reader)
            return reader, identity
        with patch.object(intake_store, '_open_pinned_source', side_effect=opened):
            with self.assertRaisesRegex(Conflict, 'renewal_authority_unavailable'):
                self.renew_directly()
        self.assertEqual(self.journal(), [])
        self.assertEqual(self.readers.active_count(self.job_id, self.generation), 1)
        held = [handle for handle in self.readers._handles.values() if handle.uncertain]
        self.assertEqual(len(held), 1)
        with self.assertRaises(SourceReaderError):
            self.queue.close()
        self.readers.fence(self.job_id, self.generation, 'renewal-release')
        with self.assertRaisesRegex(SourceReaderError, 'source_operation_busy'):
            self.readers.reserve_release(self.job_id, self.generation, 'renewal-release', authorize=lambda _db: None)
        # Only the process owner's exit may retire this in production. Reconcile
        # by hand so cleanup can close the coordinator this test still owns.
        held[0].uncertain, held[0]._known_closed = False, True
        held[0].close()

    def test_exact_retry_after_an_unknown_close_keeps_that_pin(self):
        real, wrapped = intake_store._open_pinned_source, []

        def opened(*args, **kwargs):
            reader, identity = real(*args, **kwargs)
            if not wrapped:
                wrapped.append(True)
                reader = _UnknownClose(reader)
            return reader, identity
        with patch.object(intake_store, '_open_pinned_source', side_effect=opened):
            with self.assertRaises(Conflict):
                self.renew_directly()
        self.renew_directly()
        self.assertEqual(len(self.journal()), 1)
        self.assertEqual(self.readers.active_count(self.job_id, self.generation), 1)
        held = [handle for handle in self.readers._handles.values() if handle.uncertain]
        held[0].uncertain, held[0]._known_closed = False, True
        held[0].close()

    def test_failed_reauthorization_after_open_closes_the_descriptor(self):
        real, opened_files = intake_store._open_pinned_source, []

        def opened(*args, **kwargs):
            reader, identity = real(*args, **kwargs)
            opened_files.append(reader)
            self.set_job_status('cancelled')
            return reader, identity
        with patch.object(intake_store, '_open_pinned_source', side_effect=opened):
            with self.assertRaises(Conflict):
                self.renew_directly()
        self.assertTrue(opened_files[0].closed)
        self.assertEqual(self.readers.active_count(self.job_id, self.generation), 0)
        self.assertEqual(self.journal(), [])


if __name__ == '__main__':
    unittest.main()
