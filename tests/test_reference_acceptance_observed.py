"""Assigned worker observation, durable retry and existing cleanup interaction."""
from contextlib import contextmanager
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import ssl
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import uuid

from processing import reference_protocol as p
from processing.reference_acceptance_observed import make_request, validate_request, make_ack, validate_ack, ACK_FIELDS, ensure_schema, PROTOCOL
from processing.reference_control import ReferenceControlAPI
from processing.maintenance import Conflict
from processing.reference_delivery import DeliveryReceiver
from processing.reference_driver import DriverHeld, DriverWait
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI, PREFIX
from processing.scratch_cleanup import ScratchCleanupExecutor
from tests.viewer_v2_receipt_signer import SyntheticViewerCopyV2Signer


WORKER = 'synthetic-observation-worker-token-012345'
OTHER = 'synthetic-observation-other-token-012345'
IDENTITY = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')


class AcceptanceObservationContractTests(unittest.TestCase):
    def request(self):
        return dict(schemaVersion=1, protocol=PROTOCOL, observationId=str(uuid.uuid4()),
            coordinatorAuthorityId='authority', jobId='job-one', generation=1, environment='staging',
            inputManifestSha256='a' * 64, pipelineDigest='b' * 64, resultManifestSha256='c' * 64,
            receipts=[dict(acceptanceReceiptId='acceptance-one', acceptanceSha256='d' * 64,
                viewerReceiptId='viewer-one', viewerReceiptSha256='e' * 64)])

    def test_closed_bounded_unique_request_rejects_bool_and_unknown_claims(self):
        request = self.request()
        self.assertEqual(validate_request(request), request)
        for field, value in (('schemaVersion', True), ('generation', True), ('generation', 0),
                ('observationId', 'noncanonical'), ('environment', 'development'),
                ('receipts', []), ('receipts', request['receipts'] * (p.MAX_OBJECTS + 1))):
            with self.subTest(field=field, value_type=type(value).__name__):
                changed = {**request, field: value}
                with self.assertRaises(ValueError): validate_request(changed)
        with self.assertRaises(ValueError): validate_request({**request, 'fullyAccepted': True})
        duplicate = copy.deepcopy(request); duplicate['receipts'] *= 2
        with self.assertRaises(ValueError): validate_request(duplicate)

    def test_ack_binds_exact_request_owner_time_and_closed_metadata(self):
        request = self.request()
        ack = make_ack(request, IDENTITY, '2026-11-05T12:00:00.000000Z')
        self.assertEqual(validate_ack(ack, request, IDENTITY), ack)
        for field, value in (('requestSha256', 'f' * 64), ('workerInstanceId', 'other-instance'),
                ('generation', 2), ('observedAt', 'not-a-time')):
            with self.subTest(field=field), self.assertRaises(ValueError):
                validate_ack({**ack, field: value}, request, IDENTITY)
        with self.assertRaises(ValueError): validate_ack({**ack, 'sourceReleaseAuthorized': True}, request, IDENTITY)


@unittest.skipUnless(sys.platform.startswith('linux'), 'real Linux source and receipt fixture')
class AcceptanceObservationStoreTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_v2_store_acceptance import ReferenceV2StoreAcceptanceTests
        self.temporary = tempfile.TemporaryDirectory()
        self.state = ReferenceV2StoreAcceptanceTests('runTest')._make_source_result_ready(self.temporary.name, Path('.'))
        self.queue, self.store = self.state['queue'], self.state['store']
        self.signer = SyntheticViewerCopyV2Signer()
        self.envelope = self.signer.envelope(self.state['expected'], self.state['acceptance'], self.state['viewer_result_manifest'])
        self.store.accept(self.state['job']['jobId'], self.envelope['acceptance'],
            viewer_receipt=self.envelope['viewerReceipt'], trusted_viewer_receipt_keys=self.signer.trusted_keys)
        self.job = self.store.get(self.state['job']['jobId'], private=True)
        self.intent = make_request(self.job, self.state['result'], [self.envelope], str(uuid.uuid4()))
        self.api = ReferenceWorkerAPI(self.store, DeliveryReceiver(self.store), bearer_workers={WORKER: IDENTITY,
            OTHER: dict(node_id='cluster', worker_id='other-worker', worker_instance_id='other-instance')})

    def tearDown(self):
        self.queue.close()
        self.state['mount_patch'].stop()
        self.temporary.cleanup()

    def post(self, intent=None, *, bearer=WORKER, token=None):
        return self.api.handle('POST', PREFIX + '/jobs/' + self.job['jobId'] + '/acceptance-observed',
            {'Authorization': 'Bearer ' + bearer, 'X-Claim-Token': self.state['claim_token'] if token is None else token},
            p.canonical_json(self.intent if intent is None else intent), protected_transport=True)

    def test_completed_job_retains_owner_authentication_and_rejects_untrusted_metadata(self):
        self.assertEqual(self.job['phase'], 'completed')
        self.assertEqual(self.post(bearer=OTHER).status, 409)
        self.assertEqual(self.post(token='wrong-claim').status, 409)
        self.assertEqual(self.post(bearer='unknown-worker-token').status, 401)
        for field, value in (('generation', 2), ('environment', 'production'),
                ('inputManifestSha256', 'b' * 64), ('pipelineDigest', 'b' * 64),
                ('resultManifestSha256', 'b' * 64), ('coordinatorAuthorityId', 'different-authority')):
            with self.subTest(field=field):
                intent = copy.deepcopy(self.intent); intent[field] = value
                self.assertEqual(self.post(intent).status, 409)
        forged = copy.deepcopy(self.intent)
        forged['receipts'][0]['viewerReceiptSha256'] = 'c' * 64
        self.assertEqual(self.post(forged).status, 409)
        malformed = copy.deepcopy(self.intent); malformed['fullyAccepted'] = True
        self.assertEqual(self.post(malformed).status, 400)
        malformed = copy.deepcopy(self.intent); malformed['generation'] = True
        self.assertEqual(self.post(malformed).status, 400)

    def test_exact_replay_uses_stored_time_and_no_new_quota(self):
        fixed = datetime(2026, 11, 5, 12, 34, 56, 789, tzinfo=timezone.utc)
        with patch.object(self.queue, '_clock', return_value=fixed):
            first = self.post()
        self.assertEqual(first.status, 200, first.body)
        self.assertEqual(set(first.body), set(ACK_FIELDS))
        self.assertEqual(first.body['observedAt'], '2026-11-05T12:34:56.000789Z')
        self.assertEqual(validate_ack(first.body, self.intent, IDENTITY), first.body)
        self.store.max_metadata_bytes = 1
        with patch.object(self.queue, '_clock', side_effect=AssertionError('replay must not mint a new time')):
            replay = self.post()
        self.assertEqual(replay.status, 200, replay.body)
        self.assertEqual(p.canonical_json(first.body), p.canonical_json(replay.body))
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_worker_acceptance_observations').fetchone()[0], 1)

    def test_conflicting_observation_and_changed_receipt_bytes_do_not_replace_journal(self):
        self.assertEqual(self.post().status, 200)
        changed = copy.deepcopy(self.intent); changed['observationId'] = str(uuid.uuid4())
        self.assertEqual(self.post(changed).status, 409)
        with self.queue._db() as db:
            db.execute("UPDATE reference_viewer_receipts SET digest=?", ('f' * 64,))
        self.assertEqual(self.post().status, 409)

    def test_request_receipt_subset_must_itself_cover_full_result(self):
        import base64
        from processing.viewer_copy_receipt_v2 import SIGNATURE_DOMAIN
        acceptance = copy.deepcopy(self.envelope['acceptance'])
        acceptance.update(receiptId=str(uuid.uuid4()), storageReceiptId=str(uuid.uuid4()),
            acceptedObjectIds=acceptance['acceptedObjectIds'][:1])
        signed = self.signer.receipt(self.state['expected'], acceptance, self.state['viewer_result_manifest'])
        signed['acceptedObjectIds'] = acceptance['acceptedObjectIds']
        signed['objects'] = [item for item in signed['objects'] if item['id'] in acceptance['acceptedObjectIds']]
        body = {key: value for key, value in signed.items() if key != 'signature'}
        signed['signature'] = base64.urlsafe_b64encode(self.signer._key.sign(SIGNATURE_DOMAIN + p.canonical_json(body))).decode().rstrip('=')
        from processing.maintenance import Conflict
        with self.assertRaises(Conflict):
            self.store.accept(self.job['jobId'], acceptance, viewer_receipt=signed,
                trusted_viewer_receipt_keys=self.signer.trusted_keys)
        # V2 ingestion already requires complete closure. Also exercise the
        # observation's independent coverage check against a damaged/legacy
        # journal row rather than trusting its completed phase or stored digest.
        with self.queue._db() as db:
            db.execute('INSERT INTO reference_acceptances(receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?)',
                (acceptance['receiptId'], self.job['jobId'], p.digest(acceptance), p.canonical_json(acceptance).decode()))
            db.execute('INSERT INTO reference_viewer_receipts(receipt_id,storage_receipt_id,acceptance_receipt_id,job_id,digest,receipt_json) VALUES(?,?,?,?,?,?)',
                (signed['receiptId'], signed['storageReceiptId'], acceptance['receiptId'], self.job['jobId'],
                    p.digest(signed), p.canonical_json(signed).decode()))
        partial = make_request(self.job, self.state['result'], [dict(acceptance=acceptance, viewerReceipt=signed)], str(uuid.uuid4()))
        self.assertEqual(self.post(partial).status, 409)
        # Already-completed Store status is insufficient; the selected set must
        # cover every object. The original full pair remains independently valid.
        self.assertEqual(self.post().status, 200)

    def test_observation_uuid_cannot_be_rebound_from_another_generation(self):
        with self.queue._db() as db:
            ensure_schema(db)
            db.execute('INSERT INTO reference_worker_acceptance_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                (self.job['jobId'], 2, self.intent['observationId'], 'a' * 64, '{}', '{}',
                    'cluster', 'worker', 'instance', 'b' * 64))
        self.assertEqual(self.post().status, 409)

    def test_committed_observation_replays_after_custody_hold_without_new_authority(self):
        first = self.post()
        self.assertEqual(first.status, 200)
        self.store._hold_source_intake(self.job['jobId'])
        self.assertEqual(self.queue.get(self.job['jobId'])['status'], 'held')
        with patch.object(self.queue, '_clock', side_effect=AssertionError('historical acknowledgement must remain stable')):
            replay = self.post()
        self.assertEqual(replay.status, 200, replay.body)
        self.assertEqual(replay.body, first.body)
        self.assertEqual(self.store.worker_acceptance_observation(self.job['jobId']), first.body)
        from tests.test_reference_v2_store_acceptance import VIEWER_BEARER
        lookup = self.state['api'].handle('GET', '/ltds/v2/jobs/' + self.job['jobId'] + '/source/acceptance-observation',
            {'Authorization': 'Bearer ' + VIEWER_BEARER}, protected_transport=True)
        self.assertEqual(lookup.status, 200)
        self.assertEqual(lookup.body, first.body)
        different = copy.deepcopy(self.intent); different['observationId'] = str(uuid.uuid4())
        self.assertEqual(self.post(different).status, 409)

    def test_first_observation_under_held_custody_is_refused(self):
        with self.queue._db() as db:
            db.execute("UPDATE reference_source_intakes SET source_state='held' WHERE job_id=?", (self.job['jobId'],))
        self.assertEqual(self.post().status, 409)
        self.assertIsNone(self.store.worker_acceptance_observation(self.job['jobId']))

    def test_quota_failure_is_atomic_and_then_same_intent_can_succeed(self):
        with self.queue._db() as db:
            used = self.store._metadata_used_bytes(db)
        self.store.max_metadata_bytes = used
        self.assertEqual(self.post().status, 409)
        with self.queue._db() as db:
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_worker_acceptance_observations'").fetchone():
                self.assertEqual(db.execute('SELECT count(*) FROM reference_worker_acceptance_observations').fetchone()[0], 0)
        self.store.max_metadata_bytes = used + 100_000
        self.assertEqual(self.post().status, 200)

    def test_viewer_lookup_scoped_pending_and_stable_without_private_receipts(self):
        viewer = self.state['api']
        from tests.test_reference_v2_store_acceptance import VIEWER_BEARER
        path = '/ltds/v2/jobs/' + self.job['jobId'] + '/source/acceptance-observation'
        headers = {'Authorization': 'Bearer ' + VIEWER_BEARER}
        self.assertEqual(viewer.handle('GET', path, headers, protected_transport=True).status, 404)
        ack = self.post().body
        response = viewer.handle('GET', path, headers, protected_transport=True)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, ack)
        raw = p.canonical_json(response.body).decode()
        self.assertNotIn(self.state['claim_token'], raw)
        self.assertNotIn('receipts', response.body)
        self.assertNotIn(self.envelope['viewerReceipt']['receiptId'], raw)
        with patch.object(viewer, '_bearers', {**viewer._bearers, 'synthetic-other-environment-token': 'production'}):
            wrong = viewer.handle('GET', path, {'Authorization': 'Bearer synthetic-other-environment-token'}, protected_transport=True)
        self.assertEqual(wrong.status, 404)
        self.assertEqual(viewer.handle('GET', path, {}, protected_transport=True).status, 401)

    def lookup_response(self):
        from tests.test_reference_v2_store_acceptance import VIEWER_BEARER
        return self.state['api'].handle('GET', '/ltds/v2/jobs/' + self.job['jobId'] + '/source/acceptance-observation',
            {'Authorization': 'Bearer ' + VIEWER_BEARER}, protected_transport=True)

    def test_lookup_rejects_self_consistent_ack_for_wrong_input_pipeline_result_or_environment(self):
        first = self.post()
        self.assertEqual(first.status, 200)
        for field, value in (('inputManifestSha256', 'f' * 64), ('pipelineDigest', 'f' * 64),
                ('resultManifestSha256', 'f' * 64), ('environment', 'production')):
            with self.subTest(field=field):
                request = {**self.intent, field: value}
                ack = make_ack(request, IDENTITY, first.body['observedAt'])
                with self.queue._db() as db:
                    db.execute('UPDATE reference_worker_acceptance_observations SET request_sha256=?,request_json=?,ack_json=?',
                        (p.digest(request), p.canonical_json(request).decode(), p.canonical_json(ack).decode()))
                self.assertEqual(self.lookup_response().status, 409)

    def test_lookup_requires_canonical_stored_bytes_and_original_owner_even_for_valid_ack_hash(self):
        first = self.post()
        self.assertEqual(first.status, 200)
        with self.queue._db() as db:
            original = dict(db.execute('SELECT * FROM reference_worker_acceptance_observations').fetchone())
        forged_owner = {**IDENTITY, 'worker_id': 'other-worker'}
        changes = [dict(request_json=json.dumps(self.intent, indent=2)),
            dict(ack_json=json.dumps(first.body, indent=2)), dict(claim_sha256='f' * 64),
            dict(worker_id='other-worker', ack_json=p.canonical_json(make_ack(self.intent, forged_owner, first.body['observedAt'])).decode())]
        for changed in changes:
            with self.subTest(fields=sorted(changed)):
                saved = {**original, **changed}
                with self.queue._db() as db:
                    db.execute('''UPDATE reference_worker_acceptance_observations SET request_json=?,ack_json=?,
                        node_id=?,worker_id=?,instance_id=?,claim_sha256=?''', tuple(saved[field] for field in
                            ('request_json', 'ack_json', 'node_id', 'worker_id', 'instance_id', 'claim_sha256')))
                self.assertEqual(self.lookup_response().status, 409)

    def test_lookup_rejects_stale_ack_after_current_result_row_changes(self):
        self.assertEqual(self.post().status, 200)
        changed = copy.deepcopy(self.state['result'])
        changed['objects'][0]['sha256'] = 'f' * 64
        changed['resultManifestSha256'] = p.digest({key: value for key, value in changed.items() if key != 'resultManifestSha256'})
        with self.queue._db() as db:
            db.execute('UPDATE reference_jobs SET result_json=? WHERE job_id=?', (p.canonical_json(changed).decode(), self.job['jobId']))
        self.assertEqual(self.lookup_response().status, 409)

    def test_reopen_preserves_ack_and_original_claim_ownership(self):
        ack = self.post().body
        self.queue.close()
        from processing.prototype import Queue
        self.queue = Queue(self.temporary.name, reference_enabled=True)
        self.state['queue'] = self.queue
        self.store = ReferenceStore(self.queue)
        self.api = ReferenceWorkerAPI(self.store, DeliveryReceiver(self.store), bearer_workers={WORKER: IDENTITY})
        self.assertEqual(self.post().body, ack)
        self.assertEqual(self.store.worker_acceptance_observation(self.job['jobId']), ack)


@unittest.skipUnless(sys.platform.startswith('linux'), 'real worker gate and cleanup fixture')
class AcceptanceObservationDriverTests(unittest.TestCase):
    def setUp(self):
        from tests.test_reference_driver import ReferenceDriverTests, TOKEN, VIEWER
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests, IMAGE_ID, GCP_ID, ATTEMPT
        self.fixture = fixture = ReferenceDriverTests('runTest')
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        fixture.queue.close()
        source = ReferenceSourceIntakeTests('runTest'); source.setUp()
        source.payload['manifest']['objects'] = [source.payload['manifest']['objects'][0]]
        source.payload['manifest'].update(objectCount=1, totalBytes=3)
        source.rehash()
        spool_root = fixture.root / 'source-spool'; spool_root.mkdir(mode=0o700)
        (spool_root / '.ltds-source-spool-id').write_text('a' * 64)
        (spool_root / '.ltds-source-spool-id').chmod(0o600)
        mount = patch('processing.source_spool.os.path.ismount', return_value=True)
        mount.start(); self.addCleanup(mount.stop)
        from processing.source_spool import SourceSpool
        spool = SourceSpool(spool_root, mount_id='a' * 64, safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=3)
        fixture.queue, fixture.store, _, job, _ = source._make_reserved_source_job(fixture.root / 'queue', spool, 'observed-driver')
        self.signer = SyntheticViewerCopyV2Signer()
        fixture.worker.viewer_receipt_keys = self.signer.trusted_keys
        fixture.api = ReferenceControlAPI(fixture.store, viewer_bearers={VIEWER: 'staging'},
            worker_bearers={TOKEN: dict(node_id='cluster', worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint='a' * 64, options_fingerprint=source.payload['pipeline']['odm']['optionsFingerprint'],
            allowed_odm_options={'numeric-probe'}, source_spool=spool, source_attestation_keys=source.trusted_keys,
            authority_id=fixture.queue.authority_id, viewer_receipt_keys=self.signer.trusted_keys,
            source_bearer_principals={VIEWER:'viewer-staging'})
        fixture.contents.update({IMAGE_ID: b'abc', GCP_ID: b'id,lat,lon\n1,2,3\n', 'gcp_' + ATTEMPT: b'1,2,3\n'})
        fixture.transport.options = [dict(name='numeric-probe', type='float', value=1e-7, help='Numeric setting.')]
        self.claim = fixture.store.get(job['jobId'], private=True)
        data = dict(claim=self.claim, reservation=self.claim['transferRequest'])
        with fixture.driver._db() as db:
            db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'reservation_pending',?)",
                (job['jobId'], 'source-claim-observed-driver', p.canonical_json(data).decode()))
        self.job_id = job['jobId']
        for _ in range(50):
            outcome = fixture.driver.step()
            if self.driver_row()['stage'] == 'awaiting_acceptance':
                break
            self.assertNotEqual(outcome['state'], 'held', outcome)
        else:
            self.fail('actual source V2 driver did not reach accepted result wait')
        pulled=fixture.api.handle('GET',f'/ltds/v1/jobs/{self.job_id}/results',
            {'Authorization':'Bearer '+VIEWER},protected_transport=True)
        self.assertEqual(pulled.status,200,pulled.body)
        result=pulled.body
        from tests.test_reference_v2_store_acceptance import make_pull_acceptance
        acceptance=make_pull_acceptance(job,result)
        with self.assertRaisesRegex(Conflict,'Viewer pull acceptance path'):
            fixture.store.ensure_viewer_transfer(self.job_id)
        expected = dict(fixture.store.get(self.job_id, private=True)['sourceAcceptanceBinding'], resultManifestSha256=result['resultManifestSha256'])
        self.envelope = self.signer.envelope(expected, acceptance, result)
        fixture.store.accept(self.job_id, acceptance, viewer_receipt=self.envelope['viewerReceipt'], trusted_viewer_receipt_keys=self.signer.trusted_keys)
        from processing.reference_engine_cleanup import EngineCacheCleanup
        self.cleanup = Mock(spec=EngineCacheCleanup)
        self.cleanup.worker = fixture.worker
        fixture.driver.engine_cleanup = self.cleanup
        self.now = [time.time()]
        fixture.driver.clock = lambda: self.now[0]

    def driver_row(self):
        with self.fixture.driver._db() as db:
            row = dict(db.execute('SELECT * FROM driver_jobs WHERE job=?', (self.job_id,)).fetchone())
        row['data'] = json.loads(row['data'])
        return row

    def test_background_observation_lost_reply_restarts_after_generic_result_cleanup(self):
        fixture = self.fixture
        original = fixture.driver._request
        posted = []
        def lost(method, suffix, payload=None, **kwargs):
            response = original(method, suffix, payload, **kwargs)
            if suffix.endswith('/acceptance-observed'):
                posted.append(p.canonical_json(payload))
                raise DriverWait('synthetic lost observation acknowledgement')
            return response
        fixture.driver._request = lost
        fixture.driver.cleanup_executor = ScratchCleanupExecutor(fixture.ledger)
        result = fixture.driver._background()
        self.assertEqual(result['state'], 'acceptance_observation_pending')
        pending = self.driver_row()
        self.assertEqual(pending['stage'], 'awaiting_acceptance')
        self.assertEqual(len(posted), 1)
        self.cleanup.remove.assert_not_called()
        self.assertNotIn('acceptanceObservationAck', pending['data'])
        # Apply only the existing signed-copy/grace/pin cleanup authority.
        executor = ScratchCleanupExecutor(fixture.ledger)
        while True:
            candidates = fixture.ledger.cleanup_candidates(max_count=10, max_bytes=fixture.driver.output_budget)
            if not candidates: break
            for item in candidates:
                executor.execute(item['reservation'], item['id'], generation=item['generation'],
                    sha256=item['sha'], byte_size=item['bytes'], relative_path=item['path'])
        with fixture.ledger._transaction() as db:
            self.assertTrue(all(row[0] is not None for row in db.execute('SELECT cleaned FROM objects')))
        # A later source-custody hold cannot erase the already committed fact.
        # It grants no new release permission, and replay needs no source bytes.
        with fixture.queue._db() as db:
            db.execute("UPDATE reference_source_intakes SET source_state='held',derived_state='held' WHERE job_id=?", (self.job_id,))
        self.now[0] += 2
        fixture.driver = fixture.make_driver(engine_cleanup=self.cleanup, clock=lambda: self.now[0])
        replay = []
        def resumed(method, suffix, payload=None, **kwargs):
            if suffix.endswith('/acceptance-observed'):
                replay.append(p.canonical_json(payload))
            return original(method, suffix, payload, **kwargs)
        fixture.driver._request = resumed
        self.assertEqual(fixture.driver._background()['state'], 'completed')
        self.assertEqual(replay, posted)
        self.assertEqual(self.driver_row()['stage'], 'completed')
        self.cleanup.remove.assert_called_once()

    def test_background_404_and_mismatched_ack_preserve_intent_and_native_task(self):
        fixture = self.fixture
        original = fixture.driver._request
        mode = ['404']
        intents = []
        def unavailable(method, suffix, payload=None, **kwargs):
            if suffix.endswith('/acceptance-observed'):
                intents.append(p.canonical_json(payload))
                if mode[0] == '404': raise DriverHeld('control_request_rejected')
                status, ack = original(method, suffix, payload, **kwargs)
                ack = copy.deepcopy(ack); ack['requestSha256'] = 'b' * 64
                return status, ack
            return original(method, suffix, payload, **kwargs)
        fixture.driver._request = unavailable
        self.assertEqual(fixture.driver._background()['state'], 'acceptance_observation_pending')
        mode[0] = 'bad_ack'
        self.now[0] += 2
        self.assertEqual(fixture.driver._background()['state'], 'acceptance_observation_pending')
        self.assertEqual(intents[0], intents[1])
        self.assertEqual(self.driver_row()['stage'], 'awaiting_acceptance')
        self.cleanup.remove.assert_not_called()
        fixture.driver._request = original
        self.now[0] += 4
        self.assertEqual(fixture.driver._background()['state'], 'completed')

    def test_retry_backoff_is_durable_bounded_and_does_not_stall_next_job(self):
        fixture = self.fixture
        calls = []
        relay_calls = []
        original = fixture.driver._request
        def unavailable(method, suffix, payload=None, **kwargs):
            if suffix == '/jobs/' + self.job_id + '/acceptance':
                relay_calls.append(self.now[0])
            if suffix.endswith('/acceptance-observed'):
                calls.append((self.now[0], p.canonical_json(payload)))
                raise DriverWait('synthetic coordinator outage')
            return original(method, suffix, payload, **kwargs)
        fixture.driver._request = unavailable
        fixture.driver._background()
        first = self.driver_row()['data']
        self.assertEqual(first['acceptanceObservationAttempts'], 1)
        executor = ScratchCleanupExecutor(fixture.ledger)
        fixture.driver = fixture.make_driver(engine_cleanup=self.cleanup, cleanup_executor=executor, clock=lambda: self.now[0])
        fixture.driver._request = unavailable
        fixture.driver._background()
        self.assertEqual(len(calls), 1, 'restart preserves the deferred retry window')
        self.assertEqual(len(relay_calls), 1, 'backoff also suppresses this job\'s relay GET')
        with fixture.ledger._transaction() as db:
            self.assertGreater(db.execute('SELECT count(*) FROM objects WHERE cleaned IS NOT NULL').fetchone()[0], 0,
                'existing signed-copy cleanup continues while observation is deferred')
        self.now[0] += 2
        fixture.driver._background()
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(relay_calls), 2)
        self.now[0] += 3
        fixture.driver._background()
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(relay_calls), 2)
        self.now[0] += 1
        fixture.driver._background()
        self.assertEqual(len(calls), 3)
        self.assertEqual([item[0] - calls[0][0] for item in calls], [0, 2, 6])
        for _ in range(35):
            self.now[0] += 301
            fixture.driver._background()
            state = self.driver_row()['data']
            self.assertLessEqual(state['acceptanceObservationNextAttemptAt'] - self.now[0], 300)
        self.assertEqual(self.driver_row()['data']['acceptanceObservationAttempts'], 32)
        self.assertEqual({item[1] for item in calls}, {calls[0][1]})
        next_job = fixture.submit(options={'numeric-probe': 1e-7})
        self.assertEqual(fixture.driver.step()['state'], 'claim_pending')
        with fixture.driver._db() as db:
            self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?', (next_job['jobId'],)).fetchone()[0], 'claim_pending')
        self.cleanup.remove.assert_not_called()

    def test_foreground_acceptance_branch_requires_matching_ack_before_cleanup(self):
        fixture = self.fixture
        original_db, original_request = fixture.driver._db, fixture.driver._request
        @contextmanager
        def foreground_row():
            with original_db() as db:
                class ForegroundSelection:
                    def execute(inner, sql, parameters=()):
                        if sql.startswith("SELECT * FROM driver_jobs WHERE stage NOT IN"):
                            return db.execute('SELECT * FROM driver_jobs WHERE job=?', (self.job_id,))
                        return db.execute(sql, parameters)
                yield ForegroundSelection()
        def unavailable(method, suffix, payload=None, **kwargs):
            if suffix.endswith('/acceptance-observed'):
                raise DriverWait('synthetic lost connection')
            return original_request(method, suffix, payload, **kwargs)
        fixture.driver._request = unavailable
        with patch.object(fixture.driver, '_db', side_effect=foreground_row):
            with self.assertRaises(DriverWait): fixture.driver._step()
        pending = self.driver_row()['data']['acceptanceObservationRequest']
        self.cleanup.remove.assert_not_called()
        contact = Mock(side_effect=original_request)
        fixture.driver._request = contact
        with patch.object(fixture.driver, '_db', side_effect=foreground_row):
            self.assertEqual(fixture.driver._step()['state'], 'awaiting_acceptance')
        contact.assert_not_called()
        fixture.driver._request = original_request
        self.now[0] += 2
        with patch.object(fixture.driver, '_db', side_effect=foreground_row):
            self.assertEqual(fixture.driver._step()['state'], 'completed')
        self.assertEqual(self.driver_row()['data']['acceptanceObservationRequest'], pending)
        self.cleanup.remove.assert_called_once()

    def test_actual_tls_lost_observation_reply_and_viewer_lookup_replay(self):
        from tests.test_reference_dynamic_source_intake_scale import _write_tls_material
        from tests.test_reference_driver import TOKEN, VIEWER
        from processing.reference_driver import ReferenceDriver
        from processing.reference_http import create_reference_http_server
        from urllib.request import Request, urlopen
        fixture = self.fixture
        key, certificate = _write_tls_material(fixture.root)
        server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        server_tls.load_cert_chain(certificate, key)
        client_tls = ssl.create_default_context(cafile=str(certificate))
        server = create_reference_http_server(fixture.api, ssl_context=server_tls, request_timeout=3)
        server.daemon_threads, server.block_on_close = False, True
        drop = [0]
        handler = server.RequestHandlerClass
        original_reply = handler._reply
        def lose_first_reply(connection, response, **kwargs):
            if connection.path.endswith('/acceptance-observed') and response.status == 200 and not drop[0]:
                drop[0] += 1
                connection.close_connection = True
                connection.connection.shutdown(2)
                connection.connection.close()
                return
            return original_reply(connection, response, **kwargs)
        handler._reply = lose_first_reply
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)
        thread.start()
        try:
            origin = 'https://127.0.0.1:' + str(server.server_address[1])
            old = self.driver_row()
            fixture.driver = ReferenceDriver(fixture.worker, fixture.ledger, fixture.outbox, fixture.broker,
                origin, TOKEN, state_path=fixture.root / 'actual-tls-driver.sqlite', ssl_context=client_tls,
                output_budget=1024 * 1024, transfer_ttl=240, safety_floor=1024, failure_outbox=fixture.failures,
                worker_instance_id='instance', engine_cleanup=self.cleanup, clock=lambda: self.now[0])
            with fixture.driver._db() as db:
                db.execute("INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,'awaiting_acceptance',?)",
                    (self.job_id, old['claim_request'], p.canonical_json(old['data']).decode()))
            self.assertEqual(fixture.driver._background()['state'], 'acceptance_observation_pending')
            self.assertEqual(drop[0], 1)
            pending = self.driver_row()['data']['acceptanceObservationRequest']
            self.cleanup.remove.assert_not_called()
            ack_before_replay = fixture.store.worker_acceptance_observation(self.job_id)
            self.now[0] += 2
            self.assertEqual(fixture.driver._background()['state'], 'completed')
            completed = self.driver_row()['data']
            self.assertEqual(completed['acceptanceObservationRequest'], pending)
            self.assertEqual(completed['acceptanceObservationAck'], ack_before_replay)
            lookup = Request(origin + '/ltds/v2/jobs/' + self.job_id + '/source/acceptance-observation',
                headers={'Authorization': 'Bearer ' + VIEWER})
            with urlopen(lookup, context=client_tls, timeout=3) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()), ack_before_replay)
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=3)
            self.assertFalse(thread.is_alive(), 'owned observation TLS server did not stop')


if __name__ == '__main__':
    unittest.main()
