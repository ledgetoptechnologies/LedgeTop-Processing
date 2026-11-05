"""A coordinator restart during a v2 fetch resumes the original claim, or holds.

Real coordinator, ClaimRecoveryService, HTTP routing, SourceRangeClient and
driver methods. Time is virtual and the source data is the synthetic fixture, so
a crash here is a process object going away, not ``kill -9`` or a lost fsync.
"""
import copy
import hashlib
import json
import os
import threading
import time
from unittest.mock import patch
import uuid

from processing import reference_protocol as protocol
from processing.coordinator_freeze import CoordinatorFrozen, STARTUP_TABLES
from processing.maintenance import Conflict
from processing.reference_driver import DriverHeld, DriverWait, SOURCE_RECOVERY_ATTEMPTS
from processing.reference_recovery import (SOURCE_RECEIPT_PROTOCOL, SOURCE_STATE_PROTOCOL,
    SOURCE_TRANSFER_MAX_BYTES, ClaimRecoveryService)
from processing.scratch_policy import ScratchLedger
from processing.source_range_transfer import RENEWAL_ACK_PATH, RENEWAL_PATH, validate_source_grant
from tests.test_reference_source_grant_renewal import RenewalHarness, WORKER_TOKEN, _ControlOpener, stamp

RECORD_TABLES = ('jobs', 'reference_jobs', 'reference_claims', 'reference_transfer_grants',
    'reference_source_grant_renewals', 'reference_source_grant_tokens', 'reference_source_range_usage',
    'reference_source_range_requests', 'reference_source_object_receipts', 'reference_source_object_pins',
    'reference_recovery_receipts', 'nodes')


class RecoveryHarness(RenewalHarness):
    def rows(self):
        with self.queue._db() as db:
            present = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            return {table: [tuple(row) for row in db.execute(f'SELECT * FROM {table} ORDER BY 1,2')]
                    for table in RECORD_TABLES if table in present}

    def status(self):
        with self.queue._db() as db:
            return tuple(db.execute('SELECT status,held_reason FROM jobs WHERE id=?', (self.job_id,)).fetchone())

    def receipts(self):
        with self.queue._db() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_recovery_receipts'").fetchone():
                return []
            return [json.loads(row[0]) for row in db.execute('SELECT receipt_json FROM reference_recovery_receipts')]

    def identity(self):
        return {key: self.worker[key] for key in ('node_id', 'worker_id', 'worker_instance_id')}

    def service(self):
        return self.control.worker.recovery

    def state(self, **identity):
        return self.service().state(self.job_id, identity={**self.identity(), **identity}, claim_token=self.claim_token)

    def body(self, **changes):
        state = self.state()
        claim = state['claim']
        binding = {key: claim[key] for key in ('jobId', 'generation', 'environment', 'nodeId', 'workerId',
            'workerInstanceId', 'authorityId', 'claimRequestId', 'claimTokenSha256', 'manifestSha256', 'pipelineDigest')}
        binding.update(reservationId=state['reservation']['reservationId'], inputBytes=state['reservation']['inputBytes'])
        source = state['sourceTransfer'] or {}
        value = dict(schemaVersion=2, requestId=str(uuid.uuid4()), coordinatorEpoch=state['coordinatorEpoch'],
            expectedNodeRevision=state['node']['revision'], claimBinding=binding,
            transferRequestSha256=state['reservation']['transferRequestSha256'],
            sourceClosureSha256=source.get('closureSha256', 'a' * 64), grantSha256=source.get('grantSha256', 'a' * 64),
            renewalSequence=source.get('renewalSequence', 0), committedRanges=0, committedBytes=0,
            workerEvidenceSha256='e' * 64)
        value.update(changes)
        return value

    def resume(self, body, *, claim_token=None, **identity):
        return self.service().resume(self.job_id, body, identity={**self.identity(), **identity},
            claim_token=self.claim_token if claim_token is None else claim_token)

    def attempts(self):
        return (self.data.get('sourceRecovery') or {}).get('attempts', 0)


class RetainedCustodyTests(RecoveryHarness):
    """The closure covers every retained object, including inputs a grant excludes."""

    def setUp(self):
        super().setUp()
        self.restart_coordinator()
        self.raw = next(item['id'] for item in json.loads(self.intake('source_manifest_json'))['objects']
            if item['id'] not in set(self.first_grant['objectIds']))
        self.held_body = self.body()

    def intake(self, column):
        with self.queue._db() as db:
            return db.execute(f'SELECT {column} FROM reference_source_intakes WHERE job_id=?', (self.job_id,)).fetchone()[0]

    def receipt(self):
        with self.queue._db() as db:
            return db.execute('SELECT receipt_json FROM reference_source_object_receipts WHERE object_id=?', (self.raw,)).fetchone()[0]

    def assert_closed(self, statement, *values):
        self.execute(statement, *values)
        state = self.state()
        self.assertEqual((state['eligible'], state['sourceTransfer']), (False, None), statement)
        before = self.rows()
        with self.assertRaises(Conflict):
            self.resume(self.held_body)
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def receipt_column(self, assignment, *values):
        self.assert_closed(f'UPDATE reference_source_object_receipts SET {assignment} WHERE object_id=?', *values, self.raw)

    def test_control_the_raw_input_is_outside_the_grant_and_has_no_pin(self):
        with self.queue._db() as db:
            pins = {row[0] for row in db.execute('SELECT object_id FROM reference_source_object_pins WHERE job_id=?', (self.job_id,))}
        self.assertEqual(pins, set(self.first_grant['objectIds']))
        self.assertNotIn(self.raw, pins)
        self.assertEqual(self.resume(self.held_body)['resumedStatus'], 'running')

    def test_excluded_receipt_size(self):
        self.receipt_column('byte_size=byte_size+1')

    def test_excluded_receipt_hash(self):
        self.receipt_column('sha256=?', 'f' * 64)

    def test_excluded_receipt_storage_key(self):
        self.receipt_column('storage_key=?', 'moved/elsewhere.blob')

    def test_excluded_receipt_generation(self):
        self.receipt_column('generation=generation+1')

    def test_excluded_receipt_kind(self):
        self.receipt_column("object_kind='derived'")

    def test_excluded_receipt_mount(self):
        self.receipt_column('mount_id=?', 'b' * 64)

    def test_excluded_receipt_body_size(self):
        self.receipt_column('receipt_json=?', json.dumps(dict(json.loads(self.receipt()), byteSize=1)))

    def test_excluded_receipt_body_hash(self):
        self.receipt_column('receipt_json=?', json.dumps(dict(json.loads(self.receipt()), sha256='f' * 64)))

    def test_excluded_receipt_body_storage_key(self):
        self.receipt_column('receipt_json=?', json.dumps(dict(json.loads(self.receipt()), storageKey='moved/elsewhere.blob')))

    def test_excluded_receipt_body_extra_field(self):
        self.receipt_column('receipt_json=?', json.dumps(dict(json.loads(self.receipt()), extra=True)))

    def changed_manifest(self, column):
        manifest = json.loads(self.intake(column))
        manifest['objects'][0]['byteSize'] += 1
        return json.dumps(manifest)

    def test_source_manifest_content_with_the_accepted_sha_unchanged(self):
        self.assert_closed('UPDATE reference_source_intakes SET source_manifest_json=? WHERE job_id=?',
            self.changed_manifest('source_manifest_json'), self.job_id)

    def test_derived_manifest_content_with_the_accepted_sha_unchanged(self):
        self.assert_closed('UPDATE reference_source_intakes SET derived_manifest_json=? WHERE job_id=?',
            self.changed_manifest('derived_manifest_json'), self.job_id)

    def test_manifest_and_its_stored_sha_rewritten_together_still_fail_the_signed_proof(self):
        manifest = json.loads(self.changed_manifest('source_manifest_json'))
        manifest['sha256'] = protocol.manifest_sha256(manifest)
        self.assert_closed('UPDATE reference_source_intakes SET source_manifest_json=?,source_manifest_sha=? WHERE job_id=?',
            json.dumps(manifest), manifest['sha256'], self.job_id)


class FetchRecoveryPathTests(RecoveryHarness):
    def test_resume_keeps_the_original_claim_and_starts_nothing_new(self):
        before = self.rows()
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        after = self.rows()
        self.assertEqual(self.status(), ('running', None))
        for table in ('reference_jobs', 'reference_claims', 'reference_transfer_grants', 'reference_source_object_pins',
                      'reference_source_object_receipts'):
            self.assertEqual(after[table], before[table], table)
        self.assertEqual(self.store.get(self.job_id)['phase'], 'fetching')
        receipt, = self.receipts()
        self.assertEqual((receipt['schemaVersion'], receipt['protocol'], receipt['executionPhase'], receipt['resumedStatus']),
            (2, SOURCE_RECEIPT_PROTOCOL, 'fetching', 'running'))
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.assertEqual(self.fetch(first, 0), b'abc')

    def test_restart_after_a_partial_transfer_resumes_the_same_budget(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.assertEqual(self.fetch(first, 0), b'abc')
        spent = self.usage()
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.assertEqual(self.usage(), spent)
        self.assertEqual(self.receipts()[0]['committedBytes'], 3)
        self.assertEqual(self.fetch(first, 1), b'1,2,3\n')

    def test_a_held_job_serves_no_range_until_it_resumes(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.restart_coordinator()
        with self.assertRaises(Conflict):
            self.read_range(first.token, first)
        self.assertIsNone(self.admit())
        self.read_range(first.token, first)

    def test_request_pending_renewal_with_a_live_grant(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.assertEqual(self.fetch(self.renew(), 0), b'abc')
        self.assertEqual(self.data['sourceRenewalSequence'], 1)

    def test_request_pending_renewal_whose_successor_expired_in_the_outage(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.restart_coordinator()
        self.now[0] += 3 * 24 * 3600
        self.assertIsNone(self.admit())
        self.assertEqual(self.fetch(self.renew(), 0), b'abc')
        self.assertEqual(self.data['sourceRenewalSequence'], 2)

    def test_ack_pending_whose_ack_committed_before_the_restart(self):
        self.after_lease()
        self.opener(_ControlOpener(self.control, drop=RENEWAL_ACK_PATH))
        with self.assertRaises(DriverWait):
            self.renew()
        self.assertEqual(self.data['sourceRenewal']['phase'], 'ack_pending')
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.assertEqual(self.receipts()[0]['renewalSequence'], 0)
        self.assertEqual(self.fetch(self.renew(), 1), b'1,2,3\n')

    def test_coordinator_dies_before_recovery_commits(self):
        self.restart_coordinator()
        self.down.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        request = self.data['sourceRecovery']['pending']['request']
        self.assertEqual((self.attempts(), self.status()), (1, ('held', 'coordinator_restart')))
        self.down.clear()
        self.assertIsNone(self.admit())
        self.assertEqual(self.receipts()[0]['requestId'], request['requestId'])
        self.assertEqual(self.attempts(), 0)
        self.assertIsNone(self.data['sourceRecovery']['pending'])

    def test_recovery_reply_lost_after_commit_replays_one_receipt(self):
        self.restart_coordinator()
        self.lose_reply.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        self.assertEqual(self.status(), ('running', None))
        self.crash_worker()
        self.assertIsNotNone(self.data['sourceRecovery']['pending'])
        self.assertIsNone(self.admit())
        self.assertEqual((len(self.receipts()), self.attempts()), (1, 0))
        self.assertIsNone(self.data['sourceRecovery']['pending'])

    def test_second_restart_after_a_lost_reply_retires_the_old_intent(self):
        self.restart_coordinator()
        self.lose_reply.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        old = self.data['sourceRecovery']['pending']['request']['requestId']
        self.restart_coordinator()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.assertIsNone(self.admit())
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual((len(self.receipts()), self.attempts()), (2, 0))
        self.assertNotEqual(self.receipts()[-1]['requestId'], old)

    def test_an_old_receipt_cannot_clear_a_later_restart(self):
        self.restart_coordinator()
        self.lose_reply.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        request = self.data['sourceRecovery']['pending']['request']
        self.restart_coordinator()
        self.assertEqual(self.resume(request)['requestId'], request['requestId'])
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_second_restart_after_a_saved_receipt_needs_a_new_attempt(self):
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.restart_coordinator()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.assertIsNone(self.admit())
        self.assertEqual((len(self.receipts()), self.attempts()), (2, 0))

    def test_range_served_but_never_committed_is_replayed_not_recharged(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.read_range(first.token, first)
        spent = self.usage()
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.assertEqual(self.receipts()[0]['committedRanges'], 0)
        self.assertEqual(self.fetch(first, 0), b'abc')
        self.assertEqual(self.usage()[:2], spent[:2])

    def test_worker_death_after_saving_the_intent_reuses_it(self):
        self.restart_coordinator()
        self.down.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        request = self.saved[-1]['sourceRecovery']['pending']['request']
        self.crash_worker()
        self.down.clear()
        self.assertIsNone(self.admit())
        self.assertEqual(self.receipts()[0]['requestId'], request['requestId'])
        self.assertEqual(self.attempts(), 0)

    def test_intent_is_durable_before_the_post(self):
        self.restart_coordinator()
        order = []
        real = self.driver._save
        self.driver._save = lambda *args: (order.append('save'), real(*args))[1]
        request = self.driver._request
        self.driver._request = lambda method, suffix, *args, **kwargs: (
            order.append('post') if method == 'POST' else None, request(method, suffix, *args, **kwargs))[1]
        self.assertIsNone(self.admit())
        self.assertEqual(order[:2], ['save', 'post'])

    def test_recovery_keeps_a_processing_pending_stage(self):
        self.restart_coordinator()
        self.driver._recover_source_fetch(self.job_id, 'processing_pending', self.data)
        self.assertEqual(set(self.saved_stages), {'processing_pending'})
        self.assertEqual(self.status(), ('running', None))

    def test_receipt_that_differs_is_not_believed(self):
        self.restart_coordinator()
        real = self.driver._request

        def altered(method, suffix, *args, **kwargs):
            status, body = real(method, suffix, *args, **kwargs)
            return (status, dict(body, grantSha256='0' * 64)) if method == 'POST' else (status, body)
        self.driver._request = altered
        with self.assertRaisesRegex(DriverHeld, 'source_recovery_receipt_changed'):
            self.admit()
        self.assertIsNotNone(self.data['sourceRecovery']['pending'])


class RecoveryAttemptTests(RecoveryHarness):
    def test_exact_retries_spend_no_attempt_and_no_source_budget(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.fetch(first, 0)
        spent = self.usage()
        self.restart_coordinator()
        self.down.add('recover')
        for _ in range(SOURCE_RECOVERY_ATTEMPTS * 3):
            with self.assertRaises(DriverWait):
                self.admit()
        self.assertEqual(self.attempts(), 1)
        self.assertEqual(self.usage(), spent)
        self.assertEqual(len({str(self.data['sourceRecovery']['pending'])}), 1)
        self.down.clear()
        self.assertIsNone(self.admit())
        self.assertEqual(self.attempts(), 0)

    def test_state_outage_spends_no_attempt(self):
        self.restart_coordinator()
        self.down.add('recovery-state')
        for _ in range(3):
            with self.assertRaises(DriverWait):
                self.admit()
        self.assertEqual(self.attempts(), 0)

    def test_eight_unresolved_attempts_then_a_hold_that_keeps_everything(self):
        self.down.add('recover')
        for attempt in range(1, SOURCE_RECOVERY_ATTEMPTS + 1):
            self.restart_coordinator()
            with self.assertRaises(DriverWait):
                self.admit()
            self.assertEqual(self.attempts(), attempt)
        self.restart_coordinator()
        before = self.rows()
        sent = len(self.requests)
        for _ in range(2):
            with self.assertRaisesRegex(DriverHeld, 'source_recovery_attempts_exhausted'):
                self.admit()
        self.assertEqual(self.rows(), before)
        self.assertNotIn(('POST', 'recover'), self.requests[sent:])
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.assertEqual(self.attempts(), SOURCE_RECOVERY_ATTEMPTS)
        self.assertEqual(self.receipts(), [])

    def test_a_refusal_without_a_receipt_clears_no_attempt(self):
        self.restart_coordinator()
        self.lose_reply.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        self.assertEqual((self.attempts(), self.status()), (1, ('running', None)))
        real = self.driver._request
        self.driver._request = lambda method, suffix, *args, **kwargs: (
            (409, None) if method == 'POST' else real(method, suffix, *args, **kwargs))
        self.assertIsNone(self.admit())
        self.assertEqual((self.attempts(), self.data['sourceRecovery']['pending']), (1, None))

    def test_a_receipt_that_differs_clears_no_attempt(self):
        self.restart_coordinator()
        self.down.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        self.down.clear()
        real = self.driver._request

        def changed(method, suffix, *args, **kwargs):
            status, body = real(method, suffix, *args, **kwargs)
            return (status, dict(body, grantSha256='0' * 64)) if method == 'POST' else (status, body)
        self.driver._request = changed
        with self.assertRaisesRegex(DriverHeld, 'source_recovery_receipt_changed'):
            self.admit()
        self.assertEqual(self.attempts(), 1)
        self.assertIsNotNone(self.data['sourceRecovery']['pending'])

    def test_full_metadata_quota_refuses_the_receipt_and_deletes_no_history(self):
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        kept = self.receipts()
        self.restart_coordinator()

        def full(_db, _extra=0):
            raise Conflict('reference metadata quota reached')
        with patch.object(self.store, '_quota', side_effect=full):
            for _ in range(3):
                with self.assertRaisesRegex(DriverWait, 'source_recovery_refused'):
                    self.admit()
        self.assertEqual(self.receipts(), kept)
        self.assertEqual((self.attempts(), self.status()), (1, ('held', 'coordinator_restart')))
        self.assertIsNone(self.admit())
        self.assertEqual((len(self.receipts()), self.attempts()), (2, 0))

    def test_tampered_local_journal_holds(self):
        self.restart_coordinator()
        self.data['sourceRecovery'] = {'attempts': SOURCE_RECOVERY_ATTEMPTS + 1, 'pending': None}
        with self.assertRaisesRegex(DriverHeld, 'source_recovery_journal_invalid'):
            self.admit()


class RecoveryRefusalTests(RecoveryHarness):
    """Every refusal leaves all recorded rows byte-identical and the job held."""

    def setUp(self):
        super().setUp()
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        self.fetch(first, 0)
        self.restart_coordinator()

    def assert_refused(self, body=None, **kwargs):
        body = body or self.body()
        before = self.rows()
        with self.assertRaises((Conflict, CoordinatorFrozen, ValueError)):
            self.resume(body, **kwargs)
        self.assertEqual(self.rows(), before)

    def assert_ineligible(self, change, *values):
        self.execute(change, *values)
        self.assertFalse(self.state()['eligible'], change)
        self.assert_refused()

    def test_control_the_unchanged_request_is_accepted(self):
        self.assertEqual(self.resume(self.body())['resumedStatus'], 'running')

    def test_another_instance_worker_or_claim(self):
        body = self.body()
        for change in ({'worker_instance_id': 'replacement'}, {'worker_id': 'other-worker'}):
            self.assert_refused(body, **change)
        self.assert_refused(body, claim_token='not-the-claim')

    def test_binding_of_another_claim_generation_or_environment(self):
        for key, value in (('generation', 2), ('environment', 'production'), ('workerInstanceId', 'replacement'),
                           ('claimTokenSha256', 'f' * 64), ('reservationId', 'other-reservation'), ('inputBytes', 1),
                           ('manifestSha256', 'f' * 64), ('claimRequestId', 'other-claim')):
            body = self.body()
            body['claimBinding'][key] = value
            self.assert_refused(body)

    def test_stale_epoch_revision_transfer_or_closure(self):
        for change in ({'coordinatorEpoch': str(uuid.uuid4())}, {'expectedNodeRevision': 99},
                       {'transferRequestSha256': 'f' * 64}, {'sourceClosureSha256': 'f' * 64}):
            self.assert_refused(self.body(**change))

    def test_worker_ahead_of_the_coordinator(self):
        admitted = self.state()['sourceTransfer']
        for change in ({'committedRanges': admitted['requestsAdmitted'] + 1},
                       {'committedBytes': admitted['bytesAdmitted'] + 1},
                       {'renewalSequence': 1}, {'grantSha256': 'f' * 64}):
            self.assert_refused(self.body(**change))

    def test_v1_shaped_request_for_a_v2_transfer(self):
        body = self.body(schemaVersion=1)
        with self.assertRaises(ValueError):
            self.resume(body)

    def test_operator_hold(self):
        self.assert_ineligible("UPDATE jobs SET held_reason='operator_hold' WHERE id=?", self.job_id)

    def test_terminal_or_running_job(self):
        for status, reason in (('failed', None), ('cancelled', None), ('running', None)):
            self.execute('UPDATE jobs SET status=?,held_reason=? WHERE id=?', status, reason, self.job_id)
            self.assertFalse(self.state()['eligible'], status)
            self.assert_refused()

    def test_job_that_left_fetching_with_ranges_unserved(self):
        # Schema 3 covers processing only once every range was served.
        self.assert_ineligible("UPDATE reference_jobs SET phase='processing' WHERE job_id=?", self.job_id)
        self.assertEqual(self.state()['schemaVersion'], 3)
        self.assertIsNone(self.state()['sourceTransfer'])

    def test_job_past_processing(self):
        self.assert_ineligible("UPDATE reference_jobs SET phase='waiting_for_delivery' WHERE job_id=?", self.job_id)
        self.assertEqual(self.state()['schemaVersion'], 2)

    def test_job_error_recorded(self):
        self.assert_ineligible("UPDATE reference_jobs SET error='synthetic' WHERE job_id=?", self.job_id)

    def test_source_custody_not_accepted(self):
        body = self.body()
        self.execute("UPDATE reference_source_intakes SET source_state='receiving' WHERE job_id=?", self.job_id)
        with self.assertRaises(Conflict):
            self.state()
        self.assert_refused(body)

    def test_release_fence(self):
        self.store.source_readers().fence(self.job_id, self.first_grant['generation'], 'recovery-release')
        self.assertFalse(self.state()['eligible'])
        self.assert_refused()

    def test_recorded_release(self):
        from processing.source_release import ensure_schema
        with self.queue._db() as db:
            ensure_schema(db)
            db.execute("""INSERT INTO source_releases VALUES(?,1,'release-1','viewer','{}','a','b','accepted',NULL,
                't',0,'e',0,'{}',NULL,0,'c',NULL)""", (self.job_id,))
        self.assertFalse(self.state()['eligible'])
        self.assert_refused()

    def test_freeze(self):
        self.freeze('intent')
        self.assertFalse(self.state()['eligible'])
        self.assert_refused()
        self.freeze('abort', healthy=True)
        self.assertTrue(self.state()['eligible'])

    def test_tampered_transfer_wire(self):
        with self.queue._db() as db:
            wire = db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?', (self.job_id,)).fetchone()[0]
        self.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',
            wire.replace('"generation":1', '"generation":2'), self.job_id)
        with self.assertRaises(Conflict):
            self.state()

    def test_tampered_grant(self):
        self.assert_ineligible('UPDATE reference_transfer_grants SET digest=? WHERE job_id=?', 'f' * 64, self.job_id)

    def test_missing_grant_token(self):
        self.assert_ineligible('DELETE FROM reference_source_grant_tokens WHERE job_id=?', self.job_id)

    def test_range_ledger_that_disagrees(self):
        self.assert_ineligible('UPDATE reference_source_range_usage SET bytes_used=bytes_used+1 WHERE job_id=?', self.job_id)

    def test_budget_overspent(self):
        self.execute('UPDATE reference_source_range_requests SET attempts_used=9999 WHERE job_id=?', self.job_id)
        self.assert_ineligible('UPDATE reference_source_range_usage SET attempts_used=9999 WHERE job_id=?', self.job_id)

    def test_missing_object_receipt(self):
        self.assert_ineligible('DELETE FROM reference_source_object_receipts WHERE job_id=? AND object_id=?',
            self.job_id, self.first_grant['objectIds'][0])

    def test_missing_pin(self):
        self.assert_ineligible('DELETE FROM reference_source_object_pins WHERE job_id=? AND object_id=?',
            self.job_id, self.first_grant['objectIds'][0])

    def test_custody_row_for_an_object_outside_the_closure(self):
        for table in ('reference_source_object_receipts', 'reference_source_object_pins'):
            with self.queue._db() as db:
                row = dict(db.execute(f'SELECT * FROM {table} WHERE job_id=? LIMIT 1', (self.job_id,)).fetchone())
                row['object_id'] = 'foreign-object'
                db.execute(f"INSERT INTO {table} VALUES({','.join('?' * len(row))})", tuple(row.values()))
            self.assertFalse(self.state()['eligible'], table)
            self.assert_refused()
            self.execute(f"DELETE FROM {table} WHERE job_id=? AND object_id='foreign-object'", self.job_id)
        self.assertTrue(self.state()['eligible'])

    def test_changed_pin_identity_binding(self):
        self.assert_ineligible('UPDATE reference_source_object_pins SET sha256=? WHERE job_id=? AND object_id=?',
            'f' * 64, self.job_id, self.first_grant['objectIds'][0])

    def test_changed_retention_receipt(self):
        self.assert_ineligible('UPDATE reference_source_intakes SET source_receipt_sha=? WHERE job_id=?', 'f' * 64, self.job_id)

    def test_node_slot_taken_by_another_job(self):
        self.execute("""INSERT INTO jobs(id,request_id,environment,profile,status,node_id)
            SELECT 'another-job','another-request',environment,profile,'running',node_id FROM jobs WHERE id=?""", self.job_id)
        self.assert_refused()

    def test_quota_refusal_keeps_the_hold(self):
        body = self.body()
        with self.queue._db() as db:
            self.store.max_metadata_bytes = self.store._metadata_used_bytes(db)
        self.assert_refused(body)


class RecoveryChainTests(RecoveryHarness):
    def renewed_twice(self):
        self.after_lease()
        renewed = self.renew()
        self.now[0] = renewed.expires + 1
        self.renew()
        self.restart_coordinator()

    def test_control_a_two_step_chain_resumes(self):
        self.renewed_twice()
        self.assertIsNone(self.admit())
        self.assertEqual(self.receipts()[0]['renewalSequence'], 2)

    def test_broken_hash_link(self):
        self.renewed_twice()
        self.execute('UPDATE reference_source_grant_renewals SET previous_grant_sha256=? WHERE job_id=? AND sequence=2',
            'f' * 64, self.job_id)
        self.assertFalse(self.state()['eligible'])

    def test_unacknowledged_row_before_the_last(self):
        self.renewed_twice()
        self.execute('UPDATE reference_source_grant_renewals SET acked=0 WHERE job_id=? AND sequence=1', self.job_id)
        self.assertFalse(self.state()['eligible'])

    def test_sequence_gap(self):
        self.renewed_twice()
        self.execute('DELETE FROM reference_source_grant_renewals WHERE job_id=? AND sequence=1', self.job_id)
        self.assertFalse(self.state()['eligible'])

    def test_rolled_back_coordinator_stays_held(self):
        self.renewed_twice()
        self.execute('DELETE FROM reference_source_grant_renewals WHERE job_id=? AND sequence=2', self.job_id)
        self.assertFalse(self.state()['eligible'])
        with self.assertRaisesRegex(DriverHeld, 'source_recovery_ineligible'):
            self.admit()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))

    def test_changed_newest_successor_body(self):
        self.renewed_twice()
        with self.queue._db() as db:
            body = db.execute('SELECT successor_grant_json FROM reference_source_grant_renewals WHERE job_id=? AND sequence=2',
                (self.job_id,)).fetchone()[0]
        self.execute('UPDATE reference_source_grant_renewals SET successor_grant_json=? WHERE job_id=? AND sequence=2',
            body.replace('"token":"', '"token":"x'), self.job_id)
        self.assertFalse(self.state()['eligible'])


class RecoveryMaintenanceTests(RecoveryHarness):
    def schedule(self, state):
        self.execute('UPDATE nodes SET state=?,revision=revision+1 WHERE id=?', state, 'cluster')

    def test_draining_node_resumes_its_existing_claim(self):
        self.restart_coordinator()
        self.schedule('draining')
        self.assertIsNone(self.admit())
        self.assertEqual(self.status(), ('running', None))

    def test_drain_still_admits_nothing_new(self):
        self.restart_coordinator()
        self.assertIsNone(self.admit())
        self.schedule('draining')
        self.assertIsNone(self.store.next_for_node('cluster'))

    def test_held_updating_or_failed_node_stays_held(self):
        self.restart_coordinator()
        for state in ('updating', 'held', 'failed'):
            self.schedule(state)
            self.assertFalse(self.state()['eligible'], state)
            with self.assertRaisesRegex(DriverHeld, 'source_recovery_ineligible'):
                self.admit()
            self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.assertEqual(self.attempts(), 0)

    def test_node_update_waits_for_the_job_before_and_after_recovery(self):
        self.restart_coordinator()
        now = self.queue._now().isoformat().replace('+00:00', 'Z')
        node = self.queue.schedule_node('cluster', dict(expectedRevision=self.queue.get_node('cluster')['revision'],
            drainAt=now, notBefore=now, afterJobId=None, targetImage='sha256:' + 'a' * 64))
        update = dict(expectedRevision=node['revision'], planId=node['planId'])
        with self.assertRaisesRegex(Conflict, 'running or held'):
            self.queue.begin_node_update('cluster', update)
        self.assertIsNone(self.admit())
        update['expectedRevision'] = self.queue.get_node('cluster')['revision']
        with self.assertRaisesRegex(Conflict, 'running or held'):
            self.queue.begin_node_update('cluster', update)

    def test_failed_node_cannot_be_resumed_while_the_job_is_held(self):
        self.restart_coordinator()
        self.schedule('failed')
        node = self.queue.get_node('cluster')
        with self.assertRaisesRegex(Conflict, 'running or held'):
            self.queue.resume_node('cluster', dict(expectedRevision=node['revision'], planId=node['planId'],
                note='synthetic', workerStopped=True, healthy=True))

    def test_node_revision_change_retires_the_pending_intent(self):
        self.restart_coordinator()
        self.down.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        old = self.data['sourceRecovery']['pending']['request']['requestId']
        self.down.clear()
        self.schedule('active')
        self.assertIsNone(self.admit())
        self.assertNotEqual(self.receipts()[0]['requestId'], old)
        self.assertEqual(self.attempts(), 0)

    def test_changed_closure_retires_the_pending_intent(self):
        self.restart_coordinator()
        self.down.add('recover')
        with self.assertRaises(DriverWait):
            self.admit()
        old = self.data['sourceRecovery']['pending']['request']['requestId']
        self.down.clear()
        self.execute('UPDATE reference_source_range_usage SET attempts_used=attempts_used WHERE job_id=?', self.job_id)
        real = self.store._source_fetch_closure

        def moved(db, job_id):
            return dict(real(db, job_id), closureSha256='c' * 64)
        with patch.object(self.store, '_source_fetch_closure', side_effect=moved):
            self.assertIsNone(self.admit())
        self.assertNotEqual(self.receipts()[0]['requestId'], old)
        self.assertEqual(self.attempts(), 0)

    def test_refused_post_keeps_the_exact_intent_and_its_attempt(self):
        self.restart_coordinator()
        real = self.driver._request
        self.driver._request = lambda method, suffix, *args, **kwargs: (
            (409, None) if method == 'POST' else real(method, suffix, *args, **kwargs))
        for _ in range(3):
            with self.assertRaisesRegex(DriverWait, 'source_recovery_refused'):
                self.admit()
        self.assertEqual(self.attempts(), 1)
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.driver._request = real
        self.assertIsNone(self.admit())
        self.assertEqual(self.attempts(), 0)

    def predecessor_census(self):
        from ops import coordinator_predecessor_census as census
        try:
            return census.census(self.root, key=b'k' * 32, authority_id=self.queue.authority_id,
                node_ids=('cluster',), uid=os.geteuid(), max_artifact_bytes=1024)
        except census.CensusRefused as error:
            return str(error)

    def test_predecessor_census_answers_the_same_before_and_after_recovery(self):
        self.restart_coordinator()
        before = self.predecessor_census()
        self.assertIsNone(self.admit())
        self.assertEqual(self.predecessor_census(), before)

    def test_recovery_adds_no_table_and_only_its_receipt_to_metadata(self):
        self.restart_coordinator()
        with self.queue._db() as db:
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            before = self.store._metadata_used_bytes(db)
        self.assertIsNone(self.admit())
        with self.queue._db() as db:
            after = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            used = self.store._metadata_used_bytes(db)
            stored = db.execute('SELECT * FROM reference_recovery_receipts').fetchone()
        self.assertEqual(after, tables)
        self.assertLessEqual({'reference_recovery_receipts', 'reference_recovery_startup'}, STARTUP_TABLES)
        self.assertEqual(used - before, sum(len(str(value).encode()) for value in tuple(stored)))


class RecoveryAcrossRangesTests(RecoveryHarness):
    chunk_bytes = 2

    def test_restart_mid_object_resumes_at_the_next_range(self):
        first = validate_source_grant(self.first_grant, job=self.claim, clock=lambda: self.now[0])
        commit, restarted = self.client._commit_range, []

        def commit_then_restart(key, object_id, request_id, start, length, digest):
            commit(key, object_id, request_id, start, length, digest)
            if start == 0 and not restarted:
                restarted.append(True)
                self.restart_coordinator()
                self.assertIsNone(self.admit())

        self.client._commit_range = commit_then_restart
        self.assertEqual(self.fetch(first, 0), b'abc')
        self.assertEqual(self.receipts()[0]['committedRanges'], 1)
        self.assertEqual(self.usage(), (2, 3, 2))


class RecoveryManyObjectTests(RecoveryHarness):
    extra_objects = 120

    def test_transfer_wire_past_the_v1_cap_resumes(self):
        with self.queue._db() as db:
            wire = db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?', (self.job_id,)).fetchone()[0]
        self.assertGreater(len(wire.encode()), 4096)
        self.assertLess(len(wire.encode()), SOURCE_TRANSFER_MAX_BYTES)
        self.restart_coordinator()
        self.assertTrue(self.state()['eligible'])
        self.assertIsNone(self.admit())
        self.assertEqual(self.state()['sourceTransfer']['objectCount'], 122)
        self.assertLess(len(protocol.canonical_json(self.saved[-1]['sourceRecovery'])), 4096)

    def test_wire_over_the_explicit_limit_is_refused(self):
        self.restart_coordinator()
        with patch_limit(1000):
            with self.assertRaises(Conflict):
                self.state()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))


class StepHarness(RecoveryHarness):
    """step() from the driver's own reservation to execution_pending, through a restart.

    Real step, recovery, routed v2 coordinator, fenced ledger, range client and
    journal; the reservation is the one step() built and posted, never a copy from
    the coordinator. Gate, engine and TLS are absent: nothing here reaches them.
    """
    harness_reserves = False

    def start_transfer(self):
        self.now = [time.time()]

    def first_journal(self):
        return {}

    def real_driver(self):
        driver = super().real_driver()
        del driver._save
        (self.root / 'scratch').mkdir(mode=0o700)
        self.ledger = ScratchLedger(self.root / 'ledger.sqlite', self.root / 'scratch',
            capacity_probe=lambda _: 10**9, safety_floor_bytes=0)
        self.ledger.clock = lambda: self.now[0]
        driver.ledger, driver._lock, driver._bearer = self.ledger, threading.Lock(), WORKER_TOKEN
        driver._state_owner = driver.execution_router = driver.combined_preparation = None
        driver._combined_preparation_initial = driver._active_step = driver._active_job = None
        driver.source_range_v2_enabled, driver.output_budget, driver.transfer_ttl = True, 10000, 300
        self.client.renewal_headers = driver._source_renewal_headers
        _, claim = self.worker_request('POST', '/claim', {'jobId': self.job_id, 'claimRequestId': 'source-claim-renewal'})
        driver._save(self.job_id, 'claimed', {'claim': claim})
        return driver

    def row(self):
        with self.driver._db() as db:
            stage, data = db.execute('SELECT stage,data FROM driver_jobs WHERE job=?', (self.job_id,)).fetchone()
        return stage, json.loads(data)

    def step(self, state):
        result = self.driver.step()
        self.assertEqual(result, {'state': state, 'jobId': self.job_id})

    def to_fetching(self):
        for state in ('awaiting_grant', 'fetching_pending', 'fetching'):
            self.step(state)
        return self.row()[1]['reservation']

    def finish_fetch(self):
        while self.row()[0] == 'fetching':
            self.assertIn(self.driver.step()['state'], ('fetching', 'processing_pending'))

    def fenced(self, reservation):
        with self.ledger._transaction() as db:
            row = self.ledger._reservation(db, reservation['reservationId'])
            fence = db.execute('SELECT 1 FROM transfer_fences WHERE reservation=?', (reservation['reservationId'],)).fetchone()
            count = db.execute('SELECT COUNT(*) FROM reservations').fetchone()[0]
        return row['state'], fence is not None, count

    def assert_bound(self, reservation, stage):
        """The coordinator and the receipt name the digest of what step() posted."""
        digest = protocol.digest(reservation)
        self.assertEqual(self.row()[0], stage)
        self.assertEqual(self.receipts()[0]['transferRequestSha256'], digest)
        with self.queue._db() as db:
            wire = db.execute('SELECT transfer_json FROM reference_jobs WHERE job_id=?', (self.job_id,)).fetchone()[0]
        self.assertEqual(protocol.digest(json.loads(wire)), digest)
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.fenced(reservation), ('reserved', True, 1))
        self.assertEqual(self.row()[1]['sourceRecovery'], {'attempts': 0, 'pending': None})

    def lost_phase_reply(self):
        self.step('awaiting_grant')
        self.step('fetching_pending')
        self.lose_reply.add('phase')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual((self.row()[0], self.store.get(self.job_id)['phase']), ('fetching_pending', 'fetching'))
        return self.row()[1]['reservation']


class StepThroughRecoveryTests(StepHarness):
    extra_objects = 10

    def test_mid_fetch_restart_past_the_lease_resumes_the_drivers_own_reservation(self):
        reservation = self.to_fetching()
        self.step('fetching')
        self.assertEqual(len(self.row()[1]['inputPaths']), 1)
        self.now[0] = stamp(reservation['expiresAt']) + 3600
        self.restart_coordinator()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.assertTrue(self.state()['eligible'])
        self.assertEqual(self.state()['reservation']['transferRequestSha256'], protocol.digest(reservation))
        self.step('fetching')
        self.assert_bound(reservation, 'fetching')
        _stage, data = self.row()
        self.assertEqual(len(data['inputPaths']), 2)
        self.assertGreater(self.receipts()[0]['committedBytes'], 0)
        self.finish_fetch()
        self.step('execution_pending')
        for relative in self.row()[1]['inputPaths'].values():
            self.assertTrue((self.root / 'scratch' / relative).is_file())

    def test_restart_after_the_last_object_resumes_with_no_get_left(self):
        reservation = self.to_fetching()
        self.finish_fetch()
        self.restart_coordinator()
        self.step('processing_pending')
        self.assert_bound(reservation, 'processing_pending')
        self.step('execution_pending')

    def test_lost_fetching_phase_reply_recovers_instead_of_looping_on_409(self):
        reservation = self.lost_phase_reply()
        self.restart_coordinator()
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        self.step('fetching_pending')
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.receipts()[0]['transferRequestSha256'], protocol.digest(reservation))
        self.step('fetching')
        self.finish_fetch()
        self.assertEqual(self.fenced(reservation), ('reserved', True, 1))

    def test_fetching_pending_attests_no_fence_because_the_ledger_has_none(self):
        reservation = self.lost_phase_reply()
        self.restart_coordinator()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.fenced(reservation), ('reserved', False, 1))
        self.assertIs(self.row()[1]['sourceRecovery']['pending']['evidence']['transferFenced'], False)

    def test_processing_pending_attests_the_fence_it_really_has(self):
        reservation = self.to_fetching()
        self.finish_fetch()
        self.restart_coordinator()
        self.lose_reply.add('recover')
        self.assertEqual(self.driver.step()['state'], 'retry_wait')
        self.assertEqual(self.fenced(reservation), ('reserved', True, 1))
        self.assertIs(self.row()[1]['sourceRecovery']['pending']['evidence']['transferFenced'], True)

    def test_unfenced_lapsed_lease_stays_held_and_asks_for_nothing(self):
        reservation = self.lost_phase_reply()
        self.now[0] = stamp(reservation['expiresAt']) + 1
        self.restart_coordinator()
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'reservation_expired'})
        self.assertEqual((self.receipts(), self.status()), ([], ('held', 'coordinator_restart')))
        self.assertNotIn('sourceRecovery', self.row()[1])

    def test_nine_restarts_of_one_claim_all_recover_and_reset_no_budget(self):
        reservation = self.to_fetching()
        claim = self.row()[1]['claim']['claimToken']
        grant = self.row()[1]['grant']
        spent = []
        for restart in range(1, SOURCE_RECOVERY_ATTEMPTS + 2):
            self.restart_coordinator()
            self.step('fetching')
            _stage, data = self.row()
            self.assertEqual(data['sourceRecovery'], {'attempts': 0, 'pending': None})
            self.assertEqual((len(self.receipts()), len(data['inputPaths'])), (restart, restart))
            self.assertEqual((data['claim']['claimToken'], data['grant'], data['reservation']), (claim, grant, reservation))
            spent.append(self.usage())
        self.assertEqual(self.status(), ('running', None))
        self.assertEqual(self.fenced(reservation), ('reserved', True, 1))
        self.assertEqual(self.row()[0], 'fetching')
        for before, after in zip(spent, spent[1:]):
            self.assertTrue(all(new > old for old, new in zip(before, after)), (before, after))
        self.assertEqual(len({receipt['coordinatorEpoch'] for receipt in self.receipts()}), 9)
        self.assertEqual({receipt['transferRequestSha256'] for receipt in self.receipts()}, {protocol.digest(reservation)})

    def test_a_reservation_that_is_not_the_posted_one_is_held_and_reported(self):
        reservation = self.to_fetching()
        self.restart_coordinator()
        stage, data = self.row()
        data['reservation'] = dict(reservation, nonce='another-nonce')
        self.driver._save(self.job_id, stage, data)
        self.assertEqual(self.driver.step(), {'state': 'held', 'reason': 'source_recovery_state_changed'})
        self.assertEqual(self.receipts(), [])
        self.assertEqual(self.status(), ('held', 'coordinator_restart'))
        stage, data = self.row()
        self.assertNotIn('inputPaths', data)
        self.assertEqual((data['watch']['reasonCode'], data['watch']['ack']['reasonCode']),
            ('source_transfer_unavailable', 'source_transfer_unavailable'))


def patch_limit(value):
    return patch('processing.reference_recovery.SOURCE_TRANSFER_MAX_BYTES', value)


class RecoveryWireTests(RecoveryHarness):
    def test_state_and_request_are_versioned(self):
        self.restart_coordinator()
        state = self.state()
        self.assertEqual((state['schemaVersion'], state['protocol']), (2, SOURCE_STATE_PROTOCOL))
        request = ClaimRecoveryService._request(self.body())
        self.assertEqual(request['schemaVersion'], 2)
        self.assertNotIn('parentBinding', request)
        with self.assertRaises(ValueError):
            ClaimRecoveryService._request(dict(request, extra=True))
        with self.assertRaises(ValueError):
            ClaimRecoveryService._request(dict(request, schemaVersion=3))


if __name__ == '__main__':
    import unittest
    unittest.main()
