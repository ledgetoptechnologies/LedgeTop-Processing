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
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
import uuid

from processing import reference_protocol as protocol
from processing import reference_source_intake_store as intake_store
from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.reference_driver import DriverHeld, DriverWait, ReferenceDriver
from processing.reference_source_intake_store import SOURCE_RANGE_GRANT_TTL_SECONDS
from processing.reference_transfer import TransferError
from processing.source_range_transfer import (RENEWAL_ACK_PATH, RENEWAL_PATH, SourceRangeClient,
    _request_id, validate_source_grant)
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


class SourceGrantRenewalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests
        case = ReferenceSourceIntakeTests('runTest')
        case.setUp()
        case.payload['manifest']['id'] = 'dataset-grant-renewal'
        case.rehash()
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
            self.directory.name, self.spool, 'renewal', source_range_v2=True)
        self.addCleanup(self.queue.close)
        self.job_id = job['jobId']
        self.first_grant = self.store.mint_source_transfer_grant(self.job_id, source_spool=self.spool)
        self.store.update_phase(self.job_id, 'fetching')
        self.claim = self.store.get(self.job_id, private=True)
        self.lease = stamp(self.claim['transferRequest']['expiresAt'])
        self.now = [stamp(self.first_grant['expiresAt']) - 60]
        for name in ('renew_source_transfer_grant', 'acknowledge_source_transfer_grant', 'source_range_for_grant'):
            real = getattr(self.store, name)
            setattr(self.store, name, lambda *args, real=real, **kwargs: real(
                *args, **{'clock': lambda: self.now[0], **kwargs}))
        self.worker = dict(node_id='cluster', worker_id='worker', worker_instance_id='instance',
            claim_token=self.claim_token)
        self.client = SourceRangeClient('https://broker.invalid', self.root / 'worker-ledger.sqlite',
            opener=_ControlOpener(self.control), clock=lambda: self.now[0])
        self.client.renewal_headers = self.credentials
        self.data = {'grant': copy.deepcopy(self.first_grant)}
        self.saved = []
        self.driver = SimpleNamespace(clock=lambda: self.now[0], source_range_client=self.client,
            _save=lambda *_args: self.saved.append(copy.deepcopy(self.data)))
        self.driver._source_range_grant = lambda *args: ReferenceDriver._source_range_grant(self.driver, *args)

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
        gate = self.queue._coordinator_freeze
        plan = str(uuid.uuid4())

        def change(action, **kwargs):
            gate.transition(dict(requestId=str(uuid.uuid4()), planId=plan, action=action,
                expectedRevision=gate.status()['revision'], startupId=self.queue.startup_id),
                owner_secret='synthetic-freeze-owner', **kwargs)

        with self.during_verification(lambda: change('intent')):
            with self.assertRaises(CoordinatorFrozen):
                self.renew_directly()
        self.assertEqual(self.journal(), [])
        change('abort', healthy=True)
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


if __name__ == '__main__':
    unittest.main()
