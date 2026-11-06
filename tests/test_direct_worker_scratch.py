"""Synthetic scratch accounting only, never network or actual worker custody."""
import tempfile
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch

from processing import direct_worker_scratch as d
from processing.scratch_policy import ScratchLedger, PolicyConflict


class DirectWorkerScratchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.now = 100
        self.worker = dict(authorityId=str(uuid.uuid4()), nodeId='cluster', workerId='worker',
                           workerInstanceId='instance')
        self.item = dict(reservationId=str(uuid.uuid4()), jobId=str(uuid.uuid4()), attemptId=str(uuid.uuid4()),
            generation=1, environment='production', manifestSha256='a'*64, inputBytes=100,
            outputBudget=200, ttlSeconds=10, **self.worker)
        self.ledger = self.open()

    def open(self):
        return ScratchLedger(self.root/'private'/'ledger.sqlite', self.root,
            capacity_probe=lambda _: 1000, clock=lambda: self.now, safety_floor_bytes=100)

    def prepare(self, item=None):
        return d.prepare(self.ledger, item or self.item, expected_worker=self.worker, worker_ready=True)

    def test_exact_retry_across_restart_never_claims_custody(self):
        first = self.prepare()
        self.assertFalse(first['sourceCustodyAccepted']); self.assertFalse(first['transferAuthorized'])
        self.now = 10000; self.ledger = self.open()
        self.assertEqual(self.prepare(), first)
        self.assertTrue(self.ledger.transfer_fenced(first['scratchReservationId']))

    def test_crash_after_reserve_retries_exact_or_expires_without_bytes(self):
        with patch.object(self.ledger, 'begin_transfer', side_effect=OSError('lost before fence')):
            with self.assertRaises(OSError): self.prepare()
        self.ledger = self.open()
        result = self.prepare()
        self.assertTrue(self.ledger.transfer_fenced(result['scratchReservationId']))

    def test_expired_pre_fence_reservation_cannot_be_revived(self):
        with patch.object(self.ledger, 'begin_transfer', side_effect=OSError('lost before fence')):
            with self.assertRaises(OSError): self.prepare()
        self.now = 111; self.ledger = self.open()
        with self.assertRaises(PolicyConflict): self.prepare()

    def test_crash_after_fence_cannot_expire(self):
        original = self.ledger.begin_transfer
        def interrupted(reservation):
            original(reservation)
            raise OSError('lost after fence')
        with patch.object(self.ledger, 'begin_transfer', side_effect=interrupted):
            with self.assertRaises(OSError): self.prepare()
        self.now = 10000; self.ledger = self.open()
        self.assertEqual(self.prepare()['state'], 'scratch_fenced')

    def test_changed_attempt_or_worker_never_adopts_existing_scratch(self):
        self.prepare()
        with self.assertRaises(PolicyConflict): self.prepare(dict(self.item, attemptId=str(uuid.uuid4())))
        with self.assertRaises(PolicyConflict): self.prepare(dict(self.item, workerId='other'))

    def test_unready_and_insufficient_capacity_never_produce_observation(self):
        with self.assertRaises(PolicyConflict):
            d.prepare(self.ledger, self.item, expected_worker=self.worker, worker_ready=False)
        with self.assertRaises(PolicyConflict): self.prepare(dict(self.item, inputBytes=1000))


if __name__ == '__main__': unittest.main()
