import hashlib
import os
import unittest
from unittest.mock import patch

from processing.guarded_spool_budget import GuardedSpoolBudget
from processing.host_mutation_journal import MutationHeld
from processing.node_gate import NodeGate
from tests.test_guarded_native_proxy import GuardedNativeProxyTests


@unittest.skipIf(os.name == 'nt', 'POSIX spool recovery')
class GuardedSpoolRecovery(unittest.TestCase):
    def setUp(self):
        GuardedNativeProxyTests.setUp(self)
        self.proxy.initialize(self.task, **self.auth)
        self.budget = GuardedSpoolBudget(self.spool, cap=1024, floor=0)
        self.name = 'incoming-' + 'a' * 48
        self.path = self.spool / self.name
        self.budget.reserve(self.name, 3)
        self.path.write_bytes(b'abc')
        self.path.chmod(0o600)
        self.budget.transition(self.name, 'receiving', 'spooled')
        with NodeGate('physical-engine') as gate:
            receipt = self.proxy._owner(gate, self.job)
        info = self.path.stat()
        self.budget.bind_upload(self.name, receipt=receipt, task=self.job, file_name='photo.jpg',
                                sha256=hashlib.sha256(b'abc').hexdigest(),
                                device=info.st_dev, inode=info.st_ino)
        self.budget.transition(self.name, 'spooled', 'mutation_started')

    def upload(self):
        self.proxy.upload(self.job, {**self.item, 'path': str(self.path)}, **self.auth)

    def test_pending_or_missing_ack_never_deletes(self):
        with self.assertRaises(Exception):
            self.proxy.recover_acknowledged_spool(self.budget, self.name)
        self.assertEqual(self.path.read_bytes(), b'abc')
        self.assertEqual(self.budget.inspect()['reservations'][0]['stage'], 'mutation_started')

    def test_reopen_after_ack_then_recover_without_replaying_engine(self):
        self.upload()
        reopened = GuardedSpoolBudget(self.spool, cap=1024, floor=0)
        self.proxy.recover_acknowledged_spool(reopened, self.name)
        self.assertFalse(self.path.exists())
        self.assertEqual(reopened.inspect()['reservations'], [])
        self.assertEqual(self.backend.counts()['upload'], 1)

    def test_changed_file_is_held_even_after_valid_ack(self):
        self.upload()
        self.path.write_bytes(b'xyz')
        with self.assertRaises(MutationHeld):
            self.proxy.recover_acknowledged_spool(self.budget, self.name)
        self.assertEqual(self.path.read_bytes(), b'xyz')
        self.assertEqual(len(self.budget.inspect()['reservations']), 1)

    def test_journal_cannot_authorize_a_different_file_identity(self):
        self.upload()
        with self.budget._connect() as db:
            db.execute("UPDATE spool_upload_identity SET file_name='other.jpg' WHERE name=?", (self.name,))
        with self.assertRaises(Exception):
            self.proxy.recover_acknowledged_spool(self.budget, self.name)
        self.assertEqual(self.path.read_bytes(), b'abc')
        self.assertEqual(len(self.budget.inspect()['reservations']), 1)

    def test_crash_after_unlink_retains_charge_until_retry(self):
        self.upload()
        with patch.object(self.budget, 'release_discarded', side_effect=RuntimeError('crash')):
            with self.assertRaises(RuntimeError):
                self.proxy.recover_acknowledged_spool(self.budget, self.name)
        self.assertFalse(self.path.exists())
        reopened = GuardedSpoolBudget(self.spool, cap=1024, floor=0)
        self.proxy.recover_acknowledged_spool(reopened, self.name)
        self.assertEqual(reopened.inspect()['reservations'], [])
        self.assertEqual(self.backend.counts()['upload'], 1)

    def test_process_dies_after_unlink_before_directory_sync(self):
        self.upload()
        child = os.fork()
        if child == 0:
            actual_unlink = os.unlink
            def crash_after_unlink(*args, **kwargs):
                actual_unlink(*args, **kwargs)
                os._exit(77)
            with patch('processing.guarded_native_proxy.os.unlink', side_effect=crash_after_unlink):
                self.proxy.recover_acknowledged_spool(self.budget, self.name)
            os._exit(1)
        _, status = os.waitpid(child, 0)
        self.assertEqual(os.waitstatus_to_exitcode(status), 77)
        reopened = GuardedSpoolBudget(self.spool, cap=1024, floor=0)
        self.assertEqual(reopened.inspect()['reservations'][0]['stage'], 'discardable')
        self.proxy.recover_acknowledged_spool(reopened, self.name)
        self.assertEqual(reopened.inspect()['reservations'], [])
        self.assertEqual(self.backend.counts()['upload'], 1)
