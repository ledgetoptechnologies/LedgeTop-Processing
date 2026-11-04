"""Compatibility and corruption boundaries for eager result-custody recovery."""
import unittest
import threading
import json
from unittest.mock import patch

from processing.viewer_result_adoption import CUSTODY_INTENT, new_custody, valid_custody
from processing.maintenance import Conflict
from tests import test_eager_custody_gap as eager


class CustodySemanticBoundaries(unittest.TestCase):
    def setUp(self):
        self.case = eager.EagerCustody('test_outage_after_completion_keeps_the_result')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_older_same_version_sidecar_is_rejected_without_migration(self):
        identity = {'task': 'task', 'generation': 1, 'job': 'job', 'node': 'node', 'engine': 'engine'}
        old = {'version': 1, 'identity': identity, 'state': 'unavailable', 'reason': 'engine_task_missing',
               'result': {}, 'archive': None, 'checked': False}
        self.assertEqual(old['version'], new_custody(identity, {})['version'])
        self.assertFalse(valid_custody(old, identity))

    def test_removed_legacy_unavailable_sidecar_blocks_uuid_reset(self):
        identifier = self.case.finished()
        service = self.case.service
        task = self.case.fixture.task(identifier)
        directory = self.case.directory(identifier)
        identity = {'task': task['uuid'], 'generation': task['generation'], 'job': task['job_id'],
                    'node': task['node_id'], 'engine': task['engine_uuid']}
        legacy = {'version': 1, 'identity': identity, 'state': 'unavailable', 'reason': 'engine_task_missing',
                  'result': {}, 'archive': None, 'checked': False}
        service.store.save_custody(task, legacy)
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=NULL WHERE uuid=?', (identifier,))
        self.assertEqual(service.remove(identifier), {'success': True, 'uuid': identifier})
        with self.assertRaises(Conflict):
            service.initialize(identifier, 'Synthetic survey', eager.viewer_fixture.REQUEST_OPTIONS, None, 'production')
        self.assertTrue(directory.exists())

    def test_malformed_verified_sidecar_cannot_clear_pending_intent(self):
        identifier = self.case.finished()
        service = self.case.service
        task = self.case.fixture.task(identifier)
        record = service.store.custody(task)
        record.update(state='verified', archive=None, checked=False)
        service.store.save_custody(task, record)
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=? WHERE uuid=?', (CUSTODY_INTENT, identifier))
        service._attempt_eager_custody(identifier, None)
        self.assertEqual(self.case.fixture.task(identifier)['error'], CUSTODY_INTENT)

    def test_viewer_first_download_is_recorded_checked_when_verified(self):
        identifier = self.case.finished(wait=False)
        service = self.case.service
        # Finish without scheduling an eager transfer, then let Viewer fetch.
        with patch.object(service, '_advance_eager_custody', lambda: None):
            service.tick()
        stream, _ = service.download(identifier)
        stream.close()
        self.case.settle()
        record = service.store.custody(self.case.fixture.task(identifier))
        self.assertEqual(record['state'], 'verified')
        self.assertTrue(record['checked'])

    def test_verified_sidecar_cannot_clear_intent_after_archive_corruption(self):
        identifier = self.case.finished()
        service = self.case.service
        task = self.case.fixture.task(identifier)
        path = self.case.directory(identifier) / 'all.zip'
        body = bytearray(path.read_bytes())
        body[-1] ^= 1
        path.write_bytes(body)
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=? WHERE uuid=?', (CUSTODY_INTENT, identifier))
        service._attempt_eager_custody(identifier, None)
        self.assertEqual(self.case.fixture.task(identifier)['error'], CUSTODY_INTENT)

    def test_verified_sidecar_cannot_clear_intent_after_receipt_change(self):
        identifier = self.case.finished()
        service = self.case.service
        path = self.case.directory(identifier) / 'all.zip.receipt'
        receipt = json.loads(path.read_text())
        receipt['sha256'] = '0' * 64
        path.write_text(json.dumps(receipt))
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=? WHERE uuid=?', (CUSTODY_INTENT, identifier))
        service._attempt_eager_custody(identifier, None)
        self.assertEqual(self.case.fixture.task(identifier)['error'], CUSTODY_INTENT)

    def test_restart_rehash_does_not_hold_global_or_database_write_lock(self):
        identifier = self.case.finished()
        service = self.case.service
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=? WHERE uuid=?', (CUSTODY_INTENT, identifier))
        entered, release = threading.Event(), threading.Event()
        original = service._verify_archive
        def blocked(*args):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('test verification timeout')
            return original(*args)
        errors = []
        def verify():
            try:
                service._attempt_eager_custody(identifier, None)
            except BaseException as error:
                errors.append(error)
        with patch.object(service, '_verify_archive', side_effect=blocked):
            thread = threading.Thread(target=verify)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertTrue(service._lock.acquire(timeout=2))
                service._lock.release()
                with self.case.fixture.queue._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    db.rollback()
            finally:
                release.set()
                thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertIsNone(self.case.fixture.task(identifier)['error'])

    def test_boolean_version_and_generation_are_not_integer_identity(self):
        identity = {'task': 'task', 'generation': 1, 'job': 'job', 'node': 'node', 'engine': 'engine'}
        record = new_custody(identity, {})
        record['version'] = True
        self.assertFalse(valid_custody(record, identity))
        record = new_custody(identity, {})
        record['identity'] = {**identity, 'generation': True}
        self.assertFalse(valid_custody(record, identity))


if __name__ == '__main__':
    unittest.main()
