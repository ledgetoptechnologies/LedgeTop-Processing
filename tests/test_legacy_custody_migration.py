"""Explicit, fail-closed migration of old same-version custody sidecars."""
import json
import unittest
from unittest.mock import patch

from processing.maintenance import Conflict
from processing.viewer_result_adoption import CUSTODY_INTENT, CUSTODY_LEGACY_BACKUP, CUSTODY_NAME
from tests import test_eager_custody_gap as eager


class LegacyCustodyMigration(unittest.TestCase):
    def setUp(self):
        self.case = eager.EagerCustody('test_outage_after_completion_keeps_the_result')
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.identifier = self.case.finished()
        self.service = self.case.service
        self.task = self.case.fixture.task(self.identifier)
        current = self.service.store.custody(self.task)
        self.old = {k: current[k] for k in ('version', 'identity', 'state', 'reason', 'result', 'archive', 'checked')}

    def install(self, state='unavailable'):
        self.old.update(state=state, reason='engine_task_missing' if state == 'unavailable' else None)
        self.service.store.save_custody(self.task, self.old)
        with self.case.fixture.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET error=NULL WHERE uuid=?', (self.identifier,))

    def migrate(self):
        return self.service.custody_action(self.task['job_id'], 'migrate-legacy', {'note': 'review legacy result'})

    def test_unavailable_and_verified_are_preserved_as_explicit_refusals(self):
        for state in ('unavailable', 'verified'):
            with self.subTest(state=state):
                self.install(state)
                result = self.migrate()
                self.assertEqual((result['state'], result['reason']), ('refused', 'legacy_custody_requires_inspection'))
                self.assertEqual(self.case.fixture.task(self.identifier)['error'], CUSTODY_INTENT)
                backup = self.case.directory(self.identifier) / CUSTODY_LEGACY_BACKUP
                self.assertEqual(json.loads(backup.read_text()), self.old)
                # Do not reuse a backup from the previous subcase.
                backup.unlink()

    def test_wrong_identity_boolean_version_missing_or_corrupt_fail_without_mutation(self):
        self.install()
        path = self.case.directory(self.identifier) / CUSTODY_NAME
        for invalid in ({**self.old, 'version': True}, {**self.old, 'identity': {**self.old['identity'], 'generation': True}}, None, 'corrupt'):
            with self.subTest(invalid=invalid):
                if invalid is None:
                    path.unlink(missing_ok=True)
                elif invalid == 'corrupt':
                    path.write_text('{broken')
                else:
                    path.write_text(json.dumps(invalid))
                before = path.read_bytes() if path.exists() else None
                with self.assertRaises(Conflict):
                    self.migrate()
                self.assertIsNone(self.case.fixture.task(self.identifier)['error'])
                self.assertEqual(path.read_bytes() if path.exists() else None, before)

    def test_failure_after_durable_intent_preserves_legacy_and_recovers(self):
        self.install()
        before = (self.case.directory(self.identifier) / CUSTODY_NAME).read_bytes()
        with patch.object(self.service.store, 'save_custody', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                self.migrate()
        self.assertEqual(self.case.fixture.task(self.identifier)['error'], CUSTODY_INTENT)
        self.assertEqual((self.case.directory(self.identifier) / CUSTODY_NAME).read_bytes(), before)
        self.case.restart()
        self.service = self.case.service
        self.assertEqual(self.migrate()['state'], 'refused')
        self.assertEqual(self.case.fixture.task(self.identifier)['error'], CUSTODY_INTENT)

    def test_advisory_census_distinguishes_legacy_missing_and_invalid_without_writes(self):
        path = self.case.directory(self.identifier) / CUSTODY_NAME
        self.install()
        before = path.read_bytes()
        report = self.service.legacy_custody_census(limit=1)
        self.assertTrue(report['advisoryOnly'])
        self.assertTrue(report['historicalIncarnationsNotInspected'])
        self.assertEqual(report['entries'], [{'taskId': self.identifier, 'format': 'legacy'}])
        self.assertEqual(path.read_bytes(), before)
        path.unlink()
        self.assertEqual(self.service.legacy_custody_census()['entries'][0]['format'], 'missing')
        path.write_text('{broken')
        self.assertEqual(self.service.legacy_custody_census()['entries'][0]['format'], 'inspection_required')

    def test_conflicting_backup_is_never_overwritten(self):
        self.install()
        backup = self.case.directory(self.identifier) / CUSTODY_LEGACY_BACKUP
        backup.write_text('{}')
        with self.assertRaises(Conflict):
            self.migrate()
        self.assertEqual(backup.read_text(), '{}')
        self.assertIsNone(self.case.fixture.task(self.identifier)['error'])

    def test_job_result_binding_cannot_be_forged_by_numeric_boolean(self):
        self.install()
        with self.case.fixture.queue._db() as db:
            row = db.execute('SELECT result FROM jobs WHERE id=?', (self.task['job_id'],)).fetchone()
            result = json.loads(row[0])
            result['cancelled'] = 0
            db.execute('UPDATE jobs SET result=? WHERE id=?', (json.dumps(result), self.task['job_id']))
        with self.assertRaises(Conflict):
            self.migrate()
        self.assertIsNone(self.case.fixture.task(self.identifier)['error'])


if __name__ == '__main__':
    unittest.main()
