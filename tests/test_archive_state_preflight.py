import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

from ops import archive_state_preflight as preflight, state_census
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore


class ArchiveStatePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'state'
        queue = Queue(self.root, node_ids=('cluster', 'main'), viewer_enabled=True)
        TaskStore(queue, self.root / 'viewer-tasks')
        queue.close()
        self.result = self.root / 'viewer-tasks' / 'retained.zip'
        self.result.write_bytes(b'synthetic retained result')
        self.key = b'k' * 32
        self.baseline = state_census.census(self.root, self.key)
        self.schema = self.baseline['database']['objects'].copy()

    def inspect(self):
        return preflight.inspect(self.root, self.key, self.baseline, self.schema)

    def test_unchanged_exact_qualified_state_is_read_only(self):
        self.assertEqual(self.inspect(), self.baseline)
        self.assertEqual(state_census.census(self.root, self.key), self.baseline)

    def test_changed_retained_result_holds_without_rewriting_it(self):
        self.result.write_bytes(b'changed synthetic result')
        before = state_census.census(self.root, self.key)
        with self.assertRaisesRegex(preflight.PreflightHold, 'state_changed'):
            self.inspect()
        self.assertEqual(state_census.census(self.root, self.key), before)

    def test_uncertain_partial_holds_even_when_in_baseline(self):
        partial = self.root / 'viewer-tasks' / 'all.zip.partial'
        partial.write_bytes(b'uncertain')
        self.baseline = state_census.census(self.root, self.key)
        with self.assertRaisesRegex(preflight.PreflightHold, 'partial_requires_inspection'):
            self.inspect()
        self.assertEqual(partial.read_bytes(), b'uncertain')

    def test_unreadable_state_holds_without_mutation(self):
        with patch('ops.state_census.census', side_effect=PermissionError('synthetic')):
            with self.assertRaisesRegex(preflight.PreflightHold, 'state_unavailable'):
                self.inspect()
        self.assertEqual(state_census.census(self.root, self.key), self.baseline)

    def test_unqualified_schema_holds(self):
        self.schema['table:jobs'] = '0' * 64
        with self.assertRaisesRegex(preflight.PreflightHold, 'schema_not_qualified'):
            self.inspect()
        self.assertEqual(state_census.census(self.root, self.key), self.baseline)

    def test_held_job_holds_even_if_in_reviewed_baseline(self):
        with sqlite3.connect(self.root / 'jobs.sqlite3') as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES ('synthetic-job','synthetic-request','production','odm-viewer-v1','held')")
        self.baseline = state_census.census(self.root, self.key)
        with self.assertRaisesRegex(preflight.PreflightHold, 'database_requires_inspection'):
            self.inspect()
        self.assertEqual(state_census.census(self.root, self.key), self.baseline)


if __name__ == '__main__':
    unittest.main()
