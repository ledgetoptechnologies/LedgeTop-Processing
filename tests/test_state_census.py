import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ops import state_census


def _census(root):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        state_census.main([str(root)])
    return json.loads(output.getvalue())


class StateCensusTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT)')
        db.execute('CREATE TABLE viewer_tasks (uuid TEXT, stage TEXT, removed INTEGER)')
        db.execute('CREATE TABLE coordinator_identity (singleton INTEGER PRIMARY KEY, authority_id TEXT)')
        db.executemany('INSERT INTO jobs(status) VALUES (?)', [('succeeded',), ('failed',), ('held',)])
        db.executemany('INSERT INTO viewer_tasks VALUES (?,?,?)',
                       [('a', 'succeeded', 0), ('b', 'uploading', 0), ('c', 'uploading', 1)])
        db.execute("INSERT INTO coordinator_identity VALUES (1,'synthetic-authority')")
        db.commit()
        db.close()
        (self.root / 'viewer-spool').mkdir()
        task = self.root / 'viewer-tasks' / 'task-1'
        task.mkdir(parents=True)
        (task / 'input-1').write_bytes(b'synthetic')
        (task / '.incoming-1').write_bytes(b'partial')

    def tearDown(self):
        self.temporary.cleanup()

    def test_reports_active_work_and_partials(self):
        result = _census(self.root)
        self.assertEqual(result['database']['quickCheck'], 'ok')
        self.assertEqual(result['database']['active'], {'jobs': 1, 'viewerTasks': 1})
        self.assertEqual(result['database']['authorityId'], 'synthetic-authority')
        self.assertEqual(result['tree']['incomingPartials'], 1)
        self.assertEqual(result['tree']['spoolEntries'], 0)

    def test_read_only_and_stable(self):
        before = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in self.root.rglob('*') if p.is_file()}
        first, second = _census(self.root), _census(self.root)
        after = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in self.root.rglob('*') if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(first, second)
        self.assertEqual(first['database']['sidecars'], [])

    def test_output_omits_names_and_rows(self):
        text = json.dumps(_census(self.root))
        for private in ('task-1', 'input-1', 'uploading', 'synthetic-authority-x'):
            self.assertNotIn(private, text)

    def test_detects_changed_symlink_target(self):
        link = self.root / 'viewer-tasks' / 'link'
        try:
            link.symlink_to('task-1')
        except OSError:
            self.skipTest('symlinks unavailable')
        first = _census(self.root)
        self.assertEqual(first['tree']['symlinks'], 1)
        link.unlink()
        link.symlink_to('elsewhere')
        self.assertNotEqual(first['tree']['treeSha256'], _census(self.root)['tree']['treeSha256'])

    def test_detects_in_place_row_update(self):
        first = _census(self.root)
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute("UPDATE jobs SET status='queued' WHERE status='failed'")
        db.commit()
        db.close()
        second = _census(self.root)
        self.assertEqual(first['database']['rowCounts'], second['database']['rowCounts'])
        self.assertNotEqual(first['database']['tableSha256']['jobs'], second['database']['tableSha256']['jobs'])

    def test_detects_changed_bytes(self):
        first = _census(self.root)
        (self.root / 'viewer-tasks' / 'task-1' / 'input-1').write_bytes(b'changed')
        self.assertNotEqual(first['tree']['treeSha256'], _census(self.root)['tree']['treeSha256'])


if __name__ == '__main__':
    unittest.main()
