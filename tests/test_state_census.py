import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import tempfile
import unittest

from ops import state_census

KEY = bytes(range(32))
KEY_TEXT = KEY.hex() + '\n'


def _run(argv, key=KEY_TEXT):
    output, stdin = io.StringIO(), io.StringIO(key)
    original = state_census.sys.stdin
    state_census.sys.stdin = stdin
    try:
        with contextlib.redirect_stdout(output):
            status = state_census.main(argv)
    finally:
        state_census.sys.stdin = original
    return status, json.loads(output.getvalue())


class CensusFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.root = self.base / 'state'
        self.root.mkdir()
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT)')
        db.execute('CREATE TABLE viewer_tasks (uuid TEXT, stage TEXT, removed INTEGER)')
        db.execute('CREATE TABLE coordinator_identity (singleton INTEGER PRIMARY KEY, authority_id TEXT)')
        db.executemany('INSERT INTO jobs(status) VALUES (?)', [('succeeded',), ('failed',)])
        db.execute("INSERT INTO viewer_tasks VALUES ('a','succeeded',0)")
        db.execute("INSERT INTO coordinator_identity VALUES (1,'synthetic-authority')")
        db.commit()
        db.close()
        (self.root / 'viewer-spool').mkdir()
        task = self.root / 'viewer-tasks' / 'task-1'
        task.mkdir(parents=True)
        (task / 'DJI_0001.JPG').write_bytes(b'synthetic photo bytes')
        (task / 'DJI_0002.JPG').write_bytes(b'second photo')

    def tearDown(self):
        self.temporary.cleanup()

    def census(self, root=None):
        status, document = _run(['census', str(root or self.root)])
        return status, document

    def save(self, name, document):
        path = self.base / name
        path.write_text(json.dumps(document))
        return path

    def compare(self, before, after, *extra):
        return _run(['compare', str(self.save('a.json', before)), str(self.save('b.json', after)), *extra])

    def copy(self):
        target = self.base / 'copy'
        shutil.copytree(self.root, target, copy_function=shutil.copy2)
        os.utime(target, ns=(self.root.stat().st_atime_ns, self.root.stat().st_mtime_ns))
        for path in sorted(self.root.rglob('*'), reverse=True):
            if path.is_dir():
                info = path.stat()
                os.utime(target / path.relative_to(self.root), ns=(info.st_atime_ns, info.st_mtime_ns))
        return target


class CensusTest(CensusFixture):
    def test_reports_state_and_is_stable_and_read_only(self):
        before = {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()}
        status, first = self.census()
        _, second = self.census()
        self.assertEqual(status, 0)
        self.assertEqual(first, second)
        self.assertEqual(before, {p: p.read_bytes() for p in self.root.rglob('*') if p.is_file()})
        self.assertEqual(first['database']['quickCheck'], 'ok')
        self.assertEqual(first['database']['active'], {'jobs': 0, 'viewerTasks': 0})
        self.assertEqual(first['database']['sidecars'], [])
        self.assertEqual(first['tree']['regularFiles'], 3)
        self.assertEqual(sum(1 for e in first['entries'].values() if e.get('root')), 1)

    def test_output_never_exposes_names_or_guessable_path_hashes(self):
        _, document = self.census()
        text = json.dumps(document)
        for private in ('DJI_0001', 'task-1', 'viewer-tasks', 'synthetic photo', 'succeeded'):
            self.assertNotIn(private, text)
        for relative in ('viewer-tasks/task-1/DJI_0001.JPG', 'jobs.sqlite3'):
            self.assertNotIn(hashlib.sha256(relative.encode()).hexdigest(), text)
        _, other_key = _run(['census', str(self.root)], key=bytes(32).hex())
        self.assertFalse(set(document['entries']) & set(other_key['entries']))

    def test_rejects_unsupported_file_types(self):
        cases = []
        link = self.root / 'viewer-tasks' / 'link'
        try:
            link.symlink_to('task-1')
            cases.append(('symlinks', link))
        except OSError:
            pass
        hard = self.root / 'viewer-tasks' / 'task-1' / 'hard'
        try:
            os.link(self.root / 'viewer-tasks' / 'task-1' / 'DJI_0002.JPG', hard)
            cases.append(('hardlinked', hard))
        except OSError:
            pass
        if hasattr(os, 'mkfifo'):
            fifo = self.root / 'fifo'
            os.mkfifo(fifo)
            cases.append(('other', fifo))
        if not cases:
            self.skipTest('no unsupported file types can be created here')
        for name, path in cases:
            status, document = self.census()
            self.assertEqual(status, 3, name)
            self.assertGreater(document['tree']['rejected'][name], 0)
            path.unlink()
        self.assertEqual(self.census()[0], 0)

    def test_reports_partials(self):
        (self.root / 'viewer-tasks' / 'task-1' / '.incoming-1').write_bytes(b'partial')
        self.assertEqual(self.census()[1]['tree']['incomingPartials'], 1)


class CompareTest(CensusFixture):
    def test_faithful_copy_is_identical(self):
        copy = self.copy()
        status, result = self.compare(self.census()[1], self.census(copy)[1])
        self.assertEqual(status, 0, result)
        self.assertTrue(result['identical'])

    def test_detects_changed_content_with_equal_size(self):
        before = self.census()[1]
        photo = self.root / 'viewer-tasks' / 'task-1' / 'DJI_0001.JPG'
        info = photo.stat()
        photo.write_bytes(b'SYNTHETIC PHOTO BYTES')
        os.utime(photo, ns=(info.st_atime_ns, info.st_mtime_ns))
        status, result = self.compare(before, self.census()[1])
        self.assertEqual(status, 1)
        self.assertEqual(result['fileDifferences'], {'contentChanged': 1})
        self.assertEqual(result['files'][0]['fields'], ['sha256'])
        self.assertNotIn('DJI', json.dumps(result))

    def test_detects_metadata_added_and_removed(self):
        before = self.census()[1]
        photo = self.root / 'viewer-tasks' / 'task-1' / 'DJI_0002.JPG'
        if os.name == 'posix':
            photo.chmod(0o600)
        os.utime(photo, (1, 1))
        (self.root / 'viewer-tasks' / 'task-1' / 'DJI_0001.JPG').unlink()
        (self.root / 'viewer-spool' / 'new').write_bytes(b'x')
        status, result = self.compare(before, self.census()[1])
        self.assertEqual(status, 1)
        self.assertEqual(result['fileDifferences'].get('metadataChanged', 0) >= 1, True)
        self.assertEqual(result['fileDifferences']['removed'], 1)
        self.assertEqual(result['fileDifferences']['added'], 1)

    def test_allowances_are_explicit_and_scoped(self):
        before = self.census()[1]
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('ALTER TABLE viewer_tasks ADD COLUMN evidence INTEGER NOT NULL DEFAULT 0')
        db.execute('CREATE TABLE viewer_reservations (uuid TEXT)')
        db.commit()
        db.close()
        after = self.census()[1]
        status, result = self.compare(before, after)
        self.assertEqual(status, 1)
        allow = ['--allow-path', '.', '--allow-path', 'jobs.sqlite3', '--allow-object', 'viewer_tasks',
                 '--allow-object', 'viewer_reservations', '--allow-table', 'viewer_tasks',
                 '--allow-new-table', 'viewer_reservations']
        status, result = self.compare(before, after, *allow)
        self.assertEqual(status, 0, result)
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute("INSERT INTO viewer_reservations VALUES ('x')")
        db.execute("UPDATE jobs SET status='queued' WHERE status='failed'")
        db.commit()
        db.close()
        status, result = self.compare(before, self.census()[1], *allow)
        self.assertEqual(status, 1)
        changes = {item.get('table'): item.get('change') for item in result['database']}
        self.assertEqual(changes['viewer_reservations'], 'newTableNotEmpty')
        self.assertEqual(changes['jobs'], 'contentChanged')

    def test_allowed_path_does_not_hide_other_files(self):
        before = self.census()[1]
        (self.root / 'viewer-tasks' / 'task-1' / 'DJI_0002.JPG').write_bytes(b'changed')
        status, result = self.compare(before, self.census()[1], '--allow-path', 'jobs.sqlite3',
                                      '--allow-path', '.')
        self.assertEqual(status, 1)
        self.assertEqual(result['fileDifferences'], {'contentChanged': 1})

    def test_refuses_wrong_key_and_version(self):
        with self.assertRaises(SystemExit):
            _run(['census', str(self.root)], key='not-a-key')
        before = self.census()[1]
        old = dict(before, schemaVersion=1)
        with self.assertRaises(SystemExit):
            self.compare(old, before)

    def test_keygen_is_private_and_exclusive(self):
        path = self.base / 'key'
        self.assertEqual(state_census.main(['keygen', str(path)]), 0)
        self.assertRegex(path.read_text(), r'^[0-9a-f]{64}\n$')
        if os.name == 'posix':
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        with self.assertRaises(FileExistsError):
            state_census.main(['keygen', str(path)])


if __name__ == '__main__':
    unittest.main()
