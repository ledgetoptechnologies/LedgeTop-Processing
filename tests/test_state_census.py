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

    def test_content_and_table_digests_cannot_be_matched_without_key(self):
        _, document = self.census()
        text = json.dumps(document)
        known = hashlib.sha256(b'synthetic photo bytes').hexdigest()
        self.assertNotIn(known, text)
        rows = '\n'.join(sorted(repr(row) for row in [(1, 'succeeded'), (2, 'failed')]))
        self.assertNotIn(hashlib.sha256(rows.encode()).hexdigest(), text)
        _, other_key = _run(['census', str(self.root)], key=bytes(32).hex())
        contents = lambda d: {e.get('content') for e in d['entries'].values()} - {None}
        self.assertFalse(contents(document) & contents(other_key))
        self.assertNotEqual(document['database']['tableDigest'], other_key['database']['tableDigest'])
        # Schema SQL is public source, so its hashes stay plain and reviewable.
        self.assertEqual(document['database']['objects'], other_key['database']['objects'])

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
        self.assertEqual(result['files'][0]['fields'], ['content'])
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

    DB_FIELDS = 'jobs.sqlite3:bytes,content,mtime'
    MIGRATED = "CREATE TABLE viewer_tasks (uuid TEXT, stage TEXT, removed INTEGER, evidence INTEGER NOT NULL DEFAULT 0)"
    NEW_TABLE = 'CREATE TABLE viewer_reservations (uuid TEXT)'

    def migrate(self, *extra):
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('ALTER TABLE viewer_tasks ADD COLUMN evidence INTEGER NOT NULL DEFAULT 0')
        db.execute(self.NEW_TABLE)
        for statement in extra:
            db.execute(statement)
        db.commit()
        db.close()

    def allowances(self):
        digest = lambda sql: hashlib.sha256(sql.encode()).hexdigest()
        return ['--allow-path', '.:mtime', '--allow-path', self.DB_FIELDS,
                '--allow-object', 'table:viewer_tasks=' + digest(self.MIGRATED),
                '--allow-object', 'table:viewer_reservations=' + digest(self.NEW_TABLE),
                '--allow-new-table', 'viewer_reservations']

    def empty_viewer_tasks(self):
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('DELETE FROM viewer_tasks')
        db.commit()
        db.close()

    def test_exact_migration_passes_only_with_exact_allowances(self):
        self.empty_viewer_tasks()  # live d738 has no Viewer tasks
        before = self.census()[1]
        self.migrate()
        after = self.census()[1]
        self.assertEqual(self.compare(before, after)[0], 1)
        status, result = self.compare(before, after, *self.allowances())
        self.assertEqual(status, 0, result)
        self.assertIn('transient writes are not excluded', result['scope'])

    def test_path_allowance_names_fields_and_never_covers_add_remove_or_other_fields(self):
        with self.assertRaises(SystemExit):
            self.compare(self.census()[1], self.census()[1], '--allow-path', 'jobs.sqlite3')
        with self.assertRaises(SystemExit):
            self.compare(self.census()[1], self.census()[1], '--allow-path', 'jobs.sqlite3:kind')
        database = self.root / 'jobs.sqlite3'
        if os.name == 'posix':
            database.chmod(0o644)
            self.root.chmod(0o755)
        before = self.census()[1]
        if os.name == 'posix':
            database.chmod(0o640)
            self.root.chmod(0o700)
            status, result = self.compare(before, self.census()[1], '--allow-path', self.DB_FIELDS,
                                          '--allow-path', '.:mtime')
            self.assertEqual(status, 1)
            self.assertEqual(sorted(f for item in result['files'] for f in item['fields']), ['mode', 'mode'])
            database.chmod(0o644)
            self.root.chmod(0o755)
        moved = self.base / 'moved.sqlite3'
        database.rename(moved)
        status, result = self.compare(before, self.census()[1], '--allow-path', self.DB_FIELDS)
        self.assertEqual(result['fileDifferences'].get('removed'), 1)
        moved.rename(database)
        (self.root / 'viewer-tasks' / 'task-1' / 'DJI_0002.JPG').write_bytes(b'changed')
        status, result = self.compare(before, self.census()[1], '--allow-path', self.DB_FIELDS,
                                      '--allow-path', '.:mtime')
        self.assertEqual(status, 1)
        self.assertEqual(result['fileDifferences'], {'contentChanged': 1})

    def test_object_allowance_requires_the_exact_expected_schema(self):
        before = self.census()[1]
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('ALTER TABLE viewer_tasks ADD COLUMN something_else TEXT')
        db.execute(self.NEW_TABLE)
        db.commit()
        db.close()
        status, result = self.compare(before, self.census()[1], *self.allowances())
        self.assertEqual(status, 1)
        objects = [item for item in result['database'] if item.get('object') == 'table:viewer_tasks']
        self.assertTrue(objects)
        with self.assertRaises(SystemExit):
            self.compare(before, before, '--allow-object', 'viewer_tasks')

    def test_object_allowance_is_also_an_assertion(self):
        before = self.census()[1]
        status, result = self.compare(before, before, *self.allowances())
        self.assertEqual(status, 1)
        self.assertIn({'object': 'table:viewer_tasks', 'change': 'allowanceNotMatched'}, result['database'])

    def test_new_table_must_be_empty_even_with_allow_table(self):
        before = self.census()[1]
        self.migrate("INSERT INTO viewer_reservations VALUES ('x')", "UPDATE jobs SET status='queued' WHERE status='failed'")
        status, result = self.compare(before, self.census()[1], *self.allowances(),
                                      '--allow-table', 'viewer_reservations')
        self.assertEqual(status, 1)
        changes = {item.get('table'): item.get('change') for item in result['database']}
        self.assertEqual(changes['viewer_reservations'], 'newTableNotEmpty')
        self.assertEqual(changes['jobs'], 'contentChanged')

    def test_existing_table_rows_must_not_change(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate("INSERT INTO viewer_tasks VALUES ('b','uploading',0,0)")
        status, result = self.compare(before, self.census()[1], *self.allowances())
        self.assertEqual(status, 1)
        self.assertIn({'table': 'viewer_tasks', 'change': 'contentChanged'}, result['database'])

    def test_migrating_a_nonempty_table_fails_conservatively(self):
        # Whole-row digests change when a column is added, so a migration over
        # existing rows is never silently accepted; it needs its own review.
        before = self.census()[1]
        self.migrate()
        status, result = self.compare(before, self.census()[1], *self.allowances())
        self.assertEqual(status, 1)
        self.assertIn({'table': 'viewer_tasks', 'change': 'contentChanged'}, result['database'])

    def generated_allowlist(self, after, *names):
        census_path = self.save('candidate.json', after)
        output = self.base / ('allowlist-%d.json' % len(list(self.base.glob('allowlist-*'))))
        status = state_census.main(['schema-allowlist', str(census_path), *sum((['--object', n] for n in names), []),
                                    '--output', str(output)])
        self.assertEqual(status, 0)
        return output

    def test_generated_schema_allowlist_gates_the_exact_migration(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate()
        after = self.census()[1]
        allowlist = self.generated_allowlist(after, 'table:viewer_tasks', 'table:viewer_reservations')
        if os.name == 'posix':
            self.assertEqual(stat.S_IMODE(allowlist.stat().st_mode), 0o600)
        self.assertEqual(set(json.loads(allowlist.read_text())), {'schemaAllowlistVersion', 'objects'})
        fixed = ['--allow-path', '.:mtime', '--allow-path', self.DB_FIELDS, '--allow-new-table', 'viewer_reservations']
        status, result = self.compare(before, after, *fixed, '--schema-allowlist', str(allowlist))
        self.assertEqual(status, 0, result)
        with self.assertRaises(FileExistsError):
            state_census.main(['schema-allowlist', str(self.save('c.json', after)), '--object', 'table:jobs',
                               '--output', str(allowlist)])

    def test_schema_allowlist_generator_refuses_unknown_or_repeated_objects(self):
        after = self.census()[1]
        for names in (['table:missing'], ['table:jobs', 'table:jobs'], ['jobs'], ['table:jobs;drop']):
            with self.assertRaises(SystemExit, msg=names):
                self.generated_allowlist(after, *names)

    def test_schema_allowlist_loader_rejects_anything_broader(self):
        digest = 'a' * 64
        broader = [
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': digest}, 'allowTable': ['jobs']},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': digest}, 'allowPath': ['viewer-tasks:content']},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': digest}, 'allowNewTable': ['x']},
            {'schemaAllowlistVersion': 1, 'objects': {'--allow-table': 'jobs'}},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': '--allow-path'}},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': digest + ' --allow-table jobs'}},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs --allow-table x': digest}},
            {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': 1}},
            {'schemaAllowlistVersion': 1, 'objects': {}},
            {'schemaAllowlistVersion': 1, 'objects': ['table:jobs=' + digest]},
            {'schemaAllowlistVersion': 2, 'objects': {'table:jobs': digest}},
            {'objects': {'table:jobs': digest}},
            ['--allow-table', 'jobs'],
        ]
        census = self.census()[1]
        for index, document in enumerate(broader):
            path = self.save('broad-%d.json' % index, document)
            with self.assertRaises(SystemExit, msg=document):
                self.compare(census, census, '--schema-allowlist', str(path))
        legacy_args = self.base / 'startup-allowlist.args'
        legacy_args.write_text('--allow-object table:jobs=%s --allow-table jobs\n' % digest)
        with self.assertRaises(SystemExit):
            self.compare(census, census, '--schema-allowlist', str(legacy_args))

    def test_conflicting_object_allowances_refuse(self):
        census = self.census()[1]
        path = self.save('allow.json', {'schemaAllowlistVersion': 1, 'objects': {'table:jobs': 'a' * 64}})
        with self.assertRaises(SystemExit):
            self.compare(census, census, '--schema-allowlist', str(path), '--allow-object', 'table:jobs=' + 'b' * 64)

    def add_marker(self, rows=((1, 1),), ddl=None):
        from processing.authority_lease import MARKER_DDL
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute(ddl or MARKER_DDL)
        db.executemany('INSERT INTO coordinator_authority_lease VALUES (?,?)', rows)
        db.commit()
        db.close()

    def test_marker_constant_matches_the_coordinator(self):
        from processing.authority_lease import MARKER_DDL
        self.assertEqual(state_census.LEASE_MARKER_DDL, MARKER_DDL)

    def test_exact_lease_marker_is_allowed_only_with_the_flag(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate()
        self.add_marker()
        after = self.census()[1]
        status, result = self.compare(before, after, *self.allowances())
        self.assertEqual(status, 1)
        self.assertIn({'table': 'coordinator_authority_lease', 'change': 'added'}, result['database'])
        status, result = self.compare(before, after, *self.allowances(), '--allow-lease-marker')
        self.assertEqual(status, 0, result)

    def test_lease_marker_flag_requires_the_marker(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate()
        status, result = self.compare(before, self.census()[1], *self.allowances(), '--allow-lease-marker')
        self.assertEqual(status, 1)
        self.assertIn({'object': 'table:coordinator_authority_lease', 'change': 'leaseMarkerMissing'},
                      result['database'])

    def test_lease_marker_must_be_exact_ddl_and_single_row(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate()
        self.add_marker(rows=())
        status, result = self.compare(before, self.census()[1], *self.allowances(), '--allow-lease-marker')
        self.assertEqual(status, 1)
        self.assertIn({'table': 'coordinator_authority_lease', 'change': 'leaseMarkerInvalid'}, result['database'])
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('DROP TABLE coordinator_authority_lease')
        db.commit()
        db.close()
        self.add_marker(rows=((1, 1), (2, 7)),
                        ddl='CREATE TABLE coordinator_authority_lease (singleton INTEGER, required INTEGER)')
        status, result = self.compare(before, self.census()[1], *self.allowances(), '--allow-lease-marker')
        self.assertEqual(status, 1)
        self.assertIn({'object': 'table:coordinator_authority_lease'}, result['database'])
        self.assertIn({'table': 'coordinator_authority_lease', 'change': 'leaseMarkerInvalid'}, result['database'])

    def test_lease_marker_flag_relaxes_nothing_else(self):
        self.empty_viewer_tasks()
        before = self.census()[1]
        self.migrate("CREATE TABLE other_new (x TEXT)", "INSERT INTO viewer_reservations VALUES ('x')")
        self.add_marker()
        status, result = self.compare(before, self.census()[1], *self.allowances(), '--allow-lease-marker')
        self.assertEqual(status, 1)
        changes = {item.get('table'): item.get('change') for item in result['database'] if 'table' in item}
        self.assertEqual(changes, {'other_new': 'added', 'viewer_reservations': 'newTableNotEmpty'})

    def test_preexisting_marker_is_validated_even_when_unchanged(self):
        fixed = ['--allow-path', self.DB_FIELDS, '--allow-path', '.:mtime', '--allow-lease-marker']
        cases = [
            ('empty exact marker', None, ()),
            ('malformed marker', 'CREATE TABLE coordinator_authority_lease (singleton INTEGER, required INTEGER)',
             ((1, 1),)),
            ('malformed two-row marker', 'CREATE TABLE coordinator_authority_lease (singleton INTEGER, required INTEGER)',
             ((1, 1), (2, 1))),
        ]
        for label, ddl, rows in cases:
            db = sqlite3.connect(self.root / 'jobs.sqlite3')
            db.execute('DROP TABLE IF EXISTS coordinator_authority_lease')
            db.commit()
            db.close()
            self.add_marker(rows=rows, ddl=ddl)
            same = self.census()[1]
            status, result = self.compare(same, same, *fixed)
            self.assertEqual(status, 1, label)
            self.assertTrue([d for d in result['database'] if d.get('change') in ('leaseMarkerSchemaInvalid', 'leaseMarkerInvalid')],
                            (label, result['database']))

    def test_preexisting_valid_marker_passes_unchanged(self):
        self.add_marker()
        same = self.census()[1]
        status, result = self.compare(same, same, '--allow-lease-marker')
        self.assertEqual(status, 0, result)

    def test_existing_marker_must_stay_unchanged(self):
        self.empty_viewer_tasks()
        self.add_marker()
        before = self.census()[1]
        db = sqlite3.connect(self.root / 'jobs.sqlite3')
        db.execute('DELETE FROM coordinator_authority_lease')
        db.commit()
        db.close()
        status, result = self.compare(before, self.census()[1], '--allow-path', self.DB_FIELDS,
                                      '--allow-path', '.:mtime', '--allow-lease-marker')
        self.assertEqual(status, 1)
        self.assertIn({'table': 'coordinator_authority_lease', 'change': 'contentChanged'}, result['database'])

    def test_refuses_wrong_key_and_version(self):
        with self.assertRaises(SystemExit):
            _run(['census', str(self.root)], key='not-a-key')
        before = self.census()[1]
        old = dict(before, schemaVersion=2)
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
