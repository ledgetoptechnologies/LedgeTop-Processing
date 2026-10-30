"""Narrow offline retained-input backup checks; not update qualification."""
import hashlib
import contextlib
import io
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from ops.incremental_coordinator_binding import IncrementalHeld, retained_viewer_inventory
from ops import update_reference_coordinator as updater


class GateTests(unittest.TestCase):
    def test_retained_path_is_disabled(self):
        with self.assertRaisesRegex(updater.Stopped, 'retained_backup_not_qualified'):
            updater.backup_script(incremental_profile=True, retained_viewer=True)


@unittest.skipUnless(os.name == 'posix', 'requires Linux no-follow file descriptors')
class RetainedTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'viewer-tasks').mkdir(mode=0o700)
        (self.root / 'viewer-spool').mkdir(mode=0o700)
        (self.root / 'coordinator.lock').touch()
        self.directory = self.root / 'viewer-tasks' / 'synthetic-1'
        self.directory.mkdir(mode=0o700)
        self.file = self.directory / 'input-a'
        self.file.write_bytes(b'synthetic')
        self.db = sqlite3.connect(self.root / 'jobs.sqlite3')
        self.db.executescript('CREATE TABLE viewer_tasks(uuid TEXT,directory TEXT,archive_path TEXT,removed INTEGER); CREATE TABLE viewer_files(uuid TEXT,path TEXT,bytes INTEGER,sha256 TEXT); CREATE TABLE viewer_incarnations(uuid TEXT);')
        self.db.executescript('CREATE TABLE jobs(seq INTEGER,status TEXT); CREATE TABLE coordinator_identity(singleton INTEGER,authority_id TEXT);')
        self.db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (updater.AUTHORITY,))
        self.db.execute('INSERT INTO viewer_tasks VALUES(?,?,NULL,0)', ('task', 'synthetic-1'))
        self.db.execute('INSERT INTO viewer_files VALUES(?,?,?,?)', ('task', 'input-a', 9, hashlib.sha256(b'synthetic').hexdigest()))
        self.db.commit()
        self.addCleanup(self.db.close)

    def census(self):
        return retained_viewer_inventory(self.root, uid=os.geteuid())

    def test_exact_bytes_and_changed_file(self):
        self.assertEqual(len(self.census()), 2)
        self.file.write_bytes(b'altered!!')
        with self.assertRaises(IncrementalHeld): self.census()

    def test_symlink_and_extra_file_refused(self):
        self.file.unlink(); self.file.symlink_to('/etc/passwd')
        with self.assertRaises(IncrementalHeld): self.census()

    def test_path_escape_and_untracked_file_refused(self):
        self.db.execute("UPDATE viewer_files SET path='../outside'"); self.db.commit()
        with self.assertRaises(IncrementalHeld): self.census()
        self.db.execute("UPDATE viewer_files SET path='input-a'"); self.db.commit()
        (self.directory/'extra').write_bytes(b'x')
        with self.assertRaises(IncrementalHeld): self.census()

    def test_backup_template_is_connected_only_when_test_gate_enabled(self):
        with patch.object(updater, 'RETAINED_VIEWER_BACKUP_QUALIFIED', True):
            code = updater.backup_script(incremental_profile=True, retained_viewer=True)
        self.assertIn("retained_viewer_inventory('/source')", code)
        self.assertIn('retained_viewer_inventory(target)==retained_before', code)
        self.assertIn('copy(source,target)', code)

    def test_real_backup_script_copies_and_checks_retained_bytes(self):
        with tempfile.TemporaryDirectory() as destination:
            with patch.object(updater, 'RETAINED_VIEWER_BACKUP_QUALIFIED', True):
                code = updater.backup_script(incremental_profile=True, retained_viewer=True)
            code = code.replace("'/source'", repr(str(self.root))).replace("'/backup'", repr(destination))
            code = code.replace('10001', str(os.geteuid()))
            with contextlib.redirect_stdout(io.StringIO()):
                exec(compile(code, '<isolated backup>', 'exec'), {})
            copied = Path(destination) / 'viewer-tasks' / 'synthetic-1' / 'input-a'
            self.assertEqual(copied.read_bytes(), b'synthetic')
            self.assertEqual(retained_viewer_inventory(destination, uid=os.geteuid()), self.census())
            copied.write_bytes(b'altered!!')
            with self.assertRaises(IncrementalHeld):
                retained_viewer_inventory(destination, uid=os.geteuid())

    def test_unsupported_retention_cases_hold(self):
        cases = (("UPDATE viewer_tasks SET archive_path='result.zip'", "UPDATE viewer_tasks SET archive_path=NULL"), ("UPDATE viewer_tasks SET removed=1", "UPDATE viewer_tasks SET removed=0"), ("INSERT INTO viewer_incarnations VALUES('task')", "DELETE FROM viewer_incarnations"))
        for statement, restore in cases:
            self.db.execute(statement); self.db.commit()
            with self.assertRaises(IncrementalHeld): self.census()
            self.db.execute(restore); self.db.commit()
        with tempfile.TemporaryDirectory() as external:
            os.link(self.file, Path(external) / 'other')
            with self.assertRaisesRegex(IncrementalHeld, 'retained_file_changed'): self.census()

    def test_real_backup_refuses_queued_job(self):
        self.db.execute("INSERT INTO jobs VALUES(1,'queued')"); self.db.commit()
        with tempfile.TemporaryDirectory() as destination:
            with patch.object(updater, 'RETAINED_VIEWER_BACKUP_QUALIFIED', True):
                code = updater.backup_script(incremental_profile=True, retained_viewer=True)
            code = code.replace("'/source'", repr(str(self.root))).replace("'/backup'", repr(destination))
            code = code.replace('10001', str(os.geteuid()))
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(AssertionError):
                exec(compile(code, '<isolated backup>', 'exec'), {})
