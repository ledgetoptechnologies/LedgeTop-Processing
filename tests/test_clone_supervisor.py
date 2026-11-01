"""Synthetic clone lifecycle; read-only mounts and host policy are mocked."""
import hashlib
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from ops.clone_supervisor import CloneHeld, CloneSupervisor
from ops.outer_authority_guard import OuterAuthorityGuard
from ops.pr197_operation_fence import OperationFence

AUTHORITY = '12345678-1234-1234-1234-123456789abc'


@unittest.skipUnless(os.name == 'posix' and Path('/proc/locks').exists(), 'Linux flock required')
class CloneSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve(); self.source = self.root / 'source'; self.source.mkdir()
        self.leases = self.root / 'leases'; self.leases.mkdir(mode=0o700)
        self.outer = self.root / 'outer'; self.outer.mkdir(mode=0o700)
        self.out = self.root / 'out'; self.out.mkdir(mode=0o700)
        self.destination = self.out / 'clone'
        (self.source / 'coordinator.lock').touch(mode=0o600)
        (self.leases / (AUTHORITY + '.lease')).touch(mode=0o600)
        (self.outer / (AUTHORITY + '.outer-lock')).touch(mode=0o600)
        with sqlite3.connect(self.source / 'jobs.sqlite3') as db:
            db.executescript('CREATE TABLE jobs(id INTEGER PRIMARY KEY,status TEXT);'
                             'CREATE TABLE viewer_tasks(uuid TEXT,stage TEXT,removed INTEGER);'
                             'CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT);'
                             "INSERT INTO jobs VALUES(1,'succeeded');"
                             "INSERT INTO viewer_tasks VALUES('synthetic','succeeded',0);")
            db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (AUTHORITY,))
            objects = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
        schema = hashlib.sha256(json.dumps(objects, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
        task = self.source / 'viewer-tasks' / 'synthetic'; task.mkdir(parents=True)
        (task / 'photo.jpg').write_bytes(b'synthetic')
        for target, kwargs in (
            ('ops.pr197_stopped_census._mount', {}), ('ops.clone_supervisor._mount', {}),
            ('ops.stopped_viewer_backup._require_isolated_mounts', {}),
            ('ops.pr197_stopped_census.SCHEMAS', {'new': frozenset([schema])}),
            ('ops.stopped_viewer_backup.os.statvfs', {})):
            p = patch(target, **kwargs); obj = p.start(); self.addCleanup(p.stop)
            if target.endswith('statvfs'): obj.return_value.f_flag = os.ST_RDONLY

    def test_actual_copy_and_hold_then_detect_changed_clone(self):
        with OuterAuthorityGuard(self.outer, AUTHORITY, enabled=True) as outer, \
             OperationFence(self.source, self.leases, AUTHORITY, enabled=True) as fence:
            clone = CloneSupervisor(outer, fence, self.destination, bytes(range(32)), 'a'*32,
                                    host_policy_confirmed=True)
            try:
                clone.copy()
                self.assertTrue(outer.pending.exists())
                code = ('import fcntl,os,sys\nfd=os.open(sys.argv[1],os.O_RDONLY)\n'
                        'try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)\n'
                        'except BlockingIOError: sys.exit(7)\n')
                other = subprocess.run([sys.executable, '-c', code, str(self.destination / 'coordinator.lock')],
                                       capture_output=True, timeout=3)
                self.assertEqual(other.returncode, 7)
                self.assertEqual(clone.inspect_mount(self.destination)['cutoverAuthorized'], False)
                (self.destination / 'viewer-tasks' / 'synthetic' / 'photo.jpg').write_bytes(b'changed')
                with self.assertRaisesRegex(CloneHeld, 'clone_census_changed'):
                    clone.inspect_mount(self.destination)
                self.assertTrue(outer.pending.exists())
                fence.check()
            finally:
                clone.close()

    def test_different_mount_and_intent_change_hold(self):
        with OuterAuthorityGuard(self.outer, AUTHORITY, enabled=True) as outer, \
             OperationFence(self.source, self.leases, AUTHORITY, enabled=True) as fence:
            clone = CloneSupervisor(outer, fence, self.destination, bytes(range(32)), 'b'*32,
                                    host_policy_confirmed=True)
            try:
                clone.copy()
                with self.assertRaisesRegex(CloneHeld, 'clone_mount_not_same_root'):
                    clone.inspect_mount(self.source)
                outer.pending.write_bytes(b'changed')
                with self.assertRaises(CloneHeld):
                    clone.inspect_mount(self.destination)
                fence.check()
            finally:
                clone.close()

    def test_confirmation_required_before_state(self):
        with OuterAuthorityGuard(self.outer, AUTHORITY, enabled=True) as outer, \
             OperationFence(self.source, self.leases, AUTHORITY, enabled=True) as fence:
            with self.assertRaises(CloneHeld):
                CloneSupervisor(outer, fence, self.destination, bytes(range(32)), 'c'*32)
            self.assertFalse(outer.pending.exists()); self.assertFalse(self.destination.exists())

    def test_lock_description_must_remain_exclusive(self):
        with OuterAuthorityGuard(self.outer, AUTHORITY, enabled=True) as outer, \
             OperationFence(self.source, self.leases, AUTHORITY, enabled=True) as fence:
            clone = CloneSupervisor(outer, fence, self.destination, bytes(range(32)), 'd'*32,
                                    host_policy_confirmed=True)
            replacement = None
            try:
                clone.copy()
                fcntl.flock(clone.fd, fcntl.LOCK_UN)
                replacement = os.open(self.destination / 'coordinator.lock', os.O_RDONLY)
                fcntl.flock(replacement, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(CloneHeld, 'exclusive_description_lock_required'):
                    clone.inspect_mount(self.destination)
                self.assertTrue(outer.pending.exists())
            finally:
                if replacement is not None: os.close(replacement)
                clone.close()
