import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from processing import legacy_fence_enforcement as fence
from processing.legacy_state_gate import acquire
from processing.prototype import Queue
from processing.config import Config
from processing.app import make_handler


@unittest.skipUnless(os.name == 'posix', 'POSIX flock required')
class LegacyFenceVectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.state = self.root / 'state'
        queue = Queue(self.state); queue.close()
        self.mounts = self.root / 'mounts'; self.mounts.mkdir()
        self.paths = []
        for slot in range(2):
            directory = self.mounts / str(slot); directory.mkdir()
            path = directory / 'coordinator.lock'; path.touch()
            shutil.copy2(self.state / 'jobs.sqlite3', directory / 'jobs.sqlite3')
            self.paths.append(path)

    def enroll(self):
        fence.enroll_vector_offline(self.state, self.paths)

    def enforce(self):
        return fence.enforce(self.state, required=True, vector_root=self.mounts)

    def assert_free(self):
        for path in self.paths:
            fd = acquire(path); os.close(fd)

    def test_two_locks_are_held_and_candidate_queue_lock_remains_separate(self):
        self.enroll()
        original = fence.enforce
        with patch.object(fence, 'enforce', side_effect=lambda state, required: original(state,required=required,vector_root=self.mounts)):
            handler = make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.state),legacy_fence_required=True))
            try:
                for path in self.paths:
                    with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'): acquire(path)
                with self.assertRaises(RuntimeError): Queue(self.state)
            finally: handler.prototype_queue.close()
        self.assert_free()

    def test_each_contended_lock_releases_other_acquisitions(self):
        self.enroll()
        for path in self.paths:
            fd = acquire(path)
            try:
                with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'): self.enforce()
            finally: os.close(fd)
            self.assert_free()

    def test_acquisition_order_is_device_inode_not_mount_slot(self):
        # Assign slots in the reverse of physical identity order.
        self.paths.sort(key=lambda path:(path.stat().st_dev,path.stat().st_ino),reverse=True)
        # Runtime mount numbering follows slot order without moving the files.
        rows=[(slot,str(path.stat().st_dev),str(path.stat().st_ino),
               fence._authority(self.state)) for slot,path in enumerate(self.paths)]
        order=[]; actual=fence.acquire
        def capture(path):
            order.append(Path(path)); return actual(path)
        with patch.object(fence,'acquire',side_effect=capture):
            fds=fence._vector_acquire(self.state,rows,self.paths)
        fence.close_fence(fds)
        self.assertEqual(order,list(reversed(self.paths)))
        self.assert_free()

    def test_failed_later_startup_releases_every_lock(self):
        self.enroll(); original=fence.enforce
        with patch.object(fence,'enforce',side_effect=lambda state,required:original(state,required=required,vector_root=self.mounts)):
            with patch('processing.viewer_service.ViewerService',side_effect=RuntimeError('synthetic')):
                with self.assertRaisesRegex(RuntimeError,'synthetic'):
                    make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.state),viewer_enabled=True))
        self.assert_free()

    def test_replaced_missing_and_duplicate_identities_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError,'vector_invalid'):
            fence.enroll_vector_offline(self.state,[self.paths[0],self.paths[0]])
        self.enroll(); path=self.paths[1]; replacement=path.with_name('replacement'); replacement.touch()
        os.replace(replacement,path)
        with self.assertRaisesRegex(RuntimeError,'identity_mismatch'): self.enforce()
        path.unlink()
        with self.assertRaisesRegex(RuntimeError,'unavailable'): self.enforce()
        fd=acquire(self.paths[0]);os.close(fd)

    def test_cross_process_legacy_owner_blocks_and_candidate_release_unblocks(self):
        self.enroll()
        code = "from processing.legacy_state_gate import acquire; import sys,time; f=acquire(sys.argv[1]); print('ready',flush=True); time.sleep(30)"
        for path in self.paths:
            child=subprocess.Popen([sys.executable,'-c',code,str(path)],stdout=subprocess.PIPE,text=True)
            try:
                self.assertEqual(child.stdout.readline().strip(),'ready')
                with self.assertRaisesRegex(RuntimeError,'legacy_fence_held'): self.enforce()
            finally:
                child.terminate(); child.wait(timeout=5); child.stdout.close()
            self.assert_free()
        fds=self.enforce()
        try:
            for path in self.paths:
                result=subprocess.run([sys.executable,'-c',code,str(path)],capture_output=True,text=True,timeout=5)
                self.assertNotEqual(result.returncode,0); self.assertIn('legacy_fence_held',result.stderr)
        finally:fence.close_fence(fds)
        self.assert_free()

    def test_marker_validation_and_enrollment_same_candidate(self):
        with self.assertRaisesRegex(RuntimeError,'same_candidate_lock'):
            fence.enroll_vector_offline(self.state,[self.state/'coordinator.lock',self.paths[1]])
        self.enroll()
        with sqlite3.connect(self.state/'jobs.sqlite3') as db:
            db.execute('UPDATE legacy_fence_vector SET device=?,inode=? WHERE slot=1',
                       (str(self.paths[0].stat().st_dev),str(self.paths[0].stat().st_ino)))
        with self.assertRaisesRegex(RuntimeError,'vector_invalid'):self.enforce()
        self.assert_free()

    def test_wrong_legacy_authority_fails_and_releases_other_lock(self):
        self.enroll()
        with sqlite3.connect(self.paths[1].parent/'jobs.sqlite3') as db:
            db.execute("UPDATE coordinator_identity SET authority_id='00000000-0000-4000-8000-000000000000'")
        with self.assertRaisesRegex(RuntimeError,'authority_mismatch'):self.enforce()
        self.assert_free()

    def test_vector_marker_required_when_configuration_requires_fence(self):
        with self.assertRaisesRegex(RuntimeError,'marker_required'):
            fence.enforce(self.state,required=True,vector_root=self.mounts)
