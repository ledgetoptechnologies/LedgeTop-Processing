import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.source_spool import SourceSpool, SourceSpoolError


MOUNT_ID = "a" * 64
JOB_ID = "00000000-0000-4000-8000-000000000001"
OBJECT_ID = "00000000-0000-4000-8000-000000000002"


class SourceSpoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "spool"
        self.root.mkdir(mode=0o700)
        if __import__('os').name != "nt":
            self.root.chmod(0o700)
        marker = self.root / ".ltds-source-spool-id"
        marker.write_text(MOUNT_ID, encoding="ascii")
        if __import__('os').name != "nt":
            marker.chmod(0o600)
        self.mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
        self.mount_patch.start()
        self.addCleanup(self.mount_patch.stop)
        self.spool = SourceSpool(self.root, mount_id=MOUNT_ID,
            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)

    def put(self, body=b"abc", *, obj=OBJECT_ID):
        return self.spool.receive_object(job_id=JOB_ID, generation=1, object_id=obj,
            byte_size=len(body), sha256=hashlib.sha256(body).hexdigest(), reader=io.BytesIO(body))

    def test_streamed_object_is_fsynced_published_and_exact_retry_survives_restart(self):
        receipt, replay = self.put()
        self.assertFalse(replay)
        self.assertEqual(receipt["mountId"], MOUNT_ID)
        self.assertEqual(receipt["byteSize"], 3)
        self.assertTrue(self.spool.verify_object(job_id=JOB_ID, generation=1,
            object_id=OBJECT_ID, byte_size=3, sha256=hashlib.sha256(b"abc").hexdigest()))
        restarted = SourceSpool(self.root, mount_id=MOUNT_ID,
            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)
        replayed, was_replay = restarted.receive_object(job_id=JOB_ID, generation=1,
            object_id=OBJECT_ID, byte_size=3, sha256=hashlib.sha256(b"abc").hexdigest(),
            reader=io.BytesIO(b"abc"))
        self.assertTrue(was_replay)
        self.assertEqual(replayed, receipt)

    def test_changed_bytes_under_same_object_identity_conflict(self):
        self.put()
        changed = b"xyz"
        with self.assertRaisesRegex(SourceSpoolError, "source_object_conflict"):
            self.put(changed)

    def _crash_after_publish_link(self, body=b"abc"):
        key = self.spool.storage_key(JOB_ID, 1, OBJECT_ID)
        target = self.root / key
        temporary = target.parent / ("." + target.name + ".part")
        real_unlink = os.unlink

        class SimulatedProcessCrash(BaseException):
            pass

        def crash_at_partial_unlink(path, *args, **kwargs):
            if Path(path) == temporary:
                raise SimulatedProcessCrash()
            return real_unlink(path, *args, **kwargs)

        with patch("processing.source_spool.os.unlink", side_effect=crash_at_partial_unlink):
            with self.assertRaises(SimulatedProcessCrash):
                self.put(body)
        return target, temporary

    def test_post_link_crash_pair_is_recovered_on_reconcile_and_retry(self):
        target, temporary = self._crash_after_publish_link()
        self.assertEqual(target.stat().st_ino, temporary.stat().st_ino)
        self.assertEqual(target.stat().st_nlink, 2)
        restarted = SourceSpool(self.root, mount_id=MOUNT_ID,
            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)
        digest = hashlib.sha256(b"abc").hexdigest()
        found = restarted.audit_job(job_id=JOB_ID, generation=1,
            expected_objects=[{"id": OBJECT_ID, "byteSize": 3, "sha256": digest}])
        self.assertEqual(found[OBJECT_ID]["sha256"], digest)
        self.assertFalse(temporary.exists())
        self.assertEqual(target.stat().st_nlink, 1)
        _, replay = restarted.receive_object(job_id=JOB_ID, generation=1,
            object_id=OBJECT_ID, byte_size=3, sha256=digest, reader=io.BytesIO(b"abc"))
        self.assertTrue(replay)

    def test_retry_cannot_bypass_failed_publication_directory_fsync(self):
        key = self.spool.storage_key(JOB_ID, 1, OBJECT_ID)
        target = self.root / key
        real_fsync = SourceSpool._fsync_directory

        def fail_after_publication(path):
            if Path(path) == target.parent and target.exists():
                raise OSError("injected directory fsync failure")
            return real_fsync(path)

        with patch.object(SourceSpool, "_fsync_directory", side_effect=fail_after_publication):
            with self.assertRaisesRegex(SourceSpoolError, "spool_write_failed"):
                self.put()
            self.assertTrue(target.is_file())
            self.assertEqual(target.stat().st_nlink, 1)
            with self.assertRaises(OSError):
                self.put()
        _, replay = self.put()
        self.assertTrue(replay)

    def test_post_link_pair_with_unexpected_third_hardlink_is_held(self):
        target, temporary = self._crash_after_publish_link()
        extra = Path(self.temp.name) / "unexpected-hardlink"
        os.link(target, extra)
        restarted = SourceSpool(self.root, mount_id=MOUNT_ID,
            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)
        digest = hashlib.sha256(b"abc").hexdigest()
        with self.assertRaisesRegex(SourceSpoolError, "source_object_conflict"):
            restarted.audit_job(job_id=JOB_ID, generation=1,
                expected_objects=[{"id": OBJECT_ID, "byteSize": 3, "sha256": digest}])
        with self.assertRaisesRegex(SourceSpoolError, "source_object_conflict"):
            restarted.receive_object(job_id=JOB_ID, generation=1,
                object_id=OBJECT_ID, byte_size=3, sha256=digest, reader=io.BytesIO(b"abc"))
        self.assertTrue(extra.exists())
        self.assertTrue(temporary.exists())
        self.assertEqual(target.stat().st_nlink, 3)

    def test_mismatched_partial_and_target_inodes_are_held(self):
        target, temporary = self._crash_after_publish_link()
        target.unlink()
        target.write_bytes(b"abc")
        restarted = SourceSpool(self.root, mount_id=MOUNT_ID,
            safety_floor_bytes=0, max_object_bytes=1024, chunk_bytes=2)
        digest = hashlib.sha256(b"abc").hexdigest()
        with self.assertRaisesRegex(SourceSpoolError, "source_object_conflict"):
            restarted.audit_job(job_id=JOB_ID, generation=1,
                expected_objects=[{"id": OBJECT_ID, "byteSize": 3, "sha256": digest}])
        self.assertTrue(target.exists())
        self.assertTrue(temporary.exists())

    def test_short_and_wrong_digest_streams_never_publish_or_leave_partial(self):
        for body, size, digest in ((b"a", 2, hashlib.sha256(b"ab").hexdigest()),
                                   (b"ab", 2, "0" * 64)):
            with self.subTest(body=body):
                obj = OBJECT_ID[:-1] + ("3" if body == b"a" else "4")
                with self.assertRaises(SourceSpoolError):
                    self.spool.receive_object(job_id=JOB_ID, generation=1, object_id=obj,
                        byte_size=size, sha256=digest, reader=io.BytesIO(body))
                key = self.spool.storage_key(JOB_ID, 1, obj)
                self.assertFalse((self.root / key).exists())
                self.assertFalse((self.root / key).parent.joinpath("." + Path(key).name + ".part").exists())

    def test_capacity_floor_and_object_limit_refuse_before_reading(self):
        class Reader:
            def read(self, _size):
                raise AssertionError("body must not be read")
        with patch("processing.source_spool.shutil.disk_usage", return_value=type(
                "Usage", (), {"total": 10, "used": 9, "free": 1})()):
            with self.assertRaisesRegex(SourceSpoolError, "spool_capacity_unavailable"):
                self.spool.receive_object(job_id=JOB_ID, generation=1, object_id=OBJECT_ID,
                    byte_size=2, sha256=hashlib.sha256(b"ab").hexdigest(), reader=Reader())
        with patch("processing.source_spool.shutil.disk_usage", return_value=type(
                "Usage", (), {"total": 100, "used": 0, "free": 100})()):
            with self.assertRaisesRegex(SourceSpoolError, "spool_capacity_unavailable"):
                self.spool.receive_object(job_id=JOB_ID, generation=1, object_id=OBJECT_ID[:-1] + "5",
                    byte_size=2, sha256=hashlib.sha256(b"ab").hexdigest(), reader=Reader(),
                    reserved_bytes=99)
        with self.assertRaisesRegex(SourceSpoolError, "source_object_too_large"):
            self.spool.receive_object(job_id=JOB_ID, generation=1, object_id=OBJECT_ID,
                byte_size=2048, sha256="0" * 64, reader=Reader())

    def test_mount_marker_or_identity_change_fails_closed(self):
        marker = self.root / ".ltds-source-spool-id"
        marker.write_text("b" * 64, encoding="ascii")
        with self.assertRaisesRegex(SourceSpoolError, "spool_mount_changed"):
            self.spool.available_bytes()
        marker.write_text(MOUNT_ID, encoding="ascii")
        with patch("processing.source_spool.os.path.ismount", return_value=False):
            with self.assertRaisesRegex(SourceSpoolError, "spool_mount_unavailable"):
                self.spool.available_bytes()

    def test_spool_subfolder_uses_parent_dataset_mount_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            mount = Path(directory) / "dataset"
            root = mount / "processing-spool"
            mount.mkdir(mode=0o700)
            root.mkdir(mode=0o700)
            marker = mount / ".ltds-source-spool-id"
            marker.write_text(MOUNT_ID, encoding="ascii")
            if os.name != "nt":
                marker.chmod(0o600)
            with patch("processing.source_spool.os.path.ismount", side_effect=lambda path: Path(path) == mount):
                spool = SourceSpool(root, mount_path=mount, mount_id=MOUNT_ID,
                    safety_floor_bytes=0)
                receipt, replay = spool.receive_object(job_id=JOB_ID, generation=1,
                    object_id=OBJECT_ID, byte_size=3, sha256=hashlib.sha256(b"abc").hexdigest(),
                    reader=io.BytesIO(b"abc"))
                self.assertFalse(replay)
                self.assertTrue((root / receipt["storageKey"]).is_file())

    def test_unknown_or_symlink_storage_entry_is_not_followed(self):
        outside = Path(self.temp.name) / "outside"
        outside.write_bytes(b"secret")
        key = self.spool.storage_key(JOB_ID, 1, OBJECT_ID)
        target = self.root / key
        target.parent.mkdir(parents=True, mode=0o700)
        try:
            target.symlink_to(outside)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks unavailable")
        with self.assertRaisesRegex(SourceSpoolError, "source_object_conflict"):
            self.put()
        self.assertEqual(outside.read_bytes(), b"secret")


if __name__ == "__main__":
    unittest.main()
