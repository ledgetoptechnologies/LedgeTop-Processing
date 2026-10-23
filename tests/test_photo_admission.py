import tempfile
import unittest

from processing.prototype import PHOTO_PROFILE, PROFILE, Queue


class PhotoAdmissionTests(unittest.TestCase):
    def test_photo_profile_is_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as root:
            queue = Queue(root)
            try:
                with self.assertRaises(ValueError):
                    queue.submit({"requestId": "photo", "environment": "staging", "profile": PHOTO_PROFILE})
            finally:
                queue.close()

    def test_opt_in_is_staging_only_and_legacy_worker_never_claims_it(self):
        with tempfile.TemporaryDirectory() as root:
            queue = Queue(root, photo_smoke_enabled=True)
            try:
                with self.assertRaises(ValueError):
                    queue.submit({"requestId": "photo-prod", "environment": "production", "profile": PHOTO_PROFILE})
                job, _ = queue.submit({"requestId": "photo", "environment": "staging", "profile": PHOTO_PROFILE})
                self.assertIsNone(queue.claim({"workerId": "legacy-treedar"}))
                claimed = queue.claim({"workerId": "photo", "profiles": [PHOTO_PROFILE]})
                self.assertEqual(claimed["id"], job["id"])
                self.assertEqual(claimed["profile"], PHOTO_PROFILE)
            finally:
                queue.close()

    def test_profile_filter_cannot_bypass_production(self):
        with tempfile.TemporaryDirectory() as root:
            queue = Queue(root, photo_smoke_enabled=True)
            try:
                queue.submit({"requestId": "photo", "environment": "staging", "profile": PHOTO_PROFILE})
                production, _ = queue.submit({"requestId": "prod", "environment": "production", "profile": PROFILE})
                self.assertIsNone(queue.claim({"workerId": "photo", "profiles": [PHOTO_PROFILE]}))
                self.assertEqual(queue.claim({"workerId": "treedar"})["id"], production["id"])
            finally:
                queue.close()
