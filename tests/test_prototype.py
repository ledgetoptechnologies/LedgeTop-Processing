from concurrent.futures import ThreadPoolExecutor
import tempfile
import unittest

from processing.prototype import Queue, Conflict, PROFILE


def submission(identifier, environment="staging"):
    return {"requestId": identifier, "environment": environment, "profile": PROFILE}


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.queue = Queue(self.directory.name)

    def tearDown(self):
        self.queue.close()
        self.directory.cleanup()

    def test_production_precedes_staging_and_completion_unlocks_slot(self):
        staging, _ = self.queue.submit(submission("first"))
        production, _ = self.queue.submit(submission("second", "production"))
        claim = self.queue.claim({"workerId": "worker"})
        self.assertEqual(claim["id"], production["id"])
        self.assertIsNone(self.queue.claim({"workerId": "another"}))
        payload = {"claimToken": claim["claimToken"], "status": "succeeded", "result": {"trees": 3}}
        self.queue.complete(claim["id"], payload)
        self.queue.complete(claim["id"], payload)
        self.assertEqual(self.queue.claim({"workerId": "worker"})["id"], staging["id"])

    def test_concurrent_claims_have_only_one_winner(self):
        self.queue.submit(submission("one"))
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.queue.claim({"workerId": "worker"}), range(8)))
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_idempotency_conflicts_and_immutable_environment(self):
        original, created = self.queue.submit(submission("same"))
        retried, created_again = self.queue.submit(submission("same"))
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(original["id"], retried["id"])
        with self.assertRaises(Conflict):
            self.queue.submit(submission("same", "production"))

    def test_restart_holds_running_job_and_never_replays(self):
        self.queue.submit(submission("running", "production"))
        claim = self.queue.claim({"workerId": "worker"})
        self.queue.submit(submission("waiting"))
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.assertEqual(self.queue.get(claim["id"])["status"], "held")
        self.assertIsNone(self.queue.claim({"workerId": "worker"}))
        with self.assertRaises(Conflict):
            self.queue.complete(claim["id"], {"claimToken": claim["claimToken"], "status": "succeeded", "result": {}})

    def test_stale_completion_cannot_release_slot(self):
        self.queue.submit(submission("one"))
        claim = self.queue.claim({"workerId": "worker"})
        with self.assertRaises(Conflict):
            self.queue.complete(claim["id"], {"claimToken": "incorrect", "status": "failed", "result": {}})
        self.assertEqual(self.queue.get(claim["id"])["status"], "running")
        self.assertNotIn("claimToken", self.queue.get(claim["id"]))
        self.assertNotIn("claimToken", self.queue.list()[0])

    def test_second_coordinator_cannot_reset_live_claim(self):
        self.queue.submit(submission("one"))
        claim = self.queue.claim({"workerId": "worker"})
        with self.assertRaises(RuntimeError):
            Queue(self.directory.name)
        self.assertEqual(self.queue.get(claim["id"])["status"], "running")

    def test_arbitrary_profiles_paths_and_unknown_fields_rejected(self):
        for value in ({**submission("bad"), "profile": "arbitrary-command"},
                      {**submission("bad"), "inputPath": "/data/client"},
                      submission("../../escape")):
            with self.assertRaises(ValueError):
                self.queue.submit(value)

    def test_running_staging_is_not_preempted_by_new_production(self):
        self.queue.submit(submission("first"))
        claim = self.queue.claim({"workerId": "worker"})
        production, _ = self.queue.submit(submission("second", "production"))
        self.assertIsNone(self.queue.claim({"workerId": "other"}))
        self.queue.complete(claim["id"], {"claimToken": claim["claimToken"], "status": "failed", "result": {}})
        self.assertEqual(self.queue.claim({"workerId": "worker"})["id"], production["id"])

    def test_operator_hold_does_not_release_and_resolve_requires_stop_attestation(self):
        self.queue.submit(submission("first"))
        claim = self.queue.claim({"workerId": "worker"})
        self.queue.submit(submission("second"))
        self.queue.operator(claim["id"], "hold", {"note": "inspect failed worker"})
        self.assertIsNone(self.queue.claim({"workerId": "worker"}))
        with self.assertRaises(ValueError):
            self.queue.operator(claim["id"], "resolve", {"note": "not checked", "workerStopped": False})
        self.queue.operator(claim["id"], "resolve", {"note": "verified no worker remains", "workerStopped": True})
        self.assertEqual(self.queue.get(claim["id"])["status"], "failed")
        self.assertIsNotNone(self.queue.claim({"workerId": "worker"}))
