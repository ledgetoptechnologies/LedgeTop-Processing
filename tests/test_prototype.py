import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

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
        self.assertEqual(self.queue.get(claim['id'])['heldReason'], 'coordinator_restart')
        self.queue.complete(claim["id"], {"claimToken": claim["claimToken"], "status": "succeeded", "result": {}})
        self.assertIsNotNone(self.queue.claim({'workerId': 'worker'}))

    def test_authority_survives_restart_and_independent_queues_differ(self):
        authority = self.queue.authority_id
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.assertEqual(authority, self.queue.authority_id)
        with tempfile.TemporaryDirectory() as directory:
            other = Queue(directory)
            try:
                self.assertNotEqual(authority, other.authority_id)
            finally:
                other.close()

    def test_operator_can_veto_original_completion_after_restart(self):
        self.queue.submit(submission('restart-veto'))
        claim = self.queue.claim({'workerId': 'worker'})
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.queue.operator(claim['id'], 'hold', {'note': 'inspect original process'})
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.assertEqual(self.queue.get(claim['id'])['heldReason'], 'operator')
        with self.assertRaises(Conflict):
            self.queue.complete(claim['id'], {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {}})

    def test_completion_lost_reply_is_idempotent_after_restart(self):
        self.queue.submit(submission('lost-reply'))
        claim = self.queue.claim({'workerId': 'worker'})
        payload = {'claimToken': claim['claimToken'], 'status': 'succeeded', 'result': {'artifact': 'retained'}}
        first = self.queue.complete(claim['id'], payload)
        self.queue.close()
        self.queue = Queue(self.directory.name)
        self.assertEqual(first, self.queue.complete(claim['id'], payload))
        with self.assertRaises(Conflict):
            self.queue.complete(claim['id'], {**payload, 'result': {}})

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

    def test_operator_cancel_is_queued_only_idempotent_and_preserves_fifo(self):
        cancelled, _ = self.queue.submit(submission("cancel-me"))
        next_job, _ = self.queue.submit(submission("next"))
        first = self.queue.operator(cancelled["id"], "cancel", {"note": "wrong node profile"})
        self.assertEqual(first["status"], "cancelled")
        self.assertEqual(first["result"]["operatorAction"], "cancel")
        self.assertEqual(self.queue.operator(cancelled["id"], "cancel", {"note": "retry"}), first)
        self.assertEqual(self.queue.claim({"workerId": "worker"})["id"], next_job["id"])
        with self.assertRaises(Conflict):
            self.queue.operator(next_job["id"], "cancel", {"note": "already claimed"})

    def test_cancel_and_claim_race_has_one_terminal_winner(self):
        for index in range(20):
            job, _ = self.queue.submit(submission(f"cancel-claim-race-{index}"))
            barrier = Barrier(3)

            def cancel():
                barrier.wait()
                try:
                    return ("cancelled", self.queue.operator(
                        job["id"], "cancel", {"note": "concurrent operator action"}))
                except Conflict:
                    return ("conflict", None)

            def claim():
                barrier.wait()
                return ("claimed", self.queue.claim({"workerId": "worker"}))

            with ThreadPoolExecutor(max_workers=2) as pool:
                cancel_future = pool.submit(cancel)
                claim_future = pool.submit(claim)
                barrier.wait()
                cancel_outcome, cancel_row = cancel_future.result()
                _, claimed = claim_future.result()

            if cancel_outcome == "cancelled":
                self.assertEqual(cancel_row["status"], "cancelled")
                self.assertIsNone(claimed)
            else:
                self.assertIsNotNone(claimed)
                self.assertEqual(claimed["id"], job["id"])
                self.assertEqual(self.queue.get(job["id"])["status"], "running")
                self.queue.complete(job["id"], {
                    "claimToken": claimed["claimToken"], "status": "failed", "result": {}})
