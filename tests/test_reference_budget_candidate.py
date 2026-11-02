"""Cross-language arithmetic check for Viewer proposal vectors (not runtime)."""
import hashlib
import json
from pathlib import Path
import unittest

from processing.reference_budget_candidate import plan, replay_recovery


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "contracts/reference-broker-budget-candidate/reference-range-budget-policy-vectors.json"
FIXTURE_SHA256 = "ca46a7e0aeda6cc6c945880b477b4799acc509d8a39f16f5603870022fc2ee68"


class ReferenceBudgetCandidateTests(unittest.TestCase):
    def setUp(self):
        self.raw = FIXTURE.read_bytes()
        self.fixture = json.loads(self.raw)

    def test_fixture_is_the_exact_viewer_git_blob_under_review(self):
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), FIXTURE_SHA256)
        self.assertEqual(self.fixture["status"], "synthetic-proposal-not-wire-contract")

    def test_all_plan_vectors_match_integer_arithmetic(self):
        policy = self.fixture["policy"]
        for vector in self.fixture["plans"]:
            with self.subTest(vector=vector["name"]):
                result = plan(policy, vector["objects"])
                self.assertEqual(result is not None, vector["accepted"])
                if result is not None:
                    for key in ("baseRequests", "requestBudget", "byteBudget"):
                        self.assertEqual(result[key], vector[key])

    def test_v1_zero_byte_objects_are_rejected(self):
        vector = next(v for v in self.fixture["plans"] if v["objects"][0]["bytes"] == 0)
        self.assertFalse(vector["accepted"])
        self.assertIsNone(plan(self.fixture["policy"], vector["objects"]))

    def test_ten_thousand_photo_plan_is_finite_and_under_proposed_caps(self):
        vector = next(v for v in self.fixture["plans"] if v["name"].startswith("ten thousand"))
        result = plan(self.fixture["policy"], vector["objects"])
        self.assertEqual(result, {
            "objectCount": 10_000,
            "totalBytes": 50_000_000_000,
            "baseRequests": 10_000,
            "requestBudget": 15_000,
            "byteBudget": 75_000_000_000,
        })

    def test_recovery_vectors_preserve_verified_offset_and_hold_uncertain_ranges(self):
        outcomes = replay_recovery(self.fixture["recovery"])
        self.assertEqual(
            [(item["result"], item["offset"], item["requests"], item["bytes"])
             for item in outcomes],
            [("charged", 0, 1, 10), ("verified", 10, 1, 10),
             ("verified", 10, 1, 10), ("charged", 10, 2, 20),
             ("hold", 10, 2, 20), ("hold", 10, 2, 20)],
        )

    def test_recovery_does_not_charge_a_duplicate_inflight_range(self):
        event = {"identity": "job-a:manifest-a", "range": "0-9"}
        outcomes = replay_recovery([
            dict(event, action="send", generation=1, grant="grant-a"),
            dict(event, action="send", generation=1, grant="grant-a"),
            dict(event, action="restart", generation=1, grant="grant-a"),
            dict(event, action="send", generation=2, grant="grant-b"),
        ])
        self.assertEqual([item["result"] for item in outcomes],
                         ["charged", "hold", "hold", "hold"])
        self.assertEqual([(item["requests"], item["bytes"]) for item in outcomes],
                         [(1, 10)] * 4)

    def test_unknown_restart_is_held_and_separate_input_has_separate_ledger(self):
        outcomes = replay_recovery([
            {"identity": "job-a:manifest-a", "range": "0-9", "action": "restart"},
            {"identity": "job-b:manifest-b", "range": "0-9", "action": "send"},
        ])
        self.assertEqual([(item["result"], item["requests"], item["bytes"])
                          for item in outcomes], [("hold", 0, 0), ("charged", 1, 10)])

    def test_boolean_and_unsafe_numbers_are_not_integers(self):
        policy = dict(self.fixture["policy"])
        self.assertIsNone(plan(policy, [{"bytes": True, "count": 1}]))
        policy["chunkBytes"] = 2**53
        self.assertIsNone(plan(policy, [{"bytes": 1, "count": 1}]))


if __name__ == "__main__":
    unittest.main()
