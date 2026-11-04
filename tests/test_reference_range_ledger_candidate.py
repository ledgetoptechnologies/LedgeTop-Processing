"""Disposable Python mirror of synthetic range-ledger proposal vectors."""
import json
from pathlib import Path
import tempfile
import unittest

from processing.reference_range_ledger_candidate import (
    CandidateRejected, RangeLedger, range_proof, validate_grant,
)


class ReferenceRangeLedgerCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "candidate.sqlite"
        self.key = bytes([7]) * 32
        self.policy = {"version": 1, "chunkBytes": 4,
                       "requestBudget": 2, "byteBudget": 8}
        self.reservation = {
            "version": 2,
            "environment": "staging",
            "jobId": "job-1",
            "manifestSha256": "a" * 64,
            "generation": 1,
            "reservationId": "reservation-1",
            "policy": dict(self.policy),
        }
        self.grant = {**self.reservation, "grantId": "grant-1"}

    def open_ledger(self):
        return RangeLedger(self.path, self.key)

    def test_charges_survive_restart_and_replacement_grants_without_reset(self):
        ledger = self.open_ledger()
        self.assertEqual(ledger.charge(self.reservation, self.grant, "object-1", 0, 4), "charged")
        ledger.close()

        replacement = {**self.reservation, "generation": 2, "reservationId": "reservation-2"}
        replacement_grant = {**replacement, "grantId": "grant-2"}
        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        self.assertEqual(ledger.status(replacement, "object-1", 0),
                         {"state": "uncertain", "length": 4})
        with self.assertRaises(CandidateRejected):
            ledger.charge(replacement, replacement_grant, "object-1", 0, 4)
        with self.assertRaises(CandidateRejected):
            ledger.verify(replacement, "object-1", 0, b"abcd", "0" * 64)
        self.assertEqual(ledger.status(self.reservation, "object-1", 0)["state"], "uncertain")

        proof = range_proof(self.key, self.reservation, "object-1", 0, b"abcd")
        with self.assertRaises(CandidateRejected):
            ledger.verify(self.reservation, "object-1", 0, b"abce", proof)
        self.assertEqual(ledger.status(replacement, "object-1", 0)["state"], "uncertain")
        ledger.verify(self.reservation, "object-1", 0, b"abcd", proof)
        ledger.close()

        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        self.assertEqual(ledger.status(replacement, "object-1", 0)["state"], "verified")
        self.assertEqual(ledger.charge(replacement, replacement_grant, "object-1", 4, 4), "charged")
        with self.assertRaises(CandidateRejected):
            ledger.charge(replacement, replacement_grant, "object-1", 8, 1)
        self.assertEqual(ledger.usage(replacement), {"requests": 2, "bytes": 8})

    def test_closed_reservation_grant_policy_and_identity_bindings(self):
        validate_grant(self.grant, self.reservation)
        for changed in (
            {**self.reservation, "extra": True},
            {**self.reservation, "environment": "production"},
            {**self.reservation, "manifestSha256": "b" * 64},
            {**self.reservation, "policy": {**self.policy, "requestBudget": 3}},
        ):
            with self.subTest(changed=changed), self.assertRaises(CandidateRejected):
                validate_grant(self.grant, changed)
        for changed_grant in (
            {**self.grant, "extra": True},
            {**self.grant, "grantId": "bad id"},
            {**self.grant, "policy": {**self.policy, "byteBudget": 9}},
        ):
            with self.subTest(changed_grant=changed_grant), self.assertRaises(CandidateRejected):
                validate_grant(changed_grant, self.reservation)

    def test_overlapping_ranges_changed_policy_and_limits_fail_atomically(self):
        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        ledger.charge(self.reservation, self.grant, "object-1", 0, 4)
        for start, length in ((0, 4), (2, 2), (4, 5), (-1, 1)):
            with self.subTest(start=start, length=length), self.assertRaises(CandidateRejected):
                ledger.charge(self.reservation, self.grant, "object-1", start, length)
        self.assertEqual(ledger.usage(self.reservation), {"requests": 1, "bytes": 4})
        ledger.charge(self.reservation, self.grant, "object-1", 4, 4)
        with self.assertRaises(CandidateRejected):
            ledger.charge(self.reservation, self.grant, "object-1", 8, 1)
        self.assertEqual(ledger.usage(self.reservation), {"requests": 2, "bytes": 8})

        changed_policy = {**self.reservation,
                          "policy": {**self.policy, "requestBudget": 3}}
        changed_grant = {**changed_policy, "grantId": "grant-new"}
        with self.assertRaises(CandidateRejected):
            ledger.charge(changed_policy, changed_grant, "object-1", 4, 4)
        self.assertEqual(ledger.usage(self.reservation), {"requests": 2, "bytes": 8})

    def test_verified_evidence_binds_exact_object_range_and_bytes(self):
        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        ledger.charge(self.reservation, self.grant, "object-1", 0, 4)
        proof = range_proof(self.key, self.reservation, "object-1", 0, b"abcd")
        for object_id, start, body in (("object-2", 0, b"abcd"),
                                       ("object-1", 1, b"abcd"),
                                       ("object-1", 0, b"abce"),
                                       ("object-1", 0, b"abc")):
            with self.subTest(object_id=object_id, start=start, body=body), self.assertRaises(CandidateRejected):
                ledger.verify(self.reservation, object_id, start, body, proof)
        ledger.verify(self.reservation, "object-1", 0, b"abcd", proof)
        self.assertEqual(ledger.status(self.reservation, "object-1", 0)["state"], "verified")

    def test_environment_and_job_identity_have_independent_ledgers(self):
        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        ledger.charge(self.reservation, self.grant, "object-1", 0, 4)
        other = {**self.reservation, "environment": "production"}
        other_grant = {**other, "grantId": "grant-prod"}
        ledger.charge(other, other_grant, "object-1", 0, 4)
        self.assertEqual(ledger.usage(self.reservation), {"requests": 1, "bytes": 4})
        self.assertEqual(ledger.usage(other), {"requests": 1, "bytes": 4})

    def test_policy_field_order_does_not_change_the_ledger_identity(self):
        ledger = self.open_ledger()
        self.addCleanup(ledger.close)
        ledger.charge(self.reservation, self.grant, "object-1", 0, 4)
        reordered_policy = {"byteBudget": 8, "requestBudget": 2,
                            "chunkBytes": 4, "version": 1}
        reordered = {**self.reservation, "generation": 2,
                     "reservationId": "reservation-2", "policy": reordered_policy}
        reordered_grant = {**reordered, "grantId": "grant-2"}
        self.assertEqual(ledger.charge(reordered, reordered_grant, "object-1", 4, 4), "charged")
        self.assertEqual(ledger.usage(reordered), {"requests": 2, "bytes": 8})

    def test_sample_hmac_proof_is_a_pinned_cross_language_vector(self):
        proof = range_proof(self.key, self.reservation, "object-1", 0, b"abcd")
        self.assertEqual(proof,
                         "a99a9cfc4e29ee8d463feaa42c5035977cf924d9dd8cb972cd74e807000decc9")

    def test_synthetic_sqlite_event_vectors(self):
        fixture_path = (Path(__file__).resolve().parents[1] /
                        "contracts/reference-broker-budget-candidate/reference-v2-range-ledger-vectors.json")
        fixture = json.loads(fixture_path.read_bytes())
        self.assertEqual(fixture["status"], "synthetic-proposal-not-wire-contract")
        base = fixture["reservation"]
        grants = []
        reservations = []
        for item in fixture["grants"]:
            reservation = {**base, "generation": item["generation"],
                           "reservationId": item["reservationId"]}
            reservations.append(reservation)
            grants.append({**reservation, "grantId": item["grantId"]})

        proof_key = bytes([7]) * 32
        ledger = self.open_ledger()
        try:
            for event in fixture["events"]:
                reservation = reservations[event["grant"]]
                grant = grants[event["grant"]]
                object_id, start = event["objectId"], event["start"]
                if event.get("reorderPolicy"):
                    policy = dict(reversed(list(reservation["policy"].items())))
                    reservation = {**reservation, "policy": policy}
                    grant = {**grant, "policy": policy}
                with self.subTest(event=event):
                    if event["action"] == "reopen":
                        ledger.close()
                        ledger = self.open_ledger()
                    elif event["action"] == "charge":
                        if event["expected"] == "rejected":
                            with self.assertRaises(CandidateRejected):
                                ledger.charge(reservation, grant, object_id, start, event["length"])
                        else:
                            self.assertEqual(ledger.charge(reservation, grant, object_id,
                                                           start, event["length"]), "charged")
                    elif event["action"] == "verify-invalid":
                        with self.assertRaises(CandidateRejected):
                            ledger.verify(reservation, object_id, start,
                                          event["content"].encode("utf-8"), "0" * 64)
                    elif event["action"] == "verify-valid":
                        body = event["content"].encode("utf-8")
                        proof = range_proof(proof_key, reservation, object_id, start, body)
                        self.assertEqual(proof, event["proof"])
                        ledger.verify(reservation, object_id, start, body, proof)
                    else:
                        self.fail("unknown shared fixture event")

                    current = ledger.status(reservation, object_id, start)
                    self.assertEqual(None if current is None else current["state"],
                                     event["rangeState"])
                    usage = ledger.usage(reservation) or {"requests": 0, "bytes": 0}
                    self.assertEqual(usage, {"requests": event["requests"],
                                             "bytes": event["bytes"]})
        finally:
            ledger.close()


if __name__ == "__main__":
    unittest.main()
