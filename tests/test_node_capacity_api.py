import json
import tempfile
import threading
import unittest
from pathlib import Path

from processing.node_capacity_api import NodeCapacityAPI
from processing.scratch_policy import PolicyConflict, ScratchLedger


class NodeCapacityApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "scratch"
        self.root.mkdir()
        self.free = [1_000]
        self.ledger = ScratchLedger(Path(self.temp.name) / "ledger.sqlite", self.root,
            capacity_probe=lambda _: self.free[0], safety_floor_bytes=100)
        self.api = NodeCapacityAPI(ledger=self.ledger, node_id="cluster",
            physical_host_id="host-cluster", bearer_token="t" * 40, receipt_key=b"k" * 32)
        self.auth = "Bearer " + "t" * 40

    def tearDown(self):
        self.temp.cleanup()

    def request(self, path, value):
        return self.api.handle("POST", path, self.auth,
            json.dumps(value, separators=(",", ":")).encode())

    def reserve(self, *, job="job-a", reservation="reserve-a", generation=1,
                environment="production", input_bytes=100, output=200):
        return dict(nodeId="cluster", physicalHostId="host-cluster", jobId=job,
            generation=generation, reservationId=reservation, environment=environment,
            manifestSha256="a" * 64, inputBytes=input_bytes,
            outputBudgetBytes=output, ttlSeconds=60)

    def test_auth_and_bounded_closed_schema(self):
        with self.assertRaises(PermissionError):
            self.api.handle("GET", "/capacity/v1/probe", "Bearer wrong")
        self.assertEqual(self.api.handle("POST", "/capacity/v1/reserve", self.auth,
            b"{" + b" " * 16384)[0], 413)
        self.assertEqual(self.request("/capacity/v1/reserve", dict(self.reserve(), extra=True))[0], 409)
        duplicate = b'{"nodeId":"cluster","nodeId":"other"}'
        self.assertEqual(self.api.handle("POST", "/capacity/v1/reserve", self.auth, duplicate)[0], 409)
        self.assertEqual(self.api.handle("GET", "/capacity/v1/probe", self.auth)[0], 200)

    def test_reserve_receipt_identity_replay_and_tamper(self):
        status, receipt = self.request("/capacity/v1/reserve", self.reserve())
        self.assertEqual(status, 200)
        self.assertTrue(self.api.verify_receipt(receipt, expected_job="job-a",
            expected_generation=1, expected_manifest="a" * 64))
        _, replay = self.request("/capacity/v1/reserve", self.reserve())
        self.assertEqual(receipt, replay)
        self.assertFalse(self.api.verify_receipt(receipt, expected_job="other"))
        tampered = dict(receipt, inputBytes=101)
        self.assertFalse(self.api.verify_receipt(tampered))
        wrong_host = dict(self.reserve(), physicalHostId="other-host")
        self.assertEqual(self.request("/capacity/v1/reserve", wrong_host)[0], 409)

    def test_receipt_and_reservation_survive_service_restart(self):
        status, before = self.request("/capacity/v1/reserve", self.reserve())
        self.assertEqual(status, 200)
        replacement_ledger = ScratchLedger(Path(self.temp.name) / "ledger.sqlite", self.root,
            capacity_probe=lambda _: self.free[0], safety_floor_bytes=100)
        replacement = NodeCapacityAPI(ledger=replacement_ledger, node_id="cluster",
            physical_host_id="host-cluster", bearer_token="t" * 40, receipt_key=b"k" * 32)
        status, after = replacement.handle("POST", "/capacity/v1/reserve", self.auth,
            json.dumps(self.reserve(), separators=(",", ":")).encode())
        self.assertEqual(status, 200)
        self.assertEqual(before, after)
        self.assertTrue(replacement.verify_receipt(after, expected_job="job-a", expected_generation=1))

    def test_capacity_pressure_is_atomic_across_racing_jobs(self):
        barrier = threading.Barrier(3)
        outcomes = []
        def run(n):
            barrier.wait()
            outcomes.append(self.request("/capacity/v1/reserve", self.reserve(
                job=f"job-{n}", reservation=f"reservation-{n}", input_bytes=300,
                output=300))[0])
        threads = [threading.Thread(target=run, args=(n,)) for n in range(2)]
        for thread in threads:thread.start()
        barrier.wait()
        for thread in threads:thread.join()
        self.assertCountEqual(outcomes, [200, 409])

    def test_environment_binding_and_allocation_holds_without_ttl_release(self):
        status, receipt = self.request("/capacity/v1/reserve", self.reserve())
        self.assertEqual(status, 200)
        self.assertEqual(self.request("/capacity/v1/reserve", self.reserve(
            generation=2, environment="staging", reservation="reserve-b"))[0], 409)
        started = dict(nodeId="cluster", physicalHostId="host-cluster",
            reservationId="reserve-a", state="engine_started", terminalAttested=False)
        self.assertEqual(self.request("/capacity/v1/engine-state", started)[0], 200)
        allocation = dict(nodeId="cluster", physicalHostId="host-cluster",
            reservationId="reserve-a", allocatedBytes=100)
        self.assertEqual(self.request("/capacity/v1/allocated", allocation)[0], 200)
        self.ledger.clock = lambda: 10**9
        replay_status, _ = self.request("/capacity/v1/reserve", self.reserve())
        self.assertEqual(replay_status, 200)
        self.assertTrue(self.api.verify_receipt(receipt, expected_job="job-a"))

    def test_terminal_attestation_required_before_unused_budget_settlement(self):
        self.request("/capacity/v1/reserve", self.reserve())
        request = dict(nodeId="cluster", physicalHostId="host-cluster",
            reservationId="reserve-a", state="terminal", terminalAttested=False)
        self.assertEqual(self.request("/capacity/v1/engine-state", request)[0], 409)
        request["terminalAttested"] = True
        self.assertEqual(self.request("/capacity/v1/engine-state", request)[0], 200)


if __name__ == "__main__":
    unittest.main()
