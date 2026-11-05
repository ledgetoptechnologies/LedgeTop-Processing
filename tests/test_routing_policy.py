"""Small-node-first admission boundaries without starting any processing."""

import unittest

from processing.policy import plan, plan_ordered_nodes


class RoutingPolicyTests(unittest.TestCase):
    def setUp(self):
        self.local = {"ready": True, "taskQueueCount": 0, "maxImages": 1000}
        self.main = {"ready": True, "taskQueueCount": 0, "maxImages": None}

    def route(self, photos=50, size=100, **kwargs):
        return plan(photos, size, self.local, self.main, 1000, 20_000, **kwargs)

    def test_1000_photo_boundary_prefers_local_and_1001_requires_main(self):
        for count, expected in ((10, "local"), (50, "local"), (999, "local"),
                                (1000, "local"), (1001, "main")):
            with self.subTest(count=count):
                self.assertEqual(self.route(count)["decision"], expected)

    def test_byte_boundary_requires_main_independent_of_photo_count(self):
        self.assertEqual(self.route(size=20_000)["decision"], "local")
        self.assertEqual(self.route(size=20_001)["decision"], "main")

    def test_busy_local_does_not_consume_idle_main(self):
        self.local["taskQueueCount"] = 1
        self.assertEqual(self.route(), {"decision": "wait", "reason": "local_unavailable"})

    def test_local_unavailable_requires_explicit_fallback(self):
        self.local["ready"] = False
        self.assertEqual(self.route()["decision"], "wait")
        self.assertEqual(self.route(allow_main_fallback=True),
                         {"decision": "main", "reason": "explicit_local_fallback"})
        self.assertEqual(self.route(allow_main_fallback=1)["decision"], "wait")

    def test_explicit_fallback_still_prefers_eligible_local(self):
        self.assertEqual(self.route(allow_main_fallback=True)["decision"], "local")

    def test_busy_main_does_not_block_eligible_small_work(self):
        self.main["taskQueueCount"] = 1
        self.assertEqual(self.route()["decision"], "local")

    def test_large_work_never_overflows_local_when_main_busy_or_unavailable(self):
        for state in ({"ready": False}, {"taskQueueCount": 1}, {"state": "draining"}):
            with self.subTest(state=state):
                self.main = {"ready": True, "taskQueueCount": 0, "maxImages": None, **state}
                self.assertEqual(self.route(1001, allow_main_fallback=True),
                                 {"decision": "wait", "reason": "main_unavailable"})

    def test_unusable_capacity_or_maintenance_cannot_be_selected(self):
        for field, value in (("taskQueueCount", True), ("taskQueueCount", "0"),
                             ("taskQueueCount", None), ("taskQueueCount", -1),
                             ("maxImages", True), ("maxImages", "1000"),
                             ("maxImages", 0), ("maxImages", 40),
                             ("state", "held"), ("state", "updating"),
                             ("maintenanceState", "draining"), ("ready", 1)):
            with self.subTest(field=field, value=value):
                self.local = {"ready": True, "taskQueueCount": 0, "maxImages": 1000,
                              field: value}
                self.assertEqual(self.route()["decision"], "wait")

    def test_explicit_fallback_never_uses_main_without_capacity(self):
        self.local["ready"] = False
        for state in ({"ready": False}, {"taskQueueCount": 1}, {"maxImages": 40},
                      {"state": "updating"}, {"maintenanceState": "held"}):
            with self.subTest(state=state):
                self.main = {"ready": True, "taskQueueCount": 0, "maxImages": None, **state}
                self.assertEqual(self.route(allow_main_fallback=True)["decision"], "wait")

    def test_ordered_fleet_routes_to_first_live_capacity_then_falls_through(self):
        nodes = [
            {"id": "cluster", "ready": True, "taskQueueCount": 0, "maxImages": 1000,
             "maxInputBytes": 20_000},
            {"id": "middle", "ready": True, "taskQueueCount": 0, "maxImages": 4000,
             "maxInputBytes": 90_000},
            {"id": "main", "ready": True, "taskQueueCount": 0, "maxImages": None},
        ]
        self.assertEqual(plan_ordered_nodes(100, 1000, nodes)["nodeId"], "cluster")
        self.assertEqual(plan_ordered_nodes(1001, 20_001, nodes)["nodeId"], "middle")
        self.assertEqual(plan_ordered_nodes(5000, 100_000, nodes)["nodeId"], "main")

    def test_ordered_fleet_skips_busy_maintenance_and_unavailable_nodes(self):
        nodes = [
            {"id": "cluster", "ready": True, "taskQueueCount": 1, "maxImages": 1000},
            {"id": "middle", "ready": True, "taskQueueCount": 0, "maxImages": 4000,
             "maintenanceState": "draining"},
            {"id": "main", "ready": True, "taskQueueCount": 0, "maxImages": None},
        ]
        self.assertEqual(plan_ordered_nodes(100, 1000, nodes)["nodeId"], "main")
        nodes[2]["taskQueueCount"] = 1
        self.assertEqual(plan_ordered_nodes(100, 1000, nodes),
                         {"decision": "wait", "reason": "nodes_busy"})

    def test_ordered_fleet_requires_each_requested_profile_and_feature(self):
        nodes = [
            {"id": "cluster", "ready": True, "taskQueueCount": 0, "maxImages": 1000,
             "allowedProfiles": ["odm"], "features": ["photo"]},
            {"id": "main", "ready": True, "taskQueueCount": 0, "maxImages": None,
             "allowedProfiles": ["odm", "treedar"], "features": ["photo", "lidar"]},
        ]
        result = plan_ordered_nodes(50, 100, nodes, required_profile="treedar",
                                    required_features=("lidar",))
        self.assertEqual(result["nodeId"], "main")
        self.assertEqual(plan_ordered_nodes(50, 100, nodes, required_features=("gaussian",)),
                         {"decision": "wait", "reason": "no_node_with_required_capabilities"})

    def test_ordered_fleet_rejects_duplicate_or_unbounded_inventories(self):
        node = {"id": "cluster", "ready": True, "taskQueueCount": 0}
        for nodes in ([], [node] * 9, [node, dict(node)], [dict(node, id="bad node")]):
            with self.subTest(nodes=nodes), self.assertRaises(ValueError):
                plan_ordered_nodes(0, 0, nodes)

    def test_invalid_live_engine_limit_fails_closed_for_that_node(self):
        nodes = [
            {"id": "cluster", "ready": True, "taskQueueCount": 0, "maxImages": True},
            {"id": "main", "ready": True, "taskQueueCount": 0, "maxImages": None},
        ]
        self.assertEqual(plan_ordered_nodes(100, 100, nodes)["nodeId"], "main")

    def test_malformed_legacy_node_probe_fails_closed(self):
        self.assertEqual(plan(50, 100, None, self.main, 1000, 20_000),
                         {"decision": "wait", "reason": "local_unavailable"})
        self.assertEqual(plan(1500, 100, self.local, None, 1000, 20_000),
                         {"decision": "wait", "reason": "main_unavailable"})


if __name__ == "__main__":
    unittest.main()
