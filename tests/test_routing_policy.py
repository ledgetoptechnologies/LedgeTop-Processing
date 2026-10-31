"""Small-node-first admission boundaries without starting any processing."""

import unittest

from processing.policy import plan


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

    def test_input_size_does_not_override_photo_preference(self):
        for size in (20_000, 20_001, 20 * 1024**3 + 1, 50 * 1024**3):
            with self.subTest(size=size):
                self.assertEqual(self.route(1000, size)["decision"], "local")
                self.assertEqual(self.route(1001, size)["decision"], "main")

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
                             ("maxImages", 0),
                             ("state", "held"), ("state", "updating"),
                             ("maintenanceState", "draining"), ("ready", 1)):
            with self.subTest(field=field, value=value):
                self.local = {"ready": True, "taskQueueCount": 0, "maxImages": 1000,
                              field: value}
                self.assertEqual(self.route()["decision"], "wait")

    def test_stricter_advertised_photo_ceiling_routes_only_overflow_to_main(self):
        self.local['maxImages']=40
        self.assertEqual(self.route(40)['decision'],'local')
        self.assertEqual(self.route(41),{'decision':'main','reason':'exceeds_local_limits'})
        self.main['ready']=False
        self.assertEqual(self.route(41),{'decision':'wait','reason':'main_unavailable'})

    def test_unhealthy_or_malformed_local_probe_never_causes_implicit_fallback(self):
        for state in ({'ready':False,'maxImages':40},{'ready':True,'maxImages':True},
                      {'ready':True,'maxImages':'40'},{'ready':True,'maxImages':0},
                      {'ready':True,'maxImages':40,'taskQueueCount':None},
                      {'ready':True,'maxImages':40,'taskQueueCount':True}):
            with self.subTest(state=state):
                self.local={'ready':True,'taskQueueCount':0,'maxImages':1000,**state}
                self.assertEqual(self.route(41)['decision'],'wait')

    def test_explicit_fallback_never_uses_main_without_capacity(self):
        self.local["ready"] = False
        for state in ({"ready": False}, {"taskQueueCount": 1}, {"maxImages": 40},
                      {"state": "updating"}, {"maintenanceState": "held"}):
            with self.subTest(state=state):
                self.main = {"ready": True, "taskQueueCount": 0, "maxImages": None, **state}
                self.assertEqual(self.route(allow_main_fallback=True)["decision"], "wait")


if __name__ == "__main__":
    unittest.main()
