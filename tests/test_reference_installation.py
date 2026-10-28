"""Migration fault regressions. All Docker/HTTP mutations are synthetic."""
from contextlib import ExitStack
import copy
import importlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch


@unittest.skipUnless(os.name == "posix", "installer requires Linux host ownership semantics")
class ReferenceInstallationTests(unittest.TestCase):
    def setUp(self):
        self.install = importlib.import_module("ops.install_reference_coordinator")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)

    def activity_api(self):
        install = self.install
        class API:
            origin = "http://127.0.0.1:18080"
            def json(self, method, route):
                if (method, route) == ("GET", "/v1/prototype/authority"):
                    return {"authorityId": install.AUTHORITY}
                if (method, route) == ("GET", "/v1/prototype/jobs"):
                    return {"jobs": []}
                raise AssertionError("unexpected prototype route")
        return API()

    def test_activity_uses_supported_native_info_and_readonly_task_aggregate(self):
        routes, aggregate_calls = [], []
        class Facade:
            def json(self, method, route):
                routes.append((method, route))
                if (method, route) != ("GET", "/info"):
                    raise AssertionError("the native facade has no task/list route")
                return {"taskQueueCount": 0}
        def docker(*args):
            aggregate_calls.append(args)
            self.assertEqual(args[:4], ("exec", "--user", "10001:10001", self.install.NAME))
            self.assertIn("mode=ro", args[-1])
            self.assertNotIn("SELECT *", args[-1])
            return json.dumps({"nativeViewerTasks": 0})
        with patch.object(self.install, "environment", return_value={"PROCESSING_VIEWER_TOKEN": "synthetic"}), \
                patch.object(self.install, "Client", return_value=Facade()), patch.object(self.install, "docker", docker):
            result = self.install.activity(self.activity_api())
        self.assertEqual(result["count"], 0)
        self.assertEqual(routes, [("GET", "/info")])
        self.assertEqual(len(aggregate_calls), 1)

    def test_zero_queue_count_does_not_hide_an_uncommitted_native_upload(self):
        class Facade:
            def json(self, *_args):
                return {"taskQueueCount": 0}
        with patch.object(self.install, "environment", return_value={"PROCESSING_VIEWER_TOKEN": "synthetic"}), \
                patch.object(self.install, "Client", return_value=Facade()), \
                patch.object(self.install, "docker", return_value=json.dumps({"nativeViewerTasks": 1})):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.activity(self.activity_api())

    def test_backup_installer_uses_numeric_chown_without_host_username_lookup(self):
        observed = []
        class InstallerObserved(Exception):
            pass
        original_stat = Path.stat
        def owned_stat(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            if path == self.root:
                values = list(result)
                values[4], values[5] = 1000, 1000
                return os.stat_result(values)
            return result
        def command(args):
            # Execute the emitted directory installer, with only chown mocked.
            # This proves numeric IDs reach os.chown instead of install's passwd
            # lookup, while writing exclusively beneath this disposable root.
            self.assertEqual(args[:4], ["sudo", "-n", "python3", "-c"])
            self.assertEqual(Path(args[5]), self.root / "original-state-backup")
            with patch.object(sys, "argv", ["synthetic-installer", args[5]]), \
                    patch("pathlib.Path.stat", owned_stat), \
                    patch("os.chown", side_effect=lambda path, uid, gid: observed.append((Path(path), uid, gid))):
                exec(compile(args[4], "<synthetic-backup-installer>", "exec"), {"__name__": "__main__"})
            raise InstallerObserved()
        with patch.object(self.install, "command", command):
            with self.assertRaises(InstallerObserved):
                self.install.backup(self.root, {"operation": "synthetic"})
        self.assertEqual(observed, [(self.root / "original-state-backup", 10001, 10001)])
        self.assertIs(type(observed[0][1]), int)
        self.assertIs(type(observed[0][2]), int)
        self.assertEqual(stat.S_IMODE((self.root / "original-state-backup").stat().st_mode), 0o700)

    def migration_fixture(self):
        """Real maintenance CAS state, synthetic Docker control/HTTP transport."""
        from processing.prototype import Queue
        self.fixture_count = getattr(self, "fixture_count", 0) + 1
        queue = Queue(self.root / ("queue-" + str(self.fixture_count)), node_ids=("cluster", "main"))
        self.addCleanup(queue.close)
        install = self.install
        old_id, new_id = "a"*64, "b"*64
        old_image, new_image = "sha256:" + "c"*64, "sha256:" + "d"*64
        old = dict(Id=old_id, Name="/" + install.NAME, Image=old_image,
            Config={"Labels": {}, "Env": []}, State={"Running": True, "Health": {"Status": "healthy"}})
        state = dict(operation="synthetic-operation", phase="prepared", image=new_image,
            old={"containerId": old_id, "image": old_image}, history={"count": 0, "sha256": "baseline"},
            engineImages={"cluster": "sha256:" + "e"*64, "main": install.MAIN_IMAGE},
            plans={}, rollbackName=install.NAME + "-retained", failedName=install.NAME + "-failed")
        containers, calls = {old_id: old}, []
        class API:
            origin = "http://127.0.0.1:18080"
            def json(self, method, route, *_args, **_kwargs):
                if method == "GET" and route.startswith("/v1/prototype/nodes/"):
                    return {"node": queue.get_node(route.rsplit("/", 1)[1])}
                raise AssertionError("unexpected migration HTTP read")
        api = API()
        fixture = dict(queue=queue, state=state, containers=containers, calls=calls, api=api,
                       old=old, old_id=old_id, new_id=new_id, operator_calls=[], lose=None)
        class Operator:
            def json(_self, method, route, payload):
                self.assertEqual(method, "POST")
                fixture["operator_calls"].append((route, copy.deepcopy(payload)))
                node, action = route.split("/")[-2:]
                if action == "schedule":
                    result = queue.schedule_node(node, payload)
                elif action == "cancel":
                    # The intent must already be durable before the external CAS.
                    checkpoint = json.loads((self.root / "checkpoint.json").read_text())
                    self.assertEqual(checkpoint["plans"][node]["cancelIntent"], payload)
                    result = queue.cancel_node(node, payload)
                else:
                    raise AssertionError("unexpected maintenance mutation")
                if fixture["lose"] == action:
                    fixture["lose"] = None
                    raise OSError("synthetic response lost after real maintenance commit")
                if fixture["lose"] == "cancel_then_peer_plan" and action == "cancel":
                    fixture["lose"] = None
                    queue.schedule_node(node, dict(expectedRevision=result["revision"],
                        targetImage=state["engineImages"][node], drainAt="2020-01-01T00:00:00Z",
                        notBefore="2020-01-01T00:00:00Z", afterJobId=None))
                    raise OSError("cancel committed, then another operator scheduled a plan")
                return {"node": result}
        fixture["operator"] = Operator()
        def inspected(identifier):
            if identifier == "node-odx-gpu":
                return {"Image": state["engineImages"]["cluster"]}
            matches = [value for key, value in containers.items()
                       if key.startswith(identifier) or value["Name"] == "/" + identifier]
            if len(matches) != 1:
                raise install.CutoverStopped("synthetic container identity unavailable")
            return copy.deepcopy(matches[0])
        def docker(*args, **kwargs):
            calls.append(args)
            action = args[0]
            if action == "ps":
                own = [value["Id"][:12] for value in containers.values()
                       if value["Config"]["Labels"].get("org.ledgetop.reference.cutover") == state["operation"]]
                return "\n".join(own)
            if action == "create":
                containers[new_id] = dict(Id=new_id, Name="/" + install.NAME, Image=new_image,
                    Config={"Labels": {"org.ledgetop.reference.cutover": state["operation"]}, "Env": []},
                    State={"Running": False, "Health": {"Status": "healthy"}})
                if fixture["lose"] == "create":
                    fixture["lose"] = None
                    raise OSError("synthetic create succeeded but ID response was lost")
                return new_id
            if action == "rename":
                item = containers[args[1]]
                if any(value["Name"] == "/" + args[2] for value in containers.values() if value is not item):
                    raise install.CutoverStopped("synthetic Docker name conflict")
                item["Name"] = "/" + args[2]
                return ""
            if action in ("stop", "start"):
                item = containers[args[-1]]
                item["State"]["Running"] = action == "start"
                return item["Id"]
            raise AssertionError("unexpected Docker operation")
        fixture["inspected"], fixture["docker"] = inspected, docker
        return fixture

    def migration_patches(self, fixture):
        stack = ExitStack()
        install, state = self.install, fixture["state"]
        stack.enter_context(patch.object(install, "local_clients", return_value=({}, fixture["api"], fixture["operator"])))
        stack.enter_context(patch.object(install, "inspected", fixture["inspected"]))
        stack.enter_context(patch.object(install, "docker", fixture["docker"]))
        stack.enter_context(patch.object(install, "facts", side_effect=lambda *_a, **_k: state["old"]))
        stack.enter_context(patch.object(install, "sole_owner"))
        stack.enter_context(patch.object(install, "check_native_environment"))
        stack.enter_context(patch.object(install, "activity", return_value=state["history"]))
        stack.enter_context(patch.object(install, "health"))
        stack.enter_context(patch.object(install, "backup", return_value={"sqliteQuickCheck": "ok"}))
        return stack

    def test_lost_create_id_recovers_only_install_bound_candidate_and_restores_single_owner(self):
        fixture = self.migration_fixture()
        fixture["lose"] = "create"
        with self.migration_patches(fixture):
            result = self.install.activate(self.root, fixture["state"], self.install.MAIN_IMAGE)
        self.assertEqual(result["phase"], "rolled_back")
        self.assertEqual(result["newContainerId"], fixture["new_id"])
        self.assertEqual(fixture["old"]["Name"], "/" + self.install.NAME)
        self.assertTrue(fixture["old"]["State"]["Running"])
        self.assertEqual(fixture["containers"][fixture["new_id"]]["Name"], "/" + result["failedName"])
        self.assertFalse(fixture["containers"][fixture["new_id"]]["State"]["Running"])
        self.assertEqual(sum(value["State"]["Running"] for value in fixture["containers"].values()), 1)
        self.assertEqual(sum(call[0] == "create" for call in fixture["calls"]), 1)

    def test_foreign_recorded_candidate_is_never_stopped_or_renamed(self):
        for mismatch in ("label", "image", "name"):
            with self.subTest(mismatch=mismatch):
                fixture = self.migration_fixture()
                state = fixture["state"]
                fixture["old"]["Name"] = "/" + state["rollbackName"]
                fixture["old"]["State"]["Running"] = False
                state["newContainerId"] = fixture["new_id"]
                foreign = dict(Id=fixture["new_id"], Name="/" + self.install.NAME, Image=state["image"],
                    Config={"Labels": {"org.ledgetop.reference.cutover": state["operation"]}},
                    State={"Running": True})
                if mismatch == "label":
                    foreign["Config"]["Labels"]["org.ledgetop.reference.cutover"] = "another-operation"
                elif mismatch == "image":
                    foreign["Image"] = "sha256:" + "f"*64
                else:
                    foreign["Name"] = "/unrelated-owner"
                fixture["containers"][fixture["new_id"]] = foreign
                with self.migration_patches(fixture), self.assertRaises(self.install.CutoverStopped):
                    self.install.rollback(self.root, state)
                self.assertTrue(foreign["State"]["Running"])
                self.assertFalse(any(call[0] in ("stop", "start", "rename") for call in fixture["calls"]))

    def test_missing_candidate_id_never_stops_foreign_name_owner_or_starts_second_owner(self):
        fixture = self.migration_fixture()
        fixture["old"].update(Name="/" + fixture["state"]["rollbackName"])
        fixture["old"]["State"]["Running"] = False
        foreign = dict(Id=fixture["new_id"], Name="/" + self.install.NAME, Image=fixture["state"]["image"],
            Config={"Labels": {"org.ledgetop.reference.cutover": "foreign-operation"}}, State={"Running": True})
        fixture["containers"][fixture["new_id"]] = foreign
        with self.migration_patches(fixture), self.assertRaises(self.install.CutoverStopped):
            self.install.rollback(self.root, fixture["state"])
        self.assertTrue(foreign["State"]["Running"])
        self.assertFalse(any(call[0] in ("stop", "start") for call in fixture["calls"]))

    def draining_plan(self, fixture):
        plan = fixture["queue"].schedule_node("cluster", dict(expectedRevision=0,
            targetImage=fixture["state"]["engineImages"]["cluster"], drainAt="2020-01-01T00:00:00Z",
            notBefore="2020-01-01T00:00:00Z", afterJobId=None))
        fixture["state"]["plans"]["cluster"] = {"planId": plan["planId"], "targetImage": plan["targetImage"]}
        return plan

    def test_cancel_ack_loss_reconciles_exact_cas_once_without_cancel_replay(self):
        fixture = self.migration_fixture()
        self.draining_plan(fixture)
        fixture["lose"] = "cancel"
        with self.migration_patches(fixture):
            self.install.cancel_plans(self.root, fixture["state"], fixture["api"], fixture["operator"])
            self.install.cancel_plans(self.root, fixture["state"], fixture["api"], fixture["operator"])
        self.assertEqual(fixture["queue"].get_node("cluster")["state"], "active")
        self.assertTrue(fixture["state"]["plans"]["cluster"]["cancelled"])
        self.assertEqual(len(fixture["operator_calls"]), 1)

    def test_cancel_committed_before_local_checkpoint_restarts_without_cancel_replay(self):
        fixture = self.migration_fixture()
        self.draining_plan(fixture)
        original_save = self.install.save
        def lost_checkpoint(root, state):
            if state["plans"]["cluster"].get("cancelled"):
                raise OSError("synthetic local checkpoint loss after cancel commit")
            original_save(root, state)
        with self.migration_patches(fixture):
            with patch.object(self.install, "save", lost_checkpoint), self.assertRaises(OSError):
                self.install.cancel_plans(self.root, fixture["state"], fixture["api"], fixture["operator"])
            restarted = json.loads((self.root / "checkpoint.json").read_text())
            self.assertNotIn("cancelled", restarted["plans"]["cluster"])
            self.install.cancel_plans(self.root, restarted, fixture["api"], fixture["operator"])
        self.assertTrue(restarted["plans"]["cluster"]["cancelled"])
        self.assertEqual(len(fixture["operator_calls"]), 1)

    def test_lost_cancel_never_cancels_a_new_peer_plan(self):
        fixture = self.migration_fixture()
        own = self.draining_plan(fixture)
        fixture["lose"] = "cancel_then_peer_plan"
        with self.migration_patches(fixture):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.cancel_plans(self.root, fixture["state"], fixture["api"], fixture["operator"])
            peer = fixture["queue"].get_node("cluster")
            self.assertNotEqual(peer["planId"], own["planId"])
            with self.assertRaises(self.install.CutoverStopped):
                self.install.cancel_plans(self.root, fixture["state"], fixture["api"], fixture["operator"])
        self.assertEqual(fixture["queue"].get_node("cluster"), peer)
        self.assertEqual(len(fixture["operator_calls"]), 1)

    def test_lost_schedule_ack_remains_explicit_uncertainty_without_reschedule_or_peer_cancel(self):
        fixture = self.migration_fixture()
        fixture["lose"] = "schedule"
        with self.migration_patches(fixture):
            state = self.install.activate(self.root, fixture["state"], self.install.MAIN_IMAGE)
            self.assertEqual(state["phase"], "rollback_needs_inspection")
            self.assertTrue(state["unknownScheduleOutcome"])
            self.assertIn("pendingPlan", state)
            with self.assertRaises(self.install.CutoverStopped):
                self.install.activate(self.root, state, self.install.MAIN_IMAGE)
        self.assertEqual(fixture["queue"].get_node("cluster")["state"], "draining")
        self.assertEqual(len(fixture["operator_calls"]), 1)
        self.assertTrue(fixture["operator_calls"][0][0].endswith("/schedule"))
        self.assertFalse(any(call[0] in ("create", "stop", "start", "rename") for call in fixture["calls"]))

    def test_install_operation_lock_excludes_overlapping_receipts_until_owner_exits(self):
        path = self.root / "installation.lock"
        with patch.object(self.install, "LOCK_PATH", path):
            with self.install.operation_lock():
                inode = path.stat().st_ino
                with self.assertRaises(self.install.CutoverStopped):
                    with self.install.operation_lock():
                        self.fail("second operation acquired the live host-wide lock")
            self.assertTrue(path.exists())
            with self.install.operation_lock():
                self.assertEqual(path.stat().st_ino, inode)


if __name__ == "__main__":
    unittest.main()
