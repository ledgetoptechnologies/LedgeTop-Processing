"""Migration fault regressions. All Docker/HTTP mutations are synthetic."""
from contextlib import ExitStack, redirect_stdout
import copy
import importlib
import inspect
import io
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
            return json.dumps({"incompleteViewerTasks": 0, "viewerTaskCount": 5})
        with patch.object(self.install, "environment", return_value={"PROCESSING_VIEWER_TOKEN": "synthetic"}), \
                patch.object(self.install, "Client", return_value=Facade()), patch.object(self.install, "docker", docker):
            result = self.install.activity(self.activity_api())
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["nativeViewerTasks"], 5)
        self.assertEqual(routes, [("GET", "/info")])
        self.assertEqual(len(aggregate_calls), 1)

    def test_zero_queue_count_does_not_hide_an_uncommitted_native_upload(self):
        class Facade:
            def json(self, *_args):
                return {"taskQueueCount": 0}
        with patch.object(self.install, "environment", return_value={"PROCESSING_VIEWER_TOKEN": "synthetic"}), \
                patch.object(self.install, "Client", return_value=Facade()), \
                patch.object(self.install, "docker", return_value=json.dumps({"incompleteViewerTasks": 1, "viewerTaskCount": 1})):
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

    def test_backup_script_verifies_completed_viewer_task_copy_byte_for_byte(self):
        import sqlite3
        source, target = self.root / "volume", self.root / "backup"
        source.mkdir(mode=0o700); source.chmod(0o700)
        target.mkdir(mode=0o700); target.chmod(0o700)
        artifact = source / "viewer-tasks" / "synthetic-task" / "retained.bin"
        artifact.parent.mkdir(parents=True, mode=0o700)
        artifact.write_bytes(b"synthetic retained artifact")
        artifact.chmod(0o600)
        large=artifact.parent/'large-synthetic.bin'
        with large.open('wb') as output:output.truncate(64*1024*1024+1)
        large.chmod(0o600)
        database = source / "jobs.sqlite3"
        with sqlite3.connect(database) as db:
            db.executescript("""
                CREATE TABLE jobs(seq INTEGER PRIMARY KEY);
                CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY, authority_id TEXT NOT NULL);
                INSERT INTO coordinator_identity VALUES(1,'7131882e-38ce-4d3e-99cc-4ba5a94d2ee8');
                CREATE TABLE viewer_tasks(removed INTEGER NOT NULL, phase TEXT NOT NULL, stage TEXT NOT NULL);
                INSERT INTO viewer_tasks VALUES(0,'complete','succeeded');
                INSERT INTO viewer_tasks VALUES(0,'complete','failed');
                INSERT INTO viewer_tasks VALUES(1,'complete','cancelled');
            """)

        original_lstat, original_stat, original_fstat = Path.lstat, Path.stat, os.fstat
        def remap(result):
            fields = list(result); fields[4] = 10001; fields[5] = 10001
            return os.stat_result(fields)
        def mapped_path_stat(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            return remap(result) if path == target else result
        def mapped_lstat(path, *args, **kwargs):
            result = original_lstat(path, *args, **kwargs)
            return remap(result) if source == path or source in path.parents or target == path or target in path.parents else result
        def mapped_fstat(fd):
            return remap(original_fstat(fd))
        script = self.install.BACKUP.replace("Path('/source'),Path('/backup')",
            f"Path({str(source)!r}),Path({str(target)!r})")
        output = io.StringIO()
        with patch.object(Path, "lstat", mapped_lstat), patch.object(Path, "stat", mapped_path_stat), \
                patch("os.fstat", mapped_fstat), redirect_stdout(output):
            exec(compile(script, "<synthetic-viewer-backup>", "exec"), {"__name__": "__main__"})
        census = json.loads(output.getvalue())
        self.assertEqual(census["databaseHistory"]["nativeViewerTasks"], 3)  # failed and cancelled are retained too
        self.assertEqual((target / "viewer-tasks" / "synthetic-task" / "retained.bin").read_bytes(),
            b"synthetic retained artifact")
        self.assertEqual((target / 'viewer-tasks' / 'synthetic-task' / 'large-synthetic.bin').stat().st_size,large.stat().st_size)
        self.assertEqual(census["bytes"], database.stat().st_size + len(b"synthetic retained artifact") + large.stat().st_size)

    def test_every_viewer_retention_path_states_the_same_terminal_rule(self):
        from ops import update_reference_coordinator as update
        rule = self.install.NOT_TERMINAL_VIEWER_TASK
        self.assertEqual(rule, "NOT (phase='complete' AND stage IN ('succeeded','failed','cancelled'))")
        for name, source in (("BACKUP", self.install.BACKUP),
                             ("OFFLINE_ROLLBACK_CENSUS", self.install.OFFLINE_ROLLBACK_CENSUS),
                             ("volume_census", inspect.getsource(update.volume_census)),
                             ("update backup_script", update.backup_script())):
            with self.subTest(path=name):
                self.assertIn(rule, source)
                self.assertNotIn("stage='succeeded')", source)  # no succeeded-only variant remains

    def test_legacy_helper_scripts_keep_their_guards_under_python_optimize(self):
        import ast
        import sqlite3
        # Every reviewed script the installer hands to python -c carries no assert
        # that -O or PYTHONOPTIMIZE could strip.
        emitted = []
        def capture(*args, **kwargs):
            argv = list(args[0]) if len(args) == 1 and isinstance(args[0], list) else list(args)
            emitted.append(argv[argv.index("-c") + 1])
            raise self.install.CutoverStopped("synthetic_capture")
        with patch.object(self.install, "command", capture):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.backup(self.root, {"operation": "synthetic"})
        with patch.object(self.install, "docker", side_effect=lambda action, *args, **kwargs: "" if action == "ps" else capture(*args)):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.offline_rollback_precheck({"image": "synthetic-cached-image"})
        with patch.object(self.install, "command", return_value=""),                 patch.object(Path, "lstat", return_value=os.stat_result((stat.S_IFDIR | 0o700, 0, 0, 2, 10001, 10001, 0, 0, 0, 0))),                 patch.object(self.install, "docker", side_effect=capture):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.backup(self.root, {"operation": "synthetic", "image": "synthetic-cached-image"})
        self.assertEqual(len(emitted), 3)
        for source in emitted:
            self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(ast.parse(source))))

        # The guarded census still refuses a nonterminal Viewer task when compiled
        # with optimize=2; the raw script would silently report it as rollback-safe.
        database = self.root / "jobs.sqlite3"
        with sqlite3.connect(database) as db:
            db.executescript("""
                CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY, authority_id TEXT NOT NULL);
                INSERT INTO coordinator_identity VALUES(1,'7131882e-38ce-4d3e-99cc-4ba5a94d2ee8');
                CREATE TABLE jobs(id TEXT PRIMARY KEY, request_id TEXT, environment TEXT, profile TEXT, status TEXT,
                    worker_id TEXT, node_id TEXT, held_reason TEXT, result TEXT, created_at TEXT, updated_at TEXT);
                CREATE TABLE reference_jobs(job_id TEXT PRIMARY KEY);
                CREATE TABLE viewer_tasks(removed INTEGER NOT NULL, phase TEXT NOT NULL, stage TEXT NOT NULL);
                INSERT INTO viewer_tasks VALUES(0,'complete','succeeded');
                INSERT INTO viewer_tasks VALUES(0,'complete','failed');
                INSERT INTO viewer_tasks VALUES(0,'complete','cancelled');
                INSERT INTO viewer_tasks VALUES(0,'processing','running');
            """)
        original_lstat = Path.lstat
        def container_uid(path, *args, **kwargs):
            result = original_lstat(path, *args, **kwargs)
            if path == database:
                values = list(result); values[4] = 10001
                return os.stat_result(values)
            return result
        def census(source):
            output = io.StringIO()
            with patch("pathlib.Path.lstat", container_uid), redirect_stdout(output):
                exec(compile(source.replace("/source/jobs.sqlite3", str(database)), "<optimized-census>", "exec", optimize=2),
                    {"__name__": "__main__"})
            return json.loads(output.getvalue())
        self.assertEqual(census(self.install.OFFLINE_ROLLBACK_CENSUS)["nativeViewerTasks"], 4)
        with self.assertRaises(AssertionError):
            census(self.install.explicit_guard_source(self.install.OFFLINE_ROLLBACK_CENSUS))
        with sqlite3.connect(database) as db:
            db.execute("DELETE FROM viewer_tasks WHERE phase='processing'")
        self.assertEqual(census(self.install.explicit_guard_source(self.install.OFFLINE_ROLLBACK_CENSUS))["nativeViewerTasks"], 3)

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
        state["history"]["nativeViewerTasks"] = 0
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
            if action == "exec":
                self.assertEqual(args[:4], ("exec", "--user", "10001:10001", state["newContainerId"]))
                self.assertIn("mode=ro", args[-1])
                return json.dumps({"referenceJobs": fixture.get("reference_jobs", 0)})
            if action == "ps":
                if any(str(value).startswith("volume=") for value in args):
                    return "\n".join(value["Id"][:12] for value in containers.values() if value["State"]["Running"])
                own = [value["Id"][:12] for value in containers.values()
                       if value["Config"]["Labels"].get("org.ledgetop.reference.cutover") == state["operation"]]
                return "\n".join(own)
            if action == "run":
                self.assertEqual(args[args.index("--network") + 1], "none")
                self.assertEqual(args[args.index("--user") + 1], "10001:10001")
                self.assertIn("--read-only", args)
                self.assertIn("type=volume,src=" + install.VOLUME + ",dst=/source,readonly", args)
                if fixture.get("offline_unavailable"):
                    raise OSError("synthetic offline census unavailable")
                return json.dumps(dict(authorityId=fixture.get("offline_authority", install.AUTHORITY),
                    history=fixture.get("offline_history", state["history"]),
                    nativeViewerTasks=fixture.get("offline_native_tasks", state["history"]["nativeViewerTasks"]),
                    referenceJobs=fixture.get("offline_reference_jobs", 0)))
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
                if action == "stop" and item["Id"] == new_id and fixture.pop("admission_during_drain", False):
                    # The online precheck has passed; a request commits while
                    # shutdown is draining, before the offline immutable census.
                    fixture["offline_reference_jobs"] = 1
                    fixture["offline_history"] = {"count": 1, "sha256": "admitted-during-drain", "nativeViewerTasks": 0}
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

    def completed_fixture(self):
        fixture = self.migration_fixture()
        state = fixture["state"]
        state.update(phase="completed", newContainerId=fixture["new_id"])
        fixture["old"]["Name"] = "/" + state["rollbackName"]
        fixture["old"]["State"]["Running"] = False
        fixture["containers"][fixture["new_id"]] = dict(Id=fixture["new_id"],
            Name="/" + self.install.NAME, Image=state["image"],
            Config={"Labels": {"org.ledgetop.reference.cutover": state["operation"]}, "Env": []},
            State={"Running": True, "Health": {"Status": "healthy"}})
        return fixture

    def test_completed_rollback_changed_or_unverifiable_activity_has_zero_side_effects(self):
        for interrupted in (False, True):
            for fault in ("changed_history", "active_job", "unavailable", "reference_row"):
                with self.subTest(interrupted=interrupted, fault=fault):
                    fixture = self.completed_fixture()
                    state = fixture["state"]
                    if interrupted:
                        state.update(phase="rollback_pending", rollbackFromCompleted=True)
                    if fault == "reference_row":
                        fixture["reference_jobs"] = 1
                    before = copy.deepcopy(state)
                    self.install.save(self.root, state)
                    checkpoint_before = (self.root / "checkpoint.json").read_bytes()
                    with self.migration_patches(fixture), ExitStack() as extra:
                        if fault == "changed_history":
                            extra.enter_context(patch.object(self.install, "activity", return_value={
                                "count": 1, "sha256": "new-reference-job-history"}))
                        elif fault == "active_job":
                            extra.enter_context(patch.object(self.install, "activity",
                                side_effect=self.install.CutoverStopped("queue_not_idle")))
                        elif fault == "unavailable":
                            extra.enter_context(patch.object(self.install, "activity",
                                side_effect=OSError("synthetic verification unavailable")))
                        with patch.object(self.install, "save", wraps=self.install.save) as save:
                            with self.assertRaises(self.install.CutoverStopped):
                                self.install.rollback(self.root, state)
                            save.assert_not_called()
                    self.assertEqual(state, before)
                    self.assertEqual((self.root / "checkpoint.json").read_bytes(), checkpoint_before)
                    self.assertTrue(fixture["containers"][fixture["new_id"]]["State"]["Running"])
                    self.assertFalse(fixture["old"]["State"]["Running"])
                    self.assertFalse(any(call[0] in ("stop", "rename", "start") for call in fixture["calls"]))
                    self.assertEqual(fixture["operator_calls"], [])

    def test_completed_unused_install_allows_only_the_reviewed_rollback(self):
        fixture = self.completed_fixture()
        with self.migration_patches(fixture):
            result = self.install.rollback(self.root, fixture["state"])
        self.assertEqual(result["phase"], "rolled_back")
        self.assertTrue(result["rollbackFromCompleted"])
        self.assertTrue(fixture["old"]["State"]["Running"])
        self.assertEqual(fixture["old"]["Name"], "/" + self.install.NAME)
        self.assertFalse(fixture["containers"][fixture["new_id"]]["State"]["Running"])
        self.assertEqual(sum(value["State"]["Running"] for value in fixture["containers"].values()), 1)
        self.assertEqual([call[-1] for call in fixture["calls"] if call[0] == "stop"], [fixture["new_id"]])
        self.assertEqual([call[-1] for call in fixture["calls"] if call[0] == "start"], [fixture["old_id"]])

    def test_post_stop_admission_or_failed_census_retains_only_current_replacement(self):
        for completed in (False, True):
            for fault in ("admission_during_drain", "offline_unavailable", "offline_native_tasks", "offline_authority"):
                with self.subTest(completed=completed, fault=fault):
                    fixture = self.completed_fixture()
                    if not completed:
                        fixture["state"]["phase"] = "activation_failed"
                    fixture[fault] = "changed-authority" if fault == "offline_authority" else 1
                    with self.migration_patches(fixture):
                        result = self.install.rollback(self.root, fixture["state"])
                    self.assertEqual(result["phase"], "rollback_needs_inspection")
                    self.assertTrue(result["downgradeRefused"])
                    self.assertTrue(result["replacementHealthVerified"])
                    self.assertTrue(fixture["containers"][fixture["new_id"]]["State"]["Running"])
                    self.assertEqual(fixture["containers"][fixture["new_id"]]["Name"], "/" + self.install.NAME)
                    self.assertFalse(fixture["old"]["State"]["Running"])
                    self.assertEqual([call[-1] for call in fixture["calls"] if call[0] == "stop"], [fixture["new_id"]])
                    self.assertEqual([call[-1] for call in fixture["calls"] if call[0] == "start"], [fixture["new_id"]])
                    self.assertFalse(any(call[0] == "rename" for call in fixture["calls"]))
                    self.assertEqual(fixture["operator_calls"], [])

    def test_real_offline_sqlite_census_matches_online_history_and_detects_reference_rows(self):
        from processing.prototype import PROFILE
        fixture = self.migration_fixture()
        queue = fixture["queue"]
        job, _ = queue.submit({"requestId": "synthetic-census", "environment": "staging", "profile": PROFILE})
        claim = queue.claim({"workerId": "synthetic-worker", "profiles": [PROFILE]})
        queue.complete(job["id"], {"claimToken": claim["claimToken"], "status": "succeeded", "result": {"synthetic": True}})
        with queue._db() as db:
            db.execute("CREATE TABLE viewer_tasks (removed INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL, stage TEXT NOT NULL)")
            db.execute("INSERT INTO viewer_tasks VALUES(0,'complete','succeeded')")
            db.execute("CREATE TABLE reference_jobs (job_id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO reference_jobs VALUES('synthetic-retained-reference')")
        class API:
            origin = "http://127.0.0.1:18080"
            def json(self, method, route):
                if route == "/v1/prototype/authority":
                    return {"authorityId": queue.authority_id}
                if route == "/v1/prototype/jobs":
                    return {"jobs": queue.list()}
                raise AssertionError("unexpected census route")
        with patch.object(self.install, "AUTHORITY", queue.authority_id), \
                patch.object(self.install, "environment", return_value={}), \
                patch.object(self.install, "docker", return_value=json.dumps({"incompleteViewerTasks": 0, "viewerTaskCount": 1})):
            expected = self.install.activity(API())
        queue.close()
        original_lstat = Path.lstat
        def container_uid(path, *args, **kwargs):
            result = original_lstat(path, *args, **kwargs)
            if path == queue.path:
                values = list(result)
                values[4] = 10001
                return os.stat_result(values)
            return result
        # Only remap the source path and its container UID assertion. SQLite,
        # quick_check, actual table counts and both history projections are real.
        script = self.install.OFFLINE_ROLLBACK_CENSUS.replace("/source/jobs.sqlite3", str(queue.path))
        output = io.StringIO()
        with patch("pathlib.Path.lstat", container_uid), redirect_stdout(output):
            exec(compile(script, "<offline-census-fixture>", "exec"), {"__name__": "__main__"})
        census = json.loads(output.getvalue())
        self.assertEqual(census["authorityId"], queue.authority_id)
        self.assertEqual(census["history"], expected)
        self.assertEqual(census["referenceJobs"], 1)
        self.assertEqual(census["nativeViewerTasks"], 1)
        def docker(action, *args, **kwargs):
            if action == "ps":
                return ""
            self.assertEqual(action, "run")
            return json.dumps(census)
        with patch.object(self.install, "AUTHORITY", queue.authority_id), patch.object(self.install, "docker", docker):
            with self.assertRaises(self.install.CutoverStopped):
                self.install.offline_rollback_precheck({"history": expected, "image": "synthetic-cached-image"})


if __name__ == "__main__":
    unittest.main()
