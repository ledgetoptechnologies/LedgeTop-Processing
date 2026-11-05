"""Test-only identity seam; no Docker effects or phase acceptance claim."""
import copy
import json
import os
import importlib
from pathlib import Path
import tempfile
import unittest
import uuid

from ops import qualify_enrolled_phase_transport as q


@unittest.skipUnless(os.name == 'posix', 'Linux installer/lock fixture')
class FixtureProfileTests(unittest.TestCase):
    def setUp(self):
        self.installer = importlib.import_module('ops.install_reference_coordinator')
        self.operation = str(uuid.uuid4()); self.authority = str(uuid.uuid4())
        self.name = 'ltds-phase-original-' + uuid.uuid4().hex[:16]
        self.identity = 'b' * 64
        self.original = dict(Id=self.identity, Image=q.journal.PREDECESSOR_IMAGE,
            Name='/' + self.name, Config={'Labels': {q.TASK_LABEL: self.operation}},
            Mounts=[dict(Destination='/prototype-state', Type='volume',
                         Name=self.name + '-state', RW=True)])
        original = self.original; operation = self.operation; name = self.name
        class Docker:
            def inspect(self, identity):
                assert identity == original['Id']
                return copy.deepcopy(original)
            def command(self, *args, **kwargs):
                assert args == ('volume', 'inspect', name + '-state')
                return json.dumps([dict(Name=name + '-state', Labels={q.TASK_LABEL: operation})]).encode()
        self.docker = Docker()
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)

    def profile(self, **changes):
        args = dict(operation_id=self.operation, predecessor_id=self.identity,
            authority_id=self.authority, original_name=self.name, state_volume=self.name + '-state',
            original_image_id=q.journal.PREDECESSOR_IMAGE,
            original_source=q.journal.PREDECESSOR_SOURCE, operation_directory=self.temp.name)
        args.update(changes)
        return q.fixture_enrollment(self.docker, **args)

    def test_owned_profile_keeps_real_image_and_restores_all_globals_after_error(self):
        original = (q.journal.PREDECESSOR_ID, q.journal.AUTHORITY, q.journal.NAME,
                    q.deployment.PREDECESSOR_ID, q.deployment.NAME, self.installer.LOCK_PATH)
        with self.assertRaisesRegex(RuntimeError, 'controlled fixture interruption'):
            with self.profile() as facts:
                self.assertEqual(q.journal.PREDECESSOR_ID, self.identity)
                self.assertEqual(q.journal.AUTHORITY, self.authority)
                self.assertEqual(q.deployment.NAME, self.name)
                self.assertFalse(facts['productionHostEnrollmentQualified'])
                self.assertEqual(self.installer.LOCK_PATH, Path(self.temp.name) / 'fixture-install.lock')
                raise RuntimeError('controlled fixture interruption')
        self.assertEqual(original, (q.journal.PREDECESSOR_ID, q.journal.AUTHORITY, q.journal.NAME,
                         q.deployment.PREDECESSOR_ID, q.deployment.NAME, self.installer.LOCK_PATH))

    def test_foreign_task_label_never_changes_enrollment(self):
        identity = q.journal.PREDECESSOR_ID
        self.original['Config']['Labels'][q.TASK_LABEL] = str(uuid.uuid4())
        with self.assertRaisesRegex(RuntimeError, 'task_owned_original_fixture_required'):
            with self.profile():
                self.fail('foreign fixture entered')
        self.assertEqual(q.journal.PREDECESSOR_ID, identity)

    def test_source_overlay_and_arbitrary_name_refused(self):
        for changes in ({'original_image_id': 'sha256:' + 'a' * 64},
                        {'original_source': 'a' * 40}, {'original_name': 'production-name'}):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                with self.profile(**changes):
                    self.fail('unsupported fixture entered')


@unittest.skipUnless(os.name == 'posix', 'actual Linux readonly worker source')
class ActualWorkerFixtureTests(unittest.TestCase):
    def test_normal_factory_tls_readiness_is_bound_and_stable(self):
        operation,authority=str(uuid.uuid4()),str(uuid.uuid4())
        material=q.configuration_material(operation,authority)
        with tempfile.TemporaryDirectory() as temporary:
            with q.LocalWorkerReadinessFixtures(Path(temporary)/'workers',material,authority) as workers:
                first=workers.ready();second=workers.ready()
                self.assertEqual(first,second)
                self.assertEqual([item['nodeId'] for item in first],['cluster','main'])
                self.assertTrue(all(item['engineVersion']=='3.8.5' for item in first))
                self.assertTrue(all(item['driverBindingSha256'] and item['loadedSourceSha256'] for item in first))
                # The actual committed engine pin, rather than an injected DTO,
                # must control the live TLS readiness response.
                import sqlite3
                instance=workers.instances[0]
                with sqlite3.connect(instance['root']/'state'/'driver.sqlite') as db:
                    db.execute("UPDATE driver_engine_pin SET pin='wrong-fixture-pin'")
                with self.assertRaises(RuntimeError):workers.ready()


if __name__ == '__main__':
    unittest.main()
