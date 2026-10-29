"""Host-only immutable artifact/identity checks. No real Docker calls."""
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from processing import compose_execution_registry as registry
from processing.container_update import ComposeNode, UpdateError, Uncertain

A, B, C = ('sha256:' + value * 64 for value in ('a', 'b', 'c'))


class OwnedComposeExecutionTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve(); self.root.chmod(0o700)
        self.source = self.root / 'compose.json'
        self.model = dict(name='qa', services=dict(node=dict(image=A, pull_policy='never', ports=['127.0.0.1:24783:3000'])))
        self.write_source(self.model)
        self.operation = self.root / 'operation-one'; self.operation.mkdir(mode=0o700)

    def write_source(self, model):
        self.source.write_bytes(registry.canonical(model)); self.source.chmod(0o600)

    def context(self, model=None, project='qa', source=None):
        return registry.ComposeExecutionRegistry(source or self.source, project, 'node', model or self.model)

    def resolved(self, image, model=None):
        value = copy.deepcopy(model or self.model); value['services']['node'].update(image=image, pull_policy='never')
        return value

    def test_first_update_uses_actual_registered_path_and_preserves_source_identity(self):
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        artifact = Path(proof['executed']['path'])
        self.assertNotEqual(artifact, self.source)
        self.assertEqual(proof['source']['sha256'], hashlib.sha256(self.source.read_bytes()).hexdigest())
        self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(), proof['executed']['sha256'])
        self.assertEqual(self.context().verify(str(artifact), B), proof)

    def test_second_update_and_rollback_reuse_exact_retained_artifacts(self):
        old = self.context().reserve(self.resolved(A), A, self.operation)
        first = self.context().reserve(self.resolved(B), B, self.operation)
        next_model = self.resolved(B); self.write_source(next_model)
        operation = self.root / 'operation-two'; operation.mkdir(mode=0o700)
        second_context = self.context(next_model)
        self.assertEqual(second_context.verify(first['executed']['path'], B), first)
        second = second_context.reserve(self.resolved(C, next_model), C, operation)
        rollback = second_context.reserve(self.resolved(A, next_model), A, operation)
        self.assertEqual(rollback, old)
        self.assertNotEqual(second['executed']['path'], first['executed']['path'])
        self.assertEqual(len(list((self.root / registry.DIRECTORY / 'artifacts').iterdir())), 3)

    def test_same_engine_version_new_image_still_has_distinct_execution_identity(self):
        # Registry does not invent or compare engineVersion; image identity is
        # independent. The existing runtime-publication CAS owns version/epoch.
        first = self.context().reserve(self.resolved(A), A, self.operation)
        next_proof = self.context().reserve(self.resolved(B), B, self.operation)
        self.assertNotEqual(first['executed']['sha256'], next_proof['executed']['sha256'])

    def test_wrong_receipt_path_source_project_service_or_image_holds(self):
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        wrong = self.operation / 'resolved-compose.json'; wrong.write_bytes(Path(proof['executed']['path']).read_bytes()); wrong.chmod(0o600)
        for path, image in ((str(wrong), B), (proof['executed']['path'], C), ('../' + proof['executed']['path'], B)):
            with self.subTest(path=path), self.assertRaises(ValueError): self.context().verify(path, image)
        with self.assertRaises(ValueError): self.context(project='foreign').verify(proof['executed']['path'], B)
        other = self.root / 'other.json'; other.write_bytes(self.source.read_bytes()); other.chmod(0o600)
        with self.assertRaises(ValueError): self.context(source=other).verify(proof['executed']['path'], B)
        with self.assertRaises(ValueError): registry.ComposeExecutionRegistry(self.source, 'qa', 'foreign', self.model).verify(proof['executed']['path'], B)

    def test_source_recipe_or_registered_content_change_holds(self):
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        changed = copy.deepcopy(self.model); changed['services']['node']['ports'] = ['127.0.0.1:24785:3000']
        self.write_source(changed)
        with self.assertRaises(ValueError): self.context(changed).verify(proof['executed']['path'], B)
        self.write_source(self.model)
        artifact = Path(proof['executed']['path']); artifact.write_bytes(artifact.read_bytes() + b' ')
        with self.assertRaises(ValueError): self.context().verify(str(artifact), B)

    def test_reservation_precedes_artifact_and_same_owned_interruption_can_resume(self):
        original = registry.once
        def stop(path, raw):
            if path.parent.name == 'artifacts': raise OSError('stop before artifact creation')
            return original(path, raw)
        with patch.object(registry, 'once', side_effect=stop), self.assertRaises(OSError):
            self.context().reserve(self.resolved(B), B, self.operation)
        self.assertEqual(len(list((self.root / registry.DIRECTORY / 'reservations').iterdir())), 1)
        other = self.root / 'foreign-operation'; other.mkdir(mode=0o700)
        with self.assertRaises(ValueError): self.context().reserve(self.resolved(B), B, other)
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        self.assertEqual(self.context().verify(proof['executed']['path'], B), proof)

    def test_partial_or_unknown_registry_is_never_adopted(self):
        root = self.root / registry.DIRECTORY; root.mkdir(mode=0o700)
        (root / 'legacy').write_bytes(b'unknown')
        with self.assertRaises((ValueError, FileNotFoundError)): self.context().reserve(self.resolved(B), B, self.operation)

    def test_unfinished_reservation_refuses_replaced_operation_root_inode(self):
        original = registry.once
        def stop(path, raw):
            if path.parent.name == 'artifacts': raise OSError('before artifact creation')
            return original(path, raw)
        with patch.object(registry, 'once', side_effect=stop), self.assertRaises(OSError):
            self.context().reserve(self.resolved(B), B, self.operation)
        retained = self.root / 'retained-original-operation'
        self.operation.rename(retained)
        self.operation.mkdir(mode=0o700)
        self.assertNotEqual(self.operation.stat().st_ino, retained.stat().st_ino)
        reservations = self.root / registry.DIRECTORY / 'reservations'
        before = {p.name: p.read_bytes() for p in reservations.iterdir()}
        with self.assertRaises(ValueError):
            self.context().reserve(self.resolved(B), B, self.operation)
        self.assertEqual({p.name: p.read_bytes() for p in reservations.iterdir()}, before)
        self.assertEqual(list((self.root / registry.DIRECTORY / 'artifacts').iterdir()), [])

    def test_history_cap_holds_without_pruning_previous_artifact(self):
        first = self.context().reserve(self.resolved(A), A, self.operation)
        with patch.object(registry, 'MAX_ENTRIES', 1), self.assertRaises(ValueError):
            self.context().reserve(self.resolved(B), B, self.operation)
        self.assertTrue(Path(first['executed']['path']).is_file())
        with patch.object(registry, 'MAX_ENTRIES', 1):
            self.assertEqual(self.context().reserve(self.resolved(A), A, self.operation), first)

    def test_registered_artifact_before_registration_checkpoint_resumes_same_operation(self):
        original = registry.once
        def stop(path, raw):
            if path.parent.name == 'registrations': raise OSError('stop after artifact fsync')
            return original(path, raw)
        with patch.object(registry, 'once', side_effect=stop), self.assertRaises(OSError):
            self.context().reserve(self.resolved(B), B, self.operation)
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        self.assertEqual(self.context().verify(proof['executed']['path'], B), proof)

    def test_partial_artifact_is_retained_and_held_without_overwrite(self):
        original = registry.once
        def stop(path, raw):
            if path.parent.name == 'artifacts':
                original(path, raw[:len(raw)//2]); raise OSError('interrupted artifact')
            return original(path, raw)
        with patch.object(registry, 'once', side_effect=stop), self.assertRaises(OSError):
            self.context().reserve(self.resolved(B), B, self.operation)
        artifact = next((self.root / registry.DIRECTORY / 'artifacts').iterdir()); before = artifact.read_bytes()
        with self.assertRaises(ValueError): self.context().reserve(self.resolved(B), B, self.operation)
        self.assertEqual(artifact.read_bytes(), before)

    def test_orphan_artifact_and_incomplete_registry_creation_hold(self):
        proof = self.context().reserve(self.resolved(B), B, self.operation)
        orphan = self.root / registry.DIRECTORY / 'artifacts' / ('f'*64 + '.json')
        orphan.write_bytes(b'{}'); orphan.chmod(0o600)
        with self.assertRaises(ValueError): self.context().verify(proof['executed']['path'], B)
        self.assertTrue(orphan.exists())

    def test_worker_path_mandates_provenance_without_a_configuration_downgrade(self):
        from ops.reference_worker_maintenance import WorkerComposeNode
        args = self.adapter().args
        worker = WorkerComposeNode(args, self.operation, object())
        self.assertIs(worker.owned_recipe_provenance, True)
        with self.assertRaises(TypeError): WorkerComposeNode(args, self.operation, object(), owned_recipe_provenance=False)

    def adapter(self):
        args = SimpleNamespace(project='qa', service='node', compose_file=str(self.source), expected_engine='odm',
            upload_layout='var-www-tmp-v1', process_layout='node-var-www-v1', node_origin='http://127.0.0.1:24783', node_token='')
        adapter = ComposeNode(args, self.operation, owned_recipe_provenance=True)
        adapter.engine = lambda: None; adapter.image = lambda _image: None
        def docker(command, **kwargs):
            if command[-3:] == ['config', '--format', 'json']:
                value = json.loads(self.source.read_bytes())
                if command.count('-f') == 2:
                    override = json.loads(Path(command[command.index('-f', command.index('-f') + 1) + 1]).read_bytes())
                    value['services']['node'].update(override['services']['node'])
                return json.dumps(value)
            raise Uncertain('synthetic lost execution response')
        adapter.docker = docker
        return adapter

    def test_prepare_executes_registered_bytes_not_original_or_receipt_recipe(self):
        adapter = self.adapter(); command = adapter.prepare(B)
        executed = command[command.index('-f') + 1]
        self.assertIn('/' + registry.DIRECTORY + '/artifacts/', executed.replace('\\', '/'))
        proof = adapter.configuration_identity({'com.docker.compose.project.config_files': executed}, B)
        self.assertEqual(proof['mode'], 'registered-execution')
        self.assertEqual(json.loads(Path(executed).read_bytes())['services']['node']['image'], B)

    def test_lost_execution_response_retains_exact_recipe_and_original_source(self):
        adapter = self.adapter(); before = self.source.read_bytes()
        with self.assertRaises(Uncertain): adapter.replace(B)
        receipt = json.loads((self.operation / 'compose-execution.json').read_bytes())
        artifact = Path(receipt['registry']['executed']['path'])
        self.assertTrue(artifact.is_file()); self.assertEqual(self.source.read_bytes(), before)
        self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(), receipt['registry']['executed']['sha256'])


if __name__ == '__main__':
    unittest.main()
