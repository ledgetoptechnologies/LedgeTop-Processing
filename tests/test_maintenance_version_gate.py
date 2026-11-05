"""Version and worker ownership fences; no Docker/host mutations."""
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ops.node_ingress import GateError
from ops.node_maintenance import owned_open_proof, run
from processing.container_update import ComposeNode, UpdateError, reviewed_worker_mode


class VersionBoundaryTests(unittest.TestCase):
    def test_known_reference_installation_vetoes_explicit_legacy_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            self.assertEqual(reviewed_worker_mode('legacy',home=root),'legacy')
            installed=root/'.config/ledgetop-processing/reference-worker.json'
            installed.parent.mkdir(parents=True)
            installed.write_text('{}')
            with self.assertRaisesRegex(UpdateError,'reference_worker_handover_unimplemented'):
                reviewed_worker_mode('legacy',home=root)

    def test_matching_image_and_engine_do_not_substitute_for_exact_version(self):
        adapter = ComposeNode(SimpleNamespace(project='owned',service='node',compose_file='unused',
            node_origin='http://127.0.0.1:3000',expected_engine='odx'),Path('.'))
        adapter.node = MagicMock()
        adapter.node.json.return_value = dict(engine='odx',engineVersion='old')
        with patch.object(adapter,'inspect',return_value=dict(Image='target',State=dict(Running=True))):
            self.assertFalse(adapter.serving('target'), 'unbound version must not pass')
            self.assertTrue(adapter.image_serving('target'), 'read-only baseline inventory remains available')
            adapter.bind_engine_version('new')
            self.assertFalse(adapter.serving('target'), 'right image with wrong version must not pass')
            adapter.node.json.return_value['engineVersion']='new'
            self.assertTrue(adapter.serving('target'))

    def test_equal_public_and_backend_versions_must_also_match_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'state.json').write_text(json.dumps(dict(state='open',containerId='proxy',
                expectedBackendImage='target',oldWorkers=[])))
            ingress=MagicMock(root=root)
            ingress.inspect.return_value=dict(Id='proxy')
            ingress.workers.return_value=set()
            ingress.backend.return_value=dict(engine='odx',engineVersion='old',version='2.4')
            ingress.http.return_value=(200,dict(ingress.backend.return_value))
            state=dict(targetImage='target',targetEngineVersion='new',closedIngress=dict(containerId='proxy'))
            with self.assertRaisesRegex(GateError,'target engine version'):
                owned_open_proof(ingress,state)
            ingress.backend.return_value['engineVersion']='new'
            ingress.http.return_value=(200,dict(ingress.backend.return_value))
            self.assertEqual(owned_open_proof(ingress,state)['state'],'open')

    def test_unreviewed_or_reference_mode_refuses_before_ingress_or_host_actions(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for mode,reason in ((None,'worker_mode_unbound'),('reference','reference_worker_handover_unimplemented')):
                with self.subTest(mode=mode):
                    (root/'maintenance.json').write_text(json.dumps(dict(nodeId='cluster',workerMode=mode)))
                    with patch('ops.node_maintenance.Ingress') as ingress, self.assertRaisesRegex(UpdateError,reason):
                        run(root,MagicMock(),MagicMock(),root,0)
                    ingress.assert_not_called()


if __name__ == '__main__':
    unittest.main()
