"""Inert manifest checks: no Docker, SSH, listener or service action."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ops import qualify_worker_maintenance_runtime as lab


class RuntimePlanTests(unittest.TestCase):
    def fixture(self):
        return dict(engineId=lab.ENGINE,uid=1000,containers=[dict(id=i) for i in lab.BASELINE],
            ports=[dict(port=p,loopbackBindAvailable=True) for p in lab.PORTS.values()],
            cachedImages=[dict(id=lab.WORKER),dict(id=lab.NGINX)]),dict(backendCandidates=[dict(id=i) for i in lab.BACKENDS])

    def test_plan_is_inert_and_draft_refuses_before_any_docker_observation(self):
        plan=lab.make_plan(*self.fixture())
        self.assertFalse(plan['prerequisitesSatisfied']);self.assertIsNone(plan['sourceCommit'])
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'plan.json';path.write_text(json.dumps(plan))
            with patch.object(lab,'docker_read',side_effect=AssertionError('draft must not reach Docker')):
                with self.assertRaisesRegex(ValueError,'draft'):lab.run_owned(path,Path(temp)/'receipt')

    def test_probes_are_network_none_and_never_publish_or_mount_host_data(self):
        plan=lab.make_plan(*self.fixture())
        self.assertNotEqual(*lab.BACKENDS)
        for probe in plan['probeCommands']:
            args=probe['create'];self.assertIn('--network=none',args);self.assertIn('--pull=never',args)
            self.assertNotIn('-p',args);self.assertNotIn('--publish',args);self.assertNotIn('--mount',args)
            self.assertIn('NVIDIA_VISIBLE_DEVICES=void',args)
            self.assertEqual(args[-3],probe['image'])
            self.assertNotIn('--read-only',args);self.assertIn('max-file=2',args)
            self.assertNotIn('max-file=1',args);self.assertIn('create',args);self.assertNotIn('run',args)
            self.assertEqual(args[-2:],['--config','/var/www/qa-probe.json'])
            self.assertEqual(probe['configuration']['logger']['logDirectory'],'/var/www/logs')
            self.assertEqual(probe['configuration']['logger']['maxFiles'],2)
            self.assertEqual(probe['start'][-1],'<exact-created-CID>')
            self.assertEqual(probe['copyConfiguration'][-1],'<exact-created-CID>:/var/www/')
            self.assertEqual(probe['lifetimeSecondsMaximum'],90)
            self.assertNotIn('/task/new',probe['observe'][-1])

    def test_full_recipe_and_private_mounts_reference_only_new_namespace(self):
        plan=lab.make_plan(*self.fixture());node=plan['proposedCompose']['services']['node']
        self.assertEqual(node['ports'],['127.0.0.1:24783:3000'])
        self.assertFalse(node['read_only']);self.assertEqual(node['command'],['--config','/run/ltds-backend.json'])
        self.assertEqual(node['volumes'],[lab.ROOT+'/managed/backend-config.json:/run/ltds-backend.json:ro'])
        self.assertEqual(node['logging']['options']['max-file'],'2')
        self.assertIn('--read-only',plan['proposedSetupCommands']['workerCreate'])
        self.assertEqual(plan['listenerAddress'],'127.0.0.1')
        self.assertEqual(len(plan['workerMounts']),4)
        self.assertTrue(all(m['source'].startswith(lab.ROOT+'/') for m in plan['workerMounts']))
        self.assertFalse(any('docker.sock' in m['source'] for m in plan['workerMounts']))

    def test_preflight_missing_image_busy_port_or_changed_baseline_holds(self):
        for key in ('image','port','baseline','engine'):
            preflight,cache=self.fixture()
            if key=='image':cache['backendCandidates'].pop()
            elif key=='port':preflight['ports'][0]['loopbackBindAvailable']=False
            elif key=='baseline':preflight['containers'].pop()
            else:preflight['engineId']='foreign'
            with self.subTest(key=key),self.assertRaises(ValueError):lab.make_plan(preflight,cache)


if __name__=='__main__':unittest.main()
