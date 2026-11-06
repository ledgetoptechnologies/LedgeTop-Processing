"""Real updater/owned HTTP queue/managed path; synthetic Docker and engine."""
from dataclasses import replace
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch

from ops.update_maintenance_adapter import ScheduleWindow
from ops.update_service import readiness_probes
from processing.odx_smoke_worker import SmokeError
from processing.prototype import PROFILE
from processing.update_service import UpdateService
from tests import test_update_pipeline as pipeline


@unittest.skipUnless(sys.platform.startswith('linux'),'actual private managed/HTTP updater fixture')
class EarlyDrainPipelineTests(unittest.TestCase):
    def setUp(self):
        self.fixture=pipeline.UpdatePipelineTests('runTest')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.host=self.fixture.host
        self.queue=self.host.host.fixture.queue
        self.job=self.queue.submit(dict(requestId='finish-before-update',profile=PROFILE,environment='staging'))[0]
        self.claim=self.queue.claim(dict(workerId='original-worker',nodeId='cluster'))
        self.assertEqual(self.claim['id'],self.job['id'])
        self.host.window=ScheduleWindow(0,0,0,60,prepare_before_qualification=True,after_job_id=self.job['id'])
        self.host.state=self.fixture.root/'early-adapter.sqlite'
        self.fixture.restart()
        configuration=SimpleNamespace(supervisor_policy=self.fixture.supervisor.policy,
            registry_policy=self.fixture.registry_policy,raw={'supervisor':{'statePath':str(self.fixture.supervisor.path)}})
        idle,schedule=readiness_probes(configuration,self.host.adapter,self.host.api)
        self.service=UpdateService(self.fixture.root/'early-service.sqlite',supervisor=self.fixture.supervisor,
            config_id='exact-early-drain',code_binding_sha256='e'*64,capacity_admitter=lambda *_:True,
            capacity_releaser=lambda *_:False,idle_probe=idle,can_schedule=schedule,clock=lambda:100)

    def finish_job(self):
        self.queue.complete(self.job['id'],dict(claimToken=self.claim['claimToken'],status='succeeded',result={}))

    def test_exact_job_is_drained_before_any_pull_or_qualification_then_same_plan_updates(self):
        result=self.service.tick()
        self.assertEqual(result['status'],'waiting_idle',result)
        pending=self.fixture.supervisor.pending()
        self.assertEqual(pending['state'],'draining')
        node=self.queue.get_node('cluster');plan=node['planId']
        self.assertEqual(node['afterJobId'],self.job['id'])
        self.assertIsNone(node['targetEngineVersion'])
        self.assertEqual(self.fixture.docker.mutations(),[])
        self.assertEqual(self.host.host.adapter.calls,[])
        self.queue.submit(dict(requestId='wait-through-update',profile=PROFILE,environment='staging'))
        self.assertIsNone(self.queue.claim(dict(workerId='next',nodeId='cluster')))
        self.finish_job()
        result=self.service.tick()
        self.assertEqual(result['status'],'completed',result)
        final=self.fixture.supervisor.status(pending['operationId'])
        self.assertEqual(final['plan']['planId'],plan)
        self.assertEqual(final['drain']['afterJobId'],self.job['id'])
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        self.assertEqual(len(self.host.host.adapter.calls),1)
        self.assertEqual(self.fixture.docker.mutations(),['create','start','exec','stop','rm'])

    def test_lost_early_drain_reply_reconciles_readonly_without_repeating_post(self):
        original=self.host.operator.json;posts=[]
        def lost(method,path,payload=None,**kwargs):
            result=original(method,path,payload,**kwargs)
            if method=='POST' and payload and payload.get('schemaVersion')==2:
                posts.append(payload['requestId']);raise SmokeError('lost drain ACK')
            return result
        with patch.object(self.host.operator,'json',side_effect=lost):
            self.assertEqual(self.service.tick()['status'],'held')
            self.assertEqual(self.service.tick()['status'],'reconciled')
        self.assertEqual(len(posts),1)
        self.assertEqual(self.fixture.supervisor.pending()['state'],'draining')
        self.assertEqual(self.fixture.docker.mutations(),[])

    def test_lost_bind_reply_keeps_original_boundary_and_requires_lookup(self):
        self.assertEqual(self.service.tick()['status'],'waiting_idle')
        before=self.queue.get_node('cluster')
        self.finish_job()
        original=self.host.operator.json;posts=[]
        def lost(method,path,payload=None,**kwargs):
            result=original(method,path,payload,**kwargs)
            if method=='POST' and payload and payload.get('schemaVersion')==3:
                posts.append(payload['requestId']);raise SmokeError('lost bind ACK')
            return result
        with patch.object(self.host.operator,'json',side_effect=lost):
            self.assertEqual(self.service.tick()['status'],'held')
            self.assertEqual(self.service.tick()['status'],'reconciled')
        self.assertEqual(len(posts),1)
        self.assertEqual(self.host.host.adapter.calls,[])
        after=self.queue.get_node('cluster')
        for key in ('planId','afterJobId','drainAt','notBefore','targetImage'):self.assertEqual(after[key],before[key])
        self.assertEqual(self.service.tick()['status'],'completed')

    def test_qualification_failure_never_changes_container_or_reopens_node(self):
        self.assertEqual(self.service.tick()['status'],'waiting_idle')
        self.finish_job()
        self.fixture.docker.info['engine']='unknown'
        self.assertEqual(self.service.tick()['status'],'held')
        self.assertEqual(self.queue.get_node('cluster')['state'],'draining')
        self.assertIsNone(self.queue.get_node('cluster')['targetEngineVersion'])
        self.assertEqual(self.host.host.adapter.calls,[])

    def test_cancelled_drain_does_not_allow_staging_under_a_different_plan(self):
        self.assertEqual(self.service.tick()['status'],'waiting_idle')
        node=self.queue.get_node('cluster')
        self.queue.cancel_node('cluster',dict(planId=node['planId'],expectedRevision=node['revision']))
        self.finish_job()
        result=self.service.tick()
        self.assertIn(result['status'],('held','waiting_idle'))
        self.assertEqual(self.fixture.docker.mutations(),[])
        self.assertEqual(self.host.host.adapter.calls,[])
