"""Real owned-schedule HTTP and managed finalization, fake host transports only."""
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import os
import unittest
from unittest.mock import patch

from ops import update_maintenance_adapter as module
from ops.node_ingress import save
from processing.odx_smoke_worker import SmokeError
from processing.update_supervisor import UpdateSupervisor
from tests import test_managed_update_finalization as managed_fixture
from tests import test_container_update as update_fixture


@unittest.skipIf(os.name=='nt','actual managed maintenance requires Linux')
class UpdateMaintenanceAdapterTests(unittest.TestCase):
    def setUp(self):
        self.host=managed_fixture.ManagedFinalizationTests(methodName='runTest')
        self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.root,self.api,self.operator=self.host.root,self.host.api,self.host.operator
        node=self.host.fixture.queue.get_node('cluster')
        self.operator.json('POST','/v1/prototype/nodes/cluster/cancel',dict(planId=node['planId'],expectedRevision=node['revision']))
        settings=json.loads((self.root/'maintenance.json').read_text());settings['coordinator']=self.api.origin
        save(self.root/'maintenance.json',settings)
        save(self.host.ingress.root/'gateway.json',self.host.ingress.config)
        self.now=datetime.now(timezone.utc)
        self.host.fixture.queue._clock=lambda:self.now
        self.window=module.ScheduleWindow(0,0,0,60)
        self.receipts=self.root/'adapter-receipts'
        self.state=self.root/'maintenance-adapter.sqlite'
        ingress=patch.object(module,'Ingress',return_value=self.host.ingress);ingress.start();self.addCleanup(ingress.stop)
        def compose(args,work):
            self.host.adapter.args=args
            return self.host.adapter
        adapter=patch.object(module,'ComposeNode',side_effect=compose);adapter.start();self.addCleanup(adapter.stop)
        self.adapter=self.open()
        self.operation='a'*64
        for kind in ('schedule','run'):
            (self.receipts/self.operation).mkdir(mode=0o700,exist_ok=True)
            (self.receipts/self.operation/kind).mkdir(mode=0o700)
        self.schedule_request=self.request('schedule')

    def open(self,**kwargs):
        return module.UpdateMaintenanceAdapter(self.root,state_path=self.state,receipt_root=self.receipts,
            api=self.api,operator=self.operator,node_id='cluster',authority_id=self.host.fixture.queue.authority_id,
            engine_id='synthetic-engine',adapter_id='real-maintenance-v1',window=kwargs.get('window',self.window),clock=lambda:self.now)

    def request(self,kind,plan=None):
        value=dict(schemaVersion=1,operationId=self.operation,nodeId='cluster',engineId='synthetic-engine',
            authorityId=self.host.fixture.queue.authority_id,candidateId='b'*64,repository='registry.example/namespace/engine',
            manifestDigest='sha256:'+'d'*64,configDigest=update_fixture.NEW,localImageId=update_fixture.NEW,platform='linux/amd64',
            previousImageId=update_fixture.OLD,hostGeneration=0,supervisorPolicySha256='c'*64,
            qualifierId='separate-qualifier',qualificationPolicySha256='e'*64,qualificationSha256='f'*64,
            targetEngineVersion=update_fixture.NEW_ENGINE_VERSION,
            receiptDirectory=str(self.receipts/self.operation/kind))
        if kind=='run':value['plan']=plan
        value['requestSha256']=module.digest(value)
        return value

    def scheduled(self):
        plan=self.adapter.schedule(self.schedule_request)
        self.assertEqual(UpdateSupervisor._receipt(plan,self.schedule_request,'schedule'),plan)
        return self.request('run',plan)

    def db_hash(self):return hashlib.sha256(self.state.read_bytes()).hexdigest()

    def test_actual_host_owned_schedule_and_managed_run_seal_exact_protocol(self):
        self.assertEqual(self.adapter.inspect_host(),dict(nodeId='cluster',engineId='synthetic-engine',
            authorityId=self.host.fixture.queue.authority_id,imageId=update_fixture.OLD))
        request=self.scheduled()
        self.assertIs(self.adapter.ready_to_run(request),True)
        result=self.adapter.run(request)
        self.assertEqual(UpdateSupervisor._receipt(result,request,'run'),result)
        self.assertEqual(self.host.adapter.calls,[update_fixture.NEW])
        self.assertEqual(self.host.ingress.opens,1)
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['state'],'active')
        private=json.loads((Path(request['receiptDirectory'])/'update.json').read_text())
        for secret in (private['updateToken'],self.api.token,self.operator.token,self.host.ingress.config['backendToken']):
            self.assertNotIn(secret,json.dumps(result))
        before=self.db_hash()
        with patch.object(module.managed,'run',side_effect=AssertionError('lookup cannot call run')), \
                patch.object(self.host.ingress,'change',side_effect=AssertionError('lookup cannot reload ingress')):
            self.assertEqual(self.adapter.lookup_completion(request),result)
        self.assertEqual(self.db_hash(),before)

    def test_lost_schedule_response_restart_only_reads_exact_owned_receipt(self):
        original=self.operator.json;posts=[]
        def lost(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if method=='POST' and route.endswith('/schedule-owned'):
                posts.append(payload);raise SmokeError('synthetic lost schedule response')
            return value
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.schedule(self.schedule_request)
            self.adapter=self.open()
            before=self.db_hash()
            plan=self.adapter.lookup_schedule(self.schedule_request)
            self.assertEqual(self.db_hash(),before)
            self.assertEqual(self.adapter.schedule(self.schedule_request),plan)
        self.assertEqual(len(posts),1)
        with self.adapter._db() as db:
            wire=json.loads(db.execute('SELECT wire FROM schedule_intents').fetchone()[0])
        self.assertEqual(wire,posts[0])

    def test_no_server_receipt_after_unknown_schedule_does_not_post_again(self):
        posts=[];original=self.operator.json
        def lost(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/schedule-owned'):
                posts.append(payload);raise SmokeError('synthetic unknown pre-response outcome')
            return original(method,route,payload,**kwargs)
        with patch.object(self.operator,'json',side_effect=lost):
            for _ in range(2):
                with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.schedule(self.schedule_request)
        self.assertEqual(len(posts),1)
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['state'],'active')

    def test_future_and_busy_readiness_are_read_only_and_survive_restart(self):
        # Use a new unused state journal for an explicitly different window.
        self.window=module.ScheduleWindow(10,20,0,60);self.state=self.root/'future-adapter.sqlite'
        self.adapter=self.open();request=self.scheduled()
        before=self.db_hash()
        self.assertFalse(self.adapter.ready_to_run(request))
        self.adapter=self.open();self.assertFalse(self.adapter.ready_to_run(request))
        self.assertEqual(self.db_hash(),before)
        self.now+=timedelta(seconds=30)
        self.host.adapter.busy=True
        self.assertFalse(self.adapter.ready_to_run(request))
        self.host.adapter.busy=False
        self.assertTrue(self.adapter.ready_to_run(request))
        with self.adapter._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM run_intents').fetchone()[0],0)
        self.assertEqual(self.host.adapter.calls,[])

    def test_peer_plan_and_cancelled_plan_are_not_run_or_rescheduled(self):
        request=self.scheduled();node=self.host.fixture.queue.get_node('cluster')
        self.operator.json('POST','/v1/prototype/nodes/cluster/cancel',dict(planId=node['planId'],expectedRevision=node['revision']))
        with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.ready_to_run(request)
        peer=self.host.fixture.plan()
        for call in (lambda:self.adapter.lookup_schedule(self.schedule_request),lambda:self.adapter.run(request)):
            with self.assertRaises(module.MaintenanceAdapterHeld):call()
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['planId'],peer['planId'])
        self.assertEqual(self.host.adapter.calls,[])

    def test_readiness_final_node_readback_must_still_match_owned_plan(self):
        request=self.scheduled();original=self.api.json;reads=0
        def replaced(method,route,payload=None,**kwargs):
            nonlocal reads
            value=original(method,route,payload,**kwargs)
            if method=='GET' and route=='/v1/prototype/nodes/cluster':
                reads+=1
                if reads==2:
                    value=json.loads(json.dumps(value))
                    value['node']['planId']='00000000-0000-4000-8000-000000000001'
            return value
        before=self.db_hash()
        with patch.object(self.api,'json',side_effect=replaced):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.ready_to_run(request)
        self.assertEqual(self.db_hash(),before)
        self.assertEqual(self.host.adapter.calls,[])

    def test_changed_authority_or_live_client_is_refused_without_host_mutation(self):
        original=self.api.json
        def changed(method,route,payload=None,**kwargs):
            if route=='/v1/prototype/authority':return {'authorityId':'00000000-0000-4000-8000-000000000001'}
            return original(method,route,payload,**kwargs)
        with patch.object(self.api,'json',side_effect=changed):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.inspect_host()
        with patch.object(self.api,'token','changed-private-credential'):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.inspect_host()
        self.assertEqual(self.host.adapter.calls,[])

    def test_manifest_runtime_id_is_scheduled_pinned_and_finished_without_using_config_id(self):
        # Fake host replacement target is the actual local manifest ID; the
        # registry config blob is a different artifact identity.
        self.schedule_request['configDigest']='sha256:'+'2'*64
        self.schedule_request['requestSha256']=module.digest({key:value for key,value in self.schedule_request.items() if key!='requestSha256'})
        plan=self.adapter.schedule(self.schedule_request)
        request=dict(self.schedule_request,receiptDirectory=str(self.receipts/self.operation/'run'),plan=plan)
        request['requestSha256']=module.digest({key:value for key,value in request.items() if key!='requestSha256'})
        self.assertEqual(plan['targetImageId'],update_fixture.NEW)
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['targetImage'],update_fixture.NEW)
        self.assertTrue(self.adapter.ready_to_run(request))
        result=self.adapter.run(request)
        self.assertEqual(result['configDigest'],'sha256:'+'2'*64)
        self.assertEqual((result['localImageId'],result['targetImageId'],result['imageId']),
            (update_fixture.NEW,update_fixture.NEW,update_fixture.NEW))
        self.assertEqual(self.host.adapter.calls,[update_fixture.NEW])
        self.assertEqual(self.adapter.lookup_completion(request),result)

    def test_missing_local_image_identity_cannot_fall_back_to_config_digest(self):
        request=dict(self.schedule_request);request.pop('localImageId')
        request['requestSha256']=module.digest({key:value for key,value in request.items() if key!='requestSha256'})
        with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.schedule(request)
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.host.adapter.calls,[])

    def test_managed_success_response_loss_reconstructs_read_only_completion(self):
        request=self.scheduled();original=module.managed.run
        def lost(*args,**kwargs):
            original(*args,**kwargs)
            raise OSError('synthetic adapter response loss')
        with patch.object(module.managed,'run',side_effect=lost):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.run(request)
        self.adapter=self.open();before=self.db_hash()
        with patch.object(module.managed,'run',side_effect=AssertionError('read-only lookup')):
            receipt=self.adapter.lookup_completion(request)
        self.assertTrue(receipt['coordinatorFinished'])
        self.assertEqual(self.db_hash(),before)
        self.assertEqual(self.host.adapter.calls,[update_fixture.NEW])

    def known_no_completion(self,status):
        request=self.scheduled()
        with patch.object(module.managed,'run',return_value={'status':status}) as run:
            for _ in range(2):
                with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.run(request)
            self.assertEqual(run.call_count,1)
        self.assertIsNone(self.adapter.lookup_completion(request))
        self.assertEqual(self.host.adapter.calls,[])

    def test_known_waiting_after_run_intent_never_blindly_runs_again(self):self.known_no_completion('waiting')
    def test_known_noteligible_after_run_intent_never_blindly_runs_again(self):self.known_no_completion('not_eligible')

    def test_unknown_replacement_never_infers_completion_from_target_image(self):
        request=self.scheduled();self.host.adapter.uncertain=True
        with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.run(request)
        self.assertEqual(self.host.adapter.current,update_fixture.NEW)
        self.assertIsNone(self.adapter.lookup_completion(request))
        with patch.object(module.managed,'run',side_effect=AssertionError('no replacement replay')):
            with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.run(request)
        self.assertEqual(self.host.adapter.calls,[update_fixture.NEW])

    def test_changed_request_directory_or_frozen_policy_fails_before_actions(self):
        changed=dict(self.schedule_request,receiptDirectory=str(self.root))
        changed['requestSha256']=module.digest({key:value for key,value in changed.items() if key!='requestSha256'})
        with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.schedule(changed)
        self.scheduled()
        changed=dict(self.schedule_request,qualificationSha256='1'*64)
        changed['requestSha256']=module.digest({key:value for key,value in changed.items() if key!='requestSha256'})
        with self.assertRaises(module.MaintenanceAdapterHeld):self.adapter.schedule(changed)
        with self.assertRaises(module.MaintenanceAdapterHeld):self.open(window=module.ScheduleWindow(1,2,0,60))
        self.assertEqual(self.host.adapter.calls,[])

    def test_legacy_unbound_request_is_not_upgraded_from_live_backend(self):
        request=dict(self.schedule_request);request.pop('targetEngineVersion')
        request['requestSha256']=module.digest({key:value for key,value in request.items() if key!='requestSha256'})
        with self.assertRaisesRegex(module.MaintenanceAdapterHeld,'closed_supervisor_request'):
            self.adapter.schedule(request)
        self.assertEqual(self.host.adapter.calls,[])
        self.assertEqual(self.host.fixture.queue.get_node('cluster')['state'],'active')
