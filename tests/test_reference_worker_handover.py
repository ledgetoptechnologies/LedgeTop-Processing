"""Real worker factory/TLS/SQLite/gates; synthetic systemd/Docker transports."""
import json
from contextlib import closing
import os
from pathlib import Path
import socket
import sys
import threading
import unittest
import uuid
from unittest.mock import patch

from ops import node_maintenance as maintenance
from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture
from ops.node_ingress import save
from processing.node_gate import NodeGate,GateBusy
from processing.reference_worker_readiness import source_digest
from tests import test_managed_update_finalization as managed_fixture
from tests.test_container_update import OLD,NEW,OLD_ENGINE_VERSION,NEW_ENGINE_VERSION
from tests.test_reference_dynamic_source_intake_scale import _write_tls_material


@unittest.skipUnless(sys.platform.startswith('linux'),'actual worker process leases require Linux')
class ReferenceWorkerHandoverTests(unittest.TestCase):
    def setUp(self):
        self.host=managed_fixture.ManagedFinalizationTests(methodName='runTest');self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.root=self.host.root;self.api=self.host.api;self.queue=self.host.fixture.queue
        self.backend=NativeNodeFixture(self.root/'worker-native.sqlite',engine_version=OLD_ENGINE_VERSION,synthetic_enabled=True).start()
        self.addCleanup(self.backend.close)
        key,cert=_write_tls_material(self.root)
        source=Path(__file__).resolve().parents[1]
        probe=socket.socket();probe.bind(('127.0.0.1',0));port=probe.getsockname()[1];probe.close()
        self.worker_path=self.root/'reference-worker.json'
        self.config=dict(schemaVersion=1,stateRoot=str(self.root/'worker-state'),scratchRoot=str(self.root/'worker-scratch'),
            nodeId='cluster',workerId='worker-cluster',workerInstanceId='stable-instance',authorityId=self.queue.authority_id,
            engineId='synthetic-engine',gateGeneration=1,gateCoordinatorOrigin=self.api.origin,
            coordinatorOrigin='https://coordinator.invalid',brokerOrigin='https://broker.invalid',engineOrigin=self.backend.origin,
            engineToken='',workerBearer='synthetic-worker-bearer-'+ 'a'*32,resultReadBearer='synthetic-result-bearer-'+ 'b'*32,
            caFile=str(cert),serverCertFile=str(cert),serverKeyFile=str(key),listenHost='127.0.0.1',listenPort=port,
            outputBudget=1048576,safetyFloor=1024,transferTtl=600,cleanupGrace=3600,pollSeconds=0.1,
            engineVersion=OLD_ENGINE_VERSION,shareResults=True,maintenanceReadiness=dict(schemaVersion=1,
                bearer='synthetic-maintenance-bearer-'+ 'c'*32,sourceRoot=str(source),sourceSha256=source_digest(source),activation=None))
        save(self.worker_path,self.config)
        settings=json.loads((self.root/'maintenance.json').read_text())
        settings.update(workerMode='reference',referenceWorker=dict(schemaVersion=1,
            unitName='ledgetop-processing-reference-worker.service',unitFile=str(self.root/'unused-unit.service'),unitSha256='d'*64,
            pythonExecutable=sys.executable,configPath=str(self.worker_path),sourceRoot=str(source),
            sourceSha256=self.config['maintenanceReadiness']['sourceSha256'],readinessOrigin='https://127.0.0.1:'+str(port),readinessCAFile=str(cert)))
        save(self.root/'maintenance.json',settings)
        from urllib.parse import urlsplit
        self.host.ingress.config['backendPort']=urlsplit(self.backend.origin).port
        self.host.adapter.worker_mode='reference'
        self.host.adapter.node=type('Endpoint',(),{'origin':self.backend.origin})()
        replace=self.host.adapter.replace
        def replacing(image):
            replace(image);self.backend.engine_version=NEW_ENGINE_VERSION if image==NEW else OLD_ENGINE_VERSION
        self.host.adapter.replace=replacing
        self.host.adapter.args=maintenance.adapter_args(self.root,settings,self.host.ingress)
        self.compose_patch=patch.object(maintenance,'ComposeNode',return_value=self.host.adapter)
        self.compose_patch.start();self.addCleanup(self.compose_patch.stop)
        test=self
        class ServiceControl:
            def __init__(self):
                self.thread=None;self.service=None;self.stop=None;self.generation=0;self.calls=[];self.fail_new=False
            def probe(self):
                active=self.thread is not None and self.thread.is_alive()
                return dict(pid=os.getpid() if active else 0,active='active' if active else 'inactive',substate='running' if active else 'dead',
                    cgroup='/synthetic-unit',invocationId=str(self.generation),startTime=str(self.generation))
            def stopped(self,previous):return self.thread is None or not self.thread.is_alive()
            def command(self,action):
                self.calls.append(action)
                test.assertEqual(json.loads((test.host.ingress.root/'state.json').read_text())['state'],'closed')
                test.assertIn(test.queue.get_node('cluster')['state'],('updating','failed'))
                if action=='stop':
                    # Updater owns the physical gate before it may stop a worker.
                    with test.assertRaises(GateBusy):
                        with NodeGate('synthetic-engine'):pass
                    self.close();return
                # A real factory acquires this same nonblocking gate below.
                with NodeGate('synthetic-engine') as gate:test.assertIsNone(gate.snapshot()['reservation'])
                if self.fail_new and test.backend.engine_version==NEW_ENGINE_VERSION:return
                self.start()
            def start(self):
                self.generation+=1
                config=daemon.load_config(str(test.worker_path))
                driver,server=daemon.build_worker(config)
                driver.worker.engine_probe=lambda deadline:'synthetic-engine'
                def request(method,route,payload=None,**kwargs):
                    if route=='/authority':return 200,dict(authorityId=test.queue.authority_id)
                    if route=='/next':return 200,dict(job=None)
                    raise AssertionError('unexpected synthetic coordinator route')
                driver._request=request
                self.stop=threading.Event();self.service=daemon.WorkerService(driver,server,poll_seconds=0.1)
                self.thread=threading.Thread(target=self.service.run,args=(self.stop,));self.thread.start()
            def close(self):
                if self.stop is not None:self.stop.set()
                if self.thread is not None:
                    self.thread.join(5)
                    test.assertFalse(self.thread.is_alive(),'task-owned worker failed to drain')
        self.control=ServiceControl();self.control.start();self.addCleanup(self.control.close)
        patcher=patch('ops.reference_worker_handover.SystemdWorkerControl',return_value=self.control)
        patcher.start();self.addCleanup(patcher.stop)

    def run_update(self):
        # The readonly source census crosses a Windows Docker bind in CI; allow
        # bounded startup retries independently of the queue wait budget.
        return maintenance.run(self.root,self.api,self.host.operator,self.host.receipt,5,drain_seconds=10)

    def test_real_factory_pin_migration_tls_readiness_and_gate_yield_complete_before_reopen(self):
        old=self.worker_path.read_bytes()
        result=self.run_update()
        self.assertEqual(result['status'],'succeeded',result)
        self.assertEqual(self.control.calls,['stop','start'])
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.host.ingress.opens,1)
        new=json.loads(self.worker_path.read_bytes())
        self.assertEqual(new['engineVersion'],NEW_ENGINE_VERSION)
        self.assertEqual(new['workerInstanceId'],self.config['workerInstanceId'])
        self.assertNotEqual(old,self.worker_path.read_bytes())
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        self.assertIn('workerHandoverSha256',result)
        # Finished replay neither stops/restarts a worker nor replaces again.
        self.assertEqual(self.run_update(),result)
        self.assertEqual(self.control.calls,['stop','start']);self.assertEqual(self.host.adapter.calls,[NEW])

    def test_supervisor_registry_qualification_owned_schedule_and_reference_completion(self):
        from processing.update_registry import DockerRegistry,RegistryPolicy,RegistryChannel
        from processing.update_qualifier import StartupPolicy,StartupQualifier,startup_spec
        from processing.update_supervisor import UpdateSupervisor,SupervisorPolicy
        from ops import update_maintenance_adapter as bridge
        from tests import test_update_qualifier as qf,test_update_registry as rf
        node=self.queue.get_node('cluster')
        self.host.operator.json('POST','/v1/prototype/nodes/cluster/cancel',dict(planId=node['planId'],expectedRevision=node['revision']))
        settings=json.loads((self.root/'maintenance.json').read_text());settings['coordinator']=self.api.origin
        save(self.root/'maintenance.json',settings);save(self.host.ingress.root/'gateway.json',self.host.ingress.config)
        def compose(args,work):
            self.host.adapter.args=args
            return self.host.adapter
        with patch.object(bridge,'Ingress',return_value=self.host.ingress),patch.object(bridge,'ComposeNode',side_effect=compose):
            adapter=bridge.UpdateMaintenanceAdapter(self.root,state_path=self.root/'bridge.sqlite',receipt_root=self.root/'receipts',
                api=self.api,operator=self.host.operator,node_id='cluster',authority_id=self.queue.authority_id,
                engine_id='synthetic-engine',adapter_id='real-maintenance-v1',window=bridge.ScheduleWindow(0,0,5,5))
            docker=qf.FakeDocker();docker.engine='synthetic-engine';docker.info.update(engine='odm',engineVersion=NEW_ENGINE_VERSION)
            docker.mismatch['image']=NEW;selected=rf.entry(config=NEW)
            docker.registry.selected=selected;docker.registry.first=[selected]
            docker.registry.image.update(Id=NEW,RepoDigests=[rf.REPO+'@'+selected['Descriptor']['digest']])
            registry=DockerRegistry(self.root/'registry.sqlite',RegistryPolicy(sys.executable,'unix:///var/run/docker.sock',
                'synthetic-engine',(RegistryChannel('test',rf.REPO,'approved-test'),),4096),capacity_admit=lambda *_:True,runner=docker)
            qualifier=StartupQualifier(self.root/'qualifier.sqlite',StartupPolicy('startup-only-reference-v1','synthetic-engine',
                'cluster',self.queue.authority_id,'odm',startup_spec(qf.DEFAULTS),str(self.root),'0:0'),registry=registry,runner=docker)
            policy=SupervisorPolicy('cluster','test',OLD,self.queue.authority_id,qualifier.policy.qualifier_id,
                qualifier.policy_sha256,'real-maintenance-v1',4096)
            with UpdateSupervisor(self.root/'supervisor.sqlite',policy,registry=registry,qualifier=qualifier,maintenance=adapter) as supervisor:
                identifier=supervisor.discover()['operationId']
                for _ in range(8):
                    result=supervisor.advance(identifier)
                    if result['state']=='completed':break
                self.assertEqual(result['state'],'completed',result)
                self.assertEqual(self.control.calls,['stop','start'])
                self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.host.ingress.opens,1)
                self.assertEqual(self.queue.get_node('cluster')['state'],'active')
                # Completion recovery authenticates the serving worker but cannot restart it.
                with patch.object(bridge.managed,'run',side_effect=AssertionError('completion lookup cannot mutate')):
                    self.assertEqual(supervisor.advance(identifier),result)
                self.assertEqual(self.control.calls,['stop','start'])

    def test_factory_refuses_identical_shadow_source_before_engine_or_config_changes(self):
        import shutil
        self.control.close()
        shadow=self.root/'shadow'
        for package in ('ops','processing'):
            shutil.copytree(Path(self.config['maintenanceReadiness']['sourceRoot'])/package,shadow/package,
                ignore=shutil.ignore_patterns('__pycache__'))
        copied=json.loads(self.worker_path.read_bytes());copied['maintenanceReadiness']['sourceRoot']=str(shadow)
        self.assertEqual(source_digest(shadow),copied['maintenanceReadiness']['sourceSha256'])
        save(self.worker_path,copied);before=self.worker_path.read_bytes()
        with self.assertRaisesRegex(ValueError,'installed worker source pin differs'):
            daemon.build_worker(daemon.load_config(str(self.worker_path)))
        self.assertEqual(self.worker_path.read_bytes(),before)
        self.assertEqual(self.host.adapter.calls,[]);self.assertEqual(self.control.calls,[])

    def test_transient_old_driver_busy_readiness_retries_before_any_stop(self):
        from urllib.error import HTTPError
        from ops.reference_worker_handover import ReferenceWorkerHandover
        original=ReferenceWorkerHandover._https_readiness;busy=[]
        def transient(instance,request,config):
            if not busy:
                self.assertEqual(self.control.calls,[]);busy.append(True)
                raise HTTPError('https://synthetic.invalid',503,'synthetic driver busy',{},None)
            return original(instance,request,config)
        with patch.object(ReferenceWorkerHandover,'_https_readiness',transient):
            self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.control.calls,['stop','start'])

    def test_failed_new_worker_readiness_restores_old_config_and_factory_pin_without_reopen(self):
        old=self.worker_path.read_bytes();self.control.fail_new=True
        result=self.run_update()
        self.assertEqual(result['status'],'failed',result)
        self.assertEqual(self.worker_path.read_bytes(),old)
        self.assertEqual(self.host.adapter.calls,[NEW,OLD]);self.assertEqual(self.host.ingress.opens,0)
        self.assertEqual(self.queue.get_node('cluster')['state'],'failed')
        self.assertTrue(self.control.thread.is_alive())

    def test_lost_config_commit_ack_reconciles_exact_bytes_without_replacing_twice(self):
        from ops.reference_worker_handover import ReferenceWorkerHandover
        original=ReferenceWorkerHandover._replace_config;lost=[]
        def committed_then_lost(instance,raw,expected):
            original(instance,raw,expected)
            if not lost:lost.append(True);raise OSError('synthetic lost durable config acknowledgement')
        with patch.object(ReferenceWorkerHandover,'_replace_config',committed_then_lost):
            with self.assertRaises(OSError):self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.host.ingress.opens,0)
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.control.calls,['stop','start'])

    def test_unknown_start_ack_is_only_read_reconciled_without_second_start(self):
        from ops.reference_worker_handover import WorkerHandoverUncertain
        command=self.control.command
        def lost(action):
            command(action)
            if action=='start':raise WorkerHandoverUncertain('synthetic start acknowledgement lost')
        with patch.object(self.control,'command',side_effect=lost):
            with self.assertRaises(WorkerHandoverUncertain):self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.control.calls,['stop','start']);self.assertEqual(self.host.adapter.calls,[NEW])

    def test_rejected_new_nonce_rolls_back_after_actual_driver_pin_migration(self):
        from ops.reference_worker_handover import ReferenceWorkerHandover
        import sqlite3
        original=ReferenceWorkerHandover._https_readiness;versions=[]
        def invalid_proof(instance,request,config):
            with closing(sqlite3.connect(Path(config['stateRoot'])/'driver.sqlite')) as db:
                versions.append(db.execute('SELECT pin FROM driver_engine_pin WHERE singleton=1').fetchone()[0])
            proof=original(instance,request,config)
            if config['engineVersion']==NEW_ENGINE_VERSION:proof['nonce']=str(uuid.uuid4())
            return proof
        old=self.worker_path.read_bytes()
        with patch.object(ReferenceWorkerHandover,'_https_readiness',invalid_proof):result=self.run_update()
        self.assertEqual(result['status'],'failed');self.assertEqual(self.worker_path.read_bytes(),old)
        self.assertIn(NEW_ENGINE_VERSION,versions,'new factory must actually commit its driver pin before rollback')
        self.assertEqual(versions[-1],OLD_ENGINE_VERSION)
        self.assertEqual(self.host.adapter.calls,[NEW,OLD]);self.assertEqual(self.host.ingress.opens,0)

    def test_uncommitted_finish_resume_reacquires_physical_gate_before_finishing(self):
        original=self.host.operator.json
        def unavailable(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/finish'):raise OSError('synthetic unavailable finish')
            return original(method,route,payload,**kwargs)
        with patch.object(self.host.operator,'json',side_effect=unavailable):
            with self.assertRaises(OSError):self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        def checking(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/finish'):
                with self.assertRaises(GateBusy):
                    with NodeGate('synthetic-engine'):pass
            return original(method,route,payload,**kwargs)
        with patch.object(self.host.operator,'json',side_effect=checking):
            self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.control.calls,['stop','start']);self.assertEqual(self.host.adapter.calls,[NEW])
        self.assertEqual(self.host.ingress.opens,1)

    def test_lost_finish_ack_reconciles_without_worker_or_engine_mutations(self):
        original=self.host.operator.json;lost=[]
        def finished_then_lost(method,route,payload=None,**kwargs):
            result=original(method,route,payload,**kwargs)
            if method=='POST' and route.endswith('/finish') and not lost:
                lost.append(True);raise OSError('synthetic finish acknowledgement lost')
            return result
        with patch.object(self.host.operator,'json',side_effect=finished_then_lost):
            with self.assertRaises(OSError):self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.control.calls,['stop','start']);self.assertEqual(self.host.adapter.calls,[NEW])
        self.assertEqual(self.host.ingress.opens,1)

    def test_health_failure_after_durable_compose_pin_holds_aligned_new_worker_and_engine(self):
        healthy=self.host.adapter.healthy
        def unhealthy_after_pin(image):
            if json.loads((self.root/'compose.json').read_text())['services']['node']['image']==NEW:return False
            return healthy(image)
        with patch.object(self.host.adapter,'healthy',side_effect=unhealthy_after_pin):
            with self.assertRaises(maintenance.GateError):self.run_update()
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(json.loads((self.root/'compose.json').read_text())['services']['node']['image'],NEW)
        self.assertEqual(json.loads(self.worker_path.read_text())['engineVersion'],NEW_ENGINE_VERSION)
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.control.calls,['stop','start'])
        self.assertEqual(self.host.ingress.opens,0)
        self.assertEqual(self.run_update()['status'],'succeeded')
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.control.calls,['stop','start'])
        self.assertEqual(self.host.ingress.opens,1)

    def test_foreign_config_change_is_not_overwritten_or_rolled_back(self):
        original=maintenance.finalize_success;foreign=[]
        def changed(*args,**kwargs):
            value=self.worker_path.read_bytes()+b'\n'
            self.worker_path.write_bytes(value);foreign.append(value)
            return original(*args,**kwargs)
        with patch.object(maintenance,'finalize_success',side_effect=changed):
            with self.assertRaises(ValueError):self.run_update()
        self.assertEqual(self.worker_path.read_bytes(),foreign[0])
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.host.adapter.calls,[NEW]);self.assertEqual(self.host.ingress.opens,0)


class ExistingMigrationCensusTests(unittest.TestCase):
    def test_pending_viewer_acceptance_keeps_original_sealed_result_after_pin_change(self):
        from tests import test_reference_driver as fixture_module
        from ops.reference_worker_handover import closed_worker_census
        import shutil
        fixture=fixture_module.ReferenceDriverTests(methodName='runTest');fixture.setUp();self.addCleanup(fixture.tearDown)
        first=fixture.submit();fixture.advance(first,'awaiting_acceptance')
        original=fixture.store.get(first['jobId'],private=True)['resultManifest']
        fixture.worker.version='updated-engine-2';fixture.transport.version='updated-engine-2'
        fixture.driver=fixture.make_driver()
        # Snapshot the real fixture's quiescent stores to factory-standard names.
        for source,target in (('ledger.sqlite','scratch.sqlite'),('failures.sqlite','failure-outbox.sqlite')):
            shutil.copyfile(fixture.root/source,fixture.root/target);(fixture.root/target).chmod(0o600)
        config=dict(stateRoot=str(fixture.root),nodeId='cluster',workerId='worker',engineId='engine',
            authorityId=fixture.queue.authority_id,gateCoordinatorOrigin=fixture.gate_origin,gateGeneration=1)
        proof=closed_worker_census(config)
        self.assertEqual(proof['jobs'][first['jobId']]['stage'],'awaiting_acceptance')
        self.assertEqual(fixture.store.get(first['jobId'],private=True)['resultManifest'],original)
        self.assertEqual(original['engineVersions'],{'odm':'pinned-engine-1'})


if __name__=='__main__':unittest.main()
