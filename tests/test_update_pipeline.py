"""Full updater composition; real journals/HTTP, synthetic Docker and ingress."""
import json
import sys
import unittest
from unittest.mock import patch

from ops import update_maintenance_adapter as maintenance_module
from processing.odx_smoke_worker import SmokeError
from processing.update_registry import DockerRegistry,RegistryPolicy,RegistryChannel
from processing.update_qualifier import StartupPolicy,StartupQualifier,startup_spec
from processing.update_supervisor import UpdateSupervisor,SupervisorPolicy,SupervisorHeld
from tests import test_update_maintenance_adapter as maintenance_fixture
from tests import test_update_qualifier as qualifier_fixture
from tests import test_update_registry as registry_fixture
from tests import test_container_update as engine_fixture


@unittest.skipUnless(sys.platform.startswith('linux'),'real private host maintenance composition requires Linux')
class UpdatePipelineTests(unittest.TestCase):
    def setUp(self):
        self.host=maintenance_fixture.UpdateMaintenanceAdapterTests(methodName='runTest')
        self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.root=self.host.root
        self.host.receipts=self.root/'receipts'
        self.host.state=self.root/'composed-adapter.sqlite'
        self.host.adapter=self.host.open()
        self.docker=qualifier_fixture.FakeDocker()
        self.docker.engine='synthetic-engine';self.docker.info['engine']='odm'
        self.docker.mismatch['image']=engine_fixture.NEW
        selected=registry_fixture.entry(config=engine_fixture.NEW)
        self.docker.registry.selected=selected;self.docker.registry.first=[selected]
        self.docker.registry.image.update(Id=engine_fixture.NEW,
            RepoDigests=[registry_fixture.REPO+'@'+selected['Descriptor']['digest']])
        self.registry_policy=RegistryPolicy(sys.executable,'unix:///var/run/docker.sock','synthetic-engine',
            (RegistryChannel('test',registry_fixture.REPO,'approved-test'),),4096)
        self.qualifier_policy=StartupPolicy('startup-only-pipeline-v1','synthetic-engine','cluster',
            self.host.host.fixture.queue.authority_id,'odm',startup_spec(qualifier_fixture.DEFAULTS),str(self.root),'0:0')
        self.start()
        self.addCleanup(lambda:self.supervisor.close() if self.supervisor._lease is not None else None)

    def start(self):
        self.registry=DockerRegistry(self.root/'composed-registry.sqlite',self.registry_policy,
            capacity_admit=lambda *_:True,runner=self.docker)
        self.qualifier=StartupQualifier(self.root/'composed-qualifier.sqlite',self.qualifier_policy,
            registry=self.registry,runner=self.docker)
        policy=SupervisorPolicy('cluster','test',engine_fixture.OLD,self.host.host.fixture.queue.authority_id,
            self.qualifier_policy.qualifier_id,self.qualifier.policy_sha256,'real-maintenance-v1',4096)
        self.supervisor=UpdateSupervisor(self.root/'composed-supervisor.sqlite',policy,registry=self.registry,
            qualifier=self.qualifier,maintenance=self.host.adapter)
        self.supervisor.__enter__()

    def restart(self):
        self.supervisor.close();self.host.adapter=self.host.open();self.start()

    def staged(self):
        identifier=self.supervisor.discover()['operationId']
        self.assertEqual(self.supervisor.advance(identifier)['state'],'staged')
        return identifier

    def finish(self,identifier):
        while self.supervisor.status(identifier)['state']!='completed':self.supervisor.advance(identifier)
        result=self.supervisor.status(identifier)
        self.assertEqual(self.host.host.adapter.calls,[engine_fixture.NEW])
        self.assertEqual(self.host.host.ingress.opens,1)
        self.assertEqual(self.host.host.fixture.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.supervisor._current(),engine_fixture.NEW)
        self.assertTrue(self.docker.removed)
        return result

    def test_actual_components_complete_pipeline_without_qualifier_scope_inflation(self):
        identifier=self.staged()
        self.assertEqual(self.supervisor.advance(identifier)['state'],'qualified')
        with self.qualifier._exclusive():
            request=json.loads(self.supervisor._row(identifier)['qualification_request'])
            row=self.qualifier._row(request['requestSha256'])
            evidence=json.loads(row['evidence'])
        self.assertEqual(evidence['scope'],'startup-api-options-only-v1')
        self.assertFalse(evidence['reconstructionQualified'])
        self.assertEqual(self.host.host.adapter.calls,[])
        self.assertEqual(self.supervisor.advance(identifier)['state'],'scheduled')
        result=self.finish(identifier)
        self.assertTrue(result['completion']['coordinatorFinished'])
        self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])
        self.restart()
        self.assertEqual(self.supervisor.advance(identifier),result)
        self.assertIsNone(self.supervisor.discover())

    def test_lost_qualification_remove_ack_reconciles_without_docker_replay(self):
        identifier=self.staged();self.docker.faults['rm']='after'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        mutations=self.docker.mutations();self.restart()
        self.assertEqual(self.supervisor.reconcile(identifier)['state'],'qualified')
        self.assertEqual(self.docker.mutations(),mutations)
        self.finish(identifier)

    def test_lost_owned_schedule_ack_recovers_through_real_server_receipt(self):
        identifier=self.staged();self.supervisor.advance(identifier)
        original=self.host.operator.json;posts=[]
        def lost(method,path,payload=None,**kwargs):
            value=original(method,path,payload,**kwargs)
            if method=='POST' and path.endswith('/schedule-owned'):
                posts.append(payload);raise SmokeError('synthetic response lost after commit')
            return value
        with patch.object(self.host.operator,'json',side_effect=lost):
            with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
            self.restart()
            self.assertEqual(self.supervisor.reconcile(identifier)['state'],'scheduled')
        self.finish(identifier)
        self.assertEqual(len(posts),1)

    def test_lost_managed_completion_ack_reconciles_without_running_again(self):
        identifier=self.staged();self.supervisor.advance(identifier);self.supervisor.advance(identifier)
        original=maintenance_module.managed.run
        def lost(*args,**kwargs):
            original(*args,**kwargs)
            raise OSError('synthetic completed run response lost')
        with patch.object(maintenance_module.managed,'run',side_effect=lost):
            with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.assertEqual(self.host.host.adapter.calls,[engine_fixture.NEW])
        self.restart()
        with patch.object(maintenance_module.managed,'run',side_effect=AssertionError('lookup must not run')):
            self.assertEqual(self.supervisor.reconcile(identifier)['state'],'completed')
        self.assertEqual(self.host.host.adapter.calls,[engine_fixture.NEW])
        self.assertEqual(self.supervisor._current(),engine_fixture.NEW)

    def test_unknown_startup_never_schedules_maintenance_or_restarts_engine(self):
        identifier=self.staged();self.docker.faults['start']='after'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.assertEqual(self.host.host.fixture.queue.get_node('cluster')['state'],'active')
        self.assertEqual(self.host.host.adapter.calls,[])
        mutations=self.docker.mutations();self.restart()
        with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
        self.assertEqual(self.docker.mutations(),mutations)
        with self.host.adapter._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM schedule_intents').fetchone()[0],0)
