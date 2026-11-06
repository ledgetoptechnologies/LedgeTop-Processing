"""Local fixture only; real durable journal and production acceptance checks."""
import copy
import tempfile
import unittest
import uuid
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ops import qualify_fixture_activation as f
from ops.first_worker_activation import activation_readiness_binding, guard_coordinator_progress
from ops.enrolled_coordinator_transport import digest, TransportHeld


class SyntheticActivationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name); self.root.chmod(0o700)
        self.plan={'operationId':str(uuid.uuid4())}
        self.workers=[{'nodeId':node,'settings':{'sourceRoot':'/opt/ledgetop-processing-reference/'+str(uuid.uuid4())+'/source'}} for node in ('cluster','main')]
        self.instances=[{'node':w['nodeId'],'review':{'enrollment':{'manifestSha256':'a'*64}}} for w in self.workers]
        self.driver=SimpleNamespace(plan=self.plan,root=self.root,bundle={'workers':self.workers},global_lock=lambda _:nullcontext())
        self.state={'phase':'bootstrap-frozen','pending':None,'admissionOpened':False}
        self.controller=SimpleNamespace(journal=SimpleNamespace(read=lambda:self.state))
        self.startup=str(uuid.uuid4())

    def packet(self,instance,worker):
        keys=('bootId','pid','nodeId','workerId','workerInstanceId','authorityId','engineId','gateGeneration',
              'engineVersion','configSha256','sourceSha256','driverBindingSha256','readerEpoch','activation',
              'optionsSha256','safeOptionsSha256','loadedSourceSha256')
        ready={key:None for key in keys};ready['nodeId']=worker['nodeId'];ready['pid']=7
        return {'schemaVersion':1,'protocol':'ltds-enrolled-worker-readiness-probe-v1',
                'challenge':str(uuid.uuid4()),'nodeId':worker['nodeId'],'settingsSha256':digest(worker['settings']),
                'readiness':ready,'process':{'pid':7,'startTime':'42'}}

    def test_durable_journal_and_production_gate_verify_exact_process(self):
        with self.assertRaisesRegex(TransportHeld,'activation_receipt_required'):
            guard_coordinator_progress(self.driver,'stop-bootstrap')
        with patch.object(f,'verify_fixture_ownership'), patch.object(f,'readiness',side_effect=self.packet):
            states=f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
            resumed=f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
        self.assertEqual([s['phase'] for s in states],['ready','ready'])
        self.assertEqual(states,resumed)
        guard_coordinator_progress(self.driver,'stop-bootstrap')
        packet=self.packet(self.instances[0],self.workers[0])
        self.assertIsNotNone(activation_readiness_binding(self.driver,self.workers[0],packet['readiness'],packet['process']))
        changed=copy.deepcopy(packet['process']);changed['startTime']='43'
        with self.assertRaisesRegex(TransportHeld,'changed_since_accepted'):
            activation_readiness_binding(self.driver,self.workers[0],packet['readiness'],changed)

    def test_not_frozen_or_wrong_node_cannot_register(self):
        self.state['phase']='bootstrap-draining'
        with patch.object(f,'verify_fixture_ownership'), patch.object(f,'readiness',side_effect=self.packet), self.assertRaisesRegex(TransportHeld,'not_frozen'):
            f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
        self.assertFalse((self.root/'first-worker-activation-cluster.json').exists())
        self.state['phase']='bootstrap-frozen'; self.instances[0]['node']='main'
        with patch.object(f,'verify_fixture_ownership'), self.assertRaisesRegex(TransportHeld,'order_changed'):
            f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)

    def test_wrong_controller_is_rejected_before_registration(self):
        outer=self.root/'volume';outer.mkdir(mode=0o700)
        op=outer/self.plan['operationId'];op.mkdir(mode=0o700);root=op/'operation';root.mkdir(mode=0o700)
        self.driver.root=root
        worker_root=op/'workers';worker_root.mkdir(mode=0o700)
        layout={'operationId':self.plan['operationId'],'candidateImageId':'sha256:test','installations':{}}
        for instance,worker in zip(self.instances,self.workers):
            instance.update(root=worker_root/worker['nodeId'],entry={'root':worker['settings']['sourceRoot']},manifest=worker)
            layout['installations'][worker['nodeId']]=instance['entry']
        workers=SimpleNamespace(root=worker_root,layout=layout,instances=self.instances,controller_id='a'*64)
        self.driver.docker=SimpleNamespace(inspect=lambda _: {'Id':'a'*64,'Image':'sha256:test',
            'Config':{'Labels':{'org.ledgetop.phase-controller':'wrong'}},
            'State':{'Running':True},'HostConfig':{'NetworkMode':'bridge'}})
        with patch.object(f,'Path',side_effect=lambda value: outer if value=='/prototype-state' else Path(value)), \
             patch.object(Path,'is_mount',return_value=True),patch.object(f.socket,'gethostname',return_value='a'*12):
            with self.assertRaisesRegex(TransportHeld,'owned_fixture_controller_required'):
                f.verify_fixture_ownership(self.driver,workers)
        self.assertFalse(any(root.iterdir()))

        self.driver.docker.inspect=lambda _: {'Id':'a'*64,'Image':'sha256:test',
            'Config':{'Labels':{'org.ledgetop.phase-controller':self.plan['operationId']}},
            'State':{'Running':True},'HostConfig':{'NetworkMode':'bridge'},
            'Mounts':[{'Destination':'/prototype-state','Type':'volume','RW':True,
                       'Name':'ltds-phase-forward-artifacts-'+self.plan['operationId'].replace('-','')[:16]}]}
        self.instances[0]['manifest']={'nodeId':'other'}
        with patch.object(f,'Path',side_effect=lambda value: outer if value=='/prototype-state' else Path(value)), \
             patch.object(Path,'is_mount',return_value=True),patch.object(f.socket,'gethostname',return_value='a'*12):
            with self.assertRaisesRegex(TransportHeld,'fixture_worker_binding_changed'):
                f.verify_fixture_ownership(self.driver,workers)

    def test_worker_process_change_after_observation_refuses_verification(self):
        boundary=f.FixtureActivationBoundary(self.controller,self.driver,SimpleNamespace(),self.instances[0],self.workers[0])
        current={'action':'start','requestId':str(uuid.uuid4())};first=self.packet(self.instances[0],self.workers[0])
        changed=copy.deepcopy(first);changed['process']['startTime']='99'
        with patch.object(f,'verify_fixture_ownership'),patch.object(f,'readiness',side_effect=[first,changed]):
            observation=boundary.observe({},current)
            self.assertFalse(boundary.verify({},current,observation))
