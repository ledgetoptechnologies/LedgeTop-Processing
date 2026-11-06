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
        with patch.object(f,'readiness',side_effect=self.packet):
            states=f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
        self.assertEqual([s['phase'] for s in states],['ready','ready'])
        guard_coordinator_progress(self.driver,'stop-bootstrap')
        packet=self.packet(self.instances[0],self.workers[0])
        self.assertIsNotNone(activation_readiness_binding(self.driver,self.workers[0],packet['readiness'],packet['process']))
        changed=copy.deepcopy(packet['process']);changed['startTime']='43'
        with self.assertRaisesRegex(TransportHeld,'changed_since_accepted'):
            activation_readiness_binding(self.driver,self.workers[0],packet['readiness'],changed)

    def test_not_frozen_or_wrong_node_cannot_register(self):
        self.state['phase']='bootstrap-draining'
        with patch.object(f,'readiness',side_effect=self.packet), self.assertRaisesRegex(TransportHeld,'not_frozen'):
            f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
        self.assertFalse((self.root/'first-worker-activation-cluster.json').exists())
        self.state['phase']='bootstrap-frozen'; self.instances[0]['node']='main'
        with self.assertRaisesRegex(TransportHeld,'order_changed'):
            f.activate_fixture_workers(self.controller,self.driver,SimpleNamespace(instances=self.instances),self.startup,'b'*64)
