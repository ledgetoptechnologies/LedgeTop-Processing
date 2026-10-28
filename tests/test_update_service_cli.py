import copy
from dataclasses import asdict
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid
from contextlib import redirect_stderr,redirect_stdout

from ops import update_service as module
from processing.update_registry import RegistryPolicy,RegistryChannel,RegistryNotStaged,RegistryHeld
from processing.update_qualifier import StartupPolicy,PROBE_SHA
from processing.update_supervisor import SupervisorPolicy
from processing.update_capacity import CapacityPolicy
from ops.update_maintenance_adapter import ScheduleWindow
from processing.node_gate import NodeGate
from processing.container_update import UpdateError
from processing.app import make_handler
from processing.config import Config
from processing.odx_smoke_worker import Client
from tests.test_service import start_server,TOKEN


def configuration(root,authority=None):
    authority=authority or str(uuid.uuid4());engine='cli-fixture-engine';folder=root/'supervisor'
    rp=RegistryPolicy('/usr/bin/docker','unix:///var/run/docker.sock',engine,
        (RegistryChannel('approved','docker.io/example/nodeodx','gpu'),),64*1024**3)
    qp=StartupPolicy('startup-only',engine,'main',authority,'odx','a'*64,str(folder),'0')
    qp_hash=module.sha(dict(policy=asdict(qp),registryPolicy=asdict(rp),probeSha256=PROBE_SHA,scope='startup-api-options-only-v1'))
    sp=SupervisorPolicy('main','approved','sha256:'+'b'*64,authority,qp.qualifier_id,qp_hash,'managed-main',rp.staging_reservation_bytes)
    cp=CapacityPolicy(rp.docker_executable,rp.docker_endpoint,engine,str(root/'docker-root'))
    raw=dict(schemaVersion=1,serviceId='fixture-updates',codeBindingSha256=module.code_binding_sha256(),
        registry=dict(statePath=str(root/'registry.sqlite'),policy=asdict(rp)),
        qualification=dict(statePath=str(folder/'qualifier.sqlite'),policy=asdict(qp)),
        supervisor=dict(statePath=str(folder/'supervisor.sqlite'),policy=asdict(sp)),
        maintenance=dict(managedRoot=str(root/'managed'),statePath=str(folder/'adapter.sqlite'),receiptRoot=str(folder/'receipts'),window=asdict(ScheduleWindow(0,0,60,60))),
        capacity=dict(policy=asdict(cp)),service=dict(statePath=str(folder/'service.sqlite'),discoveryIntervalSeconds=21600,errorCooldownSeconds=300,maxTransitions=8))
    return json.loads(module.canonical(raw))


@unittest.skipUnless(os.name=='posix','Linux host configuration and actual private-file modes')
class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700);self.raw=configuration(self.root)

    def file(self,text,name='policy.json'):
        p=self.root/name;p.write_text(text);p.chmod(0o600);return p

    def test_full_explicit_policy_roundtrip_preserves_existing_component_hash(self):
        parsed=module.parse_configuration(module.read_private_json(self.file(module.canonical(self.raw))))
        self.assertEqual(parsed.raw,self.raw)
        self.assertEqual(parsed.registry_policy.staging_reservation_bytes,64*1024**3)
        self.assertEqual(parsed.capacity_policy.legacy_capacity_journals,())
        self.assertEqual(parsed.config_id,module.sha(self.raw))
        changed=copy.deepcopy(self.raw);changed['service']['errorCooldownSeconds']+=1
        self.assertNotEqual(module.parse_configuration(changed).config_id,parsed.config_id)

    def test_missing_default_fields_unknown_fields_and_wrong_policy_object_types_rejected(self):
        cases=[]
        for section in ('registry','qualification','supervisor','capacity'):
            raw=copy.deepcopy(self.raw);raw[section]['policy']['unexpected']=True;cases.append(raw)
            raw=copy.deepcopy(self.raw);raw[section]['policy']=list(raw[section]['policy'].items());cases.append(raw)
        raw=copy.deepcopy(self.raw);raw['registry']['policy'].pop('timeout_seconds');cases.append(raw)
        raw=copy.deepcopy(self.raw);raw['capacity']['policy'].pop('legacy_capacity_journals');cases.append(raw)
        for raw in cases:
            with self.subTest(raw=raw),self.assertRaises((module.ServiceConfigurationError,TypeError,ValueError)):
                module.parse_configuration(raw)

    def test_cross_component_engine_authority_paths_capacity_and_timing_drift_refused(self):
        cases=[]
        changes=[('capacity','policy','engine_id','foreign-engine'),('qualification','policy','authority_id',str(uuid.uuid4())),
            ('supervisor','policy','capacity_bytes',1),('qualification','policy','supervisor_state_root',str(self.root/'other'))]
        for section,field,key,value in changes:
            raw=copy.deepcopy(self.raw);raw[section][field][key]=value;cases.append(raw)
        for key,value in [('maxTransitions',True),('errorCooldownSeconds',0),('discoveryIntervalSeconds',1)]:
            raw=copy.deepcopy(self.raw);raw['service'][key]=value;cases.append(raw)
        for raw in cases:
            with self.subTest(raw=raw),self.assertRaises(ValueError):module.parse_configuration(raw)

    def test_private_json_rejects_duplicate_nan_oversize_hardlink_and_public_mode(self):
        for index,text in enumerate(('{"a":1,"a":2}','{"a":NaN}','x'*65537)):
            with self.subTest(text=text[:20]),self.assertRaises(ValueError):
                module.read_private_json(self.file(text,str(index)+'.json'))
        path=self.file('{}');path.chmod(0o644)
        with self.assertRaises(ValueError):module.read_private_json(path)
        path.chmod(0o600);alias=self.root/'alias';os.link(path,alias)
        with self.assertRaises(ValueError):module.read_private_json(path)

    def test_control_roles_never_accept_extra_environment_or_remote_origin(self):
        managed=self.root/'managed';managed.mkdir(mode=0o700)
        settings=managed/'maintenance.json';settings.write_text(json.dumps(dict(coordinator='http://127.0.0.1:18080')));settings.chmod(0o600)
        env=managed/'maintenance.env';env.write_text('PROCESSING_API_TOKEN=synthetic-api\nPROCESSING_PROTOTYPE_OPERATOR_TOKEN=synthetic-operator\n');env.chmod(0o600)
        api,operator=module.control_clients(managed)
        self.assertEqual(api.origin,operator.origin)
        env.write_text(env.read_text()+'PATH=/tmp/unsafe\n')
        with self.assertRaises(ValueError):module.control_clients(managed)
        settings.write_text(json.dumps(dict(coordinator='https://outside.example')))
        with self.assertRaises(UpdateError):module.control_clients(managed)

    def test_changed_source_rejected_before_any_component_is_constructed(self):
        raw=copy.deepcopy(self.raw);raw['codeBindingSha256']='0'*64
        with self.assertRaisesRegex(ValueError,'reviewed_source_changed'):
            module.compose(module.parse_configuration(raw))

    def test_main_sanitizes_exception_text(self):
        path=self.file(module.canonical(self.raw));error=io.StringIO()
        with patch.object(module,'execute',side_effect=RuntimeError('SENSITIVE-SYNTHETIC-TEXT')),redirect_stderr(error):
            self.assertEqual(module.main(['tick','--config',str(path)]),2)
        self.assertNotIn('SENSITIVE',error.getvalue());self.assertEqual(json.loads(error.getvalue())['category'],'RuntimeError')

    def test_held_service_outcome_is_visible_as_nonzero_unit_exit(self):
        path=self.file(module.canonical(self.raw));output=io.StringIO()
        with patch.object(module,'execute',return_value=dict(status='held')),redirect_stdout(output):
            self.assertEqual(module.main(['tick','--config',str(path)]),2)
        self.assertEqual(json.loads(output.getvalue())['status'],'held')


class CachedAdmissionTests(unittest.TestCase):
    def test_definite_cached_artifact_needs_no_new_capacity(self):
        registry=SimpleNamespace(inspect_staged_artifact=lambda candidate:dict(imageId='synthetic'))
        self.assertTrue(module.admit_stage(lambda *_:self.fail('must not reserve cached image'),{},registry))

    def test_only_typed_definite_absence_allows_new_reservation(self):
        calls=[]
        def absent(_):raise RegistryNotStaged('synthetic-known-absence')
        registry=SimpleNamespace(inspect_staged_artifact=absent,policy=SimpleNamespace(staging_reservation_bytes=123))
        self.assertFalse(module.admit_stage(lambda candidate,amount:calls.append(amount) or False,{},registry))
        self.assertEqual(calls,[123])
        def unknown(_):raise RegistryHeld('synthetic-unknown-pull')
        registry.inspect_staged_artifact=unknown
        with self.assertRaises(RegistryHeld):module.admit_stage(lambda *_:self.fail('unknown must hold'),{},registry)


@unittest.skipUnless(os.name=='posix','Actual canonical flock and Linux mode checks')
class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.handler=make_handler(Config(TOKEN,'','',1000,20000,prototype_state_dir=str(self.root/'queue'),prototype_node_ids=('cluster','main'),prototype_operator_token='synthetic-operator'))
        self.queue=self.handler.prototype_queue;self.addCleanup(self.queue.close)
        server=start_server(self.handler);self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        self.api=Client('http://127.0.0.1:'+str(server.server_port),TOKEN)
        self.operator=Client(self.api.origin,'synthetic-operator')
        self.config=module.parse_configuration(configuration(self.root,self.queue.authority_id))
        self.gate_path=self.root/'physical-gate'
        patcher=patch('processing.node_gate.canonical_gate_path',return_value=self.gate_path);patcher.start();self.addCleanup(patcher.stop)
        with NodeGate(self.config.registry_policy.engine_id) as gate:gate.provision('main',self.api.origin,self.queue.authority_id)
        self.idle_calls=0
        def idle():self.idle_calls+=1;return True
        adapter=SimpleNamespace(_context=lambda path:(None,None,SimpleNamespace(idle=idle)))
        self.idle,self.schedule=module.readiness_probes(self.config,adapter,self.api)

    def test_actual_gate_and_http_idle_then_occupied_reservation_waits_unchanged(self):
        self.assertTrue(self.idle({}));self.assertEqual(self.idle_calls,1)
        with NodeGate(self.config.registry_policy.engine_id) as gate:
            gate.reserve('main',self.api.origin,self.queue.authority_id,1,'a'*32)
        before=(self.gate_path/'gate.json').read_bytes()
        self.assertFalse(self.idle({}));self.assertEqual(self.idle_calls,1)
        self.assertEqual((self.gate_path/'gate.json').read_bytes(),before)

    def test_foreign_manual_plan_waits_without_backend_probe_or_cancel(self):
        node=self.api.json('GET','/v1/prototype/nodes/main')['node']
        own=self.operator.json('POST','/v1/prototype/nodes/main/schedule',dict(expectedRevision=node['revision'],targetImage='sha256:'+'c'*64,afterJobId=None,drainAt='2099-01-01T00:00:00Z',notBefore='2099-01-01T00:00:00Z'))['node']
        self.assertFalse(self.schedule({}));self.assertFalse(self.idle({}));self.assertEqual(self.idle_calls,0)
        self.assertEqual(self.api.json('GET','/v1/prototype/nodes/main')['node']['planId'],own['planId'])

    def test_unprovisioned_gate_is_not_created_by_readiness(self):
        other=self.root/'missing'
        with patch('processing.node_gate.canonical_gate_path',return_value=other),self.assertRaises(OSError):self.idle({})
        self.assertFalse(other.exists())


if __name__=='__main__':unittest.main()
