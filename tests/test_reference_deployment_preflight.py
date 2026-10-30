"""Actual private loaders, synthetic staged files; no worker factory/network."""
import copy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from ops.reference_deployment_preflight import inspect_deployment,DeploymentPreflightHeld,main
from tests import test_reference_runtime as runtime_fixture


class DeploymentPreflightTests(unittest.TestCase):
    def setUp(self):
        self.f=runtime_fixture.ReferenceRuntimeTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.authority='11111111-1111-4111-8111-111111111111'
        self.origin='https://127.0.0.1:19443';self.broker='https://127.0.0.1:19444';self.workers={};self.paths={}
        for node in ('cluster','main'):
            bound=self.f.value['workers'][node]
            self.workers[node]=dict(schemaVersion=1,stateRoot=str(self.f.root/node/'state'),scratchRoot=str(self.f.root/node/'scratch'),
                nodeId=node,workerId=bound['workerId'],workerInstanceId=bound['instanceId'],authorityId=self.authority,
                engineId='synthetic-'+node,gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',
                coordinatorOrigin=self.origin,brokerOrigin=self.broker,engineOrigin='http://127.0.0.1:19001',engineToken='',
                engineVersion='synthetic-1',workerBearer=bound['bearer'],resultReadBearer=bound['resultBearer'],
                caFile=str(self.f.root/'clientCA.pem'),serverCertFile=str(self.f.root/'certificate.pem'),serverKeyFile=str(self.f.root/'privateKey.pem'),
                listenHost='127.0.0.1',listenPort=19445,outputBudget=4194304,safetyFloor=0,transferTtl=240,cleanupGrace=0,pollSeconds=0.1,
                shareResults=True,cleanupEnabled=False,engineCleanupEnabled=False,allowedEnvironments=['staging'],
                serviceProfile=dict(schemaVersion=1,mode='odm_only'),optionEvidence=dict(schemaVersion=1),
                inputCopyAccounting=dict(schemaVersion=3,fetchJournal=dict(schemaVersion=1,policyId='indexed-worker-fetch-three-retained-v1')))
            self.paths[node]=str(self.f.root/(node+'-private.json'))
        self.write()

    def write(self):
        self.f.write_config()
        for node,path in self.paths.items():
            Path(path).write_text(json.dumps(self.workers[node]))
            if os.name!='nt':Path(path).chmod(0o600)

    def inspect(self):
        return inspect_deployment(str(self.f.path),self.paths,config=self.f.config,authority_id=self.authority,
            coordinator_origin=self.origin,broker_origin=self.broker)

    def test_actual_loaders_compare_configuration_without_writes_or_runtime_construction(self):
        before={p:p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()}
        with patch('processing.prototype.Queue._initialize',side_effect=AssertionError('no Queue')),\
             patch('ops.reference_worker_daemon.build_worker',side_effect=AssertionError('no worker')),\
             patch('socket.socket',side_effect=AssertionError('no network')):
            report=self.inspect()
        self.assertTrue(report['configurationBindingsMatch']);self.assertFalse(report['servicesActivated'])
        self.assertFalse(report['deploymentApproved']);self.assertEqual(len(report['workers']),2)
        self.assertEqual(before,{p:p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()})
        for worker in self.f.value['workers'].values():
            self.assertNotIn(worker['bearer'],json.dumps(report));self.assertNotIn(worker['resultBearer'],json.dumps(report))
        self.assertNotIn('credentialSha',json.dumps(report))

    def test_cross_channel_identity_endpoint_and_profile_drift_refused(self):
        original=copy.deepcopy(self.workers)
        for key,value in [('workerId','foreign'),('workerInstanceId','foreign'),('authorityId','22222222-2222-4222-8222-222222222222'),
                          ('workerBearer','foreign-worker-'+'x'*32),('resultReadBearer','foreign-reader-'+'y'*32),
                          ('engineToken','foreign-native-token'),
                          ('coordinatorOrigin','https://foreign.example'),('brokerOrigin','https://foreign.example'),
                          ('allowedEnvironments',['production']),('cleanupEnabled',True),('engineCleanupEnabled',True),
                          ('inputCopyAccounting',{'schemaVersion':1}),('optionEvidence',{'schemaVersion':2})]:
            self.workers=copy.deepcopy(original);self.workers['main'][key]=value;self.write()
            with self.subTest(field=key),self.assertRaisesRegex(DeploymentPreflightHeld,'^staged reference configuration preflight held$'):self.inspect()

    def test_actual_loader_rejects_duplicate_json_and_missing_tls_files(self):
        path=Path(self.paths['cluster']);path.write_text('{"nodeId":"cluster","nodeId":"main"}')
        with self.assertRaises(DeploymentPreflightHeld):self.inspect()
        self.write();self.workers['main']['serverCertFile']=str(self.f.root/'absent');self.write()
        with self.assertRaises(DeploymentPreflightHeld):self.inspect()

    def test_result_budget_and_initial_lifetime_must_cover_staged_worker(self):
        from tests.test_reference_initial_grant import profile_payload
        self.f.value['streamBudget']['maxObjectBytes']=1;self.write()
        with self.assertRaises(DeploymentPreflightHeld):self.inspect()
        self.f.value['streamBudget']['maxObjectBytes']=100*1024**3
        self.f.value['initialGrantDelivery']=profile_payload();self.write()
        self.assertTrue(self.inspect()['initialGrantDelivery'])
        self.workers['main']['transferTtl']=301;self.write()
        with self.assertRaises(DeploymentPreflightHeld):self.inspect()

    def test_original_routing_and_recovery_pins_must_match_staged_workers(self):
        self.f.value['routingBindings']={n:dict(engineId='synthetic-'+n,gateGeneration=1,engineVersion='synthetic-1') for n in self.workers}
        self.f.value=self.f.recovery_config(self.authority)
        for node in self.workers:self.f.value['recoveryBindings'][node].update(engineId='synthetic-'+node,outputBudget=4194304)
        self.write();self.assertTrue(self.inspect()['configurationBindingsMatch'])
        self.f.value['routingBindings']['main']['engineVersion']='foreign';self.write()
        with self.assertRaises(DeploymentPreflightHeld):self.inspect()

    def test_cli_error_is_fixed_and_does_not_echo_private_paths_or_credentials(self):
        from contextlib import redirect_stdout
        import io
        output=io.StringIO()
        argv=['--coordinator-config',str(self.f.path),'--cluster-config',self.paths['cluster'],'--main-config',self.paths['main'],
              '--authority-id',self.authority,'--coordinator-origin',self.origin,'--broker-origin',self.broker]
        with patch('ops.reference_deployment_preflight.from_environment',return_value=self.f.config),redirect_stdout(output):
            self.workers['main']['workerBearer']='private-mismatch-'+'q'*32;self.write();result=main(argv)
        self.assertEqual(result,1);self.assertEqual(json.loads(output.getvalue()),{'error':'staged_configuration_preflight_held'})

    def test_native_origin_matches_actual_factory_literal_loopback_rule(self):
        for invalid in ('http://wrong-engine.example:9999','http://192.168.50.88:9999','http://127.0.0.1:0',
                        'http://user:secret@127.0.0.1:9999','http://127.0.0.1:9999/task','http://127.0.0.1:9999?q=1'):
            self.workers['main']['engineOrigin']=invalid;self.write()
            with self.subTest(origin=invalid),self.assertRaises(DeploymentPreflightHeld):self.inspect()
        for valid in ('http://127.0.0.1:9999','https://[::1]:9999/'):
            self.workers['main']['engineOrigin']=valid;self.write()
            self.assertTrue(self.inspect()['configurationBindingsMatch'])

    def test_cli_environment_failures_never_escape_or_echo_private_values(self):
        from contextlib import redirect_stdout
        import io
        argv=['--coordinator-config',str(self.f.path),'--cluster-config',self.paths['cluster'],'--main-config',self.paths['main'],
              '--authority-id',self.authority,'--coordinator-origin',self.origin,'--broker-origin',self.broker]
        output=io.StringIO()
        with patch('ops.reference_deployment_preflight.from_environment',side_effect=ValueError('private-sentinel-secret')),redirect_stdout(output):
            self.assertEqual(main(argv),1)
        self.assertEqual(json.loads(output.getvalue()),{'error':'staged_configuration_preflight_held'})
        output=io.StringIO()
        with patch.dict(os.environ,{'LOCAL_MAX_PHOTOS':'bad-private-value'}),redirect_stdout(output):
            self.assertEqual(main(argv),1)
        self.assertEqual(json.loads(output.getvalue()),{'error':'staged_configuration_preflight_held'})
