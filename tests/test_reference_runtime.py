import copy
import base64
from dataclasses import replace
import json
import http.client
import io
import os
from pathlib import Path
import ssl
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch
from contextlib import redirect_stderr
from http.server import ThreadingHTTPServer

from processing.app import make_handler, CoordinatorService
from processing.reference_binary import BinaryResponse
from processing.reference_http import create_reference_http_server
from processing.config import Config, from_environment
from processing.prototype import Queue, REFERENCE_PROFILE, VIEWER_PROFILE
from processing.reference_runtime import (
    ReferenceRuntime, ReferenceSettings, RuntimeConfigurationError, discovered_policy, load_reference_settings,
)
from processing.reference_recovery import RecoveryNodeBinding


class FakeServer:
    def __init__(self):
        self.events = []
        self.stop = threading.Event()

    def serve_forever(self, **kwargs):
        self.events.append("serving")
        self.stop.wait(2)

    def shutdown(self):
        self.events.append("shutdown")
        self.stop.set()

    def server_close(self):
        self.events.append("closed")


class ReferenceRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = Config(api_token="legacy-api-" + "a"*32,
            local_node_url="http://127.0.0.1:19001", main_node_url="http://127.0.0.1:19002",
            local_max_photos=1000, local_max_input_bytes=20*1024**3,
            prototype_state_dir=str(self.root / "state"), prototype_node_ids=("cluster", "main"),
            treedar_node_ids=("main",), viewer_enabled=True,
            viewer_token="legacy-native-viewer-" + "b"*32)
        paths = {}
        for name in ("certificate", "privateKey", "clientCA"):
            path = self.root / (name + ".pem")
            path.write_text("SYNTHETIC TEST PLACEHOLDER - NOT A CERTIFICATE OR KEY")
            if os.name != "nt":
                path.chmod(0o600)
            paths[name] = str(path)
        self.value = dict(schemaVersion=1, listener=dict(host="127.0.0.1", port=19443, **paths),
            viewerBearers={"staging": "reference-staging-" + "c"*32},
            workers={node: dict(bearer="reference-" + node + "-" + "d"*32,
                workerId="worker-" + node, instanceId="instance-" + node,
                resultOrigin="https://" + node + ".example",
                resultBearer="read-" + node + "-" + "e"*32) for node in ("cluster", "main")},
            streamBudget=dict(maxConcurrent=1, maxObjectBytes=100*1024**3))
        self.path = self.root / "reference-private.json"
        self.write_config()
        self.snapshot = dict(ready=True, commonOptions=[
            dict(name="future-quality", type="int", value=3, help="Quality level"),
            dict(name="future-toggle", type="bool", value=False),
            dict(name="input-file", type="string", value=""),
            dict(name="remote-url", type="string", value=""),
            dict(name="password", type="int", value=3),
            dict(name="safe-enum", type="string", value="high")], nodes=[
                dict(role=role, available=True, capabilitiesSha256="a"*64,
                     engine="odm", engineVersion="synthetic-1") for role in ("local", "main")])

    def write_config(self):
        self.path.write_text(json.dumps(self.value))
        if os.name != "nt":
            self.path.chmod(0o600)

    def settings(self):
        return load_reference_settings(str(self.path), self.config)

    def tls_patches(self):
        # No certificate generation or network: only startup loading is mocked.
        client_context = ssl.create_default_context()
        return (patch("processing.reference_runtime.ssl.SSLContext.load_cert_chain"),
                patch("processing.reference_runtime.verified_client_context", return_value=client_context))

    def runtime(self, queue, **kwargs):
        first, second = self.tls_patches()
        with first, second:
            return ReferenceRuntime(queue, self.config, self.settings(), discover=lambda: self.snapshot, **kwargs)

    def test_disabled_default_preserves_native_queue_without_reference_profile(self):
        handler = make_handler(self.config)
        self.addCleanup(handler.prototype_queue.close)
        self.addCleanup(handler.viewer_service.close)
        self.assertIsNone(handler.reference_listener)
        self.assertNotIn(REFERENCE_PROFILE, handler.prototype_queue.allowed_profiles)
        self.assertIn(VIEWER_PROFILE, handler.prototype_queue.allowed_profiles)

    def recovery_config(self,authority='11111111-1111-4111-8111-111111111111'):
        value=copy.deepcopy(self.value);value['schemaVersion']=2
        value['recoveryBindings']={node:dict(engineId='physical-'+node,authorityId=authority,
            coordinatorOrigin='http://127.0.0.1:18080',gateGeneration=1,outputBudget=1000)
            for node in self.config.prototype_node_ids}
        return value

    def test_staged_ack_config_requires_disjoint_canonical_scoped_secret(self):
        self.value['schemaVersion']=3
        token=base64.urlsafe_b64encode(b'z'*32).decode().rstrip('=')
        self.value['stagedAckPrincipals']=[{'environment':'staging','principalId':'viewer-a','bearer':token}];self.write_config()
        self.assertEqual(self.settings().staged_ack_bearers,{token:{'environment':'staging','principalId':'viewer-a'}})
        combined=self.recovery_config();combined['schemaVersion']=3;combined['stagedAckPrincipals']=[{'environment':'staging','principalId':'viewer-a','bearer':token}]
        previous=self.value;self.value=combined;self.write_config()
        self.assertIsNotNone(self.settings().recovery_bindings)
        self.value=previous
        self.value['stagedAckPrincipals']=[{'environment':'production','principalId':'viewer-a','bearer':token}];self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()
        self.value['stagedAckPrincipals']=[{'environment':'staging','principalId':'viewer-a','bearer':self.value['workers']['cluster']['bearer']}];self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()

    def test_staged_ack_runtime_requires_same_native_authority(self):
        from processing.viewer_service import ViewerService
        queue=Queue(self.config.prototype_state_dir,node_ids=self.config.prototype_node_ids,
                    treedar_node_ids=('main',),viewer_enabled=True,reference_enabled=True)
        self.addCleanup(queue.close)
        service=ViewerService(queue,self.config);self.addCleanup(service.close)
        self.value['schemaVersion']=3
        self.value['stagedAckPrincipals']=[{'environment':'staging','principalId':'viewer-a','bearer':base64.urlsafe_b64encode(b'z'*32).decode().rstrip('=')}];self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.runtime(queue)
        with self.assertRaises(RuntimeConfigurationError):self.runtime(queue,viewer_service=service)
        with patch('processing.reference_runtime.STAGED_ACK_SCHEMA_QUALIFIED',True):
            with self.assertRaises(RuntimeConfigurationError):self.runtime(queue,viewer_service=service)
            from processing.viewer_staged_ack_schema import initialize_staged_ack_schema
            initialize_staged_ack_schema(queue)
            runtime=self.runtime(queue,viewer_service=service)
        self.assertIsNotNone(runtime.api.staged_ack)

    def test_schema2_loads_closed_frozen_bindings_and_legacy_settings_default_is_none(self):
        legacy=self.settings();self.assertIsNone(legacy.recovery_bindings)
        self.assertIsNone(ReferenceSettings(legacy.listener,legacy.viewer_bearers,legacy.workers,legacy.stream_budget).recovery_bindings)
        self.value=self.recovery_config();self.write_config();loaded=self.settings()
        self.assertEqual(set(loaded.recovery_bindings),{'cluster','main'})
        self.assertTrue(all(type(value) is RecoveryNodeBinding for value in loaded.recovery_bindings.values()))
        self.assertEqual(loaded.recovery_bindings['main'].engine_id,'physical-main')
        self.value['recoveryBindings']['main']['outputBudget']=2
        self.assertEqual(loaded.recovery_bindings['main'].output_budget,1000)

    def test_schema2_configuration_rejects_unknown_missing_coverage_versions_and_types(self):
        original=self.recovery_config()
        changes=[lambda v:v.update(schemaVersion=True),lambda v:v.update(schemaVersion=2.0),lambda v:v.update(schemaVersion=3),
            lambda v:v.pop('recoveryBindings'),lambda v:v.update(recoveryBindings=None),lambda v:v.update(recoveryBindings={}),
            lambda v:v['recoveryBindings'].pop('main'),lambda v:v['recoveryBindings'].update(other=v['recoveryBindings']['main']),
            lambda v:v.update(extra=True),lambda v:v['recoveryBindings']['main'].update(nodeId='main'),
            lambda v:v['recoveryBindings']['main'].update(authorityId='22222222-2222-4222-8222-222222222222')]
        for key in ('engineId','authorityId','coordinatorOrigin','gateGeneration','outputBudget'):
            changes.append(lambda v,key=key:v['recoveryBindings']['main'].pop(key))
        for key,bad in [('engineId',True),('engineId','bad engine'),('authorityId','bad'),('authorityId',False),
            ('coordinatorOrigin','https://worker.example/'),('coordinatorOrigin','https://user:pass@worker.example'),
            ('coordinatorOrigin','https://worker.example/path'),('gateGeneration',True),('gateGeneration',1.0),
            ('gateGeneration',0),('outputBudget',False),('outputBudget',1.0),('outputBudget',0)]:
            changes.append(lambda v,key=key,bad=bad:v['recoveryBindings']['main'].update({key:bad}))
        for change in changes:
            self.value=copy.deepcopy(original);change(self.value);self.write_config()
            with self.subTest(change=change),self.assertRaises(RuntimeConfigurationError):self.settings()
        self.value=copy.deepcopy(original);self.value['schemaVersion']=1;self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()

    def test_invalid_schema2_is_rejected_before_queue_or_listener_creation(self):
        self.value=self.recovery_config();self.value['recoveryBindings'].pop('main');self.write_config()
        config=replace(self.config,reference_config_path=str(self.path))
        with patch('processing.app.Queue',side_effect=AssertionError('Queue must not open')),patch('processing.reference_runtime.create_reference_http_server',side_effect=AssertionError('listener must not open')):
            with self.assertRaises(RuntimeConfigurationError):make_handler(config)
        self.assertFalse(Path(config.prototype_state_dir).exists())

    def test_legacy_runtime_does_not_create_recovery_tables(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        runtime=self.runtime(queue);self.assertIsNone(runtime.control.worker.recovery)
        with queue._db() as db:
            tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertFalse(any(name.startswith('reference_recovery_') for name in tables))

    @unittest.skipUnless(os.name=='posix','Linux coordinator recovery authority')
    def test_schema2_authority_mismatch_fails_before_tls_services_or_listener_and_closes_queue(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True)
        actual=queue.authority_id;queue.close();self.value=self.recovery_config();self.write_config()
        self.assertNotEqual(actual,self.value['recoveryBindings']['main']['authorityId'])
        config=replace(self.config,viewer_enabled=False,reference_config_path=str(self.path))
        with patch('processing.reference_runtime.ssl.SSLContext.load_cert_chain',side_effect=AssertionError('no TLS assembly')),patch('processing.reference_runtime.create_reference_http_server',side_effect=AssertionError('no listener')):
            with self.assertRaises(RuntimeConfigurationError):make_handler(config)
        reopened=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(reopened.close)
        self.assertEqual(reopened.authority_id,actual)
        with reopened._db() as db:self.assertFalse(db.execute("SELECT name FROM sqlite_master WHERE name GLOB 'reference_recovery_*'").fetchall())

    @unittest.skipUnless(os.name=='posix','Linux coordinator recovery authority')
    def test_configured_handler_finishes_recovery_factory_before_any_listener_or_dispatcher_start(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);authority=queue.authority_id;queue.close()
        self.value=self.recovery_config(authority);self.write_config();config=replace(self.config,reference_config_path=str(self.path))
        first,second=self.tls_patches()
        with first,second:handler=make_handler(config)
        self.addCleanup(handler.prototype_queue.close);self.addCleanup(handler.viewer_service.close)
        runtime=handler.reference_listener
        self.assertIsNone(runtime._server);self.assertIsNone(runtime._thread);self.assertIsNone(handler.viewer_service._thread)
        self.assertIsNotNone(runtime.control.worker.recovery)
        with handler.prototype_queue._db() as db:
            startup=db.execute('SELECT * FROM reference_recovery_startup WHERE singleton=1').fetchone()
            self.assertEqual(startup['epoch_id'],handler.prototype_queue.startup_id)
            self.assertEqual(startup['authority_id'],authority)

    def test_direct_settings_cannot_bypass_recovery_binding_type_or_node_coverage(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        settings=self.settings()
        for bindings in ({},{'cluster':{}},{'cluster':{},'main':{}},[]):
            with self.subTest(bindings=bindings),self.assertRaises(RuntimeConfigurationError):
                ReferenceRuntime(queue,self.config,replace(settings,recovery_bindings=bindings))

    @unittest.skipUnless(os.name=='posix','Linux coordinator recovery authority')
    def test_configured_recovery_routes_resume_original_claim_and_same_epoch_reassembly_does_not_rehold(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        from processing.viewer_tasks import TaskStore
        from processing.transfer_fence import install_offline
        TaskStore(queue, self.root/'state'/'viewer-tasks')
        with queue._db() as db: install_offline(db, enabled=True)
        self.value=self.recovery_config(queue.authority_id);self.write_config();runtime=self.runtime(queue)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        from processing import reference_protocol as protocol
        checked=protocol.validate_submission(fixture['submission'],environment='staging',capability_fingerprint=fixture['submission']['capabilityFingerprint'])
        job=runtime.store.submit(checked)[0];identity=self.value['workers']['cluster']
        claim=runtime.store.claim(job['jobId'],node_id='cluster',worker_id=identity['workerId'],worker_instance_id=identity['instanceId'],claim_request_id='original-runtime-claim')
        runtime.store.reserve(job['jobId'],fixture['transferRequest'],node_id='cluster');runtime.store.update_phase(job['jobId'],'fetching');runtime.store.update_phase(job['jobId'],'processing')
        queue.close();queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        runtime=self.runtime(queue);prefix='/ltds/worker/v1/jobs/'+job['jobId']+'/'
        headers={'Authorization':'Bearer '+identity['bearer'],'X-Claim-Token':claim['claimToken']}
        def call(method,suffix,body=b'',protected=True):return runtime.api.handle(method,prefix+suffix,headers,body,protected_transport=protected)
        self.assertEqual(call('GET','recovery-state',protected=False).status,503)
        state=call('GET','recovery-state');self.assertEqual(state.status,200,state.body);state=state.body
        self.assertTrue(state['eligible']);self.assertEqual(state['claim']['heldReason'],'coordinator_restart')
        binding=state['physicalBinding'];parent={key:state['claim'][key] for key in ('jobId','generation','environment','nodeId','workerId','workerInstanceId','authorityId','claimRequestId','claimTokenSha256','manifestSha256','pipelineDigest')}
        parent.update(engineId=binding['engine_id'],coordinatorOrigin=binding['coordinator_origin'],gateGeneration=binding['gate_generation'],outputBudget=binding['output_budget'],inputBytes=state['reservation']['inputBytes'],reservationId=state['reservation']['reservationId'],gateReceiptId='a'*32)
        request=dict(schemaVersion=1,requestId=str(uuid.uuid4()),coordinatorEpoch=state['coordinatorEpoch'],expectedNodeRevision=state['node']['revision'],parentBinding=parent,transferRequestSha256=state['reservation']['transferRequestSha256'],workerEvidenceSha256='e'*64)
        with queue._db() as db:
            db.execute('INSERT INTO engine_ingress_uploads VALUES (?,?,?,?,?,?)',
                       ('unknown-upload','unrelated-job','other-engine',1,'a'*64,'unknown'))
        rejected=call('POST','recover',protocol.canonical_json(request))
        self.assertNotEqual(rejected.status,200)
        self.assertEqual(queue.get(job['jobId'])['status'],'held')
        with queue._db() as db: db.execute("DELETE FROM engine_ingress_uploads WHERE upload_id='unknown-upload'")
        resumed=call('POST','recover',protocol.canonical_json(request));self.assertEqual(resumed.status,200,resumed.body)
        with queue._db() as db:
            db.execute('INSERT INTO engine_ingress_uploads VALUES (?,?,?,?,?,?)',
                       ('unknown-upload','unrelated-job','other-engine',1,'a'*64,'unknown'))
        replay=call('POST','recover',protocol.canonical_json(request))
        self.assertEqual(replay.status,200);self.assertEqual(replay.body,resumed.body)
        self.assertEqual(queue.get(job['jobId'])['status'],'running');epoch=queue.startup_id
        second=self.runtime(queue);self.assertEqual(queue.startup_id,epoch);self.assertEqual(queue.get(job['jobId'])['status'],'running')
        receipt=second.api.handle('GET',prefix+'recovery-receipts/'+request['requestId'],headers,protected_transport=True)
        self.assertEqual(receipt.status,200);self.assertEqual(receipt.body,resumed.body)
        self.assertEqual(second.store.get(job['jobId'],private=True)['claimToken'],claim['claimToken'])

    def test_explicit_runtime_shares_exact_native_queue_authority_and_global_slot(self):
        config = replace(self.config, reference_config_path=str(self.path))
        first, second = self.tls_patches()
        with first, second:
            handler = make_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        self.addCleanup(handler.viewer_service.close)
        runtime = handler.reference_listener
        runtime.api.discover = lambda: self.snapshot
        self.assertIs(runtime.queue, handler.prototype_queue)
        self.assertIs(runtime.store.queue, handler.viewer_service.queue)
        self.assertEqual(runtime.queue.authority_id, handler.viewer_service.queue.authority_id)
        capabilities = runtime.api.handle("GET", "/ltds/v1/capabilities",
            {"Authorization": "Bearer " + self.value["viewerBearers"]["staging"]}, protected_transport=True)
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              "contracts/reference-v1.contract.json").read_text())["submission"]
        fixture["capabilityFingerprint"] = capabilities.body["capabilityFingerprint"]
        fixture["pipeline"]["odm"]["optionsFingerprint"] = capabilities.body["engines"]["odm"]["optionsFingerprint"]
        response = runtime.api.handle("POST", "/ltds/v1/jobs",
            {"Authorization": "Bearer " + self.value["viewerBearers"]["staging"]},
            json.dumps(fixture).encode(), protected_transport=True)
        self.assertEqual(response.status, 201)
        runtime.store.claim(response.body["jobId"], node_id="cluster", worker_id="worker-cluster",
            worker_instance_id="instance-cluster", claim_request_id="reference-claim")
        self.assertIsNone(handler.prototype_queue.claim({"workerId": "legacy", "profiles": [VIEWER_PROFILE]}))
        runtime.close()

    def test_shared_admission_drain_blocks_real_reference_submission(self):
        config = replace(self.config, reference_config_path=str(self.path), admission_drain_enabled=True)
        first, second = self.tls_patches()
        with first, second, patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True): handler = make_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        self.addCleanup(handler.viewer_service.close)
        runtime = handler.reference_listener
        runtime.api.discover = lambda: self.snapshot
        headers = {'Authorization': 'Bearer ' + self.value['viewerBearers']['staging']}
        caps = runtime.api.handle('GET', '/ltds/v1/capabilities', headers, protected_transport=True)
        fixture = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        fixture['capabilityFingerprint'] = caps.body['capabilityFingerprint']
        fixture['pipeline']['odm']['optionsFingerprint'] = caps.body['engines']['odm']['optionsFingerprint']
        handler.prototype_queue.admission_drain.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=handler.prototype_queue.admission_drain.status()['generation'],expected_closed=False,expected_authority=handler.prototype_queue.authority_id)
        response = runtime.api.handle('POST','/ltds/v1/jobs',headers,json.dumps(fixture).encode(),protected_transport=True)
        self.assertEqual(response.status, 503)
        self.assertEqual(handler.prototype_queue.list(), [])
        handler.prototype_queue.admission_drain.set_closed(False,request_id=str(uuid.uuid4()),expected_generation=handler.prototype_queue.admission_drain.status()['generation'],expected_closed=True,expected_authority=handler.prototype_queue.authority_id)
        response = runtime.api.handle('POST','/ltds/v1/jobs',headers,json.dumps(fixture).encode(),protected_transport=True)
        self.assertEqual(response.status, 201)
        job_id = response.body['jobId']
        claim = dict(node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster',claim_request_id='synthetic-claim')
        handler.prototype_queue.admission_drain.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=handler.prototype_queue.admission_drain.status()['generation'],expected_closed=False,expected_authority=handler.prototype_queue.authority_id)
        self.assertIsNone(runtime.store.claim(job_id, **claim))
        handler.prototype_queue.admission_drain.set_closed(False,request_id=str(uuid.uuid4()),expected_generation=handler.prototype_queue.admission_drain.status()['generation'],expected_closed=True,expected_authority=handler.prototype_queue.authority_id)
        self.assertIsNotNone(runtime.store.claim(job_id, **claim))
        runtime.close()

    def test_reference_closed_restart_preserves_job_and_blocks_new_claims(self):
        self.value['viewerBearers']['production']='reference-production-'+'p'*32
        self.write_config()
        config=replace(self.config,reference_config_path=str(self.path),admission_drain_enabled=True,
                       prototype_operator_token='operator-'+'o'*32)
        first,second=self.tls_patches()
        with first,second,patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True):handler=make_handler(config)
        runtime=handler.reference_listener;runtime.api.discover=lambda:self.snapshot
        staging={'Authorization':'Bearer '+self.value['viewerBearers']['staging']}
        caps=runtime.api.handle('GET','/ltds/v1/capabilities',staging,protected_transport=True)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        fixture['capabilityFingerprint']=caps.body['capabilityFingerprint']
        fixture['pipeline']['odm']['optionsFingerprint']=caps.body['engines']['odm']['optionsFingerprint']
        body=json.dumps(fixture).encode()
        with patch.object(runtime.store,'_submit',wraps=runtime.store._submit) as submitted:
            created=runtime.api.handle('POST','/ltds/v1/jobs',staging,body,protected_transport=True)
            validated=submitted.call_args.args[0]
        self.assertEqual(created.status,201);job_id=created.body['jobId']
        gate=handler.prototype_queue.admission_drain;state=gate.status()
        gate.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=state['generation'],expected_closed=False,expected_authority=state['authorityId'])
        runtime.close();handler.viewer_service.close();handler.prototype_queue.close()
        first,second=self.tls_patches()
        with first,second,patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True):restarted=make_handler(replace(config,admission_start_closed=True))
        self.addCleanup(restarted.prototype_queue.close);self.addCleanup(restarted.viewer_service.close)
        runtime=restarted.reference_listener;self.addCleanup(runtime.close);runtime.api.discover=lambda:self.snapshot
        self.assertTrue(restarted.prototype_queue.admission_drain.status()['closed'])
        self.assertEqual(runtime.store.get(job_id)['status'],'queued')
        from processing.admission_drain import AdmissionClosed
        with self.assertRaises(AdmissionClosed):runtime.store.submit(validated)
        with self.assertRaises(AdmissionClosed):runtime.store._submit(validated)
        for token in (self.value['viewerBearers']['production'],self.value['viewerBearers']['staging']):
            response=runtime.api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+token},body,protected_transport=True)
            self.assertEqual(response.status,503)
        self.assertEqual(runtime.api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer wrong'},body,protected_transport=True).status,401)
        claim_body=json.dumps({'jobId':job_id,'claimRequestId':'closed-test-claim'}).encode()
        worker={'Authorization':'Bearer '+self.value['workers']['cluster']['bearer']}
        self.assertEqual(runtime.api.handle('POST','/ltds/worker/v1/claim',worker,claim_body,protected_transport=True).status,409)
        self.assertEqual(runtime.api.handle('POST','/ltds/worker/v1/claim',{'Authorization':'Bearer wrong'},claim_body,protected_transport=True).status,401)
        self.assertEqual(runtime.store.get(job_id)['status'],'queued')
        self.assertEqual(len(restarted.prototype_queue.list()),1)
        # A corrupted authority marker also holds admission; no new work is
        # written even when the listener and credentials remain valid.
        with restarted.prototype_queue._db() as db:
            db.execute("UPDATE coordinator_admission SET authority_id='synthetic-wrong-authority'")
        self.assertEqual(runtime.api.handle('POST','/ltds/v1/jobs',staging,body,protected_transport=True).status,503)
        self.assertEqual(len(restarted.prototype_queue.list()),1)

    def test_reference_inflight_submission_drains_before_close_receipt(self):
        config=replace(self.config,reference_config_path=str(self.path),admission_drain_enabled=True,
                       prototype_operator_token='operator-'+'o'*32)
        first,second=self.tls_patches()
        with first,second,patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True):handler=make_handler(config)
        self.addCleanup(handler.prototype_queue.close);self.addCleanup(handler.viewer_service.close)
        runtime=handler.reference_listener;self.addCleanup(runtime.close);runtime.api.discover=lambda:self.snapshot
        headers={'Authorization':'Bearer '+self.value['viewerBearers']['staging']}
        caps=runtime.api.handle('GET','/ltds/v1/capabilities',headers,protected_transport=True)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
        fixture['capabilityFingerprint']=caps.body['capabilityFingerprint']
        fixture['pipeline']['odm']['optionsFingerprint']=caps.body['engines']['odm']['optionsFingerprint']
        body=json.dumps(fixture).encode();entered=threading.Event();release=threading.Event();responses=[];closed=[]
        server=ThreadingHTTPServer(('127.0.0.1',0),handler)
        server_thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02})
        server_thread.start()
        def native_post(token):
            connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
            try:
                connection.request('POST','/task/new/init?token='+token,body=b'{}',headers={'Content-Type':'application/json'})
                response=connection.getresponse();response.read();return response.status
            finally:connection.close()
        self.addCleanup(server_thread.join,3);self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        original=runtime.store._submit
        def blocked(value):
            entered.set()
            if not release.wait(5):raise RuntimeError('synthetic deadline')
            return original(value)
        gate=handler.prototype_queue.admission_drain;state=gate.status()
        def close():
            closed.append(gate.set_closed(True,request_id=str(uuid.uuid4()),expected_generation=state['generation'],expected_closed=False,expected_authority=state['authorityId']))
        with patch.object(runtime.store,'_submit',side_effect=blocked):
            submit=threading.Thread(target=lambda:responses.append(runtime.api.handle('POST','/ltds/v1/jobs',headers,body,protected_transport=True)))
            submit.start();closer=None
            try:
                self.assertTrue(entered.wait(3));closer=threading.Thread(target=close);closer.start()
                self.assertTrue(gate._closing.wait(3));self.assertTrue(closer.is_alive())
                denied=runtime.api.handle('POST','/ltds/v1/jobs',headers,body,protected_transport=True)
                self.assertEqual(denied.status,503)
                self.assertEqual(native_post(config.viewer_token),503)
                self.assertEqual(native_post('wrong'),401)
                release.set();submit.join(5);closer.join(5)
                self.assertFalse(submit.is_alive());self.assertFalse(closer.is_alive())
                self.assertEqual(responses[0].status,201);self.assertTrue(closed[0]['closed'])
                self.assertEqual(len(handler.prototype_queue.list()),1)
            finally:
                release.set();submit.join(6)
                if closer is not None:closer.join(6)

    @unittest.skipUnless(os.name=='posix','synthetic openssl fixture requires POSIX')
    def test_real_tls_shared_admission_close_restart_and_replay(self):
        cert=self.root/'ephemeral-cert.pem';key=self.root/'ephemeral-key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes',
            '-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=localhost',
            '-addext','subjectAltName=IP:127.0.0.1'],check=True,timeout=15,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        key.chmod(0o600)
        self.value['listener'].update(certificate=str(cert),privateKey=str(key),clientCA=str(cert))
        self.value['viewerBearers']['production']='reference-production-'+'p'*32
        self.write_config()
        operator='operator-'+'o'*32
        config=replace(self.config,reference_config_path=str(self.path),admission_drain_enabled=True,
                       prototype_operator_token=operator)
        tls_context=ssl.create_default_context(cafile=str(cert))
        stages=[];release=threading.Event();workers=[]
        def free_port():
            with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
        def start(current):
            self.value['listener']['port']=free_port();self.write_config()
            with patch('processing.config.ADMISSION_DRAIN_QUALIFIED',True):handler=make_handler(current)
            native=None
            try:
                handler.reference_listener.api.discover=lambda:self.snapshot
                native=ThreadingHTTPServer(('127.0.0.1',0),handler)
                lifecycle=CoordinatorService(native,handler);stop=threading.Event();result=[]
                thread=threading.Thread(target=lambda:result.append(lifecycle.run(stop)))
                thread.start()
            except BaseException:
                if native is not None:native.server_close()
                handler.reference_listener.close();handler.viewer_service.close();handler.prototype_queue.close()
                raise
            stages.append((stop,thread,result))
            deadline=time.monotonic()+5
            while not lifecycle.healthy():
                if not thread.is_alive() or time.monotonic()>=deadline:raise AssertionError('synthetic coordinator startup')
                time.sleep(.02)
            return handler,native.server_port,self.value['listener']['port']
        def call(port,method,path,token,body=None,*,tls=False):
            conn=(http.client.HTTPSConnection('127.0.0.1',port,timeout=5,context=tls_context) if tls
                  else http.client.HTTPConnection('127.0.0.1',port,timeout=5))
            try:
                payload=None if body is None else json.dumps(body).encode()
                headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
                conn.request(method,path,body=payload,headers=headers)
                response=conn.getresponse();data=json.loads(response.read());return response.status,data
            finally:conn.close()
        def stop_stage():
            stop,thread,result=stages[-1];stop.set();thread.join(8)
            self.assertFalse(thread.is_alive());self.assertEqual(result,[0])
        try:
            handler,native_port,tls_port=start(config)
            staging=self.value['viewerBearers']['staging'];production=self.value['viewerBearers']['production']
            worker=self.value['workers']['cluster']['bearer']
            code,caps=call(tls_port,'GET','/ltds/v1/capabilities',staging,tls=True);self.assertEqual(code,200)
            self.assertEqual(call(tls_port,'GET','/ltds/v1/capabilities','wrong',tls=True)[0],401)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())['submission']
            fixture['capabilityFingerprint']=caps['capabilityFingerprint']
            fixture['pipeline']['odm']['optionsFingerprint']=caps['engines']['odm']['optionsFingerprint']
            status,first=call(tls_port,'POST','/ltds/v1/jobs',staging,fixture,tls=True);self.assertEqual(status,201)
            second=copy.deepcopy(fixture);second['submissionId']=str(uuid.uuid4())
            gate=handler.prototype_queue.admission_drain;state=gate.status();entered=threading.Event();responses=[];closed=[]
            original=handler.reference_listener.store._submit
            def blocked(value):
                entered.set()
                if not release.wait(8):raise RuntimeError('synthetic deadline')
                return original(value)
            close_body=dict(closed=True,requestId=str(uuid.uuid4()),expectedGeneration=state['generation'],
                            expectedClosed=False,expectedAuthorityId=state['authorityId'])
            with patch.object(handler.reference_listener.store,'_submit',side_effect=blocked):
                submit=threading.Thread(target=lambda:responses.append(call(tls_port,'POST','/ltds/v1/jobs',staging,second,tls=True)))
                submit.start();workers.append(submit)
                self.assertTrue(entered.wait(4))
                closer=threading.Thread(target=lambda:closed.append(call(native_port,'POST','/v1/prototype/admission-drain',operator,close_body)))
                closer.start();workers.append(closer)
                self.assertTrue(gate._closing.wait(4));self.assertTrue(closer.is_alive())
                for token in (staging,production):self.assertEqual(call(tls_port,'POST','/ltds/v1/jobs',token,fixture,tls=True)[0],503)
                self.assertEqual(call(tls_port,'POST','/ltds/v1/jobs','wrong',fixture,tls=True)[0],401)
                self.assertEqual(call(native_port,'POST','/task/new/init?token='+config.viewer_token,'unused',{})[0],503)
                self.assertEqual(call(native_port,'POST','/task/new/init?token=wrong','unused',{})[0],401)
                release.set();submit.join(8);closer.join(8)
                self.assertFalse(submit.is_alive());self.assertFalse(closer.is_alive())
                self.assertEqual(responses[0][0],201);self.assertEqual(closed[0][0],200)
            claim={'jobId':first['jobId'],'claimRequestId':'tls-closed-claim'}
            self.assertEqual(call(tls_port,'POST','/ltds/worker/v1/claim',worker,claim,tls=True)[0],409)
            self.assertEqual(call(tls_port,'POST','/ltds/worker/v1/claim','wrong',claim,tls=True)[0],401)
            stop_stage()
            handler,native_port,tls_port=start(replace(config,admission_start_closed=True))
            self.assertTrue(handler.prototype_queue.admission_drain.status()['closed'])
            self.assertEqual(call(tls_port,'GET','/ltds/v1/jobs/'+first['jobId'],staging,tls=True)[1]['status'],'queued')
            self.assertEqual(call(tls_port,'POST','/ltds/v1/jobs',staging,fixture,tls=True)[0],503)
            self.assertEqual(call(native_port,'POST','/task/new/init?token='+config.viewer_token,'unused',{})[0],503)
            state=handler.prototype_queue.admission_drain.status()
            opened=dict(closed=False,requestId=str(uuid.uuid4()),expectedGeneration=state['generation'],
                        expectedClosed=True,expectedAuthorityId=state['authorityId'])
            self.assertEqual(call(native_port,'POST','/v1/prototype/admission-drain',operator,opened)[0],200)
            status,replay=call(tls_port,'POST','/ltds/v1/jobs',staging,fixture,tls=True)
            self.assertEqual((status,replay['jobId']),(200,first['jobId']))
            self.assertEqual(len(handler.prototype_queue.list()),2)
            stop_stage()
        finally:
            release.set()
            for stop,_,_ in stages:stop.set()
            for worker_thread in workers:worker_thread.join(9)
            for _,thread,_ in stages:thread.join(9)
            if any(thread.is_alive() for thread in workers) or any(thread.is_alive() for _,thread,_ in stages):
                raise AssertionError('synthetic admission test thread did not exit')

    def test_actual_catalog_changes_fingerprints_and_stale_admission_is_rejected(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=("cluster", "main"), reference_enabled=True)
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        headers = {"Authorization": "Bearer " + self.value["viewerBearers"]["staging"]}
        def caps():
            return runtime.api.handle("GET", "/ltds/v1/capabilities", headers, protected_transport=True)
        before = caps()
        listed = runtime.api.handle('GET','/ltds/v1/options',headers,protected_transport=True)
        self.assertEqual(listed.status,200)
        self.assertEqual(listed.body['environment'],'staging')
        self.assertEqual(listed.body['optionsFingerprint'],before.body['engines']['odm']['optionsFingerprint'])
        self.assertTrue(all(option['type'] in ('bool','int','float') for option in listed.body['options']))
        self.snapshot["commonOptions"].append(dict(name="new-engine-flag", type="bool", value=True))
        after = caps()
        refreshed = runtime.api.handle('GET','/ltds/v1/options',headers,protected_transport=True)
        self.assertIn('new-engine-flag',{option['name'] for option in refreshed.body['options']})
        refreshed.body['options'].clear()
        self.assertTrue(self.snapshot['commonOptions'])
        self.assertNotEqual(before.body["capabilityFingerprint"], after.body["capabilityFingerprint"])
        self.assertNotEqual(before.body["engines"]["odm"]["optionsFingerprint"],
                            after.body["engines"]["odm"]["optionsFingerprint"])
        raw = json.loads((Path(__file__).resolve().parents[1] /
                          "contracts/reference-v1.contract.json").read_text())["submission"]
        raw["capabilityFingerprint"] = before.body["capabilityFingerprint"]
        raw["pipeline"]["odm"]["optionsFingerprint"] = before.body["engines"]["odm"]["optionsFingerprint"]
        self.assertEqual(runtime.api.handle("POST", "/ltds/v1/jobs", headers,
            json.dumps(raw).encode(), protected_transport=True).status, 400)
        self.snapshot["ready"] = False
        self.assertEqual(caps().status, 503)

    def test_discovery_allowlist_uses_definitions_and_excludes_string_path_credentials(self):
        first = discovered_policy(self.snapshot, self.config)
        self.assertEqual(first[2], {"future-quality", "future-toggle"})
        reordered = copy.deepcopy(self.snapshot)
        reordered["commonOptions"].reverse()
        reordered["nodes"].reverse()
        self.assertEqual(first, discovered_policy(reordered, self.config))
        changed = copy.deepcopy(self.snapshot)
        changed["nodes"][0]["engineVersion"] = "synthetic-2"
        self.assertNotEqual(first[0], discovered_policy(changed, self.config)[0])

    def test_listener_only_starts_explicitly_and_stops_before_queue_release(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=("cluster", "main"), reference_enabled=True)
        self.addCleanup(queue.close)
        server, factory_calls = FakeServer(), []
        def factory(*args, **kwargs):
            self.assertEqual(kwargs["ssl_context"].protocol, ssl.PROTOCOL_TLS_SERVER)
            factory_calls.append(True)
            return server
        runtime = self.runtime(queue, server_factory=factory)
        self.assertEqual(factory_calls, [])
        runtime.start()
        runtime.start()
        runtime.close()
        self.assertEqual(len(factory_calls), 1)
        self.assertIn("shutdown", server.events)
        self.assertEqual(server.events[-1], "closed")
        self.assertFalse(server.daemon_threads)
        self.assertTrue(server.block_on_close)

    def test_plaintext_or_missing_tls_configuration_cannot_construct_listener(self):
        self.value["listener"].pop("privateKey")
        self.write_config()
        with self.assertRaises(RuntimeConfigurationError):
            self.settings()
        self.value["listener"]["privateKey"] = str(self.root / "privateKey.pem")
        self.write_config()
        queue = Queue(self.config.prototype_state_dir, node_ids=("cluster", "main"), reference_enabled=True)
        self.addCleanup(queue.close)
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue, self.config, self.settings())  # Fake files are not real TLS material.

    def test_credentials_are_separate_and_private_configuration_is_not_represented(self):
        settings = self.settings()
        for token in self.value["viewerBearers"].values():
            self.assertNotIn(token, repr(settings))
        self.value["workers"]["cluster"]["bearer"] = self.config.api_token
        self.write_config()
        with self.assertRaises(RuntimeConfigurationError) as caught:
            self.settings()
        self.assertNotIn(self.config.api_token, repr(caught.exception))

    def test_private_file_duplicate_keys_and_size_are_enforced(self):
        self.path.write_text('{"schemaVersion":1,"schemaVersion":1}')
        with self.assertRaises(RuntimeConfigurationError):
            self.settings()
        self.path.write_bytes(b" " * (64*1024 + 1))
        with self.assertRaises(RuntimeConfigurationError):
            self.settings()
        self.write_config()
        if os.name != "nt":
            self.path.chmod(0o644)
            with self.assertRaises(RuntimeConfigurationError):
                self.settings()

    def test_stream_budget_admission_is_bounded_and_released(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=("cluster", "main"), reference_enabled=True)
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        with runtime._admit_read({"byteSize": 3}):
            with self.assertRaises(RuntimeConfigurationError):
                with runtime._admit_read({"byteSize": 3}):
                    self.fail("over budget")
        with runtime._admit_read({"byteSize": 3}):
            pass
        with self.assertRaises(RuntimeConfigurationError):
            with runtime._admit_read({"byteSize": 101*1024**3}):
                self.fail("object over budget")

    def test_environment_only_names_private_file_without_enabling_default(self):
        env = {"PROCESSING_API_TOKEN": "a"*32}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(from_environment().reference_config_path, "")
        with patch.dict(os.environ, dict(env, PROCESSING_REFERENCE_CONFIG=str(self.path)), clear=True):
            config = from_environment()
            self.assertEqual(config.reference_config_path, str(self.path))
            self.assertNotIn(str(self.path), repr(config))

    def supervised_handler(self):
        config = replace(self.config, viewer_enabled=False, reference_config_path=str(self.path))
        first, second = self.tls_patches()
        with first, second:
            handler = make_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        return handler

    def wait_for(self, condition):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if condition():
                return
            threading.Event().wait(0.01)
        self.fail('coordinator lifecycle condition not reached')

    def test_coordinator_shutdown_joins_native_and_reference_handlers_before_queue_unlock(self):
        handler = self.supervised_handler()
        native_entered, native_release = threading.Event(), threading.Event()
        reference_entered, reference_release, reference_closed = threading.Event(), threading.Event(), threading.Event()
        original_get = handler.do_GET

        def blocked_get(request):
            if request.path == '/blocked':
                native_entered.set()
                native_release.wait(timeout=6)
                request._reply(200, {'ok': True})
            else:
                original_get(request)

        handler.do_GET = blocked_get

        class Stream:
            def chunks(self):
                reference_entered.set()
                reference_release.wait(timeout=6)
                yield b'abc'

            def close(self):
                reference_closed.set()

        class Adapter:
            def handle(self, *_args, **_kwargs):
                return BinaryResponse(Stream(), headers={'Content-Type':'application/octet-stream','Content-Length':'3'})

        reference_servers = []

        def reference_factory(*_args, **_kwargs):
            server = create_reference_http_server(Adapter())
            reference_servers.append(server)
            return server

        handler.reference_listener._server_factory = reference_factory
        native = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(native, handler)
        stop, exits, errors = threading.Event(), [], []
        running = threading.Thread(target=lambda: exits.append(service.run(stop)))
        running.start()
        self.wait_for(service.healthy)

        def consume(server, path):
            connection = http.client.HTTPConnection(*server.server_address, timeout=6)
            try:
                connection.request('GET', path)
                response = connection.getresponse()
                response.read()
            except Exception as error:
                errors.append(type(error).__name__)
            finally:
                connection.close()

        clients = [threading.Thread(target=consume, args=(native,'/blocked')),
                   threading.Thread(target=consume, args=(reference_servers[0],'/result'))]
        for client in clients:
            client.start()
        try:
            self.assertTrue(native_entered.wait(timeout=3))
            self.assertTrue(reference_entered.wait(timeout=3))
            stop.set()
            self.wait_for(lambda: service.health()['state'] == 'draining')
            self.assertFalse(service.healthy())
            self.wait_for(lambda: not service.health()['referenceAlive'])
            with self.assertRaises(RuntimeError):
                Queue(self.config.prototype_state_dir, node_ids=('cluster','main'))
            native_release.set()
            self.assertTrue(running.is_alive())
            self.assertFalse(reference_closed.is_set())
            with self.assertRaises(RuntimeError):
                Queue(self.config.prototype_state_dir, node_ids=('cluster','main'))
            reference_release.set()
            running.join(timeout=3)
            for client in clients:
                client.join(timeout=3)
            self.assertFalse(running.is_alive())
            self.assertTrue(reference_closed.is_set())
            self.assertEqual(exits, [0])
            self.assertEqual(errors, [])
            self.assertEqual(service.health()['state'], 'stopped')
            reopened = Queue(self.config.prototype_state_dir, node_ids=('cluster','main'))
            reopened.close()
        finally:
            native_release.set()
            reference_release.set()
            stop.set()
            running.join(timeout=7)
            for client in clients:
                client.join(timeout=7)

    def test_reference_listener_failure_fails_supervision_without_healthy_native_endpoint(self):
        handler = self.supervised_handler()
        fail = threading.Event()

        class FailingServer(FakeServer):
            def serve_forever(self, **kwargs):
                fail.wait(timeout=4)
                raise RuntimeError('private-test-credential /private/file')

        handler.reference_listener._server_factory = lambda *_args, **_kwargs: FailingServer()
        native = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(native, handler)
        stop, exits = threading.Event(), []
        errors = io.StringIO()
        with redirect_stderr(errors):
            running = threading.Thread(target=lambda: exits.append(service.run(stop)))
            running.start()
            try:
                self.wait_for(service.healthy)
                fail.set()
                running.join(timeout=3)
                self.assertFalse(running.is_alive())
                self.assertEqual(exits, [2])
                self.assertFalse(service.healthy())
                self.assertEqual(service.health()['reason'], 'reference_listener_failed')
                self.assertNotIn('private-test-credential', json.dumps(service.health()))
                self.assertNotIn('/private/file', json.dumps(service.health()))
            finally:
                fail.set()
                stop.set()
                running.join(timeout=5)
        self.assertEqual(errors.getvalue(), '')

    def test_dispatcher_inflight_work_is_joined_past_legacy_close_timeout_before_queue_unlock(self):
        config = replace(self.config, viewer_enabled=False)
        handler = make_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        entered, release, closed = threading.Event(), threading.Event(), threading.Event()

        class Dispatcher:
            def __init__(self):
                self._stop = threading.Event()

            def start(self):
                def work():
                    entered.set()
                    self._stop.wait()
                    release.wait(timeout=5)
                self._thread = threading.Thread(target=work)
                self._thread.start()

            def close(self):
                self._stop.set()
                self._thread.join(timeout=0.01)
                closed.set()

        handler.viewer_service = Dispatcher()
        native = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(native, handler)
        stop, exits = threading.Event(), []
        running = threading.Thread(target=lambda: exits.append(service.run(stop)))
        running.start()
        try:
            self.assertTrue(entered.wait(timeout=3))
            self.wait_for(service.healthy)
            stop.set()
            self.assertTrue(closed.wait(timeout=3))
            self.assertTrue(running.is_alive())
            with self.assertRaises(RuntimeError):
                Queue(self.config.prototype_state_dir, node_ids=('cluster','main'))
            release.set()
            running.join(timeout=3)
            self.assertEqual(exits, [0])
        finally:
            release.set()
            stop.set()
            running.join(timeout=6)

    def test_failed_dispatcher_drain_retains_authority_while_process_lives(self):
        config = replace(self.config, viewer_enabled=False)
        handler = make_handler(config)
        self.addCleanup(handler.prototype_queue.close)

        class UncertainDispatcher:
            def __init__(self): self._stop = threading.Event()
            def start(self): pass
            def close(self): raise RuntimeError('synthetic uncertain child')

        handler.viewer_service = UncertainDispatcher()
        native = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        service = CoordinatorService(native, handler)
        stop, exits = threading.Event(), []
        running = threading.Thread(target=lambda: exits.append(service.run(stop)))
        running.start()
        try:
            self.wait_for(service.healthy)
            stop.set()
            running.join(timeout=3)
            self.assertFalse(running.is_alive())
            self.assertEqual(exits, [2])
            self.assertEqual(service.health()['reason'], 'dispatcher_drain_failed')
            with self.assertRaises(RuntimeError):
                Queue(self.config.prototype_state_dir, node_ids=('cluster','main'))
        finally:
            stop.set()
            running.join(timeout=5)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'actual POSIX SIGTERM semantics')
    def test_main_sigterm_exits_cleanly_and_preserves_default_native_health_contract(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        state = self.root / 'signal-state'
        token = 'synthetic-signal-token-' + 'x' * 32
        environment = {key: os.environ[key] for key in ('PATH','PYTHONDONTWRITEBYTECODE') if key in os.environ}
        environment.update(PROCESSING_API_TOKEN=token, PROCESSING_BIND_HOST='127.0.0.1',
                           PROCESSING_PORT=str(port), PROCESSING_PROTOTYPE_STATE_DIR=str(state))
        process = subprocess.Popen([sys.executable, '-m', 'processing.app'],
            cwd=Path(__file__).resolve().parents[1], env=environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 4
            health = None
            while time.monotonic() < deadline and process.poll() is None:
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=0.2)
                try:
                    connection.request('GET', '/healthz')
                    response = connection.getresponse()
                    body = json.loads(response.read())
                    if response.status == 200:
                        health = body
                        break
                except OSError:
                    pass
                finally:
                    connection.close()
                threading.Event().wait(0.02)
            self.assertIsNotNone(health)
            self.assertEqual(health['status'], 'ok')
            self.assertEqual(health['mode'], 'routing_probe')
            self.assertFalse(health['viewerEnabled'])
            with self.assertRaises(RuntimeError):
                Queue(state)
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=4)
            self.assertEqual(process.returncode, 0)
            self.assertEqual(stderr, '')
            self.assertNotIn(token, stdout)
            reopened = Queue(state)
            reopened.close()
        finally:
            if process.poll() is None:
                process.kill()  # Only this test-created routing-only subprocess.
                process.communicate(timeout=4)


if __name__ == "__main__":
    unittest.main()
