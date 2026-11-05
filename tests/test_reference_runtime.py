import copy
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
from processing.config import Config, ProcessingNode, from_environment
from processing import reference_protocol as protocol
from processing.prototype import Queue, REFERENCE_PROFILE, VIEWER_PROFILE
from processing.reference_runtime import (
    ReferenceRuntime, ReferenceSettings, RuntimeConfigurationError, discovered_policy,
    load_reference_settings, validate_discovered_option_values,
)
from processing.reference_recovery import RecoveryNodeBinding
from tests.viewer_receipt_signer import SyntheticViewerSigner


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
        self.value = dict(schemaVersion=2, listener=dict(host="127.0.0.1", port=19443, **paths),
            viewerBearers={"staging": "reference-staging-" + "c"*32},
            workers={node: dict(bearer="reference-" + node + "-" + "d"*32,
                workerId="worker-" + node, instanceId="instance-" + node,
                resultOrigin="https://" + node + ".example",
                resultBearer="read-" + node + "-" + "e"*32) for node in ("cluster", "main")},
            streamBudget=dict(maxConcurrent=1, maxObjectBytes=100*1024**3))
        self.path = self.root / "reference-private.json"
        self.value = self.recovery_config()
        self.write_config()
        common_options = [
            dict(name="future-quality", type="int", value=3, domain="positive int", help="Quality level"),
            dict(name="future-toggle", type="bool", value=False),
            dict(name="input-file", type="string", value=""),
            dict(name="remote-url", type="string", value=""),
            dict(name="password", type="int", value=3),
            dict(name="safe-enum", type="enum", value="high", domain=["low", "high"]),
            dict(name="free-text", type="string", value="default text", domain="Name of project", help="A bounded note"),
            dict(name="safe-string-choice", type="string", value="first", domain='["first", "second"]')]
        self.snapshot = dict(ready=True, commonOptions=common_options, nodes=[
            dict(role=role, available=True, capabilitiesSha256="a"*64,
                     engine="odm", engineVersion="synthetic-1",
                     options=[dict(item) for item in common_options])
                for role in ("cluster", "main")])

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

    def runtime(self, queue, *, config=None, discover_capacities=None, **kwargs):
        config = config or self.config
        # Bind the synthetic multi-node runtime to this queue's persisted
        # authority, as an operator-provisioned runtime config must.
        schema_version = self.value['schemaVersion']
        self.value = self.recovery_config(queue.authority_id, config=config)
        if schema_version in (3, 4):self.value['schemaVersion'] = schema_version
        self.write_config()
        capacities = discover_capacities or (lambda: {node: dict(ready=True, maxImages=None,
            maxInputBytes=None, taskQueueCount=0) for node in config.prototype_node_ids})
        first, second = self.tls_patches()
        with first, second:
            if os.name == "nt":
                # Recovery execution is POSIX-only; Windows tests still cover
                # the validated daemon binding and scheduler path.
                with patch("processing.reference_recovery.ClaimRecoveryService", return_value=object()):
                    return ReferenceRuntime(queue, config, load_reference_settings(str(self.path), config),
                        discover=lambda: self.snapshot,
                        discover_capacities=capacities, **kwargs)
            return ReferenceRuntime(queue, config, load_reference_settings(str(self.path), config),
                discover=lambda: self.snapshot,
                discover_capacities=capacities, **kwargs)

    def make_configured_handler(self, config):
        if os.name == "nt":
            with patch("processing.reference_recovery.ClaimRecoveryService", return_value=object()):
                return make_handler(config)
        return make_handler(config)

    def reference_submission(self, images, option=None):
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              "contracts/reference-v1.contract.json").read_text())["submission"]
        raw = copy.deepcopy(fixture)
        raw["submissionId"] = str(uuid.uuid4())
        original = raw["manifest"]["objects"][0]
        raw["manifest"]["objects"] = [dict(original, id=f"image-{index + 1}",
            name=f"image-{index + 1:04}.jpg") for index in range(images)]
        raw["manifest"].update(objectCount=images, totalBytes=images * original["byteSize"],
            inputCounts={"images": images, "lidarObjects": 0, "points": 0})
        raw["manifest"]["sha256"] = protocol.manifest_sha256(raw["manifest"])
        allowed = set()
        if option is not None:
            name, value = option
            raw["pipeline"]["odm"]["options"] = {name: value}
            allowed.add(name)
        return protocol.validate_submission(raw, environment="staging",
            capability_fingerprint=raw["capabilityFingerprint"], allowed_odm_options=allowed)

    def test_disabled_default_preserves_native_queue_without_reference_profile(self):
        handler = make_handler(self.config)
        self.addCleanup(handler.prototype_queue.close)
        self.addCleanup(handler.viewer_service.close)
        self.assertIsNone(handler.reference_listener)
        self.assertNotIn(REFERENCE_PROFILE, handler.prototype_queue.allowed_profiles)
        self.assertIn(VIEWER_PROFILE, handler.prototype_queue.allowed_profiles)

    def recovery_config(self,authority='11111111-1111-4111-8111-111111111111',config=None):
        config=config or self.config
        value=copy.deepcopy(self.value);value['schemaVersion']=2
        value['recoveryBindings']={node:dict(engineId='physical-'+node,authorityId=authority,
            coordinatorOrigin='http://127.0.0.1:18080',gateGeneration=1,outputBudget=1000)
            for node in config.prototype_node_ids}
        return value

    def test_schema2_loads_closed_frozen_bindings_and_legacy_settings_default_is_none(self):
        legacy=copy.deepcopy(self.value);legacy['schemaVersion']=1;legacy.pop('recoveryBindings');self.value=legacy;self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()
        self.value=self.recovery_config();self.write_config();loaded=self.settings()
        self.assertEqual(set(loaded.recovery_bindings),{'cluster','main'})
        self.assertTrue(all(type(value) is RecoveryNodeBinding for value in loaded.recovery_bindings.values()))
        self.assertEqual(loaded.recovery_bindings['main'].engine_id,'physical-main')
        self.value['recoveryBindings']['main']['outputBudget']=2
        self.assertEqual(loaded.recovery_bindings['main'].output_budget,1000)

    def test_schema3_pins_public_viewer_receipt_keys_and_rejects_malformed_pin(self):
        signer = SyntheticViewerSigner()
        self.value = self.recovery_config()
        self.value.update(schemaVersion=3, viewerReceiptKeys=signer.trusted_keys)
        self.write_config()
        loaded = self.settings()
        self.assertEqual(loaded.viewer_receipt_keys, signer.trusted_keys)
        self.value.pop('recoveryBindings')
        self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()
        self.value = self.recovery_config()
        self.value.update(schemaVersion=3, viewerReceiptKeys=signer.trusted_keys)
        self.value['viewerReceiptKeys'][signer.key_id]['publicKeyPem'] = 'not-a-public-key'
        self.write_config()
        with self.assertRaises(RuntimeConfigurationError):
            self.settings()

    def test_schema4_loads_private_environment_scoped_viewer_transfer_credentials(self):
        signer = SyntheticViewerSigner()
        secret_path = self.root / 'viewer-service-key.txt'
        secret_path.write_text('synthetic-viewer-service-key-material\n')
        if os.name != 'nt': secret_path.chmod(0o600)
        self.value = self.recovery_config()
        self.value.update(schemaVersion=4, viewerReceiptKeys=signer.trusted_keys,
            viewerAcceptance={'production': {'origin': 'https://viewer.example',
                'keyId': 'viewer-service-v1', 'secretFile': str(secret_path)}})
        self.write_config()
        loaded = self.settings()
        self.assertEqual(loaded.viewer_acceptance_targets, {'production': {
            'origin': 'https://viewer.example', 'keyId': 'viewer-service-v1',
            'secret': 'synthetic-viewer-service-key-material'}})
        self.assertEqual(loaded.viewer_receipt_keys, signer.trusted_keys)
        queue = Queue(str(self.root / 'state'), node_ids=('cluster', 'main'), reference_enabled=True)
        queue.allowed_profiles.add('reference-pipeline-v1')
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        self.assertIsNotNone(runtime._viewer_transfer_worker)
        self.assertIsNone(runtime._viewer_transfer_thread)

    def test_schema4_rejects_untrusted_or_incomplete_viewer_targets(self):
        signer = SyntheticViewerSigner()
        secret_path = self.root / 'viewer-service-key.txt'
        secret_path.write_text('synthetic-viewer-service-key-material')
        if os.name != 'nt': secret_path.chmod(0o600)
        original = self.recovery_config()
        original.update(schemaVersion=4, viewerReceiptKeys=signer.trusted_keys,
            viewerAcceptance={'production': {'origin': 'https://viewer.example',
                'keyId': 'viewer-service-v1', 'secretFile': str(secret_path)}})
        bad_values = []
        for mutation in (
            lambda value: value['viewerAcceptance']['production'].update(origin='http://viewer.example'),
            lambda value: value['viewerAcceptance']['production'].update(extra='no'),
            lambda value: value['viewerAcceptance']['production'].update(keyId='bad id'),
            lambda value: value['viewerAcceptance']['production'].update(secretFile=str(self.root / 'missing.key')),
            lambda value: value.update(viewerReceiptKeys={}),
        ):
            value = copy.deepcopy(original); mutation(value); bad_values.append(value)
        short_secret = self.root / 'short-key.txt'; short_secret.write_text('short')
        if os.name != 'nt': short_secret.chmod(0o600)
        value = copy.deepcopy(original); value['viewerAcceptance']['production']['secretFile'] = str(short_secret)
        bad_values.append(value)
        for value in bad_values:
            self.value = value; self.write_config()
            with self.subTest(value=value), self.assertRaises(RuntimeConfigurationError): self.settings()

    def source_intake_config(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        spool_root=self.root/'source-spool';spool_root.mkdir(mode=0o700)
        marker=spool_root/'.ltds-source-spool-id';marker.write_text('a'*64,encoding='ascii')
        secret=self.root/'viewer-source-service-key.txt';secret.write_text('synthetic-viewer-service-key-material')
        if os.name!='nt':
            marker.chmod(0o600);secret.chmod(0o600)
        source_key=Ed25519PrivateKey.generate().public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
        self.value=self.recovery_config()
        self.value.update(schemaVersion=6,viewerReceiptKeys=SyntheticViewerSigner().trusted_keys,
            viewerAcceptance={'staging':{'origin':'https://viewer.example','keyId':'viewer-service-v1','secretFile':str(secret)}},
            sourceSpool={'root':str(spool_root),'mountPath':str(spool_root),'mountId':'a'*64,
                'markerName':'.ltds-source-spool-id','safetyFloorBytes':0,'maxObjectBytes':1024,'chunkBytes':16},
            sourceAttestationKeys=[{'environment':'staging','issuerId':'viewer-test','keyId':'source-v1',
                'principalId':'viewer-staging','publicKeyPem':source_key}])
        self.write_config()

    def test_schema6_loads_private_source_spool_and_pinned_attestation_signer(self):
        self.source_intake_config()
        with patch('processing.source_spool.os.path.ismount',return_value=True):
            loaded=self.settings()
        self.assertIsNotNone(loaded.source_spool)
        self.assertEqual(loaded.source_spool.mount_id,'a'*64)
        self.assertEqual(loaded.source_attestation_keys.records.keys(),{('staging','viewer-test','source-v1')})
        self.value['sourceAttestationKeys'][0]['publicKeyPem']='not-a-key';self.write_config()
        with patch('processing.source_spool.os.path.ismount',return_value=True):
            with self.assertRaises(RuntimeConfigurationError):self.settings()

    def test_schema6_pull_only_starts_no_legacy_sender_and_keeps_receipt_pins(self):
        self.source_intake_config()
        self.value['viewerAcceptance'] = {}
        queue = Queue(self.config.prototype_state_dir, node_ids=('cluster','main'), reference_enabled=True)
        self.addCleanup(queue.close)
        for binding in self.value['recoveryBindings'].values():
            binding['authorityId'] = queue.authority_id
        self.write_config()
        with patch('processing.source_spool.os.path.ismount',return_value=True):
            loaded = self.settings()
        self.assertIsNone(loaded.viewer_acceptance_targets)
        self.assertTrue(loaded.viewer_receipt_keys)
        self.assertIsNotNone(loaded.source_attestation_keys)
        first, second = self.tls_patches()
        server = FakeServer()
        with first, second, patch('processing.reference_recovery.ClaimRecoveryService',return_value=object()), \
                patch('processing.source_spool.os.path.ismount',return_value=True):
            runtime = ReferenceRuntime(queue, replace(self.config,viewer_enabled=False), loaded,
                discover=lambda:self.snapshot, server_factory=lambda *args,**kwargs:server)
        try:
            self.assertIsNone(runtime._viewer_transfer_worker)
            self.assertTrue(runtime.control.viewer.viewer_receipt_keys)
            runtime.start()
            self.assertIsNone(runtime._viewer_transfer_thread)
        finally:
            runtime.close()
        self.assertEqual(server.events[-2:],['shutdown','closed'])
        self.value['viewerReceiptKeys'] = {}
        self.write_config()
        with patch('processing.source_spool.os.path.ismount',return_value=True):
            with self.assertRaises(RuntimeConfigurationError):self.settings()

    def test_empty_push_targets_stay_invalid_in_legacy_runtime_schemas(self):
        self.source_intake_config()
        self.value['viewerAcceptance'] = {}
        self.value.pop('sourceAttestationKeys')
        for version in (4,5):
            value = copy.deepcopy(self.value)
            value['schemaVersion'] = version
            if version == 4:value.pop('sourceSpool')
            self.path.write_text(json.dumps(value))
            with self.subTest(version=version),self.assertRaises(RuntimeConfigurationError):
                self.settings()

    def test_schema2_rejects_duplicate_engine_ids_and_accepts_unique_daemon_bindings(self):
        self.value=self.recovery_config();self.write_config()
        self.assertEqual(set(self.settings().recovery_bindings),{'cluster','main'})
        self.value['recoveryBindings']['main']['engineId']='physical-cluster';self.write_config()
        with self.assertRaises(RuntimeConfigurationError):self.settings()

    def test_schema2_configuration_rejects_unknown_missing_coverage_versions_and_types(self):
        original=self.recovery_config()
        changes=[lambda v:v.update(schemaVersion=True),lambda v:v.update(schemaVersion=2.0),lambda v:v.update(schemaVersion=5),
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

    def test_multinode_reference_runtime_cannot_expose_legacy_viewer_mutations(self):
        self.value = self.recovery_config()
        self.write_config()
        config = replace(self.config, reference_config_path=str(self.path), viewer_enabled=True)
        with patch('processing.app.Queue', side_effect=AssertionError('queue must not open')):
            with self.assertRaisesRegex(RuntimeConfigurationError, 'legacy direct Viewer mutations'):
                make_handler(config)
        self.assertFalse(Path(config.prototype_state_dir).exists())

    def test_multi_node_runtime_refuses_legacy_unbound_settings(self):
        self.value=copy.deepcopy(self.value);self.value['schemaVersion']=1;self.value.pop('recoveryBindings');self.write_config()
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue,self.config,self.settings())

    def test_worker_next_uses_live_per_node_size_capacity_and_rechecks_at_claim(self):
        config = replace(self.config, local_max_photos=1,
            prototype_node_ids=('cluster', 'middle', 'main'),
            treedar_node_ids=('main',), processing_nodes=(
                ProcessingNode('cluster', self.config.local_node_url, self.config.local_node_token,
                               self.config.local_max_input_bytes),
                ProcessingNode('middle', 'http://127.0.0.1:19003', 'middle-engine-token'),
                ProcessingNode('main', self.config.main_node_url, self.config.main_node_token)))
        middle_worker = dict(bearer='reference-middle-' + 'd'*32, workerId='worker-middle',
            instanceId='instance-middle', resultOrigin='https://middle.example',
            resultBearer='read-middle-' + 'e'*32)
        self.value['workers']['middle'] = middle_worker
        self.write_config()
        middle_options = dict(self.snapshot['nodes'][0], role='middle')
        self.snapshot = dict(self.snapshot,
            nodes=[self.snapshot['nodes'][0], middle_options, self.snapshot['nodes'][1]])
        queue = Queue(config.prototype_state_dir, node_ids=('cluster', 'middle', 'main'),
                      treedar_node_ids=('main',), reference_enabled=True)
        self.addCleanup(queue.close)
        capacity = {
            'cluster': dict(ready=True, maxImages=1, maxInputBytes=None, taskQueueCount=0),
            'middle': dict(ready=True, maxImages=4, maxInputBytes=None, taskQueueCount=0),
            'main': dict(ready=True, maxImages=None, maxInputBytes=None, taskQueueCount=0),
        }
        runtime = self.runtime(queue, config=config, discover_capacities=lambda: dict(capacity))
        large, _ = runtime.store.submit(self.reference_submission(3))
        small, _ = runtime.store.submit(self.reference_submission(1))
        cluster_token = self.value['workers']['cluster']['bearer']
        middle_token = self.value['workers']['middle']['bearer']
        main_token = self.value['workers']['main']['bearer']
        prefix = '/ltds/worker/v1'
        cluster = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + cluster_token}, protected_transport=True)
        middle = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + middle_token}, protected_transport=True)
        main = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + main_token}, protected_transport=True)
        self.assertEqual(cluster.status, 200)
        self.assertEqual(cluster.body['job']['jobId'], small['jobId'])
        self.assertEqual(middle.status, 200)
        self.assertEqual(middle.body['job']['jobId'], large['jobId'])
        self.assertEqual(main.status, 200)
        self.assertIsNone(main.body['job'])

        # A lower live maxImages value after /next invalidates the intermediate
        # node's advisory assignment. The atomic claim must recompute capacity.
        capacity['middle']['maxImages'] = 2
        middle_claim = runtime.api.handle('POST', prefix + '/claim',
            {'Authorization': 'Bearer ' + middle_token},
            protocol.canonical_json(dict(jobId=large['jobId'], claimRequestId='stale-middle-claim')),
            protected_transport=True)
        self.assertEqual(middle_claim.status, 409)
        main = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + main_token}, protected_transport=True)
        self.assertEqual(main.body['job']['jobId'], large['jobId'])

        # Removing cluster availability also invalidates its stale assignment;
        # the still-available middle node can accept the smaller queued job.
        capacity['cluster'] = dict(ready=False, maxImages=None, maxInputBytes=None, taskQueueCount=0)
        claim = runtime.api.handle('POST', prefix + '/claim',
            {'Authorization': 'Bearer ' + cluster_token},
            protocol.canonical_json(dict(jobId=small['jobId'], claimRequestId='stale-cluster-claim')),
            protected_transport=True)
        self.assertEqual(claim.status, 409)
        self.assertIsNone(runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + cluster_token}, protected_transport=True).body['job'])
        self.assertEqual(runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + middle_token}, protected_transport=True).body['job']['jobId'],
            small['jobId'])

    def test_partial_engine_outage_still_admits_and_queues_work_for_healthy_node(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'),
                      treedar_node_ids=('main',), reference_enabled=True)
        self.addCleanup(queue.close)
        cluster_options, main_options = self.snapshot['nodes']
        self.snapshot = dict(self.snapshot, ready=False, reason='node_unavailable',
            commonOptions=[], availableOptions=[dict(item) for item in main_options['options']],
            availableNodeIds=['main'], nodes=[
                dict(cluster_options, available=False, options=[]), main_options])
        capacities = {
            'cluster': dict(ready=False, maxImages=None, maxInputBytes=None, taskQueueCount=0),
            'main': dict(ready=True, maxImages=None, maxInputBytes=None, taskQueueCount=0),
        }
        runtime = self.runtime(queue, discover_capacities=lambda: dict(capacities))
        viewer_token = self.value['viewerBearers']['staging']
        options = runtime.api.handle('GET', protocol.PREFIX + '/options',
            {'Authorization': 'Bearer ' + viewer_token}, protected_transport=True)
        self.assertEqual(options.status, 200)
        candidate = self.reference_submission(1, ('future-toggle', True))
        payload = {key: candidate[key] for key in
                   ('schemaVersion', 'submissionId', 'manifest', 'pipeline')}
        payload['capabilityFingerprint'] = options.body['capabilityFingerprint']
        payload['pipeline']['odm']['optionsFingerprint'] = options.body['optionsFingerprint']
        admitted = runtime.api.handle('POST', protocol.PREFIX + '/jobs',
            {'Authorization': 'Bearer ' + viewer_token}, protocol.canonical_json(payload),
            protected_transport=True)
        self.assertEqual(admitted.status, 201, admitted.body)
        self.assertEqual(admitted.body['status'], 'queued')
        worker_token = self.value['workers']['main']['bearer']
        next_job = runtime.api.handle('GET', '/ltds/worker/v1/next',
            {'Authorization': 'Bearer ' + worker_token}, protected_transport=True)
        self.assertEqual(next_job.status, 200)
        self.assertEqual(next_job.body['job']['jobId'], admitted.body['jobId'])

        self.snapshot = dict(self.snapshot, ready=False, commonOptions=[], availableOptions=[],
            availableNodeIds=[], nodes=[dict(node, available=False, options=[])
                for node in self.snapshot['nodes']])
        no_engines = runtime.api.handle('GET', protocol.PREFIX + '/options',
            {'Authorization': 'Bearer ' + viewer_token}, protected_transport=True)
        self.assertEqual(no_engines.status, 503)

    def test_worker_claim_rechecks_live_node_option_definitions_and_falls_through(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'),
                      treedar_node_ids=('main',), reference_enabled=True)
        self.addCleanup(queue.close)
        capacities = {node: dict(ready=True, maxImages=None, maxInputBytes=None,
            taskQueueCount=0) for node in ('cluster', 'main')}
        runtime = self.runtime(queue, discover_capacities=lambda: dict(capacities))
        job, _ = runtime.store.submit(self.reference_submission(1, ('future-toggle', True)))
        cluster_token = self.value['workers']['cluster']['bearer']
        main_token = self.value['workers']['main']['bearer']
        prefix = '/ltds/worker/v1'
        cluster = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + cluster_token}, protected_transport=True)
        self.assertEqual(cluster.body['job']['jobId'], job['jobId'])

        self.snapshot = dict(self.snapshot, nodes=[
            dict(node, options=[item for item in node['options'] if item['name'] != 'future-toggle'])
            if node['role'] == 'cluster' else node for node in self.snapshot['nodes']])
        rejected = runtime.api.handle('POST', prefix + '/claim',
            {'Authorization': 'Bearer ' + cluster_token},
            protocol.canonical_json(dict(jobId=job['jobId'], claimRequestId='changed-option-cluster')),
            protected_transport=True)
        self.assertEqual(rejected.status, 409)
        fallback = runtime.api.handle('GET', prefix + '/next',
            {'Authorization': 'Bearer ' + main_token}, protected_transport=True)
        self.assertEqual(fallback.body['job']['jobId'], job['jobId'])

    def test_malformed_capacity_probe_disables_only_that_node(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'),
                      treedar_node_ids=('main',), reference_enabled=True)
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        probes = {
            'cluster': dict(ready=False, maxImages='unavailable', taskQueueCount='unknown'),
            # A malformed count must not be interpreted as an idle node even
            # when the provider's top-level readiness flag says true.
            'main': dict(ready=True, maxImages=None, taskQueueCount=True),
        }
        with patch('processing.reference_runtime.probe_node',
                   side_effect=lambda node_id, *_args: probes[node_id]):
            capacities = runtime._discover_capacities()
        self.assertFalse(capacities['cluster']['ready'])
        self.assertIsNone(capacities['cluster']['maxImages'])
        self.assertEqual(capacities['cluster']['taskQueueCount'], 1)
        self.assertFalse(capacities['main']['ready'])
        self.assertIsNone(capacities['main']['maxImages'])
        self.assertEqual(capacities['main']['taskQueueCount'], 1)
        # Both records remain schema-valid, so the broken node cannot make a
        # healthy fleet refresh fail as a whole.
        runtime.store.update_fleet_snapshot(capacities, self.snapshot['nodes'])

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
        self.value=self.recovery_config(authority);self.write_config();config=replace(self.config,viewer_enabled=False,reference_config_path=str(self.path))
        first,second=self.tls_patches()
        with first,second:handler=self.make_configured_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        runtime=handler.reference_listener
        self.assertIsNone(handler.viewer_service)
        self.assertIsNone(runtime._server);self.assertIsNone(runtime._thread)
        self.assertIsNotNone(runtime.control.worker.recovery)
        with handler.prototype_queue._db() as db:
            startup=db.execute('SELECT * FROM reference_recovery_startup WHERE singleton=1').fetchone()
            self.assertEqual(startup['epoch_id'],handler.prototype_queue.startup_id)
            self.assertEqual(startup['authority_id'],authority)

    def test_direct_settings_cannot_bypass_recovery_binding_type_or_node_coverage(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        self.value=self.recovery_config(queue.authority_id);self.write_config()
        settings=self.settings()
        for bindings in (None,{}, {'cluster':{}},{'cluster':{},'main':{}},[]):
            with self.subTest(bindings=bindings),self.assertRaises(RuntimeConfigurationError):
                ReferenceRuntime(queue,self.config,replace(settings,recovery_bindings=bindings))

    def test_direct_settings_reject_duplicate_daemon_bindings_and_mismatched_queue_order(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        self.value=self.recovery_config(queue.authority_id);self.write_config()
        settings=self.settings()
        duplicate_bindings=dict(settings.recovery_bindings)
        duplicate_bindings['main']=replace(duplicate_bindings['main'],engine_id='physical-cluster')
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue,self.config,replace(settings,recovery_bindings=duplicate_bindings))
        self.value=self.recovery_config(queue.authority_id);self.write_config()
        settings=self.settings()
        wrong_config=replace(self.config,prototype_node_ids=('main','cluster'))
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue,wrong_config,settings)

    def test_direct_settings_reject_normalized_engine_origin_aliases_and_worker_coverage_gap(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
        self.value=self.recovery_config(queue.authority_id);self.write_config();settings=self.settings()
        aliases=replace(self.config,processing_nodes=(
            ProcessingNode('cluster','http://NodeODM.Example:80/'),
            ProcessingNode('main','http://nodeodm.example')))
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue,aliases,settings)
        missing_worker=replace(settings,workers={'cluster':settings.workers['cluster']})
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(queue,self.config,missing_worker)

    @unittest.skipUnless(os.name=='posix','Linux coordinator recovery authority')
    def test_configured_recovery_routes_resume_original_claim_and_same_epoch_reassembly_does_not_rehold(self):
        queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True);self.addCleanup(queue.close)
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
        resumed=call('POST','recover',protocol.canonical_json(request));self.assertEqual(resumed.status,200,resumed.body)
        self.assertEqual(queue.get(job['jobId'])['status'],'running');epoch=queue.startup_id
        second=self.runtime(queue);self.assertEqual(queue.startup_id,epoch);self.assertEqual(queue.get(job['jobId'])['status'],'running')
        receipt=second.api.handle('GET',prefix+'recovery-receipts/'+request['requestId'],headers,protected_transport=True)
        self.assertEqual(receipt.status,200);self.assertEqual(receipt.body,resumed.body)
        self.assertEqual(second.store.get(job['jobId'],private=True)['claimToken'],claim['claimToken'])

    def test_explicit_runtime_shares_exact_native_queue_authority_and_global_slot(self):
        seed=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True)
        authority=seed.authority_id;seed.close()
        self.value=self.recovery_config(authority);self.write_config()
        config = replace(self.config, reference_config_path=str(self.path), viewer_enabled=False)
        first, second = self.tls_patches()
        with first, second:
            handler = self.make_configured_handler(config)
        self.addCleanup(handler.prototype_queue.close)
        runtime = handler.reference_listener
        runtime.api.discover = lambda: self.snapshot
        self.assertIs(runtime.queue, handler.prototype_queue)
        self.assertIsNone(handler.viewer_service)
        self.assertEqual(runtime.queue.authority_id, handler.prototype_queue.authority_id)
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
        self.assertIsNone(handler.prototype_queue.claim({"workerId": "legacy", "profiles": [REFERENCE_PROFILE]}))
        runtime.close()

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
        self.assertTrue(all(option['type'] in ('bool','int','float','enum','string') for option in listed.body['options']))
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
        self.assertEqual(first[2], {"future-quality", "future-toggle", "safe-enum", "free-text", "safe-string-choice"})
        reordered = copy.deepcopy(self.snapshot)
        reordered["commonOptions"].reverse()
        reordered["nodes"].reverse()
        self.assertEqual(first, discovered_policy(reordered, self.config))
        changed = copy.deepcopy(self.snapshot)
        changed["nodes"][0]["engineVersion"] = "synthetic-2"
        self.assertNotEqual(first[0], discovered_policy(changed, self.config)[0])

    def test_actual_digit_prefixed_output_option_survives_discovery_and_admission(self):
        self.snapshot['commonOptions'].append(dict(name='3d-tiles', type='bool', value='false'))
        queue = Queue(self.config.prototype_state_dir, node_ids=('cluster', 'main'), reference_enabled=True)
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        headers = {'Authorization': 'Bearer ' + self.value['viewerBearers']['staging']}
        listed = runtime.api.handle('GET', '/ltds/v1/options', headers, protected_transport=True)
        self.assertEqual(listed.status, 200)
        self.assertIn({'name': '3d-tiles', 'type': 'bool', 'value': 'false'}, listed.body['options'])
        raw = json.loads((Path(__file__).resolve().parents[1] / 'contracts/reference-v1.contract.json').read_text())['submission']
        raw['capabilityFingerprint'] = listed.body['capabilityFingerprint']
        raw['pipeline']['odm']['optionsFingerprint'] = listed.body['optionsFingerprint']
        raw['pipeline']['odm']['options'] = {'3d-tiles': True}
        admitted = runtime.api.handle('POST', '/ltds/v1/jobs', headers, json.dumps(raw).encode(), protected_transport=True)
        self.assertEqual(admitted.status, 201, admitted.body)

    def test_enum_values_are_checked_against_fresh_engine_choices(self):
        _, _, allowed = discovered_policy(self.snapshot, self.config)
        payload = {"pipeline": {"odm": {"options": {"safe-enum": "high"}}}}
        validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"]["safe-enum"] = "unadvertised"
        with self.assertRaisesRegex(ValueError, "current engine definition"):
            validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"]["safe-enum"] = 7
        with self.assertRaisesRegex(ValueError, "current engine definition"):
            validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"] = []
        with self.assertRaisesRegex(ValueError, "current engine definition"):
            validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"] = {
            "future-quality": 1, "free-text": "bounded text", "safe-string-choice": "second"}
        validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"]["future-quality"] = 0
        with self.assertRaisesRegex(ValueError, "current engine definition"):
            validate_discovered_option_values(payload, self.snapshot, allowed)
        payload["pipeline"]["odm"]["options"]["future-quality"] = 1
        payload["pipeline"]["odm"]["options"]["free-text"] = "x" * 4001
        with self.assertRaisesRegex(ValueError, "current engine definition"):
            validate_discovered_option_values(payload, self.snapshot, allowed)

    def test_dynamic_job_admission_accepts_current_enum_and_rejects_unknown_choice(self):
        queue = Queue(self.config.prototype_state_dir, node_ids=("cluster", "main"), reference_enabled=True)
        self.addCleanup(queue.close)
        runtime = self.runtime(queue)
        headers = {"Authorization": "Bearer " + self.value["viewerBearers"]["staging"]}
        payload = json.loads((Path(__file__).resolve().parents[1] /
                              "contracts/reference-v1.contract.json").read_text())["submission"]
        payload["submissionId"] = str(uuid.uuid4())
        payload["capabilityFingerprint"] = discovered_policy(self.snapshot, self.config)[0]
        payload["pipeline"]["odm"]["optionsFingerprint"] = discovered_policy(self.snapshot, self.config)[1]
        payload["pipeline"]["odm"]["options"] = {
            "safe-enum": "high", "free-text": "synthetic note", "safe-string-choice": "second"}
        route = "/ltds/v1/jobs"
        accepted = runtime.api.handle("POST", route, headers, json.dumps(payload).encode(),
                                      protected_transport=True)
        self.assertEqual(accepted.status, 201)
        payload["submissionId"] = str(uuid.uuid4())
        payload["pipeline"]["odm"]["options"]["safe-enum"] = "unadvertised"
        rejected = runtime.api.handle("POST", route, headers, json.dumps(payload).encode(),
                                      protected_transport=True)
        self.assertEqual(rejected.status, 400)
        runtime.close()

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
        seed=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True)
        authority=seed.authority_id;seed.close()
        self.value=self.recovery_config(authority);self.write_config()
        config = replace(self.config, viewer_enabled=False, reference_config_path=str(self.path))
        first, second = self.tls_patches()
        with first, second:
            handler = self.make_configured_handler(config)
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
