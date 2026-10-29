"""Original dynamic definitions: real private TLS + NativeEngine HTTP fixture.

The evidence endpoint below is test-owned metadata built from actual persisted
claims. It is not an issuer or a replacement for coordinator cache tests.
"""
import copy
import json
import os
from pathlib import Path
import ssl
import threading
import unittest
from unittest.mock import patch

from ops import native_node_fixture
from ops import reference_worker_daemon as daemon
from processing import reference_protocol as p
from processing.reference_api import Response
from processing.reference_driver import ReferenceDriver
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_http import create_reference_http_server
from processing.reference_option_evidence import validate_option_evidence, OptionEvidenceHeld
from processing.viewer_service import NativeEngine
from tests import test_reference_driver as driver_fixture
from tests import test_reference_parent_guard as tls_fixture
from tests.test_reference_worker_service import private_config
TOKEN = driver_fixture.TOKEN


def envelope(claim, *, authority, request, definitions):
    return dict(schemaVersion=1, authorityId=authority, workerId='worker',
        workerInstanceId='instance', claimRequestId=request,
        **{key:claim[key] for key in ('jobId','generation','environment','nodeId','manifestSha256','pipelineDigest')},
        capabilityFingerprint=claim['submission']['capabilityFingerprint'],
        optionsFingerprint=claim['submission']['pipeline']['odm']['optionsFingerprint'],
        catalogSha256=p.digest({'syntheticCatalog': True}), selectedDefinitions=copy.deepcopy(definitions))


@unittest.skipUnless(os.name == 'posix', 'Linux private TLS and owned native fixture')
class OptionEvidenceClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): tls_fixture.ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        self.host = driver_fixture.ReferenceDriverTests(); self.host.setUp(); self.addCleanup(self.host.tearDown)
        h = self.host
        self.native_fixture = native_node_fixture.NativeNodeFixture(h.root/'evidence-native.sqlite',
            engine_version='pinned-engine-1', synthetic_enabled=True).start()
        self.addCleanup(self.native_fixture.close)
        native = NativeEngine(self.native_fixture.origin)
        h.worker = ReferenceEngineWorker(h.root/'evidence-worker.sqlite', ledger=h.ledger,
            outbox=h.outbox, engine=native, engine_id='engine', node_id='cluster',
            authority_id=h.queue.authority_id, coordinator_origin=h.gate_origin,
            gate_generation=1, worker_id='worker', engine_version='pinned-engine-1')
        self.definitions = {item['name']:item for item in native.read('/options')}
        self.requests = []; self.response_transform = lambda value:value
        self.unavailable = False
        owner = self
        class API:
            def handle(self, method, path, headers, body=b'', **kwargs):
                owner.requests.append((method,path,dict(headers)))
                if path.endswith('/option-evidence'):
                    if owner.unavailable: return Response(503, {'error':'synthetic_offline'})
                    if headers.get('Authorization') != 'Bearer '+TOKEN:
                        return Response(401, {'error':'unauthorized'})
                    job = path.split('/')[-2]
                    token = h.api.worker._header(headers,'X-LTDS-Claim-Token')
                    if token is None or body: return Response(400, {'error':'invalid_request'})
                    identity = dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')
                    claim = h.api.worker._owned_job(job, identity, token)
                    with h.queue._db() as db:
                        request = db.execute('SELECT request_id FROM reference_claims WHERE job_id=?',(job,)).fetchone()[0]
                    selected = [owner.definitions[name] for name in sorted(claim['submission']['pipeline']['odm']['options'])]
                    return Response(200, owner.response_transform(envelope(claim,
                        authority=h.queue.authority_id, request=request, definitions=selected)))
                return h.api.handle(method,path,headers,body,**kwargs)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(self.cert,self.key)
        self.server = create_reference_http_server(API(),ssl_context=context)
        self.thread = threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.01}); self.thread.start()
        self.addCleanup(self.stop_server)
        self.origin = 'https://127.0.0.1:'+str(self.server.server_port)
        self.context = ssl.create_default_context(cafile=str(self.cert))
        h.driver = self.make_driver()

    def stop_server(self):
        self.server.shutdown(); self.thread.join(3); self.server.server_close()
        self.assertFalse(self.thread.is_alive())

    def make_driver(self, enabled=True):
        h = self.host
        return ReferenceDriver(h.worker,h.ledger,h.outbox,h.broker,self.origin,TOKEN,
            state_path=h.root/'option-driver.sqlite',ssl_context=self.context,
            output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,
            failure_outbox=h.failures,worker_instance_id='instance',require_option_evidence=enabled)

    def queued(self, options=None):
        h = self.host
        job = h.submit(contents=(b'abc',))
        if options is not None:
            # Mutate ONLY the test-owned not-yet-claimed metadata with consistent
            # immutable digests; production API/evidence creation is peer-tested.
            with h.queue._db() as db:
                row = h.store._row(db,job['jobId']); old = json.loads(row['input_json'])
                raw = {key:old[key] for key in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
                raw['pipeline']['odm']['options'] = options
                checked = p.validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint'],
                    allowed_odm_options=set(options))
                db.execute('UPDATE reference_jobs SET input_json=?,request_digest=?,pipeline_digest=? WHERE job_id=?',
                    (p.canonical_json(checked).decode(),checked['requestDigest'],checked['pipelineDigest'],job['jobId']))
                job = h.store._public(h.store._row(db,job['jobId']))
        h.advance(job,'execution_pending')
        return job

    def test_actual_tls_first_start_and_after_start_expiry_never_refresh_evidence(self):
        h = self.host; job = self.queued({'numeric-limit':2})
        h.advance(job,'executing')
        self.assertEqual(self.native_fixture.counts().get('initialize',0),1)
        evidence = [headers for _,path,headers in self.requests if path.endswith('/option-evidence')]
        self.assertEqual(len(evidence),1)
        self.assertIn('x-ltds-claim-token',{key.lower() for key in evidence[0]})
        self.assertNotIn('x-claim-token',{key.lower() for key in evidence[0]})
        for _,path,headers in self.requests:
            if path.endswith(('/claim','/next')):
                self.assertEqual(h.api.worker._header(headers,'X-LTDS-Option-Evidence'),'1')
        self.unavailable = True
        h.driver.clock = lambda: 10**10
        h.advance(job,'awaiting_acceptance')
        self.assertEqual(len([path for _,path,_ in self.requests if path.endswith('/option-evidence')]),1)
        self.assertEqual(self.native_fixture.counts().get('initialize',0),1)

    def test_actual_fresh_options_same_name_type_metadata_drift_holds_before_mutation(self):
        h = self.host; self.queued({'numeric-limit':2})
        changed = copy.deepcopy(native_node_fixture.OPTIONS)
        next(item for item in changed if item['name']=='numeric-limit')['help'] = 'Changed meaning.'
        with patch.object(native_node_fixture,'OPTIONS',changed):
            result = h.driver.step()
        self.assertEqual(result,{'state':'held','reason':'original_engine_option_evidence_unavailable'})
        self.assertEqual(self.native_fixture.counts().get('initialize',0),0)

    def test_empty_selected_options_still_need_original_private_record(self):
        h = self.host; job = self.queued({})
        self.response_transform = lambda value:dict(value,claimRequestId='wrong-original-request')
        self.assertEqual(h.driver.step()['state'],'held')
        self.assertEqual(self.native_fixture.counts().get('initialize',0),0)
        self.response_transform = lambda value:value
        h.advance(job,'executing')
        self.assertEqual(self.native_fixture.counts().get('initialize',0),1)

    def test_every_original_identity_field_and_bounded_definition_is_checked(self):
        h = self.host; job = self.queued({'numeric-limit':2})
        claim = h.store.get(job['jobId'],private=True)
        with h.driver._db() as db:
            request = db.execute('SELECT claim_request FROM driver_jobs WHERE job=?',(job['jobId'],)).fetchone()[0]
        value = envelope(claim,authority=h.queue.authority_id,request=request,
            definitions=[self.definitions['numeric-limit']])
        arguments = dict(claim=claim,claim_request_id=request,authority_id=h.queue.authority_id,
            worker_id='worker',worker_instance_id='instance',fresh_definitions=self.definitions)
        self.assertTrue(validate_option_evidence(value,**arguments))
        for name in ('authorityId','jobId','generation','environment','nodeId','workerId',
                'workerInstanceId','claimRequestId','manifestSha256','pipelineDigest',
                'capabilityFingerprint','optionsFingerprint'):
            with self.subTest(name=name):
                wrong = dict(value); wrong[name] = 2 if name=='generation' else 'wrong-original-value'
                with self.assertRaises(OptionEvidenceHeld):validate_option_evidence(wrong,**arguments)
        for selected in ([dict(value['selectedDefinitions'][0],help='x'*262145)],
                value['selectedDefinitions']*2,[dict(value['selectedDefinitions'][0],type='bool')]):
            with self.assertRaises(OptionEvidenceHeld):
                validate_option_evidence(dict(value,selectedDefinitions=selected),**arguments)

    def test_closed_original_claim_bindings_missing_definition_and_empty_options(self):
        h = self.host; self.queued({'numeric-limit':2})
        variants = [lambda v:dict(v,extra=True), lambda v:dict(v,generation=True),
            lambda v:dict(v,authorityId='00000000-0000-4000-8000-000000000000'),
            lambda v:dict(v,claimRequestId='another-original-request'),lambda v:dict(v,selectedDefinitions=[]),
            lambda v:dict(v,selectedDefinitions=[dict(v['selectedDefinitions'][0],domain={'max':9})]),
            lambda v:dict(v,capabilityFingerprint=p.digest({'differentCapability':True})),
            lambda v:dict(v,optionsFingerprint=p.digest({'differentOptions':True}))]
        for index, variant in enumerate(variants):
            with self.subTest(index=index):
                self.response_transform = variant
                self.assertEqual(h.driver.step()['state'],'held')
                self.assertEqual(self.native_fixture.counts().get('initialize',0),0)
        self.response_transform = lambda value:value
        h.advance(h.store.list()[0],'executing')

    def test_default_off_skips_endpoint_and_runtime_or_cold_profile_drift_refused(self):
        h = self.host
        with self.assertRaises(ValueError): self.make_driver(False)
        h.driver.require_option_evidence = False
        self.assertEqual(h.driver.step(),{'state':'held','reason':'original_option_evidence_profile_changed'})
        h.driver.require_option_evidence = True
        # A separate owned journal proves the actual old/default request path.
        off = ReferenceDriver(h.worker,h.ledger,h.outbox,h.broker,self.origin,TOKEN,
            state_path=h.root/'default-option-driver.sqlite',ssl_context=self.context,
            output_budget=1024*1024,transfer_ttl=240,safety_floor=1024,
            failure_outbox=h.failures,worker_instance_id='instance')
        self.assertEqual(off._request('GET','/next')[0],200)
        self.assertNotIn('x-ltds-option-evidence',{key.lower() for key in self.requests[-1][2]})
        self.assertFalse(any(path.endswith('/option-evidence') for _,path,_ in self.requests))

    def test_factory_closed_profile_is_real_driver_binding(self):
        h = self.host
        config = private_config(h.root,self.cert,self.key,h.worker.engine,h.queue.authority_id,
            gate='engine',gate_origin=h.gate_origin)
        config['optionEvidence'] = dict(schemaVersion=1)
        path = h.root/'factory.json'; path.write_text(json.dumps(config)); path.chmod(0o600)
        loaded = daemon.load_config(str(path))
        driver, server = daemon.build_worker(loaded)
        try:
            self.assertTrue(driver.require_option_evidence)
            with driver._db() as db:
                binding = json.loads(db.execute('SELECT binding FROM driver_binding').fetchone()[0])
            self.assertEqual(binding['optionEvidence'],{'schemaVersion':1})
        finally: server.server_close()
        for value in (None,True,{'schemaVersion':True},{'schemaVersion':2},{'schemaVersion':1,'enabled':True}):
            config['optionEvidence'] = value
            path.write_text(json.dumps(config))
            with self.assertRaises(ValueError): daemon.load_config(str(path))
