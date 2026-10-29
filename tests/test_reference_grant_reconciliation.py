"""Actual ReferenceControlAPI/HTTP historical grant delivery, default off."""
from contextlib import contextmanager
import copy
import hashlib
import http.client
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from processing import reference_protocol as p
from processing.reference_api import ReferenceAPI
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_runtime import _DynamicAPI
from tests import test_reference_api as fixture


class GrantReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.f=fixture.ReferenceAPITests();self.f.setUp();self.addCleanup(self.f.tearDown)
        self.api=self.f.make_api(grant_reconciliation=True)
        if self._testMethodName=='test_canonical_grant_hash_includes_token_and_original_array_order':
            manifest=self.f.fixture['submission']['manifest']
            extra=copy.deepcopy(manifest['objects'][0]);extra.update(id='image-order-two',name='image-order-two.jpg')
            manifest['objects'].append(extra);manifest['objectCount']=2;manifest['inputCounts']['images']=2
            manifest['totalBytes']=sum(o['byteSize'] for o in manifest['objects']);manifest['sha256']=p.manifest_sha256(manifest)
            for value in (self.f.fixture['transferRequest'],self.f.fixture['transferGrant']):
                value['manifestSha256']=manifest['sha256'];value['byteBudget']=3*manifest['totalBytes']
            self.f.fixture['transferGrant']['objectIds']=[o['id'] for o in manifest['objects']]
        self.job=self.f.submit();self.claim=self.f.store.claim(self.job['jobId'],node_id='cluster',
            worker_id='worker',worker_instance_id='instance',claim_request_id='grant-delivery-test')
        self.reservation=copy.deepcopy(self.f.fixture['transferRequest'])
        # Intentionally expired NOW: source receipt is an historical fact, not
        # a permission to use the old bearer or a fabricated current clock.
        self.reservation['expiresAt']='1970-01-01T00:20:00.000Z'
        self.f.store.reserve(self.job['jobId'],self.reservation,node_id='cluster')
        self.grant=dict(copy.deepcopy(self.f.fixture['transferGrant']),**self.reservation,jobId=self.job['jobId'])
        self.expected=dict(schemaVersion=1,jobId=self.job['jobId'],generation=1,environment='staging',
            reservationId=self.grant['reservationId'],manifestSha256=self.grant['manifestSha256'],
            nonce=self.grant['nonce'],grantSha256=p.digest(self.grant))
        self.path='/jobs/'+self.job['jobId']+'/transfer-grants/reconcile'

    def publish(self):
        self.f.store.receive_transfer_grant(self.job['jobId'],self.grant,clock=lambda:100)

    def request(self,body=None,**kwargs):
        return self.f.request('POST',self.path,self.expected if body is None else body,api=self.api,**kwargs)

    def control(self,enabled=True):
        return ReferenceControlAPI(self.f.store,viewer_bearers={fixture.STAGING:'staging',fixture.PRODUCTION:'production'},
            worker_bearers={'synthetic-reconcile-worker-token':dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},
            capability_fingerprint=self.f.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.f.fixture['capabilities']['engines']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality','projection'},grant_reconciliation=enabled)

    def test_default_off_and_explicit_boolean_no_v1_capability_shape_change(self):
        self.publish()
        self.assertEqual(self.f.request('POST',self.path,self.expected).status,404)
        with self.assertRaises(ValueError):self.f.make_api(grant_reconciliation=1)
        self.assertEqual(self.f.request('GET','/capabilities').body,self.f.request('GET','/capabilities',api=self.api).body)

    def test_shared_public_synthetic_vector_exact_complete_grant_and_receipt_bytes(self):
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-grant-reconciliation-v1.contract.json').read_text())
        self.assertEqual(p.canonical_json(vector['privateGrant']).hex(),vector['canonicalPrivateGrantUtf8Hex'])
        self.assertEqual(p.digest(vector['privateGrant']),vector['grantSha256'])
        self.assertEqual(vector['request']['grantSha256'],vector['grantSha256'])
        self.assertEqual(p.canonical_json(vector['receipt']).hex(),vector['canonicalReceiptUtf8Hex'])
        self.assertEqual(vector['receipt'],dict(vector['request'],receiptType='transfer_grant_reconciliation',accepted=True,mayDownload=False))

    def test_exact_committed_receipt_after_expiry_phase_and_restart_is_stable_read_only(self):
        self.publish();self.f.store.update_phase(self.job['jobId'],'fetching');self.f.store.update_phase(self.job['jobId'],'processing')
        before=self.f.queue.path.read_bytes()
        def no_expiry_validation(*args,**kwargs):raise AssertionError('historical read must not require a live grant')
        with patch('processing.reference_transfer.validate_grant',side_effect=no_expiry_validation):
            first=self.request();second=self.request()
        self.assertEqual(first.status,200);self.assertEqual(p.canonical_json(first.body),p.canonical_json(second.body))
        self.assertEqual(first.body,dict(self.expected,receiptType='transfer_grant_reconciliation',accepted=True,mayDownload=False))
        self.assertNotIn(self.grant['token'],json.dumps(first.body));self.assertNotIn('token',first.body)
        self.assertEqual(before,self.f.queue.path.read_bytes())
        # A newly constructed actual API sees the same durable ACK, not cache.
        fresh=self.f.make_api(grant_reconciliation=True)
        self.assertEqual(self.f.request('POST',self.path,self.expected,api=fresh).body,first.body)

    def test_missing_or_different_digest_same_generic_conflict_no_creation(self):
        missing=self.request();self.assertEqual(missing.status,409)
        self.publish();bad=dict(self.expected,grantSha256='0'*64)
        mismatch=self.request(bad);self.assertEqual(mismatch.status,409);self.assertEqual(mismatch.body,missing.body)
        with self.f.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],1)

    def test_path_body_nonce_generation_environment_manifest_conflicts(self):
        self.publish()
        for key,value in [('jobId','other'),('generation',2),('environment','production'),
                          ('reservationId','other'),('manifestSha256','0'*64),('nonce','other')]:
            bad=dict(self.expected);bad[key]=value
            with self.subTest(key=key):self.assertEqual(self.request(bad).status,409)

    def test_auth_transport_and_foreign_environment_before_body_decode(self):
        self.publish()
        with patch.object(self.f.store,'reconcile_transfer_grant',side_effect=AssertionError('unauthorized store read')):
            self.assertEqual(self.request(token=fixture.PRODUCTION).status,404)
            self.assertEqual(self.request(token='wrong').status,401)
            self.assertEqual(self.request(protected=False).status,503)
            raw=self.api.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer '+fixture.PRODUCTION},b'invalid json',protected_transport=True)
            self.assertEqual(raw.status,404)

    def test_typed_closed_json_and_duplicate_keys_no_receipt(self):
        self.publish()
        for key,value in [('generation',True),('schemaVersion',True),('grantSha256','A'*64),('extra',1)]:
            bad=dict(self.expected);bad[key]=value
            with self.subTest(key=key):self.assertEqual(self.request(bad).status,400)
        raw=p.canonical_json(self.expected).decode().replace('"schemaVersion":1','"schemaVersion":1,"schemaVersion":1').encode()
        self.assertEqual(self.api.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer '+fixture.STAGING},raw,protected_transport=True).status,400)

    def test_canonical_grant_hash_includes_token_and_original_array_order(self):
        self.publish();changed=dict(self.grant,token='different-synthetic-token')
        self.assertNotEqual(p.digest(changed),self.expected['grantSha256'])
        self.assertEqual(self.request(dict(self.expected,grantSha256=p.digest(changed))).status,409)
        # Hash is the complete canonical private body, not public binding only.
        self.assertEqual(hashlib.sha256(p.canonical_json(self.grant)).hexdigest(),self.expected['grantSha256'])
        self.assertEqual(len(self.grant['objectIds']),2)
        reordered=dict(self.grant,objectIds=list(reversed(self.grant['objectIds'])))
        self.assertNotEqual(p.digest(reordered),self.expected['grantSha256'])
        self.assertEqual(self.request(dict(self.expected,grantSha256=p.digest(reordered))).status,409)

    def test_durable_grant_bytes_digest_mismatch_is_unknown_not_acceptance(self):
        self.publish()
        with self.f.queue._db() as db:
            db.execute('UPDATE reference_transfer_grants SET grant_json=? WHERE job_id=?',(json.dumps(self.grant,indent=2),self.job['jobId']))
        self.assertEqual(self.request().status,409)

    def replace_private_history(self,grant):
        raw=p.canonical_json(grant).decode();digest=p.digest(grant)
        with self.f.queue._db() as db:
            db.execute('UPDATE reference_transfer_grants SET grant_json=?,digest=? WHERE job_id=?',(raw,digest,self.job['jobId']))
        return dict(self.expected,grantSha256=digest)

    def test_rehashed_incomplete_canonical_history_never_confirms_delivery(self):
        self.publish()
        incomplete={key:self.grant[key] for key in ('jobId','generation','environment','reservationId','manifestSha256','nonce')}
        self.assertEqual(self.request(self.replace_private_history(incomplete)).status,409)

    def test_rehashed_full_history_requires_typed_object_reservation_closure(self):
        self.publish()
        for key,value in [('objectIds',['foreign']),('objectIds',[self.grant['objectIds'][0]]*2),
                          ('byteBudget',True),('byteBudget',p.MAX_SAFE_INTEGER),('generation',1.0),
                          ('expiresAt','2026-10-29T12:00:00.000Z'),('token',' whitespace'),('extra',1)]:
            changed=copy.deepcopy(self.grant);changed[key]=value
            with self.subTest(key=key):self.assertEqual(self.request(self.replace_private_history(changed)).status,409)

    def test_original_reservation_or_normalized_job_drift_cannot_be_hidden_by_digest(self):
        self.publish()
        changed=dict(self.reservation,nonce='changed-original')
        with self.f.queue._db() as db:db.execute('UPDATE reference_jobs SET transfer_json=? WHERE job_id=?',(p.canonical_json(changed).decode(),self.job['jobId']))
        self.assertEqual(self.request().status,409)
        with self.f.queue._db() as db:
            db.execute('UPDATE reference_jobs SET transfer_json=?,input_json=? WHERE job_id=?',
                (p.canonical_json(self.reservation).decode(),'{}',self.job['jobId']))
        self.assertEqual(self.request().status,409)

    def test_duplicate_private_history_and_exponent_wire_refuse_without_live_clock(self):
        self.publish()
        raw=p.canonical_json(self.grant).decode().replace('"generation":1','"generation":1,"generation":1',1)
        with self.f.queue._db() as db:db.execute('UPDATE reference_transfer_grants SET grant_json=?,digest=? WHERE job_id=?',
            (raw,hashlib.sha256(raw.encode()).hexdigest(),self.job['jobId']))
        self.assertEqual(self.request().status,409)
        request=p.canonical_json(self.expected).decode().replace('"generation":1','"generation":1e0',1).encode()
        response=self.api.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer '+fixture.STAGING},request,protected_transport=True)
        self.assertEqual(response.status,400)

    def test_real_api_scope_auth_never_materializes_private_or_input_result_json(self):
        self.publish()
        huge=dict(self.grant,oversizedPadding='x'*(p.MAX_BODY_BYTES+1))
        raw=p.canonical_json(huge).decode()
        with self.f.queue._db() as db:
            db.execute('UPDATE reference_transfer_grants SET grant_json=?,digest=? WHERE job_id=?',
                (raw,p.digest(huge),self.job['jobId']))
            db.execute('UPDATE reference_jobs SET input_json=?,result_json=? WHERE job_id=?',
                ('x'*(p.MAX_BODY_BYTES+16385),'not-json-secret-result',self.job['jobId']))
        original=self.f.queue._db;seen=[]
        @contextmanager
        def traced():
            with original() as db:
                db.set_trace_callback(seen.append)
                try:yield db
                finally:db.set_trace_callback(None)
        with patch.object(self.f.queue,'_db',traced), \
                patch.object(self.f.store,'get',side_effect=AssertionError('full job prefetch forbidden')), \
                patch('processing.reference_grant_history.decode_history',side_effect=AssertionError('private decode before bound')):
            self.assertEqual(self.request().status,409)
            self.assertEqual(self.request(token=fixture.PRODUCTION).status,404)
        self.assertTrue(any(sql.startswith('SELECT 1 FROM reference_jobs') for sql in seen))
        self.assertFalse(any('SELECT r.*' in sql or sql.startswith('SELECT grant_json') or sql.startswith('SELECT input_json') for sql in seen))

    def test_foreign_environment_scalar_scope_precedes_even_hostile_request_decode(self):
        self.publish()
        with self.f.queue._db() as db:
            db.execute('UPDATE reference_transfer_grants SET grant_json=? WHERE job_id=?',('not-json'*(1024*1024),self.job['jobId']))
        with patch.object(self.f.store,'get',side_effect=AssertionError('private job read')), \
                patch('processing.reference_api.protocol.decode_submission',side_effect=AssertionError('body decoded before auth')):
            reply=self.api.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer '+fixture.PRODUCTION},b'not-json',protected_transport=True)
        self.assertEqual(reply.status,404)

    def test_actual_control_and_dynamic_wrapper_do_not_discover_or_use_worker_credential(self):
        self.publish();control=self.control()
        dynamic=_DynamicAPI(control,object(),SimpleNamespace(),lambda:(_ for _ in ()).throw(AssertionError('discovery on historical read')))
        reply=dynamic.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer '+fixture.STAGING},p.canonical_json(self.expected),protected_transport=True)
        self.assertEqual(reply.status,200)
        reply=control.handle('POST','/ltds/v1'+self.path,{'Authorization':'Bearer synthetic-reconcile-worker-token'},p.canonical_json(self.expected),protected_transport=True)
        self.assertEqual(reply.status,401)

    def test_actual_existing_loopback_listener_roundtrip_without_mock_transport(self):
        self.publish();server=create_reference_http_server(self.control())
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02});thread.start()
        connection=http.client.HTTPConnection(*server.server_address,timeout=5)
        try:
            connection.request('POST','/ltds/v1'+self.path,p.canonical_json(self.expected),{'Authorization':'Bearer '+fixture.STAGING,'Content-Type':'application/json'})
            response=connection.getresponse();self.assertEqual(response.status,200)
            self.assertFalse(json.loads(response.read())['mayDownload'])
        finally:connection.close();server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())


if __name__=='__main__':unittest.main()
