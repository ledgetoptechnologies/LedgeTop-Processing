"""Actual initial-only Queue/Store/Control path, synthetic data and issuer key.

No real source custody, key lifecycle, worker science, deployment or NAS proof.
"""
import base64
from contextlib import contextmanager,closing
import copy
import hashlib
import hmac
import http.client
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from processing.prototype import Queue
from processing.maintenance import Conflict
from processing.reference_store import ReferenceStore
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_initial_grant import InitialGrantPolicy
from processing.reference_grant_control import expected_seed,seed_bytes
from processing import reference_protocol as p
from tests import test_reference_api as tokens
from tests.test_reference_grant_renewal import stamp


def profile_payload():
    return dict(schemaVersion=1,issuerRealm='synthetic-staging',keyId='synthetic-key',maxEpochs=0,maxLifetimeMs=300000,
        brokerCapabilities=dict(schemaVersion=2,profile='ltds-renewable-grants-v1',wholeObject200=True,
            maxResponseBytes=16*1024**2,maxConcurrentBytes=64*1024**2,maxRequestsPerEpoch=10010,
            maxEpochs=0,maxEpochTtlMs=300000,maxLifetimeMs=300000,maxTotalRequests=10010,maxTotalBytes=2**53-1))


class InitialGrantRouteTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        self.policy=InitialGrantPolicy.from_payload(profile_payload())
        self.queue=Queue(self.root,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=self.policy)
        self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue,initial_grant_policy=self.policy)
        self.api=self.control(self.store)
        self.contexts={}

    def control(self,store):
        return ReferenceControlAPI(store,viewer_bearers={tokens.STAGING:'staging',tokens.PRODUCTION:'production'},
            worker_bearers={'synthetic-cluster-worker-token':dict(node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster')},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'],
            allowed_odm_options={'pc-quality','projection'},grant_reconciliation=True)

    def request(self,method,path,body=None,token=tokens.STAGING,**kwargs):
        return self.api.handle(method,'/ltds/v1'+path,{'Authorization':'Bearer '+token,**kwargs.get('headers',{})},
            b'' if body is None else p.canonical_json(body),protected_transport=kwargs.get('protected',True))

    def prepare(self):
        reply=self.request('POST','/jobs',self.fixture['submission']);self.assertEqual(reply.status,201,reply.body)
        job=reply.body;claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster',claim_request_id='initial-'+str(uuid.uuid4()))
        reservation=copy.deepcopy(self.fixture['transferRequest']);reservation['expiresAt']=stamp(round((time.time()+100)*1000))
        self.store.reserve(job['jobId'],reservation,node_id='cluster')
        context=self.request('GET','/jobs/'+job['jobId']+'/initial-grant-context')
        self.assertEqual(context.status,200,context.body);self.contexts[job['jobId']]=context.body
        private=self.store.get(job['jobId'],private=True)
        request=dict(schemaVersion=2,profile='ltds-renewable-grants-v1',requestId=str(uuid.uuid4()),jobId=job['jobId'],generation=1,
            environment='staging',reservationId=reservation['reservationId'],manifestSha256=job['manifestSha256'],nodeId='cluster',
            workerId='worker-cluster',epoch=0,predecessorSha256=None,expiresAt=reservation['expiresAt'],nonce=reservation['nonce'])
        settings=self.policy.payload();seed=expected_seed(request,dict(job=private,
            policy=self.policy.transfer_policy(private['submission']['manifest'],byte_budget=reservation['byteBudget']),
            capabilities=settings['brokerCapabilities'],keyId=settings['keyId'],issuerRealm=settings['issuerRealm']))
        bearer=base64.urlsafe_b64encode(hmac.digest(bytes(range(32)),b'LTDS reference transfer bearer v1\0'+seed_bytes(seed),'sha256')).rstrip(b'=').decode()
        grant=dict(copy.deepcopy(self.fixture['transferGrant']),**reservation,jobId=job['jobId'],token=bearer)
        envelope=dict(schemaVersion=2,profile='ltds-renewable-grants-v1',request=request,issuanceSeed=seed,grant=grant)
        return job,claim,envelope

    def post(self,job,envelope,**kwargs):
        kwargs.setdefault('headers',{'X-LTDS-Initial-Context-SHA256':self.contexts[job['jobId']]['contextSha256']})
        return self.request('POST','/jobs/'+job['jobId']+'/initial-grant',envelope,**kwargs)

    def test_actual_control_receives_epoch_zero_worker_reads_unchanged_v1_grant(self):
        job,claim,envelope=self.prepare();ack=self.post(job,envelope)
        self.assertEqual(ack.status,200,ack.body);self.assertFalse(ack.body['mayDownload'])
        self.assertEqual(ack.body['envelopeSha256'],p.digest(envelope))
        self.assertNotIn(envelope['grant']['token'],json.dumps(ack.body))
        reply=self.api.handle('GET','/ltds/worker/v1/jobs/'+job['jobId']+'/transfer-grant',
            {'Authorization':'Bearer synthetic-cluster-worker-token','X-Claim-Token':claim['claimToken']},protected_transport=True)
        self.assertEqual(reply.status,200,reply.body);self.assertEqual(reply.body,envelope['grant'])

    def test_exact_replay_after_phase_expiry_and_compatible_restart_is_read_only(self):
        job,_,envelope=self.prepare();first=self.post(job,envelope);self.assertEqual(first.status,200)
        self.store.update_phase(job['jobId'],'fetching');self.store.update_phase(job['jobId'],'processing')
        before=self.queue.path.read_bytes()
        with patch('processing.reference_store.time.time',side_effect=AssertionError('historical replay observed current clock')):
            self.assertEqual(self.post(job,envelope).body,first.body)
        self.assertEqual(before,self.queue.path.read_bytes())
        self.queue.close()
        self.queue=Queue(self.root,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=self.policy);self.addCleanup(self.queue.close)
        self.store=ReferenceStore(self.queue,initial_grant_policy=self.policy);self.api=self.control(self.store)
        self.assertEqual(self.post(job,envelope).body,first.body)

    def test_latched_policy_omission_or_change_holds_before_queue_business_writes(self):
        self.queue.close();before=(self.root/'jobs.sqlite3').read_bytes()
        with self.assertRaises(ValueError):Queue(self.root,node_ids=('cluster','main'),reference_enabled=True)
        self.assertEqual(before,(self.root/'jobs.sqlite3').read_bytes())
        changed=profile_payload();changed['keyId']='different-key'
        with self.assertRaises(ValueError):Queue(self.root,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=InitialGrantPolicy.from_payload(changed))
        self.assertEqual(before,(self.root/'jobs.sqlite3').read_bytes())

    def test_uppercase_marker_namespace_omission_holds_before_queue_writes(self):
        for table in ('REFERENCE_INITIAL_POLICY','Reference_Initial_Jobs'):
            directory=self.root/table;directory.mkdir()
            path=directory/'jobs.sqlite3'
            with closing(sqlite3.connect(path)) as db:
                db.execute('CREATE TABLE '+table+' (record TEXT)');db.commit()
            before=path.read_bytes()
            with self.assertRaises(ValueError):Queue(directory,node_ids=('cluster','main'),reference_enabled=True)
            self.assertEqual(before,path.read_bytes())
            with self.assertRaises(ValueError):Queue(directory,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=self.policy)
            self.assertEqual(before,path.read_bytes())

    def test_public_context_is_stable_read_only_and_not_a_credential_verifier(self):
        job,_,_=self.prepare();before=self.queue.path.read_bytes()
        reply=self.request('GET','/jobs/'+job['jobId']+'/initial-grant-context')
        self.assertEqual(reply.status,200);self.assertEqual(reply.body,self.contexts[job['jobId']])
        self.assertEqual(len(reply.body),18)
        public=copy.deepcopy(reply.body);digest=public.pop('contextSha256')
        self.assertEqual(p.digest(public),digest)
        self.assertFalse({'viewerPrincipalBinding','parentClaimSha256','claimToken','claimHash'}&public.keys())
        self.assertNotIn(tokens.STAGING,json.dumps(reply.body));self.assertEqual(before,self.queue.path.read_bytes())

    def test_principal_rotation_context_change_and_missing_header_fail_closed(self):
        job,_,envelope=self.prepare()
        rotated=ReferenceControlAPI(self.store,viewer_bearers={'different-staging-bearer':'staging'},worker_bearers={'synthetic-worker':dict(node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster')},
            capability_fingerprint=self.fixture['capabilities']['capabilityFingerprint'],
            options_fingerprint=self.fixture['capabilities']['engines']['odm']['optionsFingerprint'],allowed_odm_options={'pc-quality','projection'})
        headers={'Authorization':'Bearer different-staging-bearer','X-LTDS-Initial-Context-SHA256':self.contexts[job['jobId']]['contextSha256']}
        for method,suffix,body in [('GET','initial-grant-context',b''),('POST','initial-grant',p.canonical_json(envelope))]:
            self.assertEqual(rotated.handle(method,'/ltds/v1/jobs/'+job['jobId']+'/'+suffix,headers,body,protected_transport=True).status,409)
        self.assertNotEqual(self.post(job,envelope,headers={}).status,200)
        self.assertEqual(self.post(job,envelope,headers={'X-LTDS-Initial-Context-SHA256':'0'*64}).status,409)
        with self.queue._db() as db:db.execute('UPDATE reference_claims SET instance_id=? WHERE job_id=?',('reacquired-same-worker',job['jobId']))
        self.assertEqual(self.request('GET','/jobs/'+job['jobId']+'/initial-grant-context').status,409)
        self.assertEqual(self.post(job,envelope).status,409)

    def test_context_after_delivery_is_unknown_but_exact_historical_post_remains_stable(self):
        job,_,envelope=self.prepare();first=self.post(job,envelope);self.assertEqual(first.status,200)
        self.assertEqual(self.request('GET','/jobs/'+job['jobId']+'/initial-grant-context').status,409)
        self.assertEqual(self.post(job,envelope).body,first.body)

    def test_context_expiry_and_duplicate_digest_header_hold_without_writes(self):
        job,_,envelope=self.prepare();before=self.queue.path.read_bytes()
        expiry=__import__('datetime').datetime.fromisoformat(envelope['grant']['expiresAt'].replace('Z','+00:00')).timestamp()
        with patch('processing.reference_store.time.time',return_value=expiry+1):
            self.assertEqual(self.request('GET','/jobs/'+job['jobId']+'/initial-grant-context').status,409)
        digest=self.contexts[job['jobId']]['contextSha256']
        self.assertEqual(self.post(job,envelope,headers={'X-LTDS-Initial-Context-SHA256':digest,'x-ltds-initial-context-sha256':digest}).status,400)
        self.assertEqual(before,self.queue.path.read_bytes())

    def test_first_claim_context_is_in_same_transaction(self):
        original=self.queue._db;attempted=[]
        @contextmanager
        def denied():
            with original() as db:
                def authorize(action,table,*args):
                    if action==sqlite3.SQLITE_INSERT and table=='reference_initial_claims':
                        attempted.append(table);return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
                db.set_authorizer(authorize)
                try:yield db
                finally:db.set_authorizer(None)
        reply=self.request('POST','/jobs',self.fixture['submission']);job=reply.body
        with patch.object(self.queue,'_db',denied),self.assertRaises(sqlite3.DatabaseError):
            self.store.claim(job['jobId'],node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster',claim_request_id='atomic-claim')
        self.assertEqual(attempted,['reference_initial_claims'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_claims').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT status FROM jobs').fetchone()[0],'queued')
        claim=self.store.claim(job['jobId'],node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster',claim_request_id='atomic-claim')
        self.assertIsNotNone(claim)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_claims').fetchone()[0],1)

    def test_raw_grant_fallback_and_successor_epoch_are_unavailable(self):
        job,_,envelope=self.prepare()
        self.assertEqual(self.request('POST','/jobs/'+job['jobId']+'/transfer-grants',envelope['grant']).status,409)
        self.assertEqual(self.request('POST','/jobs/'+job['jobId']+'/renewal-grants',{}).status,404)
        self.assertEqual(self.api.handle('POST','/ltds/renewable/v1/jobs/'+job['jobId']+'/renewal-requests',
            {'Authorization':'Bearer '+tokens.STAGING},b'{}',protected_transport=True).status,404)
        changed=copy.deepcopy(envelope);changed['request']['epoch']=1
        self.assertNotEqual(self.post(job,changed).status,200)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],0)

    def test_current_generation_node_worker_and_issuer_context_are_exact(self):
        job,_,envelope=self.prepare()
        for section,key,value in [('request','generation',2),('request','nodeId','main'),('request','workerId','other'),
                                  ('issuanceSeed','issuerRealm','foreign'),('issuanceSeed','keyId','foreign'),
                                  ('issuanceSeed','capabilitiesSha256','0'*64),('issuanceSeed','objectClosureSha256','0'*64)]:
            changed=copy.deepcopy(envelope);changed[section][key]=value
            with self.subTest(key=key):self.assertNotEqual(self.post(job,changed).status,200)
        self.assertEqual(self.post(job,envelope).status,200)
        with self.queue._db() as db:db.execute('UPDATE reference_jobs SET generation=2 WHERE job_id=?',(job['jobId'],))
        self.assertEqual(self.post(job,envelope).status,409)

    def test_bearer_shape_canonical_43_chars_32_bytes_required_no_hmac_claim(self):
        job,_,envelope=self.prepare()
        for token in ('short','a'*44,'a'*42+'=','A'*42+'B'):
            changed=copy.deepcopy(envelope);changed['grant']['token']=token
            with self.subTest(length=len(token)):self.assertNotEqual(self.post(job,changed).status,200)
        self.assertEqual(self.post(job,envelope).status,200)

    def test_new_token_or_nonce_after_lost_ack_cannot_replace_original(self):
        job,_,envelope=self.prepare();ack=self.post(job,envelope);self.assertEqual(ack.status,200)
        before=self.queue.path.read_bytes()
        changed=copy.deepcopy(envelope);changed['grant']['token']=base64.urlsafe_b64encode(b'X'*32).rstrip(b'=').decode()
        self.assertEqual(self.post(job,changed).status,409)
        changed=copy.deepcopy(envelope);changed['request']['nonce']='new-nonce'
        self.assertEqual(self.post(job,changed).status,409)
        self.assertEqual(before,self.queue.path.read_bytes())

    def test_real_sqlite_failure_between_two_inserts_rolls_back_both(self):
        job,_,envelope=self.prepare();original=self.queue._db;observed=[]
        @contextmanager
        def deny_receipt():
            with original() as db:
                def authorize(action,table,*args):
                    if action==sqlite3.SQLITE_INSERT:
                        observed.append(table)
                        if table=='reference_initial_receipts':return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK
                db.set_authorizer(authorize)
                try:yield db
                finally:db.set_authorizer(None)
        with patch.object(self.queue,'_db',deny_receipt):self.assertEqual(self.post(job,envelope).status,500)
        self.assertIn('reference_transfer_grants',observed);self.assertIn('reference_initial_receipts',observed)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_receipts').fetchone()[0],0)
        self.assertEqual(self.post(job,envelope).status,200)

    def test_auth_before_body_decode_and_no_public_private_token(self):
        job,_,envelope=self.prepare()
        with patch('processing.reference_api.protocol.decode_submission',side_effect=AssertionError('decoded before foreign auth')):
            self.assertEqual(self.post(job,envelope,token=tokens.PRODUCTION).status,404)
            self.assertEqual(self.post(job,envelope,token='synthetic-cluster-worker-token').status,401)
            self.assertEqual(self.post(job,envelope,protected=False).status,503)
        self.assertEqual(self.post(job,envelope).status,200)
        public=self.request('GET','/jobs/'+job['jobId']).body
        self.assertNotIn(envelope['grant']['token'],json.dumps(public))

    def test_initial_receipt_and_private_grant_charge_same_metadata_quota(self):
        job,_,envelope=self.prepare()
        with self.queue._db() as db:
            # Determine current logical usage using its actual quota oracle.
            self.store.max_metadata_bytes=1
        self.assertEqual(self.post(job,envelope).status,409)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_receipts').fetchone()[0],0)

    def test_actual_existing_loopback_http_initial_route(self):
        job,_,envelope=self.prepare();server=create_reference_http_server(self.api)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02});thread.start()
        connection=http.client.HTTPConnection(*server.server_address,timeout=5)
        try:
            connection.request('POST','/ltds/v1/jobs/'+job['jobId']+'/initial-grant',p.canonical_json(envelope),
                {'Authorization':'Bearer '+tokens.STAGING,'Content-Type':'application/json',
                 'X-LTDS-Initial-Context-SHA256':self.contexts[job['jobId']]['contextSha256']})
            response=connection.getresponse();self.assertEqual(response.status,200);self.assertTrue(json.loads(response.read())['accepted'])
        finally:connection.close();server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_actual_http_duplicate_context_headers_refused_before_writes(self):
        job,_,envelope=self.prepare();server=create_reference_http_server(self.api)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02});thread.start()
        digest=self.contexts[job['jobId']]['contextSha256'];body=p.canonical_json(envelope)
        before=self.queue.path.read_bytes()
        try:
            for second_name,second_value in [('X-LTDS-Initial-Context-SHA256',digest),
                                            ('X-LTDS-Initial-Context-SHA256','0'*64),
                                            ('x-ltds-initial-context-sha256',digest),
                                            ('x-ltds-initial-context-sha256','0'*64)]:
                with self.subTest(name=second_name,same_value=second_value==digest):
                    connection=http.client.HTTPConnection(*server.server_address,timeout=5)
                    try:
                        connection.putrequest('POST','/ltds/v1/jobs/'+job['jobId']+'/initial-grant')
                        connection.putheader('Authorization','Bearer '+tokens.STAGING)
                        connection.putheader('Content-Length',str(len(body)))
                        connection.putheader('X-LTDS-Initial-Context-SHA256',digest)
                        connection.putheader(second_name,second_value)
                        connection.endheaders(body)
                        response=connection.getresponse();self.assertEqual(response.status,400)
                        self.assertEqual(json.loads(response.read()),{'error':'duplicate_header'})
                    finally:connection.close()
                    with self.queue._db() as db:
                        self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],0)
                        self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_receipts').fetchone()[0],0)
                    self.assertEqual(before,self.queue.path.read_bytes())
        finally:server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_parent_claim_tuple_hash_authority_and_replay_snapshot_are_bound(self):
        job,_,envelope=self.prepare()
        edits=[('jobs','worker_id','other'),('jobs','node_id','main'),('jobs','claim_hash','0'*64),
               ('reference_claims','worker_id','other'),('reference_claims','node_id','main'),('reference_claims','token','changed'),
               ('coordinator_identity','authority_id','different-authority')]
        for table,column,value in edits:
            where='singleton=1' if table=='coordinator_identity' else ('id=?' if table=='jobs' else 'job_id=?')
            args=() if table=='coordinator_identity' else (job['jobId'],)
            with self.queue._db() as db:
                before=db.execute('SELECT '+column+' FROM '+table+' WHERE '+where,args).fetchone()[0]
                db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+where,(value,*args))
            with self.subTest(table=table,column=column):self.assertEqual(self.post(job,envelope).status,409)
            with self.queue._db() as db:db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+where,(before,*args))
        self.assertEqual(self.post(job,envelope).status,200)
        for column in ('instance_id','request_id'):
            with self.queue._db() as db:
                before=db.execute('SELECT '+column+' FROM reference_claims WHERE job_id=?',(job['jobId'],)).fetchone()[0]
                db.execute('UPDATE reference_claims SET '+column+'=? WHERE job_id=?',('different-snapshot',job['jobId']))
            self.assertEqual(self.post(job,envelope).status,409)
            with self.queue._db() as db:db.execute('UPDATE reference_claims SET '+column+'=? WHERE job_id=?',(before,job['jobId']))

    def test_oversized_private_digest_refused_before_materializing_on_replay(self):
        job,_,envelope=self.prepare();self.assertEqual(self.post(job,envelope).status,200)
        with self.queue._db() as db:db.execute('UPDATE reference_transfer_grants SET digest=? WHERE job_id=?',('d'*(p.MAX_BODY_BYTES+1),job['jobId']))
        original=self.queue._db;seen=[]
        @contextmanager
        def traced():
            with original() as db:
                db.set_trace_callback(seen.append)
                try:yield db
                finally:db.set_trace_callback(None)
        with patch.object(self.queue,'_db',traced):self.assertEqual(self.post(job,envelope).status,409)
        self.assertFalse(any(sql.startswith('SELECT grant_json,digest') for sql in seen))

    def test_fresh_expired_or_invalid_clock_does_not_commit_either_row(self):
        job,_,envelope=self.prepare()
        expiry=__import__('datetime').datetime.fromisoformat(envelope['grant']['expiresAt'].replace('Z','+00:00')).timestamp()
        for now,error in ((expiry+1,ValueError),(True,Conflict),(float('nan'),Conflict),(float('inf'),Conflict),(None,Conflict)):
            with self.subTest(clock=type(now).__name__),self.assertRaises(error):
                self.store.receive_initial_grant(job['jobId'],envelope,environment='staging',clock=lambda:now,
                    principal_binding=self.api.viewer._initial_principal_binding({'Authorization':'Bearer '+tokens.STAGING},'staging'),
                    context_sha256=self.contexts[job['jobId']]['contextSha256'])
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reference_transfer_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_receipts').fetchone()[0],0)

    def test_incompatible_epoch_zero_caps_refused_before_job_admission(self):
        for name,value in [('maxRequestsPerEpoch',2),('maxTotalRequests',2),('maxResponseBytes',2),('maxConcurrentBytes',2),('maxTotalBytes',8)]:
            data=profile_payload();data['brokerCapabilities'][name]=value;policy=InitialGrantPolicy.from_payload(data)
            directory=self.root/('incompatible-'+name)
            queue=Queue(directory,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=policy);self.addCleanup(queue.close)
            store=ReferenceStore(queue,initial_grant_policy=policy);api=self.control(store)
            reply=api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+tokens.STAGING},p.canonical_json(self.fixture['submission']),protected_transport=True)
            with self.subTest(cap=name):self.assertNotEqual(reply.status,201)
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT count(*) FROM reference_jobs').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_jobs').fetchone()[0],0)

    def test_unmarked_legacy_job_is_not_adopted_and_keeps_original_v1_path(self):
        directory=self.root/'legacy'
        queue=Queue(directory,node_ids=('cluster','main'),reference_enabled=True);store=ReferenceStore(queue)
        api=self.control(store)
        job=api.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+tokens.STAGING},p.canonical_json(self.fixture['submission']),protected_transport=True).body
        queue.close()
        queue=Queue(directory,node_ids=('cluster','main'),reference_enabled=True,initial_grant_policy=self.policy);self.addCleanup(queue.close)
        store=ReferenceStore(queue,initial_grant_policy=self.policy)
        store.claim(job['jobId'],node_id='cluster',worker_id='worker-cluster',worker_instance_id='instance-cluster',claim_request_id='legacy-claim')
        reservation=copy.deepcopy(self.fixture['transferRequest']);reservation['expiresAt']=stamp(round((time.time()+100)*1000))
        store.reserve(job['jobId'],reservation,node_id='cluster')
        grant=dict(copy.deepcopy(self.fixture['transferGrant']),**reservation,jobId=job['jobId'])
        self.assertTrue(store.receive_transfer_grant(job['jobId'],grant)['accepted'])
        with queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM reference_initial_jobs').fetchone()[0],0)


class InitialContextContractTests(unittest.TestCase):
    def test_public_context_canonical_cross_language_vector(self):
        from processing.reference_initial_grant import claim_context_payload
        root=Path(__file__).resolve().parents[1]/'contracts'
        original=json.loads((root/'reference-initial-grant-v1.contract.json').read_text())
        vector=json.loads((root/'reference-initial-context-v1.contract.json').read_text())
        self.assertEqual(vector['initialFixtureSha256'],p.digest(original))
        request=original['envelope']['request']
        context=claim_context_payload(original['job'],node_id=request['nodeId'],worker_id=request['workerId'],
            policy=InitialGrantPolicy.from_payload(original['profile']),context_id='a'*64,now_ms=original['validationClockMs'])
        self.assertEqual(context,vector['context']);self.assertEqual(p.canonical_json(context).hex(),vector['canonicalContextUtf8Hex'])
        self.assertEqual(vector['postHeader'],{'X-LTDS-Initial-Context-SHA256':context['contextSha256']})
        before=copy.deepcopy(context);digest=before.pop('contextSha256')
        self.assertEqual(p.digest(before),digest);self.assertEqual(len(context),18)


class InitialRuntimeWiringTests(unittest.TestCase):
    def setUp(self):
        from tests import test_reference_runtime as runtime_fixture
        self.f=runtime_fixture.ReferenceRuntimeTests();self.f.setUp();self.addCleanup(self.f.doCleanups)

    def test_private_settings_default_off_and_strict_initial_only_profile(self):
        self.assertIsNone(self.f.settings().initial_grant_delivery)
        for change in ({'maxEpochs':1},{'maxEpochs':True},{'schemaVersion':True},{'unknown':1}):
            value=profile_payload();value.update(change);self.f.value['initialGrantDelivery']=value;self.f.write_config()
            with self.assertRaises(ValueError):self.f.settings()
        self.f.value['initialGrantDelivery']=profile_payload();self.f.write_config()
        self.assertEqual(self.f.settings().initial_grant_delivery,InitialGrantPolicy.from_payload(profile_payload()))

    def test_actual_make_handler_wires_policy_before_store_and_keeps_listener_unstarted(self):
        from dataclasses import replace
        from processing.app import make_handler
        self.f.value['initialGrantDelivery']=profile_payload();self.f.write_config()
        first,second=self.f.tls_patches()
        with first,second:handler=make_handler(replace(self.f.config,reference_config_path=str(self.f.path)))
        self.addCleanup(handler.prototype_queue.close);self.addCleanup(handler.viewer_service.close);self.addCleanup(handler.reference_listener.close)
        runtime=handler.reference_listener;expected=InitialGrantPolicy.from_payload(profile_payload())
        self.assertEqual(handler.prototype_queue.initial_grant_policy,expected)
        self.assertEqual(runtime.store.initial_grant_policy,expected)
        self.assertTrue(runtime.control.viewer.grant_reconciliation)
        self.assertIsNone(runtime._server);self.assertIsNone(runtime._thread)
        runtime.close();handler.viewer_service.close();handler.prototype_queue.close()
        before=(Path(self.f.config.prototype_state_dir)/'jobs.sqlite3').read_bytes()
        with patch('processing.viewer_service.ViewerService',side_effect=AssertionError('legacy writer started before latch')):
            with self.assertRaises(ValueError):make_handler(self.f.config)
        self.assertEqual(before,(Path(self.f.config.prototype_state_dir)/'jobs.sqlite3').read_bytes())


class InitialSharedVectorTests(unittest.TestCase):
    def test_fixed_initial_only_seed_hmac_envelope_and_receipt_bytes(self):
        from processing.reference_initial_grant import validate_initial_envelope
        data=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-initial-grant-v1.contract.json').read_text())
        seed=data['envelope']['issuanceSeed'];raw=seed_bytes(seed)
        self.assertEqual(raw.hex(),data['canonicalSeedUtf8Hex'])
        mac=hmac.digest(bytes.fromhex(data['keyHex']),b'LTDS reference transfer bearer v1\0'+raw,'sha256')
        self.assertEqual(mac.hex(),data['hmacSha256Hex'])
        self.assertEqual(base64.urlsafe_b64encode(mac).rstrip(b'=').decode(),data['envelope']['grant']['token'])
        self.assertEqual(p.digest(data['envelope']),data['envelopeSha256'])
        _,ack=validate_initial_envelope(data['envelope'],job=data['job'],node_id='cluster',worker_id='worker-cluster',
            policy=InitialGrantPolicy.from_payload(data['profile']),now_ms=data['validationClockMs'])
        self.assertEqual(ack,data['receipt']);self.assertEqual(p.canonical_json(ack).hex(),data['canonicalReceiptUtf8Hex'])
        self.assertEqual(data['derivedTransferPolicy']['maxEpochs'],0)

    def test_100_gets_cannot_cover_10010_attempts_even_with_future_epoch_capability(self):
        value=profile_payload();value['brokerCapabilities'].update(maxRequestsPerEpoch=100,maxEpochs=32)
        policy=InitialGrantPolicy.from_payload(value)
        manifest=dict(totalBytes=30000,objects=[dict(byteSize=3) for _ in range(10000)])
        with self.assertRaises(ValueError):policy.transfer_policy(manifest)
        value['brokerCapabilities']['maxRequestsPerEpoch']=10010
        result=InitialGrantPolicy.from_payload(value).transfer_policy(manifest)
        self.assertEqual(result['attemptBudget'],10010);self.assertEqual(result['maxEpochs'],0)


if __name__=='__main__':unittest.main()
