"""Actual local HTTP routes with explicitly synthetic authority and private DBs."""
import base64
import copy
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid

from processing import reference_grant_control as c
from processing import reference_grant_renewal as r
from tests import test_reference_grant_renewal as renewal_fixture
stamp=renewal_fixture.stamp


class RenewableControlTests(unittest.TestCase):
    def setUp(self):
        self.f=renewal_fixture.RenewalJournalTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.journal_db=sqlite3.connect(self.root/'journal.sqlite',check_same_thread=False)
        self.control_db=sqlite3.connect(self.root/'control.sqlite',check_same_thread=False)
        self.addCleanup(self.journal_db.close);self.addCleanup(self.control_db.close)
        self.context=dict(journalDb=self.journal_db,controlDb=self.control_db,job=self.f.job,
            nodeId='main',workerId='main-worker',policy=self.f.policy,capabilities=self.f.capabilities,
            startedMs=100000,stage='awaiting_transfer',inflightDrained=True,keyId='synthetic-key',issuerRealm='synthetic-staging')
        self.now=110000
        def authorize(headers):
            role=headers.get('Authorization','').removeprefix('Bearer ')
            if role not in ('worker','issuer'):raise ValueError('synthetic credentials required')
            return dict(role=role,jobId=self.f.job['jobId'],environment='staging')
        self.api=c.create_synthetic_control(scope='synthetic-fixture-only',authorize=authorize,
            resolve=lambda job:self.context,clock_ms=lambda:self.now)
        self.initial_request=dict(schemaVersion=2,profile=r.PROFILE,requestId=str(uuid.uuid4()),
            jobId=self.f.job['jobId'],generation=1,environment='staging',reservationId=self.f.grant['reservationId'],
            manifestSha256=self.f.grant['manifestSha256'],nodeId='main',workerId='main-worker',
            epoch=0,predecessorSha256=None,expiresAt=self.f.grant['expiresAt'],nonce=self.f.grant['nonce'])
        self.seed=c.expected_seed(self.initial_request,self.context)
        self.initial_grant=copy.deepcopy(self.f.grant)
        self.initial_grant['token']=self.token(self.seed)
        self.initial=dict(schemaVersion=2,profile=r.PROFILE,request=self.initial_request,issuanceSeed=self.seed,grant=self.initial_grant)

    def token(self,seed):
        # Test-only issuer. Production receiver never receives this key.
        return base64.urlsafe_b64encode(hmac.digest(bytes(range(32)),b'LTDS reference transfer bearer v1\0'+c.seed_bytes(seed),'sha256')).rstrip(b'=').decode()

    def call(self,method,suffix,body=None,role='issuer',**options):
        return self.api.handle(method,c.PREFIX+'/jobs/'+self.f.job['jobId']+'/'+suffix,
            {'Authorization':'Bearer '+role},b'' if body is None else r.encoded(body),protected_transport=options.get('protected',True))

    def initialize(self):
        reply=self.call('POST','initial-grant',self.initial)
        self.assertEqual(reply.status,200,reply.body)
        self.context['stage']='fetching'
        return reply

    def renewal_request(self):
        return dict(self.initial_request,requestId=str(uuid.uuid4()),epoch=1,
            predecessorSha256=r.digest(self.initial_grant),expiresAt=stamp(300000),nonce='renewal-one')

    def test_production_hardoff_and_synthetic_transport_role_boundaries(self):
        self.assertEqual(c.RenewableControlAPI().handle('POST','anything',{},b'{}').status,404)
        with self.assertRaises(r.RenewalHeld):c.create_synthetic_control(scope='production',authorize=lambda _:None,resolve=lambda _:None,clock_ms=lambda:0)
        self.assertEqual(self.call('POST','initial-grant',self.initial,protected=False).status,503)
        self.assertEqual(self.call('POST','initial-grant',self.initial,role='invalid').status,401)
        self.assertEqual(self.call('POST','initial-grant',self.initial,role='worker').status,409)
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_initial_epoch_zero_is_durable_and_exact_replay_after_expiry(self):
        first=self.initialize();self.now=250000
        replay=self.call('POST','initial-grant',copy.deepcopy(self.initial))
        self.assertEqual(replay.body,first.body);self.assertNotIn(self.initial_grant['token'],json.dumps(first.body))
        private=self.call('GET','private-grant',role='worker')
        self.assertEqual(private.status,200);self.assertEqual(private.body['privateGrant'],self.initial_grant)
        changed=copy.deepcopy(self.initial);changed['request']['requestId']=str(uuid.uuid4())
        changed['issuanceSeed']=c.expected_seed(changed['request'],self.context)
        self.assertEqual(self.call('POST','initial-grant',changed).status,409)

    def test_initial_unknown_after_intent_commit_resumes_same_body_only(self):
        from unittest.mock import patch
        with patch.object(self.api,'_journal',side_effect=RuntimeError('synthetic crash after intent')):
            self.assertEqual(self.call('POST','initial-grant',self.initial).status,409)
        self.assertEqual(self.control_db.execute('SELECT count(*) FROM initial_intent').fetchone()[0],1)
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
        self.assertEqual(self.call('POST','initial-grant',self.initial).status,200)

    def test_immutable_original_job_pipeline_cannot_change_after_initial_intent(self):
        self.initialize()
        changed=copy.deepcopy(self.context['job']);changed['submission']['pipeline']['odm']['options']['new-option']=True
        self.context['job']=changed
        self.assertEqual(self.call('GET','private-grant',role='worker').status,409)
        self.assertEqual(self.call('POST','initial-grant',self.initial).status,409)

    def test_initial_seed_substitutions_and_capability_mismatch_no_effects(self):
        bad=copy.deepcopy(self.initial);bad['issuanceSeed']['policySha256']='1'*64
        self.assertEqual(self.call('POST','initial-grant',bad).status,409)
        self.context['capabilities']=dict(self.f.capabilities,maxConcurrentBytes=1)
        bad=copy.deepcopy(self.initial);bad['issuanceSeed']=c.expected_seed(bad['request'],self.context)
        self.assertEqual(self.call('POST','initial-grant',bad).status,409)
        self.assertEqual(self.control_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_role_separated_renewal_pipeline_and_reconciliation(self):
        self.initialize();request=self.renewal_request()
        self.assertEqual(self.call('POST','renewal-requests',request,role='worker').status,200)
        observed=self.call('GET','renewal-requests')
        self.assertEqual(observed.body,{'request':request})
        self.assertEqual(self.call('GET','renewal-requests',role='worker').status,405)
        grant=dict(self.initial_grant,expiresAt=request['expiresAt'],nonce=request['nonce'],token=self.token(c.expected_seed(request,self.context)))
        envelope=dict(schemaVersion=2,profile=r.PROFILE,requestId=request['requestId'],epoch=1,predecessorSha256=request['predecessorSha256'],grant=grant)
        self.assertEqual(self.call('POST','renewal-grants',envelope,role='worker').status,405)
        accepted=self.call('POST','renewal-grants',envelope)
        self.assertEqual(accepted.status,200)
        self.assertEqual(self.call('POST','renewal-grants',envelope).body,accepted.body)
        received=self.call('GET','private-grant',role='worker')
        self.assertEqual(received.body,{'epoch':1,'privateGrant':envelope})
        receipt=self.call('GET','reconcile/'+request['requestId'],role='worker')
        self.assertTrue(receipt.body['receipt']['accepted']);self.assertNotIn(grant['token'],json.dumps(receipt.body))

    def test_drain_stage_and_key_rebinding_hold_without_rotation(self):
        self.initialize();request=self.renewal_request();self.context['inflightDrained']=False
        self.assertEqual(self.call('POST','renewal-requests',request,role='worker').status,409)
        self.context['inflightDrained']=True;self.context['stage']='executing'
        self.assertEqual(self.call('POST','renewal-requests',request,role='worker').status,409)
        self.context['stage']='fetching';self.context['keyId']='rotated-key'
        self.assertEqual(self.call('GET','private-grant',role='worker').status,409)
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM renewal_epochs').fetchone()[0],1)

    def test_duplicate_wire_and_cross_job_do_not_initialize(self):
        raw=r.encoded(self.initial).decode().replace('"schemaVersion":2','"schemaVersion":2,"schemaVersion":2',1).encode()
        reply=self.api.handle('POST',c.PREFIX+'/jobs/'+self.f.job['jobId']+'/initial-grant',{'Authorization':'Bearer issuer'},raw,protected_transport=True)
        self.assertEqual(reply.status,409)
        reply=self.api.handle('POST',c.PREFIX+'/jobs/foreign/initial-grant',{'Authorization':'Bearer issuer'},r.encoded(self.initial),protected_transport=True)
        self.assertEqual(reply.status,404)
        self.assertEqual(self.control_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_actual_local_http_roundtrip_is_explicitly_synthetic(self):
        api=self.api
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                # Synthetic transport assertion ONLY, no production TLS claim.
                reply=api.handle('POST',self.path,dict(self.headers),self.rfile.read(int(self.headers['Content-Length'])),protected_transport=True)
                raw=r.encoded(reply.body);self.send_response(reply.status);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        server=HTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            request=Request('http://127.0.0.1:'+str(server.server_port)+c.PREFIX+'/jobs/'+self.f.job['jobId']+'/initial-grant',data=r.encoded(self.initial),headers={'Authorization':'Bearer issuer'},method='POST')
            with urlopen(request,timeout=5) as response:
                self.assertEqual(response.status,200);reply=json.loads(response.read())
                self.assertTrue(reply['accepted']);self.assertEqual(reply['epoch'],0)
        finally:server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_seed_closed_shape_integer_epoch_null_and_canonical_timestamp(self):
        for key,value in [('epoch',True),('generation',True),('epoch',33),('predecessorSha256','0'*64),
                          ('expiresAt','2026-10-29T12:00:00Z'),('keyId','key/foreign'),
                          ('issuerRealm','unicode-\u00e9'),('objectClosureSha256','A'*64),('extra',1)]:
            bad=copy.deepcopy(self.seed);bad[key]=value
            with self.subTest(key=key),self.assertRaises(r.RenewalHeld):c.seed_bytes(bad)
        bad=dict(self.seed,epoch=1)
        with self.assertRaises(r.RenewalHeld):c.seed_bytes(bad)

    def test_first_request_wrong_lifetime_and_byte_budget_leave_both_databases_empty(self):
        self.context['policy']=dict(self.f.policy,maxLifetimeMs=1)
        wrong=copy.deepcopy(self.initial);wrong['issuanceSeed']=c.expected_seed(wrong['request'],self.context)
        self.assertEqual(self.call('POST','initial-grant',wrong).status,409)
        self.assertEqual(self.control_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
        self.context['policy']=dict(self.f.policy,byteBudget=3)
        wrong['issuanceSeed']=c.expected_seed(wrong['request'],self.context)
        self.assertEqual(self.call('POST','initial-grant',wrong).status,409)
        self.assertEqual(self.control_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_initial_clock_integer_and_sum_range_checked_before_any_intent(self):
        for started in (True,1.5,r.p.MAX_SAFE_INTEGER):
            self.context['startedMs']=started
            self.assertEqual(self.call('POST','initial-grant',self.initial).status,409)
            self.assertEqual(self.control_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)
            self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)

    def test_unknown_oversized_catalog_sql_refused_before_materialization(self):
        self.control_db.execute('CREATE TABLE foreign_catalog("'+('x'*9000)+'" TEXT)');self.control_db.commit()
        seen=[];self.control_db.set_trace_callback(seen.append)
        self.assertEqual(self.call('POST','initial-grant',self.initial).status,409)
        self.control_db.set_trace_callback(None)
        self.assertFalse(any(sql.startswith('SELECT sql FROM main.sqlite_master') for sql in seen))
        self.assertEqual(self.journal_db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0],0)


if __name__=='__main__':unittest.main()
