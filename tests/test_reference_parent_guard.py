"""Actual TLS worker API, Queue, ledger and OS leases; entirely synthetic data."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import copy
import json
import os
from pathlib import Path
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from processing import reference_parent_guard as m
from processing import reference_protocol as p
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_api import Response
from processing.reference_delivery import DeliveryReceiver
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_tls import verified_client_context
from ops.reference_worker_daemon import DaemonStateLock
from tests import test_reference_pipeline as fixtures

WORKER='synthetic-parent-worker-secret-0001'


@unittest.skipUnless(os.name=='posix','physical Linux leases required')
class ParentGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.certdir=tempfile.TemporaryDirectory();cls.addClassCleanup(cls.certdir.cleanup)
        root=Path(cls.certdir.name);cls.cert=root/'cert.pem';cls.key=root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=synthetic-loopback',
            '-addext','subjectAltName=IP:127.0.0.1','-addext','basicConstraints=critical,CA:TRUE',
            '-keyout',str(cls.key),'-out',str(cls.cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

    def setUp(self):
        self.host=fixtures.PipelineTests();self.host.setUp();self.addCleanup(self.host.doCleanups)
        h=self.host;self.parent=h.plan['parentBinding'];self.owner=DaemonStateLock(h.root).__enter__()
        self.addCleanup(self.owner.__exit__,None,None,None)
        h.ledger.record_allocated('parent-reservation',self.parent['inputBytes'])
        transfer=dict(reservationId='parent-reservation',generation=1,expiresAt=(datetime.now(timezone.utc)+timedelta(seconds=240)).isoformat(timespec='milliseconds').replace('+00:00','Z'),
            manifestSha256=h.claim['manifestSha256'],byteBudget=self.parent['inputBytes'],nonce='synthetic-transfer')
        h.store.reserve(h.claim['jobId'],transfer,node_id='main');h.store.update_phase(h.claim['jobId'],'fetching');h.store.update_phase(h.claim['jobId'],'processing')
        self.api=ReferenceWorkerAPI(h.store,DeliveryReceiver(h.store),bearer_workers={WORKER:dict(node_id='main',worker_id='main-worker',worker_instance_id='instance-one')})
        self.requests=[];original=self.api.handle
        def observed(method,path,*args,**kwargs):
            self.requests.append((method,path));return original(method,path,*args,**kwargs)
        self.api.handle=observed
        serverctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);serverctx.load_cert_chain(self.cert,self.key)
        self.server=create_reference_http_server(self.api,ssl_context=serverctx)
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start();self.addCleanup(self.stop)
        self.context=verified_client_context(str(self.cert))
        self.policy=m.ClaimStatePolicy('https://127.0.0.1:'+str(self.server.server_port),h.policy.authority_id,'main','main-worker','instance-one','worker-main',m.tls_trust_sha256(self.context))
        self.client=m.ClaimStateClient(self.policy,m.WorkerCredential('worker-main',WORKER),ssl_context=self.context)
        binding=dict(origin=self.policy.origin,nodeId='main',workerId='main-worker',workerInstanceId='instance-one',authorityId=h.policy.authority_id,
            engineId=h.policy.engine_id,gateOrigin=h.policy.coordinator_origin,gateGeneration=1,outputBudget=200,environments=sorted(h.policy.allowed_environments))
        self.driver=h.root/'driver.sqlite'
        with closing(sqlite3.connect(self.driver)) as db,db:
            db.execute('CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT)');db.execute('INSERT INTO driver_binding VALUES(1,?)',(json.dumps(binding),))
            db.execute('CREATE TABLE driver_jobs(job TEXT PRIMARY KEY,claim_request TEXT,data TEXT)')
            db.execute('INSERT INTO driver_jobs VALUES(?,?,?)',(h.claim['jobId'],'pipeline-claim',json.dumps(dict(claim=h.store.get(h.claim['jobId'],private=True),reservation=transfer))))
        self.driver.chmod(0o600)
        self.tokens=m.DriverClaimTokens(self.driver,journal=h.journal,policy=self.policy,state_owner=self.owner)
        self.guard=m.ProductionParentGuard(journal=h.journal,ledger=h.ledger,client=self.client,tokens=self.tokens,state_owner=self.owner)

    def stop(self):
        self.server.shutdown();self.thread.join(5);self.server.server_close();self.assertFalse(self.thread.is_alive())

    def check(self,mutation=True):return self.guard.check(self.parent,mutation=mutation)

    def held(self,mutation=True):
        with self.assertRaises(m.ParentGuardHeld) as caught:self.check(mutation)
        self.assertNotIn(WORKER,str(caught.exception));self.assertNotIn(self.host.claim['claimToken'],str(caught.exception))

    def test_actual_get_is_fresh_readonly_and_never_reclaims_or_mutates_gate(self):
        h=self.host;paths=[Path(h.queue.path),self.driver,Path(h.ledger.path),h.root/'gate'/'gate.json',h.journal.path]
        before={str(path):path.read_bytes() for path in paths};lock=h.root/'gate'/'gate.lock';ctime=lock.stat().st_ctime_ns
        with patch.object(NodeGate,'__enter__',side_effect=AssertionError('no write-capable gate enter')),patch.object(h.ledger,'_transaction',side_effect=AssertionError('no ledger write')),patch.object(h.store,'claim',side_effect=AssertionError('no claim')):
            self.assertIsNone(self.check());self.assertIsNone(self.check(False))
        self.assertEqual(before,{str(path):path.read_bytes() for path in paths});self.assertEqual(lock.stat().st_ctime_ns,ctime)
        self.assertEqual(len(self.requests),2);self.assertTrue(all(method=='GET' and route.endswith('/claim-state') for method,route in self.requests))

    def test_no_cached_authorization_on_404_network_or_auth_failure(self):
        self.check()
        with patch.object(self.api,'handle',return_value=Response(404,{'error':'missing'})):self.held()
        with patch.object(self.api,'handle',return_value=Response(401,{'error':'unauthorized'})):self.held()
        self.server.shutdown();self.thread.join(5);self.server.server_close();self.held()

    def test_closed_snapshot_identity_and_strict_types(self):
        original=self.api.handle
        changes=[('extra','x'),('generation',True),('generation',1.0),('schemaVersion',True),('authorityId','0'*32),('workerInstanceId','other'),('claimRequestId','other'),('environment','production'),('manifestSha256','0'*64),('pipelineDigest','0'*64),('claimTokenSha256','0'*64),('executionPhase','fetching')]
        for key,value in changes:
            def altered(*args,**kwargs):
                response=original(*args,**kwargs);body=dict(response.body);body[key]=value;return Response(response.status,body)
            with self.subTest(key=key,value=value),patch.object(self.api,'handle',side_effect=altered):self.held()
        def missing(*args,**kwargs):
            response=original(*args,**kwargs);body=dict(response.body);del body['heldReason'];return Response(200,body)
        with patch.object(self.api,'handle',side_effect=missing):self.held()

    def test_actual_tls_claim_read_accepts_only_explicit_integer_v2_marker(self):
        original=self.api.handle
        def altered(version):
            def reply(*args,**kwargs):
                response=original(*args,**kwargs)
                return Response(response.status,{**response.body,'sourceIntakeVersion':version})
            return reply
        with patch.object(self.api,'handle',side_effect=altered(2)):
            snapshot=self.client.snapshot(self.host.claim['jobId'],self.host.claim['claimToken'])
            self.assertEqual(snapshot['sourceIntakeVersion'],2)
        for version in (True,2.0,1,3,None,'2'):
            with self.subTest(version=version),patch.object(self.api,'handle',side_effect=altered(version)):
                with self.assertRaises(m.ParentGuardHeld):
                    self.client.snapshot(self.host.claim['jobId'],self.host.claim['claimToken'])

    def test_real_private_api_refuses_wrong_worker_instance_and_claim_token(self):
        for column,value,table,key in [('instance_id','other','reference_claims','job_id'),('claim_hash','0'*64,'jobs','id')]:
            with self.host.queue._db() as db:
                old=db.execute('SELECT '+column+' FROM '+table+' WHERE '+key+'=?',(self.parent['jobId'],)).fetchone()[0]
                db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+key+'=?',(value,self.parent['jobId']))
            self.held()
            with self.host.queue._db() as db:db.execute('UPDATE '+table+' SET '+column+'=? WHERE '+key+'=?',(old,self.parent['jobId']))

    def restart(self):
        h=self.host;h.queue.close();h.queue=Queue(h.root/'queue',node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
        self.addCleanup(h.queue.close);h.store=ReferenceStore(h.queue);self.api.store=h.store

    def test_restart_only_allows_readonly_already_started_evidence(self):
        self.host.ledger.set_engine_state('parent-reservation','engine_started');self.restart()
        self.assertIsNone(self.check(False));self.held(True)

    def test_restart_reserved_and_operator_or_terminal_hold_every_mode(self):
        self.restart();self.held(False);self.held(True)
        self.host.queue.operator(self.parent['jobId'],'hold',{'note':'synthetic'})
        self.held(False);self.held(True)
        self.host.queue.operator(self.parent['jobId'],'resolve',{'note':'synthetic','workerStopped':True})
        self.held(False);self.held(True)

    def test_parent_binding_closed_and_not_configuration_hash_authorization(self):
        original=self.parent
        for key,value in [('generation',True),('inputBytes',True),('gateGeneration',2),('workerInstanceId','other'),('authorityId','0'*32),('reservationId','other'),('gateReceiptId','b'*32),('extra',0)]:
            self.parent=dict(original,**{key:value})
            with self.subTest(key=key):self.held()
        self.parent=original;self.check()

    def test_local_ledger_corruption_uncertain_terminal_and_expiry(self):
        with closing(sqlite3.connect(self.host.ledger.path)) as db,db:db.execute("UPDATE reservations SET allocated=allocated-1")
        self.held()
        with closing(sqlite3.connect(self.host.ledger.path)) as db,db:db.execute("UPDATE reservations SET allocated=allocated+1")
        self.host.ledger.set_engine_state('parent-reservation','uncertain');self.held();self.check(False)
        self.host.ledger.set_engine_state('parent-reservation','terminal',terminal_attested=True);self.held(False)

    def test_reserved_expiry_and_fence_receipt_drift(self):
        with closing(sqlite3.connect(self.host.ledger.path)) as db,db:db.execute('UPDATE reservations SET expires=0')
        self.held()

    def test_fence_keeps_the_parent_executable_past_its_deadline(self):
        past=lambda:datetime.now(timezone.utc).timestamp()+10**6
        real=self.host.ledger.clock;self.host.ledger.clock=past;self.held();self.host.ledger.clock=real
        self.host.ledger.begin_transfer('parent-reservation');self.host.ledger.clock=past;self.check();self.check(False)

    def test_no_accepted_clock_makes_a_fenced_parent_expired(self):
        self.host.ledger.begin_transfer('parent-reservation')
        for clock in (float(2**53-1),1e300):
            self.host.ledger.clock=lambda clock=clock:clock;self.check();self.check(False)

    def test_live_physical_binding_and_completion_are_not_configuration_authority(self):
        path=self.host.root/'gate'/'gate.json';original=path.read_text()
        for key,value in [('generation',2),('authorityId','00000000-0000-4000-8000-000000000000'),('nodeId','cluster')]:
            state=json.loads(original);state[key]=value;path.write_text(json.dumps(state))
            with self.subTest(key=key):self.held()
        for key,value in [('receiptId','b'*32),('completionDigest','0'*64)]:
            state=json.loads(original);state['reservation'][key]=value;path.write_text(json.dumps(state))
            with self.subTest(key=key):self.held()
        state=json.loads(original);state['reservation']=None;path.write_text(json.dumps(state));self.held()
        path.write_text(original);self.check()

    def test_ledger_settings_generation_and_retained_parent_budget_are_exact(self):
        with closing(sqlite3.connect(self.host.ledger.path)) as db:original=db.execute('SELECT settings FROM reservations').fetchone()[0]
        for key,value in [('generation',2),('environment','production'),('output_budget',199),('manifest_sha256','0'*64),('input_bytes',True)]:
            changed=json.loads(original);changed[key]=value
            with closing(sqlite3.connect(self.host.ledger.path)) as db,db:db.execute('UPDATE reservations SET settings=?',(json.dumps(changed),))
            with self.subTest(key=key):self.held()
        with closing(sqlite3.connect(self.host.ledger.path)) as db,db:db.execute('UPDATE reservations SET settings=?',(original,))
        self.host.ledger.set_engine_state('parent-reservation','engine_started')
        self.host.ledger.record_allocated('parent-reservation',self.parent['inputBytes']+20)
        self.check();self.check(False)

    def test_private_driver_binding_and_original_manifest_are_required(self):
        with closing(sqlite3.connect(self.driver)) as db:original=db.execute('SELECT binding FROM driver_binding').fetchone()[0]
        for key,value in [('origin','https://127.0.0.1:1'),('workerInstanceId','other'),('authorityId','0'*32),('gateGeneration',2),('outputBudget',199)]:
            changed=json.loads(original);changed[key]=value
            with closing(sqlite3.connect(self.driver)) as db,db:db.execute('UPDATE driver_binding SET binding=?',(json.dumps(changed),))
            with self.subTest(key=key):self.held()
        with closing(sqlite3.connect(self.driver)) as db,db:
            db.execute('UPDATE driver_binding SET binding=?',(original,));raw=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0])
            raw['claim']['submission']['manifest']['objects'][0]['sha256']='0'*64
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(raw),))
        self.held()

    def test_state_privacy_drift_is_not_silently_repaired(self):
        self.driver.chmod(0o640);self.held();self.assertEqual(self.driver.stat().st_mode&0o777,0o640)
        self.driver.chmod(0o600);self.check()
        self.host.root.chmod(0o750)
        try:self.held();self.assertEqual(self.host.root.stat().st_mode&0o777,0o750)
        finally:self.host.root.chmod(0o700)

    def test_transport_policy_refuses_plaintext_and_unverified_tls(self):
        with self.assertRaises(m.ParentGuardHeld):replace(self.policy,origin='http://127.0.0.1:1')
        with self.assertRaises(m.ParentGuardHeld):m.ClaimStateClient(self.policy,m.WorkerCredential('other-role',WORKER),ssl_context=self.context)
        self.context.minimum_version=ssl.TLSVersion.TLSv1_1
        self.held()

    def test_real_gate_lease_busy_and_released_owner(self):
        with NodeGate(self.host.policy.engine_id):self.held()
        self.owner.__exit__(None,None,None);self.held()

    def test_driver_corruption_and_readonly_wal_refused_without_sidecar(self):
        with closing(sqlite3.connect(self.driver)) as db,db:
            row=json.loads(db.execute('SELECT data FROM driver_jobs').fetchone()[0]);row['claim']['claimToken']='x'*40
            db.execute('UPDATE driver_jobs SET data=?',(json.dumps(row),))
        self.held()
        with closing(sqlite3.connect(self.driver)) as db:db.execute('PRAGMA journal_mode=WAL')
        for suffix in ('-wal','-shm'):
            path=Path(str(self.driver)+suffix)
            if path.exists():path.unlink()
        self.held();self.assertFalse(Path(str(self.driver)+'-shm').exists())

    def test_credentials_policy_and_process_identity_cannot_change(self):
        self.assertNotIn(WORKER,repr(self.client.credential));self.assertNotIn(WORKER,json.dumps(self.guard.binding))
        with self.assertRaises(m.ParentGuardHeld):self.client.credential._secret='new'
        self.context.check_hostname=False;self.held();self.context.check_hostname=True
        with patch.object(self.guard,'_pid',-1):self.held()
        with patch.object(self.client,'_open',lambda *_:None):self.held()

