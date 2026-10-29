"""Actual app/owned maintenance/Reader TLS/native HTTP; synthetic runtime data."""
import copy
from dataclasses import replace
from datetime import datetime,timezone
from http.server import ThreadingHTTPServer
import json
import os
import socket
import sqlite3
import threading
import time
from contextlib import closing
import unittest
import uuid
from urllib.request import Request,build_opener,ProxyHandler
from urllib.error import HTTPError
from unittest.mock import patch

from processing.app import make_handler
from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_runtime_versions import validate_version_receipt,validate_current_version,RuntimeVersionHeld
from tests import test_reference_routing as routing
from tests import test_reference_catalog_cache as catalog

OPERATOR='synthetic-runtime-operator-'+'x'*32
VIEWER='synthetic-runtime-viewer-'+'y'*32

def free_port():
    with socket.socket() as connection:connection.bind(('127.0.0.1',0));return connection.getsockname()[1]

@unittest.skipUnless(os.name=='posix','Linux real Queue/app/TLS qualification')
class RuntimeVersionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):routing.ReferenceRoutingTests.setUpClass()
    @classmethod
    def tearDownClass(cls):routing.ReferenceRoutingTests.doClassCleanups()
    def setUp(self):
        self.fixture=routing.ReferenceRoutingTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        f=self.fixture;f.queue.close();self.natives={};origins={}
        for node in ('cluster','main'):
            self.natives[node]=catalog.NativeCatalog();origins[node]=f.server(self.natives[node],False)
        self.path=f.root/'private-runtime.json'
        self.config=replace(f.config,prototype_state_dir=str(f.root/'queue'),reference_config_path=str(self.path),
            prototype_operator_token=OPERATOR,reference_main_fallback=True,local_node_url=origins['cluster'],main_node_url=origins['main'])
        value=dict(schemaVersion=1,listener=dict(host='127.0.0.1',port=free_port(),certificate=str(f.cert),privateKey=str(f.key),clientCA=str(f.cert)),
            viewerBearers=dict(staging=VIEWER),workers={n:dict(w,bearer='synthetic-runtime-'+n+'-'+'b'*32) for n,w in f.workers.items()},
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=1048576),routingBindings={n:dict(engineId=b.engine_id,gateGeneration=b.gate_generation,engineVersion=b.engine_version) for n,b in f.bindings.items()},
            catalogAdmission=dict(schemaVersion=1,maxAgeSeconds=60),runtimeVersionPublication=dict(schemaVersion=1))
        self.path.write_text(json.dumps(value));self.path.chmod(0o600)
        self.handler=None;self.server=None;self.thread=None
        self.open=build_opener(ProxyHandler({})).open
        self.start();self.addCleanup(self.stop)
    def start(self,port=0):
        self.handler=make_handler(self.config);self.queue=self.handler.prototype_queue
        self.runtime=self.handler.reference_listener;self.registry=self.handler.reference_runtime_versions
        self.server=ThreadingHTTPServer(('127.0.0.1',port),self.handler)
        self.server.daemon_threads=False;self.server.block_on_close=True
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.01});self.thread.start()
        self.origin='http://127.0.0.1:'+str(self.server.server_port)
    def stop(self):
        if self.server is not None:
            self.server.shutdown();self.server.server_close();self.thread.join(2);self.assertFalse(self.thread.is_alive());self.server=None
        if self.handler is not None:
            self.runtime.close();self.queue.close();self.handler=None
    def cold_reopen(self):
        port=self.server.server_port;self.stop();self.start(port)
    def request(self,method,path,body=None,*,token=OPERATOR,update_token=None):
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        if update_token is not None:headers['X-LTDS-Update-Token']=update_token
        request=Request(self.origin+path,method=method,headers=headers,data=p.canonical_json(body) if body is not None else None)
        try:response=self.open(request,timeout=10)
        except HTTPError as error:response=error
        with response:return response.status,json.loads(response.read())
    def cap(self):return self.runtime.api.handle('GET',p.PREFIX+'/capabilities',{'Authorization':'Bearer '+VIEWER},protected_transport=True)
    def route(self,node='cluster'):return '/v1/prototype/nodes/'+node+'/runtime-version'
    def prepare(self,new_version='synthetic-2',image='a',node='cluster'):
        current=self.registry.current(node)
        now=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        state=self.queue.get_node(node);owned_id=str(uuid.uuid4())
        receipt=self.queue.schedule_node_owned(node,dict(schemaVersion=1,requestId=owned_id,authorityId=self.queue.authority_id,
            expectedRevision=state['revision'],drainAt=now,notBefore=now,afterJobId=None,targetImage='sha256:'+image*64))
        begin=self.queue.begin_node_update(node,dict(expectedRevision=receipt['revision'],planId=receipt['planId']))
        payload=dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,planId=receipt['planId'],expectedRevision=begin['node']['revision'],
            ownedScheduleRequestId=owned_id,expectedVersion=current['engineVersion'],newVersion=new_version,targetImage='sha256:'+image*64,
            engineId=current['engineId'],gateGeneration=current['gateGeneration'])
        return payload,begin['updateToken']
    def observe_new(self,version,node='cluster'):
        self.natives[node].info['engineVersion']=version
        self.fixture.endpoints[node][0].body['engineVersion']=version
    def publish(self,payload,token,node='cluster'):
        return self.request('POST',self.route(node),payload,update_token=token)
    def finish(self,payload,token,node='cluster'):
        self.queue.finish_node_update(node,dict(planId=payload['planId'],updateToken=token,status='succeeded',result=dict(healthy=True)))

    def test_two_upgrades_same_version_image_and_aba_survive_restart_with_immutable_job_evidence(self):
        self.assertEqual(self.cap().status,200)
        raw=copy.deepcopy(self.fixture.fixture['submission']);cap=self.cap().body
        raw['capabilityFingerprint']=cap['capabilityFingerprint'];raw['pipeline']['odm']['optionsFingerprint']=cap['engines']['odm']['optionsFingerprint'];raw['pipeline']['odm']['options']={'future-quality':1}
        response=self.runtime.api.handle('POST',p.PREFIX+'/jobs',{'Authorization':'Bearer '+VIEWER},p.canonical_json(raw),protected_transport=True)
        self.assertEqual(response.status,201);job=response.body['jobId']
        with self.queue._db() as db:original=tuple(db.execute('SELECT * FROM reference_option_evidence WHERE job_id=?',(job,)).fetchone())
        for epoch,(version,image) in enumerate((('synthetic-2','a'),('synthetic-2','b'),('synthetic-1','c')),1):
            request,token=self.prepare(version,image);self.observe_new(version)
            code,body=self.publish(request,token);self.assertEqual((code,body.get('error')),(200,None))
            receipt=body['receipt'];validate_version_receipt(receipt,request,'cluster');self.assertEqual(receipt['epoch'],epoch)
            current=self.registry.current('cluster');validate_current_version(current,request,'cluster',receipt)
            with self.queue._db() as db:
                self.assertEqual(db.execute('SELECT invalidated FROM reference_catalog_state').fetchone()[0],1)
                self.assertEqual(tuple(db.execute('SELECT * FROM reference_option_evidence WHERE job_id=?',(job,)).fetchone()),original)
                self.runtime.store.catalog_admission.read_evidence(db,job)
            self.finish(request,token)
            self.assertEqual(self.cap().status,200)
            self.cold_reopen()
            self.assertEqual(self.cap().status,200)
            self.assertEqual(self.registry.current('cluster')['nodeEpoch'],epoch)
            self.assertEqual(self.runtime.settings.routing_bindings['cluster'].engine_version,'synthetic-1')
            self.assertEqual(self.runtime.store.routing.sample().nodes['cluster'].state,'ready')

    def test_original_update_token_owned_plan_and_fresh_readback_required(self):
        self.cap();request,token=self.prepare()
        for field,value in (('ownedScheduleRequestId',str(uuid.uuid4())),('expectedRevision',request['expectedRevision']+1),('targetImage','sha256:'+'b'*64),('authorityId',str(uuid.uuid4())),('engineId','other-engine')):
            bad=dict(request,**{field:value});self.assertEqual(self.publish(bad,token)[0],409)
        self.assertEqual(self.publish(request,'wrong')[0],409)
        self.assertNotEqual(self.publish(request,token)[0],200)
        self.observe_new(request['newVersion']);self.fixture.endpoints['cluster'][0].body['pendingJobs']=1
        self.assertNotEqual(self.publish(request,token)[0],200)
        self.fixture.endpoints['cluster'][0].body['pendingJobs']=0
        self.assertEqual(self.publish(request,token)[0],200)

    def test_historical_receipt_replay_after_later_hold_never_reauthorizes(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion'])
        code,body=self.publish(request,token);self.assertEqual(code,200)
        self.finish(request,token);self.prepare('synthetic-3','b');node=self.queue.get_node('cluster')
        self.queue.hold_node('cluster',dict(expectedRevision=node['revision'],planId=node['planId'],note='synthetic operator hold'))
        before=self.queue.path.read_bytes()
        self.assertEqual(self.publish(request,token),(200,body))
        self.assertEqual(self.request('GET',self.route()+'/receipts/'+request['requestId']),(200,body))
        self.assertEqual(self.queue.path.read_bytes(),before);self.assertEqual(self.queue.get_node('cluster')['state'],'held')
        changed=dict(request,targetImage='sha256:'+'b'*64)
        self.assertEqual(self.publish(changed,token)[0],409)

    def test_snapshot_epoch_fences_route_sample_and_late_cache_observation(self):
        self.cap();old=self.runtime.store.routing.sample();request,token=self.prepare();self.observe_new(request['newVersion'])
        self.assertEqual(self.publish(request,token)[0],200);self.finish(request,token)
        submission=dict(pipeline=dict(treedar=dict(requested=False)),manifest=dict(inputCounts=dict(images=1),totalBytes=3))
        with self.queue._db() as db,self.assertRaisesRegex(ValueError,'runtime_epoch_changed'):
            self.runtime.store.routing.choose(submission,{'cluster':'active','main':'active'},old,local_max_photos=1000,local_max_input_bytes=1000,main_node_id='main',db=db)

    def test_actual_operator_routes_reject_wrong_role_and_token_header_duplicates(self):
        self.assertEqual(self.request('GET',self.route(),token=self.config.api_token)[0],401)
        request,token=self.prepare();self.observe_new(request['newVersion'])
        self.assertEqual(self.publish(request,token)[0],200)
        wire=('POST '+self.route()+' HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer '+OPERATOR+'\r\nContent-Type: application/json\r\nX-LTDS-Update-Token: '+token+'\r\nx-ltds-update-token: '+token+'\r\nContent-Length: '+str(len(p.canonical_json(request)))+'\r\nConnection: close\r\n\r\n').encode()+p.canonical_json(request)
        with socket.create_connection(('127.0.0.1',self.server.server_port),timeout=3) as connection:
            connection.sendall(wire);answer=connection.recv(4096)
        self.assertIn(b' 401 ',answer)

    def test_lost_publication_reply_then_coordinator_restart_retains_receipt_but_holds_node(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion'])
        original=self.registry.publish;committed=[]
        def lose(*args,**kwargs):
            receipt=original(*args,**kwargs);committed.append(receipt);raise RuntimeError('synthetic reply loss after FULL commit')
        with patch.object(self.registry,'publish',lose):self.assertEqual(self.publish(request,token)[0],503)
        count=sum(len(n.calls) for n in self.natives.values())
        self.assertEqual(self.request('GET',self.route()+'/receipts/'+request['requestId']),(200,{'receipt':committed[0]}))
        self.assertEqual(sum(len(n.calls) for n in self.natives.values()),count)
        self.cold_reopen();self.assertEqual(self.queue.get_node('cluster')['state'],'held')
        before=self.queue.path.read_bytes()
        self.assertEqual(self.publish(request,token),(200,{'receipt':committed[0]}))
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.assertEqual(self.queue.get_node('cluster')['state'],'held')

    def test_busy_readback_waits_outside_all_queue_transactions_before_qualified_idle(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion'])
        worker=self.fixture.endpoints['cluster'][0];original=dict(worker.body)
        worker.body.update(workState='busy',retainedJobs=None,pendingJobs=None,awaitingAcceptance=None)
        entered=threading.Event();finished=threading.Event();errors=[]
        def release_worker():
            try:
                time.sleep(.08)
                # A canonical driver can reach the actual Queue while the
                # registry is waiting on its private health, without deadlock.
                with self.queue._db() as db:db.execute('SELECT COUNT(*) FROM jobs').fetchone()
                entered.set();worker.body=original
            except Exception as error:errors.append(type(error).__name__)
            finally:finished.set()
        thread=threading.Thread(target=release_worker);thread.start()
        try:self.assertEqual(self.publish(request,token)[0],200)
        finally:thread.join(3)
        self.assertTrue(entered.is_set());self.assertTrue(finished.is_set());self.assertEqual(errors,[])

    def test_late_catalog_observation_cannot_overwrite_atomic_version_publication(self):
        self.cap();request,token=self.prepare();cache=self.runtime.store.catalog_admission;discover=cache._discover
        receipts=[]
        def late(bindings):
            result=discover(bindings)
            self.observe_new(request['newVersion']);receipts.append(self.registry.publish('cluster',request,update_token=token))
            return result
        with patch.object(cache,'_discover',late):self.assertEqual(self.cap().status,503)
        self.assertEqual(receipts[0]['epoch'],1)
        with self.queue._db() as db:
            row=db.execute('SELECT invalidated,pending FROM reference_catalog_state').fetchone();self.assertEqual(tuple(row),(1,'0'*32))
        self.finish(request,token);self.assertEqual(self.cap().status,200)

    def test_same_version_image_and_aba_reject_old_route_epochs_even_matching_version_strings(self):
        self.cap();sample=self.runtime.store.routing.sample()
        for version,image in (('synthetic-2','a'),('synthetic-1','b')):
            request,token=self.prepare(version,image);self.observe_new(version)
            self.assertEqual(self.publish(request,token)[0],200);self.finish(request,token);self.cap()
        current=self.registry.current('cluster');self.assertEqual(current['engineVersion'],'synthetic-1');self.assertEqual(current['epoch'],2)
        raw=dict(pipeline=dict(treedar=dict(requested=False)),manifest=dict(inputCounts=dict(images=1),totalBytes=3))
        with self.queue._db() as db,self.assertRaisesRegex(ValueError,'runtime_epoch_changed'):
            self.runtime.store.routing.choose(raw,{'cluster':'active','main':'active'},sample,local_max_photos=1000,local_max_input_bytes=1000,main_node_id='main',db=db)

    def test_default_off_cold_mode_and_old_compatible_writer_are_fenced_without_changes(self):
        self.cap();self.stop();before=self.queue.path.read_bytes()
        with self.assertRaisesRegex(RuntimeError,'runtime publication ABI'):
            Queue(self.fixture.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        with closing(sqlite3.connect(self.queue.path)) as db:
            db.create_function('reference_catalog_writer_v1',0,lambda:1)
            with self.assertRaisesRegex(sqlite3.OperationalError,'reference_runtime_writer_v1'):
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('old','old','staging','reference-pipeline-v1','queued')")
        self.assertEqual(self.queue.path.read_bytes(),before)

    def test_receipt_quota_failure_rolls_back_cache_invalidation_and_current_binding(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion'])
        cache=self.runtime.store.catalog_admission;before=self.queue.path.read_bytes()
        with patch.object(cache.store,'_quota',side_effect=RuntimeError('synthetic receipt storage failure')):
            self.assertEqual(self.publish(request,token)[0],503)
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.assertEqual(self.registry.current('cluster')['epoch'],0)
        self.assertIsNone(self.registry.lookup('cluster',request['requestId']))

    def test_other_node_progress_keeps_original_node_receipt_and_current_proof_valid(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion']);first=self.publish(request,token)[1]['receipt'];self.finish(request,token)
        main,main_token=self.prepare('synthetic-main-2','b','main');self.observe_new(main['newVersion'],'main')
        self.assertEqual(self.publish(main,main_token,'main')[0],200)
        validate_current_version(self.registry.current('cluster'),request,'cluster',first)
        self.assertEqual(self.registry.current('cluster')['epoch'],2);self.assertEqual(self.registry.current('cluster')['nodeEpoch'],1)

    def test_expired_actual_readback_deadline_cannot_publish(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion']);probe=self.registry._probe
        def expired(*args):probe(*args);return time.monotonic()-1
        before=self.queue.path.read_bytes()
        with patch.object(self.registry,'_probe',expired):self.assertEqual(self.publish(request,token)[0],503)
        self.assertEqual(self.queue.path.read_bytes(),before);self.assertEqual(self.registry.current('cluster')['epoch'],0)

    def test_receipt_private_field_corruption_refuses_read_and_cold_factory_without_repair(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion']);self.assertEqual(self.publish(request,token)[0],200);self.finish(request,token)
        with self.queue._db() as db:db.execute('UPDATE reference_runtime_version_receipts SET update_token_sha=?',('0'*64,))
        before=self.queue.path.read_bytes()
        self.assertEqual(self.request('GET',self.route())[0],503)
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.stop()
        with self.assertRaises(ValueError):make_handler(self.config)
        self.assertEqual(self.queue.path.read_bytes(),before)

    def test_bounded_receipt_capacity_holds_without_new_rows_or_cache_changes(self):
        self.cap();request,token=self.prepare();self.observe_new(request['newVersion']);self.assertEqual(self.publish(request,token)[0],200);self.finish(request,token)
        next_request,next_token=self.prepare('synthetic-3','b');self.observe_new(next_request['newVersion']);before=self.queue.path.read_bytes()
        with patch('processing.reference_runtime_versions.MAX_RECEIPTS',1):self.assertEqual(self.publish(next_request,next_token)[0],503)
        self.assertEqual(self.queue.path.read_bytes(),before)
        self.assertIsNone(self.registry.lookup('cluster',next_request['requestId']))

    def test_private_policy_closed_shapes_reject_before_touching_owned_queue(self):
        original=self.path.read_bytes();before=self.queue.path.read_bytes()
        for change in ('boolean_version','extra','no_catalog','no_operator'):
            with self.subTest(change=change):
                value=json.loads(original);config=self.config
                if change=='boolean_version':value['runtimeVersionPublication']['schemaVersion']=True
                elif change=='extra':value['runtimeVersionPublication']['enabled']=True
                elif change=='no_catalog':del value['catalogAdmission']
                else:config=replace(config,prototype_operator_token='')
                self.path.write_text(json.dumps(value))
                try:
                    with self.assertRaises(ValueError):make_handler(config)
                    self.assertEqual(self.queue.path.read_bytes(),before)
                finally:self.path.write_bytes(original)
