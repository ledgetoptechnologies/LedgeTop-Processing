import io
import hashlib
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from processing.config import Config
from processing.maintenance import Conflict
from processing.app import make_handler
from processing.prototype import Queue
from processing.reference_delivery import DeliveryReceiver
from processing.reference_http import create_reference_http_server
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.viewer_service import ViewerService
from processing.viewer_tasks import TaskStore, VIEWER_PROFILE
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing import reference_protocol as protocol

TOKEN_A='a'*32;TOKEN_B='b'*32;TOKEN_MAIN='m'*32
ENV_POLICY={'cluster':('production','staging'),'main':('production','staging')}
TASK='22222222-2222-4222-8222-222222222222'
ROUTE='/ltds/worker/v1/viewer-claims'


@unittest.skipUnless(shutil.which('openssl'),'openssl required for loopback TLS')
class ViewerWorkerClaimTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.cert=self.root/'cert';self.key=self.root/'key'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=synthetic-loopback','-addext','subjectAltName=IP:127.0.0.1',
            '-addext','basicConstraints=critical,CA:TRUE','-keyout',str(self.key),'-out',str(self.cert)],
            check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
        self.tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);self.tls.load_cert_chain(self.cert,self.key)
        self.client=ssl.create_default_context(cafile=str(self.cert))
        self.server=None;self.thread=None;self.queue=None
        self.start_queue()
        self.store.initialize(TASK,name='synthetic',options=[],outputs=[],environment='production')
        self.store.upload(TASK,'photo.jpg',io.BytesIO(b'synthetic'),9,environment='production')
        self.store.commit(TASK,environment='production')
        self.job=self.store.task(TASK)['job_id']
        self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
        self.start_server()

    def start_queue(self):
        self.queue=Queue(self.root/'state',node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
        self.store=TaskStore(self.queue,self.root/'state'/'viewer-tasks')

    def start_server(self):
        workers={TOKEN_A:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a'),
                 TOKEN_B:dict(node_id='cluster',worker_id='worker-b',worker_instance_id='instance-b'),
                 TOKEN_MAIN:dict(node_id='main',worker_id='worker-main',worker_instance_id='instance-main')}
        reference=ReferenceStore(self.queue)
        self.api=ReferenceWorkerAPI(reference,DeliveryReceiver(reference),bearer_workers=workers,viewer_claims=self.claims)
        self.server=create_reference_http_server(self.api,ssl_context=self.tls)
        self.thread=threading.Thread(target=self.server.serve_forever,kwargs={'poll_interval':.05},daemon=True);self.thread.start()
        self.origin='https://127.0.0.1:'+str(self.server.server_port)

    def stop_server(self):
        if self.server is not None:
            self.server.shutdown();self.server.server_close();self.thread.join(5)
            self.assertFalse(self.thread.is_alive());self.server=None

    def tearDown(self):
        self.stop_server()
        if self.queue is not None:self.queue.close()
        self.tmp.cleanup()

    def request(self,request_id='request-1',*,token=TOKEN_A,environment='production',job=None):
        data=json.dumps(dict(jobId=job or self.job,claimRequestId=request_id,environment=environment)).encode()
        request=Request(self.origin+ROUTE,data=data,method='POST',headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        try:
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=3) as response:
                return response.status,json.load(response)
        except HTTPError as error:
            with error:return error.code,json.load(error)

    def inventory(self,cursor,claim_token,*,token=TOKEN_A,job=None):
        request=Request(self.origin+ROUTE+'/'+(job or self.job)+'/inventory/'+str(cursor),
            headers={'Authorization':'Bearer '+token,'X-Claim-Token':claim_token})
        try:
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=3) as response:
                return response.status,json.load(response)
        except HTTPError as error:
            with error:return error.code,json.load(error)

    def plan(self,claim_token,*,token=TOKEN_A):
        request=Request(self.origin+ROUTE+'/'+self.job+'/plan',headers={'Authorization':'Bearer '+token,'X-Claim-Token':claim_token})
        try:
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=3) as response:
                return response.status,json.load(response)
        except HTTPError as error:
            with error:return error.code,json.load(error)

    def input_object(self,object_id,claim_token,*,token=TOKEN_A,job=None):
        request=Request(self.origin+ROUTE+'/'+(job or self.job)+'/objects/'+object_id,
            headers={'Authorization':'Bearer '+token,'X-Claim-Token':claim_token})
        try:
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=3) as response:
                return response.status,response.read()
        except HTTPError as error:
            with error:return error.code,error.read()

    def attest(self,claim,*,token=TOKEN_A,budget=1000):
        binding=dict(schemaVersion=1,authorityId=claim['authorityId'],coordinatorOrigin='https://127.0.0.1',
            gateGeneration=1,gateReservationId='a'*32,nodeId=claim['nodeId'],workerId=claim['workerId'],engineId='engine-a',
            environment=claim['environment'],viewerTaskId=claim['viewerTaskId'],viewerGeneration=claim['viewerGeneration'],
            jobId=claim['jobId'],engineTaskId=claim['engineTaskId'],inputSetSha256=claim['inputInventorySha256'],
            pipelineSha256=claim['pipelineSha256'],outputBudgetBytes=budget)
        data=protocol.canonical_json(dict(binding=binding,slotIdentity=dict(root=[1,2,3],allocation=[1,4,3],metadata=[1,5,3])))
        request=Request(self.origin+ROUTE+'/'+claim['jobId']+'/enrollment',data=data,method='POST',
            headers={'Authorization':'Bearer '+token,'X-Claim-Token':claim['claimToken'],'Content-Type':'application/json'})
        try:
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=3) as response:
                return response.status,json.load(response)
        except HTTPError as error:
            with error:return error.code,json.load(error)

    def test_authenticated_claim_replay_after_restart_and_no_legacy_dispatch(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(claim['state'],'claimed_unenrolled')
        self.assertEqual(claim['viewerTaskId'],TASK);self.assertEqual(claim['nodeId'],'cluster')
        self.assertEqual(claim['imageCount'],1);self.assertEqual(claim['inputBytes'],9)
        self.assertNotIn('photo.jpg',json.dumps(claim));self.assertNotIn('synthetic',json.dumps(claim))
        self.assertEqual(self.request(),(200,claim))
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a: self.fail('probe forbidden'))
        service.tick();self.assertEqual(self.store.task(TASK)['phase'],'worker_claimed')
        self.stop_server();self.queue.close();self.queue=None
        self.start_queue();self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
        self.start_server()
        self.assertEqual(self.request(),(200,claim))
        self.assertEqual(self.queue.get(self.job)['status'],'held')

    def test_legacy_claim_without_object_count_fails_closed(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(claim['objectCount'],1)
        with self.queue._db() as db:
            row=db.execute('SELECT binding FROM viewer_worker_claims WHERE job_id=?',(self.job,)).fetchone()
            binding=json.loads(row['binding']);del binding['objectCount']
            db.execute('UPDATE viewer_worker_claims SET binding=? WHERE job_id=?',(protocol.canonical_json(binding).decode(),self.job))
        self.assertNotEqual(self.inventory(0,claim['claimToken'])[0],200)

    def test_inventory_requires_exact_principal_and_claim_token(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.attest(claim)[0],200)
        code,page=self.inventory(0,claim['claimToken']);self.assertEqual(code,200)
        self.assertEqual(page['inputInventorySha256'],claim['inputInventorySha256'])
        self.assertEqual(page['viewerGeneration'],claim['viewerGeneration'])
        self.assertEqual(len(page['entries']),1);self.assertIsNone(page['nextCursor'])
        entry=page['entries'][0]
        self.assertEqual(entry['name'],'photo.jpg');self.assertEqual(entry['bytes'],9)
        self.assertNotIn('path',json.dumps(page));self.assertEqual(entry['sha256'],hashlib.sha256(b'synthetic').hexdigest())
        self.assertEqual(self.inventory(0,claim['claimToken'],token='z'*32)[0],401)
        self.assertEqual(self.inventory(0,claim['claimToken'],token=TOKEN_B)[0],409)
        self.assertEqual(self.inventory(0,claim['claimToken'],token=TOKEN_MAIN)[0],409)
        self.assertEqual(self.inventory(0,'wrong')[0],409)
        self.assertEqual(self.inventory(1,claim['claimToken'])[0],409)

    def test_enrollment_is_required_and_idempotent_for_exact_principal(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.inventory(0,claim['claimToken'])[0],409)
        self.assertEqual(self.input_object('f'*64,claim['claimToken'])[0],409)
        self.assertEqual(self.attest(claim,token=TOKEN_B)[0],409)
        receipt=self.attest(claim);self.assertEqual(receipt[0],200)
        self.assertEqual(self.attest(claim),receipt)
        self.assertEqual(self.attest(claim,budget=1001)[0],409)
        self.assertEqual(self.inventory(0,claim['claimToken'])[0],200)

    def test_plan_requires_enrollment_and_remains_frozen(self):
        expected=dict(options=[dict(name='alpha',value='é'),dict(name='beta',value=2)],outputs=['pointcloud','orthophoto'])
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET options=?,outputs=? WHERE uuid=?',
                       (json.dumps(expected['options']),json.dumps(expected['outputs']),TASK))
        _,claim=self.request();token=claim['claimToken']
        self.assertEqual(self.plan(token)[0],409)
        self.assertEqual(self.attest(claim)[0],200)
        initial=self.plan(token);self.assertEqual(initial[0],200)
        self.assertEqual(initial[1]['plan'],expected)
        self.assertEqual(self.plan(token,token=TOKEN_B)[0],409)
        self.assertEqual(self.plan('wrong')[0],409)
        with self.queue._db() as db:db.execute('UPDATE viewer_tasks SET options=? WHERE uuid=?',('[{"name":"x","value":"y"}]',TASK))
        self.assertEqual(self.plan(token),initial)
        self.stop_server();self.queue.close();self.queue=None
        self.start_queue();self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
        self.start_server();self.assertEqual(self.plan(token),initial)
        with self.queue._db() as db:db.execute('UPDATE viewer_worker_plans SET plan=? WHERE job_id=?',('{"options":[],"outputs":[]}',self.job))
        self.assertEqual(self.plan(token)[0],409)
        with self.queue._db() as db:db.execute('DELETE FROM viewer_worker_plans WHERE job_id=?',(self.job,))
        self.assertEqual(self.plan(token)[0],409)

    def test_inventory_replay_after_restart_and_frozen_metadata(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.attest(claim)[0],200)
        initial=self.inventory(0,claim['claimToken']);self.assertEqual(initial[0],200)
        with self.queue._db() as db:
            db.execute("UPDATE viewer_files SET sha256=? WHERE uuid=?",('b'*64,TASK))
        # The pages come from the transactionally frozen metadata, not the
        # mutable task table; this is not proof the underlying bytes exist.
        self.assertEqual(self.inventory(0,claim['claimToken']),initial)
        self.assertEqual(self.request()[0],409)
        self.stop_server();self.queue.close();self.queue=None
        self.start_queue();self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
        self.start_server()
        self.assertEqual(self.inventory(0,claim['claimToken']),initial)

    def test_inventory_rejects_changed_job_environment(self):
        status,claim=self.request();self.assertEqual(status,200)
        with self.queue._db() as db:db.execute("UPDATE jobs SET environment='staging' WHERE id=?",(self.job,))
        self.assertEqual(self.inventory(0,claim['claimToken'])[0],409)

    def test_inventory_rejects_altered_frozen_entry(self):
        status,claim=self.request();self.assertEqual(status,200)
        with self.queue._db() as db:db.execute('UPDATE viewer_worker_inventory SET bytes=bytes+1 WHERE job_id=?',(self.job,))
        self.assertEqual(self.inventory(0,claim['claimToken'])[0],409)

    def test_claim_bound_source_stream_is_principal_and_object_scoped(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.attest(claim)[0],200)
        page=self.inventory(0,claim['claimToken'])[1];object_id=page['entries'][0]['objectId']
        self.assertEqual(self.input_object(object_id,claim['claimToken']),(200,b'synthetic'))
        self.assertEqual(self.input_object(object_id,claim['claimToken'],token='z'*32)[0],401)
        self.assertEqual(self.input_object(object_id,claim['claimToken'],token=TOKEN_B)[0],409)
        self.assertEqual(self.input_object(object_id,'wrong')[0],409)
        self.assertEqual(self.input_object('f'*64,claim['claimToken'])[0],409)

    def test_source_mutation_after_claim_refuses_stream(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.attest(claim)[0],200)
        object_id=self.inventory(0,claim['claimToken'])[1]['entries'][0]['objectId']
        path=Path(self.store.task(TASK)['files'][0]['path']);path.write_bytes(b'different')
        self.assertEqual(self.input_object(object_id,claim['claimToken'])[0],409)

    def test_source_stream_permits_are_bounded_and_released(self):
        status,claim=self.request();self.assertEqual(status,200)
        self.assertEqual(self.attest(claim)[0],200)
        object_id=self.inventory(0,claim['claimToken'])[1]['entries'][0]['objectId']
        identity=dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')
        one=self.claims.input_object(self.job,object_id,identity,claim['claimToken'])
        two=self.claims.input_object(self.job,object_id,identity,claim['claimToken'])
        try:
            with self.assertRaises(Conflict):self.claims.input_object(self.job,object_id,identity,claim['claimToken'])
        finally:one.stream.close();two.stream.close()
        third=self.claims.input_object(self.job,object_id,identity,claim['claimToken']);third.stream.close()

    def test_partial_inventory_schema_blocks_restart(self):
        self.stop_server()
        with self.queue._db() as db:db.execute('DROP TABLE viewer_worker_claims')
        self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    def test_enrollment_only_marker_blocks_legacy_claim_tick_and_startup(self):
        with self.queue._db() as db:
            db.execute('DROP TRIGGER viewer_worker_active_permit_hold')
            db.execute('DROP TABLE viewer_worker_fences')
            db.execute('DROP TABLE viewer_worker_permits')
            db.execute('DROP TABLE viewer_worker_result_metadata')
            db.execute('DROP TABLE viewer_worker_spool_config')
            db.execute('DROP TABLE viewer_worker_spool')
            db.execute('DROP TABLE viewer_worker_completions')
            db.execute('DROP TABLE viewer_worker_claims')
            db.execute('DROP TABLE viewer_worker_inventory')
            db.execute('DROP TABLE viewer_worker_plans')
            db.execute('DROP TABLE viewer_worker_grants')
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a:self.fail('probe forbidden'))
        service.tick()
        self.assertEqual(self.store.task(TASK)['phase'],'queued')
        self.assertEqual(self.queue.get(self.job)['status'],'queued')
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    def test_plan_only_marker_blocks_legacy_claim_tick_and_startup(self):
        with self.queue._db() as db:
            db.execute('DROP TRIGGER viewer_worker_active_permit_hold')
            db.execute('DROP TABLE viewer_worker_fences')
            db.execute('DROP TABLE viewer_worker_permits')
            db.execute('DROP TABLE viewer_worker_result_metadata')
            db.execute('DROP TABLE viewer_worker_spool_config')
            db.execute('DROP TABLE viewer_worker_spool')
            db.execute('DROP TABLE viewer_worker_completions')
            for table in ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_grants'):
                db.execute('DROP TABLE '+table)
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a:self.fail('probe forbidden'))
        service.tick();self.assertEqual(self.queue.get(self.job)['status'],'queued')
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    def test_grant_only_marker_blocks_legacy_claim_tick_and_startup(self):
        with self.queue._db() as db:
            db.execute('DROP TRIGGER viewer_worker_active_permit_hold')
            db.execute('DROP TABLE viewer_worker_fences')
            db.execute('DROP TABLE viewer_worker_permits')
            db.execute('DROP TABLE viewer_worker_result_metadata')
            db.execute('DROP TABLE viewer_worker_spool_config')
            db.execute('DROP TABLE viewer_worker_spool')
            db.execute('DROP TABLE viewer_worker_completions')
            for table in ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_plans'):
                db.execute('DROP TABLE '+table)
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a:self.fail('probe forbidden'))
        service.tick();self.assertEqual(self.queue.get(self.job)['status'],'queued')
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    def test_result_metadata_only_marker_blocks_legacy_paths(self):
        with self.queue._db() as db:
            db.execute('DROP TRIGGER viewer_worker_active_permit_hold')
            for table in ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_plans',
                          'viewer_worker_grants','viewer_worker_fences','viewer_worker_permits','viewer_worker_spool_config','viewer_worker_spool','viewer_worker_completions'):
                db.execute('DROP TABLE '+table)
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a:self.fail('probe forbidden'))
        service.tick();self.assertEqual(self.queue.get(self.job)['status'],'queued')
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    def test_completion_only_marker_blocks_legacy_paths(self):
        with self.queue._db() as db:
            db.execute('DROP TRIGGER viewer_worker_active_permit_hold')
            for table in ('viewer_worker_claims','viewer_worker_inventory','viewer_worker_enrollments','viewer_worker_plans',
                          'viewer_worker_grants','viewer_worker_fences','viewer_worker_permits','viewer_worker_spool_config',
                          'viewer_worker_spool','viewer_worker_result_metadata'):
                db.execute('DROP TABLE '+table)
        self.assertIsNone(self.queue.claim({'workerId':'bypass','nodeId':'cluster','profiles':[VIEWER_PROFILE]}))
        class Bomb:
            def __getattribute__(self,name):raise AssertionError('engine access forbidden')
        service=ViewerService(self.queue,Config('x'*32,'','',1000,100000,viewer_enabled=True),store=self.store,
                              engines={'cluster':Bomb(),'main':Bomb()},probe=lambda *a:self.fail('probe forbidden'))
        service.tick();self.assertEqual(self.queue.get(self.job)['status'],'queued')
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))

    @unittest.skipUnless(os.environ.get('LEDGETOP_INVENTORY_10K')=='1','bounded synthetic 10k opt-in')
    def test_ten_thousand_metadata_entries_are_paged_and_digest_matches(self):
        directory=Path(self.store.task(TASK)['directory']);sha=hashlib.sha256(b'x').hexdigest()
        values=[]
        for index in range(9999):
            name='fixture-%05d.jpg'%index;path='entry-%05d'%index
            (directory/path).write_bytes(b'x')
            values.append((TASK,name,name,path,1,sha,1))
        with self.queue._db() as db:
            db.executemany('INSERT INTO viewer_files(uuid,name_key,name,path,bytes,sha256,is_photo) VALUES(?,?,?,?,?,?,?)',values)
        status,claim=self.request(token=TOKEN_MAIN);self.assertEqual(status,200)
        self.assertEqual(self.attest(claim,token=TOKEN_MAIN)[0],200)
        digest=hashlib.sha256();digest.update(b'[');first=True;count=0;cursor=0
        while cursor is not None:
            code,page=self.inventory(cursor,claim['claimToken'],token=TOKEN_MAIN);self.assertEqual(code,200)
            self.assertLessEqual(len(page['entries']),100);self.assertEqual(page['start'],cursor)
            for item in page['entries']:
                self.assertEqual(item['ordinal'],count)
                entry={key:item[key] for key in ('name','bytes','sha256','isPhoto')}
                if not first:digest.update(b',')
                first=False;digest.update(protocol.canonical_json(entry));count+=1
            cursor=page['nextCursor']
        digest.update(b']');self.assertEqual(count,10000)
        self.assertEqual(digest.hexdigest(),claim['inputInventorySha256'])

    def test_wrong_identity_environment_node_and_request_fail(self):
        self.assertEqual(self.request(token='z'*32)[0],401)
        self.assertEqual(self.request(environment='staging')[0],409)
        self.assertEqual(self.request(token=TOKEN_MAIN)[0],409)
        first=self.request();self.assertEqual(first[0],200)
        self.assertEqual(self.request('different')[0],409)
        self.assertEqual(self.request(token=TOKEN_B)[0],409)
        self.assertEqual(self.request(),first)

    def test_authenticated_node_cannot_claim_environment_outside_policy(self):
        self.stop_server()
        self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,
            node_environments={'cluster':('staging',),'main':('production',)})
        self.start_server()
        self.assertEqual(self.request()[0],409)
        self.assertEqual(self.queue.get(self.job)['status'],'queued')

    def test_replay_rejects_changed_stored_options_or_claim_identity(self):
        self.assertEqual(self.request()[0],200)
        with self.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET options=? WHERE uuid=?",('[{\"name\":\"x\",\"value\":1}]',TASK))
        self.assertEqual(self.request()[0],409)
        self.assertEqual(self.queue.get(self.job)['status'],'running')

    def test_unrecognized_claim_schema_blocks_requests(self):
        with self.queue._db() as db:
            db.execute('CREATE TRIGGER unexpected_claim_trigger AFTER INSERT ON viewer_worker_claims BEGIN SELECT 1; END')
        self.assertEqual(self.request()[0],409)
        self.assertEqual(self.queue.get(self.job)['status'],'queued')

    def test_disabled_route_and_extra_gate_claim_do_not_authorize(self):
        reference=ReferenceStore(self.queue)
        api=ReferenceWorkerAPI(reference,DeliveryReceiver(reference),bearer_workers={
            TOKEN_A:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')})
        body=json.dumps(dict(jobId=self.job,claimRequestId='fixture',environment='production')).encode()
        self.assertEqual(api.handle('POST',ROUTE,{'Authorization':'Bearer '+TOKEN_A},body,protected_transport=True).status,404)
        self.assertEqual(self.api.handle('POST',ROUTE,{'Authorization':'Bearer '+TOKEN_A},body,protected_transport=False).status,503)
        extra=json.dumps(dict(jobId=self.job,claimRequestId='fixture',environment='production',gate='untrusted')).encode()
        self.assertEqual(self.api.handle('POST',ROUTE,{'Authorization':'Bearer '+TOKEN_A},extra,protected_transport=True).status,400)
        self.assertEqual(self.queue.get(self.job)['status'],'queued')

    def test_competing_authenticated_workers_get_one_claim(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(self.request,'request-'+str(i),token=token) for i,token in enumerate((TOKEN_A,TOKEN_B))]
            outcomes=[future.result(timeout=5) for future in futures]
        self.assertEqual(sorted(value[0] for value in outcomes),[200,409])
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_worker_claims').fetchone()[0],1)

    def test_missing_input_and_changed_plan_hold_without_claim(self):
        task=self.store.task(TASK);Path(task['files'][0]['path']).unlink()
        self.assertEqual(self.request()[0],409)
        self.assertEqual(self.queue.get(self.job)['status'],'queued')
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT count(*) FROM viewer_worker_claims').fetchone()[0],0)

    def test_corrupt_photo_classification_holds_without_claim(self):
        with self.queue._db() as db:db.execute('UPDATE viewer_files SET is_photo=2 WHERE uuid=?',(TASK,))
        self.assertEqual(self.request()[0],409)
        self.assertEqual(self.queue.get(self.job)['status'],'queued')

    def test_failed_transaction_cannot_expose_a_partial_claim(self):
        with self.queue._db() as db:
            db.execute("CREATE TRIGGER injected_failure BEFORE UPDATE ON viewer_tasks BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        self.assertEqual(self.request()[0],500)
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM viewer_worker_claims').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM viewer_worker_inventory').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT status FROM jobs WHERE id=?',(self.job,)).fetchone()[0],'queued')
            db.execute('DROP TRIGGER injected_failure')
        self.assertEqual(self.request()[0],200)

    def test_production_head_precedes_queued_staging(self):
        staging='33333333-3333-4333-8333-333333333333'
        self.store.initialize(staging,name='fixture staging',options=[],outputs=[],environment='staging')
        self.store.upload(staging,'photo.jpg',io.BytesIO(b'staging'),7,environment='staging')
        self.store.commit(staging,environment='staging')
        staging_job=self.store.task(staging)['job_id']
        self.assertEqual(self.request(environment='staging',job=staging_job)[0],409)
        self.assertEqual(self.request()[0],200)

    def test_process_killed_after_commit_before_tls_response_replays(self):
        self.stop_server();self.queue.close();self.queue=None
        ready=self.root/'child-ready';committed=self.root/'child-committed'
        code='''import sys,ssl,time
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_worker_claim import ViewerWorkerClaims
from processing.reference_store import ReferenceStore
from processing.reference_delivery import DeliveryReceiver
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_http import create_reference_http_server
root,cert,key,ready,committed=sys.argv[1:];root=Path(root)
q=Queue(root/'state',node_ids=('cluster','main'),viewer_enabled=True,reference_enabled=True)
t=TaskStore(q,root/'state'/'viewer-tasks');claims=ViewerWorkerClaims(q,t,local_max_photos=1000,local_max_input_bytes=100000,node_environments={'cluster':('production','staging'),'main':('production','staging')})
s=ReferenceStore(q);api=ReferenceWorkerAPI(s,DeliveryReceiver(s),bearer_workers={'a'*32:dict(node_id='cluster',worker_id='worker-a',worker_instance_id='instance-a')},viewer_claims=claims)
class Delay:
 def handle(self,*args,**kwargs):
  response=api.handle(*args,**kwargs)
  if response.status==200:Path(committed).write_text('committed');time.sleep(30)
  return response
tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
server=create_reference_http_server(Delay(),ssl_context=tls);Path(ready).write_text(str(server.server_port));server.serve_forever()
'''
        child=subprocess.Popen([sys.executable,'-c',code,str(self.root),str(self.cert),str(self.key),str(ready),str(committed)],
                               stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        def wait_file(path):
            deadline=time.monotonic()+5
            while not path.exists() and time.monotonic()<deadline:
                if child.poll() is not None:self.fail('child exited before barrier')
                time.sleep(.02)
            self.assertTrue(path.exists())
        def send():
            body=json.dumps(dict(jobId=self.job,claimRequestId='lost-response',environment='production')).encode()
            request=Request('https://127.0.0.1:'+ready.read_text()+ROUTE,data=body,method='POST',
                            headers={'Authorization':'Bearer '+TOKEN_A,'Content-Type':'application/json'})
            with build_opener(ProxyHandler({}),HTTPSHandler(context=self.client)).open(request,timeout=5) as response:
                return response.status
        try:
            wait_file(ready)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending=pool.submit(send);wait_file(committed);child.kill();child.wait(timeout=5)
                with self.assertRaises(Exception):pending.result(timeout=5)
        finally:
            if child.poll() is None:child.kill();child.wait(timeout=5)
        self.start_queue();self.claims=ViewerWorkerClaims(self.queue,self.store,local_max_photos=1000,local_max_input_bytes=100000,node_environments=ENV_POLICY)
        self.start_server()
        recovered=self.request('lost-response');self.assertEqual(recovered[0],200)
        self.assertEqual(self.request('lost-response'),recovered)
        self.assertEqual(self.queue.get(self.job)['status'],'held')
        self.assertEqual(recovered[1]['state'],'claimed_unenrolled')

    def test_persisted_claim_marker_prevents_unqualified_restart(self):
        self.stop_server();self.queue.close();self.queue=None
        with self.assertRaisesRegex(RuntimeError,'configuration_required'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state')))
        with self.assertRaisesRegex(RuntimeError,'schema_unqualified'):
            make_handler(Config('x'*32,'','',1000,100000,prototype_state_dir=str(self.root/'state'),viewer_worker_claim_enabled=True))
