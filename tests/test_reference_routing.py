"""Synthetic metadata, real native HTTP/private worker TLS and atomic Queue/API."""
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import ssl
import socket
import time
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid
from urllib.request import Request,build_opener,ProxyHandler,HTTPSHandler
from urllib.error import HTTPError

from processing.config import Config,from_environment
from processing.reference_routing import ReferenceRouting,RoutingNodeBinding,RoutingHeld,HEALTH_PATH
from processing.reference_http import create_reference_http_server
from processing.reference_api import Response
from processing.reference_store import ReferenceStore
from processing.reference_worker_api import ReferenceWorkerAPI,PREFIX
from processing.reference_delivery import DeliveryReceiver
from processing.prototype import Queue,PROFILE as LEGACY
from processing import reference_protocol as p


AUTHORITY='11111111-1111-4111-8111-111111111111'

def health(node):
    return dict(schemaVersion=1,profile='odm_only',scope='service_liveness',nodeId=node,identityScope='configured_service',authorityId=AUTHORITY,workerId=node+'-worker',workerInstanceId=node+'-instance',engineId='engine-'+node,gateGeneration=1,engineVersion='synthetic-1',
        state='ready',reason=None,driverAlive=True,listenerAlive=True,workState='empty',retainedJobs=0,pendingJobs=0,awaitingAcceptance=0)

class Endpoint:
    def __init__(self,node,reader,native=False):
        self.node,self.reader,self.native=node,reader,native;self.code=200
        self.body=health(node) if not native else dict(version='2.2.3',engine='ODM',engineVersion='synthetic-1',maxImages=None,taskQueueCount=0)
        self.calls=[]
    def handle(self,method,path,headers,body=b'',**kwargs):
        self.calls.append((method,path,dict(headers)))
        if method!='GET' or path!=('/info' if self.native else HEALTH_PATH):return Response(404,{'error':'not_found'})
        if not self.native and headers.get('Authorization')!='Bearer '+self.reader:return Response(401,{'error':'unauthorized'})
        return Response(self.code,self.body)

@unittest.skipUnless(os.name=='posix','Linux owned Queue/private TLS qualification')
class ReferenceRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.certs=tempfile.TemporaryDirectory();cls.addClassCleanup(cls.certs.cleanup)
        root=Path(cls.certs.name);cls.cert=root/'cert';cls.key=root/'key'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=synthetic-routing','-addext','subjectAltName=IP:127.0.0.1','-keyout',str(cls.key),'-out',str(cls.cert)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

    def server(self,api,tls):
        context=None
        if tls:
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
        server=create_reference_http_server(api,ssl_context=context)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        self.servers.append((server,thread))
        return ('https' if tls else 'http')+'://127.0.0.1:'+str(server.server_port)

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.servers=[];self.addCleanup(self.stop_servers)
        self.workers={};self.endpoints={};origins={};self.bindings={node:RoutingNodeBinding('engine-'+node,1,'synthetic-1') for node in ('cluster','main')}
        self.tls=ssl.create_default_context(cafile=str(self.cert))
        for node in ('cluster','main'):
            reader='synthetic-reader-'+node+'-'+'x'*24
            endpoint=Endpoint(node,reader);native=Endpoint(node,reader,True)
            self.endpoints[node]=(endpoint,native)
            worker_origin=self.server(endpoint,True);origins[node]=self.server(native,False)
            self.workers[node]=dict(resultOrigin=worker_origin,resultBearer=reader,workerId=node+'-worker',instanceId=node+'-instance')
        self.config=Config('synthetic-api-'+'x'*32,origins['cluster'],origins['main'],1000,20*1024**3,prototype_node_ids=('cluster','main'),reference_main_fallback=True)
        self.routing=ReferenceRouting(self.config,self.workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=self.tls,timeout=.3)
        self.queue=Queue(self.root/'queue',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',));self.addCleanup(self.queue.close)
        with self.queue._db() as db:db.execute('UPDATE coordinator_identity SET authority_id=?',(AUTHORITY,))
        self.queue.authority_id=AUTHORITY
        self.store=ReferenceStore(self.queue,routing=self.routing)
        identities={node+'-bearer-000000000000':dict(node_id=node,worker_id=node+'-worker',worker_instance_id=node+'-instance') for node in ('cluster','main')}
        self.api=ReferenceWorkerAPI(self.store,DeliveryReceiver(self.store),bearer_workers=identities)
        self.origin=self.server(self.api,True)
        self.open=build_opener(ProxyHandler({}),HTTPSHandler(context=self.tls)).open
        self.fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())

    def stop_servers(self):
        for server,thread in self.servers:
            if thread.is_alive():server.shutdown();thread.join(2)
            server.server_close()

    def submit(self,*,count=1,size=3,environment='staging',tree=False):
        raw=copy.deepcopy(self.fixture['submission']);raw['submissionId']=str(uuid.uuid4())
        template=raw['manifest']['objects'][0]
        raw['manifest']['objects']=[dict(template,id='input-'+str(i),name='image-'+str(i)+'.jpg',byteSize=size if i==0 else 1) for i in range(count)]
        raw['manifest']['totalBytes']=size+count-1;raw['manifest']['objectCount']=count;raw['manifest']['inputCounts']=dict(images=count,lidarObjects=0,points=0)
        if tree:
            raw['manifest']['objects'][0].update(kind='lidar',name='synthetic.las',mediaType='application/octet-stream')
            raw['manifest']['inputCounts']=dict(images=0,lidarObjects=1,points=0)
            raw['pipeline']['treedar'].update(requested=True,inputObjectIds=['input-0'],provenanceReceiptId='synthetic-proof')
            raw['pipeline']['odm']['requested']=False
        raw['manifest']['sha256']=p.manifest_sha256(raw['manifest'])
        submission=p.validate_submission(raw,environment=environment,capability_fingerprint=raw['capabilityFingerprint'],available_engines=('odm','treedar'))
        return self.store.submit(submission)[0]

    def request(self,node,method,path,body=None):
        request=Request(self.origin+PREFIX+path,method=method,headers={'Authorization':'Bearer '+node+'-bearer-000000000000','Content-Type':'application/json'},data=None if body is None else json.dumps(body).encode())
        try:response=self.open(request,timeout=5)
        except HTTPError as error:response=error
        with response:return response.status,json.loads(response.read())

    def claim(self,node,job):
        return self.request(node,'POST','/claim',dict(jobId=job['jobId'],claimRequestId=node+'-request-'+job['jobId']))

    def test_concurrent_real_tls_main_first_cannot_steal_small_job(self):
        job=self.submit()
        self.assertIsNone(self.request('main','GET','/next')[1]['job'])
        with ThreadPoolExecutor(max_workers=2) as pool:answers=list(pool.map(lambda n:self.claim(n,job),('main','cluster')))
        self.assertEqual(answers[0][0],409);self.assertEqual(answers[1][0],200)
        self.assertEqual(self.store.get(job['jobId'])['nodeId'],'cluster')
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],1)

    def test_daemon_offline_native_still_up_falls_back_to_main(self):
        job=self.submit();origin=self.workers['cluster']['resultOrigin']
        port=int(origin.rsplit(':',1)[1])
        for server,thread in self.servers:
            if server.server_port==port:server.shutdown();thread.join(2);server.server_close()
        self.assertEqual(self.request('main','GET','/next')[1]['job']['jobId'],job['jobId'])
        self.assertEqual(self.claim('main',job)[0],200)
        code,info=self.routing._json(self.config.local_node_url+'/info',{'Accept':'application/json'})
        self.assertEqual(code,200);self.assertEqual(info['taskQueueCount'],0)

    def test_native_offline_while_worker_up_falls_back(self):
        job=self.submit();port=int(self.config.local_node_url.rsplit(':',1)[1])
        for server,thread in self.servers:
            if server.server_port==port:server.shutdown();thread.join(2);server.server_close()
        self.assertEqual(self.routing._worker('cluster',self.routing._nodes['cluster']),'ready')
        self.assertEqual(self.claim('main',job)[0],200)

    def test_cluster_maintenance_falls_back_but_default_disabled_waits(self):
        job=self.submit()
        with self.queue._db() as db:db.execute("UPDATE nodes SET state='held' WHERE id='cluster'")
        disabled=ReferenceStore(self.queue)
        self.assertIsNone(disabled.claim(job['jobId'],node_id='main',worker_id='main-worker',worker_instance_id='main-instance',claim_request_id='default-off'))
        self.assertEqual(self.claim('main',job)[0],200)

    def test_busy_unknown_auth_decode_and_legacy_health_never_overflow(self):
        job=self.submit();worker,native=self.endpoints['cluster']
        cases=[(200,dict(health('cluster'),workState='busy',retainedJobs=None,pendingJobs=None,awaitingAcceptance=None)),
            (200,dict(health('cluster'),workState='pending_work',retainedJobs=1,pendingJobs=1)),
            (401,{'error':'unauthorized'}),(404,{'error':'legacy'}),(503,{'error':'worker_health_unavailable'}),
            (200,dict(health('cluster'),extra=True)),(200,dict(health('main'))),(200,dict(health('cluster'),retainedJobs=True))]
        for code,body in cases:
            with self.subTest(code=code,body=body):
                worker.code,worker.body=code,body
                self.assertEqual(self.claim('main',job)[0],409)
        worker.code,worker.body=200,health('cluster');native.body['taskQueueCount']=1
        self.assertEqual(self.claim('main',job)[0],409)
        self.assertEqual(self.store.get(job['jobId'])['status'],'queued')

    def test_maintenance_does_not_override_auth_or_busy_uncertainty(self):
        job=self.submit()
        with self.queue._db() as db:db.execute("UPDATE nodes SET state='held' WHERE id='cluster'")
        worker,_=self.endpoints['cluster'];worker.code=401;worker.body={'error':'unauthorized'}
        self.assertEqual(self.claim('main',job)[0],409)
        worker.code=200;worker.body=dict(health('cluster'),workState='busy',retainedJobs=None,pendingJobs=None,awaitingAcceptance=None)
        self.assertEqual(self.claim('main',job)[0],409)

    def test_unavailable_worker_cannot_mask_native_busy_unknown_auth_or_version(self):
        job=self.submit();worker,native=self.endpoints['cluster']
        worker.code=503;worker.body=dict(health('cluster'),state='failed',driverAlive=False,reason='synthetic_failure')
        good=copy.deepcopy(native.body)
        cases=[(200,dict(good,taskQueueCount=1)),(200,dict(good,taskQueueCount=True)),
            (200,dict(good,engineVersion='changed-version')),(401,{'error':'unauthorized'}),
            (503,{'error':'native_unavailable'}),(200,dict(good,maxImages=False))]
        for code,body in cases:
            with self.subTest(code=code,body=body):
                native.code,native.body=code,body
                self.assertIsNone(self.request('main','GET','/next')[1]['job'])
                self.assertEqual(self.claim('main',job)[0],409)
                self.assertEqual(self.store.get(job['jobId'])['status'],'queued')
        self.assertGreater(len(native.calls),len(cases))
        native.code,native.body=200,good
        self.assertEqual(self.claim('main',job)[0],200)

    def test_native_unavailable_cannot_mask_unknown_or_busy_worker(self):
        job=self.submit();worker,native=self.endpoints['cluster']
        port=int(self.config.local_node_url.rsplit(':',1)[1])
        for server,thread in self.servers:
            if server.server_port==port:server.shutdown();thread.join(2);server.server_close()
        for code,body in ((401,{'error':'unauthorized'}),(200,dict(health('cluster'),engineId='foreign-engine')),
                (200,dict(health('cluster'),workState='busy',retainedJobs=None,pendingJobs=None,awaitingAcceptance=None))):
            with self.subTest(code=code,body=body):
                worker.code,worker.body=code,body
                self.assertEqual(self.claim('main',job)[0],409)
        self.assertEqual(self.store.get(job['jobId'])['status'],'queued')

    def test_auth_credential_and_untrusted_tls_are_unknown_not_offline(self):
        job=self.submit();bad=copy.deepcopy(self.workers);bad['cluster']['resultBearer']='wrong-reader-secret-000000'
        for context,workers in ((self.tls,bad),(ssl.create_default_context(),self.workers)):
            routing=ReferenceRouting(self.config,workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=context,timeout=.3)
            store=ReferenceStore(self.queue,routing=routing)
            self.api.store=store
            self.assertEqual(self.claim('main',job)[0],409)
        self.assertEqual(self.store.get(job['jobId'])['status'],'queued')

    def test_configured_identity_mismatch_and_native_version_change_hold(self):
        job=self.submit();worker,native=self.endpoints['cluster']
        changes={'identityScope':'unverified','authorityId':'22222222-2222-4222-8222-222222222222',
            'workerId':'foreign-worker','workerInstanceId':'foreign-instance','engineId':'foreign-engine',
            'gateGeneration':2,'engineVersion':'foreign-version'}
        for key,value in changes.items():
            with self.subTest(key=key):
                worker.body=dict(health('cluster'),**{key:value})
                self.assertEqual(self.claim('main',job)[0],409)
        worker.body=health('cluster');native.body['engineVersion']='changed-version'
        self.assertEqual(self.claim('main',job)[0],409)
        self.assertEqual(self.store.get(job['jobId'])['status'],'queued')

    def test_advisory_fallback_is_rechecked_at_claim_after_cluster_recovers(self):
        job=self.submit();worker,_=self.endpoints['cluster'];worker.code=503;worker.body=dict(health('cluster'),state='failed',driverAlive=False,reason='synthetic_failure')
        self.assertEqual(self.request('main','GET','/next')[1]['job']['jobId'],job['jobId'])
        worker.code,worker.body=200,health('cluster')
        self.assertEqual(self.claim('main',job)[0],409);self.assertEqual(self.claim('cluster',job)[0],200)

    def test_production_head_cross_node_and_global_nonpreemption(self):
        staging=self.submit(count=1001);production=self.submit(environment='production')
        self.assertIsNone(self.request('main','GET','/next')[1]['job'])
        self.assertEqual(self.claim('main',staging)[0],409)
        self.assertEqual(self.claim('cluster',production)[0],200)
        later=self.submit(environment='production')
        self.assertEqual(self.claim('main',staging)[0],409);self.assertEqual(self.claim('cluster',later)[0],409)
        self.assertEqual(self.store.get(staging['jobId'])['status'],'queued')

    def test_active_and_held_treedar_occupy_same_global_slot(self):
        tree=self.submit(tree=True);self.assertEqual(self.claim('cluster',tree)[0],409);self.assertEqual(self.claim('main',tree)[0],200)
        small=self.submit(environment='production')
        self.assertEqual(self.claim('cluster',small)[0],409)
        with self.queue._db() as db:db.execute("UPDATE jobs SET status='held',held_reason='synthetic_hold' WHERE id=?",(tree['jobId'],))
        self.assertEqual(self.claim('cluster',small)[0],409)
        self.assertIsNone(self.queue.claim(dict(workerId='legacy',profiles=[LEGACY],nodeId='cluster')))

    def test_original_claim_replay_survives_routing_change(self):
        job=self.submit();status,claim=self.claim('cluster',job);self.assertEqual(status,200)
        self.endpoints['cluster'][0].code=401
        status,replay=self.claim('cluster',job);self.assertEqual(status,200);self.assertEqual(replay['claimToken'],claim['claimToken'])

    def test_photo_and_byte_boundaries_and_native_limit(self):
        sample=self.routing.sample();states=dict(cluster='active',main='active')
        for count,size,expected in ((1000,20*1024**3,'cluster'),(1001,3,'main'),(1,20*1024**3+1,'main')):
            raw=dict(pipeline=dict(treedar=dict(requested=False)),manifest=dict(inputCounts=dict(images=count),totalBytes=size))
            self.assertEqual(self.routing.choose(raw,states,sample,local_max_photos=1000,local_max_input_bytes=20*1024**3,main_node_id='main'),expected)
        self.endpoints['cluster'][1].body['maxImages']=2
        job=self.submit(count=3);self.assertEqual(self.claim('main',job)[0],200)

    def test_absolute_socket_tls_header_and_body_deadlines_leave_no_thread(self):
        for mode in ('http_header','tls_handshake','tls_header','tls_body'):
            with self.subTest(mode=mode):
                listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(2)
                port=listener.getsockname()[1];stop=threading.Event();errors=[]
                def drip():
                    connection=None
                    try:
                        connection,_=listener.accept();connection.settimeout(1)
                        if mode=='tls_handshake':
                            stop.wait(1);return
                        if mode.startswith('tls'):
                            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
                            connection=context.wrap_socket(connection,server_side=True)
                        connection.recv(4096)
                        head=b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n'
                        payload=head+b'{}'
                        if mode=='tls_body':connection.sendall(head);payload=b'{}';pause=.4
                        else:pause=.025
                        for byte in payload:
                            if stop.is_set():break
                            connection.sendall(bytes((byte,)))
                            if stop.wait(pause):break
                    except (OSError,ssl.SSLError):pass
                    except Exception as error:errors.append(type(error).__name__)
                    finally:
                        if connection is not None:connection.close()
                thread=threading.Thread(target=drip);thread.start()
                try:
                    origin=('http' if mode=='http_header' else 'https')+'://127.0.0.1:'+str(port)
                    before=time.monotonic();result=self.routing._json(origin+'/info',{'Accept':'application/json'});elapsed=time.monotonic()-before
                    self.assertEqual(result,('unknown',None));self.assertLess(elapsed,.65);self.assertGreater(elapsed,.2)
                finally:
                    stop.set();listener.close();thread.join(2)
                self.assertFalse(thread.is_alive());self.assertEqual(errors,[])

    def test_literal_ip_policy_prevents_any_dns_and_tls_trust_widening_holds(self):
        with patch('socket.getaddrinfo',side_effect=AssertionError('DNS must not run')):
            with self.assertRaisesRegex(RoutingHeld,'literal_ip'):
                ReferenceRouting(replace(self.config,local_node_url='http://localhost:1234'),self.workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=self.tls)
            self.assertEqual(self.routing.sample().nodes['cluster'].state,'ready')
        other=self.root/'other-ca.pem';other_key=self.root/'other-key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=another-synthetic-routing-ca','-keyout',str(other_key),'-out',str(other)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.tls.load_verify_locations(cafile=str(other))
        with self.assertRaisesRegex(RoutingHeld,'routing_transport_changed'):self.routing.sample()
        context=ssl.create_default_context(cafile=str(self.cert))
        routing=ReferenceRouting(self.config,self.workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=context)
        context.maximum_version=ssl.TLSVersion.TLSv1_2
        with self.assertRaisesRegex(RoutingHeld,'routing_transport_changed'):routing.sample()

    def test_no_generic_callback_or_unverified_tls_and_live_policy_drift(self):
        with self.assertRaises(ValueError):ReferenceStore(self.queue,routing=lambda:True)
        with self.assertRaises(RoutingHeld):ReferenceRouting(replace(self.config,reference_main_fallback=False),self.workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=self.tls)
        context=ssl._create_unverified_context()
        with self.assertRaises(RoutingHeld):ReferenceRouting(self.config,self.workers,authority_id=AUTHORITY,bindings=self.bindings,ssl_context=context)
        self.store.routing=None
        with self.assertRaises(ValueError):self.store.next_for_node('main')
        self.routing.timeout=.5
        with self.assertRaises(RoutingHeld):self.routing.sample()

class RoutingConfigTests(unittest.TestCase):
    def test_default_off_and_explicit_closed_opt_in(self):
        env=dict(PROCESSING_API_TOKEN='synthetic-api-token-'+'x'*32)
        with patch.dict(os.environ,env,clear=True):self.assertFalse(from_environment().reference_main_fallback)
        for value in ('true','2'):
            with patch.dict(os.environ,dict(env,PROCESSING_REFERENCE_MAIN_FALLBACK=value),clear=True):
                with self.assertRaises(ValueError):from_environment()
        with patch.dict(os.environ,dict(env,PROCESSING_REFERENCE_MAIN_FALLBACK='1'),clear=True):
            with self.assertRaises(ValueError):from_environment()
        with patch.dict(os.environ,dict(env,PROCESSING_REFERENCE_MAIN_FALLBACK='1',PROCESSING_REFERENCE_CONFIG='/private/config',PROCESSING_PROTOTYPE_NODE_IDS='cluster,main'),clear=True):self.assertTrue(from_environment().reference_main_fallback)


class RoutingRuntimeSettingsTests(unittest.TestCase):
    def test_independent_identity_config_and_actual_runtime_assembly(self):
        from tests.test_reference_runtime import ReferenceRuntimeTests
        from processing.reference_runtime import ReferenceRuntime,load_reference_settings,RuntimeConfigurationError
        fixture=ReferenceRuntimeTests();fixture.setUp()
        try:
            fixture.config=replace(fixture.config,reference_main_fallback=True)
            with self.assertRaises(RuntimeConfigurationError):fixture.settings()
            for worker in fixture.value['workers'].values():worker['resultOrigin']='https://127.0.0.1:19444'
            fixture.value['routingBindings']={node:dict(engineId='engine-'+node,gateGeneration=1,engineVersion='synthetic-1') for node in ('cluster','main')}
            fixture.write_config();settings=fixture.settings()
            self.assertIsNone(settings.recovery_bindings)
            self.assertEqual(settings.routing_bindings['main'],RoutingNodeBinding('engine-main',1,'synthetic-1'))
            queue=Queue(fixture.root/'routed',reference_enabled=True,node_ids=('cluster','main'))
            try:
                first,second=fixture.tls_patches()
                with first,second:runtime=ReferenceRuntime(queue,fixture.config,settings,discover=lambda:fixture.snapshot)
                self.assertIs(type(runtime.store.routing),ReferenceRouting)
                self.assertEqual(runtime.store.routing.authority_id,queue.authority_id)
                self.assertIsNone(runtime.control.worker.recovery)
            finally:queue.close()
            for key,value in (('gateGeneration',True),('engineVersion','bad version'),('extra','x')):
                fixture.value['routingBindings']['main'][key]=value;fixture.write_config()
                with self.assertRaises(RuntimeConfigurationError):fixture.settings()
                fixture.value['routingBindings']['main']=dict(engineId='engine-main',gateGeneration=1,engineVersion='synthetic-1')
        finally:fixture.doCleanups()
