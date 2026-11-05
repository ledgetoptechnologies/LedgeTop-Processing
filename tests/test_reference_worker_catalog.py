"""Private live metadata only: no image processing, engine writes or new listener."""
import copy
from email.message import Message
import io
import json
from pathlib import Path
import ssl
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import quote

from processing import reference_protocol as p
from processing.options import catalog_from_snapshots, _sha
from processing.reference_api import Response
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_runtime import _DynamicAPI
from processing.reference_worker_catalog import (PATH, PROTOCOL, MAX_RESPONSE, WorkerCatalogAPI,
                                                 WorkerCatalogClient)
from tests.test_reference_dynamic_source_intake_scale import _write_tls_material


class Fixture:
    def __init__(self, node='cluster', authority=None):
        self.node=node; self.authority=authority or str(uuid.uuid4()); self.checks=0; self.calls=[]
        self.token='private-read-'+node+'-'+ 'a'*32
        self.info=dict(version='3.8.5',engine='odm',engineVersion='3.8.5',maxImages=1000,taskQueueCount=0)
        self.options=[dict(name=name,type='bool',value=False) for name in ('pc-ept','gltf','3d-tiles')]
        fixture=self
        class Engine:
            token='private-native-'+node+'-'+ 'b'*32
            def read(self, route, *, limit):
                fixture.calls.append((route,limit))
                if route=='/info': return copy.deepcopy(fixture.info)
                if route=='/options': return copy.deepcopy(fixture.options)
                raise AssertionError('catalog cannot proxy arbitrary engine requests')
        self.engine=Engine()
        self.worker=SimpleNamespace(node_id=node,worker_id='worker-'+node,engine_id='engine-'+node,
            authority_id=self.authority,gate_generation=1,version='3.8.5',engine=self.engine,
            _verify_local_engine=self.verify)
        inner=ReferenceResultAPI(SimpleNamespace(worker=self.worker),bearer_readers={self.token:dict(
            node_id=node,worker_id='worker-'+node,environments=('production','staging'),share_policy=lambda _:True)})
        self.api=WorkerCatalogAPI(inner,self.worker,worker_instance_id='instance-'+node)

    def verify(self): self.checks+=1
    def request(self, *, token=None, nonce=None, **kwargs):
        return self.api.handle('POST',PATH,{'Authorization':'Bearer '+(token or self.token)},
            p.canonical_json(dict(schemaVersion=1,nonce=nonce or str(uuid.uuid4()))),protected_transport=True,**kwargs)
    def config(self, origin=None):
        return dict(workerId='worker-'+self.node,instanceId='instance-'+self.node,resultBearer=self.token,
                    resultOrigin=origin or 'https://'+self.node+'.invalid')


class WireResponse(io.BytesIO):
    status=200
    def __init__(self, raw, url):
        super().__init__(raw);self.url=url;self.headers=Message();self.headers['Content-Type']='application/json'
    def geturl(self): return self.url


class LocalOpener:
    def __init__(self, *fixtures): self.nodes={f.config()['resultOrigin']:f for f in fixtures};self.calls=[];self.change=None
    def open(self, request, *, timeout):
        self.calls.append(request)
        fixture=self.nodes[request.full_url[:-len(PATH)]]
        response=fixture.api.handle(request.method,PATH,dict(request.header_items()),request.data,protected_transport=True)
        if response.status != 200:
            raise HTTPError(request.full_url,response.status,'unavailable',{},io.BytesIO(b''))
        value=response.body
        if self.change: value=self.change(copy.deepcopy(value))
        return WireResponse(p.canonical_json(value),request.full_url)


class WorkerCatalogTests(unittest.TestCase):
    def setUp(self): self.fixture=Fixture()
    def client(self, *fixtures, opener=None):
        fixtures=fixtures or (self.fixture,)
        return WorkerCatalogClient({f.node:f.config() for f in fixtures},node_ids=tuple(f.node for f in fixtures),
            authority_id=self.fixture.authority,ssl_context=ssl.create_default_context(),opener=opener or LocalOpener(*fixtures))

    def test_idle_and_busy_dynamic_catalog_are_both_available_without_driver_lock(self):
        for count in (0,1,12):
            self.fixture.info['taskQueueCount']=count
            value,capacity=self.client().discover()
            self.assertTrue(value['ready']);self.assertTrue(capacity['cluster']['ready'])
            self.assertEqual(capacity['cluster']['taskQueueCount'],count)
        self.assertEqual({route for route,_ in self.fixture.calls},{'/info','/options'})
        self.assertEqual(self.fixture.checks,6)

    def test_existing_result_auth_and_protected_transport_before_native_reads(self):
        self.assertEqual(self.fixture.request(token='wrong-token').status,401)
        self.assertEqual(self.fixture.api.handle('POST',PATH,{},b'',protected_transport=False).status,503)
        self.assertEqual(self.fixture.calls,[])
        duplicate={'Authorization':'Bearer '+self.fixture.token,'authorization':'Bearer '+self.fixture.token}
        self.assertEqual(self.fixture.api.handle('POST',PATH,duplicate,b'',protected_transport=True).status,401)

    def test_no_native_write_route_or_metadata_alias_is_exposed(self):
        for method,path in (('POST','/task/new'),('POST','/task/cancel'),('GET','/info'),('GET','/options'),('GET',PATH)):
            reply=self.fixture.api.handle(method,path,{'Authorization':'Bearer '+self.fixture.token},b'',protected_transport=True)
            self.assertEqual(reply.status,404)
        self.assertEqual(self.fixture.calls,[])

    def test_malformed_oversized_and_credential_reflected_data_are_closed(self):
        for replacement in ([dict(name='new',help=self.fixture.engine.token)],
                            [dict(name='new',help=self.fixture.token)],
                            [dict(name='new',help='x'*300000)],
                            [dict(name='same'),dict(name='same')],[] ):
            self.fixture.options=replacement
            response=self.fixture.request()
            self.assertEqual(response.status,503)
            self.assertNotIn('private-',json.dumps(response.body))
        self.fixture.options=[dict(name='future')]
        for change in ({'taskQueueCount':True},{'maxImages':0},{'engineVersion':'unapproved'}):
            previous=self.fixture.info;self.fixture.info=dict(previous,**change)
            self.assertEqual(self.fixture.request().status,503);self.fixture.info=previous

    def test_url_encoded_native_credential_is_not_exposed_in_dynamic_help(self):
        self.fixture.engine.token='private&native/token with space'
        for encoded in (quote(self.fixture.engine.token,safe=''),quote(self.fixture.engine.token,safe='').lower()):
            self.fixture.options=[dict(name='future',help='url?token='+encoded)]
            self.assertEqual(self.fixture.request().status,503)

    def test_original_engine_id_grammar_and_exact_recovery_binding_are_preserved(self):
        self.fixture.worker.engine_id='engine:legacy.id-1'
        client=self.client()
        client.bindings={'cluster':SimpleNamespace(engine_id=self.fixture.worker.engine_id,gate_generation=1)}
        self.assertTrue(client.discover()[0]['ready'])
        client.bindings['cluster'].gate_generation=2
        catalog,capacity=client.discover()
        self.assertFalse(catalog['ready']);self.assertFalse(capacity['cluster']['ready'])

    def test_client_nonce_identity_binding_and_oversized_response_fail_only_the_node(self):
        other=Fixture('main',self.fixture.authority)
        opener=LocalOpener(self.fixture,other)
        for change in (lambda v:dict(v,nonce=str(uuid.uuid4())),lambda v:dict(v,workerInstanceId='other'),
                       lambda v:dict(v,authorityId=str(uuid.uuid4())),lambda v:dict(v,extra=True),
                       lambda v:dict(v,options=[dict(name='huge',help='x'*MAX_RESPONSE)])):
            opener.change=lambda v:change(v) if v['nodeId']=='cluster' else v
            catalog,capacities=self.client(self.fixture,other,opener=opener).discover()
            self.assertFalse(catalog['ready']);self.assertFalse(capacities['cluster']['ready'])
            self.assertEqual(capacities['cluster']['taskQueueCount'],1)
            self.assertTrue(capacities['main']['ready'])

    def test_live_flags_and_limits_preserve_exact_intersection_and_fingerprints(self):
        other=Fixture('main',self.fixture.authority);client=self.client(self.fixture,other)
        first,capacity=client.discover()
        option=dict(name='future-new-option',type='bool',value=False,help='Fresh engine metadata')
        self.fixture.options.append(option);other.options.append(copy.deepcopy(option))
        self.fixture.info['maxImages']=500;other.info['maxImages']=None
        second,capacity=client.discover()
        self.assertNotEqual(first['commonOptionsSha256'],second['commonOptionsSha256'])
        self.assertIn(option,second['commonOptions'])
        self.assertEqual(capacity['cluster']['maxImages'],500);self.assertIsNone(capacity['main']['maxImages'])
        self.assertEqual(second,catalog_from_snapshots(second['nodes']))
        self.assertEqual(second['nodes'][0]['optionsSha256'],_sha(sorted(self.fixture.options,key=lambda x:x['name'])))
        other.options[-1]['value']=True
        self.assertIn('future-new-option',client.discover()[0]['conflictingNames'])

    def test_claim_refresh_uses_one_fresh_fleet_packet_without_native_fallback(self):
        called=[];fixture=self.fixture
        store=SimpleNamespace(update_fleet_snapshot=lambda capacities,nodes:called.append((capacities,nodes)))
        control=SimpleNamespace(coordinator_queue=None,worker=SimpleNamespace(store=store,_authenticate=lambda _:{}),
            handle=lambda *args,**kwargs:Response(200,{'job':None}))
        client=self.client()
        api=_DynamicAPI(control,None,None,lambda:(_ for _ in ()).throw(AssertionError('extra catalog call')),
            lambda:(_ for _ in ()).throw(AssertionError('extra capacity call')),discover_fleet=client.discover)
        self.assertEqual(api.handle('GET','/ltds/worker/v1/next',{},protected_transport=True).status,200)
        self.assertEqual(len(called),1);self.assertEqual(len(fixture.calls),2)

    def test_real_existing_https_listener_auth_and_dynamic_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            key,certificate=_write_tls_material(Path(directory))
            server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_context.load_cert_chain(certificate,key)
            server=create_reference_http_server(self.fixture.api,'127.0.0.1',0,ssl_context=server_context)
            server.daemon_threads=False;server.block_on_close=True
            thread=threading.Thread(target=server.serve_forever);thread.start()
            try:
                context=ssl.create_default_context(cafile=str(certificate))
                workers={'cluster':self.fixture.config('https://127.0.0.1:'+str(server.server_port))}
                client=WorkerCatalogClient(workers,node_ids=('cluster',),authority_id=self.fixture.authority,ssl_context=context)
                self.assertTrue(client.discover()[0]['ready'])
                self.fixture.info['maxImages']=250;self.fixture.info['taskQueueCount']=1
                _,capacities=client.discover()
                self.assertEqual(capacities['cluster'],dict(ready=True,maxImages=250,taskQueueCount=1))
                workers['cluster']['resultBearer']='wrong-private-token'
                wrong=WorkerCatalogClient(workers,node_ids=('cluster',),authority_id=self.fixture.authority,ssl_context=context)
                self.assertFalse(wrong.discover()[0]['ready'])
            finally:
                server.shutdown();server.server_close();thread.join(2);self.assertFalse(thread.is_alive())


@unittest.skipUnless(sys.platform.startswith('linux'),'actual worker/Queue ownership requires Linux')
class WorkerCatalogWiringTests(unittest.TestCase):
    def test_runtime_defaults_discover_workers_without_direct_native_discovery(self):
        from tests import test_reference_runtime as fixtures
        from processing.reference_runtime import ReferenceRuntime
        from processing.prototype import Queue
        host=fixtures.ReferenceRuntimeTests(methodName='runTest');self.addCleanup(host.doCleanups);host.setUp()
        queue=Queue(host.config.prototype_state_dir,node_ids=('cluster','main'),treedar_node_ids=('main',),reference_enabled=True)
        self.addCleanup(queue.close)
        cluster,main=Fixture('cluster',queue.authority_id),Fixture('main',queue.authority_id)
        host.value=host.recovery_config(queue.authority_id)
        for fixture in (cluster,main):
            fixture.worker.engine_id=host.value['recoveryBindings'][fixture.node]['engineId']
            host.value['workers'][fixture.node].update(fixture.config())
        host.write_config()
        first,second=host.tls_patches()
        with first,second:
            runtime=ReferenceRuntime(queue,host.config,host.settings())
        self.addCleanup(runtime.close)
        runtime._worker_catalog.opener=LocalOpener(cluster,main)
        with patch('processing.options._get_json',side_effect=AssertionError('native discovery forbidden')), \
             patch('processing.provider._get_json',side_effect=AssertionError('native capacity forbidden')):
            catalog=runtime.api.discover()
            self.assertTrue(catalog['ready']);self.assertIsNotNone(runtime.api.discover_fleet)
            cluster.info['maxImages']=400;main.info['taskQueueCount']=1
            capacities=runtime._discover_capacities()
            self.assertEqual(capacities['cluster']['maxImages'],400)
            self.assertEqual(capacities['main']['taskQueueCount'],1)
            self.assertEqual(capacities['cluster']['maxInputBytes'],host.config.node_endpoints[0].max_input_bytes)

    def test_real_factory_catalog_is_below_readiness_wrapper_and_works_with_busy_driver(self):
        from tests import test_reference_worker_handover as fixtures
        host=fixtures.ReferenceWorkerHandoverTests(methodName='runTest');self.addCleanup(host.doCleanups);host.setUp()
        config=host.config
        worker=dict(workerId=config['workerId'],instanceId=config['workerInstanceId'],
                    resultOrigin='https://127.0.0.1:'+str(config['listenPort']),resultBearer=config['resultReadBearer'])
        context=ssl.create_default_context(cafile=config['caFile'])
        client=WorkerCatalogClient({'cluster':worker},node_ids=('cluster',),authority_id=config['authorityId'],ssl_context=context)
        with host.backend._db() as db: before=db.execute('SELECT COUNT(*) FROM calls').fetchone()[0]
        driver=host.control.service.driver
        self.assertTrue(driver._lock.acquire(timeout=2))
        try:
            catalog,capacity=client.discover()
            self.assertTrue(catalog['ready'],catalog)
            self.assertEqual(capacity['cluster']['maxImages'],1000)
        finally:driver._lock.release()
        with host.backend._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM calls').fetchone()[0],before)
        self.assertEqual(host.control.calls,[])


if __name__=='__main__':unittest.main()
