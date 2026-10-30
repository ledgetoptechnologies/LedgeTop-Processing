"""Negative-only metadata routing evidence; no admission/file-release authority."""
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import ssl
import unittest
from unittest.mock import patch
from urllib.request import Request,build_opener,ProxyHandler,HTTPSHandler
from urllib.error import HTTPError

from processing import reference_indexed_fetch as indexed
from processing import reference_metadata_capacity as capacity
from processing.config import Config
from processing.reference_routing import ReferenceRouting,RoutingNodeBinding,RoutingHeld,HEALTH_PATH
from processing.reference_runtime import ReferenceRuntime,load_reference_settings,RuntimeConfigurationError
from processing.prototype import Queue
from tests import test_reference_routing as routing_fixture
from tests import test_reference_runtime as runtime_fixture
from tests import test_reference_three_retained_profile as three_fixture
from tests import test_reference_worker_service as service_fixture
from ops import bootstrap_worker_input_accounting as bootstrap


def identity(node='cluster'):
    return dict(identityScope='configured_service',nodeId=node,authorityId=routing_fixture.AUTHORITY,
        workerId=node+'-worker',workerInstanceId=node+'-instance',engineId='engine-'+node,
        gateGeneration=1,engineVersion='synthetic-1')


def observation(node='cluster',count=0,policy_id=indexed.THREE_POLICY_ID):
    descriptor=indexed.compiled_profile_descriptor(policy_id)
    limit=3 if policy_id==indexed.THREE_POLICY_ID else 1
    return dict(**identity(node),schemaVersion=1,scope=capacity.SCOPE,policyId=policy_id,
        policySha256=descriptor['policySha256'],capacity=limit,sealedRetainedJobs=count,
        state='exhausted' if count==limit else 'not_exhausted')


class MetadataCapacityRoutingTests(unittest.TestCase):
    def setUp(self):
        self.config=Config('synthetic-000000000000000000','http://127.0.0.1:18001',
            'http://127.0.0.1:18002',1000,20*1024**3,prototype_node_ids=('cluster','main'),reference_main_fallback=True)
        self.workers={n:dict(resultOrigin='https://127.0.0.1:'+str(18101+i),
            resultBearer='synthetic-reader-'+n+'-0000000000000000',workerId=n+'-worker',instanceId=n+'-instance')
            for i,n in enumerate(('cluster','main'))}
        self.bindings={n:RoutingNodeBinding('engine-'+n,1,'synthetic-1') for n in self.workers}
        self.bodies={n:observation(n) for n in self.workers};self.health={n:routing_fixture.health(n) for n in self.workers}
        self.calls=[]

    def router(self,enabled=True):
        router=ReferenceRouting(self.config,self.workers,authority_id=routing_fixture.AUTHORITY,
            bindings=self.bindings,ssl_context=ssl.create_default_context(),metadata_capacity_veto=enabled)
        def read(url,headers):
            self.calls.append(url)
            node='cluster' if ':18001' in url or ':18101' in url else 'main'
            if url.endswith('/info'):
                return 200,dict(engine='odm',engineVersion='synthetic-1',version='3.0.0',maxImages=1000,taskQueueCount=0)
            self.assertEqual(headers['Authorization'],'Bearer '+self.workers[node]['resultBearer'])
            return (200,self.health[node]) if url.endswith(HEALTH_PATH) else (200,self.bodies[node])
        router._json=read
        return router

    def choose(self,router):
        submission=dict(pipeline=dict(treedar=dict(requested=False)),manifest=dict(inputCounts=dict(images=20)))
        return router.choose(submission,dict(cluster='active',main='active'),router.sample(),
            local_max_photos=1000,local_max_input_bytes=20*1024**3,main_node_id='main')

    def test_three_sealed_cluster_slots_veto_to_main(self):
        self.bodies['cluster']=observation(count=3)
        self.assertEqual(self.choose(self.router()),'main')

    def test_default_off_keeps_existing_selection_and_no_new_request(self):
        self.bodies['cluster']=observation(count=3)
        self.assertEqual(self.choose(self.router(False)),'cluster')
        self.assertFalse(any(url.endswith(capacity.PATH) for url in self.calls))

    def test_busy_is_never_broadened_by_capacity_observation(self):
        self.health['cluster'].update(workState='pending_work',retainedJobs=1,pendingJobs=1)
        self.assertIsNone(self.choose(self.router()))
        self.assertFalse(any(':18101'+capacity.PATH in url for url in self.calls))

    def test_missing_wrong_identity_or_profile_holds_without_fallback(self):
        for changes in ({'workerInstanceId':'foreign'},{'policyId':indexed.RETIRING_POLICY_ID},
                        {'sealedRetainedJobs':True},{'capacity':4},{'extra':True},
                        {'policySha256':'0'*64},{'state':'available'}):
            with self.subTest(changes=changes):
                self.bodies['cluster']={**observation(),**changes}
                self.assertIsNone(self.choose(self.router()))
        self.assertEqual(capacity.observation_state(404,{},identity=identity()),'unknown')

    def test_flag_is_strict_and_runtime_mutation_is_held(self):
        with self.assertRaises(RoutingHeld):self.router(1)
        router=self.router();router.metadata_capacity_veto=False
        with self.assertRaises(RoutingHeld):router.sample()
        router=self.router();router.metadata_capacity_veto=1
        with self.assertRaises(RoutingHeld):router.sample()

    def test_v2_effective_capacity_is_one_not_declared_history_limit(self):
        self.assertEqual(capacity.observation_state(200,observation(count=1,policy_id=indexed.POLICY_ID),identity=identity()),'exhausted')


class MetadataCapacityRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.f=runtime_fixture.ReferenceRuntimeTests();self.f.setUp();self.addCleanup(self.f.doCleanups)

    def paired(self):
        self.f.config=replace(self.f.config,reference_main_fallback=True)
        self.f.value['routingBindings']={n:dict(engineId='engine-'+n,gateGeneration=1,engineVersion='synthetic-1') for n in ('cluster','main')}
        for i,n in enumerate(('cluster','main')):self.f.value['workers'][n]['resultOrigin']='https://127.0.0.1:'+str(18101+i)
        self.f.value['metadataCapacityVeto']=dict(schemaVersion=1)
        self.f.write_config()

    def test_closed_optin_is_absent_by_default_and_requires_paired_routing(self):
        self.assertFalse(self.f.settings().metadata_capacity_veto)
        self.paired();self.assertTrue(self.f.settings().metadata_capacity_veto)
        for payload in (True,{},dict(schemaVersion=True),dict(schemaVersion=2),dict(schemaVersion=1,enabled=False)):
            self.f.value['metadataCapacityVeto']=payload;self.f.write_config()
            with self.assertRaises(RuntimeConfigurationError):self.f.settings()
        self.paired();self.f.config=replace(self.f.config,reference_main_fallback=False)
        with self.assertRaises(RuntimeConfigurationError):self.f.settings()

    def test_actual_runtime_constructs_opted_in_router_without_starting_listener(self):
        self.paired()
        queue=Queue(self.f.root/'queue',reference_enabled=True,node_ids=('cluster','main'))
        self.addCleanup(queue.close)
        with self.f.tls_patches()[0],self.f.tls_patches()[1]:
            runtime=ReferenceRuntime(queue,self.f.config,self.f.settings(),discover=lambda:self.f.snapshot)
        self.addCleanup(runtime.close)
        self.assertTrue(runtime.store.routing.metadata_capacity_veto)
        self.assertIsNone(runtime._server)


@unittest.skipUnless(os.name=='posix','actual private indexed ownership requires POSIX')
class MetadataCapacityActualJournalTests(unittest.TestCase):
    def setUp(self):
        self.fixture=three_fixture.ThreeRetainedDriverTests();self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups);self.f=self.fixture.f

    def test_three_real_sealed_jobs_exhaust_and_reads_leave_every_store_unchanged(self):
        for _ in range(3):self.f.advance(self.f.submit(),'awaiting_acceptance')
        before={str(p):p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()}
        result=capacity.observe(self.f.driver)
        self.assertEqual((result['state'],result['capacity'],result['sealedRetainedJobs']),('exhausted',3,3))
        self.assertEqual(before,{str(p):p.read_bytes() for p in self.f.root.rglob('*') if p.is_file()})
        self.assertEqual(self.f.ledger.input_cleanup_candidates(),[])

    def test_unsealed_current_preclaim_does_not_veto_its_own_third_claim(self):
        for _ in range(2):self.f.advance(self.f.submit(),'awaiting_acceptance')
        third=self.f.submit();self.assertEqual(self.f.driver.step()['state'],'claim_pending')
        result=capacity.observe(self.f.driver)
        self.assertEqual((result['state'],result['sealedRetainedJobs']),('not_exhausted',2))
        self.assertEqual(self.f.driver.step()['state'],'claimed')
        self.assertEqual(capacity.observe(self.f.driver)['state'],'exhausted')

    def test_busy_context_replacement_and_unknown_journal_are_held(self):
        self.f.driver._lock.acquire()
        try:
            with self.assertRaises(ValueError):capacity.observe(self.f.driver)
        finally:self.f.driver._lock.release()
        context=self.f.driver.indexed_fetch_context;self.f.driver.indexed_fetch_context=None
        with self.assertRaises(ValueError):capacity.observe(self.f.driver)
        self.f.driver.indexed_fetch_context=context
        path=Path(str(self.f.driver.path)+'-journal');path.write_bytes(b'unknown')
        with self.assertRaises(ValueError):capacity.observe(self.f.driver)
        self.assertEqual(path.read_bytes(),b'unknown')


@unittest.skipUnless(os.name=='posix','actual canonical worker and private TLS require POSIX')
class MetadataCapacityCanonicalServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):service_fixture.WorkerServiceTests.setUpClass.__func__(cls)
    setUp=service_fixture.WorkerServiceTests.setUp
    write_config=service_fixture.WorkerServiceTests.write_config
    build=service_fixture.WorkerServiceTests.build
    start=service_fixture.WorkerServiceTests.start

    def request(self,token=None,method='GET',body=None):
        context=ssl.create_default_context(cafile=str(self.cert))
        headers={} if token is None else {'Authorization':'Bearer '+token}
        url='https://127.0.0.1:'+str(self.server.server_port)+capacity.PATH
        try:
            response=build_opener(ProxyHandler({}),HTTPSHandler(context=context)).open(
                Request(url,headers=headers,method=method,data=body),timeout=3)
        except HTTPError as error:response=error
        with response:return response.status,json.loads(response.read(8192))

    def test_actual_v3_factory_reader_tls_returns_only_metadata_observation(self):
        self.config['inputCopyAccounting']=copy.deepcopy(three_fixture.PROFILE)
        self.write_config();bootstrap.bootstrap_input_copy_accounting(self.config)
        self.start()
        code,body=self.request(self.config['resultReadBearer'])
        self.assertEqual(code,200)
        expected={k:self.config[k] for k in capacity.IDENTITY_FIELDS if k!='identityScope'}
        expected['identityScope']='configured_service'
        self.assertEqual(capacity.observation_state(code,body,identity=expected),'not_exhausted')
        self.assertEqual((body['capacity'],body['sealedRetainedJobs']),(3,0))
        self.assertNotIn(str(self.root),json.dumps(body))
        self.assertNotIn(self.config['workerBearer'],json.dumps(body))
        with patch.object(capacity,'observe',side_effect=AssertionError('unauthenticated observation')):
            self.assertEqual(self.request()[0],401)
            self.assertEqual(self.request(self.config['workerBearer'])[0],401)
            self.assertEqual(self.request(self.config['resultReadBearer'],'POST',b'{}')[0],400)

    def test_legacy_worker_cannot_be_mistaken_for_reusable_indexed_capacity(self):
        self.start()
        self.assertEqual(self.request(self.config['resultReadBearer'])[0],503)

    def test_unstarted_service_returns_no_capacity_before_observing_database(self):
        self.build()
        api=self.server.worker_health_api
        with patch.object(capacity,'observe',side_effect=AssertionError('unstarted observation')):
            self.assertEqual(api.handle('GET',capacity.PATH,
                {'Authorization':'Bearer '+self.config['resultReadBearer']},protected_transport=True).status,503)


if __name__=='__main__':unittest.main()
