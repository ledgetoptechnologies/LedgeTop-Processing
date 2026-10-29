"""Two actual canonical worker services, real routing/TLS/Node867 transport.

Only abc image payloads and a synthetic native backend; no geospatial proof.
"""
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import copy
import hashlib
import json
import os
from pathlib import Path
import selectors
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture, ARCHIVE, _parts
from processing import reference_protocol as p
from processing.config import Config
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_catalog_cache import CatalogAdmissionPolicy
from processing.reference_routing import RoutingNodeBinding
from processing.reference_runtime import ReferenceRuntime, ReferenceSettings
from processing.viewer_service import NativeEngine
from processing.odx_smoke_worker import SmokeError
from tests import test_reference_joint_flow as joint
from tests import test_reference_worker_service as service_fixture

VIEWER='synthetic-two-node-viewer-'+'a'*32
WORKERS={n:'synthetic-two-node-'+n+'-worker-'+'b'*32 for n in ('cluster','main')}
READERS={n:'synthetic-two-node-'+n+'-reader-'+'c'*32 for n in ('cluster','main')}

class BoundedNativeUploadFixtureTests(unittest.TestCase):
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));self.root.chmod(0o700)
    def prepare(self,count=None):
        kw={} if count is None else {'expected_uploads':count}
        fixture=NativeNodeFixture(self.root/'native.sqlite',synthetic_enabled=True,**kw).start();self.stack.callback(fixture.close)
        engine=NativeEngine(fixture.origin);job=str(uuid.uuid4())
        engine.initialize(dict(name='synthetic',engine_uuid=job,options=[],outputs=None))
        files=[]
        for i in range(3):
            p=self.root/(str(i)+'.JPG');p.write_bytes(b'abc')
            files.append(dict(path=str(p),bytes=3,name=p.name,sha256=hashlib.sha256(b'abc').hexdigest()))
        return fixture,engine,job,files
    def test_default_one_upload_behavior_is_unchanged(self):
        fixture,engine,job,files=self.prepare()
        engine.upload(job,files[0])
        with self.assertRaises(SmokeError):engine.upload(job,files[1])
        engine.action('commit',job)
        self.assertEqual(fixture.counts()['upload'],1);self.assertEqual(fixture.counts()['commit'],1)
    def test_exact_two_uploads_reject_early_commit_and_oversubmission(self):
        fixture,engine,job,files=self.prepare(2)
        with self.assertRaises(SmokeError):engine.action('commit',job)
        engine.upload(job,files[0])
        with self.assertRaises(SmokeError):engine.action('commit',job)
        engine.upload(job,files[1])
        with self.assertRaises(SmokeError):engine.upload(job,files[2])
        engine.action('commit',job)
        self.assertEqual(fixture.counts()['upload'],2);self.assertEqual(fixture.counts()['commit'],1)
        with fixture._db() as db:self.assertEqual(db.execute('SELECT uploaded FROM tasks').fetchone()[0],2)
    def test_upload_policy_is_typed_bounded_and_pinned_on_reopen(self):
        for count in (True,0,51,1.5,'2'):
            with self.assertRaises(ValueError):NativeNodeFixture(self.root/'invalid.sqlite',synthetic_enabled=True,expected_uploads=count)
        self.assertFalse((self.root/'invalid.sqlite').exists())
        fixture,_,_,_=self.prepare(2);fixture.close();before=fixture.path.read_bytes()
        for count in (1,3):
            with self.assertRaises(ValueError):NativeNodeFixture(fixture.path,synthetic_enabled=True,expected_uploads=count)
            self.assertEqual(before,fixture.path.read_bytes())


@unittest.skipUnless(os.name=='posix' and os.environ.get('VIEWER_REFERENCE_ROOT'),
    'opt-in pinned Viewer867/Node24 Linux two-worker fixture')
class TwoNodeCanonicalJointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):joint.ReferenceJointFlowTests.setUpClass.__func__(cls)
    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.root=Path(self.stack.enter_context(tempfile.TemporaryDirectory()));self.root.chmod(0o700)
        gates=self.root/'physical-gates';gates.mkdir(mode=0o700)
        self.gate_ids={n:'two-node-'+n+'-engine' for n in ('cluster','main')}
        self.gate_path=lambda engine:gates/hashlib.sha256(engine.encode()).hexdigest()
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',side_effect=self.gate_path))
        self.tls=ssl.create_default_context(cafile=str(self.cert));self.tls.minimum_version=ssl.TLSVersion.TLSv1_2
        self.natives={};self.upload_names={n:[] for n in ('cluster','main')};self.services={};self.peers=[];self.trace=[]
        self.stop_sampling=threading.Event();self.samples=[];self.sample_errors=[]
    def native(self,node,count):
        fixture=NativeNodeFixture(self.root/(node+'-native.sqlite'),engine_version='synthetic-'+node+'-v1',synthetic_enabled=True,expected_uploads=count)
        dispatch=fixture._dispatch
        def observed(method,route,headers,body):
            result=dispatch(method,route,headers,body)
            if method=='POST' and route.startswith('/task/new/upload/') and result[0]==200:
                self.upload_names[node].append(_parts(headers.get('Content-Type'),body)[0][1])
            if method=='POST' and route.startswith('/task/new/commit/') and result[0]==200:
                with fixture._db() as db:db.execute('UPDATE tasks SET status=20 WHERE uuid=?',(route.rsplit('/',1)[1],))
            return result
        fixture._dispatch=observed;fixture.start();self.stack.callback(fixture.close)
        return fixture
    def assemble(self,count=1):
        self.natives={node:self.native(node,count if node=='main' else 1) for node in ('cluster','main')}
        self.queue=Queue(self.root/'coordinator',reference_enabled=True,node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.stack.callback(self.queue.close);self.authority=self.queue.authority_id;self.gate_origin='http://127.0.0.1:18080'
        for node in ('cluster','main'):
            with NodeGate(self.gate_ids[node]) as gate:gate.provision(node,self.gate_origin,self.authority)
        self.ports={node:service_fixture.free_port() for node in ('cluster','main')}
        self.config=Config('synthetic-two-node-native-'+'x'*32,self.natives['cluster'].origin,self.natives['main'].origin,
            1,20*1024**3,prototype_state_dir=str(self.root/'coordinator'),prototype_node_ids=('cluster','main'),treedar_node_ids=('main',),reference_main_fallback=True)
        self.settings=ReferenceSettings(listener=dict(host='127.0.0.1',port=0,certificate=str(self.cert),privateKey=str(self.key),clientCA=str(self.cert)),
            viewer_bearers={VIEWER:'staging'},workers={n:dict(workerId=n+'-worker',instanceId=n+'-instance',bearer=WORKERS[n],resultBearer=READERS[n],resultOrigin='https://127.0.0.1:'+str(self.ports[n])) for n in ('cluster','main')},
            stream_budget=dict(maxConcurrent=1,maxObjectBytes=4194304),routing_bindings={n:RoutingNodeBinding(self.gate_ids[n],1,'synthetic-'+n+'-v1') for n in ('cluster','main')},catalog_admission=CatalogAdmissionPolicy(60))
        self.runtime=ReferenceRuntime(self.queue,self.config,self.settings)
        original=self.runtime.api.handle
        def traced(method,path,headers,body=b'',**kwargs):
            result=original(method,path,headers,body,**kwargs)
            if path in ('/ltds/worker/v1/next','/ltds/worker/v1/claim'):
                node=next((n for n in WORKERS if headers.get('Authorization')=='Bearer '+WORKERS[n]),'unknown')
                self.trace.append((node,path,result.body.get('job') if isinstance(result.body,dict) else None))
            return result
        self.runtime.api.handle=traced;self.runtime.start();self.stack.callback(self.runtime.close)
        self.origin='https://127.0.0.1:'+str(self.runtime._server.server_port)
        self.primary=self.peer('primary',count)
        for node in ('cluster','main'):
            base=self.root/('worker-'+node);base.mkdir(mode=0o700)
            config=service_fixture.private_config(base,self.cert,self.key,NativeEngine(self.natives[node].origin),self.authority,
                coordinator=self.origin,broker=self.primary.origin,node=node,worker=node+'-worker',instance=node+'-instance',bearer=WORKERS[node],reader=READERS[node],gate=self.gate_ids[node],gate_origin=self.gate_origin)
            config.update(listenPort=self.ports[node],optionEvidence=dict(schemaVersion=1))
            private=base/'private.json';private.write_text(json.dumps(config));private.chmod(0o600)
            driver,server=daemon.build_worker(daemon.load_config(str(private)))
            self.stack.callback(server.server_close)
            self.services[node]=SimpleNamespace(driver=driver,server=server,stop=threading.Event(),outcomes=[])
    def peer(self,label,count):
        root=self.root/label;root.mkdir(mode=0o700);state=root/'viewer';state.mkdir(mode=0o700)
        headers={'Authorization':'Bearer '+VIEWER}
        from urllib.request import Request,build_opener,HTTPSHandler,ProxyHandler
        with build_opener(ProxyHandler({}),HTTPSHandler(context=self.tls)).open(Request(self.origin+'/ltds/v1/capabilities',headers=headers),timeout=5) as response:capabilities=json.loads(response.read())
        original=copy.deepcopy(json.loads((self.viewer_root/'test/fixtures/reference-v1.contract.json').read_text())['submission'])
        original['submissionId']=str(uuid.uuid4());original['capabilityFingerprint']=capabilities['capabilityFingerprint'];original['pipeline']['odm']['optionsFingerprint']=capabilities['engines']['odm']['optionsFingerprint']
        manifest=original['manifest'];template=manifest['objects'][0]
        manifest['objects']=[dict(template,id='image-'+str(i),name='Survey_'+str(i).zfill(3)+'.JPG') for i in range(1,count+1)]
        manifest['objectCount']=count;manifest['totalBytes']=3*count;manifest['inputCounts']['images']=count;manifest['sha256']=p.manifest_sha256(manifest)
        seed=dict(syntheticFixture=True,version=1,environment='staging',controlOriginSha256=hashlib.sha256(self.origin.encode()).hexdigest(),submission=original,job=None,capabilities=capabilities,results=None,grantAttempted=False)
        (state/'fixture-state.json').write_text(json.dumps(seed));(state/'fixture-state.json').chmod(0o600)
        config=dict(keyPath=str(self.key),certPath=str(self.cert),controlEndpoint=self.origin,controlToken=VIEWER,fixtureControlToken=joint.FIXTURE,environment='staging',host='127.0.0.1',port=0,fixtureRoot=str(state),failAfterLedgerCommit=False)
        path=root/'config.json';path.write_text(json.dumps(config));path.chmod(0o600)
        env=dict(os.environ,REFERENCE_SYNTHETIC_FIXTURE='1',NODE_EXTRA_CA_CERTS=str(self.cert),LTDS_REFERENCE_SYNTHETIC_IMAGES=str(count));env.pop('NODE_TLS_REJECT_UNAUTHORIZED',None)
        helper=Path(__file__).parent/'helpers/reference_joint_images_preload.cjs'
        process=subprocess.Popen(['node','--require',str(helper),str(self.viewer_root/'test/helpers/reference-joint-harness.cjs'),str(path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        peer=SimpleNamespace(process=process,viewer_state=state,submission=original)
        self.stack.callback(lambda:joint.ReferenceJointFlowTests.stop_node(peer))
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout,selectors.EVENT_READ);self.assertTrue(ready.select(8),'actual Node fixture readiness')
            value=json.loads(process.stdout.readline(16384));self.assertTrue(value['ready'])
        peer.origin='https://127.0.0.1:'+str(value['port']);self.peers.append(peer);return peer
    def call(self,peer,action):
        return joint.ReferenceJointFlowTests.call(SimpleNamespace(node_origin=peer.origin,tls=self.tls),action)
    def start_services(self):
        def sample():
            while not self.stop_sampling.wait(.01):
                try:
                    with self.queue._db() as db:owned=db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('running','held')").fetchone()[0]
                    heavy=0
                    for fixture in self.natives.values():
                        with fixture._db() as db:heavy+=db.execute('SELECT COUNT(*) FROM tasks WHERE status=20').fetchone()[0]
                    self.samples.append((owned,heavy))
                except Exception as error:self.sample_errors.append(type(error).__name__)
        sampler=threading.Thread(target=sample);sampler.start()
        self.stack.callback(lambda:(self.stop_sampling.set(),sampler.join(5)))
        for node,value in self.services.items():
            value.service=daemon.WorkerService(value.driver,value.server,poll_seconds=.1)
            value.thread=threading.Thread(target=lambda v=value:v.outcomes.append(v.service.run(v.stop)))
            value.thread.start()
            def stop(v=value):
                v.stop.set();v.thread.join(10);self.assertFalse(v.thread.is_alive())
            self.stack.callback(stop)
    def wait_stage(self,node,job,target):
        deadline=time.monotonic()+20;driver=self.services[node].driver
        while time.monotonic()<deadline:
            self.call(self.primary,'pump')
            with driver._db() as db:row=db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job,)).fetchone()
            if row and row[0]==target:return
            time.sleep(.025)
        details={n:v.service.health() for n,v in self.services.items()}
        self.fail('canonical service stage '+target+' not reached: '+repr(details))
    def finish(self,node,job,count):
        # The canonical driver's executing phase precedes its native commit.
        # Hold the synthetic engine in real status20 until the independent
        # sampler observes the actual owned/heavy overlap; never rely on a
        # 10ms sampler winning a race against the terminal fixture transition.
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            with self.natives[node]._db() as db:
                native=db.execute('SELECT status FROM tasks WHERE uuid=?',(job,)).fetchone()
            if native and native[0]==20 and (1,1) in self.samples:break
            time.sleep(.01)
        else:self.fail('actual committed heavy status20 and independent owned/heavy sample not observed')
        with self.natives[node]._db() as db:db.execute('UPDATE tasks SET status=40 WHERE uuid=?',(job,))
        self.wait_stage(node,job,'awaiting_acceptance')
        self.assertEqual(self.natives[node].counts().get('remove',0),0)
        value=self.call(self.primary,'ingest');self.assertTrue(value['acknowledged']);self.assertFalse(value['published'])
        self.wait_stage(node,job,'completed')
        deadline=time.monotonic()+5
        while (self.natives[node].counts().get('remove',0)!=1 or list(self.services[node].driver.ledger.root.glob('jobs/*/all.zip'))) and time.monotonic()<deadline:time.sleep(.025)
        self.assertEqual(self.natives[node].counts().get('remove',0),1)
        driver=self.services[node].driver
        inputs=list(driver.ledger.root.glob('jobs/*/*.input'));self.assertEqual(len(inputs),count)
        self.assertTrue(all(p.read_bytes()==b'abc' for p in inputs));self.assertEqual(list(driver.ledger.root.glob('jobs/*/all.zip')),[])
        archives=list((self.primary.viewer_state/'results').glob('*.zip'));self.assertEqual(len(archives),1);self.assertEqual(archives[0].read_bytes(),ARCHIVE)
        sources=list((self.primary.viewer_state/'input').glob('*.JPG'));self.assertEqual(len(sources),count)
        self.assertTrue(all(source.read_bytes()==b'abc' for source in sources))
        self.assertEqual(len({source.stat().st_ino for source in sources}),count)
        with closing(sqlite3.connect(self.primary.viewer_state/'acceptance.sqlite')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM acceptances').fetchone()[0],1);self.assertEqual(db.execute('SELECT acked FROM ack_outbox').fetchone()[0],1)
        with closing(sqlite3.connect(self.primary.viewer_state/'broker.sqlite')) as db:
            row=db.execute('SELECT metadata,used_bytes,reserved_bytes,in_flight FROM reference_grants').fetchone()
            self.assertEqual(json.loads(row[0])['byteBudget'],9*count);self.assertEqual(row[1:],(3*count,0,0))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_grants').fetchone()[0],1)
        self.assertEqual(sorted(self.upload_names[node]),['Survey_'+str(i).zfill(3)+'.JPG' for i in range(1,count+1)])
        self.assertTrue(self.samples);self.assertEqual(self.sample_errors,[]);self.assertEqual(max(a for a,b in self.samples),1);self.assertEqual(max(b for a,b in self.samples),1)
        for n in ('cluster','main'):
            with NodeGate(self.gate_ids[n]) as gate:self.assertIsNone(gate.snapshot()['reservation'])
    def test_healthy_small_goes_cluster_and_main_eligible_large_job_never_overlaps(self):
        self.assemble();self.start_services();job=self.call(self.primary,'submit')['jobId'];self.wait_stage('cluster',job,'executing')
        second=self.peer('waiting-second',2);other=self.call(second,'submit')['jobId']
        deadline=time.monotonic()+3;start=len(self.trace)
        while not any(node=='main' and path.endswith('/next') and value is None for node,path,value in self.trace[start:]) and time.monotonic()<deadline:time.sleep(.025)
        self.assertTrue(any(node=='main' and path.endswith('/next') and value is None for node,path,value in self.trace[start:]))
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT node_id FROM jobs WHERE id=?',(job,)).fetchone()[0],'cluster');self.assertEqual(db.execute('SELECT status FROM jobs WHERE id=?',(other,)).fetchone()[0],'queued')
        for node in ('cluster','main'):
            current=self.queue.get_node(node);stamp=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
            self.queue.schedule_node(node,dict(expectedRevision=current['revision'],drainAt=stamp,notBefore=stamp,afterJobId=job if node=='cluster' else None,targetImage='sha256:'+'1'*64))
        self.finish('cluster',job,1)
        self.assertEqual(self.natives['main'].counts()['initialize'],0)
    def test_two_photo_large_job_completes_on_canonical_main(self):
        self.assemble(2);self.start_services();job=self.call(self.primary,'submit')['jobId'];self.wait_stage('main',job,'executing');self.finish('main',job,2)
        self.assertEqual(self.natives['cluster'].counts()['initialize'],0)
    def test_unavailable_cluster_routes_small_job_to_canonical_main(self):
        self.assemble();self.start_services();self.natives['cluster'].close()
        job=self.call(self.primary,'submit')['jobId'];self.wait_stage('main',job,'executing');self.finish('main',job,1)
        self.assertEqual(self.natives['cluster'].counts()['initialize'],0)

if __name__=='__main__':unittest.main()
