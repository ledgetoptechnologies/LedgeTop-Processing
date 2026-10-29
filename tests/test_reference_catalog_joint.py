"""Actual Viewer867 + cached admission + canonical worker service composition.

Synthetic abc photos/native HTTP fixture only; no acquisition, publication or
deployed-runtime qualification. Node business logic is the unchanged pinned
Provider/Broker/Acceptance helper, never reimplemented here.
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
from urllib.error import HTTPError
from urllib.request import Request, build_opener, ProxyHandler, HTTPSHandler
import uuid

from ops import reference_worker_daemon as daemon
from ops.native_node_fixture import NativeNodeFixture, ARCHIVE
from processing.config import Config
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_catalog_cache import CatalogAdmissionPolicy
from processing.reference_api import Response
from processing import reference_protocol as p
from processing.reference_routing import RoutingNodeBinding
from processing.reference_runtime import ReferenceRuntime, ReferenceSettings
from processing.viewer_service import NativeEngine
from tests import test_reference_joint_flow as joint
from tests import test_reference_worker_service as service_fixture

PRODUCTION = 'synthetic-catalog-production-viewer-'+'a'*32
MAIN_WORKER = 'synthetic-catalog-main-worker-'+'b'*32
MAIN_READER = 'synthetic-catalog-main-reader-'+'c'*32


@unittest.skipUnless(os.name=='posix' and os.environ.get('VIEWER_REFERENCE_ROOT'),
    'opt-in pinned Viewer867 Linux/Node24 snapshot required')
class CatalogJointFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): joint.ReferenceJointFlowTests.setUpClass.__func__(cls)

    wait_stage = service_fixture.CanonicalWorkerJointTests.wait_stage

    def setUp(self):
        self.stack = ExitStack(); self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory())); self.root.chmod(0o700)
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=self.root/'gate'))
        self.tls = ssl.create_default_context(cafile=str(self.cert))
        self.peers = []; self.service = None; self.process = None
        self.native_fixture = self.native('cluster')
        self.main_fixture = self.native('main')
        self.native_origin = self.native_fixture.origin; self.main_origin = self.main_fixture.origin
        self.control_port = service_fixture.free_port(); self.worker_port = service_fixture.free_port()
        self.control_origin = 'https://127.0.0.1:'+str(self.control_port)
        self.queue = Queue(self.root/'coordinator',reference_enabled=True,
            node_ids=('cluster','main'),treedar_node_ids=('main',),catalog_admission=True)
        self.stack.callback(lambda:self.queue.close())
        self.authority = self.queue.authority_id
        with NodeGate('joint-engine') as gate:
            gate.provision('cluster','http://127.0.0.1:18080',self.authority)
        self.config = Config('synthetic-catalog-native-api-'+'x'*32,self.native_origin,self.main_origin,
            1000,20*1024**3,prototype_state_dir=str(self.root/'coordinator'),
            prototype_node_ids=('cluster','main'),treedar_node_ids=('main',))
        self.settings = ReferenceSettings(
            listener=dict(host='127.0.0.1',port=self.control_port,certificate=str(self.cert),
                privateKey=str(self.key),clientCA=str(self.cert)),
            viewer_bearers={joint.VIEWER:'staging',PRODUCTION:'production'},
            workers={
                'cluster':dict(workerId='joint-worker',instanceId='joint-instance',bearer=joint.WORKER,
                    resultBearer=joint.READER,resultOrigin='https://127.0.0.1:'+str(self.worker_port)),
                'main':dict(workerId='main-worker',instanceId='main-instance',bearer=MAIN_WORKER,
                    resultBearer=MAIN_READER,resultOrigin='https://127.0.0.1:'+str(service_fixture.free_port()))},
            stream_budget=dict(maxConcurrent=1,maxObjectBytes=4194304),
            routing_bindings={'cluster':RoutingNodeBinding('joint-engine',1,'synthetic-joint-v1'),
                'main':RoutingNodeBinding('main-engine',1,'synthetic-joint-v1')},
            catalog_admission=CatalogAdmissionPolicy(60))
        self.runtime = ReferenceRuntime(self.queue,self.config,self.settings); self.runtime.start()
        self.stack.callback(lambda:self.runtime.close()); self.store = self.runtime.store
        # First prime the durable cache from both ACTUAL live engine endpoints.
        fresh, headers = self.metadata('/capabilities',PRODUCTION)
        self.assertEqual(headers.get('X-LTDS-Catalog-Source'),'fresh')
        options, _ = self.metadata('/options',PRODUCTION)
        candidates = [item for item in options['options'] if item['type'] in ('int','integer')]
        self.assertTrue(candidates)
        selected = candidates[0]; self.selected_name = selected['name']
        self.selected_value = selected.get('domain',{}).get('min',selected.get('value',1))
        self.assertIs(type(self.selected_value),int)
        self.staging = self.peer('staging','staging',joint.VIEWER)
        self.primary = self.peer('production-first','production',PRODUCTION)
        self.second = self.peer('production-second','production',PRODUCTION)
        # Build the real canonical factory while /info is reachable, but do not
        # start its service thread until after offline admission and cold reopen.
        self.worker_config = service_fixture.private_config(self.root,self.cert,self.key,
            NativeEngine(self.native_origin),self.authority,coordinator=self.control_origin,
            broker=self.primary.origin,worker='joint-worker',instance='joint-instance',
            bearer=joint.WORKER,reader=joint.READER,gate='joint-engine')
        self.worker_config.update(listenPort=self.worker_port,allowedEnvironments=['production','staging'],
            optionEvidence=dict(schemaVersion=1))
        private = self.root/'worker-private.json'; private.write_text(json.dumps(self.worker_config)); private.chmod(0o600)
        self.driver,self.service_server = daemon.build_worker(daemon.load_config(str(private)))
        self.stack.callback(self.service_server.server_close)
        self.worker,self.ledger,self.outbox = self.driver.worker,self.driver.ledger,self.driver.outbox
        self.outbox_now = time.time(); self.outbox.clock = lambda:self.outbox_now
        self.original_claim = self.original_receipt = None
        self.stop = threading.Event(); self.outcomes = []

    def native(self, role, port=0):
        fixture = NativeNodeFixture(self.root/(role+'-native.sqlite'),port=port,
            engine_version='synthetic-joint-v1',synthetic_enabled=True)
        if role=='cluster':
            dispatch = fixture._dispatch
            def controlled(method,route,headers,body):
                response = dispatch(method,route,headers,body)
                if method=='POST' and route.startswith('/task/new/commit/') and response[0]==200:
                    with fixture._db() as db:
                        db.execute('UPDATE tasks SET status=20 WHERE uuid=?',(route.rsplit('/',1)[1],))
                return response
            fixture._dispatch = controlled
        fixture.start()
        self.stack.callback(fixture.close)
        return fixture

    def metadata(self, route, token):
        request = Request(self.control_origin+p.PREFIX+route,
            headers={'Authorization':'Bearer '+token})
        with build_opener(ProxyHandler({}),HTTPSHandler(context=self.tls)).open(request,timeout=5) as response:
            return json.loads(response.read(4194304)),response.headers

    def peer(self, label, environment, token):
        root = self.root/label; root.mkdir(mode=0o700)
        state = root/'viewer'; state.mkdir(mode=0o700)
        capabilities,_ = self.metadata('/capabilities',token)
        frozen = json.loads((self.viewer_root/'test/fixtures/reference-v1.contract.json').read_text())
        original = copy.deepcopy(frozen['submission']); original['submissionId'] = str(uuid.uuid4())
        original['capabilityFingerprint'] = capabilities['capabilityFingerprint']
        original['pipeline']['odm']['optionsFingerprint'] = capabilities['engines']['odm']['optionsFingerprint']
        original['pipeline']['odm']['options'] = {self.selected_name:self.selected_value}
        # Seed ONLY the original pre-submit synthetic fixture request. The
        # unchanged Provider.submit still performs capabilities/validation/POST.
        seed = dict(syntheticFixture=True,version=1,environment=environment,
            controlOriginSha256=hashlib.sha256(self.control_origin.encode()).hexdigest(),
            submission=original,job=None,capabilities=capabilities,results=None,grantAttempted=False)
        target = state/'fixture-state.json'; target.write_text(json.dumps(seed)); target.chmod(0o600)
        config = dict(keyPath=str(self.key),certPath=str(self.cert),controlEndpoint=self.control_origin,
            controlToken=token,fixtureControlToken=joint.FIXTURE,environment=environment,
            host='127.0.0.1',port=0,fixtureRoot=str(state),failAfterLedgerCommit=False)
        path = root/'fixture-config.json'; path.write_text(json.dumps(config)); path.chmod(0o600)
        env = dict(os.environ,REFERENCE_SYNTHETIC_FIXTURE='1',NODE_EXTRA_CA_CERTS=str(self.cert))
        env.pop('NODE_TLS_REJECT_UNAUTHORIZED',None)
        process = subprocess.Popen(['node',str(self.viewer_root/'test/helpers/reference-joint-harness.cjs'),str(path)],
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        peer = SimpleNamespace(process=process,viewer_state=state,submission=original,environment=environment)
        self.stack.callback(lambda:joint.ReferenceJointFlowTests.stop_node(peer))
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout,selectors.EVENT_READ)
            self.assertTrue(ready.select(8),'actual Node helper readiness deadline')
            line = process.stdout.readline(16384); self.assertTrue(line,'actual Node helper failed')
        value = json.loads(line); self.assertTrue(value['ready'])
        peer.origin = 'https://127.0.0.1:'+str(value['port']); self.peers.append(peer)
        return peer

    def peer_call(self, peer, action):
        endpoint = SimpleNamespace(node_origin=peer.origin,tls=self.tls)
        return joint.ReferenceJointFlowTests.call(endpoint,action)

    def call(self, action): return self.peer_call(self.primary,action)

    def start_service(self):
        self.service = daemon.WorkerService(self.driver,self.service_server,poll_seconds=.1)
        self.thread = threading.Thread(target=lambda:self.outcomes.append(self.service.run(self.stop)))
        self.thread.start()
        def stop():
            self.stop.set(); self.thread.join(10); self.assertFalse(self.thread.is_alive())
        self.stack.callback(stop)

    def test_actual_node_cached_queue_cold_reopen_and_original_worker_acceptance(self):
        ports = (int(self.native_origin.rsplit(':',1)[1]),int(self.main_origin.rsplit(':',1)[1]))
        self.native_fixture.close(); self.main_fixture.close()
        staging = self.peer_call(self.staging,'submit')['jobId']
        submissions = []; lost = [False]; original_handle = self.runtime.api.handle
        def lose_committed_ack(method,path,headers,body=b'',**kwargs):
            response = original_handle(method,path,headers,body,**kwargs)
            if method=='POST' and path==p.PREFIX+'/jobs' and json.loads(body)['submissionId']==self.primary.submission['submissionId']:
                submissions.append(bytes(body))
                if not lost[0] and response.status==201:
                    lost[0] = True
                    return Response(503,{'error':'synthetic_lost_submission_ack'})
            return response
        self.runtime.api.handle = lose_committed_ack
        with self.assertRaises(HTTPError) as missing:
            self.peer_call(self.primary,'submit')
        try:self.assertEqual(missing.exception.code,409)
        finally:missing.exception.close()
        committed = self.store.find_submission(self.primary.submission['submissionId'],environment='production')
        self.assertIsNotNone(committed)
        self.job = self.peer_call(self.primary,'submit')['jobId']
        self.assertEqual(self.job,committed['jobId']); self.assertEqual(len(submissions),2)
        self.assertEqual(submissions[0],submissions[1])
        second = self.peer_call(self.second,'submit')['jobId']
        cached,headers = self.metadata('/capabilities',PRODUCTION)
        self.assertEqual(headers.get('X-LTDS-Catalog-Source'),'cached')
        self.assertEqual(cached['capabilityFingerprint'],self.primary.submission['capabilityFingerprint'])
        with self.queue._db() as db:
            order = [(row['id'],row['environment'],row['status']) for row in db.execute(
                "SELECT * FROM jobs ORDER BY CASE environment WHEN 'production' THEN 0 ELSE 1 END,seq")]
            self.assertEqual(order,[(self.job,'production','queued'),(second,'production','queued'),(staging,'staging','queued')])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_option_evidence').fetchone()[0],3)
            baseline = db.execute('SELECT job_id,payload,digest FROM reference_option_evidence ORDER BY job_id').fetchall()
            baseline = [tuple(row) for row in baseline]
            for _,payload,_ in baseline:
                selected = json.loads(payload)['selectedDefinitions']
                self.assertEqual([definition['name'] for definition in selected],[self.selected_name])
        self.assertEqual(list(self.ledger.root.glob('jobs/*/*.input')),[])
        for peer in self.peers:
            with closing(sqlite3.connect(peer.viewer_state/'broker.sqlite')) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_grants').fetchone()[0],0)
        # The actual canonical service attempts readiness while BOTH engines
        # are absent, yet performs no claim, transfer or engine initialization.
        # This counter observes real authenticated TLS API requests and delegates
        # every response unchanged; it is not a transport or readiness stub.
        authority_reads = []
        handle = self.runtime.api.handle
        def observed(method,path,*args,**kwargs):
            if path=='/ltds/worker/v1/authority': authority_reads.append(path)
            return handle(method,path,*args,**kwargs)
        self.runtime.api.handle = observed
        self.start_service()
        until = time.monotonic()+4
        while len(authority_reads)<2 and time.monotonic()<until:self.stop.wait(.02)
        self.assertGreaterEqual(len(authority_reads),2)
        with self.driver._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM driver_jobs').fetchone()[0],0)
        with self.queue._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],0)
        self.assertEqual(list(self.ledger.root.glob('jobs/*/*.input')),[])
        self.runtime.close(); self.queue.close()
        self.queue = Queue(self.root/'coordinator',reference_enabled=True,node_ids=('cluster','main'),
            treedar_node_ids=('main',),catalog_admission=True)
        self.assertEqual(self.queue.authority_id,self.authority)
        self.runtime = ReferenceRuntime(self.queue,self.config,self.settings); self.runtime.start(); self.store = self.runtime.store
        _,headers = self.metadata('/capabilities',PRODUCTION)
        self.assertEqual(headers.get('X-LTDS-Catalog-Source'),'cached')
        with self.queue._db() as db:
            self.assertEqual([tuple(row) for row in db.execute('SELECT job_id,payload,digest FROM reference_option_evidence ORDER BY job_id')],baseline)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],0)
        self.native_fixture = self.native('cluster',ports[0]); self.main_fixture = self.native('main',ports[1])
        started = self.wait_stage('executing')
        self.assertEqual(started['claim']['environment'],'production')
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('running','held')").fetchone()[0],1)
        request = Request(self.control_origin+'/ltds/worker/v1/next',headers={
            'Authorization':'Bearer '+MAIN_WORKER,'X-LTDS-Option-Evidence':'1'})
        with build_opener(ProxyHandler({}),HTTPSHandler(context=self.tls)).open(request,timeout=5) as response:
            self.assertEqual(json.loads(response.read(8192)),{'job':None,'execution':None})
        # An actual maintenance drain holds the remaining metadata without
        # redirecting those jobs to this first project's distinct source broker.
        now = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        node = self.queue.get_node('cluster')
        self.queue.schedule_node('cluster',dict(expectedRevision=node['revision'],drainAt=now,
            notBefore=now,targetImage='sha256:'+'1'*64,afterJobId=self.job))
        with self.native_fixture._db() as db: db.execute('UPDATE tasks SET status=40 WHERE uuid=?',(self.job,))
        self.wait_stage('awaiting_acceptance')
        self.assertEqual(self.native_fixture.counts().get('remove',0),0)
        result = self.call('ingest'); self.assertTrue(result['durableCopy']); self.assertTrue(result['acknowledged']); self.assertFalse(result['published'])
        self.wait_stage('completed')
        until = time.monotonic()+5
        while list(self.ledger.root.glob('jobs/*/all.zip')) and time.monotonic()<until: self.stop.wait(.02)
        self.assertEqual(list(self.ledger.root.glob('jobs/*/all.zip')),[])
        inputs = list(self.ledger.root.glob('jobs/*/*.input')); self.assertEqual(len(inputs),1); self.assertEqual(inputs[0].read_bytes(),b'abc')
        self.assertEqual((self.primary.viewer_state/'input/Survey_001.JPG').read_bytes(),b'abc')
        self.assertEqual(list((self.primary.viewer_state/'results').glob('*.zip'))[0].read_bytes(),ARCHIVE)
        self.assertEqual({name:self.native_fixture.counts().get(name,0) for name in ('initialize','upload','commit','download','remove')},
            dict(initialize=1,upload=1,commit=1,download=1,remove=1))
        with self.queue._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_claims').fetchone()[0],1)
            self.assertEqual([(row['id'],row['status']) for row in db.execute("SELECT id,status FROM jobs WHERE status='queued' ORDER BY seq")],
                [(staging,'queued'),(second,'queued')])
        with closing(sqlite3.connect(self.primary.viewer_state/'acceptance.sqlite')) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM acceptances').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT acked FROM ack_outbox').fetchone()[0],1)
