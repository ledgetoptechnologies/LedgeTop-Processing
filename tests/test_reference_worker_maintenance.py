"""Actual canonical service/leases/TLS/Queue; synthetic Docker/ingress boundary.

Retained completed metadata is synthetic, not a scientific/NAS acceptance claim.
"""
from contextlib import ExitStack
from dataclasses import asdict,replace
from datetime import datetime,timedelta,timezone
import copy
import hashlib
from http.server import ThreadingHTTPServer
import importlib.util
import io
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
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import redirect_stderr,redirect_stdout
import uuid
from urllib.parse import urlsplit

from ops import reference_worker_maintenance as m
from ops import reference_worker_daemon as daemon
from ops import node_maintenance as managed
from ops import update_maintenance_adapter as supervisor_adapter
from ops.node_ingress import save
from ops.native_node_fixture import NativeNodeFixture
from processing.app import make_handler
from processing.config import Config
from processing.node_gate import NodeGate,GateBusy
from processing.odx_smoke_worker import Client
from processing.reference_worker_api import ReferenceWorkerAPI
from processing.reference_store import ReferenceStore,PROFILE
from processing.reference_delivery import DeliveryReceiver
from processing.reference_http import create_reference_http_server
from processing.update_registry import _run
from processing.viewer_service import NativeEngine
from tests.test_service import start_server,TOKEN
from tests import test_reference_parent_guard as tls
from tests.test_reference_worker_service import private_config,free_port

OLD='sha256:'+'1'*64
NEW='sha256:'+'2'*64
THIRD='sha256:'+'3'*64
WORKER_IMAGE='sha256:'+'4'*64
WORKER_CID='5'*64

@unittest.skipUnless(os.name=='posix','actual Linux canonical leases/worker TLS')
class WorkerMaintenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):tls.ParentGuardTests.setUpClass.__func__(cls)

    def setUp(self):
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.temp=self.stack.enter_context(tempfile.TemporaryDirectory());self.root=Path(self.temp);self.root.chmod(0o700)
        self.managed=self.root/'managed';self.managed.mkdir(mode=0o700)
        self.config_dir=self.root/'private';self.config_dir.mkdir(mode=0o700)
        for source,name in ((self.cert,'ca.crt'),(self.cert,'worker.crt'),(self.key,'worker.key')):
            target=self.config_dir/name;shutil.copyfile(source,target);target.chmod(0o600)
        self.gate_root=self.root/'gates'/'one-engine';self.gate_root.parent.mkdir(mode=0o700)
        self.stack.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=self.gate_root))
        self.native=NativeNodeFixture(self.root/'native.sqlite',engine_version='synthetic-v1',synthetic_enabled=True).start();self.stack.callback(self.native.close)
        self.bearer='synthetic-worker-'+'a'*32;self.reader='synthetic-reader-'+'b'*32
        worker_port,private_port=free_port(),free_port()
        reference=dict(schemaVersion=1,listener=dict(host='127.0.0.1',port=private_port,certificate=str(self.cert),privateKey=str(self.key),clientCA=str(self.cert)),
            viewerBearers=dict(staging='synthetic-viewer-'+'c'*32),
            workers=dict(cluster=dict(bearer=self.bearer,workerId='worker',instanceId='instance',resultOrigin='https://127.0.0.1:'+str(worker_port),resultBearer=self.reader),
                main=dict(bearer='synthetic-main-worker-'+'d'*32,workerId='main-worker',instanceId='main-instance',resultOrigin='https://127.0.0.1:19979',resultBearer='synthetic-main-reader-'+'e'*32)),
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=4194304),
            routingBindings={node:dict(engineId='test-engine' if node=='cluster' else 'main-engine',gateGeneration=1,engineVersion='synthetic-v1') for node in ('cluster','main')},
            catalogAdmission=dict(schemaVersion=1,maxAgeSeconds=60),runtimeVersionPublication=dict(schemaVersion=1))
        self.reference_path=self.root/'coordinator-private.json';save(self.reference_path,reference)
        self.coordinator_config=Config(TOKEN,self.native.origin,self.native.origin,1000,20000,prototype_state_dir=str(self.root/'queue'),prototype_node_ids=('cluster','main'),
            treedar_node_ids=('main',),prototype_operator_token='synthetic-operator-'+'z'*32,reference_config_path=str(self.reference_path))
        handler=make_handler(self.coordinator_config)
        self.queue=handler.prototype_queue;self.stack.callback(self.queue.close);self.queue.allowed_profiles.add(PROFILE)
        self.control_server=start_server(handler);self.stack.callback(self.control_server.server_close);self.stack.callback(self.control_server.shutdown)
        self.api=Client('http://127.0.0.1:'+str(self.control_server.server_port),TOKEN)
        self.operator=Client(self.api.origin,'synthetic-operator-'+'z'*32)
        self.runtime=handler.reference_listener;self.runtime.start();self.store=self.runtime.store
        self.stack.callback(self.close_private)
        with NodeGate('test-engine') as gate:gate.provision('cluster',self.api.origin,self.queue.authority_id)
        self.worker_state=self.root/'worker-state';self.scratch=self.root/'worker-scratch'
        self.config=private_config(self.root,self.config_dir/'worker.crt',self.config_dir/'worker.key',NativeEngine(self.native.origin),self.queue.authority_id,
            coordinator='https://127.0.0.1:'+str(private_port),broker='https://127.0.0.1:'+str(private_port),
            worker='worker',instance='instance',bearer=self.bearer,reader=self.reader,gate='test-engine',gate_origin=self.api.origin)
        self.config.update(stateRoot=str(self.worker_state),scratchRoot=str(self.scratch),caFile=str(self.config_dir/'ca.crt'),listenPort=worker_port,cleanupEnabled=False,engineCleanupEnabled=False,optionEvidence=dict(schemaVersion=1))
        self.config_path=self.config_dir/'worker.json';save(self.config_path,self.config)
        self.worker_running=False;self.events=[];self.loss=None;self.service=None;self.worker_thread=None
        self.loss_before=None
        self.start_worker();self.stack.callback(self.stop_worker)
        self.adapter=Backend(self)
        self.ingress=Ingress(self)
        save(self.managed/'compose.json',dict(services=dict(node=dict(image=OLD))))
        save(self.ingress.root/'state.json',dict(state='open',containerId='owned-proxy',oldWorkers=[],expectedBackendImage=OLD))
        self.recipe=self.worker_inspect()
        cert_digest=hashlib.sha256(ssl.PEM_cert_to_DER_cert(self.cert.read_text())).hexdigest()
        self.policy=m.WorkerPolicy('cluster',self.queue.authority_id,'test-engine',1,WORKER_CID,'canonical-worker',WORKER_IMAGE,m.worker_recipe_sha256(*(self.recipe[k] for k in ('image','config','host','mounts'))),str(self.config_dir),
            m.digest({k:v for k,v in self.config.items() if k!='engineVersion'}),hashlib.sha256(self.config_path.read_bytes()).hexdigest(),str(self.worker_state),str(self.scratch),str(self.gate_root),
            'https://127.0.0.1:'+str(self.config['listenPort']),cert_digest,cert_digest,(str(self.managed/'maintenance.json'),),container_config_directory=str(self.config_dir),timeout_seconds=3)
        settings=dict(nodeId='cluster',coordinator=self.api.origin,project='owned',service='node',expectedEngine='odm',uploadLayout='var-www-tmp-v1',processLayout='node-var-www-v1')
        save(self.managed/'maintenance-worker-v2.json',dict(schemaVersion=2,maintenance=settings,worker=asdict(self.policy)))
        self.stack.enter_context(patch.object(managed,'Ingress',return_value=self.ingress))
        def compose(args,receipt):self.adapter.args=args;return self.adapter
        self.stack.enter_context(patch.object(managed,'ComposeNode',side_effect=compose))
        def worker_compose(args,receipt,worker):
            self.adapter.args=args;self.adapter.worker_maintenance=worker;return self.adapter
        self.stack.enter_context(patch.object(m,'WorkerComposeNode',side_effect=worker_compose))
        original=m.WorkerMaintenance.__init__
        def constructor(controller,*args,**kwargs):original(controller,*args,**dict(kwargs,runner=self.runner))
        self.stack.enter_context(patch.object(m.WorkerMaintenance,'__init__',constructor))

    def close_private(self):
        self.runtime.close()

    def start_worker(self):
        config=daemon.load_config(str(self.config_path));driver,server=daemon.build_worker(config)
        self.driver=driver;self.service=daemon.WorkerService(driver,server,poll_seconds=.1)
        self.stop=threading.Event();self.outcomes=[]
        self.worker_thread=threading.Thread(target=lambda:self.outcomes.append(self.service.run(self.stop)))
        self.worker_thread.start();deadline=time.monotonic()+3
        while self.service.health()['state']!='ready' and time.monotonic()<deadline:time.sleep(.01)
        self.assertEqual(self.service.health()['state'],'ready');self.worker_running=True

    def stop_worker(self):
        if self.worker_thread is not None and self.worker_thread.is_alive():
            self.stop.set();self.worker_thread.join(8);self.assertFalse(self.worker_thread.is_alive());self.assertEqual(self.outcomes,[0])
        self.worker_running=False

    def worker_inspect(self):
        return dict(id=WORKER_CID,name='/canonical-worker',image=WORKER_IMAGE,
            config=dict(User='1000:1000',Entrypoint=['python','-m','ops.reference_worker_daemon'],Cmd=['--config',str(self.config_path)],StopTimeout=-1),
            host=dict(NetworkMode='host',ReadonlyRootfs=True,Privileged=False,UsernsMode='',AutoRemove=False),
            mounts=[dict(Type='bind',Source=source,Destination=destination,RW=writable) for source,destination,writable in
                ((str(self.config_dir),str(self.config_dir),False),(str(self.worker_state),str(self.worker_state),True),(str(self.scratch),str(self.scratch),True),(str(self.gate_root),'/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(b'test-engine').hexdigest(),True))],
            running=self.worker_running,pid=123 if self.worker_running else 0,exitCode=0)

    def runner(self,command,*,timeout,max_bytes):
        if command[0]=='/usr/bin/python3':
            # Test image's Python path differs from the actual host's fixed path.
            return _run([sys.executable,*command[1:]],timeout=timeout,max_bytes=max_bytes)
        self.assertEqual(command[:3],['/usr/bin/docker','--host','unix:///run/docker.sock'])
        args=command[3:];result=''
        if self.loss_before==args[0]:self.loss_before=None;raise OSError('synthetic unknown before local call')
        if args[0]=='info':result='test-engine'
        elif args[0]=='inspect':
            if args[-1]==WORKER_CID:result=json.dumps(self.worker_inspect())
            else:
                self.assertEqual(args[-1],self.adapter.cid)
                result=json.dumps(dict(id=self.adapter.cid,image=self.adapter.current,running=True,ports={'3000/tcp':[dict(HostIp='127.0.0.1',HostPort=str(urlsplit(self.native.origin).port))]},labels={'com.docker.compose.project':'owned','com.docker.compose.service':'node','com.docker.compose.project.config_files':str(self.managed/'compose.json')}))
        elif args[0]=='stop':
            self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
            with self.assertRaises(GateBusy):
                with NodeGate('test-engine'):pass
            self.events.append('stop');self.stop_worker();result=WORKER_CID
        elif args[0]=='start':
            self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
            with NodeGate('test-engine') as gate:self.assertIsNone(gate.snapshot()['reservation'])
            self.events.append('start');self.start_worker();result=WORKER_CID
        elif args[0]=='exec':
            config=json.loads(self.config_path.read_bytes());targets=json.loads(args[-1]);mapping={
                '/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(b'test-engine').hexdigest():self.gate_root}
            records=[]
            for target in targets:
                value=Path(mapping.get(target,target)).lstat();records.append([value.st_dev,value.st_ino,value.st_uid,value.st_gid,value.st_mode&0o7777])
            result=json.dumps(records)
        else:raise AssertionError(args)
        if self.loss==args[0]:self.loss=None;raise OSError('synthetic reply lost')
        return subprocess.CompletedProcess(command,0,(result+'\n').encode(),b'')

    def plan(self,target=NEW):
        now=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        request=dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,expectedRevision=self.queue.get_node('cluster')['revision'],targetImage=target,drainAt=now,notBefore=now,afterJobId=None)
        receipt=self.operator.json('POST','/v1/prototype/nodes/cluster/schedule-owned',request)['receipt']
        self.owned_schedule_request_id=request['requestId']
        folder=self.managed/('op-'+request['requestId']);folder.mkdir(mode=0o700)
        return folder,receipt

    def run_update(self,target=NEW):
        folder,plan=self.plan(target)
        result=managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id,expected_plan_id=plan['planId'],expected_target=target)
        return result,folder

    def restart_coordinator(self):
        authority=self.queue.authority_id;port=self.control_server.server_port
        self.control_server.shutdown();self.control_server.server_close();self.runtime.close();self.queue.close()
        handler=make_handler(self.coordinator_config)
        self.queue=handler.prototype_queue;self.assertEqual(self.queue.authority_id,authority);self.stack.callback(self.queue.close)
        self.runtime=handler.reference_listener;self.runtime.start();self.store=self.runtime.store
        self.control_server=ThreadingHTTPServer(('127.0.0.1',port),handler)
        thread=threading.Thread(target=self.control_server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        self.stack.callback(thread.join,3);self.stack.callback(self.control_server.server_close);self.stack.callback(self.control_server.shutdown)

    def test_actual_factory_thread_stop_rebind_restart_and_owned_finish(self):
        before_inode=self.config_path.stat().st_ino
        result,folder=self.run_update()
        self.assertEqual(result['status'],'succeeded');self.assertEqual(self.events,['stop','backend','start','open'])
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        self.assertNotEqual(self.config_path.stat().st_ino,before_inode)
        self.assertEqual(daemon.load_config(str(self.config_path))['engineVersion'],'synthetic-v2')
        self.assertEqual(self.driver.worker.version,'synthetic-v2')
        self.assertEqual(self.driver.path.parent,self.worker_state)
        state=json.loads((folder/'managed-update.json').read_text());self.assertEqual(state['worker']['phase'],'finish_pending')
        self.assertEqual(m.read_json(self.managed/'worker-current.json')['config'],state['worker']['afterConfig'])
        publication=state['worker']['runtimePublication'];current=self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version')['runtime']
        self.assertEqual(publication['phase'],'committed');self.assertEqual(current['nodeEpoch'],1)
        self.assertEqual(current['publicationRequestId'],publication['request']['requestId']);self.assertEqual(current['targetImage'],NEW)

    def test_actual_worker_aware_supervisor_adapter_schedule_run_and_lookup(self):
        save(self.ingress.root/'gateway.json',self.ingress.config)
        receipts=self.managed/'adapter-receipts';operation='a'*64
        with patch.object(supervisor_adapter,'Ingress',return_value=self.ingress),patch.object(supervisor_adapter,'ComposeNode',side_effect=lambda args,work:self.bind_adapter(args)):
            adapter=supervisor_adapter.UpdateMaintenanceAdapter(self.managed,state_path=self.managed/'adapter.sqlite',receipt_root=receipts,
                api=self.api,operator=self.operator,node_id='cluster',authority_id=self.queue.authority_id,engine_id='test-engine',
                adapter_id='canonical-worker-maintenance',window=supervisor_adapter.ScheduleWindow(0,0,0,30),worker_aware=True)
            (receipts/operation).mkdir(mode=0o700)
            for kind in ('schedule','run'):(receipts/operation/kind).mkdir(mode=0o700)
            request=dict(schemaVersion=1,operationId=operation,nodeId='cluster',engineId='test-engine',authorityId=self.queue.authority_id,
                candidateId='b'*64,repository='registry.example/namespace/engine',manifestDigest='sha256:'+'d'*64,configDigest=NEW,
                localImageId=NEW,platform='linux/amd64',previousImageId=OLD,hostGeneration=0,supervisorPolicySha256='c'*64,
                qualifierId='separate-qualifier',qualificationPolicySha256='e'*64,qualificationSha256='f'*64,receiptDirectory=str(receipts/operation/'schedule'))
            request['requestSha256']=supervisor_adapter.digest(request)
            plan=adapter.schedule(request)
            run=dict(request,plan=plan,receiptDirectory=str(receipts/operation/'run'));run.pop('requestSha256');run['requestSha256']=supervisor_adapter.digest(run)
            self.assertTrue(adapter.ready_to_run(run));result=adapter.run(run)
            self.assertTrue(result['coordinatorFinished']);self.assertEqual(adapter.lookup_completion(run),result)
            with adapter._db() as db:wire=json.loads(db.execute('SELECT wire FROM schedule_intents').fetchone()[0])
            state=m.read_json(receipts/operation/'run'/'managed-update.json')
            self.assertEqual(state['worker']['runtimePublication']['request']['ownedScheduleRequestId'],wire['requestId'])
            self.assertEqual(self.events,['stop','backend','start','open']);self.assertEqual(self.queue.get_node('cluster')['state'],'active')

    def bind_adapter(self,args):
        self.adapter.args=args;return self.adapter

    def test_owned_revision_changes_after_prepare_before_docker_up_holds(self):
        self.change_after_prepare=True
        folder,plan=self.plan()
        with self.assertRaises(m.WorkerMaintenanceHeld):
            managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id,expected_plan_id=plan['planId'],expected_target=NEW)
        self.assertEqual(self.events,['stop']);self.assertEqual(self.adapter.calls,[])
        self.assertEqual(self.adapter.current,OLD)
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(json.loads((folder/'update.json').read_text())['stage'],'replace_pending')
        self.assertEqual(json.loads(self.config_path.read_text())['engineVersion'],'synthetic-v1')

    def test_full_config_changes_after_prepare_before_docker_up_hold(self):
        self.config_change_after_prepare=True
        folder,plan=self.plan()
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'configuration_changed_before_backend'):
            managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id,
                expected_plan_id=plan['planId'],expected_target=NEW)
        self.assertEqual(self.events,['stop']);self.assertEqual(self.adapter.calls,[]);self.assertEqual(self.adapter.current,OLD)
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(m.read_json(folder/'update.json')['stage'],'replace_pending')

    def test_foreign_owned_schedule_id_cannot_stop_worker(self):
        folder,plan=self.plan()
        with self.assertRaises(Exception):
            managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=str(uuid.uuid4()),
                expected_plan_id=plan['planId'],expected_target=NEW)
        self.assertTrue(self.worker_running);self.assertEqual(self.events,[]);self.assertEqual(self.adapter.calls,[])

    def test_two_sequential_updates_preserve_completed_history_and_config_base(self):
        job=str(uuid.uuid4());history=dict(claim=dict(jobId=job,generation=1,environment='staging',manifestSha256='a'*64,pipelineDigest='b'*64),
            originalResultManifest=dict(engineVersions={'odm':'synthetic-v1'},retained=True))
        with self.driver._lock:
            with self.driver._db() as db:db.execute('INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,?,?)',(job,'historical-claim','completed',json.dumps(history)))
        old_base=m.digest({k:v for k,v in self.config.items() if k!='engineVersion'})
        first,_=self.run_update(NEW);self.assertEqual(first['status'],'succeeded')
        self.restart_coordinator()
        second,folder=self.run_update(THIRD);self.assertEqual(second['status'],'succeeded')
        self.assertEqual(self.events,['stop','backend','start','open']*2)
        with self.driver._db() as db:
            self.assertEqual(json.loads(db.execute('SELECT data FROM driver_jobs WHERE job=?',(job,)).fetchone()[0]),history)
            self.assertEqual(db.execute('SELECT pin FROM driver_engine_pin WHERE singleton=1').fetchone()[0],'synthetic-v3')
        config=daemon.load_config(str(self.config_path));self.assertEqual(config['engineVersion'],'synthetic-v3')
        self.assertEqual(m.digest({k:v for k,v in config.items() if k!='engineVersion'}),old_base)
        self.assertEqual(self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version')['runtime']['nodeEpoch'],2)

    def test_same_version_image_upgrade_still_publishes_new_epoch(self):
        self.next_version='synthetic-v1'
        result,folder=self.run_update();self.assertEqual(result['status'],'succeeded')
        state=m.read_json(folder/'managed-update.json');publication=state['worker']['runtimePublication']
        self.assertEqual(publication['request']['expectedVersion'],publication['request']['newVersion'])
        current=self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version')['runtime']
        self.assertEqual((current['targetImage'],current['engineVersion'],current['nodeEpoch']),(NEW,'synthetic-v1',1))

    def test_lost_runtime_publication_reply_reconciles_exact_receipt_without_second_post(self):
        original=self.operator.json;posts=[]
        def lost(method,route,*args,**kwargs):
            value=original(method,route,*args,**kwargs)
            if method=='POST' and route.endswith('/runtime-version'):
                posts.append(args[0]['requestId']);raise OSError('synthetic committed reply lost')
            return value
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
        self.assertEqual(state['worker']['runtimePublication']['phase'],'pending')
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating');self.assertEqual(self.events,['stop','backend','start'])
        result=managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(result['status'],'succeeded');self.assertEqual(len(posts),1)
        self.assertEqual(m.read_json(folder/'managed-update.json')['worker']['runtimePublication']['request']['requestId'],posts[0])
        self.assertEqual(self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version')['runtime']['nodeEpoch'],1)
        self.assertEqual(self.events,['stop','backend','start','open'])

    def test_restart_during_update_retains_publication_but_never_reopens_from_history(self):
        original=self.operator.json
        def lost(method,route,*args,**kwargs):
            value=original(method,route,*args,**kwargs)
            if method=='POST' and route.endswith('/runtime-version'):raise OSError('synthetic committed reply lost')
            return value
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));intent=m.read_json(folder/'managed-update.json')['worker']['runtimePublication']
        request_id=intent['request']['requestId'];authority=self.queue.authority_id
        self.restart_coordinator()
        self.assertEqual(self.queue.authority_id,authority);self.assertEqual(self.queue.get_node('cluster')['state'],'held')
        with self.assertRaisesRegex(Exception,'no longer updating'):
            managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        published=self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version/receipts/'+request_id)['receipt']
        self.assertEqual(published['requestId'],request_id);self.assertEqual(published['epoch'],1)
        self.assertEqual(self.events,['stop','backend','start'])
        self.assertEqual(m.read_json(self.ingress.root/'state.json')['state'],'closed')
        self.assertEqual(m.read_json(folder/'managed-update.json')['worker']['runtimePublication'],intent)

    def test_uncommitted_runtime_publication_never_reposts_or_finishes(self):
        original=self.operator.json;posts=[]
        def lost(method,route,*args,**kwargs):
            if method=='POST' and route.endswith('/runtime-version'):
                posts.append(args[0]['requestId']);raise OSError('synthetic uncommitted request')
            return original(method,route,*args,**kwargs)
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));before=m.read_json(folder/'managed-update.json')['worker']['runtimePublication']
        with self.assertRaises(Exception):managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(m.read_json(folder/'managed-update.json')['worker']['runtimePublication'],before)
        self.assertEqual(len(posts),1);self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        self.assertEqual(self.operator.json('GET','/v1/prototype/nodes/cluster/runtime-version')['runtime']['nodeEpoch'],0)
        self.assertEqual(self.events,['stop','backend','start'])

    def test_pending_acceptance_is_not_stopped_discarded_or_updated(self):
        job=str(uuid.uuid4())
        with self.driver._lock:
            with self.driver._db() as db:db.execute('INSERT INTO driver_jobs(job,claim_request,stage,data) VALUES(?,?,?,?)',(job,'pending-claim','awaiting_acceptance',json.dumps(dict(claim=dict(claimToken='synthetic-token')))))
        with self.assertRaises(m.WorkerMaintenanceHeld):self.run_update()
        self.assertEqual(self.events,[]);self.assertTrue(self.worker_running)
        self.assertEqual(self.queue.get_node('cluster')['state'],'updating')
        with self.driver._db() as db:self.assertEqual(db.execute('SELECT stage FROM driver_jobs WHERE job=?',(job,)).fetchone()[0],'awaiting_acceptance')

    def test_pending_failure_outbox_holds_after_graceful_stop_without_backend_change(self):
        from processing.reference_failure import make_failure
        failure=make_failure(job_id=str(uuid.uuid4()),generation=1,environment='staging',input_manifest_sha256='a'*64,pipeline_digest='b'*64,terminal_state='failed',error_code='synthetic_failure')
        outbox=self.driver.failure_outbox.persist(failure,engine_terminal=True)
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'pending_outbox'):self.run_update()
        self.assertEqual(self.events,['stop']);self.assertEqual(self.adapter.current,OLD)
        self.assertEqual(self.driver.failure_outbox.get(outbox)['state'],'pending')

    def test_uncertain_reader_pin_holds_and_is_not_force_released(self):
        with self.driver.worker.ledger._transaction() as db:
            db.execute("INSERT INTO pins VALUES('retained','object','synthetic-reader','read','uncertain')")
        with self.assertRaises((m.WorkerMaintenanceHeld,ValueError)):self.run_update()
        self.assertEqual(self.adapter.calls,[])
        with self.driver.worker.ledger._transaction() as db:self.assertEqual(db.execute("SELECT state FROM pins WHERE reference='synthetic-reader'").fetchone()[0],'uncertain')

    def test_lost_stop_reply_only_reports_original_exact_stopped_container(self):
        self.loss='stop'
        with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
        controller=m.WorkerMaintenance(self.managed,self.policy,self.api)
        try:self.assertEqual(controller.lookup_stop(state),dict(committed=True,containerId=WORKER_CID))
        finally:controller.close()
        with self.assertRaises(Exception):managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(self.events,['stop']);self.assertEqual(self.adapter.calls,[])

    def test_unknown_uncommitted_stop_never_replays(self):
        self.loss_before='stop'
        with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
        controller=m.WorkerMaintenance(self.managed,self.policy,self.api)
        try:self.assertFalse(controller.lookup_stop(state)['committed'])
        finally:controller.close()
        self.assertTrue(self.worker_running);self.assertEqual(self.events,[]);self.assertEqual(self.adapter.calls,[])

    def test_lost_start_reply_reconciles_running_original_without_second_start(self):
        self.loss='start'
        with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'))
        result=managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(result['status'],'succeeded');self.assertEqual(self.events,['stop','backend','start','open'])

    def test_unknown_uncommitted_start_holds_without_replay(self):
        self.loss_before='start'
        with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'))
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'start_outcome_unresolved'):managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(self.events,['stop','backend']);self.assertFalse(self.worker_running)

    def test_lost_atomic_replace_reply_uses_exact_published_inode_without_second_replace(self):
        original=os.replace;renames=[]
        def lose(source,target):
            original(source,target)
            if Path(target)==self.config_path:
                renames.append((source,target));raise OSError('synthetic rename reply lost')
        with patch.object(m.os,'replace',side_effect=lose):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));published=self.config_path.stat().st_ino
        result=managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(result['status'],'succeeded');self.assertEqual(len(renames),1)
        self.assertEqual(self.config_path.stat().st_ino,published);self.assertEqual(len(self.adapter.calls),1)

    def test_uncommitted_atomic_replace_holds_without_replacing_or_cleaning_temp(self):
        original=os.replace
        def lose(source,target):
            if Path(target)==self.config_path:raise OSError('synthetic rename not sent')
            return original(source,target)
        old=self.config_path.read_bytes()
        with patch.object(m.os,'replace',side_effect=lose):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'))
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'rebind_outcome_unresolved'):managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(self.config_path.read_bytes(),old);self.assertEqual(len(list(self.config_dir.glob('worker-*.tmp'))),1)
        self.assertEqual(self.events,['stop','backend'])

    def test_lost_finish_reply_and_readback_only_reconcile_original_finish(self):
        original_post=self.operator.json;original_get=self.api.json;posts=[];lose=[False]
        def post(method,route,payload=None,**kwargs):
            value=original_post(method,route,payload,**kwargs)
            if method=='POST' and route.endswith('/finish'):
                posts.append(copy.deepcopy(payload));lose[0]=True;raise OSError('synthetic finish ack lost')
            return value
        def get(method,route,*args,**kwargs):
            if lose[0] and route.endswith('/nodes/cluster'):
                lose[0]=False;raise OSError('synthetic finish lookup lost')
            return original_get(method,route,*args,**kwargs)
        with patch.object(self.operator,'json',side_effect=post),patch.object(self.api,'json',side_effect=get):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'));self.assertEqual(self.queue.get_node('cluster')['state'],'active')
        with patch.object(self.operator,'json',side_effect=post):
            result=managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(result['status'],'succeeded');self.assertEqual(len(posts),1)
        self.assertEqual(self.events,['stop','backend','start','open'])

    def test_uncommitted_finish_does_not_post_again_or_reopen_by_target_match(self):
        original=self.operator.json;posts=[]
        def lost(method,route,payload=None,**kwargs):
            if method=='POST' and route.endswith('/finish'):
                posts.append(payload);raise OSError('synthetic finish not committed')
            return original(method,route,payload,**kwargs)
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaises(OSError):self.run_update()
        folder=next(self.managed.glob('op-*'))
        with patch.object(self.operator,'json',side_effect=lost):
            with self.assertRaisesRegex(Exception,'finish requires exact read-only'):managed.run(self.managed,self.api,self.operator,folder,0,worker_aware=True,owned_schedule_request_id=self.owned_schedule_request_id)
        self.assertEqual(len(posts),1);self.assertEqual(self.queue.get_node('cluster')['state'],'updating')

    def test_immutable_config_field_and_replaced_publication_hold(self):
        config=json.loads(self.config_path.read_bytes());config['outputBudget']+=1;save(self.config_path,config)
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'immutable_worker_configuration'):self.run_update()
        self.assertEqual(self.events,[]);self.assertEqual(self.adapter.calls,[])


@unittest.skipUnless(os.name=='posix','actual archived Linux host loaders')
class WorkerMaintenanceConfigurationTests(unittest.TestCase):
    def legacy(self,name):
        directory=Path(__file__).parent/'fixtures/worker_maintenance_020c';raw=(directory/name).read_bytes()
        attrs=json.loads((directory/'fixture.attrs').read_text())
        self.assertEqual(attrs['sourceCommit'],'020c15c79577aff83af388fe476451ceca545518')
        self.assertEqual(hashlib.sha256(raw).hexdigest(),attrs['sha256'][name])
        module_name='worker_maintenance_legacy_'+name.replace('.','_')
        spec=importlib.util.spec_from_file_location(module_name,directory/name);module=importlib.util.module_from_spec(spec)
        old_path=list(sys.path);sys.modules[module_name]=module
        try:spec.loader.exec_module(module)
        finally:sys.path[:]=old_path
        return module

    def test_service_schema2_is_closed_and_actual_old020_loader_refuses(self):
        from ops import update_service
        from tests.test_update_service_cli import configuration
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);raw=configuration(root);raw['schemaVersion']=2;raw['maintenance']['workerAware']=True
            parsed=update_service.parse_configuration(raw);self.assertEqual(parsed.raw,raw)
            legacy=self.legacy('update_service.py')
            with self.assertRaises(legacy.ServiceConfigurationError):legacy.parse_configuration(raw)
            raw['maintenance']['workerAware']=False
            with self.assertRaises(update_service.ServiceConfigurationError):update_service.parse_configuration(raw)

    def test_old020_cli_cannot_read_new_profile_or_accept_new_flag(self):
        legacy=self.legacy('node_maintenance.py')
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);root.chmod(0o700)
            credentials=root/'control.env';credentials.write_text('PROCESSING_API_TOKEN=synthetic-api\nPROCESSING_PROTOTYPE_OPERATOR_TOKEN=synthetic-operator\n');credentials.chmod(0o600)
            save(root/'maintenance-worker-v2.json',dict(schemaVersion=2,maintenance={},worker={}))
            args=['legacy','--root',str(root),'--credentials',str(credentials),'status']
            with patch.object(sys,'argv',args),patch.object(legacy,'Client') as client,redirect_stdout(io.StringIO()):self.assertEqual(legacy.main(),2)
            client.assert_not_called()
            with patch.object(sys,'argv',args[:-1]+['--worker-aware','status']),redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as held:legacy.main()
            self.assertEqual(held.exception.code,2)


class Backend(m.WorkerComposeNode):
    def __init__(self,owner):
        self.owner=owner;self.current=OLD;self.cid='6'*64;self.calls=[]
        self.node=Client(owner.native.origin,'',node=True);self.args=None
    def inspect(self):return dict(Id=self.cid,Image=self.current)
    def idle(self):return self.node.json('GET','/task/list')==[] and self.node.json('GET','/info')['taskQueueCount']==0
    def image(self,image):self.owner.assertIn(image,(OLD,NEW,THIRD))
    def prepare(self,image):
        self.image(image)
        if getattr(self.owner,'change_after_prepare',False) and not self.owner.worker_running:
            with self.owner.queue._db() as db:db.execute("UPDATE nodes SET revision=revision+1 WHERE id='cluster'")
        if getattr(self.owner,'config_change_after_prepare',False) and not self.owner.worker_running:
            config=m.read_json(self.owner.config_path);config['engineVersion']='changed-after-prepare';save(self.owner.config_path,config)
        return ['synthetic-docker-up',image]
    def healthy(self,image):return image==self.current and self.idle()
    def serving(self,image):return image==self.current
    def docker(self,command,*,timeout=30):
        self.owner.assertEqual(command[0],'synthetic-docker-up');image=command[1]
        self.owner.assertFalse(self.owner.worker_running);self.owner.assertEqual(self.owner.queue.get_node('cluster')['state'],'updating')
        self.owner.events.append('backend');self.calls.append(image);self.current=image;self.cid=('7' if image==NEW else '8')*64
        self.owner.native.engine_version=getattr(self.owner,'next_version','synthetic-v2' if image==NEW else 'synthetic-v3')

class Ingress:
    def __init__(self,owner):
        self.owner=owner;self.root=owner.managed/'ingress';self.root.mkdir(mode=0o700)
        self.config=dict(backendPort=urlsplit(owner.native.origin).port,backendName='owned-backend',backendEngine='odm',engineId='test-engine',proxyName='owned-proxy',proxyImage='sha256:'+'9'*64,listenPort=3001)
    def docker(self,args):return json.dumps([dict(Id=self.owner.adapter.cid)])
    def inspect(self):return dict(Id='owned-proxy')
    def workers(self,item):return set()
    def backend(self,expected):
        self.owner.assertEqual(self.owner.adapter.current,expected);return self.owner.adapter.node.json('GET','/info')
    def http(self,port):
        state=json.loads((self.root/'state.json').read_text())
        return (200,self.backend(self.owner.adapter.current)) if state['state']=='open' else (503,None)
    def change(self,closed,expected=None,seconds=60):
        self.owner.assertEqual(self.owner.queue.get_node('cluster')['state'],'updating')
        if not closed:self.owner.assertTrue(self.owner.worker_running);self.owner.events.append('open')
        save(self.root/'state.json',dict(state='closed' if closed else 'open',containerId='owned-proxy',oldWorkers=[],expectedBackendImage=expected))
        return dict(state='closed' if closed else 'open',oldWorkersDrained=True,containerId='owned-proxy')
