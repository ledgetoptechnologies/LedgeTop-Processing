"""Disabled opt-in daemon against owned synthetic loopback services only."""
import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from ops.native_node_fixture import NativeNodeFixture
from processing import node_mutation_broker as mutation, node_reference_authority_session as authority, node_reference_plan as plans
from processing.authority_lease import AuthorityLease, record_required
from processing.broker_intent_fence import install_offline as install_broker_fence
from processing.node_reference_remote_fence import install_offline as install_remote_fence
from processing.node_gate import NodeGate, canonical_gate_path
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.reference_store import ReferenceStore
from processing.viewer_tasks import TaskStore


@unittest.skipUnless(os.name=='posix','owned POSIX fixture required')
class RemoteDaemonCandidateTests(unittest.TestCase):
    def _run_daemon_photo(self,terminal_code,*,disconnect=False):
        root=Path(tempfile.mkdtemp(prefix='lt-remote-daemon-e2e-')).resolve();root.chmod(0o700)
        print('retained synthetic remote daemon root:',root,flush=True)
        for name in ('state','scratch','leases','xdg'):(root/name).mkdir(mode=0o700)
        certs={}
        for name,subject in (('server','/CN=localhost'),('node','/CN=synthetic-node')):
            key,cert=root/(name+'.key'),root/(name+'.crt')
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj',subject,'-addext','subjectAltName=IP:127.0.0.1','-keyout',str(key),'-out',str(cert)],
                stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=10)
            key.chmod(0o600);certs[name]=(key,cert)
        server_key,server_cert=certs['server'];node_key,node_cert=certs['node']
        server_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(server_cert.read_text())).hexdigest()
        node_pin=hashlib.sha256(ssl.PEM_cert_to_DER_cert(node_cert.read_text())).hexdigest()
        secret=bytes.fromhex('12'*32);worker_token='SYNTHETIC-WORKER-'+'w'*36
        viewer_token='SYNTHETIC-VIEWER-'+'v'*36;fetch_token='SYNTHETIC-BROKER-'+'b'*36
        queue=engine=control=fetch=lease=process=listener=None
        control_thread=fetch_thread=None
        threads=[];stop=threading.Event();accepted=[];errors=[];log=None
        allow_receipt_retry=threading.Event()
        old_xdg=os.environ.get('XDG_STATE_HOME');os.environ['XDG_STATE_HOME']=str(root/'xdg')
        try:
            engine=NativeNodeFixture(root/'engine.sqlite',synthetic_enabled=True)
            engine.start()
            if terminal_code!=40:
                with sqlite3.connect(engine.path) as engine_db:
                    engine_db.execute(f"CREATE TRIGGER synthetic_terminal AFTER UPDATE OF committed ON tasks WHEN NEW.committed=1 BEGIN UPDATE tasks SET status={terminal_code} WHERE uuid=NEW.uuid; END")
            queue=Queue(root/'queue',reference_enabled=True);store=ReferenceStore(queue)
            TaskStore(queue,root/'legacy-viewer')
            lease=AuthorityLease(root/'leases',queue.authority_id);queue.authority_lease=lease;record_required(queue,lease)
            with queue._db() as db:
                install_broker_fence(db,enabled=True);install_remote_fence(db,enabled=True)
                plans.install_plan_binding_offline(db,node_id='cluster',authority_id=queue.authority_id,
                    engine_id='synthetic-engine',gate_generation=1,enabled=True)
                with patch.object(mutation,'REFERENCE_AUTHORITY_QUALIFIED',True):mutation.install_reference_completion_candidate(db,enabled=True)
            with patch.object(mutation,'REFERENCE_AUTHORITY_QUALIFIED',True):
                witness=mutation.ReferenceCoordinatorAuthority(queue)
            fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
            cap=fixture['capabilities']
            api=ReferenceControlAPI(store,viewer_bearers={viewer_token:'staging'},
                worker_bearers={worker_token:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},
                capability_fingerprint=cap['capabilityFingerprint'],options_fingerprint=cap['engines']['odm']['optionsFingerprint'])
            lost_receipt=threading.Event();first_receipts=[];replayed_receipts=[]
            if disconnect:
                from processing.reference_api import Response
                original_handle=api.handle
                def lose_once(method,path,headers,body=b'',*,protected_transport=False):
                    if (method=='POST' and path=='/ltds/worker/v1/results' and lost_receipt.is_set()
                            and not allow_receipt_retry.wait(8)):
                        return Response(503,{'error':'synthetic_retry_blocked'})
                    response=original_handle(method,path,headers,body,protected_transport=protected_transport)
                    if method=='POST' and path=='/ltds/worker/v1/results' and response.status==200 and not lost_receipt.is_set():
                        first_receipts.append(response.body)
                        lost_receipt.set()
                        return Response(503,{'error':'synthetic_lost_receipt'})
                    if method=='POST' and path=='/ltds/worker/v1/results' and response.status==200 and lost_receipt.is_set():
                        replayed_receipts.append(response.body)
                    return response
                api.handle=lose_once
            tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(server_cert,server_key)
            control=create_reference_http_server(api,'127.0.0.1',0,ssl_context=tls)
            control_thread=threading.Thread(target=control.serve_forever,kwargs={'poll_interval':0.05});control_thread.start();threads.append(control_thread)
            class Fetch(BaseHTTPRequestHandler):
                def log_message(self,*args):pass
                def do_GET(self):
                    self.connection.settimeout(2)
                    if self.headers.get('Authorization')!='Bearer '+fetch_token:
                        self.send_error(403);return
                    self.send_response(200);self.send_header('Content-Length','3');self.end_headers();self.wfile.write(b'abc')
            fetch=ThreadingHTTPServer(('127.0.0.1',0),Fetch);fetch.daemon_threads=False;fetch.block_on_close=True
            fetch.socket=tls.wrap_socket(fetch.socket,server_side=True)
            fetch_thread=threading.Thread(target=fetch.serve_forever,kwargs={'poll_interval':0.05});fetch_thread.start();threads.append(fetch_thread)
            mtls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);mtls.load_cert_chain(server_cert,server_key)
            mtls.load_verify_locations(cafile=str(node_cert));mtls.verify_mode=ssl.CERT_REQUIRED
            listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(4);listener.settimeout(0.2)
            remote_port=listener.getsockname()[1]
            def authority_loop():
                while not stop.is_set():
                    try:raw,_=listener.accept()
                    except socket.timeout:continue
                    except OSError:return
                    try:
                        if stop.is_set():return
                        raw.settimeout(3);channel=mtls.wrap_socket(raw,server_side=True);accepted.append(channel)
                        authority.serve_reference_operation(channel,witness=witness,secret=secret,peer_sha256=node_pin,timeout_seconds=10)
                    except Exception as exc:errors.append(type(exc).__name__)
                    finally:raw.close()
            authority_thread=threading.Thread(target=authority_loop);authority_thread.start();threads.append(authority_thread)
            with NodeGate('synthetic-engine') as gate:gate.provision('cluster','http://127.0.0.1:18080',queue.authority_id)
            (canonical_gate_path('synthetic-engine')/'mutation-broker').mkdir(mode=0o700)
            with socket.socket() as port:
                port.bind(('127.0.0.1',0));worker_port=port.getsockname()[1]
            config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),nodeId='cluster',workerId='worker',
                workerInstanceId='instance',authorityId=queue.authority_id,engineId='synthetic-engine',gateGeneration=1,
                gateCoordinatorOrigin='http://127.0.0.1:18080',coordinatorOrigin='https://127.0.0.1:'+str(control.server_port),
                brokerOrigin='https://127.0.0.1:'+str(fetch.server_port),engineOrigin=engine.origin,engineToken='',workerBearer=worker_token,
                resultReadBearer='SYNTHETIC-READER-'+'r'*36,caFile=str(server_cert),serverCertFile=str(server_cert),serverKeyFile=str(server_key),
                listenHost='127.0.0.1',listenPort=worker_port,outputBudget=1048576,safetyFloor=1024,transferTtl=240,cleanupGrace=3600,
                pollSeconds=0.2,engineVersion='synthetic-transport-only',shareResults=True,
                referenceRemoteBroker=dict(host='127.0.0.1',port=remote_port,epoch=queue.startup_id,peerSha256=server_pin,
                    caFile=str(server_cert),clientCertFile=str(node_cert),clientKeyFile=str(node_key),nodeMutationSecretHex=secret.hex(),
                    backendContainerId='a'*64,backendImageId='sha256:'+'b'*64,backendPort=int(engine.origin.rsplit(':',1)[1])))
            config_path=root/'worker.json';config_path.write_text(json.dumps(config),encoding='utf-8');config_path.chmod(0o600)
            raw=copy.deepcopy(fixture['submission']);raw['submissionId']=str(uuid.uuid4())
            obj=dict(id='image-'+raw['submissionId'],name='test.jpg',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='image',mediaType='image/jpeg')
            raw['manifest'].update(objects=[obj],objectCount=1,totalBytes=3,inputCounts=dict(images=1,lidarObjects=0,points=0))
            raw['manifest']['sha256']=manifest_sha256(raw['manifest'])
            job=store.submit(validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
            bootstrap="import sys; import ops.reference_worker_daemon as d; import processing.node_mutation_broker as b; import processing.node_reference_authority_session as s; import processing.reference_engine_worker as w; d.REFERENCE_REMOTE_DAEMON_QUALIFIED=True; b.QUALIFIED=True; s.REFERENCE_REMOTE_QUALIFIED=True; w.BROKER_WORKER_CANDIDATE_QUALIFIED=True; b.local_engine_identity=lambda deadline:'synthetic-engine'; b.local_backend_identity=lambda pin,deadline:None; sys.argv=['reference_worker_daemon','--odm-only','--config',sys.argv[1]]; raise SystemExit(d.main())"
            log=open(root/'daemon.log','wb')
            with patch.object(authority,'REFERENCE_REMOTE_QUALIFIED',True), patch.object(plans,'REFERENCE_PLAN_QUALIFIED',True):
                process=subprocess.Popen([os.environ.get('PYTHON','python3'),'-c',bootstrap,str(config_path)],stdout=log,stderr=log,env=dict(os.environ))
                until=time.monotonic()+35;granted=False;released=False;restarted=False
                while time.monotonic()<until:
                    if process.poll() is not None:raise AssertionError('synthetic daemon exited '+str(process.returncode))
                    state=store.get(job['jobId'],private=True)
                    if state['phase']=='awaiting_transfer' and not granted:
                        store.receive_transfer_grant(job['jobId'],dict(state['transferRequest'],jobId=job['jobId'],
                            environment='staging',objectIds=[obj['id']],token=fetch_token));granted=True
                    if disconnect and lost_receipt.is_set() and not restarted:
                        outbox_path=root/'state'/'outbox.sqlite'
                        with sqlite3.connect('file:'+str(outbox_path)+'?mode=ro',uri=True,timeout=1) as outbox_db:
                            delivery=outbox_db.execute('SELECT state FROM completions').fetchone()
                        if delivery and delivery[0]=='pending':
                            with NodeGate('synthetic-engine') as gate:self.assertIsNotNone(gate.snapshot()['reservation'])
                            process.kill();self.assertEqual(process.wait(timeout=5),-signal.SIGKILL)
                            with sqlite3.connect('file:'+str(outbox_path)+'?mode=ro',uri=True,timeout=1) as outbox_db:
                                self.assertEqual(outbox_db.execute('SELECT state FROM completions').fetchone()[0],'pending')
                            with queue._db() as db:
                                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_worker_receipts').fetchone()[0],1)
                            allow_receipt_retry.set()
                            process=subprocess.Popen([os.environ.get('PYTHON','python3'),'-c',bootstrap,str(config_path)],stdout=log,stderr=log,env=dict(os.environ))
                            restarted=True
                    if (terminal_code==40 and state['phase']=='awaiting_acceptance') or (terminal_code!=40 and state['status'] in ('failed','cancelled')):
                        with NodeGate('synthetic-engine') as gate:released=gate.snapshot()['reservation'] is None
                        if released:break
                    time.sleep(0.1)
                log.flush()
                self.assertTrue(released,'synthetic daemon did not reach durable delivery and release; log: '+(root/'daemon.log').read_text(errors='replace')[-3000:])
                if disconnect:
                    self.assertTrue(lost_receipt.is_set() and restarted)
                    self.assertEqual(len(first_receipts),1)
                    self.assertTrue(replayed_receipts)
                    self.assertEqual(replayed_receipts[0],first_receipts[0])
                self.assertEqual(engine.counts(),{'initialize':1,'upload':1,'commit':1,'download':1 if terminal_code==40 else 0})
                inputs=[path for path in (root/'scratch').rglob('*.input') if path.is_file()]
                archives=[path for path in (root/'scratch').rglob('all.zip') if path.is_file()]
                self.assertEqual(len(inputs),1);self.assertEqual(inputs[0].read_bytes(),b'abc')
                self.assertEqual(len(archives),1 if terminal_code==40 else 0)
                if terminal_code!=40:
                    self.assertEqual(state['status'],'failed' if terminal_code==30 else 'cancelled')
                    with queue._db() as db:
                        failure=db.execute('SELECT failure_json,delivery_state FROM reference_worker_failures WHERE job_id=?',(job['jobId'],)).fetchone()
                        self.assertEqual(failure['delivery_state'],'awaiting_viewer_failure_acceptance')
                        self.assertEqual(json.loads(failure['failure_json'])['terminalState'],'failed' if terminal_code==30 else 'cancelled')
                self.assertEqual(errors,[])
        finally:
            stop.set()
            allow_receipt_retry.set()
            cleanup_errors=[]
            def attempt(function):
                try:function()
                except BaseException as error:cleanup_errors.append(type(error).__name__)
            def stop_process():
                if process is not None and process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    try:process.wait(timeout=8)
                    except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
            attempt(stop_process)
            if listener is not None:attempt(listener.close)
            for channel in accepted:
                attempt(channel.close)
            if control is not None:
                if control_thread is not None and control_thread.is_alive():attempt(control.shutdown)
                attempt(control.server_close)
            if fetch is not None:
                if fetch_thread is not None and fetch_thread.is_alive():attempt(fetch.shutdown)
                attempt(fetch.server_close)
            if engine is not None:attempt(engine.close)
            for thread in threads:attempt(lambda thread=thread:thread.join(5))
            if lease is not None:attempt(lease.close)
            if queue is not None:attempt(queue.close)
            if log is not None:attempt(log.close)
            if old_xdg is None:os.environ.pop('XDG_STATE_HOME',None)
            else:os.environ['XDG_STATE_HOME']=old_xdg
            if cleanup_errors:self.fail('synthetic cleanup failed: '+','.join(cleanup_errors))
        self.assertTrue(all(not thread.is_alive() for thread in threads))

    def test_actual_daemon_photo_success(self):self._run_daemon_photo(40)
    def test_actual_daemon_photo_failure(self):self._run_daemon_photo(30)
    def test_actual_daemon_photo_cancelled(self):self._run_daemon_photo(50)
    def test_daemon_restart_after_lost_result_receipt(self):self._run_daemon_photo(40,disconnect=True)
