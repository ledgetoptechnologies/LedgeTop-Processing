"""NOT YET REVIEWED: real daemon synthetic outage and owned process-kill fixture.

Run only inside a separately reviewed isolated container and outer watchdog.
Keeps all fixture files. Uses no live engine, coordinator, or credentials.
"""
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
import sys
import tempfile
import threading
import time
import uuid
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

from ops.native_node_fixture import ARCHIVE
from processing.node_gate import NodeGate
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_protocol import manifest_sha256, validate_submission
from processing.reference_result_proxy import ResultProxy
from processing.reference_store import ReferenceStore


def wait_until(fn, *, seconds, message):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        value=fn()
        if value:return value
        time.sleep(.1)
    raise RuntimeError(message)


def engine_counts(path):
    if not path.exists():return {}
    db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)
    try:return dict(db.execute('SELECT action,COUNT(*) FROM calls GROUP BY action'))
    finally:db.close()


def outbox_row(path):
    if not path.exists():return None
    db=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)
    try:
        rows=db.execute("SELECT id,state,attempts,payload,digest,receipt FROM completions LIMIT 2").fetchall()
        return rows[0] if len(rows)==1 else None
    finally:db.close()


def main():
    root=Path(tempfile.mkdtemp(prefix='worker-process-outage-',dir=os.environ.get('SYNTHETIC_RETAINED_DIR','/tmp'))).resolve()
    root.chmod(0o700); print('RETAINED_SYNTHETIC_ROOT',root,flush=True)
    os.environ['XDG_STATE_HOME']=str(root/'xdg')
    for name in ('state','scratch'):(root/name).mkdir(mode=0o700)
    cert,key=root/'cert.pem',root/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),
                    '-out',str(cert),'-days','1','-subj','/CN=127.0.0.1',
                    '-addext','subjectAltName=IP:127.0.0.1'],check=True,timeout=15,
                   stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    key.chmod(0o600)
    engine=None;worker=None;queue=None;coordinator=None;coord_thread=None;broker=None;broker_thread=None
    try:
        engine=subprocess.Popen([sys.executable,'-m','tests.worker_process_outage_engine','--root',str(root)],
                                stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        print('OWNED_ENGINE_PID',engine.pid,flush=True)
        wait_until(lambda:(root/'engine-ready.json').is_file() and (root/'engine-ready.json').stat().st_size>0,
                   seconds=8,message='synthetic engine not ready')
        engine_info=json.loads((root/'engine-ready.json').read_text())
        if engine.poll() is not None:raise RuntimeError('synthetic engine exited')
        queue=Queue(root/'queue',reference_enabled=True);store=ReferenceStore(queue)
        fixture=json.loads(Path('contracts/reference-v1.contract.json').read_text()); cap=fixture['capabilities']
        worker_token='SYNTHETIC-WORKER-'+'w'*36;viewer_token='SYNTHETIC-VIEWER-'+'v'*36
        broker_token='SYNTHETIC-BROKER-'+'b'*36;reader_token='SYNTHETIC-READER-'+'r'*36
        api=ReferenceControlAPI(store,viewer_bearers={viewer_token:'staging'},
            worker_bearers={worker_token:dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},
            capability_fingerprint=cap['capabilityFingerprint'],
            options_fingerprint=cap['engines']['odm']['optionsFingerprint'])
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
        coordinator=create_reference_http_server(api,'127.0.0.1',0,ssl_context=tls)
        coordinator_port=coordinator.server_port
        coord_thread=threading.Thread(target=coordinator.serve_forever,kwargs={'poll_interval':.05})
        coord_thread.start()
        class Broker(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                if self.headers.get('Authorization')!='Bearer '+broker_token or not self.path.startswith('/api/processing/reference/v1/objects/'):
                    self.send_error(403);return
                self.send_response(200);self.send_header('Content-Length','3');self.end_headers();self.wfile.write(b'abc')
        broker=ThreadingHTTPServer(('127.0.0.1',0),Broker)
        broker.socket=tls.wrap_socket(broker.socket,server_side=True)
        broker_thread=threading.Thread(target=broker.serve_forever,kwargs={'poll_interval':.05});broker_thread.start()
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1',0));worker_port=reserve.getsockname()[1]
        origin='https://127.0.0.1:'+str(coordinator_port)
        client_tls=ssl.create_default_context(cafile=str(cert))
        api.viewer.result_proxy=ResultProxy({'cluster':'https://127.0.0.1:'+str(worker_port)},
            worker_bearers={'cluster':reader_token},admit_read=lambda _binding:nullcontext(),ssl_context=client_tls)
        with NodeGate('synthetic-engine') as gate:gate.provision('cluster','http://127.0.0.1:18080',queue.authority_id)
        config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),nodeId='cluster',
            workerId='worker',workerInstanceId='instance',authorityId=queue.authority_id,engineId='synthetic-engine',
            gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',coordinatorOrigin=origin,
            brokerOrigin='https://127.0.0.1:'+str(broker.server_port),engineOrigin=engine_info['origin'],engineToken='',
            workerBearer=worker_token,resultReadBearer=reader_token,caFile=str(cert),serverCertFile=str(cert),
            serverKeyFile=str(key),listenHost='127.0.0.1',listenPort=worker_port,outputBudget=1048576,
            safetyFloor=1024,transferTtl=240,cleanupGrace=3600,pollSeconds=.2,
            engineVersion='synthetic-transport-only',shareResults=True)
        configpath=root/'worker.json';configpath.write_text(json.dumps(config));configpath.chmod(0o600)
        raw=copy.deepcopy(fixture['submission']);raw['submissionId']=str(uuid.uuid4())
        obj=dict(id='image-'+raw['submissionId'],name='test.jpg',byteSize=3,sha256=hashlib.sha256(b'abc').hexdigest(),kind='image',mediaType='image/jpeg')
        raw['manifest'].update(objects=[obj],objectCount=1,totalBytes=3,inputCounts=dict(images=1,lidarObjects=0,points=0))
        raw['manifest']['sha256']=manifest_sha256(raw['manifest'])
        job=store.submit(validate_submission(raw,environment='staging',capability_fingerprint=raw['capabilityFingerprint']))[0]
        with (root/'first-worker.log').open('wb') as log:
            worker=subprocess.Popen([sys.executable,'-m','ops.reference_worker_daemon','--config',str(configpath)],
                                    stdout=log,stderr=subprocess.STDOUT)
        print('OWNED_WORKER_PID',worker.pid,flush=True)
        def grant():
            if worker.poll() is not None:raise RuntimeError('worker exited before input request')
            state=store.get(job['jobId'],private=True)
            if state['phase']!='awaiting_transfer':return False
            req=state['transferRequest'];store.receive_transfer_grant(job['jobId'],dict(req,jobId=job['jobId'],
                environment='staging',objectIds=[obj['id']],token=broker_token));return True
        wait_until(grant,seconds=30,message='worker did not request input')
        wait_until(lambda:engine_counts(root/'engine.sqlite').get('commit')==1,seconds=25,message='engine did not commit')
        # The engine stays alive in a different process, but its info response is
        # gated at running until coordinator has been cleanly taken offline.
        coordinator.shutdown();coord_thread.join(5);coordinator.server_close()
        if coord_thread.is_alive():raise RuntimeError('coordinator listener did not stop')
        coordinator=None;coord_thread=None
        (root/'engine-release').write_text('synthetic release')
        def pending_completion():
            row=outbox_row(root/'state'/'outbox.sqlite')
            return row if row and row[1]=='pending' and row[2]>=1 else None
        pending=wait_until(pending_completion,seconds=20,
                           message='worker did not retain pending completion')
        archives=list((root/'scratch').rglob('all.zip'))
        if len(archives)!=1 or archives[0].read_bytes()!=ARCHIVE:raise RuntimeError('worker archive mismatch')
        before=hashlib.sha256(archives[0].read_bytes()).hexdigest()
        if worker.poll() is not None:raise RuntimeError('worker exited before owned termination')
        # Exact direct Popen handle only; never signal a PID selected by name.
        worker.kill();worker.wait(timeout=8)
        if worker.returncode!=-signal.SIGKILL:raise RuntimeError('worker termination unconfirmed')
        print('FIRST_WORKER_REAPED',worker.pid,flush=True)
        worker=None
        coordinator=create_reference_http_server(api,'127.0.0.1',coordinator_port,ssl_context=tls)
        coord_thread=threading.Thread(target=coordinator.serve_forever,kwargs={'poll_interval':.05});coord_thread.start()
        with (root/'restarted-worker.log').open('wb') as log:
            worker=subprocess.Popen([sys.executable,'-m','ops.reference_worker_daemon','--config',str(configpath)],
                                    stdout=log,stderr=subprocess.STDOUT)
        print('RESTARTED_WORKER_PID',worker.pid,flush=True)
        def accepted():
            if worker.poll() is not None:raise RuntimeError('restarted worker exited')
            state=store.get(job['jobId'],private=True)
            return state if state['phase']=='awaiting_acceptance' else None
        state=wait_until(accepted,seconds=35,message='metadata retry did not complete')
        def acknowledged_completion():
            row=outbox_row(root/'state'/'outbox.sqlite')
            return row if row and row[1]=='acknowledged' else None
        acknowledged=wait_until(acknowledged_completion,seconds=20,message='worker did not persist receipt')
        if (acknowledged[0],acknowledged[3],acknowledged[4]) != (pending[0],pending[3],pending[4]):
            raise RuntimeError('outbox identity or payload changed across restart')
        with queue._db() as db:
            receipts=db.execute('SELECT outbox_id,payload_sha,job_id,receipt_json FROM reference_worker_receipts').fetchall()
        if len(receipts)!=1:raise RuntimeError('coordinator receipt count differs')
        receipt=receipts[0]
        if (receipt['outbox_id'],receipt['payload_sha'],receipt['job_id']) != (pending[0],pending[4],job['jobId']):
            raise RuntimeError('coordinator receipt identity differs')
        if json.loads(receipt['receipt_json']) != json.loads(acknowledged[5]):
            raise RuntimeError('worker and coordinator receipts differ')
        if json.loads(pending[3])['resultManifest'] != state['resultManifest']:
            raise RuntimeError('coordinator result metadata differs from persisted payload')
        manifest=state['resultManifest']; result=manifest['objects'][0]
        if result['sha256']!=before or archives[0].read_bytes()!=ARCHIVE:raise RuntimeError('retained archive changed')
        url=origin+'/ltds/v1/jobs/'+job['jobId']+'/results/'+result['id']
        with urlopen(Request(url,headers={'Authorization':'Bearer '+viewer_token}),context=client_tls,timeout=5) as response:
            body=response.read(len(ARCHIVE)+1)
        if body!=ARCHIVE:raise RuntimeError('proxy returned different bytes')
        counts=engine_counts(root/'engine.sqlite')
        if counts!={'initialize':1,'upload':1,'commit':1,'download':1}:raise RuntimeError('engine mutation replay detected')
        print('ASSERTIONS_COMPLETE','outbox=',pending[0],'archive_sha256=',before,'counts=',counts,flush=True)
    finally:
        errors=[]
        if worker is not None:
            try:
                if worker.poll() is None:worker.send_signal(signal.SIGTERM)
                worker.wait(timeout=8)
            except Exception:errors.append(('worker',worker.pid))
        if engine is not None:
            try:
                if engine.poll() is None:
                    engine.stdin.write(b'stop\n');engine.stdin.flush()
                engine.wait(timeout=8)
            except Exception:errors.append(('engine',engine.pid))
        for label,server,thread in (('coordinator',coordinator,coord_thread),('broker',broker,broker_thread)):
            if server is not None:
                try:
                    if thread is not None and thread.is_alive():server.shutdown();thread.join(5)
                    server.server_close()
                    if thread is not None and thread.is_alive():errors.append((label,'thread'))
                except Exception:errors.append((label,'error'))
        if queue is not None:
            try:queue.close()
            except Exception:errors.append(('queue','close'))
        if errors:
            print('OWNED_RESOURCE_UNCERTAIN',errors,flush=True)
            raise RuntimeError('owned fixture resources require inspection')
    print('FIXTURE_CLEANUP_VERIFIED',flush=True)

if __name__=='__main__':main()
