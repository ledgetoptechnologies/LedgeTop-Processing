"""UNREVIEWED. Synthetic coordinator SIGKILL/restart; only in owned container."""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen

from ops.native_node_fixture import ARCHIVE
from processing.node_gate import NodeGate
from tests.worker_process_outage_fixture import engine_counts, outbox_row, wait_until


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        return sock.getsockname()[1]


def start(module, root, name, *args):
    with (root/(name+'.log')).open('wb') as log:
        child=subprocess.Popen([sys.executable,'-m',module,*args],stdin=subprocess.PIPE,
                               stdout=log,stderr=subprocess.STDOUT)
    print('OWNED_CHILD',name,child.pid,flush=True)
    return child


def main():
    root=Path(tempfile.mkdtemp(prefix='coordinator-process-',dir=os.environ.get('SYNTHETIC_RETAINED_DIR','/tmp'))).resolve()
    root.chmod(0o700);print('RETAINED_SYNTHETIC_ROOT',root,flush=True)
    os.environ['XDG_STATE_HOME']=str(root/'xdg')
    for name in ('state','scratch'):(root/name).mkdir(mode=0o700)
    cert,key=root/'cert.pem',root/'key.pem'
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),
        '-days','1','-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1'],check=True,timeout=15,
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    key.chmod(0o600)
    engine=worker=coord=broker=broker_thread=None
    try:
        engine=start('tests.worker_process_outage_engine',root,'engine','--root',str(root))
        wait_until(lambda:(root/'engine-ready.json').is_file(),seconds=8,message='engine not ready')
        if engine.poll() is not None:raise RuntimeError('engine exited')
        engine_info=json.loads((root/'engine-ready.json').read_text())
        worker_token='SYNTHETIC-WORKER-'+'w'*36;viewer_token='SYNTHETIC-VIEWER-'+'v'*36
        broker_token='SYNTHETIC-BROKER-'+'b'*36;reader_token='SYNTHETIC-READER-'+'r'*36
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(cert,key)
        class Broker(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                if self.headers.get('Authorization')!='Bearer '+broker_token or not self.path.startswith('/api/processing/reference/v1/objects/'):
                    self.send_error(403);return
                self.send_response(200);self.send_header('Content-Length','3');self.end_headers();self.wfile.write(b'abc')
        broker=ThreadingHTTPServer(('127.0.0.1',0),Broker);broker.socket=tls.wrap_socket(broker.socket,server_side=True)
        broker_thread=threading.Thread(target=broker.serve_forever,kwargs={'poll_interval':.05});broker_thread.start()
        worker_port,coordinator_port=free_port(),free_port()
        if worker_port==coordinator_port:raise RuntimeError('listener ports collide')
        settings=dict(workerPort=worker_port,coordinatorPort=coordinator_port,workerToken=worker_token,
            viewerToken=viewer_token,readerToken=reader_token,brokerToken=broker_token,submissionId=str(uuid.uuid4()),epoch='one')
        def write_settings():
            path=root/'coordinator-config.json';path.write_text(json.dumps(settings));path.chmod(0o600)
        write_settings()
        coord=start('tests.worker_process_coordinator',root,'coordinator-one',str(root))
        def ready(epoch):
            if coord.poll() is not None:raise RuntimeError('coordinator exited before readiness')
            path=root/('coordinator-ready-'+epoch+'.json')
            return json.loads(path.read_text()) if path.exists() else None
        identity=wait_until(lambda:ready('one'),seconds=15,message='coordinator not ready')
        with NodeGate('synthetic-engine') as gate:gate.provision('cluster','http://127.0.0.1:18080',identity['authorityId'])
        config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),nodeId='cluster',
            workerId='worker',workerInstanceId='instance',authorityId=identity['authorityId'],engineId='synthetic-engine',
            gateGeneration=1,gateCoordinatorOrigin='http://127.0.0.1:18080',coordinatorOrigin='https://127.0.0.1:'+str(coordinator_port),
            brokerOrigin='https://127.0.0.1:'+str(broker.server_port),engineOrigin=engine_info['origin'],engineToken='',
            workerBearer=worker_token,resultReadBearer=reader_token,caFile=str(cert),serverCertFile=str(cert),
            serverKeyFile=str(key),listenHost='127.0.0.1',listenPort=worker_port,outputBudget=1048576,
            safetyFloor=1024,transferTtl=240,cleanupGrace=3600,pollSeconds=.2,engineVersion='synthetic-transport-only',shareResults=True)
        path=root/'worker.json';path.write_text(json.dumps(config));path.chmod(0o600)
        worker=start('ops.reference_worker_daemon',root,'worker','--config',str(path))
        wait_until(lambda:(root/'coordinator-granted').exists(),seconds=30,message='grant not persisted')
        wait_until(lambda:engine_counts(root/'engine.sqlite').get('commit')==1,seconds=25,message='engine did not commit')
        if coord.poll() is not None:raise RuntimeError('coordinator already exited')
        coord.kill();coord.wait(timeout=8)
        if coord.returncode!=-signal.SIGKILL:raise RuntimeError('coordinator termination uncertain')
        print('FIRST_COORDINATOR_REAPED',coord.pid,flush=True);coord=None
        with socket.socket() as probe:
            probe.settimeout(1)
            if probe.connect_ex(('127.0.0.1',coordinator_port))==0:raise RuntimeError('coordinator listener still reachable')
        (root/'engine-release').write_text('synthetic release')
        def pending():
            row=outbox_row(root/'state'/'outbox.sqlite')
            return row if row and row[1]=='pending' and row[2]>=1 else None
        before=wait_until(pending,seconds=20,message='pending completion absent')
        archives=list((root/'scratch').rglob('all.zip'))
        if len(archives)!=1 or archives[0].read_bytes()!=ARCHIVE:raise RuntimeError('synthetic archive mismatch')
        settings['epoch']='two';write_settings()
        coord=start('tests.worker_process_coordinator',root,'coordinator-two',str(root))
        if wait_until(lambda:ready('two'),seconds=15,message='restart not ready')!=identity:
            raise RuntimeError('coordinator job identity changed')
        def acknowledged():
            row=outbox_row(root/'state'/'outbox.sqlite')
            return row if row and row[1]=='acknowledged' else None
        after=wait_until(acknowledged,seconds=35,message='worker receipt absent')
        report=wait_until(lambda:json.loads((root/'coordinator-accepted.json').read_text()) if (root/'coordinator-accepted.json').exists() else None,
                          seconds=10,message='coordinator receipt absent')
        if (before[0],before[3],before[4])!=(after[0],after[3],after[4]):raise RuntimeError('outbox changed')
        if (report['outboxId'],report['payloadSha256'],report['jobId'])!=(before[0],before[4],identity['jobId']):
            raise RuntimeError('coordinator receipt identity mismatch')
        if report['receipt']!=json.loads(after[5]) or report['manifest']!=json.loads(before[3])['resultManifest']:
            raise RuntimeError('coordinator receipt payload mismatch')
        result=report['manifest']['objects'][0]
        if result['sha256']!=hashlib.sha256(ARCHIVE).hexdigest():raise RuntimeError('archive digest mismatch')
        url=config['coordinatorOrigin']+'/ltds/v1/jobs/'+identity['jobId']+'/results/'+result['id']
        with urlopen(Request(url,headers={'Authorization':'Bearer '+viewer_token}),context=ssl.create_default_context(cafile=str(cert)),timeout=5) as response:
            if response.read(len(ARCHIVE)+1)!=ARCHIVE:raise RuntimeError('proxy bytes mismatch')
        if archives[0].read_bytes()!=ARCHIVE or engine_counts(root/'engine.sqlite')!={'initialize':1,'upload':1,'commit':1,'download':1}:
            raise RuntimeError('archive lost or duplicate execution')
        print('COORDINATOR_RESTART_ASSERTIONS_COMPLETE',before[0],flush=True)
    finally:
        errors=[]
        if coord is not None:
            try:
                (root/'coordinator-stop').write_text('stop')
                coord.wait(timeout=8)
            except Exception:errors.append(('coordinator',coord.pid))
        if worker is not None:
            try:
                if worker.poll() is None:worker.send_signal(signal.SIGTERM)
                worker.wait(timeout=8)
            except Exception:errors.append(('worker',worker.pid))
        if engine is not None:
            try:
                if engine.poll() is None:engine.stdin.write(b'stop\n');engine.stdin.flush()
                engine.wait(timeout=8)
            except Exception:errors.append(('engine',engine.pid))
        if broker is not None:
            try:
                if broker_thread is not None and broker_thread.is_alive():broker.shutdown();broker_thread.join(5)
                broker.server_close()
                if broker_thread is not None and broker_thread.is_alive():errors.append(('broker','thread'))
            except Exception:errors.append(('broker','error'))
        if errors:
            print('OWNED_RESOURCE_UNCERTAIN',errors,flush=True)
            raise RuntimeError('owned resources require inspection')
    print('COORDINATOR_FIXTURE_CLEANUP_VERIFIED',flush=True)


if __name__=='__main__':main()
