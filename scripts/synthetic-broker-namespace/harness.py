"""INACTIVE synthetic broker namespace harness; review before any run.

Requires separately mounted run-owned /state, /control and /certs directories.
No real data. Docker identity probes are synthetic and must not be qualified.
The control endpoint below is test-only, not a production broker API.
"""
import argparse
from contextlib import ExitStack
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import ssl
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

PORT=30123
BODY=b'synthetic-init'
SECRET=b'synthetic-test-only-key-0123456789'
SOCKET='/control/broker.sock'
READY=Path('/control/grant.json')


def pin(path):
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert(Path(path).read_text())).hexdigest()


def engine():
    class Handler(BaseHTTPRequestHandler):
        calls=[]
        def log_message(self,*args):pass
        def do_POST(self):
            if self.path!='/task/new/init':self.send_error(404);return
            n=int(self.headers.get('Content-Length','0'))
            if n>65536:self.send_error(413);return
            self.rfile.read(n);self.calls.append(self.headers.get('set-uuid'))
            data=json.dumps({'uuid':self.calls[-1],'success':True}).encode()
            self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def do_GET(self):
            if self.path!='/count':self.send_error(404);return
            data=json.dumps({'count':len(self.calls)}).encode()
            self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    HTTPServer(('127.0.0.1',PORT),Handler).serve_forever()


def broker():
    from processing import node_mutation_broker as b
    from processing.node_gate import NodeGate
    from processing.engine_ingress_guard import BackendPin
    from processing.prototype import Queue, VIEWER_PROFILE
    from processing.viewer_tasks import TaskStore
    from processing.authority_lease import AuthorityLease, record_required
    from processing.broker_intent_fence import install_offline
    from processing.node_authority_session import _read,_write,_peer,SessionHeld
    import urllib.request
    root=Path('/state/broker-run');root.mkdir(mode=0o700)
    with ExitStack() as owned:
        q=Queue(root/'queue',node_ids=('cluster','main'),viewer_enabled=True)
        owned.callback(q.close)
        lease_dir=root/'lease';lease_dir.mkdir(mode=0o700)
        lease=AuthorityLease(lease_dir,q.authority_id);owned.callback(lease.close)
        q.authority_lease=lease;record_required(q,lease)
        gate_path=root/'gate';engine_id='synthetic-engine-identity';node='main';origin='http://127.0.0.1:18080'
        owned.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=gate_path))
        owned.enter_context(patch('processing.node_mutation_broker.canonical_gate_path',return_value=gate_path))
        owned.enter_context(patch.object(b,'QUALIFIED',True))
        owned.enter_context(patch.object(b,'local_engine_identity',return_value=engine_id))
        owned.enter_context(patch.object(b,'local_backend_identity',return_value=None))
        with NodeGate(engine_id) as gate:gate.provision(node,origin,q.authority_id)
        state=gate_path/'mutation-broker';state.mkdir(mode=0o700)
        task=str(uuid.uuid4());store=TaskStore(q,root/'queue'/'viewer-tasks')
        with q._db() as db:install_offline(db,enabled=True)
        store.initialize(task,'Synthetic',[],None,'production')
        store.upload(task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(task)
        claim=q.claim({'workerId':'viewer-dispatch','nodeId':node,'profiles':[VIEWER_PROFILE]})
        store.update(task,stage='running',phase='init_pending',node_id=node,engine_uuid=task,claim_token=claim['claimToken'])
        adapter=b.LoopbackNodeODM(engine_id,BackendPin('a'*64,'sha256:'+'b'*64,PORT),'')
        worker=b.NodeMutationBroker(state,node_id=node,engine_id=engine_id,authority_id=q.authority_id,
            coordinator_epoch=q.startup_id,coordinator_origin=origin,gate_generation=1,secret=SECRET,
            adapter=adapter,authority_witness=b.LocalCoordinatorAuthority(q))
        owned.callback(worker.close)
        grant,signature=b.issue_init_grant(q,job_id=claim['id'],claim_token=claim['claimToken'],
            engine_id=engine_id,gate_generation=1,operation_id=str(uuid.uuid4()),body=BODY,
            content_type='application/octet-stream',secret=SECRET,_authority_required=True)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain('/certs/server.crt','/certs/server.key')
        context.load_verify_locations(cafile='/certs/ca.crt');context.verify_mode=ssl.CERT_REQUIRED
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);owned.callback(listener.close)
        listener.settimeout(20);listener.bind(SOCKET);listener.listen(3)
        READY.write_text(json.dumps({'grant':grant,'signature':signature,'task':task}))
        accepted=0;rejected=0
        while accepted<2 or rejected<1:
            if accepted+rejected>=4:raise RuntimeError('unexpected control attempts')
            raw,_=listener.accept();raw.settimeout(4)
            try:
                with context.wrap_socket(raw,server_side=True) as channel:
                    _peer(channel,pin('/certs/client.crt'),True)
                    request=_read(channel,time.monotonic()+4)
                    if request!={'action':'synthetic-init','grant':grant,'signature':signature}:raise RuntimeError('invalid test request')
                    reply=worker.mutate(request['grant'],request['signature'],BODY)
                    _write(channel,reply,time.monotonic()+4);accepted+=1
            except (ssl.SSLError,EOFError,ConnectionError,SessionHeld):
                raw.close();rejected+=1
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f'http://127.0.0.1:{PORT}/count',timeout=3) as response:
            count=json.loads(response.read(128))['count']
        state_row=worker.db.execute('SELECT state FROM operations WHERE id=?',(grant['operationId'],)).fetchone()
        if count!=1 or state_row is None or state_row[0]!='complete':raise RuntimeError('synthetic broker outcome differs')
        print('BROKER_TEST_PASS synthetic probes only; engine requests=1; journal=complete',flush=True)


def coordinator():
    from processing.node_authority_session import _read,_write,_peer,SessionHeld
    deadline=time.monotonic()+20
    while not READY.exists():
        if time.monotonic()>deadline:raise RuntimeError('broker readiness timeout')
        time.sleep(.1)
    ready=json.loads(READY.read_text());task=ready['task']
    request={'action':'synthetic-init','grant':ready['grant'],'signature':ready['signature']}
    context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH,cafile='/certs/ca.crt');context.check_hostname=False
    # An unauthenticated handshake must not produce an application response.
    raw=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);raw.settimeout(4);raw.connect(SOCKET)
    try:
        with context.wrap_socket(raw,server_hostname='synthetic-broker') as bad:
            _peer(bad,pin('/certs/server.crt'),False)
            try:_write(bad,request,time.monotonic()+4);_read(bad,time.monotonic()+4)
            except (ssl.SSLError,EOFError,ConnectionError,SessionHeld):pass
            else:raise RuntimeError('unauthenticated control accepted')
    except ssl.SSLError:pass
    context.load_cert_chain('/certs/client.crt','/certs/client.key')
    replies=[]
    for _ in range(2):
        raw=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);raw.settimeout(4);raw.connect(SOCKET)
        with context.wrap_socket(raw,server_hostname='synthetic-broker') as channel:
            _peer(channel,pin('/certs/server.crt'),False)
            _write(channel,request,time.monotonic()+4)
            replies.append(_read(channel,time.monotonic()+4))
    if replies[0]!=replies[1] or replies[0].get('uuid')!=task:raise RuntimeError('synthetic replay differs')
    print('COORDINATOR_TEST_PASS mTLS unauthenticated denied; signed init replay identical',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('role',choices=('engine','broker','coordinator'))
    {'engine':engine,'broker':broker,'coordinator':coordinator}[parser.parse_args().role]()
