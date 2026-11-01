"""INACTIVE remote-authority broker namespace rehearsal; review before use.

All credentials and payloads are synthetic. The mTLS bootstrap is test-only,
while subsequent prepare/check/complete traffic is the actual remote session.
Docker engine identity probes are patched; this does not qualify a real node.
"""
import argparse
from contextlib import ExitStack
import hashlib
import io
import json
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
SOCKET='/control/authority.sock'


def pin(path):return hashlib.sha256(ssl.PEM_cert_to_DER_cert(Path(path).read_text())).hexdigest()


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


def coordinator():
    from processing import node_mutation_broker as b
    from processing.prototype import Queue, VIEWER_PROFILE
    from processing.viewer_tasks import TaskStore
    from processing.authority_lease import AuthorityLease, record_required
    from processing.broker_intent_fence import install_offline
    from processing.node_authority_session import _write,_peer,serve_coordinator
    root=Path('/coordinator-state/run');root.mkdir(mode=0o700)
    with ExitStack() as owned:
        q=Queue(root/'queue',node_ids=('cluster','main'),viewer_enabled=True);owned.callback(q.close)
        lease_dir=root/'lease';lease_dir.mkdir(mode=0o700)
        lease=AuthorityLease(lease_dir,q.authority_id);owned.callback(lease.close)
        q.authority_lease=lease;record_required(q,lease)
        owned.enter_context(patch.object(b,'QUALIFIED',True))
        store=TaskStore(q,root/'queue'/'viewer-tasks')
        with q._db() as db:install_offline(db,enabled=True)
        task=str(uuid.uuid4());store.initialize(task,'Synthetic',[],None,'production')
        store.upload(task,'synthetic.jpg',io.BytesIO(b'x'),1);store.commit(task)
        claim=q.claim({'workerId':'viewer-dispatch','nodeId':'main','profiles':[VIEWER_PROFILE]})
        store.update(task,stage='running',phase='init_pending',node_id='main',engine_uuid=task,claim_token=claim['claimToken'])
        grant,signature=b.issue_init_grant(q,job_id=claim['id'],claim_token=claim['claimToken'],
            engine_id='synthetic-engine-identity',gate_generation=1,operation_id=str(uuid.uuid4()),body=BODY,
            content_type='application/octet-stream',secret=SECRET,_authority_required=True)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain('/certs/server.crt','/certs/server.key')
        context.load_verify_locations(cafile='/certs/ca.crt');context.verify_mode=ssl.CERT_REQUIRED
        listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);owned.callback(listener.close)
        listener.settimeout(20);listener.bind(SOCKET);listener.listen(2)
        # First connection must fail mTLS because it presents no client cert.
        raw,_=listener.accept();raw.settimeout(4)
        try:
            with context.wrap_socket(raw,server_side=True):raise RuntimeError('unauthenticated session accepted')
        except ssl.SSLError:raw.close()
        raw,_=listener.accept();raw.settimeout(4)
        with context.wrap_socket(raw,server_side=True) as channel:
            _peer(channel,pin('/certs/client.crt'),True)
            # Only synthetic claim token; conveyed after peer authentication.
            _write(channel,{'grant':grant,'signature':signature,'claimToken':claim['claimToken']},time.monotonic()+4)
            serve_coordinator(channel,queue=q,witness=b.LocalCoordinatorAuthority(q),secret=SECRET,
                peer_sha256=pin('/certs/client.crt'),session_seconds=30,idle_seconds=12)
        with q._db() as db:
            intent=db.execute('SELECT state FROM broker_intents WHERE operation_id=?',(grant['operationId'],)).fetchone()
        if intent is None or intent[0]!='complete':raise RuntimeError('coordinator intent not complete')
        print('REMOTE_COORDINATOR_TEST_PASS unauthenticated denied; coordinator intent=complete',flush=True)


def broker():
    from processing import node_mutation_broker as b
    from processing.node_gate import NodeGate
    from processing.engine_ingress_guard import BackendPin
    from processing.node_authority_session import _read,_peer,RemoteAuthoritySession,SessionHeld
    import urllib.request
    root=Path('/node-state/run');root.mkdir(mode=0o700)
    context=ssl.create_default_context(ssl.Purpose.SERVER_AUTH,cafile='/certs/ca.crt');context.check_hostname=False
    deadline=time.monotonic()+20
    while not Path(SOCKET).exists():
        if time.monotonic()>deadline:raise RuntimeError('coordinator readiness timeout')
        time.sleep(.1)
    # Exercise an anonymous TLS attempt before loading the synthetic client key.
    raw=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);raw.settimeout(4);raw.connect(SOCKET)
    try:
        with context.wrap_socket(raw,server_hostname='synthetic-coordinator') as denied:
            _peer(denied,pin('/certs/server.crt'),False)
            try:_read(denied,time.monotonic()+4)
            except (ssl.SSLError,EOFError,ConnectionError,SessionHeld):pass
            else:raise RuntimeError('unauthenticated authority accepted')
    except ssl.SSLError:pass
    context.load_cert_chain('/certs/client.crt','/certs/client.key')
    with ExitStack() as owned:
        raw=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);owned.callback(raw.close)
        raw.settimeout(4);raw.connect(SOCKET)
        channel=context.wrap_socket(raw,server_hostname='synthetic-coordinator');owned.callback(channel.close)
        _peer(channel,pin('/certs/server.crt'),False)
        hello=_read(channel,time.monotonic()+4)
        if type(hello) is not dict or set(hello)!={'grant','signature','claimToken'}:raise RuntimeError('bootstrap differs')
        grant=hello['grant'];signature=hello['signature']
        remote=RemoteAuthoritySession(channel,authority=grant['authorityId'],epoch=grant['coordinatorEpoch'],
            claim_token=hello['claimToken'],peer_sha256=pin('/certs/server.crt'),timeout_seconds=4)
        owned.callback(remote.close)
        gate_path=root/'gate';engine_id='synthetic-engine-identity';origin='http://127.0.0.1:18080'
        owned.enter_context(patch('processing.node_gate.canonical_gate_path',return_value=gate_path))
        owned.enter_context(patch('processing.node_mutation_broker.canonical_gate_path',return_value=gate_path))
        owned.enter_context(patch.object(b,'QUALIFIED',True))
        owned.enter_context(patch.object(b,'local_engine_identity',return_value=engine_id))
        owned.enter_context(patch.object(b,'local_backend_identity',return_value=None))
        with NodeGate(engine_id) as gate:gate.provision('main',origin,grant['authorityId'])
        state=gate_path/'mutation-broker';state.mkdir(mode=0o700)
        adapter=b.LoopbackNodeODM(engine_id,BackendPin('a'*64,'sha256:'+'b'*64,PORT),'')
        worker=b.NodeMutationBroker(state,node_id='main',engine_id=engine_id,authority_id=grant['authorityId'],
            coordinator_epoch=grant['coordinatorEpoch'],coordinator_origin=origin,gate_generation=1,
            secret=SECRET,adapter=adapter,authority_witness=remote);owned.callback(worker.close)
        first=worker.mutate(grant,signature,BODY);second=worker.mutate(grant,signature,BODY)
        if first!=second or first.get('uuid')!=grant['engineTaskId']:raise RuntimeError('remote replay differs')
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f'http://127.0.0.1:{PORT}/count',timeout=3) as response:
            count=json.loads(response.read(128))['count']
        row=worker.db.execute('SELECT state FROM operations WHERE id=?',(grant['operationId'],)).fetchone()
        if count!=1 or row is None or row[0]!='complete':raise RuntimeError('remote broker outcome differs')
        print('REMOTE_BROKER_TEST_PASS remote mTLS init/replay; engine requests=1; journal=complete',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('role',choices=('engine','broker','coordinator'))
    {'engine':engine,'broker':broker,'coordinator':coordinator}[parser.parse_args().role]()
