"""Owned synthetic child; invoked only by test_engine_ingress_process."""
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sys
import threading
import uuid
from unittest.mock import patch

from processing.engine_ingress_guard import BackendPin, Binding, EngineIngressGuard, OwnerPin
from processing.node_gate import NodeGate
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore

root=Path(sys.argv[1]).resolve();mode=sys.argv[2];port=int(sys.argv[3])
with patch('processing.node_gate.canonical_gate_path',return_value=root/'gate'):
    queue=Queue(root/'queue',viewer_enabled=True);store=TaskStore(queue,root/'inputs')
    task=str(uuid.uuid4());engine=str(uuid.uuid4());upload=str(uuid.uuid4())
    store.initialize(task,'synthetic',[],None,'production');store.upload(task,'fixture.jpg',io.BytesIO(b'fixture'),7);store.commit(task)
    claim=queue.claim({'workerId':'viewer-dispatch','nodeId':'cluster','profiles':[VIEWER_PROFILE]})
    store.update(task,stage='running',phase='init_pending',node_id='cluster',engine_uuid=engine,claim_token=claim['claimToken'])
    binding=Binding('synthetic-engine','cluster',queue.authority_id,'http://127.0.0.1:18080',1,port,
                    'coordinator-'+'a'*40,'management-'+'b'*40,'backend-'+'c'*40,deadline_seconds=10)
    with NodeGate(binding.engine_id) as gate:gate.provision('cluster',binding.coordinator_origin,queue.authority_id)
    guard=EngineIngressGuard(queue,binding,owner_pin=OwnerPin.capture(queue),backend_pin=BackendPin("a"*64,"sha256:"+"b"*64,binding.backend_port),backend_probe=lambda *_:None,engine_probe=lambda _:binding.engine_id)
    headers=[('Authorization','Bearer '+binding.coordinator_secret),('X-LedgeTop-Job',claim['id']),
             ('X-LedgeTop-Claim',claim['claimToken']),('Set-UUID',engine)]
    guard.mutate('POST','/task/new/init',headers,b'{}');store.update(task,phase='upload_pending')
    source,writer=socket.socketpair();payload=b'x'*8192
    writer.sendall(payload[:4096] if mode=='mid' else payload)
    if mode!='mid':writer.shutdown(socket.SHUT_WR)
    print(json.dumps({'kind':'setup','job':claim['id'],'task':task,'engine':engine,'upload':upload}),flush=True)
    original=guard._exchange
    def exchange(*args,**kwargs):
        if mode=='pre':
            print(json.dumps({'kind':'pre'}),flush=True);threading.Event().wait(30)
        result=original(*args,**kwargs)
        if mode=='post':
            print(json.dumps({'kind':'post'}),flush=True);threading.Event().wait(30)
        return result
    guard._exchange=exchange
    guard.stream_upload('/task/new/upload/'+engine,headers+[('X-LedgeTop-Upload-ID',upload)],source,len(payload),
                        hashlib.sha256(payload).hexdigest(),'multipart/form-data; boundary=synthetic')
    os._exit(75)
