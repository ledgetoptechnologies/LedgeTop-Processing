"""Isolated, single-process gate-owner RPC experiment; never production dispatch.

Only the process-owned NativeNodeFixture is callable.  This is not a network
fence, a coordinator permit, a live engine integration, or durable success.
"""
import hmac, json, os, re, socket, threading
from pathlib import Path
from .node_gate import NodeGate
from .direct_outbox_contract import direct_pipeline_digest
from .reference_protocol import digest
from .viewer_service import NativeEngine
from .viewer_strict_mutations import initialize
from ops.native_node_fixture import NativeNodeFixture

class SyntheticRpcHeld(RuntimeError):pass

class SyntheticGateRpc:
 def __init__(self,engine_id,fixture,*,node,origin,authority,generation,receipt,job,token,enabled=False):
  if enabled is not True or type(fixture) is not NativeNodeFixture or not fixture._thread or not fixture._thread.is_alive():
   raise SyntheticRpcHeld('owned_synthetic_fixture_required')
  if type(token) is not str or not re.fullmatch('[a-f0-9]{64}',token):raise SyntheticRpcHeld('invalid_token')
  self.gate=NodeGate(engine_id);self.fixture=fixture;self.node=node;self.origin=origin;self.authority=authority
  self.generation=generation;self.receipt=receipt;self.job=job;self.token=token;self.lock=threading.Lock();self.active=False
 def _check(self):
  state=self.gate.snapshot();reservation=state.get('reservation')
  if (state.get('nodeId')!=self.node or state.get('coordinatorOrigin')!=self.origin or state.get('authorityId')!=self.authority
      or state.get('generation')!=self.generation or not reservation or reservation.get('receiptId')!=self.receipt
      or reservation.get('job')!=self.job or not self.fixture._thread.is_alive()):raise SyntheticRpcHeld('gate_binding_changed')
 def __enter__(self):
  self.gate.__enter__()
  try:self._check();self.active=True;return self
  except BaseException:self.gate.__exit__(None,None,None);raise
 def __exit__(self,*args):
  with self.lock:
   self.active=False;self.gate.__exit__(*args)
 def serve_one(self,conn):
  # Only an already-connected local synthetic socket; caller owns listener and authentication transport.
  if not self.lock.acquire(blocking=False):raise SyntheticRpcHeld('busy')
  try:
   if not self.active or conn.family!=socket.AF_UNIX:raise SyntheticRpcHeld('not_active')
   conn.settimeout(2);raw=b''
   while b'\n' not in raw and len(raw)<4096:
    chunk=conn.recv(min(1024,4096-len(raw)))
    if not chunk:break
    raw+=chunk
   if not raw.endswith(b'\n') or raw.count(b'\n')!=1:raise SyntheticRpcHeld('invalid_request')
   try:req=json.loads(raw)
   except (ValueError,UnicodeError):raise SyntheticRpcHeld('invalid_request') from None
   if (type(req) is not dict or set(req)!={'token','jobId','operation'} or type(req['token']) is not str
       or not hmac.compare_digest(req['token'],self.token) or req['jobId']!=self.job['id'] or req['operation']!='initialize'):
    raise SyntheticRpcHeld('unauthorized')
   self._check()
   # Exclusive marker remains on any ambiguity or success: never replay a mutation.
   root=Path(self.gate.root);fd=os.open(root/'synthetic-rpc-init.intent',os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
   try:os.write(fd,b'initialize\n');os.fsync(fd)
   finally:os.close(fd)
   dfd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:os.fsync(dfd)
   finally:os.close(dfd)
   self._check();plan={'options':[],'outputs':None};name='synthetic'
   initialize(NativeEngine(self.fixture.origin),self.job['id'],plan,direct_pipeline_digest([],None),name,digest(name))
   conn.sendall(b'{"state":"initialized_no_dispatch"}\n')
  finally:self.lock.release()
