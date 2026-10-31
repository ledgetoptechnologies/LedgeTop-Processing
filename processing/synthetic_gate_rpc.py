"""Isolated, single-process gate-owner RPC experiment; never production dispatch.

Only the process-owned NativeNodeFixture is callable.  This is not a network
fence, a coordinator permit, a live engine integration, or durable success.
"""
import hmac, json, os, re, socket, stat, struct, threading
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
   def pairs(values):
    result={}
    for key,value in values:
     if key in result:raise ValueError('duplicate')
     result[key]=value
    return result
   try:req=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(ValueError('constant')))
   except (ValueError,UnicodeError):raise SyntheticRpcHeld('invalid_request') from None
   if (type(req) is not dict or set(req)!={'token','jobId','operation'} or type(req['token']) is not str
       or not hmac.compare_digest(req['token'],self.token) or req['jobId']!=self.job['id'] or req['operation']!='initialize'):
    raise SyntheticRpcHeld('unauthorized')
   self._check()
   # Exclusive marker remains on any ambiguity or success: never replay a mutation.
   root=Path(self.gate.root)
   if (root/'synthetic-rpc-init.intent').exists() or (root/'synthetic-rpc-init.intent').is_symlink():raise SyntheticRpcHeld('legacy_intent_requires_inspection')
   fd=os.open(root/('synthetic-rpc-init-'+self.job['id']+'.intent'),os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
   try:os.write(fd,b'initialize\n');os.fsync(fd)
   finally:os.close(fd)
   dfd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
   try:os.fsync(dfd)
   finally:os.close(dfd)
   self._check();plan={'options':[],'outputs':None};name='synthetic'
   initialize(NativeEngine(self.fixture.origin),self.job['id'],plan,direct_pipeline_digest([],None),name,digest(name))
   conn.sendall(b'{"state":"initialized_no_dispatch"}\n')
  finally:self.lock.release()

class SyntheticGateServer:
 """Single-threaded fixture-only AF_UNIX listener with same-UID peer check.

 Caller must provision the gate and an isolated private root first. A stale
 socket is never unlinked automatically. No arbitrary engine endpoint is used.
 """
 def __init__(self,root,engine_id,*,node,origin,authority,generation,receipt,job,token,enabled=False):
  if enabled is not True or not hasattr(socket,'SO_PEERCRED'):raise SyntheticRpcHeld('synthetic_unix_required')
  self.root=Path(root)
  if not self.root.is_absolute() or self.root.resolve()!=self.root:raise SyntheticRpcHeld('private_root_required')
  st=self.root.stat()
  if st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)!=0o700 or not stat.S_ISDIR(st.st_mode):raise SyntheticRpcHeld('private_root_required')
  self.root_identity=(st.st_dev,st.st_ino);self.path=self.root/'synthetic-gate.sock';self.engine_id=engine_id
  self.arguments=dict(node=node,origin=origin,authority=authority,generation=generation,receipt=receipt,job=job,token=token,enabled=True)
  self.fixture=None;self.rpc=None;self.listener=None;self.socket_identity=None
 def _check_root(self):
  st=self.root.lstat()
  if not stat.S_ISDIR(st.st_mode) or (st.st_dev,st.st_ino)!=self.root_identity or st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)!=0o700:
   raise SyntheticRpcHeld('private_root_changed')
 def __enter__(self):
  self._check_root()
  if self.path.exists() or self.path.is_symlink():raise SyntheticRpcHeld('socket_exists')
  try:
   # The fixture class opens/chmods its path; only hand it the exact fresh file
   # we created, never a pre-existing database. Retain it on all outcomes.
   db_path=self.root/'synthetic-engine.sqlite'
   fd=os.open(db_path,os.O_CREAT|os.O_EXCL|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
   try:
    initial=os.fstat(fd);os.fsync(fd)
    dfd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(dfd)
    finally:os.close(dfd)
    self._check_root();named=db_path.lstat()
    if not stat.S_ISREG(named.st_mode) or (named.st_dev,named.st_ino)!=(initial.st_dev,initial.st_ino):raise SyntheticRpcHeld('fixture_db_changed')
    self.fixture=NativeNodeFixture(db_path,synthetic_enabled=True)
    named=db_path.lstat()
    if (named.st_dev,named.st_ino)!=(initial.st_dev,initial.st_ino):raise SyntheticRpcHeld('fixture_db_changed')
    self.fixture.start()
   finally:os.close(fd)
   self.rpc=SyntheticGateRpc(self.engine_id,self.fixture,**self.arguments).__enter__()
   self.listener=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.listener.bind(str(self.path))
   st=self.path.lstat();self.socket_identity=(st.st_dev,st.st_ino)
   os.chmod(self.path,0o600);self.listener.listen(1);self.listener.settimeout(.2);self._check_root()
   return self
  except BaseException:
   self.close();raise
 def _same_uid(self,connection):
  _,uid,_=struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))
  return uid==os.geteuid()
 def serve(self,stop):
  if self.listener is None or self.rpc is None:raise SyntheticRpcHeld('not_started')
  while not stop.is_set():
   try:connection,_=self.listener.accept()
   except socket.timeout:continue
   with connection:
    try:
     if not self._same_uid(connection):continue
     self.rpc.serve_one(connection)
    except Exception:
     try:connection.sendall(b'{"state":"held"}\n')
     except OSError:pass
 def close(self):
  if self.listener is not None:self.listener.close();self.listener=None
  if self.socket_identity is not None:
   try:
    self._check_root();st=self.path.lstat()
    if stat.S_ISSOCK(st.st_mode) and (st.st_dev,st.st_ino)==self.socket_identity:self.path.unlink()
   except (OSError,SyntheticRpcHeld):pass
   self.socket_identity=None
  if self.rpc is not None:self.rpc.__exit__(None,None,None);self.rpc=None
  if self.fixture is not None:self.fixture.close();self.fixture=None
 def __exit__(self,*_):self.close()
