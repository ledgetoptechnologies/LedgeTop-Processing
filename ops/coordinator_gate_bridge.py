"""Live stdio gate bridge; no SSH launcher, credentials, daemon or gate writes.

The trusted controller authenticates two independent SSH children. Cluster
requests fresh challenges; the controller only relays them to the actual main
holder. Ready/status files never authorize an update. Proof is point-in-time:
durable both-node drains and checks around mutations remain necessary when a
transport/holder can die after replying. Loss never authorizes recovery writes.
"""
from contextlib import ExitStack
from dataclasses import dataclass
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import select
import socket
import stat
import subprocess
import sys
import threading
import time

from processing.node_gate import NodeGate,canonical_gate_path,_binding


MAX_FRAME=4096


class BridgeError(ValueError):pass
class BridgeEOF(BridgeError):pass


def _hash(value):
    if type(value) is not str or not re.fullmatch(r'[a-f0-9]{64}',value):raise BridgeError('invalid_bridge_digest_or_nonce')


def _operation(value):
    if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}',value):raise BridgeError('invalid_bridge_operation')


def helper_digest():
    """Pin this transport and the actual imported canonical NodeGate source."""
    files=(Path(__file__),Path(sys.modules[NodeGate.__module__].__file__))
    return hashlib.sha256(json.dumps([hashlib.sha256(path.read_bytes()).hexdigest() for path in files],
        separators=(',',':')).encode()).hexdigest()


@dataclass(frozen=True)
class BridgeBinding:
    engine_id:str
    node_id:str
    authority_id:str
    coordinator_origin:str
    gate_generation:int
    helper_sha256:str

    def __post_init__(self):
        if self.node_id not in ('main','cluster'):raise BridgeError('known_gate_node_required')
        if not isinstance(self.engine_id,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,255}',self.engine_id):
            raise BridgeError('fixed_physical_engine_required')
        normalized=_binding(self.node_id,self.coordinator_origin,self.authority_id)
        if normalized['coordinatorOrigin']!=self.coordinator_origin:raise BridgeError('canonical_coordinator_origin_required')
        if type(self.gate_generation) is not int or not 1<=self.gate_generation<=2**53-1:raise BridgeError('fixed_gate_generation_required')
        _hash(self.helper_sha256)

    def public(self):
        return dict(engineId=self.engine_id,nodeId=self.node_id,authorityId=self.authority_id,
            coordinatorOrigin=self.coordinator_origin,generation=self.gate_generation,helperSha256=self.helper_sha256)


class _Pipe:
    """Nonblocking owned binary-pipe framing; one absolute deadline per frame.

    Windows requires Python's nonblocking pipe support. There is no blocking
    reader/writer thread to leak on timeout. The short Windows Peek wait monitors
    local pipe/process readiness only; it never generates remote challenges.
    """
    def __init__(self,reader=None,writer=None,*,timeout=10,clock=time.monotonic):
        if type(timeout) not in (int,float) or not 0<timeout<=300:raise BridgeError('bounded_bridge_timeout_required')
        self.reader,self.writer=reader,writer;self.timeout,self.clock=timeout,clock
        self.rfd=reader.fileno() if reader is not None else None
        self.wfd=writer.fileno() if writer is not None else None
        self.buffer=bytearray();self.poisoned=False
        try:
            for descriptor in (self.rfd,self.wfd):
                if descriptor is not None:os.set_blocking(descriptor,False)
        except Exception:raise BridgeError('nonblocking_binary_pipe_required') from None

    def _wait(self,descriptor,read,deadline,monitor):
        while True:
            if self.poisoned:raise BridgeError('bridge_transport_failed')
            if monitor:monitor()
            remaining=None if deadline is None else deadline-self.clock()
            if remaining is not None and remaining<=0:raise BridgeError('bridge_absolute_deadline_exceeded')
            wait=.02 if remaining is None else min(.02,remaining)
            if os.name=='nt':
                if read:
                    import ctypes,msvcrt
                    available=ctypes.c_ulong()
                    ok=ctypes.windll.kernel32.PeekNamedPipe(ctypes.c_void_p(msvcrt.get_osfhandle(descriptor)),None,0,None,
                        ctypes.byref(available),None)
                    if not ok:
                        code=ctypes.windll.kernel32.GetLastError()
                        if code in (109,232,233):return
                        raise BridgeError('bridge_pipe_read_failed')
                    if available.value:return
                    time.sleep(wait)
                else:
                    # os.write on a nonblocking pipe either progresses or
                    # raises EAGAIN. It cannot wait for a peer indefinitely.
                    return
            else:
                readable,writable,_=select.select([descriptor] if read else [],[] if read else [descriptor],[],wait)
                if readable or writable:return

    def line(self,deadline=None,monitor=None):
        active=deadline
        try:
            while True:
                if monitor:monitor()
                if active is not None and self.clock()>=active:raise BridgeError('bridge_absolute_deadline_exceeded')
                newline=self.buffer.find(b'\n')
                if newline>=0:
                    value=bytes(self.buffer[:newline]);del self.buffer[:newline+1]
                    if not value or len(value)+1>MAX_FRAME:raise BridgeError('invalid_bridge_frame_size')
                    return value
                if len(self.buffer)>=MAX_FRAME:raise BridgeError('bridge_frame_too_large')
                self._wait(self.rfd,True,active,monitor)
                try:chunk=os.read(self.rfd,MAX_FRAME-len(self.buffer))
                except BlockingIOError:continue
                if not chunk:
                    if self.buffer:raise BridgeError('truncated_bridge_frame')
                    raise BridgeEOF('bridge_eof')
                if active is None:active=self.clock()+self.timeout
                self.buffer.extend(chunk)
        except Exception:
            self.poisoned=True;raise

    def read(self,deadline=None,monitor=None):
        def unique(pairs):
            value={}
            for key,item in pairs:
                if key in value:raise BridgeError('duplicate_bridge_field')
                value[key]=item
            return value
        try:
            value=json.loads(self.line(deadline,monitor),object_pairs_hook=unique,
                parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
            if type(value) is not dict:raise ValueError()
            return value
        except BridgeError:raise
        except Exception:self.poisoned=True;raise BridgeError('invalid_bridge_json') from None

    def bytes_to_eof(self,deadline,limit=MAX_FRAME):
        data=bytearray()
        while True:
            if self.clock()>=deadline:raise BridgeError('bridge_absolute_deadline_exceeded')
            self._wait(self.rfd,True,deadline,None)
            try:chunk=os.read(self.rfd,min(4096,limit-len(data)+1))
            except BlockingIOError:continue
            if not chunk:return bytes(data)
            data.extend(chunk)
            if len(data)>limit:raise BridgeError('bounded_probe_output_exceeded')

    def write(self,value,deadline,monitor=None):
        try:
            raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()+b'\n'
            if len(raw)>MAX_FRAME:raise BridgeError('bridge_frame_too_large')
            offset=0
            while offset<len(raw):
                if monitor:monitor()
                if self.clock()>=deadline:raise BridgeError('bridge_absolute_deadline_exceeded')
                self._wait(self.wfd,False,deadline,monitor)
                try:count=os.write(self.wfd,raw[offset:])
                except BlockingIOError:
                    time.sleep(min(.005,max(0,deadline-self.clock())));continue
                if count<=0:raise BridgeError('bridge_pipe_write_failed')
                offset+=count
            if monitor:monitor()
            if self.clock()>=deadline:raise BridgeError('bridge_absolute_deadline_exceeded')
        except BridgeError:self.poisoned=True;raise
        except Exception:self.poisoned=True;raise BridgeError('bridge_pipe_write_failed') from None


class DockerEngineProbe:
    """Fixed local read-only Docker identity command, no env/credential output."""
    def __init__(self,executable,endpoint,*,timeout_seconds=10):
        if not Path(executable).is_absolute() or not (endpoint.startswith('unix:///') or endpoint=='npipe:////./pipe/docker_engine'):
            raise BridgeError('fixed_local_docker_probe_required')
        self.executable,self.endpoint,self.timeout=executable,endpoint,timeout_seconds

    def _output(self,args,deadline=None):
        env=os.environ.copy()
        for key in ('DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH'):env.pop(key,None)
        process=None
        try:
            deadline=deadline if deadline is not None else time.monotonic()+self.timeout
            process=subprocess.Popen([self.executable,'--host',self.endpoint,*args],
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env)
            pipe=_Pipe(process.stdout,timeout=self.timeout)
            output=pipe.bytes_to_eof(deadline).decode()
            process.wait(timeout=max(.001,deadline-time.monotonic()))
            if process.returncode!=0 or time.monotonic()>=deadline:raise ValueError()
            return output
        except Exception:raise BridgeError('local_docker_identity_unavailable') from None
        finally:
            if process is not None:
                if process.poll() is None:process.kill()
                try:process.wait(timeout=1)
                except subprocess.TimeoutExpired:pass
                if process.stdout:process.stdout.close()

    def _engine_identity(self,deadline=None):
        identity=self._output(['info','--format','{{.ID}}'],deadline)
        if identity.endswith('\n'):identity=identity[:-1].removesuffix('\r')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,255}',identity):raise BridgeError('local_docker_identity_unavailable')
        return identity

    def __call__(self):return self._engine_identity()


MAIN_CONTAINER_ID='78be3d38659e2b698c7611b6b60fad46e7632ef96aa21898a7c2edafece093c1'
MAIN_CONTAINER_NAME='/ledgetop-main-processing-nodeodx-1'
MAIN_INSPECT_TEMPLATE=('{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"running":{{json .State.Running}},'
    '"pid":{{json .State.Pid}},"health":{{with index .State "Health"}}{{json (index . "Status")}}{{else}}null{{end}}}')


def _strict_json(raw):
    def unique(pairs):
        value={}
        for key,item in pairs:
            if key in value:raise ValueError()
            value[key]=item
        return value
    return json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))


def _main_http_json(route,deadline,limit=65536):
    """Fixed local HTTP only; a total deadline includes headers and body."""
    if route not in ('/info','/task/list'):raise BridgeError('fixed_main_read_route_required')
    connection=None
    try:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise ValueError()
        connection=socket.create_connection(('127.0.0.1',13004),timeout=remaining)
        connection.setblocking(False)
        request=('GET '+route+' HTTP/1.1\r\nHost: 127.0.0.1:13004\r\nAccept: application/json\r\nConnection: close\r\n\r\n').encode()
        def ready(read):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise ValueError()
            r,w,_=select.select([connection] if read else [],[] if read else [connection],[],remaining)
            if not r and not w or time.monotonic()>=deadline:raise ValueError()
        while request:
            ready(False)
            try:sent=connection.send(request)
            except BlockingIOError:continue
            if sent<=0:raise ValueError()
            request=request[sent:]
        data=bytearray()
        while b'\r\n\r\n' not in data:
            ready(True)
            try:chunk=connection.recv(1024)
            except BlockingIOError:continue
            if not chunk:raise ValueError()
            data.extend(chunk)
            if len(data)>8192+limit:raise ValueError()
            if b'\r\n\r\n' not in data and len(data)>8192:raise ValueError()
        header,body=bytes(data).split(b'\r\n\r\n',1)
        if len(header)>8192:raise ValueError()
        lines=header.decode('ascii').split('\r\n')
        if not re.fullmatch(r'HTTP/1\.[01] 200(?: [^\r\n]*)?',lines[0]):raise ValueError()
        headers={}
        for line in lines[1:]:
            key,value=line.split(':',1);key=key.lower()
            if key in headers or not re.fullmatch('[a-z0-9-]+',key):raise ValueError()
            headers[key]=value.strip()
        if (headers.get('content-type','').split(';',1)[0].lower()!='application/json'
                or 'transfer-encoding' in headers or headers.get('content-encoding','identity')!='identity'):raise ValueError()
        length=None
        if 'content-length' in headers:
            if not re.fullmatch('[0-9]{1,8}',headers['content-length']):raise ValueError()
            length=int(headers['content-length'])
            if length>limit:raise ValueError()
        while length is None or len(body)<length:
            if len(body)>limit:raise ValueError()
            ready(True)
            try:chunk=connection.recv(min(4096,limit-len(body)+1))
            except BlockingIOError:continue
            if not chunk:break
            body+=chunk
        if len(body)>limit or length is not None and len(body)!=length:raise ValueError()
        result=_strict_json(body)
        if time.monotonic()>=deadline:raise ValueError()
        return result
    except Exception:raise BridgeError('main_readonly_api_unavailable') from None
    finally:
        if connection is not None:connection.close()


class MainEngineProbe(DockerEngineProbe):
    """Fresh fixed main container/image and idle API proof, no credentials."""
    def __init__(self,expected_image,*,executable='/usr/bin/docker',endpoint='unix:///run/docker.sock',timeout_seconds=10):
        if type(expected_image) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',expected_image):
            raise BridgeError('immutable_main_image_required')
        super().__init__(executable,endpoint,timeout_seconds=timeout_seconds)
        self.expected_image=expected_image

    def __call__(self):
        try:
            deadline=time.monotonic()+self.timeout
            identity=self._engine_identity(deadline)
            value=_strict_json(self._output(['container','inspect','--format',MAIN_INSPECT_TEMPLATE,MAIN_CONTAINER_ID],deadline))
            if (type(value) is not dict or set(value)!={'id','name','image','running','pid','health'}
                    or value['id']!=MAIN_CONTAINER_ID or value['name']!=MAIN_CONTAINER_NAME or value['image']!=self.expected_image
                    or value['running'] is not True or type(value['pid']) is not int or value['pid']<=0
                    or value['health'] not in (None,'healthy')):raise ValueError()
            info=_main_http_json('/info',deadline)
            if (type(info) is not dict or info.get('engine')!='odx' or type(info.get('taskQueueCount')) is not int
                    or info['taskQueueCount']!=0):raise ValueError()
            for key in ('version','engineVersion'):
                item=info.get(key)
                if type(item) is not str or not 1<=len(item)<=120 or any(ord(char)<32 or ord(char)==127 for char in item):raise ValueError()
            tasks=_main_http_json('/task/list',deadline)
            if type(tasks) is not list or tasks:raise ValueError()
            self._uploads_idle(deadline);self._processing_idle(deadline)
            if self._engine_identity(deadline)!=identity:raise ValueError()
            if time.monotonic()>=deadline:raise ValueError()
            return identity
        except Exception:raise BridgeError('main_container_or_idle_proof_unavailable') from None

    def _uploads_idle(self,deadline):
        raw=self._output(['exec',MAIN_CONTAINER_ID,'/bin/ls','-A','/var/www/tmp'],deadline)
        if raw=='':return
        if raw!='.gitignore\n':raise BridgeError('main_unfinished_uploads')
        path='/var/www/tmp/.gitignore'
        if (self._output(['exec',MAIN_CONTAINER_ID,'/usr/bin/stat','-c','%F %s',path],deadline)!='regular file 1\n'
                or self._output(['exec',MAIN_CONTAINER_ID,'/bin/cat',path],deadline)!='*'):
            raise BridgeError('main_upload_placeholder_unproven')

    def _processing_idle(self,deadline):
        rows=self._output(['top',MAIN_CONTAINER_ID,'-eo','pid,args'],deadline).splitlines()
        if not rows or rows[0].split() not in (['PID','COMMAND'],['PID','ARGS']):raise BridgeError('main_process_inventory_unproven')
        server=wrapper=init=0;pids=set()
        for row in rows[1:]:
            parts=row.strip().split(None,1)
            if len(parts)!=2 or not parts[0].isdigit() or int(parts[0])<=0 or parts[0] in pids:
                raise BridgeError('main_process_inventory_unproven')
            pids.add(parts[0]);command=parts[1]
            if re.match(r'^(?:/usr/bin/)?node /var/www/index\.js(?: |$)',command):server+=1
            elif re.match(r'^(?:/bin/)?bash /usr/bin/node /var/www/index\.js(?: |$)',command):wrapper+=1
            elif re.match(r'^(?:/sbin/)?docker-init(?: |$)',command):init+=1
            else:raise BridgeError('main_processing_child_present')
        if server!=1 or wrapper>1 or init>1:raise BridgeError('main_process_inventory_unproven')


class LocalGate:
    """Existing canonical gate only; never provision, reserve or clear state."""
    def __init__(self,binding,*,engine_probe):
        if type(binding) is not BridgeBinding or not callable(engine_probe):raise BridgeError('fixed_local_gate_required')
        self.binding,self.engine_probe=binding,engine_probe;self.gate=None;self.pid=os.getpid();self.identity=None

    def __enter__(self):
        if self.pid!=os.getpid() or self.gate is not None:raise BridgeError('local_gate_owner_changed')
        root=canonical_gate_path(self.binding.engine_id)
        for path,directory in ((root,True),(root/'gate.lock',False),(root/'gate.json',False)):
            if any(item.is_symlink() for item in (path,*path.parents)):raise BridgeError('canonical_existing_gate_required')
            info=path.stat()
            if (not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
                    or info.st_uid!=os.geteuid() or (not directory and info.st_nlink!=1)
                    or info.st_mode & (0o077 if directory else 0o022)):
                raise BridgeError('canonical_existing_gate_required')
        try:
            self.gate=NodeGate(self.binding.engine_id).__enter__()
            self.identity=self._identity()
            self.check();return self
        except Exception:
            if self.gate is not None:self.gate.__exit__(None,None,None);self.gate=None
            raise BridgeError('local_gate_unavailable') from None

    def _identity(self):
        if self.pid!=os.getpid() or self.gate is None or self.gate._stream is None:raise BridgeError('local_gate_owner_changed')
        root=self.gate.root;lock=root/'gate.lock';state=root/'gate.json'
        if any(item.is_symlink() for item in (root,lock,state,*root.parents)):raise BridgeError('local_gate_identity_changed')
        r,fd,path,s=root.stat(),os.fstat(self.gate._stream.fileno()),lock.stat(),state.stat()
        if (not stat.S_ISDIR(r.st_mode) or r.st_uid!=os.geteuid() or r.st_mode & 0o077
                or not stat.S_ISREG(fd.st_mode) or fd.st_nlink!=1 or path.st_nlink!=1
                or fd.st_uid!=os.geteuid() or fd.st_mode & 0o022 or os.get_inheritable(self.gate._stream.fileno())
                or (fd.st_dev,fd.st_ino)!=(path.st_dev,path.st_ino)
                or not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or s.st_uid!=os.geteuid() or s.st_mode & 0o022):
            raise BridgeError('local_gate_identity_changed')
        return dict(pid=self.pid,rootDevice=r.st_dev,rootInode=r.st_ino,lockDevice=fd.st_dev,lockInode=fd.st_ino,
                    stateDevice=s.st_dev,stateInode=s.st_ino)

    def check(self):
        if self.pid!=os.getpid():raise BridgeError('local_gate_owner_changed')
        if self._identity()!=self.identity or helper_digest()!=self.binding.helper_sha256:raise BridgeError('local_gate_identity_changed')
        if self.engine_probe()!=self.binding.engine_id:raise BridgeError('physical_docker_engine_changed')
        state=self.gate.snapshot();expected=self.binding.public()
        if (any(state.get(key)!=expected[key] for key in ('engineId','nodeId','authorityId','coordinatorOrigin','generation'))
                or state.get('reservation') is not None):raise BridgeError('local_gate_binding_or_idle_state_changed')
        return None

    def __exit__(self,*args):
        if self.pid!=os.getpid():raise BridgeError('local_gate_owner_changed')
        if self.gate is not None:self.gate.__exit__(*args);self.gate=None


def _common(binding,operation_id,session_nonce,holder_nonce,identity):
    return dict(schemaVersion=1,operationId=operation_id,sessionNonce=session_nonce,holderNonce=holder_nonce,
                binding=binding.public(),gateIdentity=identity)


def _validate(frame,binding,operation_id,session_nonce,kind,*,base=None,sequence=None,nonce=None):
    keys={'schemaVersion','type','operationId','sessionNonce','holderNonce','binding','gateIdentity'}
    if kind!='main-gate-ready':keys|={'sequence','nonce'}
    try:
        if type(frame) is not dict or set(frame)!=keys or type(frame['schemaVersion']) is not int or frame['schemaVersion']!=1:
            raise ValueError()
        if (frame['type']!=kind or frame['operationId']!=operation_id or frame['sessionNonce']!=session_nonce
                or frame['binding']!=binding.public()):raise ValueError()
        if type(frame['binding']) is not dict or type(frame['binding']['generation']) is not int:raise ValueError()
        _hash(frame['holderNonce'])
        identity=frame['gateIdentity']
        if (type(identity) is not dict or set(identity)!={'pid','rootDevice','rootInode','lockDevice','lockInode','stateDevice','stateInode'}
                or any(type(value) is not int or not 0<=value<2**128 for value in identity.values()) or identity['pid']<1):raise ValueError()
        if base and any(frame[key]!=base[key] for key in ('holderNonce','gateIdentity')):raise ValueError()
        if kind!='main-gate-ready':
            _hash(frame['nonce'])
            if type(frame['sequence']) is not int or not 1<=frame['sequence']<=2**53-1:raise ValueError()
            if sequence is not None and frame['sequence']!=sequence:raise ValueError()
            if nonce is not None and frame['nonce']!=nonce:raise ValueError()
        return frame
    except Exception:raise BridgeError('bridge_proof_binding_mismatch') from None


def serve_main(reader,writer,*,binding,operation_id,session_nonce,engine_probe,timeout_seconds=10):
    if binding.node_id!='main':raise BridgeError('main_holder_binding_required')
    _operation(operation_id);_hash(session_nonce)
    channel=_Pipe(reader,writer,timeout=timeout_seconds)
    with LocalGate(binding,engine_probe=engine_probe) as gate:
        base=_common(binding,operation_id,session_nonce,secrets.token_hex(32),gate.identity)
        gate.check();channel.write(dict(base,type='main-gate-ready'),time.monotonic()+timeout_seconds)
        sequence=0;previous=None
        while True:
            try:request=channel.read()
            except BridgeEOF:return
            _validate(request,binding,operation_id,session_nonce,'main-gate-check',base=base,sequence=sequence+1)
            if request['nonce']==previous:raise BridgeError('bridge_nonce_reused')
            gate.check();sequence=request['sequence'];previous=request['nonce']
            channel.write(dict(request,type='main-gate-checked'),time.monotonic()+timeout_seconds)


class BridgeGateClient:
    def __init__(self,reader,writer,*,binding,operation_id,session_nonce,timeout_seconds=10,nonce_factory=None):
        if type(binding) is not BridgeBinding or binding.node_id!='main':raise BridgeError('main_bridge_binding_required')
        _operation(operation_id);_hash(session_nonce)
        self.binding,self.operation_id,self.session_nonce=binding,operation_id,session_nonce
        self.timeout=timeout_seconds;self.channel=_Pipe(reader,writer,timeout=timeout_seconds)
        self.nonce_factory=nonce_factory or (lambda:secrets.token_hex(32))
        self.pid=os.getpid();self.base=None;self.sequence=0;self.previous=None;self.failed=False;self.closed=False
        self._mutex=threading.Lock()

    def __enter__(self):
        self._owner()
        if self.base is not None:raise BridgeError('bridge_client_already_entered')
        try:
            if helper_digest()!=self.binding.helper_sha256:raise BridgeError('bridge_helper_source_changed')
            self.base=_validate(self.channel.read(time.monotonic()+self.timeout),self.binding,self.operation_id,
                self.session_nonce,'main-gate-ready')
            self.check();return self
        except Exception:self.failed=True;raise

    def _owner(self):
        if self.pid!=os.getpid() or self.failed or self.closed:raise BridgeError('bridge_client_owner_or_transport_lost')

    def check(self):
        self._owner()
        with self._mutex:
            self._owner()
            if self.base is None:raise BridgeError('bridge_client_not_entered')
            try:
                if helper_digest()!=self.binding.helper_sha256:raise BridgeError('bridge_helper_source_changed')
                nonce=self.nonce_factory();_hash(nonce)
                if nonce==self.previous:raise BridgeError('bridge_nonce_reused')
                sequence=self.sequence+1;deadline=time.monotonic()+self.timeout
                request=dict(self.base,type='main-gate-check',sequence=sequence,nonce=nonce)
                self.channel.write(request,deadline)
                _validate(self.channel.read(deadline),self.binding,self.operation_id,self.session_nonce,'main-gate-checked',
                    base=self.base,sequence=sequence,nonce=nonce)
                if time.monotonic()>=deadline:raise BridgeError('bridge_absolute_deadline_exceeded')
                self.sequence=sequence;self.previous=nonce
            except Exception:self.failed=True;raise

    def __exit__(self,*_):
        if self.pid!=os.getpid():raise BridgeError('bridge_client_owner_or_transport_lost')
        if _ and _[0] is None and not self.failed and self.base is not None:
            try:
                self.channel.write(dict(self.base,type='main-gate-close',sequence=self.sequence,nonce=self.previous),
                    time.monotonic()+self.timeout)
            except Exception:self.failed=True;raise
        self.closed=True


class GateSet:
    def __init__(self,local_context,remote_client):
        if local_context.binding.node_id!='cluster' or remote_client.binding.node_id!='main':raise BridgeError('both_fixed_gate_scopes_required')
        if (local_context.binding.authority_id,local_context.binding.coordinator_origin)!=(remote_client.binding.authority_id,remote_client.binding.coordinator_origin):
            raise BridgeError('gate_coordinator_binding_mismatch')
        self.local,self.remote=local_context,remote_client;self.stack=None;self.pid=os.getpid()
        self.binding=dict(cluster=local_context.binding.public(),main=remote_client.binding.public())

    def __enter__(self):
        if self.pid!=os.getpid() or self.stack is not None:raise BridgeError('gate_set_owner_changed')
        stack=ExitStack()
        try:stack.enter_context(self.local);stack.enter_context(self.remote);self.stack=stack;self.check();return self
        except Exception:stack.close();self.stack=None;raise

    def check(self):
        if self.pid!=os.getpid() or self.stack is None:raise BridgeError('gate_set_not_held')
        self.local.check();self.remote.check();self.local.check()

    def __exit__(self,*args):
        if self.pid!=os.getpid():raise BridgeError('gate_set_owner_changed')
        if self.stack is not None:self.stack.__exit__(*args);self.stack=None


class RootRelay:
    """Relay only executor-requested challenges between actual owned children."""
    def __init__(self,main_process,cluster_process,*,binding,operation_id,session_nonce,timeout_seconds=10):
        _operation(operation_id);_hash(session_nonce)
        self.main,self.cluster=main_process,cluster_process;self.binding=binding
        self.operation_id,self.session_nonce,self.timeout=operation_id,session_nonce,timeout_seconds
        self.pid=os.getpid();self.used=False;self._cluster_exit_seen=None
        if binding.node_id!='main' or main_process is cluster_process:raise BridgeError('independent_ssh_children_required')
        self.main_pipe=_Pipe(main_process.stdout,main_process.stdin,timeout=timeout_seconds)
        self.cluster_pipe=_Pipe(cluster_process.stdout,cluster_process.stdin,timeout=timeout_seconds)

    def _main_alive(self):
        if self.pid!=os.getpid() or self.main.poll() is not None:raise BridgeError('main_holder_process_lost')

    def _alive(self):
        self._main_alive()
        if self.cluster.poll() is not None:raise BridgeError('cluster_executor_process_exited')

    def _cluster_reader_alive(self):
        self._main_alive()
        if self.cluster.poll() is not None:
            if self._cluster_exit_seen is None:self._cluster_exit_seen=time.monotonic()
            if time.monotonic()-self._cluster_exit_seen>=self.timeout:raise BridgeError('cluster_executor_pipe_not_closed')

    def _finish_cluster(self,deadline):
        # A valid close frame is required, then no extra stdout, actual exit 0,
        # and a still-live holder. EOF alone or a saved status is never enough.
        try:self.cluster_pipe.line(deadline,self._main_alive)
        except BridgeEOF:pass
        else:raise BridgeError('unexpected_executor_stdout_after_close')
        while self.cluster.poll() is None:
            self._main_alive()
            if time.monotonic()>=deadline:raise BridgeError('cluster_executor_exit_deadline_exceeded')
            time.sleep(.01)
        self._main_alive()
        if self.cluster.returncode!=0:raise BridgeError('cluster_executor_process_failed')

    def run(self):
        if self.pid!=os.getpid() or self.used:raise BridgeError('relay_cannot_be_replayed')
        self.used=True;success=False
        try:
            if helper_digest()!=self.binding.helper_sha256:raise BridgeError('bridge_helper_source_changed')
            base=_validate(self.main_pipe.read(time.monotonic()+self.timeout,self._alive),self.binding,self.operation_id,
                self.session_nonce,'main-gate-ready')
            self.cluster_pipe.write(base,time.monotonic()+self.timeout,self._alive)
            sequence=0;previous=None
            while True:
                # Idle is permitted while the executor does offline work. Its
                # process exit is observed locally; no heartbeat is injected.
                request=self.cluster_pipe.read(monitor=self._cluster_reader_alive)
                if request.get('type')=='main-gate-close':
                    _validate(request,self.binding,self.operation_id,self.session_nonce,'main-gate-close',base=base,
                        sequence=sequence,nonce=previous)
                    if sequence<1:raise BridgeError('no_live_gate_challenge_completed')
                    self._finish_cluster(time.monotonic()+self.timeout)
                    success=True;return 0
                _validate(request,self.binding,self.operation_id,self.session_nonce,'main-gate-check',base=base,sequence=sequence+1)
                if request['nonce']==previous:raise BridgeError('bridge_nonce_reused')
                deadline=time.monotonic()+self.timeout
                self.main_pipe.write(request,deadline,self._alive)
                reply=_validate(self.main_pipe.read(deadline,self._alive),self.binding,self.operation_id,self.session_nonce,
                    'main-gate-checked',base=base,sequence=request['sequence'],nonce=request['nonce'])
                self.cluster_pipe.write(reply,deadline,self._alive)
                sequence=request['sequence'];previous=request['nonce']
        finally:
            # Close actual child stdin: main holder observes EOF and drops only
            # its own OS context. Never send a remote unlock/resolve command.
            for process in (self.cluster,self.main):
                try:process.stdin.close()
                except Exception:pass
            if not success and self.cluster.poll() is None:
                try:self.cluster.terminate()
                except OSError:pass
