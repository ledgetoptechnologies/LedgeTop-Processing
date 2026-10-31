"""Unqualified authority FD handoff primitives. No runtime activation.

The supervisor must retain its original OperationFence after any outcome.
This protocol does not fence host administrators, old writers or Docker users.
"""
import array
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import time


class HandoffHeld(ValueError):pass


def _peer(sock):
    if sock.family!=socket.AF_UNIX or not hasattr(socket,'SO_PEERCRED'):raise HandoffHeld('unix_peer_required')
    return struct.unpack('3i',sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,struct.calcsize('3i')))


def _starttime(pid):
    raw=Path('/proc',str(pid),'stat').read_text()
    return int(raw[raw.rfind(')')+2:].split()[19])


class DockerProcessAttestor:
    """Host-only exact identity check; Docker/root remain trusted authorities."""
    def __init__(self,container_id,image_id,docker_config):
        import re
        if not re.fullmatch('[0-9a-f]{64}',container_id) or not re.fullmatch('sha256:[0-9a-f]{64}',image_id):
            raise HandoffHeld('exact_docker_identity_required')
        self.container=container_id;self.image=image_id;self.config=str(docker_config)

    def inspect(self,pid):
        env={k:v for k,v in os.environ.items() if not k.upper().startswith('DOCKER_')}
        env['DOCKER_CONFIG']=self.config
        fmt='{{json .Id}} {{json .Image}} {{json .State.Pid}} {{json .State.Running}}'
        try:
            result=subprocess.run(['/usr/bin/docker','--host','unix:///run/docker.sock','inspect','--format',fmt,self.container],
                                  env=env,capture_output=True,timeout=5,check=True,text=True)
            values=[json.loads(x) for x in result.stdout.split()]
            if values!=[self.container,self.image,pid,True]:raise ValueError()
            path=Path('/proc',str(pid),'cgroup').read_text()
            if not any(part in (self.container,'docker-'+self.container+'.scope')
                       for line in path.splitlines() for part in line.rsplit(':',1)[-1].split('/')):raise ValueError()
            return _starttime(pid)
        except Exception:raise HandoffHeld('candidate_identity_unavailable') from None


def _write_once(path,payload):
    parent=path.parent
    info=parent.lstat()
    import stat
    if (not path.is_absolute() or parent.resolve()!=parent or not stat.S_ISDIR(info.st_mode)
            or info.st_uid!=os.geteuid() or info.st_mode&0o077):
        raise HandoffHeld('private_intent_directory_required')
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    try:
        view=memoryview(payload)
        while view:
            n=os.write(fd,view)
            if n<=0:raise OSError('write')
            view=view[n:]
        os.fsync(fd)
    finally:os.close(fd)
    parent=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(parent)
    finally:os.close(parent)


def offer(sock,fence,attestor,intent_path,*,operation_id,expected_uid):
    """Send once after durable intent; retain fence on success and all errors."""
    from ops.pr197_operation_fence import OperationFence
    if type(fence) is not OperationFence or type(attestor) is not DockerProcessAttestor or type(expected_uid) is not int:
        raise HandoffHeld('qualified_components_required')
    if Path(intent_path)!=fence.leases/(fence.authority+'.handoff-intent'):
        raise HandoffHeld('authority_intent_path_required')
    if type(operation_id) is not str or len(operation_id)!=32 or any(x not in '0123456789abcdef' for x in operation_id):
        raise HandoffHeld('operation_id_required')
    pid,uid,_=_peer(sock)
    if uid!=expected_uid or pid<=0 or not hasattr(os,'pidfd_open'):raise HandoffHeld('candidate_peer_unavailable')
    pidfd=os.pidfd_open(pid,0)
    try:
        start=attestor.inspect(pid);fence.check()
        if select.select([pidfd],[],[],0)[0]:raise HandoffHeld('candidate_exited')
        lease=os.fstat(fence.fds[0])
        value={'version':1,'operationId':operation_id,'authorityId':fence.authority,'pid':pid,
               'startTime':start,'containerId':attestor.container,'imageId':attestor.image,
               'leaseDevice':lease.st_dev,'leaseInode':lease.st_ino}
        payload=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
        if len(payload)>4096:raise HandoffHeld('intent_too_large')
        _write_once(Path(intent_path),payload)
        fence.check()
        if attestor.inspect(pid)!=start or select.select([pidfd],[],[],0)[0]:raise HandoffHeld('candidate_changed')
        digest=hashlib.sha256(payload).hexdigest()
        sock.settimeout(5)
        sent=sock.sendmsg([len(payload).to_bytes(4,'big')+payload],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[fence.fds[0]]))])
        if sent!=len(payload)+4:raise HandoffHeld('handoff_uncertain')
        ack=b'';deadline=time.monotonic()+5
        while len(ack)<65:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise HandoffHeld('handoff_uncertain')
            sock.settimeout(remaining)
            part=sock.recv(65-len(ack))
            if not part:raise HandoffHeld('handoff_uncertain')
            ack+=part
        if ack!=digest.encode()+b'\n':raise HandoffHeld('handoff_uncertain')
        fence.check()
        if attestor.inspect(pid)!=start or select.select([pidfd],[],[],0)[0]:raise HandoffHeld('candidate_changed')
        _write_once(Path(str(intent_path)+'.ack'),digest.encode()+b'\n')
        sock.sendall(b'GO\n')
        # GO delivery itself can be ambiguous. Never release the fence or
        # retry based on this return; caller must reconcile the candidate.
        return {'state':'handoff_outcome_unknown','intentSha256':digest}
    except HandoffHeld:raise
    except Exception:raise HandoffHeld('handoff_uncertain') from None
    finally:os.close(pidfd)
