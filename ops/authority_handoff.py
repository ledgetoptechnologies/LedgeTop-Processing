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


def _read_private(path,maximum,*,details=False):
    import stat
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    try:
        before=os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid()
                or before.st_mode&0o077 or before.st_size>maximum):raise HandoffHeld('handoff_record_invalid')
        chunks=[];total=0
        while True:
            chunk=os.read(fd,min(4096,maximum-total+1))
            if not chunk:break
            total+=len(chunk)
            if total>maximum:raise HandoffHeld('handoff_record_invalid')
            chunks.append(chunk)
        after=os.fstat(fd);named=path.lstat()
        def identity(x):return (x.st_dev,x.st_ino,x.st_mode,x.st_uid,x.st_gid,x.st_nlink,x.st_size,x.st_mtime_ns,x.st_ctime_ns)
        if total!=before.st_size or identity(before)!=identity(after) or identity(after)!=identity(named):
            raise HandoffHeld('handoff_record_changed')
        result=b''.join(chunks)
        return (result,identity(after)) if details else result
    finally:os.close(fd)


def pending_status(lease_directory,authority,attestor):
    """Inspect a pending handoff; never starts a candidate or clears a hold."""
    import fcntl,re,stat
    directory=Path(lease_directory)
    if (type(attestor) is not DockerProcessAttestor or type(authority) is not str or
        not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',authority) or
        not directory.is_absolute() or directory.resolve()!=directory):
        raise HandoffHeld('pending_identity_invalid')
    try:
        info=directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
            raise HandoffHeld('pending_directory_invalid')
        path=directory/(authority+'.handoff-intent')
        raw,intent_identity=_read_private(path,4096,details=True)
        def unique(pairs):
            result={}
            for key,value in pairs:
                if key in result:raise ValueError()
                result[key]=value
            return result
        intent=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        keys={'version','operationId','authorityId','pid','startTime','containerId','imageId','leaseDevice','leaseInode'}
        if (type(intent) is not dict or set(intent)!=keys or type(intent['version']) is not int or intent['version']!=1 or intent['authorityId']!=authority
            or intent['containerId']!=attestor.container or intent['imageId']!=attestor.image
            or type(intent['pid']) is not int or intent['pid']<=0 or type(intent['startTime']) is not int
            or type(intent['leaseDevice']) is not int or type(intent['leaseInode']) is not int
            or type(intent['operationId']) is not str or not re.fullmatch('[0-9a-f]{32}',intent['operationId'])
            or json.dumps(intent,sort_keys=True,separators=(',',':')).encode()!=raw):
            raise HandoffHeld('pending_intent_invalid')
        ack_path=Path(str(path)+'.ack')
        try:ack_record=_read_private(ack_path,65,details=True)
        except FileNotFoundError:ack_record=None
        ack=None if ack_record is None else ack_record[0]
        if ack is not None and ack!=hashlib.sha256(raw).hexdigest().encode()+b'\n':
            raise HandoffHeld('pending_ack_invalid')
        lease_path=directory/(authority+'.lease')
        fd=os.open(lease_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
        try:
            held=os.fstat(fd);named=lease_path.lstat()
            if (not stat.S_ISREG(held.st_mode) or held.st_nlink!=1 or
                (held.st_dev,held.st_ino)!=(named.st_dev,named.st_ino) or
                (held.st_dev,held.st_ino)!=(intent['leaseDevice'],intent['leaseInode'])):
                raise HandoffHeld('pending_lease_changed')
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:lock='held'
            else:lock='free'
        finally:os.close(fd)
        candidate='unverified'
        try:
            pidfd=os.pidfd_open(intent['pid'],0)
            try:
                if attestor.inspect(intent['pid'])==intent['startTime'] and not select.select([pidfd],[],[],0)[0]:
                    candidate='matching_live'
            finally:os.close(pidfd)
        except Exception:pass
        if _read_private(path,4096,details=True)!=(raw,intent_identity):raise HandoffHeld('handoff_record_changed')
        try:current_ack=_read_private(ack_path,65,details=True)
        except FileNotFoundError:current_ack=None
        if current_ack!=ack_record:raise HandoffHeld('handoff_ack_changed')
        return {'state':'held','ack':'present' if ack is not None else 'absent','lease':lock,'candidate':candidate,
                'startAuthorized':False,'retryAuthorized':False,'cleanupAuthorized':False}
    except HandoffHeld:raise
    except Exception:raise HandoffHeld('pending_inspection_unavailable') from None


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
