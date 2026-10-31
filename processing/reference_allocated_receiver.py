"""Opt-in Linux indexed input receiver; not qualified for deployment.

The ledger latch does not fence already-running old writers, and NodeODM's own
working space is not reserved. Trusted, exclusively controlled ancestors and
filesystem allocation semantics are required. Uncertain artifacts are retained.
"""
import hashlib
import fcntl
import json
import os
import re
import stat
from pathlib import Path


def _mount_id(fd):
    with open('/proc/self/fdinfo/'+str(fd),encoding='ascii') as stream:
        values=[line.split(':',1)[1].strip() for line in stream if line.startswith('mnt_id:')]
    if len(values)!=1 or not values[0].isdecimal():
        raise ValueError('mount identity unavailable')
    return int(values[0])


def root_proof(root):
    """Record a bounded path identity, not an exclusive-writer authority."""
    root=Path(root)
    if not root.is_absolute() or str(root)!=str(root.resolve()) or len(str(root).encode())>4096:
        raise ValueError('literal scratch root required')
    parts=list(reversed((root,*root.parents)))
    if len(parts)>128:raise ValueError('scratch ancestry bound exceeded')
    result=[]
    for path in parts:
        fd=os.open(path,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info=os.fstat(fd);visible=path.lstat()
            fields=lambda x:(x.st_dev,x.st_ino,x.st_mode,x.st_uid,x.st_gid)
            if not stat.S_ISDIR(info.st_mode) or fields(info)!=fields(visible):
                raise ValueError('scratch ancestry changed')
            result.append([str(path),*fields(info),_mount_id(fd)])
        finally:os.close(fd)
    if len(json.dumps(result,separators=(',',':')).encode())>16384:
        raise ValueError('scratch ancestry record bound exceeded')
    return result


def validate_root_proof(proof,root):
    if (type(proof) is not list or not 1<=len(proof)<=128
            or any(type(item) is not list or len(item)!=7 or type(item[0]) is not str
                   or any(type(value) is not int or value<0 for value in item[1:]) for item in proof)
            or proof!=root_proof(root)):
        raise ValueError('scratch root identity changed')


def receive(ledger, broker, grant, reservation, object_id, attempt, identity, allocated_bytes):
    if type(attempt) is not str or re.fullmatch('[0-9a-f]{32}',attempt) is None:
        raise ValueError('exact indexed attempt required')
    if os.name != 'posix' or not hasattr(os, 'posix_fallocate') or not hasattr(os, 'pread'):
        raise ValueError('Linux allocated receiver required')
    identity=dict(identity)
    if ledger.allocated_receiver_version==2:
        if 'rootProof' in identity:raise ValueError('receiver owns root proof')
        identity['rootProof']=root_proof(ledger.root)
    target=ledger.root / identity['path']
    ledger._owned_path(reservation,identity['path'])
    parent=target.parent
    if not parent.is_dir() or parent.resolve()!=parent:
        raise ValueError('private input directory required')
    directory=os.open(parent,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    fd=None
    part='.allocated-'+attempt+'.part'
    try:
        info=os.fstat(directory)
        mount=_mount_id(directory)
        if info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)&0o077:
            raise ValueError('private input directory required')
        ledger.begin_allocated_input(reservation,object_id,attempt,identity)
        fd=os.open(part,os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW,0o600,dir_fd=directory)
        initial=os.fstat(fd)
        fcntl.flock(fd,fcntl.LOCK_EX | fcntl.LOCK_NB)
        file_mount=_mount_id(fd)
        if file_mount!=mount: raise ValueError('input mount changed')
        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink!=1:
            raise ValueError('exclusive regular input required')
        if identity['byteSize']:
            os.posix_fallocate(fd,0,identity['byteSize'])
        os.fsync(fd); os.fsync(directory)
        ledger.record_allocated_input_inode(reservation,object_id,attempt,initial.st_dev,initial.st_ino,file_mount,
                                             info.st_dev,info.st_ino,mount)
        broker.download_indexed_object_fd(grant,object_id,fd)
        current=os.fstat(fd)
        if ((current.st_dev,current.st_ino,current.st_size,current.st_nlink) !=
            (initial.st_dev,initial.st_ino,identity['byteSize'],1)):
            raise ValueError('allocated inode changed')
        if _mount_id(fd)!=file_mount or _mount_id(directory)!=mount:
            raise ValueError('input mount changed')
        digest=hashlib.sha256(); offset=0
        while offset<identity['byteSize']:
            chunk=os.pread(fd,min(1024*1024,identity['byteSize']-offset),offset)
            if not chunk: raise ValueError('allocated input truncated')
            digest.update(chunk); offset+=len(chunk)
        if digest.hexdigest()!=identity['sha256']:
            raise ValueError('allocated input digest mismatch')
        named_fd=os.open(parent,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            named=os.fstat(named_fd)
            if (named.st_dev,named.st_ino,_mount_id(named_fd))!=(info.st_dev,info.st_ino,mount):
                raise ValueError('input directory changed')
        finally:
            os.close(named_fd)
        part_info=os.stat(part,dir_fd=directory,follow_symlinks=False)
        if (part_info.st_dev,part_info.st_ino)!=(initial.st_dev,initial.st_ino):
            raise ValueError('allocated pathname changed')
        if ledger.allocated_receiver_version==2:
            validate_root_proof(identity['rootProof'],ledger.root)
            ledger.checkpoint_allocated_publication(reservation,object_id,attempt,'allocated','publishing')
        os.link(part,target.name,src_dir_fd=directory,dst_dir_fd=directory,follow_symlinks=False)
        os.fsync(directory)
        linked=os.stat(target.name,dir_fd=directory,follow_symlinks=False)
        if (linked.st_dev,linked.st_ino)!=(initial.st_dev,initial.st_ino):
            raise ValueError('published input differs')
        if ledger.allocated_receiver_version==2:
            validate_root_proof(identity['rootProof'],ledger.root)
            ledger.checkpoint_allocated_publication(reservation,object_id,attempt,'publishing','linked')
        os.unlink(part,dir_fd=directory); os.fsync(directory)
        if ledger.allocated_receiver_version==2:
            validate_root_proof(identity['rootProof'],ledger.root)
            ledger.checkpoint_allocated_publication(reservation,object_id,attempt,'linked','unlinked')
        ledger.finish_allocated_input(reservation,object_id,attempt,initial.st_dev,initial.st_ino,file_mount,
                                      info.st_dev,info.st_ino,mount,allocated_bytes)
    finally:
        if fd is not None: os.close(fd)
        os.close(directory)
